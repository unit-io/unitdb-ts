// Sessions: batches, resuming a session from its log, and reliable
// messages across a dropped link, a server restart and a client restart.
// unitdb-go's TestE2EBatchPublish, TestE2EResumeUnacknowledgedPublish and
// TestE2EResumeNotificationTheServerForgot, ported, and more.

import { create } from '@bufbuild/protobuf'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import { connect } from '../../src/client.js'
import { FlowControl, MessageType } from '../../src/codec/frame.js'
import { control, encodeMessage } from '../../src/codec/messages.js'
import { PublishMessageSchema, PublishSchema } from '../../src/gen/unitdb_pb.js'
import { fileStore } from '../../src/session/file.js'
import { memoryStore, type SessionLog, type SessionStore } from '../../src/session/store.js'
import { tcp } from '../../src/transport/tcp.js'
import { webSocket } from '../../src/transport/ws.js'
import { collect, e2e, expectPayloads, isPublishOn, mintId, newClient, newContract, RawConn, sleep, transports, waitUntil } from './harness.js'
import { Server } from './server.js'

const skip = e2e.skip !== ''
const dirs: string[] = []
const tmp = () => {
  const d = mkdtempSync(join(tmpdir(), 'unitdb-session-'))
  dirs.push(d)
  return d
}
afterEach(() => {
  for (const d of dirs.splice(0)) rmSync(d, { recursive: true, force: true })
})

let keys = 500_000 + Math.floor(Math.random() * 100_000)

/** A store that lets the test see the log a client has open. */
function watchedStore(inner: SessionStore = memoryStore()): SessionStore & { log: SessionLog | null } {
  const s = {
    log: null as SessionLog | null,
    open(clientId: string, sessionKey: number) {
      s.log = inner.open(clientId, sessionKey)
      return s.log
    },
  }
  return s
}

describe.skipIf(skip).each(Object.keys(transports) as (keyof typeof transports)[])('over %s', (name) => {
  const transport = transports[name]

  it('publishes in batches (TestE2EBatchPublish)', async () => {
    const c = await newClient(transport(), mintId(), { batchDuration: 100 })
    const got = collect(c.messages('e2e.batch'))
    await c.subscribe('e2e.batch')
    await Promise.all(Array.from({ length: 5 }, (_, i) => c.publish('e2e.batch', `batch-${i}`, { delivery: 'batch' })))
    await expectPayloads(got, 'batch-0', 'batch-1', 'batch-2', 'batch-3', 'batch-4')
  })

  it('sends a batch early past its count threshold', async () => {
    const c = await newClient(transport(), mintId(), { batchDuration: 60_000, batchCountThreshold: 3 })
    const got = collect(c.messages('e2e.full'))
    await c.subscribe('e2e.full')
    const sent = Promise.all(Array.from({ length: 4 }, (_, i) => c.publish('e2e.full', `full-${i}`, { delivery: 'batch' })))
    await sent // a minute's batch went with its fourth message
    await expectPayloads(got, 'full-0', 'full-1', 'full-2', 'full-3')
  })

  it('delays a publish', async () => {
    const c = await newClient(transport(), mintId(), { batchDuration: 50 })
    const got = collect(c.messages('e2e.delay'))
    await c.subscribe('e2e.delay')
    const start = Date.now()
    await c.publish('e2e.delay', 'later', { delay: 500 })
    await expectPayloads(got, 'later')
    expect(Date.now() - start).toBeGreaterThanOrEqual(450)
  })

  it('sends the batches waiting when it closes', async () => {
    const contract = newContract()
    const subscriber = await RawConn.connect(mintId({ contract }))
    subscriber.subscribe('e2e.closing')
    await sleep(100)
    const c = await connect({ servers: transport(), clientId: mintId({ contract }), insecure: true, batchDuration: 60_000 })
    const p = c.publish('e2e.closing', 'flushed', { delivery: 'batch' })
    p.catch(() => {}) // closing answers it NotConnected: its acknowledgement doesn't come back in time
    await c.close()
    await subscriber.waitFor('the batch', isPublishOn('e2e.closing'))
  })

  it('resumes a publish a previous run left unacknowledged (TestE2EResumeUnacknowledgedPublish)', async () => {
    const contract = newContract()
    const subscriber = await RawConn.connect(mintId({ contract }))
    subscriber.subscribe('e2e.resume', 1)
    await sleep(100)
    const clientId = mintId({ contract })
    const store = fileStore(tmp())
    const sessionKey = keys++
    // A previous run of the client left a publish the server never acknowledged.
    const prev = store.open(clientId, sessionKey)
    prev.put(
      'o9',
      encodeMessage({
        kind: 'publish',
        msg: create(PublishSchema, { MessageID: 9, Messages: [create(PublishMessageSchema, { Topic: 'e2e.resume', Payload: new TextEncoder().encode('unfinished') })] }),
      }),
    )
    prev.close()

    const c = await newClient(transport(), clientId, { store, sessionKey })
    const m = await subscriber.waitFor('resumed publish', isPublishOn('e2e.resume'))
    if (m.kind !== 'publish') throw new Error(m.kind)
    expect(new TextDecoder().decode(m.msg.Messages[0]!.Payload)).toBe('unfinished')
    // The server's acknowledgement of the resumed message is handled, and the next publish's too.
    await c.publish('e2e.resume', 'next')
    await c.close()
    const after = store.open(clientId, sessionKey)
    expect(after.entries()).toEqual([])
    after.close()
  })

  it('forgets a notification the server no longer has (TestE2EResumeNotificationTheServerForgot)', async () => {
    const clientId = mintId()
    const store = watchedStore()
    const sessionKey = keys++
    const prev = store.open(clientId, sessionKey)
    prev.put('i55', encodeMessage(control(MessageType.PUBLISH, FlowControl.NOTIFY, 55)))
    prev.close()

    const lost: Error[] = []
    const c = await newClient(transport(), clientId, { store, sessionKey, onConnectionLost: (e) => lost.push(e) })
    // The server completes the flow, the client forgets it and stays connected.
    await waitUntil('the stale notification to go', () => store.log!.entries().length === 0)
    expect(lost).toEqual([])
    await c.publish('e2e.stale', 'still connected')
  })

  it('resumes reliable publishes a client closed before they were acknowledged, in its next run', async () => {
    // A server of its own, frozen so that nothing is acknowledged.
    const s = await Server.create(e2e.bin, e2e.dir, { allowInsecure: true })
    await s.start()
    try {
      const contract = newContract()
      const clientId = mintId({ contract, conf: s.info.conf })
      const store = fileStore(tmp())
      const sessionKey = keys++
      const t = name === 'tcp' ? tcp(s.info.tcpAddr) : webSocket(s.info.wsUrl)
      const first = await connect({ servers: t, clientId, insecure: true, store, sessionKey, writeTimeout: 0 })
      s.signal('SIGSTOP')
      const calls = Array.from({ length: 5 }, (_, i) => first.publish('e2e.restart', `r${i}`, { delivery: 'reliable', ttl: '1h' }).catch(() => {}))
      await sleep(200)
      await first.close() // the calls fail; the log keeps them
      await Promise.all(calls)
      s.signal('SIGCONT')
      await sleep(200)

      // The next run of the client resumes them; a relay finds them all.
      const second = await connect({ servers: t, clientId, insecure: true, store, sessionKey })
      await sleep(500)
      const reader = await connect({ servers: tcp(s.info.tcpAddr), clientId: mintId({ contract, conf: s.info.conf }), insecure: true })
      const got = collect(reader.messages('e2e.restart'))
      await reader.relay(['e2e.restart'], { last: '1h' })
      await expectPayloads(got, 'r0', 'r1', 'r2', 'r3', 'r4')
      await reader.close()
      await second.close()
    } finally {
      await s.remove()
    }
  })

  // A server restart, clean (SIGINT: the server flushes), mid-stream. A
  // standalone server killed hard (SIGKILL) loses the reliable messages it
  // acknowledged but hadn't written yet: its durability, not the client's
  // (the server's durable acknowledgements are their own work).
  it('loses no reliable message across a server restart (autoReconnect)', async () => {
    await acrossAnInterruption(name, async (s) => {
      await s.stop()
      await s.start()
    })
  })

  // A dropped link: the server frozen past the ping timeout, so the client
  // drops the connection and connects again once it answers.
  it('loses no reliable message across a dropped link (autoReconnect)', async () => {
    await acrossAnInterruption(name, async (s) => {
      s.signal('SIGSTOP')
      await sleep(1500)
      s.signal('SIGCONT')
    }, { keepAlive: 300, pingTimeout: 500 })
  })
})

/** acrossAnInterruption publishes 20 reliable messages with interrupt halfway, and relays them all. */
async function acrossAnInterruption(name: string, interrupt: (s: Server) => Promise<void>, opts: { keepAlive?: number; pingTimeout?: number } = {}): Promise<void> {
  const s = await Server.create(e2e.bin, e2e.dir, { allowInsecure: true })
  await s.start()
  try {
    const contract = newContract()
    const clientId = mintId({ contract, conf: s.info.conf })
    const t = name === 'tcp' ? tcp(s.info.tcpAddr) : webSocket(s.info.wsUrl)
    const connects: number[] = []
    const c = await connect({
      servers: t,
      clientId,
      insecure: true,
      sessionKey: keys++,
      autoReconnect: true,
      maxReconnectInterval: 300,
      writeTimeout: 30_000,
      onConnect: () => connects.push(Date.now()),
      ...opts,
    })
    const n = 20
    const calls: Promise<void>[] = []
    for (let i = 0; i < n; i++) {
      calls.push(c.publish('e2e.across', `m${i}`, { delivery: 'reliable', ttl: '1h' }))
      if (i === n / 2) await interrupt(s)
    }
    await Promise.all(calls) // every one acknowledged, after the reconnect
    expect(connects.length).toBeGreaterThanOrEqual(2)
    const reader = await connect({ servers: tcp(s.info.tcpAddr), clientId: mintId({ contract, conf: s.info.conf }), insecure: true })
    const got = collect(reader.messages('e2e.across'))
    await reader.relay(['e2e.across'], { last: '1h' })
    await expectPayloads(got, ...Array.from({ length: n }, (_, i) => `m${i}`))
    await reader.close()
    await c.close()
  } finally {
    await s.remove()
  }
}
