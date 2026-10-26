// Topic keys, revocation, vouching and client-id renewal: unitdb-go's
// security_e2e_test.go and TestE2ESecurePubSub, ported.

import { mkdtempSync, readdirSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { create } from '@bufbuild/protobuf'
import { afterEach, describe, expect, it } from 'vitest'
import { connect } from '../../src/client.js'
import { ReturnCode, RequestError, ServerError } from '../../src/errors.js'
import { PublishMessageSchema, PublishSchema } from '../../src/gen/unitdb_pb.js'
import { encodeMessage } from '../../src/codec/messages.js'
import { fileStore } from '../../src/session/file.js'
import { tcp } from '../../src/transport/tcp.js'
import { collect, e2e, expectPayloads, isPublishOn, mintId, newClient, newContract, RawConn, sleep, transports, waitUntil } from './harness.js'
import { Server } from './server.js'
import { Events } from './cluster-harness.js'

const skip = e2e.skip !== ''
const dirs: string[] = []
const tmp = () => {
  const d = mkdtempSync(join(tmpdir(), 'unitdb-security-'))
  dirs.push(d)
  return d
}
afterEach(() => {
  for (const d of dirs.splice(0)) rmSync(d, { recursive: true, force: true })
})

let keys = 800_000 + Math.floor(Math.random() * 100_000)

/** A v2 client id: 94 characters of base64url. */
const isV2ClientId = (id: string) => id.length === 94 && /^[A-Za-z0-9_=-]+$/.test(id)
/** A uuid as the server gives it: decimal, not 0. */
const isUuid = (uuid: string) => /^[0-9]+$/.test(uuid) && BigInt(uuid) !== 0n

/** refusal returns the status of a request's RequestError, or 200. */
async function status(p: Promise<unknown>): Promise<number> {
  try {
    await p
    return 200
  } catch (err) {
    if (err instanceof RequestError) return err.status
    throw err
  }
}

/** refusedWith expects call to fail with the server's error notice of status. */
async function refusedWith(call: Promise<unknown>, want: number): Promise<void> {
  const err = await call.then(
    () => null,
    (e: unknown) => e,
  )
  expect(err).toBeInstanceOf(ServerError)
  expect((err as ServerError).status).toBe(want)
}

describe.skipIf(skip).each(Object.keys(transports) as (keyof typeof transports)[])('over %s', (name) => {
  const transport = transports[name]

  it('publishes and subscribes with a topic key (TestE2ESecurePubSub)', async () => {
    const c = await newClient(transport(), mintId(), { insecure: false })
    const answers = collect(c.messages('unitdb/keygen'))
    const [k] = await c.keygen({ topic: 'e2e.secure', type: 'rw' })
    // The answer goes to the streams too, as in unitdb-go.
    await waitUntil('the keygen answer on its topic', () => answers.length === 1)
    const got = collect(c.messages('e2e.secure'))
    await c.subscribe(`${k!.key}/e2e.secure`)
    await c.publish(`${k!.key}/e2e.secure`, 'secured')
    await expectPayloads(got, 'secured')
  })
})

describe.skipIf(skip)('security', () => {
  it('issues keys with uuids and a ttl, and revokes keys and client ids (TestE2EKeygenAndRevoke)', async () => {
    const c = await newClient(tcp(e2e.server!.tcpAddr), mintId(), { insecure: false })
    const ks = await c.keygen({ topic: 'e2e.revoke', type: 'rw' }, { topic: 'e2e.short', type: 'rw', ttl: 2000 }, { topic: 'e2e.later', type: 'rw', ttl: '1h' })
    expect(ks.map((k) => k.topic)).toEqual(['e2e.revoke', 'e2e.short', 'e2e.later'])
    for (const k of ks) {
      expect(k.key).toHaveLength(48)
      expect(isUuid(k.uuid)).toBe(true)
    }
    const [revoked, short, later] = ks as [(typeof ks)[0], (typeof ks)[0], (typeof ks)[0]]

    // The keys work, whatever '-' and '_' they hold.
    const got = collect(c.messages('e2e.revoke'))
    await c.subscribe(`${revoked.key}/e2e.revoke`)
    await c.publish(`${revoked.key}/e2e.revoke`, 'before')
    await expectPayloads(got, 'before')
    await c.subscribe(`${short.key}/e2e.short`)

    // Revoked, a key is refused: the notice names the call.
    await c.revoke(revoked.uuid)
    await refusedWith(c.subscribe(`${revoked.key}/e2e.revoke`), 401)

    // Until a time, and bad requests.
    await c.revoke(later.uuid, new Date(Date.now() + 3_600_000))
    for (const uuid of ['0', 'not-a-uuid']) expect(await status(c.revoke(uuid))).toBe(400)
    expect(await status(c.revoke(later.uuid, new Date(Date.now() - 3_600_000)))).toBe(400) // a past time

    // Expired, a key is refused too.
    await sleep(2500)
    await refusedWith(c.subscribe(`${short.key}/e2e.short`), 401)

    // A secondary client id, revoked by its uuid, is refused at connect.
    const secondary = await c.requestClientId()
    expect(isV2ClientId(secondary.clientId)).toBe(true)
    expect(isUuid(secondary.uuid)).toBe(true)
    expect(await RawConn.connectCode(secondary.clientId)).toBe(ReturnCode.Accepted)
    await c.revoke(secondary.uuid)
    expect(await RawConn.connectCode(secondary.clientId)).toBe(ReturnCode.IdentifierRejected)
    // The connection stays, and the key revoked until later is refused.
    await refusedWith(c.subscribe(`${later.key}/e2e.later`), 401)
  })

  it('revokes all of a contract, the primary id included (TestE2ERevokeAll)', async () => {
    const primary = mintId()
    const c = await newClient(tcp(e2e.server!.tcpAddr), primary, { insecure: false })
    const [k] = await c.keygen({ topic: 'e2e.all', type: 'rw' })
    const secondary = await c.requestClientId()
    // What was issued in the second of the revocation is not revoked.
    await sleep(1100)
    await c.revokeAll()
    await refusedWith(c.subscribe(`${k!.key}/e2e.all`), 401)
    for (const id of [secondary.clientId, primary]) expect(await RawConn.connectCode(id)).toBe(ReturnCode.IdentifierRejected)
  })

  it('vouches with a service id (TestE2EVouch)', async () => {
    const contract = newContract()
    const service = mintId({ contract, service: true })
    const other = mintId({ contract: contract + 1, service: true })
    const serviceConn = await RawConn.connect(service)
    // The user: a secondary client id of the service's, without the insecure flag.
    const svc = await newClient(tcp(e2e.server!.tcpAddr), service, { insecure: false })
    const user = (await svc.requestClientId()).clientId
    const c = await newClient(tcp(e2e.server!.tcpAddr), user, { insecure: false })
    const got = collect(c.messages('e2e.vouch'))

    // Without a key, the user's subscription is refused.
    await refusedWith(c.subscribe('e2e.vouch'), 400)
    // A service of another contract can't vouch for it, nor an id that is not a service's.
    expect(await status(c.vouch(other))).toBe(403)
    expect(await status(c.vouch(user))).toBe(403)

    await c.vouch(service)
    await c.subscribe('e2e.vouch')
    await sleep(200)
    serviceConn.publish('e2e.vouch', 'vouched')
    await expectPayloads(got, 'vouched')

    // Vouched for, the user is no primary client.
    expect(await status(c.revoke('1'))).toBe(403)
  })

  it('vouches again on a new connection, before subscribing again', async () => {
    const s = await Server.create(e2e.bin, e2e.dir, { allowInsecure: true })
    await s.start()
    try {
      const contract = newContract()
      const service = mintId({ contract, service: true, conf: s.info.conf })
      const events = new Events()
      const c = await connect({
        servers: tcp(s.info.tcpAddr),
        clientId: mintId({ contract, conf: s.info.conf }),
        sessionKey: keys++,
        autoReconnect: true,
        maxReconnectInterval: 300,
        ...events.options(),
      })
      try {
        await c.vouch(service)
        const got = collect(c.messages('e2e.revouch'))
        await c.subscribe('e2e.revouch')
        await s.stop()
        await events.waitLost()
        await s.start()
        await events.waitConnected(2)
        await sleep(300)
        const pub = await RawConn.connect(service, s.info.tcpAddr)
        pub.publish('e2e.revouch', 'again')
        await expectPayloads(got, 'again')
      } finally {
        await c.close()
      }
    } finally {
      await s.remove()
    }
  })

  it('fails a request whose connection is lost before the answer', async () => {
    const s = await Server.create(e2e.bin, e2e.dir, { allowInsecure: true })
    await s.start()
    try {
      const c = await connect({ servers: tcp(s.info.tcpAddr), clientId: mintId({ conf: s.info.conf }), keepAlive: 300, pingTimeout: 500, writeTimeout: 0 })
      s.signal('SIGSTOP')
      const r = c.keygen({ topic: 'e2e.lost', type: 'r' })
      const err = await r.then(
        () => null,
        (e: unknown) => e as Error,
      )
      s.signal('SIGCONT')
      expect(err?.name).toBe('NotConnected')
      await c.close()
    } finally {
      await s.remove()
    }
  })

  it('renews an ageing client id, and the session follows it (TestE2EClientIDRenewal)', async () => {
    const s = await Server.create(e2e.bin, e2e.dir, { allowInsecure: true })
    await s.start()
    try {
      // Renewed past 4 of its 6 seconds.
      const ageing = mintId({ ttl: '6s', conf: s.info.conf })
      expect(isV2ClientId(ageing)).toBe(true)
      await sleep(4200)

      const dir = tmp()
      const store = fileStore(dir)
      const sessionKey = keys++
      const renewals: string[] = []
      const events = new Events()
      const t = tcp(s.info.tcpAddr)
      const c = await connect({
        servers: t,
        clientId: ageing,
        insecure: true,
        store,
        sessionKey,
        autoReconnect: true,
        maxReconnectInterval: 500,
        onClientId: (id) => renewals.push(id),
        ...events.options(),
      })
      await waitUntil('the renewed client id', () => renewals.length === 1)
      const renewed = renewals[0]!
      expect(isV2ClientId(renewed)).toBe(true)
      expect(renewed).not.toBe(ageing)
      expect(c.clientId).toBe(renewed)
      // The log stays the old id's; the renewed id's session links to it.
      expect(readdirSync(dir).filter((f) => f.endsWith('.link'))).toHaveLength(1)
      await c.publish('e2e.renew', 'first')

      // The client reconnects with the renewed id: the ageing one has
      // expired by then, and would be refused.
      await s.stop()
      await events.waitLost()
      await s.start()
      await events.waitConnected(2)
      await c.publish('e2e.renew', 'again')
      await sleep(1000)
      expect(renewals).toHaveLength(1)
      expect(c.clientId).toBe(renewed)
      await c.close()

      // The run left a publish the server never acknowledged in its log.
      const log = store.open(renewed, sessionKey)
      log.put(
        'o9',
        encodeMessage({
          kind: 'publish',
          msg: create(PublishSchema, { MessageID: 9, Messages: [create(PublishMessageSchema, { Topic: 'e2e.renew', Payload: new TextEncoder().encode('unfinished') })] }),
        }),
      )
      log.close()

      // The next run, created with the renewed id the application kept, resumes it.
      const subscriber = await RawConn.connect(renewed, s.info.tcpAddr)
      subscriber.subscribe('e2e.renew', 1)
      await sleep(100)
      const next = await connect({ servers: t, clientId: renewed, insecure: true, store, sessionKey })
      const m = await subscriber.waitFor('the resumed publish', isPublishOn('e2e.renew'))
      if (m.kind !== 'publish') throw new Error(m.kind)
      expect(new TextDecoder().decode(m.msg.Messages[0]!.Payload)).toBe('unfinished')
      await next.close()
    } finally {
      await s.remove()
    }
  })

  it('renews an id near the end of its life, and refuses an expired one (TestE2EClientIDExpiry)', async () => {
    const ageing = mintId({ ttl: '6s' })
    const expired = mintId({ ttl: '1s' })
    await sleep(4500)
    const err = await connect({ servers: tcp(e2e.server!.tcpAddr), clientId: expired }).then(
      async (c) => {
        await c.close()
        return null
      },
      (e: unknown) => e as { code?: number },
    )
    expect(err?.code).toBe(ReturnCode.IdentifierRejected)

    const renewals: string[] = []
    const c = await newClient(tcp(e2e.server!.tcpAddr), ageing, { insecure: false, onClientId: (id) => renewals.push(id) })
    await waitUntil('the renewed client id', () => renewals.length === 1)
    expect(isV2ClientId(renewals[0]!)).toBe(true)
    expect(c.clientId).toBe(renewals[0])
  })
})
