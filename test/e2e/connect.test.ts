// unitdb-go's e2e connection tests (e2e_test.go), ported: connecting,
// refusals, keepalive, the connection's signal, and closing under traffic.

import { describe, expect, it } from 'vitest'
import { connect } from '../../src/client.js'
import { ConnectError, ReturnCode } from '../../src/errors.js'
import { tcp } from '../../src/transport/tcp.js'
import { webSocket } from '../../src/transport/ws.js'
import { e2e, mintId, newClient, newContract, RawConn, sleep, transports } from './harness.js'
import { freePort, startServer } from './server.js'

const skip = e2e.skip !== ''
const closedTcp = async () => tcp(`127.0.0.1:${await freePort()}`)

describe.skipIf(skip).each(Object.keys(transports) as (keyof typeof transports)[])('over %s', (name) => {
  const transport = transports[name]

  it('pings an idle connection open (TestE2EKeepAlive)', async () => {
    const lost: Error[] = []
    const c = await newClient(transport(), mintId(), { keepAlive: 2000, pingTimeout: 3000, onConnectionLost: (e) => lost.push(e) })
    await sleep(5000)
    expect(lost).toEqual([])
    await c.publish('e2e.keepalive', 'still here')
  })

  it('keeps an idle connection open with a ping timeout shorter than the keep alive (TestE2EShortPingTimeout)', async () => {
    const lost: Error[] = []
    const c = await newClient(transport(), mintId(), { keepAlive: 2000, pingTimeout: 500, onConnectionLost: (e) => lost.push(e) })
    await sleep(5000)
    expect(lost).toEqual([])
    await c.publish('e2e.keepalive', 'still here')
  })

  it('outlives its connect timeout (TestE2EConnectionOutlivesConnectTimeout)', async () => {
    const c = await newClient(transport(), mintId(), { connectTimeout: 1000 })
    await sleep(1500)
    await c.publish('e2e.timeout', 'still here')
  })

  it('refuses an id the server did not issue: return code 2 (TestE2EConnectWithUnknownClientID)', async () => {
    const err = await connect({ servers: transport(), clientId: 'A'.repeat(94), insecure: true }).catch((e) => e)
    expect(err).toBeInstanceOf(ConnectError)
    expect((err as ConnectError).code).toBe(ReturnCode.IdentifierRejected)
    expect(String(err)).toContain('return code 2')
    // A client connects after it.
    await newClient(transport(), mintId())
  })

  it('ends with its signal (TestE2EConnectContextBoundsConnection)', async () => {
    const ac = new AbortController()
    const lost: Error[] = []
    const c = await connect({ servers: transport(), clientId: mintId(), insecure: true, signal: ac.signal, onConnectionLost: (e) => lost.push(e) })
    await c.publish('e2e.ctx', 'connected')
    ac.abort(new Error('the test is done'))
    expect(lost.map((e) => e.message)).toEqual(['the test is done'])
    expect(c.connected).toBe(false)
  })

  it('closes under traffic, and fails calls after (TestE2EDisconnectDuringTraffic)', async () => {
    for (let i = 0; i < 5; i++) {
      const contract = newContract()
      const publisher = await RawConn.connect(mintId({ contract }))
      const c = await newClient(transport(), mintId({ contract }))
      const sub = await c.subscribe('e2e.flood')
      let stop = false
      const flood = (async () => {
        while (!stop) {
          for (let k = 0; k < 50; k++) publisher.publish('e2e.flood', 'in')
          await sleep(1)
        }
      })()
      const calls = (async () => {
        while (!stop) {
          await Promise.allSettled([c.publish('e2e.flood', 'out'), c.subscribe('e2e.flood')])
          await sleep(0) // a closed client rejects at once: let the timers run
        }
      })()
      await sleep(50)
      await c.close()
      await sleep(20)
      stop = true
      await Promise.all([flood, calls])
      await expect(c.publish('e2e.flood', 'after')).rejects.toThrow()
      sub.close()
    }
  })
})

describe.skipIf(skip)('connecting', () => {
  it('fails on a closed port (TestE2EConnectToClosedPort)', async () => {
    await expect(connect({ servers: await closedTcp(), clientId: mintId(), connectTimeout: 1000 })).rejects.toThrow()
  })

  it('retries after a failed connect (TestE2ERetryAfterFailedConnect)', async () => {
    await expect(connect({ servers: await closedTcp(), clientId: mintId(), connectTimeout: 1000 })).rejects.toThrow()
    await newClient(transports.tcp(), mintId())
  })

  it('fails over to the next server (TestE2EFailoverToNextServer)', async () => {
    const c = await connect({ servers: [await closedTcp(), transports.tcp()], clientId: mintId(), insecure: true, connectTimeout: 1000 })
    await c.publish('e2e.failover', 'via the second server')
    await c.close()
  })

  it('fails over from a closed WebSocket too', async () => {
    const c = await connect({ servers: [webSocket(`ws://127.0.0.1:${await freePort()}/`), transports.ws()], clientId: mintId(), insecure: true, connectTimeout: 1000 })
    await c.publish('e2e.failover', 'via the second server')
    await c.close()
  })

  it("refuses the insecure flag on a server that doesn't allow it, but not a service (TestE2EInsecureRefusedByDefault)", async () => {
    const s = await startServer(e2e.bin, e2e.dir, { allowInsecure: false })
    try {
      for (const t of [tcp(s.info.tcpAddr), webSocket(s.info.wsUrl)]) {
        const err = await connect({ servers: t, clientId: mintId({ conf: s.info.conf }), insecure: true }).catch((e) => e)
        expect(err).toBeInstanceOf(ConnectError)
        expect((err as ConnectError).code).toBe(ReturnCode.NotAuthorized)
        // A service needs no flag and no keys.
        const svc = await connect({ servers: t, clientId: mintId({ conf: s.info.conf, service: true }) })
        await svc.publish('e2e.service', 'trusted')
        await svc.close()
      }
    } finally {
      await s.stop()
    }
  })

  it('times out on a server that never answers the connect', async () => {
    // A listener that accepts and says nothing.
    const { createServer } = await import('node:net')
    const mute = createServer(() => {})
    await new Promise<void>((r) => mute.listen(0, '127.0.0.1', r))
    const port = (mute.address() as { port: number }).port
    const start = Date.now()
    await expect(connect({ servers: tcp(`127.0.0.1:${port}`), clientId: mintId(), connectTimeout: 500 })).rejects.toThrow(/didn't answer/)
    expect(Date.now() - start).toBeLessThan(3000)
    mute.close()
  })
})
