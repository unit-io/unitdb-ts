// unitdb-go's reconnect_e2e_test.go, ported: a client with autoReconnect
// loses its server and connects again by itself.

import { describe, expect, it } from 'vitest'
import { NotConnected } from '../../src/errors.js'
import { create } from '@bufbuild/protobuf'
import { RelayRequestSchema, RelaySchema } from '../../src/gen/unitdb_pb.js'
import { collect, e2e, expectPayloads, RawConn, sleep } from './harness.js'
import { clusterClient, Events, publishTo, serviceId, settle, startCluster, tcpOf, topics } from './cluster-harness.js'

const skip = e2e.skip !== ''

describe.skipIf(skip)('reconnecting', () => {
  it('to another node (TestReconnectToAnotherNode)', async () => {
    const c = await startCluster()
    const [dead, other] = [c.nodes[0]!, c.nodes[1]!]
    const id = serviceId(c)
    const events = new Events()
    // The node about to die first.
    const client = await clusterClient([tcpOf(dead), tcpOf(other)], id, { ...events.options(), autoReconnect: true, maxReconnectInterval: 500 })
    await events.waitConnected(1)
    const got = collect(client.messages('rc.move...'))
    const ts = topics('rc.move')
    await client.subscribeMultiple(ts)
    await settle()
    for (const t of ts) await publishTo(other, id, t, `before>${t}`)
    await expectPayloads(got, ...ts.map((t) => `before>${t}`))

    await dead.stop()
    expect(await events.waitLost()).toBeInstanceOf(Error)
    await events.waitConnected(2)
    // The survivors fail the dead node over, then the resubscribed topics deliver again.
    for (const t of ts) await publishTo(other, id, t, `after>${t}`)
    await expectPayloads(got, ...ts.map((t) => `after>${t}`))
  })

  it('after the server restarts (TestReconnectAfterServerRestart)', async () => {
    const c = await startCluster()
    const n = c.nodes[0]!
    const id = serviceId(c)
    const events = new Events()
    const client = await clusterClient(tcpOf(n), id, { ...events.options(), autoReconnect: true, maxReconnectInterval: 500 })
    const got = collect(client.messages('rc.restart'))
    await client.subscribe('rc.restart')

    await n.stop()
    await events.waitLost()
    await sleep(1000) // the server is down a while: the client keeps trying
    await n.start()
    await events.waitConnected(2)
    await settle()
    await publishTo(n, id, 'rc.restart', 'back')
    await expectPayloads(got, 'back')
  })

  it('queues the calls made meanwhile (TestReconnectQueuesCalls)', async () => {
    const c = await startCluster()
    const [n, other] = [c.nodes[0]!, c.nodes[1]!]
    const id = serviceId(c)
    const events = new Events()
    const client = await clusterClient(tcpOf(n), id, { ...events.options(), autoReconnect: true, maxReconnectInterval: 500, writeTimeout: 20_000 })

    await n.stop()
    await events.waitLost()
    // Published while the client is down: it waits for the connection.
    const r = client.publish('rc.queued', 'queued', { ttl: '1h' })
    await sleep(500)
    await n.start()
    await events.waitConnected(2)
    await r

    // Stored: relayed from another node once the cluster has settled.
    await sleep(3000)
    const relay = await RawConn.connect(id, other.info.tcpAddr)
    relay.send({ kind: 'relay', msg: create(RelaySchema, { MessageID: 1, relayRequests: [create(RelayRequestSchema, { Topic: 'rc.queued', Last: '1h' })] }) })
    await relay.waitFor('the queued message', (m) => m.kind === 'publish' && m.msg.Messages.some((pm) => new TextDecoder().decode(pm.Payload) === 'queued'))
  })

  it('closes while reconnecting, and stays closed (TestDisconnectWhileReconnecting)', async () => {
    const c = await startCluster()
    const n = c.nodes[0]!
    const events = new Events()
    const client = await clusterClient(tcpOf(n), serviceId(c), { ...events.options(), autoReconnect: true, maxReconnectInterval: 200 })
    await n.stop()
    await events.waitLost()
    const start = Date.now()
    await client.close()
    expect(Date.now() - start).toBeLessThan(1000)
    await n.start()
    await sleep(2000)
    expect(events.connected).toBe(1)
    await expect(client.publish('rc.closed', 'x')).rejects.toBeInstanceOf(NotConnected)
  })

  it('not by default (TestNoReconnectByDefault)', async () => {
    const c = await startCluster()
    const n = c.nodes[0]!
    const events = new Events()
    const client = await clusterClient(tcpOf(n), serviceId(c), events.options())
    await n.stop()
    await events.waitLost()
    await n.start()
    await sleep(2000)
    expect(events.connected).toBe(1)
    await expect(client.publish('rc.default', 'x')).rejects.toThrow()
  })

  it('when its node drains (TestReconnectWhenNodeDrains)', async () => {
    const c = await startCluster()
    const [draining, other] = [c.nodes[0]!, c.nodes[1]!]
    const id = serviceId(c)
    const events = new Events()
    const client = await clusterClient([tcpOf(draining), tcpOf(other)], id, { ...events.options(), autoReconnect: true, maxReconnectInterval: 500 })
    const got = collect(client.messages('rc.drain...'))
    const ts = topics('rc.drain')
    await client.subscribeMultiple(ts)
    await settle()

    // A deploy stops the node gracefully: it leaves the cluster and closes
    // its clients' connections, and they move to another node.
    draining.signal('SIGTERM')
    await events.waitLost()
    await events.waitConnected(2)
    await Promise.race([draining.exited(), sleep(20_000).then(() => Promise.reject(new Error("the node didn't exit on SIGTERM")))])
    await settle()
    for (const t of ts) await publishTo(other, id, t, `moved>${t}`)
    await expectPayloads(got, ...ts.map((t) => `moved>${t}`))
  })
})
