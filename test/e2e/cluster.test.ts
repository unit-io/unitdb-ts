// unitdb-go's cluster_e2e_test.go, ported: the client against a three-node
// cluster, over TCP and the WebSocket.

import { create } from '@bufbuild/protobuf'
import { describe, expect, it } from 'vitest'
import { FlowControl, MessageType } from '../../src/codec/frame.js'
import { PublishMessageSchema, PublishSchema, SubscribeSchema, SubscriptionSchema } from '../../src/gen/unitdb_pb.js'
import { collect, e2e, expectPayloads, isAck, mintId, RawConn, sleep } from './harness.js'
import { clusterClient, others, publishTo, serviceId, settle, startCluster, tcpOf, topics, wsOf } from './cluster-harness.js'

const skip = e2e.skip !== ''
const decode = (b: Uint8Array) => new TextDecoder().decode(b)

describe.skipIf(skip)('a cluster', () => {
  it('takes the keys any node issued on every node (TestClusterClientSecurePubSub)', async () => {
    const c = await startCluster()
    // A primary client id, with no insecure flag (a node refuses it): keys needed.
    const id = mintId({ conf: c.nodes[0]!.info.conf })
    for (const n of c.nodes) {
      const client = await clusterClient(tcpOf(n), id)
      const topic = `cl.secure.${n.info.name}`
      const [k] = await client.keygen({ topic, type: 'rw' })
      const got = collect(client.messages(topic))
      await client.subscribe(`${k!.key}/${topic}`)
      await settle()
      const want: string[] = []
      for (const pn of c.nodes) {
        const pub = await RawConn.connect(id, pn.info.tcpAddr, { insecure: false })
        pub.publish(`${k!.key}/${topic}`, `${pn.info.name}>${topic}`)
        want.push(`${pn.info.name}>${topic}`)
      }
      await expectPayloads(got, ...want)
      await client.close()
    }
  })

  it('delivers to a subscriber on any node, from any node (TestClusterClientSubscribes)', async () => {
    const c = await startCluster()
    const id = serviceId(c)
    for (const n of c.nodes) {
      for (const [transport, to] of [['tcp', tcpOf], ['ws', wsOf]] as const) {
        const prefix = `cl.sub.${n.info.name}.${transport}`
        const client = await clusterClient(to(n), id)
        const got = collect(client.messages(`${prefix}...`))
        const ts = topics(prefix)
        await client.subscribeMultiple(ts)
        await settle()
        const want: string[] = []
        for (const pn of c.nodes) {
          const pub = await RawConn.connect(id, pn.info.tcpAddr)
          for (const t of ts) {
            pub.publish(t, `${pn.info.name}>${t}`)
            want.push(`${pn.info.name}>${t}`)
          }
        }
        await expectPayloads(got, ...want)
        await client.close()
      }
    }
  })

  it("publishes to subscribers on every node (TestClusterClientPublishes)", async () => {
    const c = await startCluster()
    const id = serviceId(c)
    for (const n of c.nodes) {
      const ts = topics(`cl.pub.${n.info.name}`)
      const subs: RawConn[] = []
      for (const sn of c.nodes) {
        const sub = await RawConn.connect(id, sn.info.tcpAddr)
        for (const t of ts) sub.subscribe(t)
        subs.push(sub)
      }
      await settle()
      const client = await clusterClient(tcpOf(n), id)
      for (const t of ts) await client.publish(t, `${n.info.name}>${t}`)
      for (const sub of subs) {
        for (const t of ts) {
          const want = `${n.info.name}>${t}`
          await sub.waitFor(want, (m) => m.kind === 'publish' && m.msg.Messages.some((pm) => decode(pm.Payload) === want))
        }
      }
    }
  })

  it('delivers reliably across nodes (TestClusterClientReliableDelivery)', async () => {
    const c = await startCluster()
    const id = serviceId(c)
    for (const n of c.nodes) {
      const prefix = `cl.reliable.${n.info.name}`
      const client = await clusterClient(tcpOf(n), id)
      const got = collect(client.messages(`${prefix}...`))
      const ts = topics(prefix)
      await client.subscribeMultiple(ts, { delivery: 'reliable' })
      await settle()
      const want: string[] = []
      for (const pn of others(c, n)) {
        const pub = await RawConn.connect(id, pn.info.tcpAddr)
        for (const t of ts) {
          pub.publish(t, `${pn.info.name}>${t}`, 1)
          want.push(`${pn.info.name}>${t}`)
        }
      }
      await expectPayloads(got, ...want)
    }
  })

  it('relays what other nodes stored (TestClusterClientRelay)', async () => {
    const c = await startCluster()
    const id = serviceId(c)
    for (const n of c.nodes) {
      const prefix = `cl.relay.${n.info.name}`
      const ts = topics(prefix)
      const want: string[] = []
      for (const pn of others(c, n)) {
        const pub = await RawConn.connect(id, pn.info.tcpAddr)
        for (const t of ts) {
          const mid = pub.publish(t, `${pn.info.name}>${t}`, 0, '1h')
          await pub.waitFor('publish acknowledge', (m) => m.kind === 'control' && m.messageType === MessageType.PUBLISH && m.flowControl === FlowControl.ACKNOWLEDGE && m.msg.MessageID === mid)
          want.push(`${pn.info.name}>${t}`)
        }
      }
      const client = await clusterClient(wsOf(n), id)
      const got = collect(client.messages(`${prefix}...`))
      await client.relay(ts, { last: '1h' })
      await expectPayloads(got, ...want)
    }
  })

  it('takes a wildcard subscription on every node (TestClusterClientWildcardSubscription)', async () => {
    const c = await startCluster()
    const id = serviceId(c)
    for (const n of c.nodes) {
      const prefix = `cl.wild.${n.info.name}`
      const client = await clusterClient(tcpOf(n), id)
      const got = collect(client.messages(`${prefix}...`))
      await client.subscribe(`${prefix}...`)
      await settle()
      const want: string[] = []
      for (const pn of c.nodes) {
        const pub = await RawConn.connect(id, pn.info.tcpAddr)
        for (const t of topics(`${prefix}.${pn.info.name}`)) {
          pub.publish(t, `${pn.info.name}>${t}`)
          want.push(`${pn.info.name}>${t}`)
        }
      }
      await expectPayloads(got, ...want)
    }
  })

  for (const victim of ['leader', 'follower']) {
    it(`keeps a client when the ${victim} dies (TestClusterClientSurvivesNodeFailure)`, async () => {
      const c = await startCluster()
      const leader = await c.waitLeader(c.nodes)
      const dead = c.nodes.find((n) => (victim === 'leader') === (n.info.name === leader))!
      const [clientNode, pubNode] = others(c, dead) as [typeof dead, typeof dead]
      const id = serviceId(c)
      const lost: Error[] = []
      const client = await clusterClient(tcpOf(clientNode), id, { onConnectionLost: (e) => lost.push(e) })
      const got = collect(client.messages('cl.failover...'))
      const ts = topics('cl.failover')
      await client.subscribeMultiple(ts)
      await settle()
      const check = async (when: string) => {
        const pub = await RawConn.connect(id, pubNode.info.tcpAddr)
        // Each acknowledged before the next, as unitdb-go's: the survivors
        // take the dead node's topics over meanwhile.
        for (const t of ts) {
          const mid = pub.publish(t, `${when}>${t}`)
          await pub.waitFor('publish acknowledge', isAck(MessageType.PUBLISH, mid), 30_000)
        }
        await expectPayloads(got, ...ts.map((t) => `${when}>${t}`))
      }
      await check('before')
      // Some topics were the dead node's; its survivors take them over with the client's subscriptions.
      await dead.kill()
      expect(lost).toEqual([])
      await check('after')
    })
  }

  it('resumes a session on another node (TestClusterClientResumesSessionOnAnotherNode)', async () => {
    const c = await startCluster()
    const dead = c.nodes[0]!
    const live = others(c, dead)
    const id = serviceId(c)
    const sessionKey = 900_000 + Math.floor(Math.random() * 50_000)
    const n = 5
    const topic = 'cl.session'

    // A subscriber of the session on the node about to die, notified of
    // reliable messages it doesn't receive yet.
    const held = await RawConn.connect(id, dead.info.tcpAddr, { sessKey: sessionKey, notify: false })
    held.send({ kind: 'subscribe', msg: create(SubscribeSchema, { MessageID: 1, Subscriptions: [create(SubscriptionSchema, { Topic: topic, DeliveryMode: 1 })] }) })
    await settle()

    const pub = await RawConn.connect(id, live[1]!.info.tcpAddr)
    for (let i = 0; i < n; i++) {
      pub.send({
        kind: 'publish',
        msg: create(PublishSchema, { MessageID: 10 + i, DeliveryMode: 1, Messages: [create(PublishMessageSchema, { Topic: topic, Payload: new TextEncoder().encode(`r${i}`), Ttl: '1h' })] }),
      })
      // The server's answer to a raw reliable publish: ACKNOWLEDGE (COMPLETE after a RECEIPT on other servers).
      await pub.waitFor('publish answer', (m) => m.kind === 'control' && (m.flowControl === FlowControl.ACKNOWLEDGE || m.flowControl === FlowControl.COMPLETE) && m.msg.MessageID === 10 + i)
    }
    for (let i = 0; i < n; i++) await held.waitFor('notify', (m) => m.kind === 'control' && m.flowControl === FlowControl.NOTIFY)
    await sleep(500) // session logs replicate asynchronously

    held.close()
    await dead.kill()

    // The client resumes the session on a survivor, the dead node first in its list.
    const client = await clusterClient([tcpOf(dead), tcpOf(live[0]!)], id, { sessionKey, connectTimeout: 2000 })
    const got = collect(client.messages(topic))
    await expectPayloads(got, ...Array.from({ length: n }, (_, i) => `r${i}`))
  })
})
