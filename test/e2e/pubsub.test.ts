// unitdb-go's e2e pub/sub tests (e2e_test.go), ported one for one, each over
// TCP and over the WebSocket. Where unitdb-go uses a secondary client id of
// the publisher's contract, these use a second id minted for that contract.

import { create } from '@bufbuild/protobuf'
import { describe, expect, it } from 'vitest'
import { ServerError } from '../../src/errors.js'
import { MAX_FRAME } from '../../src/codec/frame.js'
import { PublishMessageSchema, PublishSchema } from '../../src/gen/unitdb_pb.js'
import { toBinary } from '@bufbuild/protobuf'
import { collect, e2e, expectPayloads, isPublishOn, mintId, newClient, newContract, RawConn, sleep, transports } from './harness.js'

const skip = e2e.skip !== ''

describe.skipIf(skip).each(Object.keys(transports) as (keyof typeof transports)[])('over %s', (name) => {
  const transport = transports[name]

  it('publishes and subscribes (TestE2EPubSub)', async () => {
    const c = await newClient(transport(), mintId())
    const got = collect(c.messages('e2e.room...'))
    await c.subscribe('e2e.room.lobby')
    for (let i = 0; i < 3; i++) await c.publish('e2e.room.lobby', `hello-${i}`)
    await expectPayloads(got, 'hello-0', 'hello-1', 'hello-2')
    await c.unsubscribe('e2e.room.lobby')
    await c.close()
  })

  it('receives from another connection (TestE2EReceiveFromAnotherConnection)', async () => {
    const contract = newContract()
    const publisher = await RawConn.connect(mintId({ contract }))
    const c = await newClient(transport(), mintId({ contract }))
    const sub = await c.subscribe('e2e.chat')
    const got = collect(sub)
    publisher.publish('e2e.chat', 'from another connection', 1)
    await expectPayloads(got, 'from another connection')
  })

  it('publishes to another connection (TestE2EPublishToAnotherConnection)', async () => {
    const contract = newContract()
    const subscriber = await RawConn.connect(mintId({ contract }))
    subscriber.subscribe('e2e.orders', 1)
    await subscriber.waitFor('subscribe acknowledgement', (m) => m.kind === 'control')
    const c = await newClient(transport(), mintId({ contract }))
    await c.publish('e2e.orders', 'order-1', { ttl: '1m' })
    const m = await subscriber.waitFor('published message', isPublishOn('e2e.orders'))
    if (m.kind !== 'publish') throw new Error(m.kind)
    expect(new TextDecoder().decode(m.msg.Messages[0]!.Payload)).toBe('order-1')
  })

  it("keeps contracts apart (TestE2EContractIsolation)", async () => {
    const stranger = await RawConn.connect(mintId())
    const c = await newClient(transport(), mintId())
    const got = collect(c.messages('e2e.shared'))
    await c.subscribe('e2e.shared')
    stranger.publish('e2e.shared', 'not for you')
    await sleep(500)
    expect(got).toEqual([])
  })

  it('delivers reliably: NOTIFY, RECEIVE, RECEIPT (TestE2EReliableDelivery)', async () => {
    const contract = newContract()
    const publisher = await RawConn.connect(mintId({ contract }))
    const c = await newClient(transport(), mintId({ contract }))
    const got = collect(c.messages('e2e.reliable'))
    await c.subscribe('e2e.reliable', { delivery: 'reliable' })
    publisher.publish('e2e.reliable', 'reliable-1', 1)
    await expectPayloads(got, 'reliable-1')
  })

  it('publishes reliably: RECEIPT, then COMPLETE', async () => {
    const c = await newClient(transport(), mintId())
    const got = collect(c.messages('e2e.stored'))
    await c.subscribe('e2e.stored')
    await c.publish('e2e.stored', 'kept', { delivery: 'reliable', ttl: '1m' })
    await expectPayloads(got, 'kept')
  })

  it('relays stored messages, each once (TestE2ERelay)', async () => {
    const contract = newContract()
    const publisher = await RawConn.connect(mintId({ contract }))
    for (let i = 0; i < 3; i++) publisher.publish('e2e.history', `stored-${i}`)
    await sleep(200)
    const c = await newClient(transport(), mintId({ contract }))
    const got = collect(c.messages('e2e.history'))
    await c.relay(['e2e.history'], { last: '1m' })
    await expectPayloads(got, 'stored-0', 'stored-1', 'stored-2')
    await sleep(500)
    expect(got).toHaveLength(3)
  })

  it('subscribes with a wildcard (TestE2EWildcardSubscription)', async () => {
    const contract = newContract()
    const publisher = await RawConn.connect(mintId({ contract }))
    const c = await newClient(transport(), mintId({ contract }))
    const got = collect(c.messages('e2e.sensors...'))
    await c.subscribe('e2e.sensors...')
    publisher.publish('e2e.sensors.room1.temp', '21')
    await expectPayloads(got, '21')
  })

  it('carries a message just under the frame limit (TestE2ELargeMessage)', async () => {
    // A body of MAX_FRAME - 8 bytes, as unitdb-go sizes it.
    const size = (n: number) =>
      toBinary(PublishSchema, create(PublishSchema, { MessageID: 65535, Messages: [create(PublishMessageSchema, { Topic: 'e2e.large', Payload: new Uint8Array(n) })] })).length
    let n = (4 << 20) - 100
    n += MAX_FRAME - 8 - size(n)
    const c = await newClient(transport(), mintId())
    const sub = await c.subscribe('e2e.large')
    await c.publish('e2e.large', new Uint8Array(n).fill(120))
    const m = await sub.next()
    expect(m.done).toBe(false)
    expect(m.value!.payload.length).toBe(n)
  })

  it('fails a call the server refuses, with its error notice', async () => {
    const c = await newClient(transport(), mintId())
    // Topics whose first part starts with $ are the server's.
    await expect(c.publish('$sys.secret', 'x')).rejects.toBeInstanceOf(ServerError)
    // And the connection goes on.
    await c.publish('e2e.after', 'still here')
  })

  it('subscriptions end when the client closes, and calls fail after', async () => {
    const c = await newClient(transport(), mintId())
    const sub = await c.subscribe('e2e.end')
    await c.close()
    expect((await sub.next()).done).toBe(true)
    await expect(c.publish('e2e.end', 'late')).rejects.toThrow(/not connected|closed/)
  })
})
