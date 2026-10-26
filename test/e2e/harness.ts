// Helpers of the e2e tests: ids from mintid, clients on the shared server,
// and a raw uTP connection for the second party (unitdb-go's rawConn).

import { execFileSync } from 'node:child_process'
import { connect as netConnect, type Socket } from 'node:net'
import { create } from '@bufbuild/protobuf'
import { inject, onTestFinished } from 'vitest'
import { connect, type ConnectOptions, type Client } from '../../src/client.js'
import { control, decodeMessage, encodeMessage, type Msg } from '../../src/codec/messages.js'
import { FlowControl, FrameDecoder, MessageType } from '../../src/codec/frame.js'
import { ConnectSchema, PublishMessageSchema, PublishSchema, SubscribeSchema, SubscriptionSchema } from '../../src/gen/unitdb_pb.js'
import { tcp } from '../../src/transport/tcp.js'
import { webSocket } from '../../src/transport/ws.js'
import type { Transport } from '../../src/transport/transport.js'
import type { Message } from '../../src/stream.js'

export const e2e = inject('e2e')
export const E2E_TIMEOUT = 10_000

/** The shared server's transports, by name: every connecting test runs on both. */
export const transports = {
  tcp: () => tcp(e2e.server!.tcpAddr),
  ws: () => webSocket(e2e.server!.wsUrl),
} satisfies Record<string, () => Transport>

let contracts = 1000 + Math.floor(Math.random() * 1_000_000)

/** newContract returns a contract no other test uses. */
export function newContract(): number {
  return contracts++
}

/** mintId mints a client id with the server's mintid, keyed as the test servers are. */
export function mintId({ contract = newContract(), service = false, conf = e2e.server!.conf, ttl = '' } = {}): string {
  const args = ['-config', conf, '-contract', String(contract)]
  if (service) args.push('-service')
  if (ttl) args.push('-ttl', ttl)
  // The key from the config: no keyring from the environment.
  const env: Record<string, string | undefined> = { ...process.env }
  delete env['UNITDB_KEYRING']
  delete env['UNITDB_ENCRYPTION_KEY']
  const out = execFileSync(e2e.mintid, args, { env }).toString()
  const m = /^client id: (\S+)$/m.exec(out)
  if (!m) throw new Error(`mintid printed no client id:\n${out}`)
  return m[1]!
}

let sessKeys = 1 + Math.floor(Math.random() * 1_000_000)

/** newClient connects a client to the shared server, insecure by default, closed when the test ends. */
export async function newClient(transport: Transport, clientId: string, opts: Partial<ConnectOptions> = {}): Promise<Client> {
  const c = await connect({ servers: transport, clientId, insecure: true, sessionKey: sessKeys++, ...opts })
  onTestFinished(() => c.close())
  return c
}

/** collect gathers a stream's payloads as text. */
export function collect(stream: AsyncIterable<Message>): string[] {
  const got: string[] = []
  ;(async () => {
    try {
      for await (const m of stream) got.push(new TextDecoder().decode(m.payload))
    } catch {
      // the client closed
    }
  })()
  return got
}

/** waitUntil waits for cond, up to E2E_TIMEOUT. */
export async function waitUntil(what: string, cond: () => boolean, ms = E2E_TIMEOUT): Promise<void> {
  const deadline = Date.now() + ms
  while (!cond()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`)
    await sleep(20)
  }
}

/** expectPayloads waits until got holds every one of want. */
export async function expectPayloads(got: string[], ...want: string[]): Promise<void> {
  await waitUntil(`messages ${want.join(', ')} (got ${got.join(', ')})`, () => want.every((w) => got.includes(w)))
}

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

/** RawConn speaks uTP directly, through the codec: the second party of a test. */
export class RawConn {
  private readonly decoder = new FrameDecoder()
  readonly received: Msg[] = []
  private nextId = 1
  /** Whether it answers a NOTIFY with RECEIVE, and a reliable delivery with RECEIPT. */
  notifyReceive = true

  private constructor(private readonly sock: Socket) {
    sock.on('data', (b: Buffer) => {
      for (const f of this.decoder.push(new Uint8Array(b))) {
        const m = decodeMessage(f, 'server')
        this.received.push(m)
        if (!this.notifyReceive) continue
        if (m.kind === 'control' && m.flowControl === FlowControl.NOTIFY) this.send(control(MessageType.PUBLISH, FlowControl.RECEIVE, m.msg.MessageID))
        if (m.kind === 'publish' && m.msg.DeliveryMode !== 0) this.send(control(MessageType.PUBLISH, FlowControl.RECEIPT, m.msg.MessageID))
        if (m.kind === 'publish' && m.msg.DeliveryMode === 0) this.send(control(MessageType.PUBLISH, FlowControl.ACKNOWLEDGE, m.msg.MessageID))
        if (m.kind === 'control' && m.flowControl === FlowControl.RECEIPT) this.send(control(MessageType.PUBLISH, FlowControl.RECEIPT, m.msg.MessageID))
      }
    })
  }

  close(): void {
    this.sock.destroy()
  }

  /** connectCode connects with clientId, without the insecure flag, and returns the server's return code. */
  static async connectCode(clientId: string, addr = e2e.server!.tcpAddr): Promise<number> {
    const r = await RawConn.open(addr)
    r.send({ kind: 'connect', msg: create(ConnectSchema, { ClientID: clientId, KeepAlive: 30, SessKey: sessKeys++ }) })
    const ack = await r.waitFor('connack', (m) => m.kind === 'connack')
    r.close()
    return ack.kind === 'connack' ? ack.connack.ReturnCode : -1
  }

  private static async open(addr: string): Promise<RawConn> {
    const [host, port] = [addr.slice(0, addr.lastIndexOf(':')), Number(addr.slice(addr.lastIndexOf(':') + 1))]
    const sock = await new Promise<Socket>((resolve, reject) => {
      const s = netConnect({ host, port }, () => resolve(s))
      s.once('error', reject)
    })
    onTestFinished(() => {
      sock.destroy()
    })
    return new RawConn(sock)
  }

  /** connect opens a raw connection with clientId, insecure (a service's id needs no flag, but takes it), and waits for its CONNACK. */
  static async connect(clientId: string, addr = e2e.server!.tcpAddr, { sessKey = sessKeys++, notify = true, insecure = true } = {}): Promise<RawConn> {
    const r = await RawConn.open(addr)
    r.notifyReceive = notify
    r.send({ kind: 'connect', msg: create(ConnectSchema, { ClientID: clientId, InsecureFlag: insecure, KeepAlive: 30, SessKey: sessKey }) })
    const ack = await r.waitFor('connack', (m) => m.kind === 'connack')
    if (ack.kind === 'connack' && ack.connack.ReturnCode !== 0) throw new Error(`connect return code ${ack.connack.ReturnCode}`)
    return r
  }

  send(m: Msg): void {
    this.sock.write(encodeMessage(m))
  }

  publish(topic: string, payload: string, mode = 0, ttl = '1m'): number {
    const id = this.nextId++
    this.send({
      kind: 'publish',
      msg: create(PublishSchema, { MessageID: id, DeliveryMode: mode, Messages: [create(PublishMessageSchema, { Topic: topic, Payload: new TextEncoder().encode(payload), Ttl: ttl })] }),
    })
    return id
  }

  subscribe(topic: string, mode = 0): number {
    const id = this.nextId++
    this.send({ kind: 'subscribe', msg: create(SubscribeSchema, { MessageID: id, Subscriptions: [create(SubscriptionSchema, { Topic: topic, DeliveryMode: mode })] }) })
    return id
  }

  async waitFor(what: string, pred: (m: Msg) => boolean, ms = E2E_TIMEOUT): Promise<Msg> {
    const deadline = Date.now() + ms
    for (;;) {
      const i = this.received.findIndex(pred)
      if (i >= 0) return this.received.splice(i, 1)[0]!
      if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`)
      await sleep(10)
    }
  }
}

/** isAck matches the acknowledgement of message id of type. */
export const isAck = (type: number, id: number) => (m: Msg) =>
  m.kind === 'control' && m.messageType === type && m.flowControl === FlowControl.ACKNOWLEDGE && m.msg.MessageID === id

/** isPublishOn matches a delivered publish on topic. */
export const isPublishOn = (topic: string) => (m: Msg) => m.kind === 'publish' && m.msg.Messages.some((x) => x.Topic === topic || x.Topic.endsWith(`/${topic}`))
