// The client: connections to a server, publish, subscribe and relay, its
// session's log, resuming and reconnecting, requests to the server's API and
// client-id renewal. Its behaviour
// follows unitdb-go (client.go, hdl_conn.go, batch.go, requests.go,
// internal/store).

import { create } from '@bufbuild/protobuf'
import {
  ConnectSchema,
  PublishMessageSchema,
  PublishSchema,
  RelayRequestSchema,
  RelaySchema,
  SubscribeSchema,
  SubscriptionSchema,
  UnsubscribeSchema,
  type Publish,
  type PublishMessage,
} from './gen/unitdb_pb.js'
import { FlowControl, FrameDecoder, MAX_FRAME, MessageType, type Frame } from './codec/frame.js'
import { control, decodeMessage, encodeMessage, type Msg } from './codec/messages.js'
import { ConnectError, InvalidArgument, NotConnected, RequestError, ReturnCode, ServerError, Timeout, UnitdbError } from './errors.js'
import { MessageStream, type Message } from './stream.js'
import { parseTopic, validateTopic, wire, matches, type Topic } from './topic.js'
import { abortReason, type Conn, type Transport } from './transport/transport.js'
import { memoryStore, scratchLog, type SessionLog, type SessionStore } from './session/store.js'

/**
 * How a message is delivered: at most once (express), at least once
 * (reliable), or at least once in a batch the client gathers (batch).
 */
export type Delivery = 'express' | 'reliable' | 'batch'

const deliveryMode = { express: 0, reliable: 1, batch: 2 } as const

export interface ConnectOptions {
  /** The server, or several, tried in turn (a cluster's nodes). */
  servers: Transport | Transport[]
  /** The client id: sealed by the server, or minted for a service. */
  clientId: string
  /** The insecure flag: no topic keys. A server refuses it unless it allows insecure clients. */
  insecure?: boolean
  /**
   * The session key. With one, the client's session (its log, and the
   * server's) is resumed by the next client of the same id and key; without,
   * it lasts as long as the client.
   */
  sessionKey?: number
  /** Start a new session rather than resume this one. */
  cleanSession?: boolean
  /** Where sessions' logs are kept: memoryStore() (this process) by default; fileStore(dir) to outlive it. */
  store?: SessionStore
  /** On resuming a session from the log, send its unacknowledged subscribes and unsubscribes again too. */
  resumeSubs?: boolean
  /** Connect again by itself when the connection is lost, as unitdb-go's WithAutoReconnect. */
  autoReconnect?: boolean
  /** The longest pause between reconnect attempts: ms, 10 s by default. */
  maxReconnectInterval?: number
  username?: string
  password?: Uint8Array
  /** Ping the server after this long without traffic: ms, 30 s by default. */
  keepAlive?: number
  /** The connection is lost if a ping isn't answered within: ms, 30 s by default. */
  pingTimeout?: number
  /** How long to reach each server and have its answer: ms, 30 s by default. */
  connectTimeout?: number
  /** How long a call waits for the server's answer, reconnecting included: ms, 30 s by default; 0, for ever. */
  writeTimeout?: number
  /** Batches: gathered for this long (ms, 100), sent sooner past these many bytes (3.5 MiB) or messages (1000). */
  batchDuration?: number
  batchByteThreshold?: number
  batchCountThreshold?: number
  /** Ends the client when aborted, as unitdb-go's ConnectContext. */
  signal?: AbortSignal
  /** Called each time a connection is made, the first included. */
  onConnect?: (c: Client) => void
  /** Called each time a connection is lost (not on close). */
  onConnectionLost?: (err: Error) => void
  /**
   * Called with an error notice from the server that names no call, or the
   * server's refusal to vouch for a new connection again (see vouch).
   */
  onError?: (err: ServerError | RequestError) => void
  /**
   * Called when the server renewed the client id (sealed again, with a new
   * expiry): the client connects with it from then on, and its session's log
   * follows it. Save it for the next run, as unitdb-go's WithClientIDHandler.
   */
  onClientId?: (clientId: string) => void
}

export interface PublishOptions {
  delivery?: Delivery
  /** Send it in a batch due this much later: ms. A delay implies a batch. */
  delay?: number
  /** How long the server keeps it: "1h", "10m"; or the topic's ?ttl= option. */
  ttl?: string
}

export interface SubscribeOptions {
  delivery?: 'express' | 'reliable'
  /** Delay deliveries by this long: ms. */
  delay?: number
}

export interface RelayOptions {
  /** How far back: "1h", "10m". */
  last?: string
}

/** A request for a topic key (see keygen). */
export interface KeyRequest {
  /** The topic, or wildcard topic, the key opens. */
  topic: string
  /** The key's access: any of "r" (read), "w" (write), "a" (admin) and "o" (owner), as in "rw". */
  type: string
  /** How long the key lasts: ms, or a duration as "1h". None leaves it to the server (its topic_key_ttl). */
  ttl?: number | string
}

/** A topic key the server issued. */
export interface TopicKey {
  /** The topic it opens. */
  topic: string
  /** The key, to prefix the topic with, as in `${key}/${topic}`: opaque (base64url, '-' and '_' included). */
  key: string
  /** The key's uuid in decimal, to revoke it with. */
  uuid: string
}

/** A secondary client id the server issued (see requestClientId). */
export interface IssuedClientId {
  clientId: string
  /** Its uuid in decimal, to revoke it with. */
  uuid: string
}

/** A subscription: its messages, and unsubscribe. */
export interface Subscription extends AsyncIterable<Message> {
  readonly topic: string
  /** unsubscribe ends the subscription on the server, and the stream. */
  unsubscribe(): Promise<void>
  /** close ends the stream here; the server keeps the subscription until unsubscribe or disconnect. */
  close(): void
  next(): Promise<IteratorResult<Message>>
  readonly buffered: number
}

type Kind = 'publish' | 'subscribe' | 'unsubscribe' | 'relay'

interface Pending {
  kind: Kind
  /** A reliable or batch publish: its flow ends with COMPLETE, not ACKNOWLEDGE. */
  reliable: boolean
  /** The server's RECEIPT came, and was confirmed: COMPLETE comes next. */
  receipted: boolean
  /** A request's publish: not logged, so not resumed. */
  request?: true
  /** A call's: absent for a message resent from the log, or a call that timed out. */
  call?: { resolve: () => void; reject: (err: Error) => void; timer?: ReturnType<typeof setTimeout> }
}

interface Filter {
  topic: Topic
  stream: MessageStream
  /** The subscription it serves, if any (subscribe's topic, as given). */
  subscription?: string
}

interface Batch {
  messages: PublishMessage[]
  size: number
  waiters: { resolve: () => void; reject: (err: Error) => void }[]
}

/** A request to the server's API, queued until the server answers it. */
interface Request {
  /** unitdb/<name>: the server answers on the same topic. */
  topic: string
  payload: Uint8Array
  /** The connection it was written on: 0 until then. */
  gen: number
  /** Its publish's message id, while that is in flight. */
  id?: number
  resolve: (answer: Uint8Array) => void
  reject: (err: Error) => void
  timer?: ReturnType<typeof setTimeout>
}

interface Handshake {
  conn: Conn
  decoder: FrameDecoder
  connId: number
  epoch: number
  server: string
  /** What came with the CONNACK. */
  early: Msg[]
}

const DEFAULTS = {
  keepAlive: 30_000,
  pingTimeout: 30_000,
  connectTimeout: 30_000,
  writeTimeout: 30_000,
  maxReconnectInterval: 10_000,
  batchDuration: 100,
  batchByteThreshold: 3.5 * 1024 * 1024,
  batchCountThreshold: 1000,
}

/** The store of clients given none: their logs last as long as the process. */
const processStore = memoryStore()

/**
 * connect connects to the first server of opts.servers that accepts the
 * client, and returns the client. A refusal for the client's own reasons
 * (its id, its key, the insecure flag) is final; a server that can't be
 * reached, or answers with a server error (0x05), gives way to the next.
 * With a session key and no clean session, what the session's log holds is
 * sent again.
 */
export async function connect(opts: ConnectOptions): Promise<Client> {
  const c = new Client(opts)
  await c.start()
  return c
}

/** A client of a server: its session goes on across its connections. */
export class Client {
  private readonly servers: Transport[]
  private readonly log: SessionLog
  private readonly pending = new Map<number, Pending>()
  private readonly filters = new Set<Filter>()
  /** The subscriptions to make again on a new connection, by topic as sent. */
  private readonly subs = new Map<string, { mode: number; delay: number }>()
  private readonly batches = new Map<number, Batch>()
  /** The requests to the server's API not answered yet, in the order they were written. */
  private readonly requests: Request[] = []
  /** The connection: counted from 1. */
  private gen = 0
  /** The service id vouch() was last answered for: vouched with again on each new connection. */
  private vouched: string | null = null
  private _clientId: string
  private readonly o: Required<
    Pick<
      ConnectOptions,
      'keepAlive' | 'pingTimeout' | 'connectTimeout' | 'writeTimeout' | 'maxReconnectInterval' | 'batchDuration' | 'batchByteThreshold' | 'batchCountThreshold'
    >
  >
  private conn: Conn | null = null
  private decoder = new FrameDecoder()
  private nextId = 0
  private closed = false
  private lastAction = Date.now()
  private pingSent = 0
  private lastPong = 0
  private keepAliveTimer: ReturnType<typeof setInterval> | undefined
  private batchTimer: ReturnType<typeof setInterval> | undefined
  private wake: (() => void) | null = null
  private _server = ''
  private _connId = 0
  private _epoch = 0

  /** Use connect(). */
  constructor(private readonly opts: ConnectOptions) {
    this.servers = Array.isArray(opts.servers) ? opts.servers : [opts.servers]
    if (this.servers.length === 0) throw new UnitdbError('unitdb: no server to connect to')
    this.o = {
      keepAlive: opts.keepAlive ?? DEFAULTS.keepAlive,
      pingTimeout: opts.pingTimeout ?? DEFAULTS.pingTimeout,
      connectTimeout: opts.connectTimeout ?? DEFAULTS.connectTimeout,
      writeTimeout: opts.writeTimeout ?? DEFAULTS.writeTimeout,
      maxReconnectInterval: opts.maxReconnectInterval ?? DEFAULTS.maxReconnectInterval,
      batchDuration: opts.batchDuration ?? DEFAULTS.batchDuration,
      batchByteThreshold: opts.batchByteThreshold ?? DEFAULTS.batchByteThreshold,
      batchCountThreshold: opts.batchCountThreshold ?? DEFAULTS.batchCountThreshold,
    }
    this._clientId = opts.clientId
    this.log = opts.sessionKey ? (opts.store ?? processStore).open(opts.clientId, opts.sessionKey) : scratchLog()
    if (opts.cleanSession) this.log.clear()
  }

  /** The server connected to now ('' while reconnecting), the connection's id and the server's epoch. */
  get server(): string {
    return this._server
  }
  get connId(): number {
    return this._connId
  }
  get epoch(): number {
    return this._epoch
  }

  /** The client id the client connects with: the one it was given, or the server renewed it with since. */
  get clientId(): string {
    return this._clientId
  }

  /** Whether the client is connected now. */
  get connected(): boolean {
    return !this.closed && this.conn !== null
  }

  /** Whether the client is closed: by close(), its signal, or a lost connection without autoReconnect. */
  get isClosed(): boolean {
    return this.closed
  }

  /** start makes the first connection. */
  async start(): Promise<void> {
    if (this.opts.signal?.aborted) {
      this.log.close()
      throw abortReason(this.opts.signal)
    }
    let h
    try {
      h = await this.attempt(this.opts.cleanSession ?? false)
    } catch (err) {
      this.log.close()
      throw err
    }
    this.attach(h)
    // The signal ends the client: its connection lost, as unitdb-go's ConnectContext.
    this.opts.signal?.addEventListener(
      'abort',
      () => {
        const reason = abortReason(this.opts.signal!)
        if (this.closed) return
        if (this.conn) {
          this.detach()
          this.opts.onConnectionLost?.(reason)
        }
        this.end(reason, true)
      },
      { once: true },
    )
    this.resume()
    this.opts.onConnect?.(this)
  }

  // Calls.

  /**
   * publish sends payload to topic, and resolves once the server took it:
   * once acknowledged for express, once stored for reliable (RECEIPT, then
   * COMPLETE), once its batch was for batch (or a delay). The topic may
   * carry options: "a.b?ttl=1m".
   */
  publish(topic: string, payload: Uint8Array | string, opts: PublishOptions = {}): Promise<void> {
    let t: Topic
    try {
      t = this.topic(topic, true)
    } catch (err) {
      return Promise.reject(err)
    }
    const ttl = opts.ttl ?? t.options.get('ttl') ?? ''
    const mode = deliveryMode[opts.delivery ?? 'express']
    if (mode === undefined) return Promise.reject(new InvalidArgument(`unitdb: delivery ${opts.delivery}`))
    const body = typeof payload === 'string' ? new TextEncoder().encode(payload) : payload
    const pm = create(PublishMessageSchema, { Topic: wire(t), Payload: body, Ttl: ttl })
    if (mode === 2 || (opts.delay ?? 0) > 0) return this.batch(pm, opts.delay ?? 0)
    return this.call('publish', mode !== 0, (id) => ({ kind: 'publish', msg: create(PublishSchema, { MessageID: id, DeliveryMode: mode, Messages: [pm] }) }))
  }

  /**
   * messages returns the stream of the messages on topic (wildcards too)
   * that reach the client, whatever subscription or relay brought them,
   * as unitdb-go's TopicFilter. It doesn't subscribe.
   */
  messages(topic: string): MessageStream {
    return this.filter(this.topic(topic, false)).stream
  }

  /** subscribe subscribes to topic, and returns its messages once the server took it. */
  async subscribe(topic: string, opts: SubscribeOptions = {}): Promise<Subscription> {
    const t = this.topic(topic, false)
    const mode = deliveryMode[opts.delivery ?? 'express']
    if (mode === undefined) throw new InvalidArgument(`unitdb: delivery ${opts.delivery}`)
    const delay = opts.delay ?? 0
    const f = this.filter(t, topic)
    try {
      await this.call('subscribe', false, (id) => ({
        kind: 'subscribe',
        msg: create(SubscribeSchema, { MessageID: id, Subscriptions: [create(SubscriptionSchema, { Topic: wire(t), DeliveryMode: mode, Delay: delay })] }),
      }))
    } catch (err) {
      f.stream.close()
      throw err
    }
    this.subs.set(wire(t), { mode, delay })
    const stream = f.stream
    return {
      topic,
      unsubscribe: () => this.unsubscribe(topic),
      close: () => stream.close(),
      next: () => stream.next(),
      get buffered() {
        return stream.buffered
      },
      [Symbol.asyncIterator]: () => stream[Symbol.asyncIterator](),
    }
  }

  /**
   * subscribeMultiple subscribes to topics with one request, as
   * unitdb-go's SubscribeMultiple; their messages come to the streams of
   * messages() whose topics they match.
   */
  async subscribeMultiple(topics: string[], opts: SubscribeOptions = {}): Promise<void> {
    const ts = topics.map((topic) => this.topic(topic, false))
    const mode = deliveryMode[opts.delivery ?? 'express']
    if (mode === undefined) throw new InvalidArgument(`unitdb: delivery ${opts.delivery}`)
    const delay = opts.delay ?? 0
    await this.call('subscribe', false, (id) => ({
      kind: 'subscribe',
      msg: create(SubscribeSchema, {
        MessageID: id,
        Subscriptions: ts.map((t) => create(SubscriptionSchema, { Topic: wire(t), DeliveryMode: mode, Delay: delay })),
      }),
    }))
    for (const t of ts) this.subs.set(wire(t), { mode, delay })
  }

  /** unsubscribe ends the client's subscriptions to topics, and their streams. */
  async unsubscribe(...topics: string[]): Promise<void> {
    const ts = topics.map((topic) => this.topic(topic, false))
    for (const t of ts) this.subs.delete(wire(t))
    await this.call('unsubscribe', false, (id) => ({
      kind: 'unsubscribe',
      msg: create(UnsubscribeSchema, { MessageID: id, Subscriptions: ts.map((t) => create(SubscriptionSchema, { Topic: wire(t) })) }),
    }))
    for (const f of [...this.filters]) {
      if (f.subscription !== undefined && topics.includes(f.subscription)) f.stream.close()
    }
  }

  /**
   * relay asks for the messages stored on topics, for the last while; they
   * come as messages, to the streams of messages() and subscribe() whose
   * topics they match. It resolves once the server took the request.
   */
  relay(topics: string[], opts: RelayOptions = {}): Promise<void> {
    let ts: Topic[]
    try {
      ts = topics.map((topic) => this.topic(topic, false))
    } catch (err) {
      return Promise.reject(err)
    }
    return this.call('relay', false, (id) => ({
      kind: 'relay',
      msg: create(RelaySchema, {
        MessageID: id,
        relayRequests: ts.map((t) => create(RelayRequestSchema, { Topic: wire(t), Last: opts.last ?? t.options.get('last') ?? '' })),
      }),
    }))
  }

  // Requests to the server's API (unitdb-go's requests.go). The server
  // answers them on their topic, in order; they aren't logged, and one the
  // server hadn't answered when its connection is lost fails.

  /**
   * keygen asks the server for topic keys, one for each request, in order.
   * Only a primary client id may, or a connection a trusted service vouched
   * for. A refusal of any of them fails it with a RequestError.
   */
  async keygen(...reqs: KeyRequest[]): Promise<TopicKey[]> {
    const body = reqs.map((r) => {
      const k: { topic: string; type: string; ttl?: string } = { topic: r.topic, type: r.type }
      if (typeof r.ttl === 'number') {
        if (!(r.ttl >= 0)) throw new InvalidArgument(`unitdb: keygen: ttl ${r.ttl}`)
        if (r.ttl > 0) k.ttl = `${Math.ceil(r.ttl)}ms`
      } else if (r.ttl) {
        k.ttl = r.ttl
      }
      return k
    })
    const answer = parseAnswer(await this.request('keygen', json(body)))
    if (!Array.isArray(answer)) {
      refused(answer)
      throw unexpected(answer)
    }
    // A refusal is a status; keys are a list, each with its own status.
    const keys: TopicKey[] = []
    let refusal: RequestError | null = null
    for (const k of answer as KeyAnswer[]) {
      if (k.status !== 200) refusal = new RequestError(k.status ?? 0, k.message ?? '')
      else keys.push({ topic: k.topic ?? '', key: k.key ?? '', uuid: k.uuid ?? '' })
    }
    if (refusal) throw refusal
    return keys
  }

  /**
   * requestClientId asks the server for a new secondary client id of the
   * client's contract. Only a primary client id may.
   */
  async requestClientId(): Promise<IssuedClientId> {
    const answer = parseAnswer(await this.request('clientid', new Uint8Array())) as Answer & { key?: string; uuid?: string }
    refused(answer)
    return { clientId: answer.key ?? '', uuid: answer.uuid ?? '' }
  }

  /**
   * revoke revokes the client id or topic key of the client's contract with
   * uuid, as keygen and requestClientId give it, until a time (a Date, or
   * ms since the epoch), or for ever. Only a primary client id may. A
   * revoked id is refused when it connects, a revoked key when it is used;
   * what is open already stays open.
   */
  async revoke(uuid: string, until?: Date | number): Promise<void> {
    const body: { uuid: string; until?: number } = { uuid }
    if (until !== undefined) body.until = Math.floor((typeof until === 'number' ? until : until.getTime()) / 1000)
    refused(parseAnswer(await this.request('revoke', json(body))))
  }

  /**
   * revokeAll revokes every client id and topic key the client's contract
   * was issued before now, in whole seconds: the client's own id included,
   * which is refused the next time it connects. Only a primary client id may.
   */
  async revokeAll(): Promise<void> {
    refused(parseAnswer(await this.request('revoke', json({ all: true }))))
  }

  /**
   * vouch vouches for the connection with a trusted service's client id of
   * the client's contract: it then needs no topic keys. The client vouches
   * again on each new connection, before it resumes and subscribes again;
   * a refusal then goes to onError. Keep the service's id on the service,
   * never on clients or devices.
   */
  async vouch(serviceId: string): Promise<void> {
    refused(parseAnswer(await this.request('service', json({ client_id: serviceId }))))
    this.vouched = serviceId
  }

  /**
   * close sends the batches waiting, disconnects, and ends the client:
   * calls not answered yet fail, and every stream ends. What the session's
   * log holds stays, for the next client of the session.
   */
  async close(): Promise<void> {
    if (this.closed) return
    for (const slot of [...this.batches.keys()]) this.flush(slot)
    if (this.conn) this.send({ kind: 'disconnect' })
    this.end(new NotConnected('unitdb: the client closed'), false)
  }

  // Connections.

  /** attempt connects to the first server that takes the client. */
  private async attempt(clean: boolean): Promise<Handshake> {
    let last: Error = new UnitdbError('unitdb: no server answered')
    for (const t of this.servers) {
      if (this.closed) throw new NotConnected('unitdb: the client closed')
      try {
        return await this.handshake(t, clean)
      } catch (err) {
        last = err as Error
        if (err instanceof ConnectError && err.code !== ReturnCode.ServerError) throw err
        if (this.opts.signal?.aborted) throw last
      }
    }
    throw last
  }

  /** handshake opens t and has the server's CONNACK. */
  private async handshake(t: Transport, clean: boolean): Promise<Handshake> {
    const timeout = AbortSignal.timeout(this.o.connectTimeout)
    const signal = this.opts.signal ? AbortSignal.any([this.opts.signal, timeout]) : timeout
    const conn = await t.open(signal)
    const decoder = new FrameDecoder()
    const early: Msg[] = []
    const opts = this.opts
    const connack = await new Promise<{ code: number; connId: number; epoch: number }>((resolve, reject) => {
      const fail = (err: Error) => {
        signal.removeEventListener('abort', onAbort)
        conn.onData = () => {}
        conn.onClose = () => {}
        conn.close()
        reject(err)
      }
      const onAbort = () => fail(timeout.aborted ? new Timeout(`unitdb: ${t.address} didn't answer the connect`) : abortReason(signal))
      signal.addEventListener('abort', onAbort, { once: true })
      conn.onClose = (err) => fail(err ?? new NotConnected(`unitdb: ${t.address} closed the connection before answering`))
      conn.onData = (chunk) => {
        let msgs: Msg[]
        try {
          msgs = decoder.push(chunk).map((f) => decodeMessage(f, 'server'))
        } catch (err) {
          return fail(err as Error)
        }
        const i = msgs.findIndex((m) => m.kind === 'connack')
        if (i < 0) return
        const m = msgs[i]!
        if (m.kind !== 'connack') return
        // What came with the CONNACK is the session's: the client handles
        // it, and the decoder, holding any part of a frame, goes to it.
        early.push(...msgs.slice(i + 1))
        conn.onData = (more) => {
          try {
            early.push(...decoder.push(more).map((f: Frame) => decodeMessage(f, 'server')))
          } catch {
            // the client sees the stream fail on its next read
          }
        }
        signal.removeEventListener('abort', onAbort)
        resolve({ code: m.connack.ReturnCode, connId: m.connack.ConnID, epoch: m.connack.Epoch })
      }
      conn.send(
        encodeMessage({
          kind: 'connect',
          msg: create(ConnectSchema, {
            ClientID: this._clientId,
            InsecureFlag: opts.insecure ?? false,
            KeepAlive: Math.max(1, Math.round(this.o.keepAlive / 1000)),
            CleanSessFlag: clean,
            SessKey: (opts.sessionKey ?? 0) | 0,
            Username: opts.username ?? '',
            ...(opts.username && opts.password ? { Password: opts.password } : {}),
          }),
        }),
      )
    })
    if (connack.code !== 0) {
      conn.onClose = () => {}
      conn.close()
      throw new ConnectError(t.address, connack.code)
    }
    return { conn, decoder, connId: connack.connId, epoch: connack.epoch, server: t.address, early }
  }

  /** attach makes h the client's connection. */
  private attach(h: Handshake): void {
    const conn = h.conn
    this.conn = conn
    this.gen++
    this.decoder = h.decoder
    this._server = h.server
    this._connId = h.connId
    this._epoch = h.epoch
    this.lastAction = Date.now()
    this.pingSent = this.lastPong = 0
    conn.onData = (chunk) => {
      if (this.conn === conn) this.onData(chunk)
    }
    conn.onClose = (err) => {
      if (this.conn === conn) this.lost(err ?? new NotConnected('unitdb: the connection closed'))
    }
    for (const m of h.early) {
      this.handle(m)
      if (this.conn !== conn) return
    }
    const interval = Math.min(Math.max(this.o.keepAlive / 2, 100), 5_000)
    this.keepAliveTimer = setInterval(() => this.keepAlive(), interval)
    unref(this.keepAliveTimer)
  }

  /** detach drops the connection. */
  private detach(): void {
    if (this.keepAliveTimer) clearInterval(this.keepAliveTimer)
    this.keepAliveTimer = undefined
    const conn = this.conn
    this.conn = null
    this._server = ''
    if (conn) {
      conn.onData = () => {}
      conn.onClose = () => {}
      conn.close()
    }
  }

  /**
   * resume sends again what the session's log holds, in order, as
   * unitdb-go's resume: the client's messages not acknowledged and its
   * receipts not completed, and the server's notifications not received.
   */
  private resume(): void {
    for (const [key, frame] of this.log.entries()) {
      if (!this.conn) return
      const id = Number(key.slice(1))
      let m: Msg | null = null
      try {
        const [f] = new FrameDecoder().push(frame)
        if (f) m = decodeMessage(f, key[0] === 'o' && f.flowControl === FlowControl.NONE ? 'client' : 'server')
      } catch {
        m = null
      }
      if (!m) {
        this.log.delete(key)
        continue
      }
      if (key[0] === 'i') {
        // The server's message, not finished.
        if (m.kind === 'control' && m.flowControl === FlowControl.NOTIFY) {
          this.send(control(MessageType.PUBLISH, FlowControl.RECEIVE, id))
        } else if (m.kind === 'control' && m.flowControl === FlowControl.RECEIPT) {
          this.write(frame)
        } else {
          this.log.delete(key)
        }
        continue
      }
      // The client's own.
      const live = this.pending.get(id)
      switch (m.kind) {
        case 'publish':
        case 'relay':
          if (!live) this.pending.set(id, { kind: m.kind, reliable: m.kind === 'publish' && m.msg.DeliveryMode !== 0, receipted: false })
          this.write(frame)
          break
        case 'subscribe':
        case 'unsubscribe':
          if (live || this.opts.resumeSubs) {
            if (!live) this.pending.set(id, { kind: m.kind, reliable: false, receipted: false })
            this.write(frame)
          } else {
            this.log.delete(key)
          }
          break
        case 'control':
          if (m.flowControl === FlowControl.RECEIPT) {
            if (!live) this.pending.set(id, { kind: 'publish', reliable: true, receipted: true })
            else live.receipted = true
            this.write(frame)
          } else {
            this.log.delete(key)
          }
          break
        default:
          this.log.delete(key)
      }
    }
  }

  /** resubscribe subscribes again to the client's topics: the server drops a connection's subscriptions when it ends. */
  private resubscribe(): void {
    for (const [topic, s] of this.subs) {
      const id = this.allocate()
      this.pending.set(id, { kind: 'subscribe', reliable: false, receipted: false })
      this.send({
        kind: 'subscribe',
        msg: create(SubscribeSchema, { MessageID: id, Subscriptions: [create(SubscriptionSchema, { Topic: topic, DeliveryMode: s.mode, Delay: s.delay })] }),
      })
    }
  }

  /** lost handles a connection's end: reconnect, or end the client. */
  private lost(err: Error): void {
    if (this.closed || !this.conn) return
    this.detach()
    this.failConnection(new NotConnected(`unitdb: the connection was lost before the server answered: ${err.message}`))
    this.opts.onConnectionLost?.(err)
    if (this.opts.autoReconnect && !this.opts.signal?.aborted) {
      void this.reconnect()
    } else {
      this.end(err, true)
    }
  }

  /**
   * reconnect connects again, trying each server in turn with a pause that
   * doubles up to maxReconnectInterval, until it succeeds or the client
   * closes; then it resumes the session and subscribes again.
   */
  private async reconnect(): Promise<void> {
    let pause = 100
    while (!this.closed) {
      await new Promise<void>((resolve) => {
        const t = setTimeout(resolve, pause)
        this.wake = () => {
          clearTimeout(t)
          resolve()
        }
      })
      this.wake = null
      if (this.closed) return
      try {
        const h = await this.attempt(false)
        if (this.closed) {
          h.conn.close()
          return
        }
        this.attach(h)
        if (!this.conn) return // lost already: lost() reconnects
        this.vouchAgain()
        for (const r of this.requests.filter((r) => r.gen === 0)) this.sendRequest(r)
        this.resume()
        this.resubscribe()
        this.opts.onConnect?.(this)
        return
      } catch {
        pause = Math.min(pause * 2, this.o.maxReconnectInterval)
      }
    }
  }

  /** end ends the client: with failed, its streams end with err. */
  private end(err: Error, failed: boolean): void {
    if (this.closed) return
    this.closed = true
    this.wake?.()
    if (this.batchTimer) clearInterval(this.batchTimer)
    this.detach()
    const notConnected = err instanceof NotConnected ? err : new NotConnected(`unitdb: the connection was lost: ${err.message}`)
    for (const [, p] of this.pending) {
      if (p.call) {
        if (p.call.timer) clearTimeout(p.call.timer)
        p.call.reject(notConnected)
      }
    }
    this.pending.clear()
    for (const r of [...this.requests]) this.dropRequest(r, notConnected)
    this.failConnection(notConnected)
    for (const b of this.batches.values()) for (const w of b.waiters) w.reject(notConnected)
    this.batches.clear()
    for (const f of this.filters) f.stream.end(failed ? err : undefined)
    this.filters.clear()
    this.log.close()
  }

  // The machinery.

  private topic(text: string, publish: boolean): Topic {
    const t = parseTopic(text)
    if (!t) throw new InvalidArgument(`unitdb: no topic in "${text}"`)
    const why = validateTopic(t, { publish })
    if (why) throw new InvalidArgument(`unitdb: ${why}: "${text}"`)
    return t
  }

  private filter(topic: Topic, subscription?: string): Filter {
    if (this.closed) throw new NotConnected()
    const f: Filter = { topic, stream: new MessageStream(() => this.filters.delete(f)) }
    if (subscription !== undefined) f.subscription = subscription
    this.filters.add(f)
    return f
  }

  /**
   * call logs the message make builds with a free id, sends it (or leaves
   * it for the next connection to resume), and settles when the server
   * answers it, or after writeTimeout.
   */
  private call(kind: Kind, reliable: boolean, make: (id: number) => Msg): Promise<void> {
    if (this.closed) return Promise.reject(new NotConnected())
    const id = this.allocate()
    const bytes = encodeMessage(make(id))
    if (bytes.length > MAX_FRAME + 64) {
      return Promise.reject(new InvalidArgument(`unitdb: a ${kind} of ${bytes.length} bytes (at most ${MAX_FRAME})`))
    }
    return new Promise<void>((resolve, reject) => {
      const p: Pending = { kind, reliable, receipted: false, call: { resolve, reject } }
      if (this.o.writeTimeout > 0) {
        p.call!.timer = setTimeout(() => {
          if (this.pending.get(id) !== p || !p.call) return
          // The id stays taken, and the message logged, until the server
          // answers: a resumed session may still send it.
          delete p.call
          reject(new Timeout(`unitdb: the server didn't answer the ${kind} in ${this.o.writeTimeout} ms`))
        }, this.o.writeTimeout)
        unref(p.call!.timer)
      }
      this.pending.set(id, p)
      this.log.put(`o${id}`, bytes)
      if (this.conn) this.write(bytes)
    })
  }

  /**
   * request publishes body on unitdb/<name>, and resolves with the server's
   * answer. Made while the client reconnects, it waits for the connection,
   * up to writeTimeout.
   */
  private request(name: string, body: Uint8Array): Promise<Uint8Array> {
    if (this.closed) return Promise.reject(new NotConnected())
    return new Promise<Uint8Array>((resolve, reject) => {
      const r: Request = { topic: `unitdb/${name}`, payload: body, gen: 0, resolve, reject }
      if (this.o.writeTimeout > 0) {
        r.timer = setTimeout(() => {
          const err = new Timeout(`unitdb: the server didn't answer the ${name} request in ${this.o.writeTimeout} ms`)
          // Written, it keeps its place: its answer, late, is still its own.
          if (r.gen === 0) this.dropRequest(r, err)
          else reject(err)
        }, this.o.writeTimeout)
        unref(r.timer)
      }
      this.requests.push(r)
      if (this.conn) this.sendRequest(r)
    })
  }

  /** sendRequest writes r on the connection: last in the queue, the order the server answers in. */
  private sendRequest(r: Request): void {
    const i = this.requests.indexOf(r)
    if (i >= 0) this.requests.splice(i, 1)
    this.requests.push(r)
    r.gen = this.gen
    const id = this.allocate()
    r.id = id
    // Its acknowledgement comes after the answer; an error notice naming it fails it.
    this.pending.set(id, { kind: 'publish', reliable: false, receipted: false, request: true, call: { resolve: () => delete r.id, reject: (err) => this.dropRequest(r, err) } })
    this.send({ kind: 'publish', msg: create(PublishSchema, { MessageID: id, Messages: [create(PublishMessageSchema, { Topic: r.topic, Payload: r.payload })] }) })
  }

  /** dropRequest takes r out of the queue, and fails it. */
  private dropRequest(r: Request, err: Error): void {
    const i = this.requests.indexOf(r)
    if (i >= 0) this.requests.splice(i, 1)
    if (r.timer) clearTimeout(r.timer)
    if (r.id !== undefined) this.pending.delete(r.id)
    r.reject(err)
  }

  /** answered hands the answer on topic to the oldest request on it this connection wrote. */
  private answered(topic: string, payload: Uint8Array): void {
    const r = this.requests.find((r) => r.gen === this.gen && r.topic === topic)
    if (!r) return
    this.requests.splice(this.requests.indexOf(r), 1)
    if (r.timer) clearTimeout(r.timer)
    r.resolve(payload)
  }

  /** failConnection fails what the connection lost had in flight that isn't resumed: its requests. */
  private failConnection(err: Error): void {
    // Requests' publishes, answered or not: their acknowledgements won't come.
    for (const [id, p] of this.pending) {
      if (!p.request) continue
      this.pending.delete(id)
      p.call?.reject(err)
    }
    for (const r of this.requests.filter((r) => r.gen !== 0 && r.gen === this.gen)) this.dropRequest(r, err)
  }

  /** vouchAgain vouches for a new connection with the service id vouch() was last answered for. */
  private vouchAgain(): void {
    const id = this.vouched
    if (!id) return
    this.request('service', json({ client_id: id })).then(
      (answer) => {
        try {
          refused(parseAnswer(answer))
        } catch (err) {
          if (this.vouched === id) this.vouched = null
          if (err instanceof RequestError) this.opts.onError?.(err)
        }
      },
      () => {}, // the connection lost again: the next one vouches
    )
  }

  /** renew adopts the client id the server renewed the client's with. */
  private renew(id: string): void {
    if (!/^[A-Za-z0-9_=-]{1,255}$/.test(id) || id === this._clientId) return
    this._clientId = id
    this.log.follow?.(id)
    this.opts.onClientId?.(id)
  }

  /** allocate returns a message id not in use: 1 to 65535, in turn. */
  private allocate(): number {
    for (let i = 0; i < 65535; i++) {
      this.nextId = (this.nextId % 65535) + 1
      if (!this.pending.has(this.nextId)) return this.nextId
    }
    throw new UnitdbError('unitdb: 65535 calls in flight')
  }

  /** settle ends the client's flow id: its call resolves, or fails with err. */
  private settle(id: number, err?: Error): void {
    const p = this.pending.get(id)
    if (!p) return
    this.pending.delete(id)
    this.log.delete(`o${id}`)
    if (!p.call) return
    if (p.call.timer) clearTimeout(p.call.timer)
    if (err) p.call.reject(err)
    else p.call.resolve()
  }

  /** batch adds pm to the batch due after delay, and resolves when its batch is taken. */
  private batch(pm: PublishMessage, delay: number): Promise<void> {
    if (this.closed) return Promise.reject(new NotConnected())
    const d = this.o.batchDuration
    const slot = Math.floor((Date.now() + d + delay) / d) * d
    let b = this.batches.get(slot)
    if (!b) this.batches.set(slot, (b = { messages: [], size: 0, waiters: [] }))
    const done = new Promise<void>((resolve, reject) => b!.waiters.push({ resolve, reject }))
    b.messages.push(pm)
    b.size += pm.Payload.length
    if (b.messages.length > this.o.batchCountThreshold || b.size > this.o.batchByteThreshold) {
      this.flush(slot)
    } else if (!this.batchTimer) {
      this.batchTimer = setInterval(() => {
        const now = Date.now()
        for (const s of [...this.batches.keys()]) if (s <= now) this.flush(s)
        if (this.batches.size === 0 && this.batchTimer) {
          clearInterval(this.batchTimer)
          this.batchTimer = undefined
        }
      }, d)
      unref(this.batchTimer)
    }
    return done
  }

  /** flush sends the batch of slot, as one publish of delivery mode 2. */
  private flush(slot: number): void {
    const b = this.batches.get(slot)
    if (!b) return
    this.batches.delete(slot)
    this.call('publish', true, (id) => ({ kind: 'publish', msg: create(PublishSchema, { MessageID: id, DeliveryMode: 2, Messages: b.messages }) })).then(
      () => b.waiters.forEach((w) => w.resolve()),
      (err: Error) => b.waiters.forEach((w) => w.reject(err)),
    )
  }

  private send(m: Msg): void {
    this.write(encodeMessage(m))
  }

  private write(bytes: Uint8Array): void {
    if (!this.conn) return
    this.lastAction = Date.now()
    this.conn.send(bytes)
  }

  private onData(chunk: Uint8Array): void {
    this.lastAction = Date.now()
    let msgs: Msg[]
    try {
      msgs = this.decoder.push(chunk).map((f) => decodeMessage(f, 'server'))
    } catch (err) {
      return this.lost(err as Error)
    }
    const conn = this.conn
    for (const m of msgs) {
      this.handle(m)
      if (this.conn !== conn) return
    }
  }

  private handle(m: Msg): void {
    switch (m.kind) {
      case 'control': {
        const id = m.msg.MessageID
        switch (m.flowControl) {
          case FlowControl.ACKNOWLEDGE:
            if (m.messageType === MessageType.PINGREQ) this.lastPong = Date.now()
            else this.settle(id)
            return
          case FlowControl.RECEIPT: {
            // The server stored a reliable publish of ours: confirm it (and
            // log the confirmation in its place), and its COMPLETE settles it.
            const p = this.pending.get(id)
            const receipt = encodeMessage(control(MessageType.PUBLISH, FlowControl.RECEIPT, id))
            if (p && p.kind === 'publish') {
              p.receipted = true
              this.log.put(`o${id}`, receipt)
            }
            this.write(receipt)
            return
          }
          case FlowControl.COMPLETE: {
            // Our reliable publish's flow, or the server's whose message we
            // receipted: ids are the sender's own, so both may be in flight;
            // ours, if it waits for this.
            const p = this.pending.get(id)
            if (p && p.receipted) this.settle(id)
            else this.log.delete(`i${id}`)
            return
          }
          case FlowControl.NOTIFY:
            // A reliable message for us: kept until received, and asked for.
            this.log.put(`i${id}`, encodeMessage(m))
            this.send(control(MessageType.PUBLISH, FlowControl.RECEIVE, id))
            return
        }
        return
      }
      case 'publish':
        return this.deliver(m.msg)
      case 'disconnect':
        return this.lost(new NotConnected('unitdb: the server disconnected the client'))
    }
  }

  private deliver(pub: Publish): void {
    for (const pm of pub.Messages) {
      if (/^\/?unitdb\/error/.test(pm.Topic)) {
        this.errorNotice(pm.Payload)
        continue
      }
      // The server's messages for the client itself: a renewed id, and
      // answers to requests. They go to the streams as well, as in unitdb-go.
      if (pm.Topic === 'unitdb/clientid/') this.renew(new TextDecoder().decode(pm.Payload))
      else if (requestTopics.has(pm.Topic)) this.answered(pm.Topic, pm.Payload)
      const t = parseTopic(pm.Topic)
      if (!t) continue
      const msg: Message = { topic: t.name, payload: pm.Payload, ttl: pm.Ttl }
      for (const f of this.filters) {
        if (matches(f.topic, t)) f.stream.push(msg)
      }
    }
    // Acknowledged once handed to the streams: at least once for reliable,
    // whose receipt is logged until the server completes the flow.
    if (pub.DeliveryMode === 1 || pub.DeliveryMode === 2) {
      const receipt = encodeMessage(control(MessageType.PUBLISH, FlowControl.RECEIPT, pub.MessageID))
      this.log.put(`i${pub.MessageID}`, receipt)
      this.write(receipt)
    } else {
      this.send(control(MessageType.PUBLISH, FlowControl.ACKNOWLEDGE, pub.MessageID))
    }
  }

  private errorNotice(payload: Uint8Array): void {
    let e: { status?: number; message?: string; id?: number }
    try {
      e = JSON.parse(new TextDecoder().decode(payload)) as typeof e
    } catch {
      return
    }
    const err = new ServerError(e.status ?? 0, e.message ?? '')
    // The call it refuses, by its message id; none (0) for one the server can't name.
    const id = e.id ?? 0
    if (id && this.pending.has(id)) return this.settle(id, err)
    this.opts.onError?.(err)
  }

  private keepAlive(): void {
    if (!this.conn) return
    const now = Date.now()
    if (this.pingSent > this.lastPong) {
      if (now - this.pingSent >= this.o.pingTimeout) this.lost(new Timeout("unitdb: the server didn't answer a ping"))
      return
    }
    if (now - this.lastAction >= this.o.keepAlive) {
      this.pingSent = now
      this.send({ kind: 'pingreq' })
    }
  }
}

/** The topics of the requests to the server's API, which it answers on. */
const requestTopics = new Set(['unitdb/keygen', 'unitdb/clientid', 'unitdb/revoke', 'unitdb/service'])

/** An answer of the server's API: its status, and its explanation of a refusal. */
interface Answer {
  status?: number
  message?: string
}

interface KeyAnswer extends Answer {
  key?: string
  topic?: string
  uuid?: string
}

function json(v: unknown): Uint8Array {
  return new TextEncoder().encode(JSON.stringify(v))
}

function unexpected(v: unknown): UnitdbError {
  return new UnitdbError(`unitdb: unexpected answer from the server: ${typeof v === 'string' ? v : JSON.stringify(v)}`)
}

function parseAnswer(payload: Uint8Array): unknown {
  const text = new TextDecoder().decode(payload)
  try {
    return JSON.parse(text) as unknown
  } catch {
    throw unexpected(text)
  }
}

/** refused throws the RequestError of an answer that refuses its request. */
function refused(answer: unknown): void {
  if (typeof answer !== 'object' || answer === null || Array.isArray(answer)) throw unexpected(answer)
  const a = answer as Answer
  if (a.status !== 200) throw new RequestError(a.status ?? 0, a.message ?? '')
}

/** unref lets Node exit with the timer running; elsewhere a no-op. */
function unref(t: unknown): void {
  ;(t as { unref?: () => void } | undefined)?.unref?.()
}
