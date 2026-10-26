// A stream of messages: an async iterable with a buffer, for one consumer.

/** A message delivered to the client. */
export interface Message {
  /** The topic it was published on, without key or options. */
  topic: string
  payload: Uint8Array
  /** How long the server keeps it ("1h"), or ''. */
  ttl: string
}

/** Decodes a message's payload as UTF-8 text. */
export function text(m: Message): string {
  return new TextDecoder().decode(m.payload)
}

/**
 * MessageStream yields the messages pushed to it, in order, until it is
 * closed, by its consumer (break, return, close) or by the client (the
 * connection ended: the iteration throws why, unless the client was closed).
 */
export class MessageStream implements AsyncIterable<Message> {
  private queue: Message[] = []
  private waiting: { resolve: (r: IteratorResult<Message>) => void; reject: (e: Error) => void } | null = null
  private done = false
  private failure: Error | null = null

  constructor(private readonly onClose: () => void) {}

  /** push hands m to the consumer. */
  push(m: Message): void {
    if (this.done) return
    if (this.waiting) {
      const w = this.waiting
      this.waiting = null
      w.resolve({ value: m, done: false })
    } else {
      this.queue.push(m)
    }
  }

  /** end ends the stream once the buffered messages are read: with err, the iteration throws it. */
  end(err?: Error): void {
    if (this.done) return
    this.done = true
    this.failure = err ?? null
    if (this.waiting && this.queue.length === 0) {
      const w = this.waiting
      this.waiting = null
      if (this.failure) w.reject(this.failure)
      else w.resolve({ value: undefined, done: true })
    }
  }

  /** close stops the stream: no more messages, and the client forgets it. */
  close(): void {
    this.queue = []
    this.end()
    this.onClose()
  }

  /** The messages buffered, not read yet. */
  get buffered(): number {
    return this.queue.length
  }

  /** next resolves with the next message, or done once the stream ended. */
  next(): Promise<IteratorResult<Message>> {
    const m = this.queue.shift()
    if (m) return Promise.resolve({ value: m, done: false })
    if (this.done) {
      if (this.failure) return Promise.reject(this.failure)
      return Promise.resolve({ value: undefined, done: true })
    }
    return new Promise((resolve, reject) => {
      this.waiting = { resolve, reject }
    })
  }

  [Symbol.asyncIterator](): AsyncIterator<Message> {
    return {
      next: () => this.next(),
      return: () => {
        this.close()
        return Promise.resolve({ value: undefined, done: true })
      },
    }
  }
}
