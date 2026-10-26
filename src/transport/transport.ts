// A transport carries the stream of frames: TCP (./tcp.ts, Node) or a
// WebSocket (./ws.ts). The client sees bytes in, bytes out.

/** An open connection. */
export interface Conn {
  /** send writes bytes; a closed connection drops them. */
  send(bytes: Uint8Array): void
  /** close ends the connection; onClose is called once, if not yet. */
  close(): void
  /** Called with each chunk read, in order. */
  onData: (chunk: Uint8Array) => void
  /** Called once when the connection ends, with why if it failed. */
  onClose: (err?: Error) => void
}

/** A way to reach one server. */
export interface Transport {
  /** Where it goes, for errors: "tcp://host:port", "wss://host/". */
  readonly address: string
  /** open connects, within signal. */
  open(signal: AbortSignal): Promise<Conn>
}

/** abortReason is signal's reason as an Error. */
export function abortReason(signal: AbortSignal): Error {
  const r = signal.reason
  return r instanceof Error ? r : new Error(String(r ?? 'aborted'))
}
