// A WebSocket to the server's listen port (server/internal/net/hdl_grpc_web.go):
// subprotocol "grpc_web", binary messages, each a chunk of the frame stream.
// The global WebSocket: browsers, Deno, Node 22+.

import { abortReason, type Conn, type Transport } from './transport.js'

// The part of the WebSocket API used: declared here, so that the package
// builds without the DOM's types.
interface WS {
  readonly readyState: number
  readonly protocol: string
  binaryType: string
  send(data: Uint8Array): void
  close(code?: number): void
  onopen: (() => void) | null
  onmessage: ((e: { data: unknown }) => void) | null
  onerror: (() => void) | null
  onclose: ((e: { code: number; reason: string }) => void) | null
}
interface WSConstructor {
  new (url: string, protocol: string): WS
  readonly OPEN: number
}

/** The subprotocol the server's WebSocket speaks. */
export const SUBPROTOCOL = 'grpc_web'

/** webSocket is the server at url ("ws://host:port/", "wss://host/path"). */
export function webSocket(url: string): Transport {
  return {
    address: url,
    open(signal) {
      return new Promise<Conn>((resolve, reject) => {
        if (signal.aborted) return reject(abortReason(signal))
        const WS = (globalThis as { WebSocket?: WSConstructor }).WebSocket
        if (!WS) return reject(new Error('unitdb: no WebSocket in this runtime (Node 22 or later has one)'))
        const ws = new WS(url, SUBPROTOCOL)
        ws.binaryType = 'arraybuffer'
        let opened = false
        let closed = false
        const onAbort = () => {
          ws.close()
          reject(abortReason(signal))
        }
        signal.addEventListener('abort', onAbort, { once: true })
        const conn: Conn = {
          send: (b) => {
            if (!closed && ws.readyState === WS.OPEN) ws.send(b)
          },
          close: () => {
            if (closed) return
            closed = true
            ws.close(1000)
            conn.onClose()
          },
          onData: () => {},
          onClose: () => {},
        }
        ws.onopen = () => {
          signal.removeEventListener('abort', onAbort)
          if (ws.protocol !== SUBPROTOCOL) {
            ws.close()
            return reject(new Error(`unitdb: ${url} answered with subprotocol "${ws.protocol}", not ${SUBPROTOCOL}`))
          }
          opened = true
          resolve(conn)
        }
        ws.onmessage = (e) => {
          if (e.data instanceof ArrayBuffer) conn.onData(new Uint8Array(e.data))
        }
        ws.onerror = () => {
          if (!opened) {
            signal.removeEventListener('abort', onAbort)
            reject(new Error(`unitdb: no WebSocket to ${url}`))
          }
        }
        ws.onclose = (e) => {
          if (!opened) {
            signal.removeEventListener('abort', onAbort)
            return reject(new Error(`unitdb: no WebSocket to ${url} (${e.code})`))
          }
          if (closed) return
          closed = true
          conn.onClose(new Error(`unitdb: ${url} closed the WebSocket (${e.code}${e.reason ? `: ${e.reason}` : ''})`))
        }
      })
    },
  }
}
