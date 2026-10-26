// TCP, and TCP with TLS, for Node: the server's listen port takes the frames
// as they are. The only module of the package that uses Node (ci.sh checks).
/// <reference types="node" />

import { connect as netConnect, type Socket } from 'node:net'
import { connect as tlsConnect, type ConnectionOptions } from 'node:tls'
import { abortReason, type Conn, type Transport } from './transport.js'

export interface TcpOptions {
  /** TLS, with these options (servername, ca…); plain TCP if unset. */
  tls?: ConnectionOptions
}

/**
 * tcp is the server at address ("host:port", or "tcp://host:port"; "tls://"
 * implies TLS).
 */
export function tcp(address: string, opts: TcpOptions = {}): Transport {
  let tls = opts.tls
  let hostPort = address
  const m = /^(tcp|tls):\/\/(.*)$/.exec(address)
  if (m) {
    hostPort = m[2]!.replace(/\/$/, '')
    if (m[1] === 'tls') tls ??= {}
  }
  const i = hostPort.lastIndexOf(':')
  if (i <= 0) throw new TypeError(`unitdb: ${address} isn't host:port`)
  const host = hostPort.slice(0, i).replace(/^\[(.*)\]$/, '$1')
  const port = Number(hostPort.slice(i + 1))
  const label = `${tls ? 'tls' : 'tcp'}://${hostPort}`
  return {
    address: label,
    open(signal) {
      return new Promise<Conn>((resolve, reject) => {
        if (signal.aborted) return reject(abortReason(signal))
        const sock: Socket = tls
          ? tlsConnect({ host, port, servername: host, ...tls })
          : netConnect({ host, port })
        sock.setNoDelay(true)
        const onAbort = () => {
          sock.destroy()
          reject(abortReason(signal))
        }
        signal.addEventListener('abort', onAbort, { once: true })
        const onError = (err: Error) => {
          signal.removeEventListener('abort', onAbort)
          reject(err)
        }
        sock.once('error', onError)
        sock.once(tls ? 'secureConnect' : 'connect', () => {
          signal.removeEventListener('abort', onAbort)
          sock.off('error', onError)
          let closed = false
          const conn: Conn = {
            send: (b) => {
              if (!closed) sock.write(b)
            },
            close: () => {
              if (closed) return
              closed = true
              sock.destroy()
              conn.onClose()
            },
            onData: () => {},
            onClose: () => {},
          }
          sock.on('data', (chunk: Buffer) => conn.onData(new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.length)))
          let failure: Error | undefined
          sock.on('error', (err) => {
            failure = err
          })
          sock.on('close', () => {
            if (closed) return
            closed = true
            conn.onClose(failure ?? new Error(`unitdb: ${label} closed the connection`))
          })
          resolve(conn)
        })
      })
    },
  }
}
