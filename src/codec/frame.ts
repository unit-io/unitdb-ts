// Frames (server/utp/message.go): a varint length, a protobuf FixedHeader
// of that length, then FixedHeader.MessageLength bytes of the body.

import { create, fromBinary, toBinary } from '@bufbuild/protobuf'
import { FixedHeaderSchema } from '../gen/unitdb_pb.js'
import { decodeLength, encodeLength } from './varint.js'

/** A frame's message type (server/utp MessageType). */
export const MessageType = {
  CONNECT: 1,
  PUBLISH: 2,
  RELAY: 3,
  SUBSCRIBE: 4,
  UNSUBSCRIBE: 5,
  PINGREQ: 6,
  DISCONNECT: 7,
  FLOWCONTROL: 8,
} as const
export type MessageType = (typeof MessageType)[keyof typeof MessageType]

/** A frame's flow control (server/utp FlowControl): NONE for a message. */
export const FlowControl = {
  NONE: 0,
  ACKNOWLEDGE: 1,
  NOTIFY: 2,
  RECEIVE: 3,
  RECEIPT: 4,
  COMPLETE: 5,
} as const
export type FlowControl = (typeof FlowControl)[keyof typeof FlowControl]

/** The largest body read (server/internal/net MaxFrameSize). */
export const MAX_FRAME = 4 << 20
/** The largest header: a FixedHeader is three small fields. */
const MAX_HEADER = 32

/** A frame: its header's fields, and its body's bytes. */
export interface Frame {
  messageType: number
  flowControl: number
  body: Uint8Array
}

/** The frame error: the stream can't be read past it. */
export class FrameError extends Error {
  override name = 'FrameError'
}

/** encodeFrame returns the bytes of a frame with body. */
export function encodeFrame(messageType: number, flowControl: number, body: Uint8Array): Uint8Array {
  const header = toBinary(
    FixedHeaderSchema,
    create(FixedHeaderSchema, { MessageType: messageType, FlowControl: flowControl, MessageLength: body.length }),
  )
  const prefix = encodeLength(header.length)
  const out = new Uint8Array(prefix.length + header.length + body.length)
  out.set(prefix, 0)
  out.set(header, prefix.length)
  out.set(body, prefix.length + header.length)
  return out
}

/** The largest body accepted for a frame with this header: MAX_FRAME, whatever its type. */
export function maxBody(_messageType: number, _flowControl: number): number {
  return MAX_FRAME
}

/**
 * FrameDecoder reads frames from a byte stream that comes in chunks of any
 * size: a frame may span chunks, and a chunk may hold several frames, as
 * WebSocket messages and TCP reads both do.
 */
export class FrameDecoder {
  // buf[start:end] holds the bytes not read yet. buf grows by doubling, so
  // a large frame in small chunks costs linear time, not quadratic.
  private buf: Uint8Array = new Uint8Array(0)
  private start = 0
  private end = 0

  /** push takes the next chunk, and returns the frames it completes. */
  push(chunk: Uint8Array): Frame[] {
    this.append(chunk)
    const frames: Frame[] = []
    for (;;) {
      const f = this.next()
      if (!f) break
      frames.push(f)
    }
    if (this.start === this.end) {
      this.start = this.end = 0
    }
    return frames
  }

  /** The bytes held for a frame not complete yet. */
  get pending(): number {
    return this.end - this.start
  }

  private append(chunk: Uint8Array): void {
    if (chunk.length === 0) return
    if (this.end + chunk.length > this.buf.length) {
      const live = this.end - this.start
      let cap = Math.max(this.buf.length, 4096)
      while (cap < live + chunk.length) cap *= 2
      if (cap === this.buf.length) {
        this.buf.copyWithin(0, this.start, this.end) // room enough once the read bytes go
      } else {
        const b = new Uint8Array(cap)
        b.set(this.buf.subarray(this.start, this.end), 0)
        this.buf = b
      }
      this.end = live
      this.start = 0
    }
    this.buf.set(chunk, this.end)
    this.end += chunk.length
  }

  private next(): Frame | null {
    const view = this.buf.subarray(this.start, this.end)
    let len
    try {
      len = decodeLength(view, 0)
    } catch (e) {
      throw new FrameError((e as Error).message)
    }
    if (len === null) return null
    if (len.value === 0 || len.value > MAX_HEADER) {
      throw new FrameError(`unitdb: a frame header of ${len.value} bytes`)
    }
    const headerEnd = len.size + len.value
    if (headerEnd > view.length) return null
    let h
    try {
      h = fromBinary(FixedHeaderSchema, view.subarray(len.size, headerEnd))
    } catch (e) {
      throw new FrameError(`unitdb: an unreadable frame header: ${(e as Error).message}`)
    }
    if (h.MessageType === 0) {
      throw new FrameError('unitdb: a frame of message type 0')
    }
    const n = h.MessageLength
    if (n < 0 || n > maxBody(h.MessageType, h.FlowControl)) {
      throw new FrameError(`unitdb: a frame body of ${n} bytes`)
    }
    if (headerEnd + n > view.length) return null
    const body = view.slice(headerEnd, headerEnd + n)
    this.start += headerEnd + n
    return { messageType: h.MessageType, flowControl: h.FlowControl, body }
  }
}
