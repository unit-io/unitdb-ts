// The decoder's refusals: what the Go readers refuse, and a stream that
// can't be read past.

import { create, toBinary } from '@bufbuild/protobuf'
import { describe, expect, it } from 'vitest'
import { FixedHeaderSchema } from '../src/gen/unitdb_pb.js'
import {
  decodeMessage,
  encodeFrame,
  encodeLength,
  FlowControl,
  FrameDecoder,
  FrameError,
  MAX_FRAME,
  MessageType,
} from '../src/index.js'

/** A frame's header alone, claiming a body of n bytes. */
function header(messageType: number, flowControl: number, n: number): Uint8Array {
  const h = toBinary(FixedHeaderSchema, create(FixedHeaderSchema, { MessageType: messageType, FlowControl: flowControl, MessageLength: n }))
  return Uint8Array.from([...encodeLength(h.length), ...h])
}

describe('the decoder refuses', () => {
  it('a body over the frame limit', () => {
    expect(() => new FrameDecoder().push(header(MessageType.PUBLISH, FlowControl.NONE, MAX_FRAME + 1))).toThrow(FrameError)
  })
  it('a control frame over the frame limit', () => {
    expect(() => new FrameDecoder().push(header(MessageType.PUBLISH, FlowControl.ACKNOWLEDGE, MAX_FRAME + 1))).toThrow(FrameError)
  })
  it('a negative body length', () => {
    expect(() => new FrameDecoder().push(header(MessageType.PUBLISH, FlowControl.NONE, -1))).toThrow(FrameError)
  })
  it('an empty header, and one too long', () => {
    expect(() => new FrameDecoder().push(Uint8Array.of(0))).toThrow(FrameError)
    expect(() => new FrameDecoder().push(Uint8Array.of(100))).toThrow(FrameError)
  })
  it('message type 0', () => {
    expect(() => new FrameDecoder().push(header(0, FlowControl.ACKNOWLEDGE, 0))).toThrow(FrameError)
  })
  it('an unreadable header', () => {
    expect(() => new FrameDecoder().push(Uint8Array.of(3, 0xff, 0xff, 0xff))).toThrow(FrameError)
  })
})

describe('messages', () => {
  it("that aren't the sender's are refused", () => {
    const sub = { messageType: MessageType.SUBSCRIBE, flowControl: FlowControl.NONE, body: new Uint8Array(0) }
    expect(() => decodeMessage(sub, 'server')).toThrow(FrameError)
    expect(decodeMessage(sub, 'client').kind).toBe('subscribe')
  })
  it('message type 9 (a store frame of another server) is refused', () => {
    const f = { messageType: 9, flowControl: FlowControl.NONE, body: new Uint8Array(0) }
    expect(() => decodeMessage(f, 'server')).toThrow(FrameError)
  })
  it('an unreadable body is a FrameError', () => {
    const f = { messageType: MessageType.PUBLISH, flowControl: FlowControl.NONE, body: Uint8Array.of(0x0a, 0x05, 1) }
    expect(() => decodeMessage(f, 'server')).toThrow(FrameError)
  })
  it('PINGREQ and DISCONNECT go with empty bodies', () => {
    expect(encodeFrame(MessageType.PINGREQ, FlowControl.NONE, new Uint8Array(0)).at(-1)).not.toBe(undefined)
    const [f] = new FrameDecoder().push(encodeFrame(MessageType.PINGREQ, FlowControl.NONE, new Uint8Array(0)))
    expect(f!.body.length).toBe(0)
  })
})
