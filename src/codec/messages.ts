// Messages: a frame's body decoded as the Go readers decode it
// (server/internal/net/message.go for what a client sends, unitdb-go's
// internal/net for what the server sends), and encoded back as server/utp
// encodes it.

import { create, fromBinary, toBinary, type MessageInitShape } from '@bufbuild/protobuf'
import {
  ConnectAcknowledgeSchema,
  ConnectSchema,
  ControlMessageSchema,
  PublishSchema,
  RelaySchema,
  SubscribeSchema,
  UnsubscribeSchema,
  type Connect,
  type ConnectAcknowledge,
  type ControlMessage,
  type Publish,
  type Relay,
  type Subscribe,
  type Unsubscribe,
} from '../gen/unitdb_pb.js'
import { encodeFrame, FlowControl, FrameError, MessageType, type Frame } from './frame.js'

/** Who sent a frame: what it holds depends on it. */
export type Direction = 'client' | 'server'

/** A decoded message. */
export type Msg =
  | { kind: 'connect'; msg: Connect }
  | { kind: 'connack'; connack: ConnectAcknowledge; messageId: number }
  | { kind: 'publish'; msg: Publish }
  | { kind: 'relay'; msg: Relay }
  | { kind: 'subscribe'; msg: Subscribe }
  | { kind: 'unsubscribe'; msg: Unsubscribe }
  | { kind: 'pingreq' }
  | { kind: 'disconnect' }
  | { kind: 'control'; messageType: number; flowControl: number; msg: ControlMessage }

function decode<T>(what: string, f: () => T): T {
  try {
    return f()
  } catch (e) {
    throw new FrameError(`unitdb: an unreadable ${what}: ${(e as Error).message}`)
  }
}

/** decodeMessage decodes a frame from direction's side. */
export function decodeMessage(frame: Frame, from: Direction): Msg {
  const { messageType: t, flowControl: fc, body } = frame
  // PINGREQ and DISCONNECT carry nothing; the server doesn't read their bodies.
  if (fc === FlowControl.NONE && t === MessageType.PINGREQ) return { kind: 'pingreq' }
  if (fc === FlowControl.NONE && t === MessageType.DISCONNECT) return { kind: 'disconnect' }
  if (fc !== FlowControl.NONE) {
    const msg = decode('control message', () => fromBinary(ControlMessageSchema, body))
    if (t === MessageType.CONNECT && fc === FlowControl.ACKNOWLEDGE) {
      // CONNACK: a ControlMessage whose Message is a ConnectAcknowledge.
      const connack = decode('connect acknowledgement', () => fromBinary(ConnectAcknowledgeSchema, msg.Message))
      return { kind: 'connack', connack, messageId: msg.MessageID }
    }
    return { kind: 'control', messageType: t, flowControl: fc, msg }
  }
  if (t === MessageType.PUBLISH) return { kind: 'publish', msg: decode('publish', () => fromBinary(PublishSchema, body)) }
  if (from === 'client') {
    switch (t) {
      case MessageType.CONNECT:
        return { kind: 'connect', msg: decode('connect', () => fromBinary(ConnectSchema, body)) }
      case MessageType.RELAY:
        return { kind: 'relay', msg: decode('relay', () => fromBinary(RelaySchema, body)) }
      case MessageType.SUBSCRIBE:
        return { kind: 'subscribe', msg: decode('subscribe', () => fromBinary(SubscribeSchema, body)) }
      case MessageType.UNSUBSCRIBE:
        return { kind: 'unsubscribe', msg: decode('unsubscribe', () => fromBinary(UnsubscribeSchema, body)) }
    }
  }
  throw new FrameError(`unitdb: message type ${t} isn't one the ${from} sends`)
}

/** encodeMessage returns the frame of m. */
export function encodeMessage(m: Msg): Uint8Array {
  switch (m.kind) {
    case 'connect':
      return encodeFrame(MessageType.CONNECT, FlowControl.NONE, toBinary(ConnectSchema, m.msg))
    case 'connack': {
      const inner = toBinary(ConnectAcknowledgeSchema, m.connack)
      const ctrl = create(ControlMessageSchema, { MessageID: m.messageId, Message: inner })
      return encodeFrame(MessageType.CONNECT, FlowControl.ACKNOWLEDGE, toBinary(ControlMessageSchema, ctrl))
    }
    case 'publish':
      return encodeFrame(MessageType.PUBLISH, FlowControl.NONE, toBinary(PublishSchema, m.msg))
    case 'relay':
      return encodeFrame(MessageType.RELAY, FlowControl.NONE, toBinary(RelaySchema, m.msg))
    case 'subscribe':
      return encodeFrame(MessageType.SUBSCRIBE, FlowControl.NONE, toBinary(SubscribeSchema, m.msg))
    case 'unsubscribe':
      return encodeFrame(MessageType.UNSUBSCRIBE, FlowControl.NONE, toBinary(UnsubscribeSchema, m.msg))
    case 'pingreq':
      return encodeFrame(MessageType.PINGREQ, FlowControl.NONE, new Uint8Array(0))
    case 'disconnect':
      return encodeFrame(MessageType.DISCONNECT, FlowControl.NONE, new Uint8Array(0))
    case 'control':
      return encodeFrame(m.messageType, m.flowControl, toBinary(ControlMessageSchema, m.msg))
  }
}

/** control returns a flow-control message: an acknowledgement, a receipt… */
export function control(messageType: number, flowControl: number, messageId: number, message?: Uint8Array): Msg {
  const init: MessageInitShape<typeof ControlMessageSchema> = { MessageID: messageId }
  if (message) init.Message = message
  return { kind: 'control', messageType, flowControl, msg: create(ControlMessageSchema, init) }
}
