// @unit-io/unitdb: a client for the unitdb server. Transports are subpaths:
// @unit-io/unitdb/tcp (Node) and @unit-io/unitdb/ws (browsers, Deno, Node
// 22+); file session logs are @unit-io/unitdb/storage-fs (Node).

export {
  connect,
  Client,
  type ConnectOptions,
  type Delivery,
  type PublishOptions,
  type SubscribeOptions,
  type RelayOptions,
  type Subscription,
  type KeyRequest,
  type TopicKey,
  type IssuedClientId,
} from './client.js'
export { MessageStream, text, type Message } from './stream.js'
export { memoryStore, SessionInUse, type SessionLog, type SessionStore } from './session/store.js'
export { ConnectError, InvalidArgument, NotConnected, RequestError, ReturnCode, ServerError, Timeout, UnitdbError } from './errors.js'
export { parseTopic, matches, wire, validateTopic, type Topic } from './topic.js'
export type { Transport, Conn } from './transport/transport.js'

// The wire codec.
export { encodeLength, decodeLength, MAX_VARINT } from './codec/varint.js'
export { MessageType, FlowControl, MAX_FRAME, FrameDecoder, FrameError, encodeFrame, maxBody, type Frame } from './codec/frame.js'
export { decodeMessage, encodeMessage, control, type Msg, type Direction } from './codec/messages.js'
export * as schema from './gen/unitdb_pb.js'
