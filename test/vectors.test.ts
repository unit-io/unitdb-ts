// The wire vectors the server's Go code writes (its utp package, run by
// scripts/vectors.mjs): every frame decodes to what Go says it holds, and
// encodes back to the same bytes.

import { readFileSync } from 'node:fs'
import { toJson, type DescMessage } from '@bufbuild/protobuf'
import { describe, expect, it } from 'vitest'
import {
  ConnectAcknowledgeSchema,
  ConnectSchema,
  ControlMessageSchema,
  PublishSchema,
  RelaySchema,
  SubscribeSchema,
  UnsubscribeSchema,
} from '../src/gen/unitdb_pb.js'
import {
  decodeLength,
  decodeMessage,
  encodeLength,
  encodeMessage,
  FrameDecoder,
  MAX_FRAME,
  type Msg,
} from '../src/index.js'

interface Vectors {
  varints: { value: number; hex: string }[]
  frames: {
    name: string
    direction: 'client' | 'server'
    hex: string
    messageType: number
    flowControl: number
    body?: string
    message?: unknown
    connack?: unknown
  }[]
  stream: { hex: string; frames: string[] }
  limits: { maxFrame: number }
}

const v: Vectors = JSON.parse(readFileSync(new URL('./vectors.json', import.meta.url), 'utf8'))
const bytes = (hex: string) => Uint8Array.from(Buffer.from(hex, 'hex'))
const hexOf = (b: Uint8Array) => Buffer.from(b).toString('hex')

const schemas: Record<string, DescMessage> = {
  'unitdb.schema.Connect': ConnectSchema,
  'unitdb.schema.ControlMessage': ControlMessageSchema,
  'unitdb.schema.Publish': PublishSchema,
  'unitdb.schema.Relay': RelaySchema,
  'unitdb.schema.Subscribe': SubscribeSchema,
  'unitdb.schema.Unsubscribe': UnsubscribeSchema,
}

/** The protobuf message a decoded Msg holds, and its schema. */
function bodyOf(m: Msg): { schema: DescMessage; msg: unknown } | null {
  switch (m.kind) {
    case 'connect':
      return { schema: ConnectSchema, msg: m.msg }
    case 'publish':
      return { schema: PublishSchema, msg: m.msg }
    case 'relay':
      return { schema: RelaySchema, msg: m.msg }
    case 'subscribe':
      return { schema: SubscribeSchema, msg: m.msg }
    case 'unsubscribe':
      return { schema: UnsubscribeSchema, msg: m.msg }
    case 'control':
      return { schema: ControlMessageSchema, msg: m.msg }
    default:
      return null
  }
}

describe('varints', () => {
  for (const { value, hex } of v.varints) {
    it(`${value}`, () => {
      expect(hexOf(encodeLength(value))).toBe(hex)
      expect(decodeLength(bytes(hex))).toEqual({ value, size: hex.length / 2 })
      expect(decodeLength(bytes(hex).subarray(0, hex.length / 2 - 1))).toBeNull()
    })
  }
  it('refuses a fifth byte', () => {
    expect(() => decodeLength(Uint8Array.of(0xff, 0xff, 0xff, 0xff, 0x01))).toThrow()
  })
})

describe('limits', () => {
  it("are the server's", () => {
    expect(MAX_FRAME).toBe(v.limits.maxFrame)
  })
})

describe('frames', () => {
  for (const f of v.frames) {
    it(`${f.name} (from the ${f.direction})`, () => {
      const frames = new FrameDecoder().push(bytes(f.hex))
      expect(frames).toHaveLength(1)
      const fr = frames[0]!
      expect(fr.messageType).toBe(f.messageType)
      expect(fr.flowControl).toBe(f.flowControl)

      const m = decodeMessage(fr, f.direction)
      if (m.kind === 'connack') {
        // A CONNACK: the ControlMessage, and the ConnectAcknowledge in it.
        expect(f.body).toBe('unitdb.schema.ControlMessage')
        expect(toJson(ConnectAcknowledgeSchema, m.connack)).toEqual(f.connack ?? {})
      } else {
        const b = bodyOf(m)
        if (f.body) {
          expect(b).not.toBeNull()
          expect(b!.schema).toBe(schemas[f.body])
          expect(toJson(b!.schema, b!.msg as never)).toEqual(f.message)
        } else if (b) {
          expect(toJson(b.schema, b.msg as never)).toEqual({}) // an empty body
        }
      }
      expect(hexOf(encodeMessage(m))).toBe(f.hex)
    })
  }
})

describe('the stream', () => {
  const all = bytes(v.stream.hex)
  const names = (msgs: Msg[]) => msgs.map((m) => m.kind)
  const whole = names(new FrameDecoder().push(all).map((f) => decodeMessage(f, 'server')))
  it('reads every frame at once', () => {
    expect(whole).toHaveLength(v.stream.frames.length)
  })
  it('reads them split at every byte', () => {
    for (let i = 0; i <= all.length; i++) {
      const d = new FrameDecoder()
      const got = [...d.push(all.subarray(0, i)), ...d.push(all.subarray(i))]
      expect(names(got.map((f) => decodeMessage(f, 'server'))), `split at ${i}`).toEqual(whole)
      expect(d.pending).toBe(0)
    }
  })
  it('reads them a byte at a time', () => {
    const d = new FrameDecoder()
    const got = []
    for (const b of all) got.push(...d.push(Uint8Array.of(b)))
    expect(got).toHaveLength(whole.length)
  })
})

describe('a large frame in small chunks', () => {
  it('reads 64 KiB in 100-byte chunks', () => {
    const f = v.frames.find((x) => x.name === 'publish 64 KiB')!
    const all = bytes(f.hex)
    const d = new FrameDecoder()
    const got = []
    for (let i = 0; i < all.length; i += 100) got.push(...d.push(all.subarray(i, i + 100)))
    expect(got).toHaveLength(1)
    expect(hexOf(encodeMessage(decodeMessage(got[0]!, 'client')))).toBe(f.hex)
  })
})
