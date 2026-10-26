// The frame header's length prefix: a base-128 varint, low 7 bits first, the
// high bit set on every byte but the last (server/utp/message.go
// encodeLength, decodeLength). At most 4 bytes: lengths below 2^28.

/** The largest length the prefix carries. */
export const MAX_VARINT = 2 ** 28 - 1

/** encodeLength returns n's prefix. */
export function encodeLength(n: number): Uint8Array {
  if (!Number.isInteger(n) || n < 0 || n > MAX_VARINT) {
    throw new RangeError(`unitdb: length ${n} out of range`)
  }
  const out: number[] = []
  do {
    let digit = n % 128
    n = Math.floor(n / 128)
    if (n > 0) digit |= 0x80
    out.push(digit)
  } while (n > 0)
  return Uint8Array.from(out)
}

/**
 * decodeLength reads a prefix from buf at offset: its value and size, or
 * null if buf ends before it does. A fifth byte is an error: the server
 * reads at most four.
 */
export function decodeLength(buf: Uint8Array, offset = 0): { value: number; size: number } | null {
  let value = 0
  for (let i = 0; i < 4; i++) {
    if (offset + i >= buf.length) return null
    const digit = buf[offset + i]!
    value += (digit & 0x7f) * 2 ** (7 * i)
    if ((digit & 0x80) === 0) return { value, size: i + 1 }
  }
  throw new RangeError('unitdb: a frame length of more than 4 bytes')
}
