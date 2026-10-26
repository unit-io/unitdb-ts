// The session stores: memory and files.

import { appendFileSync, mkdtempSync, readdirSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import { fileStore } from '../src/session/file.js'
import { memoryStore, SessionInUse, type SessionStore } from '../src/session/store.js'

const b = (s: string) => new TextEncoder().encode(s)
const t = (u: Uint8Array) => new TextDecoder().decode(u)
const dirs: string[] = []
const tmp = () => {
  const d = mkdtempSync(join(tmpdir(), 'unitdb-store-'))
  dirs.push(d)
  return d
}
afterEach(() => {
  for (const d of dirs.splice(0)) rmSync(d, { recursive: true, force: true })
})

const stores: [string, () => SessionStore][] = [
  ['memory', () => memoryStore()],
  ['file', () => fileStore(tmp())],
]

describe.each(stores)('%s store', (_, make) => {
  it('keeps entries in the order of first put, across opens', () => {
    const s = make()
    const log = s.open('client-a', 7)
    log.put('o1', b('publish'))
    log.put('i9', b('notify'))
    log.put('o2', b('subscribe'))
    log.put('o1', b('receipt')) // replaced: keeps its place
    log.delete('o2')
    log.close()
    const again = s.open('client-a', 7)
    expect(again.entries().map(([k, v]) => [k, t(v)])).toEqual([
      ['o1', 'receipt'],
      ['i9', 'notify'],
    ])
    again.close()
  })

  it('keeps sessions apart by client and key', () => {
    const s = make()
    const a = s.open('client-a', 1)
    a.put('o1', b('a'))
    a.close()
    const other = s.open('client-b', 1)
    expect(other.entries()).toEqual([])
    other.close()
    const key2 = s.open('client-a', 2)
    expect(key2.entries()).toEqual([])
    key2.close()
  })

  it('refuses a session open twice, until closed', () => {
    const s = make()
    const a = s.open('client-a', 1)
    expect(() => s.open('client-a', 1)).toThrow(SessionInUse)
    a.close()
    s.open('client-a', 1).close()
  })

  it('clears', () => {
    const s = make()
    const a = s.open('client-a', 1)
    a.put('o1', b('a'))
    a.clear()
    a.put('o2', b('b'))
    a.close()
    const again = s.open('client-a', 1)
    expect(again.entries().map(([k]) => k)).toEqual(['o2'])
    again.close()
  })
})

describe('file store', () => {
  it('drops a torn last line', () => {
    const dir = tmp()
    const s = fileStore(dir)
    const a = s.open('client-a', 1)
    a.put('o1', b('whole'))
    a.close()
    const [log] = readdirSync(dir).filter((f) => f.endsWith('.log'))
    appendFileSync(join(dir, log!), '{"k":"o2","v":"dG9y') // a crash mid-write
    const again = s.open('client-a', 1)
    expect(again.entries().map(([k, v]) => [k, t(v)])).toEqual([['o1', 'whole']])
    again.close()
  })

  it("refuses a session another live process holds, and takes a dead one's", () => {
    const dir = tmp()
    const s = fileStore(dir)
    s.open('client-a', 1).close()
    const [log] = readdirSync(dir).filter((f) => f.endsWith('.log'))
    const lock = join(dir, log!.replace(/\.log$/, '.lock'))
    writeFileSync(lock, '1') // pid 1 lives
    expect(() => s.open('client-a', 1)).toThrow(SessionInUse)
    writeFileSync(lock, '999999999') // no such process
    s.open('client-a', 1).close()
  })
})

describe.each(stores)('%s store, a renewed client id', (_, make) => {
  it('finds the log it follows, across renewals', () => {
    const s = make()
    const log = s.open('client-old', 3)
    log.put('o1', b('publish'))
    log.follow?.('client-new')
    log.close()
    // The next run, with the renewed id, finds the log; renewed again, too.
    const next = s.open('client-new', 3)
    expect(next.entries().map(([k, v]) => [k, t(v)])).toEqual([['o1', 'publish']])
    next.put('o2', b('more'))
    next.follow?.('client-newer')
    next.close()
    const newer = s.open('client-newer', 3)
    expect(newer.entries().map(([k]) => k)).toEqual(['o1', 'o2'])
    // One log: open under any of its ids, it is in use.
    expect(() => s.open('client-old', 3)).toThrow(SessionInUse)
    newer.close()
    // Another session key of the renewed id is a session of its own.
    const other = s.open('client-new', 4)
    expect(other.entries()).toEqual([])
    other.close()
  })

  it("doesn't take over a session the renewed id has already", () => {
    const s = make()
    const own = s.open('client-new', 5)
    own.put('o7', b('its own'))
    own.close()
    const log = s.open('client-old', 5)
    log.follow?.('client-new')
    log.close()
    const again = s.open('client-new', 5)
    expect(again.entries().map(([k]) => k)).toEqual(['o7'])
    again.close()
  })
})
