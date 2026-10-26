// fileStore: session logs in files, for Node (the only module besides the TCP
// transport that uses Node; ci.sh checks): a client's unfinished
// messages survive its process. One file a session, appended to and synced
// before the frame it records is sent, compacted when opened and closed.
/// <reference types="node" />

import { createHash } from 'node:crypto'
import { closeSync, fdatasyncSync, mkdirSync, openSync, readFileSync, renameSync, unlinkSync, writeFileSync, writeSync, existsSync } from 'node:fs'
import { join } from 'node:path'
import { SessionInUse, type SessionLog, type SessionStore } from './store.js'

export interface FileStoreOptions {
  /** Sync each write to the disk before the frame goes out: true by default. */
  sync?: boolean
}

// A record: {"k":"o5","v":"<base64 frame>"} or {"k":"o5","d":1}, a line each.
interface Record {
  k: string
  v?: string
  d?: 1
}

const b64 = (b: Uint8Array) => Buffer.from(b.buffer, b.byteOffset, b.length).toString('base64')

/**
 * fileStore keeps logs under dir, a file for each session, named by a hash
 * of the client id and the session key. A process holding a log open has a
 * lock file beside it; another process opening it is refused while that
 * process runs.
 */
export function fileStore(dir: string, opts: FileStoreOptions = {}): SessionStore {
  const sync = opts.sync ?? true
  mkdirSync(dir, { recursive: true, mode: 0o700 })
  return {
    open(clientId, sessionKey) {
      const name = resolve(dir, hashName(clientId, sessionKey))
      const path = join(dir, `${name}.log`)
      const lock = join(dir, `${name}.lock`)
      takeLock(lock, sessionKey)

      // Read, keeping the last of each key; a torn last line (a crash) is dropped.
      const entries = new Map<string, Uint8Array>()
      if (existsSync(path)) {
        for (const line of readFileSync(path, 'utf8').split('\n')) {
          if (!line) continue
          let r: Record
          try {
            r = JSON.parse(line) as Record
          } catch {
            continue
          }
          if (r.d) entries.delete(r.k)
          else if (r.v !== undefined) entries.set(r.k, new Uint8Array(Buffer.from(r.v, 'base64')))
        }
      }
      const compact = () => {
        const tmp = `${path}.tmp`
        writeFileSync(tmp, [...entries].map(([k, v]) => JSON.stringify({ k, v: b64(v) }) + '\n').join(''), { mode: 0o600 })
        renameSync(tmp, path)
      }
      compact()
      let fd: number | null = openSync(path, 'a', 0o600)
      const append = (r: Record) => {
        if (fd === null) return
        writeSync(fd, JSON.stringify(r) + '\n')
        if (sync) fdatasyncSync(fd)
      }
      const log: SessionLog = {
        entries: () => [...entries],
        put: (k, f) => {
          entries.set(k, f) // a key put again keeps its place: the order of first put
          append({ k, v: b64(f) })
        },
        delete: (k) => {
          if (entries.delete(k)) append({ k, d: 1 })
        },
        clear: () => {
          entries.clear()
          if (fd !== null) closeSync(fd)
          compact()
          fd = openSync(path, 'a', 0o600)
        },
        close: () => {
          if (fd === null) return
          closeSync(fd)
          fd = null
          compact()
          try {
            unlinkSync(lock)
          } catch {
            // gone already
          }
        },
        follow: (id) => {
          // A link: a small file, named after the renewed id's session,
          // holding the name of this log. A log keeps its name across any
          // number of renewals.
          const renewed = hashName(id, sessionKey)
          if (renewed === name || existsSync(join(dir, `${renewed}.log`)) || existsSync(join(dir, `${renewed}.link`))) return
          const tmp = join(dir, `${renewed}.link.tmp`)
          writeFileSync(tmp, name + '\n', { mode: 0o600 })
          renameSync(tmp, join(dir, `${renewed}.link`))
        },
      }
      return log
    },
  }
}

function hashName(clientId: string, sessionKey: number): string {
  return createHash('sha256').update(`${sessionKey}\u0000${clientId}`).digest('hex').slice(0, 32)
}

/** resolve returns the name of the log of session name: its own, or the one its link names. */
function resolve(dir: string, name: string): string {
  const link = join(dir, `${name}.link`)
  if (existsSync(join(dir, `${name}.log`)) || !existsSync(link)) return name
  const target = readFileSync(link, 'utf8').trim()
  return /^[0-9a-f]{32}$/.test(target) ? target : name
}

/** takeLock takes the session's lock file, or refuses if a live process holds it. */
function takeLock(lock: string, sessionKey: number): void {
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const fd = openSync(lock, 'wx', 0o600)
      writeSync(fd, String(process.pid))
      closeSync(fd)
      return
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code !== 'EEXIST') throw err
      const pid = Number(readFileSync(lock, 'utf8'))
      if (pid && pid !== process.pid && alive(pid)) {
        throw new SessionInUse(`unitdb: session ${sessionKey} is open in process ${pid}`)
      }
      if (pid === process.pid) throw new SessionInUse(`unitdb: session ${sessionKey} is open already`)
      unlinkSync(lock) // a dead process's: take it
    }
  }
  throw new SessionInUse(`unitdb: session ${sessionKey}'s lock can't be taken`)
}

function alive(pid: number): boolean {
  try {
    process.kill(pid, 0)
    return true
  } catch (err) {
    return (err as NodeJS.ErrnoException).code === 'EPERM'
  }
}
