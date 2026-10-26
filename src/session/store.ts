// Session stores: what the client hasn't finished sending and receiving,
// kept so that a resumed session (after a dropped link, a restart of the
// server or of the client) sends it again: unitdb-go's message log
// (internal/store). Entries are frames, by key: "o<id>" for the client's
// own (a publish, subscribe, unsubscribe or relay not yet acknowledged, or
// its receipt for a reliable publish of its own), "i<id>" for the server's
// (a notification not yet received, or the client's receipt of a reliable
// message whose flow the server hasn't completed).

/** A session's log. */
export interface SessionLog {
  /** The entries, in the order they were first put. */
  entries(): [key: string, frame: Uint8Array][]
  put(key: string, frame: Uint8Array): void
  delete(key: string): void
  /** clear forgets every entry: a clean session. */
  clear(): void
  /** close releases the log; the entries stay for the next open. */
  close(): void
  /**
   * follow makes the log the session's for clientId too, the id the server
   * renewed the client's with: a client opened with it, the next run's,
   * finds this log (as unitdb-go links a renewed id to its store). A store
   * without it starts the renewed id's sessions afresh.
   */
  follow?(clientId: string): void
}

/** Where sessions' logs are kept. */
export interface SessionStore {
  /**
   * open opens the log of the session of clientId and sessionKey. A log
   * open in this process already is refused: two clients sharing a session
   * take it from each other.
   */
  open(clientId: string, sessionKey: number): SessionLog
}

/** SessionInUse: the session's log is open already. */
export class SessionInUse extends Error {
  override name = 'SessionInUse'
}

/**
 * memoryStore keeps logs in memory: they outlive a connection and a
 * client, not the process. The default.
 */
export function memoryStore(): SessionStore {
  const logs = new Map<string, Map<string, Uint8Array>>()
  const open = new Set<string>()
  // A renewed client id's session, to the name of the log it follows.
  const links = new Map<string, string>()
  return {
    open(clientId, sessionKey) {
      const own = `${sessionKey}\u0000${clientId}`
      const name = links.get(own) ?? own
      if (open.has(name)) throw new SessionInUse(`unitdb: session ${sessionKey} is open already`)
      open.add(name)
      let m = logs.get(name)
      if (!m) logs.set(name, (m = new Map()))
      const entries = m
      return {
        entries: () => [...entries],
        put: (k, f) => {
          entries.set(k, f)
        },
        delete: (k) => {
          entries.delete(k)
        },
        clear: () => entries.clear(),
        close: () => {
          open.delete(name)
        },
        follow: (id) => {
          const renewed = `${sessionKey}\u0000${id}`
          if (renewed !== name && !logs.has(renewed) && !links.has(renewed)) links.set(renewed, name)
        },
      }
    },
  }
}

/** A log for one connection only: a client without a session key keeps nothing past itself. */
export function scratchLog(): SessionLog {
  const entries = new Map<string, Uint8Array>()
  return {
    entries: () => [...entries],
    put: (k, f) => {
      entries.set(k, f)
    },
    delete: (k) => {
      entries.delete(k)
    },
    clear: () => entries.clear(),
    close: () => {},
  }
}
