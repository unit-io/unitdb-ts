// Topics as unitdb-go reads them (topic.go): an optional key before '/', the
// name, and options after '?' ("teams.alpha?ttl=1m&last=1h"). Parts are
// separated by '.'; '*' matches one part, a trailing '...' any number.

export const WILDCARD = '*'
export const MULTI_WILDCARD = '...'
export const MAX_TOPIC_LENGTH = 65535
export const MAX_TOPIC_DEPTH = 100

export interface Topic {
  /** The key, or ''. */
  key: string
  /** The name as given, without key or options ("teams..." keeps its wildcard). */
  name: string
  /** The parts matched: the name split at '.', with '...' last if it ends so. */
  parts: string[]
  options: Map<string, string>
}

/** parseTopic reads text, or returns null if it holds no name. */
export function parseTopic(text: string): Topic | null {
  const slash = text.split('/').filter((s) => s !== '')
  if (slash.length === 0) return null
  let key = ''
  let rest = slash[0]!
  if (slash.length > 1) {
    key = slash[0]!
    rest = slash[1]!
  }
  const q = rest.split('?').filter((s) => s !== '')
  if (q.length === 0) return null
  const name = q[0]!
  const options = new Map<string, string>()
  if (q.length > 1) {
    for (const o of q[1]!.split('&').filter((s) => s !== '')) {
      const kv = o.split('=').filter((s) => s !== '')
      if (kv.length >= 2) options.set(kv[0]!, kv[1]!)
    }
  }
  let base = name
  const multi = base.endsWith(MULTI_WILDCARD)
  if (multi) base = base.replace(/\.+$/, '')
  const parts = base.split('.').filter((s) => s !== '')
  if (multi) parts.push(MULTI_WILDCARD)
  return { key, name, parts, options }
}

/** wire is the topic as it goes to the server: key/name, without options. */
export function wire(t: Topic): string {
  return t.key === '' ? t.name : `${t.key}/${t.name}`
}

/** validateTopic returns why t isn't a topic to send, or null. */
export function validateTopic(t: Topic, { publish = false } = {}): string | null {
  const base = t.name.endsWith(MULTI_WILDCARD) ? t.name.replace(/\.+$/, '') : t.name
  if (base.length === 0 && !(t.parts.length === 1 && t.parts[0] === MULTI_WILDCARD)) {
    return 'a topic needs at least one character'
  }
  if (t.name.length > MAX_TOPIC_LENGTH) return `a topic of ${t.name.length} characters (at most ${MAX_TOPIC_LENGTH})`
  if (t.parts.length > MAX_TOPIC_DEPTH) return `a topic of ${t.parts.length} parts (at most ${MAX_TOPIC_DEPTH})`
  const at = t.name.indexOf(MULTI_WILDCARD)
  if (at >= 0 && at !== t.name.length - MULTI_WILDCARD.length) return "the wildcard '...' only at a topic's end"
  for (let i = 0; i < t.parts.length; i++) {
    const p = t.parts[i]!
    if (p === MULTI_WILDCARD) {
      if (i !== t.parts.length - 1) return "the wildcard '...' only at a topic's end"
      continue
    }
    if (p.includes(MULTI_WILDCARD)) return "the wildcard '...' only at a topic's end"
    if (p.includes(WILDCARD) && p !== WILDCARD) return "a part with a wildcard is '*' alone"
  }
  if (publish && t.parts.some((p) => p === WILDCARD || p === MULTI_WILDCARD)) {
    return 'a publish goes to a topic without wildcards'
  }
  return null
}

/** matches reports whether a message on topic msg reaches a subscription to sub. */
export function matches(sub: Topic, msg: Topic): boolean {
  if (sub.parts[0] === MULTI_WILDCARD) return true
  for (let i = 0; i < sub.parts.length; i++) {
    const s = sub.parts[i]!
    if (s === MULTI_WILDCARD) return true
    if (i >= msg.parts.length) return false
    if (s !== WILDCARD && s !== msg.parts[i]) return false
  }
  return msg.parts.length === sub.parts.length
}
