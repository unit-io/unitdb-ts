// unitdb-go's topic_test.go, ported.

import { describe, expect, it } from 'vitest'
import { MAX_TOPIC_DEPTH, MAX_TOPIC_LENGTH, matches, parseTopic, validateTopic, wire, type Topic } from '../src/topic.js'

const parse = (text: string): Topic => {
  const t = parseTopic(text)
  if (!t) throw new Error(`parse(${text}) failed`)
  return t
}

describe('topics', () => {
  it('parse', () => {
    const cases: [string, string, string[], Record<string, string>][] = [
      ['teams.alpha.ch1', 'teams.alpha.ch1', ['teams', 'alpha', 'ch1'], {}],
      ['teams.alpha?ttl=1m&delay=10', 'teams.alpha', ['teams', 'alpha'], { ttl: '1m', delay: '10' }],
      ['teams...', 'teams...', ['teams', '...'], {}],
      ['teams.*.ch1', 'teams.*.ch1', ['teams', '*', 'ch1'], {}],
      ['teams?flag', 'teams', ['teams'], {}], // an option without a value is dropped
    ]
    for (const [text, name, parts, opts] of cases) {
      const t = parse(text)
      expect(t.name, text).toBe(name)
      expect(t.parts, text).toEqual(parts)
      expect(Object.fromEntries(t.options), text).toEqual(opts)
    }
  })

  it('wire', () => {
    const cases: [string, string][] = [
      ['teams.alpha', 'teams.alpha'],
      ['KEY/teams.alpha', 'KEY/teams.alpha'],
      ['KEY/teams.alpha?ttl=1m', 'KEY/teams.alpha'],
      ['teams...', 'teams...'],
      ['KEY/teams...?last=1m', 'KEY/teams...'],
      ['unitdb/keygen', 'unitdb/keygen'],
    ]
    for (const [text, want] of cases) expect(wire(parse(text)), text).toBe(want)
    const t = parse('KEY/teams.alpha')
    expect([t.key, t.name]).toEqual(['KEY', 'teams.alpha'])
  })

  it('parse refuses an empty topic', () => {
    for (const text of ['', '/', '?']) expect(parseTopic(text), text).toBeNull()
  })

  it('validate', () => {
    const deep = Array(MAX_TOPIC_DEPTH + 1).fill('a').join('.')
    const cases: [string, boolean][] = [
      ['teams.alpha', false],
      ['teams...', false],
      ['teams.*.ch1', false],
      ['teams.a*.ch1', true],
      ['teams...ch1', true],
      [deep, true],
      ['a'.repeat(MAX_TOPIC_LENGTH + 1), true],
    ]
    for (const [text, bad] of cases) expect(validateTopic(parse(text)) !== null, text.slice(0, 20)).toBe(bad)
    expect(validateTopic(parse('teams.*'), { publish: true })).not.toBeNull()
    expect(validateTopic(parse('teams.alpha'), { publish: true })).toBeNull()
  })

  it('matches', () => {
    const cases: [string, string, boolean][] = [
      ['teams.alpha', 'teams.alpha', true],
      ['teams.alpha', 'teams.beta', false],
      ['teams...', 'teams.alpha', true],
      ['teams...', 'teams.alpha.ch1', true],
      ['teams...', 'other.alpha', false],
      ['teams.*', 'teams.alpha', true],
      ['teams.*', 'teams', false],
      ['teams.*', 'teams.alpha.ch1', false],
      ['teams.*.ch1', 'teams.alpha.ch1', true],
      ['teams.*.ch1', 'teams.alpha.ch2', false],
      ['teams.alpha', 'teams.alpha.ch1', false],
      ['teams.alpha.ch1', 'teams.alpha', false],
      ['...', 'anything.at.all', true],
      ['KEY/teams.alpha', 'teams.alpha', true], // the key doesn't count
    ]
    for (const [sub, msg, want] of cases) expect(matches(parse(sub), parse(msg)), `${sub} ~ ${msg}`).toBe(want)
  })
})
