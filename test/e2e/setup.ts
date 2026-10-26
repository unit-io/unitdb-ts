// The e2e suite's global setup, as unitdb-go's e2e_test.go: build the
// server and mintid from a unitdb checkout ($UNITDB_SERVER_DIR, else
// ../unitdb), start a shared server that allows insecure clients, and stop
// it at the end. Without Go or the source, the suite is skipped (its tests
// read `skip`).

import { execFileSync } from 'node:child_process'
import { existsSync, mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import type { TestProject } from 'vitest/node'
import { Cluster, startServer, type ServerInfo } from './server.js'

export interface E2E {
  skip: string
  bin: string
  mintid: string
  dir: string
  server: ServerInfo | null
}

declare module 'vitest' {
  export interface ProvidedContext {
    e2e: E2E
  }
}

export default async function setup(project: TestProject) {
  try {
    return await run(project)
  } catch (err) {
    // Printed here: vitest's own report of a setup error can fail on its source map.
    console.error('e2e setup failed:', err)
    throw err
  }
}

async function run(project: TestProject) {
  const src = resolve(process.env.UNITDB_SERVER_DIR ?? '../unitdb')
  const e2e: E2E = { skip: '', bin: '', mintid: '', dir: '', server: null }
  let stop = async () => {}
  if (!existsSync(join(src, 'server', 'main.go')) || !existsSync(join(src, 'server', 'cmd', 'mintid'))) {
    e2e.skip = `no unitdb server source (v0.6.0 or later) in ${src} (set UNITDB_SERVER_DIR)`
  } else {
    try {
      execFileSync('go', ['version'], { stdio: 'ignore' })
    } catch {
      e2e.skip = 'no Go toolchain to build the server'
    }
  }
  if (!e2e.skip) {
    e2e.dir = mkdtempSync(join(tmpdir(), 'unitdb-ts-e2e-'))
    e2e.bin = join(e2e.dir, 'unitdb-server')
    e2e.mintid = join(e2e.dir, 'mintid')
    execFileSync('go', ['build', '-o', e2e.bin, './server'], { cwd: src, stdio: 'inherit' })
    execFileSync('go', ['build', '-o', e2e.mintid, './server/cmd/mintid'], { cwd: src, stdio: 'inherit' })
    const s = await startServer(e2e.bin, e2e.dir, { allowInsecure: true })
    e2e.server = s.info
    // Fail the run early if a cluster can't form: the reconnect and cluster tests need one.
    const probe = await Cluster.start(e2e.bin, e2e.dir)
    await probe.remove()
    stop = async () => {
      await s.stop()
      rmSync(e2e.dir, { recursive: true, force: true })
    }
  }
  project.provide('e2e', e2e)
  return stop
}
