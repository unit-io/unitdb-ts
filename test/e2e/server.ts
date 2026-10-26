// unitdb servers for the e2e suite: one standalone, or a cluster of three
// with failover (as the server's own e2e harness starts them). A server can
// be stopped and started again on the same ports and data, signalled
// (SIGTERM drains it, SIGSTOP freezes it), or killed.

import { spawn, type ChildProcess } from 'node:child_process'
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { connect, createServer } from 'node:net'
import { dirname, join, relative } from 'node:path'

/** The key the test servers seal ids with: for tests only. */
export const TEST_KEY = 'ts-e2e-key-0123456789abcdefghijk'

export interface ServerInfo {
  name: string
  tcpAddr: string
  grpcAddr: string
  /** The listen port's WebSocket. */
  wsUrl: string
  conf: string
}

export interface ServerOptions {
  allowInsecure?: boolean
  /** cluster_config, and this node's name (-cluster_self). */
  cluster?: { config: unknown; node: string }
}

/** freePort returns a port nobody listens on now. */
export async function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const s = createServer()
    s.once('error', reject)
    s.listen(0, '127.0.0.1', () => {
      const port = (s.address() as { port: number }).port
      s.close(() => resolve(port))
    })
  })
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

async function listening(addr: string): Promise<boolean> {
  const i = addr.lastIndexOf(':')
  return new Promise<boolean>((resolve) => {
    const s = connect({ host: addr.slice(0, i), port: Number(addr.slice(i + 1)) }, () => {
      s.destroy()
      resolve(true)
    })
    s.once('error', () => resolve(false))
  })
}

/** A server process. */
export class Server {
  private proc: ChildProcess | null = null
  private exit: Promise<void> = Promise.resolve()
  private log = ''

  private constructor(
    private readonly bin: string,
    readonly dir: string,
    readonly info: ServerInfo,
    private readonly args: string[],
  ) {}

  static async create(bin: string, parent: string, opts: ServerOptions = {}): Promise<Server> {
    const dir = mkdtempSync(join(parent, 'srv-'))
    const [tcp, grpc, monitor] = [await freePort(), await freePort(), await freePort()]
    const conf = join(dir, 'unitdb.conf')
    writeFileSync(
      conf,
      JSON.stringify({
        listen: `127.0.0.1:${tcp}`,
        grpc_listen: `127.0.0.1:${grpc}`,
        monitor_listen: `127.0.0.1:${monitor}`,
        // Info: the cluster harness reads the leader from the logs.
        logging_level: opts.cluster ? 'Info' : 'Error',
        allow_insecure: opts.allowInsecure ?? false,
        encryption_config: { key: TEST_KEY, identifier: 'local', sealed: false, timestamp: 1522325758 },
        // None for a standalone server, which takes any as a cluster's.
        ...(opts.cluster ? { cluster_config: opts.cluster.config } : {}),
        store_config: { reset: false, adapters: { unitdb: { database: 'unitdb', mem_size: 64 << 20 } } },
      }),
    )
    // Relative to the binary's directory, where the process runs: the server
    // reads its config from there, whatever path it is given.
    const args = ['-config', relative(dirname(bin), conf), '-db_path', join(dir, 'db')]
    if (opts.cluster) args.push('-cluster_self', opts.cluster.node)
    const info: ServerInfo = {
      name: opts.cluster?.node ?? 'server',
      tcpAddr: `127.0.0.1:${tcp}`,
      grpcAddr: `127.0.0.1:${grpc}`,
      wsUrl: `ws://127.0.0.1:${tcp}/`,
      conf,
    }
    return new Server(bin, dir, info, args)
  }

  /** start starts the process, and waits until it listens. */
  async start(): Promise<void> {
    const env: Record<string, string | undefined> = {
      ...process.env,
      AUTHENTICATOR_NAME: 'e2e',
      AUTHENTICATOR_RP_NAME: 'localhost',
      AUTHENTICATOR_RP_ORIGIN: 'http://localhost:8080',
    }
    for (const k of ['WEB_PORT', 'GRPC_PORT', 'UNITDB_KEYRING']) delete env[k]
    const proc = spawn(this.bin, this.args, { cwd: dirname(this.bin), env, stdio: ['ignore', 'pipe', 'pipe'] })
    this.proc = proc
    let exited: string | null = null
    this.exit = new Promise((resolve) => {
      proc.on('exit', (code, sig) => {
        exited = `${this.info.name} exited (${code ?? sig})`
        if (this.proc === proc) this.proc = null
        resolve()
      })
    })
    proc.stdout!.on('data', (b: Buffer) => (this.log += b.toString()))
    proc.stderr!.on('data', (b: Buffer) => (this.log += b.toString()))
    const deadline = Date.now() + 30_000
    while (!(await listening(this.info.tcpAddr))) {
      if (exited) throw new Error(`${exited}:\n${this.log}`)
      if (Date.now() > deadline) {
        proc.kill('SIGKILL')
        throw new Error(`${this.info.name} didn't listen on ${this.info.tcpAddr}:\n${this.log}`)
      }
      await sleep(50)
    }
  }

  /** signal sends sig to the process. */
  signal(sig: NodeJS.Signals): void {
    this.proc?.kill(sig)
  }

  /** exited resolves when the process has exited. */
  exited(): Promise<void> {
    return this.exit
  }

  /** stop stops it (SIGINT, then SIGKILL after 5 s); its data stays for a start. */
  async stop(): Promise<void> {
    if (!this.proc) return
    this.proc.kill('SIGCONT')
    this.proc.kill('SIGINT')
    const t = setTimeout(() => this.proc?.kill('SIGKILL'), 5000)
    await this.exit
    clearTimeout(t)
  }

  /** kill kills it at once. */
  async kill(): Promise<void> {
    if (!this.proc) return
    this.proc.kill('SIGKILL')
    await this.exit
  }

  /** remove stops it and deletes its data. */
  async remove(): Promise<void> {
    await this.stop()
    rmSync(this.dir, { recursive: true, force: true })
  }

  logs(): string {
    return this.log
  }
}

/** startServer creates and starts a standalone server. */
export async function startServer(bin: string, parent: string, opts: ServerOptions = {}): Promise<{ info: ServerInfo; stop: () => Promise<void>; server: Server }> {
  const s = await Server.create(bin, parent, opts)
  await s.start()
  return { info: s.info, stop: () => s.remove(), server: s }
}

const electedSelf = /Elected myself as a new leader/
const leaderIs = /leader (?:set to )?'([a-z]+)'(?: elected)?/

/** A cluster of three nodes, with failover. */
export class Cluster {
  private constructor(readonly nodes: Server[]) {}

  static async start(bin: string, parent: string, names = ['one', 'two', 'three']): Promise<Cluster> {
    const peers = []
    for (const name of names) peers.push({ name, addr: `127.0.0.1:${await freePort()}` })
    const config = {
      self: '', // set per node with -cluster_self
      nodes: peers,
      failover: { enabled: true, heartbeat: 100, vote_after: 8, node_fail_after: 16 },
    }
    const nodes: Server[] = []
    for (const name of names) nodes.push(await Server.create(bin, parent, { cluster: { config, node: name } }))
    await Promise.all(nodes.map((n) => n.start()))
    const c = new Cluster(nodes)
    await c.waitLeader(nodes)
    return c
  }

  /** leaderSeen is the leader n last logged, or ''. */
  leaderSeen(n: Server): string {
    let leader = ''
    for (const line of n.logs().split('\n')) {
      if (electedSelf.test(line)) leader = n.info.name
      else {
        const m = leaderIs.exec(line)
        if (m && !line.includes('wrong leader')) leader = m[1]!
      }
    }
    return leader
  }

  /** waitLeader waits until the live nodes agree on one leader. */
  async waitLeader(live: Server[], ms = 20_000): Promise<string> {
    const deadline = Date.now() + ms
    for (;;) {
      const seen = new Set(live.map((n) => this.leaderSeen(n)))
      if (seen.size === 1 && !seen.has('')) return [...seen][0]!
      if (Date.now() > deadline) throw new Error(`the nodes didn't agree on a leader: ${[...seen].join(', ')}`)
      await sleep(100)
    }
  }

  async remove(): Promise<void> {
    await Promise.all(this.nodes.map((n) => n.remove()))
  }
}
