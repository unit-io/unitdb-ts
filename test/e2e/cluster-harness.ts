// Helpers of the cluster tests, as unitdb-go's cluster_e2e_test.go: a
// cluster each test, a service's id (a cluster node refuses the insecure
// flag), and connection events.

import { onTestFinished } from 'vitest'
import { connect, type Client, type ConnectOptions } from '../../src/client.js'
import { tcp } from '../../src/transport/tcp.js'
import { webSocket } from '../../src/transport/ws.js'
import type { Transport } from '../../src/transport/transport.js'
import { e2e, mintId, RawConn, sleep, waitUntil, E2E_TIMEOUT } from './harness.js'
import { Cluster, type Server } from './server.js'

/** The topics under a prefix each test uses: enough to land on every node. */
export const CLUSTER_TOPICS = 30

export function topics(prefix: string): string[] {
  return Array.from({ length: CLUSTER_TOPICS }, (_, i) => `${prefix}.t${i}`)
}

/** settle lets forwarded subscriptions reach their topics' owners. */
export const settle = () => sleep(200)

/** startCluster starts a cluster of three, removed when the test ends. */
export async function startCluster(): Promise<Cluster> {
  const c = await Cluster.start(e2e.bin, e2e.dir)
  onTestFinished(() => c.remove())
  return c
}

/** serviceId mints a service's id for the cluster: no insecure flag, no keys. */
export function serviceId(c: Cluster): string {
  return mintId({ service: true, conf: c.nodes[0]!.info.conf })
}

export const tcpOf = (n: Server): Transport => tcp(n.info.tcpAddr)
export const wsOf = (n: Server): Transport => webSocket(n.info.wsUrl)

/** others are the nodes of c but n. */
export const others = (c: Cluster, n: Server) => c.nodes.filter((o) => o !== n)

let sessKeys = 700_000 + Math.floor(Math.random() * 100_000)

/** Connection events: losses and connections. */
export class Events {
  lost: Error[] = []
  connected = 0
  options(): Partial<ConnectOptions> {
    return { onConnectionLost: (e) => this.lost.push(e), onConnect: () => this.connected++ }
  }
  async waitLost(n = 1): Promise<Error> {
    await waitUntil('the connection lost handler', () => this.lost.length >= n)
    return this.lost[n - 1]!
  }
  async waitConnected(n: number, ms = E2E_TIMEOUT): Promise<void> {
    await waitUntil(`connection ${n}`, () => this.connected >= n, ms)
  }
}

/** clusterClient connects a service's client, closed when the test ends. */
export async function clusterClient(servers: Transport | Transport[], clientId: string, opts: Partial<ConnectOptions> = {}): Promise<Client> {
  const c = await connect({ servers, clientId, sessionKey: sessKeys++, ...opts })
  onTestFinished(() => c.close())
  return c
}

/** publishTo publishes payload on topic from a raw connection to n. */
export async function publishTo(n: Server, clientId: string, topic: string, payload: string, mode = 0): Promise<void> {
  const pub = await RawConn.connect(clientId, n.info.tcpAddr)
  pub.publish(topic, payload, mode)
}
