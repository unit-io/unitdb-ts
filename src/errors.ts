// The client's errors, named as unitdb-go names them (returncode.go,
// requests.go) where it has a name.

/** The base of the client's errors. */
export class UnitdbError extends Error {
  override name = 'UnitdbError'
}

/**
 * A connect return code (CONNACK), as the server sends it (v0.6.0 and
 * later), and as unitdb-go's returncode.go names it.
 */
export const ReturnCode = {
  Accepted: 0x00,
  BadProtocolVersion: 0x01,
  IdentifierRejected: 0x02,
  BadIdentifier: 0x03,
  /** A refused key, or the insecure flag on a server that refuses it. */
  NotAuthorized: 0x04,
  /** A server error: another server may take the client. */
  ServerError: 0x05,
  AuthenticationFailed: 0x06,
  Forbidden: 0x07,
  SessionInUse: 0x08,
  UnknownEpoch: 0x09,
} as const

const reasons: Record<number, string> = {
  0x01: 'unacceptable protocol version',
  0x02: 'identifier rejected (unknown, expired, revoked or a v1 id)',
  0x03: 'bad identifier',
  0x04: 'not authorized (a refused key, or the insecure flag on a server that refuses it)',
  0x05: 'server error',
  0x06: 'authentication failed',
  0x07: 'forbidden',
  0x08: 'session in use',
  0x09: 'unknown epoch',
}

/** The server refused the connection: code is its return code. */
export class ConnectError extends UnitdbError {
  override name = 'ConnectError'
  constructor(
    readonly server: string,
    readonly code: number,
  ) {
    super(`unitdb: ${server} refused the connection: return code ${code}, ${reasons[code] ?? 'unknown'}`)
  }
}

/**
 * The server refused a call with an error notice that names it (unitdb/error/,
 * with its message id): its status, as an HTTP status.
 */
export class ServerError extends UnitdbError {
  override name = 'ServerError'
  constructor(
    readonly status: number,
    readonly serverMessage: string,
  ) {
    super(`unitdb: the server refused the request (${status}): ${serverMessage}`)
  }
  /** Sending it again may work: the server, or a node it needs, couldn't take it now (503). */
  get temporary(): boolean {
    return this.status === 503
  }
}

/** The client isn't connected, or closed before the server answered. */
export class NotConnected extends UnitdbError {
  override name = 'NotConnected'
  constructor(message = 'unitdb: not connected') {
    super(message)
  }
}

/** The server didn't answer in time. */
export class Timeout extends UnitdbError {
  override name = 'Timeout'
}

/** A call the client refuses before sending it (a bad topic, a payload too large). */
export class InvalidArgument extends UnitdbError {
  override name = 'InvalidArgument'
}

/**
 * The server refused a request to its API (keygen, requestClientId, revoke,
 * revokeAll, vouch), as unitdb-go's RequestError: its status, as an HTTP
 * status (400 a bad request, 403 not allowed).
 */
export class RequestError extends UnitdbError {
  override name = 'RequestError'
  constructor(
    readonly status: number,
    readonly serverMessage: string,
  ) {
    super(serverMessage ? `unitdb: request refused with status ${status}: ${serverMessage}` : `unitdb: request refused with status ${status}`)
  }
}
