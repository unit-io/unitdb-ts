# unitdb-ts

A TypeScript client for the [unitdb](https://github.com/unit-io/unitdb)
server. It speaks the server's uTP protocol (protobuf frames) over TCP or
WebSocket, for Node 22+, browsers and Deno. Its API and behaviour follow the
Go client, [unitdb-go](https://github.com/unit-io/unitdb-go). It works with
unitdb v0.6.0 and later, and its tests run against v0.7.0.

It covers:
- connecting, with several servers tried in turn;
- publishing (express, reliable, batched and delayed), subscribing and relaying;
- sessions whose unfinished messages are logged (in memory, or in files) and sent again on resume;
- reconnecting across a cluster's nodes;
- topic keys, revocation, vouching and client-id renewal.

```ts
import { connect, text } from '@unit-io/unitdb'
import { tcp } from '@unit-io/unitdb/tcp'       // Node
import { webSocket } from '@unit-io/unitdb/ws'  // browsers, Deno, Node 22+

const db = await connect({
  servers: [tcp('node-one:6060'), tcp('node-two:6060')], // tried in turn
  clientId,
  keepAlive: 30_000,
  onConnectionLost: (err) => console.warn(err),
})

const sub = await db.subscribe('KEY/groups.team.chat', { delivery: 'reliable' })
await db.publish('KEY/groups.team.chat', 'hello', { delivery: 'reliable', ttl: '1h' })
for await (const m of sub) console.log(m.topic, text(m))

const history = db.messages('KEY/groups.team...') // what arrives, without subscribing
await db.relay(['KEY/groups.team.chat'], { last: '1h' })
await db.close()
```

## Sessions and reconnecting

```ts
import { fileStore } from '@unit-io/unitdb/storage-fs'  // Node

const db = await connect({
  servers: [tcp('node-one:6060'), tcp('node-two:6060')],
  clientId,
  sessionKey: 42,                          // resumed by the next client of this id and key
  store: fileStore('/var/lib/app/unitdb'), // memory by default: this process only
  autoReconnect: true,                     // off by default: a lost connection closes the client
  onConnect: () => console.log('connected'),
  onConnectionLost: (err) => console.warn(err),
})
await db.publish('KEY/a.b', 'x', { delivery: 'batch' }) // gathered for batchDuration (100 ms)
await db.publish('KEY/a.b', 'y', { delay: 500 })        // in a batch due half a second later
```

A call made while the client reconnects waits for the connection, up to
`writeTimeout`. What the session's log holds is sent again when the session
resumes, so a message may arrive twice. That covers calls not acknowledged,
receipts not completed, and notifications not received.

## Keys, revocation and vouching

These work as unitdb-go's `Keygen`, `RequestClientID`, `Revoke`, `RevokeAll`
and `Vouch`:

```ts
const [key] = await db.keygen({ topic: 'groups.team.chat', type: 'rw', ttl: 3_600_000 }) // a primary id
await db.subscribe(`${key.key}/groups.team.chat`)
const { clientId: secondary, uuid } = await db.requestClientId()
await db.revoke(key.uuid)                    // for ever; or revoke(uuid, until)
await db.revokeAll()                         // every id and key of the contract issued before now
await db.vouch(serviceId)                    // a user's connection opened by a trusted service
```

The server answers requests in order, on their topic. They aren't logged,
and one the server hadn't answered when the connection is lost fails. After
a reconnect the client vouches again (a refusal goes to `onError`), before
it resumes and subscribes again.

When the server renews the client id (sealed again near the end of its
life), the client connects with it from then on, and its session's log
follows it. `onClientId(id)` is called: save the id for the next run.

## Errors

- `ConnectError`: the server's return code, as `ReturnCode` names it.
- `ServerError`: an error notice naming the call; `temporary` on 503.
- `RequestError`: a refused request, with its `status`.
- `NotConnected`, `Timeout`, `InvalidArgument`.

## The codec

```ts
import { FrameDecoder, decodeMessage, encodeMessage, control, MessageType, FlowControl } from '@unit-io/unitdb'

const decoder = new FrameDecoder()
socket.onmessage = (e) => {
  for (const frame of decoder.push(new Uint8Array(e.data))) {
    const m = decodeMessage(frame, 'server') // { kind: 'connack' | 'publish' | 'control' | … }
  }
}
socket.send(encodeMessage(control(MessageType.PUBLISH, FlowControl.RECEIPT, id)))
```

A frame is a base-128 varint length, then a protobuf `FixedHeader` of that
length, then the body (`MessageLength` bytes). `FrameDecoder` takes the
stream in chunks of any size. It refuses bodies over 4 MiB, as the server
does.

## Development

```sh
npm ci
npm test                                     # vitest
UNITDB_SERVER_DIR=../unitdb npm run e2e      # against a server built from a unitdb checkout (needs Go)
npm run typecheck
UNITDB_SERVER_DIR=../unitdb npm run sync     # proto/unitdb.proto from the server
UNITDB_SERVER_DIR=../unitdb npm run vectors  # test/vectors.json from the server's Go code
PROTOC=/path/to/protoc npm run gen           # src/gen from proto/
./ci.sh                                      # every check
```

`test/vectors.json` holds every frame type in both directions, with what
each one holds. The server's own `utp` package writes it:
`scripts/vectors/vectors_test.go` runs inside the unitdb checkout through
`go test -overlay`, which leaves the checkout unchanged. The codec must
decode each frame to the same fields and encode it back to the same bytes.
`src/gen` was generated with protoc 36.2 and protoc-gen-es 2.16.

## License

MIT (LICENSE). See NOTICE for the generated code and dependencies.
