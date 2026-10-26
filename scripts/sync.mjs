// Copies the server's protocol into this repo: proto/unitdb.proto, from
// server/proto/unitdb.proto in a unitdb checkout ($UNITDB_SERVER_DIR, else
// ../unitdb). With --check, fails if it differs from the copy here instead.
// The copy loses its go_package option, which means nothing here.
//
//   UNITDB_SERVER_DIR=../unitdb node scripts/sync.mjs [--check]
import { existsSync, readFileSync, writeFileSync } from 'node:fs'
import { join, resolve } from 'node:path'

const server = resolve(process.env.UNITDB_SERVER_DIR ?? '../unitdb')
const check = process.argv.includes('--check')
const from = join(server, 'server', 'proto', 'unitdb.proto')
const to = 'proto/unitdb.proto'

if (!existsSync(from)) {
  console.log(`sync: no ${from} (set UNITDB_SERVER_DIR to a unitdb checkout): skipped`)
  process.exit(0)
}
const want = Buffer.from(readFileSync(from, 'utf8').replace(/^option go_package\s*=.*\r?\n/m, ''))
const have = existsSync(to) ? readFileSync(to) : null
if (have && have.equals(want)) process.exit(0)
if (check) {
  console.error(`sync: ${to} differs from the server's: run npm run sync`)
  process.exit(1)
}
writeFileSync(to, want)
console.log(`sync: ${to} from ${from}`)
