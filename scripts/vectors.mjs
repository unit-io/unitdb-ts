// Writes test/vectors.json: frames as the server's own Go code encodes them
// (its utp package), with what each holds, for the codec's tests. The Go
// test that writes them, scripts/vectors/vectors_test.go, runs inside a
// unitdb checkout ($UNITDB_SERVER_DIR, else ../unitdb) by an overlay, so
// the checkout gets no file. With --check, fails if test/vectors.json
// differs from what the server writes.
//
//   UNITDB_SERVER_DIR=../unitdb node scripts/vectors.mjs [--check]
import { execFileSync } from 'node:child_process'
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'

const server = resolve(process.env.UNITDB_SERVER_DIR ?? '../unitdb')
const check = process.argv.includes('--check')
const utp = join(server, 'server', 'utp')
if (!existsSync(utp)) {
  console.log(`vectors: no ${utp} (set UNITDB_SERVER_DIR to a unitdb checkout): skipped`)
  process.exit(0)
}
const tmp = mkdtempSync(join(tmpdir(), 'unitdb-vectors-'))
try {
  const overlay = join(tmp, 'overlay.json')
  writeFileSync(
    overlay,
    JSON.stringify({
      Replace: {
        [join(utp, 'vectors_ts_test.go')]: resolve('scripts/vectors/vectors_test.go'),
        [join(utp, 'export_ts_test.go')]: resolve('scripts/vectors/export_test.go'),
      },
    }),
  )
  const out = join(tmp, 'vectors.json')
  execFileSync('go', ['test', '-count=1', '-overlay', overlay, '-run', '^TestTSVectors$', './server/utp', '-args', '-out', out], { cwd: server, stdio: 'inherit' })
  const want = readFileSync(out)
  const have = existsSync('test/vectors.json') ? readFileSync('test/vectors.json') : null
  if (!have || !have.equals(want)) {
    if (check) {
      console.error('vectors: test/vectors.json differs from what the server writes: run npm run vectors')
      process.exit(1)
    }
    writeFileSync('test/vectors.json', want)
    console.log('vectors: test/vectors.json written')
  }
} finally {
  rmSync(tmp, { recursive: true, force: true })
}
