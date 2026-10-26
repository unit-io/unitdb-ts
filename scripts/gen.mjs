// Generates src/gen from proto/*.proto with protoc and protoc-gen-es.
// With --check, generates into a temporary directory and fails if it
// differs from src/gen.
//
//   PROTOC=/path/to/protoc node scripts/gen.mjs [--check]   (protoc on PATH if unset)
import { execFileSync } from 'node:child_process'
import { mkdtempSync, readdirSync, readFileSync, rmSync, existsSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

const check = process.argv.includes('--check')
const protoc = process.env.PROTOC ?? 'protoc'
const out = check ? mkdtempSync(join(tmpdir(), 'unitdb-gen-')) : 'src/gen'
const protos = readdirSync('proto').filter((f) => f.endsWith('.proto')).map((f) => join('proto', f))
execFileSync(protoc, [
  '--plugin=protoc-gen-es=node_modules/.bin/protoc-gen-es',
  `--es_out=${out}`,
  '--es_opt=target=ts,import_extension=js',
  '-I', 'proto',
  ...protos,
], { stdio: 'inherit' })

if (check) {
  let stale = 0
  for (const f of readdirSync(out)) {
    const want = readFileSync(join(out, f))
    const have = existsSync(join('src/gen', f)) ? readFileSync(join('src/gen', f)) : null
    if (!have || !have.equals(want)) {
      console.error(`gen: src/gen/${f} is out of date: run npm run gen`)
      stale++
    }
  }
  rmSync(out, { recursive: true })
  process.exit(stale ? 1 : 0)
}
