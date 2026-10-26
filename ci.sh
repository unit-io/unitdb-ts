#!/usr/bin/env bash
# ci.sh - every check, run before each merge. Needs Node 22+; the
# generated-code check needs protoc (PROTOC, or on PATH); the protocol,
# vectors and e2e checks need Go and a unitdb checkout (UNITDB_SERVER_DIR,
# default ../unitdb).
set -euo pipefail
cd "$(dirname "$0")"
step() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }

step "install (from the lockfile)"
npm ci --no-audit --no-fund >/dev/null

step "typecheck (src without Node's types; tests with them)"
npm run --silent typecheck

step "tests"
npx vitest run

step "build"
npm run --silent build

step "only the TCP transport and the file store import Node"
if grep -rnE "from ['\"]node:|require\(" dist --include=*.js | grep -vE '^dist/(transport/tcp|session/file)\.js:'; then
    echo "dist imports a Node built-in outside the TCP transport and the file store" >&2; exit 1
fi

step "generated code is current"
if [[ -n "${PROTOC:-}" ]] || command -v protoc >/dev/null; then
    node scripts/gen.mjs --check
else
    echo "skipped: no protoc (set PROTOC)"
fi

step "the protocol and vectors match the server's"
node scripts/sync.mjs --check
if command -v go >/dev/null; then
    node scripts/vectors.mjs --check
else
    echo "vectors: skipped: no Go toolchain"
fi

step "e2e: against a server built from source"
if command -v go >/dev/null; then
    npx vitest run --config vitest.e2e.config.ts
else
    echo "skipped: no Go toolchain"
fi

step "no secrets or local paths"
pattern='/Users/|/home/[a-z]|192\.168\.|BEGIN [A-Z ]*PRIVATE KEY|AKIA[0-9A-Z]{16}|gh[pousr]_[A-Za-z0-9]{30,}'
if git ls-files -co --exclude-standard | grep -vE '^(package-lock\.json|ci\.sh)$' | xargs grep -nIE "$pattern"; then
    echo "a secret or a local path (above)" >&2; exit 1
fi

step "ok"
