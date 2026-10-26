import { defineConfig } from 'vitest/config'

// The unit tests; the e2e suite has its own config (vitest.e2e.config.ts).
export default defineConfig({
  test: { include: ['test/**/*.test.ts'], exclude: ['test/e2e/**', 'node_modules/**'] },
})
