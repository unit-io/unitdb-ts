import { defineConfig } from 'vitest/config'

// The e2e suite: against a server built from source (test/e2e/setup.ts).
export default defineConfig({
  test: {
    include: ['test/e2e/**/*.test.ts'],
    exclude: ['test/e2e/browser/**', '**/node_modules/**'],
    globalSetup: ['test/e2e/setup.ts'],
    testTimeout: 60_000,
    hookTimeout: 120_000,
    // One server, many clients: tests in a file run in turn, files in parallel.
    fileParallelism: true,
  },
})
