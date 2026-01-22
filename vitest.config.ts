import { defineConfig } from 'vitest/config';
import path from 'path';

export default defineConfig({
  test: {
    globals: true,
    // Default to jsdom for React component tests
    environment: 'jsdom',
    setupFiles: ['./src/test/setup.ts'],
    include: [
      'src/**/*.{test,spec}.{js,ts,tsx}',
      'server/test/guardrails/**/*.{test,spec}.js',
      'server/**/*.{test,spec}.{js,ts}',
      'scripts/ingest/test/**/*.{test,spec}.js',
    ],
    // Exclude integration/e2e tests (require running server) and standalone scripts
    exclude: [
      '**/node_modules/**',
      'dist',
      'server/test/integration/**',
      'server/test/e2e/**',
      // This file uses a custom check() runner, not vitest
      'scripts/ingest/test/pipeline-health.test.js',
    ],
coverage: {
      provider: 'v8',
      reporter: ['text', 'json', 'json-summary', 'html', 'lcov'],
      reportsDirectory: './coverage',
      // Focus on directly-importable pure logic that coverage can instrument
      // Runtime clients and API routes are validated by integration/e2e tests
      include: [
        // Core server business logic
        'server/lib/**/*.js',
        'server/engine/aggregations.js',
        // Pure frontend utilities
        'src/lib/amount-utils.ts',
        'src/lib/utils.ts',
        // Ingest scripts - core transform/validation logic
        'scripts/ingest/acs-schema.js',
        'scripts/ingest/data-schema.js',
        'scripts/ingest/encoding.js',
        'scripts/ingest/path-utils.js',
        'scripts/ingest/atomic-cursor.js',
      ],
      exclude: [
        '**/*.test.ts',
        '**/*.test.tsx',
        '**/*.spec.ts',
        '**/*.test.js',
        '**/*.spec.js',
        '**/node_modules/**',
        'src/components/ui/**',
        // Runtime clients tested via integration tests
        'src/lib/duckdb-api-client.ts',
        'src/lib/api-client.ts',
      ],
      // Thresholds for pure business logic
      thresholds: {
        statements: 80,
        branches: 65,
        functions: 85,
        lines: 80,
      },
    },
    // Server tests need node environment for DuckDB, fs, etc.
    environmentMatchGlobs: [
      ['server/**/*.test.js', 'node'],
      ['server/**/*.spec.js', 'node'],
      ['server/test/**/*.js', 'node'],
    ],
    // Increase timeout for slow database queries
    testTimeout: 15000,
  },
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
});
