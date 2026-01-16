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
    ],
    // Exclude integration/e2e tests (require running server) and node_modules
    exclude: [
      '**/node_modules/**',
      'dist',
      'scripts',
      'server/test/integration/**',
      'server/test/e2e/**',
    ],
coverage: {
      provider: 'v8',
      reporter: ['text', 'json', 'json-summary', 'html', 'lcov'],
      reportsDirectory: './coverage',
      // Focus on directly-importable logic that coverage can actually instrument
      // API routes execute via server runtime - validated by integration/e2e tests
      include: [
        // Core business logic (directly imported, sync execution)
        'server/lib/**/*.js',
        'server/engine/aggregations.js',
        // Frontend utilities (directly imported)
        'src/lib/amount-utils.ts',
        'src/lib/utils.ts',
        'src/lib/duckdb-api-client.ts',
        // Hooks that don't require full runtime bootstrap
        'src/hooks/use-toast.ts',
      ],
      exclude: [
        '**/*.test.ts',
        '**/*.test.tsx',
        '**/*.spec.ts',
        '**/*.test.js',
        '**/*.spec.js',
        '**/node_modules/**',
        'src/components/ui/**', // shadcn components
        // Exclude runtime-bootstrapped code (tested via integration/e2e)
        'server/api/**/*.js',
        'server/engine/worker.js',
        'server/engine/api.js',
        'server/engine/ingest.js',
        'server/engine/decoder.js',
        'server/engine/file-index.js',
        'server/engine/gap-detector.js',
        'server/engine/schema.js',
        'server/duckdb/**/*.js',
        // Frontend runtime components (tested via component tests)
        'src/components/**/*.tsx',
        'src/hooks/**/*.ts',
        'src/lib/api-client.ts',
        'src/lib/backend-config.ts',
        'src/lib/config-sync.ts',
      ],
      // Thresholds for core logic coverage
      thresholds: {
        statements: 80,
        branches: 70,
        functions: 80,
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
