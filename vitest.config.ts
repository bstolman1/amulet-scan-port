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
      include: [
        'src/lib/**/*.ts',
        'src/hooks/**/*.ts',
        'src/components/**/*.tsx',
        'server/lib/**/*.js',
        'server/api/**/*.js',
        'server/duckdb/**/*.js',
        'server/engine/**/*.js',
      ],
      exclude: [
        '**/*.test.ts',
        '**/*.test.tsx',
        '**/*.spec.ts',
        '**/*.test.js',
        '**/*.spec.js',
        '**/node_modules/**',
        'src/components/ui/**', // shadcn components
      ],
      // Thresholds for CI enforcement
      thresholds: {
        statements: 20,
        branches: 15,
        functions: 20,
        lines: 20,
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
