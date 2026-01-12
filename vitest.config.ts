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
      'server/**/*.{test,spec}.{js,ts}',
    ],
    // Important: avoid accidentally running dependency test suites (e.g. server/node_modules/**)
    exclude: ['**/node_modules/**', 'dist', 'scripts'],
    coverage: {
      provider: 'v8',
      reporter: ['text', 'json', 'html'],
      include: [
        'src/lib/**/*.ts',
        'src/hooks/**/*.ts',
        'server/lib/**/*.js',
        'server/duckdb/**/*.js',
      ],
      exclude: [
        '**/*.test.ts',
        '**/*.spec.ts',
        '**/node_modules/**',
      ],
    },
    // Server tests need node environment for DuckDB, fs, etc.
    environmentMatchGlobs: [
      ['server/**/*.test.js', 'node'],
      ['server/**/*.spec.js', 'node'],
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
