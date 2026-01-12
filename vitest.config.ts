import { defineConfig } from 'vitest/config';
import path from 'path';

export default defineConfig({
  test: {
    globals: true,
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
    // Server-side tests need node environment
    environmentMatchGlobs: [
      ['server/**/*.test.js', 'node'],
      ['server/**/*.integration.test.js', 'node'],
      ['server/**/*.e2e.test.js', 'node'],
    ],
    deps: {
      // Allow supertest and other node modules to be resolved
      inline: [/supertest/],
    },
  },
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
});
