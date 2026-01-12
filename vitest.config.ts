import { defineConfig } from 'vitest/config';
import path from 'path';

export default defineConfig({
  test: {
    globals: true,
    // Default to jsdom so React Testing Library tests have a DOM.
    // Server-side tests are switched back to node via environmentMatchGlobs below.
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
    // Server tests must run in node (duckdb, fs, fileURLToPath, etc.)
    environmentMatchGlobs: [
      ['server/**', 'node'],
    ],
  },
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  // SSR settings for server-side module resolution
  ssr: {
    noExternal: ['supertest'],
  },
  // Optimizations for node modules
  optimizeDeps: {
    include: ['supertest'],
  },
});
