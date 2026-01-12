import { defineConfig } from 'vitest/config';
import path from 'path';

export default defineConfig({
  test: {
    globals: true,
    // Default to node for server tests (duckdb, fs, fileURLToPath, etc.)
    environment: 'node',
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
    // Frontend tests need jsdom for React Testing Library
    environmentMatchGlobs: [
      ['src/**', 'jsdom'],
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
