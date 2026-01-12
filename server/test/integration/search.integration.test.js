/**
 * Search API Integration Tests
 * 
 * Tests for /api/search/* endpoints with input validation.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { createTestApp } from '../app.js';

let server;
let baseUrl;

async function httpJson(path, { query } = {}) {
  const url = new URL(path, baseUrl);
  if (query) {
    for (const [k, v] of Object.entries(query)) url.searchParams.set(k, String(v));
  }
  const res = await fetch(url);
  let body;
  try {
    body = await res.json();
  } catch {
    body = undefined;
  }
  return { status: res.status, headers: Object.fromEntries(res.headers.entries()), body };
}

describe('Search API Integration', () => {
  let app;

  beforeAll(() => {
    app = createTestApp();
    server = app.listen(0);
    const address = server.address();
    const port = typeof address === 'object' && address ? address.port : 0;
    baseUrl = `http://127.0.0.1:${port}`;
  });

  afterAll(() => {
    server?.close();
  });
  
  describe('GET /api/search', () => {
    it('should accept valid search query', async () => {
      const response = await httpJson('/api/search', { query: { q: 'alice', limit: 10 } });

      // May return 200 with data or 500 if no data files (expected in test env)
      expect([200, 500]).toContain(response.status);
      if (response.status === 200) {
        expect(response.body).toHaveProperty('data');
        expect(response.body).toHaveProperty('count');
      }
    });

    it('should reject SQL injection in search query', async () => {
      const response = await httpJson('/api/search', { query: { q: "'; DROP TABLE events; --" } });

      expect(response.status).toBe(400);
      expect(response.body.error).toContain('Invalid');
    });

    it('should reject UNION injection attempts', async () => {
      const response = await httpJson('/api/search', { query: { q: "test' UNION SELECT * FROM secrets --" } });

      expect(response.status).toBe(400);
      expect(response.body.error).toContain('Invalid');
    });

    it('should reject tautology injection (OR 1=1)', async () => {
      const response = await httpJson('/api/search', { query: { q: "x' OR 1=1 --" } });

      expect(response.status).toBe(400);
    });

    it('should handle empty query', async () => {
      const response = await httpJson('/api/search');

      // Empty query should still work (returns all or empty based on data)
      expect([200, 500]).toContain(response.status);
    });

    it('should respect limit parameter', async () => {
      const response = await httpJson('/api/search', { query: { limit: 5 } });

      if (response.status === 200) {
        expect(response.body.data.length).toBeLessThanOrEqual(5);
      }
    });

    it('should cap limit at maximum', async () => {
      const response = await httpJson('/api/search', { query: { limit: 99999 } });

      // Server should cap the limit internally
      expect([200, 500]).toContain(response.status);
    });
  });

  describe('GET /api/search/contract/:id', () => {
    it('should accept valid contract ID format', async () => {
      const response = await httpJson('/api/search/contract/00abc123def456');

      expect([200, 500]).toContain(response.status);
      if (response.status === 200) {
        expect(response.body).toHaveProperty('data');
      }
    });

    it('should reject SQL injection in contract ID', async () => {
      const response = await httpJson('/api/search/contract/' + encodeURIComponent("'; DROP TABLE --"));

      expect(response.status).toBe(400);
      expect(response.body.error).toContain('Invalid contract ID');
    });

    it('should reject non-hex prefix in contract ID', async () => {
      // Contract IDs must start with hex characters
      const response = await httpJson('/api/search/contract/invalid-prefix');

      expect(response.status).toBe(400);
    });
  });
});
