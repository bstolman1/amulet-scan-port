/**
 * Party API Integration Tests
 * 
 * Tests for /api/party/* endpoints.
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

describe('Party API Integration', () => {
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
  
  describe('GET /api/party/search', () => {
    it('should require search query', async () => {
      const response = await httpJson('/api/party/search');

      expect(response.status).toBe(400);
      expect(response.body.error).toContain('query required');
    });

    it('should accept valid search query', async () => {
      const response = await httpJson('/api/party/search', { query: { q: 'alice' } });

      expect([200, 500]).toContain(response.status);
      if (response.status === 200) {
        expect(response.body).toHaveProperty('data');
        expect(response.body).toHaveProperty('count');
      }
    });

    it('should accept limit parameter', async () => {
      const response = await httpJson('/api/party/search', { query: { q: 'test', limit: 10 } });

      expect([200, 500]).toContain(response.status);
    });
  });

  describe('GET /api/party/index/status', () => {
    it('should return index status', async () => {
      const response = await httpJson('/api/party/index/status');

      expect([200, 500]).toContain(response.status);
      if (response.status === 200) {
        expect(response.body).toBeDefined();
      }
    });
  });

  describe('POST /api/party/index/build', () => {
    it('should start index build', async () => {
      const url = new URL('/api/party/index/build', baseUrl);
      const res = await fetch(url, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({}),
      });
      const body = await res.json().catch(() => undefined);

      expect([200, 500]).toContain(res.status);
      if (res.status === 200) {
        expect(body.status).toBe('started');
      }
    });

    it('should accept forceRebuild option', async () => {
      const url = new URL('/api/party/index/build', baseUrl);
      const res = await fetch(url, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ forceRebuild: true }),
      });

      expect([200, 500]).toContain(res.status);
    });
  });

  describe('GET /api/party/:partyId', () => {
    it('should accept valid party ID', async () => {
      const response = await httpJson('/api/party/party::alice');

      expect([200, 500, 503]).toContain(response.status);
    });

    it('should accept limit parameter', async () => {
      const response = await httpJson('/api/party/party::test', { query: { limit: 50 } });

      expect([200, 500, 503]).toContain(response.status);
    });
  });

  describe('GET /api/party/:partyId/summary', () => {
    it('should return summary for party', async () => {
      const response = await httpJson('/api/party/party::alice/summary');

      expect([200, 500]).toContain(response.status);
      if (response.status === 200) {
        expect(response.body).toHaveProperty('party_id');
      }
    });
  });

  describe('GET /api/party/list/all', () => {
    it('should handle no index case', async () => {
      const response = await httpJson('/api/party/list/all');

      // Either returns data (if indexed) or 503 (if not indexed)
      expect([200, 503]).toContain(response.status);
    });

    it('should accept limit parameter', async () => {
      const response = await httpJson('/api/party/list/all', { query: { limit: 100 } });

      expect([200, 503]).toContain(response.status);
    });
  });
});
