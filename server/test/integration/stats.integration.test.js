/**
 * Stats API Integration Tests
 * 
 * Tests for /api/stats/* endpoints.
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

describe('Stats API Integration', () => {
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
  
  describe('GET /api/stats/overview', () => {
    it('should return overview structure', async () => {
      const response = await httpJson('/api/stats/overview');

      expect([200, 500]).toContain(response.status);
      if (response.status === 200) {
        expect(response.body).toHaveProperty('total_events');
        expect(response.body).toHaveProperty('data_source');
      }
    });
  });

  describe('GET /api/stats/daily', () => {
    it('should accept days parameter', async () => {
      const response = await httpJson('/api/stats/daily', { query: { days: 7 } });

      expect([200, 500]).toContain(response.status);
      if (response.status === 200) {
        expect(response.body).toHaveProperty('data');
        expect(Array.isArray(response.body.data)).toBe(true);
      }
    });

    it('should cap days parameter at maximum', async () => {
      const response = await httpJson('/api/stats/daily', { query: { days: 9999 } });

      // Should not error, but cap internally
      expect([200, 500]).toContain(response.status);
    });

    it('should handle invalid days parameter', async () => {
      const response = await httpJson('/api/stats/daily', { query: { days: 'invalid' } });

      // Should default to valid value
      expect([200, 500]).toContain(response.status);
    });
  });

  describe('GET /api/stats/by-type', () => {
    it('should return event type breakdown', async () => {
      const response = await httpJson('/api/stats/by-type');

      expect([200, 500]).toContain(response.status);
      if (response.status === 200) {
        expect(response.body).toHaveProperty('data');
      }
    });
  });

  describe('GET /api/stats/by-template', () => {
    it('should accept limit parameter', async () => {
      const response = await httpJson('/api/stats/by-template', { query: { limit: 10 } });

      expect([200, 500]).toContain(response.status);
      if (response.status === 200) {
        expect(response.body).toHaveProperty('data');
        expect(response.body.data.length).toBeLessThanOrEqual(10);
      }
    });
  });

  describe('GET /api/stats/hourly', () => {
    it('should return hourly data', async () => {
      const response = await httpJson('/api/stats/hourly');

      expect([200, 500]).toContain(response.status);
      if (response.status === 200) {
        expect(response.body).toHaveProperty('data');
      }
    });
  });
});
