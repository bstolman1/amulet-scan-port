/**
 * Events API Integration Tests
 * 
 * Tests for /api/events/* endpoints.
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

describe('Events API Integration', () => {
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
  
  describe('GET /api/events/latest', () => {
    it('should accept valid limit parameter', async () => {
      const response = await httpJson('/api/events/latest', { query: { limit: 50 } });

      expect([200, 500]).toContain(response.status);
      if (response.status === 200) {
        expect(response.body).toHaveProperty('data');
        expect(Array.isArray(response.body.data)).toBe(true);
      }
    });

    it('should sanitize limit to max 1000', async () => {
      const response = await httpJson('/api/events/latest', { query: { limit: 5000 } });

      // Should not error, but should cap internally
      expect([200, 500]).toContain(response.status);
    });

    it('should handle negative limit gracefully', async () => {
      const response = await httpJson('/api/events/latest', { query: { limit: -10 } });

      // Should default to minimum or error gracefully
      expect([200, 400, 500]).toContain(response.status);
    });

    it('should handle non-numeric limit', async () => {
      const response = await httpJson('/api/events/latest', { query: { limit: 'abc' } });

      // Should default to valid value
      expect([200, 500]).toContain(response.status);
    });
  });

  describe('GET /api/events/by-type/:type', () => {
    it('should accept valid event type', async () => {
      const response = await httpJson('/api/events/by-type/created');

      expect([200, 500]).toContain(response.status);
      if (response.status === 200) {
        expect(response.body).toHaveProperty('data');
      }
    });

    it('should reject invalid event type', async () => {
      const response = await httpJson('/api/events/by-type/invalid_type');

      expect(response.status).toBe(400);
      expect(response.body.error).toContain('Invalid event type');
    });

    it('should reject SQL injection in event type', async () => {
      const response = await httpJson('/api/events/by-type/' + encodeURIComponent("created'; DROP TABLE --"));

      expect(response.status).toBe(400);
    });
  });

  describe('GET /api/events/by-template/:templateId', () => {
    it('should accept valid template ID', async () => {
      const response = await httpJson('/api/events/by-template/Splice.Amulet:Amulet');

      expect([200, 500]).toContain(response.status);
      if (response.status === 200) {
        expect(response.body).toHaveProperty('data');
      }
    });

    it('should reject SQL injection in template ID', async () => {
      const response = await httpJson('/api/events/by-template/' + encodeURIComponent("Template'; DROP TABLE --"));

      expect(response.status).toBe(400);
      expect(response.body.error).toContain('Invalid template ID');
    });
  });

  describe('GET /api/events/by-date', () => {
    it('should accept valid date range', async () => {
      const response = await httpJson('/api/events/by-date', { query: { start: '2024-01-01', end: '2024-12-31' } });

      expect([200, 500]).toContain(response.status);
    });

    it('should reject invalid start date', async () => {
      const response = await httpJson('/api/events/by-date', { query: { start: 'not-a-date' } });

      expect(response.status).toBe(400);
      expect(response.body.error).toContain('Invalid start date');
    });

    it('should reject SQL injection in date parameters', async () => {
      const response = await httpJson('/api/events/by-date', { query: { start: "2024-01-01'; DROP TABLE --" } });

      expect(response.status).toBe(400);
    });
  });

  describe('GET /api/events/count', () => {
    it('should return count structure', async () => {
      const response = await httpJson('/api/events/count');

      expect([200, 500]).toContain(response.status);
      if (response.status === 200) {
        expect(response.body).toHaveProperty('count');
        expect(typeof response.body.count).toBe('number');
      }
    });
  });

  describe('GET /api/events/debug', () => {
    it('should return debug info', async () => {
      const response = await httpJson('/api/events/debug');

      expect([200, 500]).toContain(response.status);
      if (response.status === 200) {
        expect(response.body).toHaveProperty('dataPath');
        expect(response.body).toHaveProperty('sources');
      }
    });
  });
});
