/**
 * Stats API Integration Tests
 * 
 * Tests for /api/stats/* endpoints.
 * IMPORTANT: Tests REQUIRE 200 status - no silent passes on 500 errors.
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
    it('should return overview structure with required fields', async () => {
      const response = await httpJson('/api/stats/overview');

      // STRICT: Must return 200
      expect(response.status, `Expected 200 but got ${response.status}: ${JSON.stringify(response.body)}`).toBe(200);
      
      expect(response.body).toHaveProperty('total_events');
      expect(response.body).toHaveProperty('data_source');
      expect(typeof response.body.total_events).toBe('number');
      expect(typeof response.body.data_source).toBe('string');
    });
  });

  describe('GET /api/stats/daily', () => {
    it('should return daily stats with data array', async () => {
      const response = await httpJson('/api/stats/daily', { query: { days: 7 } });

      expect(response.status, `Expected 200 but got ${response.status}`).toBe(200);
      expect(response.body).toHaveProperty('data');
      expect(Array.isArray(response.body.data)).toBe(true);
    });

    it('should cap days parameter at maximum gracefully', async () => {
      const response = await httpJson('/api/stats/daily', { query: { days: 9999 } });

      // Should return 200 with capped value, not error
      expect(response.status).toBe(200);
      expect(response.body).toHaveProperty('data');
    });

    it('should handle invalid days parameter by using defaults', async () => {
      const response = await httpJson('/api/stats/daily', { query: { days: 'invalid' } });

      // Should default to valid value, return 200
      expect(response.status).toBe(200);
      expect(response.body).toHaveProperty('data');
    });
  });

  describe('GET /api/stats/by-type', () => {
    it('should return event type breakdown as array', async () => {
      const response = await httpJson('/api/stats/by-type');

      expect(response.status, `Expected 200 but got ${response.status}`).toBe(200);
      expect(response.body).toHaveProperty('data');
      expect(Array.isArray(response.body.data)).toBe(true);
    });

    it('should have proper item structure if data exists', async () => {
      const response = await httpJson('/api/stats/by-type');

      expect(response.status).toBe(200);
      
      if (response.body.data.length > 0) {
        const item = response.body.data[0];
        // Should have event_type and count
        expect(item).toHaveProperty('event_type');
        expect(item).toHaveProperty('count');
        expect(typeof item.event_type).toBe('string');
        expect(typeof item.count).toBe('number');
      }
    });
  });

  describe('GET /api/stats/by-template', () => {
    it('should return template breakdown respecting limit', async () => {
      const response = await httpJson('/api/stats/by-template', { query: { limit: 10 } });

      expect(response.status, `Expected 200 but got ${response.status}`).toBe(200);
      expect(response.body).toHaveProperty('data');
      expect(Array.isArray(response.body.data)).toBe(true);
      expect(response.body.data.length).toBeLessThanOrEqual(10);
    });

    it('should have proper item structure if data exists', async () => {
      const response = await httpJson('/api/stats/by-template', { query: { limit: 5 } });

      expect(response.status).toBe(200);
      
      if (response.body.data.length > 0) {
        const item = response.body.data[0];
        // Should have template_id and count fields
        expect(item).toHaveProperty('template_id');
        expect(typeof item.template_id).toBe('string');
      }
    });
  });

  describe('GET /api/stats/hourly', () => {
    it('should return hourly data as array', async () => {
      const response = await httpJson('/api/stats/hourly');

      expect(response.status, `Expected 200 but got ${response.status}`).toBe(200);
      expect(response.body).toHaveProperty('data');
      expect(Array.isArray(response.body.data)).toBe(true);
    });

    it('should have proper hourly item structure if data exists', async () => {
      const response = await httpJson('/api/stats/hourly');

      expect(response.status).toBe(200);
      
      if (response.body.data.length > 0) {
        const item = response.body.data[0];
        expect(item).toHaveProperty('hour');
        expect(item).toHaveProperty('event_count');
      }
    });
  });

  describe('GET /api/stats/burn', () => {
    it('should return burn stats as array', async () => {
      const response = await httpJson('/api/stats/burn');

      expect(response.status, `Expected 200 but got ${response.status}`).toBe(200);
      expect(response.body).toHaveProperty('data');
      expect(Array.isArray(response.body.data)).toBe(true);
    });
  });

  describe('GET /api/stats/sources', () => {
    it('should return data sources information', async () => {
      const response = await httpJson('/api/stats/sources');

      expect(response.status, `Expected 200 but got ${response.status}`).toBe(200);
      expect(response.body).toBeDefined();
      expect(typeof response.body).toBe('object');
    });
  });
});
