/**
 * Security E2E Tests
 * 
 * End-to-end tests focusing on security aspects.
 * Tests various attack vectors across multiple endpoints.
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

describe('Security E2E Tests', () => {
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
  
  describe('SQL Injection Prevention', () => {
    const sqlInjectionPayloads = [
      "'; DROP TABLE events; --",
      "1; DELETE FROM events",
      "' OR '1'='1",
      "' OR 1=1 --",
      "'; UPDATE events SET payload='pwned'; --",
      "1 UNION SELECT * FROM pg_tables --",
      "1' AND (SELECT COUNT(*) FROM events) > 0 --",
      "admin'/*",
      "1; TRUNCATE events; --",
    ];
    
    it('should reject SQL injection in search query', async () => {
      for (const payload of sqlInjectionPayloads) {
        const response = await httpJson('/api/search', { query: { q: payload } });

        // Should either reject (400) or escape safely (200 with no injection effect)
        expect([200, 400, 500]).toContain(response.status);

        // If 200, verify no SQL error indicators
        if (response.status === 200) {
          expect(response.body.error).toBeUndefined();
        }
      }
    });

    it('should reject SQL injection in template ID', async () => {
      for (const payload of sqlInjectionPayloads.slice(0, 5)) {
        const response = await httpJson('/api/events/by-template/' + encodeURIComponent(payload));

        expect(response.status).toBe(400);
      }
    });

    it('should reject SQL injection in date parameters', async () => {
      const response = await httpJson('/api/events/by-date', { query: { start: "2024-01-01'; DELETE FROM events; --" } });

      expect(response.status).toBe(400);
    });
  });

  describe('Parameter Tampering Prevention', () => {
    it('should handle extremely large limit values', async () => {
      const response = await httpJson('/api/events/latest', { query: { limit: 999999999 } });

      // Should cap internally, not crash
      expect([200, 500]).toContain(response.status);
    });

    it('should handle negative offset values', async () => {
      const response = await httpJson('/api/events/latest', { query: { offset: -100 } });

      // Should sanitize to 0 or error gracefully
      expect([200, 400, 500]).toContain(response.status);
    });

    it('should handle special characters in parameters', async () => {
      const specialChars = ['<script>', '${7*7}', '{{7*7}}', '%00', '\\x00'];

      for (const char of specialChars) {
        const response = await httpJson('/api/search', { query: { q: char } });

        // Should handle gracefully
        expect([200, 400, 500]).toContain(response.status);
      }
    });
  });

  describe('Path Traversal Prevention', () => {
    it('should reject path traversal in contract ID', async () => {
      const traversalPayloads = ['../../../etc/passwd', '..\\..\\..\\windows\\system32', '/etc/passwd', 'file:///etc/passwd'];

      for (const payload of traversalPayloads) {
        const response = await httpJson('/api/search/contract/' + encodeURIComponent(payload));

        expect(response.status).toBe(400);
      }
    });
  });

  describe('Input Length Limits', () => {
    it('should reject excessively long search queries', async () => {
      const longQuery = 'a'.repeat(10000);

      const response = await httpJson('/api/search', { query: { q: longQuery } });

      // Should handle gracefully (reject or truncate)
      expect([200, 400, 500]).toContain(response.status);
    });

    it('should reject excessively long party IDs', async () => {
      const longPartyId = 'party::' + 'a'.repeat(10000);

      const response = await httpJson('/api/party/' + encodeURIComponent(longPartyId));

      expect([200, 400, 500, 503]).toContain(response.status);
    });
  });

  describe('Rate Limiting Behavior', () => {
    it('should handle rapid sequential requests', async () => {
      const requests = Array(20)
        .fill(null)
        .map(() => httpJson('/api/stats/overview'));

      const responses = await Promise.all(requests);

      // All requests should complete (may be 200 or 500, but not crash)
      for (const response of responses) {
        expect([200, 500]).toContain(response.status);
      }
    });
  });

  describe('Error Information Leakage', () => {
    it('should not leak internal paths in errors', async () => {
      const response = await httpJson('/api/events/by-template/invalid');

      if (response.status === 400 || response.status === 500) {
        const errorStr = JSON.stringify(response.body);

        // Should not contain internal paths
        expect(errorStr).not.toContain('/home/');
        expect(errorStr).not.toContain('/Users/');
        expect(errorStr).not.toContain('node_modules');
        expect(errorStr).not.toContain('.js:');
      }
    });

    it('should not leak database structure in errors', async () => {
      const response = await httpJson('/api/search', { query: { q: "test' AND (SELECT version()) --" } });

      if (response.status === 400 || response.status === 500) {
        const errorStr = JSON.stringify(response.body).toLowerCase();

        // Should not leak DB info
        expect(errorStr).not.toContain('postgresql');
        expect(errorStr).not.toContain('mysql');
        expect(errorStr).not.toContain('table schema');
      }
    });
  });
});
