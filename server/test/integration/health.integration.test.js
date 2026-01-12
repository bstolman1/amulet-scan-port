/**
 * Health Endpoint Integration Tests
 * 
 * Tests for /health and root endpoints using native fetch.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { createTestApp } from '../app.js';

describe('Health Endpoints Integration', () => {
  let app;
  let server;
  let baseUrl;
  
  beforeAll(async () => {
    app = createTestApp();
    await new Promise((resolve) => {
      server = app.listen(0, () => {
        const { port } = server.address();
        baseUrl = `http://127.0.0.1:${port}`;
        resolve();
      });
    });
  });
  
  afterAll(async () => {
    if (server) {
      await new Promise((resolve) => server.close(resolve));
    }
  });
  
  async function httpJson(path) {
    const res = await fetch(`${baseUrl}${path}`);
    const body = await res.json();
    return { status: res.status, body, headers: res.headers };
  }
  
  describe('GET /health', () => {
    it('should return 200 with status ok', async () => {
      const response = await httpJson('/health');
      
      expect(response.status).toBe(200);
      expect(response.body.status).toBe('ok');
      expect(response.body.timestamp).toBeDefined();
      expect(new Date(response.body.timestamp).getTime()).toBeGreaterThan(0);
    });
  });
  
  describe('GET /', () => {
    it('should return API info', async () => {
      const response = await httpJson('/');
      
      expect(response.status).toBe(200);
      expect(response.body.name).toBe('Amulet Scan DuckDB API');
      expect(response.body.version).toBe('1.0.0');
      expect(response.body.status).toBe('ok');
    });
  });
});
