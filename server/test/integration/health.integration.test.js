/**
 * Health Endpoint Integration Tests
 * 
 * Tests for /health and root endpoints using supertest.
 */

import { describe, it, expect, beforeAll } from 'vitest';
import { createTestApp } from '../app.js';
import request from 'supertest';

describe('Health Endpoints Integration', () => {
  let app;
  
  beforeAll(() => {
    app = createTestApp();
  });
  
  describe('GET /health', () => {
    it('should return 200 with status ok', async () => {
      const response = await request(app)
        .get('/health')
        .expect('Content-Type', /json/)
        .expect(200);
      
      expect(response.body.status).toBe('ok');
      expect(response.body.timestamp).toBeDefined();
      expect(new Date(response.body.timestamp).getTime()).toBeGreaterThan(0);
    });
  });
  
  describe('GET /', () => {
    it('should return API info', async () => {
      const response = await request(app)
        .get('/')
        .expect('Content-Type', /json/)
        .expect(200);
      
      expect(response.body.name).toBe('Amulet Scan DuckDB API');
      expect(response.body.version).toBe('1.0.0');
      expect(response.body.status).toBe('ok');
    });
  });
});
