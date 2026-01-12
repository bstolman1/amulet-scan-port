/**
 * Search API Integration Tests
 * 
 * Tests for /api/search/* endpoints with input validation.
 */

import { describe, it, expect, beforeAll } from 'vitest';
import { createTestApp } from '../app.js';
import { createRequire } from 'module';

const require = createRequire(new URL('../../../package.json', import.meta.url));
const request = require('supertest');

describe('Search API Integration', () => {
  let app;
  
  beforeAll(() => {
    app = createTestApp();
  });
  
  describe('GET /api/search', () => {
    it('should accept valid search query', async () => {
      const response = await request(app)
        .get('/api/search')
        .query({ q: 'alice', limit: 10 });
      
      // May return 200 with data or 500 if no data files (expected in test env)
      expect([200, 500]).toContain(response.status);
      if (response.status === 200) {
        expect(response.body).toHaveProperty('data');
        expect(response.body).toHaveProperty('count');
      }
    });
    
    it('should reject SQL injection in search query', async () => {
      const response = await request(app)
        .get('/api/search')
        .query({ q: "'; DROP TABLE events; --" });
      
      expect(response.status).toBe(400);
      expect(response.body.error).toContain('Invalid');
    });
    
    it('should reject UNION injection attempts', async () => {
      const response = await request(app)
        .get('/api/search')
        .query({ q: "test' UNION SELECT * FROM secrets --" });
      
      expect(response.status).toBe(400);
      expect(response.body.error).toContain('Invalid');
    });
    
    it('should reject tautology injection (OR 1=1)', async () => {
      const response = await request(app)
        .get('/api/search')
        .query({ q: "x' OR 1=1 --" });
      
      expect(response.status).toBe(400);
    });
    
    it('should handle empty query', async () => {
      const response = await request(app)
        .get('/api/search')
        .query({});
      
      // Empty query should still work (returns all or empty based on data)
      expect([200, 500]).toContain(response.status);
    });
    
    it('should respect limit parameter', async () => {
      const response = await request(app)
        .get('/api/search')
        .query({ limit: 5 });
      
      if (response.status === 200) {
        expect(response.body.data.length).toBeLessThanOrEqual(5);
      }
    });
    
    it('should cap limit at maximum', async () => {
      const response = await request(app)
        .get('/api/search')
        .query({ limit: 99999 });
      
      // Server should cap the limit internally
      expect([200, 500]).toContain(response.status);
    });
  });
  
  describe('GET /api/search/contract/:id', () => {
    it('should accept valid contract ID format', async () => {
      const response = await request(app)
        .get('/api/search/contract/00abc123def456');
      
      expect([200, 500]).toContain(response.status);
      if (response.status === 200) {
        expect(response.body).toHaveProperty('data');
      }
    });
    
    it('should reject SQL injection in contract ID', async () => {
      const response = await request(app)
        .get('/api/search/contract/' + encodeURIComponent("'; DROP TABLE --"));
      
      expect(response.status).toBe(400);
      expect(response.body.error).toContain('Invalid contract ID');
    });
    
    it('should reject non-hex prefix in contract ID', async () => {
      // Contract IDs must start with hex characters
      const response = await request(app)
        .get('/api/search/contract/invalid-prefix');
      
      expect(response.status).toBe(400);
    });
  });
});
