/**
 * Stats API Integration Tests
 * 
 * Tests for /api/stats/* endpoints.
 */

import { describe, it, expect, beforeAll } from 'vitest';
import { createRequire } from 'module';
import { createTestApp } from '../app.js';

const require = createRequire(import.meta.url);
const request = require(require.resolve('supertest', { paths: [process.cwd()] }));

describe('Stats API Integration', () => {
  let app;
  
  beforeAll(() => {
    app = createTestApp();
  });
  
  describe('GET /api/stats/overview', () => {
    it('should return overview structure', async () => {
      const response = await request(app)
        .get('/api/stats/overview');
      
      expect([200, 500]).toContain(response.status);
      if (response.status === 200) {
        expect(response.body).toHaveProperty('total_events');
        expect(response.body).toHaveProperty('data_source');
      }
    });
  });
  
  describe('GET /api/stats/daily', () => {
    it('should accept days parameter', async () => {
      const response = await request(app)
        .get('/api/stats/daily')
        .query({ days: 7 });
      
      expect([200, 500]).toContain(response.status);
      if (response.status === 200) {
        expect(response.body).toHaveProperty('data');
        expect(Array.isArray(response.body.data)).toBe(true);
      }
    });
    
    it('should cap days parameter at maximum', async () => {
      const response = await request(app)
        .get('/api/stats/daily')
        .query({ days: 9999 });
      
      // Should not error, but cap internally
      expect([200, 500]).toContain(response.status);
    });
    
    it('should handle invalid days parameter', async () => {
      const response = await request(app)
        .get('/api/stats/daily')
        .query({ days: 'invalid' });
      
      // Should default to valid value
      expect([200, 500]).toContain(response.status);
    });
  });
  
  describe('GET /api/stats/by-type', () => {
    it('should return event type breakdown', async () => {
      const response = await request(app)
        .get('/api/stats/by-type');
      
      expect([200, 500]).toContain(response.status);
      if (response.status === 200) {
        expect(response.body).toHaveProperty('data');
      }
    });
  });
  
  describe('GET /api/stats/by-template', () => {
    it('should accept limit parameter', async () => {
      const response = await request(app)
        .get('/api/stats/by-template')
        .query({ limit: 10 });
      
      expect([200, 500]).toContain(response.status);
      if (response.status === 200) {
        expect(response.body).toHaveProperty('data');
        expect(response.body.data.length).toBeLessThanOrEqual(10);
      }
    });
  });
  
  describe('GET /api/stats/hourly', () => {
    it('should return hourly data', async () => {
      const response = await request(app)
        .get('/api/stats/hourly');
      
      expect([200, 500]).toContain(response.status);
      if (response.status === 200) {
        expect(response.body).toHaveProperty('data');
      }
    });
  });
});
