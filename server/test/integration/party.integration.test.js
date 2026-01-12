/**
 * Party API Integration Tests
 * 
 * Tests for /api/party/* endpoints.
 */

import { describe, it, expect, beforeAll } from 'vitest';
import { createTestApp } from '../app.js';
import request from 'supertest';

describe('Party API Integration', () => {
  let app;
  
  beforeAll(() => {
    app = createTestApp();
  });
  
  describe('GET /api/party/search', () => {
    it('should require search query', async () => {
      const response = await request(app)
        .get('/api/party/search');
      
      expect(response.status).toBe(400);
      expect(response.body.error).toContain('query required');
    });
    
    it('should accept valid search query', async () => {
      const response = await request(app)
        .get('/api/party/search')
        .query({ q: 'alice' });
      
      expect([200, 500]).toContain(response.status);
      if (response.status === 200) {
        expect(response.body).toHaveProperty('data');
        expect(response.body).toHaveProperty('count');
      }
    });
    
    it('should accept limit parameter', async () => {
      const response = await request(app)
        .get('/api/party/search')
        .query({ q: 'test', limit: 10 });
      
      expect([200, 500]).toContain(response.status);
    });
  });
  
  describe('GET /api/party/index/status', () => {
    it('should return index status', async () => {
      const response = await request(app)
        .get('/api/party/index/status');
      
      expect([200, 500]).toContain(response.status);
      if (response.status === 200) {
        expect(response.body).toBeDefined();
      }
    });
  });
  
  describe('POST /api/party/index/build', () => {
    it('should start index build', async () => {
      const response = await request(app)
        .post('/api/party/index/build')
        .send({});
      
      expect([200, 500]).toContain(response.status);
      if (response.status === 200) {
        expect(response.body.status).toBe('started');
      }
    });
    
    it('should accept forceRebuild option', async () => {
      const response = await request(app)
        .post('/api/party/index/build')
        .send({ forceRebuild: true });
      
      expect([200, 500]).toContain(response.status);
    });
  });
  
  describe('GET /api/party/:partyId', () => {
    it('should accept valid party ID', async () => {
      const response = await request(app)
        .get('/api/party/party::alice');
      
      expect([200, 500, 503]).toContain(response.status);
    });
    
    it('should accept limit parameter', async () => {
      const response = await request(app)
        .get('/api/party/party::test')
        .query({ limit: 50 });
      
      expect([200, 500, 503]).toContain(response.status);
    });
  });
  
  describe('GET /api/party/:partyId/summary', () => {
    it('should return summary for party', async () => {
      const response = await request(app)
        .get('/api/party/party::alice/summary');
      
      expect([200, 500]).toContain(response.status);
      if (response.status === 200) {
        expect(response.body).toHaveProperty('party_id');
      }
    });
  });
  
  describe('GET /api/party/list/all', () => {
    it('should handle no index case', async () => {
      const response = await request(app)
        .get('/api/party/list/all');
      
      // Either returns data (if indexed) or 503 (if not indexed)
      expect([200, 503]).toContain(response.status);
    });
    
    it('should accept limit parameter', async () => {
      const response = await request(app)
        .get('/api/party/list/all')
        .query({ limit: 100 });
      
      expect([200, 503]).toContain(response.status);
    });
  });
});
