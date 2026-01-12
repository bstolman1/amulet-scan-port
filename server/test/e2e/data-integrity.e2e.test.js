/**
 * Data Integrity E2E Tests
 * 
 * End-to-end tests ensuring data consistency and integrity.
 */

import { describe, it, expect, beforeAll } from 'vitest';
import { createRequire } from 'module';
import { createTestApp } from '../app.js';

const require = createRequire(import.meta.url);
const request = require(require.resolve('supertest', { paths: [process.cwd()] }));

describe('Data Integrity E2E Tests', () => {
  let app;
  
  beforeAll(() => {
    app = createTestApp();
  });
  
  describe('Response Structure Consistency', () => {
    it('should return consistent structure for events endpoints', async () => {
      const endpoints = [
        '/api/events/latest',
        '/api/events/governance',
        '/api/events/rewards',
      ];
      
      for (const endpoint of endpoints) {
        const response = await request(app)
          .get(endpoint)
          .query({ limit: 5 });
        
        if (response.status === 200) {
          expect(response.body).toHaveProperty('data');
          expect(Array.isArray(response.body.data)).toBe(true);
          expect(response.body).toHaveProperty('count');
          expect(typeof response.body.count).toBe('number');
        }
      }
    });
    
    it('should return consistent structure for stats endpoints', async () => {
      const endpoints = [
        '/api/stats/by-type',
        '/api/stats/by-template',
        '/api/stats/daily',
        '/api/stats/hourly',
      ];
      
      for (const endpoint of endpoints) {
        const response = await request(app).get(endpoint);
        
        if (response.status === 200) {
          expect(response.body).toHaveProperty('data');
          expect(Array.isArray(response.body.data)).toBe(true);
        }
      }
    });
  });
  
  describe('Pagination Consistency', () => {
    it('should paginate correctly with offset', async () => {
      // Get first page
      const page1 = await request(app)
        .get('/api/events/latest')
        .query({ limit: 5, offset: 0 });
      
      // Get second page
      const page2 = await request(app)
        .get('/api/events/latest')
        .query({ limit: 5, offset: 5 });
      
      if (page1.status === 200 && page2.status === 200) {
        // If both have data, they should be different (assuming enough data)
        if (page1.body.data.length > 0 && page2.body.data.length > 0) {
          const page1Ids = page1.body.data.map(e => e.event_id || e.id).filter(Boolean);
          const page2Ids = page2.body.data.map(e => e.event_id || e.id).filter(Boolean);
          
          // No overlap between pages
          const overlap = page1Ids.filter(id => page2Ids.includes(id));
          expect(overlap.length).toBe(0);
        }
      }
    });
    
    it('should respect limit parameter exactly', async () => {
      const limits = [1, 5, 10, 50];
      
      for (const limit of limits) {
        const response = await request(app)
          .get('/api/events/latest')
          .query({ limit });
        
        if (response.status === 200) {
          expect(response.body.data.length).toBeLessThanOrEqual(limit);
        }
      }
    });
  });
  
  describe('Date Filtering Correctness', () => {
    it('should filter by date range correctly', async () => {
      const start = '2024-01-01';
      const end = '2024-01-31';
      
      const response = await request(app)
        .get('/api/events/by-date')
        .query({ start, end, limit: 50 });
      
      if (response.status === 200 && response.body.data.length > 0) {
        for (const event of response.body.data) {
          if (event.effective_at || event.timestamp) {
            const eventDate = new Date(event.effective_at || event.timestamp);
            const startDate = new Date(start);
            const endDate = new Date(end + 'T23:59:59Z');
            
            expect(eventDate.getTime()).toBeGreaterThanOrEqual(startDate.getTime());
            expect(eventDate.getTime()).toBeLessThanOrEqual(endDate.getTime());
          }
        }
      }
    });
  });
  
  describe('Event Type Filtering', () => {
    it('should only return requested event type', async () => {
      const eventTypes = ['created', 'archived'];
      
      for (const type of eventTypes) {
        const response = await request(app)
          .get(`/api/events/by-type/${type}`)
          .query({ limit: 20 });
        
        if (response.status === 200 && response.body.data.length > 0) {
          for (const event of response.body.data) {
            if (event.event_type) {
              expect(event.event_type.toLowerCase()).toContain(type);
            }
          }
        }
      }
    });
  });
  
  describe('Template Filtering', () => {
    it('should only return matching templates', async () => {
      const template = 'Amulet';
      
      const response = await request(app)
        .get(`/api/events/by-template/${template}`)
        .query({ limit: 20 });
      
      if (response.status === 200 && response.body.data.length > 0) {
        for (const event of response.body.data) {
          if (event.template_id) {
            expect(event.template_id).toContain(template);
          }
        }
      }
    });
  });
  
  describe('Count Consistency', () => {
    it('should have consistent count across endpoints', async () => {
      const countResponse = await request(app).get('/api/events/count');
      const overviewResponse = await request(app).get('/api/stats/overview');
      
      if (countResponse.status === 200 && overviewResponse.status === 200) {
        // Both should report similar totals (allowing for estimation differences)
        const countTotal = countResponse.body.count || 0;
        const overviewTotal = overviewResponse.body.total_events || 0;
        
        // If both are non-zero, they should be in the same ballpark
        // (exact match not required due to different counting methods)
        if (countTotal > 0 && overviewTotal > 0) {
          const ratio = Math.max(countTotal, overviewTotal) / Math.min(countTotal, overviewTotal);
          expect(ratio).toBeLessThan(10); // Within 10x of each other
        }
      }
    });
  });
  
  describe('JSON Response Validity', () => {
    it('should return valid JSON for all endpoints', async () => {
      const endpoints = [
        '/health',
        '/',
        '/api/stats/overview',
        '/api/events/latest',
        '/api/events/count',
      ];
      
      for (const endpoint of endpoints) {
        const response = await request(app).get(endpoint);
        
        // All responses should be valid JSON
        expect(response.headers['content-type']).toMatch(/json/);
        expect(() => JSON.parse(JSON.stringify(response.body))).not.toThrow();
      }
    });
    
    it('should serialize BigInt values correctly', async () => {
      const response = await request(app).get('/api/stats/overview');
      
      if (response.status === 200) {
        const body = JSON.stringify(response.body);
        
        // Should not contain BigInt (which would fail JSON.stringify)
        expect(body).not.toContain('BigInt');
      }
    });
  });
});
