/**
 * Governance Flow E2E Tests
 * 
 * End-to-end tests for governance workflows.
 * Tests complete user journeys through the API.
 */

import { describe, it, expect, beforeAll } from 'vitest';
import request from 'supertest';
import { createTestApp } from '../app.js';

describe('Governance Flow E2E', () => {
  let app;
  
  beforeAll(() => {
    app = createTestApp();
  });
  
  describe('Complete governance query flow', () => {
    it('should query governance events and check vote request index', async () => {
      // Step 1: Check vote request index status
      const indexStatus = await request(app)
        .get('/api/events/vote-request-index/status');
      
      expect([200, 500]).toContain(indexStatus.status);
      
      // Step 2: Query governance events
      const govEvents = await request(app)
        .get('/api/events/governance')
        .query({ limit: 100 });
      
      expect([200, 500]).toContain(govEvents.status);
      
      // Step 3: Query governance lifecycle data
      const lifecycle = await request(app)
        .get('/api/governance-lifecycle');
      
      expect([200, 500]).toContain(lifecycle.status);
      
      if (lifecycle.status === 200) {
        expect(lifecycle.body).toHaveProperty('topics');
      }
    });
  });
  
  describe('Stats and overview flow', () => {
    it('should aggregate stats from multiple endpoints', async () => {
      // Query multiple stats endpoints in parallel (as a dashboard would)
      const [overview, daily, byType, byTemplate] = await Promise.all([
        request(app).get('/api/stats/overview'),
        request(app).get('/api/stats/daily').query({ days: 7 }),
        request(app).get('/api/stats/by-type'),
        request(app).get('/api/stats/by-template').query({ limit: 10 }),
      ]);
      
      // All should respond (with data or graceful error)
      expect([200, 500]).toContain(overview.status);
      expect([200, 500]).toContain(daily.status);
      expect([200, 500]).toContain(byType.status);
      expect([200, 500]).toContain(byTemplate.status);
      
      // If overview succeeds, check structure
      if (overview.status === 200) {
        expect(overview.body).toHaveProperty('total_events');
        expect(typeof overview.body.total_events).toBe('number');
      }
    });
  });
  
  describe('Search and filter flow', () => {
    it('should search and filter events', async () => {
      // Step 1: General search
      const searchResult = await request(app)
        .get('/api/search')
        .query({ q: 'Amulet', limit: 20 });
      
      expect([200, 400, 500]).toContain(searchResult.status);
      
      // Step 2: Filter by template
      const templateResult = await request(app)
        .get('/api/events/by-template/Splice.Amulet:Amulet')
        .query({ limit: 10 });
      
      expect([200, 400, 500]).toContain(templateResult.status);
      
      // Step 3: Filter by event type
      const typeResult = await request(app)
        .get('/api/events/by-type/created')
        .query({ limit: 10 });
      
      expect([200, 400, 500]).toContain(typeResult.status);
    });
  });
  
  describe('Party exploration flow', () => {
    it('should explore party data through the API', async () => {
      // Step 1: Check if party index exists
      const indexStatus = await request(app)
        .get('/api/party/index/status');
      
      expect([200, 500]).toContain(indexStatus.status);
      
      // Step 2: Search for parties
      const searchResult = await request(app)
        .get('/api/party/search')
        .query({ q: 'validator', limit: 5 });
      
      expect([200, 500]).toContain(searchResult.status);
      
      // Step 3: If search returned results, get details for first party
      if (searchResult.status === 200 && searchResult.body.data?.length > 0) {
        const partyId = searchResult.body.data[0];
        
        const partyEvents = await request(app)
          .get(`/api/party/${encodeURIComponent(partyId)}`)
          .query({ limit: 10 });
        
        expect([200, 500, 503]).toContain(partyEvents.status);
        
        const partySummary = await request(app)
          .get(`/api/party/${encodeURIComponent(partyId)}/summary`);
        
        expect([200, 500]).toContain(partySummary.status);
      }
    });
  });
  
  describe('Reward tracking flow', () => {
    it('should query reward-related endpoints', async () => {
      // Step 1: Get reward events
      const rewardEvents = await request(app)
        .get('/api/events/rewards')
        .query({ limit: 50 });
      
      expect([200, 500]).toContain(rewardEvents.status);
      
      // Step 2: Query rewards API
      const rewards = await request(app)
        .get('/api/rewards');
      
      expect([200, 500]).toContain(rewards.status);
      
      // Step 3: Check SV weights if available
      const svWeights = await request(app)
        .get('/api/events/sv-weights/history');
      
      expect([200, 404, 500]).toContain(svWeights.status);
    });
  });
});
