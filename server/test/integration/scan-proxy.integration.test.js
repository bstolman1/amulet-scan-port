/**
 * Scan API Proxy Integration Tests
 * 
 * Tests the scan-proxy endpoints that proxy requests to the Canton Scan API.
 * These tests verify that the proxy correctly forwards requests and handles responses.
 * 
 * Note: These tests require network access to the actual Canton Scan API.
 * They validate real-world API behavior with proper POST requests and JSON bodies.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { createTestApp } from '../app.js';

describe('Scan Proxy Integration Tests', () => {
  let app;
  let server;
  let baseUrl;
  
  // Increase timeout for network requests
  const NETWORK_TIMEOUT = 30000;
  
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
  
  /**
   * Helper to make JSON POST requests through the proxy
   */
  async function proxyPost(path, body = {}) {
    const res = await fetch(`${baseUrl}/api/scan-proxy${path}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    
    const contentType = res.headers.get('content-type') || '';
    let data;
    if (contentType.includes('application/json')) {
      data = await res.json();
    } else {
      data = await res.text();
    }
    
    return { status: res.status, data, headers: res.headers };
  }
  
  /**
   * Helper to make GET requests through the proxy
   */
  async function proxyGet(path) {
    const res = await fetch(`${baseUrl}/api/scan-proxy${path}`);
    
    const contentType = res.headers.get('content-type') || '';
    let data;
    if (contentType.includes('application/json')) {
      data = await res.json();
    } else {
      data = await res.text();
    }
    
    return { status: res.status, data, headers: res.headers };
  }

  /* =========================================
   * Proxy Health & Status Endpoints
   * ========================================= */
  
  describe('Proxy Health Endpoints', () => {
    it('GET /_health should return proxy status', async () => {
      const res = await proxyGet('/_health');
      
      expect(res.status).toBe(200);
      expect(res.data).toHaveProperty('status', 'ok');
      expect(res.data).toHaveProperty('currentEndpoint');
      expect(res.data).toHaveProperty('endpoints');
      expect(Array.isArray(res.data.endpoints)).toBe(true);
      expect(res.data.endpoints.length).toBeGreaterThan(0);
    });
    
    it('should list all configured endpoints with health info', async () => {
      const res = await proxyGet('/_health');
      
      const endpoint = res.data.endpoints[0];
      expect(endpoint).toHaveProperty('name');
      expect(endpoint).toHaveProperty('healthy');
      expect(endpoint).toHaveProperty('consecutiveFailures');
      expect(endpoint).toHaveProperty('totalRequests');
    });
  });

  /* =========================================
   * Core DSO/Network Endpoints
   * ========================================= */
  
  describe('DSO Endpoints', () => {
    it('POST /v0/dso should return DSO info', async () => {
      const res = await proxyPost('/v0/dso', {});
      
      expect(res.status).toBe(200);
      expect(res.data).toHaveProperty('dso_party_id');
      // DSO rules and amulet rules should be present
      expect(res.data).toHaveProperty('dso_rules');
      expect(res.data).toHaveProperty('amulet_rules');
    }, NETWORK_TIMEOUT);
    
    it('POST /v0/dso-party-id should return DSO party ID', async () => {
      const res = await proxyPost('/v0/dso-party-id', {});
      
      expect(res.status).toBe(200);
      expect(res.data).toHaveProperty('dso_party_id');
      expect(typeof res.data.dso_party_id).toBe('string');
    }, NETWORK_TIMEOUT);
    
    it('POST /v0/scans should return list of scans', async () => {
      const res = await proxyPost('/v0/scans', {});
      
      expect(res.status).toBe(200);
      expect(res.data).toHaveProperty('scans');
      expect(Array.isArray(res.data.scans)).toBe(true);
    }, NETWORK_TIMEOUT);
    
    it('POST /v0/dso-sequencers should return sequencer info', async () => {
      const res = await proxyPost('/v0/dso-sequencers', {});
      
      expect(res.status).toBe(200);
      expect(res.data).toHaveProperty('domainSequencers');
      expect(Array.isArray(res.data.domainSequencers)).toBe(true);
    }, NETWORK_TIMEOUT);
  });

  /* =========================================
   * Round & Mining Endpoints
   * ========================================= */
  
  describe('Mining Round Endpoints', () => {
    it('POST /v0/round-of-latest-data should return latest round', async () => {
      const res = await proxyPost('/v0/round-of-latest-data', {});
      
      expect(res.status).toBe(200);
      expect(res.data).toHaveProperty('round');
      expect(typeof res.data.round).toBe('number');
      expect(res.data).toHaveProperty('effectiveAt');
    }, NETWORK_TIMEOUT);
    
    it('POST /v0/open-and-issuing-mining-rounds should return active rounds', async () => {
      const res = await proxyPost('/v0/open-and-issuing-mining-rounds', {});
      
      expect(res.status).toBe(200);
      expect(res.data).toHaveProperty('open_mining_rounds');
      expect(res.data).toHaveProperty('issuing_mining_rounds');
      expect(res.data).toHaveProperty('time_to_live_in_microseconds');
    }, NETWORK_TIMEOUT);
    
    it('POST /v0/closed-rounds should return closed rounds', async () => {
      const res = await proxyPost('/v0/closed-rounds', {});
      
      expect(res.status).toBe(200);
      expect(res.data).toHaveProperty('rounds');
      expect(Array.isArray(res.data.rounds)).toBe(true);
    }, NETWORK_TIMEOUT);
    
    it('POST /v0/round-totals should return round statistics', async () => {
      // First get the latest round to query valid range
      const latestRes = await proxyPost('/v0/round-of-latest-data', {});
      const latestRound = latestRes.data.round;
      
      const res = await proxyPost('/v0/round-totals', {
        start_round: Math.max(0, latestRound - 5),
        end_round: latestRound,
      });
      
      expect(res.status).toBe(200);
      expect(res.data).toHaveProperty('entries');
      expect(Array.isArray(res.data.entries)).toBe(true);
    }, NETWORK_TIMEOUT);
    
    it('POST /v0/aggregated-rounds should return aggregated round range', async () => {
      const res = await proxyPost('/v0/aggregated-rounds', {});
      
      expect(res.status).toBe(200);
      expect(res.data).toHaveProperty('start');
      expect(res.data).toHaveProperty('end');
      expect(typeof res.data.start).toBe('number');
      expect(typeof res.data.end).toBe('number');
    }, NETWORK_TIMEOUT);
  });

  /* =========================================
   * Validator Endpoints
   * ========================================= */
  
  describe('Validator Endpoints', () => {
    it('POST /v0/top-validators-by-validator-faucets should return top validators', async () => {
      const res = await proxyPost('/v0/top-validators-by-validator-faucets', { limit: 10 });
      
      expect(res.status).toBe(200);
      expect(res.data).toHaveProperty('validatorsByReceivedFaucets');
      expect(Array.isArray(res.data.validatorsByReceivedFaucets)).toBe(true);
    }, NETWORK_TIMEOUT);
    
    it('POST /v0/admin/validator/licenses should return validator licenses', async () => {
      const res = await proxyPost('/v0/admin/validator/licenses', { limit: 10 });
      
      expect(res.status).toBe(200);
      expect(res.data).toHaveProperty('validator_licenses');
      expect(Array.isArray(res.data.validator_licenses)).toBe(true);
    }, NETWORK_TIMEOUT);
  });

  /* =========================================
   * ANS (Amulet Name Service) Endpoints
   * ========================================= */
  
  describe('ANS Endpoints', () => {
    it('POST /v0/ans-entries should return ANS entries', async () => {
      const res = await proxyPost('/v0/ans-entries', { page_size: 10 });
      
      expect(res.status).toBe(200);
      expect(res.data).toHaveProperty('entries');
      expect(Array.isArray(res.data.entries)).toBe(true);
    }, NETWORK_TIMEOUT);
  });

  /* =========================================
   * Featured Apps Endpoints
   * ========================================= */
  
  describe('Featured Apps Endpoints', () => {
    it('POST /v0/featured-apps should return featured apps', async () => {
      const res = await proxyPost('/v0/featured-apps', {});
      
      expect(res.status).toBe(200);
      expect(res.data).toHaveProperty('featured_apps');
      expect(Array.isArray(res.data.featured_apps)).toBe(true);
    }, NETWORK_TIMEOUT);
  });

  /* =========================================
   * Transaction Endpoints
   * ========================================= */
  
  describe('Transaction Endpoints', () => {
    it('POST /v0/transactions should return transaction history', async () => {
      const res = await proxyPost('/v0/transactions', {
        page_size: 5,
        sort_order: 'desc',
      });
      
      expect(res.status).toBe(200);
      expect(res.data).toHaveProperty('transactions');
      expect(Array.isArray(res.data.transactions)).toBe(true);
    }, NETWORK_TIMEOUT);
  });

  /* =========================================
   * Updates Endpoints (v1 and v2)
   * ========================================= */
  
  describe('Updates Endpoints', () => {
    it('POST /v1/updates should return update history', async () => {
      const res = await proxyPost('/v1/updates', {
        page_size: 5,
      });
      
      expect(res.status).toBe(200);
      expect(res.data).toHaveProperty('transactions');
      expect(Array.isArray(res.data.transactions)).toBe(true);
    }, NETWORK_TIMEOUT);
    
    it('POST /v2/updates should return update history', async () => {
      const res = await proxyPost('/v2/updates', {
        page_size: 5,
      });
      
      expect(res.status).toBe(200);
      expect(res.data).toHaveProperty('transactions');
      expect(Array.isArray(res.data.transactions)).toBe(true);
    }, NETWORK_TIMEOUT);
  });

  /* =========================================
   * State/ACS Endpoints
   * ========================================= */
  
  describe('State/ACS Endpoints', () => {
    it('POST /v0/state/acs/snapshot/timestamp should return snapshot timestamp', async () => {
      // First get latest round to get a valid timestamp
      const latestRes = await proxyPost('/v0/round-of-latest-data', {});
      const effectiveAt = latestRes.data.effectiveAt;
      
      const res = await proxyPost('/v0/state/acs/snapshot/timestamp', {
        record_time: effectiveAt,
        migration_id: 0,
      });
      
      expect(res.status).toBe(200);
      expect(res.data).toHaveProperty('record_time');
    }, NETWORK_TIMEOUT);
    
    it('POST /v0/state/acs should return ACS entries', async () => {
      // First get snapshot timestamp
      const latestRes = await proxyPost('/v0/round-of-latest-data', {});
      const effectiveAt = latestRes.data.effectiveAt;
      
      const snapRes = await proxyPost('/v0/state/acs/snapshot/timestamp', {
        record_time: effectiveAt,
        migration_id: 0,
      });
      
      const res = await proxyPost('/v0/state/acs', {
        migration_id: 0,
        record_time: snapRes.data.record_time,
        page_size: 10,
      });
      
      expect(res.status).toBe(200);
      expect(res.data).toHaveProperty('created_events');
      expect(Array.isArray(res.data.created_events)).toBe(true);
    }, NETWORK_TIMEOUT);
  });

  /* =========================================
   * Governance Endpoints
   * ========================================= */
  
  describe('Governance Endpoints', () => {
    it('POST /v0/admin/sv/voterequests should return active vote requests', async () => {
      const res = await proxyPost('/v0/admin/sv/voterequests', {});
      
      expect(res.status).toBe(200);
      expect(res.data).toHaveProperty('dso_rules_vote_requests');
      expect(Array.isArray(res.data.dso_rules_vote_requests)).toBe(true);
    }, NETWORK_TIMEOUT);
    
    it('POST /v0/admin/sv/voteresults should return vote results', async () => {
      const res = await proxyPost('/v0/admin/sv/voteresults', { limit: 10 });
      
      expect(res.status).toBe(200);
      expect(res.data).toHaveProperty('dso_rules_vote_results');
      expect(Array.isArray(res.data.dso_rules_vote_results)).toBe(true);
    }, NETWORK_TIMEOUT);
  });

  /* =========================================
   * App Rewards Endpoints
   * ========================================= */
  
  describe('App Rewards Endpoints', () => {
    it('POST /v0/top-providers-by-app-rewards should return top providers', async () => {
      // First get the latest round
      const latestRes = await proxyPost('/v0/round-of-latest-data', {});
      const round = latestRes.data.round;
      
      const res = await proxyPost('/v0/top-providers-by-app-rewards', {
        round,
        limit: 10,
      });
      
      expect(res.status).toBe(200);
      expect(res.data).toHaveProperty('providersAndRewards');
      expect(Array.isArray(res.data.providersAndRewards)).toBe(true);
    }, NETWORK_TIMEOUT);
  });

  /* =========================================
   * Splice Instance Names
   * ========================================= */
  
  describe('Splice Instance Endpoints', () => {
    it('POST /v0/splice-instance-names should return instance names', async () => {
      const res = await proxyPost('/v0/splice-instance-names', {});
      
      expect(res.status).toBe(200);
      expect(res.data).toHaveProperty('network_name');
      expect(res.data).toHaveProperty('amulet_name');
      expect(res.data).toHaveProperty('amulet_name_acronym');
    }, NETWORK_TIMEOUT);
  });

  /* =========================================
   * Migration Schedule
   * ========================================= */
  
  describe('Migration Endpoints', () => {
    it('POST /v0/migrations/schedule should return migration schedule', async () => {
      const res = await proxyPost('/v0/migrations/schedule', {});
      
      // This endpoint may return 404 if no migration is scheduled
      // Both 200 and 404 are valid responses
      expect([200, 404]).toContain(res.status);
      if (res.status === 200) {
        expect(res.data).toHaveProperty('time');
        expect(res.data).toHaveProperty('migration_id');
      }
    }, NETWORK_TIMEOUT);
  });

  /* =========================================
   * External Party Rules
   * ========================================= */
  
  describe('External Party Endpoints', () => {
    it('POST /v0/external-party-amulet-rules should return rules', async () => {
      const res = await proxyPost('/v0/external-party-amulet-rules', {});
      
      // This endpoint may return 404 if no rules exist
      expect([200, 404]).toContain(res.status);
    }, NETWORK_TIMEOUT);
  });

  /* =========================================
   * Error Handling
   * ========================================= */
  
  describe('Error Handling', () => {
    it('should return error for invalid endpoint', async () => {
      const res = await proxyPost('/v0/nonexistent-endpoint', {});
      
      // Should return 4xx error
      expect(res.status).toBeGreaterThanOrEqual(400);
    }, NETWORK_TIMEOUT);
    
    it('should include X-Scan-Endpoint header on successful requests', async () => {
      const res = await proxyPost('/v0/dso', {});
      
      if (res.status === 200) {
        expect(res.headers.get('x-scan-endpoint')).toBeTruthy();
      }
    }, NETWORK_TIMEOUT);
  });
});
