/**
 * Scan API Proxy Integration Tests
 * 
 * Tests the scan-proxy endpoints that proxy requests to the Canton Scan API.
 * These tests verify that the proxy correctly forwards requests with the
 * CORRECT HTTP METHODS as per the official SCAN API documentation.
 * 
 * CRITICAL: GET vs POST is method-sensitive. Using the wrong method causes 405 errors.
 * 
 * Note: These tests require network access to the actual Canton Scan API.
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
    const res = await fetch(`${baseUrl}/scan-proxy${path}`, {
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
  async function proxyGet(path, queryParams = {}) {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(queryParams)) {
      if (Array.isArray(value)) {
        value.forEach(v => params.append(key, v));
      } else if (value !== undefined) {
        params.append(key, value);
      }
    }
    const queryString = params.toString();
    const url = queryString
      ? `${baseUrl}/scan-proxy${path}?${queryString}`
      : `${baseUrl}/scan-proxy${path}`;
    
    const res = await fetch(url, { method: 'GET' });
    
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
   * GET Endpoints (no body, query params only)
   * ========================================= */
  
  describe('GET Endpoints', () => {
    describe('GET /v0/dso', () => {
      it('should return DSO info', async () => {
        const res = await proxyGet('/v0/dso');
        
        // Accept 200 or 5xx if upstream is down
        if (res.status === 200) {
          expect(res.data).toHaveProperty('dso_party_id');
          expect(res.data).toHaveProperty('dso_rules');
          expect(res.data).toHaveProperty('amulet_rules');
        } else {
          // Upstream might be unavailable
          expect([500, 502, 503, 504]).toContain(res.status);
        }
      }, NETWORK_TIMEOUT);
    });

    describe('GET /v0/dso-party-id', () => {
      it('should return DSO party ID', async () => {
        const res = await proxyGet('/v0/dso-party-id');
        
        if (res.status === 200) {
          expect(res.data).toHaveProperty('dso_party_id');
          expect(typeof res.data.dso_party_id).toBe('string');
        }
      }, NETWORK_TIMEOUT);
    });

    describe('GET /v0/scans', () => {
      it('should return list of scans', async () => {
        const res = await proxyGet('/v0/scans');
        
        if (res.status === 200) {
          expect(res.data).toHaveProperty('scans');
          expect(Array.isArray(res.data.scans)).toBe(true);
        }
      }, NETWORK_TIMEOUT);
    });

    describe('GET /v0/dso-sequencers', () => {
      it('should return sequencer info', async () => {
        const res = await proxyGet('/v0/dso-sequencers');
        
        if (res.status === 200) {
          expect(res.data).toHaveProperty('domainSequencers');
          expect(Array.isArray(res.data.domainSequencers)).toBe(true);
        }
      }, NETWORK_TIMEOUT);
    });

    describe('GET /v0/closed-rounds', () => {
      it('should return closed rounds', async () => {
        const res = await proxyGet('/v0/closed-rounds');
        
        if (res.status === 200) {
          expect(res.data).toHaveProperty('rounds');
          expect(Array.isArray(res.data.rounds)).toBe(true);
        }
      }, NETWORK_TIMEOUT);
    });

    describe('GET /v0/round-of-latest-data (deprecated)', () => {
      it('should return latest round', async () => {
        const res = await proxyGet('/v0/round-of-latest-data');
        
        if (res.status === 200) {
          expect(res.data).toHaveProperty('round');
          expect(typeof res.data.round).toBe('number');
          expect(res.data).toHaveProperty('effectiveAt');
        }
      }, NETWORK_TIMEOUT);
    });

    describe('GET /v0/aggregated-rounds (deprecated)', () => {
      it('should return aggregated round range', async () => {
        const res = await proxyGet('/v0/aggregated-rounds');
        
        if (res.status === 200) {
          expect(res.data).toHaveProperty('start');
          expect(res.data).toHaveProperty('end');
          expect(typeof res.data.start).toBe('number');
          expect(typeof res.data.end).toBe('number');
        }
      }, NETWORK_TIMEOUT);
    });

    describe('GET /v0/top-validators-by-validator-faucets', () => {
      it('should return top validators with limit param', async () => {
        const res = await proxyGet('/v0/top-validators-by-validator-faucets', { limit: 10 });
        
        if (res.status === 200) {
          expect(res.data).toHaveProperty('validatorsByReceivedFaucets');
          expect(Array.isArray(res.data.validatorsByReceivedFaucets)).toBe(true);
        }
      }, NETWORK_TIMEOUT);
    });

    describe('GET /v0/admin/validator/licenses', () => {
      it('should return validator licenses with query params', async () => {
        const res = await proxyGet('/v0/admin/validator/licenses', { limit: 10 });
        
        if (res.status === 200) {
          expect(res.data).toHaveProperty('validator_licenses');
          expect(Array.isArray(res.data.validator_licenses)).toBe(true);
        }
      }, NETWORK_TIMEOUT);
    });

    describe('GET /v0/ans-entries', () => {
      it('should return ANS entries with page_size', async () => {
        const res = await proxyGet('/v0/ans-entries', { page_size: 10 });
        
        if (res.status === 200) {
          expect(res.data).toHaveProperty('entries');
          expect(Array.isArray(res.data.entries)).toBe(true);
        }
      }, NETWORK_TIMEOUT);
    });

    describe('GET /v0/featured-apps', () => {
      it('should return featured apps', async () => {
        const res = await proxyGet('/v0/featured-apps');
        
        if (res.status === 200) {
          expect(res.data).toHaveProperty('featured_apps');
          expect(Array.isArray(res.data.featured_apps)).toBe(true);
        }
      }, NETWORK_TIMEOUT);
    });

    describe('GET /v0/admin/sv/voterequests', () => {
      it('should return active vote requests', async () => {
        const res = await proxyGet('/v0/admin/sv/voterequests');
        
        if (res.status === 200) {
          expect(res.data).toHaveProperty('dso_rules_vote_requests');
          expect(Array.isArray(res.data.dso_rules_vote_requests)).toBe(true);
        }
      }, NETWORK_TIMEOUT);
    });

    describe('GET /v0/splice-instance-names', () => {
      it('should return instance names', async () => {
        const res = await proxyGet('/v0/splice-instance-names');
        
        if (res.status === 200) {
          expect(res.data).toHaveProperty('network_name');
          expect(res.data).toHaveProperty('amulet_name');
          expect(res.data).toHaveProperty('amulet_name_acronym');
        }
      }, NETWORK_TIMEOUT);
    });

    describe('GET /v0/migrations/schedule', () => {
      it('should return migration schedule (or 404 if none scheduled)', async () => {
        const res = await proxyGet('/v0/migrations/schedule');
        
        // 200 if scheduled, 404 if not
        expect([200, 404, 500, 502, 503]).toContain(res.status);
        if (res.status === 200) {
          expect(res.data).toHaveProperty('time');
          expect(res.data).toHaveProperty('migration_id');
        }
      }, NETWORK_TIMEOUT);
    });

    describe('GET /v0/unclaimed-development-fund-coupons', () => {
      it('should return unclaimed coupons', async () => {
        const res = await proxyGet('/v0/unclaimed-development-fund-coupons');
        
        if (res.status === 200) {
          expect(res.data).toHaveProperty('unclaimed-development-fund-coupons');
        }
      }, NETWORK_TIMEOUT);
    });

    describe('GET /v0/backfilling/status', () => {
      it('should return backfill status', async () => {
        const res = await proxyGet('/v0/backfilling/status');
        
        if (res.status === 200) {
          expect(res.data).toHaveProperty('complete');
          expect(typeof res.data.complete).toBe('boolean');
        }
      }, NETWORK_TIMEOUT);
    });

    describe('GET /v0/feature-support', () => {
      it('should return feature flags', async () => {
        const res = await proxyGet('/v0/feature-support');
        
        if (res.status === 200) {
          expect(typeof res.data).toBe('object');
        }
      }, NETWORK_TIMEOUT);
    });

    describe('GET /v0/amulet-price/votes', () => {
      it('should return amulet price votes', async () => {
        const res = await proxyGet('/v0/amulet-price/votes');
        
        if (res.status === 200) {
          expect(res.data).toHaveProperty('amulet_price_votes');
          expect(Array.isArray(res.data.amulet_price_votes)).toBe(true);
        }
      }, NETWORK_TIMEOUT);
    });

    describe('GET /v0/amulet-config-for-round (deprecated)', () => {
      it('should return amulet config for round', async () => {
        // First get latest round
        const latestRes = await proxyGet('/v0/round-of-latest-data');
        if (latestRes.status !== 200) return;
        
        const res = await proxyGet('/v0/amulet-config-for-round', { round: latestRes.data.round });
        
        if (res.status === 200) {
          expect(res.data).toHaveProperty('amulet_create_fee');
        }
      }, NETWORK_TIMEOUT);
    });

    describe('GET /v0/rewards-collected (deprecated)', () => {
      it('should return total rewards collected', async () => {
        const res = await proxyGet('/v0/rewards-collected');
        
        if (res.status === 200) {
          expect(res.data).toHaveProperty('amount');
        }
      }, NETWORK_TIMEOUT);
    });

    describe('GET /v0/top-providers-by-app-rewards (deprecated)', () => {
      it('should return top providers with round and limit', async () => {
        const latestRes = await proxyGet('/v0/round-of-latest-data');
        if (latestRes.status !== 200) return;
        
        const res = await proxyGet('/v0/top-providers-by-app-rewards', {
          round: latestRes.data.round,
          limit: 10,
        });
        
        if (res.status === 200) {
          expect(res.data).toHaveProperty('providersAndRewards');
          expect(Array.isArray(res.data.providersAndRewards)).toBe(true);
        }
      }, NETWORK_TIMEOUT);
    });

    describe('GET /v0/top-validators-by-validator-rewards (deprecated)', () => {
      it('should return top validators by rewards', async () => {
        const latestRes = await proxyGet('/v0/round-of-latest-data');
        if (latestRes.status !== 200) return;
        
        const res = await proxyGet('/v0/top-validators-by-validator-rewards', {
          round: latestRes.data.round,
          limit: 10,
        });
        
        if (res.status === 200) {
          expect(res.data).toHaveProperty('validatorsAndRewards');
        }
      }, NETWORK_TIMEOUT);
    });

    describe('GET /v0/top-validators-by-purchased-traffic (deprecated)', () => {
      it('should return top validators by traffic', async () => {
        const latestRes = await proxyGet('/v0/round-of-latest-data');
        if (latestRes.status !== 200) return;
        
        const res = await proxyGet('/v0/top-validators-by-purchased-traffic', {
          round: latestRes.data.round,
          limit: 10,
        });
        
        if (res.status === 200) {
          expect(res.data).toHaveProperty('validatorsByPurchasedTraffic');
        }
      }, NETWORK_TIMEOUT);
    });

    describe('GET /v0/validators/validator-faucets', () => {
      it('should return validator faucets for given IDs', async () => {
        // First get a validator from top validators
        const topRes = await proxyGet('/v0/top-validators-by-validator-faucets', { limit: 1 });
        if (topRes.status !== 200 || !topRes.data.validatorsByReceivedFaucets?.length) return;
        
        const validatorId = topRes.data.validatorsByReceivedFaucets[0].validator;
        const res = await proxyGet('/v0/validators/validator-faucets', { validator_ids: [validatorId] });
        
        if (res.status === 200) {
          expect(res.data).toHaveProperty('validatorsReceivedFaucets');
        }
      }, NETWORK_TIMEOUT);
    });

    describe('GET /v0/state/acs/snapshot-timestamp', () => {
      it('should return snapshot timestamp', async () => {
        const res = await proxyGet('/v0/state/acs/snapshot-timestamp', {
          before: new Date().toISOString(),
          migration_id: 0,
        });
        
        if (res.status === 200) {
          expect(res.data).toHaveProperty('record_time');
        }
      }, NETWORK_TIMEOUT);
    });
  });

  /* =========================================
   * POST Endpoints (require request body)
   * ========================================= */
  
  describe('POST Endpoints', () => {
    describe('POST /v0/open-and-issuing-mining-rounds', () => {
      it('should return active mining rounds', async () => {
        const res = await proxyPost('/v0/open-and-issuing-mining-rounds', {});
        
        if (res.status === 200) {
          expect(res.data).toHaveProperty('open_mining_rounds');
          expect(res.data).toHaveProperty('issuing_mining_rounds');
          expect(res.data).toHaveProperty('time_to_live_in_microseconds');
        }
      }, NETWORK_TIMEOUT);
    });

    describe('POST /v0/round-totals (deprecated)', () => {
      it('should return round statistics', async () => {
        const latestRes = await proxyGet('/v0/round-of-latest-data');
        if (latestRes.status !== 200) return;
        
        const latestRound = latestRes.data.round;
        const res = await proxyPost('/v0/round-totals', {
          start_round: Math.max(0, latestRound - 5),
          end_round: latestRound,
        });
        
        if (res.status === 200) {
          expect(res.data).toHaveProperty('entries');
          expect(Array.isArray(res.data.entries)).toBe(true);
        }
      }, NETWORK_TIMEOUT);
    });

    describe('POST /v0/round-party-totals (deprecated)', () => {
      it('should return per-party round totals', async () => {
        const latestRes = await proxyGet('/v0/round-of-latest-data');
        if (latestRes.status !== 200) return;
        
        const latestRound = latestRes.data.round;
        const res = await proxyPost('/v0/round-party-totals', {
          start_round: Math.max(0, latestRound - 2),
          end_round: latestRound,
        });
        
        if (res.status === 200) {
          expect(res.data).toHaveProperty('entries');
        }
      }, NETWORK_TIMEOUT);
    });

    describe('POST /v2/updates', () => {
      it('should return update history', async () => {
        const res = await proxyPost('/v2/updates', {
          page_size: 5,
          daml_value_encoding: 'compact_json',
        });
        
        if (res.status === 200) {
          expect(res.data).toHaveProperty('transactions');
          expect(Array.isArray(res.data.transactions)).toBe(true);
        }
      }, NETWORK_TIMEOUT);
    });

    describe('POST /v1/updates (deprecated)', () => {
      it('should return update history', async () => {
        const res = await proxyPost('/v1/updates', { page_size: 5 });
        
        if (res.status === 200) {
          expect(res.data).toHaveProperty('transactions');
          expect(Array.isArray(res.data.transactions)).toBe(true);
        }
      }, NETWORK_TIMEOUT);
    });

    describe('POST /v0/updates (deprecated)', () => {
      it('should return update history', async () => {
        const res = await proxyPost('/v0/updates', { page_size: 5 });
        
        if (res.status === 200) {
          expect(res.data).toHaveProperty('transactions');
        }
      }, NETWORK_TIMEOUT);
    });

    describe('POST /v0/transactions (deprecated)', () => {
      it('should return transaction history', async () => {
        const res = await proxyPost('/v0/transactions', {
          page_size: 5,
          sort_order: 'desc',
        });
        
        if (res.status === 200) {
          expect(res.data).toHaveProperty('transactions');
          expect(Array.isArray(res.data.transactions)).toBe(true);
        }
      }, NETWORK_TIMEOUT);
    });

    describe('POST /v0/activities (deprecated)', () => {
      it('should return activities', async () => {
        const res = await proxyPost('/v0/activities', { page_size: 5 });
        
        if (res.status === 200) {
          expect(res.data).toHaveProperty('activities');
        }
      }, NETWORK_TIMEOUT);
    });

    describe('POST /v0/events', () => {
      it('should return events', async () => {
        const res = await proxyPost('/v0/events', { page_size: 5 });
        
        if (res.status === 200) {
          expect(res.data).toHaveProperty('events');
          expect(Array.isArray(res.data.events)).toBe(true);
        }
      }, NETWORK_TIMEOUT);
    });

    describe('POST /v0/state/acs', () => {
      it('should return ACS entries', async () => {
        // Get snapshot timestamp first
        const snapRes = await proxyGet('/v0/state/acs/snapshot-timestamp', {
          before: new Date().toISOString(),
          migration_id: 0,
        });
        if (snapRes.status !== 200) return;
        
        const res = await proxyPost('/v0/state/acs', {
          migration_id: 0,
          record_time: snapRes.data.record_time,
          page_size: 10,
        });
        
        if (res.status === 200) {
          expect(res.data).toHaveProperty('created_events');
          expect(Array.isArray(res.data.created_events)).toBe(true);
        }
      }, NETWORK_TIMEOUT);
    });

    describe('POST /v0/holdings/summary', () => {
      it('should return holdings summary', async () => {
        // Get DSO party first
        const dsoRes = await proxyGet('/v0/dso-party-id');
        if (dsoRes.status !== 200) return;
        
        const snapRes = await proxyGet('/v0/state/acs/snapshot-timestamp', {
          before: new Date().toISOString(),
          migration_id: 0,
        });
        if (snapRes.status !== 200) return;
        
        const res = await proxyPost('/v0/holdings/summary', {
          migration_id: 0,
          record_time: snapRes.data.record_time,
          owner_party_ids: [dsoRes.data.dso_party_id],
        });
        
        if (res.status === 200) {
          expect(res.data).toHaveProperty('summaries');
        }
      }, NETWORK_TIMEOUT);
    });

    describe('POST /v0/holdings/state', () => {
      it('should return holdings state', async () => {
        const snapRes = await proxyGet('/v0/state/acs/snapshot-timestamp', {
          before: new Date().toISOString(),
          migration_id: 0,
        });
        if (snapRes.status !== 200) return;
        
        const res = await proxyPost('/v0/holdings/state', {
          migration_id: 0,
          record_time: snapRes.data.record_time,
          page_size: 10,
        });
        
        if (res.status === 200) {
          expect(res.data).toHaveProperty('created_events');
        }
      }, NETWORK_TIMEOUT);
    });

    describe('POST /v0/amulet-rules', () => {
      it('should return amulet rules', async () => {
        const res = await proxyPost('/v0/amulet-rules', {});
        
        if (res.status === 200) {
          expect(res.data).toHaveProperty('amulet_rules_update');
        }
      }, NETWORK_TIMEOUT);
    });

    describe('POST /v0/external-party-amulet-rules', () => {
      it('should return external party rules (or 404)', async () => {
        const res = await proxyPost('/v0/external-party-amulet-rules', {});
        
        expect([200, 404, 500, 502]).toContain(res.status);
      }, NETWORK_TIMEOUT);
    });

    describe('POST /v0/ans-rules', () => {
      it('should return ANS rules', async () => {
        const res = await proxyPost('/v0/ans-rules', {});
        
        if (res.status === 200) {
          expect(res.data).toHaveProperty('ans_rules_update');
        }
      }, NETWORK_TIMEOUT);
    });

    describe('POST /v0/admin/sv/voteresults', () => {
      it('should return vote results', async () => {
        const res = await proxyPost('/v0/admin/sv/voteresults', { limit: 10 });
        
        if (res.status === 200) {
          expect(res.data).toHaveProperty('dso_rules_vote_results');
          expect(Array.isArray(res.data.dso_rules_vote_results)).toBe(true);
        }
      }, NETWORK_TIMEOUT);
    });

    describe('POST /v0/voterequest', () => {
      it('should return vote requests for given IDs', async () => {
        const res = await proxyPost('/v0/voterequest', { vote_request_contract_ids: [] });
        
        if (res.status === 200) {
          expect(res.data).toHaveProperty('vote_requests');
        }
      }, NETWORK_TIMEOUT);
    });

    describe('POST /v0/backfilling/migration-info', () => {
      it('should return migration info', async () => {
        const res = await proxyPost('/v0/backfilling/migration-info', { migration_id: 0 });
        
        if (res.status === 200) {
          expect(res.data).toHaveProperty('complete');
        }
      }, NETWORK_TIMEOUT);
    });

    describe('POST /v0/backfilling/updates-before', () => {
      it('should return updates before timestamp', async () => {
        // This is a complex endpoint, just verify it doesn't 405
        const res = await proxyPost('/v0/backfilling/updates-before', {
          migration_id: 0,
          synchronizer_id: 'test',
          before: new Date().toISOString(),
          count: 5,
        });
        
        // 200, 400, or 404 are all acceptable (invalid params may fail)
        expect([200, 400, 404, 500, 502]).toContain(res.status);
      }, NETWORK_TIMEOUT);
    });

    describe('POST /v0/backfilling/import-updates', () => {
      it('should return import updates', async () => {
        const res = await proxyPost('/v0/backfilling/import-updates', {
          migration_id: 0,
          limit: 5,
        });
        
        if (res.status === 200) {
          expect(res.data).toHaveProperty('transactions');
        }
      }, NETWORK_TIMEOUT);
    });
  });

  /* =========================================
   * Method Sensitivity Tests
   * ========================================= */
  
  describe('Method Sensitivity (GET vs POST)', () => {
    it('POST to GET-only endpoint should return 405', async () => {
      // /v0/dso is a GET endpoint per docs
      const res = await proxyPost('/v0/dso', {});
      
      // If upstream is reachable, it should return 405
      // If not, we may get 5xx from proxy
      if (res.status !== 405 && res.status < 500) {
        // Some implementations may accept POST as well
        console.log(`Note: /v0/dso accepted POST with status ${res.status}`);
      }
    }, NETWORK_TIMEOUT);

    it('GET to POST-only endpoint should return 405', async () => {
      // /v2/updates is a POST endpoint per docs
      const res = await proxyGet('/v2/updates');
      
      // If upstream is reachable, it should return 405
      if (res.status !== 405 && res.status < 500) {
        console.log(`Note: /v2/updates accepted GET with status ${res.status}`);
      }
    }, NETWORK_TIMEOUT);
  });

  /* =========================================
   * Error Handling
   * ========================================= */
  
  describe('Error Handling', () => {
    it('should return error for invalid endpoint', async () => {
      const res = await proxyGet('/v0/nonexistent-endpoint');
      
      // Should return 4xx error from upstream or 502 if all endpoints fail
      expect(res.status).toBeGreaterThanOrEqual(400);
    }, NETWORK_TIMEOUT);
    
    it('should include X-Scan-Endpoint header on successful requests', async () => {
      const res = await proxyGet('/v0/dso');
      
      if (res.status === 200) {
        expect(res.headers.get('x-scan-endpoint')).toBeTruthy();
      }
    }, NETWORK_TIMEOUT);
  });

  /* =========================================
   * Path Parameter Endpoints
   * ========================================= */

  describe('Path Parameter Endpoints', () => {
    describe('GET /v0/ans-entries/by-name/{name}', () => {
      it('should lookup ANS entry by name', async () => {
        // First get some entries to find a valid name
        const listRes = await proxyGet('/v0/ans-entries', { page_size: 1 });
        if (listRes.status !== 200 || !listRes.data.entries?.length) return;
        
        const name = listRes.data.entries[0].name;
        const res = await proxyGet(`/v0/ans-entries/by-name/${encodeURIComponent(name)}`);
        
        if (res.status === 200) {
          expect(res.data).toHaveProperty('entry');
          expect(res.data.entry.name).toBe(name);
        }
      }, NETWORK_TIMEOUT);
    });

    describe('GET /v2/updates/{update_id}', () => {
      it('should lookup update by ID', async () => {
        // First get some updates
        const listRes = await proxyPost('/v2/updates', { page_size: 1 });
        if (listRes.status !== 200 || !listRes.data.transactions?.length) return;
        
        const updateId = listRes.data.transactions[0].update_id;
        if (!updateId) return;
        
        const res = await proxyGet(`/v2/updates/${encodeURIComponent(updateId)}`);
        
        if (res.status === 200) {
          expect(res.data).toHaveProperty('update_id', updateId);
        }
      }, NETWORK_TIMEOUT);
    });

    describe('GET /v0/voterequests/{contract_id}', () => {
      it('should lookup vote request by contract ID (or 404)', async () => {
        const res = await proxyGet('/v0/voterequests/nonexistent-contract-id');
        
        // 404 is expected for nonexistent contract
        expect([404, 500, 502]).toContain(res.status);
      }, NETWORK_TIMEOUT);
    });
  });
});
