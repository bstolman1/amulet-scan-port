/**
 * SCAN API Client Tests
 * 
 * Comprehensive test suite for all Canton Scan API endpoints.
 * Tests verify correct HTTP methods (GET vs POST), URL construction,
 * query parameters, request bodies, and error handling.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { scanApi } from './api-client';

// Mock fetch globally
const mockFetch = vi.fn();
global.fetch = mockFetch;

// Helper to create successful response
const mockSuccess = <T>(data: T) => ({
  ok: true,
  json: async () => data,
  text: async () => JSON.stringify(data),
});

// Helper to create error response
const mockError = (status: number, message: string) => ({
  ok: false,
  status,
  json: async () => ({ error: message }),
  text: async () => message,
});

describe('SCAN API Client', () => {
  beforeEach(() => {
    mockFetch.mockReset();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  /* ==========================================================
   *  POST ENDPOINTS
   * ========================================================== */
  
  describe('POST endpoints', () => {
    describe('fetchUpdates (POST /v2/updates)', () => {
      it('should use POST method with correct body', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ transactions: [] }));

        await scanApi.fetchUpdates({ page_size: 50 });

        expect(mockFetch).toHaveBeenCalledWith(
          '/api/scan-proxy/v2/updates',
          expect.objectContaining({
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ page_size: 50 }),
          })
        );
      });

      it('should include after cursor when provided', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ transactions: [] }));

        await scanApi.fetchUpdates({
          after: { after_migration_id: 1, after_record_time: '2025-01-01T00:00:00Z' },
          page_size: 100,
          daml_value_encoding: 'compact_json',
        });

        const body = JSON.parse(mockFetch.mock.calls[0][1].body);
        expect(body.after).toEqual({ after_migration_id: 1, after_record_time: '2025-01-01T00:00:00Z' });
        expect(body.page_size).toBe(100);
        expect(body.daml_value_encoding).toBe('compact_json');
      });
    });

    describe('fetchOpenAndIssuingRounds (POST /v0/open-and-issuing-mining-rounds)', () => {
      it('should POST with empty body by default', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({
          time_to_live_in_microseconds: 1000,
          open_mining_rounds: {},
          issuing_mining_rounds: {},
        }));

        await scanApi.fetchOpenAndIssuingRounds();

        expect(mockFetch).toHaveBeenCalledWith(
          '/api/scan-proxy/v0/open-and-issuing-mining-rounds',
          expect.objectContaining({
            method: 'POST',
            body: JSON.stringify({}),
          })
        );
      });

      it('should include cached contract IDs when provided', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({
          time_to_live_in_microseconds: 1000,
          open_mining_rounds: {},
          issuing_mining_rounds: {},
        }));

        await scanApi.fetchOpenAndIssuingRounds({
          cached_open_mining_round_contract_ids: ['contract-1'],
          cached_issuing_round_contract_ids: ['contract-2'],
        });

        const body = JSON.parse(mockFetch.mock.calls[0][1].body);
        expect(body.cached_open_mining_round_contract_ids).toEqual(['contract-1']);
        expect(body.cached_issuing_round_contract_ids).toEqual(['contract-2']);
      });
    });

    describe('fetchStateAcs (POST /v0/state/acs)', () => {
      it('should POST with required params', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({
          record_time: '2025-01-01T00:00:00Z',
          migration_id: 1,
          created_events: [],
        }));

        await scanApi.fetchStateAcs({
          migration_id: 1,
          record_time: '2025-01-01T00:00:00Z',
          page_size: 100,
        });

        const body = JSON.parse(mockFetch.mock.calls[0][1].body);
        expect(body.migration_id).toBe(1);
        expect(body.record_time).toBe('2025-01-01T00:00:00Z');
        expect(body.page_size).toBe(100);
      });

      it('should include optional filters', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({
          record_time: '2025-01-01T00:00:00Z',
          migration_id: 1,
          created_events: [],
          next_page_token: 50,
        }));

        await scanApi.fetchStateAcs({
          migration_id: 1,
          record_time: '2025-01-01T00:00:00Z',
          record_time_match: 'before',
          after: 0,
          page_size: 100,
          party_ids: ['party-1'],
          templates: ['Template:Test'],
        });

        const body = JSON.parse(mockFetch.mock.calls[0][1].body);
        expect(body.record_time_match).toBe('before');
        expect(body.party_ids).toEqual(['party-1']);
        expect(body.templates).toEqual(['Template:Test']);
      });
    });

    describe('fetchHoldingsSummary (POST /v0/holdings/summary)', () => {
      it('should POST with party IDs', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({
          record_time: '2025-01-01T00:00:00Z',
          migration_id: 1,
          computed_as_of_round: 100,
          summaries: [],
        }));

        await scanApi.fetchHoldingsSummary({
          migration_id: 1,
          record_time: '2025-01-01T00:00:00Z',
          owner_party_ids: ['party-1', 'party-2'],
        });

        const body = JSON.parse(mockFetch.mock.calls[0][1].body);
        expect(body.owner_party_ids).toEqual(['party-1', 'party-2']);
      });
    });

    describe('fetchEvents (POST /v0/events)', () => {
      it('should POST with page_size', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ events: [] }));

        await scanApi.fetchEvents({ page_size: 50 });

        expect(mockFetch).toHaveBeenCalledWith(
          '/api/scan-proxy/v0/events',
          expect.objectContaining({ method: 'POST' })
        );
      });
    });

    describe('fetchAmuletRules (POST /v0/amulet-rules)', () => {
      it('should POST with cached IDs when provided', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ amulet_rules_update: {} }));

        await scanApi.fetchAmuletRules('contract-id', 'domain-id');

        const body = JSON.parse(mockFetch.mock.calls[0][1].body);
        expect(body.cached_amulet_rules_contract_id).toBe('contract-id');
        expect(body.cached_amulet_rules_domain_id).toBe('domain-id');
      });
    });

    describe('fetchExternalPartyAmuletRules (POST /v0/external-party-amulet-rules)', () => {
      it('should POST with empty body', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ external_party_amulet_rules_update: {} }));

        await scanApi.fetchExternalPartyAmuletRules();

        expect(mockFetch).toHaveBeenCalledWith(
          '/api/scan-proxy/v0/external-party-amulet-rules',
          expect.objectContaining({
            method: 'POST',
            body: JSON.stringify({}),
          })
        );
      });
    });

    describe('fetchAnsRules (POST /v0/ans-rules)', () => {
      it('should POST with empty body', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ ans_rules_update: {} }));

        await scanApi.fetchAnsRules();

        expect(mockFetch).toHaveBeenCalledWith(
          '/api/scan-proxy/v0/ans-rules',
          expect.objectContaining({ method: 'POST' })
        );
      });
    });

    describe('fetchVoteRequestsBatch (POST /v0/voterequest)', () => {
      it('should POST with contract IDs array', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ vote_requests: [] }));

        await scanApi.fetchVoteRequestsBatch(['contract-1', 'contract-2']);

        const body = JSON.parse(mockFetch.mock.calls[0][1].body);
        expect(body.vote_request_contract_ids).toEqual(['contract-1', 'contract-2']);
      });
    });

    describe('fetchVoteResults (POST /v0/admin/sv/voteresults)', () => {
      it('should POST with filter parameters', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ dso_rules_vote_results: [] }));

        await scanApi.fetchVoteResults({
          actionName: 'SRARC_AddSv',
          accepted: true,
          limit: 50,
        });

        const body = JSON.parse(mockFetch.mock.calls[0][1].body);
        expect(body.actionName).toBe('SRARC_AddSv');
        expect(body.accepted).toBe(true);
        expect(body.limit).toBe(50);
      });
    });

    describe('fetchMigrationInfo (POST /v0/backfilling/migration-info)', () => {
      it('should POST with migration_id', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({
          previous_migration_id: 0,
          complete: true,
        }));

        await scanApi.fetchMigrationInfo(1);

        const body = JSON.parse(mockFetch.mock.calls[0][1].body);
        expect(body.migration_id).toBe(1);
      });
    });

    describe('fetchBackfillUpdatesBefore (POST /v0/backfilling/updates-before)', () => {
      it('should POST with required params', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ transactions: [] }));

        await scanApi.fetchBackfillUpdatesBefore({
          migration_id: 1,
          synchronizer_id: 'sync-1',
          before: '2025-01-01T00:00:00Z',
          count: 100,
        });

        const body = JSON.parse(mockFetch.mock.calls[0][1].body);
        expect(body.migration_id).toBe(1);
        expect(body.synchronizer_id).toBe('sync-1');
        expect(body.before).toBe('2025-01-01T00:00:00Z');
        expect(body.count).toBe(100);
      });
    });

    describe('fetchUpdatesV1 (POST /v1/updates)', () => {
      it('should use POST for deprecated v1 endpoint', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ transactions: [] }));

        await scanApi.fetchUpdatesV1({ page_size: 50 });

        expect(mockFetch).toHaveBeenCalledWith(
          '/api/scan-proxy/v1/updates',
          expect.objectContaining({ method: 'POST' })
        );
      });
    });

    describe('forceAcsSnapshot (POST /v0/state/acs/force)', () => {
      it('should POST with empty body', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({
          record_time: '2025-01-01T00:00:00Z',
          migration_id: 1,
        }));

        await scanApi.forceAcsSnapshot();

        expect(mockFetch).toHaveBeenCalledWith(
          '/api/scan-proxy/v0/state/acs/force',
          expect.objectContaining({ method: 'POST' })
        );
      });
    });

    describe('fetchHoldingsState (POST /v0/holdings/state)', () => {
      it('should POST with required params', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({
          record_time: '2025-01-01T00:00:00Z',
          migration_id: 1,
          created_events: [],
        }));

        await scanApi.fetchHoldingsState({
          migration_id: 1,
          record_time: '2025-01-01T00:00:00Z',
          page_size: 100,
        });

        const body = JSON.parse(mockFetch.mock.calls[0][1].body);
        expect(body.migration_id).toBe(1);
        expect(body.page_size).toBe(100);
      });
    });

    describe('fetchTransactions (POST /v0/transactions)', () => {
      it('should POST with page_size', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ transactions: [] }));

        await scanApi.fetchTransactions({ page_size: 50, sort_order: 'desc' });

        const body = JSON.parse(mockFetch.mock.calls[0][1].body);
        expect(body.page_size).toBe(50);
        expect(body.sort_order).toBe('desc');
      });
    });

    describe('fetchRoundTotals (POST /v0/round-totals)', () => {
      it('should POST with round range', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ entries: [] }));

        await scanApi.fetchRoundTotals({ start_round: 100, end_round: 150 });

        const body = JSON.parse(mockFetch.mock.calls[0][1].body);
        expect(body.start_round).toBe(100);
        expect(body.end_round).toBe(150);
      });
    });

    describe('fetchActivities (POST /v0/activities)', () => {
      it('should POST with page_size', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ activities: [] }));

        await scanApi.fetchActivities({ page_size: 100 });

        const body = JSON.parse(mockFetch.mock.calls[0][1].body);
        expect(body.page_size).toBe(100);
      });
    });

    describe('fetchRoundPartyTotals (POST /v0/round-party-totals)', () => {
      it('should POST with round range', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ entries: [] }));

        await scanApi.fetchRoundPartyTotals({ start_round: 50, end_round: 100 });

        const body = JSON.parse(mockFetch.mock.calls[0][1].body);
        expect(body.start_round).toBe(50);
        expect(body.end_round).toBe(100);
      });
    });

    describe('fetchUpdatesV0 (POST /v0/updates)', () => {
      it('should POST with lossless option', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ transactions: [] }));

        await scanApi.fetchUpdatesV0({ page_size: 50, lossless: true });

        const body = JSON.parse(mockFetch.mock.calls[0][1].body);
        expect(body.lossless).toBe(true);
      });
    });

    describe('fetchBackfillImportUpdates (POST /v0/backfilling/import-updates)', () => {
      it('should POST with required params', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ transactions: [] }));

        await scanApi.fetchBackfillImportUpdates({
          migration_id: 1,
          limit: 100,
        });

        const body = JSON.parse(mockFetch.mock.calls[0][1].body);
        expect(body.migration_id).toBe(1);
        expect(body.limit).toBe(100);
      });
    });
  });

  /* ==========================================================
   *  GET ENDPOINTS
   * ========================================================== */
  
  describe('GET endpoints', () => {
    describe('fetchDsoInfo (GET /v0/dso)', () => {
      it('should use GET method without body', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({
          sv_user: 'test-sv',
          dso_party_id: 'dso-party-1',
        }));

        await scanApi.fetchDsoInfo();

        expect(mockFetch).toHaveBeenCalledWith(
          '/api/scan-proxy/v0/dso',
          expect.objectContaining({
            method: 'GET',
            headers: { Accept: 'application/json' },
          })
        );
        // GET requests should not have a body
        expect(mockFetch.mock.calls[0][1].body).toBeUndefined();
      });
    });

    describe('fetchClosedRounds (GET /v0/closed-rounds)', () => {
      it('should use GET method', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ rounds: [] }));

        await scanApi.fetchClosedRounds();

        expect(mockFetch).toHaveBeenCalledWith(
          '/api/scan-proxy/v0/closed-rounds',
          expect.objectContaining({ method: 'GET' })
        );
      });
    });

    describe('fetchScans (GET /v0/scans)', () => {
      it('should use GET method', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ scans: [] }));

        await scanApi.fetchScans();

        expect(mockFetch).toHaveBeenCalledWith(
          '/api/scan-proxy/v0/scans',
          expect.objectContaining({ method: 'GET' })
        );
      });
    });

    describe('fetchValidatorLicenses (GET /v0/admin/validator/licenses)', () => {
      it('should include query params', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ validator_licenses: [] }));

        await scanApi.fetchValidatorLicenses(50, 100);

        expect(mockFetch).toHaveBeenCalledWith(
          '/api/scan-proxy/v0/admin/validator/licenses?limit=100&after=50',
          expect.objectContaining({ method: 'GET' })
        );
      });

      it('should omit after param when not provided', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ validator_licenses: [] }));

        await scanApi.fetchValidatorLicenses(undefined, 50);

        const url = mockFetch.mock.calls[0][0];
        expect(url).toContain('limit=50');
        expect(url).not.toContain('after=');
      });
    });

    describe('fetchDsoSequencers (GET /v0/dso-sequencers)', () => {
      it('should use GET method', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ domainSequencers: [] }));

        await scanApi.fetchDsoSequencers();

        expect(mockFetch).toHaveBeenCalledWith(
          '/api/scan-proxy/v0/dso-sequencers',
          expect.objectContaining({ method: 'GET' })
        );
      });
    });

    describe('fetchParticipantId (GET /v0/domains/{domain_id}/parties/{party_id}/participant-id)', () => {
      it('should encode path parameters', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ participant_id: 'participant-1' }));

        await scanApi.fetchParticipantId('domain::1220abc', 'party::9876def');

        const url = mockFetch.mock.calls[0][0];
        expect(url).toContain('domain%3A%3A1220abc');
        expect(url).toContain('party%3A%3A9876def');
      });
    });

    describe('fetchTrafficStatus (GET /v0/domains/{domain_id}/members/{member_id}/traffic-status)', () => {
      it('should encode path parameters', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({
          traffic_status: {
            actual: { total_consumed: 100, total_limit: 1000 },
            target: { total_purchased: 500 },
          },
        }));

        await scanApi.fetchTrafficStatus('domain-1', 'PAR::member-1');

        const url = mockFetch.mock.calls[0][0];
        expect(url).toContain('PAR%3A%3Amember-1');
      });
    });

    describe('fetchAcsSnapshotTimestamp (GET /v0/state/acs/snapshot-timestamp)', () => {
      it('should include query params', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ record_time: '2025-01-01T00:00:00Z' }));

        await scanApi.fetchAcsSnapshotTimestamp('2025-01-01T00:00:00Z', 1);

        const url = mockFetch.mock.calls[0][0];
        expect(url).toContain('before=2025-01-01T00%3A00%3A00Z');
        expect(url).toContain('migration_id=1');
      });
    });

    describe('fetchAcsSnapshotTimestampAfter (GET /v0/state/acs/snapshot-timestamp-after)', () => {
      it('should include query params', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ record_time: '2025-01-01T00:00:00Z' }));

        await scanApi.fetchAcsSnapshotTimestampAfter('2025-01-01T00:00:00Z', 1);

        const url = mockFetch.mock.calls[0][0];
        expect(url).toContain('after=2025-01-01T00%3A00%3A00Z');
        expect(url).toContain('migration_id=1');
      });
    });

    describe('fetchAnsEntries (GET /v0/ans-entries)', () => {
      it('should include page_size param', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ entries: [] }));

        await scanApi.fetchAnsEntries('test', 50);

        const url = mockFetch.mock.calls[0][0];
        expect(url).toContain('page_size=50');
        expect(url).toContain('name_prefix=test');
      });

      it('should work without name_prefix', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ entries: [] }));

        await scanApi.fetchAnsEntries();

        const url = mockFetch.mock.calls[0][0];
        expect(url).toContain('page_size=100');
        expect(url).not.toContain('name_prefix');
      });
    });

    describe('fetchAnsEntryByParty (GET /v0/ans-entries/by-party/{party})', () => {
      it('should encode party ID in path', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({
          entry: { name: 'test', user: 'party-1' },
        }));

        await scanApi.fetchAnsEntryByParty('party::1220abc');

        const url = mockFetch.mock.calls[0][0];
        expect(url).toContain('party%3A%3A1220abc');
      });
    });

    describe('fetchAnsEntryByName (GET /v0/ans-entries/by-name/{name})', () => {
      it('should encode name in path', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({
          entry: { name: 'test.cns', user: 'party-1' },
        }));

        await scanApi.fetchAnsEntryByName('test.cns');

        const url = mockFetch.mock.calls[0][0];
        expect(url).toContain('test.cns');
      });
    });

    describe('fetchDsoPartyId (GET /v0/dso-party-id)', () => {
      it('should use GET method', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ dso_party_id: 'dso-party-1' }));

        await scanApi.fetchDsoPartyId();

        expect(mockFetch).toHaveBeenCalledWith(
          '/api/scan-proxy/v0/dso-party-id',
          expect.objectContaining({ method: 'GET' })
        );
      });
    });

    describe('fetchFeaturedApps (GET /v0/featured-apps)', () => {
      it('should use GET method', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ featured_apps: [] }));

        await scanApi.fetchFeaturedApps();

        expect(mockFetch).toHaveBeenCalledWith(
          '/api/scan-proxy/v0/featured-apps',
          expect.objectContaining({ method: 'GET' })
        );
      });
    });

    describe('fetchFeaturedApp (GET /v0/featured-apps/{provider_party_id})', () => {
      it('should encode provider ID in path', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ featured_app_right: null }));

        await scanApi.fetchFeaturedApp('provider::1220abc');

        const url = mockFetch.mock.calls[0][0];
        expect(url).toContain('provider%3A%3A1220abc');
      });
    });

    describe('fetchTopValidatorsByFaucets (GET /v0/top-validators-by-validator-faucets)', () => {
      it('should include limit param', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ validatorsByReceivedFaucets: [] }));

        await scanApi.fetchTopValidatorsByFaucets(20);

        const url = mockFetch.mock.calls[0][0];
        expect(url).toContain('limit=20');
      });
    });

    describe('fetchValidatorLiveness (GET /v0/validators/validator-faucets)', () => {
      it('should append repeated validator_ids params', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ validatorsReceivedFaucets: [] }));

        await scanApi.fetchValidatorLiveness(['validator-1', 'validator-2']);

        const url = mockFetch.mock.calls[0][0];
        expect(url).toContain('validator_ids=validator-1');
        expect(url).toContain('validator_ids=validator-2');
      });
    });

    describe('fetchTransferPreapprovalByParty (GET /v0/transfer-preapprovals/by-party/{party})', () => {
      it('should encode party in path', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ transfer_preapproval: {} }));

        await scanApi.fetchTransferPreapprovalByParty('party::abc');

        const url = mockFetch.mock.calls[0][0];
        expect(url).toContain('party%3A%3Aabc');
      });
    });

    describe('fetchTransferCommandCounter (GET /v0/transfer-command-counter/{party})', () => {
      it('should encode party in path', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ transfer_command_counter: {} }));

        await scanApi.fetchTransferCommandCounter('party::abc');

        const url = mockFetch.mock.calls[0][0];
        expect(url).toContain('party%3A%3Aabc');
      });
    });

    describe('fetchTransferCommandStatus (GET /v0/transfer-command/status)', () => {
      it('should include query params', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ transfer_commands_by_contract_id: {} }));

        await scanApi.fetchTransferCommandStatus('sender-party', 5);

        const url = mockFetch.mock.calls[0][0];
        expect(url).toContain('sender=sender-party');
        expect(url).toContain('nonce=5');
      });
    });

    describe('fetchMigrationSchedule (GET /v0/migrations/schedule)', () => {
      it('should use GET method', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({
          time: '2025-01-01T00:00:00Z',
          migration_id: 2,
        }));

        await scanApi.fetchMigrationSchedule();

        expect(mockFetch).toHaveBeenCalledWith(
          '/api/scan-proxy/v0/migrations/schedule',
          expect.objectContaining({ method: 'GET' })
        );
      });
    });

    describe('fetchSpliceInstanceNames (GET /v0/splice-instance-names)', () => {
      it('should use GET method', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({
          network_name: 'TestNet',
          amulet_name: 'CC',
        }));

        await scanApi.fetchSpliceInstanceNames();

        expect(mockFetch).toHaveBeenCalledWith(
          '/api/scan-proxy/v0/splice-instance-names',
          expect.objectContaining({ method: 'GET' })
        );
      });
    });

    describe('fetchActiveVoteRequests (GET /v0/admin/sv/voterequests)', () => {
      it('should use GET method', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ dso_rules_vote_requests: [] }));

        await scanApi.fetchActiveVoteRequests();

        expect(mockFetch).toHaveBeenCalledWith(
          '/api/scan-proxy/v0/admin/sv/voterequests',
          expect.objectContaining({ method: 'GET' })
        );
      });
    });

    describe('fetchVoteRequestById (GET /v0/voterequests/{vote_request_contract_id})', () => {
      it('should encode contract ID in path', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ dso_rules_vote_request: {} }));

        await scanApi.fetchVoteRequestById('contract::123');

        const url = mockFetch.mock.calls[0][0];
        expect(url).toContain('contract%3A%3A123');
      });
    });

    describe('fetchUnclaimedDevFundCoupons (GET /v0/unclaimed-development-fund-coupons)', () => {
      it('should use GET method', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ 'unclaimed-development-fund-coupons': [] }));

        await scanApi.fetchUnclaimedDevFundCoupons();

        expect(mockFetch).toHaveBeenCalledWith(
          '/api/scan-proxy/v0/unclaimed-development-fund-coupons',
          expect.objectContaining({ method: 'GET' })
        );
      });
    });

    describe('fetchBackfillStatus (GET /v0/backfilling/status)', () => {
      it('should use GET method', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ complete: true }));

        await scanApi.fetchBackfillStatus();

        expect(mockFetch).toHaveBeenCalledWith(
          '/api/scan-proxy/v0/backfilling/status',
          expect.objectContaining({ method: 'GET' })
        );
      });
    });

    describe('fetchFeatureSupport (GET /v0/feature-support)', () => {
      it('should use GET method', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ no_holding_fees_on_transfers: true }));

        await scanApi.fetchFeatureSupport();

        expect(mockFetch).toHaveBeenCalledWith(
          '/api/scan-proxy/v0/feature-support',
          expect.objectContaining({ method: 'GET' })
        );
      });
    });

    describe('fetchUpdateByIdV2 (GET /v2/updates/{update_id})', () => {
      it('should encode update_id in path', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ update_id: 'update-1' }));

        await scanApi.fetchUpdateByIdV2('update::abc');

        const url = mockFetch.mock.calls[0][0];
        expect(url).toContain('update%3A%3Aabc');
      });

      it('should include daml_value_encoding param', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ update_id: 'update-1' }));

        await scanApi.fetchUpdateByIdV2('update-1', 'compact_json');

        const url = mockFetch.mock.calls[0][0];
        expect(url).toContain('daml_value_encoding=compact_json');
      });
    });

    describe('fetchEventById (GET /v0/events/{update_id})', () => {
      it('should encode update_id in path', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ update: {}, verdict: {} }));

        await scanApi.fetchEventById('event::123');

        const url = mockFetch.mock.calls[0][0];
        expect(url).toContain('event%3A%3A123');
      });
    });

    describe('fetchAcsSnapshot (GET /v0/acs/{party})', () => {
      it('should encode party in path', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ acs_snapshot: '' }));

        await scanApi.fetchAcsSnapshot('party::abc');

        const url = mockFetch.mock.calls[0][0];
        expect(url).toContain('party%3A%3Aabc');
      });

      it('should include record_time param when provided', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ acs_snapshot: '' }));

        await scanApi.fetchAcsSnapshot('party-1', '2025-01-01T00:00:00Z');

        const url = mockFetch.mock.calls[0][0];
        expect(url).toContain('record_time=2025-01-01T00%3A00%3A00Z');
      });
    });

    describe('fetchLatestRound (GET /v0/round-of-latest-data)', () => {
      it('should use GET method', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({
          round: 100,
          effectiveAt: '2025-01-01T00:00:00Z',
        }));

        await scanApi.fetchLatestRound();

        expect(mockFetch).toHaveBeenCalledWith(
          '/api/scan-proxy/v0/round-of-latest-data',
          expect.objectContaining({ method: 'GET' })
        );
      });
    });

    describe('fetchAggregatedRounds (GET /v0/aggregated-rounds)', () => {
      it('should use GET method', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ start: 1, end: 100 }));

        await scanApi.fetchAggregatedRounds();

        expect(mockFetch).toHaveBeenCalledWith(
          '/api/scan-proxy/v0/aggregated-rounds',
          expect.objectContaining({ method: 'GET' })
        );
      });
    });

    describe('fetchAmuletConfigForRound (GET /v0/amulet-config-for-round)', () => {
      it('should include round param', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ amulet_config: {} }));

        await scanApi.fetchAmuletConfigForRound(50);

        const url = mockFetch.mock.calls[0][0];
        expect(url).toContain('round=50');
      });
    });

    describe('fetchRewardsCollected (GET /v0/rewards-collected)', () => {
      it('should work without round param', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ amount: '1000' }));

        await scanApi.fetchRewardsCollected();

        expect(mockFetch).toHaveBeenCalledWith(
          '/api/scan-proxy/v0/rewards-collected',
          expect.objectContaining({ method: 'GET' })
        );
      });

      it('should include round param when provided', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ amount: '1000' }));

        await scanApi.fetchRewardsCollected(50);

        const url = mockFetch.mock.calls[0][0];
        expect(url).toContain('round=50');
      });
    });

    describe('fetchTopValidatorsByRewards (GET /v0/top-validators-by-validator-rewards)', () => {
      it('should include round and limit params', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ validatorsAndRewards: [] }));

        await scanApi.fetchTopValidatorsByRewards(100, 20);

        const url = mockFetch.mock.calls[0][0];
        expect(url).toContain('round=100');
        expect(url).toContain('limit=20');
      });
    });

    describe('fetchTopValidatorsByPurchasedTraffic (GET /v0/top-validators-by-purchased-traffic)', () => {
      it('should include round and limit params', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ validatorsByPurchasedTraffic: [] }));

        await scanApi.fetchTopValidatorsByPurchasedTraffic(100, 20);

        const url = mockFetch.mock.calls[0][0];
        expect(url).toContain('round=100');
        expect(url).toContain('limit=20');
      });
    });

    describe('fetchAmuletPriceVotes (GET /v0/amulet-price/votes)', () => {
      it('should use GET method', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ amulet_price_votes: [] }));

        await scanApi.fetchAmuletPriceVotes();

        expect(mockFetch).toHaveBeenCalledWith(
          '/api/scan-proxy/v0/amulet-price/votes',
          expect.objectContaining({ method: 'GET' })
        );
      });
    });

    describe('fetchTopProviders (GET /v0/top-providers-by-app-rewards)', () => {
      it('should fetch latest round first then use it in request', async () => {
        // First call for fetchLatestRound
        mockFetch.mockResolvedValueOnce(mockSuccess({ round: 100, effectiveAt: '2025-01-01T00:00:00Z' }));
        // Second call for the actual endpoint
        mockFetch.mockResolvedValueOnce(mockSuccess({ providersAndRewards: [] }));

        await scanApi.fetchTopProviders(50);

        // Second call should have round param from first call
        const url = mockFetch.mock.calls[1][0];
        expect(url).toContain('round=100');
        expect(url).toContain('limit=50');
      });
    });

    describe('fetchSynchronizerIdentities (GET /v0/synchronizer-identities/{domain_id_prefix})', () => {
      it('should encode domain_id_prefix in path', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ sequencer_id: 'seq-1' }));

        await scanApi.fetchSynchronizerIdentities('domain::prefix');

        const url = mockFetch.mock.calls[0][0];
        expect(url).toContain('domain%3A%3Aprefix');
      });
    });

    describe('fetchSynchronizerBootstrappingTransactions (GET /v0/synchronizer-bootstrapping-transactions/{domain_id_prefix})', () => {
      it('should encode domain_id_prefix in path', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ domain_parameters: '' }));

        await scanApi.fetchSynchronizerBootstrappingTransactions('domain::prefix');

        const url = mockFetch.mock.calls[0][0];
        expect(url).toContain('domain%3A%3Aprefix');
      });
    });

    describe('fetchUpdateByIdV0 (GET /v0/updates/{update_id})', () => {
      it('should encode update_id in path', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ update_id: 'update-1' }));

        await scanApi.fetchUpdateByIdV0('update::abc');

        const url = mockFetch.mock.calls[0][0];
        expect(url).toContain('update%3A%3Aabc');
      });

      it('should include lossless param when true', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ update_id: 'update-1' }));

        await scanApi.fetchUpdateByIdV0('update-1', true);

        const url = mockFetch.mock.calls[0][0];
        expect(url).toContain('lossless=true');
      });
    });
  });

  /* ==========================================================
   *  ERROR HANDLING
   * ========================================================== */
  
  describe('Error handling', () => {
    it('should throw descriptive error on POST failure', async () => {
      mockFetch.mockResolvedValueOnce(mockError(400, 'Invalid request body'));

      await expect(scanApi.fetchUpdates({ page_size: -1 })).rejects.toThrow(
        /SCAN POST \/v2\/updates failed \(400\)/
      );
    });

    it('should throw descriptive error on GET failure', async () => {
      mockFetch.mockResolvedValueOnce(mockError(404, 'Not found'));

      await expect(scanApi.fetchDsoInfo()).rejects.toThrow(
        /SCAN GET \/v0\/dso failed \(404\)/
      );
    });

    it('should propagate network errors', async () => {
      mockFetch.mockRejectedValueOnce(new Error('Network unreachable'));

      await expect(scanApi.fetchDsoInfo()).rejects.toThrow('Network unreachable');
    });

    it('should handle 500 Internal Server Error', async () => {
      mockFetch.mockResolvedValueOnce(mockError(500, 'Internal server error'));

      await expect(scanApi.fetchClosedRounds()).rejects.toThrow(/failed \(500\)/);
    });

    it('should handle 503 Service Unavailable', async () => {
      mockFetch.mockResolvedValueOnce(mockError(503, 'Service unavailable'));

      await expect(scanApi.fetchBackfillStatus()).rejects.toThrow(/failed \(503\)/);
    });
  });

  /* ==========================================================
   *  COMPOSITE METHODS
   * ========================================================== */
  
  describe('Composite methods', () => {
    describe('fetchTopValidators', () => {
      it('should map faucets response to rewards format', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({
          validatorsByReceivedFaucets: [
            { validator: 'v1', numRoundsCollected: 50, firstCollectedInRound: 1 },
            { validator: 'v2', numRoundsCollected: 30, firstCollectedInRound: 10 },
          ],
        }));

        const result = await scanApi.fetchTopValidators();

        expect(result.validatorsAndRewards).toHaveLength(2);
        expect(result.validatorsAndRewards[0].provider).toBe('v1');
        expect(result.validatorsAndRewards[0].rewards).toBe('50');
      });
    });

    describe('fetchTotalBalance', () => {
      it('should fetch latest round then round totals', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ round: 100, effectiveAt: '2025-01-01T00:00:00Z' }));
        mockFetch.mockResolvedValueOnce(mockSuccess({
          entries: [{ total_amulet_balance: '1000000' }],
        }));

        const result = await scanApi.fetchTotalBalance();

        expect(result.total_balance).toBe('1000000');
      });

      it('should throw if no round totals returned', async () => {
        mockFetch.mockResolvedValueOnce(mockSuccess({ round: 100, effectiveAt: '2025-01-01T00:00:00Z' }));
        mockFetch.mockResolvedValueOnce(mockSuccess({ entries: [] }));

        await expect(scanApi.fetchTotalBalance()).rejects.toThrow('No round totals for latest round');
      });
    });
  });

  /* ==========================================================
   *  URL ENCODING
   * ========================================================== */
  
  describe('URL encoding', () => {
    it('should properly encode colons in party IDs', async () => {
      mockFetch.mockResolvedValueOnce(mockSuccess({ entry: {} }));

      await scanApi.fetchAnsEntryByParty('PAR::1220abcdef::fingerprint');

      const url = mockFetch.mock.calls[0][0];
      expect(url).toContain('PAR%3A%3A1220abcdef%3A%3Afingerprint');
    });

    it('should properly encode special characters in names', async () => {
      mockFetch.mockResolvedValueOnce(mockSuccess({ entry: {} }));

      await scanApi.fetchAnsEntryByName('test name+special');

      const url = mockFetch.mock.calls[0][0];
      expect(url).toContain('test%20name%2Bspecial');
    });

    it('should properly encode slashes in domain IDs', async () => {
      mockFetch.mockResolvedValueOnce(mockSuccess({ participant_id: 'p1' }));

      await scanApi.fetchParticipantId('domain/with/slashes', 'party-1');

      const url = mockFetch.mock.calls[0][0];
      expect(url).toContain('domain%2Fwith%2Fslashes');
    });
  });
});
