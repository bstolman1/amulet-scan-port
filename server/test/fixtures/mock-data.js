/**
 * Mock data fixtures for API integration tests
 */

// Sample event records
export const mockEvents = [
  {
    event_id: 'evt-001',
    update_id: 'upd-001',
    event_type: 'created',
    contract_id: '00abc123::contract1',
    template_id: 'Splice.Amulet:Amulet',
    package_name: 'splice-amulet',
    timestamp: '2025-01-10T12:00:00.000Z',
    effective_at: '2025-01-10T12:00:00.000Z',
    signatories: ['party1'],
    observers: ['party2'],
    payload: { amount: { amount: 1000 } },
  },
  {
    event_id: 'evt-002',
    update_id: 'upd-002',
    event_type: 'archived',
    contract_id: '00abc123::contract2',
    template_id: 'Splice.DsoRules:VoteRequest',
    package_name: 'splice-dso',
    timestamp: '2025-01-10T13:00:00.000Z',
    effective_at: '2025-01-10T13:00:00.000Z',
    signatories: ['party1', 'party2'],
    observers: [],
    payload: { action: { tag: 'ARC_DsoRules' } },
  },
  {
    event_id: 'evt-003',
    update_id: 'upd-003',
    event_type: 'created',
    contract_id: '00def456::contract3',
    template_id: 'Splice.Amulet:BurnMintSummary',
    package_name: 'splice-amulet',
    timestamp: '2025-01-10T14:00:00.000Z',
    effective_at: '2025-01-10T14:00:00.000Z',
    signatories: ['party3'],
    observers: ['party1'],
    payload: { burnAmount: { amount: 500 } },
  },
];

// Sample governance events
export const mockGovernanceEvents = [
  {
    event_id: 'gov-001',
    event_type: 'created',
    contract_id: '00gov001::voterequest1',
    template_id: 'Splice.DsoRules:VoteRequest',
    package_name: 'splice-dso',
    timestamp: '2025-01-09T10:00:00.000Z',
    effective_at: '2025-01-09T10:00:00.000Z',
    signatories: ['dso'],
    observers: [],
    payload: {
      action: { tag: 'ARC_AmuletRules', value: {} },
      requester: 'party1',
      reason: { url: 'https://example.com', body: 'Test reason' },
    },
  },
];

// Sample stats overview response
export const mockStatsOverview = {
  total_events: 15000,
  unique_contracts: 5000,
  unique_templates: 25,
  earliest_event: '2024-01-01T00:00:00.000Z',
  latest_event: '2025-01-10T14:00:00.000Z',
  data_source: 'binary',
};

// Sample daily stats
export const mockDailyStats = {
  data: [
    { date: '2025-01-10', event_count: 500, contract_count: 150 },
    { date: '2025-01-09', event_count: 480, contract_count: 145 },
    { date: '2025-01-08', event_count: 520, contract_count: 160 },
  ],
};

// Sample template stats
export const mockTemplateStats = {
  data: [
    { template_id: 'Splice.Amulet:Amulet', event_count: 5000, contract_count: 2000 },
    { template_id: 'Splice.DsoRules:VoteRequest', event_count: 150, contract_count: 150 },
    { template_id: 'Splice.Amulet:BurnMintSummary', event_count: 1000, contract_count: 500 },
  ],
};

// Sample search results
export const mockSearchResults = {
  data: mockEvents,
  count: 3,
  query: { q: 'amulet', type: null, template: null, party: null },
};

// Sample party data
export const mockPartyData = {
  party_id: 'party1',
  events: mockEvents.filter(e => e.signatories?.includes('party1')),
  total_events: 2,
};

// Helper to create a mock request object
export function createMockRequest(overrides = {}) {
  return {
    params: {},
    query: {},
    body: {},
    headers: {},
    ...overrides,
  };
}

// Helper to create a mock response object
export function createMockResponse() {
  const res = {
    statusCode: 200,
    _data: null,
    status(code) {
      this.statusCode = code;
      return this;
    },
    json(data) {
      this._data = data;
      return this;
    },
    send(data) {
      this._data = data;
      return this;
    },
    getData() {
      return this._data;
    },
    getStatus() {
      return this.statusCode;
    },
  };
  return res;
}
