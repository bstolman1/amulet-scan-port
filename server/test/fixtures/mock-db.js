/**
 * Mock database connection for testing
 * Provides stubbed versions of db methods
 */

import { mockEvents, mockGovernanceEvents } from './mock-data.js';

// Simulated data path
export const DATA_PATH = '/mock/data/path';

// Track query calls for assertions
const queryCalls = [];

/**
 * Mock safeQuery implementation
 * Returns appropriate mock data based on query patterns
 */
export async function safeQuery(sql) {
  queryCalls.push(sql);
  
  // Stats overview query
  if (sql.includes('COUNT(*)') && sql.includes('unique_contracts')) {
    return [{
      total_events: 15000n,
      unique_contracts: 5000n,
      unique_templates: 25n,
      earliest_event: '2024-01-01T00:00:00.000Z',
      latest_event: '2025-01-10T14:00:00.000Z',
    }];
  }
  
  // Daily stats query
  if (sql.includes("DATE_TRUNC('day'")) {
    return [
      { date: '2025-01-10', event_count: 500n, contract_count: 150n },
      { date: '2025-01-09', event_count: 480n, contract_count: 145n },
    ];
  }
  
  // By type query
  if (sql.includes('GROUP BY event_type')) {
    return [
      { event_type: 'created', count: 10000n },
      { event_type: 'archived', count: 5000n },
    ];
  }
  
  // By template query
  if (sql.includes('GROUP BY template_id')) {
    return [
      { template_id: 'Splice.Amulet:Amulet', event_count: 5000n, contract_count: 2000n },
      { template_id: 'Splice.DsoRules:VoteRequest', event_count: 150n, contract_count: 150n },
    ];
  }
  
  // Governance events query
  if (sql.includes('VoteRequest') || sql.includes('Confirmation') || sql.includes('DsoRules')) {
    return mockGovernanceEvents;
  }
  
  // Default: return mock events
  return mockEvents;
}

/**
 * Mock hasFileType implementation
 */
export function hasFileType(prefix, extension) {
  // Simulate having parquet files
  return extension === '.parquet';
}

/**
 * Get recorded query calls (for test assertions)
 */
export function getQueryCalls() {
  return [...queryCalls];
}

/**
 * Clear recorded query calls
 */
export function clearQueryCalls() {
  queryCalls.length = 0;
}

export default {
  DATA_PATH,
  safeQuery,
  hasFileType,
  getQueryCalls,
  clearQueryCalls,
};
