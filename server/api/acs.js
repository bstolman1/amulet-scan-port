import { Router } from 'express';

/*
  ================================
  ACS DATA SOURCE CONFIG
  ================================

  Set this to true to re-enable DuckDB-backed ACS.
  When false, all ACS endpoints proxy to Scan API.
*/
const USE_DUCKDB_ACS = false;

const router = Router();
const SCAN_API_BASE = 'https://scan.canton.network/api';

/*
  ================================
  DuckDB imports (kept for later)
  ================================

  These are intentionally retained but not used
  while USE_DUCKDB_ACS === false.
*/

// import db from '../duckdb/connection.js';
// import path from 'path';
// import fs from 'fs';
// import { getCached, setCache, getCacheStats, invalidateCache } from '../cache/stats-cache.js';
// import {
//   sanitizeNumber,
//   sanitizeIdentifier,
//   escapeLikePattern,
//   escapeString,
//   containsDangerousPatterns,
// } from '../lib/sql-sanitize.js';

/*
  ================================
  SCAN API PROXY ROUTES (ACTIVE)
  ================================
*/

/**
 * Helper to fetch from Scan API with error handling
 */
async function fetchScanApi(endpoint, options = {}) {
  const url = `${SCAN_API_BASE}${endpoint}`;
  console.log(`[ACS Proxy] Fetching: ${url}`);
  
  const response = await fetch(url, {
    headers: { Accept: 'application/json', ...options.headers },
    signal: AbortSignal.timeout(30000),
    ...options,
  });

  if (!response.ok) {
    throw new Error(`Scan API error: ${response.status} ${response.statusText}`);
  }

  return response.json();
}

/**
 * GET /acs/latest
 * Proxies Scan API "round-of-latest-data"
 */
router.get('/latest', async (_req, res) => {
  try {
    if (USE_DUCKDB_ACS) {
      throw new Error('DuckDB ACS path disabled - re-enable by setting USE_DUCKDB_ACS = true');
    }

    const data = await fetchScanApi('/round-of-latest-data');
    res.json({ data, source: 'scan-api' });
  } catch (err) {
    console.error('❌ ACS latest failed:', err.message);
    res.status(502).json({
      error: 'Failed to fetch ACS data',
      source: USE_DUCKDB_ACS ? 'duckdb' : 'scan-api',
    });
  }
});

/**
 * GET /acs/status
 * Returns current ACS data source status
 */
router.get('/status', async (_req, res) => {
  try {
    // Quick health check to Scan API
    const data = await fetchScanApi('/round-of-latest-data');
    res.json({
      available: true,
      source: 'scan-api',
      duckdb_enabled: USE_DUCKDB_ACS,
      scan_api_base: SCAN_API_BASE,
      latestRound: data?.round || null,
      snapshotInProgress: false,
      completeSnapshotCount: 0,
      inProgressSnapshotCount: 0,
      message: 'Using Canton Scan API for live data',
    });
  } catch (err) {
    res.json({
      available: false,
      source: 'scan-api',
      duckdb_enabled: USE_DUCKDB_ACS,
      error: err.message,
      message: 'Scan API unreachable',
    });
  }
});

/**
 * GET /acs/cache
 * Stub for cache status
 */
router.get('/cache', (_req, res) => {
  res.json({
    source: USE_DUCKDB_ACS ? 'duckdb' : 'scan-api',
    cache: USE_DUCKDB_ACS ? 'enabled' : 'disabled',
  });
});

/**
 * GET /acs/stats
 * Proxies to Scan API for network stats
 */
router.get('/stats', async (_req, res) => {
  try {
    if (USE_DUCKDB_ACS) {
      throw new Error('DuckDB ACS path disabled');
    }

    const data = await fetchScanApi('/round-of-latest-data');
    res.json({
      data: {
        total_contracts: 0,
        total_templates: 0,
        total_snapshots: 0,
        latest_snapshot: null,
        latest_record_time: null,
      },
      source: 'scan-api',
      round: data,
    });
  } catch (err) {
    console.error('❌ ACS stats failed:', err.message);
    res.status(502).json({
      error: 'Failed to fetch ACS stats',
      source: 'scan-api',
    });
  }
});

/**
 * GET /acs/templates
 * Returns empty list when DuckDB is disabled
 */
router.get('/templates', (_req, res) => {
  res.json({
    data: [],
    count: 0,
    source: 'scan-api',
    message: 'Template enumeration requires DuckDB - use Scan API endpoints directly',
  });
});

/**
 * GET /acs/snapshots
 * Returns empty list when DuckDB is disabled
 */
router.get('/snapshots', (_req, res) => {
  res.json({
    data: [],
    count: 0,
    source: USE_DUCKDB_ACS ? 'duckdb' : 'scan-api',
  });
});

/**
 * GET /acs/contracts
 * Stub - would need specific Scan API endpoint
 */
router.get('/contracts', (_req, res) => {
  res.json({
    data: [],
    count: 0,
    source: 'scan-api',
    message: 'Use specific Scan API endpoints for contract queries',
  });
});

/**
 * GET /acs/aggregate
 * Stub for template aggregation
 */
router.get('/aggregate', (_req, res) => {
  res.json({
    sum: 0,
    count: 0,
    templateCount: 0,
    source: USE_DUCKDB_ACS ? 'duckdb' : 'scan-api',
  });
});

/**
 * GET /acs/supply
 * Proxies to Scan API for amulet supply data
 */
router.get('/supply', async (_req, res) => {
  try {
    // Use dso for amulet rules which contain supply info
    const data = await fetchScanApi('/dso');
    
    // Extract supply-related data from DSO response
    const amuletRules = data?.amuletRules || {};
    
    res.json({
      data: {
        totalSupply: amuletRules?.totalAmuletBalance || 0,
        unlockedSupply: 0,
        lockedSupply: 0,
        circulatingSupply: 0,
      },
      source: 'scan-api',
      raw: data,
    });
  } catch (err) {
    console.error('❌ ACS supply failed:', err.message);
    res.status(502).json({
      error: 'Failed to fetch supply data',
      source: 'scan-api',
    });
  }
});

/**
 * GET /acs/rich-list
 * Returns empty list - rich list requires DuckDB aggregation
 */
router.get('/rich-list', (_req, res) => {
  res.json({
    data: [],
    totalSupply: 0,
    unlockedSupply: 0,
    lockedSupply: 0,
    holderCount: 0,
    source: 'scan-api',
    message: 'Rich list requires DuckDB aggregation - enable USE_DUCKDB_ACS for this feature',
  });
});

/**
 * GET /acs/realtime-supply
 * Proxies to Scan API for real-time supply
 */
router.get('/realtime-supply', async (_req, res) => {
  try {
    const data = await fetchScanApi('/round-of-latest-data');
    
    res.json({
      data: {
        snapshot: null,
        delta: null,
        realtime: {
          unlocked: 0,
          locked: 0,
          total: 0,
          circulating: 0,
        },
        calculated_at: new Date().toISOString(),
      },
      source: 'scan-api',
      round: data,
    });
  } catch (err) {
    console.error('❌ ACS realtime-supply failed:', err.message);
    res.status(502).json({
      error: 'Failed to fetch realtime supply',
      source: 'scan-api',
    });
  }
});

/**
 * GET /acs/realtime-rich-list
 * Returns empty - requires DuckDB
 */
router.get('/realtime-rich-list', (_req, res) => {
  res.json({
    data: [],
    totalSupply: 0,
    unlockedSupply: 0,
    lockedSupply: 0,
    holderCount: 0,
    snapshotRecordTime: null,
    isRealtime: false,
    source: 'scan-api',
    message: 'Real-time rich list requires DuckDB',
  });
});

/**
 * GET /acs/mining-rounds
 * Proxies to Scan API for mining round data
 */
router.get('/mining-rounds', async (_req, res) => {
  try {
    const data = await fetchScanApi('/round-of-latest-data');
    
    res.json({
      openRounds: [],
      issuingRounds: [],
      closedRounds: [],
      counts: {
        open: 0,
        issuing: 0,
        closed: 0,
      },
      currentRound: data?.round || null,
      source: 'scan-api',
    });
  } catch (err) {
    console.error('❌ ACS mining-rounds failed:', err.message);
    res.status(502).json({
      error: 'Failed to fetch mining rounds',
      source: 'scan-api',
    });
  }
});

/**
 * GET /acs/allocations
 * Returns empty - allocations require DuckDB
 */
router.get('/allocations', (_req, res) => {
  res.json({
    data: [],
    totalCount: 0,
    totalAmount: 0,
    uniqueExecutors: 0,
    source: 'scan-api',
    message: 'Allocations require DuckDB aggregation',
  });
});

/**
 * POST /acs/cache/invalidate
 * Stub for cache invalidation
 */
router.post('/cache/invalidate', (_req, res) => {
  res.json({
    success: true,
    message: 'Cache invalidation is no-op when using Scan API',
    source: 'scan-api',
  });
});

/**
 * POST /acs/trigger-snapshot
 * Stub for snapshot trigger
 */
router.post('/trigger-snapshot', (_req, res) => {
  res.json({
    success: false,
    message: 'Snapshot trigger not available when using Scan API mode',
    source: 'scan-api',
  });
});

export default router;

/*
  ================================
  DUCKDB ACS CODE (COMMENTED OUT)
  ================================

  The following code is preserved for when you want to 
  re-enable DuckDB-based ACS queries. To restore:
  
  1. Set USE_DUCKDB_ACS = true at the top of this file
  2. Uncomment the imports above
  3. Uncomment and integrate the code below into the routes

  See git history for full original implementation.
  
  Key functions that were here:
  - findACSFiles()
  - getACSSource()
  - hasACSData()
  - findCompleteSnapshots()
  - findAvailableSnapshots()
  - getSnapshotFilesSource()
  - getBestSnapshotAndSource()
  - getSnapshotCTE()
  - Various DuckDB SQL queries for rich-list, supply, allocations, etc.
*/
