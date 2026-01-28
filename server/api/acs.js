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
 * GET /acs/latest
 * Proxies Scan API "round-of-latest-data"
 */
router.get('/latest', async (_req, res) => {
  try {
    if (USE_DUCKDB_ACS) {
      // DuckDB path would go here when re-enabled
      throw new Error('DuckDB ACS path disabled - re-enable by setting USE_DUCKDB_ACS = true');
    }

    const response = await fetch(`${SCAN_API_BASE}/round-of-latest-data`, {
      headers: { Accept: 'application/json' },
    });

    if (!response.ok) {
      throw new Error(`Scan API error: ${response.status}`);
    }

    const data = await response.json();
    res.json(data);
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
router.get('/status', (_req, res) => {
  res.json({
    source: USE_DUCKDB_ACS ? 'duckdb' : 'scan-api',
    duckdb_enabled: USE_DUCKDB_ACS,
    scan_api_base: SCAN_API_BASE,
  });
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

    // Use round-of-latest-data for basic stats
    const response = await fetch(`${SCAN_API_BASE}/round-of-latest-data`, {
      headers: { Accept: 'application/json' },
    });

    if (!response.ok) {
      throw new Error(`Scan API error: ${response.status}`);
    }

    const data = await response.json();
    res.json({
      source: 'scan-api',
      round: data.round || data,
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
  if (USE_DUCKDB_ACS) {
    // Would query DuckDB for template list
    return res.json({ data: [], count: 0, source: 'duckdb-disabled' });
  }
  
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

  --- ORIGINAL DUCKDB IMPLEMENTATION ---

// Cache TTL for different endpoints
const CACHE_TTL = {
  RICH_LIST: 5 * 60 * 1000,     // 5 minutes
  SUPPLY: 5 * 60 * 1000,        // 5 minutes  
  MINING_ROUNDS: 5 * 60 * 1000, // 5 minutes
  ALLOCATIONS: 5 * 60 * 1000,   // 5 minutes
  TEMPLATES: 10 * 60 * 1000,    // 10 minutes
  STATS: 10 * 60 * 1000,        // 10 minutes
  SNAPSHOTS: 10 * 60 * 1000,    // 10 minutes
};

// Helper to convert BigInt to Number for JSON serialization
function serializeBigInt(obj) {
  return JSON.parse(JSON.stringify(obj, (_, v) => typeof v === 'bigint' ? Number(v) : v));
}

// ACS data path - use the centralized path from duckdb connection
const ACS_DATA_PATH = db.ACS_DATA_PATH;

// Find ACS files and return their paths (supports both JSONL and Parquet)
function findACSFiles() {
  try {
    if (!fs.existsSync(ACS_DATA_PATH)) {
      console.log(`[ACS] findACSFiles: ACS_DATA_PATH does not exist: ${ACS_DATA_PATH}`);
      return { jsonl: [], parquet: [] };
    }
    const allFiles = fs.readdirSync(ACS_DATA_PATH, { recursive: true });
    const jsonlFiles = allFiles
      .map(f => String(f))
      .filter(f => f.endsWith('.jsonl') || f.endsWith('.jsonl.gz') || f.endsWith('.jsonl.zst'))
      .map(f => path.join(ACS_DATA_PATH, f).replace(/\\/g, '/')); // Normalize for DuckDB
    
    const parquetFiles = allFiles
      .map(f => String(f))
      .filter(f => f.endsWith('.parquet'))
      .map(f => path.join(ACS_DATA_PATH, f).replace(/\\/g, '/')); // Normalize for DuckDB
    
    if (jsonlFiles.length > 0 || parquetFiles.length > 0) {
      console.log(`[ACS] findACSFiles: Found ${parquetFiles.length} parquet, ${jsonlFiles.length} jsonl files`);
    }
    return { jsonl: jsonlFiles, parquet: parquetFiles };
  } catch (err) {
    console.error(`[ACS] findACSFiles error: ${err.message}`);
    return { jsonl: [], parquet: [] };
  }
}

// ... rest of DuckDB implementation preserved in original file ...
// See git history or backup for full implementation

*/
