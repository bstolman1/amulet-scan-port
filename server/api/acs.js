import { Router } from 'express';
import db from '../duckdb/connection.js';
import path from 'path';
import fs from 'fs';
import { getCached, setCache, getCacheStats, invalidateCache } from '../cache/stats-cache.js';

const router = Router();

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

// ACS data path - data is written to DATA_PATH/acs/ by the ingest scripts
const ACS_DATA_PATH = path.resolve(db.DATA_PATH, 'acs');

// Find ACS files and return their paths
function findACSFiles() {
  try {
    if (!fs.existsSync(ACS_DATA_PATH)) return [];
    const allFiles = fs.readdirSync(ACS_DATA_PATH, { recursive: true });
    return allFiles
      .map(f => String(f))
      .filter(f => f.endsWith('.jsonl') || f.endsWith('.jsonl.gz') || f.endsWith('.jsonl.zst'))
      .map(f => path.join(ACS_DATA_PATH, f).replace(/\\/g, '/')); // Normalize for DuckDB
  } catch {
    return [];
  }
}

// Helper to get ACS source - builds query from actual files found
const getACSSource = () => {
  const files = findACSFiles();
  if (files.length === 0) {
    return `(SELECT NULL as placeholder WHERE false)`;
  }
  
  // For small file counts, use explicit list
  if (files.length <= 100) {
    const selects = files.map(f => 
      `SELECT * FROM read_json_auto('${f}', union_by_name=true, ignore_errors=true)`
    );
    return `(${selects.join(' UNION ALL ')})`;
  }
  
  // For large counts, use glob but only for file types that exist
  const hasJsonl = files.some(f => f.endsWith('.jsonl') && !f.endsWith('.jsonl.gz') && !f.endsWith('.jsonl.zst'));
  const hasGz = files.some(f => f.endsWith('.jsonl.gz'));
  const hasZst = files.some(f => f.endsWith('.jsonl.zst'));
  const acsPath = ACS_DATA_PATH.replace(/\\/g, '/');
  
  const parts = [];
  if (hasJsonl) {
    parts.push(`SELECT * FROM read_json_auto('${acsPath}/**/*.jsonl', union_by_name=true, ignore_errors=true)`);
  }
  if (hasGz) {
    parts.push(`SELECT * FROM read_json_auto('${acsPath}/**/*.jsonl.gz', union_by_name=true, ignore_errors=true)`);
  }
  if (hasZst) {
    parts.push(`SELECT * FROM read_json_auto('${acsPath}/**/*.jsonl.zst', union_by_name=true, ignore_errors=true)`);
  }
  
  return parts.length > 0 ? `(${parts.join(' UNION ALL ')})` : `(SELECT NULL as placeholder WHERE false)`;
};

// Check if ACS data exists
function hasACSData() {
  return findACSFiles().length > 0;
}

// Helper function to get CTE for latest snapshot (filters by latest migration_id first)
function getLatestSnapshotCTE(acsSource) {
  return `
    latest_migration AS (
      SELECT MAX(migration_id) as migration_id FROM ${acsSource}
    ),
    latest_snapshot AS (
      SELECT MAX(snapshot_time) as snapshot_time, (SELECT migration_id FROM latest_migration) as migration_id
      FROM ${acsSource}
      WHERE migration_id = (SELECT migration_id FROM latest_migration)
    )
  `;
}

// GET /api/acs/cache - Get cache statistics (for debugging)
router.get('/cache', (req, res) => {
  res.json(getCacheStats());
});

// POST /api/acs/cache/invalidate - Invalidate cache
router.post('/cache/invalidate', (req, res) => {
  const { prefix } = req.body || {};
  invalidateCache(prefix || 'acs:');
  res.json({ status: 'ok', message: 'Cache invalidated' });
});

// GET /api/acs/snapshots - List all available snapshots
router.get('/snapshots', async (req, res) => {
  try {
    if (!hasACSData()) {
      return res.json({ data: [], message: 'No ACS data available' });
    }

    // Check cache first
    const cacheKey = 'acs:snapshots';
    const cached = getCached(cacheKey);
    if (cached) {
      return res.json(cached);
    }

    // First, get distinct migration_ids to understand what's available
    const migrationSql = `
      SELECT DISTINCT migration_id, COUNT(*) as count
      FROM ${getACSSource()}
      GROUP BY migration_id
      ORDER BY migration_id DESC
    `;
    
    const migrations = await db.safeQuery(migrationSql);
    console.log('Available migrations:', migrations.map(m => `migration_id=${m.migration_id} (${m.count} contracts)`).join(', '));

    const sql = `
      SELECT 
        snapshot_time,
        migration_id,
        COUNT(*) as contract_count,
        COUNT(DISTINCT template_id) as template_count,
        MIN(record_time) as record_time
      FROM ${getACSSource()}
      GROUP BY snapshot_time, migration_id
      ORDER BY migration_id DESC, snapshot_time DESC
      LIMIT 50
    `;

    const rows = await db.safeQuery(sql);
    
    // Transform to match the UI's expected format
    const snapshots = rows.map((row, index) => ({
      id: `local-${row.migration_id}-${index}`,
      timestamp: row.snapshot_time,
      migration_id: row.migration_id,
      record_time: row.record_time,
      entry_count: row.contract_count,
      template_count: row.template_count,
      status: 'completed',
      source: 'local',
    }));

    console.log(`Returning ${snapshots.length} snapshots:`, snapshots.map(s => `M${s.migration_id}`).join(', '));
    const result = serializeBigInt({ data: snapshots });
    setCache(cacheKey, result, CACHE_TTL.SNAPSHOTS);
    res.json(result);
  } catch (err) {
    console.error('ACS snapshots error:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/acs/latest - Get latest snapshot summary with supply metrics
router.get('/latest', async (req, res) => {
  try {
    if (!hasACSData()) {
      return res.json({ data: null, message: 'No ACS data available' });
    }

    // Check cache
    const cacheKey = 'acs:latest';
    const cached = getCached(cacheKey);
    if (cached) {
      return res.json(cached);
    }

    // Get basic snapshot info - latest migration and latest snapshot within it
    const acsSource = getACSSource();
    const basicSql = `
      WITH ${getLatestSnapshotCTE(acsSource)}
      SELECT 
        acs.snapshot_time,
        acs.migration_id,
        COUNT(*) as contract_count,
        COUNT(DISTINCT template_id) as template_count,
        MIN(record_time) as record_time
      FROM ${acsSource} acs
      WHERE acs.migration_id = (SELECT migration_id FROM latest_migration)
        AND acs.snapshot_time = (SELECT snapshot_time FROM latest_snapshot)
      GROUP BY acs.snapshot_time, acs.migration_id
    `;

    const basicRows = await db.safeQuery(basicSql);
    
    if (basicRows.length === 0) {
      return res.json({ data: null });
    }

    const row = basicRows[0];
    const snapshotTime = row.snapshot_time;
    const migrationId = row.migration_id;

    // Calculate supply metrics from Amulet and LockedAmulet contracts
    const supplySql = `
      WITH latest_contracts AS (
        SELECT template_id, entity_name, payload
        FROM ${acsSource}
        WHERE migration_id = ${migrationId}
          AND snapshot_time = '${snapshotTime}'
      ),
      amulet_totals AS (
        SELECT 
          COALESCE(SUM(
            CAST(
              COALESCE(
                payload->>'$.amount.initialAmount',
                payload->'amount'->>'initialAmount',
                '0'
              ) AS DOUBLE
            )
          ), 0) as amulet_total
        FROM latest_contracts
        WHERE entity_name = 'Amulet' OR template_id LIKE '%:Amulet:%'
      ),
      locked_totals AS (
        SELECT 
          COALESCE(SUM(
            CAST(
              COALESCE(
                payload->>'$.amulet.amount.initialAmount',
                payload->'amulet'->'amount'->>'initialAmount',
                '0'
              ) AS DOUBLE
            )
          ), 0) as locked_total
        FROM latest_contracts
        WHERE entity_name = 'LockedAmulet' OR template_id LIKE '%:LockedAmulet:%'
      )
      SELECT 
        amulet_totals.amulet_total,
        locked_totals.locked_total
      FROM amulet_totals, locked_totals
    `;

    let amuletTotal = 0;
    let lockedTotal = 0;
    
    try {
      const supplyRows = await db.safeQuery(supplySql);
      if (supplyRows.length > 0) {
        amuletTotal = supplyRows[0].amulet_total || 0;
        lockedTotal = supplyRows[0].locked_total || 0;
      }
    } catch (supplyErr) {
      console.warn('Could not calculate supply metrics:', supplyErr.message);
    }

    const circulatingSupply = amuletTotal - lockedTotal;

    const result = serializeBigInt({
      data: {
        id: 'local-latest',
        timestamp: row.snapshot_time,
        migration_id: row.migration_id,
        record_time: row.record_time,
        entry_count: row.contract_count,
        template_count: row.template_count,
        amulet_total: amuletTotal,
        locked_total: lockedTotal,
        circulating_supply: circulatingSupply,
        status: 'completed',
        source: 'local',
      }
    });
    
    setCache(cacheKey, result, CACHE_TTL.SUPPLY);
    res.json(result);
  } catch (err) {
    console.error('ACS latest error:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/acs/templates - Get template statistics from latest snapshot
router.get('/templates', async (req, res) => {
  try {
    if (!hasACSData()) {
      return res.json({ data: [] });
    }

    const limit = Math.min(parseInt(req.query.limit) || 100, 500);
    
    // Check cache
    const cacheKey = `acs:templates:${limit}`;
    const cached = getCached(cacheKey);
    if (cached) {
      return res.json(cached);
    }

    const acsSource = getACSSource();
    const sql = `
      WITH ${getLatestSnapshotCTE(acsSource)}
      SELECT 
        template_id,
        entity_name,
        module_name,
        COUNT(*) as contract_count,
        COUNT(DISTINCT contract_id) as unique_contracts
      FROM ${acsSource} acs
      WHERE acs.migration_id = (SELECT migration_id FROM latest_migration)
        AND acs.snapshot_time = (SELECT snapshot_time FROM latest_snapshot)
      GROUP BY template_id, entity_name, module_name
      ORDER BY contract_count DESC
      LIMIT ${limit}
    `;

    const rows = await db.safeQuery(sql);
    const result = serializeBigInt({ data: rows });
    setCache(cacheKey, result, CACHE_TTL.TEMPLATES);
    res.json(result);
  } catch (err) {
    console.error('ACS templates error:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/acs/contracts - Get contracts by template with parsed payload
// Note: Not cached because it depends on template/entity query params and is paginated
router.get('/contracts', async (req, res) => {
  try {
    if (!hasACSData()) {
      console.log('[ACS] No ACS data available');
      return res.json({ data: [] });
    }

    const { template, entity } = req.query;
    const limit = Math.min(parseInt(req.query.limit) || 100, 10000);
    const offset = parseInt(req.query.offset) || 0;

    console.log(`[ACS] Contracts request: template=${template}, entity=${entity}, limit=${limit}`);

    let whereClause = '1=1';
    if (template) {
      whereClause = `template_id LIKE '%${template}%'`;
    } else if (entity) {
      // Match by entity_name OR template_id containing the entity name
      whereClause = `(entity_name = '${entity}' OR template_id LIKE '%:${entity}:%' OR template_id LIKE '%:${entity}')`;
    }

    console.log(`[ACS] WHERE clause: ${whereClause}`);

    const acsSource = getACSSource();
    const sql = `
      WITH ${getLatestSnapshotCTE(acsSource)}
      SELECT 
        contract_id,
        template_id,
        entity_name,
        module_name,
        signatories,
        observers,
        payload,
        record_time,
        snapshot_time
      FROM ${acsSource} acs
      WHERE acs.migration_id = (SELECT migration_id FROM latest_migration)
        AND acs.snapshot_time = (SELECT snapshot_time FROM latest_snapshot)
        AND ${whereClause}
      ORDER BY contract_id
      LIMIT ${limit}
      OFFSET ${offset}
    `;

    const rows = await db.safeQuery(sql);
    console.log(`[ACS] Found ${rows.length} contracts for entity=${entity}`);
    
    // Parse payload JSON and flatten for frontend consumption
    const parsedRows = rows.map(row => {
      let parsedPayload = row.payload;
      if (typeof row.payload === 'string') {
        try {
          parsedPayload = JSON.parse(row.payload);
        } catch {
          // Keep as string if parsing fails
        }
      }
      
      // Return the parsed payload fields at the top level for frontend compatibility
      return {
        ...row,
        ...parsedPayload, // Spread payload fields (owner, amount, amulet, etc.)
        payload: parsedPayload, // Keep original payload too
      };
    });
    
    res.json(serializeBigInt({ data: parsedRows }));
  } catch (err) {
    console.error('ACS contracts error:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/acs/rich-list - Get aggregated holder balances (server-side calculation)
router.get('/rich-list', async (req, res) => {
  try {
    if (!hasACSData()) {
      return res.json({ data: [], totalSupply: 0, holderCount: 0 });
    }

    const limit = Math.min(parseInt(req.query.limit) || 100, 1000);
    const search = req.query.search || '';

    // Check cache - use search-specific key
    const cacheKey = `acs:rich-list:${limit}:${search}`;
    const cached = getCached(cacheKey);
    if (cached) {
      console.log(`[ACS] Rich list cache HIT: ${cacheKey}`);
      return res.json(cached);
    }

    console.log(`[ACS] Rich list cache MISS: ${cacheKey}`);

    // Try to use pre-computed aggregation first
    const aggregation = getCached('aggregation:holder-balances');
    if (aggregation && !search) {
      // Use pre-computed data
      const holders = aggregation.holders.slice(0, limit).map(row => ({
        owner: row.owner,
        amount: row.unlocked_balance,
        locked: row.locked_balance,
        total: row.total_balance,
      }));
      
      const result = serializeBigInt({
        data: holders,
        totalSupply: aggregation.totalSupply,
        unlockedSupply: aggregation.unlockedSupply,
        lockedSupply: aggregation.lockedSupply,
        holderCount: aggregation.holderCount,
        cached: true,
        refreshedAt: aggregation.refreshedAt,
      });
      
      setCache(cacheKey, result, CACHE_TTL.RICH_LIST);
      return res.json(result);
    }

    // Fall back to query (for search or if no pre-computed data)
    const acsSource = getACSSource();
    const sql = `
      WITH ${getLatestSnapshotCTE(acsSource)},
      amulet_balances AS (
        SELECT 
          json_extract_string(payload, '$.owner') as owner,
          CAST(COALESCE(
            json_extract_string(payload, '$.amount.initialAmount'),
            '0'
          ) AS DOUBLE) as amount
        FROM ${acsSource} acs
        WHERE acs.migration_id = (SELECT migration_id FROM latest_migration)
          AND acs.snapshot_time = (SELECT snapshot_time FROM latest_snapshot)
          AND (entity_name = 'Amulet' OR template_id LIKE '%:Amulet:%' OR template_id LIKE '%:Amulet')
          AND json_extract_string(payload, '$.owner') IS NOT NULL
      ),
      locked_balances AS (
        SELECT 
          COALESCE(
            json_extract_string(payload, '$.amulet.owner'),
            json_extract_string(payload, '$.owner')
          ) as owner,
          CAST(COALESCE(
            json_extract_string(payload, '$.amulet.amount.initialAmount'),
            json_extract_string(payload, '$.amount.initialAmount'),
            '0'
          ) AS DOUBLE) as amount
        FROM ${acsSource} acs
        WHERE acs.migration_id = (SELECT migration_id FROM latest_migration)
          AND acs.snapshot_time = (SELECT snapshot_time FROM latest_snapshot)
          AND (entity_name = 'LockedAmulet' OR template_id LIKE '%:LockedAmulet:%' OR template_id LIKE '%:LockedAmulet')
          AND (json_extract_string(payload, '$.amulet.owner') IS NOT NULL 
               OR json_extract_string(payload, '$.owner') IS NOT NULL)
      ),
      combined AS (
        SELECT owner, amount, 0.0 as locked FROM amulet_balances
        UNION ALL
        SELECT owner, 0.0 as amount, amount as locked FROM locked_balances
      ),
      aggregated AS (
        SELECT 
          owner,
          SUM(amount) as unlocked_balance,
          SUM(locked) as locked_balance,
          SUM(amount) + SUM(locked) as total_balance
        FROM combined
        WHERE owner IS NOT NULL AND owner != ''
        GROUP BY owner
      )
      SELECT * FROM aggregated
      ${search ? `WHERE owner ILIKE '%${search.replace(/'/g, "''")}%'` : ''}
      ORDER BY total_balance DESC
      LIMIT ${limit}
    `;

    const rows = await db.safeQuery(sql);
    console.log(`[ACS] Rich list returned ${rows.length} holders`);

    // Get total supply and holder count (using same acsSource)
    const statsSql = `
      WITH ${getLatestSnapshotCTE(acsSource)},
      amulet_total AS (
        SELECT COALESCE(SUM(
          CAST(COALESCE(
            json_extract_string(payload, '$.amount.initialAmount'),
            '0'
          ) AS DOUBLE)
        ), 0) as total
        FROM ${acsSource} acs
        WHERE acs.migration_id = (SELECT migration_id FROM latest_migration)
          AND acs.snapshot_time = (SELECT snapshot_time FROM latest_snapshot)
          AND (entity_name = 'Amulet' OR template_id LIKE '%:Amulet:%')
      ),
      locked_total AS (
        SELECT COALESCE(SUM(
          CAST(COALESCE(
            json_extract_string(payload, '$.amulet.amount.initialAmount'),
            json_extract_string(payload, '$.amount.initialAmount'),
            '0'
          ) AS DOUBLE)
        ), 0) as total
        FROM ${acsSource} acs
        WHERE acs.migration_id = (SELECT migration_id FROM latest_migration)
          AND acs.snapshot_time = (SELECT snapshot_time FROM latest_snapshot)
          AND (entity_name = 'LockedAmulet' OR template_id LIKE '%:LockedAmulet:%')
      ),
      holder_count AS (
        SELECT COUNT(DISTINCT COALESCE(
          json_extract_string(payload, '$.amulet.owner'),
          json_extract_string(payload, '$.owner')
        )) as count
        FROM ${acsSource} acs
        WHERE acs.migration_id = (SELECT migration_id FROM latest_migration)
          AND acs.snapshot_time = (SELECT snapshot_time FROM latest_snapshot)
          AND (entity_name IN ('Amulet', 'LockedAmulet') 
               OR template_id LIKE '%:Amulet:%' 
               OR template_id LIKE '%:LockedAmulet:%')
      )
      SELECT 
        amulet_total.total + locked_total.total as total_supply,
        amulet_total.total as unlocked_supply,
        locked_total.total as locked_supply,
        holder_count.count as holder_count
      FROM amulet_total, locked_total, holder_count
    `;

    const stats = await db.safeQuery(statsSql);
    const totalSupply = stats[0]?.total_supply || 0;
    const unlockedSupply = stats[0]?.unlocked_supply || 0;
    const lockedSupply = stats[0]?.locked_supply || 0;
    const holderCount = stats[0]?.holder_count || 0;

    const result = serializeBigInt({
      data: rows.map(row => ({
        owner: row.owner,
        amount: row.unlocked_balance,
        locked: row.locked_balance,
        total: row.total_balance,
      })),
      totalSupply,
      unlockedSupply,
      lockedSupply,
      holderCount,
    });
    
    setCache(cacheKey, result, CACHE_TTL.RICH_LIST);
    res.json(result);
  } catch (err) {
    console.error('ACS rich-list error:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/acs/supply - Get supply statistics (Amulet contracts)
router.get('/supply', async (req, res) => {
  try {
    if (!hasACSData()) {
      return res.json({ data: null });
    }

    // Check cache
    const cacheKey = 'acs:supply';
    const cached = getCached(cacheKey);
    if (cached) {
      return res.json(cached);
    }

    const acsSource = getACSSource();
    const sql = `
      WITH ${getLatestSnapshotCTE(acsSource)}
      SELECT 
        COUNT(*) as amulet_count,
        snapshot_time,
        (SELECT migration_id FROM latest_migration) as migration_id
      FROM ${acsSource} acs
      WHERE acs.migration_id = (SELECT migration_id FROM latest_migration)
        AND acs.snapshot_time = (SELECT snapshot_time FROM latest_snapshot)
        AND (entity_name = 'Amulet' OR template_id LIKE '%Amulet%')
      GROUP BY snapshot_time
    `;

    const rows = await db.safeQuery(sql);
    const result = serializeBigInt({ data: rows[0] || null });
    setCache(cacheKey, result, CACHE_TTL.SUPPLY);
    res.json(result);
  } catch (err) {
    console.error('ACS supply error:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/acs/allocations - Get amulet allocations with server-side aggregation
router.get('/allocations', async (req, res) => {
  try {
    if (!hasACSData()) {
      return res.json({ data: [], totalCount: 0, totalAmount: 0 });
    }

    const limit = Math.min(parseInt(req.query.limit) || 100, 1000);
    const offset = parseInt(req.query.offset) || 0;
    const search = (req.query.search || '').trim();

    // Check cache for paginated data
    const cacheKey = `acs:allocations:${limit}:${offset}:${search}`;
    const cached = getCached(cacheKey);
    if (cached) {
      return res.json(cached);
    }

    console.log(`[ACS] Allocations request: limit=${limit}, offset=${offset}, search=${search}`);

    const acsSource = getACSSource();
    const sql = `
      WITH ${getLatestSnapshotCTE(acsSource)}
      SELECT 
        contract_id,
        json_extract_string(payload, '$.allocation.settlement.executor') as executor,
        json_extract_string(payload, '$.allocation.transferLeg.sender') as sender,
        json_extract_string(payload, '$.allocation.transferLeg.receiver') as receiver,
        CAST(COALESCE(json_extract_string(payload, '$.allocation.transferLeg.amount'), '0') AS DOUBLE) as amount,
        json_extract_string(payload, '$.allocation.settlement.requestedAt') as requested_at,
        json_extract_string(payload, '$.allocation.transferLegId') as transfer_leg_id,
        payload
      FROM ${acsSource} acs
      WHERE acs.migration_id = (SELECT migration_id FROM latest_migration)
        AND acs.snapshot_time = (SELECT snapshot_time FROM latest_snapshot)
        AND (entity_name = 'AmuletAllocation' OR template_id LIKE '%:AmuletAllocation:%' OR template_id LIKE '%:AmuletAllocation')
        ${search ? `AND (
          json_extract_string(payload, '$.allocation.settlement.executor') ILIKE '%${search.replace(/'/g, "''")}%'
          OR json_extract_string(payload, '$.allocation.transferLeg.sender') ILIKE '%${search.replace(/'/g, "''")}%'
          OR json_extract_string(payload, '$.allocation.transferLeg.receiver') ILIKE '%${search.replace(/'/g, "''")}%'
        )` : ''}
      ORDER BY amount DESC
      LIMIT ${limit} OFFSET ${offset}
    `;

    const rows = await db.safeQuery(sql);

    // Get totals (cached separately from paginated data to avoid refetching on page change)
    const statsCacheKey = `acs:allocations-stats:${search}`;
    let stats = getCached(statsCacheKey);
    
    if (!stats) {
      const statsSql = `
        WITH ${getLatestSnapshotCTE(acsSource)}
        SELECT 
          COUNT(*) as total_count,
          COALESCE(SUM(CAST(COALESCE(json_extract_string(payload, '$.allocation.transferLeg.amount'), '0') AS DOUBLE)), 0) as total_amount,
          COUNT(DISTINCT json_extract_string(payload, '$.allocation.settlement.executor')) as unique_executors
        FROM ${acsSource} acs
        WHERE acs.migration_id = (SELECT migration_id FROM latest_migration)
          AND acs.snapshot_time = (SELECT snapshot_time FROM latest_snapshot)
          AND (entity_name = 'AmuletAllocation' OR template_id LIKE '%:AmuletAllocation:%' OR template_id LIKE '%:AmuletAllocation')
          ${search ? `AND (
            json_extract_string(payload, '$.allocation.settlement.executor') ILIKE '%${search.replace(/'/g, "''")}%'
            OR json_extract_string(payload, '$.allocation.transferLeg.sender') ILIKE '%${search.replace(/'/g, "''")}%'
            OR json_extract_string(payload, '$.allocation.transferLeg.receiver') ILIKE '%${search.replace(/'/g, "''")}%'
          )` : ''}
      `;

      const statsRows = await db.safeQuery(statsSql);
      stats = statsRows[0] || { total_count: 0, total_amount: 0, unique_executors: 0 };
      setCache(statsCacheKey, stats, CACHE_TTL.ALLOCATIONS);
    }

    const result = serializeBigInt({
      data: rows,
      totalCount: stats.total_count || 0,
      totalAmount: stats.total_amount || 0,
      uniqueExecutors: stats.unique_executors || 0,
    });
    
    setCache(cacheKey, result, CACHE_TTL.ALLOCATIONS);
    res.json(result);
  } catch (err) {
    console.error('ACS allocations error:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/acs/mining-rounds - Get mining rounds with server-side aggregation
router.get('/mining-rounds', async (req, res) => {
  try {
    if (!hasACSData()) {
      return res.json({ openRounds: [], issuingRounds: [], closedRounds: [], counts: {} });
    }

    const closedLimit = Math.min(parseInt(req.query.closedLimit) || 20, 100);

    // Check cache
    const cacheKey = `acs:mining-rounds:${closedLimit}`;
    const cached = getCached(cacheKey);
    if (cached) {
      console.log(`[ACS] Mining rounds cache HIT`);
      return res.json(cached);
    }

    console.log(`[ACS] Mining rounds cache MISS: ${cacheKey}`);

    // Try to use pre-computed aggregation first
    const aggregation = getCached('aggregation:mining-rounds');
    if (aggregation) {
      const result = serializeBigInt({
        openRounds: aggregation.openRounds,
        issuingRounds: aggregation.issuingRounds,
        closedRounds: aggregation.closedRounds.slice(0, closedLimit),
        counts: aggregation.counts,
        cached: true,
        refreshedAt: aggregation.refreshedAt,
      });
      
      setCache(cacheKey, result, CACHE_TTL.MINING_ROUNDS);
      return res.json(result);
    }

    // Fall back to query - use latest migration_id AND latest snapshot_time
    const acsSource = getACSSource();
    const sql = `
      WITH latest_migration AS (
        SELECT MAX(migration_id) as migration_id FROM ${acsSource}
      ),
      latest_snapshot AS (
        SELECT MAX(snapshot_time) as snapshot_time 
        FROM ${acsSource}
        WHERE migration_id = (SELECT migration_id FROM latest_migration)
      ),
      all_rounds AS (
        SELECT 
          contract_id,
          entity_name,
          template_id,
          -- Try multiple extraction paths for round number
          COALESCE(
            NULLIF(json_extract_string(payload, '$.round.number'), ''),
            NULLIF(CAST(json_extract(payload, '$.round.number') AS VARCHAR), ''),
            NULLIF(json_extract_string(payload, '$.round'), ''),
            NULLIF(CAST(json_extract(payload, '$.round') AS VARCHAR), '')
          ) as round_number,
          json_extract_string(payload, '$.opensAt') as opens_at,
          json_extract_string(payload, '$.targetClosesAt') as target_closes_at,
          json_extract_string(payload, '$.amuletPrice') as amulet_price,
          payload
        FROM ${acsSource} acs
        WHERE acs.migration_id = (SELECT migration_id FROM latest_migration)
          AND acs.snapshot_time = (SELECT snapshot_time FROM latest_snapshot)
          AND (entity_name IN ('OpenMiningRound', 'IssuingMiningRound', 'ClosedMiningRound')
               OR template_id LIKE '%MiningRound%')
      )
      SELECT * FROM all_rounds
      ORDER BY entity_name, CAST(COALESCE(NULLIF(round_number, ''), '0') AS BIGINT) DESC
    `;

    const rows = await db.safeQuery(sql);

    // Separate by type
    const openRounds = rows.filter(r => r.entity_name === 'OpenMiningRound' || r.template_id?.includes('OpenMiningRound'));
    const issuingRounds = rows.filter(r => r.entity_name === 'IssuingMiningRound' || r.template_id?.includes('IssuingMiningRound'));
    const allClosedRounds = rows.filter(r => r.entity_name === 'ClosedMiningRound' || r.template_id?.includes('ClosedMiningRound'));
    const closedRounds = allClosedRounds.slice(0, closedLimit);

    const result = serializeBigInt({
      openRounds,
      issuingRounds,
      closedRounds,
      counts: {
        open: openRounds.length,
        issuing: issuingRounds.length,
        closed: allClosedRounds.length,
      }
    });
    
    setCache(cacheKey, result, CACHE_TTL.MINING_ROUNDS);
    res.json(result);
  } catch (err) {
    console.error('ACS mining-rounds error:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/acs/stats - Overview statistics
router.get('/stats', async (req, res) => {
  try {
    if (!hasACSData()) {
      return res.json({ 
        data: {
          total_contracts: 0,
          total_templates: 0,
          total_snapshots: 0,
          latest_snapshot: null,
        }
      });
    }

    // Check cache
    const cacheKey = 'acs:stats';
    const cached = getCached(cacheKey);
    if (cached) {
      return res.json(cached);
    }

    const sql = `
      SELECT 
        COUNT(*) as total_contracts,
        COUNT(DISTINCT template_id) as total_templates,
        COUNT(DISTINCT snapshot_time) as total_snapshots,
        MAX(snapshot_time) as latest_snapshot,
        MAX(record_time) as latest_record_time
      FROM ${getACSSource()}
    `;

    const rows = await db.safeQuery(sql);
    const result = serializeBigInt({ data: rows[0] || {} });
    setCache(cacheKey, result, CACHE_TTL.STATS);
    res.json(result);
  } catch (err) {
    console.error('ACS stats error:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/acs/debug - Debug endpoint to show entity names and template IDs
router.get('/debug', async (req, res) => {
  try {
    if (!hasACSData()) {
      return res.json({ data: null, message: 'No ACS data available' });
    }

    // Get distinct entity_names
    const entitySql = `
      SELECT DISTINCT entity_name, COUNT(*) as count
      FROM ${getACSSource()}
      GROUP BY entity_name
      ORDER BY count DESC
      LIMIT 100
    `;

    // Get sample template_ids
    const templateSql = `
      SELECT DISTINCT template_id, COUNT(*) as count
      FROM ${getACSSource()}
      GROUP BY template_id
      ORDER BY count DESC
      LIMIT 100
    `;

    // Get sample of columns
    const columnsSql = `
      SELECT * FROM ${getACSSource()} LIMIT 1
    `;

    const [entities, templates, sample] = await Promise.all([
      db.safeQuery(entitySql),
      db.safeQuery(templateSql),
      db.safeQuery(columnsSql),
    ]);

    res.json(serializeBigInt({
      data: {
        entity_names: entities,
        template_ids: templates,
        sample_columns: sample.length > 0 ? Object.keys(sample[0]) : [],
        sample_record: sample[0] || null,
      }
    }));
  } catch (err) {
    console.error('ACS debug error:', err);
    res.status(500).json({ error: err.message });
  }
});

export default router;
