import { Router } from 'express';
import db from '../duckdb/connection.js';
import path from 'path';
import fs from 'fs';

const router = Router();

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

// GET /api/acs/snapshots - List all available snapshots
router.get('/snapshots', async (req, res) => {
  try {
    if (!hasACSData()) {
      return res.json({ data: [], message: 'No ACS data available' });
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
    res.json(serializeBigInt({ data: snapshots }));
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

    // Get basic snapshot info
    const basicSql = `
      SELECT 
        snapshot_time,
        migration_id,
        COUNT(*) as contract_count,
        COUNT(DISTINCT template_id) as template_count,
        MIN(record_time) as record_time
      FROM ${getACSSource()}
      GROUP BY snapshot_time, migration_id
      ORDER BY snapshot_time DESC
      LIMIT 1
    `;

    const basicRows = await db.safeQuery(basicSql);
    
    if (basicRows.length === 0) {
      return res.json({ data: null });
    }

    const row = basicRows[0];
    const snapshotTime = row.snapshot_time;

    // Calculate supply metrics from Amulet and LockedAmulet contracts
    const supplySql = `
      WITH latest_contracts AS (
        SELECT template_id, entity_name, payload
        FROM ${getACSSource()}
        WHERE snapshot_time = '${snapshotTime}'
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

    res.json(serializeBigInt({
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
    }));
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

    const sql = `
      WITH latest_snapshot AS (
        SELECT MAX(snapshot_time) as snapshot_time FROM ${getACSSource()}
      )
      SELECT 
        template_id,
        entity_name,
        module_name,
        COUNT(*) as contract_count,
        COUNT(DISTINCT contract_id) as unique_contracts
      FROM ${getACSSource()} acs
      WHERE acs.snapshot_time = (SELECT snapshot_time FROM latest_snapshot)
      GROUP BY template_id, entity_name, module_name
      ORDER BY contract_count DESC
      LIMIT ${limit}
    `;

    const rows = await db.safeQuery(sql);
    res.json(serializeBigInt({ data: rows }));
  } catch (err) {
    console.error('ACS templates error:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/acs/contracts - Get contracts by template with parsed payload
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

    const sql = `
      WITH latest_snapshot AS (
        SELECT MAX(snapshot_time) as snapshot_time FROM ${getACSSource()}
      )
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
      FROM ${getACSSource()} acs
      WHERE acs.snapshot_time = (SELECT snapshot_time FROM latest_snapshot)
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

    console.log(`[ACS] Rich list request: limit=${limit}, search=${search}`);

    // Calculate holder balances aggregated by owner in SQL using DuckDB json_extract_string
    const sql = `
      WITH latest_snapshot AS (
        SELECT MAX(snapshot_time) as snapshot_time FROM ${getACSSource()}
      ),
      amulet_balances AS (
        SELECT 
          json_extract_string(payload, '$.owner') as owner,
          CAST(COALESCE(
            json_extract_string(payload, '$.amount.initialAmount'),
            '0'
          ) AS DOUBLE) / 10000000000.0 as amount
        FROM ${getACSSource()} acs
        WHERE acs.snapshot_time = (SELECT snapshot_time FROM latest_snapshot)
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
          ) AS DOUBLE) / 10000000000.0 as amount
        FROM ${getACSSource()} acs
        WHERE acs.snapshot_time = (SELECT snapshot_time FROM latest_snapshot)
          AND (entity_name = 'LockedAmulet' OR template_id LIKE '%:LockedAmulet:%' OR template_id LIKE '%:LockedAmulet')
          AND (json_extract_string(payload, '$.amulet.owner') IS NOT NULL OR json_extract_string(payload, '$.owner') IS NOT NULL)
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

    // Get total supply and holder count
    const statsSql = `
      WITH latest_snapshot AS (
        SELECT MAX(snapshot_time) as snapshot_time FROM ${getACSSource()}
      ),
      amulet_total AS (
        SELECT COALESCE(SUM(
          CAST(COALESCE(
            json_extract_string(payload, '$.amount.initialAmount'),
            '0'
          ) AS DOUBLE) / 10000000000.0
        ), 0) as total
        FROM ${getACSSource()} acs
        WHERE acs.snapshot_time = (SELECT snapshot_time FROM latest_snapshot)
          AND (entity_name = 'Amulet' OR template_id LIKE '%:Amulet:%')
      ),
      locked_total AS (
        SELECT COALESCE(SUM(
          CAST(COALESCE(
            json_extract_string(payload, '$.amulet.amount.initialAmount'),
            json_extract_string(payload, '$.amount.initialAmount'),
            '0'
          ) AS DOUBLE) / 10000000000.0
        ), 0) as total
        FROM ${getACSSource()} acs
        WHERE acs.snapshot_time = (SELECT snapshot_time FROM latest_snapshot)
          AND (entity_name = 'LockedAmulet' OR template_id LIKE '%:LockedAmulet:%')
      ),
      holder_count AS (
        SELECT COUNT(DISTINCT COALESCE(
          json_extract_string(payload, '$.amulet.owner'),
          json_extract_string(payload, '$.owner')
        )) as count
        FROM ${getACSSource()} acs
        WHERE acs.snapshot_time = (SELECT snapshot_time FROM latest_snapshot)
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

    res.json(serializeBigInt({
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
    }));
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

    const sql = `
      WITH latest_snapshot AS (
        SELECT MAX(snapshot_time) as snapshot_time FROM ${getACSSource()}
      )
      SELECT 
        COUNT(*) as amulet_count,
        snapshot_time
      FROM ${getACSSource()} acs
      WHERE acs.snapshot_time = (SELECT snapshot_time FROM latest_snapshot)
        AND (entity_name = 'Amulet' OR template_id LIKE '%Amulet%')
      GROUP BY snapshot_time
    `;

    const rows = await db.safeQuery(sql);
    res.json(serializeBigInt({ data: rows[0] || null }));
  } catch (err) {
    console.error('ACS supply error:', err);
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
    res.json(serializeBigInt({ data: rows[0] || {} }));
  } catch (err) {
    console.error('ACS stats error:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/acs/supply-stats - Get aggregated supply statistics (server-side calculation)
router.get('/supply-stats', async (req, res) => {
  try {
    if (!hasACSData()) {
      return res.json({ 
        totalSupply: 0, 
        unlockedSupply: 0, 
        lockedSupply: 0,
        amuletCount: 0,
        lockedCount: 0,
      });
    }

    console.log('[ACS] Supply stats request');

    // Calculate supply totals in SQL
    const sql = `
      WITH latest_snapshot AS (
        SELECT MAX(snapshot_time) as snapshot_time FROM ${getACSSource()}
      ),
      amulet_stats AS (
        SELECT 
          COUNT(*) as count,
          COALESCE(SUM(
            CAST(COALESCE(
              json_extract_string(payload, '$.amount.initialAmount'),
              '0'
            ) AS DOUBLE) / 10000000000.0
          ), 0) as total
        FROM ${getACSSource()} acs
        WHERE acs.snapshot_time = (SELECT snapshot_time FROM latest_snapshot)
          AND (entity_name = 'Amulet' OR template_id LIKE '%:Amulet:%' OR template_id LIKE '%:Amulet')
      ),
      locked_stats AS (
        SELECT 
          COUNT(*) as count,
          COALESCE(SUM(
            CAST(COALESCE(
              json_extract_string(payload, '$.amulet.amount.initialAmount'),
              json_extract_string(payload, '$.amount.initialAmount'),
              '0'
            ) AS DOUBLE) / 10000000000.0
          ), 0) as total
        FROM ${getACSSource()} acs
        WHERE acs.snapshot_time = (SELECT snapshot_time FROM latest_snapshot)
          AND (entity_name = 'LockedAmulet' OR template_id LIKE '%:LockedAmulet:%' OR template_id LIKE '%:LockedAmulet')
      )
      SELECT 
        amulet_stats.total + locked_stats.total as total_supply,
        amulet_stats.total as unlocked_supply,
        locked_stats.total as locked_supply,
        amulet_stats.count as amulet_count,
        locked_stats.count as locked_count
      FROM amulet_stats, locked_stats
    `;

    const rows = await db.safeQuery(sql);
    const result = rows[0] || {};
    
    console.log(`[ACS] Supply stats: total=${result.total_supply}, unlocked=${result.unlocked_supply}, locked=${result.locked_supply}`);

    res.json(serializeBigInt({
      totalSupply: result.total_supply || 0,
      unlockedSupply: result.unlocked_supply || 0,
      lockedSupply: result.locked_supply || 0,
      amuletCount: result.amulet_count || 0,
      lockedCount: result.locked_count || 0,
    }));
  } catch (err) {
    console.error('ACS supply-stats error:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/acs/validator-stats - Get aggregated validator license statistics
router.get('/validator-stats', async (req, res) => {
  try {
    if (!hasACSData()) {
      return res.json({ 
        licenseCount: 0, 
        couponCount: 0, 
        livenessCount: 0,
        rightsCount: 0,
        uniqueValidators: 0,
      });
    }

    console.log('[ACS] Validator stats request');

    const sql = `
      WITH latest_snapshot AS (
        SELECT MAX(snapshot_time) as snapshot_time FROM ${getACSSource()}
      ),
      licenses AS (
        SELECT COUNT(*) as count
        FROM ${getACSSource()} acs
        WHERE acs.snapshot_time = (SELECT snapshot_time FROM latest_snapshot)
          AND (entity_name = 'ValidatorLicense' OR template_id LIKE '%:ValidatorLicense:%' OR template_id LIKE '%ValidatorLicense:ValidatorLicense')
      ),
      coupons AS (
        SELECT COUNT(*) as count
        FROM ${getACSSource()} acs
        WHERE acs.snapshot_time = (SELECT snapshot_time FROM latest_snapshot)
          AND (entity_name = 'ValidatorFaucetCoupon' OR template_id LIKE '%:ValidatorFaucetCoupon:%' OR template_id LIKE '%ValidatorFaucetCoupon')
      ),
      liveness AS (
        SELECT COUNT(*) as count
        FROM ${getACSSource()} acs
        WHERE acs.snapshot_time = (SELECT snapshot_time FROM latest_snapshot)
          AND (entity_name = 'ValidatorLivenessActivityRecord' OR template_id LIKE '%:ValidatorLivenessActivityRecord:%' OR template_id LIKE '%ValidatorLivenessActivityRecord')
      ),
      rights AS (
        SELECT COUNT(*) as count
        FROM ${getACSSource()} acs
        WHERE acs.snapshot_time = (SELECT snapshot_time FROM latest_snapshot)
          AND (entity_name = 'ValidatorRight' OR template_id LIKE '%:ValidatorRight:%' OR template_id LIKE '%:ValidatorRight')
      ),
      unique_validators AS (
        SELECT COUNT(DISTINCT json_extract_string(payload, '$.validator')) as count
        FROM ${getACSSource()} acs
        WHERE acs.snapshot_time = (SELECT snapshot_time FROM latest_snapshot)
          AND (entity_name = 'ValidatorLicense' OR template_id LIKE '%:ValidatorLicense:%' OR template_id LIKE '%ValidatorLicense:ValidatorLicense')
      )
      SELECT 
        licenses.count as license_count,
        coupons.count as coupon_count,
        liveness.count as liveness_count,
        rights.count as rights_count,
        unique_validators.count as unique_validators
      FROM licenses, coupons, liveness, rights, unique_validators
    `;

    const rows = await db.safeQuery(sql);
    const result = rows[0] || {};
    
    console.log(`[ACS] Validator stats: licenses=${result.license_count}, coupons=${result.coupon_count}`);

    res.json(serializeBigInt({
      licenseCount: result.license_count || 0,
      couponCount: result.coupon_count || 0,
      livenessCount: result.liveness_count || 0,
      rightsCount: result.rights_count || 0,
      uniqueValidators: result.unique_validators || 0,
    }));
  } catch (err) {
    console.error('ACS validator-stats error:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/acs/traffic-stats - Get aggregated member traffic statistics
router.get('/traffic-stats', async (req, res) => {
  try {
    if (!hasACSData()) {
      return res.json({ 
        totalRecords: 0, 
        uniqueMembers: 0, 
        totalTrafficBytes: 0,
      });
    }

    console.log('[ACS] Traffic stats request');

    const sql = `
      WITH latest_snapshot AS (
        SELECT MAX(snapshot_time) as snapshot_time FROM ${getACSSource()}
      ),
      traffic_data AS (
        SELECT 
          json_extract_string(payload, '$.member') as member,
          CAST(COALESCE(
            json_extract_string(payload, '$.totalTrafficBytes'),
            json_extract_string(payload, '$.totalPurchased'),
            '0'
          ) AS BIGINT) as traffic_bytes
        FROM ${getACSSource()} acs
        WHERE acs.snapshot_time = (SELECT snapshot_time FROM latest_snapshot)
          AND (entity_name = 'MemberTraffic' OR template_id LIKE '%:MemberTraffic:%' OR template_id LIKE '%:MemberTraffic')
      )
      SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT member) as unique_members,
        COALESCE(SUM(traffic_bytes), 0) as total_traffic_bytes
      FROM traffic_data
    `;

    const rows = await db.safeQuery(sql);
    const result = rows[0] || {};
    
    console.log(`[ACS] Traffic stats: records=${result.total_records}, members=${result.unique_members}, bytes=${result.total_traffic_bytes}`);

    res.json(serializeBigInt({
      totalRecords: result.total_records || 0,
      uniqueMembers: result.unique_members || 0,
      totalTrafficBytes: result.total_traffic_bytes || 0,
    }));
  } catch (err) {
    console.error('ACS traffic-stats error:', err);
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

    // Get sample Amulet payload for debugging amount extraction
    const amuletSampleSql = `
      SELECT payload
      FROM ${getACSSource()}
      WHERE entity_name = 'Amulet' OR template_id LIKE '%:Amulet:%'
      LIMIT 3
    `;

    const [entities, templates, sample, amuletSamples] = await Promise.all([
      db.safeQuery(entitySql),
      db.safeQuery(templateSql),
      db.safeQuery(columnsSql),
      db.safeQuery(amuletSampleSql),
    ]);

    res.json(serializeBigInt({
      data: {
        entity_names: entities,
        template_ids: templates,
        sample_columns: sample.length > 0 ? Object.keys(sample[0]) : [],
        sample_record: sample[0] || null,
        amulet_payloads: amuletSamples.map(s => {
          try {
            return typeof s.payload === 'string' ? JSON.parse(s.payload) : s.payload;
          } catch {
            return s.payload;
          }
        }),
      }
    }));
  } catch (err) {
    console.error('ACS debug error:', err);
    res.status(500).json({ error: err.message });
  }
});

export default router;
