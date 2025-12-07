import { Router } from 'express';
import db from '../duckdb/connection.js';
import path from 'path';
import fs from 'fs';

const router = Router();

// ACS data path - data is written to DATA_PATH/acs/ by the ingest scripts
const ACS_DATA_PATH = path.resolve(db.DATA_PATH, 'acs');

// Helper to get ACS file glob (uses UNION for cross-platform compatibility)
const getACSSource = () => {
  return `(
    SELECT * FROM read_json_auto('${ACS_DATA_PATH}/**/*.jsonl', union_by_name=true, ignore_errors=true)
    UNION ALL
    SELECT * FROM read_json_auto('${ACS_DATA_PATH}/**/*.jsonl.gz', union_by_name=true, ignore_errors=true)
  )`;
};

// Check if ACS data exists
function hasACSData() {
  try {
    if (!fs.existsSync(ACS_DATA_PATH)) return false;
    const files = fs.readdirSync(ACS_DATA_PATH, { recursive: true });
    return files.some(f => String(f).endsWith('.jsonl') || String(f).endsWith('.jsonl.gz'));
  } catch {
    return false;
  }
}

// GET /api/acs/snapshots - List all available snapshots
router.get('/snapshots', async (req, res) => {
  try {
    if (!hasACSData()) {
      return res.json({ data: [], message: 'No ACS data available' });
    }

    const sql = `
      SELECT 
        snapshot_time,
        migration_id,
        COUNT(*) as contract_count,
        COUNT(DISTINCT template_id) as template_count,
        MIN(record_time) as record_time
      FROM ${getACSSource()}
      GROUP BY snapshot_time, migration_id
      ORDER BY snapshot_time DESC
      LIMIT 20
    `;

    const rows = await db.safeQuery(sql);
    
    // Transform to match the UI's expected format
    const snapshots = rows.map((row, index) => ({
      id: `local-${index}`,
      timestamp: row.snapshot_time,
      migration_id: row.migration_id,
      record_time: row.record_time,
      entry_count: row.contract_count,
      template_count: row.template_count,
      status: 'completed',
      source: 'local',
    }));

    res.json({ data: snapshots });
  } catch (err) {
    console.error('ACS snapshots error:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/acs/latest - Get latest snapshot summary
router.get('/latest', async (req, res) => {
  try {
    if (!hasACSData()) {
      return res.json({ data: null, message: 'No ACS data available' });
    }

    const sql = `
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

    const rows = await db.safeQuery(sql);
    
    if (rows.length === 0) {
      return res.json({ data: null });
    }

    const row = rows[0];
    res.json({
      data: {
        id: 'local-latest',
        timestamp: row.snapshot_time,
        migration_id: row.migration_id,
        record_time: row.record_time,
        entry_count: row.contract_count,
        template_count: row.template_count,
        status: 'completed',
        source: 'local',
      }
    });
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
    res.json({ data: rows });
  } catch (err) {
    console.error('ACS templates error:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/acs/contracts - Get contracts by template
router.get('/contracts', async (req, res) => {
  try {
    if (!hasACSData()) {
      return res.json({ data: [] });
    }

    const { template, entity } = req.query;
    const limit = Math.min(parseInt(req.query.limit) || 100, 500);
    const offset = parseInt(req.query.offset) || 0;

    let whereClause = '1=1';
    if (template) {
      whereClause = `template_id LIKE '%${template}%'`;
    } else if (entity) {
      whereClause = `entity_name = '${entity}'`;
    }

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
    res.json({ data: rows });
  } catch (err) {
    console.error('ACS contracts error:', err);
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
    res.json({ data: rows[0] || null });
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
    res.json({ data: rows[0] || {} });
  } catch (err) {
    console.error('ACS stats error:', err);
    res.status(500).json({ error: err.message });
  }
});

export default router;
