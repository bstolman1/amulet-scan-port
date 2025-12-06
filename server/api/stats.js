import { Router } from 'express';
import db from '../duckdb/connection.js';

const router = Router();

// Helper to get the correct read function for JSONL files (supports both .jsonl and .jsonl.gz)
// Uses UNION for cross-platform compatibility (Windows doesn't support brace expansion)
const getUpdatesSource = () => `(
  SELECT * FROM read_json_auto('${db.DATA_PATH}/**/updates-*.jsonl', union_by_name=true, ignore_errors=true)
  UNION ALL
  SELECT * FROM read_json_auto('${db.DATA_PATH}/**/updates-*.jsonl.gz', union_by_name=true, ignore_errors=true)
)`;

// GET /api/stats/overview - Dashboard overview stats
router.get('/overview', async (req, res) => {
  try {
    const sql = `
      SELECT 
        COUNT(*) as total_events,
        COUNT(DISTINCT contract_id) as unique_contracts,
        COUNT(DISTINCT template_id) as unique_templates,
        MIN(timestamp) as earliest_event,
        MAX(timestamp) as latest_event
      FROM ${getUpdatesSource()}
    `;
    
    const rows = await db.safeQuery(sql);
    res.json(rows[0] || {});
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/stats/daily - Daily event counts
router.get('/daily', async (req, res) => {
  try {
    const days = Math.min(parseInt(req.query.days) || 30, 365);
    
    const sql = `
      SELECT 
        DATE_TRUNC('day', timestamp) as date,
        COUNT(*) as event_count,
        COUNT(DISTINCT contract_id) as contract_count
      FROM ${getUpdatesSource()}
      WHERE timestamp >= NOW() - INTERVAL '${days} days'
      GROUP BY DATE_TRUNC('day', timestamp)
      ORDER BY date DESC
    `;
    
    const rows = await db.safeQuery(sql);
    res.json({ data: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/stats/by-type - Event counts by type
router.get('/by-type', async (req, res) => {
  try {
    const sql = `
      SELECT 
        event_type,
        COUNT(*) as count
      FROM ${getUpdatesSource()}
      GROUP BY event_type
      ORDER BY count DESC
    `;
    
    const rows = await db.safeQuery(sql);
    res.json({ data: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/stats/by-template - Event counts by template
router.get('/by-template', async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 50, 500);
    
    const sql = `
      SELECT 
        template_id,
        COUNT(*) as event_count,
        COUNT(DISTINCT contract_id) as contract_count,
        MIN(timestamp) as first_seen,
        MAX(timestamp) as last_seen
      FROM ${getUpdatesSource()}
      WHERE template_id IS NOT NULL
      GROUP BY template_id
      ORDER BY event_count DESC
      LIMIT ${limit}
    `;
    
    const rows = await db.safeQuery(sql);
    res.json({ data: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/stats/hourly - Hourly activity (last 24h)
router.get('/hourly', async (req, res) => {
  try {
    const sql = `
      SELECT 
        DATE_TRUNC('hour', timestamp) as hour,
        COUNT(*) as event_count
      FROM ${getUpdatesSource()}
      WHERE timestamp >= NOW() - INTERVAL '24 hours'
      GROUP BY DATE_TRUNC('hour', timestamp)
      ORDER BY hour DESC
    `;
    
    const rows = await db.safeQuery(sql);
    res.json({ data: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/stats/burn - Burn statistics (if applicable)
router.get('/burn', async (req, res) => {
  try {
    const sql = `
      SELECT 
        DATE_TRUNC('day', timestamp) as date,
        SUM(CAST(json_extract(payload, '$.amount.amount') AS DOUBLE)) as burn_amount
      FROM ${getUpdatesSource()}
      WHERE template_id LIKE '%BurnMintSummary%'
      GROUP BY DATE_TRUNC('day', timestamp)
      ORDER BY date DESC
      LIMIT 30
    `;
    
    const rows = await db.safeQuery(sql);
    res.json({ data: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

export default router;
