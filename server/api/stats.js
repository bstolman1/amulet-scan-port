import { Router } from 'express';
import db from '../duckdb/connection.js';

const router = Router();

// Helper sources - try parquet first, then JSONL
const getParquetSource = () => `read_parquet('${db.DATA_PATH}/**/updates-*.parquet', union_by_name=true)`;
const getJsonlSource = () => `read_json_auto('${db.DATA_PATH}/**/updates-*.jsonl', union_by_name=true, ignore_errors=true)`;

async function queryWithFallback(parquetSql, jsonlSql) {
  try {
    return await db.safeQuery(parquetSql);
  } catch (e) {
    return await db.safeQuery(jsonlSql);
  }
}

// GET /api/stats/overview - Dashboard overview stats
router.get('/overview', async (req, res) => {
  try {
    const sql = (source) => `
      SELECT 
        COUNT(*) as total_events,
        COUNT(DISTINCT contract_id) as unique_contracts,
        COUNT(DISTINCT template_id) as unique_templates,
        MIN(timestamp) as earliest_event,
        MAX(timestamp) as latest_event
      FROM ${source}
    `;
    
    const rows = await queryWithFallback(sql(getParquetSource()), sql(getJsonlSource()));
    res.json(rows[0] || {});
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/stats/daily - Daily event counts
router.get('/daily', async (req, res) => {
  try {
    const days = Math.min(parseInt(req.query.days) || 30, 365);
    
    const sql = (source) => `
      SELECT 
        DATE_TRUNC('day', timestamp) as date,
        COUNT(*) as event_count,
        COUNT(DISTINCT contract_id) as contract_count
      FROM ${source}
      WHERE timestamp >= NOW() - INTERVAL '${days} days'
      GROUP BY DATE_TRUNC('day', timestamp)
      ORDER BY date DESC
    `;
    
    const rows = await queryWithFallback(sql(getParquetSource()), sql(getJsonlSource()));
    res.json({ data: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/stats/by-type - Event counts by type
router.get('/by-type', async (req, res) => {
  try {
    const sql = (source) => `
      SELECT 
        event_type,
        COUNT(*) as count
      FROM ${source}
      GROUP BY event_type
      ORDER BY count DESC
    `;
    
    const rows = await queryWithFallback(sql(getParquetSource()), sql(getJsonlSource()));
    res.json({ data: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/stats/by-template - Event counts by template
router.get('/by-template', async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 50, 500);
    
    const sql = (source) => `
      SELECT 
        template_id,
        COUNT(*) as event_count,
        COUNT(DISTINCT contract_id) as contract_count,
        MIN(timestamp) as first_seen,
        MAX(timestamp) as last_seen
      FROM ${source}
      WHERE template_id IS NOT NULL
      GROUP BY template_id
      ORDER BY event_count DESC
      LIMIT ${limit}
    `;
    
    const rows = await queryWithFallback(sql(getParquetSource()), sql(getJsonlSource()));
    res.json({ data: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/stats/hourly - Hourly activity (last 24h)
router.get('/hourly', async (req, res) => {
  try {
    const sql = (source) => `
      SELECT 
        DATE_TRUNC('hour', timestamp) as hour,
        COUNT(*) as event_count
      FROM ${source}
      WHERE timestamp >= NOW() - INTERVAL '24 hours'
      GROUP BY DATE_TRUNC('hour', timestamp)
      ORDER BY hour DESC
    `;
    
    const rows = await queryWithFallback(sql(getParquetSource()), sql(getJsonlSource()));
    res.json({ data: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/stats/burn - Burn statistics (if applicable)
router.get('/burn', async (req, res) => {
  try {
    const sql = (source) => `
      SELECT 
        DATE_TRUNC('day', timestamp) as date,
        SUM(CAST(json_extract(payload, '$.amount.amount') AS DOUBLE)) as burn_amount
      FROM ${source}
      WHERE template_id LIKE '%BurnMintSummary%'
      GROUP BY DATE_TRUNC('day', timestamp)
      ORDER BY date DESC
      LIMIT 30
    `;
    
    const rows = await queryWithFallback(sql(getParquetSource()), sql(getJsonlSource()));
    res.json({ data: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

export default router;
