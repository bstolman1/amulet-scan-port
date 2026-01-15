/**
 * Updates API - DuckDB Parquet Only
 * 
 * Data Authority: All queries use DuckDB over Parquet files.
 * See docs/architecture.md for the Data Authority Contract.
 */

import { Router } from 'express';
import { safeQuery, hasFileType, DATA_PATH, IS_TEST, TEST_FIXTURES_PATH } from '../duckdb/connection.js';

const router = Router();

/**
 * Get the SQL source for updates data
 * Prefers Parquet, falls back to JSONL
 */
const getUpdatesSource = () => {
  // In test mode, use test fixtures
  if (IS_TEST) {
    return `(SELECT * FROM read_json_auto('${TEST_FIXTURES_PATH}/updates-*.jsonl', union_by_name=true, ignore_errors=true))`;
  }
  
  const basePath = DATA_PATH.replace(/\\/g, '/');
  
  // Prefer Parquet
  if (hasFileType('updates', '.parquet')) {
    return `read_parquet('${basePath}/**/updates-*.parquet', union_by_name=true)`;
  }

  // Fall back to JSONL variants
  const hasJsonl = hasFileType('updates', '.jsonl');
  const hasGzip = hasFileType('updates', '.jsonl.gz');
  const hasZstd = hasFileType('updates', '.jsonl.zst');

  if (!hasJsonl && !hasGzip && !hasZstd) {
    return `(SELECT NULL::VARCHAR as update_id, NULL::VARCHAR as update_type, NULL::TIMESTAMP as record_time WHERE false)`;
  }

  const queries = [];
  if (hasJsonl) queries.push(`SELECT * FROM read_json_auto('${basePath}/**/updates-*.jsonl', union_by_name=true, ignore_errors=true)`);
  if (hasGzip) queries.push(`SELECT * FROM read_json_auto('${basePath}/**/updates-*.jsonl.gz', union_by_name=true, ignore_errors=true)`);
  if (hasZstd) queries.push(`SELECT * FROM read_json_auto('${basePath}/**/updates-*.jsonl.zst', union_by_name=true, ignore_errors=true)`);
  
  return `(${queries.join(' UNION ')})`;
};

/**
 * Get data source info for response metadata
 */
function getDataSourceInfo() {
  if (IS_TEST) return 'test';
  if (hasFileType('updates', '.parquet')) return 'parquet';
  if (hasFileType('updates', '.jsonl') || hasFileType('updates', '.jsonl.gz') || hasFileType('updates', '.jsonl.zst')) return 'jsonl';
  return 'none';
}

// GET /api/updates/latest - Fetch latest updates
router.get('/latest', async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 100, 1000);
    const offset = parseInt(req.query.offset) || 0;

    const sql = `
      SELECT *
      FROM ${getUpdatesSource()}
      ORDER BY record_time DESC
      LIMIT ${limit}
      OFFSET ${offset}
    `;

    const rows = await safeQuery(sql);
    
    // Check if there are more results
    const countSql = `SELECT COUNT(*) as total FROM ${getUpdatesSource()}`;
    const countResult = await safeQuery(countSql);
    const total = Number(countResult[0]?.total || 0);
    
    res.json({ 
      data: rows, 
      count: rows.length, 
      total,
      hasMore: offset + rows.length < total,
      source: getDataSourceInfo() 
    });
  } catch (err) {
    console.error('Error fetching latest updates:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/updates/count - Get total update count
router.get('/count', async (req, res) => {
  try {
    const sql = `SELECT COUNT(*) as total FROM ${getUpdatesSource()}`;
    const rows = await safeQuery(sql);
    
    res.json({ 
      count: Number(rows[0]?.total || 0), 
      source: getDataSourceInfo() 
    });
  } catch (err) {
    console.error('Error counting updates:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/updates/by-type - Updates grouped by type
router.get('/by-type', async (req, res) => {
  try {
    const sql = `
      SELECT 
        update_type,
        COUNT(*) as count
      FROM ${getUpdatesSource()}
      GROUP BY update_type
      ORDER BY count DESC
    `;
    
    const rows = await safeQuery(sql);
    res.json({ data: rows, source: getDataSourceInfo() });
  } catch (err) {
    console.error('Error fetching updates by type:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/updates/daily - Daily update counts
router.get('/daily', async (req, res) => {
  try {
    const days = Math.min(parseInt(req.query.days) || 30, 365);
    
    const sql = `
      SELECT 
        DATE_TRUNC('day', record_time) as date,
        COUNT(*) as count
      FROM ${getUpdatesSource()}
      WHERE record_time >= NOW() - INTERVAL '${days} days'
      GROUP BY DATE_TRUNC('day', record_time)
      ORDER BY date DESC
    `;
    
    const rows = await safeQuery(sql);
    res.json({ data: rows, source: getDataSourceInfo() });
  } catch (err) {
    console.error('Error fetching daily updates:', err);
    res.status(500).json({ error: err.message });
  }
});

export default router;
