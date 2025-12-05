import { Router } from 'express';
import db from '../duckdb/connection.js';

const router = Router();

// Helper to get the data source - supports both JSONL (old pipeline) and Parquet (new DuckDB pipeline)
const getUpdatesSource = () => {
  // Try parquet first (faster), fallback to JSONL
  return `(
    SELECT * FROM read_parquet('${db.DATA_PATH}/**/updates-*.parquet', union_by_name=true, filename=true)
    UNION ALL BY NAME
    SELECT * FROM read_json_auto('${db.DATA_PATH}/**/updates-*.jsonl', union_by_name=true, ignore_errors=true, filename=true)
  )`;
};

// Simplified source for when we know files exist (avoids union overhead)
const getParquetSource = () => `read_parquet('${db.DATA_PATH}/**/updates-*.parquet', union_by_name=true)`;
const getJsonlSource = () => `read_json_auto('${db.DATA_PATH}/**/updates-*.jsonl', union_by_name=true, ignore_errors=true)`;

// GET /api/events/latest - Get latest events
router.get('/latest', async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 100, 1000);
    const offset = parseInt(req.query.offset) || 0;
    
    // Try parquet first, then JSONL
    let rows;
    try {
      const sql = `
        SELECT 
          event_id,
          event_type,
          contract_id,
          template_id,
          package_name,
          timestamp,
          signatories,
          observers,
          payload
        FROM ${getParquetSource()}
        ORDER BY timestamp DESC
        LIMIT ${limit}
        OFFSET ${offset}
      `;
      rows = await db.safeQuery(sql);
    } catch (e) {
      // Fallback to JSONL
      const sql = `
        SELECT 
          event_id,
          event_type,
          contract_id,
          template_id,
          package_name,
          timestamp,
          signatories,
          observers,
          payload
        FROM ${getJsonlSource()}
        ORDER BY timestamp DESC
        LIMIT ${limit}
        OFFSET ${offset}
      `;
      rows = await db.safeQuery(sql);
    }
    
    res.json({ data: rows, count: rows.length });
  } catch (err) {
    console.error('Error fetching latest events:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/by-type/:type - Get events by type
router.get('/by-type/:type', async (req, res) => {
  try {
    const { type } = req.params;
    const limit = Math.min(parseInt(req.query.limit) || 100, 1000);
    
    let rows;
    try {
      rows = await db.safeQuery(`
        SELECT * FROM ${getParquetSource()}
        WHERE event_type = '${type}'
        ORDER BY timestamp DESC
        LIMIT ${limit}
      `);
    } catch (e) {
      rows = await db.safeQuery(`
        SELECT * FROM ${getJsonlSource()}
        WHERE event_type = '${type}'
        ORDER BY timestamp DESC
        LIMIT ${limit}
      `);
    }
    
    res.json({ data: rows, count: rows.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/by-template/:templateId - Get events by template
router.get('/by-template/:templateId', async (req, res) => {
  try {
    const { templateId } = req.params;
    const limit = Math.min(parseInt(req.query.limit) || 100, 1000);
    
    let rows;
    try {
      rows = await db.safeQuery(`
        SELECT * FROM ${getParquetSource()}
        WHERE template_id LIKE '%${templateId}%'
        ORDER BY timestamp DESC
        LIMIT ${limit}
      `);
    } catch (e) {
      rows = await db.safeQuery(`
        SELECT * FROM ${getJsonlSource()}
        WHERE template_id LIKE '%${templateId}%'
        ORDER BY timestamp DESC
        LIMIT ${limit}
      `);
    }
    
    res.json({ data: rows, count: rows.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/by-date - Get events for a specific date range
router.get('/by-date', async (req, res) => {
  try {
    const { start, end } = req.query;
    const limit = Math.min(parseInt(req.query.limit) || 1000, 10000);
    
    let whereClause = '';
    if (start) whereClause += ` AND timestamp >= '${start}'`;
    if (end) whereClause += ` AND timestamp <= '${end}'`;
    
    let rows;
    try {
      rows = await db.safeQuery(`
        SELECT * FROM ${getParquetSource()}
        WHERE 1=1 ${whereClause}
        ORDER BY timestamp DESC
        LIMIT ${limit}
      `);
    } catch (e) {
      rows = await db.safeQuery(`
        SELECT * FROM ${getJsonlSource()}
        WHERE 1=1 ${whereClause}
        ORDER BY timestamp DESC
        LIMIT ${limit}
      `);
    }
    
    res.json({ data: rows, count: rows.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/count - Get total event count
router.get('/count', async (req, res) => {
  try {
    let rows;
    try {
      rows = await db.safeQuery(`SELECT COUNT(*) as total FROM ${getParquetSource()}`);
    } catch (e) {
      rows = await db.safeQuery(`SELECT COUNT(*) as total FROM ${getJsonlSource()}`);
    }
    res.json({ count: rows[0]?.total || 0 });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

export default router;
