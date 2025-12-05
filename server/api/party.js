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

// GET /api/party/:partyId - Get all events for a party
router.get('/:partyId', async (req, res) => {
  try {
    const { partyId } = req.params;
    const limit = Math.min(parseInt(req.query.limit) || 100, 1000);
    
    const sql = (source) => `
      SELECT *
      FROM ${source}
      WHERE 
        list_contains(signatories, '${partyId}')
        OR list_contains(observers, '${partyId}')
      ORDER BY timestamp DESC
      LIMIT ${limit}
    `;
    
    const rows = await queryWithFallback(sql(getParquetSource()), sql(getJsonlSource()));
    res.json({ data: rows, count: rows.length, party_id: partyId });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/party/:partyId/summary - Get party activity summary
router.get('/:partyId/summary', async (req, res) => {
  try {
    const { partyId } = req.params;
    
    const sql = (source) => `
      SELECT 
        event_type,
        COUNT(*) as count,
        MIN(timestamp) as first_seen,
        MAX(timestamp) as last_seen
      FROM ${source}
      WHERE 
        list_contains(signatories, '${partyId}')
        OR list_contains(observers, '${partyId}')
      GROUP BY event_type
      ORDER BY count DESC
    `;
    
    const rows = await queryWithFallback(sql(getParquetSource()), sql(getJsonlSource()));
    res.json({ data: rows, party_id: partyId });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/party/list/all - Get all unique parties
router.get('/list/all', async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 1000, 10000);
    
    const sql = (source) => `
      WITH all_parties AS (
        SELECT DISTINCT unnest(signatories) as party_id
        FROM ${source}
        UNION
        SELECT DISTINCT unnest(observers) as party_id
        FROM ${source}
      )
      SELECT party_id FROM all_parties
      WHERE party_id IS NOT NULL
      LIMIT ${limit}
    `;
    
    const rows = await queryWithFallback(sql(getParquetSource()), sql(getJsonlSource()));
    res.json({ data: rows.map(r => r.party_id), count: rows.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

export default router;
