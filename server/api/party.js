import { Router } from 'express';
import db from '../duckdb/connection.js';

const router = Router();

// Helper to get the correct read function for JSONL files (supports .jsonl, .jsonl.gz, .jsonl.zst)
// Uses UNION for cross-platform compatibility (Windows doesn't support brace expansion)
const getUpdatesSource = () => `(
  SELECT * FROM read_json_auto('${db.DATA_PATH}/**/updates-*.jsonl', union_by_name=true, ignore_errors=true)
  UNION ALL
  SELECT * FROM read_json_auto('${db.DATA_PATH}/**/updates-*.jsonl.gz', union_by_name=true, ignore_errors=true)
  UNION ALL
  SELECT * FROM read_json_auto('${db.DATA_PATH}/**/updates-*.jsonl.zst', union_by_name=true, ignore_errors=true)
)`;

// GET /api/party/:partyId - Get all events for a party
router.get('/:partyId', async (req, res) => {
  try {
    const { partyId } = req.params;
    const limit = Math.min(parseInt(req.query.limit) || 100, 1000);
    
    // Search in signatories and observers arrays
    const sql = `
      SELECT *
      FROM ${getUpdatesSource()}
      WHERE 
        list_contains(signatories, '${partyId}')
        OR list_contains(observers, '${partyId}')
      ORDER BY timestamp DESC
      LIMIT ${limit}
    `;
    
    const rows = await db.safeQuery(sql);
    res.json({ data: rows, count: rows.length, party_id: partyId });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/party/:partyId/summary - Get party activity summary
router.get('/:partyId/summary', async (req, res) => {
  try {
    const { partyId } = req.params;
    
    const sql = `
      SELECT 
        event_type,
        COUNT(*) as count,
        MIN(timestamp) as first_seen,
        MAX(timestamp) as last_seen
      FROM ${getUpdatesSource()}
      WHERE 
        list_contains(signatories, '${partyId}')
        OR list_contains(observers, '${partyId}')
      GROUP BY event_type
      ORDER BY count DESC
    `;
    
    const rows = await db.safeQuery(sql);
    res.json({ data: rows, party_id: partyId });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/party/list/all - Get all unique parties
router.get('/list/all', async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 1000, 10000);
    
    const sql = `
      WITH all_parties AS (
        SELECT DISTINCT unnest(signatories) as party_id
        FROM ${getUpdatesSource()}
        UNION
        SELECT DISTINCT unnest(observers) as party_id
        FROM ${getUpdatesSource()}
      )
      SELECT party_id FROM all_parties
      WHERE party_id IS NOT NULL
      LIMIT ${limit}
    `;
    
    const rows = await db.safeQuery(sql);
    res.json({ data: rows.map(r => r.party_id), count: rows.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

export default router;
