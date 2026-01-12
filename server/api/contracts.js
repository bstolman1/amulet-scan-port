import { Router } from 'express';
import db from '../duckdb/connection.js';

const router = Router();

// Helper to get the correct read function for JSONL files (supports .jsonl, .jsonl.gz, .jsonl.zst)
// Uses UNION for cross-platform compatibility (Windows doesn't support brace expansion)
// Use UNION (not UNION ALL) to prevent duplicate records
const getUpdatesSource = () => `(
  SELECT * FROM read_json_auto('${db.DATA_PATH}/**/updates-*.jsonl', union_by_name=true, ignore_errors=true)
  UNION
  SELECT * FROM read_json_auto('${db.DATA_PATH}/**/updates-*.jsonl.gz', union_by_name=true, ignore_errors=true)
  UNION
  SELECT * FROM read_json_auto('${db.DATA_PATH}/**/updates-*.jsonl.zst', union_by_name=true, ignore_errors=true)
)`;

// GET /api/contracts/:contractId - Get contract lifecycle
router.get('/:contractId', async (req, res) => {
  try {
    const { contractId } = req.params;
    
    const sql = `
      SELECT *
      FROM ${getUpdatesSource()}
      WHERE contract_id = '${contractId}'
      ORDER BY effective_at ASC
    `;
    
    const rows = await db.safeQuery(sql);
    res.json({ data: rows, contract_id: contractId });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/contracts/active/by-template/:templateSuffix - Get active contracts by template
router.get('/active/by-template/:templateSuffix', async (req, res) => {
  try {
    const { templateSuffix } = req.params;
    const limit = Math.min(parseInt(req.query.limit) || 100, 1000);
    
    // Find contracts that have been created but not archived
    const sql = `
      WITH created AS (
        SELECT contract_id, template_id, effective_at as created_at, payload
        FROM ${getUpdatesSource()}
        WHERE event_type = 'created' AND template_id LIKE '%${templateSuffix}'
      ),
      archived AS (
        SELECT DISTINCT contract_id
        FROM ${getUpdatesSource()}
        WHERE event_type = 'archived'
      )
      SELECT c.*
      FROM created c
      LEFT JOIN archived a ON c.contract_id = a.contract_id
      WHERE a.contract_id IS NULL
      ORDER BY c.created_at DESC
      LIMIT ${limit}
    `;
    
    const rows = await db.safeQuery(sql);
    res.json({ data: rows, count: rows.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/contracts/templates - List all unique templates
router.get('/templates/list', async (req, res) => {
  try {
    const sql = `
      SELECT 
        template_id,
        COUNT(*) as event_count,
        COUNT(DISTINCT contract_id) as contract_count
      FROM ${getUpdatesSource()}
      WHERE template_id IS NOT NULL
      GROUP BY template_id
      ORDER BY contract_count DESC
    `;
    
    const rows = await db.safeQuery(sql);
    res.json({ data: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

export default router;
