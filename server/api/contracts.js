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

// GET /api/contracts/:contractId - Get contract lifecycle
router.get('/:contractId', async (req, res) => {
  try {
    const { contractId } = req.params;
    
    const sql = (source) => `
      SELECT *
      FROM ${source}
      WHERE contract_id = '${contractId}'
      ORDER BY timestamp ASC
    `;
    
    const rows = await queryWithFallback(sql(getParquetSource()), sql(getJsonlSource()));
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
    
    const sql = (source) => `
      WITH created AS (
        SELECT contract_id, template_id, timestamp as created_at, payload
        FROM ${source}
        WHERE event_type = 'created' AND template_id LIKE '%${templateSuffix}'
      ),
      archived AS (
        SELECT DISTINCT contract_id
        FROM ${source}
        WHERE event_type = 'archived'
      )
      SELECT c.*
      FROM created c
      LEFT JOIN archived a ON c.contract_id = a.contract_id
      WHERE a.contract_id IS NULL
      ORDER BY c.created_at DESC
      LIMIT ${limit}
    `;
    
    const rows = await queryWithFallback(sql(getParquetSource()), sql(getJsonlSource()));
    res.json({ data: rows, count: rows.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/contracts/templates - List all unique templates
router.get('/templates/list', async (req, res) => {
  try {
    const sql = (source) => `
      SELECT 
        template_id,
        COUNT(*) as event_count,
        COUNT(DISTINCT contract_id) as contract_count
      FROM ${source}
      WHERE template_id IS NOT NULL
      GROUP BY template_id
      ORDER BY contract_count DESC
    `;
    
    const rows = await queryWithFallback(sql(getParquetSource()), sql(getJsonlSource()));
    res.json({ data: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

export default router;
