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

// GET /api/search - Full text search across events
router.get('/', async (req, res) => {
  try {
    const { q, type, template, party } = req.query;
    const limit = Math.min(parseInt(req.query.limit) || 100, 1000);
    
    let conditions = [];
    
    if (q) {
      conditions.push(`(
        contract_id LIKE '%${q}%' 
        OR template_id LIKE '%${q}%'
        OR CAST(payload AS VARCHAR) LIKE '%${q}%'
      )`);
    }
    
    if (type) {
      conditions.push(`event_type = '${type}'`);
    }
    
    if (template) {
      conditions.push(`template_id LIKE '%${template}%'`);
    }
    
    if (party) {
      conditions.push(`(list_contains(signatories, '${party}') OR list_contains(observers, '${party}'))`);
    }
    
    const whereClause = conditions.length > 0 
      ? `WHERE ${conditions.join(' AND ')}` 
      : '';
    
    const sql = (source) => `
      SELECT *
      FROM ${source}
      ${whereClause}
      ORDER BY timestamp DESC
      LIMIT ${limit}
    `;
    
    const rows = await queryWithFallback(sql(getParquetSource()), sql(getJsonlSource()));
    res.json({ data: rows, count: rows.length, query: { q, type, template, party } });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/search/contract/:id - Search by contract ID prefix
router.get('/contract/:id', async (req, res) => {
  try {
    const { id } = req.params;
    
    const sql = (source) => `
      SELECT DISTINCT contract_id, template_id, MIN(timestamp) as created_at
      FROM ${source}
      WHERE contract_id LIKE '${id}%'
      GROUP BY contract_id, template_id
      ORDER BY created_at DESC
      LIMIT 50
    `;
    
    const rows = await queryWithFallback(sql(getParquetSource()), sql(getJsonlSource()));
    res.json({ data: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

export default router;
