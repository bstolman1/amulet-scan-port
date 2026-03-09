import { Router } from 'express';
import db from '../duckdb/connection.js';
// FIX: import sanitisation helpers (previously missing — caused SQL injection)
import {
  sanitizeNumber,
  sanitizeContractId,
  sanitizeIdentifier,
  escapeString,
  escapeLikePattern,
} from '../lib/sql-sanitize.js';

const router = Router();

// Helper to get the correct read function for JSONL files (supports .jsonl, .jsonl.gz, .jsonl.zst)
// Uses UNION for cross-platform compatibility (Windows doesn't support brace expansion)
// Use UNION (not UNION ALL) to prevent duplicate records
// In test mode, uses small fixture dataset to avoid scanning hundreds of files
const getUpdatesSource = () => {
  // In test mode, use test fixtures to avoid 415-file scans
  if (db.IS_TEST) {
    return `(SELECT * FROM read_json_auto('${db.TEST_FIXTURES_PATH}/updates-*.jsonl', union_by_name=true, ignore_errors=true))`;
  }
  
  const basePath = db.DATA_PATH.replace(/\\/g, '/');
  return `(
    SELECT * FROM read_json_auto('${basePath}/**/updates-*.jsonl', union_by_name=true, ignore_errors=true)
    UNION
    SELECT * FROM read_json_auto('${basePath}/**/updates-*.jsonl.gz', union_by_name=true, ignore_errors=true)
    UNION
    SELECT * FROM read_json_auto('${basePath}/**/updates-*.jsonl.zst', union_by_name=true, ignore_errors=true)
  )`;
};

// GET /api/contracts/:contractId - Get contract lifecycle
router.get('/:contractId', async (req, res) => {
  try {
    const { contractId } = req.params;

    // FIX: validate contractId before interpolation.
    // Previously `'${contractId}'` was used directly — a classic SQL injection vector
    // (e.g. `' OR 1=1 --`).  Other modules (events.js, search.js) already used
    // sanitizeContractId(); this brings contracts.js into line.
    const sanitized = sanitizeContractId(contractId);
    if (!sanitized) {
      return res.status(400).json({ error: 'Invalid contract ID format' });
    }

    const sql = `
      SELECT *
      FROM ${getUpdatesSource()}
      WHERE contract_id = '${escapeString(sanitized)}'
      ORDER BY COALESCE(timestamp, effective_at) ASC
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
    // FIX: sanitize limit via helper instead of raw parseInt (consistent with other modules)
    const limit = sanitizeNumber(req.query.limit, { min: 1, max: 1000, defaultValue: 100 });

    // FIX: validate templateSuffix before interpolation.
    // Previously `'%${templateSuffix}'` was used directly — same SQL injection risk.
    const sanitizedSuffix = sanitizeIdentifier(templateSuffix);
    if (!sanitizedSuffix) {
      return res.status(400).json({ error: 'Invalid template suffix format' });
    }
    const escapedSuffix = escapeLikePattern(sanitizedSuffix);
    
    // Find contracts that have been created but not archived
    const sql = `
      WITH created AS (
        SELECT contract_id, template_id, effective_at as created_at, payload
        FROM ${getUpdatesSource()}
        WHERE event_type = 'created' AND template_id LIKE '%${escapedSuffix}' ESCAPE '\\\\'
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
