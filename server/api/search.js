import { Router } from 'express';
import db from '../duckdb/connection.js';
import { 
  sanitizeNumber, 
  sanitizeIdentifier, 
  sanitizeEventType,
  sanitizeContractId,
  escapeLikePattern,
  buildLikeCondition,
  buildEqualCondition,
  containsDangerousPatterns,
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

// GET /api/search - Full text search across events
router.get('/', async (req, res) => {
  try {
    const { q, type, template, party } = req.query;
    const limit = sanitizeNumber(req.query.limit, { min: 1, max: 1000, defaultValue: 100 });
    const offset = sanitizeNumber(req.query.offset, { min: 0, max: 100000, defaultValue: 0 });
    
    let conditions = [];
    
    // Validate and sanitize search query - reject dangerous patterns
    if (q && typeof q === 'string' && q.length <= 500) {
      if (containsDangerousPatterns(q)) {
        return res.status(400).json({ error: 'Invalid search query' });
      }
      const escaped = escapeLikePattern(q);
      if (!escaped) {
        return res.status(400).json({ error: 'Invalid search query' });
      }
      conditions.push(`(
        contract_id LIKE '%${escaped}%' ESCAPE '\\'
        OR template_id LIKE '%${escaped}%' ESCAPE '\\'
        OR CAST(payload AS VARCHAR) LIKE '%${escaped}%' ESCAPE '\\'
      )`);
    }
    
    // Validate event type (whitelist approach)
    if (type) {
      const sanitizedType = sanitizeEventType(type);
      if (sanitizedType) {
        const condition = buildEqualCondition('event_type', sanitizedType);
        if (condition) conditions.push(condition);
      }
    }
    
    // Validate template filter
    if (template) {
      const sanitizedTemplate = sanitizeIdentifier(template);
      if (sanitizedTemplate) {
        const condition = buildLikeCondition('template_id', sanitizedTemplate);
        if (condition) conditions.push(condition);
      }
    }
    
    // Validate party filter - reject dangerous patterns
    if (party && typeof party === 'string' && party.length <= 500) {
      if (containsDangerousPatterns(party)) {
        return res.status(400).json({ error: 'Invalid party filter' });
      }
      const escaped = escapeLikePattern(party);
      if (!escaped) {
        return res.status(400).json({ error: 'Invalid party filter' });
      }
      // Use array_to_string for safer party matching
      conditions.push(`(
        array_to_string(signatories, ',') LIKE '%${escaped}%' ESCAPE '\\'
        OR array_to_string(observers, ',') LIKE '%${escaped}%' ESCAPE '\\'
      )`);
    }
    
    const whereClause = conditions.length > 0 
      ? `WHERE ${conditions.join(' AND ')}` 
      : '';
    
    const sql = `
      SELECT *
      FROM ${getUpdatesSource()}
      ${whereClause}
      ORDER BY timestamp DESC
      LIMIT ${limit}
      OFFSET ${offset}
    `;
    
    const rows = await db.safeQuery(sql);
    res.json({ data: rows, count: rows.length, query: { q, type, template, party } });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/search/contract/:id - Search by contract ID prefix
router.get('/contract/:id', async (req, res) => {
  try {
    const { id } = req.params;
    
    // Validate contract ID format using centralized sanitizer
    const sanitizedId = sanitizeContractId(id);
    if (!sanitizedId) {
      return res.status(400).json({ error: 'Invalid contract ID format' });
    }
    
    const escaped = escapeLikePattern(sanitizedId);
    if (!escaped) {
      return res.status(400).json({ error: 'Invalid contract ID format' });
    }
    
    const sql = `
      SELECT DISTINCT contract_id, template_id, MIN(timestamp) as created_at
      FROM ${getUpdatesSource()}
      WHERE contract_id LIKE '${escaped}%' ESCAPE '\\'
      GROUP BY contract_id, template_id
      ORDER BY created_at DESC
      LIMIT 50
    `;
    
    const rows = await db.safeQuery(sql);
    res.json({ data: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

export default router;