import { Router } from 'express';
import db from '../duckdb/connection.js';
import { 
  sanitizeNumber, 
  sanitizeIdentifier, 
  sanitizeEventType,
  escapeLikePattern,
  buildLikeCondition,
  buildEqualCondition,
} from '../lib/sql-sanitize.js';

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

// GET /api/search - Full text search across events
router.get('/', async (req, res) => {
  try {
    const { q, type, template, party } = req.query;
    const limit = sanitizeNumber(req.query.limit, { min: 1, max: 1000, defaultValue: 100 });
    const offset = sanitizeNumber(req.query.offset, { min: 0, defaultValue: 0 });
    
    let conditions = [];
    
    // Validate and sanitize search query
    if (q && typeof q === 'string' && q.length <= 500) {
      const escaped = escapeLikePattern(q);
      conditions.push(`(
        contract_id LIKE '%${escaped}%' ESCAPE '\\\\'
        OR template_id LIKE '%${escaped}%' ESCAPE '\\\\'
        OR CAST(payload AS VARCHAR) LIKE '%${escaped}%' ESCAPE '\\\\'
      )`);
    }
    
    // Validate event type
    if (type) {
      const sanitizedType = sanitizeEventType(type);
      if (sanitizedType) {
        conditions.push(buildEqualCondition('event_type', sanitizedType));
      }
    }
    
    // Validate template filter
    if (template) {
      const sanitizedTemplate = sanitizeIdentifier(template);
      if (sanitizedTemplate) {
        conditions.push(buildLikeCondition('template_id', sanitizedTemplate));
      }
    }
    
    // Validate party filter (party IDs are typically alphanumeric with some special chars)
    if (party && typeof party === 'string' && party.length <= 500) {
      const escaped = escapeLikePattern(party);
      // Use array_to_string for safer party matching
      conditions.push(`(
        array_to_string(signatories, ',') LIKE '%${escaped}%' ESCAPE '\\\\'
        OR array_to_string(observers, ',') LIKE '%${escaped}%' ESCAPE '\\\\'
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
    
    // Validate contract ID format (hex characters, dashes allowed)
    if (!id || typeof id !== 'string' || id.length > 200 || !/^[a-fA-F0-9:-]+$/.test(id)) {
      return res.status(400).json({ error: 'Invalid contract ID format' });
    }
    
    const escaped = escapeLikePattern(id);
    
    const sql = `
      SELECT DISTINCT contract_id, template_id, MIN(timestamp) as created_at
      FROM ${getUpdatesSource()}
      WHERE contract_id LIKE '${escaped}%' ESCAPE '\\\\'
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