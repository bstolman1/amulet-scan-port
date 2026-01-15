import { Router } from 'express';
import db from '../duckdb/connection.js';
import {
  sanitizeNumber,
  sanitizeEventType,
  sanitizeIdentifier,
  sanitizeTimestamp,
  escapeLikePattern,
  escapeString,
} from '../lib/sql-sanitize.js';

const router = Router();

// Helper to convert BigInt values to numbers for JSON serialization
function convertBigInts(obj) {
  if (obj === null || obj === undefined) return obj;
  if (typeof obj === 'bigint') return Number(obj);
  if (Array.isArray(obj)) return obj.map(convertBigInts);
  if (typeof obj === 'object') {
    const result = {};
    for (const [key, value] of Object.entries(obj)) {
      result[key] = convertBigInts(value);
    }
    return result;
  }
  return obj;
}

/**
 * Get DuckDB source for events (Parquet files via glob)
 * Parquet is the single source of truth - no binary fallback
 */
const getEventsSource = () => {
  if (db.IS_TEST) {
    return `(SELECT * FROM read_json_auto('${db.TEST_FIXTURES_PATH}/events-*.jsonl', union_by_name=true, ignore_errors=true))`;
  }
  
  const basePath = db.DATA_PATH.replace(/\\/g, '/');
  const hasParquet = db.hasFileType('events', '.parquet');
  
  if (hasParquet) {
    return `read_parquet('${basePath}/**/events-*.parquet', union_by_name=true)`;
  }
  
  // Fallback to JSONL if no Parquet (legacy data only)
  const hasJsonl = db.hasFileType('events', '.jsonl');
  const hasGzip = db.hasFileType('events', '.jsonl.gz');
  const hasZstd = db.hasFileType('events', '.jsonl.zst');
  
  if (!hasJsonl && !hasGzip && !hasZstd) {
    return `(SELECT NULL::VARCHAR as event_id, NULL::VARCHAR as event_type, NULL::VARCHAR as contract_id, 
             NULL::VARCHAR as template_id, NULL::VARCHAR as package_name,
             NULL::TIMESTAMP as timestamp, NULL::TIMESTAMP as effective_at,
             NULL::VARCHAR[] as signatories, NULL::VARCHAR[] as observers, NULL::JSON as payload WHERE false)`;
  }
  
  const queries = [];
  if (hasJsonl) queries.push(`SELECT * FROM read_json_auto('${basePath}/**/events-*.jsonl', union_by_name=true, ignore_errors=true)`);
  if (hasGzip) queries.push(`SELECT * FROM read_json_auto('${basePath}/**/events-*.jsonl.gz', union_by_name=true, ignore_errors=true)`);
  if (hasZstd) queries.push(`SELECT * FROM read_json_auto('${basePath}/**/events-*.jsonl.zst', union_by_name=true, ignore_errors=true)`);
  
  return `(${queries.join(' UNION ALL ')})`;
};

/**
 * Get data source info for API responses
 */
function getDataSourceInfo() {
  const hasParquet = db.hasFileType('events', '.parquet');
  return {
    source: hasParquet ? 'parquet' : 'jsonl',
    engine: 'duckdb',
  };
}

// GET /api/events/latest - Get latest events
router.get('/latest', async (req, res) => {
  try {
    const limit = sanitizeNumber(req.query.limit, { min: 1, max: 1000, defaultValue: 100 });
    const offset = sanitizeNumber(req.query.offset, { min: 0, max: 100000, defaultValue: 0 });
    const sourceInfo = getDataSourceInfo();
    
    const sql = `
      SELECT 
        event_id, update_id, event_type, contract_id, template_id, package_name,
        migration_id, synchronizer_id, timestamp, effective_at, signatories, observers, payload
      FROM ${getEventsSource()}
      ORDER BY COALESCE(timestamp, effective_at) DESC NULLS LAST
      LIMIT ${limit}
      OFFSET ${offset}
    `;
    
    const rows = await db.safeQuery(sql);
    res.json(convertBigInts({ data: rows, count: rows.length, ...sourceInfo }));
  } catch (err) {
    console.error('Error fetching latest events:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/by-type/:type - Get events by type
router.get('/by-type/:type', async (req, res) => {
  try {
    const { type } = req.params;
    const sanitizedType = sanitizeEventType(type);
    if (!sanitizedType) {
      return res.status(400).json({ error: 'Invalid event type' });
    }
    
    const limit = sanitizeNumber(req.query.limit, { min: 1, max: 1000, defaultValue: 100 });
    const offset = sanitizeNumber(req.query.offset, { min: 0, defaultValue: 0 });
    const sourceInfo = getDataSourceInfo();
    
    const sql = `
      SELECT *
      FROM ${getEventsSource()}
      WHERE event_type = '${escapeString(sanitizedType)}'
      ORDER BY COALESCE(timestamp, effective_at) DESC NULLS LAST
      LIMIT ${limit}
      OFFSET ${offset}
    `;
    
    const rows = await db.safeQuery(sql);
    res.json(convertBigInts({ data: rows, count: rows.length, ...sourceInfo }));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/by-template/:templateId - Get events by template
router.get('/by-template/:templateId', async (req, res) => {
  try {
    const { templateId } = req.params;
    const sanitizedTemplateId = sanitizeIdentifier(templateId);
    if (!sanitizedTemplateId) {
      return res.status(400).json({ error: 'Invalid template ID format' });
    }
    
    const limit = sanitizeNumber(req.query.limit, { min: 1, max: 1000, defaultValue: 100 });
    const offset = sanitizeNumber(req.query.offset, { min: 0, defaultValue: 0 });
    const sourceInfo = getDataSourceInfo();
    
    const escaped = escapeLikePattern(sanitizedTemplateId);
    const sql = `
      SELECT *
      FROM ${getEventsSource()}
      WHERE template_id LIKE '%${escaped}%' ESCAPE '\\\\'
      ORDER BY COALESCE(timestamp, effective_at) DESC NULLS LAST
      LIMIT ${limit}
      OFFSET ${offset}
    `;
    
    const rows = await db.safeQuery(sql);
    res.json(convertBigInts({ data: rows, count: rows.length, ...sourceInfo }));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/by-date - Get events for a specific date range
router.get('/by-date', async (req, res) => {
  try {
    const { start, end } = req.query;
    const limit = sanitizeNumber(req.query.limit, { min: 1, max: 1000, defaultValue: 100 });
    const offset = sanitizeNumber(req.query.offset, { min: 0, defaultValue: 0 });
    const sourceInfo = getDataSourceInfo();
    
    const sanitizedStart = start ? sanitizeTimestamp(start) : null;
    const sanitizedEnd = end ? sanitizeTimestamp(end) : null;
    
    if (start && !sanitizedStart) {
      return res.status(400).json({ error: 'Invalid start date format' });
    }
    if (end && !sanitizedEnd) {
      return res.status(400).json({ error: 'Invalid end date format' });
    }
    
    let whereClause = '';
    if (sanitizedStart) whereClause += ` AND COALESCE(timestamp, effective_at) >= '${escapeString(sanitizedStart)}'`;
    if (sanitizedEnd) whereClause += ` AND COALESCE(timestamp, effective_at) <= '${escapeString(sanitizedEnd)}'`;
    
    const sql = `
      SELECT *
      FROM ${getEventsSource()}
      WHERE 1=1 ${whereClause}
      ORDER BY COALESCE(timestamp, effective_at) DESC NULLS LAST
      LIMIT ${limit}
      OFFSET ${offset}
    `;
    
    const rows = await db.safeQuery(sql);
    res.json(convertBigInts({ data: rows, count: rows.length, ...sourceInfo }));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/count - Get total event count
router.get('/count', async (req, res) => {
  try {
    const sourceInfo = getDataSourceInfo();
    const sql = `SELECT COUNT(*) as total FROM ${getEventsSource()}`;
    const rows = await db.safeQuery(sql);
    res.json(convertBigInts({ count: rows[0]?.total || 0, ...sourceInfo }));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/debug - Debug endpoint showing data sources
router.get('/debug', async (req, res) => {
  try {
    const sourceInfo = getDataSourceInfo();
    const hasParquet = db.hasFileType('events', '.parquet');
    const hasJsonl = db.hasFileType('events', '.jsonl');
    
    // Get sample record via DuckDB
    let sampleRecord = null;
    try {
      const sql = `SELECT * FROM ${getEventsSource()} LIMIT 1`;
      const rows = await db.safeQuery(sql);
      sampleRecord = rows[0] || null;
    } catch (e) {
      sampleRecord = { error: e.message };
    }
    
    res.json(convertBigInts({
      dataPath: db.DATA_PATH,
      ...sourceInfo,
      hasParquetFiles: hasParquet,
      hasJsonlFiles: hasJsonl,
      sampleRecord,
    }));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/governance - Get governance-related events via DuckDB analytical query
router.get('/governance', async (req, res) => {
  try {
    const limit = sanitizeNumber(req.query.limit, { min: 1, max: 1000, defaultValue: 200 });
    const offset = sanitizeNumber(req.query.offset, { min: 0, max: 100000, defaultValue: 0 });
    const sourceInfo = getDataSourceInfo();
    
    const governanceTemplates = ['VoteRequest', 'Confirmation', 'DsoRules', 'AmuletRules', 'AmuletPriceVote'];
    const templateFilter = governanceTemplates.map(t => `template_id LIKE '%${t}%'`).join(' OR ');
    
    const sql = `
      SELECT *
      FROM ${getEventsSource()}
      WHERE ${templateFilter}
      ORDER BY COALESCE(timestamp, effective_at) DESC NULLS LAST
      LIMIT ${limit}
      OFFSET ${offset}
    `;
    
    const rows = await db.safeQuery(sql);
    res.json(convertBigInts({ data: rows, count: rows.length, ...sourceInfo }));
  } catch (err) {
    console.error('Error fetching governance events:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/rewards - Get reward-related events via DuckDB analytical query
router.get('/rewards', async (req, res) => {
  try {
    const limit = sanitizeNumber(req.query.limit, { min: 1, max: 2000, defaultValue: 500 });
    const offset = sanitizeNumber(req.query.offset, { min: 0, max: 100000, defaultValue: 0 });
    const sourceInfo = getDataSourceInfo();
    
    const rewardTemplates = ['RewardCoupon', 'AppRewardCoupon', 'ValidatorRewardCoupon', 'SvRewardCoupon'];
    const templateFilter = rewardTemplates.map(t => `template_id LIKE '%${t}%'`).join(' OR ');
    
    const sql = `
      SELECT *
      FROM ${getEventsSource()}
      WHERE ${templateFilter}
      ORDER BY COALESCE(timestamp, effective_at) DESC NULLS LAST
      LIMIT ${limit}
      OFFSET ${offset}
    `;
    
    const rows = await db.safeQuery(sql);
    res.json(convertBigInts({ data: rows, count: rows.length, ...sourceInfo }));
  } catch (err) {
    console.error('Error fetching reward events:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/member-traffic - Get member traffic events via DuckDB
router.get('/member-traffic', async (req, res) => {
  try {
    const limit = sanitizeNumber(req.query.limit, { min: 1, max: 1000, defaultValue: 200 });
    const offset = sanitizeNumber(req.query.offset, { min: 0, max: 100000, defaultValue: 0 });
    const sourceInfo = getDataSourceInfo();
    
    const sql = `
      SELECT *
      FROM ${getEventsSource()}
      WHERE template_id LIKE '%MemberTraffic%'
      ORDER BY COALESCE(timestamp, effective_at) DESC NULLS LAST
      LIMIT ${limit}
      OFFSET ${offset}
    `;
    
    const rows = await db.safeQuery(sql);
    res.json(convertBigInts({ data: rows, count: rows.length, ...sourceInfo }));
  } catch (err) {
    console.error('Error fetching member traffic events:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/by-contract/:contractId - Get all events for a specific contract
router.get('/by-contract/:contractId', async (req, res) => {
  try {
    const { contractId } = req.params;
    const limit = sanitizeNumber(req.query.limit, { min: 1, max: 1000, defaultValue: 100 });
    const sourceInfo = getDataSourceInfo();
    
    const sql = `
      SELECT *
      FROM ${getEventsSource()}
      WHERE contract_id = '${escapeString(contractId)}'
      ORDER BY COALESCE(timestamp, effective_at) DESC NULLS LAST
      LIMIT ${limit}
    `;
    
    const rows = await db.safeQuery(sql);
    res.json(convertBigInts({ data: rows, count: rows.length, ...sourceInfo }));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/sources - Check what data sources are available
router.get('/sources', (req, res) => {
  try {
    const hasParquet = db.hasFileType('events', '.parquet');
    const hasJsonl = db.hasFileType('events', '.jsonl');
    res.json({
      primarySource: hasParquet ? 'parquet' : 'jsonl',
      hasParquet,
      hasJsonl,
      engine: 'duckdb',
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ============= SV Node API endpoints (live queries to SV node) =============
const SV_API_BASE = process.env.SV_API_BASE || 'https://sv.sv-1.global.canton.network.sync.global/api/sv';

// GET /api/events/sv-node/vote-requests - Fetch active vote requests from SV node
router.get('/sv-node/vote-requests', async (req, res) => {
  try {
    const url = `${SV_API_BASE}/admin/sv/voterequests`;
    console.log(`[SV-NODE] Fetching from: ${url}`);
    
    const response = await fetch(url);
    
    if (!response.ok) {
      const text = await response.text();
      console.error(`[SV-NODE] Error: ${response.status} - ${text.slice(0, 500)}`);
      return res.status(response.status).json({ error: text.slice(0, 200), url });
    }
    
    const data = await response.json();
    const requests = data.dso_rules_vote_requests || [];
    
    res.json({
      vote_requests: requests,
      count: requests.length,
      source: 'sv-node-live',
      fetched_at: new Date().toISOString(),
    });
  } catch (err) {
    console.error('[SV-NODE] Error fetching vote requests:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/sv-node/vote-results - Fetch vote results from SV node
router.get('/sv-node/vote-results', async (req, res) => {
  try {
    const { accepted, requester, effectiveFrom, effectiveTo, limit } = req.query;
    const url = `${SV_API_BASE}/admin/sv/voteresults`;
    
    const response = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        accepted: accepted !== undefined ? accepted === 'true' : undefined,
        requester: requester || undefined,
        effectiveFrom: effectiveFrom || undefined,
        effectiveTo: effectiveTo || undefined,
        limit: parseInt(limit) || 100,
      }),
    });
    
    if (!response.ok) {
      const text = await response.text();
      return res.status(response.status).json({ error: text.slice(0, 500), url });
    }
    
    const data = await response.json();
    res.json({
      vote_results: data.dso_rules_vote_results || [],
      count: data.dso_rules_vote_results?.length || 0,
      source: 'sv-node-live',
      fetched_at: new Date().toISOString(),
    });
  } catch (err) {
    console.error('[SV-NODE] Error fetching vote results:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/sv-node/all-proposals - Fetch all proposals (active + historical)
router.get('/sv-node/all-proposals', async (req, res) => {
  try {
    const [activeResponse, acceptedResponse, rejectedResponse] = await Promise.all([
      fetch(`${SV_API_BASE}/admin/sv/voterequests`),
      fetch(`${SV_API_BASE}/admin/sv/voteresults`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ accepted: true, limit: 1000 }),
      }),
      fetch(`${SV_API_BASE}/admin/sv/voteresults`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ accepted: false, limit: 1000 }),
      }),
    ]);
    
    if (!activeResponse.ok) {
      const text = await activeResponse.text();
      return res.status(activeResponse.status).json({ error: text.slice(0, 200) });
    }
    
    const activeData = await activeResponse.json();
    let acceptedData = { dso_rules_vote_results: [] };
    let rejectedData = { dso_rules_vote_results: [] };
    
    if (acceptedResponse.ok) acceptedData = await acceptedResponse.json();
    if (rejectedResponse.ok) rejectedData = await rejectedResponse.json();
    
    const activeRequests = activeData.dso_rules_vote_requests || [];
    const acceptedResults = acceptedData.dso_rules_vote_results || [];
    const rejectedResults = rejectedData.dso_rules_vote_results || [];
    
    const activeProposals = activeRequests.map(vr => ({
      contract_id: vr.contract_id,
      template_id: vr.template_id,
      status: 'in_progress',
      payload: vr.payload,
      created_at: vr.created_at,
      source_type: 'active_request',
    }));
    
    const historicalProposals = [...acceptedResults, ...rejectedResults].map(vr => ({
      contract_id: vr.request?.tracking_cid || vr.contract_id || 'unknown',
      template_id: vr.request?.template_id || 'unknown',
      status: vr.outcome?.accepted ? 'executed' : 'rejected',
      payload: vr.request,
      outcome: vr.outcome,
      effective_at: vr.outcome?.effective_at || vr.request?.vote_before,
      source_type: 'vote_result',
    }));
    
    const proposalMap = new Map();
    for (const p of activeProposals) proposalMap.set(p.contract_id, p);
    for (const p of historicalProposals) proposalMap.set(p.contract_id, p);
    
    const allProposals = Array.from(proposalMap.values());
    
    res.json({
      proposals: allProposals,
      stats: {
        total: allProposals.length,
        active: activeRequests.length,
        accepted: acceptedResults.length,
        rejected: rejectedResults.length,
      },
      source: 'sv-node-live',
      fetched_at: new Date().toISOString(),
    });
  } catch (err) {
    console.error('[SV-NODE] Error fetching all proposals:', err);
    res.status(500).json({ error: err.message });
  }
});

export default router;
