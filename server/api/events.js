import { Router } from 'express';
import fs from 'fs';
import path from 'path';
import db from '../duckdb/connection.js';
import binaryReader from '../duckdb/binary-reader.js';
import {
  sanitizeNumber,
  sanitizeEventType,
  sanitizeIdentifier,
  sanitizeTimestamp,
  sanitizeContractId,
  escapeLikePattern,
  escapeString,
  containsDangerousPatterns,
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
 * Helper to get raw event data as an object.
 */
function getRawEvent(event) {
  if (!event) return {};
  if (event.raw_event) {
    if (typeof event.raw_event === 'string') {
      try {
        return JSON.parse(event.raw_event);
      } catch {
        return {};
      }
    }
    return event.raw_event;
  }
  if (event.raw && typeof event.raw === 'object') {
    return event.raw;
  }
  return {};
}

// Helper to get the correct read function for Parquet files (primary) or JSONL files (fallback)
const getEventsSource = () => {
  if (db.IS_TEST) {
    return `(SELECT * FROM read_json_auto('${db.TEST_FIXTURES_PATH}/events-*.jsonl', union_by_name=true, ignore_errors=true))`;
  }
  
  const hasParquet = db.hasFileType('events', '.parquet');
  if (hasParquet) {
    return `read_parquet('${db.DATA_PATH.replace(/\\/g, '/')}/**/events-*.parquet', union_by_name=true)`;
  }
  
  const hasJsonl = db.hasFileType('events', '.jsonl');
  const hasGzip = db.hasFileType('events', '.jsonl.gz');
  const hasZstd = db.hasFileType('events', '.jsonl.zst');
  
  if (!hasJsonl && !hasGzip && !hasZstd) {
    return `(SELECT NULL::VARCHAR as event_id, NULL::VARCHAR as event_type, NULL::VARCHAR as contract_id, 
             NULL::VARCHAR as template_id, NULL::VARCHAR as package_name,
             NULL::TIMESTAMP as timestamp, NULL::TIMESTAMP as effective_at,
             NULL::VARCHAR[] as signatories, NULL::VARCHAR[] as observers, NULL::JSON as payload WHERE false)`;
  }
  
  const basePath = db.DATA_PATH.replace(/\\/g, '/');
  const queries = [];
  if (hasJsonl) queries.push(`SELECT * FROM read_json_auto('${basePath}/**/events-*.jsonl', union_by_name=true, ignore_errors=true)`);
  if (hasGzip) queries.push(`SELECT * FROM read_json_auto('${basePath}/**/events-*.jsonl.gz', union_by_name=true, ignore_errors=true)`);
  if (hasZstd) queries.push(`SELECT * FROM read_json_auto('${basePath}/**/events-*.jsonl.zst', union_by_name=true, ignore_errors=true)`);
  
  return `(${queries.join(' UNION ')})`;
};

// Check what data sources are available
function getDataSources() {
  const hasBinaryEvents = binaryReader.hasBinaryFiles(db.DATA_PATH, 'events');
  const hasParquetEvents = db.hasFileType('events', '.parquet');
  return { 
    hasBinaryEvents, 
    hasParquetEvents,
    primarySource: hasBinaryEvents ? 'binary' : hasParquetEvents ? 'parquet' : 'jsonl' 
  };
}

// GET /api/events/latest - Get latest events
router.get('/latest', async (req, res) => {
  try {
    const limit = sanitizeNumber(req.query.limit, { min: 1, max: 1000, defaultValue: 100 });
    const offset = sanitizeNumber(req.query.offset, { min: 0, max: 100000, defaultValue: 0 });
    const sources = getDataSources();
    
    if (sources.primarySource === 'binary') {
      const result = await binaryReader.streamRecords(db.DATA_PATH, 'events', {
        limit,
        offset,
        maxDays: 7,
        maxFilesToScan: 200,
        sortBy: 'timestamp',
      });
      return res.json(convertBigInts({ data: result.records, count: result.records.length, hasMore: result.hasMore, source: 'binary' }));
    }
    
    const sql = `
      SELECT 
        event_id, update_id, event_type, contract_id, template_id, package_name,
        migration_id, synchronizer_id, timestamp, effective_at, signatories, observers, payload
      FROM ${getEventsSource()}
      ORDER BY COALESCE(timestamp, effective_at) DESC
      LIMIT ${limit}
      OFFSET ${offset}
    `;
    
    const rows = await db.safeQuery(sql);
    res.json(convertBigInts({ data: rows, count: rows.length, source: sources.primarySource }));
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
    const sources = getDataSources();
    
    if (sources.primarySource === 'binary') {
      const result = await binaryReader.streamRecords(db.DATA_PATH, 'events', {
        limit,
        offset,
        maxDays: 30,
        maxFilesToScan: 200,
        sortBy: 'effective_at',
        filter: (e) => e.event_type === sanitizedType
      });
      return res.json(convertBigInts({ data: result.records, count: result.records.length, hasMore: result.hasMore, source: 'binary' }));
    }
    
    const sql = `
      SELECT *
      FROM ${getEventsSource()}
      WHERE event_type = '${escapeString(sanitizedType)}'
      ORDER BY COALESCE(timestamp, effective_at) DESC
      LIMIT ${limit}
    `;
    
    const rows = await db.safeQuery(sql);
    res.json(convertBigInts({ data: rows, count: rows.length, source: sources.primarySource }));
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
    const sources = getDataSources();
    
    if (sources.primarySource === 'binary') {
      const result = await binaryReader.streamRecords(db.DATA_PATH, 'events', {
        limit,
        offset,
        maxDays: 30,
        maxFilesToScan: 200,
        sortBy: 'effective_at',
        filter: (e) => e.template_id?.includes(sanitizedTemplateId)
      });
      return res.json(convertBigInts({ data: result.records, count: result.records.length, hasMore: result.hasMore, source: 'binary' }));
    }
    
    const escaped = escapeLikePattern(sanitizedTemplateId);
    const sql = `
      SELECT *
      FROM ${getEventsSource()}
      WHERE template_id LIKE '%${escaped}%' ESCAPE '\\\\'
      ORDER BY COALESCE(timestamp, effective_at) DESC
      LIMIT ${limit}
    `;
    
    const rows = await db.safeQuery(sql);
    res.json(convertBigInts({ data: rows, count: rows.length, source: sources.primarySource }));
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
    const sources = getDataSources();
    
    const sanitizedStart = start ? sanitizeTimestamp(start) : null;
    const sanitizedEnd = end ? sanitizeTimestamp(end) : null;
    
    if (start && !sanitizedStart) {
      return res.status(400).json({ error: 'Invalid start date format' });
    }
    if (end && !sanitizedEnd) {
      return res.status(400).json({ error: 'Invalid end date format' });
    }
    
    if (sources.primarySource === 'binary') {
      const startDate = sanitizedStart ? new Date(sanitizedStart).getTime() : 0;
      const endDate = sanitizedEnd ? new Date(sanitizedEnd).getTime() : Date.now();
      
      const result = await binaryReader.streamRecords(db.DATA_PATH, 'events', {
        limit,
        offset,
        maxDays: 90,
        maxFilesToScan: 300,
        sortBy: 'effective_at',
        filter: (e) => {
          if (!e.effective_at) return false;
          const ts = new Date(e.effective_at).getTime();
          return ts >= startDate && ts <= endDate;
        }
      });
      return res.json(convertBigInts({ data: result.records, count: result.records.length, hasMore: result.hasMore, source: 'binary' }));
    }
    
    let whereClause = '';
    if (sanitizedStart) whereClause += ` AND COALESCE(timestamp, effective_at) >= '${escapeString(sanitizedStart)}'`;
    if (sanitizedEnd) whereClause += ` AND COALESCE(timestamp, effective_at) <= '${escapeString(sanitizedEnd)}'`;
    
    const sql = `
      SELECT *
      FROM ${getEventsSource()}
      WHERE 1=1 ${whereClause}
      ORDER BY COALESCE(timestamp, effective_at) DESC
      LIMIT ${limit}
    `;
    
    const rows = await db.safeQuery(sql);
    res.json(convertBigInts({ data: rows, count: rows.length, source: sources.primarySource }));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/count - Get total event count
router.get('/count', async (req, res) => {
  try {
    const sources = getDataSources();
    
    if (sources.primarySource === 'binary') {
      const fileCount = binaryReader.countBinaryFiles(db.DATA_PATH, 'events');
      const estimated = fileCount * 100;
      return res.json(convertBigInts({ count: estimated, estimated: true, fileCount, source: 'binary' }));
    }
    
    const sql = `SELECT COUNT(*) as total FROM ${getEventsSource()}`;
    const rows = await db.safeQuery(sql);
    res.json(convertBigInts({ count: rows[0]?.total || 0, source: sources.primarySource }));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/debug - Debug endpoint
router.get('/debug', async (req, res) => {
  try {
    const sources = getDataSources();
    const newestFiles = binaryReader.findBinaryFilesFast(db.DATA_PATH, 'events', { maxDays: 7, maxFiles: 10 });
    const fileCount = binaryReader.countBinaryFiles(db.DATA_PATH, 'events');
    
    let sampleRecord = null;
    if (newestFiles.length > 0) {
      try {
        const result = await binaryReader.readBinaryFile(newestFiles[0]);
        sampleRecord = result.records[0] || null;
      } catch (e) {
        sampleRecord = { error: e.message };
      }
    }
    
    res.json(convertBigInts({
      dataPath: db.DATA_PATH,
      sources,
      totalBinaryFiles: fileCount,
      newestByDataDate: newestFiles.slice(0, 5),
      sampleNewestRecord: sampleRecord,
    }));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/governance - Get governance-related events
router.get('/governance', async (req, res) => {
  try {
    const limit = sanitizeNumber(req.query.limit, { min: 1, max: 1000, defaultValue: 200 });
    const offset = sanitizeNumber(req.query.offset, { min: 0, max: 100000, defaultValue: 0 });
    const sources = getDataSources();
    
    const governanceTemplates = ['VoteRequest', 'Confirmation', 'DsoRules', 'AmuletRules', 'AmuletPriceVote'];
    
    if (sources.primarySource === 'binary') {
      const result = await binaryReader.streamRecords(db.DATA_PATH, 'events', {
        limit,
        offset,
        maxDays: 365,
        maxFilesToScan: 500,
        sortBy: 'effective_at',
        filter: (e) => governanceTemplates.some(t => e.template_id?.includes(t))
      });
      return res.json(convertBigInts({ data: result.records, count: result.records.length, hasMore: result.hasMore, source: 'binary' }));
    }
    
    const templateFilter = governanceTemplates.map(t => `template_id LIKE '%${t}%'`).join(' OR ');
    const sql = `
      SELECT *
      FROM ${getEventsSource()}
      WHERE ${templateFilter}
      ORDER BY COALESCE(timestamp, effective_at) DESC
      LIMIT ${limit}
      OFFSET ${offset}
    `;
    
    const rows = await db.safeQuery(sql);
    res.json(convertBigInts({ data: rows, count: rows.length, source: sources.primarySource }));
  } catch (err) {
    console.error('Error fetching governance events:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/rewards - Get reward-related events
router.get('/rewards', async (req, res) => {
  try {
    const limit = sanitizeNumber(req.query.limit, { min: 1, max: 2000, defaultValue: 500 });
    const offset = sanitizeNumber(req.query.offset, { min: 0, max: 100000, defaultValue: 0 });
    const sources = getDataSources();
    
    const rewardTemplates = ['RewardCoupon', 'AppRewardCoupon', 'ValidatorRewardCoupon', 'SvRewardCoupon'];
    
    if (sources.primarySource === 'binary') {
      const result = await binaryReader.streamRecords(db.DATA_PATH, 'events', {
        limit,
        offset,
        maxDays: 90,
        maxFilesToScan: 300,
        sortBy: 'effective_at',
        filter: (e) => rewardTemplates.some(t => e.template_id?.includes(t))
      });
      return res.json(convertBigInts({ data: result.records, count: result.records.length, hasMore: result.hasMore, source: 'binary' }));
    }
    
    const templateFilter = rewardTemplates.map(t => `template_id LIKE '%${t}%'`).join(' OR ');
    const sql = `
      SELECT *
      FROM ${getEventsSource()}
      WHERE ${templateFilter}
      ORDER BY COALESCE(timestamp, effective_at) DESC
      LIMIT ${limit}
      OFFSET ${offset}
    `;
    
    const rows = await db.safeQuery(sql);
    res.json(convertBigInts({ data: rows, count: rows.length, source: sources.primarySource }));
  } catch (err) {
    console.error('Error fetching reward events:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/member-traffic - Get member traffic events
router.get('/member-traffic', async (req, res) => {
  try {
    const limit = sanitizeNumber(req.query.limit, { min: 1, max: 1000, defaultValue: 200 });
    const offset = sanitizeNumber(req.query.offset, { min: 0, max: 100000, defaultValue: 0 });
    const sources = getDataSources();
    
    if (sources.primarySource === 'binary') {
      const result = await binaryReader.streamRecords(db.DATA_PATH, 'events', {
        limit,
        offset,
        maxDays: 90,
        maxFilesToScan: 300,
        sortBy: 'effective_at',
        filter: (e) => e.template_id?.includes('MemberTraffic')
      });
      return res.json(convertBigInts({ data: result.records, count: result.records.length, hasMore: result.hasMore, source: 'binary' }));
    }
    
    const sql = `
      SELECT *
      FROM ${getEventsSource()}
      WHERE template_id LIKE '%MemberTraffic%'
      ORDER BY COALESCE(timestamp, effective_at) DESC
      LIMIT ${limit}
      OFFSET ${offset}
    `;
    
    const rows = await db.safeQuery(sql);
    res.json(convertBigInts({ data: rows, count: rows.length, source: sources.primarySource }));
  } catch (err) {
    console.error('Error fetching member traffic events:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/by-contract/:contractId - Get all events for a specific contract
router.get('/by-contract/:contractId', async (req, res) => {
  try {
    const { contractId } = req.params;
    const sources = getDataSources();
    
    if (sources.primarySource === 'binary') {
      const result = await binaryReader.streamRecords(db.DATA_PATH, 'events', {
        limit: 100,
        offset: 0,
        maxDays: 3650,
        maxFilesToScan: 10000,
        filter: (e) => e.contract_id === contractId
      });
      return res.json(convertBigInts({ data: result.records, count: result.records.length, source: 'binary' }));
    }
    
    const sql = `
      SELECT *
      FROM ${getEventsSource()}
      WHERE contract_id = '${escapeString(contractId)}'
      ORDER BY COALESCE(timestamp, effective_at) DESC
      LIMIT 100
    `;
    
    const rows = await db.safeQuery(sql);
    res.json(convertBigInts({ data: rows, count: rows.length, source: sources.primarySource }));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/sources - Check what data sources are available
router.get('/sources', (req, res) => {
  try {
    const sources = getDataSources();
    res.json(sources);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// SV Node API endpoints (live queries to SV node)
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
