import { Router } from 'express';
import fs from 'fs';
import path from 'path';
import db from '../duckdb/connection.js';
import binaryReader from '../duckdb/binary-reader.js';
import * as voteRequestIndexer from '../engine/vote-request-indexer.js';
import * as rewardIndexer from '../engine/reward-indexer.js';
import * as svIndexer from '../engine/sv-indexer.js';
import * as voteOutcomeAnalyzer from '../engine/vote-outcome-analyzer.js';
import {
  getFilesForTemplate,
  isTemplateIndexPopulated,
} from '../engine/template-file-index.js';

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

// VoteRequest cache - only used when index is not populated
let voteRequestCache = null;
let voteRequestCacheTime = 0;
const VOTE_REQUEST_CACHE_TTL = 5 * 60 * 1000; // 5 minutes

// Helper to get the correct read function for Parquet files (primary) or JSONL files (fallback)
const getEventsSource = () => {
  const hasParquet = db.hasFileType('events', '.parquet');
  if (hasParquet) {
    return `read_parquet('${db.DATA_PATH.replace(/\\/g, '/')}/**/events-*.parquet', union_by_name=true)`;
  }
  
  // Check if any JSONL files exist before trying to read
  const hasJsonl = db.hasFileType('events', '.jsonl');
  const hasGzip = db.hasFileType('events', '.jsonl.gz');
  const hasZstd = db.hasFileType('events', '.jsonl.zst');
  
  if (!hasJsonl && !hasGzip && !hasZstd) {
    // Return empty table if no files exist
    return `(SELECT NULL::VARCHAR as event_id, NULL::VARCHAR as event_type, NULL::VARCHAR as contract_id, 
             NULL::VARCHAR as template_id, NULL::VARCHAR as package_name, NULL::TIMESTAMP as timestamp,
             NULL::VARCHAR[] as signatories, NULL::VARCHAR[] as observers, NULL::JSON as payload WHERE false)`;
  }
  
  const basePath = db.DATA_PATH.replace(/\\/g, '/');
  const queries = [];
  if (hasJsonl) queries.push(`SELECT * FROM read_json_auto('${basePath}/**/events-*.jsonl', union_by_name=true, ignore_errors=true)`);
  if (hasGzip) queries.push(`SELECT * FROM read_json_auto('${basePath}/**/events-*.jsonl.gz', union_by_name=true, ignore_errors=true)`);
  if (hasZstd) queries.push(`SELECT * FROM read_json_auto('${basePath}/**/events-*.jsonl.zst', union_by_name=true, ignore_errors=true)`);
  
  // Use UNION (not UNION ALL) to prevent duplicate records
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
    const limit = Math.min(parseInt(req.query.limit) || 100, 1000);
    const offset = parseInt(req.query.offset) || 0;
    const sources = getDataSources();
    
    if (sources.primarySource === 'binary') {
      // For huge datasets, keep this endpoint snappy by scanning only recent partitions
      // Sort by timestamp (record_time when event was written) to show recently ingested data
      // NOT effective_at, which during backfill can be old historical times
      const result = await binaryReader.streamRecords(db.DATA_PATH, 'events', {
        limit,
        offset,
        maxDays: 7, // Recent partitions only
        maxFilesToScan: 200,
        sortBy: 'timestamp', // Use write time, not effective_at
      });
      return res.json(convertBigInts({ data: result.records, count: result.records.length, hasMore: result.hasMore, source: 'binary' }));
    }
    
    const sql = `
      SELECT 
        event_id,
        update_id,
        event_type,
        contract_id,
        template_id,
        package_name,
        migration_id,
        synchronizer_id,
        timestamp,
        effective_at,
        signatories,
        observers,
        payload
      FROM ${getEventsSource()}
      ORDER BY effective_at DESC
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
    const limit = Math.min(parseInt(req.query.limit) || 100, 1000);
    const offset = parseInt(req.query.offset) || 0;
    const sources = getDataSources();
    
    if (sources.primarySource === 'binary') {
      const result = await binaryReader.streamRecords(db.DATA_PATH, 'events', {
        limit,
        offset,
        maxDays: 30,
        maxFilesToScan: 200,
        sortBy: 'effective_at',
        filter: (e) => e.event_type === type
      });
      return res.json(convertBigInts({ data: result.records, count: result.records.length, hasMore: result.hasMore, source: 'binary' }));
    }
    
    const sql = `
      SELECT *
      FROM ${getEventsSource()}
      WHERE event_type = '${type}'
      ORDER BY effective_at DESC
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
    const limit = Math.min(parseInt(req.query.limit) || 100, 1000);
    const offset = parseInt(req.query.offset) || 0;
    const sources = getDataSources();
    
    if (sources.primarySource === 'binary') {
      const result = await binaryReader.streamRecords(db.DATA_PATH, 'events', {
        limit,
        offset,
        maxDays: 30,
        maxFilesToScan: 200,
        sortBy: 'effective_at',
        filter: (e) => e.template_id?.includes(templateId)
      });
      return res.json(convertBigInts({ data: result.records, count: result.records.length, hasMore: result.hasMore, source: 'binary' }));
    }
    
    const sql = `
      SELECT *
      FROM ${getEventsSource()}
      WHERE template_id LIKE '%${templateId}%'
      ORDER BY effective_at DESC
      LIMIT ${limit}
    `;
    
    const rows = await db.safeQuery(sql);
    res.json(convertBigInts({ data: rows, count: rows.length, source: sources.primarySource }));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/by-date - Get events for a specific date range (filters by effective_at)
router.get('/by-date', async (req, res) => {
  try {
    const { start, end } = req.query;
    const limit = Math.min(parseInt(req.query.limit) || 100, 1000);
    const offset = parseInt(req.query.offset) || 0;
    const sources = getDataSources();
    
    if (sources.primarySource === 'binary') {
      const startDate = start ? new Date(start).getTime() : 0;
      const endDate = end ? new Date(end).getTime() : Date.now();
      
      const result = await binaryReader.streamRecords(db.DATA_PATH, 'events', {
        limit,
        offset,
        maxDays: 90, // Wider range for date queries
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
    if (start) whereClause += ` AND effective_at >= '${start}'`;
    if (end) whereClause += ` AND effective_at <= '${end}'`;
    
    const sql = `
      SELECT *
      FROM ${getEventsSource()}
      WHERE 1=1 ${whereClause}
      ORDER BY effective_at DESC
      LIMIT ${limit}
    `;
    
    const rows = await db.safeQuery(sql);
    res.json(convertBigInts({ data: rows, count: rows.length, source: sources.primarySource }));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/count - Get total event count (estimates for binary)
router.get('/count', async (req, res) => {
  try {
    const sources = getDataSources();
    
    if (sources.primarySource === 'binary') {
      // Use fast count function that doesn't load all paths into memory
      const fileCount = binaryReader.countBinaryFiles(db.DATA_PATH, 'events');
      const estimated = fileCount * 100; // ~100 records per file estimate
      return res.json(convertBigInts({ count: estimated, estimated: true, fileCount, source: 'binary' }));
    }
    
    const sql = `
      SELECT COUNT(*) as total
      FROM ${getEventsSource()}
    `;
    
    const rows = await db.safeQuery(sql);
    res.json(convertBigInts({ count: rows[0]?.total || 0, source: sources.primarySource }));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/debug - Debug endpoint to show data paths and latest files
router.get('/debug', async (req, res) => {
  try {
    const sources = getDataSources();
    
    // Use FAST finder for newest files (no full scan needed)
    const newestFiles = binaryReader.findBinaryFilesFast(db.DATA_PATH, 'events', { maxDays: 7, maxFiles: 10 });
    const fileCount = binaryReader.countBinaryFiles(db.DATA_PATH, 'events');
    
    // Sample first record from newest file (by data date)
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
      newestByDataDate: newestFiles.slice(0, 5).map(f => ({
        path: f,
        dataDate: (() => {
          const y = f.match(/year=(\d{4})/)?.[1];
          const m = f.match(/month=(\d{2})/)?.[1];
          const d = f.match(/day=(\d{2})/)?.[1];
          return y && m && d ? `${y}-${m}-${d}` : null;
        })(),
        writeTimestamp: (() => {
          const match = f.match(/events-(\d+)-/);
          return match ? new Date(parseInt(match[1])).toISOString() : null;
        })()
      })),
      sampleNewestRecord: sampleRecord,
    }));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/governance - Get governance-related events (VoteRequest, Confirmation, etc.)
router.get('/governance', async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 200, 1000);
    const offset = parseInt(req.query.offset) || 0;
    const sources = getDataSources();
    
    // Governance templates to filter for
    const governanceTemplates = [
      'VoteRequest',
      'Confirmation',
      'DsoRules',
      'AmuletRules',
      'AmuletPriceVote',
    ];
    
    if (sources.primarySource === 'binary') {
      const result = await binaryReader.streamRecords(db.DATA_PATH, 'events', {
        limit,
        offset,
        maxDays: 365, // Governance events span long periods
        maxFilesToScan: 500,
        sortBy: 'effective_at',
        filter: (e) => governanceTemplates.some(t => e.template_id?.includes(t))
      });
      return res.json(convertBigInts({ 
        data: result.records, 
        count: result.records.length, 
        hasMore: result.hasMore, 
        source: 'binary' 
      }));
    }
    
    const templateFilter = governanceTemplates.map(t => `template_id LIKE '%${t}%'`).join(' OR ');
    const sql = `
      SELECT *
      FROM ${getEventsSource()}
      WHERE ${templateFilter}
      ORDER BY effective_at DESC
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

// GET /api/events/rewards - Get reward-related events (RewardCoupon, etc.)
router.get('/rewards', async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 500, 2000);
    const offset = parseInt(req.query.offset) || 0;
    const sources = getDataSources();
    
    // Reward templates to filter for
    const rewardTemplates = [
      'RewardCoupon',
      'AppRewardCoupon',
      'ValidatorRewardCoupon',
      'SvRewardCoupon',
    ];
    
    if (sources.primarySource === 'binary') {
      const result = await binaryReader.streamRecords(db.DATA_PATH, 'events', {
        limit,
        offset,
        maxDays: 90,
        maxFilesToScan: 300,
        sortBy: 'effective_at',
        filter: (e) => rewardTemplates.some(t => e.template_id?.includes(t))
      });
      return res.json(convertBigInts({ 
        data: result.records, 
        count: result.records.length, 
        hasMore: result.hasMore, 
        source: 'binary' 
      }));
    }
    
    const templateFilter = rewardTemplates.map(t => `template_id LIKE '%${t}%'`).join(' OR ');
    const sql = `
      SELECT *
      FROM ${getEventsSource()}
      WHERE ${templateFilter}
      ORDER BY effective_at DESC
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
    const limit = Math.min(parseInt(req.query.limit) || 200, 1000);
    const offset = parseInt(req.query.offset) || 0;
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
      return res.json(convertBigInts({ 
        data: result.records, 
        count: result.records.length, 
        hasMore: result.hasMore, 
        source: 'binary' 
      }));
    }
    
    const sql = `
      SELECT *
      FROM ${getEventsSource()}
      WHERE template_id LIKE '%MemberTraffic%'
      ORDER BY effective_at DESC
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

// GET /api/events/governance-history - Get historical governance events (completed votes, rule changes)
router.get('/governance-history', async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 500, 2000);
    const offset = parseInt(req.query.offset) || 0;
    const verbose = req.query.verbose === 'true';
    const sources = getDataSources();
    
    // Templates specifically for governance (NOT AmuletRules which is too broad)
    const governanceTemplates = [
      'VoteRequest',           // Vote proposals
      'Confirmation',          // Confirmed governance actions
      'ElectionRequest',       // Election-related
    ];
    
    // Governance-related choices to look for in any template
    const governanceChoices = [
      // DsoRules governance choices
      'DsoRules_ConfirmAction',
      'DsoRules_AddConfirmedAction', 
      'DsoRules_CloseVoteRequest',
      'DsoRules_CastVote',
      'DsoRules_ExecuteConfirmedAction',
      // VoteRequest choices
      'VoteRequest_CastVote',
      'VoteRequest_ExpireVoteRequest',
      'VoteRequest_Accept',
      'VoteRequest_Reject',
      // AmuletRules governance choices (featured apps, validators)
      'AmuletRules_DevNet_AddFeaturedAppRight',
      'AmuletRules_AddFeaturedAppRight',
      'AmuletRules_RemoveFeaturedAppRight',
      // Confirmation choices
      'Confirmation_Confirm',
      'Confirmation_Expire',
    ];
    
    console.log(`\n📊 GOVERNANCE-HISTORY: Fetching with limit=${limit}, offset=${offset}, verbose=${verbose}`);
    console.log(`   Primary source: ${sources.primarySource}`);
    console.log(`   Looking for templates: ${governanceTemplates.join(', ')}`);
    console.log(`   OR choices: ${governanceChoices.slice(0, 5).join(', ')}...`);
    
    if (sources.primarySource === 'binary') {
      // Fast path: if the persistent VoteRequest index exists, use it for completed votes.
      // This avoids scanning tens of thousands of binary files (which can take many minutes).
      const voteIndexPopulated = await voteRequestIndexer.isIndexPopulated();
      if (voteIndexPopulated) {
        console.log('   ⚡ VoteRequest index is populated → using indexed historical votes');

        const indexedHistorical = await voteRequestIndexer.queryVoteRequests({
          limit,
          offset,
          status: 'historical',
        });

        // Still include a small set of other governance events (confirmations/elections/dso rules)
        // so the endpoint stays compatible with the UI.
        const otherResult = await binaryReader.streamRecords(db.DATA_PATH, 'events', {
          limit: Math.min(limit * 5, 5000),
          offset: 0,
          maxDays: 365 * 3,
          maxFilesToScan: 5000,
          sortBy: 'effective_at',
          filter: (e) => {
            if (e.template_id?.includes('Confirmation') && e.event_type === 'created') return true;
            if (e.template_id?.includes('ElectionRequest') && e.event_type === 'created') return true;
            const choiceMatch = governanceChoices.includes(e.choice);
            if (e.template_id?.includes('DsoRules') && choiceMatch) return true;
            return false;
          }
        });

        const indexedHistory = indexedHistorical.map((vr) => ({
          event_id: vr.event_id,
          event_type: 'archived',
          choice: null,
          contract_id: vr.contract_id,
          template_id: vr.template_id,
          effective_at: vr.effective_at,
          // We don't have a separate timestamp in the index; use effective_at for sorting/display.
          timestamp: vr.effective_at,
          action_tag: vr.action_tag || null,
          action_value: vr.action_value || null,
          requester: vr.requester || null,
          confirmer: null,
          reason: (() => {
            // Keep existing shape: prefer JSON object if available
            const r = vr.reason;
            if (!r) return null;
            if (typeof r !== 'string') return r;
            try {
              return JSON.parse(r);
            } catch {
              return { body: r };
            }
          })(),
          votes: Array.isArray(vr.votes) ? vr.votes : [],
          vote_before: vr.vote_before || null,
          expires_at: null,
          dso: vr.dso || null,
          exercise_result: null,
        }));

        // Convert "other" events into the same response shape used below.
        const otherHistory = (otherResult.records || []).map((event) => ({
          event_id: event.event_id,
          event_type: event.event_type,
          choice: event.choice || null,
          contract_id: event.contract_id,
          template_id: event.template_id,
          effective_at: event.effective_at,
          timestamp: event.timestamp,
          action_tag: event.payload?.action?.tag || null,
          action_value: event.payload?.action?.value ? Object.keys(event.payload.action.value) : null,
          requester: event.payload?.requester || event.payload?.confirmer || null,
          confirmer: event.payload?.confirmer || null,
          reason: event.payload?.reason || null,
          votes: event.payload?.votes || [],
          vote_before: event.payload?.voteBefore || null,
          expires_at: event.payload?.expiresAt || null,
          dso: event.payload?.dso || null,
          exercise_result: event.exercise_result || null,
        }));

        const merged = [...indexedHistory, ...otherHistory]
          .filter(Boolean)
          .sort((a, b) => new Date(b.effective_at).getTime() - new Date(a.effective_at).getTime())
          .slice(0, limit);

        return res.json(convertBigInts({
          data: merged,
          count: merged.length,
          hasMore: indexedHistorical.length === limit,
          source: 'binary',
          _debug: {
            usedVoteRequestIndex: true,
            indexedHistoricalVotes: indexedHistorical.length,
            otherGovernanceIncluded: otherHistory.length,
          }
        }));
      }

      console.log('   ⚠️ VoteRequest index is empty → falling back to binary scans (slow)');

      // VoteRequest events are EXTREMELY sparse, and scanning everything can take a long time.
      // Strategy: Run scans in parallel.

      // Scan 1: Deep scan specifically for VoteRequest archived events (completed votes)
      const voteRequestArchivedPromise = binaryReader.streamRecords(db.DATA_PATH, 'events', {
        limit: 1000,
        offset: 0,
        maxDays: 365 * 3,
        maxFilesToScan: 100000,
        sortBy: 'effective_at',
        filter: (e) => e.template_id?.includes('VoteRequest') && e.event_type === 'archived',
      });

      // Scan 2: VoteRequest created events (for active proposals context)
      const voteRequestCreatedPromise = binaryReader.streamRecords(db.DATA_PATH, 'events', {
        limit: 100,
        offset: 0,
        maxDays: 365,
        maxFilesToScan: 50000,
        sortBy: 'effective_at',
        filter: (e) => e.template_id?.includes('VoteRequest') && e.event_type === 'created',
      });

      // Scan 3: Standard governance scan (Confirmation, DsoRules choices)
      const otherGovernancePromise = binaryReader.streamRecords(db.DATA_PATH, 'events', {
        limit: limit * 100,
        offset,
        maxDays: 365 * 3,
        maxFilesToScan: 20000,
        sortBy: 'effective_at',
        filter: (e) => {
          if (e.template_id?.includes('Confirmation') && e.event_type === 'created') return true;
          if (e.template_id?.includes('ElectionRequest') && e.event_type === 'created') return true;
          const choiceMatch = governanceChoices.includes(e.choice);
          if (e.template_id?.includes('DsoRules') && choiceMatch) return true;
          return false;
        }
      });
      
      // Run all scans in parallel
      const [voteRequestArchivedResult, voteRequestCreatedResult, otherResult] = await Promise.all([
        voteRequestArchivedPromise, 
        voteRequestCreatedPromise,
        otherGovernancePromise
      ]);
      
      console.log(`   Found ${voteRequestArchivedResult.records.length} ARCHIVED VoteRequest events (completed votes)`);
      console.log(`   Found ${voteRequestCreatedResult.records.length} CREATED VoteRequest events`);
      console.log(`   Found ${otherResult.records.length} other governance events`);
      
      // Priority: Archived VoteRequests first (completed votes), then Created, then other
      // Reserve up to 60% of slots for archived VoteRequests (the main history)
      const archivedSlots = Math.min(Math.ceil(limit * 0.6), voteRequestArchivedResult.records.length);
      const createdSlots = Math.min(Math.ceil(limit * 0.2), voteRequestCreatedResult.records.length);
      const otherSlots = limit - archivedSlots - createdSlots;
      
      // Sort each set by effective_at descending
      voteRequestArchivedResult.records.sort((a, b) => new Date(b.effective_at) - new Date(a.effective_at));
      voteRequestCreatedResult.records.sort((a, b) => new Date(b.effective_at) - new Date(a.effective_at));
      otherResult.records.sort((a, b) => new Date(b.effective_at) - new Date(a.effective_at));
      
      // Take allocated slots from each
      const selectedArchived = voteRequestArchivedResult.records.slice(0, archivedSlots);
      const selectedCreated = voteRequestCreatedResult.records.slice(0, createdSlots);
      const selectedOther = otherResult.records.slice(0, otherSlots);
      
      // Merge and dedupe
      const allRecords = [...selectedArchived, ...selectedCreated, ...selectedOther];
      const seenIds = new Set();
      const dedupedRecords = allRecords.filter(r => {
        if (seenIds.has(r.event_id)) return false;
        seenIds.add(r.event_id);
        return true;
      });
      
      // Sort merged results by effective_at descending
      dedupedRecords.sort((a, b) => new Date(b.effective_at) - new Date(a.effective_at));
      
      console.log(`   Archived VoteRequests included: ${selectedArchived.length}/${voteRequestArchivedResult.records.length}`);
      console.log(`   Created VoteRequests included: ${selectedCreated.length}/${voteRequestCreatedResult.records.length}`);
      console.log(`   Other governance included: ${selectedOther.length}/${otherResult.records.length}`);
      console.log(`   Merged to ${dedupedRecords.length} unique governance events`);
      
      // Final records (already limited by slot allocation)
      const limitedRecords = dedupedRecords;
      
      // Group by template and event type for summary
      const templateCounts = {};
      const eventTypeCounts = {};
      const choiceCounts = {};
      
      // Process to extract governance history details
      const history = limitedRecords.map((event, idx) => {
        // Count templates - extract just the template name
        const templateParts = event.template_id?.split(':') || [];
        const template = templateParts[templateParts.length - 1] || 'unknown';
        templateCounts[template] = (templateCounts[template] || 0) + 1;
        
        // Count event types
        const eventType = event.event_type || 'unknown';
        eventTypeCounts[eventType] = (eventTypeCounts[eventType] || 0) + 1;
        
        // Count choices (for exercised events)
        if (event.choice) {
          choiceCounts[event.choice] = (choiceCounts[event.choice] || 0) + 1;
        }
        
        // Verbose logging for first few events
        if (verbose && idx < 5) {
          console.log(`\n   📝 Event #${idx + 1}:`);
          console.log(`      template_id: ${event.template_id}`);
          console.log(`      event_type: ${event.event_type}`);
          console.log(`      choice: ${event.choice || 'N/A'}`);
          console.log(`      effective_at: ${event.effective_at}`);
          console.log(`      payload keys: ${event.payload ? Object.keys(event.payload).join(', ') : 'null'}`);
          if (event.payload) {
            console.log(`      payload.action: ${event.payload.action ? JSON.stringify(event.payload.action).slice(0, 200) : 'null'}`);
            console.log(`      payload.requester: ${event.payload.requester || 'null'}`);
            const reason = event.payload.reason;
            console.log(`      payload.reason: ${typeof reason === 'string' ? reason.slice(0, 100) : JSON.stringify(reason)?.slice(0, 100) || 'null'}`);
            console.log(`      payload.votes: ${event.payload.votes ? `[${event.payload.votes.length} votes]` : 'null'}`);
            console.log(`      payload.voteBefore: ${event.payload.voteBefore || 'null'}`);
          }
          if (event.exercise_result) {
            console.log(`      exercise_result keys: ${Object.keys(event.exercise_result).join(', ')}`);
          }
        }
        
        return {
          event_id: event.event_id,
          event_type: event.event_type,
          choice: event.choice || null,
          contract_id: event.contract_id,
          template_id: event.template_id,
          effective_at: event.effective_at,
          timestamp: event.timestamp,
          // Template-specific field extraction
          // VoteRequest: action, requester, reason, votes, voteBefore
          // Confirmation: action, confirmer, expiresAt
          action_tag: event.payload?.action?.tag || null,
          action_value: event.payload?.action?.value ? Object.keys(event.payload.action.value) : null,
          // VoteRequest has requester, Confirmation has confirmer
          requester: event.payload?.requester || event.payload?.confirmer || null,
          confirmer: event.payload?.confirmer || null,
          reason: event.payload?.reason || null,
          votes: event.payload?.votes || [],
          vote_before: event.payload?.voteBefore || null,
          expires_at: event.payload?.expiresAt || null,
          dso: event.payload?.dso || null,
          // Include exercise result for executed actions
          exercise_result: event.exercise_result || null,
          // Include full payload keys in verbose mode
          ...(verbose ? { _payload_keys: event.payload ? Object.keys(event.payload) : null } : {}),
        };
      });
      
      // Log summary
      console.log(`\n   📈 Template breakdown:`, templateCounts);
      console.log(`   📈 Event type breakdown:`, eventTypeCounts);
      console.log(`   📈 Choice breakdown:`, choiceCounts);
      
      // Count events with key governance fields
      const withAction = history.filter(h => h.action_tag).length;
      const withRequester = history.filter(h => h.requester).length;
      const withConfirmer = history.filter(h => h.confirmer).length;
      const withReason = history.filter(h => h.reason).length;
      const withVotes = history.filter(h => h.votes?.length > 0).length;
      const withExerciseResult = history.filter(h => h.exercise_result).length;
      
      console.log(`\n   🔍 Field coverage:`);
      console.log(`      Events with action_tag: ${withAction}/${history.length}`);
      console.log(`      Events with requester: ${withRequester}/${history.length}`);
      console.log(`      Events with confirmer: ${withConfirmer}/${history.length}`);
      console.log(`      Events with reason: ${withReason}/${history.length}`);
      console.log(`      Events with votes: ${withVotes}/${history.length}`);
      console.log(`      Events with exercise_result: ${withExerciseResult}/${history.length}`);
      
      return res.json(convertBigInts({ 
        data: history, 
        count: history.length, 
        hasMore: limitedRecords.length < dedupedRecords.length, 
        source: 'binary',
        _debug: {
          templateCounts,
          eventTypeCounts,
          choiceCounts,
          fieldCoverage: { withAction, withRequester, withConfirmer, withReason, withVotes, withExerciseResult },
          totalScanned: dedupedRecords.length,
          archivedVoteRequestsFound: voteRequestArchivedResult.records.length,
          createdVoteRequestsFound: voteRequestCreatedResult.records.length,
          otherGovernanceFound: otherResult.records.length,
        }
      }));
    }
    
    // Fallback to DuckDB query
    const templateFilter = governanceTemplates.map(t => `template_id LIKE '%${t}%'`).join(' OR ');
    const choiceFilter = governanceChoices.map(c => `choice = '${c}'`).join(' OR ');
    const sql = `
      SELECT 
        event_id,
        event_type,
        choice,
        contract_id,
        template_id,
        effective_at,
        timestamp,
        payload
      FROM ${getEventsSource()}
      WHERE (${templateFilter}) OR (${choiceFilter}) OR template_id LIKE '%DsoRules%'
      ORDER BY effective_at DESC
      LIMIT ${limit}
      OFFSET ${offset}
    `;
    
    const rows = await db.safeQuery(sql);
    console.log(`   Found ${rows.length} governance events from DuckDB`);
    
    // Process rows to extract governance details
    const history = rows.map(row => ({
      event_id: row.event_id,
      event_type: row.event_type,
      choice: row.choice || null,
      contract_id: row.contract_id,
      template_id: row.template_id,
      effective_at: row.effective_at,
      timestamp: row.timestamp,
      action_tag: row.payload?.action?.tag || null,
      requester: row.payload?.requester || null,
      reason: row.payload?.reason || null,
      votes: row.payload?.votes || [],
      vote_before: row.payload?.voteBefore || null,
    }));
    
    res.json(convertBigInts({ data: history, count: history.length, source: sources.primarySource }));
  } catch (err) {
    console.error('Error fetching governance history:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/vote-requests - Dedicated endpoint for VoteRequest events with deep scanning
router.get('/vote-requests', async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 100, 10000);
    const status = req.query.status || 'all'; // 'active', 'historical', 'all'
    const verbose = req.query.verbose === 'true';
    const ensureFresh = req.query.ensureFresh === 'true';
    const offset = parseInt(req.query.offset) || 0;
    const sources = getDataSources();
    const now = new Date();
    
    console.log(`\n🗳️ VOTE-REQUESTS: Fetching with limit=${limit}, status=${status}, verbose=${verbose}, ensureFresh=${ensureFresh}`);
    
    // First, try to use the persistent DuckDB index
    const indexPopulated = await voteRequestIndexer.isIndexPopulated();
    const indexing = voteRequestIndexer.isIndexingInProgress();
    
    // If caller requests fresh/complete data AND index is NOT already indexing, trigger a rebuild.
    // But only on explicit ensureFresh (usually a manual trigger, NOT normal page loads).
    if (ensureFresh && indexPopulated && !indexing) {
      console.log('   ensureFresh=true → triggering background VoteRequest index rebuild');
      voteRequestIndexer.buildVoteRequestIndex({ force: true }).catch(err => {
        console.error('Background index rebuild failed:', err);
      });
    }
    
    // Use index if populated (even if a rebuild is running in background, serve from existing data)
    if (indexPopulated) {
      console.log(`   Using persistent DuckDB index`);
      console.log(`   Using persistent DuckDB index`);
      
      const voteRequests = await voteRequestIndexer.queryVoteRequests({ limit, status, offset });
      const stats = await voteRequestIndexer.getVoteRequestStats();
      const indexState = await voteRequestIndexer.getIndexState();
      
      return res.json(convertBigInts({
        data: voteRequests,
        count: voteRequests.length,
        totalFound: status === 'all' ? stats.total : (status === 'active' ? stats.active : stats.historical),
        source: 'duckdb-index',
        _summary: {
          activeCount: stats.active,
          historicalCount: stats.historical,
          closedCount: stats.closed,
          statusFilter: status,
        },
        _debug: verbose ? {
          indexedAt: indexState.last_indexed_at,
          totalIndexed: indexState.total_indexed,
          fromIndex: true,
        } : undefined,
      }));
    }
    
    console.log(`   Primary source: ${sources.primarySource} (index not populated)`);
    
    if (sources.primarySource === 'binary') {
      // Check cache first
      const cacheAge = Date.now() - voteRequestCacheTime;
      const useCache = voteRequestCache && cacheAge < VOTE_REQUEST_CACHE_TTL;
      
      let allVoteRequests;
      let debugInfo;
      
      if (useCache) {
        console.log(`   Using cached data (age: ${Math.round(cacheAge / 1000)}s)`);
        allVoteRequests = voteRequestCache.allVoteRequests;
        debugInfo = { ...voteRequestCache.debugInfo, fromCache: true, cacheAgeSeconds: Math.round(cacheAge / 1000) };
      } else {
        console.log(`   Scanning binary files (cache expired or missing)...`);
        
        // Prefer template-to-file index to avoid scanning all 35K+ files.
        const templateIndexReady = await isTemplateIndexPopulated();
        let voteRequestFiles = [];
        
        if (templateIndexReady) {
          voteRequestFiles = await getFilesForTemplate('VoteRequest');
          console.log(`   📋 Template index available → ${voteRequestFiles.length} VoteRequest files to scan`);
        }
        
        const scanVoteRequestFiles = async (files, kind) => {
          const records = [];
          let filesScanned = 0;
          const start = Date.now();
          let lastLog = start;
          
          for (const file of files) {
            try {
              const result = await binaryReader.readBinaryFile(file);
              const fileRecords = result.records || [];
              
              for (const e of fileRecords) {
                if (!e.template_id?.includes('VoteRequest')) continue;
                if (kind === 'created') {
                  if (e.event_type === 'created') records.push(e);
                } else {
                  if (e.event_type !== 'exercised') continue;
                  if (e.choice === 'Archive' || (typeof e.choice === 'string' && e.choice.startsWith('VoteRequest_'))) {
                    records.push(e);
                  }
                }
              }
              
              filesScanned++;
              const now = Date.now();
              if (now - lastLog > 5000) {
                const pct = files.length ? ((filesScanned / files.length) * 100).toFixed(0) : '0';
                console.log(`   📂 [${pct}%] ${filesScanned}/${files.length} VoteRequest files | ${records.length} ${kind}`);
                lastLog = now;
              }
            } catch {
              // ignore unreadable files
            }
          }
          
          return { records, filesScanned, elapsedMs: Date.now() - start };
        };

        let createdResult;
        let exercisedResult;

        if (templateIndexReady && voteRequestFiles.length > 0) {
          // Fast path: only scan files known to contain VoteRequest events
          [createdResult, exercisedResult] = await Promise.all([
            scanVoteRequestFiles(voteRequestFiles, 'created'),
            scanVoteRequestFiles(voteRequestFiles, 'exercised'),
          ]);
        } else {
          // Fallback: full scan (slow, but safe)
          console.log('   ⚠️ Template index not ready/empty → full scan fallback');

          const createdPromise = binaryReader.streamRecords(db.DATA_PATH, 'events', {
            limit: 10000,
            offset: 0,
            fullScan: true,
            sortBy: 'effective_at',
            filter: (e) => e.template_id?.includes('VoteRequest') && e.event_type === 'created'
          });

          const exercisedPromise = binaryReader.streamRecords(db.DATA_PATH, 'events', {
            limit: 100000,
            offset: 0,
            fullScan: true,
            sortBy: 'effective_at',
            filter: (e) => {
              if (!e.template_id?.includes('VoteRequest')) return false;
              if (e.event_type !== 'exercised') return false;
              return e.choice === 'Archive' || (typeof e.choice === 'string' && e.choice.startsWith('VoteRequest_'));
            }
          });

          [createdResult, exercisedResult] = await Promise.all([createdPromise, exercisedPromise]);
        }

        console.log(`   Created scan: ${createdResult.filesScanned || '?'} files scanned`);
        console.log(`   Exercised scan: ${exercisedResult.filesScanned || '?'} files scanned`);

        const closedContractIds = new Set(
          (exercisedResult.records || [])
            .map(r => r.contract_id)
            .filter(Boolean)
        );

        console.log(`   Found ${createdResult.records.length} VoteRequest created, ${exercisedResult.records.length} exercised`);

        // Process to extract full VoteRequest details
        allVoteRequests = createdResult.records.map((event) => {
          const voteBefore = event.payload?.voteBefore;
          const voteBeforeDate = voteBefore ? new Date(voteBefore) : null;
          const isClosed = !!event.contract_id && closedContractIds.has(event.contract_id);
          const isActive = !isClosed && (voteBeforeDate ? voteBeforeDate > now : true);

          return {
            event_id: event.event_id,
            contract_id: event.contract_id,
            template_id: event.template_id,
            effective_at: event.effective_at,
            timestamp: event.timestamp,
            status: isActive ? 'active' : 'historical',
            is_closed: isClosed,
            action_tag: event.payload?.action?.tag || null,
            action_value: event.payload?.action?.value || null,
            requester: event.payload?.requester || null,
            reason: event.payload?.reason || null,
            votes: event.payload?.votes || [],
            vote_count: event.payload?.votes?.length || 0,
            vote_before: voteBefore || null,
            target_effective_at: event.payload?.targetEffectiveAt || null,
            tracking_cid: event.payload?.trackingCid || null,
            dso: event.payload?.dso || null,
          };
        });
        
        // Get date range of scanned data
        const allDates = [...createdResult.records, ...(exercisedResult.records || [])]
          .map(r => r.effective_at)
          .filter(Boolean)
          .sort();
        
        // Build debug info
        debugInfo = {
          createdEventsFound: createdResult.records.length,
          exercisedEventsFound: (exercisedResult.records || []).length,
          closedContractIds: closedContractIds.size,
          createdFilesScanned: createdResult.filesScanned || 0,
          createdTotalFiles: createdResult.totalFiles || 0,
          dateRangeCovered: { 
            oldest: allDates[0] || null, 
            newest: allDates[allDates.length - 1] || null 
          },
          fromCache: false,
        };
        
        // Cache the results
        voteRequestCache = { allVoteRequests, debugInfo };
        voteRequestCacheTime = Date.now();
        console.log(`   Cached ${allVoteRequests.length} VoteRequests`);
      }
      
      // Filter by status
      let filteredVoteRequests = allVoteRequests;
      if (status === 'active') {
        filteredVoteRequests = allVoteRequests.filter(v => v.status === 'active');
      } else if (status === 'historical') {
        filteredVoteRequests = allVoteRequests.filter(v => v.status === 'historical');
      }
      
      // Apply limit
      const voteRequests = filteredVoteRequests.slice(0, limit);
      
      // Summary stats
      const activeCount = allVoteRequests.filter(v => v.status === 'active').length;
      const historicalCount = allVoteRequests.filter(v => v.status === 'historical').length;
      const closedCount = allVoteRequests.filter(v => v.is_closed).length;
      const withReason = voteRequests.filter(v => v.reason).length;
      const withVotes = voteRequests.filter(v => v.votes?.length > 0).length;
      const actionTags = [...new Set(voteRequests.map(v => v.action_tag).filter(Boolean))];
      
      return res.json(convertBigInts({
        data: voteRequests,
        count: voteRequests.length,
        totalFound: allVoteRequests.length,
        source: sources.primarySource,
        _summary: {
          activeCount,
          historicalCount,
          closedCount,
          withReason,
          withVotes,
          actionTags,
          statusFilter: status,
        },
        _debug: debugInfo,
      }));
    }
    
    // Fallback to DuckDB/Parquet
    const sql = `
      SELECT 
        event_id,
        contract_id,
        template_id,
        effective_at,
        timestamp,
        payload
      FROM ${getEventsSource()}
      WHERE template_id LIKE '%VoteRequest%'
        AND event_type = 'created'
      ORDER BY effective_at DESC
      LIMIT ${limit}
    `;
    
    const rows = await db.safeQuery(sql);
    console.log(`   Found ${rows.length} VoteRequest events from DuckDB`);
    
    const voteRequests = rows.map(row => ({
      event_id: row.event_id,
      contract_id: row.contract_id,
      template_id: row.template_id,
      effective_at: row.effective_at,
      timestamp: row.timestamp,
      action_tag: row.payload?.action?.tag || null,
      action_value: row.payload?.action?.value || null,
      requester: row.payload?.requester || null,
      reason: row.payload?.reason || null,
      votes: row.payload?.votes || [],
      vote_before: row.payload?.voteBefore || null,
      target_effective_at: row.payload?.targetEffectiveAt || null,
      tracking_cid: row.payload?.trackingCid || null,
      dso: row.payload?.dso || null,
    }));
    
    res.json(convertBigInts({ data: voteRequests, count: voteRequests.length, source: sources.primarySource }));
  } catch (err) {
    console.error('Error fetching vote requests:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/vote-requests/debug-table - Debug endpoint to inspect vote_requests DuckDB table
router.get('/vote-requests/debug-table', async (req, res) => {
  try {
    console.log('\n🔍 DEBUG: Inspecting vote_requests DuckDB table...');
    
    // Get overall stats
    const statsRows = await db.safeQuery(`
      SELECT 
        COUNT(*) as total,
        COUNT(CASE WHEN action_tag IS NOT NULL THEN 1 END) as with_action_tag,
        COUNT(CASE WHEN requester IS NOT NULL THEN 1 END) as with_requester,
        COUNT(CASE WHEN reason IS NOT NULL THEN 1 END) as with_reason,
        COUNT(CASE WHEN vote_count > 0 THEN 1 END) as with_votes,
        COUNT(CASE WHEN is_human = true THEN 1 END) as human_proposals,
        COUNT(CASE WHEN payload IS NOT NULL THEN 1 END) as with_payload
      FROM vote_requests
    `);
    
    // Get sample records with key fields
    const sampleRows = await db.safeQuery(`
      SELECT 
        event_id,
        contract_id,
        action_tag,
        requester,
        reason,
        vote_count,
        is_human,
        status,
        effective_at,
        CASE WHEN payload IS NULL THEN 'NULL' 
             WHEN payload = '' THEN 'EMPTY_STRING'
             WHEN payload = '{}' THEN 'EMPTY_OBJECT'
             WHEN payload = 'null' THEN 'NULL_STRING'
             ELSE 'HAS_DATA' END as payload_status,
        LENGTH(payload) as payload_length
      FROM vote_requests
      ORDER BY effective_at DESC
      LIMIT 20
    `);
    
    // Get status breakdown
    const statusRows = await db.safeQuery(`
      SELECT 
        status,
        COUNT(*) as count,
        COUNT(CASE WHEN is_human = true THEN 1 END) as human_count
      FROM vote_requests
      GROUP BY status
      ORDER BY count DESC
    `);
    
    // Get action_tag breakdown
    const actionTagRows = await db.safeQuery(`
      SELECT 
        COALESCE(action_tag, 'NULL') as action_tag,
        COUNT(*) as count
      FROM vote_requests
      GROUP BY action_tag
      ORDER BY count DESC
      LIMIT 20
    `);
    
    // CRITICAL: Sample records with NULL action_tag to see what's in their payload
    let nullActionTagSample = [];
    try {
      nullActionTagSample = await db.safeQuery(`
        SELECT 
          event_id,
          contract_id,
          status,
          vote_count,
          is_human,
          LENGTH(payload) as payload_length,
          SUBSTRING(payload, 1, 800) as payload_preview
        FROM vote_requests
        WHERE action_tag IS NULL
        LIMIT 5
      `);
    } catch (nullSampleErr) {
      console.error('Error fetching nullActionTagSample:', nullSampleErr);
      nullActionTagSample = [{ error: nullSampleErr.message }];
    }
    
    const stats = statsRows[0] || {};
    console.log(`   Total records: ${stats.total}`);
    console.log(`   With action_tag: ${stats.with_action_tag}`);
    console.log(`   With requester: ${stats.with_requester}`);
    console.log(`   With reason: ${stats.with_reason}`);
    console.log(`   With votes: ${stats.with_votes}`);
    console.log(`   Human proposals: ${stats.human_proposals}`);
    console.log(`   With payload: ${stats.with_payload}`);
    
    res.json(convertBigInts({
      stats,
      statusBreakdown: statusRows,
      actionTagBreakdown: actionTagRows,
      sample: sampleRows,
      nullActionTagSample,
      message: 'Debug data from vote_requests DuckDB table'
    }));
  } catch (err) {
    console.error('Error in vote-requests debug-table:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/template-scan - Scan all binary files to find unique templates (for debugging)
router.get('/template-scan', async (req, res) => {
  try {
    const maxFiles = Math.min(parseInt(req.query.maxFiles) || 5000, 20000);
    const searchTemplate = req.query.template || null;
    const sources = getDataSources();
    
    console.log(`\n🔍 TEMPLATE-SCAN: Scanning up to ${maxFiles} files...`);
    if (searchTemplate) {
      console.log(`   Searching for template containing: "${searchTemplate}"`);
    }
    
    if (sources.primarySource !== 'binary') {
      return res.json({ error: 'Template scan only works with binary source', source: sources.primarySource });
    }
    
    const templateCounts = {};
    const choiceCounts = {};
    const sampleEvents = [];
    let totalEvents = 0;
    let filesScanned = 0;
    let oldestDate = null;
    let newestDate = null;
    
    // Scan with wide date range
    const result = await binaryReader.streamRecords(db.DATA_PATH, 'events', {
      limit: 100000, // High limit to scan many records
      offset: 0,
      maxDays: 365 * 3, // 3 years
      maxFilesToScan: maxFiles,
      sortBy: 'effective_at',
      filter: (e) => {
        totalEvents++;
        
        // Track date range
        if (e.effective_at) {
          const d = new Date(e.effective_at);
          if (!oldestDate || d < oldestDate) oldestDate = d;
          if (!newestDate || d > newestDate) newestDate = d;
        }
        
        // Extract template name
        const templateParts = e.template_id?.split(':') || [];
        const template = templateParts[templateParts.length - 1] || 'unknown';
        templateCounts[template] = (templateCounts[template] || 0) + 1;
        
        // Track choices
        if (e.choice) {
          choiceCounts[e.choice] = (choiceCounts[e.choice] || 0) + 1;
        }
        
        // If searching for a specific template, collect samples
        if (searchTemplate && e.template_id?.toLowerCase().includes(searchTemplate.toLowerCase())) {
          if (sampleEvents.length < 5) {
            sampleEvents.push({
              event_id: e.event_id,
              template_id: e.template_id,
              event_type: e.event_type,
              choice: e.choice,
              effective_at: e.effective_at,
              payload_keys: e.payload ? Object.keys(e.payload) : null,
              has_action: !!e.payload?.action,
              has_votes: !!e.payload?.votes,
              has_requester: !!e.payload?.requester,
            });
          }
          return true; // Include in results
        }
        
        return false; // Don't include in results (we just want counts)
      }
    });
    
    // Sort templates by count
    const sortedTemplates = Object.entries(templateCounts)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 50);
    
    // Find governance-related templates
    const governanceTemplates = Object.entries(templateCounts)
      .filter(([name]) => 
        name.includes('VoteRequest') || 
        name.includes('Confirmation') || 
        name.includes('Election') ||
        name.includes('DsoRules')
      )
      .sort((a, b) => b[1] - a[1]);
    
    console.log(`\n   📊 Scanned ${totalEvents} events`);
    console.log(`   📅 Date range: ${oldestDate?.toISOString().split('T')[0]} to ${newestDate?.toISOString().split('T')[0]}`);
    console.log(`   📈 Unique templates: ${Object.keys(templateCounts).length}`);
    console.log(`   🏛️ Governance templates found:`, governanceTemplates);
    
    res.json(convertBigInts({
      totalEventsScanned: totalEvents,
      dateRange: {
        oldest: oldestDate?.toISOString(),
        newest: newestDate?.toISOString(),
      },
      uniqueTemplates: Object.keys(templateCounts).length,
      uniqueChoices: Object.keys(choiceCounts).length,
      topTemplates: sortedTemplates,
      governanceTemplates,
      topChoices: Object.entries(choiceCounts).sort((a, b) => b[1] - a[1]).slice(0, 30),
      sampleEvents: sampleEvents.length > 0 ? sampleEvents : undefined,
    }));
  } catch (err) {
    console.error('Error in template scan:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/vote-requests/raw - Raw VoteRequest events directly from binary files via template index
// No deduplication, no status processing - just raw events
router.get('/vote-requests/raw', async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 500, 10000);
    const offset = parseInt(req.query.offset) || 0;
    const eventType = req.query.type || 'all'; // 'created', 'exercised', 'all'
    
    console.log(`\n🗳️ VOTE-REQUESTS/RAW: Fetching raw events, limit=${limit}, offset=${offset}, type=${eventType}`);
    
    // Check if template index is available
    const templateIndexReady = await isTemplateIndexPopulated();
    if (!templateIndexReady) {
      return res.status(503).json({ 
        error: 'Template index not populated. Build it first via /api/engine/template-index/build',
        templateIndexReady: false,
      });
    }
    
    // Get all files containing VoteRequest events
    const voteRequestFiles = await getFilesForTemplate('VoteRequest');
    console.log(`   📋 Found ${voteRequestFiles.length} VoteRequest files in template index`);
    
    if (voteRequestFiles.length === 0) {
      return res.json({ 
        data: [], 
        count: 0, 
        totalFiles: 0,
        source: 'template-index-binary',
      });
    }
    
    // Read all VoteRequest events from binary files
    const allEvents = [];
    let filesScanned = 0;
    const startTime = Date.now();
    
    for (const file of voteRequestFiles) {
      try {
        const result = await binaryReader.readBinaryFile(file);
        const fileRecords = result.records || [];
        
        for (const e of fileRecords) {
          if (!e.template_id?.includes('VoteRequest')) continue;
          
          // Filter by event type if specified
          if (eventType === 'created' && e.event_type !== 'created') continue;
          if (eventType === 'exercised' && e.event_type !== 'exercised') continue;
          
          allEvents.push({
            event_id: e.event_id,
            contract_id: e.contract_id,
            template_id: e.template_id,
            event_type: e.event_type,
            choice: e.choice || null,
            effective_at: e.effective_at,
            timestamp: e.timestamp,
            payload: e.payload,
          });
        }
        
        filesScanned++;
      } catch (err) {
        // Skip unreadable files
        console.warn(`   ⚠️ Could not read file: ${file}`);
      }
    }
    
    const scanDuration = Date.now() - startTime;
    console.log(`   ✅ Scanned ${filesScanned} files in ${scanDuration}ms, found ${allEvents.length} raw VoteRequest events`);
    
    // Sort by effective_at descending
    allEvents.sort((a, b) => {
      const dateA = new Date(a.effective_at || 0).getTime();
      const dateB = new Date(b.effective_at || 0).getTime();
      return dateB - dateA;
    });
    
    // Apply pagination
    const paginatedEvents = allEvents.slice(offset, offset + limit);
    
    res.json(convertBigInts({ 
      data: paginatedEvents, 
      count: paginatedEvents.length,
      totalFound: allEvents.length,
      totalFiles: voteRequestFiles.length,
      filesScanned,
      scanDurationMs: scanDuration,
      hasMore: offset + limit < allEvents.length,
      source: 'template-index-binary',
    }));
  } catch (err) {
    console.error('Error fetching raw vote requests:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/vote-request-index/status - Get index status
router.get('/vote-request-index/status', async (req, res) => {
  try {
    const stats = await voteRequestIndexer.getVoteRequestStats();
    const canonicalStats = await voteRequestIndexer.getCanonicalProposalStats();
    const state = await voteRequestIndexer.getIndexState();
    const isIndexing = voteRequestIndexer.isIndexingInProgress();
    const progress = voteRequestIndexer.getIndexingProgress?.();
    const lastSuccessfulBuild = await voteRequestIndexer.getLastSuccessfulBuild();
    
    // Check if a stale lock exists (lock file present but we're not indexing)
    let lockExists = false;
    if (!isIndexing) {
      const lockPath = path.join(db.DATA_PATH, '.locks', 'vote_request_index.lock');
      try {
        await fs.promises.access(lockPath);
        lockExists = true;
      } catch {
        // No lock file
      }
    }

    res.json(convertBigInts({
      populated: stats.total > 0,
      isIndexing,
      lockExists,
      // Raw stats (all records including config maintenance)
      stats,
      // Human-readable stats (explorer-matching: excludes SetConfig, requires narrative or votes)
      humanStats: {
        total: canonicalStats.humanProposals,
        inProgress: canonicalStats.byStatus.in_progress,
        executed: canonicalStats.byStatus.executed,
        rejected: canonicalStats.byStatus.rejected,
        expired: canonicalStats.byStatus.expired,
      },
      // Summary counts for understanding the data layers
      layers: {
        rawEvents: canonicalStats.rawEvents,
        lifecycleProposals: canonicalStats.lifecycleProposals,
        humanProposals: canonicalStats.humanProposals,
      },
      lastIndexedAt: state.last_indexed_at,
      totalIndexed: state.total_indexed,
      progress: isIndexing ? progress : null,
      lastSuccessfulBuild,
    }));
  } catch (err) {
    console.error('Error getting vote request index status:', err);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/events/vote-request-index/build - Trigger index build
router.post('/vote-request-index/build', async (req, res) => {
  try {
    const force = req.query.force === 'true' || req.body?.force === true;

    if (voteRequestIndexer.isIndexingInProgress()) {
      return res.json({ status: 'in_progress', message: 'Indexing already in progress' });
    }

    // Start indexing in background
    res.json({ status: 'started', message: 'VoteRequest index build started', force });

    // Run async
    voteRequestIndexer.buildVoteRequestIndex({ force }).catch(err => {
      console.error('Background index build failed:', err);
    });
  } catch (err) {
    console.error('Error starting vote request index build:', err);
    res.status(500).json({ error: err.message });
  }
});

// DELETE /api/events/vote-request-index/lock - Clear stale lock
router.delete('/vote-request-index/lock', async (req, res) => {
  try {
    const result = await voteRequestIndexer.clearStaleLock();
    res.json(result);
  } catch (err) {
    console.error('Error clearing vote request index lock:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/vote-request-index/validate - Self-test: sample proposals and show evidence
router.get('/vote-request-index/validate', async (req, res) => {
  try {
    const sampleSize = Math.min(parseInt(req.query.size) || 5, 20);
    
    // Query samples from each status category
    const sampleStatuses = ['executed', 'rejected', 'expired', 'in_progress'];
    const samples = {};
    
    for (const status of sampleStatuses) {
      const rows = await db.safeQuery(`
        SELECT 
          contract_id, event_id, template_id, effective_at,
          status, is_closed, action_tag, action_value,
          requester, reason, votes, vote_count,
          vote_before, payload
        FROM vote_requests
        WHERE status = '${status}'
        ORDER BY effective_at DESC
        LIMIT ${sampleSize}
      `);
      
      samples[status] = rows.map(r => {
        // Parse votes to show accept/reject breakdown
        let acceptCount = 0;
        let rejectCount = 0;
        const votesArray = Array.isArray(r.votes) ? r.votes : (typeof r.votes === 'string' ? JSON.parse(r.votes || '[]') : []);
        
        for (const vote of votesArray) {
          const [, voteData] = Array.isArray(vote) ? vote : ['', vote];
          if (!voteData || typeof voteData !== 'object') continue;
          
          // Normalize vote
          if (voteData.accept === true || voteData.Accept === true) acceptCount++;
          else if (voteData.reject === true || voteData.Reject === true) rejectCount++;
          else if (voteData.accept === false) rejectCount++;
          else {
            const tag = voteData.tag || voteData.Tag || voteData.vote?.tag;
            if (typeof tag === 'string') {
              const t = tag.toLowerCase();
              if (t === 'accept') acceptCount++;
              else if (t === 'reject') rejectCount++;
            } else if (Object.prototype.hasOwnProperty.call(voteData, 'Accept')) acceptCount++;
            else if (Object.prototype.hasOwnProperty.call(voteData, 'Reject')) rejectCount++;
          }
        }
        
        const voteBefore = r.vote_before ? new Date(r.vote_before) : null;
        const isExpired = voteBefore && voteBefore < new Date();
        
        return {
          contract_id: r.contract_id,
          event_id: r.event_id,
          effective_at: r.effective_at,
          status: r.status,
          is_closed: r.is_closed,
          action_tag: r.action_tag,
          vote_count: r.vote_count || votesArray.length,
          accept_count: acceptCount,
          reject_count: rejectCount,
          vote_before: r.vote_before,
          deadline_passed: isExpired,
          reason_preview: typeof r.reason === 'string' ? r.reason.slice(0, 100) : null,
          // Evidence summary
          evidence: {
            has_votes: votesArray.length > 0,
            has_rejects: rejectCount > 0,
            has_accepts: acceptCount > 0,
            closed: r.is_closed,
            expired: isExpired,
          },
        };
      });
    }
    
    // Summary
    const summary = {
      executed: samples.executed?.length || 0,
      rejected: samples.rejected?.length || 0,
      expired: samples.expired?.length || 0,
      in_progress: samples.in_progress?.length || 0,
    };
    
    // Potential issues: executed with reject votes, rejected with only accept votes
    const potentialIssues = [];
    for (const r of samples.executed || []) {
      if (r.reject_count > 0) {
        potentialIssues.push({
          type: 'executed_with_rejects',
          contract_id: r.contract_id,
          message: `Executed proposal has ${r.reject_count} reject votes`,
        });
      }
    }
    for (const r of samples.rejected || []) {
      if (r.accept_count > 0 && r.reject_count === 0) {
        potentialIssues.push({
          type: 'rejected_but_only_accepts',
          contract_id: r.contract_id,
          message: `Rejected proposal has ${r.accept_count} accept votes and 0 reject votes`,
        });
      }
    }
    
    res.json(convertBigInts({
      summary,
      potentialIssues,
      samples,
    }));
  } catch (err) {
    console.error('Error validating vote request index:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/vote-request-index/debug-event - Show raw event structure for a contract_id
router.get('/vote-request-index/debug-event', async (req, res) => {
  try {
    const { contract_id } = req.query;
    if (!contract_id) {
      return res.status(400).json({ error: 'contract_id query param required' });
    }

    // Scan for events with this contract_id
    const sources = getDataSources();
    if (sources.primarySource !== 'binary') {
      return res.status(400).json({ error: 'Debug only works with binary source' });
    }

    const result = await binaryReader.streamRecords(db.DATA_PATH, 'events', {
      limit: 100,
      offset: 0,
      maxDays: 365 * 3,
      maxFilesToScan: 10000,
      sortBy: 'effective_at',
      filter: (e) => e.contract_id === contract_id,
    });

    const events = result.records.map(e => ({
      event_id: e.event_id,
      event_type: e.event_type,
      choice: e.choice || null,
      template_id: e.template_id,
      effective_at: e.effective_at,
      // Show ALL fields to debug vote location
      payload_keys: e.payload ? Object.keys(e.payload) : [],
      payload_votes: e.payload?.votes,
      payload_votes_length: e.payload?.votes?.length,
      payload_vote_sample: e.payload?.votes?.[0],
      // For exercised events, check argument
      exercise_result: e.exercise_result,
      exercise_result_keys: e.exercise_result ? Object.keys(e.exercise_result) : [],
      // Raw payload structure
      full_payload: e.payload,
    }));

    res.json(convertBigInts({ contract_id, events }));
  } catch (err) {
    console.error('Error debugging event:', err);
    res.status(500).json({ error: err.message });
  }
});

// ============ REWARD COUPON INDEX ROUTES ============

// GET /api/events/reward-coupon-index/status - Get index status
router.get('/reward-coupon-index/status', async (req, res) => {
  try {
    const stats = await rewardIndexer.getRewardCouponStats();
    const state = await rewardIndexer.getIndexState();
    const isIndexing = rewardIndexer.isIndexingInProgress();
    const progress = rewardIndexer.getIndexingProgress();
    
    res.json(convertBigInts({
      populated: stats.total > 0,
      isIndexing,
      stats,
      lastIndexedAt: state.last_indexed_at,
      totalIndexed: state.total_indexed,
      progress: isIndexing ? progress : null,
    }));
  } catch (err) {
    console.error('Error getting reward coupon index status:', err);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/events/reward-coupon-index/build - Trigger index build
router.post('/reward-coupon-index/build', async (req, res) => {
  try {
    const force = req.body?.force === true || req.query.force === 'true';
    
    if (rewardIndexer.isIndexingInProgress()) {
      return res.json({ status: 'in_progress', message: 'Indexing already in progress' });
    }
    
    // Start indexing in background
    res.json({ status: 'started', message: 'RewardCoupon index build started' });
    
    // Run async
    rewardIndexer.buildRewardCouponIndex({ force }).catch(err => {
      console.error('Background reward index build failed:', err);
    });
  } catch (err) {
    console.error('Error starting reward coupon index build:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/reward-coupon-index/query - Query indexed rewards
router.get('/reward-coupon-index/query', async (req, res) => {
  try {
    const { limit, offset, couponType, beneficiary, startRound, endRound, startDate, endDate } = req.query;
    
    const results = await rewardIndexer.queryRewardCoupons({
      limit: parseInt(limit) || 100,
      offset: parseInt(offset) || 0,
      couponType: couponType || null,
      beneficiary: beneficiary || null,
      startRound: startRound ? parseInt(startRound) : null,
      endRound: endRound ? parseInt(endRound) : null,
      startDate: startDate || null,
      endDate: endDate || null,
    });
    
    const stats = await rewardIndexer.getRewardCouponStats();
    
    res.json(convertBigInts({
      data: results,
      count: results.length,
      stats,
    }));
  } catch (err) {
    console.error('Error querying reward coupons:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/reward-coupon-index/beneficiary/:id - Get rewards for a specific beneficiary
router.get('/reward-coupon-index/beneficiary/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const { startRound, endRound, startDate, endDate } = req.query;
    
    const result = await rewardIndexer.getRewardsByBeneficiary(id, {
      startRound: startRound ? parseInt(startRound) : null,
      endRound: endRound ? parseInt(endRound) : null,
      startDate: startDate || null,
      endDate: endDate || null,
    });
    
    res.json(convertBigInts(result));
  } catch (err) {
    console.error('Error getting rewards for beneficiary:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/by-contract/:contractId - Get all events for a specific contract (debug endpoint)
router.get('/by-contract/:contractId', async (req, res) => {
  try {
    const { contractId } = req.params;
    const sources = getDataSources();
    
    console.log(`\n🔍 Searching for events with contract_id: ${contractId.slice(0, 20)}...`);
    
    if (sources.primarySource === 'binary') {
      // Deep scan all binary files for this specific contract
      const result = await binaryReader.streamRecords(db.DATA_PATH, 'events', {
        limit: 100,
        offset: 0,
        maxDays: 3650, // 10 years - search all data
        maxFilesToScan: 10000, // Search extensively
        sortBy: 'effective_at',
        filter: (e) => e.contract_id === contractId
      });
      
      console.log(`   Found ${result.records.length} events for contract`);
      result.records.forEach((e, i) => {
        console.log(`   Event ${i + 1}: type=${e.event_type}, choice=${e.choice || 'N/A'}, template=${e.template_id?.split(':').pop()}`);
      });
      
      return res.json(convertBigInts({ 
        data: result.records.map(r => ({
          event_id: r.event_id,
          event_type: r.event_type,
          choice: r.choice,
          template_id: r.template_id,
          effective_at: r.effective_at,
          payload: r.payload,
        })),
        count: result.records.length, 
        hasMore: result.hasMore, 
        source: 'binary' 
      }));
    }
    
    res.json({ data: [], count: 0, source: 'none', error: 'Binary source required' });
  } catch (err) {
    console.error('Error fetching events by contract:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/debug-vote-requests - Analyze sample vote requests to understand choice patterns
router.get('/debug-vote-requests', async (req, res) => {
  try {
    const contractIds = (req.query.ids || '').split(',').filter(Boolean);
    if (contractIds.length === 0) {
      return res.status(400).json({ error: 'Provide contract IDs as ?ids=id1,id2,id3' });
    }
    
    console.log(`\n🔍 Analyzing ${contractIds.length} vote request contracts...`);
    const sources = getDataSources();
    
    if (sources.primarySource !== 'binary') {
      return res.status(400).json({ error: 'Binary source required' });
    }
    
    const results = [];
    const choiceStats = {};
    
    for (const contractId of contractIds.slice(0, 50)) { // Max 50
      const result = await binaryReader.streamRecords(db.DATA_PATH, 'events', {
        limit: 50,
        maxDays: 3650,
        maxFilesToScan: 10000,
        filter: (e) => e.contract_id === contractId
      });
      
      const events = result.records.map(r => ({
        event_type: r.event_type,
        choice: r.choice || null,
        template: r.template_id?.split(':').pop() || r.template_id,
      }));
      
      // Track choice patterns
      events.forEach(e => {
        if (e.choice) {
          choiceStats[e.choice] = (choiceStats[e.choice] || 0) + 1;
        }
      });
      
      results.push({
        contract_id: contractId.slice(0, 20) + '...',
        events,
      });
    }
    
    console.log(`\n📊 Choice breakdown across ${contractIds.length} contracts:`, choiceStats);
    
    res.json(convertBigInts({
      analyzed: results.length,
      choiceStats,
      results,
    }));
  } catch (err) {
    console.error('Error debugging vote requests:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/debug-vote-request/:contractId - Get detailed debug info for a single VoteRequest
router.get('/debug-vote-request/:contractId', async (req, res) => {
  try {
    const { contractId } = req.params;
    if (!contractId) {
      return res.status(400).json({ error: 'Contract ID required' });
    }

    console.log(`\n🔍 Debug VoteRequest: ${contractId.slice(0, 30)}...`);
    const sources = getDataSources();

    if (sources.primarySource !== 'binary') {
      return res.status(400).json({ error: 'Binary source required for debug' });
    }

    // Find all events for this contract_id
    const result = await binaryReader.streamRecords(db.DATA_PATH, 'events', {
      limit: 100,
      maxDays: 3650,
      maxFilesToScan: 100000,
      fullScan: true,
      filter: (e) => e.contract_id === contractId
    });

    // Dedupe by event_id (id field in the JSON)
    const seen = new Set();
    const deduped = [];
    for (const r of result.records) {
      const eventId = r.event_id || r.id;
      if (eventId && seen.has(eventId)) continue;
      if (eventId) seen.add(eventId);
      deduped.push(r);
    }

    // Identify created event and closing event(s)
    let createdEvent = null;
    const exercisedEvents = [];
    const closingEvents = [];

    for (const r of deduped) {
      if (r.event_type === 'created' && r.template_id?.endsWith(':VoteRequest')) {
        createdEvent = r;
      } else if (r.event_type === 'exercised' || r.event_type === 'archived') {
        exercisedEvents.push(r);
        
        // Check if this is a "closing" choice
        const choice = String(r.choice || '');
        const isClosingChoice =
          choice === 'Archive' ||
          /(^|_)VoteRequest_(Accept|Reject|Expire)/.test(choice) ||
          choice === 'VoteRequest_ExpireVoteRequest';

        if (isClosingChoice) {
          closingEvents.push(r);
        }
      }
    }

    // Get indexed record from DuckDB if available
    let indexedRecord = null;
    try {
      const indexed = await voteRequestIndexer.queryVoteRequests({ limit: 1, status: 'all', offset: 0 });
      // Query specifically by contract_id
      const rows = await db.safeQuery(`
        SELECT * FROM vote_requests WHERE contract_id = '${contractId.replace(/'/g, "''")}'
      `);
      if (rows.length > 0) {
        indexedRecord = rows[0];
      }
    } catch {
      // Index may not exist
    }

    // Parse payload fields from created event
    const payload = createdEvent?.payload || {};
    const parsedFields = {
      action: payload.action || null,
      requester: payload.requester || null,
      reason: payload.reason || null,
      votes: payload.votes || null,
      voteBefore: payload.voteBefore || null,
      targetEffectiveAt: payload.targetEffectiveAt || null,
      trackingCid: payload.trackingCid || null,
      dso: payload.dso || null,
    };

    res.json(convertBigInts({
      contractId,
      totalEventsFound: result.records.length,
      dedupedCount: deduped.length,
      createdEvent: createdEvent ? {
        event_id: createdEvent.event_id,
        event_type: createdEvent.event_type,
        template_id: createdEvent.template_id,
        effective_at: createdEvent.effective_at,
        timestamp: createdEvent.timestamp,
        file: createdEvent._source_file || null,
      } : null,
      exercisedEvents: exercisedEvents.map(e => ({
        event_id: e.event_id,
        event_type: e.event_type,
        choice: e.choice,
        template_id: e.template_id,
        effective_at: e.effective_at,
        file: e._source_file || null,
        isClosingChoice: closingEvents.includes(e),
      })),
      closingEventUsed: closingEvents.length > 0 ? {
        event_id: closingEvents[0].event_id,
        choice: closingEvents[0].choice,
        template_id: closingEvents[0].template_id,
        effective_at: closingEvents[0].effective_at,
        file: closingEvents[0]._source_file || null,
        exerciseResult: closingEvents[0].exercise_result || null,
      } : null,
      parsedPayloadFields: parsedFields,
      indexedRecord: indexedRecord ? {
        event_id: indexedRecord.event_id,
        status: indexedRecord.status,
        is_closed: indexedRecord.is_closed,
        action_tag: indexedRecord.action_tag,
        vote_count: indexedRecord.vote_count,
        vote_before: indexedRecord.vote_before,
        requester: indexedRecord.requester,
        reason: indexedRecord.reason,
      } : null,
    }));
  } catch (err) {
    console.error('Error debugging vote request:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/debug/dsorules-choices - Sample DsoRules exercised events to see what choice names exist
// Memory-efficient: counts without accumulating records
router.get('/debug/dsorules-choices', async (req, res) => {
  try {
    const sources = getDataSources();
    if (sources.primarySource !== 'binary') {
      return res.status(400).json({ error: 'Binary files required for this diagnostic' });
    }

    const maxFiles = Math.min(parseInt(req.query.files) || 500, 2000);
    const choiceCounts = {};
    let filesScanned = 0;
    let exercisedCount = 0;
    let dsoRulesCount = 0;
    const sampleEvents = [];

    // Use the binary reader's file finder which handles partition structure
    const allFiles = binaryReader.findBinaryFiles(db.DATA_PATH, 'events');
    
    if (allFiles.length === 0) {
      return res.json({ error: 'No binary event files found', path: db.DATA_PATH });
    }

    // Take a sample from the files (spread across the dataset)
    const step = Math.max(1, Math.floor(allFiles.length / maxFiles));
    const binFiles = allFiles.filter((_, i) => i % step === 0).slice(0, maxFiles);
    for (const filePath of binFiles) {
      try {
        const result = await binaryReader.readBinaryFile(filePath);
        const events = result.records || [];
        filesScanned++;

        for (const e of events) {
          if (e.event_type !== 'exercised') continue;
          exercisedCount++;
          
          const tmpl = e.template_id || e.template || '';
          if (!tmpl.includes('DsoRules')) continue;
          dsoRulesCount++;

          const choice = e.choice || '(no choice)';
          choiceCounts[choice] = (choiceCounts[choice] || 0) + 1;

          // Keep samples of vote/close-related
          const cl = choice.toLowerCase();
          if (sampleEvents.length < 10 && (cl.includes('vote') || cl.includes('close'))) {
            sampleEvents.push({
              choice,
              contract_id: e.contract_id,
              template_id: tmpl,
              has_exercise_result: !!e.exercise_result,
              exercise_result_keys: e.exercise_result ? Object.keys(e.exercise_result) : [],
            });
          }
        }

        // Log progress every 50 files
        if (filesScanned % 50 === 0) {
          console.log(`[dsorules-choices] ${filesScanned}/${binFiles.length} files | ${dsoRulesCount} DsoRules exercised`);
        }
      } catch (err) {
        // Skip unreadable files
      }
    }

    const sortedChoices = Object.entries(choiceCounts)
      .sort((a, b) => b[1] - a[1]);

    const voteRelated = sortedChoices.filter(([c]) => {
      const cl = c.toLowerCase();
      return cl.includes('vote') || cl.includes('close') || cl.includes('expire') || cl.includes('reject') || cl.includes('accept');
    });

    res.json(convertBigInts({
      summary: {
        filesScanned,
        totalFiles: binFiles.length,
        exercisedEventsFound: exercisedCount,
        dsoRulesExercisedFound: dsoRulesCount,
        uniqueChoices: sortedChoices.length,
        voteRelatedChoices: voteRelated.length,
      },
      voteRelatedChoices: voteRelated,
      allChoices: sortedChoices.slice(0, 100),
      sampleVoteEvents: sampleEvents,
    }));
  } catch (err) {
    console.error('Error in dsorules-choices diagnostic:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/debug/execute-confirmed-action - Sample DsoRules_ExecuteConfirmedAction events
router.get('/debug/execute-confirmed-action', async (req, res) => {
  try {
    const sources = getDataSources();
    if (sources.primarySource !== 'binary') {
      return res.status(400).json({ error: 'Binary files required for this diagnostic' });
    }

    const maxFiles = Math.min(parseInt(req.query.files) || 500, 2000);
    const maxSamples = Math.min(parseInt(req.query.samples) || 20, 100);
    let filesScanned = 0;
    let matchCount = 0;
    const samples = [];

    const allFiles = binaryReader.findBinaryFiles(db.DATA_PATH, 'events');
    
    if (allFiles.length === 0) {
      return res.json({ error: 'No binary event files found', path: db.DATA_PATH });
    }

    // Spread sampling across the dataset
    const step = Math.max(1, Math.floor(allFiles.length / maxFiles));
    const binFiles = allFiles.filter((_, i) => i % step === 0).slice(0, maxFiles);

    for (const filePath of binFiles) {
      if (samples.length >= maxSamples) break;
      
      try {
        const result = await binaryReader.readBinaryFile(filePath);
        const events = result.records || [];
        filesScanned++;

        for (const e of events) {
          if (e.event_type !== 'exercised') continue;
          if (e.choice !== 'DsoRules_ExecuteConfirmedAction') continue;
          
          matchCount++;
          
          if (samples.length < maxSamples) {
            const exerciseResult = e.exercise_result || {};
            const payload = e.payload || {};
            const raw = e.raw || {};
            
            // Choice arguments are typically in payload or raw.choice_argument
            const choiceArg = raw.choice_argument || payload.choice_argument || payload;
            
            samples.push({
              contract_id: e.contract_id,
              template_id: e.template_id,
              timestamp: e.timestamp,
              // Check all possible locations for the confirmation data
              payload_keys: Object.keys(payload),
              raw_keys: Object.keys(raw),
              choice_argument_preview: JSON.stringify(choiceArg).slice(0, 3000),
              exercise_result_preview: JSON.stringify(exerciseResult).slice(0, 500),
              // Look for confirmation-related fields
              has_confirmation_cid: !!(choiceArg.confirmationCid || choiceArg.confirmation),
              confirmation_cid: choiceArg.confirmationCid || choiceArg.confirmation || null,
            });
          }
        }

        if (filesScanned % 100 === 0) {
          console.log(`[execute-confirmed] ${filesScanned}/${binFiles.length} files | ${matchCount} matches`);
        }
      } catch (err) {
        // Skip unreadable files
      }
    }

    // Analyze the structure patterns based on payload keys
    const structurePatterns = {};
    for (const s of samples) {
      const key = (s.payload_keys || []).sort().join(',') || '(empty)';
      structurePatterns[key] = (structurePatterns[key] || 0) + 1;
    }

    res.json(convertBigInts({
      summary: {
        filesScanned,
        totalFilesInDataset: allFiles.length,
        executeConfirmedActionCount: matchCount,
        samplesCollected: samples.length,
      },
      structurePatterns: Object.entries(structurePatterns).sort((a, b) => b[1] - a[1]),
      samples,
    }));
  } catch (err) {
    console.error('Error in execute-confirmed-action diagnostic:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/debug/confirm-action - Sample DsoRules_ConfirmAction events to see Arc action structure
router.get('/debug/confirm-action', async (req, res) => {
  try {
    const sources = getDataSources();
    if (sources.primarySource !== 'binary') {
      return res.status(400).json({ error: 'Binary files required for this diagnostic' });
    }

    const maxFiles = Math.min(parseInt(req.query.files) || 500, 2000);
    const maxSamples = Math.min(parseInt(req.query.samples) || 20, 100);
    let filesScanned = 0;
    let matchCount = 0;
    const samples = [];
    const actionTagCounts = {};

    const allFiles = binaryReader.findBinaryFiles(db.DATA_PATH, 'events');
    
    if (allFiles.length === 0) {
      return res.json({ error: 'No binary event files found', path: db.DATA_PATH });
    }

    // Spread sampling across the dataset
    const step = Math.max(1, Math.floor(allFiles.length / maxFiles));
    const binFiles = allFiles.filter((_, i) => i % step === 0).slice(0, maxFiles);

    for (const filePath of binFiles) {
      try {
        const result = await binaryReader.readBinaryFile(filePath);
        const events = result.records || [];
        filesScanned++;

        for (const e of events) {
          if (e.event_type !== 'exercised') continue;
          if (e.choice !== 'DsoRules_ConfirmAction') continue;
          
          matchCount++;
          
          const payload = e.payload || {};
          const raw = e.raw || {};
          const choiceArg = raw.choice_argument || payload.choice_argument || payload;
          
          // Extract action tag from the Arc structure
          // Handle multiple encoding styles:
          // 1. Protobuf-style: record.fields[0].value.variant.constructor
          // 2. JSON-style: action.tag or action.value.tag
          // 3. Variant-as-key: { ARC_DsoRules: {...} }
          
          let actionTag = null;
          let innerActionTag = null;
          let actionStructure = null;
          
          // Try protobuf-style first (record.fields)
          const recordFields = choiceArg?.record?.fields;
          if (Array.isArray(recordFields) && recordFields.length > 0) {
            const firstField = recordFields[0]?.value?.variant;
            if (firstField?.constructor) {
              actionTag = firstField.constructor;
              actionStructure = firstField.value;
              
              // Try to get inner action tag (e.g., CRARC_MiningRound_Archive inside ARC_AmuletRules)
              const innerFields = firstField.value?.record?.fields;
              if (Array.isArray(innerFields) && innerFields.length > 0) {
                const innerVariant = innerFields[0]?.value?.variant;
                if (innerVariant?.constructor) {
                  innerActionTag = innerVariant.constructor;
                }
              }
            }
          }
          
          // Fallback: try JSON-style encoding
          if (!actionTag) {
            const action = choiceArg.action || choiceArg.confirmedAction || {};
            if (action.tag) {
              actionTag = action.tag;
            } else if (action.value?.tag) {
              actionTag = action.value.tag;
            } else {
              // Check for variant-as-key encoding
              const keys = Object.keys(action);
              if (keys.length > 0 && keys[0].startsWith('ARC_')) {
                actionTag = keys[0];
              }
            }
          }
          
          // Count both outer and inner action tags
          const fullTag = innerActionTag ? `${actionTag}::${innerActionTag}` : actionTag;
          if (fullTag) {
            actionTagCounts[fullTag] = (actionTagCounts[fullTag] || 0) + 1;
          }
          if (actionTag && !innerActionTag) {
            actionTagCounts[actionTag] = (actionTagCounts[actionTag] || 0) + 1;
          }
          
          if (samples.length < maxSamples) {
            samples.push({
              contract_id: e.contract_id,
              template_id: e.template_id,
              timestamp: e.timestamp,
              action_tag: actionTag,
              inner_action_tag: innerActionTag,
              full_tag: innerActionTag ? `${actionTag}::${innerActionTag}` : actionTag,
              payload_keys: Object.keys(payload),
              raw_keys: Object.keys(raw),
              choice_argument_preview: JSON.stringify(choiceArg).slice(0, 5000),
              action_structure_preview: actionStructure ? JSON.stringify(actionStructure).slice(0, 2000) : null,
            });
          }
        }

        if (filesScanned % 100 === 0) {
          console.log(`[confirm-action] ${filesScanned}/${binFiles.length} files | ${matchCount} matches`);
        }
      } catch (err) {
        // Skip unreadable files
      }
    }

    // Sort action tags by frequency
    const sortedActionTags = Object.entries(actionTagCounts)
      .sort((a, b) => b[1] - a[1]);

    res.json(convertBigInts({
      summary: {
        filesScanned,
        totalFilesInDataset: allFiles.length,
        confirmActionCount: matchCount,
        samplesCollected: samples.length,
        uniqueActionTags: sortedActionTags.length,
      },
      actionTagCounts: sortedActionTags,
      samples,
    }));
  } catch (err) {
    console.error('Error in confirm-action diagnostic:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/debug/vote-request-lifecycle - Diagnostic endpoint to find VoteRequest created/archived events
router.get('/debug/vote-request-lifecycle', async (req, res) => {
  try {
    const sources = getDataSources();
    if (sources.primarySource !== 'binary') {
      return res.status(400).json({ error: 'Binary files required for this diagnostic' });
    }

    const maxFiles = Math.min(parseInt(req.query.files) || 500, 5000);
    const limit = Math.min(parseInt(req.query.limit) || 50, 500);
    const focusArchived = req.query.archived === 'true';
    
    const allFiles = binaryReader.findBinaryFiles(db.DATA_PATH, 'events');
    
    if (allFiles.length === 0) {
      return res.json({ error: 'No binary event files found', path: db.DATA_PATH });
    }
    
    // Spread sampling across the dataset
    const step = Math.max(1, Math.floor(allFiles.length / maxFiles));
    const binFiles = allFiles.filter((_, i) => i % step === 0).slice(0, maxFiles);
    
    const createdEvents = [];
    const archivedEvents = [];
    let filesScanned = 0;
    let totalVoteRequestEvents = 0;
    
    for (const filePath of binFiles) {
      // Keep scanning if we're focusing on archived and haven't found enough
      const shouldStop = focusArchived 
        ? archivedEvents.length >= limit
        : (createdEvents.length >= limit && archivedEvents.length >= limit);
      if (shouldStop) break;
      
      try {
        const result = await binaryReader.readBinaryFile(filePath);
        const events = result.records || [];
        filesScanned++;
        
        for (const evt of events) {
          // Look for VoteRequest template events
          const templateId = evt.template_id || '';
          const isVoteRequest = templateId.includes('VoteRequest');
          
          if (!isVoteRequest) continue;
          totalVoteRequestEvents++;
          
          const eventType = evt.event_type || '';
          const raw = evt.raw || {};
          const payload = evt.payload || {};
          
          if (eventType === 'created' && createdEvents.length < limit) {
            createdEvents.push({
              event_id: evt.event_id,
              contract_id: evt.contract_id,
              template_id: templateId,
              timestamp: evt.timestamp,
              signatories: evt.signatories,
              observers: evt.observers,
              payload_keys: Object.keys(payload),
              raw_keys: Object.keys(raw),
              payload_preview: JSON.stringify(payload).substring(0, 1500),
            });
          } else if (eventType === 'archived' && archivedEvents.length < limit) {
            archivedEvents.push({
              event_id: evt.event_id,
              contract_id: evt.contract_id,
              template_id: templateId,
              timestamp: evt.timestamp,
              raw_keys: Object.keys(raw),
              raw_preview: JSON.stringify(raw).substring(0, 500),
            });
          }
        }
        
        if (filesScanned % 200 === 0) {
          console.log(`[vote-request-lifecycle] ${filesScanned}/${binFiles.length} files | created: ${createdEvents.length}, archived: ${archivedEvents.length}, total VR events: ${totalVoteRequestEvents}`);
        }
      } catch (err) {
        // Skip unreadable files
      }
    }
    
    res.json(convertBigInts({
      summary: {
        filesScanned,
        totalFilesInDataset: allFiles.length,
        createdCount: createdEvents.length,
        archivedCount: archivedEvents.length,
        totalVoteRequestEvents,
        focusArchived,
      },
      createdEvents,
      archivedEvents,
    }));
  } catch (err) {
    console.error('Error in vote-request-lifecycle diagnostic:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/debug/cast-vote - Diagnostic endpoint to sample DsoRules_CastVote events
router.get('/debug/cast-vote', async (req, res) => {
  try {
    const sources = getDataSources();
    if (sources.primarySource !== 'binary') {
      return res.status(400).json({ error: 'Binary files required for this diagnostic' });
    }

    const maxFiles = Math.min(parseInt(req.query.files) || 500, 2000);
    const maxSamples = Math.min(parseInt(req.query.limit) || 20, 100);
    
    const allFiles = binaryReader.findBinaryFiles(db.DATA_PATH, 'events');
    
    if (allFiles.length === 0) {
      return res.json({ error: 'No binary event files found', path: db.DATA_PATH });
    }
    
    // Spread sampling across the dataset
    const step = Math.max(1, Math.floor(allFiles.length / maxFiles));
    const binFiles = allFiles.filter((_, i) => i % step === 0).slice(0, maxFiles);
    
    const samples = [];
    let filesScanned = 0;
    let matchCount = 0;
    
    for (const filePath of binFiles) {
      if (samples.length >= maxSamples) break;
      
      try {
        const result = await binaryReader.readBinaryFile(filePath);
        const events = result.records || [];
        filesScanned++;
        
        for (const evt of events) {
          if (evt.event_type !== 'exercised') continue;
          
          const choice = evt.choice || '';
          
          // Look for CastVote choice exercises
          if (choice.includes('CastVote') || choice === 'DsoRules_CastVote') {
            matchCount++;
            
            if (samples.length < maxSamples) {
              const raw = evt.raw || {};
              const payload = evt.payload || {};
              const choiceArg = raw.choice_argument || payload.choice_argument || payload;
              
              samples.push({
                contract_id: evt.contract_id,
                template_id: evt.template_id,
                timestamp: evt.timestamp,
                choice: choice,
                event_type: evt.event_type,
                payload_keys: Object.keys(payload),
                raw_keys: Object.keys(raw),
                choice_argument_preview: JSON.stringify(choiceArg)?.substring(0, 1500),
                exercise_result_preview: JSON.stringify(raw.exercise_result)?.substring(0, 500),
                acting_parties: evt.acting_parties,
                child_event_ids_count: (evt.child_event_ids || []).length,
              });
            }
          }
        }
        
        if (filesScanned % 100 === 0) {
          console.log(`[cast-vote] ${filesScanned}/${binFiles.length} files | ${matchCount} matches`);
        }
      } catch (err) {
        // Skip unreadable files
      }
    }
    
    // Analyze structure patterns
    const structurePatterns = {};
    for (const s of samples) {
      const key = (s.payload_keys || []).sort().join(',') || '(empty)';
      structurePatterns[key] = (structurePatterns[key] || 0) + 1;
    }
    
    res.json(convertBigInts({
      summary: {
        filesScanned,
        totalFilesInDataset: allFiles.length,
        castVoteCount: matchCount,
        samplesCollected: samples.length,
      },
      structurePatterns: Object.entries(structurePatterns).sort((a, b) => b[1] - a[1]),
      samples,
    }));
  } catch (err) {
    console.error('Error in cast-vote diagnostic:', err);
    res.status(500).json({ error: err.message });
  }
});

// ============================================================================
// CANONICAL GOVERNANCE PROPOSAL ENDPOINTS
// ============================================================================
// These endpoints implement the canonical governance proposal model that
// matches major explorer semantics:
// - proposal_id = COALESCE(tracking_cid, contract_id) for lifecycle identity
// - is_human = true for explorer-visible proposals (excludes config maintenance)
// - Yields ~200-250 human-readable proposals matching explorers
// ============================================================================

// GET /api/events/canonical-proposals - Canonical governance proposals (explorer semantics)
// This is the PRIMARY endpoint for governance UIs matching explorer counts
router.get('/canonical-proposals', async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 100, 1000);
    const offset = parseInt(req.query.offset) || 0;
    const status = req.query.status || 'all';
    const humanOnly = req.query.human !== 'false'; // Default to human-only
    
    // Check if index is populated
    const indexPopulated = await voteRequestIndexer.isIndexPopulated();
    if (!indexPopulated) {
      return res.json({
        proposals: [],
        total: 0,
        source: 'index-empty',
        message: 'VoteRequest index not built yet. Trigger a rebuild at /api/events/vote-request-index/build',
      });
    }
    
    // Query canonical proposals from the index
    const proposals = await voteRequestIndexer.queryCanonicalProposals({ 
      limit, 
      status, 
      offset, 
      humanOnly 
    });
    const stats = await voteRequestIndexer.getCanonicalProposalStats();
    const indexState = await voteRequestIndexer.getIndexState();
    
    res.json(convertBigInts({
      proposals,
      total: humanOnly ? stats.humanProposals : stats.lifecycleProposals,
      stats: {
        rawEvents: stats.rawEvents,
        lifecycleProposals: stats.lifecycleProposals,
        humanProposals: stats.humanProposals,
        byStatus: stats.byStatus,
      },
      source: 'duckdb-index-canonical',
      indexedAt: indexState.last_indexed_at,
      _meta: {
        endpoint: '/api/events/canonical-proposals',
        description: 'Canonical governance proposals matching explorer semantics',
        model: {
          proposal_id: 'COALESCE(tracking_cid, contract_id) - lifecycle identity',
          is_human: 'true for explorer-visible proposals (excludes config maintenance)',
          accept_count: 'Number of accept votes',
          reject_count: 'Number of reject votes',
          related_count: 'Number of contract_ids for this proposal (migrations)',
        },
        counts: {
          rawEvents: 'All VoteRequest events (4500+)',
          lifecycleProposals: 'Unique proposal_ids (~400)',
          humanProposals: 'Explorer-visible proposals (~200-250)',
        },
      },
    }));
  } catch (err) {
    console.error('Error in canonical-proposals:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/canonical-proposals/stats - Get canonical proposal statistics
router.get('/canonical-proposals/stats', async (req, res) => {
  try {
    const indexPopulated = await voteRequestIndexer.isIndexPopulated();
    if (!indexPopulated) {
      return res.json({
        rawEvents: 0,
        lifecycleProposals: 0,
        humanProposals: 0,
        byStatus: { in_progress: 0, executed: 0, rejected: 0, expired: 0 },
        source: 'index-empty',
      });
    }
    
    const stats = await voteRequestIndexer.getCanonicalProposalStats();
    const indexState = await voteRequestIndexer.getIndexState();
    
    res.json(convertBigInts({
      ...stats,
      source: 'duckdb-index-canonical',
      indexedAt: indexState.last_indexed_at,
    }));
  } catch (err) {
    console.error('Error in canonical-proposals/stats:', err);
    res.status(500).json({ error: err.message });
  }
});

// ============================================================================
// LEGACY GOVERNANCE PROPOSAL ENDPOINTS (semantic_key based)
// ============================================================================

// GET /api/events/governance-proposals - Get governance proposals grouped by semantic key (from index)
// @deprecated Use /api/events/canonical-proposals for explorer-matching semantics
router.get('/governance-proposals', async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 100, 1000);
    const offset = parseInt(req.query.offset) || 0;
    const status = req.query.status || 'all';
    
    // Check if index is populated
    const indexPopulated = await voteRequestIndexer.isIndexPopulated();
    if (!indexPopulated) {
      return res.json({
        data: [],
        count: 0,
        source: 'index-empty',
        message: 'VoteRequest index not built yet. Trigger a rebuild at /api/events/vote-requests/index/build',
      });
    }
    
    // Query grouped proposals from the index
    const proposals = await voteRequestIndexer.queryGovernanceProposals({ limit, status, offset });
    const stats = await voteRequestIndexer.getVoteRequestStats();
    const indexState = await voteRequestIndexer.getIndexState();
    const canonicalStats = await voteRequestIndexer.getCanonicalProposalStats();
    
    res.json(convertBigInts({
      data: proposals,
      count: proposals.length,
      stats,
      canonicalStats, // Include canonical stats for migration
      source: 'duckdb-index',
      indexedAt: indexState.last_indexed_at,
      totalIndexed: indexState.total_indexed,
      _meta: {
        endpoint: '/api/events/governance-proposals',
        description: 'Governance proposals grouped by semantic_key for deduplication',
        deprecated: 'Use /api/events/canonical-proposals for explorer-matching semantics',
        fields: {
          semantic_key: 'Unique key combining action_type + subject for linking re-submitted proposals',
          action_subject: 'The target of the action (provider, sv, validator, etc.)',
          proposal_id: 'COALESCE(tracking_cid, contract_id) - canonical identity',
          is_human: 'true for explorer-visible proposals',
          timeline: {
            firstSeen: 'When the first proposal for this subject was created',
            lastSeen: 'When the most recent proposal for this subject was created',
            relatedCount: 'Number of related proposals (re-submissions)',
          },
        },
      },
    }));
  } catch (err) {
    console.error('Error in governance-proposals:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/governance-proposals/:semanticKey/timeline - Get full timeline for a proposal
router.get('/governance-proposals/:semanticKey/timeline', async (req, res) => {
  try {
    const semanticKey = decodeURIComponent(req.params.semanticKey);
    
    const timeline = await voteRequestIndexer.queryProposalTimeline(semanticKey);
    
    if (timeline.length === 0) {
      return res.status(404).json({ 
        error: 'Proposal not found',
        semanticKey,
      });
    }
    
    // Compute summary stats
    const latest = timeline[0];
    const oldest = timeline[timeline.length - 1];
    
    res.json(convertBigInts({
      semanticKey,
      latestStatus: latest.status,
      latestContractId: latest.contract_id,
      timeline,
      summary: {
        totalVersions: timeline.length,
        firstCreated: oldest.effective_at,
        lastUpdated: latest.effective_at,
        finalVoteCount: latest.vote_count,
        actionType: latest.action_tag,
        subject: latest.action_subject,
      },
      source: 'duckdb-index',
    }));
  } catch (err) {
    console.error('Error in governance-proposals timeline:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/governance/proposals - Get latest state for each unique proposal
router.get('/governance/proposals', async (req, res) => {
  try {
    const sources = getDataSources();
    if (sources.primarySource !== 'binary') {
      return res.status(400).json({ error: 'Binary files required' });
    }

    // Allow 'all' or specific count - default to 2000 for quick scans, 'all' for full scan
    const filesParam = req.query.files;
    const scanAll = filesParam === 'all';
    const maxFiles = scanAll ? Infinity : Math.min(parseInt(filesParam) || 2000, 100000);
    
    const allFiles = binaryReader.findBinaryFiles(db.DATA_PATH, 'events');
    
    if (allFiles.length === 0) {
      return res.json({ error: 'No binary event files found', path: db.DATA_PATH });
    }
    
    // If scanning all files, use them directly; otherwise spread sample
    let binFiles;
    if (scanAll) {
      binFiles = allFiles;
    } else {
      const step = Math.max(1, Math.floor(allFiles.length / maxFiles));
      binFiles = allFiles.filter((_, i) => i % step === 0).slice(0, maxFiles);
    }
    
    // Map to track proposals by unique key (action type + reason URL)
    const proposalMap = new Map();
    let filesScanned = 0;
    let totalVoteRequests = 0;
    
    for (const filePath of binFiles) {
      try {
        const result = await binaryReader.readBinaryFile(filePath);
        const events = result.records || [];
        filesScanned++;
        
        for (const evt of events) {
          const templateId = evt.template_id || '';
          if (!templateId.includes('VoteRequest')) continue;
          if (evt.event_type !== 'created') continue;
          
          totalVoteRequests++;
          const payload = evt.payload || {};
          
          // Parse the proposal data - handle both old (record.fields) and new (named) formats
          let proposal;
          try {
            proposal = parseVoteRequestPayload(payload);
          } catch (parseErr) {
            continue; // Skip unparseable payloads
          }
          
          if (!proposal) continue;
          
          // Create unique key from action type + reason URL
          const proposalKey = `${proposal.actionType}::${proposal.reasonUrl}`;
          
          const existing = proposalMap.get(proposalKey);
          const eventTimestamp = new Date(evt.timestamp).getTime();
          
          // Keep the most recent version
          if (!existing || eventTimestamp > existing.latestTimestamp) {
            proposalMap.set(proposalKey, {
              proposalKey,
              latestTimestamp: eventTimestamp,
              latestContractId: evt.contract_id,
              ...proposal,
              rawTimestamp: evt.timestamp,
            });
          }
        }
        
        if (filesScanned % 200 === 0) {
          console.log(`[governance/proposals] ${filesScanned}/${binFiles.length} files | ${proposalMap.size} unique proposals`);
        }
      } catch (err) {
        // Skip unreadable files
      }
    }
    
    // Convert to array and sort by latest timestamp (newest first)
    const proposals = Array.from(proposalMap.values())
      .sort((a, b) => b.latestTimestamp - a.latestTimestamp);
    
    // Calculate vote statistics
    const stats = {
      total: proposals.length,
      byActionType: {},
      byStatus: { approved: 0, rejected: 0, pending: 0 },
    };
    
    for (const p of proposals) {
      stats.byActionType[p.actionType] = (stats.byActionType[p.actionType] || 0) + 1;
      
      // Determine status based on votes
      const now = Date.now();
      const voteBefore = p.voteBeforeTimestamp || 0;
      
      if (voteBefore && voteBefore < now) {
        // Voting period ended - check if approved
        if (p.votesFor > p.votesAgainst && p.votesFor > 0) {
          stats.byStatus.approved++;
        } else {
          stats.byStatus.rejected++;
        }
      } else {
        stats.byStatus.pending++;
      }
    }
    
    res.json(convertBigInts({
      summary: {
        filesScanned,
        totalFilesInDataset: allFiles.length,
        totalVoteRequests,
        uniqueProposals: proposals.length,
      },
      stats,
      proposals,
    }));
  } catch (err) {
    console.error('Error in governance/proposals:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/governance/proposals/stream - SSE endpoint for real-time progress
router.get('/governance/proposals/stream', async (req, res) => {
  const scanStartTime = Date.now();
  console.log(`[SCAN] === Starting governance proposals stream scan ===`);
  
  try {
    const sources = getDataSources();
    console.log(`[SCAN] Data sources: ${JSON.stringify(sources)}`);
    
    if (sources.primarySource !== 'binary') {
      console.log(`[SCAN] ERROR: Binary files required, got ${sources.primarySource}`);
      return res.status(400).json({ error: 'Binary files required' });
    }

    // Set up SSE headers
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.flushHeaders();
    console.log(`[SCAN] SSE headers sent`);

    const sendEvent = (type, data) => {
      res.write(`event: ${type}\ndata: ${JSON.stringify(data)}\n\n`);
    };

    console.log(`[SCAN] Finding binary files in ${db.DATA_PATH}...`);
    const findFilesStart = Date.now();
    const allFiles = binaryReader.findBinaryFiles(db.DATA_PATH, 'events');
    console.log(`[SCAN] Found ${allFiles.length} binary files in ${Date.now() - findFilesStart}ms`);
    
    if (allFiles.length === 0) {
      console.log(`[SCAN] ERROR: No binary event files found`);
      sendEvent('error', { error: 'No binary event files found' });
      res.end();
      return;
    }

    // Parallel processing options
    const concurrency = parseInt(req.query.concurrency) || 10; // Process N files at once
    const maxFiles = req.query.limit ? parseInt(req.query.limit) : null;
    console.log(`[SCAN] Config: concurrency=${concurrency}, limit=${maxFiles || 'none'}`);
    
    // Use all files or limit
    let binFiles = allFiles;
    if (maxFiles && maxFiles > 0) {
      binFiles = allFiles.slice(-maxFiles); // Take most recent files (last N)
    }
    const totalFiles = binFiles.length;
    console.log(`[SCAN] Will process ${totalFiles} files`);
    
    sendEvent('start', { totalFiles, concurrency, totalAvailable: allFiles.length });
    
    // Map to track proposals by unique key
    const proposalMap = new Map();
    // Debug: track deduplication events
    const dedupLog = [];
    const debug = req.query.debug === 'true';
    const rawMode = req.query.raw === 'true'; // Output all events without deduplication
    const allRawVoteRequests = rawMode ? [] : null;
    
    let filesScanned = 0;
    let totalVoteRequests = 0;
    let lastProgressUpdate = 0;
    let totalFileReadTime = 0;
    let totalParseTime = 0;
    let filesWithErrors = 0;
    
    // Track exercised events for ledger-based status determination
    const exercisedEventsMap = new Map(); // contract_id -> { choice, template_id, timestamp }
    
    // Process files in parallel batches
    const processFile = async (filePath) => {
      const fileStart = Date.now();
      try {
        const result = await binaryReader.readBinaryFile(filePath);
        const fileReadTime = Date.now() - fileStart;
        
        const parseStart = Date.now();
        const events = result.records || [];
        const voteRequests = [];
        const exercisedEvents = []; // Collect exercised VoteRequest events
        
        for (const evt of events) {
          const templateId = evt.template_id || '';
          if (!templateId.includes('VoteRequest')) continue;
          
          if (evt.event_type === 'created') {
            const payload = evt.payload || {};
            let proposal;
            try {
              proposal = parseVoteRequestPayload(payload);
            } catch (parseErr) {
              continue;
            }
            if (!proposal) continue;
            
            voteRequests.push({ evt, proposal });
          } else if (evt.event_type === 'exercised' && evt.consuming === true) {
            // Collect consuming exercised events for status determination
            exercisedEvents.push({
              contract_id: evt.contract_id,
              choice: evt.choice || '',
              template_id: templateId,
              timestamp: evt.timestamp,
            });
          }
        }
        
        return { voteRequests, exercisedEvents, fileReadTime, parseTime: Date.now() - parseStart, events: events.length, error: null };
      } catch (err) {
        console.log(`[SCAN] ERROR reading file ${filePath}: ${err.message}`);
        return { voteRequests: [], exercisedEvents: [], fileReadTime: Date.now() - fileStart, parseTime: 0, events: 0, error: err.message };
      }
    };
    
    // Process in batches for parallelism
    let batchNum = 0;
    for (let i = 0; i < binFiles.length; i += concurrency) {
      batchNum++;
      const batchStart = Date.now();
      const batch = binFiles.slice(i, i + concurrency);
      console.log(`[SCAN] Processing batch ${batchNum}: files ${i+1}-${Math.min(i+concurrency, binFiles.length)} of ${totalFiles}`);
      const results = await Promise.all(batch.map(processFile));
      const batchTime = Date.now() - batchStart;
      
      // Collect batch stats
      let batchEvents = 0;
      let batchVoteRequests = 0;
      let batchReadTime = 0;
      let batchParseTime = 0;
      
      // Merge results from this batch
      for (const result of results) {
        if (result.error) {
          filesWithErrors++;
        }
        batchEvents += result.events || 0;
        batchReadTime += result.fileReadTime || 0;
        batchParseTime += result.parseTime || 0;
        totalFileReadTime += result.fileReadTime || 0;
        totalParseTime += result.parseTime || 0;
        
        // Collect exercised events for status determination (ledger model)
        for (const exercised of (result.exercisedEvents || [])) {
          // Keep the latest exercised event per contract_id
          const existing = exercisedEventsMap.get(exercised.contract_id);
          if (!existing || new Date(exercised.timestamp) > new Date(existing.timestamp)) {
            exercisedEventsMap.set(exercised.contract_id, exercised);
          }
        }
        
        const voteRequests = result.voteRequests || [];
        batchVoteRequests += voteRequests.length;
        
        for (const { evt, proposal } of voteRequests) {
          totalVoteRequests++;
          
          // Generate a unique proposal key
          let proposalKey;
          let keySource;
          if (proposal.trackingCid) {
            proposalKey = `cid::${proposal.trackingCid}`;
            keySource = 'trackingCid';
          } else {
            const actionSpecific = extractActionSpecificKey(proposal.actionDetails);
            proposalKey = `${proposal.actionType}::${proposal.requester}::${proposal.reasonUrl || 'no-url'}::${actionSpecific}`;
            keySource = 'composite';
          }
          
          // In raw mode, collect all events without deduplication
          if (rawMode) {
            allRawVoteRequests.push({
              contractId: evt.contract_id,
              eventId: evt.event_id,
              timestamp: evt.timestamp,
              proposalKey,
              keySource,
              ...proposal,
            });
          }
          
          const existing = proposalMap.get(proposalKey);
          const eventTimestamp = new Date(evt.timestamp).getTime();
          
          if (debug && existing) {
            dedupLog.push({
              key: proposalKey.slice(0, 100),
              keySource,
              action: 'merged',
              existingTs: existing.rawTimestamp,
              newTs: evt.timestamp,
              kept: eventTimestamp > existing.latestTimestamp ? 'new' : 'existing',
              actionType: proposal.actionType,
              requester: proposal.requester,
              reasonUrl: (proposal.reasonUrl || '').slice(0, 80),
            });
          }
          
          if (!existing || eventTimestamp > existing.latestTimestamp) {
            proposalMap.set(proposalKey, {
              proposalKey,
              keySource,
              latestTimestamp: eventTimestamp,
              latestContractId: evt.contract_id,
              ...proposal,
              rawTimestamp: evt.timestamp,
              mergeCount: existing ? (existing.mergeCount || 1) + 1 : 1,
            });
          }
        }
      }
      
      filesScanned += batch.length;
      const elapsedSec = (Date.now() - scanStartTime) / 1000;
      const filesPerSec = filesScanned / elapsedSec;
      
      console.log(`[SCAN] Batch ${batchNum} complete: ${batchTime}ms, ${batchEvents} events, ${batchVoteRequests} VoteRequests, ${proposalMap.size} unique proposals, ${filesPerSec.toFixed(1)} files/sec`);
      
      // Send progress update every batch
      const now = Date.now();
      if (now - lastProgressUpdate > 200) {
        const percent = Math.round((filesScanned / totalFiles) * 100);
        sendEvent('progress', {
          filesScanned,
          totalFiles,
          percent,
          uniqueProposals: proposalMap.size,
          totalVoteRequests,
          rawCount: rawMode ? allRawVoteRequests.length : undefined,
        });
        lastProgressUpdate = now;
      }
    }
    
    const totalScanTime = Date.now() - scanStartTime;
    console.log(`[SCAN] === File scanning complete ===`);
    console.log(`[SCAN] Total time: ${totalScanTime}ms (${(totalScanTime/1000).toFixed(1)}s)`);
    console.log(`[SCAN] Files: ${filesScanned} processed, ${filesWithErrors} errors`);
    console.log(`[SCAN] Performance: ${(filesScanned/(totalScanTime/1000)).toFixed(1)} files/sec`);
    console.log(`[SCAN] Cumulative file read time: ${totalFileReadTime}ms, parse time: ${totalParseTime}ms`);
    console.log(`[SCAN] Results: ${totalVoteRequests} VoteRequests -> ${proposalMap.size} unique proposals`);
    console.log(`[SCAN] Exercised events collected: ${exercisedEventsMap.size} for status determination`);
    
    // Convert to array and sort by latest timestamp (newest first)
    const proposals = Array.from(proposalMap.values())
      .sort((a, b) => b.latestTimestamp - a.latestTimestamp);
    
    // Calculate vote statistics using LEDGER MODEL (not vote thresholds)
    // Status is determined by:
    // - finalized (executed/rejected/expired): Has consuming exercised event
    // - in_progress: No consuming exercise, before deadline  
    // - expired_unfinalized: No consuming exercise, after deadline
    const stats = {
      total: proposals.length,
      byActionType: {},
      byStatus: { executed: 0, rejected: 0, expired: 0, in_progress: 0 },
    };
    
    const nowMs = Date.now();
    for (const p of proposals) {
      stats.byActionType[p.actionType] = (stats.byActionType[p.actionType] || 0) + 1;
      
      // Check if this proposal has a consuming exercised event (ledger model)
      const exercisedEvent = exercisedEventsMap.get(p.latestContractId);
      const voteBefore = p.voteBeforeTimestamp || 0;
      const isExpired = voteBefore && voteBefore < nowMs;
      
      let status;
      if (exercisedEvent) {
        // Finalized - determine outcome from choice
        const choice = String(exercisedEvent.choice || '').toLowerCase();
        if (choice.includes('accept') && !choice.includes('reject')) {
          status = 'executed';
        } else if (choice.includes('reject')) {
          status = 'rejected';
        } else if (choice.includes('expire')) {
          status = 'expired';
        } else {
          // Unknown choice - default to executed (it was finalized)
          status = 'executed';
        }
      } else {
        // Not finalized - check deadline
        if (isExpired) {
          status = 'expired';
        } else {
          status = 'in_progress';
        }
      }
      
      // Add status to proposal object for UI
      p.status = status;
      stats.byStatus[status] = (stats.byStatus[status] || 0) + 1;
    }
    
    // Analyze merge patterns
    const byKeySource = { trackingCid: 0, composite: 0 };
    const highMergeProposals = [];
    for (const p of proposals) {
      byKeySource[p.keySource] = (byKeySource[p.keySource] || 0) + 1;
      if (p.mergeCount > 5) {
        highMergeProposals.push({
          key: p.proposalKey.slice(0, 80),
          keySource: p.keySource,
          mergeCount: p.mergeCount,
          actionType: p.actionType,
          requester: p.requester,
          reasonUrl: (p.reasonUrl || '').slice(0, 60),
        });
      }
    }
    
    // Send final result
    console.log(`[SCAN] Sending complete event with ${proposals.length} proposals...`);
    sendEvent('complete', {
      summary: {
        filesScanned,
        totalFilesInDataset: allFiles.length,
        totalVoteRequests,
        uniqueProposals: proposals.length,
        rawMode: rawMode,
        scanTimeMs: totalScanTime,
      },
      stats,
      proposals,
      // Raw mode: include all vote requests without deduplication
      rawVoteRequests: rawMode ? allRawVoteRequests : undefined,
      debug: debug ? {
        dedupLog: dedupLog.slice(-500), // Last 500 dedup events
        byKeySource,
        highMergeProposals: highMergeProposals.slice(0, 50),
        sampleKeys: proposals.slice(0, 20).map(p => ({
          key: p.proposalKey.slice(0, 100),
          keySource: p.keySource,
          mergeCount: p.mergeCount,
          actionType: p.actionType,
        })),
      } : undefined,
    });
    
    console.log(`[SCAN] === Scan complete, closing connection ===`);
    res.end();
  } catch (err) {
    console.error(`[SCAN] FATAL ERROR in governance/proposals/stream:`, err);
    console.error(`[SCAN] Stack:`, err.stack);
    res.write(`event: error\ndata: ${JSON.stringify({ error: err.message })}\n\n`);
    res.end();
  }
});

// Helper function to parse VoteRequest payload (handles both old and new formats)
function parseVoteRequestPayload(payload) {
  // New format with named fields
  if (payload.requester && payload.action) {
    const votes = parseVotesArray(payload.votes);
    return {
      requester: payload.requester,
      actionType: extractActionType(payload.action),
      actionDetails: payload.action,
      reasonUrl: payload.reason?.url || '',
      reasonBody: payload.reason?.body || '',
      voteBefore: payload.voteBefore,
      voteBeforeTimestamp: parseTimestamp(payload.voteBefore),
      votes: votes.list,
      votesFor: votes.votesFor,
      votesAgainst: votes.votesAgainst,
      trackingCid: payload.trackingCid,
    };
  }
  
  // Old format with record.fields array
  if (payload.record?.fields) {
    const fields = payload.record.fields;
    if (fields.length < 6) return null;
    
    const requester = fields[1]?.value?.text || '';
    const actionVariant = fields[2]?.value?.variant || {};
    const reason = fields[3]?.value?.record?.fields || [];
    const voteBefore = fields[4]?.value?.timestamp || '';
    const votesGenMap = fields[5]?.value?.genMap?.entries || [];
    
    const votes = parseVotesGenMap(votesGenMap);
    
    return {
      requester,
      actionType: extractActionTypeFromVariant(actionVariant),
      actionDetails: actionVariant,
      reasonUrl: reason[0]?.value?.text || '',
      reasonBody: reason[1]?.value?.text || '',
      voteBefore,
      voteBeforeTimestamp: parseTimestamp(voteBefore),
      votes: votes.list,
      votesFor: votes.votesFor,
      votesAgainst: votes.votesAgainst,
      trackingCid: fields[6]?.value?.optional?.value?.contractId || null,
    };
  }
  
  return null;
}

// Extract action type from new format
function extractActionType(action) {
  if (!action) return 'unknown';
  const tag = action.tag || action.constructor;
  const innerAction = action.value?.dsoAction || action.value;
  const innerTag = innerAction?.tag || innerAction?.constructor;
  return innerTag || tag || 'unknown';
}

// Extract action type from old variant format
function extractActionTypeFromVariant(variant) {
  if (!variant) return 'unknown';
  const outerType = variant.constructor || '';
  const innerVariant = variant.value?.record?.fields?.[0]?.value?.variant;
  const innerType = innerVariant?.constructor || '';
  return innerType || outerType || 'unknown';
}

// Parse votes from new format array
function parseVotesArray(votes) {
  if (!Array.isArray(votes)) return { list: [], votesFor: 0, votesAgainst: 0 };
  
  let votesFor = 0;
  let votesAgainst = 0;
  const list = [];
  
  for (const [name, voteData] of votes) {
    const accept = voteData?.accept ?? false;
    if (accept) votesFor++;
    else votesAgainst++;
    
    list.push({
      svName: name,
      sv: voteData?.sv,
      accept,
      reasonUrl: voteData?.reason?.url || '',
      reasonBody: voteData?.reason?.body || '',
      castAt: voteData?.optCastAt,
    });
  }
  
  return { list, votesFor, votesAgainst };
}

// Parse votes from old genMap format
function parseVotesGenMap(entries) {
  if (!Array.isArray(entries)) return { list: [], votesFor: 0, votesAgainst: 0 };
  
  let votesFor = 0;
  let votesAgainst = 0;
  const list = [];
  
  for (const entry of entries) {
    const svName = entry.key?.text || '';
    const voteRecord = entry.value?.record?.fields || [];
    const sv = voteRecord[0]?.value?.party || '';
    const accept = voteRecord[1]?.value?.bool ?? false;
    const reasonFields = voteRecord[2]?.value?.record?.fields || [];
    
    if (accept) votesFor++;
    else votesAgainst++;
    
    list.push({
      svName,
      sv,
      accept,
      reasonUrl: reasonFields[0]?.value?.text || '',
      reasonBody: reasonFields[1]?.value?.text || '',
    });
  }
  
  return { list, votesFor, votesAgainst };
}

// Parse timestamp from various formats
function parseTimestamp(ts) {
  if (!ts) return null;
  
  // ISO string format
  if (typeof ts === 'string' && ts.includes('-')) {
    return new Date(ts).getTime();
  }
  
  // Microsecond timestamp (16+ digits)
  if (typeof ts === 'string' && /^\d{16,}$/.test(ts)) {
    return Math.floor(parseInt(ts) / 1000); // Convert micros to millis
  }
  
  // Millisecond timestamp
  if (typeof ts === 'number' || /^\d+$/.test(ts)) {
    const num = parseInt(ts);
    if (num > 1e15) return Math.floor(num / 1000); // Micros
    if (num > 1e12) return num; // Millis
    return num * 1000; // Seconds
  }
  
  return null;
}

// Extract action-specific identifier for deduplication
function extractActionSpecificKey(actionDetails) {
  if (!actionDetails) return 'none';
  
  try {
    // Try new format first
    const dsoAction = actionDetails.value?.dsoAction?.value || actionDetails.value?.value || actionDetails.value;
    
    // GrantFeaturedAppRight - use provider
    if (dsoAction?.provider) {
      return `provider:${dsoAction.provider}`;
    }
    
    // RevokeFeaturedAppRight - use rightCid
    if (dsoAction?.rightCid) {
      return `rightCid:${dsoAction.rightCid}`;
    }
    
    // UpdateSvRewardWeight - use svParty + weight
    if (dsoAction?.svParty) {
      return `sv:${dsoAction.svParty}:${dsoAction.newRewardWeight || ''}`;
    }
    
    // CreateUnallocatedUnclaimedActivityRecord - use beneficiary
    if (dsoAction?.beneficiary) {
      return `beneficiary:${dsoAction.beneficiary}:${dsoAction.amount || ''}`;
    }
    
    // SetConfig or AddFutureAmuletConfigSchedule - try to get a hash of the config
    if (dsoAction?.newSchedule || dsoAction?.config) {
      const configStr = JSON.stringify(dsoAction.newSchedule || dsoAction.config).slice(0, 100);
      return `config:${configStr}`;
    }
    
    // Old format - try to extract from variant structure
    const variant = actionDetails.record?.fields?.[0]?.value?.variant;
    if (variant?.value?.record?.fields) {
      const innerFields = variant.value.record.fields;
      // Look for provider, party, etc.
      for (const f of innerFields) {
        if (f.value?.party) return `party:${f.value.party}`;
        if (f.value?.contractId) return `cid:${f.value.contractId}`;
        if (f.value?.text && f.value.text.length < 100) return `txt:${f.value.text}`;
      }
    }
    
    // Fallback: use a short hash of the stringified action
    const str = JSON.stringify(actionDetails).slice(0, 200);
    return `hash:${simpleHash(str)}`;
  } catch (e) {
    return 'err';
  }
}

// Simple hash function for deduplication
function simpleHash(str) {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash = hash & hash;
  }
  return Math.abs(hash).toString(36);
}

// ============ SV MEMBERSHIP INDEX ROUTES ============

// GET /api/events/sv-index/test - Simple test endpoint
router.get('/sv-index/test', async (req, res) => {
  res.json({ 
    ok: true, 
    message: 'SV membership index endpoint is working',
    timestamp: new Date().toISOString()
  });
});

// GET /api/events/sv-index/status - Get SV index status
router.get('/sv-index/status', async (req, res) => {
  try {
    const stats = await svIndexer.getSvIndexStats();
    res.json(convertBigInts(stats));
  } catch (err) {
    console.error('Error getting SV index status:', err);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/events/sv-index/build - Build SV membership index
router.post('/sv-index/build', async (req, res) => {
  try {
    const force = req.body?.force === true || req.query.force === 'true';
    
    if (svIndexer.isIndexingInProgress()) {
      return res.json({ status: 'in_progress', message: 'SV indexing already in progress' });
    }
    
    // Start indexing in background
    res.json({ status: 'started', message: 'SV membership index build started' });
    
    svIndexer.buildSvMembershipIndex({ force }).catch(err => {
      console.error('Background SV index build failed:', err);
    });
  } catch (err) {
    console.error('Error starting SV index build:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/sv-index/count-at - Get SV count at a specific date
router.get('/sv-index/count-at', async (req, res) => {
  try {
    const { date } = req.query;
    if (!date) {
      return res.status(400).json({ error: 'date query parameter required (ISO format)' });
    }
    
    const count = await svIndexer.getSvCountAt(date);
    const activeSvs = await svIndexer.getActiveSvsAt(date);
    const thresholds = svIndexer.calculateVotingThreshold(count);
    
    res.json({
      date,
      svCount: count,
      thresholds,
      activeSvs,
    });
  } catch (err) {
    console.error('Error getting SV count at date:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/sv-index/timeline - Get SV membership timeline
router.get('/sv-index/timeline', async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 100, 500);
    const events = await svIndexer.getSvMembershipTimeline(limit);
    res.json({ events, count: events.length });
  } catch (err) {
    console.error('Error getting SV timeline:', err);
    res.status(500).json({ error: err.message });
  }
});

// ============ VOTE OUTCOME ANALYZER ROUTES ============

// GET /api/events/proposals/raw-grouped - Get raw proposals grouped by tracking_cid/contract_id
router.get('/proposals/raw-grouped', async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 200, 1000);
    const offset = parseInt(req.query.offset) || 0;
    
    const result = await voteOutcomeAnalyzer.getRawProposalsGrouped({ limit, offset });
    res.json(convertBigInts(result));
  } catch (err) {
    console.error('Error getting raw proposals:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/proposals/raw-stats - Get raw proposal statistics
router.get('/proposals/raw-stats', async (req, res) => {
  try {
    const stats = await voteOutcomeAnalyzer.getRawProposalStats();
    res.json(stats);
  } catch (err) {
    console.error('Error getting raw proposal stats:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/proposals/analyze/:id - Analyze a single proposal's outcome
router.get('/proposals/analyze/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const result = await voteOutcomeAnalyzer.analyzeProposalOutcome(id);
    res.json(convertBigInts(result));
  } catch (err) {
    console.error('Error analyzing proposal:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/proposals/analyze-all - Analyze all proposals and find mismatches
router.get('/proposals/analyze-all', async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 100, 500);
    const offset = parseInt(req.query.offset) || 0;
    
    const result = await voteOutcomeAnalyzer.analyzeAllProposalOutcomes({ limit, offset });
    res.json(convertBigInts(result));
  } catch (err) {
    console.error('Error analyzing all proposals:', err);
    res.status(500).json({ error: err.message });
  }
});

// ============ DEBUG: EXERCISED EVENTS INSPECTION ============

// GET /api/events/debug/exercised - Read-only query of all Exercised events from binary files
// Returns raw event data to inspect available fields (contract_id, choice, consuming, result, etc.)
router.get('/debug/exercised', async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 100, 1000);
    const offset = parseInt(req.query.offset) || 0;
    const templateFilter = req.query.template || null; // Optional: filter by template (e.g., "VoteRequest")
    const maxDays = parseInt(req.query.days) || 30;
    const maxFiles = parseInt(req.query.files) || 200;
    
    console.log(`\n🔍 DEBUG: Querying Exercised events - limit=${limit}, offset=${offset}, template=${templateFilter || 'any'}, days=${maxDays}`);
    
    const result = await binaryReader.streamRecords(db.DATA_PATH, 'events', {
      limit,
      offset,
      maxDays,
      maxFilesToScan: maxFiles,
      sortBy: 'effective_at',
      filter: (e) => {
        const isExercised = e.event_type === 'exercised';
        if (!isExercised) return false;
        if (templateFilter && !e.template_id?.includes(templateFilter)) return false;
        return true;
      }
    });
    
    // Extract key fields for inspection
    const events = result.records.map((e, idx) => ({
      _index: offset + idx,
      event_id: e.event_id,
      event_type: e.event_type,
      contract_id: e.contract_id,
      template_id: e.template_id,
      effective_at: e.effective_at,
      // Key fields to inspect
      choice: e.choice || e.payload?.choice || null,
      consuming: e.consuming ?? e.payload?.consuming ?? null,
      argument: e.argument || e.payload?.argument || null,
      result: e.result || e.payload?.result || null,
      // Show all top-level keys for structure inspection
      _topLevelKeys: Object.keys(e),
      _payloadKeys: e.payload ? Object.keys(e.payload) : [],
      // Raw payload for full inspection
      _rawPayload: e.payload,
    }));
    
    // Summary stats
    const uniqueTemplates = [...new Set(events.map(e => e.template_id?.split(':').pop() || 'unknown'))];
    const uniqueChoices = [...new Set(events.map(e => e.choice).filter(Boolean))];
    
    res.json(convertBigInts({
      summary: {
        returned: events.length,
        offset,
        hasMore: result.hasMore,
        maxDays,
        maxFiles,
        templateFilter,
        uniqueTemplates: uniqueTemplates.slice(0, 20),
        uniqueChoices: uniqueChoices.slice(0, 30),
      },
      events,
    }));
  } catch (err) {
    console.error('Error fetching exercised events:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/debug/exercised/sample - Get a single sample Exercised event with full structure
router.get('/debug/exercised/sample', async (req, res) => {
  try {
    const templateFilter = req.query.template || null;
    
    console.log(`\n🔍 DEBUG: Getting sample Exercised event - template=${templateFilter || 'any'}`);
    
    const result = await binaryReader.streamRecords(db.DATA_PATH, 'events', {
      limit: 1,
      offset: 0,
      maxDays: 90,
      maxFilesToScan: 500,
      sortBy: 'effective_at',
      filter: (e) => {
        const isExercised = e.event_type === 'exercised';
        if (!isExercised) return false;
        if (templateFilter && !e.template_id?.includes(templateFilter)) return false;
        return true;
      }
    });
    
    if (result.records.length === 0) {
      return res.json({ found: false, message: 'No Exercised events found matching criteria' });
    }
    
    const sample = result.records[0];
    
    res.json(convertBigInts({
      found: true,
      templateFilter,
      event: sample,
      structure: {
        topLevelKeys: Object.keys(sample),
        payloadType: typeof sample.payload,
        payloadKeys: sample.payload ? Object.keys(sample.payload) : [],
        hasChoice: 'choice' in sample || (sample.payload && 'choice' in sample.payload),
        hasConsuming: 'consuming' in sample || (sample.payload && 'consuming' in sample.payload),
        hasArgument: 'argument' in sample || (sample.payload && 'argument' in sample.payload),
        hasResult: 'result' in sample || (sample.payload && 'result' in sample.payload),
      }
    }));
  } catch (err) {
    console.error('Error fetching sample exercised event:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/debug/exercised/voterequest - Get all VoteRequest Exercised events specifically
router.get('/debug/exercised/voterequest', async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 100, 500);
    const offset = parseInt(req.query.offset) || 0;
    const maxDays = parseInt(req.query.days) || 365; // Governance spans long periods
    const maxFiles = parseInt(req.query.files) || 1000;
    
    console.log(`\n🔍 DEBUG: Querying VoteRequest Exercised events - limit=${limit}, offset=${offset}, days=${maxDays}`);
    
    const result = await binaryReader.streamRecords(db.DATA_PATH, 'events', {
      limit,
      offset,
      maxDays,
      maxFilesToScan: maxFiles,
      sortBy: 'effective_at',
      filter: (e) => {
        return e.event_type === 'exercised' && e.template_id?.includes('VoteRequest');
      }
    });
    
    // Extract and categorize by choice
    const events = result.records.map((e, idx) => {
      const choice = e.choice || e.payload?.choice || 'unknown';
      return {
        _index: offset + idx,
        event_id: e.event_id,
        contract_id: e.contract_id,
        template_id: e.template_id?.split(':').pop(),
        effective_at: e.effective_at,
        choice,
        consuming: e.consuming ?? e.payload?.consuming ?? null,
        // Show argument/result structure
        argumentKeys: e.argument ? Object.keys(e.argument) : (e.payload?.argument ? Object.keys(e.payload.argument) : []),
        resultKeys: e.result ? Object.keys(e.result) : (e.payload?.result ? Object.keys(e.payload.result) : []),
      };
    });
    
    // Group by choice
    const byChoice = {};
    for (const e of events) {
      byChoice[e.choice] = (byChoice[e.choice] || 0) + 1;
    }
    
    res.json(convertBigInts({
      summary: {
        returned: events.length,
        offset,
        hasMore: result.hasMore,
        maxDays,
        byChoice,
      },
      events,
    }));
  } catch (err) {
    console.error('Error fetching VoteRequest exercised events:', err);
    res.status(500).json({ error: err.message });
  }
});

export default router;
