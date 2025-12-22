import { Router } from 'express';
import fs from 'fs';
import path from 'path';
import db from '../duckdb/connection.js';
import binaryReader from '../duckdb/binary-reader.js';
import * as voteRequestIndexer from '../engine/vote-request-indexer.js';
import * as rewardIndexer from '../engine/reward-indexer.js';
import {
  getFilesForTemplate,
  isTemplateIndexPopulated,
} from '../engine/template-file-index.js';

const router = Router();

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
      return res.json({ data: result.records, count: result.records.length, hasMore: result.hasMore, source: 'binary' });
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
    res.json({ data: rows, count: rows.length, source: sources.primarySource });
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
      return res.json({ data: result.records, count: result.records.length, hasMore: result.hasMore, source: 'binary' });
    }
    
    const sql = `
      SELECT *
      FROM ${getEventsSource()}
      WHERE event_type = '${type}'
      ORDER BY effective_at DESC
      LIMIT ${limit}
    `;
    
    const rows = await db.safeQuery(sql);
    res.json({ data: rows, count: rows.length, source: sources.primarySource });
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
      return res.json({ data: result.records, count: result.records.length, hasMore: result.hasMore, source: 'binary' });
    }
    
    const sql = `
      SELECT *
      FROM ${getEventsSource()}
      WHERE template_id LIKE '%${templateId}%'
      ORDER BY effective_at DESC
      LIMIT ${limit}
    `;
    
    const rows = await db.safeQuery(sql);
    res.json({ data: rows, count: rows.length, source: sources.primarySource });
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
      return res.json({ data: result.records, count: result.records.length, hasMore: result.hasMore, source: 'binary' });
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
    res.json({ data: rows, count: rows.length, source: sources.primarySource });
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
      return res.json({ count: estimated, estimated: true, fileCount, source: 'binary' });
    }
    
    const sql = `
      SELECT COUNT(*) as total
      FROM ${getEventsSource()}
    `;
    
    const rows = await db.safeQuery(sql);
    res.json({ count: rows[0]?.total || 0, source: sources.primarySource });
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
    
    res.json({
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
    });
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
      return res.json({ 
        data: result.records, 
        count: result.records.length, 
        hasMore: result.hasMore, 
        source: 'binary' 
      });
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
    res.json({ data: rows, count: rows.length, source: sources.primarySource });
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
      return res.json({ 
        data: result.records, 
        count: result.records.length, 
        hasMore: result.hasMore, 
        source: 'binary' 
      });
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
    res.json({ data: rows, count: rows.length, source: sources.primarySource });
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
      return res.json({ 
        data: result.records, 
        count: result.records.length, 
        hasMore: result.hasMore, 
        source: 'binary' 
      });
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
    res.json({ data: rows, count: rows.length, source: sources.primarySource });
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

        return res.json({
          data: merged,
          count: merged.length,
          hasMore: indexedHistorical.length === limit,
          source: 'binary',
          _debug: {
            usedVoteRequestIndex: true,
            indexedHistoricalVotes: indexedHistorical.length,
            otherGovernanceIncluded: otherHistory.length,
          }
        });
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
      
      return res.json({ 
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
      });
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
    
    res.json({ data: history, count: history.length, source: sources.primarySource });
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
      
      return res.json({
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
      });
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
      
      return res.json({
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
      });
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
    
    res.json({ data: voteRequests, count: voteRequests.length, source: sources.primarySource });
  } catch (err) {
    console.error('Error fetching vote requests:', err);
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
    
    res.json({
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
    });
  } catch (err) {
    console.error('Error in template scan:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/vote-request-index/status - Get index status
router.get('/vote-request-index/status', async (req, res) => {
  try {
    const stats = await voteRequestIndexer.getVoteRequestStats();
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

    const payload = {
      populated: stats.total > 0,
      isIndexing,
      lockExists,
      stats,
      lastIndexedAt: state.last_indexed_at,
      totalIndexed: state.total_indexed,
      progress: isIndexing ? progress : null,
      lastSuccessfulBuild,
    };

    // DuckDB can return BIGINT values as JS BigInt, which Express can't JSON.stringify.
    const safePayload = JSON.parse(
      JSON.stringify(payload, (_key, value) => (typeof value === 'bigint' ? value.toString() : value))
    );

    res.json(safePayload);
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

// ============ REWARD COUPON INDEX ROUTES ============

// GET /api/events/reward-coupon-index/status - Get index status
router.get('/reward-coupon-index/status', async (req, res) => {
  try {
    const stats = await rewardIndexer.getRewardCouponStats();
    const state = await rewardIndexer.getIndexState();
    const isIndexing = rewardIndexer.isIndexingInProgress();
    const progress = rewardIndexer.getIndexingProgress();
    
    res.json({
      populated: stats.total > 0,
      isIndexing,
      stats,
      lastIndexedAt: state.last_indexed_at,
      totalIndexed: state.total_indexed,
      progress: isIndexing ? progress : null,
    });
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
    
    res.json({
      data: results,
      count: results.length,
      stats,
    });
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
    
    res.json(result);
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
      
      return res.json({ 
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
      });
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
    
    res.json({
      analyzed: results.length,
      choiceStats,
      results,
    });
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

    res.json({
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
    });
  } catch (err) {
    console.error('Error debugging vote request:', err);
    res.status(500).json({ error: err.message });
  }
});

export default router;
