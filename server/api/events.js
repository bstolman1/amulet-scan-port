import { Router } from 'express';
import db from '../duckdb/connection.js';
import binaryReader from '../duckdb/binary-reader.js';

const router = Router();

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
      // VoteRequest events are EXTREMELY sparse (~26 per 275K events)
      // Strategy: Run TWO parallel scans - one for VoteRequests specifically, one for other governance
      
      // Scan 1: Deep scan specifically for VoteRequest created events
      const voteRequestPromise = binaryReader.streamRecords(db.DATA_PATH, 'events', {
        limit: 500, // VoteRequests are rare, 500 should be plenty
        offset: 0,
        maxDays: 365 * 3,
        maxFilesToScan: 100000, // Scan ALL files for VoteRequests
        sortBy: 'effective_at',
        filter: (e) => {
          return e.template_id?.includes('VoteRequest') && e.event_type === 'created';
        }
      });
      
      // Scan 2: Standard governance scan (Confirmation, DsoRules choices)
      const otherGovernancePromise = binaryReader.streamRecords(db.DATA_PATH, 'events', {
        limit: limit * 100,
        offset,
        maxDays: 365 * 3,
        maxFilesToScan: 20000,
        sortBy: 'effective_at',
        filter: (e) => {
          // Confirmation created events
          if (e.template_id?.includes('Confirmation') && e.event_type === 'created') return true;
          // ElectionRequest created events
          if (e.template_id?.includes('ElectionRequest') && e.event_type === 'created') return true;
          // DsoRules governance choices
          const choiceMatch = governanceChoices.includes(e.choice);
          if (e.template_id?.includes('DsoRules') && choiceMatch) return true;
          return false;
        }
      });
      
      // Run both scans in parallel
      const [voteRequestResult, otherResult] = await Promise.all([voteRequestPromise, otherGovernancePromise]);
      
      console.log(`   Found ${voteRequestResult.records.length} VoteRequest events (deep scan)`);
      console.log(`   Found ${otherResult.records.length} other governance events`);
      
      // Smart merge: Ensure VoteRequests are always represented
      // Reserve up to 30% of slots for VoteRequests since they have the richest data
      const voteRequestSlots = Math.min(Math.ceil(limit * 0.3), voteRequestResult.records.length);
      const otherSlots = limit - voteRequestSlots;
      
      // Sort each set by effective_at descending
      voteRequestResult.records.sort((a, b) => new Date(b.effective_at) - new Date(a.effective_at));
      otherResult.records.sort((a, b) => new Date(b.effective_at) - new Date(a.effective_at));
      
      // Take allocated slots from each
      const selectedVoteRequests = voteRequestResult.records.slice(0, voteRequestSlots);
      const selectedOther = otherResult.records.slice(0, otherSlots);
      
      // Merge and dedupe
      const allRecords = [...selectedVoteRequests, ...selectedOther];
      const seenIds = new Set();
      const dedupedRecords = allRecords.filter(r => {
        if (seenIds.has(r.event_id)) return false;
        seenIds.add(r.event_id);
        return true;
      });
      
      // Sort merged results by effective_at descending
      dedupedRecords.sort((a, b) => new Date(b.effective_at) - new Date(a.effective_at));
      
      console.log(`   VoteRequests included: ${selectedVoteRequests.length}/${voteRequestResult.records.length}`);
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
          voteRequestsFound: voteRequestResult.records.length,
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
    const limit = Math.min(parseInt(req.query.limit) || 100, 1000);
    const status = req.query.status || 'all'; // 'active', 'historical', 'all'
    const verbose = req.query.verbose === 'true';
    const sources = getDataSources();
    const now = new Date();
    
    console.log(`\n🗳️ VOTE-REQUESTS: Fetching with limit=${limit}, status=${status}, verbose=${verbose}`);
    console.log(`   Primary source: ${sources.primarySource}`);
    
    if (sources.primarySource === 'binary') {
      // VoteRequest events are VERY sparse (~26 per 275K events)
      // Also: many VoteRequests quickly become historical via an exercised (Archive/Accept/Reject/Expire) event.
      // Strategy: scan for BOTH created VoteRequests (payload-rich) and exercised VoteRequest events (closure signal).

      const createdPromise = binaryReader.streamRecords(db.DATA_PATH, 'events', {
        limit: 10000, // Get as many as possible
        offset: 0,
        maxDays: 365 * 10, // 10 years - get everything we have
        maxFilesToScan: 500000, // Scan as many files as exist
        sortBy: 'effective_at',
        filter: (e) => {
          return e.template_id?.includes('VoteRequest') && e.event_type === 'created';
        }
      });

      const exercisedPromise = binaryReader.streamRecords(db.DATA_PATH, 'events', {
        limit: 100000, // exercised events can be more common; we only need contract_id + choice
        offset: 0,
        maxDays: 365 * 10,
        maxFilesToScan: 500000,
        sortBy: 'effective_at',
        filter: (e) => {
          if (!e.template_id?.includes('VoteRequest')) return false;
          if (e.event_type !== 'exercised') return false;
          // Archive is empty payload but is the main closure signal; keep other terminal choices too
          return e.choice === 'Archive' || (typeof e.choice === 'string' && e.choice.startsWith('VoteRequest_'));
        }
      });

      const [createdResult, exercisedResult] = await Promise.all([createdPromise, exercisedPromise]);

      const closedContractIds = new Set(
        (exercisedResult.records || [])
          .map(r => r.contract_id)
          .filter(Boolean)
      );

      console.log(`   Found ${createdResult.records.length} VoteRequest created events from binary files`);
      console.log(`   Found ${exercisedResult.records.length} VoteRequest exercised events (closure signals)`);
      console.log(`   Unique closed contracts: ${closedContractIds.size}`);

      // Process to extract full VoteRequest details with active/historical status
      const allVoteRequests = createdResult.records.map((event, idx) => {
        const voteBefore = event.payload?.voteBefore;
        const voteBeforeDate = voteBefore ? new Date(voteBefore) : null;
        const isClosed = !!event.contract_id && closedContractIds.has(event.contract_id);
        const isActive = !isClosed && (voteBeforeDate ? voteBeforeDate > now : true);

        if (verbose && idx < 3) {
          console.log(`\n   📝 VoteRequest #${idx + 1}:`);
          console.log(`      effective_at: ${event.effective_at}`);
          console.log(`      action.tag: ${event.payload?.action?.tag || 'null'}`);
          console.log(`      requester: ${event.payload?.requester || 'null'}`);
          const reason = event.payload?.reason;
          console.log(`      reason: ${typeof reason === 'string' ? reason.slice(0, 100) : JSON.stringify(reason)?.slice(0, 100) || 'null'}`);
          console.log(`      votes: ${event.payload?.votes ? `[${event.payload.votes.length} votes]` : 'null'}`);
          console.log(`      voteBefore: ${voteBefore || 'null'}`);
          console.log(`      closedByExercise: ${isClosed}`);
          console.log(`      status: ${isActive ? 'ACTIVE' : 'HISTORICAL'}`);
        }

        return {
          event_id: event.event_id,
          contract_id: event.contract_id,
          template_id: event.template_id,
          effective_at: event.effective_at,
          timestamp: event.timestamp,
          // Status
          status: isActive ? 'active' : 'historical',
          is_closed: isClosed,
          // VoteRequest-specific fields
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
      
      // Filter by status if requested
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
      
      console.log(`\n   📊 VoteRequest stats:`);
      console.log(`      Total found: ${allVoteRequests.length}`);
      console.log(`      Active: ${activeCount}`);
      console.log(`      Historical: ${historicalCount}`);
      console.log(`      Closed by exercise: ${closedCount}`);
      console.log(`      Returned: ${voteRequests.length} (filtered by: ${status})`);
      console.log(`      With reason: ${withReason}`);
      console.log(`      With votes: ${withVotes}`);
      console.log(`      Action types: ${actionTags.join(', ')}`);
      
      // Debug: sample contract IDs from created vs exercised
      const sampleCreatedIds = createdResult.records.slice(0, 3).map(r => r.contract_id?.slice(0, 40));
      const sampleExercisedIds = (exercisedResult.records || []).slice(0, 3).map(r => r.contract_id?.slice(0, 40));
      
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
        _debug: {
          createdEventsFound: createdResult.records.length,
          exercisedEventsFound: (exercisedResult.records || []).length,
          closedContractIds: closedContractIds.size,
          sampleCreatedContractIds: sampleCreatedIds,
          sampleExercisedContractIds: sampleExercisedIds,
        }
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

export default router;
