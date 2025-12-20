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
      const result = await binaryReader.streamRecords(db.DATA_PATH, 'events', {
        limit: limit * 10, // Fetch more to filter down
        offset,
        maxDays: 365 * 2, // 2 years of history
        maxFilesToScan: 1000,
        sortBy: 'effective_at',
        filter: (e) => {
          // Match by template (VoteRequest, Confirmation, etc.)
          const templateMatch = governanceTemplates.some(t => e.template_id?.includes(t));
          // OR match by governance choice
          const choiceMatch = governanceChoices.includes(e.choice);
          // OR match DsoRules template with any choice (DsoRules is governance-specific)
          const dsoRulesMatch = e.template_id?.includes('DsoRules');
          return templateMatch || choiceMatch || dsoRulesMatch;
        }
      });
      
      console.log(`   Found ${result.records.length} potential governance events from binary files`);
      
      // Take only the requested limit
      const limitedRecords = result.records.slice(0, limit);
      
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
            console.log(`      payload.reason: ${event.payload.reason?.slice(0, 100) || 'null'}`);
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
          // Extract action details from payload if available
          action_tag: event.payload?.action?.tag || null,
          action_value: event.payload?.action?.value ? Object.keys(event.payload.action.value) : null,
          requester: event.payload?.requester || null,
          reason: event.payload?.reason || null,
          votes: event.payload?.votes || [],
          vote_before: event.payload?.voteBefore || null,
          // Include exercise result for completed votes
          exercise_result: event.exercise_result || null,
          // Include full payload in verbose mode
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
      const withReason = history.filter(h => h.reason).length;
      const withVotes = history.filter(h => h.votes?.length > 0).length;
      const withExerciseResult = history.filter(h => h.exercise_result).length;
      
      console.log(`\n   🔍 Field coverage:`);
      console.log(`      Events with action_tag: ${withAction}/${history.length}`);
      console.log(`      Events with requester: ${withRequester}/${history.length}`);
      console.log(`      Events with reason: ${withReason}/${history.length}`);
      console.log(`      Events with votes: ${withVotes}/${history.length}`);
      console.log(`      Events with exercise_result: ${withExerciseResult}/${history.length}`);
      
      return res.json({ 
        data: history, 
        count: history.length, 
        hasMore: result.hasMore || limitedRecords.length < result.records.length, 
        source: 'binary',
        _debug: {
          templateCounts,
          eventTypeCounts,
          choiceCounts,
          fieldCoverage: { withAction, withRequester, withReason, withVotes, withExerciseResult },
          totalScanned: result.records.length,
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

export default router;
