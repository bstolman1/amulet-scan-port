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

// GET /api/events/templates-sample - Debug endpoint to show what templates exist in recent data
router.get('/templates-sample', async (req, res) => {
  try {
    const sources = getDataSources();
    const templateCounts = {};
    
    if (sources.primarySource === 'binary') {
      // Scan a sample of files to see what templates exist
      const result = await binaryReader.streamRecords(db.DATA_PATH, 'events', {
        limit: 10000, // Get a good sample
        offset: 0,
        maxDays: 30,
        maxFilesToScan: 100,
        sortBy: 'effective_at',
      });
      
      for (const record of result.records) {
        const template = record.template_id || 'unknown';
        // Extract just the template name (last part after colon)
        const shortName = template.includes(':') ? template.split(':').pop() : template;
        templateCounts[shortName] = (templateCounts[shortName] || 0) + 1;
      }
      
      // Sort by count descending
      const sorted = Object.entries(templateCounts)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 50);
      
      return res.json({
        filesScanned: result.filesScanned,
        totalRecordsSampled: result.records.length,
        templates: sorted.map(([name, count]) => ({ name, count })),
        hasGovernance: sorted.some(([name]) => 
          ['VoteRequest', 'Confirmation', 'DsoRules', 'AmuletRules'].some(g => name.includes(g))
        ),
      });
    }
    
    res.json({ error: 'Only binary source supported for this debug endpoint' });
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
    const sources = getDataSources();
    
    // Templates for governance history - match on template suffix (after the colon)
    const governanceTemplates = [
      'VoteRequest',
      'Confirmation',
      'DsoRules',
      'AmuletRules',
    ];
    
    if (sources.primarySource === 'binary') {
      // Scan MORE files for governance since these events are rare (maybe 1 in 1000 events)
      // Also scan ALL days available, not just recent ones
      const result = await binaryReader.streamRecords(db.DATA_PATH, 'events', {
        limit,
        offset,
        maxDays: 365 * 5, // 5 years of history (scan all available partitions)
        maxFilesToScan: 5000, // Scan many more files since governance is rare
        sortBy: 'effective_at',
        filter: (e) => {
          if (!e.template_id) return false;
          // Match governance templates - template_id format is like "Splice.DsoRules:VoteRequest"
          return governanceTemplates.some(t => e.template_id.includes(t));
        }
      });
      
      console.log(`📋 Governance history: scanned ${result.filesScanned || 'unknown'} files, found ${result.records.length} matching records`);
      
      // Return full event data with payload for frontend processing
      const history = result.records.map(event => ({
        event_id: event.event_id,
        event_type: event.event_type,
        contract_id: event.contract_id,
        template_id: event.template_id,
        effective_at: event.effective_at,
        timestamp: event.timestamp,
        payload: event.payload, // Return FULL payload for frontend to process like Active Proposals
        signatories: event.signatories,
        observers: event.observers,
      }));
      
      return res.json({ 
        data: history, 
        count: history.length, 
        hasMore: result.hasMore,
        filesScanned: result.filesScanned,
        totalFilesAvailable: result.totalFiles,
        source: 'binary' 
      });
    }
    
    // Fallback to DuckDB query
    const templateFilter = governanceTemplates.map(t => `template_id LIKE '%${t}%'`).join(' OR ');
    const sql = `
      SELECT 
        event_id,
        event_type,
        contract_id,
        template_id,
        effective_at,
        timestamp,
        payload
      FROM ${getEventsSource()}
      WHERE ${templateFilter}
      ORDER BY effective_at DESC
      LIMIT ${limit}
      OFFSET ${offset}
    `;
    
    const rows = await db.safeQuery(sql);
    
    // Return full event data with payload for frontend processing
    const history = rows.map(row => ({
      event_id: row.event_id,
      event_type: row.event_type,
      contract_id: row.contract_id,
      template_id: row.template_id,
      effective_at: row.effective_at,
      timestamp: row.timestamp,
      payload: row.payload, // Return FULL payload for frontend to process
      signatories: row.signatories,
      observers: row.observers,
    }));
    
    res.json({ data: history, count: history.length, source: sources.primarySource });
  } catch (err) {
    console.error('Error fetching governance history:', err);
    res.status(500).json({ error: err.message });
  }
});

export default router;
