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
  
  return `(${queries.join(' UNION ALL ')})`;
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

// GET /api/events/delta - Get created/archived counts for a template since a timestamp
router.get('/delta', async (req, res) => {
  try {
    const { since, template } = req.query;
    if (!since || !template) {
      return res.status(400).json({ error: 'since and template parameters required' });
    }
    
    const sources = getDataSources();
    
    if (sources.primarySource === 'binary') {
      const sinceDate = new Date(since).getTime();
      let created = 0, archived = 0;
      
      const result = await binaryReader.streamRecords(db.DATA_PATH, 'events', {
        limit: 10000,
        maxDays: 7,
        maxFilesToScan: 500,
        filter: (e) => {
          if (!e.template_id?.includes(template)) return false;
          const ts = new Date(e.effective_at || e.timestamp).getTime();
          if (ts <= sinceDate) return false;
          if (e.event_type === 'created') created++;
          else if (e.event_type === 'archived') archived++;
          return false; // Don't collect records, just count
        }
      });
      
      return res.json({ 
        data: { 
          template_suffix: template,
          created_count: created, 
          archived_count: archived,
          net_change: created - archived,
          since 
        },
        source: 'binary' 
      });
    }
    
    const sql = `
      SELECT 
        COUNT(CASE WHEN event_type = 'created' THEN 1 END) as created_count,
        COUNT(CASE WHEN event_type = 'archived' THEN 1 END) as archived_count
      FROM ${getEventsSource()}
      WHERE template_id LIKE '%${template}%'
        AND COALESCE(effective_at, timestamp) > '${since}'::TIMESTAMP
    `;
    
    const rows = await db.safeQuery(sql);
    const created = parseInt(rows[0]?.created_count) || 0;
    const archived = parseInt(rows[0]?.archived_count) || 0;
    
    res.json({ 
      data: {
        template_suffix: template,
        created_count: created,
        archived_count: archived,
        net_change: created - archived,
        since
      },
      source: sources.primarySource 
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/multi-delta - Get created/archived counts for multiple templates
router.get('/multi-delta', async (req, res) => {
  try {
    const { since, templates } = req.query;
    if (!since || !templates) {
      return res.status(400).json({ error: 'since and templates parameters required' });
    }
    
    const templateList = templates.split(',').map(t => t.trim());
    const sources = getDataSources();
    
    if (sources.primarySource === 'binary') {
      const sinceDate = new Date(since).getTime();
      const counts = {};
      templateList.forEach(t => counts[t] = { created: 0, archived: 0 });
      
      await binaryReader.streamRecords(db.DATA_PATH, 'events', {
        limit: 50000,
        maxDays: 7,
        maxFilesToScan: 500,
        filter: (e) => {
          const matchedTemplate = templateList.find(t => e.template_id?.includes(t));
          if (!matchedTemplate) return false;
          const ts = new Date(e.effective_at || e.timestamp).getTime();
          if (ts <= sinceDate) return false;
          if (e.event_type === 'created') counts[matchedTemplate].created++;
          else if (e.event_type === 'archived') counts[matchedTemplate].archived++;
          return false;
        }
      });
      
      const result = {};
      templateList.forEach(t => {
        result[t] = {
          template_suffix: t,
          created_count: counts[t].created,
          archived_count: counts[t].archived,
          net_change: counts[t].created - counts[t].archived,
          since
        };
      });
      
      return res.json({ data: result, source: 'binary' });
    }
    
    // SQL version for parquet/jsonl
    const templateConditions = templateList.map(t => `template_id LIKE '%${t}%'`).join(' OR ');
    const sql = `
      SELECT 
        template_id,
        COUNT(CASE WHEN event_type = 'created' THEN 1 END) as created_count,
        COUNT(CASE WHEN event_type = 'archived' THEN 1 END) as archived_count
      FROM ${getEventsSource()}
      WHERE (${templateConditions})
        AND COALESCE(effective_at, timestamp) > '${since}'::TIMESTAMP
      GROUP BY template_id
    `;
    
    const rows = await db.safeQuery(sql);
    
    // Aggregate by template suffix
    const result = {};
    templateList.forEach(t => {
      const matchingRows = rows.filter(r => r.template_id?.includes(t));
      const created = matchingRows.reduce((s, r) => s + parseInt(r.created_count || 0), 0);
      const archived = matchingRows.reduce((s, r) => s + parseInt(r.archived_count || 0), 0);
      result[t] = {
        template_suffix: t,
        created_count: created,
        archived_count: archived,
        net_change: created - archived,
        since
      };
    });
    
    res.json({ data: result, source: sources.primarySource });
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

export default router;
