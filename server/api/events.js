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
      // Use streaming for memory efficiency
      const result = await binaryReader.streamRecords(db.DATA_PATH, 'events', { limit, offset });
      return res.json({ data: result.records, count: result.records.length, hasMore: result.hasMore, source: 'binary' });
    }
    
    const sql = `
      SELECT 
        event_id,
        event_type,
        contract_id,
        template_id,
        package_name,
        timestamp,
        signatories,
        observers,
        payload
      FROM ${getEventsSource()}
      ORDER BY timestamp DESC
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
        filter: (e) => e.event_type === type
      });
      return res.json({ data: result.records, count: result.records.length, hasMore: result.hasMore, source: 'binary' });
    }
    
    const sql = `
      SELECT *
      FROM ${getEventsSource()}
      WHERE event_type = '${type}'
      ORDER BY timestamp DESC
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
        filter: (e) => e.template_id?.includes(templateId)
      });
      return res.json({ data: result.records, count: result.records.length, hasMore: result.hasMore, source: 'binary' });
    }
    
    const sql = `
      SELECT *
      FROM ${getEventsSource()}
      WHERE template_id LIKE '%${templateId}%'
      ORDER BY timestamp DESC
      LIMIT ${limit}
    `;
    
    const rows = await db.safeQuery(sql);
    res.json({ data: rows, count: rows.length, source: sources.primarySource });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/by-date - Get events for a specific date range
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
        filter: (e) => {
          if (!e.timestamp) return false;
          const ts = new Date(e.timestamp).getTime();
          return ts >= startDate && ts <= endDate;
        }
      });
      return res.json({ data: result.records, count: result.records.length, hasMore: result.hasMore, source: 'binary' });
    }
    
    let whereClause = '';
    if (start) whereClause += ` AND timestamp >= '${start}'`;
    if (end) whereClause += ` AND timestamp <= '${end}'`;
    
    const sql = `
      SELECT *
      FROM ${getEventsSource()}
      WHERE 1=1 ${whereClause}
      ORDER BY timestamp DESC
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
      // Just count files * estimated records per file to avoid OOM
      const files = binaryReader.findBinaryFiles(db.DATA_PATH, 'events');
      const estimated = files.length * 100; // ~100 records per file estimate
      return res.json({ count: estimated, estimated: true, fileCount: files.length, source: 'binary' });
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

export default router;
