import { Router } from 'express';
import db from '../duckdb/connection.js';
import binaryReader from '../duckdb/binary-reader.js';

const router = Router();

// Helper to get the correct read function for JSONL files (supports .jsonl, .jsonl.gz, .jsonl.zst)
const getUpdatesSource = () => `(
  SELECT * FROM read_json_auto('${db.DATA_PATH}/**/updates-*.jsonl', union_by_name=true, ignore_errors=true)
  UNION ALL
  SELECT * FROM read_json_auto('${db.DATA_PATH}/**/updates-*.jsonl.gz', union_by_name=true, ignore_errors=true)
  UNION ALL
  SELECT * FROM read_json_auto('${db.DATA_PATH}/**/updates-*.jsonl.zst', union_by_name=true, ignore_errors=true)
)`;

// Check what data sources are available
function getDataSources() {
  const hasBinaryEvents = binaryReader.hasBinaryFiles(db.DATA_PATH, 'events');
  return { hasBinaryEvents, primarySource: hasBinaryEvents ? 'binary' : 'jsonl' };
}

// GET /api/events/latest - Get latest events
router.get('/latest', async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 100, 1000);
    const offset = parseInt(req.query.offset) || 0;
    const sources = getDataSources();
    
    if (sources.primarySource === 'binary') {
      const events = await binaryReader.loadAllRecords(db.DATA_PATH, 'events');
      
      // Sort by timestamp descending
      const sorted = events
        .filter(e => e.timestamp)
        .sort((a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime())
        .slice(offset, offset + limit);
      
      return res.json({ data: sorted, count: sorted.length, source: 'binary' });
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
      FROM ${getUpdatesSource()}
      ORDER BY timestamp DESC
      LIMIT ${limit}
      OFFSET ${offset}
    `;
    
    const rows = await db.safeQuery(sql);
    res.json({ data: rows, count: rows.length });
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
    const sources = getDataSources();
    
    if (sources.primarySource === 'binary') {
      const events = await binaryReader.loadAllRecords(db.DATA_PATH, 'events');
      
      const filtered = events
        .filter(e => e.event_type === type)
        .sort((a, b) => new Date(b.timestamp || 0).getTime() - new Date(a.timestamp || 0).getTime())
        .slice(0, limit);
      
      return res.json({ data: filtered, count: filtered.length, source: 'binary' });
    }
    
    const sql = `
      SELECT *
      FROM ${getUpdatesSource()}
      WHERE event_type = '${type}'
      ORDER BY timestamp DESC
      LIMIT ${limit}
    `;
    
    const rows = await db.safeQuery(sql);
    res.json({ data: rows, count: rows.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/by-template/:templateId - Get events by template
router.get('/by-template/:templateId', async (req, res) => {
  try {
    const { templateId } = req.params;
    const limit = Math.min(parseInt(req.query.limit) || 100, 1000);
    const sources = getDataSources();
    
    if (sources.primarySource === 'binary') {
      const events = await binaryReader.loadAllRecords(db.DATA_PATH, 'events');
      
      const filtered = events
        .filter(e => e.template_id?.includes(templateId))
        .sort((a, b) => new Date(b.timestamp || 0).getTime() - new Date(a.timestamp || 0).getTime())
        .slice(0, limit);
      
      return res.json({ data: filtered, count: filtered.length, source: 'binary' });
    }
    
    const sql = `
      SELECT *
      FROM ${getUpdatesSource()}
      WHERE template_id LIKE '%${templateId}%'
      ORDER BY timestamp DESC
      LIMIT ${limit}
    `;
    
    const rows = await db.safeQuery(sql);
    res.json({ data: rows, count: rows.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/by-date - Get events for a specific date range
router.get('/by-date', async (req, res) => {
  try {
    const { start, end } = req.query;
    const limit = Math.min(parseInt(req.query.limit) || 1000, 10000);
    const sources = getDataSources();
    
    if (sources.primarySource === 'binary') {
      const events = await binaryReader.loadAllRecords(db.DATA_PATH, 'events');
      
      const startDate = start ? new Date(start).getTime() : 0;
      const endDate = end ? new Date(end).getTime() : Date.now();
      
      const filtered = events
        .filter(e => {
          if (!e.timestamp) return false;
          const ts = new Date(e.timestamp).getTime();
          return ts >= startDate && ts <= endDate;
        })
        .sort((a, b) => new Date(b.timestamp || 0).getTime() - new Date(a.timestamp || 0).getTime())
        .slice(0, limit);
      
      return res.json({ data: filtered, count: filtered.length, source: 'binary' });
    }
    
    let whereClause = '';
    if (start) whereClause += ` AND timestamp >= '${start}'`;
    if (end) whereClause += ` AND timestamp <= '${end}'`;
    
    const sql = `
      SELECT *
      FROM ${getUpdatesSource()}
      WHERE 1=1 ${whereClause}
      ORDER BY timestamp DESC
      LIMIT ${limit}
    `;
    
    const rows = await db.safeQuery(sql);
    res.json({ data: rows, count: rows.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/events/count - Get total event count
router.get('/count', async (req, res) => {
  try {
    const sources = getDataSources();
    
    if (sources.primarySource === 'binary') {
      const events = await binaryReader.loadAllRecords(db.DATA_PATH, 'events');
      return res.json({ count: events.length, source: 'binary' });
    }
    
    const sql = `
      SELECT COUNT(*) as total
      FROM ${getUpdatesSource()}
    `;
    
    const rows = await db.safeQuery(sql);
    res.json({ count: rows[0]?.total || 0 });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

export default router;
