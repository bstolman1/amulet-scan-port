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
  const hasBinaryUpdates = binaryReader.hasBinaryFiles(db.DATA_PATH, 'updates');
  const hasJsonl = db.hasFileType ? db.hasFileType('events', '.jsonl') : false;
  
  return {
    hasBinaryEvents,
    hasBinaryUpdates,
    hasJsonl,
    primarySource: hasBinaryEvents ? 'binary' : (hasJsonl ? 'jsonl' : 'none')
  };
}

// GET /api/stats/overview - Dashboard overview stats
router.get('/overview', async (req, res) => {
  try {
    const sources = getDataSources();
    
    if (sources.primarySource === 'binary') {
      // Read from binary files
      const events = await binaryReader.loadAllRecords(db.DATA_PATH, 'events');
      
      if (events.length === 0) {
        return res.json({
          total_events: 0,
          unique_contracts: 0,
          unique_templates: 0,
          earliest_event: null,
          latest_event: null,
          data_source: 'binary'
        });
      }
      
      const contracts = new Set();
      const templates = new Set();
      let earliest = null;
      let latest = null;
      
      for (const event of events) {
        if (event.contract_id) contracts.add(event.contract_id);
        if (event.template_id) templates.add(event.template_id);
        if (event.timestamp) {
          if (!earliest || event.timestamp < earliest) earliest = event.timestamp;
          if (!latest || event.timestamp > latest) latest = event.timestamp;
        }
      }
      
      return res.json({
        total_events: events.length,
        unique_contracts: contracts.size,
        unique_templates: templates.size,
        earliest_event: earliest,
        latest_event: latest,
        data_source: 'binary'
      });
    }
    
    // Fallback to JSONL/DuckDB
    const sql = `
      SELECT 
        COUNT(*) as total_events,
        COUNT(DISTINCT contract_id) as unique_contracts,
        COUNT(DISTINCT template_id) as unique_templates,
        MIN(timestamp) as earliest_event,
        MAX(timestamp) as latest_event
      FROM ${getUpdatesSource()}
    `;
    
    const rows = await db.safeQuery(sql);
    res.json({ ...rows[0], data_source: 'jsonl' } || {});
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/stats/daily - Daily event counts
router.get('/daily', async (req, res) => {
  try {
    const days = Math.min(parseInt(req.query.days) || 30, 365);
    const sources = getDataSources();
    
    if (sources.primarySource === 'binary') {
      const events = await binaryReader.loadAllRecords(db.DATA_PATH, 'events');
      const cutoff = new Date(Date.now() - days * 24 * 60 * 60 * 1000);
      
      // Group by day
      const dailyMap = new Map();
      const contractsByDay = new Map();
      
      for (const event of events) {
        if (!event.timestamp) continue;
        const eventDate = new Date(event.timestamp);
        if (eventDate < cutoff) continue;
        
        const dayKey = eventDate.toISOString().split('T')[0];
        dailyMap.set(dayKey, (dailyMap.get(dayKey) || 0) + 1);
        
        if (!contractsByDay.has(dayKey)) contractsByDay.set(dayKey, new Set());
        if (event.contract_id) contractsByDay.get(dayKey).add(event.contract_id);
      }
      
      const data = Array.from(dailyMap.entries())
        .map(([date, event_count]) => ({
          date,
          event_count,
          contract_count: contractsByDay.get(date)?.size || 0
        }))
        .sort((a, b) => b.date.localeCompare(a.date));
      
      return res.json({ data });
    }
    
    const sql = `
      SELECT 
        DATE_TRUNC('day', timestamp) as date,
        COUNT(*) as event_count,
        COUNT(DISTINCT contract_id) as contract_count
      FROM ${getUpdatesSource()}
      WHERE timestamp >= NOW() - INTERVAL '${days} days'
      GROUP BY DATE_TRUNC('day', timestamp)
      ORDER BY date DESC
    `;
    
    const rows = await db.safeQuery(sql);
    res.json({ data: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/stats/by-type - Event counts by type
router.get('/by-type', async (req, res) => {
  try {
    const sources = getDataSources();
    
    if (sources.primarySource === 'binary') {
      const events = await binaryReader.loadAllRecords(db.DATA_PATH, 'events');
      const typeCounts = new Map();
      
      for (const event of events) {
        const type = event.event_type || 'unknown';
        typeCounts.set(type, (typeCounts.get(type) || 0) + 1);
      }
      
      const data = Array.from(typeCounts.entries())
        .map(([event_type, count]) => ({ event_type, count }))
        .sort((a, b) => b.count - a.count);
      
      return res.json({ data });
    }
    
    const sql = `
      SELECT 
        event_type,
        COUNT(*) as count
      FROM ${getUpdatesSource()}
      GROUP BY event_type
      ORDER BY count DESC
    `;
    
    const rows = await db.safeQuery(sql);
    res.json({ data: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/stats/by-template - Event counts by template
router.get('/by-template', async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 50, 500);
    const sources = getDataSources();
    
    if (sources.primarySource === 'binary') {
      const events = await binaryReader.loadAllRecords(db.DATA_PATH, 'events');
      const templateStats = new Map();
      
      for (const event of events) {
        const template = event.template_id;
        if (!template) continue;
        
        if (!templateStats.has(template)) {
          templateStats.set(template, {
            template_id: template,
            event_count: 0,
            contracts: new Set(),
            first_seen: event.timestamp,
            last_seen: event.timestamp
          });
        }
        
        const stats = templateStats.get(template);
        stats.event_count++;
        if (event.contract_id) stats.contracts.add(event.contract_id);
        if (event.timestamp) {
          if (event.timestamp < stats.first_seen) stats.first_seen = event.timestamp;
          if (event.timestamp > stats.last_seen) stats.last_seen = event.timestamp;
        }
      }
      
      const data = Array.from(templateStats.values())
        .map(s => ({
          template_id: s.template_id,
          event_count: s.event_count,
          contract_count: s.contracts.size,
          first_seen: s.first_seen,
          last_seen: s.last_seen
        }))
        .sort((a, b) => b.event_count - a.event_count)
        .slice(0, limit);
      
      return res.json({ data });
    }
    
    const sql = `
      SELECT 
        template_id,
        COUNT(*) as event_count,
        COUNT(DISTINCT contract_id) as contract_count,
        MIN(timestamp) as first_seen,
        MAX(timestamp) as last_seen
      FROM ${getUpdatesSource()}
      WHERE template_id IS NOT NULL
      GROUP BY template_id
      ORDER BY event_count DESC
      LIMIT ${limit}
    `;
    
    const rows = await db.safeQuery(sql);
    res.json({ data: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/stats/hourly - Hourly activity (last 24h)
router.get('/hourly', async (req, res) => {
  try {
    const sources = getDataSources();
    
    if (sources.primarySource === 'binary') {
      const events = await binaryReader.loadAllRecords(db.DATA_PATH, 'events');
      const cutoff = new Date(Date.now() - 24 * 60 * 60 * 1000);
      const hourlyMap = new Map();
      
      for (const event of events) {
        if (!event.timestamp) continue;
        const eventDate = new Date(event.timestamp);
        if (eventDate < cutoff) continue;
        
        const hourKey = eventDate.toISOString().substring(0, 13) + ':00:00Z';
        hourlyMap.set(hourKey, (hourlyMap.get(hourKey) || 0) + 1);
      }
      
      const data = Array.from(hourlyMap.entries())
        .map(([hour, event_count]) => ({ hour, event_count }))
        .sort((a, b) => b.hour.localeCompare(a.hour));
      
      return res.json({ data });
    }
    
    const sql = `
      SELECT 
        DATE_TRUNC('hour', timestamp) as hour,
        COUNT(*) as event_count
      FROM ${getUpdatesSource()}
      WHERE timestamp >= NOW() - INTERVAL '24 hours'
      GROUP BY DATE_TRUNC('hour', timestamp)
      ORDER BY hour DESC
    `;
    
    const rows = await db.safeQuery(sql);
    res.json({ data: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/stats/burn - Burn statistics
router.get('/burn', async (req, res) => {
  try {
    const sources = getDataSources();
    
    if (sources.primarySource === 'binary') {
      const events = await binaryReader.loadAllRecords(db.DATA_PATH, 'events');
      const burnByDay = new Map();
      
      for (const event of events) {
        if (!event.template_id?.includes('BurnMintSummary')) continue;
        if (!event.timestamp) continue;
        
        const dayKey = new Date(event.timestamp).toISOString().split('T')[0];
        const amount = event.payload?.amount?.amount || 0;
        burnByDay.set(dayKey, (burnByDay.get(dayKey) || 0) + parseFloat(amount));
      }
      
      const data = Array.from(burnByDay.entries())
        .map(([date, burn_amount]) => ({ date, burn_amount }))
        .sort((a, b) => b.date.localeCompare(a.date))
        .slice(0, 30);
      
      return res.json({ data });
    }
    
    const sql = `
      SELECT 
        DATE_TRUNC('day', timestamp) as date,
        SUM(CAST(json_extract(payload, '$.amount.amount') AS DOUBLE)) as burn_amount
      FROM ${getUpdatesSource()}
      WHERE template_id LIKE '%BurnMintSummary%'
      GROUP BY DATE_TRUNC('day', timestamp)
      ORDER BY date DESC
      LIMIT 30
    `;
    
    const rows = await db.safeQuery(sql);
    res.json({ data: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/stats/sources - Get info about available data sources
router.get('/sources', async (req, res) => {
  try {
    const sources = getDataSources();
    const binaryEventFiles = binaryReader.findBinaryFiles(db.DATA_PATH, 'events').length;
    const binaryUpdateFiles = binaryReader.findBinaryFiles(db.DATA_PATH, 'updates').length;
    
    res.json({
      ...sources,
      binaryEventFiles,
      binaryUpdateFiles,
      dataPath: db.DATA_PATH
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/stats/refresh-cache - Invalidate cache
router.post('/refresh-cache', async (req, res) => {
  try {
    binaryReader.invalidateCache();
    res.json({ status: 'ok', message: 'Cache invalidated' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

export default router;
