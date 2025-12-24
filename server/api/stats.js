import { Router } from 'express';
import db from '../duckdb/connection.js';
import binaryReader from '../duckdb/binary-reader.js';
import { getTotalCounts, getTimeRange, getTemplateEventCounts } from '../engine/aggregations.js';
import { getIngestionStats } from '../engine/ingest.js';
import { query } from '../duckdb/connection.js';
import { initEngineSchema } from '../engine/schema.js';

const router = Router();

// POST /api/stats/init-engine-schema - Initialize the engine schema (creates tables)
router.post('/init-engine-schema', async (req, res) => {
  try {
    await initEngineSchema();
    res.json({ success: true, message: 'Engine schema initialized successfully' });
  } catch (err) {
    console.error('Error initializing engine schema:', err);
    res.status(500).json({ error: err.message });
  }
});

// Check if engine mode is enabled
const ENGINE_ENABLED = process.env.ENGINE_ENABLED === 'true';

// Helper to get the correct read function for events data
// IMPORTANT: use UNION (not UNION ALL) to prevent duplicate records when multiple patterns overlap
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

  return `(${queries.join(' UNION ')})`;
};

// Check what data sources are available
function getDataSources() {
  // When engine is enabled, use engine as primary source
  if (ENGINE_ENABLED) {
    return {
      hasBinaryEvents: false,
      hasBinaryUpdates: false,
      hasParquet: false,
      hasJsonl: false,
      primarySource: 'engine'
    };
  }
  
  const hasBinaryEvents = binaryReader.hasBinaryFiles(db.DATA_PATH, 'events');
  const hasBinaryUpdates = binaryReader.hasBinaryFiles(db.DATA_PATH, 'updates');
  const hasParquet = db.hasFileType ? db.hasFileType('events', '.parquet') : false;
  const hasJsonl = db.hasFileType ? db.hasFileType('events', '.jsonl') : false;
  
  return {
    hasBinaryEvents,
    hasBinaryUpdates,
    hasParquet,
    hasJsonl,
    primarySource: hasBinaryEvents ? 'binary' : (hasParquet ? 'parquet' : (hasJsonl ? 'jsonl' : 'none'))
  };
}

// GET /api/stats/overview - Dashboard overview stats
router.get('/overview', async (req, res) => {
  try {
    const sources = getDataSources();
    
    // Use engine aggregations when engine is enabled
    if (sources.primarySource === 'engine') {
      try {
        const [counts, timeRange, ingestionStats] = await Promise.all([
          getTotalCounts(),
          getTimeRange(),
          getIngestionStats(),
        ]);
        
        return res.json({
          total_events: counts.events,
          unique_contracts: 0, // Not tracked yet in engine
          unique_templates: 0, // Not tracked yet in engine
          earliest_event: timeRange.min_ts,
          latest_event: timeRange.max_ts,
          data_source: 'engine',
          ingestion: ingestionStats,
        });
      } catch (err) {
        console.error('Engine stats error:', err.message);
        // Fall through to other methods
      }
    }
    
    if (sources.primarySource === 'binary') {
      // For large datasets, sample newest files to get time range
      const eventFiles = binaryReader.findBinaryFiles(db.DATA_PATH, 'events');
      
      if (eventFiles.length > 100) {
        // Sort by data date (from partition path) to get newest files
        eventFiles.sort((a, b) => {
          const yearA = a.match(/year=(\d{4})/)?.[1] || '0';
          const monthA = a.match(/month=(\d{2})/)?.[1] || '00';
          const dayA = a.match(/day=(\d{2})/)?.[1] || '00';
          const yearB = b.match(/year=(\d{4})/)?.[1] || '0';
          const monthB = b.match(/month=(\d{2})/)?.[1] || '00';
          const dayB = b.match(/day=(\d{2})/)?.[1] || '00';
          return `${yearB}${monthB}${dayB}`.localeCompare(`${yearA}${monthA}${dayA}`);
        });
        
        // Sample newest and oldest files to estimate time range
        const newestFiles = eventFiles.slice(0, 10);
        const oldestFiles = eventFiles.slice(-10);
        
        let earliest = null;
        let latest = null;
        
        // Read a few records from newest files to find latest timestamp
        for (const file of newestFiles.slice(0, 3)) {
          try {
            const result = await binaryReader.readBinaryFile(file);
            for (const r of result.records) {
              if (r.timestamp) {
                if (!latest || r.timestamp > latest) latest = r.timestamp;
              }
            }
          } catch (e) { /* ignore */ }
        }
        
        // Read a few records from oldest files to find earliest timestamp
        for (const file of oldestFiles.slice(0, 3)) {
          try {
            const result = await binaryReader.readBinaryFile(file);
            for (const r of result.records) {
              if (r.timestamp) {
                if (!earliest || r.timestamp < earliest) earliest = r.timestamp;
              }
            }
          } catch (e) { /* ignore */ }
        }
        
        return res.json({
          total_events: eventFiles.length * 100, // ~100 records per file estimate
          unique_contracts: 0,
          unique_templates: 0,
          earliest_event: earliest,
          latest_event: latest,
          data_source: 'binary',
          file_count: eventFiles.length,
          estimated: true,
          note: 'Large dataset - using estimates. Set ENGINE_ENABLED=true for precise stats.'
        });
      }
      
      // For smaller datasets, load all records
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
      FROM ${getEventsSource()}
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
    
    // Engine mode - return empty for now (would need to aggregate from events_raw)
    if (sources.primarySource === 'engine') {
      return res.json({ data: [], data_source: 'engine' });
    }
    
    if (sources.primarySource === 'binary') {
      const eventFiles = binaryReader.findBinaryFiles(db.DATA_PATH, 'events');
      if (eventFiles.length > 1000) {
        return res.json({ data: [], warning: 'Dataset too large' });
      }
      
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
      FROM ${getEventsSource()}
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
    
    // Engine mode - return from engine aggregations
    if (sources.primarySource === 'engine') {
      return res.json({ data: [], data_source: 'engine' });
    }
    
    if (sources.primarySource === 'binary') {
      const eventFiles = binaryReader.findBinaryFiles(db.DATA_PATH, 'events');
      if (eventFiles.length > 1000) {
        return res.json({ data: [], warning: 'Dataset too large' });
      }
      
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
      FROM ${getEventsSource()}
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
    
    // Engine mode - use engine aggregations
    if (sources.primarySource === 'engine') {
      try {
        const templateCounts = await getTemplateEventCounts(limit);
        return res.json({ data: templateCounts, data_source: 'engine' });
      } catch (err) {
        console.error('Engine template stats error:', err.message);
        return res.json({ data: [], error: err.message });
      }
    }
    
    if (sources.primarySource === 'binary') {
      const eventFiles = binaryReader.findBinaryFiles(db.DATA_PATH, 'events');
      if (eventFiles.length > 1000) {
        return res.json({ data: [], warning: 'Dataset too large' });
      }
      
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
      FROM ${getEventsSource()}
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
    
    // Engine mode
    if (sources.primarySource === 'engine') {
      return res.json({ data: [], data_source: 'engine' });
    }
    
    if (sources.primarySource === 'binary') {
      const eventFiles = binaryReader.findBinaryFiles(db.DATA_PATH, 'events');
      if (eventFiles.length > 1000) {
        return res.json({ data: [], warning: 'Dataset too large' });
      }
      
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
      FROM ${getEventsSource()}
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
    
    // Engine mode
    if (sources.primarySource === 'engine') {
      return res.json({ data: [], data_source: 'engine' });
    }
    
    if (sources.primarySource === 'binary') {
      const eventFiles = binaryReader.findBinaryFiles(db.DATA_PATH, 'events');
      if (eventFiles.length > 1000) {
        return res.json({ data: [], warning: 'Dataset too large' });
      }
      
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
      FROM ${getEventsSource()}
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

// GET /api/stats/ccview-comparison - CCVIEW-style counting comparison
// CCVIEW counts individual events within transactions, not just transactions
router.get('/ccview-comparison', async (req, res) => {
  try {
    const sources = getDataSources();
    const basePath = db.DATA_PATH.replace(/\\/g, '/');
    
    // Count updates (transactions/reassignments)
    let updateCount = 0;
    let eventCount = 0;
    let createdEventCount = 0;
    let archivedEventCount = 0;
    let exercisedEventCount = 0;
    let reassignCreateCount = 0;
    let reassignArchiveCount = 0;
    let eventsWithContractId = 0;
    let eventsWithoutContractId = 0;
    let sampleContractIds = [];
    
    // Check for parquet files first
    const hasParquetUpdates = db.hasFileType('updates', '.parquet');
    const hasParquetEvents = db.hasFileType('events', '.parquet');
    
    // Always check for binary files regardless of engine mode
    const hasBinaryEvents = binaryReader.hasBinaryFiles(db.DATA_PATH, 'events');
    const hasBinaryUpdates = binaryReader.hasBinaryFiles(db.DATA_PATH, 'updates');
    
    let actualSource = 'none';
    
    if (hasParquetUpdates) {
      try {
        const updateResult = await db.safeQuery(`
          SELECT COUNT(*) as count FROM read_parquet('${basePath}/**/updates-*.parquet', union_by_name=true)
        `);
        updateCount = Number(updateResult[0]?.count || 0);
        actualSource = 'parquet';
      } catch (e) {
        console.warn('Parquet updates count failed:', e.message);
      }
    }
    
    if (hasParquetEvents) {
      try {
        // Get total event count
        const eventResult = await db.safeQuery(`
          SELECT COUNT(*) as count FROM read_parquet('${basePath}/**/events-*.parquet', union_by_name=true)
        `);
        eventCount = Number(eventResult[0]?.count || 0);
        actualSource = 'parquet';
        
        // Get breakdown by event_type (CCVIEW style)
        const typeBreakdown = await db.safeQuery(`
          SELECT 
            event_type,
            COUNT(*) as count
          FROM read_parquet('${basePath}/**/events-*.parquet', union_by_name=true)
          GROUP BY event_type
        `);
        
        for (const row of typeBreakdown) {
          const type = row.event_type;
          const cnt = Number(row.count || 0);
          if (type === 'created') createdEventCount = cnt;
          else if (type === 'archived') archivedEventCount = cnt;
          else if (type === 'exercised') exercisedEventCount = cnt;
          else if (type === 'reassign_create') reassignCreateCount = cnt;
          else if (type === 'reassign_archive') reassignArchiveCount = cnt;
        }
        
        // Check contract_id presence
        const contractIdCheck = await db.safeQuery(`
          SELECT 
            COUNT(CASE WHEN contract_id IS NOT NULL AND contract_id != '' THEN 1 END) as with_contract_id,
            COUNT(CASE WHEN contract_id IS NULL OR contract_id = '' THEN 1 END) as without_contract_id
          FROM read_parquet('${basePath}/**/events-*.parquet', union_by_name=true)
        `);
        eventsWithContractId = Number(contractIdCheck[0]?.with_contract_id || 0);
        eventsWithoutContractId = Number(contractIdCheck[0]?.without_contract_id || 0);
        
        // Sample some contract IDs
        const sampleResult = await db.safeQuery(`
          SELECT DISTINCT contract_id 
          FROM read_parquet('${basePath}/**/events-*.parquet', union_by_name=true)
          WHERE contract_id IS NOT NULL AND contract_id != ''
          LIMIT 5
        `);
        sampleContractIds = sampleResult.map(r => r.contract_id);
        
      } catch (e) {
        console.warn('Parquet events count failed:', e.message);
      }
    }
    
    // If no parquet data found, try binary files (regardless of engine mode)
    if (eventCount === 0 && hasBinaryEvents) {
      actualSource = 'binary';
      const eventFiles = binaryReader.findBinaryFiles(db.DATA_PATH, 'events');
      const updateFiles = binaryReader.findBinaryFiles(db.DATA_PATH, 'updates');
      
      updateCount = updateFiles.length * 100; // Estimate ~100 records per file
      
      // Sample some files for event type breakdown
      const sampleFiles = eventFiles.slice(0, Math.min(100, eventFiles.length));
      let sampledEvents = 0;
      
      for (const file of sampleFiles) {
        try {
          const result = await binaryReader.readBinaryFile(file);
          for (const r of result.records) {
            sampledEvents++;
            const type = r.type || r.event_type;
            if (type === 'created') createdEventCount++;
            else if (type === 'archived') archivedEventCount++;
            else if (type === 'exercised') exercisedEventCount++;
            else if (type === 'reassign_create') reassignCreateCount++;
            else if (type === 'reassign_archive') reassignArchiveCount++;
            
            if (r.contract_id && r.contract_id !== '') {
              eventsWithContractId++;
              if (sampleContractIds.length < 5) sampleContractIds.push(r.contract_id);
            } else {
              eventsWithoutContractId++;
            }
          }
        } catch (e) { /* ignore */ }
      }
      
      // Extrapolate from sample
      if (sampledEvents > 0 && sampleFiles.length < eventFiles.length) {
        const ratio = eventFiles.length / sampleFiles.length;
        eventCount = Math.round(sampledEvents * ratio);
        createdEventCount = Math.round(createdEventCount * ratio);
        archivedEventCount = Math.round(archivedEventCount * ratio);
        exercisedEventCount = Math.round(exercisedEventCount * ratio);
        reassignCreateCount = Math.round(reassignCreateCount * ratio);
        reassignArchiveCount = Math.round(reassignArchiveCount * ratio);
        eventsWithContractId = Math.round(eventsWithContractId * ratio);
        eventsWithoutContractId = Math.round(eventsWithoutContractId * ratio);
      } else {
        eventCount = sampledEvents;
      }
    }
    
    // CCVIEW counts all individual events
    const ccviewStyleCount = createdEventCount + archivedEventCount + exercisedEventCount + reassignCreateCount + reassignArchiveCount;
    
    res.json({
      // Your system's counts
      your_counts: {
        updates: updateCount,
        events: eventCount,
        description: 'Updates = transactions/reassignments, Events = individual contract events'
      },
      // CCVIEW-style breakdown
      ccview_style: {
        total_events: ccviewStyleCount,
        breakdown: {
          created_events: createdEventCount,
          archived_events: archivedEventCount,
          exercised_events: exercisedEventCount,
          reassign_create_events: reassignCreateCount,
          reassign_archive_events: reassignArchiveCount,
        },
        description: 'CCVIEW counts each created/archived/exercised event separately'
      },
      // Contract ID verification
      contract_id_check: {
        events_with_contract_id: eventsWithContractId,
        events_without_contract_id: eventsWithoutContractId,
        sample_contract_ids: sampleContractIds,
        percentage_with_id: eventCount > 0 ? ((eventsWithContractId / eventCount) * 100).toFixed(2) + '%' : 'N/A'
      },
      // Explanation
      explanation: {
        discrepancy_reason: 'CCVIEW likely counts 97M events (created+archived+exercised). Your 69M "updates" are transactions which each contain multiple events.',
        expected_ratio: 'Typically 1 update contains 1-3 events on average',
        your_ratio: updateCount > 0 ? (eventCount / updateCount).toFixed(2) : 'N/A'
      },
      data_source: actualSource,
      data_path: basePath,
      files_found: {
        binary_events: hasBinaryEvents,
        binary_updates: hasBinaryUpdates,
        parquet_events: hasParquetEvents,
        parquet_updates: hasParquetUpdates
      }
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/stats/live-status - Live ingestion status
router.get('/live-status', async (req, res) => {
  try {
    const fs = await import('fs');
    const path = await import('path');
    
    const DATA_DIR = process.env.DATA_DIR || db.DATA_PATH;
    const CURSOR_DIR = path.join(DATA_DIR, 'cursors');
    const LIVE_CURSOR_FILE = path.join(CURSOR_DIR, 'live-cursor.json');
    
    let liveCursor = null;
    let backfillCursors = [];
    let latestFileTimestamp = null;
    let earliestFileTimestamp = null;
    
    // Read live cursor if exists
    if (fs.existsSync(LIVE_CURSOR_FILE)) {
      try {
        liveCursor = JSON.parse(fs.readFileSync(LIVE_CURSOR_FILE, 'utf8'));
      } catch (e) { /* ignore */ }
    }
    
    // Read all backfill cursors
    if (fs.existsSync(CURSOR_DIR)) {
      const cursorFiles = fs.readdirSync(CURSOR_DIR).filter(f => f.endsWith('.json') && f !== 'live-cursor.json');
      for (const file of cursorFiles) {
        try {
          const cursor = JSON.parse(fs.readFileSync(path.join(CURSOR_DIR, file), 'utf8'));
          if (cursor.migration_id !== undefined) {
            backfillCursors.push({
              file,
              ...cursor
            });
          }
        } catch (e) { /* ignore */ }
      }
    }
    
    // Check if all backfill cursors are complete
    const allBackfillComplete = backfillCursors.length > 0 && backfillCursors.every(c => c.complete === true);
    
    // Find latest file timestamp from raw directory
    const rawDir = path.join(DATA_DIR, 'raw');
    if (fs.existsSync(rawDir)) {
      const findLatestFiles = (dir, files = []) => {
        const items = fs.readdirSync(dir);
        for (const item of items) {
          const itemPath = path.join(dir, item);
          const stat = fs.statSync(itemPath);
          if (stat.isDirectory()) {
            findLatestFiles(itemPath, files);
          } else if (item.endsWith('.pb.zst')) {
            files.push({ path: itemPath, mtime: stat.mtime });
          }
        }
        return files;
      };
      
      try {
        const files = findLatestFiles(rawDir);
        if (files.length > 0) {
          files.sort((a, b) => b.mtime - a.mtime);
          latestFileTimestamp = files[0].mtime.toISOString();
          earliestFileTimestamp = files[files.length - 1].mtime.toISOString();
        }
      } catch (e) {
        console.warn('Failed to scan raw directory:', e.message);
      }
    }
    
    // Determine ingestion mode
    let mode = 'unknown';
    let status = 'stopped';
    let currentRecordTime = null;
    
    if (liveCursor && liveCursor.updated_at) {
      const lastUpdate = new Date(liveCursor.updated_at);
      const ageMs = Date.now() - lastUpdate.getTime();
      if (ageMs < 60000) { // Updated within last minute
        status = 'running';
        mode = 'live';
        currentRecordTime = liveCursor.record_time;
      } else if (ageMs < 300000) { // Within 5 minutes
        status = 'idle';
        mode = 'live';
        currentRecordTime = liveCursor.record_time;
      }
    }
    
    // Check if backfill is still running based on cursor files
    const latestBackfill = backfillCursors.sort((a, b) => (b.migration_id || 0) - (a.migration_id || 0))[0];
    if (latestBackfill && !latestBackfill.complete) {
      mode = 'backfill';
      currentRecordTime = latestBackfill.max_time || latestBackfill.last_before;
    }
    
    res.json({
      mode,
      status,
      live_cursor: liveCursor,
      backfill_cursors: backfillCursors,
      all_backfill_complete: allBackfillComplete,
      latest_file_write: latestFileTimestamp,
      earliest_file_write: earliestFileTimestamp,
      current_record_time: currentRecordTime,
      suggestion: !allBackfillComplete 
        ? 'Backfill not complete. Run backfill scripts or mark cursors as complete.'
        : !liveCursor
          ? 'Backfill complete! Run: node scripts/ingest/fetch-updates-parquet.js --live'
          : null
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/stats/aggregation-state - Aggregation progress table (DuckDB)
router.get('/aggregation-state', async (req, res) => {
  try {
    // First check if the aggregation_state table exists
    const tableCheck = await query(`
      SELECT COUNT(*) as cnt 
      FROM information_schema.tables 
      WHERE table_name = 'aggregation_state'
    `);
    
    if (!tableCheck?.[0]?.cnt || Number(tableCheck[0].cnt) === 0) {
      // Table doesn't exist yet - return empty but valid response
      return res.json({ states: [], tableExists: false });
    }
    
    // aggregation_state is used by engine/aggregations for incremental progress.
    const rows = await query(`
      SELECT agg_name, last_file_id, last_updated
      FROM aggregation_state
      ORDER BY last_updated DESC
    `);

    res.json({
      states: rows.map(r => ({
        agg_name: r.agg_name,
        last_file_id: Number(r.last_file_id || 0),
        last_updated: r.last_updated,
      })),
      tableExists: true
    });
  } catch (err) {
    // If engine schema isn't initialized yet, the table may not exist.
    const errMsg = String(err?.message || '').toLowerCase();
    if (errMsg.includes('does not exist') || errMsg.includes('not exist') || errMsg.includes('no such table')) {
      return res.json({ states: [], tableExists: false });
    }
    console.error('Error fetching aggregation state:', err);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/stats/aggregation-state/reset - Reset aggregation state to reprocess all data
router.post('/aggregation-state/reset', async (req, res) => {
  try {
    // Check if table exists
    const tableCheck = await query(`
      SELECT COUNT(*) as cnt 
      FROM information_schema.tables 
      WHERE table_name = 'aggregation_state'
    `);
    
    if (!tableCheck?.[0]?.cnt || Number(tableCheck[0].cnt) === 0) {
      return res.status(400).json({ error: 'Aggregation state table does not exist' });
    }
    
    // Reset all aggregation states to file 0
    await query(`UPDATE aggregation_state SET last_file_id = 0, last_updated = NOW()`);
    
    // Fetch updated state
    const rows = await query(`
      SELECT agg_name, last_file_id, last_updated
      FROM aggregation_state
      ORDER BY last_updated DESC
    `);
    
    console.log('🔄 Reset aggregation state - all aggregations will reprocess from start');
    
    res.json({
      success: true,
      message: 'Aggregation state reset. All aggregations will reprocess from the beginning.',
      states: rows.map(r => ({
        agg_name: r.agg_name,
        last_file_id: Number(r.last_file_id || 0),
        last_updated: r.last_updated,
      }))
    });
  } catch (err) {
    console.error('Error resetting aggregation state:', err);
    res.status(500).json({ error: err.message });
  }
});

// DELETE /api/stats/live-cursor - Purge live cursor to stop live ingestion tracking
router.delete('/live-cursor', async (req, res) => {
  try {
    const path = await import('path');
    const fs = await import('fs');

    const DATA_DIR = process.env.DATA_DIR || db.DATA_PATH;
    const CURSOR_DIR = path.join(DATA_DIR, 'cursors');
    const LIVE_CURSOR_FILE = path.join(CURSOR_DIR, 'live-cursor.json');

    if (fs.existsSync(LIVE_CURSOR_FILE)) {
      fs.unlinkSync(LIVE_CURSOR_FILE);
      console.log('🗑️ Deleted live cursor file:', LIVE_CURSOR_FILE);
      res.json({
        success: true,
        message: 'Live cursor deleted. The live ingestion script will need to be restarted.',
        deleted_file: LIVE_CURSOR_FILE
      });
    } else {
      res.json({
        success: true,
        message: 'No live cursor file found.',
        deleted_file: null
      });
    }
  } catch (err) {
    console.error('Error deleting live cursor:', err);
    res.status(500).json({ error: err.message });
  }
});

export default router;
