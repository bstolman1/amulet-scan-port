/**
 * Stats API - DuckDB Parquet Only
 * 
 * Data Authority: All queries use DuckDB over Parquet files.
 * See docs/architecture.md for the Data Authority Contract.
 */

import { Router } from 'express';
import { 
  safeQuery, 
  query,
  hasFileType, 
  DATA_PATH, 
  IS_TEST, 
  TEST_FIXTURES_PATH 
} from '../duckdb/connection.js';
import { getTotalCounts, getTimeRange, getTemplateEventCounts } from '../engine/aggregations.js';
import { getIngestionStats } from '../engine/ingest.js';
import { initEngineSchema } from '../engine/schema.js';
import fs from 'fs';
import path from 'path';

const router = Router();

// Check if engine mode is enabled
const ENGINE_ENABLED = process.env.ENGINE_ENABLED === 'true';

/**
 * Get the SQL source for events data
 * Prefers Parquet, falls back to JSONL
 */
const getEventsSource = () => {
  // In test mode, use test fixtures
  if (IS_TEST) {
    return `(SELECT * FROM read_json_auto('${TEST_FIXTURES_PATH}/events-*.jsonl', union_by_name=true, ignore_errors=true))`;
  }
  
  const basePath = DATA_PATH.replace(/\\/g, '/');
  
  // Prefer Parquet
  if (hasFileType('events', '.parquet')) {
    return `read_parquet('${basePath}/**/events-*.parquet', union_by_name=true)`;
  }

  // Fall back to JSONL variants
  const hasJsonl = hasFileType('events', '.jsonl');
  const hasGzip = hasFileType('events', '.jsonl.gz');
  const hasZstd = hasFileType('events', '.jsonl.zst');

  if (!hasJsonl && !hasGzip && !hasZstd) {
    return `(SELECT NULL::VARCHAR as event_id, NULL::VARCHAR as event_type, NULL::VARCHAR as contract_id, 
             NULL::VARCHAR as template_id, NULL::VARCHAR as package_name, NULL::TIMESTAMP as timestamp,
             NULL::VARCHAR[] as signatories, NULL::VARCHAR[] as observers, NULL::JSON as payload WHERE false)`;
  }

  const queries = [];
  if (hasJsonl) queries.push(`SELECT * FROM read_json_auto('${basePath}/**/events-*.jsonl', union_by_name=true, ignore_errors=true)`);
  if (hasGzip) queries.push(`SELECT * FROM read_json_auto('${basePath}/**/events-*.jsonl.gz', union_by_name=true, ignore_errors=true)`);
  if (hasZstd) queries.push(`SELECT * FROM read_json_auto('${basePath}/**/events-*.jsonl.zst', union_by_name=true, ignore_errors=true)`);

  return `(${queries.join(' UNION ')})`;
};

/**
 * Get data source info for response metadata
 */
function getDataSourceInfo() {
  if (ENGINE_ENABLED) return 'engine';
  if (IS_TEST) return 'test';
  if (hasFileType('events', '.parquet')) return 'parquet';
  if (hasFileType('events', '.jsonl') || hasFileType('events', '.jsonl.gz') || hasFileType('events', '.jsonl.zst')) return 'jsonl';
  return 'none';
}

// POST /api/stats/init-engine-schema - Initialize the engine schema
router.post('/init-engine-schema', async (req, res) => {
  try {
    await initEngineSchema();
    res.json({ success: true, message: 'Engine schema initialized successfully' });
  } catch (err) {
    console.error('Error initializing engine schema:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/stats/overview - Dashboard overview stats
// DuckDB is disabled; return graceful stub response
router.get('/overview', (_req, res) => {
  res.json({
    mode: 'scan-only',
    message: 'Stats disabled (DuckDB offline)',
    total_events: null,
    unique_contracts: null,
    unique_templates: null,
    earliest_event: null,
    latest_event: null,
    data_source: 'none',
  });
});

// GET /api/stats/daily - Daily event counts
router.get('/daily', async (req, res) => {
  try {
    const days = Math.min(parseInt(req.query.days) || 30, 365);
    
    if (ENGINE_ENABLED) {
      return res.json({ data: [], data_source: 'engine' });
    }
    
    const sql = `
      SELECT 
        DATE_TRUNC('day', COALESCE(timestamp, effective_at)) as date,
        COUNT(*) as event_count,
        COUNT(DISTINCT contract_id) as contract_count
      FROM ${getEventsSource()}
      WHERE COALESCE(timestamp, effective_at) >= NOW() - INTERVAL '${days} days'
      GROUP BY DATE_TRUNC('day', COALESCE(timestamp, effective_at))
      ORDER BY date DESC
    `;
    
    const rows = await safeQuery(sql);
    res.json({ data: rows, data_source: getDataSourceInfo() });
  } catch (err) {
    console.error('Error fetching daily stats:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/stats/by-type - Event counts by type
router.get('/by-type', async (req, res) => {
  try {
    if (ENGINE_ENABLED) {
      return res.json({ data: [], data_source: 'engine' });
    }
    
    const sql = `
      SELECT 
        event_type,
        COUNT(*) as count
      FROM ${getEventsSource()}
      GROUP BY event_type
      ORDER BY count DESC
    `;
    
    const rows = await safeQuery(sql);
    res.json({ data: rows, data_source: getDataSourceInfo() });
  } catch (err) {
    console.error('Error fetching by-type stats:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/stats/by-template - Event counts by template
router.get('/by-template', async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 50, 500);
    
    if (ENGINE_ENABLED) {
      try {
        const templateCounts = await getTemplateEventCounts(limit);
        return res.json({ data: templateCounts, data_source: 'engine' });
      } catch (err) {
        console.error('Engine template stats error:', err.message);
        return res.json({ data: [], error: err.message });
      }
    }
    
    const sql = `
      SELECT 
        template_id,
        COUNT(*) as event_count,
        COUNT(DISTINCT contract_id) as contract_count,
        MIN(COALESCE(timestamp, effective_at)) as first_seen,
        MAX(COALESCE(timestamp, effective_at)) as last_seen
      FROM ${getEventsSource()}
      WHERE template_id IS NOT NULL
      GROUP BY template_id
      ORDER BY event_count DESC
      LIMIT ${limit}
    `;
    
    const rows = await safeQuery(sql);
    res.json({ data: rows, data_source: getDataSourceInfo() });
  } catch (err) {
    console.error('Error fetching by-template stats:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/stats/hourly - Hourly activity (last 24h)
router.get('/hourly', async (req, res) => {
  try {
    if (ENGINE_ENABLED) {
      return res.json({ data: [], data_source: 'engine' });
    }
    
    const sql = `
      SELECT 
        DATE_TRUNC('hour', COALESCE(timestamp, effective_at)) as hour,
        COUNT(*) as event_count
      FROM ${getEventsSource()}
      WHERE COALESCE(timestamp, effective_at) >= NOW() - INTERVAL '24 hours'
      GROUP BY DATE_TRUNC('hour', COALESCE(timestamp, effective_at))
      ORDER BY hour DESC
    `;
    
    const rows = await safeQuery(sql);
    res.json({ data: rows, data_source: getDataSourceInfo() });
  } catch (err) {
    console.error('Error fetching hourly stats:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/stats/burn - Burn statistics
router.get('/burn', async (req, res) => {
  try {
    if (ENGINE_ENABLED) {
      return res.json({ data: [], data_source: 'engine' });
    }
    
    const sql = `
      SELECT 
        DATE_TRUNC('day', COALESCE(timestamp, effective_at)) as date,
        SUM(CAST(json_extract(payload, '$.amount.amount') AS DOUBLE)) as burn_amount
      FROM ${getEventsSource()}
      WHERE template_id LIKE '%BurnMintSummary%'
      GROUP BY DATE_TRUNC('day', COALESCE(timestamp, effective_at))
      ORDER BY date DESC
      LIMIT 30
    `;
    
    const rows = await safeQuery(sql);
    res.json({ data: rows, data_source: getDataSourceInfo() });
  } catch (err) {
    console.error('Error fetching burn stats:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/stats/sources - Get info about available data sources
router.get('/sources', async (req, res) => {
  try {
    const hasParquetEvents = hasFileType('events', '.parquet');
    const hasParquetUpdates = hasFileType('updates', '.parquet');
    const hasJsonlEvents = hasFileType('events', '.jsonl');
    const hasJsonlUpdates = hasFileType('updates', '.jsonl');
    
    res.json({
      hasParquetEvents,
      hasParquetUpdates,
      hasJsonlEvents,
      hasJsonlUpdates,
      primarySource: getDataSourceInfo(),
      dataPath: DATA_PATH,
      engineEnabled: ENGINE_ENABLED,
    });
  } catch (err) {
    console.error('Error fetching sources:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/stats/ccview-comparison - CCVIEW-style counting comparison
router.get('/ccview-comparison', async (req, res) => {
  try {
    const basePath = DATA_PATH.replace(/\\/g, '/');
    const hasParquetEvents = hasFileType('events', '.parquet');
    const hasParquetUpdates = hasFileType('updates', '.parquet');
    
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
    
    if (hasParquetUpdates) {
      const updateResult = await safeQuery(`
        SELECT COUNT(*) as count FROM read_parquet('${basePath}/**/updates-*.parquet', union_by_name=true)
      `);
      updateCount = Number(updateResult[0]?.count || 0);
    }
    
    if (hasParquetEvents) {
      // Get total event count
      const eventResult = await safeQuery(`
        SELECT COUNT(*) as count FROM read_parquet('${basePath}/**/events-*.parquet', union_by_name=true)
      `);
      eventCount = Number(eventResult[0]?.count || 0);
      
      // Get breakdown by event_type
      const typeBreakdown = await safeQuery(`
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
      const contractIdCheck = await safeQuery(`
        SELECT 
          COUNT(CASE WHEN contract_id IS NOT NULL AND contract_id != '' THEN 1 END) as with_contract_id,
          COUNT(CASE WHEN contract_id IS NULL OR contract_id = '' THEN 1 END) as without_contract_id
        FROM read_parquet('${basePath}/**/events-*.parquet', union_by_name=true)
      `);
      eventsWithContractId = Number(contractIdCheck[0]?.with_contract_id || 0);
      eventsWithoutContractId = Number(contractIdCheck[0]?.without_contract_id || 0);
      
      // Sample some contract IDs
      const sampleResult = await safeQuery(`
        SELECT DISTINCT contract_id 
        FROM read_parquet('${basePath}/**/events-*.parquet', union_by_name=true)
        WHERE contract_id IS NOT NULL AND contract_id != ''
        LIMIT 5
      `);
      sampleContractIds = sampleResult.map(r => r.contract_id);
    }
    
    const ccviewStyleCount = createdEventCount + archivedEventCount + exercisedEventCount + reassignCreateCount + reassignArchiveCount;
    
    res.json({
      your_counts: {
        updates: updateCount,
        events: eventCount,
        description: 'Updates = transactions/reassignments, Events = individual contract events'
      },
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
      contract_id_check: {
        events_with_contract_id: eventsWithContractId,
        events_without_contract_id: eventsWithoutContractId,
        sample_contract_ids: sampleContractIds,
        percentage_with_id: eventCount > 0 ? ((eventsWithContractId / eventCount) * 100).toFixed(2) + '%' : 'N/A'
      },
      explanation: {
        discrepancy_reason: 'CCVIEW likely counts events (created+archived+exercised). Updates are transactions which each contain multiple events.',
        expected_ratio: 'Typically 1 update contains 1-3 events on average',
        your_ratio: updateCount > 0 ? (eventCount / updateCount).toFixed(2) : 'N/A'
      },
      data_source: 'parquet',
      data_path: basePath,
      files_found: {
        parquet_events: hasParquetEvents,
        parquet_updates: hasParquetUpdates
      }
    });
  } catch (err) {
    console.error('Error in ccview-comparison:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/stats/live-status - Live ingestion status
router.get('/live-status', async (req, res) => {
  try {
    const dataDir = process.env.DATA_DIR || DATA_PATH;
    const cursorDir = path.join(dataDir, 'cursors');
    const liveCursorFile = path.join(cursorDir, 'live-cursor.json');
    
    let liveCursor = null;
    let backfillCursors = [];
    let latestFileTimestamp = null;
    let earliestFileTimestamp = null;
    
    // Read live cursor if exists
    if (fs.existsSync(liveCursorFile)) {
      try {
        liveCursor = JSON.parse(fs.readFileSync(liveCursorFile, 'utf8'));
      } catch (e) { /* ignore */ }
    }
    
    // Read all backfill cursors
    if (fs.existsSync(cursorDir)) {
      const cursorFiles = fs.readdirSync(cursorDir).filter(f => f.endsWith('.json') && f !== 'live-cursor.json');
      for (const file of cursorFiles) {
        try {
          const cursor = JSON.parse(fs.readFileSync(path.join(cursorDir, file), 'utf8'));
          if (cursor.migration_id !== undefined) {
            backfillCursors.push({ file, ...cursor });
          }
        } catch (e) { /* ignore */ }
      }
    }
    
    const allBackfillComplete = backfillCursors.length > 0 && backfillCursors.every(c => c.complete === true);
    
    // Determine ingestion mode
    let mode = 'unknown';
    let status = 'stopped';
    let currentRecordTime = null;
    
    if (liveCursor && liveCursor.updated_at) {
      const lastUpdate = new Date(liveCursor.updated_at);
      const ageMs = Date.now() - lastUpdate.getTime();
      if (ageMs < 60000) {
        status = 'running';
        mode = 'live';
        currentRecordTime = liveCursor.record_time;
      } else if (ageMs < 300000) {
        status = 'idle';
        mode = 'live';
        currentRecordTime = liveCursor.record_time;
      }
    }
    
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
    console.error('Error fetching live status:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/stats/aggregation-state - Aggregation progress table
router.get('/aggregation-state', async (req, res) => {
  try {
    const tableCheck = await query(`
      SELECT COUNT(*) as cnt 
      FROM information_schema.tables 
      WHERE table_name = 'aggregation_state'
    `);
    
    if (!tableCheck?.[0]?.cnt || Number(tableCheck[0].cnt) === 0) {
      return res.json({ states: [], tableExists: false });
    }
    
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
    const errMsg = String(err?.message || '').toLowerCase();
    if (errMsg.includes('does not exist') || errMsg.includes('not exist') || errMsg.includes('no such table')) {
      return res.json({ states: [], tableExists: false });
    }
    console.error('Error fetching aggregation state:', err);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/stats/aggregation-state/reset - Reset aggregation state
router.post('/aggregation-state/reset', async (req, res) => {
  try {
    const tableCheck = await query(`
      SELECT COUNT(*) as cnt 
      FROM information_schema.tables 
      WHERE table_name = 'aggregation_state'
    `);
    
    if (!tableCheck?.[0]?.cnt || Number(tableCheck[0].cnt) === 0) {
      return res.status(400).json({ error: 'Aggregation state table does not exist' });
    }
    
    await query(`UPDATE aggregation_state SET last_file_id = 0, last_updated = NOW()`);
    
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

// DELETE /api/stats/live-cursor - Purge live cursor
router.delete('/live-cursor', async (req, res) => {
  try {
    const dataDir = process.env.DATA_DIR || DATA_PATH;
    const cursorDir = path.join(dataDir, 'cursors');
    const liveCursorFile = path.join(cursorDir, 'live-cursor.json');

    if (fs.existsSync(liveCursorFile)) {
      fs.unlinkSync(liveCursorFile);
      console.log('🗑️ Deleted live cursor file:', liveCursorFile);
      res.json({
        success: true,
        message: 'Live cursor deleted. The live ingestion script will need to be restarted.',
        deleted_file: liveCursorFile
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

// GET /api/stats/sv-weight-history - SV weight distribution over time
router.get('/sv-weight-history', async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 100, 500);
    
    const rules = await query(`
      SELECT 
        contract_id,
        effective_from,
        effective_until,
        sv_count,
        sv_parties,
        rule_version
      FROM dso_rules_state
      WHERE effective_from IS NOT NULL
      ORDER BY effective_from ASC
      LIMIT ${limit}
    `);

    const timeline = rules.map(rule => {
      let svParties = [];
      try {
        svParties = rule.sv_parties ? JSON.parse(rule.sv_parties) : [];
      } catch (e) {
        svParties = [];
      }

      return {
        timestamp: rule.effective_from,
        effectiveUntil: rule.effective_until,
        svCount: Number(rule.sv_count || 0),
        svParties: svParties,
        contractId: rule.contract_id,
      };
    });

    const byDay = {};
    for (const entry of timeline) {
      const date = new Date(entry.timestamp).toISOString().slice(0, 10);
      if (!byDay[date] || new Date(entry.timestamp) > new Date(byDay[date].timestamp)) {
        byDay[date] = entry;
      }
    }

    const dailyData = Object.entries(byDay)
      .map(([date, entry]) => ({
        date,
        svCount: entry.svCount,
        svParties: entry.svParties,
        timestamp: entry.timestamp,
      }))
      .sort((a, b) => a.date.localeCompare(b.date));

    const allSvNames = new Set();
    for (const day of dailyData) {
      for (const party of day.svParties || []) {
        const name = party.split('::')[0] || party.substring(0, 20);
        allSvNames.add(name);
      }
    }

    const stackedData = dailyData.map(day => {
      const svCounts = {};
      for (const party of day.svParties || []) {
        const name = party.split('::')[0] || party.substring(0, 20);
        svCounts[name] = (svCounts[name] || 0) + 1;
      }
      return {
        date: day.date,
        timestamp: day.timestamp,
        total: day.svCount,
        ...svCounts,
      };
    });

    res.json({
      data: timeline,
      dailyData,
      stackedData,
      svNames: Array.from(allSvNames).sort(),
      totalRules: timeline.length,
    });
  } catch (err) {
    console.error('Error fetching SV weight history:', err);
    res.status(500).json({ error: err.message });
  }
});

export default router;
