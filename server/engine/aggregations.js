/**
 * Aggregations - Incremental aggregation updates (streaming-only)
 * 
 * Maintains aggregate tables based on ingested data,
 * tracking last-processed file to avoid reprocessing.
 * Uses streaming queries with LIMIT/OFFSET for large result sets.
 */

import { query } from '../duckdb/connection.js';

const STREAM_PAGE_SIZE = 10000; // Max rows per query page

/**
 * Get the last processed file ID for an aggregation
 */
async function getLastFileId(aggName) {
  const rows = await query(`
    SELECT last_file_id FROM aggregation_state WHERE agg_name = '${aggName}'
  `);
  return rows[0]?.last_file_id || 0;
}

/**
 * Update the last processed file ID
 */
async function setLastFileId(aggName, fileId) {
  await query(`
    INSERT INTO aggregation_state (agg_name, last_file_id, last_updated)
    VALUES ('${aggName}', ${fileId}, NOW())
    ON CONFLICT (agg_name) DO UPDATE SET 
      last_file_id = ${fileId},
      last_updated = NOW()
  `);
}

/**
 * Get the max file_id that has been ingested
 */
async function getMaxIngestedFileId() {
  const rows = await query(`
    SELECT MAX(file_id) as max_id FROM raw_files WHERE ingested = TRUE
  `);
  return rows[0]?.max_id || 0;
}

/**
 * Check if there's new data to process
 */
export async function hasNewData(aggName) {
  const lastProcessed = await getLastFileId(aggName);
  const maxIngested = await getMaxIngestedFileId();
  return maxIngested > lastProcessed;
}

/**
 * Get counts by event type from newly ingested events (streaming aggregation)
 */
export async function updateEventTypeCounts() {
  const aggName = 'event_type_counts';
  const lastFileId = await getLastFileId(aggName);
  const maxFileId = await getMaxIngestedFileId();
  
  if (maxFileId <= lastFileId) return null;
  
  const rows = await query(`
    SELECT 
      type,
      COUNT(*) as count
    FROM events_raw
    WHERE _file_id > ${lastFileId} AND _file_id <= ${maxFileId}
    GROUP BY type
  `);
  
  await setLastFileId(aggName, maxFileId);
  
  // Convert BigInt counts to Number for JSON serialization
  return rows.map(r => ({ ...r, count: Number(r.count) }));
}

/**
 * Stream template event counts with pagination
 */
export async function* streamTemplateEventCounts(pageSize = STREAM_PAGE_SIZE) {
  let offset = 0;
  let hasMore = true;
  
  while (hasMore) {
    const rows = await query(`
      SELECT 
        template,
        type,
        COUNT(*) as count
      FROM events_raw
      GROUP BY template, type
      ORDER BY count DESC
      LIMIT ${pageSize} OFFSET ${offset}
    `);
    
    for (const row of rows) {
      yield row;
    }
    
    hasMore = rows.length === pageSize;
    offset += pageSize;
  }
}

/**
 * Get recent event counts by template (paginated)
 */
export async function getTemplateEventCounts(limit = 100) {
  const rows = await query(`
    SELECT 
      template,
      type,
      COUNT(*) as count
    FROM events_raw
    GROUP BY template, type
    ORDER BY count DESC
    LIMIT ${limit}
  `);
  
  // Convert BigInt counts to Number for JSON serialization
  return rows.map(r => ({ ...r, count: Number(r.count) }));
}

/**
 * Get total record counts (efficient - uses DuckDB stats)
 */
export async function getTotalCounts() {
  const [events, updates] = await Promise.all([
    query('SELECT COUNT(*) as count FROM events_raw'),
    query('SELECT COUNT(*) as count FROM updates_raw'),
  ]);
  
  // Convert BigInt to Number for JSON serialization
  return {
    events: Number(events[0]?.count || 0),
    updates: Number(updates[0]?.count || 0),
  };
}

/**
 * Get time range of ingested data (efficient - MIN/MAX scan)
 */
export async function getTimeRange() {
  const rows = await query(`
    SELECT 
      MIN(recorded_at) as min_ts,
      MAX(recorded_at) as max_ts
    FROM events_raw
    WHERE recorded_at IS NOT NULL
  `);
  
  return rows[0] || {};
}

/**
 * Stream events with pagination
 */
export async function* streamEvents(options = {}) {
  const { template, type, pageSize = STREAM_PAGE_SIZE } = options;
  let offset = 0;
  let hasMore = true;
  
  const whereClause = [];
  if (template) whereClause.push(`template = '${template.replace(/'/g, "''")}'`);
  if (type) whereClause.push(`type = '${type.replace(/'/g, "''")}'`);
  const where = whereClause.length > 0 ? `WHERE ${whereClause.join(' AND ')}` : '';
  
  while (hasMore) {
    const rows = await query(`
      SELECT * FROM events_raw
      ${where}
      ORDER BY recorded_at DESC
      LIMIT ${pageSize} OFFSET ${offset}
    `);
    
    for (const row of rows) {
      yield row;
    }
    
    hasMore = rows.length === pageSize;
    offset += pageSize;
  }
}

/**
 * Update all aggregations incrementally
 */
export async function updateAllAggregations() {
  const results = {};
  
  try {
    results.eventTypeCounts = await updateEventTypeCounts();
    results.totals = await getTotalCounts();
    results.timeRange = await getTimeRange();
  } catch (err) {
    console.error('❌ Aggregation error:', err.message);
    results.error = err.message;
  }
  
  return results;
}
