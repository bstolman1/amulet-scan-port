/**
 * Aggregations - Incremental aggregation updates (streaming-only)
 * 
 * Maintains aggregate tables based on ingested data,
 * tracking last-processed file to avoid reprocessing.
 * Uses streaming queries with LIMIT/OFFSET for large result sets.
 * 
 * NOTE: The connection pool in connection.js now handles concurrent queries safely.
 * Each query gets its own connection, so parallel queries are now supported.
 */
// @ts-nocheck
function stryNS_9fa48() {
  var g = typeof globalThis === 'object' && globalThis && globalThis.Math === Math && globalThis || new Function("return this")();
  var ns = g.__stryker__ || (g.__stryker__ = {});
  if (ns.activeMutant === undefined && g.process && g.process.env && g.process.env.__STRYKER_ACTIVE_MUTANT__) {
    ns.activeMutant = g.process.env.__STRYKER_ACTIVE_MUTANT__;
  }
  function retrieveNS() {
    return ns;
  }
  stryNS_9fa48 = retrieveNS;
  return retrieveNS();
}
stryNS_9fa48();
function stryCov_9fa48() {
  var ns = stryNS_9fa48();
  var cov = ns.mutantCoverage || (ns.mutantCoverage = {
    static: {},
    perTest: {}
  });
  function cover() {
    var c = cov.static;
    if (ns.currentTestId) {
      c = cov.perTest[ns.currentTestId] = cov.perTest[ns.currentTestId] || {};
    }
    var a = arguments;
    for (var i = 0; i < a.length; i++) {
      c[a[i]] = (c[a[i]] || 0) + 1;
    }
  }
  stryCov_9fa48 = cover;
  cover.apply(null, arguments);
}
function stryMutAct_9fa48(id) {
  var ns = stryNS_9fa48();
  function isActive(id) {
    if (ns.activeMutant === id) {
      if (ns.hitCount !== void 0 && ++ns.hitCount > ns.hitLimit) {
        throw new Error('Stryker: Hit count limit reached (' + ns.hitCount + ')');
      }
      return true;
    }
    return false;
  }
  stryMutAct_9fa48 = isActive;
  return isActive(id);
}
import { query, queryParallel } from '../duckdb/connection.js';
const STREAM_PAGE_SIZE = 10000; // Max rows per query page

/**
 * Get the last processed file ID for an aggregation
 */
async function getLastFileId(aggName) {
  if (stryMutAct_9fa48("0")) {
    {}
  } else {
    stryCov_9fa48("0");
    const rows = await query(`
    SELECT last_file_id FROM aggregation_state WHERE agg_name = '${aggName}'
  `);
    return stryMutAct_9fa48("4") ? rows[0]?.last_file_id && 0 : stryMutAct_9fa48("3") ? false : stryMutAct_9fa48("2") ? true : (stryCov_9fa48("2", "3", "4"), (stryMutAct_9fa48("5") ? rows[0].last_file_id : (stryCov_9fa48("5"), rows[0]?.last_file_id)) || 0);
  }
}

/**
 * Update the last processed file ID
 */
async function setLastFileId(aggName, fileId) {
  if (stryMutAct_9fa48("6")) {
    {}
  } else {
    stryCov_9fa48("6");
    await query(`
    INSERT INTO aggregation_state (agg_name, last_file_id, last_updated)
    VALUES ('${aggName}', ${fileId}, NOW())
    ON CONFLICT (agg_name) DO UPDATE SET 
      last_file_id = ${fileId},
      last_updated = NOW()
  `);
  }
}

/**
 * Get the max file_id that has been ingested
 */
async function getMaxIngestedFileId() {
  if (stryMutAct_9fa48("8")) {
    {}
  } else {
    stryCov_9fa48("8");
    const rows = await query(`
    SELECT MAX(file_id) as max_id FROM raw_files WHERE ingested = TRUE
  `);
    return stryMutAct_9fa48("12") ? rows[0]?.max_id && 0 : stryMutAct_9fa48("11") ? false : stryMutAct_9fa48("10") ? true : (stryCov_9fa48("10", "11", "12"), (stryMutAct_9fa48("13") ? rows[0].max_id : (stryCov_9fa48("13"), rows[0]?.max_id)) || 0);
  }
}

/**
 * Check if there's new data to process
 */
export async function hasNewData(aggName) {
  if (stryMutAct_9fa48("14")) {
    {}
  } else {
    stryCov_9fa48("14");
    const lastProcessed = await getLastFileId(aggName);
    const maxIngested = await getMaxIngestedFileId();
    return stryMutAct_9fa48("18") ? maxIngested <= lastProcessed : stryMutAct_9fa48("17") ? maxIngested >= lastProcessed : stryMutAct_9fa48("16") ? false : stryMutAct_9fa48("15") ? true : (stryCov_9fa48("15", "16", "17", "18"), maxIngested > lastProcessed);
  }
}

/**
 * Get counts by event type from newly ingested events (streaming aggregation)
 */
export async function updateEventTypeCounts() {
  if (stryMutAct_9fa48("19")) {
    {}
  } else {
    stryCov_9fa48("19");
    const aggName = 'event_type_counts';
    const lastFileId = await getLastFileId(aggName);
    const maxFileId = await getMaxIngestedFileId();
    if (stryMutAct_9fa48("24") ? maxFileId > lastFileId : stryMutAct_9fa48("23") ? maxFileId < lastFileId : stryMutAct_9fa48("22") ? false : stryMutAct_9fa48("21") ? true : (stryCov_9fa48("21", "22", "23", "24"), maxFileId <= lastFileId)) return null;
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
    return rows.map(stryMutAct_9fa48("26") ? () => undefined : (stryCov_9fa48("26"), r => ({
      ...r,
      count: Number(r.count)
    })));
  }
}

/**
 * Stream template event counts with pagination
 */
export async function* streamTemplateEventCounts(pageSize = STREAM_PAGE_SIZE) {
  if (stryMutAct_9fa48("28")) {
    {}
  } else {
    stryCov_9fa48("28");
    let offset = 0;
    let hasMore = stryMutAct_9fa48("29") ? false : (stryCov_9fa48("29"), true);
    while (stryMutAct_9fa48("30") ? false : (stryCov_9fa48("30"), hasMore)) {
      if (stryMutAct_9fa48("31")) {
        {}
      } else {
        stryCov_9fa48("31");
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
          if (stryMutAct_9fa48("33")) {
            {}
          } else {
            stryCov_9fa48("33");
            yield row;
          }
        }
        hasMore = stryMutAct_9fa48("36") ? rows.length !== pageSize : stryMutAct_9fa48("35") ? false : stryMutAct_9fa48("34") ? true : (stryCov_9fa48("34", "35", "36"), rows.length === pageSize);
        stryMutAct_9fa48("37") ? offset -= pageSize : (stryCov_9fa48("37"), offset += pageSize);
      }
    }
  }
}

/**
 * Get recent event counts by template (paginated)
 */
export async function getTemplateEventCounts(limit = 100) {
  if (stryMutAct_9fa48("38")) {
    {}
  } else {
    stryCov_9fa48("38");
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
    return rows.map(stryMutAct_9fa48("40") ? () => undefined : (stryCov_9fa48("40"), r => ({
      ...r,
      count: Number(r.count)
    })));
  }
}

/**
 * Get total record counts (efficient - uses DuckDB stats)
 * Uses parallel queries via the connection pool for performance
 */
export async function getTotalCounts() {
  if (stryMutAct_9fa48("42")) {
    {}
  } else {
    stryCov_9fa48("42");
    const [events, updates] = await queryParallel(stryMutAct_9fa48("43") ? [] : (stryCov_9fa48("43"), [{
      sql: 'SELECT COUNT(*) as count FROM events_raw'
    }, {
      sql: 'SELECT COUNT(*) as count FROM updates_raw'
    }]));

    // Convert BigInt to Number for JSON serialization
    return {
      events: Number(stryMutAct_9fa48("51") ? events[0]?.count && 0 : stryMutAct_9fa48("50") ? false : stryMutAct_9fa48("49") ? true : (stryCov_9fa48("49", "50", "51"), (stryMutAct_9fa48("52") ? events[0].count : (stryCov_9fa48("52"), events[0]?.count)) || 0)),
      updates: Number(stryMutAct_9fa48("55") ? updates[0]?.count && 0 : stryMutAct_9fa48("54") ? false : stryMutAct_9fa48("53") ? true : (stryCov_9fa48("53", "54", "55"), (stryMutAct_9fa48("56") ? updates[0].count : (stryCov_9fa48("56"), updates[0]?.count)) || 0))
    };
  }
}

/**
 * Get time range of ingested data (efficient - MIN/MAX scan)
 */
export async function getTimeRange() {
  if (stryMutAct_9fa48("57")) {
    {}
  } else {
    stryCov_9fa48("57");
    const rows = await query(`
    SELECT 
      MIN(recorded_at) as min_ts,
      MAX(recorded_at) as max_ts
    FROM events_raw
    WHERE recorded_at IS NOT NULL
  `);
    return stryMutAct_9fa48("61") ? rows[0] && {} : stryMutAct_9fa48("60") ? false : stryMutAct_9fa48("59") ? true : (stryCov_9fa48("59", "60", "61"), rows[0] || {});
  }
}

/**
 * Stream events with pagination
 */
export async function* streamEvents(options = {}) {
  if (stryMutAct_9fa48("62")) {
    {}
  } else {
    stryCov_9fa48("62");
    const {
      template,
      type,
      pageSize = STREAM_PAGE_SIZE
    } = options;
    let offset = 0;
    let hasMore = stryMutAct_9fa48("63") ? false : (stryCov_9fa48("63"), true);
    const whereClause = stryMutAct_9fa48("64") ? ["Stryker was here"] : (stryCov_9fa48("64"), []);
    if (stryMutAct_9fa48("66") ? false : stryMutAct_9fa48("65") ? true : (stryCov_9fa48("65", "66"), template)) whereClause.push(`template = '${template.replace(/'/g, "''")}'`);
    if (stryMutAct_9fa48("70") ? false : stryMutAct_9fa48("69") ? true : (stryCov_9fa48("69", "70"), type)) whereClause.push(`type = '${type.replace(/'/g, "''")}'`);
    const where = (stryMutAct_9fa48("76") ? whereClause.length <= 0 : stryMutAct_9fa48("75") ? whereClause.length >= 0 : stryMutAct_9fa48("74") ? false : stryMutAct_9fa48("73") ? true : (stryCov_9fa48("73", "74", "75", "76"), whereClause.length > 0)) ? `WHERE ${whereClause.join(' AND ')}` : '';
    while (stryMutAct_9fa48("80") ? false : (stryCov_9fa48("80"), hasMore)) {
      if (stryMutAct_9fa48("81")) {
        {}
      } else {
        stryCov_9fa48("81");
        const rows = await query(`
      SELECT * FROM events_raw
      ${where}
      ORDER BY recorded_at DESC
      LIMIT ${pageSize} OFFSET ${offset}
    `);
        for (const row of rows) {
          if (stryMutAct_9fa48("83")) {
            {}
          } else {
            stryCov_9fa48("83");
            yield row;
          }
        }
        hasMore = stryMutAct_9fa48("86") ? rows.length !== pageSize : stryMutAct_9fa48("85") ? false : stryMutAct_9fa48("84") ? true : (stryCov_9fa48("84", "85", "86"), rows.length === pageSize);
        stryMutAct_9fa48("87") ? offset -= pageSize : (stryCov_9fa48("87"), offset += pageSize);
      }
    }
  }
}

/**
 * Update all aggregations incrementally
 */
export async function updateAllAggregations() {
  if (stryMutAct_9fa48("88")) {
    {}
  } else {
    stryCov_9fa48("88");
    const results = {};
    try {
      if (stryMutAct_9fa48("89")) {
        {}
      } else {
        stryCov_9fa48("89");
        results.eventTypeCounts = await updateEventTypeCounts();
        results.totals = await getTotalCounts();
        results.timeRange = await getTimeRange();
      }
    } catch (err) {
      if (stryMutAct_9fa48("90")) {
        {}
      } else {
        stryCov_9fa48("90");
        console.error('❌ Aggregation error:', err.message);
        results.error = err.message;
      }
    }
    return results;
  }
}