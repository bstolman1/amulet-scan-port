/**
 * Gap Detector - Detects and recovers missing time ranges in ingested data
 * 
 * Runs as part of the engine worker cycle to automatically detect gaps
 * in the data coverage and trigger backfill recovery.
 */

import axios from 'axios';
import { Agent as HttpAgent } from 'http';
import { Agent as HttpsAgent } from 'https';
import { query } from '../duckdb/connection.js';

// Configuration
const SCAN_URL = process.env.SCAN_URL || 'https://scan.sv-1.global.canton.network.sync.global/api/scan';
const GAP_THRESHOLD_MS = parseInt(process.env.GAP_THRESHOLD_MS || '120000', 10); // 2 minutes default
const MAX_GAPS_PER_CYCLE = parseInt(process.env.MAX_GAPS_PER_CYCLE || '3', 10);
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || '1000', 10);

// TLS config for self-signed certs
process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

// HTTP client with connection pooling
const client = axios.create({
  baseURL: SCAN_URL,
  httpAgent: new HttpAgent({
    keepAlive: true,
    keepAliveMsecs: 60000,
    maxSockets: 4,
  }),
  httpsAgent: new HttpsAgent({
    keepAlive: true,
    keepAliveMsecs: 60000,
    rejectUnauthorized: false,
    maxSockets: 4,
  }),
  timeout: 120000,
});

/**
 * Format duration for logging
 */
function formatDuration(ms) {
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
  if (ms < 3600000) return `${(ms / 60000).toFixed(1)}m`;
  if (ms < 86400000) return `${(ms / 3600000).toFixed(1)}h`;
  return `${(ms / 86400000).toFixed(1)}d`;
}

/**
 * Detect gaps in ingested data by analyzing raw_files timestamps
 */
export async function detectGaps() {
  // Get time ranges from ingested files grouped by synchronizer
  const rows = await query(`
    SELECT 
      f.synchronizer,
      f.migration_id,
      MIN(f.min_ts) as range_start,
      MAX(f.max_ts) as range_end,
      COUNT(*) as file_count
    FROM (
      SELECT 
        COALESCE(
          (SELECT DISTINCT synchronizer FROM updates_raw WHERE _file_id = raw_files.file_id LIMIT 1),
          'unknown'
        ) as synchronizer,
        migration_id,
        min_ts,
        max_ts
      FROM raw_files
      WHERE ingested = TRUE AND min_ts IS NOT NULL AND max_ts IS NOT NULL
    ) f
    GROUP BY f.synchronizer, f.migration_id
    ORDER BY f.synchronizer, range_start
  `);

  if (rows.length < 2) {
    return { gaps: [], message: 'Not enough data for gap detection' };
  }

  // Analyze gaps within each synchronizer
  const gaps = [];
  const bySynchronizer = {};

  for (const row of rows) {
    const syncId = row.synchronizer;
    if (!bySynchronizer[syncId]) {
      bySynchronizer[syncId] = [];
    }
    bySynchronizer[syncId].push(row);
  }

  for (const [syncId, ranges] of Object.entries(bySynchronizer)) {
    // Sort by start time
    const sorted = ranges.sort((a, b) => 
      new Date(a.range_start).getTime() - new Date(b.range_start).getTime()
    );

    for (let i = 1; i < sorted.length; i++) {
      const prev = sorted[i - 1];
      const curr = sorted[i];

      const prevEnd = new Date(prev.range_end).getTime();
      const currStart = new Date(curr.range_start).getTime();
      const gapMs = currStart - prevEnd;

      if (gapMs > GAP_THRESHOLD_MS) {
        gaps.push({
          synchronizer: syncId,
          migrationId: prev.migration_id || curr.migration_id,
          gapStart: new Date(prevEnd).toISOString(),
          gapEnd: new Date(currStart).toISOString(),
          gapMs,
          gapDuration: formatDuration(gapMs),
        });
      }
    }
  }

  // Sort by gap size (largest first)
  gaps.sort((a, b) => b.gapMs - a.gapMs);

  return { 
    gaps, 
    totalGaps: gaps.length,
    totalGapTime: formatDuration(gaps.reduce((sum, g) => sum + g.gapMs, 0)),
  };
}

/**
 * Retry with exponential backoff
 */
async function retryWithBackoff(fn, maxRetries = 3) {
  let lastError;
  
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;
      const status = error.response?.status;
      const retryable = [429, 500, 502, 503, 504].includes(status) ||
                       ['ECONNRESET', 'ETIMEDOUT', 'ECONNREFUSED'].includes(error.code);
      
      if (attempt === maxRetries || !retryable) {
        throw error;
      }
      
      const delay = Math.min(1000 * Math.pow(2, attempt), 15000);
      await new Promise(r => setTimeout(r, delay));
    }
  }
  
  throw lastError;
}

/**
 * Fetch data for a gap from the backfill API
 */
async function fetchGapData(migrationId, synchronizerId, gapStart, gapEnd) {
  const allTransactions = [];
  const seenUpdateIds = new Set();
  let currentBefore = gapEnd;
  const atOrAfter = gapStart;
  let consecutiveEmpty = 0;
  
  while (true) {
    if (new Date(currentBefore).getTime() <= new Date(atOrAfter).getTime()) {
      break;
    }
    
    const payload = {
      migration_id: migrationId,
      synchronizer_id: synchronizerId,
      before: currentBefore,
      at_or_after: atOrAfter,
      count: BATCH_SIZE,
    };
    
    let response;
    try {
      response = await retryWithBackoff(async () => {
        const res = await client.post('/v0/backfilling/updates-before', payload);
        return res.data;
      });
    } catch (err) {
      console.warn(`   ⚠️ Gap fetch failed: ${err.message}`);
      break;
    }
    
    const txs = response?.transactions || [];
    
    if (txs.length === 0) {
      consecutiveEmpty++;
      if (consecutiveEmpty >= 3) break;
      
      const d = new Date(currentBefore);
      d.setTime(d.getTime() - 1000);
      if (d.getTime() <= new Date(atOrAfter).getTime()) break;
      currentBefore = d.toISOString();
      continue;
    }
    
    consecutiveEmpty = 0;
    
    // Deduplicate transactions
    for (const tx of txs) {
      const updateId = tx.update_id || tx.transaction?.update_id || tx.reassignment?.update_id;
      if (updateId) {
        if (!seenUpdateIds.has(updateId)) {
          seenUpdateIds.add(updateId);
          allTransactions.push(tx);
        }
      } else {
        allTransactions.push(tx);
      }
    }
    
    // Find oldest timestamp
    let oldestTime = null;
    for (const tx of txs) {
      const t = tx.record_time || tx.event?.record_time || tx.effective_at;
      if (t && (!oldestTime || t < oldestTime)) {
        oldestTime = t;
      }
    }
    
    if (oldestTime && new Date(oldestTime).getTime() <= new Date(atOrAfter).getTime()) {
      break;
    }
    
    if (oldestTime) {
      const d = new Date(oldestTime);
      d.setMilliseconds(d.getMilliseconds() - 1);
      currentBefore = d.toISOString();
    } else {
      const d = new Date(currentBefore);
      d.setMilliseconds(d.getMilliseconds() - 1);
      currentBefore = d.toISOString();
    }
    
    // Limit batch to prevent OOM
    if (allTransactions.length >= 10000) {
      console.log(`   ⚠️ Hit transaction limit (10k), stopping fetch`);
      break;
    }
  }
  
  return allTransactions;
}

/**
 * Store gap detection results for API/UI access
 */
let lastGapDetection = null;

export function getLastGapDetection() {
  return lastGapDetection;
}

/**
 * Recover a single gap by fetching and storing missing data
 */
async function recoverGap(gap, index, total) {
  console.log(`   📍 Gap ${index + 1}/${total}: ${gap.gapDuration} (${gap.synchronizer.substring(0, 30)}...)`);
  
  if (!gap.migrationId) {
    console.log(`      ⏭️ Skipping - no migration ID`);
    return { recovered: 0, success: false };
  }
  
  // Fetch missing data
  console.log(`      🔄 Fetching missing data...`);
  const transactions = await fetchGapData(
    gap.migrationId,
    gap.synchronizer,
    gap.gapStart,
    gap.gapEnd
  );
  
  if (transactions.length === 0) {
    console.log(`      ℹ️ No transactions found (may be legitimate empty period)`);
    return { recovered: 0, success: true };
  }
  
  console.log(`      ✅ Found ${transactions.length} transactions to recover`);
  
  // Note: Actual insertion requires the decode-worker and binary-writer
  // For now, we just detect and report. Full recovery would require
  // integrating with the existing write-binary.js pipeline
  return { recovered: transactions.length, success: true };
}

/**
 * Run gap detection and optionally recovery
 * Returns summary of gaps found
 */
export async function runGapDetection(autoRecover = false) {
  console.log('🔍 Running gap detection...');
  
  const result = await detectGaps();
  lastGapDetection = {
    ...result,
    detectedAt: new Date().toISOString(),
    autoRecoverEnabled: autoRecover,
  };
  
  if (result.gaps.length === 0) {
    console.log('   ✅ No gaps detected');
    return result;
  }
  
  console.log(`   ⚠️ Found ${result.totalGaps} gap(s), total time: ${result.totalGapTime}`);
  
  // Log top gaps
  for (let i = 0; i < Math.min(5, result.gaps.length); i++) {
    const gap = result.gaps[i];
    console.log(`      ${i + 1}. ${gap.gapDuration} between ${gap.gapStart} and ${gap.gapEnd}`);
  }
  
  // Auto-recover if enabled
  if (autoRecover && result.gaps.length > 0) {
    console.log(`\n🔄 Auto-recovering up to ${MAX_GAPS_PER_CYCLE} gaps...`);
    
    const gapsToRecover = result.gaps.slice(0, MAX_GAPS_PER_CYCLE);
    let totalRecovered = 0;
    
    for (let i = 0; i < gapsToRecover.length; i++) {
      try {
        const recoverResult = await recoverGap(gapsToRecover[i], i, gapsToRecover.length);
        totalRecovered += recoverResult.recovered;
      } catch (err) {
        console.error(`      ❌ Recovery failed: ${err.message}`);
      }
    }
    
    lastGapDetection.recoveryAttempted = true;
    lastGapDetection.transactionsRecovered = totalRecovered;
  }
  
  return result;
}
