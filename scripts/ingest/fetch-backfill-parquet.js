#!/usr/bin/env node
/**
 * Canton Ledger Backfill Script - OPTIMIZED Parquet Version
 * 
 * Fetches historical ledger data using the backfilling API
 * and writes to partitioned parquet files.
 * 
 * Optimizations:
 * - Parallel API fetching (configurable concurrency)
 * - Prefetch queue for continuous data flow
 * - Minimal blocking between fetch and write
 * - Multithreaded decode via Piscina worker pool
 */

import dotenv from 'dotenv';
dotenv.config();

import axios from 'axios';
import { Agent as HttpAgent } from 'http';
import { Agent as HttpsAgent } from 'https';
import { existsSync, readFileSync, writeFileSync, mkdirSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import os from 'os';
import Piscina from 'piscina';
import { normalizeUpdate, normalizeEvent, getPartitionPath } from './parquet-schema.js';

// Use binary writer (Protobuf + ZSTD) instead of JSONL
import { bufferUpdates, bufferEvents, flushAll, getBufferStats, waitForWrites, purgeMigrationData, shutdown } from './write-binary.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// TLS config - must be set before any requests
process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

// Configuration - BALANCED DEFAULTS for stability
const SCAN_URL = process.env.SCAN_URL || 'https://scan.sv-1.global.canton.network.sync.global/api/scan';
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 1000; // API max is 1000
const BASE_DATA_DIR = process.env.DATA_DIR || join(__dirname, '../../data');
const CURSOR_DIR = process.env.CURSOR_DIR || join(BASE_DATA_DIR, 'cursors');
const PURGE_AFTER_MIGRATION = process.env.PURGE_AFTER_MIGRATION === 'true';
const FLUSH_EVERY_BATCHES = parseInt(process.env.FLUSH_EVERY_BATCHES) || 5;

// Sharding configuration
const SHARD_INDEX = parseInt(process.env.SHARD_INDEX) || 0;
const SHARD_TOTAL = parseInt(process.env.SHARD_TOTAL) || 1;
const TARGET_MIGRATION = process.env.TARGET_MIGRATION ? parseInt(process.env.TARGET_MIGRATION) : null;

// ==========================================
// AUTO-TUNING: PARALLEL FETCHES
// ==========================================
const BASE_PARALLEL_FETCHES = parseInt(process.env.PARALLEL_FETCHES) || 2;
const MIN_PARALLEL_FETCHES = parseInt(process.env.MIN_PARALLEL_FETCHES) || 1;
const MAX_PARALLEL_FETCHES = parseInt(process.env.MAX_PARALLEL_FETCHES) || 6;

let dynamicParallelFetches = Math.min(
  Math.max(BASE_PARALLEL_FETCHES, MIN_PARALLEL_FETCHES),
  MAX_PARALLEL_FETCHES
);

const FETCH_TUNE_WINDOW_MS = 30_000;
let fetchStats = {
  windowStart: Date.now(),
  successCount: 0,
  retry503Count: 0,
};

// ==========================================
// AUTO-TUNING: DECODE WORKERS
// ==========================================
const cpuCount = os.cpus()?.length || 4;
const BASE_DECODE_WORKERS = parseInt(process.env.DECODE_WORKERS) || Math.floor(cpuCount / 2);
const MIN_DECODE_WORKERS = parseInt(process.env.MIN_DECODE_WORKERS) || 4;
const MAX_DECODE_WORKERS = parseInt(process.env.MAX_DECODE_WORKERS) || 16;

let dynamicDecodeWorkers = Math.min(
  Math.max(BASE_DECODE_WORKERS, MIN_DECODE_WORKERS),
  MAX_DECODE_WORKERS
);

const DECODE_TUNE_WINDOW_MS = 30_000;
let decodeStats = {
  windowStart: Date.now(),
  startQueued: 0,
  endQueued: 0,
  decoded: 0,
};

// ==========================================
// DECODE WORKER POOL (Dynamic)
// ==========================================
let decodePool = null;

function createDecodePool(maxThreads) {
  return new Piscina({
    filename: new URL('./decode-worker.js', import.meta.url).href,
    minThreads: MIN_DECODE_WORKERS,
    maxThreads: maxThreads,
  });
}

function getDecodePool() {
  if (!decodePool) {
    decodePool = createDecodePool(dynamicDecodeWorkers);
  }
  return decodePool;
}

// ==========================================
// HTTP CLIENT (uses MAX for socket pool)
// ==========================================
const client = axios.create({
  baseURL: SCAN_URL,
  httpAgent: new HttpAgent({
    keepAlive: true,
    keepAliveMsecs: 60000,
    maxSockets: MAX_PARALLEL_FETCHES * 4,
  }),
  httpsAgent: new HttpsAgent({
    keepAlive: true,
    keepAliveMsecs: 60000,
    rejectUnauthorized: false,
    maxSockets: MAX_PARALLEL_FETCHES * 4,
  }),
  timeout: 180000,
});

/**
 * Sleep helper
 */
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Retry with exponential backoff + fetch stats tracking
 */
async function retryWithBackoff(fn, options = {}) {
  const {
    maxRetries = 5,
    baseDelay = 1000,
    maxDelay = 30000,
    shouldRetry = (error) => {
      const retryableCodes = ['ECONNRESET', 'ETIMEDOUT', 'ECONNREFUSED', 'EPIPE'];
      const retryableStatuses = [429, 500, 502, 503, 504];
      
      if (error.code && retryableCodes.includes(error.code)) return true;
      if (error.response?.status && retryableStatuses.includes(error.response.status)) return true;
      return false;
    }
  } = options;

  let lastError;
  
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      const result = await fn();
      // ✅ Count successful API calls
      fetchStats.successCount++;
      return result;
    } catch (error) {
      lastError = error;
      const status = error.response?.status;

      // ❌ Count 503/429 as "rate-limit" signals for tuning
      if (status === 503 || status === 429 || error.code === 'ERR_BAD_RESPONSE') {
        fetchStats.retry503Count++;
      }
      
      if (attempt === maxRetries || !shouldRetry(error)) {
        throw error;
      }

      const exponentialDelay = Math.min(baseDelay * Math.pow(2, attempt), maxDelay);
      const jitter = Math.random() * exponentialDelay * 0.3;
      const delay = exponentialDelay + jitter;
      
      console.log(
        `   ⏳ Retry attempt ${attempt + 1}/${maxRetries} ` +
        `after ${Math.round(delay)}ms (error: ${error.code || status || error.message})`
      );
      await sleep(delay);
    }
  }
  
  throw lastError;
}

/**
 * Auto-tune parallel fetches based on 503 rate
 */
function maybeTuneParallelFetches(shardLabel = '') {
  const now = Date.now();
  const elapsed = now - fetchStats.windowStart;
  if (elapsed < FETCH_TUNE_WINDOW_MS) return;

  const { successCount, retry503Count } = fetchStats;
  const total = successCount + retry503Count;

  if (total === 0) {
    fetchStats = { windowStart: now, successCount: 0, retry503Count: 0 };
    return;
  }

  const errorRate = retry503Count / total;

  // Too many 503s → scale down parallelism
  if (retry503Count >= 5 && errorRate > 0.10 && dynamicParallelFetches > MIN_PARALLEL_FETCHES) {
    const old = dynamicParallelFetches;
    dynamicParallelFetches = Math.max(MIN_PARALLEL_FETCHES, dynamicParallelFetches - 1);
    console.log(
      `   🔧 Auto-tune${shardLabel}: high 503 rate (${retry503Count}/${total}, ${(errorRate * 100).toFixed(1)}%) → PARALLEL_FETCHES ${old} → ${dynamicParallelFetches}`
    );
  }
  // No 503s and plenty of successes → cautiously scale up
  else if (retry503Count === 0 && successCount >= 20 && dynamicParallelFetches < MAX_PARALLEL_FETCHES) {
    const old = dynamicParallelFetches;
    dynamicParallelFetches = Math.min(MAX_PARALLEL_FETCHES, dynamicParallelFetches + 1);
    console.log(
      `   🔧 Auto-tune${shardLabel}: stable (${successCount} ok, 0 503s) → PARALLEL_FETCHES ${old} → ${dynamicParallelFetches}`
    );
  }

  fetchStats = { windowStart: now, successCount: 0, retry503Count: 0 };
}

/**
 * Auto-tune decode workers based on queue depth
 */
async function maybeTuneDecodeWorkers(shardLabel = '') {
  const now = Date.now();
  const elapsed = now - decodeStats.windowStart;
  if (elapsed < DECODE_TUNE_WINDOW_MS) return;

  const { startQueued, endQueued, decoded } = decodeStats;
  const queueGrowth = endQueued - startQueued;

  // Rule 1 — queue is growing → add workers
  if (queueGrowth > 0 && dynamicDecodeWorkers < MAX_DECODE_WORKERS) {
    const old = dynamicDecodeWorkers;
    dynamicDecodeWorkers++;
    console.log(`   🔧 Auto-tune${shardLabel}: decode queue growing (+${queueGrowth}) → workers ${old} → ${dynamicDecodeWorkers}`);

    // Recreate pool with new size
    if (decodePool) {
      await decodePool.destroy();
    }
    decodePool = createDecodePool(dynamicDecodeWorkers);
  }
  // Rule 2 — queue empty/shrinking → reduce workers
  else if (queueGrowth <= 0 && decoded > 0 && dynamicDecodeWorkers > MIN_DECODE_WORKERS) {
    const old = dynamicDecodeWorkers;
    dynamicDecodeWorkers--;
    console.log(`   🔧 Auto-tune${shardLabel}: decode queue stable/shrinking → workers ${old} → ${dynamicDecodeWorkers}`);

    if (decodePool) {
      await decodePool.destroy();
    }
    decodePool = createDecodePool(dynamicDecodeWorkers);
  }

  // Reset stats
  decodeStats = {
    windowStart: now,
    startQueued: endQueued,
    endQueued: 0,
    decoded: 0,
  };
}

/**
 * Load cursor from file (shard-aware)
 */
function loadCursor(migrationId, synchronizerId, shardIndex = null) {
  const shardSuffix = shardIndex !== null ? `-shard${shardIndex}` : '';
  const cursorFile = join(CURSOR_DIR, `cursor-${migrationId}-${sanitize(synchronizerId)}${shardSuffix}.json`);
  
  if (existsSync(cursorFile)) {
    return JSON.parse(readFileSync(cursorFile, 'utf8'));
  }
  
  return null;
}

/**
 * Save cursor to file (shard-aware)
 */
function saveCursor(migrationId, synchronizerId, cursor, minTime, maxTime, shardIndex = null) {
  try {
    mkdirSync(CURSOR_DIR, { recursive: true });
    
    const shardSuffix = shardIndex !== null ? `-shard${shardIndex}` : '';
    const cursorFile = join(CURSOR_DIR, `cursor-${migrationId}-${sanitize(synchronizerId)}${shardSuffix}.json`);
    
    const cursorData = {
      ...cursor,
      migration_id: migrationId,
      synchronizer_id: synchronizerId,
      shard_index: shardIndex,
      shard_total: shardIndex !== null ? SHARD_TOTAL : null,
      id: `cursor-${migrationId}-${sanitize(synchronizerId)}${shardSuffix}`,
      cursor_name: `migration-${migrationId}-${synchronizerId.substring(0, 20)}${shardSuffix}`,
      min_time: minTime || cursor.min_time,
      max_time: maxTime || cursor.max_time,
      last_processed_round: cursor.last_processed_round || 0,
    };
    writeFileSync(cursorFile, JSON.stringify(cursorData, null, 2));
  } catch (err) {
    console.error(`   [saveCursor] ❌ FAILED: ${err.message}`);
  }
}

/**
 * Sanitize string for filename
 */
function sanitize(str) {
  return str.replace(/[^a-zA-Z0-9-_]/g, '_').substring(0, 50);
}

/**
 * Detect all available migrations via POST /v0/backfilling/migration-info
 * Iterates migration_id from 1 upward until 404 (not found)
 */
async function detectMigrations() {
  console.log("🔎 Detecting available migrations via /v0/backfilling/migration-info");

  const migrations = [];
  let id = 1;

  while (true) {
    try {
      const res = await client.post("/v0/backfilling/migration-info", {
        migration_id: id,
      });

      if (res.data?.record_time_range) {
        const ranges = res.data.record_time_range || [];
        const minTime = ranges[0]?.min || 'unknown';
        const maxTime = ranges[0]?.max || 'unknown';
        migrations.push(id);
        console.log(`  • migration_id=${id} ranges=${ranges.length} (${minTime} to ${maxTime})`);
        id++;
      } else {
        break;
      }
    } catch (err) {
      if (err.response && err.response.status === 404) {
        // No more migrations
        break;
      }
      console.error(`❌ Error probing migration_id=${id}:`, err.response?.status, err.message);
      break;
    }
  }

  console.log(`✅ Found migrations: ${migrations.join(", ")}`);
  return migrations;
}

/**
 * Get migration info via POST /v0/backfilling/migration-info
 */
async function getMigrationInfo(migrationId) {
  try {
    const res = await client.post("/v0/backfilling/migration-info", {
      migration_id: migrationId,
    });
    return res.data;
  } catch (err) {
    if (err.response && err.response.status === 404) {
      console.log(`ℹ️  No backfilling info for migration_id=${migrationId} (404)`);
      return null;
    }
    throw err;
  }
}

/**
 * Fetch backfill data before a timestamp (single request)
 */
async function fetchBackfillBefore(migrationId, synchronizerId, before, atOrAfter) {
  const payload = {
    migration_id: migrationId,
    synchronizer_id: synchronizerId,
    before,
    count: BATCH_SIZE,
  };
  
  if (atOrAfter) {
    payload.at_or_after = atOrAfter;
  }
  
  return await retryWithBackoff(async () => {
    const response = await client.post('/v0/backfilling/updates-before', payload);
    return response.data;
  });
}

/**
 * Get event time from transaction or reassignment
 */
function getEventTime(txOrReassign) {
  return (
    txOrReassign.record_time ||
    txOrReassign.event?.record_time ||
    txOrReassign.effective_at
  );
}

/**
 * Process backfill items using multithreaded decode (Piscina)
 * Falls back to single-threaded if pool unavailable
 * Tracks decode stats for auto-tuning
 */
async function processBackfillItems(transactions, migrationId) {
  const pool = getDecodePool();
  
  // Track queue depth at start for auto-tuning
  if (decodeStats.startQueued === 0) {
    decodeStats.startQueued = pool.queueSize || 0;
  }
  
  // Submit all transactions to worker pool in parallel
  const tasks = transactions.map((tx) => 
    pool.run({ tx, migrationId }).then(result => {
      decodeStats.decoded++;
      return result;
    }).catch(err => {
      console.warn(`   ⚠️ Worker decode failed, using main thread: ${err.message}`);
      decodeStats.decoded++;
      return decodeInMainThread(tx, migrationId);
    })
  );
  
  const results = await Promise.all(tasks);

  // Track queue depth at end for auto-tuning
  decodeStats.endQueued = pool.queueSize || 0;

  const updates = [];
  const events = [];

  for (const r of results) {
    if (!r) continue;
    if (r.update) updates.push(r.update);
    if (Array.isArray(r.events) && r.events.length > 0) {
      events.push(...r.events);
    }
  }

  await bufferUpdates(updates);
  await bufferEvents(events);

  return { updates: updates.length, events: events.length };
}

/**
 * Fallback: decode in main thread (same logic as decode-worker.js)
 */
function decodeInMainThread(tx, migrationId) {
  const isReassignment = !!tx.event;
  const update = normalizeUpdate(tx);
  update.migration_id = migrationId;

  const events = [];
  const txData = tx.transaction || tx.reassignment || tx;

  const updateInfo = {
    record_time: txData.record_time,
    effective_at: txData.effective_at,
    synchronizer_id: txData.synchronizer_id,
    source: txData.source || null,
    target: txData.target || null,
    unassign_id: txData.unassign_id || null,
    submitter: txData.submitter || null,
    counter: txData.counter ?? null,
  };

  if (isReassignment) {
    const ce = tx.event?.created_event;
    const ae = tx.event?.archived_event;

    if (ce) {
      const ev = normalizeEvent(ce, update.update_id, migrationId, tx, updateInfo);
      ev.event_type = 'reassign_create';
      events.push(ev);
    }
    if (ae) {
      const ev = normalizeEvent(ae, update.update_id, migrationId, tx, updateInfo);
      ev.event_type = 'reassign_archive';
      events.push(ev);
    }
  } else {
    const eventsById = txData.events_by_id || tx.events_by_id || {};
    for (const [eventId, rawEvent] of Object.entries(eventsById)) {
      const ev = normalizeEvent(rawEvent, update.update_id, migrationId, rawEvent, updateInfo);
      ev.event_id = eventId;
      events.push(ev);
    }
  }

  return { update, events };
}

/**
 * Parallel fetch with sliding window - fetches multiple pages concurrently
 * Maintains full parallelism by refilling slots as requests complete
 * Now accepts dynamic concurrency for auto-tuning
 * 
 * OPTIMIZED: Better empty gap handling to avoid slow crawling
 */
async function parallelFetchBatch(migrationId, synchronizerId, startBefore, atOrAfter, maxBatches, concurrency) {
  const results = [];
  const pending = new Map();
  const seenUpdateIds = new Set(); // Track by update_id to avoid duplicates
  let nextBefore = startBefore;
  let fetched = 0;
  let reachedEnd = false;
  let consecutiveEmpty = 0; // Track consecutive empty responses
  const MAX_CONSECUTIVE_EMPTY = 10; // Safety limit
  
  const startFetch = (before) => {
    if (reachedEnd) return false;
    
    const fetchId = fetched++;
    pending.set(fetchId, {
      before,
      promise: fetchBackfillBefore(migrationId, synchronizerId, before, atOrAfter)
        .then(data => ({ fetchId, before, data, error: null }))
        .catch(error => ({ fetchId, before, data: null, error }))
    });
    return true;
  };
  
  // Fill initial parallel slots
  startFetch(nextBefore);
  
  while (pending.size > 0 && !reachedEnd) {
    const completed = await Promise.race(
      Array.from(pending.values()).map(p => p.promise)
    );
    
    pending.delete(completed.fetchId);
    
    if (completed.error) {
      throw completed.error;
    }
    
    const txs = completed.data?.transactions || [];
    
    if (txs.length === 0) {
      consecutiveEmpty++;
      
      if (completed.before <= atOrAfter) {
        reachedEnd = true;
        break;
      }
      
      // Progressive backoff for empty gaps - jump larger chunks
      // This prevents slow crawling through empty time ranges
      const skipMs = Math.min(
        1000 * Math.pow(2, consecutiveEmpty), // Exponential: 1s, 2s, 4s, 8s...
        3600000 // Cap at 1 hour jumps
      );
      
      const d = new Date(completed.before);
      d.setTime(d.getTime() - skipMs);
      
      // Don't go past the lower bound
      if (d.getTime() <= new Date(atOrAfter).getTime()) {
        reachedEnd = true;
        break;
      }
      
      nextBefore = d.toISOString();
      
      if (consecutiveEmpty >= MAX_CONSECUTIVE_EMPTY) {
        console.log(`   ⏭️ Skipping ${consecutiveEmpty} empty responses, jumping ${skipMs}ms`);
        reachedEnd = true;
        break;
      }
      
      continue;
    }
    
    // Reset empty counter on success
    consecutiveEmpty = 0;
    
    // Deduplicate transactions by update_id
    const uniqueTxs = [];
    for (const tx of txs) {
      const updateId = tx.update_id || tx.transaction?.update_id || tx.reassignment?.update_id;
      if (updateId && !seenUpdateIds.has(updateId)) {
        seenUpdateIds.add(updateId);
        uniqueTxs.push(tx);
      } else if (!updateId) {
        // No update_id, include anyway (shouldn't happen but be safe)
        uniqueTxs.push(tx);
      }
    }
    
    if (uniqueTxs.length > 0) {
      results.push({ transactions: uniqueTxs, before: completed.before });
    }
    
    let earliest = null;
    for (const tx of uniqueTxs) {
      const t = getEventTime(tx);
      if (t && (!earliest || t < earliest)) earliest = t;
    }
    
    if (earliest && earliest <= atOrAfter) {
      reachedEnd = true;
      break;
    }
    
    if (earliest && earliest < nextBefore) {
      nextBefore = earliest;
    }
    
    // Refill slots based on *current* dynamic concurrency
    while (pending.size < concurrency && fetched < maxBatches && !reachedEnd) {
      if (!startFetch(nextBefore)) break;
      // Small decrement to get next page
      const d = new Date(nextBefore);
      d.setMilliseconds(d.getMilliseconds() - 1);
      nextBefore = d.toISOString();
    }
  }
  
  return { results, reachedEnd };
}

/**
 * Backfill a single synchronizer with parallel fetching (shard-aware)
 * Uses auto-tuning for parallel fetches and decode workers
 */
async function backfillSynchronizer(migrationId, synchronizerId, minTime, maxTime, shardIndex = null) {
  const shardLabel = shardIndex !== null ? ` [shard ${shardIndex}/${SHARD_TOTAL}]` : '';
  console.log(`\n📍 Backfilling migration ${migrationId}, synchronizer ${synchronizerId.substring(0, 30)}...${shardLabel}`);
  console.log(`   Range: ${minTime} to ${maxTime}`);
  console.log(`   Parallel fetches (auto-tuned): ${dynamicParallelFetches} (min=${MIN_PARALLEL_FETCHES}, max=${MAX_PARALLEL_FETCHES})`);
  console.log(`   Decode workers (auto-tuned): ${dynamicDecodeWorkers} (min=${MIN_DECODE_WORKERS}, max=${MAX_DECODE_WORKERS})`);
  
  // Load existing cursor (shard-aware)
  let cursor = loadCursor(migrationId, synchronizerId, shardIndex);
  let before = cursor?.last_before || maxTime;
  const atOrAfter = minTime;
  
  let totalUpdates = 0;
  let totalEvents = 0;
  let batchCount = 0;
  const startTime = Date.now();
  
  // Save initial cursor
  saveCursor(migrationId, synchronizerId, {
    last_before: before,
    total_updates: 0,
    total_events: 0,
    started_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
  }, minTime, maxTime, shardIndex);
  
  while (true) {
    try {
      // Use current dynamic concurrency values
      const localParallel = dynamicParallelFetches;
      
      const { results, reachedEnd } = await parallelFetchBatch(
        migrationId, synchronizerId, before, atOrAfter, 
        localParallel * 2,  // maxBatches per cycle
        localParallel       // actual concurrency
      );
      
      if (results.length === 0) {
        console.log(`   ✅ No more transactions. Marking complete.${shardLabel}`);
        break;
      }
      
      // Process all fetched batches
      let batchUpdates = 0;
      let batchEvents = 0;
      let earliestTime = before;
      
      for (const { transactions } of results) {
        const { updates, events } = await processBackfillItems(transactions, migrationId);
        batchUpdates += updates;
        batchEvents += events;
        
        // Track earliest timestamp
        for (const tx of transactions) {
          const t = getEventTime(tx);
          if (t && t < earliestTime) earliestTime = t;
        }
      }
      
      totalUpdates += batchUpdates;
      totalEvents += batchEvents;
      batchCount++;
      
      // Update cursor position
      before = earliestTime;
      
      // Calculate throughput
      const elapsed = (Date.now() - startTime) / 1000;
      const throughput = Math.round(totalUpdates / elapsed);
      
      // Save cursor and log progress
      saveCursor(migrationId, synchronizerId, {
        last_before: before,
        total_updates: totalUpdates,
        total_events: totalEvents,
        updated_at: new Date().toISOString(),
      }, minTime, maxTime, shardIndex);
      
      const stats = getBufferStats();
      const writeSpeed = String(stats.mbPerSec ?? '0.00');
      const compression = String(stats.compressionRatio ?? '---');
      const queuedJobs = Number(stats.queuedJobs ?? 0);
      const activeWorkers = Number(stats.activeWorkers ?? 0);
      
      // Force flush periodically to prevent memory buildup
      if (batchCount % FLUSH_EVERY_BATCHES === 0) {
        await flushAll();
      }
      
      // Auto-tune after processing this wave
      maybeTuneParallelFetches(shardLabel);
      await maybeTuneDecodeWorkers(shardLabel);
      
      // Main progress line with current tuning values
      console.log(`   📦${shardLabel} Batch ${batchCount}: +${batchUpdates} upd, +${batchEvents} evt | Total: ${totalUpdates.toLocaleString()} @ ${throughput}/s | F:${dynamicParallelFetches} D:${dynamicDecodeWorkers} | Q: ${queuedJobs}/${activeWorkers}`);
      
      if (reachedEnd || earliestTime <= atOrAfter) {
        console.log(`   ✅ Reached lower bound. Complete.${shardLabel}`);
        break;
      }
      
    } catch (err) {
      const status = err.response?.status;
      const msg = err.response?.data?.error || err.message;
      console.error(`   ❌ Error at batch ${batchCount} (status ${status || "n/a"}): ${msg}${shardLabel}`);
      
      // Save cursor and retry for transient errors
      if ([429, 500, 502, 503, 504].includes(status)) {
        console.log(`   ⏳ Transient error, backing off...${shardLabel}`);
        saveCursor(migrationId, synchronizerId, {
          last_before: before,
          total_updates: totalUpdates,
          total_events: totalEvents,
          error: msg,
          updated_at: new Date().toISOString(),
        }, minTime, maxTime, shardIndex);
        await sleep(5000);
        continue;
      }
      
      throw err;
    }
  }
  
  // Flush remaining data
  await flushAll();
  
  // Mark as complete
  saveCursor(migrationId, synchronizerId, {
    last_before: before,
    total_updates: totalUpdates,
    total_events: totalEvents,
    complete: true,
    updated_at: new Date().toISOString(),
  }, minTime, maxTime, shardIndex);
  
  const totalTime = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`   ⏱️ Completed in ${totalTime}s (${Math.round(totalUpdates / parseFloat(totalTime))}/s avg)${shardLabel}`);
  
  return { updates: totalUpdates, events: totalEvents };
}

/**
 * Calculate time slice for a shard
 * Divides the time range into equal parts
 */
function calculateShardTimeRange(minTime, maxTime, shardIndex, shardTotal) {
  const minMs = new Date(minTime).getTime();
  const maxMs = new Date(maxTime).getTime();
  const rangeMs = maxMs - minMs;
  const sliceMs = rangeMs / shardTotal;
  
  // Shards work backwards in time (maxTime to minTime)
  // Shard 0 gets the most recent slice, shard N-1 gets the oldest
  const shardMaxMs = maxMs - (shardIndex * sliceMs);
  const shardMinMs = maxMs - ((shardIndex + 1) * sliceMs);
  
  return {
    minTime: new Date(shardMinMs).toISOString(),
    maxTime: new Date(shardMaxMs).toISOString(),
  };
}

/**
 * Main backfill function (shard-aware)
 */
async function runBackfill() {
  const isSharded = SHARD_TOTAL > 1;
  const shardLabel = isSharded ? ` [SHARD ${SHARD_INDEX}/${SHARD_TOTAL}]` : '';
  
  console.log("\n" + "=".repeat(80));
  console.log(`🚀 Starting Canton ledger backfill (Auto-Tuning Mode)${shardLabel}`);
  console.log("   SCAN_URL:", SCAN_URL);
  console.log("   BATCH_SIZE:", BATCH_SIZE);
  console.log("   PARALLEL_FETCHES:", `${dynamicParallelFetches} (auto-tuning: ${MIN_PARALLEL_FETCHES}-${MAX_PARALLEL_FETCHES})`);
  console.log("   DECODE_WORKERS:", `${dynamicDecodeWorkers} (auto-tuning: ${MIN_DECODE_WORKERS}-${MAX_DECODE_WORKERS})`);
  console.log("   PURGE_AFTER_MIGRATION:", PURGE_AFTER_MIGRATION);
  if (isSharded) {
    console.log(`   SHARDING: Shard ${SHARD_INDEX} of ${SHARD_TOTAL} (0-indexed)`);
  }
  if (TARGET_MIGRATION) {
    console.log(`   TARGET_MIGRATION: ${TARGET_MIGRATION} only`);
  }
  console.log("   Processing: Migrations sequentially (1 → 2 → 3...)");
  console.log("   CURSOR_DIR:", CURSOR_DIR);
  console.log("=".repeat(80));
  
  // Ensure cursor directory exists
  mkdirSync(CURSOR_DIR, { recursive: true });
  
  // Detect migrations
  let migrations = await detectMigrations();
  
  // Filter to target migration if specified
  if (TARGET_MIGRATION) {
    migrations = migrations.filter(id => id === TARGET_MIGRATION);
    if (!migrations.length) {
      console.log(`⚠️ Target migration ${TARGET_MIGRATION} not found. Exiting.`);
      return;
    }
  }
  
  if (!migrations.length) {
    console.log("⚠️ No migrations found. Exiting.");
    return;
  }
  
  let grandTotalUpdates = 0;
  let grandTotalEvents = 0;
  const grandStartTime = Date.now();
  
  for (const migrationId of migrations) {
    console.log(`\n${"─".repeat(80)}`);
    console.log(`📘 Migration ${migrationId}: fetching backfilling metadata${shardLabel}`);
    console.log(`${"─".repeat(80)}`);
    
    const info = await getMigrationInfo(migrationId);
    
    if (!info) {
      console.log("   ℹ️  No backfilling info; skipping this migration.");
      continue;
    }
    
    const ranges = info.record_time_range || [];
    
    if (!ranges.length) {
      console.log("   ℹ️  No synchronizer ranges; skipping.");
      continue;
    }
    
    console.log(`   Found ${ranges.length} synchronizer ranges for migration ${migrationId}`);
    
    for (const range of ranges) {
      const synchronizerId = range.synchronizer_id;
      let minTime = range.min;
      let maxTime = range.max;
      
      // Apply sharding if enabled
      if (isSharded) {
        const shardRange = calculateShardTimeRange(minTime, maxTime, SHARD_INDEX, SHARD_TOTAL);
        minTime = shardRange.minTime;
        maxTime = shardRange.maxTime;
        console.log(`   🔀 Shard ${SHARD_INDEX}: time slice ${minTime} to ${maxTime}`);
      }
      
      // Check if already complete (shard-aware)
      const cursor = loadCursor(migrationId, synchronizerId, isSharded ? SHARD_INDEX : null);
      if (cursor?.complete) {
        console.log(`   ⏭️ Skipping ${synchronizerId.substring(0, 30)}... (already complete)${shardLabel}`);
        continue;
      }
      
      const { updates, events } = await backfillSynchronizer(
        migrationId, synchronizerId, minTime, maxTime, 
        isSharded ? SHARD_INDEX : null
      );
      grandTotalUpdates += updates;
      grandTotalEvents += events;
    }
    
    console.log(`✅ Completed migration ${migrationId}${shardLabel}`);
    
    // Purge migration data to free disk space before next migration (only if not sharded)
    if (PURGE_AFTER_MIGRATION && !isSharded) {
      console.log(`\n🗑️ Purging migration ${migrationId} data to free disk space...`);
      await waitForWrites(); // Ensure all writes are complete
      purgeMigrationData(migrationId);
    }
  }
  
  const grandTotalTime = ((Date.now() - grandStartTime) / 1000).toFixed(1);
  
  console.log(`\n${"═".repeat(80)}`);
  console.log(`🎉 Backfill complete!`);
  console.log(`   Total updates: ${grandTotalUpdates.toLocaleString()}`);
  console.log(`   Total events: ${grandTotalEvents.toLocaleString()}`);
  console.log(`   Total time: ${grandTotalTime}s`);
  console.log(`   Average throughput: ${Math.round(grandTotalUpdates / parseFloat(grandTotalTime))}/s`);
  console.log(`${"═".repeat(80)}\n`);
  
  return { success: true, totalUpdates: grandTotalUpdates, totalEvents: grandTotalEvents };
}

/**
 * Start live updates ingestion after backfill completes
 */
async function startLiveUpdates() {
  const { spawn } = await import('child_process');
  const liveUpdatesScript = join(__dirname, 'fetch-updates-parquet.js');
  
  console.log(`\n${"═".repeat(80)}`);
  console.log(`🔄 Starting live updates ingestion...`);
  console.log(`   Script: ${liveUpdatesScript}`);
  console.log(`${"═".repeat(80)}\n`);
  
  // Spawn the live updates process, inheriting stdio so logs are visible
  const child = spawn('node', [liveUpdatesScript], {
    stdio: 'inherit',
    cwd: __dirname,
    env: process.env
  });
  
  child.on('error', (err) => {
    console.error('❌ Failed to start live updates:', err.message);
    process.exit(1);
  });
  
  child.on('exit', (code) => {
    console.log(`Live updates process exited with code ${code}`);
    process.exit(code || 0);
  });
}

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('\n🛑 Shutting down... waiting for writes to complete');
  await flushAll();
  await waitForWrites();
  await shutdown();
  if (decodePool) {
    await decodePool.destroy();
  }
  console.log('✅ All writes complete');
  process.exit(0);
});

// Run backfill, then start live updates
runBackfill()
  .then(async (result) => {
    if (result?.success) {
      // Small delay to ensure all file handles are released
      await new Promise(resolve => setTimeout(resolve, 1000));
      await startLiveUpdates();
    }
  })
  .catch(async err => {
    console.error('\n❌ FATAL:', err.message);
    console.error(err.stack);
    await flushAll();
    await waitForWrites();
    await shutdown();
    if (decodePool) {
      await decodePool.destroy();
    }
    process.exit(1);
  });
