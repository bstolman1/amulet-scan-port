#!/usr/bin/env node
/**
 * Canton Ledger Backfill Script - Direct Parquet Version
 * 
 * Fetches historical ledger data using the backfilling API
 * and writes directly to Parquet files (default) or binary (--keep-raw).
 * 
 * CRITICAL FIXES APPLIED:
 * 1. Explicit fetch result states (SUCCESS_DATA, SUCCESS_EMPTY, FAILURE)
 *    - Network errors now FAIL HARD instead of silently continuing
 *    - No more "0 updates but success" on transient failures
 * 
 * 2. Atomic cursor transactions
 *    - Cursor only advances AFTER data is confirmed on disk
 *    - Uses write-to-temp-then-rename pattern for crash safety
 *    - Recovery from partial writes on restart
 * 
 * Usage:
 *   node fetch-backfill.js              # Writes directly to Parquet (default)
 *   node fetch-backfill.js --keep-raw   # Also writes to .pb.zst files
 * 
 * Optimizations:
 * - Parallel API fetching (configurable concurrency)
 * - Prefetch queue for continuous data flow
 * - Minimal blocking between fetch and write
 * - Multithreaded decode via Piscina worker pool
 */

// CRITICAL: Load .env BEFORE any other imports that depend on env vars
// Note: ESM hoists static imports, so we use a sync approach here
// and rely on write-parquet.js using dynamic env checks, not module-level constants
import dotenv from 'dotenv';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

// Get the directory of this script to find .env reliably
const __filename_early = fileURLToPath(import.meta.url);
const __dirname_early = dirname(__filename_early);
dotenv.config({ path: join(__dirname_early, '.env') });

import axios from 'axios';
import { Agent as HttpAgent } from 'http';
import { Agent as HttpsAgent } from 'https';
import { existsSync, readFileSync, writeFileSync, mkdirSync } from 'fs';

import v8 from 'v8';
// Piscina removed: main-thread decode is faster (structured clone overhead > normalization cost)
import { normalizeUpdate, normalizeEvent, getPartitionPath } from './data-schema.js';

// Parse command line arguments
const args = process.argv.slice(2);
const KEEP_RAW = args.includes('--keep-raw') || args.includes('--raw');
const RAW_ONLY = args.includes('--raw-only') || args.includes('--legacy');
const USE_PARQUET = !RAW_ONLY;
const USE_BINARY = KEEP_RAW || RAW_ONLY;

// Use Parquet writer by default, binary writer only if --keep-raw or --raw-only
import * as parquetWriter from './write-parquet.js';
import * as binaryWriter from './write-binary.js';

// CRITICAL FIX #1: Import explicit fetch result types
import {
  FetchResultType,
  successData,
  successEmpty,
  failure,
  isRetryableError,
  retryFetch,
  assertSuccess,
} from './fetch-result.js';

// CRITICAL FIX #2: Import atomic cursor operations
import { AtomicCursor, loadCursorLegacy, isCursorComplete } from './atomic-cursor.js';

// Structured JSON logging for long run debugging
import {
  log,
  logBatch,
  logCursor,
  logError,
  logFatal,
  logTune,
  logMetrics,
  logMigration,
  logSynchronizer,
  logSummary,
} from './structured-logger.js';

// Unified writer functions that delegate to appropriate writer(s)
async function bufferUpdates(updates) {
  if (USE_BINARY) {
    await binaryWriter.bufferUpdates(updates);
  }
  if (USE_PARQUET) {
    return parquetWriter.bufferUpdates(updates);
  }
}

async function bufferEvents(events) {
  if (USE_BINARY) {
    await binaryWriter.bufferEvents(events);
  }
  if (USE_PARQUET) {
    return parquetWriter.bufferEvents(events);
  }
}

async function flushAll() {
  const results = [];
  if (USE_BINARY) {
    const binaryResults = await binaryWriter.flushAll();
    results.push(...binaryResults);
  }
  if (USE_PARQUET) {
    const parquetResults = await parquetWriter.flushAll();
    results.push(...parquetResults);
  }
  return results;
}

function getBufferStats() {
  // Return parquet stats by default, include binary if using it
  const stats = USE_PARQUET ? parquetWriter.getBufferStats() : { updates: 0, events: 0, pendingWrites: 0 };
  if (USE_BINARY) {
    const binaryStats = binaryWriter.getBufferStats();
    stats.binaryPendingWrites = binaryStats.pendingWrites;
    stats.binaryQueuedJobs = binaryStats.queuedJobs;
  }
  return stats;
}

async function waitForWrites() {
  if (USE_BINARY) {
    await binaryWriter.waitForWrites();
  }
  if (USE_PARQUET) {
    await parquetWriter.waitForWrites();
  }
}

async function shutdown() {
  if (USE_BINARY) {
    await binaryWriter.shutdown();
  }
  if (USE_PARQUET) {
    await parquetWriter.shutdown();
  }
}

// Import bulletproof components for zero data loss
import {
  IntegrityCursor,
  WriteVerifier,
  DedupTracker,
  EmptyResponseHandler,
  BatchIntegrityTracker,
} from './bulletproof-backfill.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// TLS config (secure by default)
// Set INSECURE_TLS=true only in controlled environments with self-signed certs.
// Uses per-agent rejectUnauthorized (consistent with fetch-updates.js) instead of
// a global TLS override env var which affects ALL HTTP clients in the process.
const INSECURE_TLS = process.env.INSECURE_TLS === 'true';

// Configuration - BALANCED DEFAULTS for stability
const SCAN_URL = process.env.SCAN_URL || 'https://scan.sv-1.global.canton.network.sync.global/api/scan';
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 1000; // API max is 1000
// Cross-platform path handling
import { getBaseDataDir, getCursorDir, isGCSMode, logPathConfig, validateGCSBucket } from './path-utils.js';
// GCS preflight checks
import { runPreflightChecks } from './gcs-preflight.js';

const BASE_DATA_DIR = getBaseDataDir();
const CURSOR_DIR = getCursorDir();
const GCS_MODE = isGCSMode();
const FLUSH_EVERY_BATCHES = parseInt(process.env.FLUSH_EVERY_BATCHES) || 5;

// GCS checkpoint interval: drain upload queue every N batches for crash safety
// Lower = safer but slower, Higher = faster but larger re-fetch window on crash
const GCS_CHECKPOINT_INTERVAL = parseInt(process.env.GCS_CHECKPOINT_INTERVAL) || 50;

// Import GCS upload queue functions for checkpoint draining
import { drainUploads, getUploadQueue } from './gcs-upload-queue.js';

// ==========================================
// MEMORY-AWARE THROTTLING (Heap Pressure)
// ==========================================
// Prevents OOM crashes by pausing ingestion when heap usage exceeds threshold
const HEAP_PRESSURE_THRESHOLD = parseFloat(process.env.HEAP_PRESSURE_THRESHOLD) || 0.80; // 80% of max heap
const HEAP_CRITICAL_THRESHOLD = parseFloat(process.env.HEAP_CRITICAL_THRESHOLD) || 0.90; // 90% = emergency
const HEAP_CHECK_INTERVAL_MS = parseInt(process.env.HEAP_CHECK_INTERVAL_MS) || 5000; // Check every 5s minimum

let lastHeapCheck = 0;
let heapPressureEvents = 0;

/**
 * Get current heap usage as a fraction of max heap size
 */
function getHeapUsage() {
  const { heapUsed } = process.memoryUsage();
  const { heap_size_limit } = v8.getHeapStatistics();
  return {
    used: heapUsed,
    limit: heap_size_limit,
    ratio: heapUsed / heap_size_limit,
    usedMB: Math.round(heapUsed / 1024 / 1024),
    limitMB: Math.round(heap_size_limit / 1024 / 1024),
  };
}

/**
 * Check if heap is under memory pressure
 */
function checkMemoryPressure() {
  const now = Date.now();
  // Throttle checks to avoid overhead
  if (now - lastHeapCheck < HEAP_CHECK_INTERVAL_MS) {
    return { pressure: false, critical: false };
  }
  lastHeapCheck = now;
  
  const heap = getHeapUsage();
  const pressure = heap.ratio > HEAP_PRESSURE_THRESHOLD;
  const critical = heap.ratio > HEAP_CRITICAL_THRESHOLD;
  
  if (pressure) {
    heapPressureEvents++;
  }
  
  return { pressure, critical, heap };
}

/**
 * Wait for memory pressure to subside by draining queues
 * Returns when heap usage drops below threshold or timeout
 */
async function waitForMemoryRelief(shardLabel = '') {
  const maxWaitMs = 60000; // Max 60 seconds waiting
  const startWait = Date.now();
  let drainCycles = 0;
  
  while (Date.now() - startWait < maxWaitMs) {
    const heap = getHeapUsage();
    
    if (heap.ratio <= HEAP_PRESSURE_THRESHOLD * 0.9) {
      // Below 90% of threshold = safe to continue
      if (drainCycles > 0) {
        console.log(`   ✅ Memory pressure relieved: ${heap.usedMB}MB / ${heap.limitMB}MB (${(heap.ratio * 100).toFixed(1)}%)${shardLabel}`);
      }
      return;
    }
    
    drainCycles++;
    console.log(`   ⚠️ Memory pressure (${heap.usedMB}MB / ${heap.limitMB}MB = ${(heap.ratio * 100).toFixed(1)}%) - draining queues (cycle ${drainCycles})...${shardLabel}`);
    
    // Flush and drain all pending work
    await flushAll();
    await waitForWrites();
    if (GCS_MODE) {
      await drainUploads();
    }
    
    // Give GC a chance to run
    if (global.gc) {
      global.gc();
    }
    
    // Brief pause before re-checking
    await new Promise(r => setTimeout(r, 2000));
  }
  
  // Timeout - log warning but continue (let it crash naturally if truly OOM)
  const heap = getHeapUsage();
  console.warn(`   ⚠️ Memory pressure timeout after ${maxWaitMs}ms - continuing at ${heap.usedMB}MB / ${heap.limitMB}MB${shardLabel}`);
}

// Sharding configuration
const SHARD_INDEX = parseInt(process.env.SHARD_INDEX) || 0;
const SHARD_TOTAL = parseInt(process.env.SHARD_TOTAL) || 1;
const TARGET_MIGRATION = process.env.TARGET_MIGRATION ? parseInt(process.env.TARGET_MIGRATION) : null;

function assertConfig(condition, message) {
  if (!condition) {
    throw new Error(`[config] ${message}`);
  }
}

// Basic config validation (fail fast)
assertConfig(Number.isFinite(SHARD_TOTAL) && SHARD_TOTAL >= 1, `SHARD_TOTAL must be >= 1 (got ${process.env.SHARD_TOTAL})`);
assertConfig(Number.isFinite(SHARD_INDEX) && SHARD_INDEX >= 0 && SHARD_INDEX < SHARD_TOTAL, `SHARD_INDEX must be between 0 and SHARD_TOTAL-1 (got ${process.env.SHARD_INDEX})`);
assertConfig(Number.isFinite(BATCH_SIZE) && BATCH_SIZE >= 1 && BATCH_SIZE <= 1000, `BATCH_SIZE must be 1..1000 (got ${process.env.BATCH_SIZE})`);

// ==========================================
// AUTO-TUNING: PARALLEL FETCHES (Enhanced with Latency Tracking)
// ==========================================
const BASE_PARALLEL_FETCHES = parseInt(process.env.PARALLEL_FETCHES) || 8;
const MIN_PARALLEL_FETCHES = parseInt(process.env.MIN_PARALLEL_FETCHES) || 2;
const MAX_PARALLEL_FETCHES = parseInt(process.env.MAX_PARALLEL_FETCHES) || 24;

let dynamicParallelFetches = Math.min(
  Math.max(BASE_PARALLEL_FETCHES, MIN_PARALLEL_FETCHES),
  MAX_PARALLEL_FETCHES
);

assertConfig(Number.isFinite(BASE_PARALLEL_FETCHES) && BASE_PARALLEL_FETCHES >= 1, `PARALLEL_FETCHES must be >= 1 (got ${process.env.PARALLEL_FETCHES})`);
assertConfig(Number.isFinite(MIN_PARALLEL_FETCHES) && MIN_PARALLEL_FETCHES >= 1, `MIN_PARALLEL_FETCHES must be >= 1 (got ${process.env.MIN_PARALLEL_FETCHES})`);
assertConfig(Number.isFinite(MAX_PARALLEL_FETCHES) && MAX_PARALLEL_FETCHES >= MIN_PARALLEL_FETCHES, `MAX_PARALLEL_FETCHES must be >= MIN_PARALLEL_FETCHES`);

// Latency thresholds (milliseconds)
const LATENCY_LOW_MS = parseInt(process.env.LATENCY_LOW_MS) || 500;
const LATENCY_HIGH_MS = parseInt(process.env.LATENCY_HIGH_MS) || 2000;
const LATENCY_CRITICAL_MS = parseInt(process.env.LATENCY_CRITICAL_MS) || 5000;

const FETCH_TUNE_WINDOW_MS = 15_000; // Faster tuning window (was 30s)
let fetchStats = {
  windowStart: Date.now(),
  successCount: 0,
  retry503Count: 0,
  latencies: [],           // Rolling window of recent response times
  avgLatency: 0,
  p95Latency: 0,
  consecutiveStableWindows: 0,
};

// NOTE: Decode worker pool removed — main-thread decode is faster
// because structured clone serialization cost exceeds normalization cost

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
    rejectUnauthorized: !INSECURE_TLS,
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
 * 
 * CRITICAL FIX: This function now FAILS HARD on exhausted retries.
 * It will throw an error that MUST be handled by the caller.
 * No more silent continuation on network failures.
 */
async function retryWithBackoff(fn, options = {}) {
  const {
    maxRetries = 5,
    baseDelay = 1000,
    maxDelay = 30000,
    context = '',
    shouldRetry = (error) => isRetryableError(error),
  } = options;

  let lastError;
  
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      const callStart = Date.now();
      const result = await fn();
      const latency = Date.now() - callStart;
      
      // Track latency (rolling window of last 100 requests)
      fetchStats.latencies.push(latency);
      if (fetchStats.latencies.length > 100) {
        fetchStats.latencies.shift();
      }
      
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
      
      // CRITICAL FIX: Track error rate for auto-tuning safety
      fetchStats.errorCount = (fetchStats.errorCount || 0) + 1;
      
      // Check if we should retry
      if (attempt === maxRetries || !shouldRetry(error)) {
        // CRITICAL: Log and throw - do not silently continue
        const contextStr = context ? `[${context}] ` : '';
        console.error(
          `❌ ${contextStr}FATAL: Fetch failed after ${attempt + 1} attempts. ` +
          `Error: ${error.code || status || error.message}`
        );
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
  
  // CRITICAL: This should never be reached, but if it is, fail hard
  throw lastError || new Error('Unknown fetch error');
}

/**
 * Reset fetch stats for new tuning window
 */
function resetFetchStats(now) {
  fetchStats.windowStart = now;
  fetchStats.successCount = 0;
  fetchStats.retry503Count = 0;
  fetchStats.errorCount = 0;  // CRITICAL FIX: Track errors for safety
  fetchStats.latencies = [];
}

/**
 * Auto-tune parallel fetches based on error rate AND latency
 * 
 * CRITICAL FIX: Now error-aware - never tune up while error rate > 0
 */
function maybeTuneParallelFetches(shardLabel = '') {
  const now = Date.now();
  const elapsed = now - fetchStats.windowStart;
  if (elapsed < FETCH_TUNE_WINDOW_MS) return;

  const { successCount, retry503Count, latencies, errorCount = 0 } = fetchStats;
  const total = successCount + retry503Count + errorCount;

  if (total === 0) {
    resetFetchStats(now);
    return;
  }

  // Calculate latency percentiles
  let avgLatency = 0;
  let p95Latency = 0;
  if (latencies.length > 0) {
    const sorted = [...latencies].sort((a, b) => a - b);
    avgLatency = sorted.reduce((a, b) => a + b, 0) / sorted.length;
    p95Latency = sorted[Math.floor(sorted.length * 0.95)] || sorted[sorted.length - 1];
    fetchStats.avgLatency = avgLatency;
    fetchStats.p95Latency = p95Latency;
  }

  const errorRate = (retry503Count + errorCount) / total;
  let action = null;

  // CRITICAL FIX: ANY errors = scale down immediately
  // This prevents cascading failures during degraded API conditions
  if (errorCount > 0 || retry503Count > 0) {
    if (dynamicParallelFetches > MIN_PARALLEL_FETCHES) {
      const old = dynamicParallelFetches;
      const reduction = errorCount > 2 ? 3 : (retry503Count >= 3 ? 2 : 1);
      dynamicParallelFetches = Math.max(MIN_PARALLEL_FETCHES, dynamicParallelFetches - reduction);
      console.log(`   🔧 Auto-tune${shardLabel}: ERRORS DETECTED (${errorCount} errors, ${retry503Count} 503s, ${(errorRate*100).toFixed(1)}% rate) → PARALLEL ${old} → ${dynamicParallelFetches}`);
      action = 'down';
      fetchStats.consecutiveStableWindows = 0;
    }
  }
  // RULE 2: Critical latency → scale down
  else if (p95Latency > LATENCY_CRITICAL_MS || avgLatency > LATENCY_HIGH_MS) {
    if (dynamicParallelFetches > MIN_PARALLEL_FETCHES) {
      const old = dynamicParallelFetches;
      dynamicParallelFetches = Math.max(MIN_PARALLEL_FETCHES, dynamicParallelFetches - 1);
      console.log(`   🔧 Auto-tune${shardLabel}: HIGH LATENCY (avg=${avgLatency.toFixed(0)}ms, p95=${p95Latency.toFixed(0)}ms) → PARALLEL ${old} → ${dynamicParallelFetches}`);
      action = 'down';
    }
  }
  // RULE 3: Low errors + low latency → scale up aggressively
  // CRITICAL FIX: Only scale up if ZERO errors
  else if (errorCount === 0 && retry503Count === 0 && successCount >= 15 && avgLatency < LATENCY_LOW_MS && avgLatency > 0) {
    if (dynamicParallelFetches < MAX_PARALLEL_FETCHES) {
      const old = dynamicParallelFetches;
      const increment = avgLatency < 300 ? 2 : 1;
      dynamicParallelFetches = Math.min(MAX_PARALLEL_FETCHES, dynamicParallelFetches + increment);
      console.log(`   🔧 Auto-tune${shardLabel}: FAST+STABLE (avg=${avgLatency.toFixed(0)}ms, ${successCount} ok) → PARALLEL ${old} → ${dynamicParallelFetches}`);
      action = 'up';
    }
  }
  // RULE 4: Stable with moderate latency → cautious scale up
  else if (errorCount === 0 && retry503Count === 0 && successCount >= 20 && avgLatency < LATENCY_HIGH_MS) {
    fetchStats.consecutiveStableWindows++;
    if (fetchStats.consecutiveStableWindows >= 2 && dynamicParallelFetches < MAX_PARALLEL_FETCHES) {
      const old = dynamicParallelFetches;
      dynamicParallelFetches = Math.min(MAX_PARALLEL_FETCHES, dynamicParallelFetches + 1);
      console.log(`   🔧 Auto-tune${shardLabel}: STABLE x${fetchStats.consecutiveStableWindows} → PARALLEL ${old} → ${dynamicParallelFetches}`);
      action = 'up';
      fetchStats.consecutiveStableWindows = 0;
    }
  }

  // Reset window
  if (action !== 'up') fetchStats.consecutiveStableWindows = 0;
  resetFetchStats(now);
}

/**
 * Auto-tune decode workers based on queue depth
 */
// maybeTuneDecodeWorkers removed — no longer using worker pool

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
    if (!existsSync(CURSOR_DIR)) {
      console.log(`   📁 Creating cursor directory: ${CURSOR_DIR}`);
      mkdirSync(CURSOR_DIR, { recursive: true });
    }
    
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
    console.error(`   [saveCursor] ❌ FAILED to save cursor: ${err.message}`);
    console.error(`   [saveCursor] CURSOR_DIR: ${CURSOR_DIR}`);
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
  let id = 0;

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
let fetchCount = 0;
let firstCallLogged = false;
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
  
  const thisCall = ++fetchCount;
  const startTime = Date.now();
  
  return await retryWithBackoff(async () => {
    const response = await client.post('/v0/backfilling/updates-before', payload);
    // Log only the first successful call
    if (!firstCallLogged) {
      firstCallLogged = true;
      const elapsed = Date.now() - startTime;
      console.log(`   ✅ API connected: first call returned ${response.data?.transactions?.length || 0} txs in ${elapsed}ms`);
    }
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
 * Process backfill items using main-thread decode (zero serialization overhead)
 * 
 * normalizeUpdate/normalizeEvent is pure field mapping (~μs per call).
 * Piscina structured clone serialization of 1000 large JSON objects per page
 * costs MORE than the normalization itself, so main-thread is faster.
 */
async function processBackfillItems(transactions, migrationId) {
  const updates = [];
  const events = [];

  for (const tx of transactions) {
    const r = decodeInMainThread(tx, migrationId);
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
 * Decode in main thread — primary decode path (no worker pool overhead)
 * Includes effective_at guard to prevent partition crashes from null timestamps.
 */
export function decodeInMainThread(tx, migrationId) {
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
      if (ev.effective_at) {
        events.push(ev);
      } else {
        console.warn(`⚠️ [decode] Skipping reassign_create with no effective_at: update=${update.update_id}`);
      }
    }
    if (ae) {
      const ev = normalizeEvent(ae, update.update_id, migrationId, tx, updateInfo);
      ev.event_type = 'reassign_archive';
      if (ev.effective_at) {
        events.push(ev);
      } else {
        console.warn(`⚠️ [decode] Skipping reassign_archive with no effective_at: update=${update.update_id}`);
      }
    }
  } else {
    const eventsById = txData.events_by_id || tx.events_by_id || {};
    for (const [eventId, rawEvent] of Object.entries(eventsById)) {
      const ev = normalizeEvent(rawEvent, update.update_id, migrationId, rawEvent, updateInfo);
      ev.event_id = eventId;
      if (ev.effective_at) {
        events.push(ev);
      } else {
        console.warn(`⚠️ [decode] Skipping event ${eventId} with no effective_at: update=${update.update_id}`);
      }
    }
  }

  return { update, events };
}

/**
 * Fetch a single time slice with STREAMING processing
 * Instead of accumulating all transactions, process them immediately to avoid OOM.
 * Returns stats only, not the raw transactions.
 */
async function fetchTimeSliceStreaming(migrationId, synchronizerId, sliceBefore, sliceAfter, sliceIndex, processCallback) {
  const seenUpdateIds = new Set();
  let currentBefore = sliceBefore;
  const emptyHandler = new EmptyResponseHandler();
  let totalTxs = 0;
  let earliestTime = sliceBefore;
  
  // Pipeline: allow up to N process callbacks to run concurrently while fetching
  const MAX_INFLIGHT_PROCESS = 3;
  const inflightProcesses = [];
  
  while (true) {
    // Check if we've passed the lower bound of this slice
    if (new Date(currentBefore).getTime() <= new Date(sliceAfter).getTime()) {
      break;
    }
    
    let response;
    try {
      response = await fetchBackfillBefore(migrationId, synchronizerId, currentBefore, sliceAfter);
    } catch (err) {
      throw err;
    }
    
    const txs = response?.transactions || [];
    
    if (txs.length === 0) {
      const { action, newBefore, consecutiveEmpty, stepMs } = emptyHandler.handleEmpty(currentBefore, sliceAfter);
      if (action === 'done' || !newBefore) {
        break;
      }

      // Keep logs accurate (step size may increase across gaps)
      if (consecutiveEmpty % 100 === 0) {
        console.log(`   ⚠️ ${consecutiveEmpty} consecutive empty responses. Stepping back ${stepMs}ms at a time.`);
      }

      currentBefore = newBefore;
      continue;
    }
    
    emptyHandler.resetOnData();
    
    // Deduplicate within this slice
    const uniqueTxs = [];
    for (const tx of txs) {
      const updateId = tx.update_id || tx.transaction?.update_id || tx.reassignment?.update_id;
      if (updateId) {
        if (!seenUpdateIds.has(updateId)) {
          seenUpdateIds.add(updateId);
          uniqueTxs.push(tx);
        }
      } else {
        uniqueTxs.push(tx);
      }
    }
    
    // PIPELINE: Fire-and-forget process callback, apply backpressure if too many inflight
    if (uniqueTxs.length > 0) {
      // Wait for oldest inflight to complete if at capacity
      if (inflightProcesses.length >= MAX_INFLIGHT_PROCESS) {
        await inflightProcesses.shift();
      }
      const processPromise = processCallback(uniqueTxs).catch(err => {
        console.error(`   ❌ Process callback error in slice ${sliceIndex}: ${err.message}`);
      });
      inflightProcesses.push(processPromise);
      totalTxs += uniqueTxs.length;
    }
    
    // Track earliest time for cursor
    for (const tx of txs) {
      const t = getEventTime(tx);
      if (t && t < earliestTime) earliestTime = t;
    }
    
    // Find oldest timestamp for next page
    let oldestTime = null;
    for (const tx of txs) {
      const t = getEventTime(tx);
      if (t && (!oldestTime || t < oldestTime)) {
        oldestTime = t;
      }
    }
    
    if (oldestTime && new Date(oldestTime).getTime() <= new Date(sliceAfter).getTime()) {
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
    
    // Memory safety
    if (seenUpdateIds.size > 50000) {
      seenUpdateIds.clear();
    }
  }
  
  // Wait for all remaining inflight processes to complete
  if (inflightProcesses.length > 0) {
    await Promise.all(inflightProcesses);
  }
  
  return { sliceIndex, totalTxs, earliestTime };
}

/**
 * Parallel fetch with STREAMING processing
 * 
 * Divides the time range into N non-overlapping slices and fetches each in parallel.
 * Each slice STREAMS its transactions to processBackfillItems immediately to avoid OOM.
 * Returns aggregated stats instead of raw transactions.
 * 
 * CRITICAL FIX: Cursor advancement is now CONSERVATIVE.
 * - Tracks per-slice completion status and boundaries
 * - Cursor only advances to the OLDEST INCOMPLETE slice boundary
 * - Prevents data gaps if a newer slice fails after older slices complete
 */
async function parallelFetchBatch(migrationId, synchronizerId, startBefore, atOrAfter, maxBatches, concurrency, cursorCallback = null) {
  const startMs = new Date(atOrAfter).getTime();
  const endMs = new Date(startBefore).getTime();
  const rangeMs = endMs - startMs;

  // Don't parallelize tiny ranges
  if (rangeMs < 60000 * concurrency) {
    return sequentialFetchBatch(migrationId, synchronizerId, startBefore, atOrAfter, maxBatches);
  }

  // Divide into non-overlapping-ish time slices
  const sliceMs = rangeMs / concurrency;

  console.log(`   🔀 Parallel fetch: ${concurrency} slices of ${Math.round(sliceMs / 1000)}s each`);

  // Shared stats across all slices
  let totalUpdates = 0;
  let totalEvents = 0;
  let earliestTime = startBefore;
  let pageCount = 0;
  const streamStartTime = Date.now();

  // Global dedup across slices to avoid duplicates at slice boundaries
  const globalSeenUpdateIds = new Set();
  // IMPORTANT: This set can grow without bound on large ranges and eventually
  // throw "Set maximum size exceeded". We cap it and clear opportunistically.
  // Clearing is safe here because it only guards boundary duplicates; correctness
  // is still ensured by cursor-based pagination.
  const GLOBAL_DEDUP_MAX = Number(process.env.GLOBAL_DEDUP_MAX || 250_000);

  // =========================================================================
  // CRITICAL FIX: Track per-slice completion for safe cursor advancement
  // =========================================================================
  // Slices are numbered 0 (newest) to N-1 (oldest).
  // We can ONLY safely advance cursor to the boundary of a CONTIGUOUS block
  // of completed slices starting from slice 0.
  // 
  // Example with 4 slices covering time 1000 → 0:
  //   Slice 0: 1000-750 (newest)
  //   Slice 1: 750-500
  //   Slice 2: 500-250
  //   Slice 3: 250-0 (oldest)
  // 
  // If slices 1, 2, 3 complete but slice 0 is still running:
  //   safeCursorBoundary = 1000 (can't advance past incomplete slice 0)
  // 
  // If slices 0, 1 complete but slices 2, 3 still running:
  //   safeCursorBoundary = 500 (boundary of completed slice 1)
  // =========================================================================
  const sliceBoundaries = []; // [{ sliceBefore, sliceAfter }] indexed by slice
  const sliceCompleted = [];  // boolean[] indexed by slice
  const sliceEarliestTime = []; // ISO string[] indexed by slice (actual data processed)
  
  // Pre-compute slice boundaries
  for (let i = 0; i < concurrency; i++) {
    const sliceBefore = new Date(endMs - (i * sliceMs)).toISOString();
    const sliceAfter = new Date(endMs - ((i + 1) * sliceMs)).toISOString();
    sliceBoundaries.push({ sliceBefore, sliceAfter });
    sliceCompleted.push(false);
    sliceEarliestTime.push(sliceBefore); // Initialize to slice start
  }

  /**
   * Calculate safe cursor boundary based on contiguous completed slices.
   * Only advances cursor to the oldest boundary of a contiguous block
   * of completed slices starting from slice 0 (newest).
   */
  function getSafeCursorBoundary() {
    // Find the first incomplete slice starting from 0
    let contiguousCompleteCount = 0;
    for (let i = 0; i < concurrency; i++) {
      if (sliceCompleted[i]) {
        contiguousCompleteCount++;
      } else {
        break; // Found first incomplete
      }
    }
    
    if (contiguousCompleteCount === 0) {
      // No slices complete yet - cursor stays at startBefore
      return startBefore;
    }
    
    // Safe boundary is the END (sliceAfter) of the last contiguously completed slice
    // This is the oldest timestamp we can safely claim as processed
    const lastCompleteIdx = contiguousCompleteCount - 1;
    
    // Use actual earliest processed time if available, else use slice boundary
    const safeTime = sliceEarliestTime[lastCompleteIdx] || sliceBoundaries[lastCompleteIdx].sliceAfter;
    
    return safeTime;
  }

  // Process callback that handles transactions immediately with progress logging
  const processCallback = async (transactions, sliceIndex) => {
    const { updates, events } = await processBackfillItems(transactions, migrationId);
    totalUpdates += updates;
    totalEvents += events;
    pageCount++;

    // Track earliest time from transactions for progress tracking
    for (const tx of transactions) {
      const t = getEventTime(tx);
      if (t && t < earliestTime) earliestTime = t;
      // Also track per-slice earliest for safe cursor calculation
      if (t && t < sliceEarliestTime[sliceIndex]) {
        sliceEarliestTime[sliceIndex] = t;
      }
    }

    // Log progress every 10 pages
    if (pageCount % 10 === 0) {
      const elapsed = (Date.now() - streamStartTime) / 1000;
      const throughput = Math.round(totalUpdates / elapsed);
      const stats = getBufferStats();
      
      // Build progress string with upload queue info if in GCS mode
      let progressLine = `   📥 M${migrationId} Page ${pageCount}: ${totalUpdates.toLocaleString()} upd @ ${throughput}/s | W: ${stats.queuedJobs || 0}/${stats.activeWorkers || 0}`;
      
      // Add upload queue stats if available
      if (stats.uploadQueuePending !== undefined || stats.uploadQueueActive !== undefined) {
        const pending = stats.uploadQueuePending || 0;
        const active = stats.uploadQueueActive || 0;
        const mbps = stats.uploadThroughputMBps || '0.00';
        const pauseIndicator = stats.uploadQueuePaused ? ' ⏸️' : '';
        progressLine += ` | ☁️ ${pending}+${active} @ ${mbps}MB/s${pauseIndicator}`;
      }
      
      console.log(progressLine);

      // Save cursor every 100 pages for UI visibility
      // CRITICAL: Use SAFE cursor boundary, not raw earliestTime
      if (cursorCallback && pageCount % 100 === 0) {
        const safeBoundary = getSafeCursorBoundary();
        cursorCallback(totalUpdates, totalEvents, safeBoundary);
      }
    }
  };

  // Helper to run a single slice with retries
  const SLICE_MAX_RETRIES = 3;
  const runSliceWithRetry = async (sliceIndex, sliceBefore, sliceAfter) => {
    let lastError;
    for (let attempt = 0; attempt < SLICE_MAX_RETRIES; attempt++) {
      try {
        const result = await fetchTimeSliceStreaming(migrationId, synchronizerId, sliceBefore, sliceAfter, sliceIndex, async (txs) => {
          // Cross-slice dedup (cheap and safe)
          const unique = [];
          for (const tx of txs) {
            const updateId = tx.update_id || tx.transaction?.update_id || tx.reassignment?.update_id;
            if (!updateId) {
              unique.push(tx);
              continue;
            }
            if (globalSeenUpdateIds.size >= GLOBAL_DEDUP_MAX) {
              // Keep memory bounded. This may allow some duplicates at slice boundaries
              // after the clear, but downstream processing can handle occasional dups,
              // and cursor advancement remains conservative.
              globalSeenUpdateIds.clear();
            }
            if (globalSeenUpdateIds.has(updateId)) continue;
            globalSeenUpdateIds.add(updateId);
            unique.push(tx);
          }
          if (unique.length > 0) {
            // Pass sliceIndex to processCallback for per-slice tracking
            await processCallback(unique, sliceIndex);
          }
        });
        
        // Mark slice as completed ONLY after all its data is processed
        sliceCompleted[sliceIndex] = true;
        
        return result; // Success
      } catch (err) {
        lastError = err;
        if (attempt < SLICE_MAX_RETRIES - 1) {
          const delay = Math.min(1000 * Math.pow(2, attempt), 10000) + Math.random() * 1000;
          console.log(`   ⏳ Slice ${sliceIndex} failed (attempt ${attempt + 1}/${SLICE_MAX_RETRIES}): ${err.message}. Retrying in ${Math.round(delay)}ms...`);
          await new Promise(r => setTimeout(r, delay));
        }
      }
    }
    // All retries exhausted - slice NOT marked complete
    console.error(`   ❌ Slice ${sliceIndex} failed after ${SLICE_MAX_RETRIES} attempts: ${lastError.message}`);
    return { sliceIndex, totalTxs: 0, earliestTime: sliceBefore, error: lastError };
  };

  // Launch all slices in parallel with retry logic
  const slicePromises = [];
  for (let i = 0; i < concurrency; i++) {
    const { sliceBefore, sliceAfter } = sliceBoundaries[i];
    slicePromises.push(runSliceWithRetry(i, sliceBefore, sliceAfter));
  }

  // Wait for all slices to complete
  const sliceResults = await Promise.all(slicePromises);

  // Find earliest time across all slices (for stats only)
  for (const slice of sliceResults) {
    if (slice.earliestTime && slice.earliestTime < earliestTime) {
      earliestTime = slice.earliestTime;
    }
  }

  const totalTxs = sliceResults.reduce((sum, s) => sum + (s.totalTxs || 0), 0);
  const failedSlices = sliceResults.filter(s => s.error);
  const hasError = failedSlices.length > 0;

  // Calculate final safe cursor boundary
  const safeCursorBoundary = getSafeCursorBoundary();
  
  // Log completion status for debugging
  const completedCount = sliceCompleted.filter(Boolean).length;
  if (completedCount < concurrency) {
    console.log(`   ⚠️ Only ${completedCount}/${concurrency} slices completed. Safe cursor: ${safeCursorBoundary}`);
  }

  // Return stats-only result (transactions already processed via streaming)
  return {
    results: totalTxs > 0 ? [{
      transactions: [], // Already processed
      processedUpdates: totalUpdates,
      processedEvents: totalEvents,
      before: safeCursorBoundary // Use SAFE boundary, not raw earliestTime
    }] : [],
    reachedEnd: !hasError,
    earliestTime: safeCursorBoundary, // Use SAFE boundary for cursor advancement
    totalUpdates,
    totalEvents,
    // CRITICAL: Include failed slices for caller to handle
    failedSlices: failedSlices.map(s => ({
      sliceIndex: s.sliceIndex,
      error: s.error?.message || 'Unknown error',
      status: s.error?.response?.status || null,
      code: s.error?.code || null,
    })),
    // Additional metadata for debugging
    sliceCompletionStatus: sliceCompleted.map((complete, i) => ({
      sliceIndex: i,
      complete,
      boundary: sliceBoundaries[i],
      earliestProcessed: sliceEarliestTime[i],
    })),
  };
}

/**
 * Sequential fallback for small ranges or when parallel fails
 */
async function sequentialFetchBatch(migrationId, synchronizerId, startBefore, atOrAfter, maxBatches) {
  const results = [];
  const seenUpdateIds = new Set();
  let currentBefore = startBefore;
  let batchesFetched = 0;
  const emptyHandler = new EmptyResponseHandler();
  
  while (batchesFetched < maxBatches) {
    if (new Date(currentBefore).getTime() <= new Date(atOrAfter).getTime()) {
      return { results, reachedEnd: true };
    }
    
    let response;
    try {
      response = await fetchBackfillBefore(migrationId, synchronizerId, currentBefore, atOrAfter);
    } catch (err) {
      throw err;
    }
    
    const txs = response?.transactions || [];
    batchesFetched++;
    
    if (txs.length === 0) {
      const { action, newBefore, consecutiveEmpty, stepMs } = emptyHandler.handleEmpty(currentBefore, atOrAfter);
      if (action === 'done' || !newBefore) {
        return { results, reachedEnd: true };
      }

      if (consecutiveEmpty % 100 === 0) {
        console.log(`   ⚠️ ${consecutiveEmpty} consecutive empty responses in sequential mode. Stepping back ${stepMs}ms at a time.`);
      }

      currentBefore = newBefore;
      continue;
    }
    
    emptyHandler.resetOnData();
    
    const uniqueTxs = [];
    for (const tx of txs) {
      const updateId = tx.update_id || tx.transaction?.update_id || tx.reassignment?.update_id;
      if (updateId) {
        if (!seenUpdateIds.has(updateId)) {
          seenUpdateIds.add(updateId);
          uniqueTxs.push(tx);
        }
      } else {
        uniqueTxs.push(tx);
      }
    }
    
    if (uniqueTxs.length > 0) {
      results.push({ transactions: uniqueTxs, before: currentBefore });
    }
    
    let oldestTime = null;
    for (const tx of txs) {
      const t = getEventTime(tx);
      if (t && (!oldestTime || t < oldestTime)) {
        oldestTime = t;
      }
    }
    
    if (oldestTime && new Date(oldestTime).getTime() <= new Date(atOrAfter).getTime()) {
      return { results, reachedEnd: true };
    }
    
    if (oldestTime) {
      // CRITICAL: Subtract 1ms to avoid fetching the same record again
      const d = new Date(oldestTime);
      d.setMilliseconds(d.getMilliseconds() - 1);
      currentBefore = d.toISOString();
    } else {
      const d = new Date(currentBefore);
      d.setMilliseconds(d.getMilliseconds() - 1);
      currentBefore = d.toISOString();
    }
    
    if (seenUpdateIds.size > 100000) {
      seenUpdateIds.clear();
    }
  }
  
  return { results, reachedEnd: false };
}

/**
 * Backfill a single synchronizer with parallel fetching (shard-aware)
 * Uses auto-tuning for parallel fetches and decode workers
 * 
 * CRITICAL FIX: Now uses AtomicCursor for crash-safe cursor management
 */
async function backfillSynchronizer(migrationId, synchronizerId, minTime, maxTime, shardIndex = null) {
  const shardLabel = shardIndex !== null ? ` [shard ${shardIndex}/${SHARD_TOTAL}]` : '';
  
  // Structured log: synchronizer start
  logSynchronizer('start', {
    migrationId,
    synchronizerId,
    shardIndex,
    minTime,
    maxTime,
    extra: {
      parallel_fetches: dynamicParallelFetches,
      decode: 'main-thread',
    },
  });
  
  console.log(`\n📍 Backfilling migration ${migrationId}, synchronizer ${synchronizerId.substring(0, 30)}...${shardLabel}`);
  console.log(`   Range: ${minTime} to ${maxTime}`);
  console.log(`   Parallel fetches (auto-tuned): ${dynamicParallelFetches} (min=${MIN_PARALLEL_FETCHES}, max=${MAX_PARALLEL_FETCHES})`);
  console.log(`   Decode: main-thread (zero serialization overhead)`);
  
  // CRITICAL FIX: Use AtomicCursor for transactional cursor management
  const atomicCursor = new AtomicCursor(migrationId, synchronizerId, shardIndex);
  
  // Load existing cursor state
  let cursorState = atomicCursor.load();
  let before = cursorState?.last_before || maxTime;
  const atOrAfter = minTime;
  
  // CRITICAL: Check if cursor.last_before is already at or before minTime
  // This means we've already processed everything.
  if (cursorState && cursorState.last_before) {
    const lastBeforeMs = new Date(cursorState.last_before).getTime();
    const minTimeMs = new Date(minTime).getTime();

    if (lastBeforeMs <= minTimeMs) {
      log('info', 'synchronizer_already_complete', {
        migration: migrationId,
        synchronizer: synchronizerId.substring(0, 30),
        shard: shardIndex,
        last_before: cursorState.last_before,
        min_time: minTime,
      });
      
      console.log(`   ⚠️ Cursor last_before (${cursorState.last_before}) is at or before minTime (${minTime})`);
      console.log(`   ⚠️ This synchronizer appears complete. Ensuring writer queues are drained before marking complete.`);

      // Flush any in-memory buffers and wait for pending writes.
      try {
        await flushAll();
      } catch {}

      try {
        await waitForWrites();
      } catch {}

      const finalStats = getBufferStats();
      const pendingWritesAccurate =
        Number(finalStats.pendingWrites || 0) +
        Number(finalStats.queuedWrites ?? finalStats.queuedJobs ?? 0) +
        Number(finalStats.activeWrites ?? finalStats.activeWorkers ?? 0);
      const bufferedRecords = Number((finalStats.updatesBuffered || 0) + (finalStats.eventsBuffered || 0));
      const hasPendingWork = pendingWritesAccurate > 0 || bufferedRecords > 0;

      // Mark complete atomically
      if (!cursorState.complete || hasPendingWork) {
        atomicCursor.saveAtomic({
          ...cursorState,
          pending_writes: pendingWritesAccurate,
          buffered_records: bufferedRecords,
          complete: !hasPendingWork,
          min_time: minTime,
          max_time: maxTime,
        });

        logCursor('finalized', {
          migrationId,
          synchronizerId,
          shardIndex,
          lastBefore: cursorState.last_before,
          totalUpdates: cursorState.total_updates || 0,
          totalEvents: cursorState.total_events || 0,
          complete: !hasPendingWork,
          pendingWrites: pendingWritesAccurate,
        });

        if (hasPendingWork) {
          console.log(`   ⏳ Writes still pending (pending_writes=${pendingWritesAccurate}, buffered_records=${bufferedRecords}). Cursor left in finalizing state.`);
        }
      }

      return { updates: cursorState.total_updates || 0, events: cursorState.total_events || 0 };
    }

    // Log cursor state for debugging
    logCursor('resume', {
      migrationId,
      synchronizerId,
      shardIndex,
      lastBefore: cursorState.last_before,
      totalUpdates: cursorState.total_updates || 0,
      totalEvents: cursorState.total_events || 0,
      complete: cursorState.complete || false,
    });
    
    console.log(
      `   📍 Resuming from cursor: last_before=${cursorState.last_before}, updates=${cursorState.total_updates || 0}, complete=${cursorState.complete || false}`,
    );
  }
  
  let totalUpdates = cursorState?.total_updates || 0;
  let totalEvents = cursorState?.total_events || 0;
  let batchCount = 0;
  const startTime = Date.now();
  let lastMetricsLog = Date.now();
  const METRICS_LOG_INTERVAL_MS = 60000; // Log metrics every minute
  
  // Save initial cursor only if this is a fresh start
  if (!cursorState) {
    atomicCursor.saveAtomic({
      last_before: before,
      total_updates: 0,
      total_events: 0,
      started_at: new Date().toISOString(),
      min_time: minTime,
      max_time: maxTime,
    });
    
    logCursor('created', {
      migrationId,
      synchronizerId,
      shardIndex,
      lastBefore: before,
      totalUpdates: 0,
      totalEvents: 0,
    });
  }
  
  // Transient error tracking for exponential backoff and cooldown mode
  let consecutiveTransientErrors = 0;
  let cooldownUntil = 0;
  
  while (true) {
    const batchStartTime = Date.now();
    
    // MEMORY SAFETY: Check heap pressure before starting new batch
    const memCheck = checkMemoryPressure();
    if (memCheck.pressure) {
      console.log(`   ⚠️ Heap pressure detected: ${memCheck.heap.usedMB}MB / ${memCheck.heap.limitMB}MB (${(memCheck.heap.ratio * 100).toFixed(1)}%)${shardLabel}`);
      await waitForMemoryRelief(shardLabel);
    }
    
    try {
      // Use current dynamic concurrency values
      const localParallel = dynamicParallelFetches;
      const cursorBeforeBatch = before;
      
      // Cursor callback for streaming progress updates (transactional)
      const cursorCallback = (streamUpdates, streamEvents, streamEarliest) => {
        atomicCursor.saveAtomic({
          last_before: streamEarliest || before,
          total_updates: totalUpdates + streamUpdates,
          total_events: totalEvents + streamEvents,
          min_time: minTime,
          max_time: maxTime,
        });
      };
      
      const fetchResult = await parallelFetchBatch(
        migrationId, synchronizerId, before, atOrAfter, 
        localParallel * 2,  // maxBatches per cycle
        localParallel,      // actual concurrency
        cursorCallback      // pass cursor callback for streaming updates
      );
      
      const { results, reachedEnd, earliestTime: resultEarliestTime, totalUpdates: batchUpdates, totalEvents: batchEvents, failedSlices } = fetchResult;
      
      // CRITICAL DATA INTEGRITY FIX: If any slice failed, throw immediately
      // This prevents advancing the cursor past unfetched data ranges
      if (failedSlices && failedSlices.length > 0) {
        const sliceList = failedSlices.map(s => `slice ${s.sliceIndex}: ${s.error}`).join(', ');

        // Treat slice failures as transient if they are predominantly retryable
        // (e.g. 503 from upstream). This ensures the existing batch backoff
        // logic runs instead of hard-failing batch 0.
        const statuses = failedSlices.map(s => s.status).filter(Boolean);
        const has503 = statuses.includes(503) || sliceList.includes('status code 503');
        const has429 = statuses.includes(429) || sliceList.includes('status code 429');
        const has5xx = statuses.some(s => [500, 502, 503, 504].includes(s));
        const isTransient = has503 || has429 || has5xx;

        const e = new Error(`${failedSlices.length} slice(s) failed: ${sliceList}. Cursor NOT advanced to prevent data gaps.`);
        if (isTransient) {
          // Mimic axios error shape so the catch block treats it as transient.
          e.response = { status: has503 ? 503 : (has429 ? 429 : 503) };
        }
        throw e;
      }
      
      if (results.length === 0 && !batchUpdates) {
        log('info', 'no_more_transactions', {
          migration: migrationId,
          synchronizer: synchronizerId.substring(0, 30),
          shard: shardIndex,
          batch: batchCount,
        });
        console.log(`   ✅ No more transactions. Marking complete.${shardLabel}`);
        break;
      }
      
      // Streaming mode: transactions already processed, just use stats
      totalUpdates += batchUpdates || 0;
      totalEvents += batchEvents || 0;
      batchCount++;
      
      // Update cursor position from streaming result
      const newEarliestTime = resultEarliestTime || (results[0]?.before);
      if (newEarliestTime && newEarliestTime !== before) {
        const d = new Date(newEarliestTime);
        d.setMilliseconds(d.getMilliseconds() - 1);
        before = d.toISOString();
      } else {
        // No new data found, small step back
        const d = new Date(before);
        d.setMilliseconds(d.getMilliseconds() - 1);
        before = d.toISOString();
      }
      
      // Calculate throughput
      const elapsed = (Date.now() - startTime) / 1000;
      const throughput = Math.round(totalUpdates / elapsed);
      const batchLatency = Date.now() - batchStartTime;
      
      // Get buffer stats
      const stats = getBufferStats();
      const pendingWritesAccurate =
        Number(stats.pendingWrites || 0) +
        Number(stats.queuedWrites ?? stats.queuedJobs ?? 0) +
        Number(stats.activeWrites ?? stats.activeWorkers ?? 0);
      const queuedJobs = Number(stats.queuedJobs ?? 0);
      const activeWorkers = Number(stats.activeWorkers ?? 0);

      // CRITICAL FIX: Atomic cursor save AFTER data is confirmed buffered
      // Use transaction pattern: begin -> addPending -> commit
      if (!atomicCursor.inTransaction) {
        atomicCursor.beginTransaction(batchUpdates, batchEvents, before);
      } else {
        atomicCursor.addPending(batchUpdates, batchEvents, before);
      }
      atomicCursor.commit();
      
      // SUCCESS: Reset transient error tracking
      consecutiveTransientErrors = 0;
      
      // Check if cooldown expired - allow auto-tuner to scale back up
      if (cooldownUntil && Date.now() > cooldownUntil) {
        console.log(`   🔥 Cooldown expired, auto-tuner will scale up if API is healthy${shardLabel}`);
        cooldownUntil = 0;
      }
      
      // Update time bounds
      atomicCursor.setTimeBounds(minTime, maxTime);
      
      // Structured batch log
      logBatch({
        migrationId,
        synchronizerId,
        shardIndex,
        batchCount,
        updates: batchUpdates || 0,
        events: batchEvents || 0,
        totalUpdates,
        totalEvents,
        cursorBefore: cursorBeforeBatch,
        cursorAfter: before,
        throughput,
        latencyMs: batchLatency,
        parallelFetches: dynamicParallelFetches,
        queuedJobs,
        activeWorkers,
      });
      
      // Periodic metrics log (every minute)
      if (Date.now() - lastMetricsLog >= METRICS_LOG_INTERVAL_MS) {
        logMetrics({
          migrationId,
          shardIndex,
          elapsedSeconds: elapsed,
          totalUpdates,
          totalEvents,
          avgThroughput: throughput,
          currentThroughput: Math.round((batchUpdates || 0) / (batchLatency / 1000)),
          parallelFetches: dynamicParallelFetches,
          avgLatencyMs: fetchStats.avgLatency,
          p95LatencyMs: fetchStats.p95Latency,
          errorCount: fetchStats.errorCount || 0,
          retryCount: fetchStats.retry503Count,
        });
        lastMetricsLog = Date.now();
      }
      
      // Force flush periodically to prevent memory buildup
      if (batchCount % FLUSH_EVERY_BATCHES === 0) {
        await flushAll();
      }
      
      // GCS CRASH SAFETY: Periodic checkpoint - drain upload queue and confirm GCS position
      // This bounds the re-fetch window on VM crash to ~GCS_CHECKPOINT_INTERVAL batches
      if (GCS_MODE && batchCount % GCS_CHECKPOINT_INTERVAL === 0) {
        const checkpointStart = Date.now();
        console.log(`   ⏱️ GCS checkpoint: draining upload queue...${shardLabel}`);
        
        // Wait for all GCS uploads to complete
        await drainUploads();
        
        // Confirm GCS position in cursor
        atomicCursor.confirmGCS(before, totalUpdates, totalEvents);
        
        const checkpointMs = Date.now() - checkpointStart;
        console.log(`   ✅ GCS checkpoint confirmed at ${before} (${checkpointMs}ms)${shardLabel}`);
      }
      
      // Auto-tune after processing this wave
      maybeTuneParallelFetches(shardLabel);
      
      // Main progress line with current tuning values
      console.log(`   📦${shardLabel} Batch ${batchCount}: +${batchUpdates || 0} upd, +${batchEvents || 0} evt | Total: ${totalUpdates.toLocaleString()} @ ${throughput}/s | F:${dynamicParallelFetches} | Q: ${queuedJobs}/${activeWorkers}`);
      
      if (reachedEnd || new Date(before).getTime() <= new Date(atOrAfter).getTime()) {
        log('info', 'reached_lower_bound', {
          migration: migrationId,
          synchronizer: synchronizerId.substring(0, 30),
          shard: shardIndex,
          before,
          at_or_after: atOrAfter,
        });
        console.log(`   ✅ Reached lower bound. Complete.${shardLabel}`);
        break;
      }
      
    } catch (err) {
      const status = err.response?.status;
      const msg = err.response?.data?.error || err.message;
      
      logError('batch', err, {
        migration: migrationId,
        synchronizer: synchronizerId.substring(0, 30),
        shard: shardIndex,
        batch: batchCount,
        cursor_before: before,
      });
      
      console.error(`   ❌ Error at batch ${batchCount} (status ${status || "n/a"}): ${msg}${shardLabel}`);
      
      // Save cursor and retry for transient errors
      if ([429, 500, 502, 503, 504].includes(status)) {
        consecutiveTransientErrors++;
        
        // COOLDOWN MODE: After 3 consecutive transient errors, drop to minimal concurrency
        if (consecutiveTransientErrors >= 3 && dynamicParallelFetches > 1) {
          console.log(`   🧊 Cooldown mode: Dropping to 1 parallel fetch for 60s${shardLabel}`);
          dynamicParallelFetches = 1;
          cooldownUntil = Date.now() + 60000; // 60 second cooldown
        }
        
        // EXPONENTIAL BACKOFF: 5s, 10s, 20s, 40s, max 60s
        const backoffDelay = Math.min(5000 * Math.pow(2, consecutiveTransientErrors - 1), 60000);
        console.log(`   ⏳ Transient error #${consecutiveTransientErrors}, backing off ${Math.round(backoffDelay / 1000)}s...${shardLabel}`);
        
        atomicCursor.saveAtomic({
          last_before: before,
          total_updates: totalUpdates,
          total_events: totalEvents,
          error: msg,
          error_at: new Date().toISOString(),
          min_time: minTime,
          max_time: maxTime,
        });
        
        await sleep(backoffDelay);
        continue;
      }
      
      // CRITICAL: Non-transient error - log fatal and throw
      logFatal('batch', err, {
        migration: migrationId,
        synchronizer: synchronizerId.substring(0, 30),
        shard: shardIndex,
        batch: batchCount,
        total_updates: totalUpdates,
        total_events: totalEvents,
      });
      
      throw err;
    }
  }
  
  // Flush remaining data
  await flushAll();
  
  // CRITICAL: Wait for all writes to complete before marking as done
  console.log(`   ⏳ Waiting for all pending writes to complete...${shardLabel}`);
  await waitForWrites();
  
  // GCS CRASH SAFETY: Final drain before marking complete
  if (GCS_MODE) {
    console.log(`   ⏱️ Final GCS drain before marking complete...${shardLabel}`);
    await drainUploads();
  }
  
  // Get final write stats
  const finalStats = getBufferStats();
  console.log(`   ✅ All writes complete. Final queue: ${finalStats.queuedJobs || 0} pending, ${finalStats.activeWorkers || 0} active${shardLabel}`);
  
  const totalTime = (Date.now() - startTime) / 1000;
  
  // CRITICAL FIX: Atomic cursor save - mark complete ONLY after all writes confirmed
  // Also confirms GCS position so restart will be from GCS-confirmed point
  atomicCursor.saveAtomic({
    last_before: before,
    total_updates: totalUpdates,
    total_events: totalEvents,
    complete: true,
    completed_at: new Date().toISOString(),
    min_time: minTime,
    max_time: maxTime,
  });
  
  // Confirm GCS for the final position
  if (GCS_MODE) {
    atomicCursor.confirmGCS(before, totalUpdates, totalEvents);
    console.log(`   ✅ GCS cursor confirmed at final position${shardLabel}`);
  }
  
  // Structured log: synchronizer complete
  logSynchronizer('complete', {
    migrationId,
    synchronizerId,
    shardIndex,
    minTime,
    maxTime,
    totalUpdates,
    totalEvents,
    elapsedSeconds: totalTime.toFixed(1),
    extra: {
      avg_throughput: Math.round(totalUpdates / totalTime),
      batch_count: batchCount,
    },
  });
  
  logCursor('completed', {
    migrationId,
    synchronizerId,
    shardIndex,
    lastBefore: before,
    totalUpdates,
    totalEvents,
    complete: true,
  });
  
  console.log(`   ⏱️ Completed in ${totalTime.toFixed(1)}s (${Math.round(totalUpdates / totalTime)}/s avg)${shardLabel}`);
  
  return { updates: totalUpdates, events: totalEvents };
}

/**
 * Calculate time slice for a shard
 * Divides the time range into equal parts using integer math to avoid floating point issues
 */
function calculateShardTimeRange(minTime, maxTime, shardIndex, shardTotal) {
  const minMs = new Date(minTime).getTime();
  const maxMs = new Date(maxTime).getTime();
  const rangeMs = maxMs - minMs;

  if (!Number.isFinite(minMs) || !Number.isFinite(maxMs) || rangeMs <= 0) {
    throw new Error(`[sharding] invalid time range: minTime=${minTime}, maxTime=${maxTime}`);
  }

  // Use integer division to avoid floating point precision issues.
  // Shards work backwards in time (maxTime to minTime)
  // Shard 0 gets the most recent slice, shard N-1 gets the oldest.
  const shardMaxMsRaw = maxMs - Math.floor((shardIndex * rangeMs) / shardTotal);
  const shardMinMs = maxMs - Math.floor(((shardIndex + 1) * rangeMs) / shardTotal);

  // Avoid boundary duplicates between adjacent shards by making the upper bound exclusive-ish.
  const shardMaxMs = shardIndex === 0 ? shardMaxMsRaw : Math.max(shardMinMs, shardMaxMsRaw - 1);

  return {
    minTime: new Date(shardMinMs).toISOString(),
    maxTime: new Date(shardMaxMs).toISOString(),
  };
}

/**
 * Check if ALL migrations are fully complete (all cursors marked complete)
 */
async function areAllMigrationsComplete() {
  // Detect all available migrations
  const allMigrations = await detectMigrations();
  
  if (allMigrations.length === 0) {
    return { complete: true, pendingMigrations: [] };
  }
  
  const pendingMigrations = [];
  
  for (const migrationId of allMigrations) {
    const info = await getMigrationInfo(migrationId);
    if (!info) continue;
    
    const ranges = info.record_time_range || [];
    for (const range of ranges) {
      const synchronizerId = range.synchronizer_id;
      
      // For sharded setups, check all shards
      if (SHARD_TOTAL > 1) {
        for (let shard = 0; shard < SHARD_TOTAL; shard++) {
          const cursor = loadCursor(migrationId, synchronizerId, shard);
          if (!cursor?.complete) {
            pendingMigrations.push({ migrationId, synchronizerId, shard });
          }
        }
      } else {
        const cursor = loadCursor(migrationId, synchronizerId, null);
        if (!cursor?.complete) {
          pendingMigrations.push({ migrationId, synchronizerId, shard: null });
        }
      }
    }
  }
  
  return {
    complete: pendingMigrations.length === 0,
    pendingMigrations,
    totalMigrations: allMigrations.length,
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
  console.log("   INSECURE_TLS:", INSECURE_TLS ? 'ENABLED (unsafe)' : 'disabled');
  console.log("=".repeat(80));
  
  // ─────────────────────────────────────────────────────────────
  // GCS / DISK MODE CONFIGURATION
  // ─────────────────────────────────────────────────────────────
  try {
    // Initialize parquet writer with current env vars (after dotenv loaded)
    if (USE_PARQUET) {
      parquetWriter.initParquetWriter();
    }
    
    if (GCS_MODE) {
      // GCS mode requires bucket
      validateGCSBucket(true);
      console.log("\n🔍 Running GCS preflight checks...");
      runPreflightChecks({ quick: false, throwOnFail: true });
      console.log("\n☁️  GCS Mode ENABLED:");
      console.log(`   Bucket: gs://${process.env.GCS_BUCKET}/`);
      console.log("   Local scratch: /tmp/ledger_raw");
      console.log("   Files are uploaded to GCS immediately after creation");
    } else {
      console.log(`\n📂 Disk Mode: Writing to ${BASE_DATA_DIR}`);
      if (process.env.GCS_BUCKET) {
        console.log(`   GCS bucket configured: gs://${process.env.GCS_BUCKET}/`);
        console.log("   Uploads disabled (GCS_ENABLED=false) - writing to local disk only");
      } else {
        console.log("   GCS not configured - writing to local disk only");
      }
    }
  } catch (err) {
    logFatal('gcs_preflight_failed', err);
    throw err;
  }
  
  console.log("\n⚙️  Auto-Tuning Configuration:");
  console.log(`   Parallel Fetches: ${dynamicParallelFetches} (range: ${MIN_PARALLEL_FETCHES}-${MAX_PARALLEL_FETCHES})`);
  console.log(`   Decode: main-thread (no worker pool overhead)`);
  console.log(`   Tune Window: ${FETCH_TUNE_WINDOW_MS/1000}s | Latency thresholds: ${LATENCY_LOW_MS}ms / ${LATENCY_HIGH_MS}ms / ${LATENCY_CRITICAL_MS}ms`);
  console.log(`   FLUSH_EVERY_BATCHES: ${FLUSH_EVERY_BATCHES}`);
  if (isSharded) {
    console.log(`   SHARDING: Shard ${SHARD_INDEX} of ${SHARD_TOTAL} (0-indexed)`);
  }
  if (TARGET_MIGRATION) {
    console.log(`   TARGET_MIGRATION: ${TARGET_MIGRATION} only`);
  }
  console.log("   Processing: Migrations sequentially (0 → 1 → 2 → 3...) ");
  console.log("   CURSOR_DIR:", CURSOR_DIR);
  console.log("=".repeat(80));

  // Ensure cursor directory exists
  mkdirSync(CURSOR_DIR, { recursive: true });

  let grandTotalUpdates = 0;
  let grandTotalEvents = 0;
  const grandStartTime = Date.now();

  // If new migrations appear mid-run, loop and pick them up.
  const processedMigrations = new Set();
  const MAX_MIGRATION_RESCAN_ROUNDS = 10;

  for (let round = 0; round < MAX_MIGRATION_RESCAN_ROUNDS; round++) {
    let migrations = await detectMigrations();

    // Filter to target migration if specified
    if (TARGET_MIGRATION) {
      migrations = migrations.filter(id => id === TARGET_MIGRATION);
      if (!migrations.length) {
        console.log(`⚠️ Target migration ${TARGET_MIGRATION} not found. Exiting.`);
        return { success: false, allMigrationsComplete: false };
      }
    }

    // Only process migrations we haven't processed yet
    const pending = migrations.filter(id => !processedMigrations.has(id));
    if (pending.length === 0) break;

    for (const migrationId of pending) {
      processedMigrations.add(migrationId);

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

        const minMs = new Date(minTime).getTime();
        const maxMs = new Date(maxTime).getTime();
        if (!Number.isFinite(minMs) || !Number.isFinite(maxMs) || minMs >= maxMs) {
          throw new Error(`[range] Invalid time bounds for migration ${migrationId}: min=${minTime} max=${maxTime}`);
        }

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
          isSharded ? SHARD_INDEX : null,
        );
        grandTotalUpdates += updates;
        grandTotalEvents += events;
      }

      console.log(`✅ Completed migration ${migrationId}${shardLabel}`);
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
  
  // Structured run summary
  logSummary({
    success: true,
    totalUpdates: grandTotalUpdates,
    totalEvents: grandTotalEvents,
    totalTimeSeconds: parseFloat(grandTotalTime),
    avgThroughput: Math.round(grandTotalUpdates / parseFloat(grandTotalTime)),
    migrationsProcessed: processedMigrations.size,
    allComplete: false, // Will be updated below
    pendingCount: 0,
  });

  // Check if ALL migrations are complete
  const completionStatus = await areAllMigrationsComplete();

  if (!completionStatus.complete) {
    console.log(`\n⚠️ Not all migrations are complete yet:`);
    const pendingByMigration = {};
    for (const p of completionStatus.pendingMigrations) {
      if (!pendingByMigration[p.migrationId]) pendingByMigration[p.migrationId] = [];
      pendingByMigration[p.migrationId].push(p);
    }
    for (const [mig, items] of Object.entries(pendingByMigration)) {
      console.log(`   • Migration ${mig}: ${items.length} cursor(s) pending`);
    }
    console.log(`\n   Live updates will NOT start until all migrations are backfilled.`);
  }

  return {
    success: true,
    totalUpdates: grandTotalUpdates,
    totalEvents: grandTotalEvents,
    allMigrationsComplete: completionStatus.complete,
    pendingMigrations: completionStatus.pendingMigrations,
  };
}

/**
 * Start live updates ingestion after backfill completes
 */
async function startLiveUpdates() {
  const { spawn } = await import('child_process');
  const liveUpdatesScript = join(__dirname, 'fetch-updates.js');
  
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

// Graceful shutdown handler
let isShuttingDown = false;

async function gracefulShutdown(signal) {
  if (isShuttingDown) return;
  isShuttingDown = true;
  
  console.log(`\n🛑 Received ${signal}. Shutting down gracefully...`);
  
  try {
    console.log('   Flushing pending writes...');
    await flushAll();
    await waitForWrites();
    await shutdown();
    // decode pool removed — main-thread decode
    console.log('✅ Graceful shutdown complete');
    process.exit(0);
  } catch (err) {
    console.error('❌ Error during shutdown:', err.message);
    process.exit(1);
  }
}

// Handle multiple termination signals
process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));

// Catch uncaught exceptions to prevent exit code null
process.on('uncaughtException', async (err) => {
  console.error('\n💥 Uncaught exception:', err.message);
  console.error(err.stack);
  
  try {
    await flushAll();
    await waitForWrites();
    await shutdown();
  } catch (shutdownErr) {
    console.error('Error during emergency shutdown:', shutdownErr.message);
  }
  
  process.exit(1);
});

// Handle unhandled promise rejections
process.on('unhandledRejection', (reason, promise) => {
  console.error('\n💥 Unhandled rejection at:', promise);
  console.error('Reason:', reason);
  // Let the uncaughtException handler deal with it
  throw reason;
});

// Run backfill, then start live updates ONLY if all migrations are complete
runBackfill()
  .then(async (result) => {
    if (result?.success && result?.allMigrationsComplete) {
      // Small delay to ensure all file handles are released
      await new Promise(resolve => setTimeout(resolve, 1000));
      await startLiveUpdates();
    } else if (result?.success && !result?.allMigrationsComplete) {
      console.log(`\n${"═".repeat(80)}`);
      console.log(`⏸️ Backfill for target migration complete, but other migrations remain.`);
      console.log(`   Live updates will start once ALL migrations are backfilled.`);
      console.log(`   Run backfill again without TARGET_MIGRATION to process remaining migrations.`);
      console.log(`${"═".repeat(80)}\n`);
      process.exit(0);
    }
  })
  .catch(async err => {
    console.error('\n❌ FATAL:', err.message);
    console.error(err.stack);
    await flushAll();
    await waitForWrites();
    await shutdown();
    // decode pool removed — main-thread decode
    process.exit(1);
  });
