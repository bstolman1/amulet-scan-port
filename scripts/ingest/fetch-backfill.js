#!/usr/bin/env node
/**
 * ⚠️  DEPRECATED — DO NOT USE FOR NEW BACKFILL OPERATIONS  ⚠️
 *
 * This script has a SYSTEMATIC DATA-LOSS BUG: the cursor advancement uses
 *   `new Date(...).setMilliseconds(getMilliseconds() - 1)`
 * which truncates Canton's microsecond-precision record_time to milliseconds
 * (JS `Date` only stores ms), then jumps the cursor by a full millisecond.
 * Any records that share a millisecond with the batch's earliest record but
 * weren't included in that batch are permanently SKIPPED.
 *
 * Empirically observed loss: ~0.1-0.3% of updates per backfill day, with
 * heavier loss on busier days (more records per ms → more boundary
 * collisions). Discovered 2026-04-25 by verify-scan-completeness.js,
 * which compared GCS counts to Scan API counts across 5 sampled M4
 * backfill days — every one drifted by exactly this pattern.
 *
 * Use `reingest-updates.js` instead for ALL historical fetches. It uses
 * /v2/updates with full ISO precision in the cursor and has been verified
 * to produce exact counts matching Scan API (3 sampled days, 0 drift).
 *
 * See scripts/ingest/DEPRECATED.md for the full incident write-up,
 * remediation plan, and migration path.
 *
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
import dotenv from 'dotenv';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { promisify } from 'util';
import { exec as execCb, execFile as execFileCb } from 'child_process';

const execAsync = promisify(execCb);
// FIX #6: Use execFile (safer, no shell injection) for gsutil calls
const execFileAsync = promisify(execFileCb);

const __filename_early = fileURLToPath(import.meta.url);
const __dirname_early = dirname(__filename_early);
dotenv.config({ path: join(__dirname_early, '.env') });

import axios from 'axios';
import { Agent as HttpAgent } from 'http';
import { Agent as HttpsAgent } from 'https';
import { existsSync, mkdirSync, statfsSync } from 'fs';

import v8 from 'v8';
import Piscina from 'piscina';
import { normalizeUpdate, normalizeEvent, getPartitionPath, flattenEventsInTreeOrder } from './data-schema.js';

// Parse command line arguments
const args = process.argv.slice(2);
const KEEP_RAW = args.includes('--keep-raw') || args.includes('--raw');
const RAW_ONLY = args.includes('--raw-only') || args.includes('--legacy');
const USE_PARQUET = !RAW_ONLY;
const USE_BINARY = KEEP_RAW || RAW_ONLY;

import * as parquetWriter from './write-parquet.js';
import * as binaryWriter from './write-binary.js';

// CRITICAL FIX #1: Import explicit fetch result types
import {
  FetchResultType,
  successData,
  successEmpty,
  failure,
  isRetryableError,
  // FIX #8: retryFetch and assertSuccess are imported by fetch-result.js but
  // never used here — retryWithBackoff is the authoritative retry mechanism in
  // this file. Removing the dead imports prevents confusion about which path runs.
} from './fetch-result.js';

// CRITICAL FIX #2: Import atomic cursor operations
import { AtomicCursor, atomicWriteFile, loadCursorLegacy, isCursorComplete } from './atomic-cursor.js';

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

// ─── Unified writer helpers ────────────────────────────────────────────────
// FIX: Run binary and Parquet writers concurrently and independently.
// Previously USE_BINARY write ran first with `await`; if it threw, the
// USE_PARQUET write never ran — a binary failure silently dropped the
// Parquet output for that batch. Now both are fired in parallel and each
// error is re-thrown so the caller sees failures from either path.
async function bufferUpdates(updates) {
  const tasks = [];
  if (USE_BINARY)  tasks.push(binaryWriter.bufferUpdates(updates));
  if (USE_PARQUET) tasks.push(parquetWriter.bufferUpdates(updates));
  if (tasks.length === 0) return;
  const results = await Promise.allSettled(tasks);
  const errs = results.filter(r => r.status === 'rejected').map(r => r.reason);
  if (errs.length > 0) throw errs[0];  // surface first error; both were attempted
}

async function bufferEvents(events) {
  const tasks = [];
  if (USE_BINARY)  tasks.push(binaryWriter.bufferEvents(events));
  if (USE_PARQUET) tasks.push(parquetWriter.bufferEvents(events));
  if (tasks.length === 0) return;
  const results = await Promise.allSettled(tasks);
  const errs = results.filter(r => r.status === 'rejected').map(r => r.reason);
  if (errs.length > 0) throw errs[0];
}

async function flushAll() {
  const results = [];
  if (USE_BINARY) results.push(...await binaryWriter.flushAll());
  if (USE_PARQUET) results.push(...await parquetWriter.flushAll());
  return results;
}

function getBufferStats() {
  const stats = USE_PARQUET ? parquetWriter.getBufferStats() : { updates: 0, events: 0, pendingWrites: 0 };
  if (USE_BINARY) {
    const binaryStats = binaryWriter.getBufferStats();
    stats.binaryPendingWrites = binaryStats.pendingWrites;
    stats.binaryQueuedJobs = binaryStats.queuedJobs;
  }
  return stats;
}

async function waitForWrites() {
  if (USE_BINARY) await binaryWriter.waitForWrites();
  if (USE_PARQUET) await parquetWriter.waitForWrites();
}

async function shutdown() {
  if (USE_BINARY) await binaryWriter.shutdown();
  if (USE_PARQUET) await parquetWriter.shutdown();
}

import {
  IntegrityCursor,
  WriteVerifier,
  DedupTracker,
  EmptyResponseHandler,
  BatchIntegrityTracker,
} from './bulletproof-backfill.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const INSECURE_TLS = process.env.INSECURE_TLS === 'true';

const SCAN_URL = process.env.SCAN_URL || 'https://scan.sv-1.global.canton.network.sync.global/api/scan';
let activeScanUrl = SCAN_URL;

const BACKFILL_SCAN_ENDPOINTS = [
  SCAN_URL,
  'https://scan.sv-1.global.canton.network.digitalasset.com/api/scan',
  'https://scan.sv-2.global.canton.network.digitalasset.com/api/scan',
  'https://scan.sv-1.global.canton.network.cumberland.io/api/scan',
  'https://scan.sv-2.global.canton.network.cumberland.io/api/scan',
  'https://scan.sv-1.global.canton.network.fivenorth.io/api/scan',
  'https://scan.sv-1.global.canton.network.tradeweb.com/api/scan',
  'https://scan.sv-1.global.canton.network.proofgroup.xyz/api/scan',
  'https://scan.sv-1.global.canton.network.lcv.mpch.io/api/scan',
  'https://scan.sv-1.global.canton.network.mpch.io/api/scan',
  'https://scan.sv-1.global.canton.network.orb1lp.mpch.io/api/scan',
  'https://scan.sv.global.canton.network.sv-nodeops.com/api/scan',
  'https://scan.sv-1.global.canton.network.c7.digital/api/scan',
];

const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 1000;

import { getBaseDataDir, getCursorDir, isGCSMode, logPathConfig, validateGCSBucket } from './path-utils.js';
import { runPreflightChecks } from './gcs-preflight.js';

const BASE_DATA_DIR = getBaseDataDir();
const CURSOR_DIR = getCursorDir();
const GCS_MODE = isGCSMode();
const FLUSH_EVERY_BATCHES = parseInt(process.env.FLUSH_EVERY_BATCHES) || 5;
const GCS_CHECKPOINT_INTERVAL = parseInt(process.env.GCS_CHECKPOINT_INTERVAL) || 50;

import { drainUploads, getUploadQueue } from './gcs-upload-queue.js';

const GCS_CURSOR_BACKUP_INTERVAL = parseInt(process.env.GCS_CURSOR_BACKUP_INTERVAL) || 3;
let gcsCursorBackupCounter = 0;
let gcsCursorBackupConsecutiveFailures = 0;
const GCS_CURSOR_BACKUP_MAX_FAILURES = parseInt(process.env.GCS_CURSOR_BACKUP_MAX_FAILURES) || 5;

/**
 * Backup cursor to GCS (non-blocking async).
 *
 * FIX #6: Replaced synchronous execSync (which blocked the event loop for up
 * to 10 seconds per call) with async execFileAsync so HTTP I/O, pending writes,
 * and GCS uploads can continue while the gsutil copy runs. Uses execFile instead
 * of exec to avoid shell injection risks from cursorPath.
 */
async function backupCursorToGCS(cursorPath) {
  const GCS_BUCKET = process.env.GCS_BUCKET;
  if (!GCS_BUCKET) return;

  const cursorName = cursorPath.split('/').pop();
  const gcsPath = `gs://${GCS_BUCKET}/cursors/${cursorName}`;

  try {
    // FIX #6: execFileAsync — async, no event loop block, no shell injection
    await execFileAsync('gsutil', ['-q', 'cp', cursorPath, gcsPath], { timeout: 15000 });
    gcsCursorBackupConsecutiveFailures = 0;
    console.log(`  ☁️ Cursor backed up: ${gcsPath}`);
  } catch (err) {
    gcsCursorBackupConsecutiveFailures++;
    const stderr = err.stderr?.toString?.() || '';
    console.warn(
      `  ⚠️ Failed to backup cursor to GCS ` +
      `(${gcsCursorBackupConsecutiveFailures}/${GCS_CURSOR_BACKUP_MAX_FAILURES}): ${err.message}`
    );
    if (stderr) console.warn(`     gsutil stderr: ${stderr.trim()}`);

    if (gcsCursorBackupConsecutiveFailures >= GCS_CURSOR_BACKUP_MAX_FAILURES) {
      const fatalMsg =
        `🔴 FATAL: GCS cursor backup failed ${gcsCursorBackupConsecutiveFailures} consecutive times. ` +
        `Cursor progress is NOT being persisted to cloud — risk of major data re-processing on crash.`;
      console.error(fatalMsg);
      throw new Error(fatalMsg);
    }
  }
}

/**
 * Restore cursor files from GCS if they don't exist locally.
 */
async function restoreCursorsFromGCS() {
  const GCS_BUCKET = process.env.GCS_BUCKET;
  if (!GCS_BUCKET) return;

  try {
    const { stdout } = await execAsync(
      `gsutil ls gs://${GCS_BUCKET}/cursors/cursor-*.json 2>/dev/null`,
      { timeout: 15000 }
    );

    const output = stdout.trim();
    if (!output) {
      console.log('  ℹ️ No cursor backups found in GCS');
      return;
    }

    const gcsCursors = output.split('\n').filter(Boolean);
    let restored = 0;

    for (const gcsPath of gcsCursors) {
      const fileName = gcsPath.split('/').pop();
      const localPath = join(CURSOR_DIR, fileName);
      if (existsSync(localPath)) continue;

      try {
        await execFileAsync('gsutil', ['-q', 'cp', gcsPath, localPath], { timeout: 15000 });
        restored++;
        console.log(`  ☁️ Restored cursor from GCS: ${fileName}`);
      } catch (cpErr) {
        console.warn(`  ⚠️ Failed to restore ${fileName}: ${cpErr.message}`);
      }
    }

    if (restored > 0) {
      console.log(`  ✅ Restored ${restored} cursor(s) from GCS`);
    } else {
      console.log('  ℹ️ All GCS cursors already exist locally');
    }
  } catch (err) {
    if (!err.message?.includes('matched no objects')) {
      console.warn(`  ⚠️ GCS cursor restore failed: ${err.message}`);
    }
  }
}

// ==========================================
// MEMORY-AWARE THROTTLING
// ==========================================
const HEAP_PRESSURE_THRESHOLD = parseFloat(process.env.HEAP_PRESSURE_THRESHOLD) || 0.80;
const HEAP_CRITICAL_THRESHOLD = parseFloat(process.env.HEAP_CRITICAL_THRESHOLD) || 0.90;
const HEAP_CHECK_INTERVAL_MS = parseInt(process.env.HEAP_CHECK_INTERVAL_MS) || 5000;

let lastHeapCheck = 0;
let heapPressureEvents = 0;

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

function checkMemoryPressure() {
  const now = Date.now();
  if (now - lastHeapCheck < HEAP_CHECK_INTERVAL_MS) return { pressure: false, critical: false };
  lastHeapCheck = now;

  const heap = getHeapUsage();
  const pressure = heap.ratio > HEAP_PRESSURE_THRESHOLD;
  const critical = heap.ratio > HEAP_CRITICAL_THRESHOLD;
  if (pressure) heapPressureEvents++;

  let diskPressure = false;
  if (isGCSMode()) {
    try {
      const stats = statfsSync('/tmp');
      const freeBytes = stats.bfree * stats.bsize;
      const freeMB = Math.round(freeBytes / 1024 / 1024);
      const totalBytes = stats.blocks * stats.bsize;
      const usedPct = ((1 - freeBytes / totalBytes) * 100).toFixed(1);
      if (freeMB < 500) {
        diskPressure = true;
        console.warn(`   💾 CRITICAL: /tmp only ${freeMB}MB free (${usedPct}% used)`);
      } else if (freeMB < 1024) {
        console.warn(`   💾 WARNING: /tmp only ${freeMB}MB free (${usedPct}% used)`);
      }
    } catch { /* statfsSync not available on all platforms */ }
  }

  return { pressure: pressure || diskPressure, critical, heap, diskPressure };
}

async function waitForMemoryRelief(shardLabel = '') {
  const maxWaitMs = 60000;
  const startWait = Date.now();
  let drainCycles = 0;

  while (Date.now() - startWait < maxWaitMs) {
    const heap = getHeapUsage();
    let diskOk = true;
    if (isGCSMode()) {
      try {
        const stats = statfsSync('/tmp');
        diskOk = Math.round((stats.bfree * stats.bsize) / 1024 / 1024) >= 500;
      } catch { diskOk = true; }
    }

    if (heap.ratio <= HEAP_PRESSURE_THRESHOLD * 0.9 && diskOk) {
      if (drainCycles > 0) {
        console.log(`   ✅ Memory pressure relieved: ${heap.usedMB}MB / ${heap.limitMB}MB${shardLabel}`);
      }
      return;
    }

    drainCycles++;
    console.log(`   ⚠️ Memory pressure (${heap.usedMB}MB / ${heap.limitMB}MB = ${(heap.ratio * 100).toFixed(1)}%) - draining (cycle ${drainCycles})...${shardLabel}`);
    await flushAll();
    await waitForWrites();
    if (GCS_MODE) await drainUploads();
    if (global.gc) global.gc();
    await new Promise(r => setTimeout(r, 2000));
  }

  const heap = getHeapUsage();
  console.warn(`   ⚠️ Memory pressure timeout after ${maxWaitMs}ms - continuing at ${heap.usedMB}MB / ${heap.limitMB}MB${shardLabel}`);
}

// Sharding configuration
const SHARD_INDEX = parseInt(process.env.SHARD_INDEX) || 0;
const SHARD_TOTAL = parseInt(process.env.SHARD_TOTAL) || 1;
const TARGET_MIGRATION = process.env.TARGET_MIGRATION ? parseInt(process.env.TARGET_MIGRATION) : null;
const START_MIGRATION = process.env.START_MIGRATION != null ? parseInt(process.env.START_MIGRATION) : null;
const END_MIGRATION = process.env.END_MIGRATION != null ? parseInt(process.env.END_MIGRATION) : null;

function assertConfig(condition, message) {
  if (!condition) throw new Error(`[config] ${message}`);
}

assertConfig(Number.isFinite(SHARD_TOTAL) && SHARD_TOTAL >= 1, `SHARD_TOTAL must be >= 1 (got ${process.env.SHARD_TOTAL})`);
assertConfig(Number.isFinite(SHARD_INDEX) && SHARD_INDEX >= 0 && SHARD_INDEX < SHARD_TOTAL, `SHARD_INDEX must be between 0 and SHARD_TOTAL-1 (got ${process.env.SHARD_INDEX})`);
assertConfig(Number.isFinite(BATCH_SIZE) && BATCH_SIZE >= 1 && BATCH_SIZE <= 1000, `BATCH_SIZE must be 1..1000 (got ${process.env.BATCH_SIZE})`);

// ==========================================
// AUTO-TUNING: PARALLEL FETCHES
// ==========================================
const BASE_PARALLEL_FETCHES = parseInt(process.env.PARALLEL_FETCHES) || 8;
const MIN_PARALLEL_FETCHES = parseInt(process.env.MIN_PARALLEL_FETCHES) || 2;
const MAX_PARALLEL_FETCHES = parseInt(process.env.MAX_PARALLEL_FETCHES) || 24;

let dynamicParallelFetches = Math.min(Math.max(BASE_PARALLEL_FETCHES, MIN_PARALLEL_FETCHES), MAX_PARALLEL_FETCHES);

assertConfig(Number.isFinite(BASE_PARALLEL_FETCHES) && BASE_PARALLEL_FETCHES >= 1, `PARALLEL_FETCHES must be >= 1`);
assertConfig(Number.isFinite(MIN_PARALLEL_FETCHES) && MIN_PARALLEL_FETCHES >= 1, `MIN_PARALLEL_FETCHES must be >= 1`);
assertConfig(Number.isFinite(MAX_PARALLEL_FETCHES) && MAX_PARALLEL_FETCHES >= MIN_PARALLEL_FETCHES, `MAX_PARALLEL_FETCHES must be >= MIN_PARALLEL_FETCHES`);

const LATENCY_LOW_MS = parseInt(process.env.LATENCY_LOW_MS) || 500;
const LATENCY_HIGH_MS = parseInt(process.env.LATENCY_HIGH_MS) || 2000;
const LATENCY_CRITICAL_MS = parseInt(process.env.LATENCY_CRITICAL_MS) || 5000;
const FETCH_TUNE_WINDOW_MS = 15_000;

let fetchStats = {
  windowStart: Date.now(),
  successCount: 0,
  retry503Count: 0,
  latencies: [],
  avgLatency: 0,
  p95Latency: 0,
  consecutiveStableWindows: 0,
  errorCount: 0,
};

// ==========================================
// BATCHED DECODE WORKER POOL
// ==========================================
const DECODE_BATCH_SIZE = parseInt(process.env.DECODE_BATCH_SIZE) || 250;
const DECODE_WORKERS = parseInt(process.env.DECODE_WORKERS) || 4;

let decodePool = null;
let decodePoolFailed = false;

function getDecodePool() {
  if (!decodePool && !decodePoolFailed) {
    try {
      decodePool = new Piscina({
        filename: new URL('./decode-worker.js', import.meta.url).href,
        minThreads: 2,
        maxThreads: DECODE_WORKERS,
      });
      console.log(`   ✅ Batched decode pool ready (${DECODE_WORKERS} workers, batch=${DECODE_BATCH_SIZE})`);
    } catch (err) {
      console.error(`   ❌ Failed to create decode pool: ${err.message}`);
      decodePoolFailed = true;
    }
  }
  return decodePool;
}

// ==========================================
// HTTP CLIENT
// ==========================================
const client = axios.create({
  baseURL: activeScanUrl,
  httpAgent: new HttpAgent({ keepAlive: true, keepAliveMsecs: 60000, maxSockets: MAX_PARALLEL_FETCHES * 4 }),
  httpsAgent: new HttpsAgent({ keepAlive: true, keepAliveMsecs: 60000, rejectUnauthorized: !INSECURE_TLS, maxSockets: MAX_PARALLEL_FETCHES * 4 }),
  timeout: 180000,
});

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

const SCAN_ROTATE_COOLDOWN_MS = parseInt(process.env.SCAN_ROTATE_COOLDOWN_MS || '8000', 10);
let lastScanRotationAt = 0;

const TRANSIENT_NETWORK_CODES = new Set([
  'ERR_BAD_RESPONSE', 'ECONNRESET', 'ETIMEDOUT', 'ECONNABORTED',
  'EPIPE', 'ENOTFOUND', 'EAI_AGAIN', 'EPROTO',
]);

function isTransientFetchError(error) {
  const status = error?.response?.status;
  const code = String(error?.code || '').toUpperCase();
  const message = String(error?.message || '');
  if (Number.isFinite(status) && (status === 429 || (status >= 500 && status <= 599))) return true;
  if (TRANSIENT_NETWORK_CODES.has(code)) return true;
  return /timeout|socket|hang up|ECONNRESET|ETIMEDOUT|ssl3_get_record|wrong version number/i.test(message);
}

function buildScanEndpointRotation() {
  const seen = new Set();
  const ordered = [];
  for (const url of BACKFILL_SCAN_ENDPOINTS) {
    if (!url || seen.has(url)) continue;
    seen.add(url);
    ordered.push(url);
  }
  return ordered;
}

const SCAN_PROBE_TIMEOUT_MS = parseInt(process.env.SCAN_PROBE_TIMEOUT_MS || '10000', 10);
let scanEndpointRotation = buildScanEndpointRotation();
let currentScanEndpointIndex = Math.max(0, scanEndpointRotation.indexOf(activeScanUrl));

function rotateScanEndpoint(reason = '') {
  if (scanEndpointRotation.length < 2) return false;
  currentScanEndpointIndex = (currentScanEndpointIndex + 1) % scanEndpointRotation.length;
  activeScanUrl = scanEndpointRotation[currentScanEndpointIndex];
  client.defaults.baseURL = activeScanUrl;
  console.warn(`   🔁 Rotating Scan endpoint -> ${activeScanUrl}${reason ? ` (${reason})` : ''}`);
  return true;
}

function maybeRotateScanEndpoint(error, context = '') {
  if (!isTransientFetchError(error)) return false;
  const now = Date.now();
  if (now - lastScanRotationAt < SCAN_ROTATE_COOLDOWN_MS) return false;
  lastScanRotationAt = now;
  const reason = `${context || 'fetch'}: ${error?.code || error?.response?.status || error?.message || 'transient error'}`;
  return rotateScanEndpoint(reason);
}

async function probeScanEndpoints() {
  if (!scanEndpointRotation.length) return;
  console.log(`\n🔎 Probing ${scanEndpointRotation.length} Scan endpoints (GET /v0/dso)...`);

  const probeResults = await Promise.allSettled(
    scanEndpointRotation.map(async (url) => {
      const started = Date.now();
      try {
        const response = await client.get('/v0/dso', { baseURL: url, timeout: SCAN_PROBE_TIMEOUT_MS, headers: { Accept: 'application/json' } });
        return { url, healthy: response.status >= 200 && response.status < 300, status: response.status, latencyMs: Date.now() - started };
      } catch (error) {
        return { url, healthy: false, status: error?.response?.status || null, error: error?.code || error?.message || 'probe_failed', latencyMs: Date.now() - started };
      }
    })
  );

  const results = probeResults.filter(r => r.status === 'fulfilled').map(r => r.value);
  const healthy = results.filter(r => r.healthy).sort((a, b) => a.latencyMs - b.latencyMs);

  for (const r of results) {
    const icon = r.healthy ? '✅' : '❌';
    const detail = r.healthy ? `HTTP ${r.status} in ${r.latencyMs}ms` : `${r.error || `HTTP ${r.status || 'n/a'}`} (${r.latencyMs}ms)`;
    console.log(`   ${icon} ${r.url} — ${detail}`);
  }

  if (healthy.length === 0) {
    console.warn('   ⚠️ No healthy Scan endpoints found; keeping original rotation list.');
    return;
  }

  scanEndpointRotation = healthy.map(r => r.url);

  if (!scanEndpointRotation.includes(activeScanUrl)) {
    const previous = activeScanUrl;
    activeScanUrl = scanEndpointRotation[0];
    client.defaults.baseURL = activeScanUrl;
    currentScanEndpointIndex = 0;
    console.warn(`   🔁 Active endpoint was unhealthy. Switching ${previous} -> ${activeScanUrl}`);
  } else {
    currentScanEndpointIndex = scanEndpointRotation.indexOf(activeScanUrl);
  }

  console.log(`   ✅ Endpoint rotation initialized with ${scanEndpointRotation.length} healthy endpoint(s).`);
}

/**
 * Retry with exponential backoff + fetch stats tracking.
 * CRITICAL: Throws on exhausted retries — caller must handle.
 */
async function retryWithBackoff(fn, options = {}) {
  const {
    maxRetries = 5,
    baseDelay = 1000,
    maxDelay = 30000,
    context = '',
    shouldRetry = (error) => isTransientFetchError(error) || isRetryableError(error),
  } = options;

  let lastError;

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      const callStart = Date.now();
      const result = await fn();
      const latency = Date.now() - callStart;
      fetchStats.latencies.push(latency);
      if (fetchStats.latencies.length > 100) fetchStats.latencies.shift();
      fetchStats.successCount++;
      return result;
    } catch (error) {
      lastError = error;
      const status = error.response?.status;
      if (status === 503 || status === 429 || error.code === 'ERR_BAD_RESPONSE') fetchStats.retry503Count++;
      fetchStats.errorCount = (fetchStats.errorCount || 0) + 1;

      const retryable = shouldRetry(error);
      if (retryable && attempt >= 1) maybeRotateScanEndpoint(error, context || 'backfill');

      if (attempt === maxRetries || !retryable) {
        const contextStr = context ? `[${context}] ` : '';
        console.error(`❌ ${contextStr}FATAL: Fetch failed after ${attempt + 1} attempts. Error: ${error.code || status || error.message}`);
        throw error;
      }

      const exponentialDelay = Math.min(baseDelay * Math.pow(2, attempt), maxDelay);
      const delay = exponentialDelay + Math.random() * exponentialDelay * 0.3;
      console.log(`   ⏳ Retry ${attempt + 1}/${maxRetries} after ${Math.round(delay)}ms (${error.code || status || error.message})`);
      await sleep(delay);
    }
  }

  throw lastError || new Error('Unknown fetch error');
}

function resetFetchStats(now) {
  fetchStats.windowStart = now;
  fetchStats.successCount = 0;
  fetchStats.retry503Count = 0;
  fetchStats.errorCount = 0;
  fetchStats.latencies = [];
}

function maybeTuneParallelFetches(shardLabel = '') {
  const now = Date.now();
  if (now - fetchStats.windowStart < FETCH_TUNE_WINDOW_MS) return;

  const { successCount, retry503Count, latencies, errorCount = 0 } = fetchStats;
  const total = successCount + retry503Count + errorCount;
  if (total === 0) { resetFetchStats(now); return; }

  let avgLatency = 0, p95Latency = 0;
  if (latencies.length > 0) {
    const sorted = [...latencies].sort((a, b) => a - b);
    avgLatency = sorted.reduce((a, b) => a + b, 0) / sorted.length;
    p95Latency = sorted[Math.floor(sorted.length * 0.95)] || sorted[sorted.length - 1];
    fetchStats.avgLatency = avgLatency;
    fetchStats.p95Latency = p95Latency;
  }

  let action = null;
  const errorRate = (retry503Count + errorCount) / total;

  if (errorCount > 0 || retry503Count > 0) {
    if (dynamicParallelFetches > MIN_PARALLEL_FETCHES) {
      const old = dynamicParallelFetches;
      const reduction = errorCount > 2 ? 3 : (retry503Count >= 3 ? 2 : 1);
      dynamicParallelFetches = Math.max(MIN_PARALLEL_FETCHES, dynamicParallelFetches - reduction);
      console.log(`   🔧 Auto-tune${shardLabel}: ERRORS (${errorCount} errors, ${retry503Count} 503s) → PARALLEL ${old} → ${dynamicParallelFetches}`);
      action = 'down';
      fetchStats.consecutiveStableWindows = 0;
    }
  } else if (p95Latency > LATENCY_CRITICAL_MS || avgLatency > LATENCY_HIGH_MS) {
    if (dynamicParallelFetches > MIN_PARALLEL_FETCHES) {
      const old = dynamicParallelFetches;
      dynamicParallelFetches = Math.max(MIN_PARALLEL_FETCHES, dynamicParallelFetches - 1);
      console.log(`   🔧 Auto-tune${shardLabel}: HIGH LATENCY (avg=${avgLatency.toFixed(0)}ms, p95=${p95Latency.toFixed(0)}ms) → PARALLEL ${old} → ${dynamicParallelFetches}`);
      action = 'down';
    }
  } else if (errorCount === 0 && retry503Count === 0 && successCount >= 15 && avgLatency < LATENCY_LOW_MS && avgLatency > 0) {
    if (dynamicParallelFetches < MAX_PARALLEL_FETCHES) {
      const old = dynamicParallelFetches;
      dynamicParallelFetches = Math.min(MAX_PARALLEL_FETCHES, dynamicParallelFetches + (avgLatency < 300 ? 2 : 1));
      console.log(`   🔧 Auto-tune${shardLabel}: FAST+STABLE (avg=${avgLatency.toFixed(0)}ms) → PARALLEL ${old} → ${dynamicParallelFetches}`);
      action = 'up';
    }
  } else if (errorCount === 0 && retry503Count === 0 && successCount >= 20 && avgLatency < LATENCY_HIGH_MS) {
    fetchStats.consecutiveStableWindows++;
    if (fetchStats.consecutiveStableWindows >= 2 && dynamicParallelFetches < MAX_PARALLEL_FETCHES) {
      const old = dynamicParallelFetches;
      dynamicParallelFetches = Math.min(MAX_PARALLEL_FETCHES, dynamicParallelFetches + 1);
      console.log(`   🔧 Auto-tune${shardLabel}: STABLE x${fetchStats.consecutiveStableWindows} → PARALLEL ${old} → ${dynamicParallelFetches}`);
      action = 'up';
      fetchStats.consecutiveStableWindows = 0;
    }
  }

  if (action !== 'up') fetchStats.consecutiveStableWindows = 0;
  resetFetchStats(now);
}

/**
 * Load cursor from file (shard-aware).
 * Delegates to loadCursorLegacy which uses readCursorSafe for .bak recovery.
 */
function loadCursor(migrationId, synchronizerId, shardIndex = null) {
  return loadCursorLegacy(migrationId, synchronizerId, shardIndex);
}

/**
 * Sanitize string for filename.
 */
function sanitize(str) {
  return str.replace(/[^a-zA-Z0-9-_]/g, '_').substring(0, 50);
}

// FIX #3: saveCursor() removed — it was dead code (never called) that duplicated
// AtomicCursor with weaker safety guarantees. All cursor writes go through
// atomicCursor.saveAtomic() / beginTransaction() / commit() / rollback().

async function detectMigrations() {
  console.log('🔎 Detecting available migrations via /v0/backfilling/migration-info');
  const MIGRATION_DETECT_RETRIES = 3;
  const migrations = [];
  let id = 0;

  while (true) {
    let succeeded = false;
    let confirmed404 = false;

    for (let attempt = 1; attempt <= MIGRATION_DETECT_RETRIES; attempt++) {
      try {
        const res = await client.post('/v0/backfilling/migration-info', { migration_id: id });
        if (res.data?.record_time_range) {
          const ranges = res.data.record_time_range || [];
          const minTime = ranges[0]?.min || 'unknown';
          const maxTime = ranges[0]?.max || 'unknown';
          migrations.push(id);
          console.log(`  • migration_id=${id} ranges=${ranges.length} (${minTime} to ${maxTime})`);
          succeeded = true;
          break;
        } else {
          confirmed404 = true;
          break;
        }
      } catch (err) {
        const status = err.response?.status;
        if (status === 404) { confirmed404 = true; break; }
        const isTransient = [429, 500, 502, 503, 504].includes(status) || /timeout|ETIMEDOUT|ECONNRESET|socket hang up/i.test(err.message);
        if (isTransient && attempt < MIGRATION_DETECT_RETRIES) {
          const delay = 2000 * Math.pow(2, attempt - 1);
          console.warn(`   ⚠️ Transient error probing migration_id=${id} (attempt ${attempt}/${MIGRATION_DETECT_RETRIES}), retrying in ${delay}ms...`);
          await sleep(delay);
          continue;
        }
        console.error(`❌ Error probing migration_id=${id} after ${attempt} attempts:`, status, err.message);
        confirmed404 = true;
        break;
      }
    }

    if (confirmed404) break;
    if (succeeded) { id++; }
    else { console.warn(`   ⚠️ Could not confirm migration_id=${id}, stopping scan`); break; }
  }

  console.log(`✅ Found migrations: ${migrations.join(', ')}`);
  return migrations;
}

async function getMigrationInfo(migrationId) {
  try {
    const res = await client.post('/v0/backfilling/migration-info', { migration_id: migrationId });
    return res.data;
  } catch (err) {
    if (err.response?.status === 404) {
      console.log(`ℹ️  No backfilling info for migration_id=${migrationId} (404)`);
      return null;
    }
    throw err;
  }
}

let fetchCount = 0;
let firstCallLogged = false;

async function fetchBackfillBefore(migrationId, synchronizerId, before, atOrAfter) {
  const payload = { migration_id: migrationId, synchronizer_id: synchronizerId, before, count: BATCH_SIZE };
  if (atOrAfter) payload.at_or_after = atOrAfter;

  const thisCall = ++fetchCount;
  const startTime = Date.now();

  return await retryWithBackoff(async () => {
    const response = await client.post('/v0/backfilling/updates-before', payload);
    if (!firstCallLogged) {
      firstCallLogged = true;
      console.log(`   ✅ API connected: first call returned ${response.data?.transactions?.length || 0} txs in ${Date.now() - startTime}ms`);
    }
    return response.data;
  });
}

function getEventTime(txOrReassign) {
  return txOrReassign.record_time || txOrReassign.event?.record_time || txOrReassign.effective_at;
}

/**
 * Process backfill items using BATCHED worker pool decode.
 *
 * FIX #7: Worker batch results now have their errors[] array inspected and
 * logged. Per-transaction decode failures no longer silently disappear.
 */
async function processBackfillItems(transactions, migrationId) {
  const pool = getDecodePool();

  if (!pool) {
    // FIX #3: per-tx try/catch — one malformed tx cannot abort the whole batch.
    // Previously a throw from decodeInMainThread propagated directly, killing
    // the entire batch. Matches decode-worker's per-tx error isolation.
    const updates = [], events = [], errors = [];
    for (const tx of transactions) {
      try {
        const r = decodeInMainThread(tx, migrationId);
        if (!r) continue;
        if (r.update) updates.push(r.update);
        if (Array.isArray(r.events) && r.events.length > 0) events.push(...r.events);
      } catch (err) {
        const txId = tx?.update_id || tx?.transaction?.update_id || tx?.reassignment?.update_id || 'UNKNOWN';
        console.error(`   [decode-main] tx ${txId} failed: ${err.message}`);
        errors.push({ tx_id: txId, error: err.message });
      }
    }
    if (errors.length > 0) {
      console.warn(`   ⚠️ processBackfillItems (no-pool): ${errors.length}/${transactions.length} tx(s) failed decode`);
    }
    await Promise.all([bufferUpdates(updates), bufferEvents(events)]);
    return { updates: updates.length, events: events.length, errors };
  }

  const batchPromises = [];
  for (let i = 0; i < transactions.length; i += DECODE_BATCH_SIZE) {
    const chunk = transactions.slice(i, i + DECODE_BATCH_SIZE);
    batchPromises.push(
      pool.run({ txs: chunk, migrationId }).catch(err => {
        console.warn(`   ⚠️ Worker batch failed, main-thread fallback: ${err.message}`);
        // FIX #4: per-tx try/catch in fallback path; return real errors (not []).
        // Previously decodeInMainThread could throw inside the .catch() handler,
        // re-rejecting the promise and losing the chunk entirely. Also, errors
        // were always returned as [] — making fallback failures invisible.
        const updates = [], events = [], errors = [];
        for (const tx of chunk) {
          try {
            const r = decodeInMainThread(tx, migrationId);
            if (!r) continue;
            if (r.update) updates.push(r.update);
            if (Array.isArray(r.events) && r.events.length > 0) events.push(...r.events);
          } catch (decErr) {
            const txId = tx?.update_id || tx?.transaction?.update_id || tx?.reassignment?.update_id || 'UNKNOWN';
            console.error(`   [decode-fallback] tx ${txId} failed: ${decErr.message}`);
            errors.push({ tx_id: txId, error: decErr.message });
          }
        }
        return { updates, events, errors };
      })
    );
  }

  const batchResults = await Promise.all(batchPromises);

  const allUpdates = [], allEvents = [];
  for (const r of batchResults) {
    if (r.updates) allUpdates.push(...r.updates);
    if (r.events) allEvents.push(...r.events);
    // FIX #7: Surface per-transaction decode errors from the worker
    if (r.errors && r.errors.length > 0) {
      for (const e of r.errors) {
        console.error(`   [decode-worker] tx ${e.tx_id} failed: ${e.error}`);
      }
    }
  }

  await Promise.all([bufferUpdates(allUpdates), bufferEvents(allEvents)]);
  return { updates: allUpdates.length, events: allEvents.length };
}

/**
 * Decode a single transaction on the main thread.
 *
 * FIX #1: isReassignment now checks tx.reassignment (the actual Scan API
 * wrapper field) instead of tx.event, which is unrelated and caused false
 * positives.
 *
 * FIX #1: Transaction events are now traversed in preorder tree order via
 * flattenEventsInTreeOrder instead of Object.entries order.
 *
 * FIX #1: event_id overwrites now warn on key/field mismatch rather than
 * silently replacing without checking.
 *
 * FIX #2: Silent effective_at warn-and-skip guards removed. normalizeEvent
 * throws on null effective_at with full context — trusting that contract
 * here avoids stale defensive code that contradicts the upstream guarantee.
 */
export function decodeInMainThread(tx, migrationId) {
  // FIX #1: Use tx.reassignment — the actual Scan API wrapper field
  const isReassignment = !!tx.reassignment;

  const update = normalizeUpdate({ ...tx, migration_id: migrationId });
  const events = [];
  const txData = tx.transaction || tx.reassignment || tx;

  const updateInfo = {
    record_time:    txData.record_time,
    effective_at:   txData.effective_at,
    synchronizer_id: txData.synchronizer_id,
    source:         txData.source || null,
    target:         txData.target || null,
    unassign_id:    txData.unassign_id || null,
    submitter:      txData.submitter || null,
    counter:        txData.counter ?? null,
  };

  if (isReassignment) {
    // FIX #1: Navigate the correct path — tx.reassignment.event.{created,archived}_event
    const ce = tx.reassignment?.event?.created_event;
    const ae = tx.reassignment?.event?.archived_event;

    if (ce) {
      const ev = normalizeEvent(ce, update.update_id, migrationId, tx, updateInfo);
      ev.event_type = 'reassign_create';
      // FIX #2: normalizeEvent throws on null effective_at — no warn-and-skip needed
      events.push(ev);
    }
    if (ae) {
      const ev = normalizeEvent(ae, update.update_id, migrationId, tx, updateInfo);
      ev.event_type = 'reassign_archive';
      events.push(ev);
    }
  } else {
    const eventsById = txData.events_by_id || tx.events_by_id || {};
    const rootEventIds = txData.root_event_ids || tx.root_event_ids || [];

    // FIX #1: Use flattenEventsInTreeOrder for correct preorder traversal
    // per Scan API docs (root_event_ids → child_event_ids).
    const orderedEvents = flattenEventsInTreeOrder(eventsById, rootEventIds);

    for (const rawEvent of orderedEvents) {
      const ev = normalizeEvent(rawEvent, update.update_id, migrationId, rawEvent, updateInfo);

      // FIX #1: Warn on event_id key/field mismatch instead of silently overwriting
      const mapKeyId = rawEvent.event_id;
      if (mapKeyId && ev.event_id && mapKeyId !== ev.event_id) {
        console.warn(
          `[decode-main] event_id mismatch for update=${update.update_id}: ` +
          `eventsById key="${mapKeyId}" vs event.event_id="${ev.event_id}". ` +
          `Using map key as authoritative.`
        );
        ev.event_id = mapKeyId;
      } else if (mapKeyId && !ev.event_id) {
        ev.event_id = mapKeyId;
      }

      // FIX #2: No silent effective_at filter — normalizeEvent throws if null
      events.push(ev);
    }
  }

  return { update, events };
}

async function fetchTimeSliceStreaming(migrationId, synchronizerId, sliceBefore, sliceAfter, sliceIndex, processCallback) {
  const seenUpdateIds = new Set();
  let currentBefore = sliceBefore;
  const emptyHandler = new EmptyResponseHandler();
  let totalTxs = 0;
  let earliestTime = sliceBefore;

  const MAX_INFLIGHT_PROCESS = parseInt(process.env.MAX_INFLIGHT_PROCESS || '8', 10);
  const inflightProcesses = [];

  while (true) {
    if (new Date(currentBefore).getTime() <= new Date(sliceAfter).getTime()) break;

    let response;
    try {
      response = await fetchBackfillBefore(migrationId, synchronizerId, currentBefore, sliceAfter);
    } catch (err) {
      throw err;
    }

    const txs = response?.transactions || [];

    if (txs.length === 0) {
      const { action, newBefore, consecutiveEmpty, stepMs } = emptyHandler.handleEmpty(currentBefore, sliceAfter);
      if (action === 'done' || !newBefore) break;
      if (consecutiveEmpty % 100 === 0) {
        console.log(`   ⚠️ ${consecutiveEmpty} consecutive empty responses. Stepping back ${stepMs}ms.`);
      }
      currentBefore = newBefore;
      continue;
    }

    emptyHandler.resetOnData();

    const uniqueTxs = [];
    for (const tx of txs) {
      const updateId = tx.update_id || tx.transaction?.update_id || tx.reassignment?.update_id;
      if (updateId) {
        if (!seenUpdateIds.has(updateId)) { seenUpdateIds.add(updateId); uniqueTxs.push(tx); }
      } else {
        uniqueTxs.push(tx);
      }
    }

    if (uniqueTxs.length > 0) {
      // FIX #8: Don't await individual promises via shift() — a rejection on
      // promise[N] while awaiting promise[0] via shift() creates an unhandled
      // rejection in Node 15+. Collect all promises and await them together
      // at the end, where Promise.all propagates any failure.
      // Cap concurrent in-flight to MAX_INFLIGHT_PROCESS by waiting only when
      // at capacity — same backpressure, safe error propagation.
      if (inflightProcesses.length >= MAX_INFLIGHT_PROCESS) {
        await Promise.all(inflightProcesses);
        inflightProcesses.length = 0;
      }
      inflightProcesses.push(processCallback(uniqueTxs));
      totalTxs += uniqueTxs.length;
    }

    for (const tx of txs) {
      const t = getEventTime(tx);
      if (t && t < earliestTime) earliestTime = t;
    }

    let oldestTime = null;
    for (const tx of txs) {
      const t = getEventTime(tx);
      if (t && (!oldestTime || t < oldestTime)) oldestTime = t;
    }

    if (oldestTime && new Date(oldestTime).getTime() <= new Date(sliceAfter).getTime()) break;

    if (oldestTime) {
      const d = new Date(oldestTime);
      d.setMilliseconds(d.getMilliseconds() - 1);
      currentBefore = d.toISOString();
    } else {
      const d = new Date(currentBefore);
      d.setMilliseconds(d.getMilliseconds() - 1);
      currentBefore = d.toISOString();
    }

    // FIX #5: evict oldest half instead of clearing entirely.
    // A full clear means any update_id that was just evicted can appear again
    // in the next batch, writing duplicate records to Parquet. Evicting the
    // oldest half preserves recent dedup coverage across the boundary.
    if (seenUpdateIds.size > 500000) {
      const evictCount = Math.floor(seenUpdateIds.size / 2);
      let n = 0;
      for (const id of seenUpdateIds) { seenUpdateIds.delete(id); if (++n >= evictCount) break; }
    }
  }

  if (inflightProcesses.length > 0) await Promise.all(inflightProcesses);

  return { sliceIndex, totalTxs, earliestTime };
}

async function parallelFetchBatch(migrationId, synchronizerId, startBefore, atOrAfter, maxBatches, concurrency, cursorCallback = null) {
  const startMs = new Date(atOrAfter).getTime();
  const endMs = new Date(startBefore).getTime();
  const rangeMs = endMs - startMs;

  if (rangeMs < 60000 * concurrency) {
    return sequentialFetchBatch(migrationId, synchronizerId, startBefore, atOrAfter, maxBatches);
  }

  const sliceMs = rangeMs / concurrency;
  console.log(`   🔀 Parallel fetch: ${concurrency} slices of ${Math.round(sliceMs / 1000)}s each`);

  let totalUpdates = 0, totalEvents = 0;
  let earliestTime = startBefore;
  let pageCount = 0;
  const streamStartTime = Date.now();

  const globalSeenUpdateIds = new Set();
  // NOTE: globalSeenUpdateIds is cleared when it exceeds GLOBAL_DEDUP_MAX to
  // bound memory. This is safe because cursor correctness is guaranteed by
  // AtomicCursor — the cursor only advances after data is confirmed written,
  // so a cleared dedup set can at most allow boundary duplicates which are
  // handled by downstream dedup at query time.
  // FIX #10: Comment updated to explicitly reference AtomicCursor guarantee.
  const GLOBAL_DEDUP_MAX = Number(process.env.GLOBAL_DEDUP_MAX || 250_000);

  const sliceBoundaries = [], sliceCompleted = [], sliceEarliestTime = [];

  for (let i = 0; i < concurrency; i++) {
    const sliceBefore = new Date(endMs - (i * sliceMs)).toISOString();
    const sliceAfter = new Date(endMs - ((i + 1) * sliceMs)).toISOString();
    sliceBoundaries.push({ sliceBefore, sliceAfter });
    sliceCompleted.push(false);
    sliceEarliestTime.push(sliceBefore);
  }

  function getSafeCursorBoundary() {
    let contiguousCompleteCount = 0;
    for (let i = 0; i < concurrency; i++) {
      if (sliceCompleted[i]) contiguousCompleteCount++;
      else break;
    }
    if (contiguousCompleteCount === 0) return startBefore;
    const lastCompleteIdx = contiguousCompleteCount - 1;
    return sliceEarliestTime[lastCompleteIdx] || sliceBoundaries[lastCompleteIdx].sliceAfter;
  }

  const processCallback = async (transactions, sliceIndex) => {
    const { updates, events } = await processBackfillItems(transactions, migrationId);
    totalUpdates += updates;
    totalEvents += events;
    pageCount++;

    for (const tx of transactions) {
      const t = getEventTime(tx);
      if (t && t < earliestTime) earliestTime = t;
      if (t && t < sliceEarliestTime[sliceIndex]) sliceEarliestTime[sliceIndex] = t;
    }

    if (pageCount % 10 === 0) {
      const elapsed = (Date.now() - streamStartTime) / 1000;
      const throughput = Math.round(totalUpdates / elapsed);
      const stats = getBufferStats();
      let progressLine = `   📥 M${migrationId} Page ${pageCount}: ${totalUpdates.toLocaleString()} upd @ ${throughput}/s | W: ${stats.queuedJobs || 0}/${stats.activeWorkers || 0}`;
      if (stats.uploadQueuePending !== undefined || stats.uploadQueueActive !== undefined) {
        const pending = stats.uploadQueuePending || 0;
        const active = stats.uploadQueueActive || 0;
        const mbps = stats.uploadThroughputMBps || '0.00';
        const pauseIndicator = stats.uploadQueuePaused ? ' ⏸️' : '';
        progressLine += ` | ☁️ ${pending}+${active} @ ${mbps}MB/s${pauseIndicator}`;
      }
      console.log(progressLine);

      if (cursorCallback && pageCount % 100 === 0) {
        cursorCallback(totalUpdates, totalEvents, getSafeCursorBoundary());
      }
    }
  };

  const SLICE_MAX_RETRIES = 3;
  const SALVAGE_MAX_RETRIES = 5;

  const runSliceWithRetry = async (sliceIndex, sliceBefore, sliceAfter, options = {}) => {
    const { maxRetries = SLICE_MAX_RETRIES, phase = 'parallel' } = options;
    let lastError;

    for (let attempt = 0; attempt < maxRetries; attempt++) {
      try {
        const result = await fetchTimeSliceStreaming(
          migrationId, synchronizerId, sliceBefore, sliceAfter, sliceIndex,
          async (txs) => {
            const unique = [];
            for (const tx of txs) {
              const updateId = tx.update_id || tx.transaction?.update_id || tx.reassignment?.update_id;
              if (!updateId) { unique.push(tx); continue; }
              // FIX #5: evict oldest half instead of clearing — preserves recent dedup
              // coverage so boundary transactions are not written twice.
              if (globalSeenUpdateIds.size >= GLOBAL_DEDUP_MAX) {
                const evictCount = Math.floor(globalSeenUpdateIds.size / 2);
                let n = 0;
                for (const id of globalSeenUpdateIds) { globalSeenUpdateIds.delete(id); if (++n >= evictCount) break; }
              }
              if (globalSeenUpdateIds.has(updateId)) continue;
              globalSeenUpdateIds.add(updateId);
              unique.push(tx);
            }
            if (unique.length > 0) await processCallback(unique, sliceIndex);
          }
        );
        sliceCompleted[sliceIndex] = true;
        return result;
      } catch (err) {
        lastError = err;
        if (attempt < maxRetries - 1) {
          const delay = Math.min(1000 * Math.pow(2, attempt), 10000) + Math.random() * 1000;
          console.log(`   ⏳ Slice ${sliceIndex} failed (${phase}, attempt ${attempt + 1}/${maxRetries}): ${err.message}. Retrying in ${Math.round(delay)}ms...`);
          await sleep(delay);
        }
      }
    }

    console.error(`   ❌ Slice ${sliceIndex} failed after ${maxRetries} attempts (${phase}): ${lastError.message}`);
    return { sliceIndex, totalTxs: 0, earliestTime: sliceBefore, error: lastError };
  };

  const slicePromises = sliceBoundaries.map(({ sliceBefore, sliceAfter }, i) =>
    runSliceWithRetry(i, sliceBefore, sliceAfter)
  );

  let sliceResults = await Promise.all(slicePromises);

  let failedSlices = sliceResults.filter(s => s.error);
  if (failedSlices.length > 0) {
    console.warn(`   🛟 Salvage mode: retrying ${failedSlices.length} failed slice(s) sequentially...`);
    for (const failed of failedSlices) {
      const idx = failed.sliceIndex;
      const { sliceBefore, sliceAfter } = sliceBoundaries[idx];
      const salvaged = await runSliceWithRetry(idx, sliceBefore, sliceAfter, { maxRetries: SALVAGE_MAX_RETRIES, phase: 'salvage' });
      sliceResults[idx] = salvaged;
    }
    failedSlices = sliceResults.filter(s => s.error);
  }

  for (const slice of sliceResults) {
    if (slice.earliestTime && slice.earliestTime < earliestTime) earliestTime = slice.earliestTime;
  }

  const totalTxs = sliceResults.reduce((sum, s) => sum + (s.totalTxs || 0), 0);
  const hasError = failedSlices.length > 0;
  const safeCursorBoundary = getSafeCursorBoundary();

  const completedCount = sliceCompleted.filter(Boolean).length;
  if (completedCount < concurrency) {
    console.log(`   ⚠️ Only ${completedCount}/${concurrency} slices completed. Safe cursor: ${safeCursorBoundary}`);
  }

  return {
    results: totalTxs > 0 ? [{ transactions: [], processedUpdates: totalUpdates, processedEvents: totalEvents, before: safeCursorBoundary }] : [],
    reachedEnd: !hasError,
    earliestTime: safeCursorBoundary,
    totalUpdates,
    totalEvents,
    failedSlices: failedSlices.map(s => ({
      sliceIndex: s.sliceIndex,
      error: s.error?.message || 'Unknown error',
      status: s.error?.response?.status || null,
      code: s.error?.code || null,
    })),
    sliceCompletionStatus: sliceCompleted.map((complete, i) => ({
      sliceIndex: i, complete, boundary: sliceBoundaries[i], earliestProcessed: sliceEarliestTime[i],
    })),
  };
}

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
      if (action === 'done' || !newBefore) return { results, reachedEnd: true };
      if (consecutiveEmpty % 100 === 0) {
        console.log(`   ⚠️ ${consecutiveEmpty} consecutive empty responses. Stepping back ${stepMs}ms.`);
      }
      currentBefore = newBefore;
      continue;
    }

    emptyHandler.resetOnData();

    const uniqueTxs = [];
    for (const tx of txs) {
      const updateId = tx.update_id || tx.transaction?.update_id || tx.reassignment?.update_id;
      if (updateId) {
        if (!seenUpdateIds.has(updateId)) { seenUpdateIds.add(updateId); uniqueTxs.push(tx); }
      } else {
        uniqueTxs.push(tx);
      }
    }

    if (uniqueTxs.length > 0) results.push({ transactions: uniqueTxs, before: currentBefore });

    let oldestTime = null;
    for (const tx of txs) {
      const t = getEventTime(tx);
      if (t && (!oldestTime || t < oldestTime)) oldestTime = t;
    }

    if (oldestTime && new Date(oldestTime).getTime() <= new Date(atOrAfter).getTime()) {
      return { results, reachedEnd: true };
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

    // FIX #5: evict oldest half instead of clearing — same boundary-duplication
    // fix as fetchTimeSliceStreaming's seenUpdateIds.
    if (seenUpdateIds.size > 100000) {
      const evictCount = Math.floor(seenUpdateIds.size / 2);
      let n = 0;
      for (const id of seenUpdateIds) { seenUpdateIds.delete(id); if (++n >= evictCount) break; }
    }
  }

  return { results, reachedEnd: false };
}

/**
 * Backfill a single synchronizer with parallel fetching (shard-aware).
 *
 * FIX #4: beginTransaction/addPending/commit pattern removed from the main
 * batch loop. Data is already confirmed buffered before the cursor save, so
 * the correct primitive is saveAtomic — not a transaction that wraps nothing.
 *
 * FIX #5: The catch block now calls atomicCursor.rollback() before saveAtomic
 * so a transaction left open by a mid-batch error doesn't cause saveAtomic to
 * throw, which would mask the original error.
 */
async function backfillSynchronizer(migrationId, synchronizerId, minTime, maxTime, shardIndex = null) {
  const shardLabel = shardIndex !== null ? ` [shard ${shardIndex}/${SHARD_TOTAL}]` : '';

  logSynchronizer('start', { migrationId, synchronizerId, shardIndex, minTime, maxTime,
    extra: { parallel_fetches: dynamicParallelFetches, decode: 'main-thread' } });

  console.log(`\n📍 Backfilling migration ${migrationId}, synchronizer ${synchronizerId.substring(0, 30)}...${shardLabel}`);
  console.log(`   Range: ${minTime} to ${maxTime}`);
  console.log(`   Parallel fetches (auto-tuned): ${dynamicParallelFetches} (min=${MIN_PARALLEL_FETCHES}, max=${MAX_PARALLEL_FETCHES})`);
  console.log(`   Decode: main-thread (zero serialization overhead)`);

  const atomicCursor = new AtomicCursor(migrationId, synchronizerId, shardIndex);
  let cursorState = atomicCursor.load();
  let before = cursorState?.lastBefore || maxTime;
  const atOrAfter = minTime;

  if (cursorState?.lastBefore) {
    const lastBeforeMs = new Date(cursorState.lastBefore).getTime();
    const minTimeMs = new Date(minTime).getTime();

    if (lastBeforeMs <= minTimeMs) {
      logCursor('already_complete', { migrationId, synchronizerId, shardIndex, lastBefore: cursorState.lastBefore, minTime });
      console.log(`   ⚠️ Cursor last_before (${cursorState.lastBefore}) is at or before minTime (${minTime})`);
      console.log(`   ⚠️ This synchronizer appears complete. Draining writer queues before marking complete.`);

      // FIX #10: Do NOT swallow flush errors before marking a synchronizer complete.
      // If the flush fails here, records that were in the buffer are not written.
      // Marking the synchronizer complete with unwritten data means those records
      // are permanently lost — the backfill will not re-visit this synchronizer.
      // On error: log, skip marking complete, and let the outer loop retry.
      let flushOk = true;
      try { await flushAll(); } catch (err) {
        console.error(`   ❌ flushAll failed before marking synchronizer complete: ${err.message}`);
        flushOk = false;
      }
      if (flushOk) {
        try { await waitForWrites(); } catch (err) {
          console.error(`   ❌ waitForWrites failed before marking synchronizer complete: ${err.message}`);
          flushOk = false;
        }
      }

      if (!flushOk) {
        console.error(`   ❌ Skipping synchronizer completion mark — flush failed. Will retry.`);
        // Return without marking complete; outer loop will attempt this synchronizer again
        return;
      }

      const finalStats = getBufferStats();
      const pendingWritesAccurate =
        Number(finalStats.pendingWrites || 0) +
        Number(finalStats.queuedWrites ?? finalStats.queuedJobs ?? 0) +
        Number(finalStats.activeWrites ?? finalStats.activeWorkers ?? 0);
      const bufferedRecords = Number((finalStats.updatesBuffered || 0) + (finalStats.eventsBuffered || 0));
      const hasPendingWork = pendingWritesAccurate > 0 || bufferedRecords > 0;

      if (!cursorState.complete || hasPendingWork) {
        atomicCursor.saveAtomic({ complete: !hasPendingWork, min_time: minTime, max_time: maxTime });
        logCursor('finalized', { migrationId, synchronizerId, shardIndex, lastBefore: cursorState.lastBefore,
          totalUpdates: cursorState.totalUpdates || 0, totalEvents: cursorState.totalEvents || 0,
          complete: !hasPendingWork, pendingWrites: pendingWritesAccurate });
        if (hasPendingWork) {
          console.log(`   ⏳ Writes still pending. Cursor left in finalizing state.`);
        }
      }

      return { updates: cursorState.totalUpdates || 0, events: cursorState.totalEvents || 0 };
    }

    logCursor('resume', { migrationId, synchronizerId, shardIndex, lastBefore: cursorState.lastBefore,
      totalUpdates: cursorState.totalUpdates || 0, totalEvents: cursorState.totalEvents || 0, complete: cursorState.complete || false });
    console.log(`   📍 Resuming from cursor: last_before=${cursorState.lastBefore}, updates=${cursorState.totalUpdates || 0}`);
  }

  let totalUpdates = cursorState?.totalUpdates || 0;
  let totalEvents = cursorState?.totalEvents || 0;
  let batchCount = 0;
  const startTime = Date.now();
  let lastMetricsLog = Date.now();
  const METRICS_LOG_INTERVAL_MS = 60000;

  if (!cursorState) {
    atomicCursor.saveAtomic({ last_before: before, total_updates: 0, total_events: 0,
      started_at: new Date().toISOString(), min_time: minTime, max_time: maxTime });
    logCursor('created', { migrationId, synchronizerId, shardIndex, lastBefore: before, totalUpdates: 0, totalEvents: 0 });
  }

  let consecutiveTransientErrors = 0;
  let cooldownUntil = 0;

  while (true) {
    const batchStartTime = Date.now();

    const memCheck = checkMemoryPressure();
    if (memCheck.pressure) {
      console.log(`   ⚠️ Heap pressure: ${memCheck.heap.usedMB}MB / ${memCheck.heap.limitMB}MB${shardLabel}`);
      await waitForMemoryRelief(shardLabel);
    }

    try {
      const localParallel = dynamicParallelFetches;
      const cursorBeforeBatch = before;

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
        localParallel * 2, localParallel, cursorCallback
      );

      const { results, reachedEnd, earliestTime: resultEarliestTime, totalUpdates: batchUpdates, totalEvents: batchEvents, failedSlices } = fetchResult;

      if (failedSlices && failedSlices.length > 0) {
        const sliceList = failedSlices.map(s => `slice ${s.sliceIndex}: ${s.error}`).join(', ');
        const statuses = failedSlices.map(s => s.status).filter(Boolean);
        const codes = failedSlices.map(s => String(s.code || '').toUpperCase()).filter(Boolean);
        const isTransient = statuses.some(s => s === 429 || (s >= 500 && s <= 599)) ||
          codes.some(c => TRANSIENT_NETWORK_CODES.has(c)) ||
          codes.includes('ERR_BAD_RESPONSE');
        const e = new Error(`${failedSlices.length} slice(s) failed: ${sliceList}. Cursor NOT advanced to prevent data gaps.`);
        if (isTransient) {
          e.response = { status: statuses.includes(429) ? 429 : 503 };
          e.code = codes.includes('ERR_BAD_RESPONSE') ? 'ERR_BAD_RESPONSE' : (codes[0] || undefined);
        }
        throw e;
      }

      if (results.length === 0 && !batchUpdates) {
        console.log(`   ✅ No more transactions. Marking complete.${shardLabel}`);
        break;
      }

      totalUpdates += batchUpdates || 0;
      totalEvents += batchEvents || 0;
      batchCount++;

      const newEarliestTime = resultEarliestTime || results[0]?.before;
      if (newEarliestTime && newEarliestTime !== before) {
        const d = new Date(newEarliestTime);
        d.setMilliseconds(d.getMilliseconds() - 1);
        before = d.toISOString();
      } else {
        const d = new Date(before);
        d.setMilliseconds(d.getMilliseconds() - 1);
        before = d.toISOString();
      }

      const elapsed = (Date.now() - startTime) / 1000;
      const throughput = Math.round(totalUpdates / elapsed);
      const batchLatency = Date.now() - batchStartTime;
      const stats = getBufferStats();
      const queuedJobs = Number(stats.queuedJobs ?? 0);
      const activeWorkers = Number(stats.activeWorkers ?? 0);

      // FIX #4: Data is already confirmed buffered by processBackfillItems at
      // this point — use saveAtomic directly. The beginTransaction/addPending/
      // commit pattern was misleading because no I/O confirmation happened
      // between begin and commit, making it semantically equivalent to saveAtomic
      // but much harder to reason about.
      atomicCursor.saveAtomic({
        last_before: before,
        total_updates: totalUpdates,
        total_events: totalEvents,
        min_time: minTime,
        max_time: maxTime,
      });

      consecutiveTransientErrors = 0;
      if (cooldownUntil && Date.now() > cooldownUntil) {
        console.log(`   🔥 Cooldown expired, auto-tuner will scale back up${shardLabel}`);
        cooldownUntil = 0;
      }

      atomicCursor.setTimeBounds(minTime, maxTime);

      logBatch({ migrationId, synchronizerId, shardIndex, batchCount,
        updates: batchUpdates || 0, events: batchEvents || 0, totalUpdates, totalEvents,
        cursorBefore: cursorBeforeBatch, cursorAfter: before, throughput,
        latencyMs: batchLatency, parallelFetches: dynamicParallelFetches, queuedJobs, activeWorkers });

      if (Date.now() - lastMetricsLog >= METRICS_LOG_INTERVAL_MS) {
        logMetrics({ migrationId, shardIndex, elapsedSeconds: elapsed, totalUpdates, totalEvents,
          avgThroughput: throughput, currentThroughput: Math.round((batchUpdates || 0) / (batchLatency / 1000)),
          parallelFetches: dynamicParallelFetches, avgLatencyMs: fetchStats.avgLatency,
          p95LatencyMs: fetchStats.p95Latency, errorCount: fetchStats.errorCount || 0, retryCount: fetchStats.retry503Count });
        lastMetricsLog = Date.now();
      }

      if (batchCount % FLUSH_EVERY_BATCHES === 0) await flushAll();

      if (GCS_MODE && batchCount % GCS_CHECKPOINT_INTERVAL === 0) {
        const checkpointStart = Date.now();
        console.log(`   ⏱️ GCS checkpoint: draining upload queue...${shardLabel}`);
        await drainUploads();
        atomicCursor.confirmGCS(before, totalUpdates, totalEvents);
        gcsCursorBackupCounter++;
        if (gcsCursorBackupCounter % GCS_CURSOR_BACKUP_INTERVAL === 0) {
          // FIX #6: await the now-async backupCursorToGCS so errors surface
          // and the event loop is not blocked during the gsutil copy.
          await backupCursorToGCS(atomicCursor.cursorPath);
        }
        console.log(`   ✅ GCS checkpoint confirmed at ${before} (${Date.now() - checkpointStart}ms)${shardLabel}`);
      }

      maybeTuneParallelFetches(shardLabel);

      console.log(`   📦${shardLabel} Batch ${batchCount}: +${batchUpdates || 0} upd, +${batchEvents || 0} evt | Total: ${totalUpdates.toLocaleString()} @ ${throughput}/s | F:${dynamicParallelFetches} | Q: ${queuedJobs}/${activeWorkers}`);

      if (reachedEnd || new Date(before).getTime() <= new Date(atOrAfter).getTime()) {
        console.log(`   ✅ Reached lower bound. Complete.${shardLabel}`);
        break;
      }

    } catch (err) {
      const status = err.response?.status;
      const msg = err.response?.data?.error || err.message;
      const errCode = err.code || '';

      logError('batch', err, { migration: migrationId, synchronizer: synchronizerId.substring(0, 30),
        shard: shardIndex, batch: batchCount, cursor_before: before });

      console.error(`   ❌ Error at batch ${batchCount} (status ${status || 'n/a'}, code=${errCode}): ${msg}${shardLabel}`);

      const isHttpTransient = Number.isFinite(status) && (status === 429 || (status >= 500 && status <= 599));
      const normalizedCode = String(errCode).toUpperCase();
      const isCodeTransient = TRANSIENT_NETWORK_CODES.has(normalizedCode);
      const isDiskTransient = /ENOSPC|EMFILE|ENFILE|EAGAIN|EBUSY|disk full|no space left/i.test(msg + errCode);
      const isWorkerTransient = /worker crashed|worker error|worker exited/i.test(msg);
      const isGCSTransient = /timeout|timed out|connection reset|ECONNRESET|ETIMEDOUT|socket hang up|ssl3_get_record|wrong version number/i.test(msg + errCode);
      const isSliceTransient = /slice.*failed/i.test(msg) && (/\b(429|5\d\d)\b/.test(msg) || isCodeTransient);
      const isTransient = isHttpTransient || isCodeTransient || isDiskTransient || isWorkerTransient || isGCSTransient || isSliceTransient;

      if (isTransient) {
        consecutiveTransientErrors++;
        const errorType = isHttpTransient ? 'HTTP' : isCodeTransient ? 'NETWORK_CODE' : isDiskTransient ? 'DISK' : isWorkerTransient ? 'WORKER' : isGCSTransient ? 'NETWORK' : 'SLICE';

        const MAX_CONSECUTIVE_TRANSIENT_ERRORS = 50;
        if (consecutiveTransientErrors >= MAX_CONSECUTIVE_TRANSIENT_ERRORS) {
          console.error(`   💀 ${MAX_CONSECUTIVE_TRANSIENT_ERRORS} consecutive transient errors — saving cursor and exiting${shardLabel}`);
          // FIX #5: rollback() before saveAtomic() so a transaction left open
          // by a mid-batch error doesn't cause saveAtomic to throw a second
          // exception, which would mask the original error.
          if (atomicCursor.inTransaction) atomicCursor.rollback();
          atomicCursor.saveAtomic({
            last_before: before, total_updates: totalUpdates, total_events: totalEvents,
            error: `MAX_TRANSIENT_ERRORS: ${errorType}: ${msg}`, error_at: new Date().toISOString(),
            min_time: minTime, max_time: maxTime,
          });
          process.exit(1);
        }

        console.log(`   🔍 Error classified as transient (${errorType})${shardLabel}`);

        if (isDiskTransient && GCS_MODE) {
          console.log(`   💾 Disk pressure — draining GCS upload queue to free /tmp space...${shardLabel}`);
          try { await flushAll(); await waitForWrites(); await drainUploads(); }
          catch (drainErr) { console.error(`   ⚠️ GCS drain failed: ${drainErr.message}${shardLabel}`); }
        }

        if (consecutiveTransientErrors >= 3 && dynamicParallelFetches > 1) {
          console.log(`   🧊 Cooldown mode: Dropping to 1 parallel fetch for 60s${shardLabel}`);
          dynamicParallelFetches = 1;
          cooldownUntil = Date.now() + 60000;
        }

        const baseDelay = isDiskTransient ? 10000 : 5000;
        const backoffDelay = Math.min(baseDelay * Math.pow(2, consecutiveTransientErrors - 1), 60000);
        console.log(`   ⏳ Transient error #${consecutiveTransientErrors} (${errorType}), backing off ${Math.round(backoffDelay / 1000)}s...${shardLabel}`);

        // FIX #5: rollback before saveAtomic in the normal transient-error path too
        if (atomicCursor.inTransaction) atomicCursor.rollback();
        atomicCursor.saveAtomic({
          last_before: before, total_updates: totalUpdates, total_events: totalEvents,
          error: `${errorType}: ${msg}`, error_at: new Date().toISOString(),
          min_time: minTime, max_time: maxTime,
        });

        await sleep(backoffDelay);
        continue;
      }

      console.error(`   💀 NON-TRANSIENT ERROR — process will exit. Error: ${msg}${shardLabel}`);
      logFatal('batch', err, { migration: migrationId, synchronizer: synchronizerId.substring(0, 30),
        shard: shardIndex, batch: batchCount, total_updates: totalUpdates, total_events: totalEvents });
      throw err;
    }
  }

  await flushAll();
  console.log(`   ⏳ Waiting for all pending writes to complete...${shardLabel}`);
  await waitForWrites();

  if (GCS_MODE) {
    console.log(`   ⏱️ Final GCS drain before marking complete...${shardLabel}`);
    await drainUploads();
  }

  const finalStats = getBufferStats();
  console.log(`   ✅ All writes complete. Final queue: ${finalStats.queuedJobs || 0} pending, ${finalStats.activeWorkers || 0} active${shardLabel}`);

  const totalTime = (Date.now() - startTime) / 1000;

  atomicCursor.saveAtomic({
    last_before: before, total_updates: totalUpdates, total_events: totalEvents,
    complete: true, completed_at: new Date().toISOString(), min_time: minTime, max_time: maxTime,
  });

  if (GCS_MODE) {
    atomicCursor.confirmGCS(before, totalUpdates, totalEvents);
    // FIX #6: await async backup — non-blocking to event loop, errors surfaced
    await backupCursorToGCS(atomicCursor.cursorPath);
    console.log(`   ✅ GCS cursor confirmed and backed up at final position${shardLabel}`);
  }

  logSynchronizer('complete', { migrationId, synchronizerId, shardIndex, minTime, maxTime,
    totalUpdates, totalEvents, elapsedSeconds: totalTime.toFixed(1),
    extra: { avg_throughput: Math.round(totalUpdates / totalTime), batch_count: batchCount } });
  logCursor('completed', { migrationId, synchronizerId, shardIndex, lastBefore: before, totalUpdates, totalEvents, complete: true });

  console.log(`   ⏱️ Completed in ${totalTime.toFixed(1)}s (${Math.round(totalUpdates / totalTime)}/s avg)${shardLabel}`);
  return { updates: totalUpdates, events: totalEvents };
}

function calculateShardTimeRange(minTime, maxTime, shardIndex, shardTotal) {
  const minMs = new Date(minTime).getTime();
  const maxMs = new Date(maxTime).getTime();
  const rangeMs = maxMs - minMs;

  if (!Number.isFinite(minMs) || !Number.isFinite(maxMs) || rangeMs <= 0) {
    throw new Error(`[sharding] invalid time range: minTime=${minTime}, maxTime=${maxTime}`);
  }

  const shardMaxMsRaw = maxMs - Math.floor((shardIndex * rangeMs) / shardTotal);
  const shardMinMs = maxMs - Math.floor(((shardIndex + 1) * rangeMs) / shardTotal);
  const shardMaxMs = shardIndex === 0 ? shardMaxMsRaw : Math.max(shardMinMs, shardMaxMsRaw - 1);

  return {
    minTime: new Date(shardMinMs).toISOString(),
    maxTime: new Date(shardMaxMs).toISOString(),
  };
}

async function areAllMigrationsComplete() {
  const allMigrations = await detectMigrations();
  if (allMigrations.length === 0) return { complete: true, pendingMigrations: [] };

  const pendingMigrations = [];
  for (const migrationId of allMigrations) {
    const info = await getMigrationInfo(migrationId);
    if (!info) continue;
    const ranges = info.record_time_range || [];
    for (const range of ranges) {
      const synchronizerId = range.synchronizer_id;
      if (SHARD_TOTAL > 1) {
        for (let shard = 0; shard < SHARD_TOTAL; shard++) {
          const cursor = loadCursor(migrationId, synchronizerId, shard);
          if (!cursor?.complete) pendingMigrations.push({ migrationId, synchronizerId, shard });
        }
      } else {
        const cursor = loadCursor(migrationId, synchronizerId, null);
        if (!cursor?.complete) pendingMigrations.push({ migrationId, synchronizerId, shard: null });
      }
    }
  }

  return { complete: pendingMigrations.length === 0, pendingMigrations, totalMigrations: allMigrations.length };
}

async function runBackfill() {
  const isSharded = SHARD_TOTAL > 1;
  const shardLabel = isSharded ? ` [SHARD ${SHARD_INDEX}/${SHARD_TOTAL}]` : '';

  try { await probeScanEndpoints(); }
  catch (err) { console.warn(`⚠️ Endpoint probe failed, continuing: ${err.message}`); }

  console.log('\n' + '='.repeat(80));
  console.log(`🚀 Starting Canton ledger backfill (Auto-Tuning Mode)${shardLabel}`);
  console.log('   SCAN_URL (active):', activeScanUrl);
  console.log('   SCAN failover endpoints:', scanEndpointRotation.length);
  console.log('   BATCH_SIZE:', BATCH_SIZE);
  console.log('   INSECURE_TLS:', INSECURE_TLS ? 'ENABLED (unsafe)' : 'disabled');
  console.log('='.repeat(80));

  try {
    if (USE_PARQUET) parquetWriter.initParquetWriter();
    if (GCS_MODE) {
      validateGCSBucket(true);
      console.log('\n🔍 Running GCS preflight checks...');
      runPreflightChecks({ quick: false, throwOnFail: true });
      console.log(`\n☁️  GCS Mode ENABLED: gs://${process.env.GCS_BUCKET}/`);
    } else {
      console.log(`\n📂 Disk Mode: Writing to ${BASE_DATA_DIR}`);
    }
  } catch (err) {
    logFatal('gcs_preflight_failed', err);
    throw err;
  }

  console.log('\n⚙️  Auto-Tuning Configuration:');
  console.log(`   Parallel Fetches: ${dynamicParallelFetches} (range: ${MIN_PARALLEL_FETCHES}-${MAX_PARALLEL_FETCHES})`);
  console.log(`   Tune Window: ${FETCH_TUNE_WINDOW_MS / 1000}s | Latency: ${LATENCY_LOW_MS}ms / ${LATENCY_HIGH_MS}ms / ${LATENCY_CRITICAL_MS}ms`);
  console.log(`   FLUSH_EVERY_BATCHES: ${FLUSH_EVERY_BATCHES}`);
  if (isSharded) console.log(`   SHARDING: Shard ${SHARD_INDEX} of ${SHARD_TOTAL}`);
  if (TARGET_MIGRATION != null) console.log(`   TARGET_MIGRATION: ${TARGET_MIGRATION} only`);
  else if (START_MIGRATION != null || END_MIGRATION != null) console.log(`   MIGRATION RANGE: ${START_MIGRATION ?? 0} → ${END_MIGRATION ?? '∞'}`);
  console.log('   CURSOR_DIR:', CURSOR_DIR);
  console.log('='.repeat(80));

  mkdirSync(CURSOR_DIR, { recursive: true });

  if (GCS_MODE) await restoreCursorsFromGCS();

  let grandTotalUpdates = 0, grandTotalEvents = 0;
  const grandStartTime = Date.now();
  const processedMigrations = new Set();
  const MAX_MIGRATION_RESCAN_ROUNDS = 10;

  for (let round = 0; round < MAX_MIGRATION_RESCAN_ROUNDS; round++) {
    console.log(`\n🔄 Migration scan round ${round + 1}/${MAX_MIGRATION_RESCAN_ROUNDS}...`);
    let migrations = await detectMigrations();

    if (TARGET_MIGRATION != null) {
      migrations = migrations.filter(id => id === TARGET_MIGRATION);
      if (!migrations.length) { console.log(`⚠️ Target migration ${TARGET_MIGRATION} not found. Exiting.`); return { success: false, allMigrationsComplete: false }; }
    } else if (START_MIGRATION != null || END_MIGRATION != null) {
      const lo = START_MIGRATION ?? 0, hi = END_MIGRATION ?? Infinity;
      migrations = migrations.filter(id => id >= lo && id <= hi);
      if (!migrations.length) { console.log(`⚠️ No migrations in range ${lo}–${hi}. Exiting.`); return { success: false, allMigrationsComplete: false }; }
    }

    const pending = migrations.filter(id => !processedMigrations.has(id));
    console.log(`   Detected: [${migrations.join(', ')}] | Already processed: [${[...processedMigrations].join(', ')}] | Pending: [${pending.join(', ')}]`);
    if (pending.length === 0) { console.log(`   ✅ No pending migrations in round ${round + 1}.`); break; }

    for (const migrationId of pending) {
      processedMigrations.add(migrationId);
      console.log(`\n${'─'.repeat(80)}\n📘 Migration ${migrationId}: fetching metadata${shardLabel}\n${'─'.repeat(80)}`);

      const info = await getMigrationInfo(migrationId);
      if (!info) { console.log('   ℹ️  No backfilling info; skipping.'); continue; }

      const ranges = info.record_time_range || [];
      if (!ranges.length) { console.log('   ℹ️  No synchronizer ranges; skipping.'); continue; }
      console.log(`   Found ${ranges.length} synchronizer ranges for migration ${migrationId}`);

      for (const range of ranges) {
        const synchronizerId = range.synchronizer_id;
        let minTime = range.min, maxTime = range.max;

        const minMs = new Date(minTime).getTime(), maxMs = new Date(maxTime).getTime();
        if (!Number.isFinite(minMs) || !Number.isFinite(maxMs) || minMs >= maxMs) {
          throw new Error(`[range] Invalid time bounds for migration ${migrationId}: min=${minTime} max=${maxTime}`);
        }

        if (isSharded) {
          const shardRange = calculateShardTimeRange(minTime, maxTime, SHARD_INDEX, SHARD_TOTAL);
          minTime = shardRange.minTime;
          maxTime = shardRange.maxTime;
          console.log(`   🔀 Shard ${SHARD_INDEX}: ${minTime} to ${maxTime}`);
        }

        const cursor = loadCursor(migrationId, synchronizerId, isSharded ? SHARD_INDEX : null);
        if (cursor?.complete) { console.log(`   ⏭️ Skipping (already complete)${shardLabel}`); continue; }

        const { updates, events } = await backfillSynchronizer(migrationId, synchronizerId, minTime, maxTime, isSharded ? SHARD_INDEX : null);
        grandTotalUpdates += updates;
        grandTotalEvents += events;
      }

      console.log(`✅ Completed migration ${migrationId}${shardLabel}`);
    }
  }

  const grandTotalTime = ((Date.now() - grandStartTime) / 1000).toFixed(1);

  console.log(`\n${'═'.repeat(80)}\n🎉 Backfill complete!\n   Total updates: ${grandTotalUpdates.toLocaleString()}\n   Total events: ${grandTotalEvents.toLocaleString()}\n   Total time: ${grandTotalTime}s\n   Avg throughput: ${Math.round(grandTotalUpdates / parseFloat(grandTotalTime))}/s\n${'═'.repeat(80)}\n`);

  logSummary({ success: true, totalUpdates: grandTotalUpdates, totalEvents: grandTotalEvents,
    totalTimeSeconds: parseFloat(grandTotalTime), avgThroughput: Math.round(grandTotalUpdates / parseFloat(grandTotalTime)),
    migrationsProcessed: processedMigrations.size, allComplete: false, pendingCount: 0 });

  const completionStatus = await areAllMigrationsComplete();
  if (!completionStatus.complete) {
    const pendingByMigration = {};
    for (const p of completionStatus.pendingMigrations) {
      if (!pendingByMigration[p.migrationId]) pendingByMigration[p.migrationId] = [];
      pendingByMigration[p.migrationId].push(p);
    }
    console.log(`\n⚠️ Not all migrations complete:`);
    for (const [mig, items] of Object.entries(pendingByMigration)) {
      console.log(`   • Migration ${mig}: ${items.length} cursor(s) pending`);
    }
  }

  return { success: true, totalUpdates: grandTotalUpdates, totalEvents: grandTotalEvents,
    allMigrationsComplete: completionStatus.complete, pendingMigrations: completionStatus.pendingMigrations };
}

async function startLiveUpdates() {
  const { spawn } = await import('child_process');
  const liveUpdatesScript = join(__dirname, 'fetch-updates.js');
  console.log(`\n${'═'.repeat(80)}\n🔄 Starting live updates ingestion...\n   Script: ${liveUpdatesScript}\n${'═'.repeat(80)}\n`);

  const child = spawn('node', [liveUpdatesScript], { stdio: 'inherit', cwd: __dirname, env: process.env });
  child.on('error', (err) => { console.error('❌ Failed to start live updates:', err.message); process.exit(1); });
  child.on('exit', (code) => { console.log(`Live updates process exited with code ${code}`); process.exit(code || 0); });
}

// ==========================================
// GRACEFUL SHUTDOWN
// ==========================================
let isShuttingDown = false;

async function gracefulShutdown(signal) {
  if (isShuttingDown) return;
  isShuttingDown = true;
  console.log(`\n🛑 Received ${signal}. Shutting down gracefully...`);
  try {
    await flushAll(); await waitForWrites(); await shutdown();
    if (decodePool) await decodePool.destroy();
    console.log('✅ Graceful shutdown complete');
    process.exit(0);
  } catch (err) {
    console.error('❌ Error during shutdown:', err.message);
    process.exit(1);
  }
}

process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));

let exitReason = 'unknown';
process.on('exit', (code) => {
  console.log(`\n🚪 Process exiting with code ${code} (reason: ${exitReason})`);
  const heap = getHeapUsage();
  console.log(`   Heap at exit: ${heap.usedMB}MB / ${heap.limitMB}MB (${(heap.ratio * 100).toFixed(1)}%)`);
});

process.on('uncaughtException', async (err) => {
  exitReason = `uncaughtException: ${err.message}`;
  console.error('\n💥 Uncaught exception:', err.message, err.stack);
  try { await flushAll(); await waitForWrites(); await shutdown(); } catch {}
  process.exit(1);
});

/**
 * FIX #9: Unhandled rejections are no longer silently swallowed forever.
 *
 * A counter tracks consecutive unhandled rejections. After a threshold is
 * exceeded within a short window the process exits — this catches real bugs
 * (a broken async chain that never surfaces otherwise) while still tolerating
 * occasional isolated rejections from third-party libraries.
 *
 * Resetting the counter after the window prevents a single noisy library from
 * triggering an exit during an otherwise healthy run.
 */
let unhandledRejectionCount = 0;
let unhandledRejectionWindowStart = Date.now();
const UNHANDLED_REJECTION_THRESHOLD = 5;   // max before exit
const UNHANDLED_REJECTION_WINDOW_MS = 10_000; // within this window

process.on('unhandledRejection', (reason, promise) => {
  exitReason = `unhandledRejection: ${reason?.message || reason}`;
  console.error('\n💥 Unhandled rejection at:', promise);
  console.error('Reason:', reason);

  const now = Date.now();
  if (now - unhandledRejectionWindowStart > UNHANDLED_REJECTION_WINDOW_MS) {
    // Reset window — isolated rejections are tolerated
    unhandledRejectionCount = 0;
    unhandledRejectionWindowStart = now;
  }

  unhandledRejectionCount++;
  console.error(`   Unhandled rejection count: ${unhandledRejectionCount}/${UNHANDLED_REJECTION_THRESHOLD} in ${UNHANDLED_REJECTION_WINDOW_MS / 1000}s window`);

  if (unhandledRejectionCount >= UNHANDLED_REJECTION_THRESHOLD) {
    console.error(`   ❌ Threshold reached — exiting to prevent silent data loss`);
    process.exit(1);
  }

  console.warn(`   ⚠️ Below threshold — continuing. Subsystems should handle their own rejections.`);
});

// ==========================================
// ENTRY POINT
// ==========================================
runBackfill()
  .then(async (result) => {
    console.log(`\n📋 Exit decision: success=${result?.success}, allMigrationsComplete=${result?.allMigrationsComplete}`);
    if (result?.pendingMigrations?.length) {
      console.log(`   Pending: ${result.pendingMigrations.map(p => `M${p.migrationId}${p.shard !== null ? `/S${p.shard}` : ''}`).join(', ')}`);
    }

    if (result?.success && result?.allMigrationsComplete) {
      exitReason = 'all_migrations_complete → starting live updates';
      await new Promise(resolve => setTimeout(resolve, 1000));
      await startLiveUpdates();
    } else if (result?.success && !result?.allMigrationsComplete) {
      exitReason = 'migrations_remaining';
      console.log(`\n${'═'.repeat(80)}\n⏸️ Backfill complete for processed migrations, but others remain.\n${'═'.repeat(80)}\n`);
      if (TARGET_MIGRATION != null) console.log(`   TARGET_MIGRATION=${TARGET_MIGRATION} was set. Unset to process all.`);
      else if (START_MIGRATION != null || END_MIGRATION != null) console.log(`   MIGRATION RANGE set. Unset to process all.`);
      process.exit(0);
    }
  })
  .catch(async err => {
    exitReason = `fatal_error: ${err.message}`;
    console.error('\n❌ FATAL:', err.message, err.stack);
    await flushAll(); await waitForWrites(); await shutdown();
    if (decodePool) await decodePool.destroy();
    process.exit(1);
  });
