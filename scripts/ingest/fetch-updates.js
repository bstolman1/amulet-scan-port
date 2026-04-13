#!/usr/bin/env node
/**
 * Canton Ledger Ingestion Script - Live Updates
 *
 * Fetches ledger updates from Canton Scan API and writes directly to Parquet (default)
 * or to binary files with --keep-raw flag.
 *
 * Usage:
 *   node fetch-updates.js            # Resume from backfill cursor, write to Parquet
 *   node fetch-updates.js --live     # Start from current API time (live mode)
 *   node fetch-updates.js --keep-raw # Also write to .pb.zst files
 *
 * FIXES APPLIED:
 *
 * FIX #1  execSync → execFileAsync in loadCursorFromGCS and backupCursorToGCS
 *         execSync blocked the event loop for up to 15s / 10s on every GCS call.
 *         execFile (not exec) — no shell, no injection risk from GCS path strings.
 *
 * FIX #2  loadLiveCursor, saveLiveCursor, backupCursorToGCS made async
 *         They call the now-async GCS functions; every call site updated to await.
 *
 * FIX #3  findLatestTimestamp: duplicate LIVE_MODE/RESUME branches collapsed
 *         The if/else blocks were byte-for-byte identical — pure dead code.
 *         Unreachable `return null` after the if/else also removed.
 *
 * FIX #4  shutdown saves _liveAfterRecordTime, not stale lastTimestamp
 *         lastTimestamp is set at startup and never updated during the run.
 *         afterRecordTime advances every batch — that is what must be persisted.
 *         _liveAfterMigrationId/_liveAfterRecordTime bridge the scope gap.
 *
 * FIX #5  processUpdates: isReassignment = !!item.reassignment only
 *         Old: !!item.reassignment || !!item.event — item.event is not a Scan
 *         API field; produced false positives routing transactions through the
 *         reassignment path (same bug as fetch-backfill.js, now fixed there too).
 *
 * FIX #6  processUpdates: reassignment event path corrected
 *         Old: item.event?.created_event || u?.created_event
 *              (item.event is always undefined for reassignment wrappers)
 *         New: item.reassignment?.event?.created_event
 *              Consistent with fixed decodeInMainThread in fetch-backfill.js.
 *
 * FIX #7  processUpdates: silent effective_at quarantine guards removed
 *         normalizeEvent (fixed upstream) throws on null effective_at with full
 *         context. The warn-and-skip contradicts that contract and silently drops
 *         events. Per-item try/catch (FIX #9) handles the error correctly.
 *
 * FIX #8  processUpdates: event_id mismatch now warns instead of silent overwrite
 *         Consistent with fixed decodeInMainThread in fetch-backfill.js and
 *         decode-worker.js.
 *
 * FIX #9  processUpdates: per-item try/catch added
 *         One malformed tx can no longer abort the entire batch. Errors are
 *         collected, logged, and returned; partial results are still written.
 *
 * FIX #10 findLatestFromRawData: T23:59:59 fallback replaced for ALL days
 *         Original applied the 5-min buffer only to today; historical days still
 *         used T23:59:59.999999Z, overshooting by hours on any incomplete day
 *         and causing the live cursor to skip real data after restart.
 *
 * FIX #11 /tmp GCS temp files → CURSOR_DIR-relative
 *         /tmp is a separate filesystem on some container setups. Cross-filesystem
 *         rename in atomicWriteFile fails silently. All temp files now live under
 *         CURSOR_DIR alongside the real cursor file.
 *
 * FIX #12 normalizeUpdate called with migration_id injected
 *         Old: normalizeUpdate(item) — migration_id was never passed in, so the
 *         normalised update had whatever migration_id (if any) the raw API item
 *         carried, not the locally-tracked migrationId.
 *         New: normalizeUpdate({ ...item, migration_id: migrationId })
 *              Consistent with fixed decodeInMainThread and decode-worker.js.
 *
 * FIX #13 Adaptive page_size / timeout for stuck cursors
 *         When the same cursor times out repeatedly, the API is struggling with
 *         a large response at that position. Every ENDPOINT_ROTATE_AFTER_ERRORS
 *         consecutive timeout hits on the same cursor: page_size is halved
 *         (min 1) and timeout is increased by 1.5x (max 3x base). On a
 *         successful fetch with data, both reset to their configured defaults.
 *         This lets the script break through "heavy" cursor positions (e.g.
 *         large transactions at 2026-03-31T23:40:24) instead of retrying the
 *         exact same failing request 100 times and exiting.
 */

// CRITICAL: Load .env BEFORE any other imports that depend on env vars
import dotenv from 'dotenv';
import { fileURLToPath } from 'url';
import path from 'path';
import { promisify } from 'util';
// FIX #1: execFile (no shell, no injection risk) replaces execSync.
// exec is also imported for DuckDB CLI invocation (needs shell for stdin redirection).
import { execFile as execFileCb, exec as execCb } from 'child_process';

const execFileAsync = promisify(execFileCb);
const execAsync     = promisify(execCb);

// FIX #14: Lazy-loaded GCS SDK for cursor backup/restore.
// Replaces gsutil subprocess calls which depend on gcloud CLI auth that
// expires and requires interactive re-authentication — breaking unattended
// long-running scripts. The SDK uses Application Default Credentials (ADC)
// which auto-refreshes from service account metadata, matching the auth
// path used by gcs-upload-queue.js for data uploads.
let _gcsStorage = null;
let _gcsBucket  = null;

async function getGCSBucket() {
  if (_gcsBucket) return _gcsBucket;
  const { Storage } = await import('@google-cloud/storage');
  _gcsStorage = new Storage();
  _gcsBucket  = _gcsStorage.bucket(process.env.GCS_BUCKET);
  return _gcsBucket;
}

const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);
dotenv.config({ path: path.join(__dirname, '.env') });

import axios from 'axios';
import https from 'https';
import fs   from 'fs';
import { createHash } from 'crypto';
import { normalizeUpdate, normalizeEvent, flattenEventsInTreeOrder, groupByPartition } from './data-schema.js';
import {
  log,
  logBatch,
  logCursor,
  logError,
  logFatal,
  logMetrics,
  logSummary,
} from './structured-logger.js';
import { alert, Severity, logAlertConfig } from './alert.js';
import {
  findLatestFromGCS as findLatestFromGCSImported,
  scanGCSHivePartition as scanGCSHivePartitionImported,
  extractTimestampFromGCSFiles as extractTimestampFromGCSFilesImported,
} from './gcs-scanner.js';
import { atomicWriteFile as _atomicWriteFile } from './atomic-cursor.js';

function atomicWriteFileForLive(filePath, data) {
  _atomicWriteFile(filePath, data);
}

// ─── CLI args ──────────────────────────────────────────────────────────────
const args      = process.argv.slice(2);
const LIVE_MODE  = args.includes('--live') || args.includes('-l');
const KEEP_RAW   = args.includes('--keep-raw') || args.includes('--raw');
const RAW_ONLY   = args.includes('--raw-only') || args.includes('--legacy');
const USE_PARQUET = !RAW_ONLY;
const USE_BINARY  = KEEP_RAW || RAW_ONLY;

import { mapUpdateRecord, mapEventRecord } from './write-parquet.js';
import * as binaryWriter  from './write-binary.js';

// ─── Unified writer helpers ────────────────────────────────────────────────
// The Parquet path uses deterministic per-batch writes (writeBatchToGCS)
// directly from the main loop — no buffering, no flush needed.
// These wrappers now only manage the optional binary writer (--keep-raw).
async function flushAll() {
  if (USE_BINARY) return await binaryWriter.flushAll();
  return [];
}

function getBufferStats() {
  const stats = { updates: 0, events: 0, pendingWrites: 0 };
  if (USE_BINARY) stats.binaryPendingWrites = binaryWriter.getBufferStats().pendingWrites;
  return stats;
}

function setMigrationId(id) {
  if (USE_BINARY) binaryWriter.setMigrationId(id);
  // Parquet path reads migrationId from the local `migrationId` variable
}

function setDataSource(_source) {
  // Parquet path is hardcoded to 'updates' source via writeBatchToGCS.
  // Binary writer always uses backfill path (legacy).
}

// ─── Deterministic write path (exactly-once semantics) ────────────────────
// Same approach as reingest-updates.js: each API batch is written directly to
// GCS with deterministic filenames derived from cursor position. No buffering.
//
// Guarantees:
//   Same cursor position → same API response → same records → same filename
//   → GCS overwrite (not new file) → zero duplicates
//   Cursor saved AFTER GCS upload → zero gaps

// DuckDB SQL path escaper — doubles single quotes to prevent SQL injection
function sqlStr(rawPath) {
  return rawPath.replace(/'/g, "''");
}

// DuckDB column definitions — must match write-parquet.js writeToParquetCLI exactly
const UPDATES_DUCKDB_COLUMNS = [
  "update_id: 'VARCHAR'", "update_type: 'VARCHAR'", "synchronizer_id: 'VARCHAR'",
  "effective_at: 'VARCHAR'", "recorded_at: 'VARCHAR'", "record_time: 'VARCHAR'",
  "timestamp: 'VARCHAR'", "command_id: 'VARCHAR'", "workflow_id: 'VARCHAR'", "kind: 'VARCHAR'",
  "migration_id: 'BIGINT'", '"offset": \'BIGINT\'', "event_count: 'INTEGER'",
  "root_event_ids: 'VARCHAR[]'", "source_synchronizer: 'VARCHAR'",
  "target_synchronizer: 'VARCHAR'", "unassign_id: 'VARCHAR'", "submitter: 'VARCHAR'",
  "reassignment_counter: 'BIGINT'", "trace_context: 'VARCHAR'", "update_data: 'VARCHAR'",
].join(', ');

const EVENTS_DUCKDB_COLUMNS = [
  "event_id: 'VARCHAR'", "update_id: 'VARCHAR'", "event_type: 'VARCHAR'",
  "event_type_original: 'VARCHAR'", "synchronizer_id: 'VARCHAR'", "effective_at: 'VARCHAR'",
  "recorded_at: 'VARCHAR'", "created_at_ts: 'VARCHAR'", "timestamp: 'VARCHAR'",
  "contract_id: 'VARCHAR'", "template_id: 'VARCHAR'", "package_name: 'VARCHAR'",
  "migration_id: 'BIGINT'", "signatories: 'VARCHAR[]'", "observers: 'VARCHAR[]'",
  "acting_parties: 'VARCHAR[]'", "witness_parties: 'VARCHAR[]'", "child_event_ids: 'VARCHAR[]'",
  "consuming: 'BOOLEAN'", "reassignment_counter: 'BIGINT'", "choice: 'VARCHAR'",
  "interface_id: 'VARCHAR'", "source_synchronizer: 'VARCHAR'", "target_synchronizer: 'VARCHAR'",
  "unassign_id: 'VARCHAR'", "submitter: 'VARCHAR'", "payload: 'VARCHAR'",
  "contract_key: 'VARCHAR'", "exercise_result: 'VARCHAR'", "raw_event: 'VARCHAR'",
  "trace_context: 'VARCHAR'",
].join(', ');

const LIVE_TMP_DIR = '/tmp/live-ingest';

// DuckDB temp directory for spilling intermediate state to disk when the
// in-memory 200 MB limit is reached. Without this, `:memory:` databases
// can't spill and a wide batch will OOM the COPY.
const DUCKDB_SPILL_DIR = path.join(LIVE_TMP_DIR, 'duckdb_spill');

// Maximum JSONL payload size handed to a single DuckDB invocation. Above
// this, the partition's records are split into multiple Parquet chunks.
// At 50 MiB, typical update batches (34-38 MiB) pass unsplit; event
// batches (75-85 MiB) split into 2 chunks. See reingest-updates.js for
// the full budget calculation.
const MAX_JSONL_BYTES_PER_CHUNK = 50 * 1024 * 1024; // 50 MiB

// Largest single JSON object DuckDB will accept. Must be ≥ the biggest
// individual record we ever see — a single oversized record gets its own
// chunk, and that chunk's one line has to fit inside this buffer.
const DUCKDB_MAX_OBJECT_SIZE = 48 * 1024 * 1024; // 48 MiB

/**
 * Deterministic filename based on cursor position and partition.
 * Same cursor → same data → same filename → GCS overwrite → no dup.
 * Uses 'live' prefix to distinguish from reingest files ('ri').
 *
 * When a single-partition batch exceeds MAX_JSONL_BYTES_PER_CHUNK, it is
 * split into multiple Parquet chunks. Each chunk gets a `-c{i}of{N}`
 * suffix. Because the chunk decision is a pure function of the input
 * records' serialized bytes, the split is reproducible across retries.
 */
function deterministicFileName(type, afterRecordTime, partition, chunkIdx = 0, chunkCount = 1) {
  const hash = createHash('sha256')
    .update(`${afterRecordTime}|${partition}`)
    .digest('hex')
    .slice(0, 16);
  return chunkCount > 1
    ? `${type}-live-${hash}-c${chunkIdx}of${chunkCount}.parquet`
    : `${type}-live-${hash}.parquet`;
}

/**
 * Run DuckDB CLI to convert a single JSONL file to a single Parquet file.
 * Memory knobs tuned for ~244 MiB OS cap on constrained VMs:
 *   * preserve_insertion_order=false — streams rows without holding the
 *     whole input in RAM just to preserve order
 *   * temp_directory — lets DuckDB spill to disk when memory is tight
 *   * maximum_object_size=48 MiB — single-JSON-object read buffer (was 64
 *     MiB). Must be ≥ the biggest individual record we see, so an
 *     oversized line gets its own chunk that still fits.
 *   * ROW_GROUP_SIZE 5000 — more frequent row-group flushes
 */
async function jsonlToParquetViaDuckDB(jsonlPath, parquetPath, sqlFilePath, type) {
  const columns = type === 'events' ? EVENTS_DUCKDB_COLUMNS : UPDATES_DUCKDB_COLUMNS;
  fs.mkdirSync(DUCKDB_SPILL_DIR, { recursive: true });
  const sql = [
    "SET memory_limit='200MB';",
    "SET threads=1;",
    "SET preserve_insertion_order=false;",
    `SET temp_directory='${sqlStr(DUCKDB_SPILL_DIR)}';`,
    `COPY (SELECT * FROM read_json_auto('${sqlStr(jsonlPath)}', columns={${columns}}, union_by_name=true, maximum_object_size=${DUCKDB_MAX_OBJECT_SIZE}))`,
    `TO '${sqlStr(parquetPath)}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 5000);`,
  ].join('\n');
  fs.writeFileSync(sqlFilePath, sql);

  await execAsync(`duckdb :memory: < "${sqlFilePath}"`, {
    timeout: 180000,
    maxBuffer: 10 * 1024 * 1024,
  });
}

/**
 * Greedy byte-based deterministic chunker. Same input → same chunks → same
 * filenames → GCS overwrite semantics preserved across retries.
 */
function chunkLinesByBytes(lines) {
  const ranges = [];
  let chunkStart = 0;
  let chunkBytes = 0;
  for (let i = 0; i < lines.length; i++) {
    const lineBytes = Buffer.byteLength(lines[i], 'utf8') + 1; // +1 for '\n'
    if (chunkBytes > 0 && chunkBytes + lineBytes > MAX_JSONL_BYTES_PER_CHUNK) {
      ranges.push({ start: chunkStart, end: i });
      chunkStart = i;
      chunkBytes = 0;
    }
    chunkBytes += lineBytes;
  }
  if (chunkStart < lines.length) {
    ranges.push({ start: chunkStart, end: lines.length });
  }
  return ranges;
}

/**
 * Write a single partition's records to Parquet and upload to GCS.
 *
 * For small partitions this writes one file (legacy single-file naming).
 * For large partitions whose serialized JSONL would exceed
 * MAX_JSONL_BYTES_PER_CHUNK, the records are split into N byte-balanced
 * chunks and written as N Parquet files — each through its own DuckDB
 * invocation, so each stays well under the ~244 MiB OS memory cap.
 *
 * Exactly-once semantics are preserved because the chunk count and chunk
 * boundaries are a pure function of the input records' serialized bytes.
 */
async function writePartitionToGCS(records, type, partition, afterRecordTime) {
  if (records.length === 0) return;

  const mapped = records.map(type === 'updates' ? mapUpdateRecord : mapEventRecord);
  const lines = mapped.map(r => JSON.stringify(r));

  const chunkRanges = chunkLinesByBytes(lines);
  const chunkCount = chunkRanges.length;

  if (chunkCount > 1) {
    let totalBytes = 0;
    for (const line of lines) totalBytes += Buffer.byteLength(line, 'utf8') + 1;
    log('info', 'partition_chunked', {
      type, partition, records: records.length, chunks: chunkCount,
      jsonl_mib: +(totalBytes / (1024 * 1024)).toFixed(1),
    });
  }

  fs.mkdirSync(LIVE_TMP_DIR, { recursive: true });

  for (let chunkIdx = 0; chunkIdx < chunkRanges.length; chunkIdx++) {
    const { start, end } = chunkRanges[chunkIdx];
    const chunkLines = lines.slice(start, end);
    const chunkRecordCount = end - start;

    const fileName    = deterministicFileName(type, afterRecordTime, partition, chunkIdx, chunkCount);
    const jsonlPath   = path.join(LIVE_TMP_DIR, fileName.replace('.parquet', '.jsonl'));
    const parquetPath = path.join(LIVE_TMP_DIR, fileName);
    const sqlFilePath = path.join(LIVE_TMP_DIR, fileName.replace('.parquet', '.sql'));

    try {
      // 1. Write this chunk's JSONL slice
      fs.writeFileSync(jsonlPath, chunkLines.join('\n') + '\n');

      // 2. Convert JSONL → Parquet via memory-tuned DuckDB CLI
      await jsonlToParquetViaDuckDB(jsonlPath, parquetPath, sqlFilePath, type);

      // 3. Upload to GCS via SDK (deterministic path → overwrite = idempotent, zero dups)
      const gcsObjectPath = `raw/${partition}/${fileName}`;
      const bucket = await getGCSBucket();
      await bucket.upload(parquetPath, {
        destination: gcsObjectPath,
        metadata: { contentType: 'application/octet-stream' },
      });

      log('info', 'parquet_uploaded', {
        type, records: chunkRecordCount, partition, fileName,
        ...(chunkCount > 1 ? { chunk: chunkIdx + 1, chunks: chunkCount } : {}),
      });
    } finally {
      for (const p of [jsonlPath, parquetPath, sqlFilePath]) {
        try { if (fs.existsSync(p)) fs.unlinkSync(p); } catch {}
      }
    }
  }
}

/**
 * Write a full API batch (updates + events) to GCS with deterministic filenames.
 * Each partition is written and uploaded serially to minimize memory.
 */
async function writeBatchToGCS(updates, events, batchMigrationId, afterRecordTime) {
  if (updates.length > 0) {
    const groups = groupByPartition(updates, 'updates', 'updates', batchMigrationId);
    for (const [partition, records] of Object.entries(groups)) {
      await writePartitionToGCS(records, 'updates', partition, afterRecordTime);
    }
  }
  if (events.length > 0) {
    const groups = groupByPartition(events, 'events', 'updates', batchMigrationId);
    for (const [partition, records] of Object.entries(groups)) {
      await writePartitionToGCS(records, 'events', partition, afterRecordTime);
    }
  }
}

// ─── Configuration ─────────────────────────────────────────────────────────
let activeScanUrl = process.env.SCAN_URL || 'https://scan.sv-1.global.canton.network.sync.global/api/scan';
const BATCH_SIZE                     = parseInt(process.env.BATCH_SIZE)                     || 100;
const POLL_INTERVAL                  = parseInt(process.env.POLL_INTERVAL)                  || 5000;
const FETCH_TIMEOUT_MS               = parseInt(process.env.FETCH_TIMEOUT_MS)               || 30000;
const STALL_DETECTION_INTERVAL_MS    = parseInt(process.env.STALL_DETECTION_INTERVAL_MS)    || 30000;
const STALL_THRESHOLD_MS             = parseInt(process.env.STALL_THRESHOLD_MS)             || 120000;
const GCS_CURSOR_BACKUP_MAX_FAILURES = parseInt(process.env.GCS_CURSOR_BACKUP_MAX_FAILURES) || 5;
const MAX_TRANSIENT_ERRORS           = parseInt(process.env.MAX_TRANSIENT_ERRORS)           || 100;
const ENDPOINT_ROTATE_AFTER_ERRORS   = parseInt(process.env.ENDPOINT_ROTATE_AFTER_ERRORS)  || 3;

const ALL_SCAN_ENDPOINTS = [
  { name: 'Global-Synchronizer-Foundation',  url: 'https://scan.sv-1.global.canton.network.sync.global/api/scan' },
  { name: 'Digital-Asset-1',                 url: 'https://scan.sv-1.global.canton.network.digitalasset.com/api/scan' },
  { name: 'Digital-Asset-2',                 url: 'https://scan.sv-2.global.canton.network.digitalasset.com/api/scan' },
  { name: 'Cumberland-1',                    url: 'https://scan.sv-1.global.canton.network.cumberland.io/api/scan' },
  { name: 'Cumberland-2',                    url: 'https://scan.sv-2.global.canton.network.cumberland.io/api/scan' },
  { name: 'Five-North-1',                    url: 'https://scan.sv-1.global.canton.network.fivenorth.io/api/scan' },
  { name: 'Tradeweb-Markets-1',              url: 'https://scan.sv-1.global.canton.network.tradeweb.com/api/scan' },
  { name: 'Proof-Group-1',                   url: 'https://scan.sv-1.global.canton.network.proofgroup.xyz/api/scan' },
  { name: 'Liberty-City-Ventures-1',         url: 'https://scan.sv-1.global.canton.network.lcv.mpch.io/api/scan' },
  { name: 'MPC-Holding-Inc',                 url: 'https://scan.sv-1.global.canton.network.mpch.io/api/scan' },
  { name: 'Orb-1-LP-1',                      url: 'https://scan.sv-1.global.canton.network.orb1lp.mpch.io/api/scan' },
  { name: 'SV-Nodeops-Limited',              url: 'https://scan.sv.global.canton.network.sv-nodeops.com/api/scan' },
  { name: 'C7-Technology-Services-Limited',  url: 'https://scan.sv-1.global.canton.network.c7.digital/api/scan' },
];

/**
 * Determine TLS rejectUnauthorized setting.
 */
export function getTLSRejectUnauthorized() {
  return process.env.INSECURE_TLS !== 'true';
}

/**
 * Create a fresh axios client. Called at startup and after endpoint failover
 * to ensure stale TCP connections from a hung node don't block new requests.
 */
function createClient(baseURL) {
  return axios.create({
    baseURL,
    timeout:    FETCH_TIMEOUT_MS,
    httpsAgent: new https.Agent({
      rejectUnauthorized: getTLSRejectUnauthorized(),
      keepAlive: true,
      keepAliveMsecs: 10000,
      maxSockets: 4,
      timeout: FETCH_TIMEOUT_MS,  // socket-level timeout
    }),
  });
}

let client = createClient(activeScanUrl);

import { getBaseDataDir, getCursorDir, isGCSMode, logPathConfig, validateGCSBucket } from './path-utils.js';
import { runPreflightChecks } from './gcs-preflight.js';

const DATA_DIR = getBaseDataDir();
const GCS_MODE = isGCSMode();

// ─── Runtime state ─────────────────────────────────────────────────────────
let lastTimestamp   = null;   // Set once at startup — NOT updated during the loop
let lastMigrationId = null;
let migrationId     = null;
let isRunning       = true;

// FIX #4: Track the in-loop cursor so shutdown() can persist the most recently
// confirmed position rather than the stale startup lastTimestamp.
// Updated on every periodic saveLiveCursor call inside the main loop.
let _liveAfterMigrationId = null;
let _liveAfterRecordTime  = null;

let sessionErrorCount     = 0;
let sessionStartTime      = Date.now();
let lastProgressTimestamp = Date.now();
let stallWatchdogInterval = null;
let heartbeatInterval     = null;
let currentCycleId        = 0;
let gcsCursorBackupConsecutiveFailures = 0;

// FIX #13: Adaptive fetch parameters for stuck cursors.
// When the same cursor times out repeatedly, the API is likely struggling with
// a large/complex response at that position. Progressively reduce page_size
// and increase timeout to break through.
let _adaptivePageSize    = BATCH_SIZE;     // current effective page_size
let _adaptiveTimeoutMs   = FETCH_TIMEOUT_MS; // current effective timeout
let _stuckCursor         = null;           // cursor value that is repeatedly failing
let _stuckCursorHits     = 0;             // consecutive errors at the same cursor

const CURSOR_DIR       = getCursorDir();
const LIVE_CURSOR_FILE = path.join(CURSOR_DIR, 'live-cursor.json');

// ─── API time helper ───────────────────────────────────────────────────────
async function getCurrentAPITime() {
  try {
    const response = await client.post('/v2/updates', {
      page_size: 1,
      daml_value_encoding: 'compact_json',
    });
    const transactions = response.data?.transactions || [];
    if (transactions.length > 0) {
      return { recordTime: transactions[0].record_time, migrationId: transactions[0].migration_id };
    }
    return null;
  } catch (err) {
    console.error('Failed to get current API time:', err.message);
    return null;
  }
}

// ─── Cursor: load ──────────────────────────────────────────────────────────

/**
 * Load live cursor — tries local file first, then GCS backup.
 *
 * FIX #2: Now async (calls async loadCursorFromGCS).
 */
async function loadLiveCursor() {
  if (fs.existsSync(LIVE_CURSOR_FILE)) {
    try {
      const content = fs.readFileSync(LIVE_CURSOR_FILE, 'utf8').trim();
      if (!content || content.length === 0) {
        log('warn', 'cursor_empty', { file: LIVE_CURSOR_FILE });
      } else {
        const data = JSON.parse(content);
        if (!data.migration_id || !data.record_time) {
          log('warn', 'cursor_invalid', { file: LIVE_CURSOR_FILE, reason: 'missing_fields' });
        } else {
          const cursorTime = new Date(data.record_time).getTime();
          if (cursorTime > Date.now()) {
            log('warn', 'cursor_future', { file: LIVE_CURSOR_FILE, record_time: data.record_time });
          } else {
            logCursor('loaded', { migrationId: data.migration_id, lastBefore: data.record_time, source: 'local' });
            return data;
          }
        }
      }
    } catch (err) {
      logError('cursor_read', err, { file: LIVE_CURSOR_FILE });
    }
  }

  if (GCS_MODE) {
    // FIX #2: await the now-async GCS restore
    const gcsCursor = await loadCursorFromGCS();
    if (gcsCursor) {
      log('info', 'cursor_restored_from_gcs', { migration: gcsCursor.migration_id, recordTime: gcsCursor.record_time });
      saveLiveCursorLocal(gcsCursor.migration_id, gcsCursor.record_time);
      return gcsCursor;
    }
  }

  return null;
}

/**
 * Save cursor to local file only (no GCS backup).
 * Used when restoring from GCS to avoid circular backup.
 */
function saveLiveCursorLocal(migId, afterRecordTime) {
  if (!fs.existsSync(CURSOR_DIR)) fs.mkdirSync(CURSOR_DIR, { recursive: true });
  const cursor = {
    migration_id:  migId,
    record_time:   afterRecordTime,
    updated_at:    new Date().toISOString(),
    mode:          'live',
    semantics:     'forward',
    restored_from: 'gcs',
  };
  atomicWriteFileForLive(LIVE_CURSOR_FILE, cursor);
}

/**
 * Load cursor from GCS backup.
 *
 * FIX #14: Uses @google-cloud/storage SDK instead of gsutil subprocess.
 *   gsutil depends on gcloud CLI auth which expires and requires interactive
 *   re-authentication. The SDK uses Application Default Credentials (service
 *   account metadata on GCE/GKE, or GOOGLE_APPLICATION_CREDENTIALS env var)
 *   which auto-refreshes without user interaction.
 */
async function loadCursorFromGCS() {
  const GCS_BUCKET = process.env.GCS_BUCKET;
  if (!GCS_BUCKET) return null;

  try {
    const bucket = await getGCSBucket();
    const file   = bucket.file('cursors/live-cursor.json');

    const [exists] = await file.exists();
    if (!exists) return null;

    const [contents] = await file.download();
    const content = contents.toString('utf8').trim();
    if (!content) return null;

    const data = JSON.parse(content);
    if (!data.migration_id || !data.record_time) {
      log('warn', 'gcs_cursor_invalid', { reason: 'missing_fields' });
      return null;
    }
    if (new Date(data.record_time).getTime() > Date.now()) {
      log('warn', 'gcs_cursor_future', { record_time: data.record_time });
      return null;
    }

    console.log(`☁️ Restored cursor from GCS: migration=${data.migration_id}, time=${data.record_time}`);
    return data;
  } catch (err) {
    if (!err.message?.includes('No such object') && err.code !== 404) {
      console.warn(`⚠️ Could not load cursor from GCS: ${err.message}`);
    }
    return null;
  }
}

/**
 * Save live cursor state.
 *
 * FIX #2: Now async — awaits backupCursorToGCS so errors surface and the
 * event loop is not blocked during the gsutil upload.
 */
async function saveLiveCursor(migId, afterRecordTime) {
  try {
    if (!fs.existsSync(CURSOR_DIR)) {
      fs.mkdirSync(CURSOR_DIR, { recursive: true });
    }

    const cursor = {
      migration_id: migId,
      record_time:  afterRecordTime,
      updated_at:   new Date().toISOString(),
      mode:         'live',
      semantics:    'forward',
    };

    atomicWriteFileForLive(LIVE_CURSOR_FILE, cursor);
    console.log(`  ✅ Local cursor saved: ${afterRecordTime}`);

    if (GCS_MODE) {
      // FIX #2: await the now-async backup — does not block event loop
      await backupCursorToGCS(cursor);
    } else {
      console.log(`  ⚠️ GCS_MODE disabled, skipping GCS backup`);
    }
  } catch (err) {
    console.error(`  ❌ Failed to save cursor: ${err.message}`);
    console.error(`     Stack: ${err.stack}`);
  }
}

/**
 * Backup cursor to GCS.
 *
 * FIX #14: Uses @google-cloud/storage SDK instead of gsutil subprocess.
 *   Eliminates dependency on gcloud CLI auth which expires during long runs.
 *   SDK uploads the JSON directly from memory — no temp files needed.
 */
async function backupCursorToGCS(cursor) {
  const GCS_BUCKET = process.env.GCS_BUCKET;
  if (!GCS_BUCKET) {
    console.log('  ⚠️ Cursor backup skipped: GCS_BUCKET not set');
    return;
  }

  const gcsPath = `gs://${GCS_BUCKET}/cursors/live-cursor.json`;

  try {
    const bucket = await getGCSBucket();
    const file   = bucket.file('cursors/live-cursor.json');

    await file.save(JSON.stringify(cursor, null, 2), {
      contentType: 'application/json',
      resumable:   false, // small file, no need for resumable upload
    });

    console.log(`  ☁️ Cursor backed up: ${gcsPath}`);
    gcsCursorBackupConsecutiveFailures = 0;
    log('debug', 'cursor_backed_up_to_gcs', {
      gcsPath,
      migration:  cursor.migration_id,
      recordTime: cursor.record_time,
    });
  } catch (err) {
    gcsCursorBackupConsecutiveFailures++;
    console.warn(`  ⚠️ Failed to backup cursor to GCS (${gcsCursorBackupConsecutiveFailures}/${GCS_CURSOR_BACKUP_MAX_FAILURES}): ${err.message}`);
    console.warn(`     Target path: ${gcsPath}`);

    if (gcsCursorBackupConsecutiveFailures >= GCS_CURSOR_BACKUP_MAX_FAILURES) {
      logFatal('gcs_cursor_backup_failed', new Error(
        `GCS cursor backup failed ${gcsCursorBackupConsecutiveFailures} consecutive times. ` +
        `Cursor is NOT being persisted to cloud.`
      ));
      alert(Severity.CRITICAL, 'gcs_cursor_backup_failed', 'GCS cursor backup failing repeatedly', {
        'Consecutive Failures': gcsCursorBackupConsecutiveFailures,
        'Error': err.message,
        'GCS Path': gcsPath,
      });
    }
  }
}

// ─── Timestamp discovery ───────────────────────────────────────────────────

/**
 * Find the latest timestamp from all available sources.
 *
 * FIX #2: Awaits the now-async loadLiveCursor and saveLiveCursor.
 * FIX #3: The original had two if/else branches (LIVE_MODE vs RESUME) that were
 *   byte-for-byte identical — pure dead code. Collapsed into one branch.
 *   The unreachable `return null` after the if/else block is also removed.
 */
async function findLatestTimestamp() {
  const rawDir = path.join(DATA_DIR, 'raw');
  let rawDataResult = null;
  if (fs.existsSync(rawDir)) {
    rawDataResult = await findLatestFromRawData(rawDir);
  }

  let gcsDataResult = null;
  if (GCS_MODE) {
    console.log('\n🔍 Scanning GCS for latest data position...');
    // FIX #7: findLatestFromGCS is now async (gcs-scanner.js FIX #1) — must await
    gcsDataResult = await findLatestFromGCS();
  }

  // FIX #2: await the now-async loadLiveCursor
  const liveCursor        = await loadLiveCursor();
  const backfillTime      = findLatestFromCursors();
  const backfillMigration = lastMigrationId;

  const candidates = [];
  if (liveCursor)    candidates.push({ source: 'live-cursor',    migration: liveCursor.migration_id,  time: liveCursor.record_time });
  if (backfillTime)  candidates.push({ source: 'backfill-cursor', migration: backfillMigration,        time: backfillTime });
  if (rawDataResult) candidates.push({ source: 'raw-data-local', migration: rawDataResult.migrationId, time: rawDataResult.timestamp });
  if (gcsDataResult) candidates.push({ source: 'gcs-data',       migration: gcsDataResult.migrationId, time: gcsDataResult.timestamp });

  // FIX #3: Collapsed LIVE_MODE / non-LIVE_MODE branches — they were identical
  if (candidates.length === 0) {
    const label = LIVE_MODE ? '🔴 LIVE MODE' : '📁';
    console.log(`${label}: No cursors or raw data found, starting fresh`);
    return null;
  }

  candidates.sort((a, b) => {
    if (a.migration !== b.migration) return b.migration - a.migration;
    return new Date(b.time).getTime() - new Date(a.time).getTime();
  });

  const best = candidates[0];
  console.log(`📍 Best resume point: ${best.source} -> migration=${best.migration}, time=${best.time}`);
  for (const c of candidates) {
    console.log(`   ${c === best ? '✓' : ' '} ${c.source}: m${c.migration} @ ${c.time}`);
  }

  if (best.source !== 'live-cursor' && best.source !== 'backfill-cursor') {
    console.log(`  🔄 Auto-syncing cursor: ${best.source} is ahead of cursors`);
    // FIX #2: await the now-async saveLiveCursor
    await saveLiveCursor(best.migration, best.time);
  }

  lastMigrationId = best.migration;
  return best.time;
  // FIX #3: Removed unreachable `return null` that followed the original if/else.
}

/**
 * Find latest timestamp from backfill cursor files.
 * For live updates we want to continue FORWARD from max_time.
 */
function findLatestFromCursors() {
  if (!fs.existsSync(CURSOR_DIR)) {
    console.log('📁 No cursor directory found');
    return null;
  }

  const cursorFiles = fs.readdirSync(CURSOR_DIR).filter(f => f.endsWith('.json'));
  if (cursorFiles.length === 0) {
    console.log('📁 No cursor files found');
    return null;
  }
  console.log(`📁 Found ${cursorFiles.length} cursor file(s)`);

  let latestTimestamp = null;
  let latestMigration = null;
  let selectedCursor  = null;

  for (const file of cursorFiles) {
    try {
      const cursorPath = path.join(CURSOR_DIR, file);
      const cursor     = JSON.parse(fs.readFileSync(cursorPath, 'utf8'));
      if (!cursor.migration_id && !cursor.max_time && !cursor.min_time) continue;

      const migration = cursor.migration_id;
      const maxTime   = cursor.max_time;

      if (maxTime) {
        const timestamp   = new Date(maxTime).getTime();
        const currentBest = latestTimestamp ? new Date(latestTimestamp).getTime() : 0;

        if (!latestTimestamp ||
            migration > latestMigration ||
            (migration === latestMigration && timestamp > currentBest)) {
          latestTimestamp = maxTime;
          latestMigration = migration;
          selectedCursor  = cursor;
        }
      }

      console.log(`   • ${file}: migration=${migration}, max_time=${maxTime}, complete=${cursor.complete || false}`);
    } catch (err) {
      console.warn(`   ⚠️ Failed to read cursor ${file}: ${err.message}`);
    }
  }

  if (latestTimestamp && selectedCursor) {
    console.log(`📍 Live updates will continue from: migration=${latestMigration}, timestamp=${latestTimestamp}`);
    lastMigrationId = latestMigration;
    return latestTimestamp;
  }
  return null;
}

/**
 * Find the latest data timestamp in GCS.
 * FIX #7: Now async — findLatestFromGCSImported (gcs-scanner.js) is async after FIX #1.
 */
async function findLatestFromGCS() {
  const result = await findLatestFromGCSImported({
    bucket: process.env.GCS_BUCKET,
    logFn:  (level, msg, data) => log(level, msg, data),
  });
  if (result) {
    console.log(`  ☁️ GCS data scan: latest partition at migration=${result.migrationId}, time=${result.timestamp}`);
    console.log(`     Source: ${result.source}`);
  }
  return result;
}

/**
 * Find latest timestamp from raw binary data files.
 *
 * FIX #10: T23:59:59 fallback replaced with end-of-day-minus-5-minutes for ALL days.
 *
 *   Original behaviour:
 *     today     → now minus 5min         ✓ (correct)
 *     other day → T23:59:59.999999Z      ✗ (overshoots by hours)
 *
 *   When the live cursor is set to T23:59:59 on a day whose last real record is
 *   e.g. T16:00:00, the AFTER query skips ~8 hours of data that was never
 *   ingested. end-of-day-minus-5-min is conservative enough to avoid
 *   re-ingesting the last few minutes without overshooting by hours.
 *   This fix applies to both the Hive-partition path and the legacy path.
 */
async function findLatestFromRawData(rawDir) {
  let latestResult = null;

  const backfillUpdatesDir = path.join(rawDir, 'backfill', 'updates');
  const updatesDir         = path.join(rawDir, 'updates');
  const backfillDir        = path.join(rawDir, 'backfill');

  let searchDir = rawDir;
  if      (fs.existsSync(backfillUpdatesDir)) searchDir = backfillUpdatesDir;
  else if (fs.existsSync(updatesDir))         searchDir = updatesDir;
  else if (fs.existsSync(backfillDir))        searchDir = backfillDir;

  const migrationDirs = fs.readdirSync(searchDir)
    .filter(d => d.startsWith('migration='))
    .map(d => ({ name: d, id: parseInt(d.replace('migration=', '')) || 0 }))
    .sort((a, b) => b.id - a.id);

  outer:
  for (const migDir of migrationDirs) {
    const migPath = path.join(searchDir, migDir.name);

    const yearDirs = fs.readdirSync(migPath)
      .filter(d => d.startsWith('year='))
      .map(d => parseInt(d.replace('year=', '')) || 0)
      .sort((a, b) => b - a);

    for (const year of yearDirs) {
      const yearPath = path.join(migPath, `year=${year}`);

      const monthDirs = fs.readdirSync(yearPath)
        .filter(d => d.startsWith('month='))
        .map(d => parseInt(d.replace('month=', '')) || 0)
        .sort((a, b) => b - a);

      for (const month of monthDirs) {
        const monthPath = path.join(yearPath, `month=${month}`);

        const dayDirs = fs.readdirSync(monthPath)
          .filter(d => d.startsWith('day='))
          .map(d => parseInt(d.replace('day=', '')) || 0)
          .sort((a, b) => b - a);

        if (dayDirs.length > 0) {
          const latestDay = dayDirs[0];
          const dateStr   = `${year}-${String(month).padStart(2, '0')}-${String(latestDay).padStart(2, '0')}`;
          const dayPath   = path.join(monthPath, `day=${latestDay}`);
          let timestamp   = null;

          try {
            const files = fs.readdirSync(dayPath)
              .filter(f => f.endsWith('.pb.zst') || f.endsWith('.parquet'))
              .sort()
              .reverse();
            if (files.length > 0) {
              const match = files[0].match(/(\d{4}-\d{2}-\d{2}T[\d-]+\.\d+Z)/);
              if (match) {
                timestamp = match[1].replace(/(\d{2})-(\d{2})-(\d{2})\./, '$1:$2:$3.');
              }
            }
          } catch {
            // Ignore directory read errors
          }

          if (!timestamp) {
            // FIX #10: end-of-day-minus-5-min for ALL days, not just today.
            // T23:59:59 on any day with an earlier last record causes the live
            // cursor to skip real data between the actual last record and midnight.
            const endOfDay = new Date(`${dateStr}T23:59:59.999Z`);
            endOfDay.setMinutes(endOfDay.getMinutes() - 5);
            timestamp = endOfDay.toISOString();
          }

          latestResult = {
            migrationId: migDir.id,
            timestamp,
            source: `migration=${migDir.id}/year=${year}/month=${month}/day=${latestDay}`,
          };
          break outer;
        }
      }
    }
  }

  // Legacy format: raw/events/migration-X/YYYY-MM-DD/
  if (!latestResult) {
    for (const subDir of ['events', 'updates']) {
      const targetDir = path.join(rawDir, subDir);
      if (!fs.existsSync(targetDir)) continue;

      const legacyMigDirs = fs.readdirSync(targetDir)
        .filter(d => d.startsWith('migration-'))
        .map(d => ({ name: d, id: parseInt(d.replace('migration-', '')) || 0 }))
        .sort((a, b) => b.id - a.id);

      for (const migDir of legacyMigDirs) {
        const migPath  = path.join(targetDir, migDir.name);
        const dateDirs = fs.readdirSync(migPath)
          .filter(d => /^\d{4}-\d{2}-\d{2}$/.test(d))
          .sort()
          .reverse();

        if (dateDirs.length > 0) {
          const latestDate = dateDirs[0];
          // FIX #10: same fix applied to the legacy path
          const endOfDay = new Date(`${latestDate}T23:59:59.999Z`);
          endOfDay.setMinutes(endOfDay.getMinutes() - 5);
          const timestamp = endOfDay.toISOString();

          if (!latestResult ||
              migDir.id > latestResult.migrationId ||
              (migDir.id === latestResult.migrationId && new Date(timestamp) > new Date(latestResult.timestamp))) {
            latestResult = { migrationId: migDir.id, timestamp, source: `${subDir}/${migDir.name}/${latestDate}` };
          }
          break;
        }
      }
    }
  }

  if (latestResult) {
    console.log(`📁 Found raw data: ${latestResult.source} -> migration=${latestResult.migrationId}, time=${latestResult.timestamp}`);
    return latestResult;
  }
  return null;
}

// ─── Migration detection ───────────────────────────────────────────────────

async function detectLatestMigration() {
  if (lastMigrationId !== null) {
    migrationId = lastMigrationId;
    console.log(`📍 Using migration_id from backfill cursor: ${migrationId}`);
    setMigrationId(migrationId);
    return migrationId;
  }
  try {
    const response     = await client.post('/v2/updates', { page_size: 1 });
    const transactions = response.data?.transactions || [];
    if (transactions.length > 0 && transactions[0].migration_id !== undefined) {
      migrationId = transactions[0].migration_id;
      console.log(`📍 Detected migration_id from API: ${migrationId}`);
      setMigrationId(migrationId);
      return migrationId;
    }
    console.warn('⚠️ Could not detect migration_id, using default: 1');
    migrationId = 1;
    setMigrationId(migrationId);
    return migrationId;
  } catch (err) {
    console.error('Failed to detect migration:', err.message);
    console.warn('⚠️ Using fallback migration_id: 1');
    migrationId = 1;
    setMigrationId(migrationId);
    return migrationId;
  }
}

// ─── Fetch ─────────────────────────────────────────────────────────────────

async function fetchUpdates(afterMigrationId = null, afterRecordTime = null) {
  // FIX #13: Use adaptive timeout/page_size when stuck at the same cursor
  const effectiveTimeout  = _adaptiveTimeoutMs;
  const effectivePageSize = _adaptivePageSize;

  const controller = new AbortController();
  const timeoutId  = setTimeout(() => {
    controller.abort();
    log('warn', 'fetch_timeout', { migration: afterMigrationId, cursor: afterRecordTime, timeout_ms: effectiveTimeout, page_size: effectivePageSize });
  }, effectiveTimeout);

  try {
    const payload = { page_size: effectivePageSize, daml_value_encoding: 'compact_json' };

    if (afterMigrationId !== null && afterRecordTime) {
      payload.after = { after_migration_id: afterMigrationId, after_record_time: afterRecordTime };
      if (!fetchUpdates._loggedFirst) {
        console.log(`📡 LIVE query: after=(migration=${afterMigrationId}, time=${afterRecordTime})`);
        fetchUpdates._loggedFirst = true;
      }
    }

    const fetchStart     = Date.now();
    const response       = await client.post('/v2/updates', payload, { signal: controller.signal });
    const fetchLatencyMs = Date.now() - fetchStart;
    const transactions   = response.data?.transactions || [];

    log('info', 'fetch_complete', {
      updatesFetched: transactions.length,
      eventsFetched:  transactions.reduce((sum, t) => {
        const u = t.transaction || t.reassignment || t;
        return sum + Object.keys(u?.events_by_id || u?.eventsById || {}).length;
      }, 0),
      apiLatencyMs: fetchLatencyMs,
      page_size: effectivePageSize,
    });

    return {
      items:           transactions,
      lastMigrationId: transactions.length > 0 ? transactions[transactions.length - 1].migration_id : null,
      // FIX #2: Use the MAX record_time across the entire batch, not the last element's.
      // If the API returns transactions in an order where the last item does not have
      // the highest record_time, advancing the cursor to transactions[last].record_time
      // could set it lower than some items already returned — causing re-fetches of
      // already-seen data (harmless via dedup) or, in the opposite case, skipping
      // transactions at a record_time that the cursor overshoots.
      lastRecordTime: transactions.length > 0
        ? transactions.reduce((max, tx) =>
            tx.record_time > max ? tx.record_time : max,
            transactions[0].record_time)
        : null,
    };
  } catch (err) {
    if (err.name === 'AbortError' || err.code === 'ERR_CANCELED') {
      const e = new Error(`Fetch timed out after ${effectiveTimeout}ms (page_size=${effectivePageSize})`);
      e.code  = 'FETCH_TIMEOUT';
      throw e;
    }
    if (err.response?.status === 404) return { items: [], lastMigrationId: null, lastRecordTime: null };
    if (err.response?.status >= 400 && err.response?.status < 500) {
      // Auth/client errors need immediate visibility
      alert(
        err.response.status === 401 || err.response.status === 403 ? Severity.CRITICAL : Severity.WARNING,
        `http_${err.response.status}`,
        `API returned HTTP ${err.response.status}${err.response.status === 401 ? ' (Unauthorized)' : err.response.status === 403 ? ' (Forbidden)' : ''}`,
        {
          'Status': err.response.status,
          'URL': activeScanUrl + '/v2/updates',
          'Cursor': afterRecordTime,
          'Migration': afterMigrationId,
          'Response': typeof err.response.data === 'string' ? err.response.data.slice(0, 200) : JSON.stringify(err.response.data)?.slice(0, 200),
        }
      );
      console.error('\n' + '='.repeat(60));
      console.error(`🔐 ${err.response.status} ERROR - Full diagnostic info:`);
      console.error('='.repeat(60));
      console.error('Request URL:', activeScanUrl + '/v2/updates');
      console.error('After migration:', afterMigrationId);
      console.error('After record_time:', afterRecordTime);
      console.error('\nResponse status:', err.response.status);
      console.error('Response statusText:', err.response.statusText);
      console.error('Response headers:', JSON.stringify(err.response.headers || {}, null, 2));
      console.error('Response body:', typeof err.response.data === 'string'
        ? err.response.data
        : JSON.stringify(err.response.data, null, 2));
      console.error('='.repeat(60) + '\n');
    }
    throw err;
  } finally {
    clearTimeout(timeoutId);
  }
}

// ─── Process ───────────────────────────────────────────────────────────────

/**
 * Process a batch of live updates into normalised updates + events.
 *
 * FIX #5: isReassignment = !!item.reassignment only.
 *   Old: !!item.reassignment || !!item.event — item.event is not a Scan API
 *   field; it produced false positives routing transactions through the
 *   reassignment path. Same bug fixed in decodeInMainThread (fetch-backfill.js).
 *
 * FIX #6: Reassignment event path corrected to item.reassignment.event.{created,archived}_event.
 *   Old: item.event?.created_event || u?.created_event
 *   item.event is always undefined for reassignment wrappers, so ce/ae were
 *   always null and every reassignment silently produced zero events.
 *   Consistent with fixed decodeInMainThread in fetch-backfill.js.
 *
 * FIX #7: Silent effective_at quarantine guards removed.
 *   normalizeEvent (fixed upstream) throws on null effective_at with full context.
 *   Keeping warn-and-skip contradicts that contract and silently drops events.
 *   Per-item try/catch (FIX #9) catches the throw and logs it with tx context.
 *
 * FIX #8: event_id mismatch now warns instead of silently overwriting.
 *   Consistent with decodeInMainThread (fetch-backfill.js) and decode-worker.js.
 *
 * FIX #9: Per-item try/catch so one malformed tx cannot abort the entire batch.
 *   Errors are collected and logged; partial results are still buffered.
 *
 * FIX #12: normalizeUpdate called with migration_id injected via spread.
 *   Old: normalizeUpdate(item) — migration_id from the raw item may be absent
 *   or stale. Consistent with decodeInMainThread and decode-worker.js.
 */
async function processUpdates(items) {
  const updates = [];
  const events  = [];
  const errors  = [];

  // Single stable timestamp for the whole batch (consistent recorded_at)
  const batchTimestamp = new Date();

  for (const item of items) {
    // FIX #9: per-item try/catch — one bad tx cannot abort the whole batch
    try {
      // FIX #12: inject migration_id so normalizeUpdate always has it
      const update = normalizeUpdate({ ...item, migration_id: migrationId }, { batchTimestamp });
      updates.push(update);

      // FIX #5: !!item.reassignment only — item.event is not a Scan API field
      const isReassignment = !!item.reassignment;

      const u = item.transaction || item.reassignment || item;

      const updateInfo = {
        record_time:     u.record_time,
        effective_at:    u.effective_at,
        synchronizer_id: u.synchronizer_id,
        source:          u.source     || null,
        target:          u.target     || null,
        unassign_id:     u.unassign_id || null,
        submitter:       u.submitter  || null,
        counter:         u.counter    ?? null,
      };

      if (isReassignment) {
        // FIX #6: correct API path — item.reassignment.event.{created,archived}_event
        const ce = item.reassignment?.event?.created_event;
        const ae = item.reassignment?.event?.archived_event;

        if (ce) {
          const ev = normalizeEvent(ce, update.update_id, migrationId, item, updateInfo, { batchTimestamp });
          ev.event_type = 'reassign_create';
          // FIX #7: normalizeEvent throws on null effective_at — no quarantine guard needed
          events.push(ev);
        }
        if (ae) {
          const ev = normalizeEvent(ae, update.update_id, migrationId, item, updateInfo, { batchTimestamp });
          ev.event_type = 'reassign_archive';
          events.push(ev);
        }

      } else {
        // Transaction path
        const eventsById   = u?.events_by_id  || u?.eventsById   || {};
        const rootEventIds = u?.root_event_ids || u?.rootEventIds || [];

        const flattened = flattenEventsInTreeOrder(eventsById, rootEventIds);
        for (const rawEvent of flattened) {
          const ev = normalizeEvent(rawEvent, update.update_id, migrationId, rawEvent, updateInfo, { batchTimestamp });

          // FIX #8: warn on event_id key/field mismatch instead of silently overwriting
          const mapKeyId = rawEvent.event_id;
          if (mapKeyId && ev.event_id && mapKeyId !== ev.event_id) {
            console.warn(
              `[fetch-updates] event_id mismatch for update=${update.update_id}: ` +
              `eventsById key="${mapKeyId}" vs event.event_id="${ev.event_id}". ` +
              `Using map key as authoritative (structural API identifier).`
            );
            ev.event_id = mapKeyId;
          } else if (mapKeyId && !ev.event_id) {
            ev.event_id = mapKeyId;
          }

          // FIX #7: normalizeEvent throws on null effective_at — no quarantine guard needed
          events.push(ev);
        }
      }

    } catch (err) {
      // FIX #9: collect per-tx errors; partial batch still buffered for writing
      const txId = item?.update_id || item?.transaction?.update_id || item?.reassignment?.update_id || 'UNKNOWN';
      console.error(`[fetch-updates] Failed to process tx ${txId}: ${err.message}`);
      errors.push({ tx_id: txId, error: err.message, stack: err.stack });
    }
  }

  if (errors.length > 0) {
    log('warn', 'batch_process_errors', {
      count:  errors.length,
      total:  items.length,
      tx_ids: errors.map(e => e.tx_id),
    });
  }

  // Return normalized records for the caller to write.
  // Previously this called bufferUpdates/bufferEvents which accumulated records
  // in the parquetWriter's internal buffer with random filenames. Now the caller
  // writes directly to GCS with deterministic filenames (zero dups).
  return { updates, events, updateCount: updates.length, eventCount: events.length, errors: errors.length };
}

// ─── Endpoint probe ────────────────────────────────────────────────────────

async function probeAllScanEndpoints() {
  console.log(`\n${'─'.repeat(60)}`);
  console.log(`  SCAN API ENDPOINT REACHABILITY CHECK`);
  console.log(`  Probing ${ALL_SCAN_ENDPOINTS.length} endpoints with GET /v0/dso ...`);
  console.log(`${'─'.repeat(60)}`);

  const results = await Promise.allSettled(
    ALL_SCAN_ENDPOINTS.map(async (ep) => {
      const probeStart = Date.now();
      try {
        const response = await axios.get(`${ep.url}/v0/dso`, {
          timeout:    10000,
          httpsAgent: new https.Agent({ rejectUnauthorized: getTLSRejectUnauthorized() }),
          headers:    { Accept: 'application/json' },
        });
        return { name: ep.name, url: ep.url, healthy: true,  status: response.status,       latencyMs: Date.now() - probeStart };
      } catch (err) {
        return { name: ep.name, url: ep.url, healthy: false, status: err.response?.status || null, error: err.code || err.message, latencyMs: Date.now() - probeStart };
      }
    })
  );

  const healthyEndpoints = results
    .filter(r => r.status === 'fulfilled' && r.value.healthy)
    .map(r => r.value)
    .sort((a, b) => a.latencyMs - b.latencyMs);

  const activeEndpointName = ALL_SCAN_ENDPOINTS.find(e => e.url === activeScanUrl)?.name || 'Custom';

  for (const r of results) {
    const ep     = r.status === 'fulfilled' ? r.value : { name: '?', healthy: false, error: 'Promise rejected', latencyMs: 0 };
    const icon   = ep.healthy ? '✅' : '❌';
    const active = ep.name === activeEndpointName ? ' ← ACTIVE' : '';
    const detail = ep.healthy ? `HTTP ${ep.status} in ${ep.latencyMs}ms` : `${ep.error || 'HTTP ' + ep.status} (${ep.latencyMs}ms)`;
    console.log(`  ${icon}  ${ep.name} — ${detail}${active}`);
  }

  console.log(`${'─'.repeat(60)}`);
  console.log(`  ${healthyEndpoints.length}/${ALL_SCAN_ENDPOINTS.length} endpoints reachable`);

  if (healthyEndpoints.length === 0) {
    console.error(`\n🔴 FATAL: No Scan API endpoints are reachable! Check network/DNS.`);
    log('error', 'all_endpoints_unreachable', { probed: ALL_SCAN_ENDPOINTS.length });
    alert(Severity.CRITICAL, 'all_endpoints_unreachable', 'All Scan API endpoints unreachable', {
      'Endpoints Probed': ALL_SCAN_ENDPOINTS.length,
      'Action': 'Check network, DNS, and endpoint health',
    });
  } else {
    const activeReachable = healthyEndpoints.some(ep => ep.name === activeEndpointName);
    if (!activeReachable) {
      const best = healthyEndpoints[0];
      console.warn(`\n⚠️  Active endpoint "${activeEndpointName}" is NOT reachable!`);
      console.log(`🔄 AUTO-FAILOVER: Switching to fastest healthy endpoint: ${best.name} (${best.latencyMs}ms)`);
      activeScanUrl = best.url;
      client.defaults.baseURL = best.url;
      log('warn', 'auto_failover', { from: activeEndpointName, to: best.name, latencyMs: best.latencyMs, newUrl: best.url });
    }
  }

  console.log(`${'─'.repeat(60)}\n`);
  log('info', 'endpoint_probe_complete', {
    total:     ALL_SCAN_ENDPOINTS.length,
    healthy:   healthyEndpoints.length,
    active:    ALL_SCAN_ENDPOINTS.find(e => e.url === activeScanUrl)?.name || 'Custom',
    activeUrl: activeScanUrl,
  });
}

/**
 * Lightweight endpoint probe for runtime failover.
 * Returns the fastest healthy endpoint that is NOT the current one,
 * or null if no alternatives are reachable.
 */
async function probeAllScanEndpoints_fast() {
  const candidates = ALL_SCAN_ENDPOINTS.filter(ep => ep.url !== activeScanUrl);
  if (candidates.length === 0) return null;

  const results = await Promise.allSettled(
    candidates.map(async (ep) => {
      const probeStart = Date.now();
      try {
        const response = await axios.get(`${ep.url}/v0/dso`, {
          timeout:    10000,
          httpsAgent: new https.Agent({ rejectUnauthorized: getTLSRejectUnauthorized() }),
          headers:    { Accept: 'application/json' },
        });
        if (response.status >= 200 && response.status < 300) {
          return { name: ep.name, url: ep.url, latencyMs: Date.now() - probeStart };
        }
        return null;
      } catch {
        return null;
      }
    })
  );

  const healthy = results
    .filter(r => r.status === 'fulfilled' && r.value !== null)
    .map(r => r.value)
    .sort((a, b) => a.latencyMs - b.latencyMs);

  return healthy.length > 0 ? healthy[0] : null;
}

// ─── Sleep / timeout helpers ───────────────────────────────────────────────

async function sleep(ms, reason = 'unknown') {
  log('debug', 'sleep_start', { ms, reason });
  await new Promise(resolve => setTimeout(resolve, ms));
  log('debug', 'sleep_end', { ms, reason });
}

async function withTimeout(promise, ms, label) {
  return Promise.race([
    promise,
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error(`Timeout: ${label} after ${ms}ms`)), ms)
    ),
  ]);
}

// ─── Main ingestion loop ───────────────────────────────────────────────────

async function runIngestion() {
  const modeLabel  = LIVE_MODE ? 'LIVE' : 'RESUME';
  sessionStartTime = Date.now();

  console.log('\n' + '='.repeat(60));
  console.log(`🚀 Starting Canton ledger updates (${modeLabel} mode)`);
  console.log('   SCAN_URL:', activeScanUrl);
  console.log('   BATCH_SIZE:', BATCH_SIZE);
  console.log('   POLL_INTERVAL:', POLL_INTERVAL, 'ms');
  console.log('   PAGINATION: FORWARD (after semantics, NOT before)');
  logAlertConfig();

  await probeAllScanEndpoints();

  try {
    if (GCS_MODE) {
      validateGCSBucket(true);
      console.log('\n🔍 Running GCS preflight checks...');
      runPreflightChecks({ quick: false, throwOnFail: true });
      console.log('\n☁️  GCS Mode ENABLED:');
      console.log(`   Bucket: gs://${process.env.GCS_BUCKET}/`);
      console.log('   Local scratch: /tmp/ledger_raw');
      console.log('   Files are uploaded to GCS immediately after creation');
    } else {
      console.log(`\n📂 Disk Mode: Writing to ${DATA_DIR}`);
      if (process.env.GCS_BUCKET) {
        console.log(`   GCS bucket configured: gs://${process.env.GCS_BUCKET}/`);
        console.log('   Uploads disabled (GCS_ENABLED=false) - writing to local disk only');
      } else {
        console.log('   GCS not configured - writing to local disk only');
      }
    }
  } catch (err) {
    logFatal('gcs_preflight_failed', err);
    throw err;
  }
  console.log('='.repeat(60));

  setDataSource('updates');
  console.log(`📁 Data source: 'updates' (writing to raw/updates/)`);

  log('info', 'ingestion_start', {
    mode:          modeLabel,
    scan_url:      activeScanUrl,
    batch_size:    BATCH_SIZE,
    poll_interval: POLL_INTERVAL,
    gcs_mode:      GCS_MODE,
    gcs_bucket:    process.env.GCS_BUCKET || null,
    data_source:   'updates',
  });

  // FIX #2: findLatestTimestamp is now async throughout
  lastTimestamp = await findLatestTimestamp();
  await detectLatestMigration();

  let afterMigrationId = lastMigrationId || migrationId;
  let afterRecordTime  = lastTimestamp;

  // FIX #4: Initialise shutdown-cursor tracking to the startup position.
  _liveAfterMigrationId = afterMigrationId;
  _liveAfterRecordTime  = afterRecordTime;

  if (afterRecordTime) {
    console.log(`\n📍 FORWARD CURSOR: migration=${afterMigrationId}, after_record_time=${afterRecordTime}`);
    console.log(`   (Will fetch transactions AFTER this timestamp, advancing forward in time)`);
    logCursor('resume', { migrationId: afterMigrationId, afterRecordTime, mode: modeLabel, semantics: 'forward' });
  } else {
    log('info', 'ingestion_fresh_start', { mode: modeLabel });
    console.log('\n📍 FRESH START: No cursor found, starting from earliest available');
    afterMigrationId = null;
  }

  await alert(Severity.INFO, 'ingestion_started', 'Ingestion pipeline started', {
    'Mode': modeLabel,
    'Cursor': afterRecordTime || 'FRESH START',
    'Migration': afterMigrationId ?? 'auto-detect',
    'Scan URL': activeScanUrl,
  });

  let totalUpdates    = 0;
  let totalEvents     = 0;
  let emptyPolls      = 0;
  let batchCount      = 0;
  let lastMetricsTime = Date.now();

  heartbeatInterval = setInterval(() => {
    log('info', 'heartbeat', {
      cursor:       afterRecordTime,
      migration:    afterMigrationId,
      inflight:     0,
      workers:      1,
      totalUpdates,
      totalEvents,
      cycleId:      currentCycleId,
      isRunning,
    });
  }, 30_000);

  lastProgressTimestamp = Date.now();
  stallWatchdogInterval = setInterval(() => {
    const staleMs = Date.now() - lastProgressTimestamp;
    if (staleMs > STALL_THRESHOLD_MS) {
      log('warn', 'ingestion_stall_detected', {
        cursor:          afterRecordTime,
        migration:       afterMigrationId,
        workers:         1,
        inflight:        0,
        lastProgressTs:  new Date(lastProgressTimestamp).toISOString(),
        staleDurationMs: staleMs,
      });
      // Alert on stall — rate-limited so it won't spam every 30s
      alert(Severity.WARNING, 'ingestion_stall', 'Ingestion stall detected — no progress', {
        'Stale Duration': `${Math.round(staleMs / 1000)}s`,
        'Cursor': afterRecordTime,
        'Migration': afterMigrationId,
        'Last Progress': new Date(lastProgressTimestamp).toISOString(),
        'Error Count': sessionErrorCount,
      });
    }
  }, STALL_DETECTION_INTERVAL_MS);

  while (isRunning) {
    try {
      currentCycleId++;
      const batchStart = Date.now();

      log('info', 'ingestion_cycle_start', {
        cycleId:   currentCycleId,
        migration: afterMigrationId,
        cursor:    afterRecordTime,
        workers:   1,
        inflight:  0,
      });

      const data = await fetchUpdates(afterMigrationId, afterRecordTime);
      lastProgressTimestamp = Date.now();

      if (!data.items || data.items.length === 0) {
        emptyPolls++;

        log('info', 'empty_batch', {
          cycleId:              currentCycleId,
          migration:            afterMigrationId,
          cursor:               afterRecordTime,
          consecutiveEmptyPolls: emptyPolls,
        });

        if (emptyPolls === 1) {
          log('info', 'flush_start', { batchId: currentCycleId });
          const flushed = await withTimeout(flushAll(), 60_000, 'flushAll');
          log('info', 'flush_complete', { filesWritten: flushed.length });

          if (afterRecordTime) {
            // FIX #2: await the now-async saveLiveCursor
            await saveLiveCursor(afterMigrationId, afterRecordTime);
            // FIX #4: keep shutdown-cursor tracking in sync
            _liveAfterMigrationId = afterMigrationId;
            _liveAfterRecordTime  = afterRecordTime;
            log('info', 'cursor_advanced', { newCursor: afterRecordTime, migration: afterMigrationId });
            logCursor('saved', { migrationId: afterMigrationId, lastBefore: afterRecordTime, totalUpdates, totalEvents });
          }
        }

        const now = Date.now();
        if (now - lastMetricsTime >= 60000) {
          lastMetricsTime = now;
          const elapsedSeconds = (now - sessionStartTime) / 1000;
          logMetrics({ migrationId: afterMigrationId, elapsedSeconds, totalUpdates, totalEvents, avgThroughput: totalUpdates / Math.max(1, elapsedSeconds), currentThroughput: 0, errorCount: sessionErrorCount });
        }

        log('info', 'cycle_reenter', { cycleId: currentCycleId, sleepMs: POLL_INTERVAL, reason: 'empty_batch' });
        await sleep(POLL_INTERVAL, 'empty_batch_poll');
        continue;
      }

      emptyPolls = 0;
      batchCount++;

      if (sessionErrorCount > 0) {
        log('info', 'cooldown_exit', { previous_error_count: sessionErrorCount, migration: afterMigrationId });
        sessionErrorCount = 0;
      }

      // FIX #13: Reset adaptive parameters on successful fetch with data.
      // The stuck cursor has been passed — restore normal page_size and timeout.
      if (_adaptivePageSize !== BATCH_SIZE || _adaptiveTimeoutMs !== FETCH_TIMEOUT_MS) {
        log('info', 'adaptive_params_reset', {
          previousPageSize: _adaptivePageSize,
          previousTimeoutMs: _adaptiveTimeoutMs,
          restoredPageSize: BATCH_SIZE,
          restoredTimeoutMs: FETCH_TIMEOUT_MS,
        });
        console.log(`🔧 Adaptive params reset: page_size → ${BATCH_SIZE}, timeout → ${FETCH_TIMEOUT_MS / 1000}s (cursor unstuck)`);
        _adaptivePageSize  = BATCH_SIZE;
        _adaptiveTimeoutMs = FETCH_TIMEOUT_MS;
      }
      _stuckCursor     = null;
      _stuckCursorHits = 0;

      const processResult = await withTimeout(processUpdates(data.items), 60_000, 'processUpdates');
      const { updates, events, updateCount, eventCount, errors: decodeErrors } = processResult;
      totalUpdates += updateCount;
      totalEvents  += eventCount;

      // FIX #1: Decode failures must be visible and must gate cursor advancement.
      // Previously errors[] was returned from processUpdates but destructured
      // without checking — the cursor advanced unconditionally, permanently
      // skipping any tx that failed to decode.
      // Now: if decodeErrors > 0, log prominently and DO NOT advance the cursor.
      // The batch's records are still written (the successful subset), but the
      // cursor stays at its current position so the failed txs are re-attempted
      // on the next poll. If the same txs keep failing, this will stall — which
      // is intentional: a persistent decode error requires operator intervention,
      // not silent data loss.
      if (decodeErrors > 0) {
        log('error', 'decode_failures_block_cursor', {
          decodeErrors,
          batchSize: data.items.length,
          cursorHeld: afterRecordTime,
          migration:  afterMigrationId,
          message:    'Cursor NOT advanced — fix decode errors or they will repeat',
        });
        console.error(
          `❌ [fetch-updates] ${decodeErrors} tx(s) failed to decode in this batch. ` +
          `Cursor held at ${afterRecordTime}. Resolve errors to proceed.`
        );
        await alert(Severity.CRITICAL, 'decode_failures', 'Decode errors blocking cursor advancement', {
          'Decode Errors': decodeErrors,
          'Batch Size': data.items.length,
          'Cursor Held At': afterRecordTime,
          'Migration': afterMigrationId,
        });
        // Skip cursor advancement for this batch — outer loop will re-poll same position
        continue;
      }

      // Write to GCS/binary BEFORE advancing cursor.
      // afterRecordTime here is the cursor that produced this data — used for
      // deterministic filenames so the same cursor always produces the same files.
      if (USE_PARQUET && (updateCount > 0 || eventCount > 0)) {
        await writeBatchToGCS(updates, events, migrationId, afterRecordTime);
      }
      if (USE_BINARY) {
        await binaryWriter.bufferUpdates(updates);
        await binaryWriter.bufferEvents(events);
      }

      // NOW advance cursor — data is already confirmed in GCS
      const cursorBefore = afterRecordTime;
      if (data.lastMigrationId !== null) afterMigrationId = data.lastMigrationId;
      if (data.lastRecordTime)           afterRecordTime  = data.lastRecordTime;

      const batchLatency = Date.now() - batchStart;

      logBatch({
        migrationId: afterMigrationId,
        batchCount,
        updates: updateCount,
        events: eventCount,
        totalUpdates,
        totalEvents,
        cursorBefore,
        cursorAfter:  afterRecordTime,
        latencyMs:    batchLatency,
        throughput:   updateCount / (batchLatency / 1000),
      });

      log('info', 'cursor_advanced', { newCursor: afterRecordTime, migration: afterMigrationId, batchCount });

      // Save cursor after EVERY batch — data is already in GCS (written
      // synchronously by writeBatchToGCS before cursor advances).
      // On crash, worst case: re-fetch one batch → same deterministic
      // filename → GCS overwrite → zero dups, zero gaps.
      if (afterRecordTime) {
        // FIX #2: await the now-async saveLiveCursor
        await saveLiveCursor(afterMigrationId, afterRecordTime);
        // FIX #4: keep shutdown-cursor tracking in sync on every save
        _liveAfterMigrationId = afterMigrationId;
        _liveAfterRecordTime  = afterRecordTime;
      }

      log('info', 'cycle_reenter', { cycleId: currentCycleId, reason: 'batch_complete' });

      const now = Date.now();
      if (now - lastMetricsTime >= 60000) {
        lastMetricsTime = now;
        const elapsedSeconds = (now - sessionStartTime) / 1000;
        logMetrics({ migrationId: afterMigrationId, elapsedSeconds, totalUpdates, totalEvents, avgThroughput: totalUpdates / Math.max(1, elapsedSeconds), currentThroughput: updates / (batchLatency / 1000), errorCount: sessionErrorCount });
      }

    } catch (err) {
      sessionErrorCount++;

      const isTimeout   = err.code === 'ECONNABORTED' || err.code === 'ETIMEDOUT' || err.code === 'ECONNRESET' || err.code === 'FETCH_TIMEOUT' || err.message?.includes('timeout');
      const isTransient = isTimeout || err.response?.status === 503 || err.response?.status === 429 || err.response?.status >= 500;

      logError('fetch', err, { cycleId: currentCycleId, migration: afterMigrationId, cursor: afterRecordTime, error_count: sessionErrorCount, is_transient: isTransient });

      // FIX #13: Adaptive page_size / timeout when stuck at the same cursor.
      // If the API consistently times out for a cursor, the response at that
      // position is likely very large. Halve page_size (min 1) and increase
      // timeout (up to 3x) so we can break through without losing data.
      if (isTimeout) {
        if (_stuckCursor === afterRecordTime) {
          _stuckCursorHits++;
        } else {
          _stuckCursor     = afterRecordTime;
          _stuckCursorHits = 1;
        }

        // Every ENDPOINT_ROTATE_AFTER_ERRORS hits at the same cursor, halve page_size
        if (_stuckCursorHits > 0 && _stuckCursorHits % ENDPOINT_ROTATE_AFTER_ERRORS === 0) {
          const oldPageSize = _adaptivePageSize;
          _adaptivePageSize = Math.max(1, Math.floor(_adaptivePageSize / 2));
          // Increase timeout: 1.5x base per reduction step, capped at 3x base
          _adaptiveTimeoutMs = Math.min(FETCH_TIMEOUT_MS * 3, Math.round(_adaptiveTimeoutMs * 1.5));
          if (_adaptivePageSize !== oldPageSize) {
            log('warn', 'adaptive_page_size_reduced', {
              cursor: afterRecordTime,
              stuckHits: _stuckCursorHits,
              oldPageSize,
              newPageSize: _adaptivePageSize,
              newTimeoutMs: _adaptiveTimeoutMs,
            });
            console.log(
              `🔧 Adaptive retry: page_size ${oldPageSize} → ${_adaptivePageSize}, ` +
              `timeout ${Math.round(_adaptiveTimeoutMs / 1000)}s ` +
              `(stuck at same cursor for ${_stuckCursorHits} errors)`
            );
            // Alert once when adaptive kicks in (rate-limited)
            alert(Severity.WARNING, 'cursor_stuck', 'Cursor stuck — reducing page size to break through', {
              'Cursor': afterRecordTime,
              'Stuck Hits': _stuckCursorHits,
              'Page Size': `${oldPageSize} → ${_adaptivePageSize}`,
              'Timeout': `${Math.round(_adaptiveTimeoutMs / 1000)}s`,
              'Migration': afterMigrationId,
            });
          }
        }
      }

      if (isTransient) {
        // Rotate to a different endpoint after repeated failures on the same one
        // Also recreate the HTTP client to drop stale TCP connections from hung nodes
        if (sessionErrorCount > 0 && sessionErrorCount % ENDPOINT_ROTATE_AFTER_ERRORS === 0) {
          const healthyEndpoints = (await probeAllScanEndpoints_fast());
          if (healthyEndpoints) {
            const oldName = ALL_SCAN_ENDPOINTS.find(e => e.url === activeScanUrl)?.name || 'Custom';
            activeScanUrl = healthyEndpoints.url;
            client = createClient(healthyEndpoints.url);
            log('warn', 'endpoint_rotated_on_error', { from: oldName, to: healthyEndpoints.name, error_count: sessionErrorCount });
            console.log(`🔄 Rotated endpoint: ${oldName} → ${healthyEndpoints.name} (after ${sessionErrorCount} errors, fresh connection)`);
          } else {
            // No healthy alternative — still recreate client to drop stuck connections
            client = createClient(activeScanUrl);
            log('warn', 'client_reset_no_alternatives', { url: activeScanUrl, error_count: sessionErrorCount });
            console.log(`🔄 Reset HTTP client (no healthy alternatives, fresh connection to ${activeScanUrl})`);
          }
        }

        // Exit after too many consecutive transient errors to allow process supervisor to restart
        if (sessionErrorCount >= MAX_TRANSIENT_ERRORS) {
          logFatal('max_transient_errors', err, { error_count: sessionErrorCount, max: MAX_TRANSIENT_ERRORS, migration: afterMigrationId });
          console.error(`🔴 FATAL: ${sessionErrorCount} consecutive transient errors (max=${MAX_TRANSIENT_ERRORS}). Exiting for restart.`);
          await alert(Severity.FATAL, 'max_transient_errors', 'Ingestion stopped: max transient errors reached', {
            'Error': err.message,
            'Error Count': sessionErrorCount,
            'Cursor': afterRecordTime,
            'Migration': afterMigrationId,
            'Total Updates': totalUpdates,
            'Total Events': totalEvents,
            'Last Error Code': err.code || err.response?.status || 'unknown',
          });
          throw err;
        }

        const backoffMs  = Math.min(60000, 10000 * Math.pow(2, Math.min(sessionErrorCount - 1, 3)));
        const cooldownMs = backoffMs + Math.random() * 5000;

        log('warn', 'cooldown_mode', { cycleId: currentCycleId, migration: afterMigrationId, cursor: afterRecordTime, error_count: sessionErrorCount, cooldown_ms: Math.round(cooldownMs), reason: err.code || err.response?.status || 'unknown' });
        console.log(`⏳ Cooldown: sleeping ${Math.round(cooldownMs / 1000)}s before retry (error #${sessionErrorCount})...`);
        await sleep(cooldownMs, 'cooldown_backoff');

        log('info', 'cycle_reenter', { cycleId: currentCycleId, reason: 'error_recovery', cooldownMs: Math.round(cooldownMs) });
      } else {
        if (sessionErrorCount >= 10) {
          logFatal('too_many_errors', err, { error_count: sessionErrorCount, migration: afterMigrationId });
          await alert(Severity.FATAL, 'too_many_nontransient_errors', 'Ingestion stopped: repeated non-transient errors', {
            'Error': err.message,
            'Status': err.response?.status || 'N/A',
            'Error Count': sessionErrorCount,
            'Cursor': afterRecordTime,
            'Migration': afterMigrationId,
          });
          throw err;
        }
        log('info', 'cycle_reenter', { cycleId: currentCycleId, reason: 'non_transient_error_retry' });
        await sleep(10000, 'non_transient_error_retry');
      }
    }
  }

  if (heartbeatInterval)    { clearInterval(heartbeatInterval);    heartbeatInterval    = null; }
  if (stallWatchdogInterval) { clearInterval(stallWatchdogInterval); stallWatchdogInterval = null; }

  const elapsedSeconds = (Date.now() - sessionStartTime) / 1000;
  logSummary({ success: true, totalUpdates, totalEvents, totalTimeSeconds: elapsedSeconds, avgThroughput: totalUpdates / Math.max(1, elapsedSeconds), migrationsProcessed: 1, allComplete: false });
}

// ─── Graceful shutdown ─────────────────────────────────────────────────────

/**
 * FIX #4: shutdown now saves _liveAfterRecordTime / _liveAfterMigrationId
 * (the in-loop cursor, updated on every periodic save) rather than
 * lastTimestamp (the startup value, never updated during the run).
 *
 * Original: saveLiveCursor(lastMigrationId || migrationId, lastTimestamp)
 * After running for hours and receiving a SIGTERM, this would revert the
 * cursor to the startup position, forcing a full re-ingest on restart.
 *
 * _liveAfterRecordTime is updated on every periodic save (every 5 batches)
 * and on every empty-poll flush, so worst-case re-ingest on shutdown is
 * 5 batches rather than the full session.
 */
async function shutdown() {
  log('info', 'shutdown_started', { mode: LIVE_MODE ? 'LIVE' : 'RESUME' });
  isRunning = false;

  if (heartbeatInterval)    { clearInterval(heartbeatInterval);    heartbeatInterval    = null; }
  if (stallWatchdogInterval) { clearInterval(stallWatchdogInterval); stallWatchdogInterval = null; }

  log('info', 'flush_start', { reason: 'shutdown' });
  try {
    const flushed = await withTimeout(flushAll(), 30_000, 'shutdown_flushAll');
    log('info', 'flush_complete', { filesWritten: flushed.length });
  } catch (err) {
    log('error', 'flush_timeout_on_shutdown', { error: err.message });
  }

  // FIX #4: save the in-loop cursor, not the stale startup lastTimestamp
  const cursorMig  = _liveAfterMigrationId  || lastMigrationId || migrationId;
  const cursorTime = _liveAfterRecordTime   || lastTimestamp;

  if (cursorTime) {
    // FIX #2: await the now-async saveLiveCursor
    await saveLiveCursor(cursorMig, cursorTime);
    log('info', 'cursor_advanced', { newCursor: cursorTime, migration: cursorMig, reason: 'shutdown' });
    logCursor('shutdown_saved', { migrationId: cursorMig, lastBefore: cursorTime });
  }

  const elapsedSeconds = (Date.now() - sessionStartTime) / 1000;
  log('info', 'shutdown_complete', { elapsed_s: elapsedSeconds, error_count: sessionErrorCount });

  process.exit(0);
}

process.on('SIGINT',  () => shutdown());
process.on('SIGTERM', () => shutdown());

// ─── Unhandled error safety net ───────────────────────────────────────────
// Catch truly unexpected errors, alert, save cursor, then exit for restart.

process.on('uncaughtException', async (err) => {
  console.error('[uncaughtException]', err);
  await alert(Severity.FATAL, 'uncaught_exception', 'Ingestion crashed: uncaught exception', {
    'Error': err.message,
    'Stack': err.stack?.split('\n').slice(0, 5).join('\n'),
    'Cursor': _liveAfterRecordTime || 'unknown',
    'Migration': _liveAfterMigrationId || 'unknown',
  }).catch(() => {});
  process.exit(1);
});

process.on('unhandledRejection', async (reason) => {
  const msg = reason instanceof Error ? reason.message : String(reason);
  const stack = reason instanceof Error ? reason.stack?.split('\n').slice(0, 5).join('\n') : '';
  console.error('[unhandledRejection]', reason);
  await alert(Severity.FATAL, 'unhandled_rejection', 'Ingestion crashed: unhandled promise rejection', {
    'Error': msg,
    'Stack': stack,
    'Cursor': _liveAfterRecordTime || 'unknown',
    'Migration': _liveAfterMigrationId || 'unknown',
  }).catch(() => {});
  process.exit(1);
});

// ─── Entry point ───────────────────────────────────────────────────────────
runIngestion().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
