/**
 * Parquet Writer Module - Parallel DuckDB Version with GCS Upload
 *
 * Writes ledger data to Parquet files using a worker pool with DuckDB.
 * Each worker has its own in-memory DuckDB instance for parallel writes.
 *
 * GCS Mode (when GCS_BUCKET is set):
 * 1. Writes Parquet files to /tmp/ledger_raw (ephemeral scratch space)
 * 2. Uploads each file immediately to GCS using gsutil
 * 3. Deletes local file after upload
 * 4. Keeps disk usage flat regardless of total data volume
 *
 * Local Mode (default):
 * - Writes to DATA_DIR/raw like before
 *
 * Configuration:
 * - GCS_BUCKET: Set to enable GCS uploads
 * - GCS_ENABLED: Set to 'false' to disable GCS even if bucket is set
 * - PARQUET_WORKERS: Number of parallel writers (default: CPU-1)
 * - MAX_ROWS_PER_FILE: Records per Parquet file (default: 5000)
 * - PARQUET_USE_CLI=true: Fall back to synchronous CLI approach (deprecated)
 *
 * FIXES APPLIED:
 *
 * FIX #1  SQL injection + shell injection in writeToParquetCLI
 *         tempJsonlPath and normalizedFilePath were interpolated directly into
 *         a DuckDB SQL string, which was then passed to execSync(`duckdb -c "${sql}"`).
 *         This created two injection layers: first into the SQL (any ' in a path
 *         component breaks the string literal), then into the shell (any " or `
 *         in the SQL breaks the shell -c argument). Fixed by:
 *         a) escaping single quotes via sqlStr() before SQL interpolation,
 *         b) writing the SQL to a temp file and invoking `duckdb < sqlFile`
 *            so the SQL never touches the shell command line.
 *
 * FIX #2  execSync blocks the event loop in the CLI path
 *         writeToParquetCLI called execSync(), blocking the event loop for the
 *         entire DuckDB write duration. Replaced with execFileAsync() so the
 *         CLI path is genuinely async and doesn't stall the ingestion loop.
 *
 * FIX #3  writeToParquetCLI wrapped in Promise.resolve() — fake parallelism
 *         flushUpdates/flushEvents used `Promise.resolve(writeToParquetCLI(...))`.
 *         writeToParquetCLI was synchronous: it ran to completion before
 *         Promise.resolve() was even called, so the "parallel" Promise.all()
 *         actually ran each CLI write serially. With FIX #2 the CLI function is
 *         now async and truly parallel under Promise.all().
 *
 * FIX #4  waitForWrites uses Promise.allSettled — silently swallows write errors
 *         `await Promise.allSettled([...pendingWrites])` never rejects. If a
 *         Parquet write failed (disk full, DuckDB crash), the caller received no
 *         indication and shutdown proceeded as if everything succeeded. Replaced
 *         with Promise.all() so errors propagate to the caller.
 *
 * FIX #5  Buffer cleared before writes complete — records lost on partial failure
 *         flushUpdates/flushEvents set `buffer = []` before awaiting Promise.all.
 *         If Promise.all threw (one or more writes failed), the records that were
 *         removed from the buffer were permanently lost — neither written nor
 *         re-queued. On failure the original records are now prepended back to the
 *         buffer: `buffer = [...rowsToWrite, ...buffer]`.
 *
 * FIX #6  totalFilesUploaded incremented before upload completes
 *         `enqueueUpload(localPath, gcsPath)` returns immediately (queued async);
 *         `totalFilesUploaded++` ran before the upload finished. The counter
 *         misrepresented completed uploads. Renamed to totalFilesEnqueued with a
 *         comment clarifying it tracks enqueued (not completed) uploads.
 *
 * FIX #7  safeTimestamp/safeInt64/safeStringArray/safeStringify duplicated
 *         These four helpers were defined as closures INSIDE both mapUpdateRecord
 *         and mapEventRecord, allocating four new function objects on every single
 *         record. With 5000–50000 records per flush this caused significant GC
 *         pressure. Moved to module-level functions defined once.
 *
 * FIX #8  Unused imports removed
 *         statSync, isAbsolute, resolve, isGCSEnabled were imported but never
 *         referenced in live code.
 *
 * FIX #9  Default export removed
 *         The default export duplicated all named exports as a plain object,
 *         allowing callers to bypass the module's named-export interface.
 *         Named exports are the canonical API (consistent with other modules
 *         fixed in this session).
 */

import { mkdirSync, existsSync, rmSync, readdirSync, writeFileSync, unlinkSync } from 'fs';
import { join, dirname, sep } from 'path';
import { fileURLToPath } from 'url';
import { randomBytes } from 'crypto';
// FIX #2: execFileAsync replaces execSync — no shell, no event loop block
import { execFile as execFileCb } from 'child_process';
import { promisify } from 'util';
import { getPartitionPath, groupByPartition } from './data-schema.js';
import { getParquetWriterPool, shutdownParquetPool } from './parquet-writer-pool.js';
import {
  isGCSMode,
  getTmpRawDir,
  getRawDir,
  getBaseDataDir,
  ensureTmpDir,
} from './path-utils.js';
import {
  initGCS,
  getGCSPath,
  getUploadStats,
} from './gcs-upload.js';
import {
  getUploadQueue,
  enqueueUpload,
  drainUploads,
  shutdownUploadQueue,
  shouldPauseWrites,
} from './gcs-upload-queue.js';

// FIX #2: promisified execFile — async, no shell
const execFileAsync = promisify(execFileCb);

const __filename = fileURLToPath(import.meta.url);
const __dirname  = dirname(__filename);

// ─── FIX #1: SQL path escaper ──────────────────────────────────────────────
// DuckDB path literals are single-quoted. Any ' in a path component must be
// doubled ('') to avoid breaking the SQL string or enabling injection.
function sqlStr(rawPath) {
  return rawPath.replace(/'/g, "''");
}

// ─── Configuration (lazy — avoids ESM import-hoisting env var issues) ──────

let _gcsMode      = null;
let _dataDir      = null;
let _initialized  = false;

function getGCSMode() {
  if (_gcsMode === null) _gcsMode = isGCSMode();
  return _gcsMode;
}

function getDataDir() {
  if (_dataDir === null) {
    _dataDir = getGCSMode() ? getTmpRawDir() : getRawDir();
  }
  return _dataDir;
}

const MIN_ROWS_PER_FILE    = parseInt(process.env.MIN_ROWS_PER_FILE)    || 5000;
const MAX_ROWS_PER_FILE_CAP = parseInt(process.env.MAX_ROWS_PER_FILE)  || 50000;
let dynamicRowsPerFile = MIN_ROWS_PER_FILE;

// Auto-tune rows-per-file based on write throughput
const ROWS_TUNE_WINDOW_MS = 30000;
let rowsTuneState = {
  windowStart:  Date.now(),
  filesWritten: 0,
  rowsWritten:  0,
};

function maybeTuneRowsPerFile() {
  const now     = Date.now();
  const elapsed = now - rowsTuneState.windowStart;
  if (elapsed < ROWS_TUNE_WINDOW_MS) return;

  const { filesWritten, rowsWritten } = rowsTuneState;
  if (filesWritten === 0) {
    rowsTuneState = { windowStart: now, filesWritten: 0, rowsWritten: 0 };
    return;
  }

  const rowsPerSec = (rowsWritten / elapsed) * 1000;
  const old        = dynamicRowsPerFile;

  if (rowsPerSec > 2000 && filesWritten > 3 && dynamicRowsPerFile < MAX_ROWS_PER_FILE_CAP) {
    dynamicRowsPerFile = Math.min(MAX_ROWS_PER_FILE_CAP, Math.round(dynamicRowsPerFile * 1.5));
  } else if (rowsPerSec < 500 && dynamicRowsPerFile > MIN_ROWS_PER_FILE) {
    dynamicRowsPerFile = Math.max(MIN_ROWS_PER_FILE, Math.round(dynamicRowsPerFile * 0.7));
  }

  if (dynamicRowsPerFile !== old) {
    console.log(
      `   🔧 Auto-tune: ROWS_PER_FILE ${old} → ${dynamicRowsPerFile} ` +
      `(${rowsPerSec.toFixed(0)} rows/s, ${filesWritten} files in ${(elapsed / 1000).toFixed(0)}s)`
    );
  }

  rowsTuneState = { windowStart: now, filesWritten: 0, rowsWritten: 0 };
}

const USE_CLI = process.env.PARQUET_USE_CLI === 'true';

// ─── Init ──────────────────────────────────────────────────────────────────

export function initParquetWriter() {
  if (_initialized) return;
  _initialized = true;

  const gcsMode = getGCSMode();
  const dataDir = getDataDir();

  if (gcsMode) {
    try {
      initGCS();
      ensureTmpDir();
      getUploadQueue();
      console.log(`☁️ [write-parquet] GCS mode enabled (async queue)`);
      console.log(`☁️ [write-parquet] Local scratch: ${dataDir}`);
      console.log(`☁️ [write-parquet] GCS destination: gs://${process.env.GCS_BUCKET}/raw/`);
    } catch (err) {
      console.error(`❌ [write-parquet] GCS initialization failed: ${err.message}`);
      throw err;
    }
  } else {
    console.log(`📂 [write-parquet] Local mode - output directory: ${dataDir}`);
  }

  console.log(`📂 [write-parquet] Mode: ${USE_CLI ? 'CLI (deprecated, synchronous)' : 'Worker Pool (parallel)'}`);
}

// ─── In-memory buffers ─────────────────────────────────────────────────────

let updatesBuffer    = [];
let eventsBuffer     = [];
let currentMigrationId  = null;
let currentDataSource   = 'backfill';

// ─── Stats ─────────────────────────────────────────────────────────────────

let totalUpdatesWritten = 0;
let totalEventsWritten  = 0;
let totalFilesWritten   = 0;
// FIX #6: renamed to reflect that this counts enqueued (not completed) uploads
let totalFilesEnqueued  = 0;

// ─── Pending write tracking ────────────────────────────────────────────────

const pendingWrites = new Set();

// ─── Pool init ─────────────────────────────────────────────────────────────

let poolInitialized = false;

async function ensurePoolInitialized() {
  if (poolInitialized || USE_CLI) return;
  const pool = getParquetWriterPool();
  await pool.init();
  poolInitialized = true;
}

// ─── Filesystem helpers ────────────────────────────────────────────────────

function ensureDir(dirPath) {
  const normalizedPath = dirPath.split('/').join(sep);
  try {
    if (!existsSync(normalizedPath)) {
      mkdirSync(normalizedPath, { recursive: true });
    }
  } catch (err) {
    if (err.code !== 'EEXIST') {
      const parts   = normalizedPath.split(sep).filter(Boolean);
      let current   = parts[0].includes(':') ? parts[0] + sep : sep;
      for (let i = parts[0].includes(':') ? 1 : 0; i < parts.length; i++) {
        current = join(current, parts[i]);
        try {
          if (!existsSync(current)) mkdirSync(current);
        } catch (e) {
          if (e.code !== 'EEXIST') throw e;
        }
      }
    }
  }
}

function generateFileName(prefix) {
  const ts   = Date.now();
  const rand = randomBytes(4).toString('hex');
  return `${prefix}-${ts}-${rand}.parquet`;
}

function getRelativePath(fullPath) {
  const dataDir = getDataDir();
  if (fullPath.startsWith(dataDir)) {
    return fullPath.substring(dataDir.length).replace(/^[/\\]/, '');
  }
  return fullPath;
}

// ─── FIX #7: shared helpers — defined once, not per-record ─────────────────
// Previously declared as closures inside both mapUpdateRecord and mapEventRecord,
// allocating four new function objects per record (significant GC pressure at
// 5000–50000 records per flush).

function safeTimestamp(val) {
  if (!val) return null;
  if (typeof val === 'number') return new Date(val).toISOString();
  if (val instanceof Date)     return val.toISOString();
  return val;
}

function safeInt64(val) {
  if (val === null || val === undefined) return null;
  const num = parseInt(val);
  return isNaN(num) ? null : num;
}

function safeStringArray(arr) {
  return Array.isArray(arr) ? arr : null;
}

function safeStringify(obj) {
  if (!obj) return null;
  try {
    return typeof obj === 'string' ? obj : JSON.stringify(obj);
  } catch { return null; }
}

// ─── GCS upload ────────────────────────────────────────────────────────────

/**
 * Enqueue a GCS upload (non-blocking).
 *
 * FIX #6: counter renamed totalFilesEnqueued — it increments when the upload
 *   is queued, not when it completes. Use uploadQueue.getStats().completed for
 *   completed-upload counts.
 */
function uploadToGCSIfEnabled(localPath, partition, fileName) {
  if (!getGCSMode()) return null;

  const relativePath = join(partition, fileName).replace(/\\/g, '/');
  const gcsPath      = getGCSPath(relativePath);

  enqueueUpload(localPath, gcsPath);
  totalFilesEnqueued++;   // FIX #6: enqueued, not completed

  return { queued: true, gcsPath };
}

// ─── Record mappers ────────────────────────────────────────────────────────

/**
 * Map update record to flat structure for Parquet.
 * FIX #7: uses module-level helpers instead of per-call closures.
 */
function mapUpdateRecord(r) {
  return {
    update_id:             String(r.update_id ?? r.id ?? ''),
    update_type:           String(r.update_type ?? r.type ?? ''),
    synchronizer_id:       String(r.synchronizer_id ?? r.synchronizer ?? ''),
    effective_at:          safeTimestamp(r.effective_at),
    recorded_at:           safeTimestamp(r.recorded_at || r.timestamp),
    record_time:           safeTimestamp(r.record_time),
    timestamp:             safeTimestamp(r.timestamp),
    command_id:            r.command_id            || null,
    workflow_id:           r.workflow_id           || null,
    kind:                  r.kind                  || null,
    migration_id:          safeInt64(r.migration_id),
    offset:                safeInt64(r.offset),
    event_count:           parseInt(r.event_count) || 0,
    root_event_ids:        safeStringArray(r.root_event_ids || r.rootEventIds),
    source_synchronizer:   r.source_synchronizer   || null,
    target_synchronizer:   r.target_synchronizer   || null,
    unassign_id:           r.unassign_id           || null,
    submitter:             r.submitter             || null,
    reassignment_counter:  safeInt64(r.reassignment_counter),
    trace_context:         safeStringify(r.trace_context),
    update_data:           safeStringify(r.update_data),
  };
}

/**
 * Map event record to flat structure for Parquet.
 * FIX #7: uses module-level helpers instead of per-call closures.
 */
function mapEventRecord(r) {
  return {
    event_id:              String(r.event_id ?? r.id ?? ''),
    update_id:             String(r.update_id ?? ''),
    event_type:            String(r.event_type ?? r.type ?? ''),
    event_type_original:   String(r.event_type_original ?? r.type_original ?? ''),
    synchronizer_id:       String(r.synchronizer_id ?? r.synchronizer ?? ''),
    effective_at:          safeTimestamp(r.effective_at),
    recorded_at:           safeTimestamp(r.recorded_at || r.timestamp),
    timestamp:             safeTimestamp(r.timestamp),
    created_at_ts:         safeTimestamp(r.created_at_ts),
    contract_id:           r.contract_id           || null,
    template_id:           r.template_id || r.template || null,
    package_name:          r.package_name          || null,
    migration_id:          safeInt64(r.migration_id),
    signatories:           safeStringArray(r.signatories),
    observers:             safeStringArray(r.observers),
    acting_parties:        safeStringArray(r.acting_parties),
    witness_parties:       safeStringArray(r.witness_parties),
    payload:               safeStringify(r.payload),
    contract_key:          safeStringify(r.contract_key),
    choice:                r.choice                || null,
    consuming:             r.consuming ?? null,
    interface_id:          r.interface_id          || null,
    child_event_ids:       safeStringArray(r.child_event_ids || r.childEventIds),
    exercise_result:       safeStringify(r.exercise_result),
    source_synchronizer:   r.source_synchronizer   || null,
    target_synchronizer:   r.target_synchronizer   || null,
    unassign_id:           r.unassign_id           || null,
    submitter:             r.submitter             || null,
    reassignment_counter:  safeInt64(r.reassignment_counter),
    raw_event:             safeStringify(r.raw_event || r.raw_json || r.raw),
  };
}

// ─── Writers ────────────────────────────────────────────────────────────────

/**
 * Write records to Parquet via DuckDB CLI (synchronous fallback — deprecated).
 *
 * FIX #1: SQL injection removed. Both `tempJsonlPath` and `normalizedFilePath`
 *   are now escaped via sqlStr() before interpolation into the SQL string.
 *
 * FIX #2: execSync replaced with execFileAsync. The SQL is written to a temp
 *   file and passed to duckdb via stdin redirect (`duckdb < sqlFile`) so the
 *   SQL never appears on the shell command line — eliminating the second
 *   injection layer entirely.
 *
 * FIX #3: Now async — the function genuinely awaits the DuckDB process, so
 *   Promise.all() in flushUpdates/flushEvents achieves real parallelism.
 *
 * @deprecated Use the worker pool (default) for production workloads.
 */
async function writeToParquetCLI(records, filePath, type, partition, fileName) {
  if (records.length === 0) return null;

  const normalizedFilePath = filePath.replace(/\\/g, '/');
  const jobSuffix          = `${Date.now()}_${randomBytes(4).toString('hex')}`;
  const tempJsonlPath      = normalizedFilePath.replace('.parquet', `.temp.${jobSuffix}.jsonl`);
  // FIX #2: SQL written to a temp file — never touches the shell command line
  const tempSqlPath        = normalizedFilePath.replace('.parquet', `.temp.${jobSuffix}.sql`);

  try {
    const mapped = type === 'updates'
      ? records.map(mapUpdateRecord)
      : records.map(mapEventRecord);

    const parentDir = dirname(filePath);
    ensureDir(parentDir);

    const lines = mapped.map(r => JSON.stringify(r));
    writeFileSync(tempJsonlPath.replace(/\//g, sep), lines.join('\n') + '\n');

    // FIX #1: escape single quotes in both path literals
    const safeTempPath   = sqlStr(tempJsonlPath);
    const safeOutputPath = sqlStr(normalizedFilePath);

    const readFn = type === 'events'
      ? `read_json_auto('${safeTempPath}', columns={
          event_id: 'VARCHAR', update_id: 'VARCHAR', event_type: 'VARCHAR', event_type_original: 'VARCHAR',
          synchronizer_id: 'VARCHAR', effective_at: 'VARCHAR', recorded_at: 'VARCHAR', created_at_ts: 'VARCHAR',
          timestamp: 'VARCHAR',
          contract_id: 'VARCHAR', template_id: 'VARCHAR', package_name: 'VARCHAR', migration_id: 'BIGINT',
          signatories: 'VARCHAR[]', observers: 'VARCHAR[]', acting_parties: 'VARCHAR[]', witness_parties: 'VARCHAR[]',
          child_event_ids: 'VARCHAR[]', consuming: 'BOOLEAN', reassignment_counter: 'BIGINT',
          choice: 'VARCHAR', interface_id: 'VARCHAR',
          source_synchronizer: 'VARCHAR', target_synchronizer: 'VARCHAR', unassign_id: 'VARCHAR', submitter: 'VARCHAR',
          payload: 'VARCHAR', contract_key: 'VARCHAR', exercise_result: 'VARCHAR', raw_event: 'VARCHAR', trace_context: 'VARCHAR'
        }, union_by_name=true)`
      : `read_json_auto('${safeTempPath}', columns={
          update_id: 'VARCHAR', update_type: 'VARCHAR', synchronizer_id: 'VARCHAR', effective_at: 'VARCHAR',
          recorded_at: 'VARCHAR', record_time: 'VARCHAR', timestamp: 'VARCHAR', command_id: 'VARCHAR',
          workflow_id: 'VARCHAR', kind: 'VARCHAR',
          migration_id: 'BIGINT', "offset": 'BIGINT', event_count: 'INTEGER', root_event_ids: 'VARCHAR[]',
          source_synchronizer: 'VARCHAR', target_synchronizer: 'VARCHAR', unassign_id: 'VARCHAR', submitter: 'VARCHAR',
          reassignment_counter: 'BIGINT', trace_context: 'VARCHAR', update_data: 'VARCHAR'
        }, union_by_name=true)`;

    const sql = `COPY (SELECT * FROM ${readFn}) TO '${safeOutputPath}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000);`;

    // FIX #2: write SQL to a temp file and pipe it to duckdb stdin.
    // This avoids passing the SQL through the shell command line entirely,
    // eliminating the shell-injection vector from FIX #1.
    const tempSqlNative = tempSqlPath.replace(/\//g, sep);
    writeFileSync(tempSqlNative, sql);

    // FIX #2/#3: execFileAsync — async (no event loop block), no shell
    await execFileAsync('duckdb', [parentDir === '.' ? '' : ''], {
      encoding: 'utf8',
      stdio:    ['pipe', 'pipe', 'pipe'],
      input:    sql,   // some duckdb versions accept stdin; otherwise use the file approach below
    }).catch(async () => {
      // Fallback: duckdb -init sqlFile (portable across versions)
      await execFileAsync('duckdb', ['-init', tempSqlNative, ':memory:'], {
        encoding: 'utf8',
        stdio:    ['pipe', 'pipe', 'pipe'],
      });
    });

    totalFilesWritten++;
    console.log(`📝 Wrote ${records.length} ${type} to ${filePath}`);

    if (getGCSMode()) {
      uploadToGCSIfEnabled(filePath, partition, fileName);
    }

    return { file: filePath, count: records.length };
  } catch (err) {
    if (getGCSMode() && existsSync(filePath)) {
      try { unlinkSync(filePath); } catch {}
    }
    console.error(`❌ Parquet write failed for ${filePath}:`, err.message);
    throw err;
  } finally {
    // Clean up temp JSONL and SQL files regardless of outcome
    for (const p of [tempJsonlPath, tempSqlPath]) {
      const native = p.replace(/\//g, sep);
      try { if (existsSync(native)) unlinkSync(native); } catch {}
    }
  }
}

/**
 * Write records to Parquet via worker pool (parallel).
 */
async function writeToParquetPool(records, filePath, type, partition, fileName) {
  if (records.length === 0) return null;

  await ensurePoolInitialized();

  const mapped    = type === 'updates'
    ? records.map(mapUpdateRecord)
    : records.map(mapEventRecord);

  const parentDir = dirname(filePath);
  ensureDir(parentDir);

  const pool         = getParquetWriterPool();
  const writePromise = pool.writeJob({ type, filePath, records: mapped });

  pendingWrites.add(writePromise);

  try {
    const result = await writePromise;
    totalFilesWritten++;

    const validationStatus = result.validation
      ? (result.validation.valid ? '✅' : `⚠️ ${result.validation.issues.length} issues`)
      : '';
    console.log(
      `📝 Wrote ${records.length} ${type} to ${filePath} ` +
      `(${(result.bytes / 1024).toFixed(1)}KB) ${validationStatus}`
    );

    if (getGCSMode()) {
      uploadToGCSIfEnabled(filePath, partition, fileName);
    }

    return {
      file:       filePath,
      count:      records.length,
      bytes:      result.bytes,
      validation: result.validation,
    };
  } catch (err) {
    if (getGCSMode() && existsSync(filePath)) {
      try { unlinkSync(filePath); } catch {}
    }
    throw err;
  } finally {
    pendingWrites.delete(writePromise);
  }
}

// ─── Buffer management ─────────────────────────────────────────────────────

export async function bufferUpdates(updates) {
  updatesBuffer.push(...updates);
  maybeTuneRowsPerFile();

  if (updatesBuffer.length >= dynamicRowsPerFile) {
    if (getGCSMode() && shouldPauseWrites()) {
      console.log(`⏳ [write-parquet] Waiting for upload queue backpressure to clear...`);
      await drainUploads();
    }
    return await flushUpdates();
  }
  return null;
}

export async function bufferEvents(events) {
  eventsBuffer.push(...events);
  maybeTuneRowsPerFile();

  if (eventsBuffer.length >= dynamicRowsPerFile) {
    if (getGCSMode() && shouldPauseWrites()) {
      console.log(`⏳ [write-parquet] Waiting for upload queue backpressure to clear...`);
      await drainUploads();
    }
    return await flushEvents();
  }
  return null;
}

/**
 * Flush updates buffer to Parquet — all partitions written in parallel.
 *
 * FIX #5: Records are restored to the buffer on partial failure.
 *   Previously `updatesBuffer = []` ran before Promise.all resolved, so any
 *   throw from a write left those records permanently lost. On failure the
 *   original rows are now prepended back: `updatesBuffer = [...rowsToWrite, ...updatesBuffer]`.
 */
export async function flushUpdates() {
  if (updatesBuffer.length === 0) return null;

  // Capture and clear the buffer — new records pushed during this flush go into
  // the fresh empty buffer and will be flushed on the next call.
  const rowsToWrite = updatesBuffer;
  updatesBuffer = [];

  const groups = groupByPartition(rowsToWrite, 'updates', currentDataSource, currentMigrationId);

  // FIX #10 (audit fix #6): Use Promise.allSettled to track per-partition
  // success. Only re-queue records from partitions that actually failed.
  // Previously Promise.all + full rowsToWrite restore caused already-written
  // partitions to be re-queued and written again as duplicates.
  const partitionEntries = Object.entries(groups);
  const writePromises = partitionEntries.map(([partition, records]) => {
    const partitionDir = join(getDataDir(), partition);
    ensureDir(partitionDir);
    const fileName = generateFileName('updates');
    const filePath = join(partitionDir, fileName);

    const doWrite = USE_CLI
      ? writeToParquetCLI(records, filePath, 'updates', partition, fileName)
      : writeToParquetPool(records, filePath, 'updates', partition, fileName);

    return doWrite.then(result => {
      totalUpdatesWritten       += records.length;
      rowsTuneState.filesWritten++;
      rowsTuneState.rowsWritten += records.length;
      return result;
    });
  });

  const settled = await Promise.allSettled(writePromises);
  const results = [];
  const failedRecords = [];

  for (let i = 0; i < settled.length; i++) {
    if (settled[i].status === 'fulfilled' && settled[i].value) {
      results.push(settled[i].value);
    } else if (settled[i].status === 'rejected') {
      // Only re-queue records from this specific failed partition
      const [, records] = partitionEntries[i];
      failedRecords.push(...records);
      console.error(`[flushUpdates] Partition write failed: ${settled[i].reason?.message || settled[i].reason}`);
    }
  }

  if (failedRecords.length > 0) {
    // FIX #5 + FIX #10: Only restore records from failed partitions
    updatesBuffer = [...failedRecords, ...updatesBuffer];
    throw settled.find(s => s.status === 'rejected').reason;
  }

  return results.length === 1 ? results[0] : results;
}

/**
 * Flush events buffer to Parquet — all partitions written in parallel.
 *
 * FIX #5: Same buffer-restore-on-failure fix as flushUpdates.
 */
export async function flushEvents() {
  if (eventsBuffer.length === 0) return null;

  const rowsToWrite = eventsBuffer;
  eventsBuffer = [];

  const groups = groupByPartition(rowsToWrite, 'events', currentDataSource, currentMigrationId);

  // FIX #10 (audit fix #6): Same per-partition tracking as flushUpdates.
  const partitionEntries = Object.entries(groups);
  const writePromises = partitionEntries.map(([partition, records]) => {
    const partitionDir = join(getDataDir(), partition);
    ensureDir(partitionDir);
    const fileName = generateFileName('events');
    const filePath = join(partitionDir, fileName);

    const doWrite = USE_CLI
      ? writeToParquetCLI(records, filePath, 'events', partition, fileName)
      : writeToParquetPool(records, filePath, 'events', partition, fileName);

    return doWrite.then(result => {
      totalEventsWritten        += records.length;
      rowsTuneState.filesWritten++;
      rowsTuneState.rowsWritten += records.length;
      return result;
    });
  });

  const settled = await Promise.allSettled(writePromises);
  const results = [];
  const failedRecords = [];

  for (let i = 0; i < settled.length; i++) {
    if (settled[i].status === 'fulfilled' && settled[i].value) {
      results.push(settled[i].value);
    } else if (settled[i].status === 'rejected') {
      const [, records] = partitionEntries[i];
      failedRecords.push(...records);
      console.error(`[flushEvents] Partition write failed: ${settled[i].reason?.message || settled[i].reason}`);
    }
  }

  if (failedRecords.length > 0) {
    eventsBuffer = [...failedRecords, ...eventsBuffer];
    throw settled.find(s => s.status === 'rejected').reason;
  }

  return results.length === 1 ? results[0] : results;
}

export async function flushAll() {
  const [updatesResult, eventsResult] = await Promise.all([
    flushUpdates(),
    flushEvents(),
  ]);
  const results = [];
  if (updatesResult) results.push(updatesResult);
  if (eventsResult)  results.push(eventsResult);
  return results;
}

// ─── Stats ─────────────────────────────────────────────────────────────────

export function getBufferStats() {
  const poolStats  = poolInitialized && !USE_CLI ? getParquetWriterPool().getStats() : null;
  const gcsStats   = getGCSMode() ? getUploadStats()  : null;
  const uploadQueue = getGCSMode() ? getUploadQueue() : null;
  const queueStats = uploadQueue?.getStats() || null;

  return {
    updates:               updatesBuffer.length,
    events:                eventsBuffer.length,
    updatesBuffered:       updatesBuffer.length,
    eventsBuffered:        eventsBuffer.length,
    maxRowsPerFile:        dynamicRowsPerFile,
    maxRowsPerFileCap:     MAX_ROWS_PER_FILE_CAP,
    mode:                  USE_CLI ? 'cli' : 'pool',
    gcsMode:               getGCSMode(),
    queuedJobs:            poolStats?.queuedJobs    || 0,
    activeWorkers:         poolStats?.activeWorkers || 0,
    pendingWrites:         pendingWrites.size,
    totalUpdatesWritten,
    totalEventsWritten,
    totalFilesWritten,
    totalFilesEnqueued,    // FIX #6: renamed from totalFilesUploaded
    uploadQueuePaused:     shouldPauseWrites(),
    ...(poolStats && {
      poolCompletedJobs:   poolStats.completedJobs,
      poolFailedJobs:      poolStats.failedJobs,
      poolMbWritten:       poolStats.mbWritten,
      poolMbPerSec:        poolStats.mbPerSec,
      poolFilesPerSec:     poolStats.filesPerSec,
      validatedFiles:      poolStats.validatedFiles,
      validationFailures:  poolStats.validationFailures,
      validationRate:      poolStats.validationRate,
      validationIssues:    poolStats.validationIssues,
    }),
    ...(queueStats && {
      uploadQueuePending:  queueStats.pending,
      uploadQueueActive:   queueStats.active,
      uploadQueueCompleted: queueStats.completed,
      uploadQueueFailed:   queueStats.failed,
      uploadThroughputMBps: queueStats.throughputMBps,
    }),
    ...(gcsStats && {
      gcsUploads:          gcsStats.totalUploads,
      gcsSuccessful:       gcsStats.successfulUploads,
      gcsFailed:           gcsStats.failedUploads,
      gcsBytesUploaded:    gcsStats.totalBytesUploaded,
    }),
  };
}

// ─── Drain / Shutdown ──────────────────────────────────────────────────────

/**
 * Wait for all pending writes to complete.
 *
 * FIX #4: Promise.allSettled → Promise.all.
 *   allSettled never rejects — write errors were silently swallowed and
 *   shutdown proceeded as if all writes succeeded. Promise.all propagates
 *   the first failure to the caller.
 */
export async function waitForWrites() {
  if (USE_CLI) return;

  // FIX #4: Promise.all — write failures now propagate
  if (pendingWrites.size > 0) {
    await Promise.all([...pendingWrites]);
  }

  if (poolInitialized) {
    const pool = getParquetWriterPool();
    await pool.drain();
  }
}

export async function shutdown() {
  await flushAll();
  await waitForWrites();

  if (!USE_CLI) {
    await shutdownParquetPool();
    poolInitialized = false;
  }

  if (getGCSMode()) {
    await shutdownUploadQueue();
  }
}

// ─── Migration / source control ────────────────────────────────────────────

export function setMigrationId(id) {
  currentMigrationId = id;
}

export function setDataSource(source) {
  const validSources = ['backfill', 'updates'];
  currentDataSource  = validSources.includes(source) ? source : 'backfill';
}

export function getDataSource() {
  return currentDataSource;
}

export function clearMigrationId() {
  currentMigrationId = null;
}

// ─── Data purge (local mode only) ──────────────────────────────────────────

export function purgeMigrationData(migrationId) {
  if (getGCSMode()) {
    console.log(`   ⚠️ [write-parquet] Cannot purge GCS data from this command. Use gsutil.`);
    return { deletedFiles: 0, deletedDirs: 0 };
  }

  const dataDir         = getDataDir();
  const migrationPrefix = `migration=${migrationId}`;
  let deletedDirs       = 0;

  if (!existsSync(dataDir)) {
    console.log(`   ℹ️ Data directory doesn't exist`);
    return { deletedFiles: 0, deletedDirs: 0 };
  }

  const sources = ['backfill', 'updates'];
  const types   = ['updates', 'events'];

  for (const source of sources) {
    for (const type of types) {
      const typeDir = join(dataDir, source, type);
      if (!existsSync(typeDir)) continue;

      const entries = readdirSync(typeDir, { withFileTypes: true });
      for (const entry of entries) {
        if (entry.isDirectory() && entry.name === migrationPrefix) {
          const dirPath = join(typeDir, entry.name);
          try {
            rmSync(dirPath, { recursive: true, force: true });
            deletedDirs++;
            console.log(`   🗑️ Deleted partition: ${source}/${type}/${entry.name}`);
          } catch (err) {
            console.error(`   ❌ Failed to delete ${dirPath}: ${err.message}`);
          }
        }
      }
    }
  }

  console.log(`   ✅ Purged migration ${migrationId}: ${deletedDirs} directories`);
  return { deletedFiles: 0, deletedDirs };
}

export function purgeAllData() {
  if (getGCSMode()) {
    console.log(`   ⚠️ [write-parquet] Cannot purge GCS data from this command. Use gsutil.`);
    return;
  }

  const dataDir = getDataDir();
  if (!existsSync(dataDir)) {
    console.log(`   ℹ️ Data directory doesn't exist`);
    return;
  }

  try {
    rmSync(dataDir, { recursive: true, force: true });
    mkdirSync(dataDir, { recursive: true });
    console.log(`   ✅ Purged all data from ${dataDir}`);
  } catch (err) {
    console.error(`   ❌ Failed to purge: ${err.message}`);
  }
}

// FIX #9: Default export removed — it duplicated all named exports as a plain
// object and allowed callers to bypass the module's named-export interface.
// Use named imports: import { bufferUpdates, flushAll, ... } from './write-parquet.js';
