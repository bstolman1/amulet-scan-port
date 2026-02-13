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
 * - PARQUET_USE_CLI=true: Fall back to synchronous CLI approach
 */

import { mkdirSync, existsSync, rmSync, readdirSync, writeFileSync, unlinkSync, statSync } from 'fs';
import { join, dirname, sep, isAbsolute, resolve } from 'path';
import { fileURLToPath } from 'url';
import { randomBytes } from 'crypto';
import { execSync } from 'child_process';
import { getPartitionPath, groupByPartition } from './data-schema.js';
import { getParquetWriterPool, shutdownParquetPool } from './parquet-writer-pool.js';
import { 
  isGCSMode, 
  getTmpRawDir, 
  getRawDir, 
  getBaseDataDir,
  ensureTmpDir 
} from './path-utils.js';
import {
  initGCS,
  isGCSEnabled,
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

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
// Configuration - LAZY INITIALIZATION
// These are computed lazily to avoid ESM import hoisting issues where
// env vars might not be loaded yet when this module is first imported.
let _gcsMode = null;
let _dataDir = null;
let _initialized = false;

function getGCSMode() {
  if (_gcsMode === null) {
    _gcsMode = isGCSMode();
  }
  return _gcsMode;
}

function getDataDir() {
  if (_dataDir === null) {
    _dataDir = getGCSMode() ? getTmpRawDir() : getRawDir();
  }
  return _dataDir;
}

const MAX_ROWS_PER_FILE = parseInt(process.env.MAX_ROWS_PER_FILE) || 5000;
const USE_CLI = process.env.PARQUET_USE_CLI === 'true';

/**
 * Initialize the parquet writer (call once before first write)
 */
export function initParquetWriter() {
  if (_initialized) return;
  _initialized = true;
  
  const gcsMode = getGCSMode();
  const dataDir = getDataDir();
  
  if (gcsMode) {
    try {
      initGCS();
      ensureTmpDir();
      // Initialize upload queue for async background uploads
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

  console.log(`📂 [write-parquet] Mode: ${USE_CLI ? 'CLI (synchronous)' : 'Worker Pool (parallel)'}`);
}

// In-memory buffers
let updatesBuffer = [];
let eventsBuffer = [];
let currentMigrationId = null;
let currentDataSource = 'backfill';  // 'backfill' or 'updates'

// Stats tracking
let totalUpdatesWritten = 0;
let totalEventsWritten = 0;
let totalFilesWritten = 0;
let totalFilesUploaded = 0;

// Pending write promises (for tracking parallel writes)
const pendingWrites = new Set();

// Pool initialization flag
let poolInitialized = false;

/**
 * Initialize the writer pool (called automatically on first write)
 */
async function ensurePoolInitialized() {
  if (poolInitialized || USE_CLI) return;
  
  const pool = getParquetWriterPool();
  await pool.init();
  poolInitialized = true;
}

/**
 * Ensure directory exists (Windows-safe)
 */
function ensureDir(dirPath) {
  const normalizedPath = dirPath.split('/').join(sep);
  try {
    if (!existsSync(normalizedPath)) {
      mkdirSync(normalizedPath, { recursive: true });
    }
  } catch (err) {
    if (err.code !== 'EEXIST') {
      const parts = normalizedPath.split(sep).filter(Boolean);
      let current = parts[0].includes(':') ? parts[0] + sep : sep;
      
      for (let i = parts[0].includes(':') ? 1 : 0; i < parts.length; i++) {
        current = join(current, parts[i]);
        try {
          if (!existsSync(current)) {
            mkdirSync(current);
          }
        } catch (e) {
          if (e.code !== 'EEXIST') throw e;
        }
      }
    }
  }
}

/**
 * Generate unique filename for Parquet files
 */
function generateFileName(prefix) {
  const ts = Date.now();
  const rand = randomBytes(4).toString('hex');
  return `${prefix}-${ts}-${rand}.parquet`;
}

/**
 * Get the relative path from DATA_DIR for GCS upload
 */
function getRelativePath(fullPath) {
  // Remove the DATA_DIR prefix to get relative path
  const dataDir = getDataDir();
  if (fullPath.startsWith(dataDir)) {
    return fullPath.substring(dataDir.length).replace(/^[/\\]/, '');
  }
  return fullPath;
}

/**
 * Upload file to GCS if in GCS mode (non-blocking, queued)
 */
function uploadToGCSIfEnabled(localPath, partition, fileName) {
  if (!getGCSMode()) return null;
  
  const relativePath = join(partition, fileName).replace(/\\/g, '/');
  const gcsPath = getGCSPath(relativePath);
  
  // Enqueue for background upload (returns immediately)
  enqueueUpload(localPath, gcsPath);
  totalFilesUploaded++;
  
  return { queued: true, gcsPath };
}

/**
 * Map update record to flat structure for Parquet
 */
function mapUpdateRecord(r) {
  const safeTimestamp = (val) => {
    if (!val) return null;
    if (typeof val === 'number') return new Date(val).toISOString();
    if (val instanceof Date) return val.toISOString();
    return val;
  };
  
  const safeInt64 = (val) => {
    if (val === null || val === undefined) return null;
    const num = parseInt(val);
    return isNaN(num) ? null : num;
  };
  
  const safeStringArray = (arr) => Array.isArray(arr) ? arr : null;
  
  const safeStringify = (obj) => {
    if (!obj) return null;
    try {
      return typeof obj === 'string' ? obj : JSON.stringify(obj);
    } catch { return null; }
  };

  return {
    update_id: String(r.update_id || r.id || ''),
    update_type: String(r.update_type || r.type || ''),
    synchronizer_id: String(r.synchronizer_id || r.synchronizer || ''),
    effective_at: safeTimestamp(r.effective_at),
    recorded_at: safeTimestamp(r.recorded_at || r.timestamp),
    record_time: safeTimestamp(r.record_time),
    // Keep a dedicated ingestion timestamp column for audit/debug parity with schema
    timestamp: safeTimestamp(r.timestamp),
    command_id: r.command_id || null,
    workflow_id: r.workflow_id || null,
    kind: r.kind || null,
    migration_id: safeInt64(r.migration_id),
    offset: safeInt64(r.offset),
    event_count: parseInt(r.event_count) || 0,
    root_event_ids: safeStringArray(r.root_event_ids || r.rootEventIds),
    source_synchronizer: r.source_synchronizer || null,
    target_synchronizer: r.target_synchronizer || null,
    unassign_id: r.unassign_id || null,
    submitter: r.submitter || null,
    reassignment_counter: safeInt64(r.reassignment_counter),
    trace_context: safeStringify(r.trace_context),
    update_data: safeStringify(r.update_data),
  };
}

/**
 * Map event record to flat structure for Parquet
 */
function mapEventRecord(r) {
  const safeTimestamp = (val) => {
    if (!val) return null;
    if (typeof val === 'number') return new Date(val).toISOString();
    if (val instanceof Date) return val.toISOString();
    return val;
  };
  
  const safeInt64 = (val) => {
    if (val === null || val === undefined) return null;
    const num = parseInt(val);
    return isNaN(num) ? null : num;
  };
  
  const safeStringArray = (arr) => Array.isArray(arr) ? arr : null;
  
  const safeStringify = (obj) => {
    if (!obj) return null;
    try {
      return typeof obj === 'string' ? obj : JSON.stringify(obj);
    } catch { return null; }
  };

  return {
    event_id: String(r.event_id || r.id || ''),
    update_id: String(r.update_id || ''),
    event_type: String(r.event_type || r.type || ''),
    event_type_original: String(r.event_type_original || r.type_original || ''),
    synchronizer_id: String(r.synchronizer_id || r.synchronizer || ''),
    effective_at: safeTimestamp(r.effective_at),
    recorded_at: safeTimestamp(r.recorded_at || r.timestamp),
    // Keep a dedicated ingestion timestamp column for audit/debug parity with schema
    timestamp: safeTimestamp(r.timestamp),
    created_at_ts: safeTimestamp(r.created_at_ts),
    contract_id: r.contract_id || null,
    template_id: r.template_id || r.template || null,
    package_name: r.package_name || null,
    migration_id: safeInt64(r.migration_id),
    signatories: safeStringArray(r.signatories),
    observers: safeStringArray(r.observers),
    acting_parties: safeStringArray(r.acting_parties),
    witness_parties: safeStringArray(r.witness_parties),
    payload: safeStringify(r.payload),
    contract_key: safeStringify(r.contract_key),
    choice: r.choice || null,
    consuming: r.consuming ?? null,
    interface_id: r.interface_id || null,
    child_event_ids: safeStringArray(r.child_event_ids || r.childEventIds),
    exercise_result: safeStringify(r.exercise_result),
    source_synchronizer: r.source_synchronizer || null,
    target_synchronizer: r.target_synchronizer || null,
    unassign_id: r.unassign_id || null,
    submitter: r.submitter || null,
    reassignment_counter: safeInt64(r.reassignment_counter),
    raw_event: safeStringify(r.raw_event || r.raw_json || r.raw),
  };
}

/**
 * Write records to Parquet via CLI (synchronous fallback)
 */
function writeToParquetCLI(records, filePath, type, partition, fileName) {
  if (records.length === 0) return null;
  
  const normalizedFilePath = filePath.replace(/\\/g, '/');
  const tempJsonlPath = normalizedFilePath.replace('.parquet', '.temp.jsonl');
  
  try {
    const mapped = type === 'updates' 
      ? records.map(mapUpdateRecord)
      : records.map(mapEventRecord);
    
    const parentDir = dirname(filePath);
    ensureDir(parentDir);
    
    const lines = mapped.map(r => JSON.stringify(r));
    writeFileSync(tempJsonlPath.replace(/\//g, sep), lines.join('\n') + '\n');
    
    const readFn = type === 'events'
      ? `read_json_auto('${tempJsonlPath}', columns={
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
      : `read_json_auto('${tempJsonlPath}', columns={
          update_id: 'VARCHAR', update_type: 'VARCHAR', synchronizer_id: 'VARCHAR', effective_at: 'VARCHAR',
          recorded_at: 'VARCHAR', record_time: 'VARCHAR', timestamp: 'VARCHAR', command_id: 'VARCHAR', workflow_id: 'VARCHAR', kind: 'VARCHAR',
          migration_id: 'BIGINT', "offset": 'BIGINT', event_count: 'INTEGER', root_event_ids: 'VARCHAR[]',
          source_synchronizer: 'VARCHAR', target_synchronizer: 'VARCHAR', unassign_id: 'VARCHAR', submitter: 'VARCHAR',
          reassignment_counter: 'BIGINT', trace_context: 'VARCHAR', update_data: 'VARCHAR'
        }, union_by_name=true)`;

    const sql = `COPY (SELECT * FROM ${readFn}) TO '${normalizedFilePath}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000);`;
    
    execSync(`duckdb -c "${sql}"`, { 
      encoding: 'utf8',
      stdio: ['pipe', 'pipe', 'pipe'],
      cwd: parentDir,
    });
    
    const tempNativePath = tempJsonlPath.replace(/\//g, sep);
    if (existsSync(tempNativePath)) {
      unlinkSync(tempNativePath);
    }
    
    totalFilesWritten++;
    console.log(`📝 Wrote ${records.length} ${type} to ${filePath}`);
    
    // Upload to GCS if enabled (this also deletes the local file)
    if (getGCSMode()) {
      uploadToGCSIfEnabled(filePath, partition, fileName);
    }
    
    return { file: filePath, count: records.length };
  } catch (err) {
    const tempNativePath = tempJsonlPath.replace(/\//g, sep);
    if (existsSync(tempNativePath)) {
      try { unlinkSync(tempNativePath); } catch {}
    }
    // In GCS mode, always clean up local file on error
    if (getGCSMode() && existsSync(filePath)) {
      try { unlinkSync(filePath); } catch {}
    }
    console.error(`❌ Parquet write failed for ${filePath}:`, err.message);
    throw err;
  }
}

/**
 * Write records to Parquet via worker pool (parallel)
 */
async function writeToParquetPool(records, filePath, type, partition, fileName) {
  if (records.length === 0) return null;
  
  await ensurePoolInitialized();
  
  // Map records before sending to worker
  const mapped = type === 'updates' 
    ? records.map(mapUpdateRecord)
    : records.map(mapEventRecord);
  
  // Ensure parent directory exists
  const parentDir = dirname(filePath);
  ensureDir(parentDir);
  
  // Enqueue job to pool
  const pool = getParquetWriterPool();
  const writePromise = pool.writeJob({
    type,
    filePath,
    records: mapped,
  });
  
  // Track pending write
  pendingWrites.add(writePromise);
  
  try {
    const result = await writePromise;
    totalFilesWritten++;
    
    // Log with validation status
    const validationStatus = result.validation 
      ? (result.validation.valid ? '✅' : `⚠️ ${result.validation.issues.length} issues`)
      : '';
    console.log(`📝 Wrote ${records.length} ${type} to ${filePath} (${(result.bytes / 1024).toFixed(1)}KB) ${validationStatus}`);
    
    // Upload to GCS if enabled (this also deletes the local file)
    if (getGCSMode()) {
      uploadToGCSIfEnabled(filePath, partition, fileName);
    }
    
    return { 
      file: filePath, 
      count: records.length, 
      bytes: result.bytes,
      validation: result.validation,
    };
  } catch (err) {
    // In GCS mode, always clean up local file on error
    if (getGCSMode() && existsSync(filePath)) {
      try { unlinkSync(filePath); } catch {}
    }
    throw err;
  } finally {
    pendingWrites.delete(writePromise);
  }
}

/**
 * Add updates to buffer
 */
export async function bufferUpdates(updates) {
  updatesBuffer.push(...updates);
  
  if (updatesBuffer.length >= MAX_ROWS_PER_FILE) {
    return await flushUpdates();
  }
  return null;
}

/**
 * Add events to buffer
 */
export async function bufferEvents(events) {
  eventsBuffer.push(...events);
  
  if (eventsBuffer.length >= MAX_ROWS_PER_FILE) {
    return await flushEvents();
  }
  return null;
}

/**
 * Flush updates buffer to Parquet file
 */
export async function flushUpdates() {
  if (updatesBuffer.length === 0) return null;
  
  const rowsToWrite = updatesBuffer;
  updatesBuffer = [];
  
  // Group by each record's own effective_at — no more first-record-wins
  const groups = groupByPartition(rowsToWrite, 'updates', currentDataSource, currentMigrationId);
  const results = [];
  
  for (const [partition, records] of Object.entries(groups)) {
    const partitionDir = join(getDataDir(), partition);
    ensureDir(partitionDir);
    const fileName = generateFileName('updates');
    const filePath = join(partitionDir, fileName);
    
    const result = USE_CLI 
      ? writeToParquetCLI(records, filePath, 'updates', partition, fileName)
      : await writeToParquetPool(records, filePath, 'updates', partition, fileName);
    
    totalUpdatesWritten += records.length;
    if (result) results.push(result);
  }
  
  return results.length === 1 ? results[0] : results;
}

/**
 * Flush events buffer to Parquet file
 */
export async function flushEvents() {
  if (eventsBuffer.length === 0) return null;
  
  const rowsToWrite = eventsBuffer;
  eventsBuffer = [];
  
  // Group by each record's own effective_at — no more first-record-wins
  const groups = groupByPartition(rowsToWrite, 'events', currentDataSource, currentMigrationId);
  const results = [];
  
  for (const [partition, records] of Object.entries(groups)) {
    const partitionDir = join(getDataDir(), partition);
    ensureDir(partitionDir);
    const fileName = generateFileName('events');
    const filePath = join(partitionDir, fileName);
    
    const result = USE_CLI
      ? writeToParquetCLI(records, filePath, 'events', partition, fileName)
      : await writeToParquetPool(records, filePath, 'events', partition, fileName);
    
    totalEventsWritten += records.length;
    if (result) results.push(result);
  }
  
  return results.length === 1 ? results[0] : results;
}

/**
 * Flush all buffers
 */
export async function flushAll() {
  const results = [];
  
  const updatesResult = await flushUpdates();
  if (updatesResult) results.push(updatesResult);
  
  const eventsResult = await flushEvents();
  if (eventsResult) results.push(eventsResult);
  
  return results;
}

/**
 * Get buffer and pool stats
 */
export function getBufferStats() {
  const poolStats = poolInitialized && !USE_CLI 
    ? getParquetWriterPool().getStats() 
    : null;
  
  const gcsStats = getGCSMode() ? getUploadStats() : null;
  const uploadQueue = getGCSMode() ? getUploadQueue() : null;
  const queueStats = uploadQueue?.getStats() || null;
  
  return {
    updates: updatesBuffer.length,
    events: eventsBuffer.length,
    updatesBuffered: updatesBuffer.length,
    eventsBuffered: eventsBuffer.length,
    maxRowsPerFile: MAX_ROWS_PER_FILE,
    mode: USE_CLI ? 'cli' : 'pool',
    gcsMode: getGCSMode(),
    queuedJobs: poolStats?.queuedJobs || 0,
    activeWorkers: poolStats?.activeWorkers || 0,
    pendingWrites: pendingWrites.size,
    totalUpdatesWritten,
    totalEventsWritten,
    totalFilesWritten,
    totalFilesUploaded,
    // Upload queue backpressure status
    uploadQueuePaused: shouldPauseWrites(),
    // Pool-specific stats
    ...(poolStats && {
      poolCompletedJobs: poolStats.completedJobs,
      poolFailedJobs: poolStats.failedJobs,
      poolMbWritten: poolStats.mbWritten,
      poolMbPerSec: poolStats.mbPerSec,
      poolFilesPerSec: poolStats.filesPerSec,
      // Validation stats
      validatedFiles: poolStats.validatedFiles,
      validationFailures: poolStats.validationFailures,
      validationRate: poolStats.validationRate,
      validationIssues: poolStats.validationIssues,
    }),
    // Upload queue stats
    ...(queueStats && {
      uploadQueuePending: queueStats.pending,
      uploadQueueActive: queueStats.active,
      uploadQueueCompleted: queueStats.completed,
      uploadQueueFailed: queueStats.failed,
      uploadThroughputMBps: queueStats.throughputMBps,
    }),
    // GCS-specific stats (legacy, for backward compat)
    ...(gcsStats && {
      gcsUploads: gcsStats.totalUploads,
      gcsSuccessful: gcsStats.successfulUploads,
      gcsFailed: gcsStats.failedUploads,
      gcsBytesUploaded: gcsStats.totalBytesUploaded,
    }),
  };
}

/**
 * Wait for all pending writes to complete
 */
export async function waitForWrites() {
  if (USE_CLI) return;
  
  // Wait for any pending write promises
  if (pendingWrites.size > 0) {
    await Promise.allSettled([...pendingWrites]);
  }
  
  // Drain the pool
  if (poolInitialized) {
    const pool = getParquetWriterPool();
    await pool.drain();
  }
}

/**
 * Shutdown writer pool
 */
export async function shutdown() {
  await flushAll();
  await waitForWrites();
  
  if (!USE_CLI) {
    await shutdownParquetPool();
    poolInitialized = false;
  }
  
  if (getGCSMode()) {
    // Drain the async upload queue
    await shutdownUploadQueue();
  }
}

/**
 * Set current migration ID
 */
export function setMigrationId(id) {
  currentMigrationId = id;
}

/**
 * Set current data source ('backfill' or 'updates')
 * This controls which top-level folder data is written to.
 */
export function setDataSource(source) {
  const validSources = ['backfill', 'updates'];
  currentDataSource = validSources.includes(source) ? source : 'backfill';
}

/**
 * Get current data source
 */
export function getDataSource() {
  return currentDataSource;
}

/**
 * Clear migration ID
 */
export function clearMigrationId() {
  currentMigrationId = null;
}

/**
 * Purge migration data (local mode only)
 */
export function purgeMigrationData(migrationId) {
  if (getGCSMode()) {
    console.log(`   ⚠️ [write-parquet] Cannot purge GCS data from this command. Use gsutil.`);
    return { deletedFiles: 0, deletedDirs: 0 };
  }
  
  const dataDir = getDataDir();
  const migrationPrefix = `migration=${migrationId}`;
  let deletedDirs = 0;
  
  if (!existsSync(dataDir)) {
    console.log(`   ℹ️ Data directory doesn't exist`);
    return { deletedFiles: 0, deletedDirs: 0 };
  }
  
  const entries = readdirSync(dataDir, { withFileTypes: true });
  
  for (const entry of entries) {
    if (entry.isDirectory() && entry.name.startsWith(migrationPrefix)) {
      const dirPath = join(dataDir, entry.name);
      try {
        rmSync(dirPath, { recursive: true, force: true });
        deletedDirs++;
        console.log(`   🗑️ Deleted partition: ${entry.name}`);
      } catch (err) {
        console.error(`   ❌ Failed to delete ${dirPath}: ${err.message}`);
      }
    }
  }
  
  console.log(`   ✅ Purged migration ${migrationId}: ${deletedDirs} directories`);
  return { deletedFiles: 0, deletedDirs };
}

/**
 * Purge all data (local mode only)
 */
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

export default {
  initParquetWriter,
  bufferUpdates,
  bufferEvents,
  flushUpdates,
  flushEvents,
  flushAll,
  getBufferStats,
  waitForWrites,
  shutdown,
  setMigrationId,
  setDataSource,
  getDataSource,
  clearMigrationId,
  purgeMigrationData,
  purgeAllData,
};
