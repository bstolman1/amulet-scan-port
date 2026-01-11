/**
 * Parquet Writer Module - Parallel DuckDB Version
 * 
 * Writes ledger data to Parquet files using a worker pool with DuckDB.
 * Each worker has its own in-memory DuckDB instance for parallel writes.
 * 
 * Drop-in replacement for the original synchronous CLI-based version.
 * 
 * How it works:
 * 1. Buffer records in memory (same as before)
 * 2. On flush: Enqueue job to worker pool (non-blocking)
 * 3. Worker writes Parquet with ZSTD via DuckDB Node.js bindings
 * 
 * Configuration:
 * - PARQUET_WORKERS: Number of parallel writers (default: CPU-1)
 * - MAX_ROWS_PER_FILE: Records per Parquet file (default: 5000)
 * - PARQUET_USE_CLI=true: Fall back to synchronous CLI approach
 */

import { mkdirSync, existsSync, rmSync, readdirSync, writeFileSync, unlinkSync, statSync } from 'fs';
import { join, dirname, sep, isAbsolute, resolve } from 'path';
import { fileURLToPath } from 'url';
import { randomBytes } from 'crypto';
import { execSync } from 'child_process';
import { getPartitionPath } from './data-schema.js';
import { getParquetWriterPool, shutdownParquetPool } from './parquet-writer-pool.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Configuration
const WIN_DEFAULT = 'C:\\ledger_raw';
const BASE_DATA_DIR_RAW = process.env.DATA_DIR || WIN_DEFAULT;

if (process.env.DATA_DIR && !isAbsolute(process.env.DATA_DIR)) {
  throw new Error(`[write-parquet] DATA_DIR must be an absolute path (got: ${process.env.DATA_DIR})`);
}

const BASE_DATA_DIR = resolve(BASE_DATA_DIR_RAW);
const DATA_DIR = join(BASE_DATA_DIR, 'raw');
const MAX_ROWS_PER_FILE = parseInt(process.env.MAX_ROWS_PER_FILE) || 5000;
const USE_CLI = process.env.PARQUET_USE_CLI === 'true';

// Log configuration on module load
console.log(`📂 [write-parquet] Output directory: ${DATA_DIR}`);
console.log(`📂 [write-parquet] Mode: ${USE_CLI ? 'CLI (synchronous)' : 'Worker Pool (parallel)'}`);

// In-memory buffers
let updatesBuffer = [];
let eventsBuffer = [];
let currentMigrationId = null;

// Stats tracking
let totalUpdatesWritten = 0;
let totalEventsWritten = 0;
let totalFilesWritten = 0;

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
function writeToParquetCLI(records, filePath, type) {
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
          contract_id: 'VARCHAR', template_id: 'VARCHAR', package_name: 'VARCHAR', migration_id: 'BIGINT',
          signatories: 'VARCHAR[]', observers: 'VARCHAR[]', acting_parties: 'VARCHAR[]', witness_parties: 'VARCHAR[]',
          child_event_ids: 'VARCHAR[]', consuming: 'BOOLEAN', reassignment_counter: 'BIGINT',
          payload: 'VARCHAR', contract_key: 'VARCHAR', exercise_result: 'VARCHAR', raw_event: 'VARCHAR', trace_context: 'VARCHAR'
        }, union_by_name=true)`
      : `read_json_auto('${tempJsonlPath}', columns={
          update_id: 'VARCHAR', update_type: 'VARCHAR', synchronizer_id: 'VARCHAR', effective_at: 'VARCHAR',
          recorded_at: 'VARCHAR', record_time: 'VARCHAR', command_id: 'VARCHAR', workflow_id: 'VARCHAR', kind: 'VARCHAR',
          migration_id: 'BIGINT', offset: 'BIGINT', event_count: 'INTEGER', root_event_ids: 'VARCHAR[]',
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
    
    return { file: filePath, count: records.length };
  } catch (err) {
    const tempNativePath = tempJsonlPath.replace(/\//g, sep);
    if (existsSync(tempNativePath)) {
      try { unlinkSync(tempNativePath); } catch {}
    }
    console.error(`❌ Parquet write failed for ${filePath}:`, err.message);
    throw err;
  }
}

/**
 * Write records to Parquet via worker pool (parallel)
 */
async function writeToParquetPool(records, filePath, type) {
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
    console.log(`📝 Wrote ${records.length} ${type} to ${filePath} (${(result.bytes / 1024).toFixed(1)}KB)`);
    return { file: filePath, count: records.length, bytes: result.bytes };
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
  
  const firstRecord = updatesBuffer[0];
  const effectiveAt = firstRecord?.effective_at || firstRecord?.record_time || firstRecord?.timestamp || new Date();
  const migrationId = currentMigrationId || firstRecord?.migration_id || null;
  const partition = getPartitionPath(effectiveAt, migrationId);
  const partitionDir = join(DATA_DIR, partition);
  
  ensureDir(partitionDir);
  
  const fileName = generateFileName('updates');
  const filePath = join(partitionDir, fileName);
  
  const rowsToWrite = updatesBuffer;
  updatesBuffer = [];
  
  const result = USE_CLI 
    ? writeToParquetCLI(rowsToWrite, filePath, 'updates')
    : await writeToParquetPool(rowsToWrite, filePath, 'updates');
  
  totalUpdatesWritten += rowsToWrite.length;
  
  return result;
}

/**
 * Flush events buffer to Parquet file
 */
export async function flushEvents() {
  if (eventsBuffer.length === 0) return null;
  
  const firstRecord = eventsBuffer[0];
  const effectiveAt = firstRecord?.effective_at || firstRecord?.recorded_at || firstRecord?.timestamp || new Date();
  const migrationId = currentMigrationId || firstRecord?.migration_id || null;
  const partition = getPartitionPath(effectiveAt, migrationId);
  const partitionDir = join(DATA_DIR, partition);
  
  ensureDir(partitionDir);
  
  const fileName = generateFileName('events');
  const filePath = join(partitionDir, fileName);
  
  const rowsToWrite = eventsBuffer;
  eventsBuffer = [];
  
  const result = USE_CLI
    ? writeToParquetCLI(rowsToWrite, filePath, 'events')
    : await writeToParquetPool(rowsToWrite, filePath, 'events');
  
  totalEventsWritten += rowsToWrite.length;
  
  return result;
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
  
  return {
    updates: updatesBuffer.length,
    events: eventsBuffer.length,
    updatesBuffered: updatesBuffer.length,
    eventsBuffered: eventsBuffer.length,
    maxRowsPerFile: MAX_ROWS_PER_FILE,
    mode: USE_CLI ? 'cli' : 'pool',
    queuedJobs: poolStats?.queuedJobs || 0,
    activeWorkers: poolStats?.activeWorkers || 0,
    pendingWrites: pendingWrites.size,
    totalUpdatesWritten,
    totalEventsWritten,
    totalFilesWritten,
    // Pool-specific stats
    ...(poolStats && {
      poolCompletedJobs: poolStats.completedJobs,
      poolFailedJobs: poolStats.failedJobs,
      poolMbWritten: poolStats.mbWritten,
      poolMbPerSec: poolStats.mbPerSec,
      poolFilesPerSec: poolStats.filesPerSec,
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
}

/**
 * Set current migration ID
 */
export function setMigrationId(id) {
  currentMigrationId = id;
}

/**
 * Clear migration ID
 */
export function clearMigrationId() {
  currentMigrationId = null;
}

/**
 * Purge migration data
 */
export function purgeMigrationData(migrationId) {
  const migrationPrefix = `migration=${migrationId}`;
  let deletedDirs = 0;
  
  if (!existsSync(DATA_DIR)) {
    console.log(`   ℹ️ Data directory doesn't exist`);
    return { deletedFiles: 0, deletedDirs: 0 };
  }
  
  const entries = readdirSync(DATA_DIR, { withFileTypes: true });
  
  for (const entry of entries) {
    if (entry.isDirectory() && entry.name.startsWith(migrationPrefix)) {
      const dirPath = join(DATA_DIR, entry.name);
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
 * Purge all data
 */
export function purgeAllData() {
  if (!existsSync(DATA_DIR)) {
    console.log(`   ℹ️ Data directory doesn't exist`);
    return;
  }
  
  try {
    rmSync(DATA_DIR, { recursive: true, force: true });
    mkdirSync(DATA_DIR, { recursive: true });
    console.log(`   ✅ Purged all data from ${DATA_DIR}`);
  } catch (err) {
    console.error(`   ❌ Failed to purge: ${err.message}`);
  }
}

export default {
  bufferUpdates,
  bufferEvents,
  flushUpdates,
  flushEvents,
  flushAll,
  getBufferStats,
  waitForWrites,
  shutdown,
  setMigrationId,
  clearMigrationId,
  purgeMigrationData,
  purgeAllData,
};
