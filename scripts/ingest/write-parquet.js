/**
 * Parquet Writer Module - Direct DuckDB Version
 * 
 * Writes ledger data directly to Parquet files using DuckDB.
 * This eliminates the need for a separate materialization step.
 * 
 * Drop-in replacement for write-binary.js with same API surface.
 * 
 * How it works:
 * 1. Buffer records in memory (same as binary writer)
 * 2. On flush: Write temp JSONL, convert to Parquet via DuckDB, delete temp
 * 3. DuckDB handles Parquet + ZSTD compression natively
 */

import { mkdirSync, existsSync, rmSync, readdirSync, writeFileSync, unlinkSync } from 'fs';
import { join, dirname, sep, isAbsolute, resolve } from 'path';
import { fileURLToPath } from 'url';
import { randomBytes } from 'crypto';
import { execSync } from 'child_process';
import { getPartitionPath } from './data-schema.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Configuration - Default Windows path: C:\ledger_raw
const WIN_DEFAULT = 'C:\\ledger_raw';
const BASE_DATA_DIR_RAW = process.env.DATA_DIR || WIN_DEFAULT;

// Safety: require an absolute path
if (process.env.DATA_DIR && !isAbsolute(process.env.DATA_DIR)) {
  throw new Error(`[write-parquet] DATA_DIR must be an absolute path (got: ${process.env.DATA_DIR})`);
}

const BASE_DATA_DIR = resolve(BASE_DATA_DIR_RAW);
const DATA_DIR = join(BASE_DATA_DIR, 'raw'); // Parquet files go in raw/ subdirectory
const MAX_ROWS_PER_FILE = parseInt(process.env.MAX_ROWS_PER_FILE) || 5000;

// Log the output directory on module load
console.log(`📂 [write-parquet] Output directory: ${DATA_DIR}`);

// In-memory buffers
let updatesBuffer = [];
let eventsBuffer = [];
let currentMigrationId = null;

// Stats tracking
let totalUpdatesWritten = 0;
let totalEventsWritten = 0;
let totalFilesWritten = 0;

/**
 * Ensure directory exists (Windows-safe with race condition handling)
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
 * Write records to Parquet file via DuckDB
 */
async function writeToParquet(records, filePath, type) {
  if (records.length === 0) return null;
  
  const tempJsonlPath = filePath.replace('.parquet', '.temp.jsonl');
  
  try {
    // Map records to flat structure
    const mapped = type === 'updates' 
      ? records.map(mapUpdateRecord)
      : records.map(mapEventRecord);
    
    // Ensure parent directory exists
    const parentDir = dirname(filePath);
    ensureDir(parentDir);
    
    // Write temp JSONL
    const lines = mapped.map(r => JSON.stringify(r));
    writeFileSync(tempJsonlPath, lines.join('\n') + '\n');
    
    // Verify temp file was written
    if (!existsSync(tempJsonlPath)) {
      throw new Error(`Failed to write temp file: ${tempJsonlPath}`);
    }
    
    // Convert to Parquet via DuckDB CLI
    const sql = `
      COPY (
        SELECT * FROM read_json_auto('${tempJsonlPath.replace(/\\/g, '/')}')
      ) TO '${filePath.replace(/\\/g, '/')}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000);
    `;
    
    try {
      execSync(`duckdb -c "${sql}"`, { stdio: 'pipe' });
    } catch (duckErr) {
      console.error(`❌ DuckDB CLI failed. Is DuckDB installed and in PATH?`);
      console.error(`   Command: duckdb -c "..."`);
      console.error(`   Error: ${duckErr.message}`);
      throw duckErr;
    }
    
    // Verify parquet file was created
    if (!existsSync(filePath)) {
      throw new Error(`DuckDB ran but parquet file not created: ${filePath}`);
    }
    
    // Clean up temp file
    unlinkSync(tempJsonlPath);
    
    totalFilesWritten++;
    console.log(`📝 Wrote ${records.length} ${type} to ${filePath}`);
    
    return { file: filePath, count: records.length };
  } catch (err) {
    // Clean up temp file on error
    if (existsSync(tempJsonlPath)) {
      try { unlinkSync(tempJsonlPath); } catch {}
    }
    console.error(`❌ Parquet write failed for ${filePath}:`, err.message);
    throw err;
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
  
  // Use effective_at for partitioning
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
  
  const result = await writeToParquet(rowsToWrite, filePath, 'updates');
  totalUpdatesWritten += rowsToWrite.length;
  
  return result;
}

/**
 * Flush events buffer to Parquet file
 */
export async function flushEvents() {
  if (eventsBuffer.length === 0) return null;
  
  // Use effective_at for partitioning
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
  
  const result = await writeToParquet(rowsToWrite, filePath, 'events');
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
 * Get buffer and stats
 */
export function getBufferStats() {
  return {
    updates: updatesBuffer.length,
    events: eventsBuffer.length,
    updatesBuffered: updatesBuffer.length,
    eventsBuffered: eventsBuffer.length,
    maxRowsPerFile: MAX_ROWS_PER_FILE,
    queuedJobs: 0,
    activeWorkers: 0,
    pendingWrites: 0,
    totalUpdatesWritten,
    totalEventsWritten,
    totalFilesWritten,
  };
}

/**
 * Wait for writes (no-op for synchronous Parquet writes)
 */
export async function waitForWrites() {
  // Parquet writes are synchronous via DuckDB CLI
  return;
}

/**
 * Shutdown (no-op for synchronous writes)
 */
export async function shutdown() {
  await flushAll();
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
