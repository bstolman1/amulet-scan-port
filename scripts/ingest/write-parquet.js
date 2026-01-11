/**
 * Parquet Writer Module - DuckDB Node.js Library Version
 * 
 * Writes ledger data directly to Parquet files using DuckDB Node.js library.
 * This eliminates the need for a separate materialization step.
 * 
 * Drop-in replacement for write-binary.js with same API surface.
 * 
 * How it works:
 * 1. Buffer records in memory (same as binary writer)
 * 2. On flush: Create in-memory DuckDB table, insert records, COPY TO parquet
 * 3. DuckDB handles Parquet + ZSTD compression natively
 */

import { mkdirSync, existsSync, rmSync, readdirSync, writeFileSync, unlinkSync } from 'fs';
import { join, dirname, sep, isAbsolute, resolve } from 'path';
import { fileURLToPath } from 'url';
import { randomBytes } from 'crypto';
import duckdb from 'duckdb';
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

// Create a single DuckDB instance for the module (in-memory, just for writing)
const db = new duckdb.Database(':memory:');

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
 * Run a DuckDB query and return results as a promise
 */
function runQuery(sql) {
  return new Promise((resolve, reject) => {
    db.all(sql, (err, rows) => {
      if (err) reject(err);
      else resolve(rows);
    });
  });
}

/**
 * Run a DuckDB exec (no results) as a promise
 */
function runExec(sql) {
  return new Promise((resolve, reject) => {
    db.exec(sql, (err) => {
      if (err) reject(err);
      else resolve();
    });
  });
}

/**
 * Escape a string value for SQL
 */
function escapeStr(val) {
  if (val === null || val === undefined) return 'NULL';
  const str = String(val).replace(/'/g, "''");
  return `'${str}'`;
}

/**
 * Write records to Parquet file via DuckDB Node.js library
 */
async function writeToParquet(records, filePath, type) {
  if (records.length === 0) return null;
  
  // Normalize path for DuckDB (forward slashes work on all platforms)
  const normalizedFilePath = filePath.replace(/\\/g, '/');
  
  try {
    // Map records to flat structure
    const mapped = type === 'updates' 
      ? records.map(mapUpdateRecord)
      : records.map(mapEventRecord);
    
    // Ensure parent directory exists
    const parentDir = dirname(filePath);
    ensureDir(parentDir);
    
    // Create a temp table name
    const tableName = `temp_${type}_${Date.now()}_${randomBytes(4).toString('hex')}`;
    
    // Build CREATE TABLE statement based on type
    const columns = type === 'updates' ? [
      'update_id VARCHAR',
      'update_type VARCHAR',
      'synchronizer_id VARCHAR',
      'effective_at VARCHAR',
      'recorded_at VARCHAR',
      'record_time VARCHAR',
      'command_id VARCHAR',
      'workflow_id VARCHAR',
      'kind VARCHAR',
      'migration_id BIGINT',
      '"offset" BIGINT',
      'event_count INTEGER',
      'root_event_ids VARCHAR',
      'source_synchronizer VARCHAR',
      'target_synchronizer VARCHAR',
      'unassign_id VARCHAR',
      'submitter VARCHAR',
      'reassignment_counter BIGINT',
      'trace_context VARCHAR',
      'update_data VARCHAR',
    ] : [
      'event_id VARCHAR',
      'update_id VARCHAR',
      'event_type VARCHAR',
      'event_type_original VARCHAR',
      'synchronizer_id VARCHAR',
      'effective_at VARCHAR',
      'recorded_at VARCHAR',
      'created_at_ts VARCHAR',
      'contract_id VARCHAR',
      'template_id VARCHAR',
      'package_name VARCHAR',
      'migration_id BIGINT',
      'signatories VARCHAR',
      'observers VARCHAR',
      'acting_parties VARCHAR',
      'witness_parties VARCHAR',
      'payload VARCHAR',
      'contract_key VARCHAR',
      'choice VARCHAR',
      'consuming BOOLEAN',
      'interface_id VARCHAR',
      'child_event_ids VARCHAR',
      'exercise_result VARCHAR',
      'source_synchronizer VARCHAR',
      'target_synchronizer VARCHAR',
      'unassign_id VARCHAR',
      'submitter VARCHAR',
      'reassignment_counter BIGINT',
      'raw_event VARCHAR',
    ];
    
    // Create temp table
    await runExec(`CREATE TABLE ${tableName} (${columns.join(', ')})`);
    
    // Insert records in batches (to avoid SQL statement size limits)
    const BATCH_SIZE = 100;
    for (let i = 0; i < mapped.length; i += BATCH_SIZE) {
      const batch = mapped.slice(i, i + BATCH_SIZE);
      
      const valueRows = batch.map(r => {
        if (type === 'updates') {
          return `(${escapeStr(r.update_id)}, ${escapeStr(r.update_type)}, ${escapeStr(r.synchronizer_id)}, ${escapeStr(r.effective_at)}, ${escapeStr(r.recorded_at)}, ${escapeStr(r.record_time)}, ${escapeStr(r.command_id)}, ${escapeStr(r.workflow_id)}, ${escapeStr(r.kind)}, ${r.migration_id ?? 'NULL'}, ${r.offset ?? 'NULL'}, ${r.event_count ?? 0}, ${escapeStr(JSON.stringify(r.root_event_ids))}, ${escapeStr(r.source_synchronizer)}, ${escapeStr(r.target_synchronizer)}, ${escapeStr(r.unassign_id)}, ${escapeStr(r.submitter)}, ${r.reassignment_counter ?? 'NULL'}, ${escapeStr(r.trace_context)}, ${escapeStr(r.update_data)})`;
        } else {
          return `(${escapeStr(r.event_id)}, ${escapeStr(r.update_id)}, ${escapeStr(r.event_type)}, ${escapeStr(r.event_type_original)}, ${escapeStr(r.synchronizer_id)}, ${escapeStr(r.effective_at)}, ${escapeStr(r.recorded_at)}, ${escapeStr(r.created_at_ts)}, ${escapeStr(r.contract_id)}, ${escapeStr(r.template_id)}, ${escapeStr(r.package_name)}, ${r.migration_id ?? 'NULL'}, ${escapeStr(JSON.stringify(r.signatories))}, ${escapeStr(JSON.stringify(r.observers))}, ${escapeStr(JSON.stringify(r.acting_parties))}, ${escapeStr(JSON.stringify(r.witness_parties))}, ${escapeStr(r.payload)}, ${escapeStr(r.contract_key)}, ${escapeStr(r.choice)}, ${r.consuming ? 'TRUE' : 'FALSE'}, ${escapeStr(r.interface_id)}, ${escapeStr(JSON.stringify(r.child_event_ids))}, ${escapeStr(r.exercise_result)}, ${escapeStr(r.source_synchronizer)}, ${escapeStr(r.target_synchronizer)}, ${escapeStr(r.unassign_id)}, ${escapeStr(r.submitter)}, ${r.reassignment_counter ?? 'NULL'}, ${escapeStr(r.raw_event)})`;
        }
      });
      
      await runExec(`INSERT INTO ${tableName} VALUES ${valueRows.join(', ')}`);
    }
    
    // Export to Parquet
    await runExec(`COPY ${tableName} TO '${normalizedFilePath}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)`);
    
    // Drop temp table
    await runExec(`DROP TABLE ${tableName}`);
    
    // Verify file was created
    if (!existsSync(filePath)) {
      throw new Error(`DuckDB completed but parquet file not created: ${filePath}`);
    }
    
    totalFilesWritten++;
    console.log(`📝 Wrote ${records.length} ${type} to ${filePath}`);
    
    return { file: filePath, count: records.length };
  } catch (err) {
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
