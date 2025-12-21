/**
 * Binary Writer Module - PROTOBUF + ZSTD VERSION
 * 
 * Handles writing ledger data to partitioned binary files using:
 * - Protobuf encoding (no giant JSON.stringify)
 * - ZSTD compression in worker threads
 * - Each worker has its own heap (sidesteps 4GB limit)
 * 
 * Drop-in replacement for write-parquet.js with same API surface.
 */

import { mkdirSync, existsSync, rmSync, readdirSync } from 'fs';
import { join, dirname, sep, isAbsolute, resolve } from 'path';
import { fileURLToPath } from 'url';
import { randomBytes } from 'crypto';
import { getPartitionPath } from './parquet-schema.js';
import { getBinaryWriterPool, shutdownBinaryPool } from './binary-writer-pool.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const REPO_DATA_DIR = join(resolve(__dirname, '..', '..'), 'data');

// Configuration - Default paths checked in order
const WSL_DEFAULT = '/mnt/c/ledger_raw';
const WINDOWS_DEFAULT = 'C:\\\\ledger_raw';

function selectBaseDataDir() {
  if (process.env.DATA_DIR) {
    if (!isAbsolute(process.env.DATA_DIR)) {
      throw new Error(`[write-binary] DATA_DIR must be an absolute path (got: ${process.env.DATA_DIR})`);
    }
    return process.env.DATA_DIR;
  }

  if (existsSync(REPO_DATA_DIR)) return REPO_DATA_DIR;
  if (existsSync(WSL_DEFAULT)) return WSL_DEFAULT;
  if (existsSync(WINDOWS_DEFAULT)) return WINDOWS_DEFAULT;
  return WSL_DEFAULT; // Fallback for consistency
}

const BASE_DATA_DIR_RAW = selectBaseDataDir();

const BASE_DATA_DIR = resolve(BASE_DATA_DIR_RAW);
const DATA_DIR = join(BASE_DATA_DIR, 'raw'); // Binary files go in raw/ subdirectory
const MAX_ROWS_PER_FILE = parseInt(process.env.MAX_ROWS_PER_FILE) || 5000; // Reduced to flush more often
const ZSTD_LEVEL = parseInt(process.env.ZSTD_LEVEL) || 1;
const MAX_PENDING_WRITES = parseInt(process.env.MAX_PENDING_WRITES) || 50; // Limit concurrent writes

// In-memory buffers
let updatesBuffer = [];
let eventsBuffer = [];
let currentMigrationId = null;

// Pool instance
let writerPool = null;

// Track pending writes for drain - use a Set to avoid memory buildup
let pendingWrites = 0;
let writePromises = new Set();

/**
 * Initialize binary writer pool
 */
async function ensureWriterPool() {
  if (!writerPool) {
    writerPool = getBinaryWriterPool();
    await writerPool.init();
  }
  return writerPool;
}

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
 * Generate unique filename for binary protobuf files
 */
function generateFileName(prefix) {
  const ts = Date.now();
  const rand = randomBytes(4).toString('hex');
  return `${prefix}-${ts}-${rand}.pb.zst`;
}

/**
 * Map records to protobuf-compatible format
 * CRITICAL: These mappings must preserve ALL fields from the Scan API
 * NOTE: This is the ONLY mapping step - worker-writer.js uses these directly (no re-mapping)
 */
function mapUpdateRecord(r) {
  // Helper for safe timestamp conversion
  const safeTimestamp = (val) => {
    if (!val) return 0;
    if (typeof val === 'number') return val;
    try {
      const ts = new Date(val).getTime();
      return isNaN(ts) ? 0 : ts;
    } catch { return 0; }
  };
  
  // Helper for safe int64
  const safeInt64 = (val) => {
    if (val === null || val === undefined) return 0;
    const num = parseInt(val);
    return isNaN(num) ? 0 : num;
  };
  
  // Helper for safe string array
  const safeStringArray = (arr) => Array.isArray(arr) ? arr.map(String) : [];
  
  // Helper for safe JSON stringify
  const safeStringify = (obj) => {
    if (!obj) return '';
    try {
      return typeof obj === 'string' ? obj : JSON.stringify(obj);
    } catch { return ''; }
  };

  return {
    // Core identifiers
    id: String(r.update_id || r.id || ''),
    type: String(r.update_type || r.type || ''),
    synchronizer: String(r.synchronizer_id || r.synchronizer || ''),

    // Timestamps
    effectiveAt: safeTimestamp(r.effective_at),
    recordedAt: safeTimestamp(r.recorded_at || r.timestamp),
    recordTime: safeTimestamp(r.record_time),

    // Transaction metadata
    commandId: String(r.command_id || ''),
    workflowId: String(r.workflow_id || ''),
    kind: String(r.kind || ''),

    // Numeric fields
    migrationId: safeInt64(r.migration_id),
    offset: safeInt64(r.offset),
    eventCount: parseInt(r.event_count) || 0,

    // Event references (Scan API)
    // Some sources nest these under update_data
    rootEventIds: safeStringArray(
      r.root_event_ids || r.rootEventIds || r.update_data?.root_event_ids || r.update_data?.rootEventIds,
    ),

    // Reassignment-specific fields
    sourceSynchronizer: String(r.source_synchronizer || ''),
    targetSynchronizer: String(r.target_synchronizer || ''),
    unassignId: String(r.unassign_id || ''),
    submitter: String(r.submitter || ''),
    reassignmentCounter: safeInt64(r.reassignment_counter),

    // CRITICAL: Full data preservation
    traceContextJson: r.trace_context_json || (r.trace_context ? safeStringify(r.trace_context) : ''),
    updateDataJson: r.update_data_json || (r.update_data ? safeStringify(r.update_data) : ''),
  };
}

function mapEventRecord(r) {
  // Helper for safe timestamp conversion
  const safeTimestamp = (val) => {
    if (!val) return 0;
    if (typeof val === 'number') return val;
    try {
      const ts = new Date(val).getTime();
      return isNaN(ts) ? 0 : ts;
    } catch { return 0; }
  };
  
  // Helper for safe int64
  const safeInt64 = (val) => {
    if (val === null || val === undefined) return 0;
    const num = parseInt(val);
    return isNaN(num) ? 0 : num;
  };
  
  // Helper for safe string array
  const safeStringArray = (arr) => Array.isArray(arr) ? arr.map(String) : [];
  
  // Helper for safe JSON stringify
  const safeStringify = (obj) => {
    if (!obj) return '';
    try {
      return typeof obj === 'string' ? obj : JSON.stringify(obj);
    } catch { return ''; }
  };

  return {
    // Core identifiers
    id: String(r.event_id || r.id || ''),
    updateId: String(r.update_id || ''),
    type: String(r.event_type || r.type || ''),
    // Preserve original API type if present; fall back to type to avoid empty string in protobuf
    typeOriginal: String(
      r.event_type_original || r.type_original || r.typeOriginal || r.type_original || r.event_type || r.type || '',
    ),
    synchronizer: String(r.synchronizer_id || r.synchronizer || ''),

    // Timestamps
    effectiveAt: safeTimestamp(r.effective_at),
    recordedAt: safeTimestamp(r.recorded_at || r.timestamp),
    createdAtTs: safeTimestamp(r.created_at_ts),

    // Contract info (Scan API fields can be nested per event-kind)
    contractId: String(
      r.contract_id ||
        r.contractId ||
        r.created?.contract_id ||
        r.created?.contractId ||
        r.exercised?.contract_id ||
        r.exercised?.contractId ||
        r.reassignment?.contract_id ||
        r.reassignment?.contractId ||
        '',
    ),
    template: String(r.template_id || r.template || ''),
    packageName: String(r.package_name || ''),
    migrationId: safeInt64(r.migration_id),

    // Parties
    signatories: safeStringArray(r.signatories),
    observers: safeStringArray(r.observers),
    actingParties: safeStringArray(r.acting_parties),
    witnessParties: safeStringArray(r.witness_parties),

    // Payload data
    payloadJson: r.payload_json || (r.payload ? safeStringify(r.payload) : ''),
    contractKeyJson: r.contract_key_json || (r.contract_key ? safeStringify(r.contract_key) : ''),

    // Exercise-specific fields
    choice: String(r.choice || ''),
    consuming: Boolean(r.consuming ?? false),
    interfaceId: String(r.interface_id || ''),
    childEventIds: safeStringArray(r.child_event_ids || r.childEventIds),
    exerciseResultJson: r.exercise_result_json || (r.exercise_result ? safeStringify(r.exercise_result) : ''),

    // Reassignment-specific fields
    sourceSynchronizer: String(r.source_synchronizer || ''),
    targetSynchronizer: String(r.target_synchronizer || ''),
    unassignId: String(r.unassign_id || ''),
    submitter: String(r.submitter || ''),
    reassignmentCounter: safeInt64(r.reassignment_counter),

    // CRITICAL: Complete original event for full data preservation
    rawJson: r.raw_json || (r.raw ? safeStringify(r.raw) : ''),

    // Deprecated field kept for backwards compatibility
    party: String(r.party || ''),
  };
}

/**
 * Queue a binary write job with backpressure
 */
async function queueBinaryWrite(records, filePath, type) {
  if (records.length === 0) {
    return null;
  }
  
  // Apply backpressure - wait if too many pending writes
  while (pendingWrites >= MAX_PENDING_WRITES) {
    await new Promise(resolve => setTimeout(resolve, 50));
  }
  
  const pool = await ensureWriterPool();
  
  // Map records to protobuf format - clear source array reference after mapping
  const mappedRecords = type === 'updates' 
    ? records.map(mapUpdateRecord)
    : records.map(mapEventRecord);
  
  // Clear the source records to free memory immediately
  records.length = 0;
  
  pendingWrites++;
  
  const promise = pool.writeJob({
    type,
    filePath,
    records: mappedRecords,
    zstdLevel: ZSTD_LEVEL,
  }).then(result => {
    pendingWrites--;
    writePromises.delete(promise); // Remove from tracking immediately
    const ratio = result.originalSize > 0 
      ? ((result.compressedSize / result.originalSize) * 100).toFixed(1)
      : 0;
    console.log(`📝 Wrote ${result.count} ${type} to ${filePath.split('/').pop()} (${ratio}% of original)`);
    return result;
  }).catch(err => {
    pendingWrites--;
    writePromises.delete(promise); // Remove from tracking on error too
    console.error(`❌ Binary write failed for ${filePath}:`, err.message);
    throw err;
  });
  
  writePromises.add(promise);
  
  return { file: filePath, count: records.length, queued: true };
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
 * Flush updates buffer to binary file
 */
export async function flushUpdates() {
  if (updatesBuffer.length === 0) return null;
  
  // CRITICAL: Use effective_at (data time) for partitioning, not timestamp (write time)
  // This ensures files are organized by when the data HAPPENED, not when we wrote it
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
  
  return queueBinaryWrite(rowsToWrite, filePath, 'updates');
}

/**
 * Flush events buffer to binary file
 */
export async function flushEvents() {
  if (eventsBuffer.length === 0) return null;
  
  // CRITICAL: Use effective_at (data time) for partitioning, not timestamp (write time)
  // This ensures files are organized by when the data HAPPENED, not when we wrote it
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
  
  return queueBinaryWrite(rowsToWrite, filePath, 'events');
}

/**
 * Flush all buffers and wait for writes to complete
 */
export async function flushAll() {
  const results = [];
  
  const updatesResult = await flushUpdates();
  if (updatesResult) results.push(updatesResult);
  
  const eventsResult = await flushEvents();
  if (eventsResult) results.push(eventsResult);
  
  // Wait for all pending writes
  await waitForWrites();
  
  return results;
}

/**
 * Get buffer and queue stats
 */
export function getBufferStats() {
  const defaultStats = { 
    activeWorkers: 0, 
    queuedJobs: 0, 
    completedJobs: 0,
    failedJobs: 0,
    totalRecords: 0,
    compressionRatio: '---',
    mbPerSec: '0.00',
    mbWritten: '0.00',
    recordsPerSec: 0,
  };
  
  const poolStats = writerPool ? writerPool.getStats() : defaultStats;
  
  return {
    updates: updatesBuffer.length,
    events: eventsBuffer.length,
    updatesBuffered: updatesBuffer.length,
    eventsBuffered: eventsBuffer.length,
    maxRowsPerFile: MAX_ROWS_PER_FILE,
    queuedWrites: poolStats.queuedJobs || 0,
    activeWrites: poolStats.activeWorkers || 0,
    pendingWrites,
    ...poolStats
  };
}

/**
 * Wait for write queue to drain
 */
export async function waitForWrites() {
  // Wait for all tracked promises
  if (writePromises.size > 0) {
    await Promise.allSettled([...writePromises]);
    writePromises.clear();
  }
  
  // Also drain the pool
  if (writerPool) {
    await writerPool.drain();
  }
}

/**
 * Shutdown writer pool (call before process exit)
 */
export async function shutdown() {
  await waitForWrites();
  await shutdownBinaryPool();
  writerPool = null;
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
