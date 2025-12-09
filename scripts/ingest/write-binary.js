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
import { join, dirname, sep } from 'path';
import { fileURLToPath } from 'url';
import { randomBytes } from 'crypto';
import { getPartitionPath } from './parquet-schema.js';
import { getBinaryWriterPool, shutdownBinaryPool } from './binary-writer-pool.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Configuration
const DATA_DIR = process.env.DATA_DIR || join(__dirname, '../../data/raw');
const MAX_ROWS_PER_FILE = parseInt(process.env.MAX_ROWS_PER_FILE) || 10000;
const ZSTD_LEVEL = parseInt(process.env.ZSTD_LEVEL) || 1;

// In-memory buffers
let updatesBuffer = [];
let eventsBuffer = [];
let currentMigrationId = null;

// Pool instance
let writerPool = null;

// Track pending writes for drain
let pendingWrites = 0;
let writePromises = [];

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
 */
function mapUpdateRecord(r) {
  return {
    id: r.update_id || r.id || '',
    synchronizer: r.synchronizer_id || r.synchronizer || '',
    effective_at: r.effective_at ? new Date(r.effective_at).getTime() : 0,
    recorded_at: r.recorded_at ? new Date(r.recorded_at).getTime() : (r.timestamp ? new Date(r.timestamp).getTime() : 0),
    transaction_id: r.transaction_id || '',
    command_id: r.command_id || '',
    workflow_id: r.workflow_id || '',
    status: r.status || '',
  };
}

function mapEventRecord(r) {
  return {
    id: r.event_id || r.id || '',
    update_id: r.update_id || '',
    type: r.event_type || r.type || '',
    synchronizer: r.synchronizer_id || r.synchronizer || '',
    effective_at: r.effective_at ? new Date(r.effective_at).getTime() : 0,
    recorded_at: r.recorded_at ? new Date(r.recorded_at).getTime() : (r.timestamp ? new Date(r.timestamp).getTime() : 0),
    contract_id: r.contract_id || '',
    party: r.party || '',
    template: r.template_id || r.template || '',
    payload_json: r.payload ? (typeof r.payload === 'string' ? r.payload : JSON.stringify(r.payload)) : '',
    signatories: r.signatories || [],
    observers: r.observers || [],
    package_name: r.package_name || '',
  };
}

/**
 * Queue a binary write job
 */
async function queueBinaryWrite(records, filePath, type) {
  if (records.length === 0) {
    return null;
  }
  
  const pool = await ensureWriterPool();
  
  // Map records to protobuf format
  const mappedRecords = type === 'updates' 
    ? records.map(mapUpdateRecord)
    : records.map(mapEventRecord);
  
  pendingWrites++;
  
  const promise = pool.writeJob({
    type,
    filePath,
    records: mappedRecords,
    zstdLevel: ZSTD_LEVEL,
  }).then(result => {
    pendingWrites--;
    const ratio = result.originalSize > 0 
      ? ((result.compressedSize / result.originalSize) * 100).toFixed(1)
      : 0;
    console.log(`📝 Wrote ${result.count} ${type} to ${filePath.split('/').pop()} (${ratio}% of original)`);
    return result;
  }).catch(err => {
    pendingWrites--;
    console.error(`❌ Binary write failed for ${filePath}:`, err.message);
    throw err;
  });
  
  writePromises.push(promise);
  
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
  
  const timestamp = updatesBuffer[0]?.timestamp || new Date();
  const migrationId = currentMigrationId || updatesBuffer[0]?.migration_id || null;
  const partition = getPartitionPath(timestamp, migrationId);
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
  
  const timestamp = eventsBuffer[0]?.timestamp || new Date();
  const migrationId = currentMigrationId || eventsBuffer[0]?.migration_id || null;
  const partition = getPartitionPath(timestamp, migrationId);
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
  if (writePromises.length > 0) {
    await Promise.allSettled(writePromises);
    writePromises = [];
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
