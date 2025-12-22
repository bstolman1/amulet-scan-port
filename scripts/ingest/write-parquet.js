/**
 * Parquet Writer Module - WORKER THREAD VERSION
 * 
 * Handles writing ledger data to partitioned parquet files.
 * 
 * Optimizations:
 * - Worker thread pool for CPU-intensive compression
 * - Main thread stays responsive (non-blocking)
 * - Backpressure handling to prevent memory buildup
 * - Timestamp-based filenames (no directory scans)
 * - Row size limits to prevent memory explosion
 */

import { createWriteStream, mkdirSync, existsSync, rmSync, readdirSync } from 'fs';
import { writeFile } from 'fs/promises';
import { join, dirname, sep } from 'path';
import { fileURLToPath } from 'url';
import { randomBytes } from 'crypto';
import { getPartitionPath } from './parquet-schema.js';
import { getWorkerPool, shutdownPool } from './worker-pool.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Configuration - Default Windows path: C:/ledger_raw/raw
const WIN_DEFAULT = 'C:/ledger_raw/raw';
const DATA_DIR = process.env.DATA_DIR ? join(process.env.DATA_DIR, 'raw') : WIN_DEFAULT;
const MAX_ROWS_PER_FILE = parseInt(process.env.MAX_ROWS_PER_FILE) || 10000;
const MAX_CONCURRENT_WRITES = parseInt(process.env.MAX_CONCURRENT_WRITES) || 4;
const GZIP_LEVEL = parseInt(process.env.GZIP_LEVEL) || 1;
const WORKER_POOL_SIZE = parseInt(process.env.WORKER_POOL_SIZE) || parseInt(process.env.MAX_CONCURRENT_WRITES) || 0; // 0 = auto (CPU cores - 1)

const MAX_ROW_SIZE_BYTES = 5 * 1024 * 1024; // 5MB max per row
const MAX_RETRIES = 2;

// In-memory buffers
let updatesBuffer = [];
let eventsBuffer = [];
let currentMigrationId = null;

// Write queue with controlled parallel execution
let writeQueue = [];
let activeWrites = 0;
let queueProcessing = false;

// Worker pool instance
let workerPool = null;

/**
 * Initialize worker pool
 */
async function ensureWorkerPool() {
  if (!workerPool) {
    workerPool = getWorkerPool(WORKER_POOL_SIZE || undefined);
    await workerPool.init();
  }
  return workerPool;
}

/**
 * Ensure directory exists (Windows-safe with race condition handling)
 */
function ensureDir(dirPath) {
  // Normalize path separators for cross-platform compatibility
  const normalizedPath = dirPath.split('/').join(sep);
  try {
    if (!existsSync(normalizedPath)) {
      mkdirSync(normalizedPath, { recursive: true });
    }
  } catch (err) {
    // Handle race condition where directory was created between check and mkdir
    if (err.code !== 'EEXIST') {
      // Try creating parent directories explicitly for Windows edge cases
      const parts = normalizedPath.split(sep).filter(Boolean);
      let current = parts[0].includes(':') ? parts[0] + sep : sep; // Handle Windows drive letters
      
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
 * Generate unique filename (now using .zst for ZSTD compression)
 */
function generateFileName(prefix) {
  const ts = Date.now();
  const rand = randomBytes(4).toString('hex');
  return `${prefix}-${ts}-${rand}.jsonl.zst`;
}

/**
 * Write compressed data using worker thread pool
 * Compression happens off main thread!
 */
async function writeWithWorker(rows, filePath) {
  const pool = await ensureWorkerPool();
  
  // Compress in worker thread (non-blocking!)
  const { data: compressed } = await pool.compress(rows, GZIP_LEVEL);
  
  // Write compressed buffer to file
  await writeFile(filePath, compressed);
}

/**
 * Process the async write queue
 */
async function processWriteQueue() {
  if (queueProcessing) return;
  queueProcessing = true;
  
  while (writeQueue.length > 0 && activeWrites < MAX_CONCURRENT_WRITES) {
    const task = writeQueue.shift();
    activeWrites++;
    
    executeWrite(task).finally(() => {
      activeWrites--;
      if (writeQueue.length > 0) {
        setImmediate(() => processWriteQueue());
      }
    });
  }
  
  queueProcessing = false;
}

/**
 * Execute a single write task with retries
 */
async function executeWrite(task) {
  try {
    await task.execute();
    console.log(`📝 Wrote ${task.count} ${task.type} to ${task.file}`);
  } catch (err) {
    task.retries = (task.retries || 0) + 1;
    
    if (task.retries >= MAX_RETRIES) {
      console.error(`❌ Write failed for ${task.file} after ${MAX_RETRIES} retries:`, err.message);
      return;
    }
    
    console.error(`⚠️ Write error for ${task.file} (retry ${task.retries}/${MAX_RETRIES}):`, err.message);
    const delay = 1000 * Math.pow(2, task.retries - 1);
    await new Promise(r => setTimeout(r, delay));
    writeQueue.unshift(task);
  }
}

/**
 * Filter out oversized rows
 */
function filterOversizedRows(rows, type) {
  const filtered = [];
  let skipped = 0;
  
  for (const row of rows) {
    try {
      const size = Buffer.byteLength(JSON.stringify(row));
      if (size > MAX_ROW_SIZE_BYTES) {
        skipped++;
      } else {
        filtered.push(row);
      }
    } catch {
      skipped++;
    }
  }
  
  if (skipped > 0) {
    console.warn(`⚠️ Skipped ${skipped} oversized ${type}`);
  }
  
  return filtered;
}

/**
 * Queue a write task with backpressure
 */
async function queueWrite(rows, filePath, type) {
  const safeRows = filterOversizedRows(rows, type);
  
  if (safeRows.length === 0) {
    console.log(`⚠️ No ${type} to write after filtering`);
    return null;
  }
  
  // BACKPRESSURE: Wait if queue is too full
  const MAX_QUEUE_SIZE = MAX_CONCURRENT_WRITES * 2;
  while (writeQueue.length >= MAX_QUEUE_SIZE) {
    await new Promise(r => setTimeout(r, 50));
  }
  
  const task = {
    type,
    file: filePath,
    count: safeRows.length,
    retries: 0,
    execute: () => writeWithWorker(safeRows, filePath)
  };
  
  writeQueue.push(task);
  setImmediate(() => processWriteQueue());
  
  return { file: filePath, count: safeRows.length, queued: true };
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
 * Flush updates buffer to file
 */
export async function flushUpdates() {
  if (updatesBuffer.length === 0) return null;
  
  // CRITICAL: Use effective_at (data time) for partitioning, not timestamp (write time)
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
  
  return queueWrite(rowsToWrite, filePath, 'updates');
}

/**
 * Flush events buffer to file
 */
export async function flushEvents() {
  if (eventsBuffer.length === 0) return null;
  
  // CRITICAL: Use effective_at (data time) for partitioning, not timestamp (write time)
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
  
  return queueWrite(rowsToWrite, filePath, 'events');
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
  
  // Wait for all queued writes
  while (writeQueue.length > 0 || activeWrites > 0) {
    await new Promise(r => setTimeout(r, 100));
  }
  
  // Also wait for worker pool to drain
  if (workerPool) {
    await workerPool.drain();
  }
  
  return results;
}

/**
 * Get buffer and queue stats
 */
export function getBufferStats() {
  const poolStats = workerPool ? workerPool.getStats() : { totalWorkers: 0, busyWorkers: 0 };
  
  return {
    updates: updatesBuffer.length,
    events: eventsBuffer.length,
    maxRowsPerFile: MAX_ROWS_PER_FILE,
    queuedWrites: writeQueue.length,
    activeWrites,
    maxConcurrentWrites: MAX_CONCURRENT_WRITES,
    workerPool: poolStats
  };
}

/**
 * Wait for write queue to drain
 */
export async function waitForWrites() {
  while (writeQueue.length > 0 || activeWrites > 0) {
    await new Promise(r => setTimeout(r, 100));
  }
  
  if (workerPool) {
    await workerPool.drain();
  }
}

/**
 * Shutdown worker pool (call before process exit)
 */
export async function shutdown() {
  await waitForWrites();
  await shutdownPool();
  workerPool = null;
}

/**
 * Convert JSON-lines to parquet using DuckDB
 */
export function createConversionScript() {
  return `
-- DuckDB script to convert JSON-lines to Parquet
-- Note: .zst files are ZSTD compressed

COPY (
  SELECT * FROM read_json_auto('data/raw/**/updates-*.jsonl.zst')
) TO 'data/raw/updates.parquet' (
  FORMAT PARQUET, 
  COMPRESSION ZSTD,
  ROW_GROUP_SIZE 100000
);

COPY (
  SELECT * FROM read_json_auto('data/raw/**/events-*.jsonl.zst')
) TO 'data/raw/events.parquet' (
  FORMAT PARQUET, 
  COMPRESSION ZSTD,
  ROW_GROUP_SIZE 100000
);
`;
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
  createConversionScript,
  setMigrationId,
  clearMigrationId,
  purgeMigrationData,
  purgeAllData,
};
