/**
 * Parquet Writer Module - OPTIMIZED for Maximum Throughput
 * 
 * Handles writing ledger data to partitioned parquet files.
 * 
 * Optimizations:
 * - Timestamp-based filenames (no directory scans)
 * - Batch JSON serialization with join()
 * - Parallel concurrent writes (configurable)
 * - Buffer swap instead of copy
 * - Large I/O buffers (256KB)
 */

import { createWriteStream, mkdirSync, existsSync, rmSync, readdirSync } from 'fs';
import { createGzip } from 'zlib';
import { pipeline } from 'stream/promises';
import { Readable } from 'stream';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import { randomBytes } from 'crypto';
import { getPartitionPath } from './parquet-schema.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Configuration
const DATA_DIR = process.env.DATA_DIR || join(__dirname, '../../data/raw');
const MAX_ROWS_PER_FILE = parseInt(process.env.MAX_ROWS_PER_FILE) || 10000;
const MAX_CONCURRENT_WRITES = parseInt(process.env.MAX_CONCURRENT_WRITES) || 3;
const IO_BUFFER_SIZE = 256 * 1024; // 256KB for better disk throughput
const USE_GZIP = process.env.DISABLE_GZIP !== 'true'; // Gzip enabled by default

// In-memory buffers for batching
let updatesBuffer = [];
let eventsBuffer = [];
let currentMigrationId = null;

// Async write queue with parallel execution
let writeQueue = [];
let activeWrites = 0;

/**
 * Ensure directory exists
 */
function ensureDir(dirPath) {
  if (!existsSync(dirPath)) {
    mkdirSync(dirPath, { recursive: true });
  }
}

/**
 * Generate unique filename using timestamp + random suffix (no directory scan needed)
 */
function generateFileName(prefix) {
  const ts = Date.now();
  const rand = randomBytes(4).toString('hex');
  const ext = USE_GZIP ? '.jsonl.gz' : '.jsonl';
  return `${prefix}-${ts}-${rand}${ext}`;
}

/**
 * Write rows to JSON-lines file using streaming with batch serialization
 * Optionally compresses with gzip for ~5-10x smaller files
 * Returns a promise that resolves when writing is complete
 */
async function writeJsonLines(rows, filePath) {
  // Batch serialize all rows
  const chunkSize = 2000;
  const chunks = [];
  
  for (let i = 0; i < rows.length; i += chunkSize) {
    const end = Math.min(i + chunkSize, rows.length);
    chunks.push(rows.slice(i, end).map(r => JSON.stringify(r)).join('\n') + '\n');
  }
  
  const content = chunks.join('');
  
  if (USE_GZIP) {
    // Gzip compressed write
    const readable = Readable.from([content]);
    const gzip = createGzip({ level: 6 }); // Level 6 is good balance of speed/compression
    const writable = createWriteStream(filePath, { highWaterMark: IO_BUFFER_SIZE });
    
    await pipeline(readable, gzip, writable);
  } else {
    // Plain text write
    return new Promise((resolve, reject) => {
      const stream = createWriteStream(filePath, { 
        encoding: 'utf8',
        highWaterMark: IO_BUFFER_SIZE
      });
      
      stream.on('error', reject);
      stream.on('finish', resolve);
      stream.write(content);
      stream.end();
    });
  }
}

/**
 * Process the async write queue with parallel execution
 */
async function processWriteQueue() {
  // Start multiple writes concurrently up to MAX_CONCURRENT_WRITES
  while (writeQueue.length > 0 && activeWrites < MAX_CONCURRENT_WRITES) {
    const task = writeQueue.shift();
    activeWrites++;
    
    // Execute write without awaiting (parallel)
    executeWrite(task).finally(() => {
      activeWrites--;
      // Trigger next batch when a write completes
      setImmediate(() => processWriteQueue());
    });
  }
}

/**
 * Execute a single write task
 */
async function executeWrite(task) {
  try {
    await task.execute();
    console.log(`📝 Wrote ${task.count} ${task.type} to ${task.file}`);
  } catch (err) {
    console.error(`❌ Write error for ${task.file}:`, err.message);
    // Re-queue on failure with delay
    await new Promise(r => setTimeout(r, 1000));
    writeQueue.unshift(task);
  }
}

/**
 * Queue a write task for async parallel processing
 */
function queueWrite(rows, filePath, type) {
  const task = {
    type,
    file: filePath,
    count: rows.length,
    execute: () => writeJsonLines(rows, filePath)
  };
  
  writeQueue.push(task);
  
  // Start processing queue in background (non-blocking)
  setImmediate(() => processWriteQueue());
  
  return { file: filePath, count: rows.length, queued: true };
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
 * Flush updates buffer to file (async queued)
 */
export async function flushUpdates() {
  if (updatesBuffer.length === 0) return null;
  
  const timestamp = updatesBuffer[0]?.timestamp || new Date();
  const migrationId = currentMigrationId || updatesBuffer[0]?.migration_id || null;
  const partition = getPartitionPath(timestamp, migrationId);
  const partitionDir = join(DATA_DIR, partition);
  
  ensureDir(partitionDir);
  
  // Use timestamp-based filename (no directory scan)
  const fileName = generateFileName('updates');
  const filePath = join(partitionDir, fileName);
  
  // Swap buffer reference instead of copying (faster)
  const rowsToWrite = updatesBuffer;
  updatesBuffer = [];
  
  // Queue write for async parallel processing
  return queueWrite(rowsToWrite, filePath, 'updates');
}

/**
 * Flush events buffer to file (async queued)
 */
export async function flushEvents() {
  if (eventsBuffer.length === 0) return null;
  
  const timestamp = eventsBuffer[0]?.timestamp || new Date();
  const migrationId = currentMigrationId || eventsBuffer[0]?.migration_id || null;
  const partition = getPartitionPath(timestamp, migrationId);
  const partitionDir = join(DATA_DIR, partition);
  
  ensureDir(partitionDir);
  
  // Use timestamp-based filename (no directory scan)
  const fileName = generateFileName('events');
  const filePath = join(partitionDir, fileName);
  
  // Swap buffer reference instead of copying (faster)
  const rowsToWrite = eventsBuffer;
  eventsBuffer = [];
  
  // Queue write for async parallel processing
  return queueWrite(rowsToWrite, filePath, 'events');
}

/**
 * Flush all buffers and wait for write queue to complete
 */
export async function flushAll() {
  const results = [];
  
  // Flush any remaining buffered data
  const updatesResult = await flushUpdates();
  if (updatesResult) results.push(updatesResult);
  
  const eventsResult = await flushEvents();
  if (eventsResult) results.push(eventsResult);
  
  // Wait for all queued writes to complete
  while (writeQueue.length > 0 || activeWrites > 0) {
    await new Promise(r => setTimeout(r, 100));
  }
  
  return results;
}

/**
 * Get buffer and queue stats
 */
export function getBufferStats() {
  return {
    updates: updatesBuffer.length,
    events: eventsBuffer.length,
    maxRowsPerFile: MAX_ROWS_PER_FILE,
    queuedWrites: writeQueue.length,
    activeWrites,
    maxConcurrentWrites: MAX_CONCURRENT_WRITES,
  };
}

/**
 * Wait for write queue to drain (useful before shutdown)
 */
export async function waitForWrites() {
  while (writeQueue.length > 0 || activeWrites > 0) {
    await new Promise(r => setTimeout(r, 100));
  }
}

/**
 * Convert JSON-lines to parquet using DuckDB
 * Run this as a separate step after ingestion
 */
export function createConversionScript() {
  return `
-- DuckDB script to convert JSON-lines to Parquet
-- Run: duckdb -c ".read convert-to-parquet.sql"
-- Supports both .jsonl and .jsonl.gz files (uses UNION for Windows compatibility)

-- Convert updates with optimized settings
COPY (
  SELECT * FROM read_json_auto('data/raw/**/updates-*.jsonl')
  UNION ALL
  SELECT * FROM read_json_auto('data/raw/**/updates-*.jsonl.gz')
) TO 'data/raw/updates.parquet' (
  FORMAT PARQUET, 
  COMPRESSION ZSTD,
  ROW_GROUP_SIZE 100000
);

-- Convert events with optimized settings
COPY (
  SELECT * FROM read_json_auto('data/raw/**/events-*.jsonl')
  UNION ALL
  SELECT * FROM read_json_auto('data/raw/**/events-*.jsonl.gz')
) TO 'data/raw/events.parquet' (
  FORMAT PARQUET, 
  COMPRESSION ZSTD,
  ROW_GROUP_SIZE 100000
);
`;
}

/**
 * Set current migration ID for partitioning
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
 * Purge all data files for a specific migration
 * Call this after migration data has been processed/uploaded to free disk space
 */
export function purgeMigrationData(migrationId) {
  const migrationPrefix = `migration_id=${migrationId}`;
  let deletedFiles = 0;
  let deletedDirs = 0;
  
  if (!existsSync(DATA_DIR)) {
    console.log(`   ℹ️ Data directory doesn't exist, nothing to purge`);
    return { deletedFiles: 0, deletedDirs: 0 };
  }
  
  // Find and delete migration-specific partition directories
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
  
  console.log(`   ✅ Purged migration ${migrationId}: ${deletedDirs} directories removed`);
  return { deletedFiles, deletedDirs };
}

/**
 * Purge all data in the raw directory
 */
export function purgeAllData() {
  if (!existsSync(DATA_DIR)) {
    console.log(`   ℹ️ Data directory doesn't exist, nothing to purge`);
    return;
  }
  
  try {
    rmSync(DATA_DIR, { recursive: true, force: true });
    mkdirSync(DATA_DIR, { recursive: true });
    console.log(`   ✅ Purged all data from ${DATA_DIR}`);
  } catch (err) {
    console.error(`   ❌ Failed to purge data: ${err.message}`);
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
  createConversionScript,
  setMigrationId,
  clearMigrationId,
  purgeMigrationData,
  purgeAllData,
};
