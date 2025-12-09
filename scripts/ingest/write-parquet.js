/**
 * Parquet Writer Module - TRUE STREAMING VERSION
 * 
 * Handles writing ledger data to partitioned parquet files.
 * 
 * Optimizations:
 * - TRUE STREAMING writes with small chunk sizes
 * - Incremental compression (compress small chunks, not entire file)
 * - Backpressure handling to prevent memory buildup
 * - Timestamp-based filenames (no directory scans)
 * - Row size limits to prevent memory explosion
 */

import { createWriteStream, mkdirSync, existsSync, rmSync, readdirSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import { randomBytes } from 'crypto';
import { createGzip } from 'zlib';
import { getPartitionPath } from './parquet-schema.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Configuration - CONSERVATIVE DEFAULTS to prevent OOM
const DATA_DIR = process.env.DATA_DIR || join(__dirname, '../../data/raw');
const MAX_ROWS_PER_FILE = parseInt(process.env.MAX_ROWS_PER_FILE) || 10000; // Much smaller default
const MAX_CONCURRENT_WRITES = parseInt(process.env.MAX_CONCURRENT_WRITES) || 4; // Much lower concurrency
const IO_BUFFER_SIZE = parseInt(process.env.IO_BUFFER_SIZE) || 256 * 1024; // 256KB
const GZIP_LEVEL = parseInt(process.env.GZIP_LEVEL) || 1; // 1=fastest

const MAX_ROW_SIZE_BYTES = 5 * 1024 * 1024; // 5MB max per row - skip larger ones
const MAX_RETRIES = 2;

// In-memory buffers for batching - use smaller thresholds
let updatesBuffer = [];
let eventsBuffer = [];
let currentMigrationId = null;

// Async write queue with controlled parallel execution
let writeQueue = [];
let activeWrites = 0;
let queueProcessing = false;

/**
 * Ensure directory exists
 */
function ensureDir(dirPath) {
  if (!existsSync(dirPath)) {
    mkdirSync(dirPath, { recursive: true });
  }
}

/**
 * Generate unique filename using timestamp + random suffix
 */
function generateFileName(prefix) {
  const ts = Date.now();
  const rand = randomBytes(4).toString('hex');
  return `${prefix}-${ts}-${rand}.jsonl.gz`;
}

/**
 * TRUE STREAMING write - writes rows incrementally with gzip compression
 * Never holds entire file in memory - uses Node's native streaming
 */
async function writeJsonLinesStreaming(rows, filePath) {
  return new Promise((resolve, reject) => {
    const gzip = createGzip({ 
      level: GZIP_LEVEL,
      chunkSize: IO_BUFFER_SIZE 
    });
    
    const output = createWriteStream(filePath, { 
      highWaterMark: IO_BUFFER_SIZE 
    });
    
    // Pipe gzip to file
    gzip.pipe(output);
    
    output.on('finish', resolve);
    output.on('error', reject);
    gzip.on('error', reject);
    
    let i = 0;
    
    function writeNext() {
      let ok = true;
      
      // Write rows until backpressure or done
      while (i < rows.length && ok) {
        const line = JSON.stringify(rows[i]) + '\n';
        i++;
        
        if (i === rows.length) {
          // Last row - end the stream
          gzip.end(line);
        } else {
          // More rows - check for backpressure
          ok = gzip.write(line);
        }
      }
      
      if (i < rows.length) {
        // Backpressure - wait for drain
        gzip.once('drain', writeNext);
      }
    }
    
    // Start writing
    if (rows.length === 0) {
      gzip.end();
    } else {
      writeNext();
    }
  });
}

/**
 * Process the async write queue with controlled parallel execution
 */
async function processWriteQueue() {
  if (queueProcessing) return;
  queueProcessing = true;
  
  while (writeQueue.length > 0 && activeWrites < MAX_CONCURRENT_WRITES) {
    const task = writeQueue.shift();
    activeWrites++;
    
    // Execute write (parallel but controlled)
    executeWrite(task).finally(() => {
      activeWrites--;
      // Continue processing if more tasks
      if (writeQueue.length > 0) {
        setImmediate(() => processWriteQueue());
      }
    });
  }
  
  queueProcessing = false;
}

/**
 * Execute a single write task with limited retries
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
 * Filter out oversized rows to prevent memory issues
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
    console.warn(`⚠️ Skipped ${skipped} oversized ${type} (>${MAX_ROW_SIZE_BYTES / 1024 / 1024}MB each)`);
  }
  
  return filtered;
}

/**
 * Queue a write task - blocks if queue is too full (backpressure)
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
    execute: () => writeJsonLinesStreaming(safeRows, filePath)
  };
  
  writeQueue.push(task);
  
  // Start processing (non-blocking)
  setImmediate(() => processWriteQueue());
  
  return { file: filePath, count: safeRows.length, queued: true };
}

/**
 * Add updates to buffer - flushes immediately at smaller threshold
 */
export async function bufferUpdates(updates) {
  updatesBuffer.push(...updates);
  
  if (updatesBuffer.length >= MAX_ROWS_PER_FILE) {
    return await flushUpdates();
  }
  return null;
}

/**
 * Add events to buffer - flushes immediately at smaller threshold
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
  
  const timestamp = updatesBuffer[0]?.timestamp || new Date();
  const migrationId = currentMigrationId || updatesBuffer[0]?.migration_id || null;
  const partition = getPartitionPath(timestamp, migrationId);
  const partitionDir = join(DATA_DIR, partition);
  
  ensureDir(partitionDir);
  
  const fileName = generateFileName('updates');
  const filePath = join(partitionDir, fileName);
  
  // Swap buffer reference (faster than copy)
  const rowsToWrite = updatesBuffer;
  updatesBuffer = [];
  
  return queueWrite(rowsToWrite, filePath, 'updates');
}

/**
 * Flush events buffer to file
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
  
  // Swap buffer reference
  const rowsToWrite = eventsBuffer;
  eventsBuffer = [];
  
  return queueWrite(rowsToWrite, filePath, 'events');
}

/**
 * Flush all buffers and wait for write queue to complete
 */
export async function flushAll() {
  const results = [];
  
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
 * Wait for write queue to drain
 */
export async function waitForWrites() {
  while (writeQueue.length > 0 || activeWrites > 0) {
    await new Promise(r => setTimeout(r, 100));
  }
}

/**
 * Convert JSON-lines to parquet using DuckDB
 */
export function createConversionScript() {
  return `
-- DuckDB script to convert JSON-lines to Parquet
-- Supports .jsonl.gz (gzip compressed) files

COPY (
  SELECT * FROM read_json_auto('data/raw/**/updates-*.jsonl.gz')
) TO 'data/raw/updates.parquet' (
  FORMAT PARQUET, 
  COMPRESSION ZSTD,
  ROW_GROUP_SIZE 100000
);

COPY (
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
 */
export function purgeMigrationData(migrationId) {
  const migrationPrefix = `migration=${migrationId}`;
  let deletedDirs = 0;
  
  if (!existsSync(DATA_DIR)) {
    console.log(`   ℹ️ Data directory doesn't exist, nothing to purge`);
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
  
  console.log(`   ✅ Purged migration ${migrationId}: ${deletedDirs} directories removed`);
  return { deletedFiles: 0, deletedDirs };
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
