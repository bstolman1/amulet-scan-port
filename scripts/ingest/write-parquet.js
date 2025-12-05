/**
 * Parquet Writer Module - Optimized for High Throughput
 * 
 * Handles writing ledger data to partitioned parquet files.
 * Uses large buffers and async write queues for maximum performance.
 * 
 * Optimizations:
 * - Large buffer size (100K rows) for better compression
 * - Async write queue for parallel fetch + write
 * - Streaming writes to avoid memory pressure
 */

import { createWriteStream, mkdirSync, existsSync, readdirSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import { getPartitionPath, UPDATES_COLUMNS, EVENTS_COLUMNS } from './parquet-schema.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Configuration - OPTIMIZED for throughput
const DATA_DIR = process.env.DATA_DIR || join(__dirname, '../../data/raw');
const MAX_ROWS_PER_FILE = parseInt(process.env.MAX_ROWS_PER_FILE) || 100000; // 20x larger buffers
const COMPRESSION = process.env.PARQUET_COMPRESSION || 'zstd';

// In-memory buffers for batching
let updatesBuffer = [];
let eventsBuffer = [];

// Async write queue for parallel fetch + write
let writeQueue = [];
let isWriting = false;

/**
 * Ensure directory exists
 */
function ensureDir(dirPath) {
  if (!existsSync(dirPath)) {
    mkdirSync(dirPath, { recursive: true });
  }
}

/**
 * Get next file number for a partition
 */
function getNextFileNumber(partitionDir, prefix) {
  ensureDir(partitionDir);
  
  const files = readdirSync(partitionDir)
    .filter(f => f.startsWith(prefix) && f.endsWith('.jsonl'));
  
  if (files.length === 0) return 1;
  
  const numbers = files.map(f => {
    const match = f.match(/-(\d+)\.jsonl$/);
    return match ? parseInt(match[1]) : 0;
  });
  
  return Math.max(...numbers) + 1;
}

/**
 * Write rows to a JSON-lines file using streaming to avoid memory limits
 * Returns a promise that resolves when writing is complete
 */
function writeJsonLines(rows, filePath) {
  return new Promise((resolve, reject) => {
    const stream = createWriteStream(filePath, { 
      encoding: 'utf8',
      highWaterMark: 64 * 1024 // 64KB write buffer for better I/O
    });
    
    stream.on('error', reject);
    stream.on('finish', resolve);
    
    // Write in chunks for better memory efficiency
    const chunkSize = 1000;
    let i = 0;
    
    function writeChunk() {
      let ok = true;
      while (i < rows.length && ok) {
        const end = Math.min(i + chunkSize, rows.length);
        for (let j = i; j < end; j++) {
          ok = stream.write(JSON.stringify(rows[j]) + '\n');
        }
        i = end;
      }
      
      if (i < rows.length) {
        // Wait for drain before continuing
        stream.once('drain', writeChunk);
      } else {
        stream.end();
      }
    }
    
    writeChunk();
  });
}

/**
 * Process the async write queue
 * Runs writes in background while fetching continues
 */
async function processWriteQueue() {
  if (isWriting || writeQueue.length === 0) return;
  
  isWriting = true;
  
  while (writeQueue.length > 0) {
    const task = writeQueue.shift();
    try {
      await task.execute();
      console.log(`📝 Wrote ${task.count} ${task.type} to ${task.file}`);
    } catch (err) {
      console.error(`❌ Write error for ${task.file}:`, err.message);
      // Re-queue on failure
      writeQueue.unshift(task);
      await new Promise(r => setTimeout(r, 1000));
    }
  }
  
  isWriting = false;
}

/**
 * Queue a write task for async processing
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
 * Convert JSON-lines to parquet using DuckDB
 * Run this as a separate step after ingestion
 */
export function createConversionScript() {
  return `
-- DuckDB script to convert JSON-lines to Parquet
-- Run: duckdb -c ".read convert-to-parquet.sql"

-- Convert updates with optimized settings
COPY (
  SELECT * FROM read_json_auto('data/raw/**/updates-*.jsonl')
) TO 'data/raw/updates.parquet' (
  FORMAT PARQUET, 
  COMPRESSION ZSTD,
  ROW_GROUP_SIZE 100000
);

-- Convert events with optimized settings
COPY (
  SELECT * FROM read_json_auto('data/raw/**/events-*.jsonl')
) TO 'data/raw/events.parquet' (
  FORMAT PARQUET, 
  COMPRESSION ZSTD,
  ROW_GROUP_SIZE 100000
);
`;
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
  const partition = getPartitionPath(timestamp);
  const partitionDir = join(DATA_DIR, partition);
  
  ensureDir(partitionDir);
  
  const fileNum = getNextFileNumber(partitionDir, 'updates');
  const fileName = `updates-${String(fileNum).padStart(5, '0')}.jsonl`;
  const filePath = join(partitionDir, fileName);
  
  // Copy buffer and clear immediately (allows fetching to continue)
  const rowsToWrite = [...updatesBuffer];
  updatesBuffer = [];
  
  // Queue write for async processing
  return queueWrite(rowsToWrite, filePath, 'updates');
}

/**
 * Flush events buffer to file (async queued)
 */
export async function flushEvents() {
  if (eventsBuffer.length === 0) return null;
  
  const timestamp = eventsBuffer[0]?.timestamp || new Date();
  const partition = getPartitionPath(timestamp);
  const partitionDir = join(DATA_DIR, partition);
  
  ensureDir(partitionDir);
  
  const fileNum = getNextFileNumber(partitionDir, 'events');
  const fileName = `events-${String(fileNum).padStart(5, '0')}.jsonl`;
  const filePath = join(partitionDir, fileName);
  
  // Copy buffer and clear immediately (allows fetching to continue)
  const rowsToWrite = [...eventsBuffer];
  eventsBuffer = [];
  
  // Queue write for async processing
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
  while (writeQueue.length > 0 || isWriting) {
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
    isWriting,
  };
}

/**
 * Wait for write queue to drain (useful before shutdown)
 */
export async function waitForWrites() {
  while (writeQueue.length > 0 || isWriting) {
    await new Promise(r => setTimeout(r, 100));
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
};
