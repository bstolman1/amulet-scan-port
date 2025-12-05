/**
 * Parquet Writer Module
 * 
 * Handles writing ledger data to partitioned parquet files.
 * Uses streaming writes to avoid memory limits with large datasets.
 */

import { createWriteStream, mkdirSync, existsSync, readdirSync } from 'fs';
import { join } from 'path';
import { getPartitionPath, UPDATES_COLUMNS, EVENTS_COLUMNS } from './parquet-schema.js';

// Configuration
const DATA_DIR = process.env.DATA_DIR || './data/raw';
const MAX_ROWS_PER_FILE = parseInt(process.env.MAX_ROWS_PER_FILE) || 5000;
const COMPRESSION = process.env.PARQUET_COMPRESSION || 'snappy';

// In-memory buffers for batching
let updatesBuffer = [];
let eventsBuffer = [];

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
 */
function writeJsonLines(rows, filePath) {
  return new Promise((resolve, reject) => {
    const stream = createWriteStream(filePath, { encoding: 'utf8' });
    
    stream.on('error', reject);
    stream.on('finish', resolve);
    
    for (const row of rows) {
      stream.write(JSON.stringify(row) + '\n');
    }
    
    stream.end();
  });
}

/**
 * Convert JSON-lines to parquet using DuckDB
 * Run this as a separate step after ingestion
 */
export function createConversionScript() {
  return `
-- DuckDB script to convert JSON-lines to Parquet
-- Run: duckdb -c ".read convert-to-parquet.sql"

-- Convert updates
COPY (
  SELECT * FROM read_json_auto('data/raw/**/updates-*.jsonl')
) TO 'data/raw/updates.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

-- Convert events  
COPY (
  SELECT * FROM read_json_auto('data/raw/**/events-*.jsonl')
) TO 'data/raw/events.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);
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
 * Flush updates buffer to file
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
  
  await writeJsonLines(updatesBuffer, filePath);
  
  const count = updatesBuffer.length;
  updatesBuffer = [];
  
  console.log(`📝 Wrote ${count} updates to ${filePath}`);
  return { file: filePath, count };
}

/**
 * Flush events buffer to file
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
  
  await writeJsonLines(eventsBuffer, filePath);
  
  const count = eventsBuffer.length;
  eventsBuffer = [];
  
  console.log(`📝 Wrote ${count} events to ${filePath}`);
  return { file: filePath, count };
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
 * Get buffer stats
 */
export function getBufferStats() {
  return {
    updates: updatesBuffer.length,
    events: eventsBuffer.length,
    maxRowsPerFile: MAX_ROWS_PER_FILE,
  };
}

export default {
  bufferUpdates,
  bufferEvents,
  flushUpdates,
  flushEvents,
  flushAll,
  getBufferStats,
  createConversionScript,
};
