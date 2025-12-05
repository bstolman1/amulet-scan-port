/**
 * Parquet Writer Module
 * 
 * Handles writing ledger data to partitioned parquet files.
 * Uses parquet-wasm for Node.js parquet generation.
 */

import { writeFileSync, mkdirSync, existsSync, readdirSync } from 'fs';
import { join } from 'path';
import { getPartitionPath, UPDATES_COLUMNS, EVENTS_COLUMNS } from './parquet-schema.js';

// Configuration
const DATA_DIR = process.env.DATA_DIR || './data/raw';
const MAX_ROWS_PER_FILE = parseInt(process.env.MAX_ROWS_PER_FILE) || 25000;
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
    .filter(f => f.startsWith(prefix) && f.endsWith('.parquet'));
  
  if (files.length === 0) return 1;
  
  const numbers = files.map(f => {
    const match = f.match(/-(\d+)\.parquet$/);
    return match ? parseInt(match[1]) : 0;
  });
  
  return Math.max(...numbers) + 1;
}

/**
 * Write rows to a JSON-lines file (for later parquet conversion)
 * This is a simpler approach that can be converted to parquet via DuckDB
 */
function writeJsonLines(rows, filePath) {
  const content = rows.map(r => JSON.stringify(r)).join('\n');
  writeFileSync(filePath, content);
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
export function bufferUpdates(updates) {
  updatesBuffer.push(...updates);
  
  if (updatesBuffer.length >= MAX_ROWS_PER_FILE) {
    return flushUpdates();
  }
  return null;
}

/**
 * Add events to buffer
 */
export function bufferEvents(events) {
  eventsBuffer.push(...events);
  
  if (eventsBuffer.length >= MAX_ROWS_PER_FILE) {
    return flushEvents();
  }
  return null;
}

/**
 * Flush updates buffer to file
 */
export function flushUpdates() {
  if (updatesBuffer.length === 0) return null;
  
  const timestamp = updatesBuffer[0]?.timestamp || new Date();
  const partition = getPartitionPath(timestamp);
  const partitionDir = join(DATA_DIR, partition);
  
  ensureDir(partitionDir);
  
  const fileNum = getNextFileNumber(partitionDir, 'updates');
  const fileName = `updates-${String(fileNum).padStart(5, '0')}.jsonl`;
  const filePath = join(partitionDir, fileName);
  
  writeJsonLines(updatesBuffer, filePath);
  
  const count = updatesBuffer.length;
  updatesBuffer = [];
  
  console.log(`📝 Wrote ${count} updates to ${filePath}`);
  return { file: filePath, count };
}

/**
 * Flush events buffer to file
 */
export function flushEvents() {
  if (eventsBuffer.length === 0) return null;
  
  const timestamp = eventsBuffer[0]?.timestamp || new Date();
  const partition = getPartitionPath(timestamp);
  const partitionDir = join(DATA_DIR, partition);
  
  ensureDir(partitionDir);
  
  const fileNum = getNextFileNumber(partitionDir, 'events');
  const fileName = `events-${String(fileNum).padStart(5, '0')}.jsonl`;
  const filePath = join(partitionDir, fileName);
  
  writeJsonLines(eventsBuffer, filePath);
  
  const count = eventsBuffer.length;
  eventsBuffer = [];
  
  console.log(`📝 Wrote ${count} events to ${filePath}`);
  return { file: filePath, count };
}

/**
 * Flush all buffers
 */
export function flushAll() {
  const results = [];
  
  const updatesResult = flushUpdates();
  if (updatesResult) results.push(updatesResult);
  
  const eventsResult = flushEvents();
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
