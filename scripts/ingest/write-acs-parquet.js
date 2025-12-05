/**
 * ACS Parquet Writer Module
 * 
 * Handles writing ACS snapshot data to partitioned files.
 * Uses streaming writes to avoid memory limits.
 */

import { createWriteStream, mkdirSync, existsSync, readdirSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import { getACSPartitionPath, ACS_COLUMNS } from './acs-schema.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Configuration - use absolute path relative to project root
const DATA_DIR = process.env.DATA_DIR || join(__dirname, '../../data/raw');
const MAX_ROWS_PER_FILE = parseInt(process.env.ACS_MAX_ROWS_PER_FILE) || 10000;

// In-memory buffer for batching
let contractsBuffer = [];
let currentSnapshotTime = null;

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
 * Write rows to a JSON-lines file using streaming
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
 * Set current snapshot time (for partitioning)
 */
export function setSnapshotTime(time) {
  currentSnapshotTime = time;
}

/**
 * Add contracts to buffer
 */
export async function bufferContracts(contracts) {
  contractsBuffer.push(...contracts);
  
  if (contractsBuffer.length >= MAX_ROWS_PER_FILE) {
    return await flushContracts();
  }
  return null;
}

/**
 * Flush contracts buffer to file
 */
export async function flushContracts() {
  if (contractsBuffer.length === 0) return null;
  
  const timestamp = currentSnapshotTime || contractsBuffer[0]?.snapshot_time || new Date();
  const partition = getACSPartitionPath(timestamp);
  const partitionDir = join(DATA_DIR, partition);
  
  ensureDir(partitionDir);
  
  const fileNum = getNextFileNumber(partitionDir, 'contracts');
  const fileName = `contracts-${String(fileNum).padStart(5, '0')}.jsonl`;
  const filePath = join(partitionDir, fileName);
  
  await writeJsonLines(contractsBuffer, filePath);
  
  const count = contractsBuffer.length;
  contractsBuffer = [];
  
  console.log(`📝 Wrote ${count} contracts to ${filePath}`);
  return { file: filePath, count };
}

/**
 * Flush all buffers
 */
export async function flushAll() {
  const results = [];
  
  const contractsResult = await flushContracts();
  if (contractsResult) results.push(contractsResult);
  
  return results;
}

/**
 * Get buffer stats
 */
export function getBufferStats() {
  return {
    contracts: contractsBuffer.length,
    maxRowsPerFile: MAX_ROWS_PER_FILE,
  };
}

/**
 * Clear buffers (for new snapshot)
 */
export function clearBuffers() {
  contractsBuffer = [];
  currentSnapshotTime = null;
}

export default {
  setSnapshotTime,
  bufferContracts,
  flushContracts,
  flushAll,
  getBufferStats,
  clearBuffers,
};
