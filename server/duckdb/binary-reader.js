/**
 * Binary Reader for Server
 * 
 * Decodes .pb.zst files (Protobuf + ZSTD with chunked format) for the DuckDB API.
 * Streams records from binary files with caching for efficient repeated queries.
 */

import fs from 'fs';
import path from 'path';
import protobuf from 'protobufjs';
import { decompress } from '@mongodb-js/zstd';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// Proto schema definition (inline to avoid file dependency)
const PROTO_SCHEMA = `
syntax = "proto3";
package ledger;

message Event {
  string id = 1;
  string update_id = 2;
  string type = 3;
  string synchronizer = 4;
  int64 effective_at = 5;
  int64 recorded_at = 6;
  string contract_id = 7;
  string party = 8;
  string template = 9;
  string payload_json = 10;
  repeated string signatories = 11;
  repeated string observers = 12;
  string package_name = 13;
  string raw_json = 14;
}

message Update {
  string id = 1;
  string synchronizer = 2;
  int64 effective_at = 3;
  int64 recorded_at = 4;
  string transaction_id = 5;
  string command_id = 6;
  string workflow_id = 7;
  string status = 8;
}

message EventBatch {
  repeated Event events = 1;
}

message UpdateBatch {
  repeated Update updates = 1;
}
`;

let rootPromise = null;

async function getRoot() {
  if (!rootPromise) {
    rootPromise = protobuf.parse(PROTO_SCHEMA).root;
  }
  return rootPromise;
}

async function getEncoders() {
  const root = await getRoot();
  const Event = root.lookupType('ledger.Event');
  const Update = root.lookupType('ledger.Update');
  const EventBatch = root.lookupType('ledger.EventBatch');
  const UpdateBatch = root.lookupType('ledger.UpdateBatch');
  return { Event, Update, EventBatch, UpdateBatch };
}

/**
 * Convert protobuf record to plain object with readable timestamps
 */
function toPlainObject(record, isEvent) {
  if (isEvent) {
    return {
      event_id: record.id || null,
      update_id: record.updateId || record.update_id || null,
      event_type: record.type || null,
      synchronizer_id: record.synchronizer || null,
      timestamp: record.recordedAt ? new Date(Number(record.recordedAt)).toISOString() : null,
      effective_at: record.effectiveAt ? new Date(Number(record.effectiveAt)).toISOString() : null,
      contract_id: record.contractId || record.contract_id || null,
      party: record.party || null,
      template_id: record.template || null,
      payload: record.payloadJson ? tryParseJson(record.payloadJson) : null,
      signatories: record.signatories || [],
      observers: record.observers || [],
      package_name: record.packageName || record.package_name || null,
      raw: record.rawJson ? tryParseJson(record.rawJson) : null, // Complete original event
    };
  }
  
  // Update record
  return {
    update_id: record.id || null,
    synchronizer_id: record.synchronizer || null,
    timestamp: record.recordedAt ? new Date(Number(record.recordedAt)).toISOString() : null,
    effective_at: record.effectiveAt ? new Date(Number(record.effectiveAt)).toISOString() : null,
    transaction_id: record.transactionId || record.transaction_id || null,
    command_id: record.commandId || record.command_id || null,
    workflow_id: record.workflowId || record.workflow_id || null,
    status: record.status || null,
  };
}

function tryParseJson(str) {
  if (!str) return null;
  try {
    return JSON.parse(str);
  } catch {
    return str;
  }
}

/**
 * Extract timestamp from filename pattern: events-{timestamp}-{random}.pb.zst
 * This is the WRITE timestamp (when the file was created)
 */
function extractWriteTimestampFromPath(filePath) {
  const basename = path.basename(filePath);
  const match = basename.match(/(?:events|updates)-(\d+)-/);
  if (match) {
    return parseInt(match[1], 10);
  }
  // Fallback to file mtime
  try {
    return fs.statSync(filePath).mtimeMs;
  } catch {
    return 0;
  }
}

/**
 * Extract DATA date from partition path: .../year=YYYY/month=MM/day=DD/...
 * Returns epoch ms representing the DATA date, not write date
 */
function extractDataDateFromPath(filePath) {
  const yearMatch = filePath.match(/year=(\d{4})/);
  const monthMatch = filePath.match(/month=(\d{2})/);
  const dayMatch = filePath.match(/day=(\d{2})/);
  
  if (yearMatch && monthMatch && dayMatch) {
    const year = parseInt(yearMatch[1], 10);
    const month = parseInt(monthMatch[1], 10) - 1; // JS months are 0-indexed
    const day = parseInt(dayMatch[1], 10);
    return new Date(year, month, day).getTime();
  }
  
  // Fallback to write timestamp
  return extractWriteTimestampFromPath(filePath);
}

// Alias for backward compatibility
const extractTimestampFromPath = extractWriteTimestampFromPath;

/**
 * Read and decode a single .pb.zst file
 */
export async function readBinaryFile(filePath) {
  const { EventBatch, UpdateBatch } = await getEncoders();
  
  const basename = path.basename(filePath);
  const isEvents = basename.startsWith('events-');
  const isUpdates = basename.startsWith('updates-');
  
  if (!isEvents && !isUpdates) {
    throw new Error(`Cannot determine type from filename: ${basename}`);
  }
  
  const BatchType = isEvents ? EventBatch : UpdateBatch;
  const recordKey = isEvents ? 'events' : 'updates';
  
  const fileBuffer = fs.readFileSync(filePath);
  const allRecords = [];
  let offset = 0;
  
  // Read chunks: [4-byte length][compressed data]...
  while (offset < fileBuffer.length) {
    if (offset + 4 > fileBuffer.length) break;
    
    const chunkLength = fileBuffer.readUInt32BE(offset);
    offset += 4;
    
    if (offset + chunkLength > fileBuffer.length) break;
    
    const compressedChunk = fileBuffer.subarray(offset, offset + chunkLength);
    offset += chunkLength;
    
    // Decompress chunk
    const decompressed = await decompress(compressedChunk);
    
    // Decode protobuf
    const message = BatchType.decode(decompressed);
    const records = message[recordKey] || [];
    
    for (const r of records) {
      allRecords.push(toPlainObject(r, isEvents));
    }
  }
  
  return {
    type: recordKey,
    count: allRecords.length,
    records: allRecords
  };
}

/**
 * Fast file finder that uses partition structure for recent data
 * Returns files sorted by data date (newest first) without scanning all 55k+ files
 */
export function findBinaryFilesFast(dirPath, type = 'events', options = {}) {
  const { maxDays = 7, maxFiles = 1000 } = options;
  const files = [];
  
  // Generate date paths for the last N days (most likely to have recent data)
  const today = new Date();
  const datePaths = [];
  
  for (let i = 0; i < maxDays; i++) {
    const d = new Date(today);
    d.setDate(d.getDate() - i);
    const year = d.getFullYear();
    const month = String(d.getMonth() + 1).padStart(2, '0');
    const day = String(d.getDate()).padStart(2, '0');
    datePaths.push({ year, month, day, dateMs: d.getTime() });
  }
  
  // Scan migration folders
  const migrations = [];
  try {
    const entries = fs.readdirSync(dirPath, { withFileTypes: true });
    for (const entry of entries) {
      if (entry.isDirectory() && entry.name.startsWith('migration=')) {
        migrations.push(path.join(dirPath, entry.name));
      }
    }
  } catch { }
  
  // For each date (newest first), scan for files
  for (const { year, month, day, dateMs } of datePaths) {
    for (const migrationDir of migrations) {
      const dayDir = path.join(migrationDir, `year=${year}`, `month=${month}`, `day=${day}`);
      try {
        if (fs.existsSync(dayDir)) {
          const entries = fs.readdirSync(dayDir, { withFileTypes: true });
          for (const entry of entries) {
            if (entry.isFile() && entry.name.startsWith(`${type}-`) && entry.name.endsWith('.pb.zst')) {
              files.push({
                path: path.join(dayDir, entry.name),
                dataDateMs: dateMs,
                writeTs: extractWriteTimestampFromPath(path.join(dayDir, entry.name))
              });
            }
          }
        }
      } catch { }
    }
    
    // Stop if we have enough files
    if (files.length >= maxFiles) break;
  }
  
  // Sort by data date desc, then write timestamp desc
  files.sort((a, b) => {
    if (a.dataDateMs !== b.dataDateMs) return b.dataDateMs - a.dataDateMs;
    return b.writeTs - a.writeTs;
  });
  
  return files.slice(0, maxFiles).map(f => f.path);
}

/**
 * Find all .pb.zst files in a directory (full scan - use sparingly)
 */
export function findBinaryFiles(dirPath, type = 'events') {
  const files = [];
  
  function scanDir(dir) {
    try {
      const entries = fs.readdirSync(dir, { withFileTypes: true });
      for (const entry of entries) {
        const fullPath = path.join(dir, entry.name);
        if (entry.isDirectory()) {
          scanDir(fullPath);
        } else if (entry.isFile() && entry.name.startsWith(`${type}-`) && entry.name.endsWith('.pb.zst')) {
          files.push(fullPath);
        }
      }
    } catch (err) {
      // Ignore read errors
    }
  }
  
  if (fs.existsSync(dirPath)) {
    scanDir(dirPath);
  }
  
  return files;
}

/**
 * Count total binary files without loading them all (fast)
 */
export function countBinaryFiles(dirPath, type = 'events') {
  let count = 0;
  
  function scanDir(dir) {
    try {
      const entries = fs.readdirSync(dir, { withFileTypes: true });
      for (const entry of entries) {
        const fullPath = path.join(dir, entry.name);
        if (entry.isDirectory()) {
          scanDir(fullPath);
        } else if (entry.isFile() && entry.name.startsWith(`${type}-`) && entry.name.endsWith('.pb.zst')) {
          count++;
        }
      }
    } catch { }
  }
  
  if (fs.existsSync(dirPath)) {
    scanDir(dirPath);
  }
  
  return count;
}

/**
 * Check if binary files exist for a type
 */
export function hasBinaryFiles(dirPath, type = 'events') {
  try {
    if (!fs.existsSync(dirPath)) return false;
    const stack = [dirPath];
    while (stack.length > 0) {
      const dir = stack.pop();
      const entries = fs.readdirSync(dir, { withFileTypes: true });
      for (const entry of entries) {
        if (entry.isDirectory()) {
          stack.push(path.join(dir, entry.name));
        } else if (entry.name.startsWith(`${type}-`) && entry.name.endsWith('.pb.zst')) {
          return true;
        }
      }
    }
    return false;
  } catch {
    return false;
  }
}

// Configurable scan limit via env var (default 500, up from 200)
const MAX_FILES_TO_SCAN = parseInt(process.env.BINARY_READER_MAX_FILES) || 500;

// Stream records with pagination (memory efficient for large datasets)
export async function streamRecords(dirPath, type = 'events', options = {}) {
  const {
    limit = 100,
    offset = 0,
    filter = null,
    sortBy = 'effective_at',
    maxDays = 30,
    maxFilesToScan: maxFilesToScanOverride,
  } = options;
  
  const maxFilesToScanLimit = Math.min(
    typeof maxFilesToScanOverride === 'number' ? maxFilesToScanOverride : MAX_FILES_TO_SCAN,
    MAX_FILES_TO_SCAN,
  );
  
  // Use FAST finder that leverages partition structure instead of scanning 55k+ files
  const files = findBinaryFilesFast(dirPath, type, { maxDays, maxFiles: maxFilesToScanLimit });
  
  if (files.length === 0) {
    return { records: [], total: 0, hasMore: false };
  }
  
  // Files are already sorted by data date desc from findBinaryFilesFast
  const allRecords = [];
  const maxFilesToScan = Math.min(files.length, maxFilesToScanLimit);
  
  for (let i = 0; i < maxFilesToScan; i++) {
    const file = files[i];
    try {
      const result = await readBinaryFile(file);
      let fileRecords = result.records;
      
      // Apply filter if provided
      if (filter) {
        fileRecords = fileRecords.filter(filter);
      }
      
      allRecords.push(...fileRecords);
      
      // Early stop once we have comfortably more than we need.
      // Since files are ordered newest data date → oldest, later files are unlikely to contain newer records.
      if (allRecords.length >= offset + limit + 2000) {
        break;
      }
    } catch (err) {
      console.error(`Failed to read ${file}: ${err.message}`);
    }
  }
  
  // Sort all collected records by the requested field (default: effective_at descending)
  allRecords.sort((a, b) => {
    const dateA = new Date(a[sortBy] || a.effective_at || a.timestamp || 0).getTime();
    const dateB = new Date(b[sortBy] || b.effective_at || b.timestamp || 0).getTime();
    return dateB - dateA; // Descending (newest first)
  });
  
  // Apply pagination
  const paginatedRecords = allRecords.slice(offset, offset + limit);
  
  return { 
    records: paginatedRecords, 
    total: allRecords.length,
    hasMore: offset + limit < allRecords.length,
    source: 'binary',
    filesScanned: maxFilesToScan,
    totalFiles: files.length,
  };
}

// Legacy function - now uses streaming for large datasets
export async function loadAllRecords(dirPath, type = 'events') {
  const files = findBinaryFiles(dirPath, type);
  
  // For large datasets, refuse to load all
  if (files.length > 50) {
    console.warn(`⚠️ Too many files (${files.length}), use streamRecords() instead`);
    // Return first batch only
    const result = await streamRecords(dirPath, type, { limit: 100 });
    return result.records;
  }
  
  console.log(`📖 Loading ${files.length} ${type} files...`);
  const startTime = Date.now();
  
  const allRecords = [];
  
  for (const file of files) {
    try {
      const result = await readBinaryFile(file);
      allRecords.push(...result.records);
    } catch (err) {
      console.error(`Failed to read ${file}: ${err.message}`);
    }
  }
  
  const elapsed = Date.now() - startTime;
  console.log(`✅ Loaded ${allRecords.length} ${type} records in ${elapsed}ms`);
  
  return allRecords;
}

/**
 * Invalidate cache (call after new data is ingested)
 */
export function invalidateCache() {
  recordCache.events = null;
  recordCache.updates = null;
  recordCache.eventsTimestamp = 0;
  recordCache.updatesTimestamp = 0;
}

export default {
  readBinaryFile,
  findBinaryFiles,
  findBinaryFilesFast,
  countBinaryFiles,
  hasBinaryFiles,
  loadAllRecords,
  streamRecords,
};
