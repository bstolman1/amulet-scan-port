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
 */
function extractTimestampFromPath(filePath) {
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
 * Find all .pb.zst files in a directory
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

// Stream records with pagination (memory efficient for large datasets)
export async function streamRecords(dirPath, type = 'events', options = {}) {
  const { limit = 100, offset = 0, filter = null, sortBy = 'effective_at' } = options;
  
  const files = findBinaryFiles(dirPath, type);
  
  if (files.length === 0) {
    return { records: [], total: 0, hasMore: false };
  }
  
  // Sort files by timestamp embedded in filename (descending = newest first)
  // Filename pattern: events-{timestamp}-{random}.pb.zst
  files.sort((a, b) => {
    const tsA = extractTimestampFromPath(a);
    const tsB = extractTimestampFromPath(b);
    return tsB - tsA; // Descending order (newest written first)
  });
  
  // For effective_at sorting, we need to read more files and sort globally
  // because file write order doesn't match event effective_at order
  const allRecords = [];
  const maxFilesToScan = Math.min(files.length, 200); // Cap to prevent memory issues
  
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
      
      // If we have enough records and sortBy is timestamp (write order), we can stop early
      if (sortBy === 'timestamp' && allRecords.length >= offset + limit + 1000) {
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
  hasBinaryFiles,
  loadAllRecords,
  streamRecords,
};
