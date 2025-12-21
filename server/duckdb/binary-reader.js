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

// Proto schema definition - MUST MATCH scripts/ingest/schema/ledger.proto exactly!
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
  int64 created_at_ts = 7;

  string contract_id = 8;
  string template = 9;
  string package_name = 10;
  int64 migration_id = 11;

  repeated string signatories = 12;
  repeated string observers = 13;
  repeated string acting_parties = 14;
  repeated string witness_parties = 15;

  string payload_json = 16;
  
  string contract_key_json = 17;
  
  string choice = 18;
  bool consuming = 19;
  string interface_id = 20;
  repeated string child_event_ids = 21;
  string exercise_result_json = 22;
  
  string source_synchronizer = 23;
  string target_synchronizer = 24;
  string unassign_id = 25;
  string submitter = 26;
  int64 reassignment_counter = 27;
  
  string raw_json = 28;
  
  string party = 29;
  
  string type_original = 30;
}

message Update {
  string id = 1;
  string type = 2;
  string synchronizer = 3;

  int64 effective_at = 4;
  int64 recorded_at = 5;
  int64 record_time = 6;

  string command_id = 7;
  string workflow_id = 8;
  string kind = 9;
  
  int64 migration_id = 10;
  int64 offset = 11;
  
  repeated string root_event_ids = 12;
  int32 event_count = 13;
  
  string source_synchronizer = 14;
  string target_synchronizer = 15;
  string unassign_id = 16;
  string submitter = 17;
  int64 reassignment_counter = 18;
  
  string trace_context_json = 19;
  
  string update_data_json = 20;
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
 * Extract migration id from partition path: .../migration=N/...
 */
function extractMigrationIdFromPath(filePath) {
  const m = filePath.match(/migration=(\d+)/);
  return m ? parseInt(m[1], 10) : null;
}

/**
 * Convert Long object to number (protobufjs returns int64 as Long)
 */
function toLong(val) {
  if (val === null || val === undefined) return null;
  if (typeof val === 'number') return val;
  if (typeof val === 'object' && 'low' in val) {
    // Long object from protobufjs
    return val.toNumber ? val.toNumber() : Number(val.low);
  }
  return Number(val);
}

/**
 * Convert protobuf record to plain object with readable timestamps
 */
function toPlainObject(record, isEvent, filePath) {
  // Extract migration_id from file path as fallback
  const pathMigrationId = filePath ? extractMigrationIdFromPath(filePath) : null;

  if (isEvent) {
    // Use protobuf migration_id if available, else fall back to path
    const migrationId = toLong(record.migrationId ?? record.migration_id) ?? pathMigrationId;
    
    return {
      event_id: record.id || null,
      update_id: record.updateId || record.update_id || null,
      event_type: record.type || null,
      event_type_original: record.typeOriginal || record.type_original || null,
      synchronizer_id: record.synchronizer || null,
      migration_id: migrationId,
      timestamp: record.recordedAt ? new Date(Number(record.recordedAt)).toISOString() : null,
      effective_at: record.effectiveAt ? new Date(Number(record.effectiveAt)).toISOString() : null,
      created_at_ts: record.createdAtTs ? new Date(Number(record.createdAtTs)).toISOString() : null,
      contract_id: record.contractId || record.contract_id || null,
      template_id: record.template || null,
      package_name: record.packageName || record.package_name || null,
      payload: record.payloadJson ? tryParseJson(record.payloadJson) : null,
      signatories: record.signatories || [],
      observers: record.observers || [],
      acting_parties: record.actingParties || record.acting_parties || [],
      witness_parties: record.witnessParties || record.witness_parties || [],
      // Exercised event fields
      choice: record.choice || null,
      consuming: record.consuming || false,
      interface_id: record.interfaceId || record.interface_id || null,
      child_event_ids: record.childEventIds || record.child_event_ids || [],
      exercise_result: record.exerciseResultJson ? tryParseJson(record.exerciseResultJson) : null,
      // Created event fields
      contract_key: record.contractKeyJson ? tryParseJson(record.contractKeyJson) : null,
      // Reassignment fields
      source_synchronizer: record.sourceSynchronizer || record.source_synchronizer || null,
      target_synchronizer: record.targetSynchronizer || record.target_synchronizer || null,
      unassign_id: record.unassignId || record.unassign_id || null,
      submitter: record.submitter || null,
      reassignment_counter: toLong(record.reassignmentCounter || record.reassignment_counter),
      // Complete original event
      raw: record.rawJson ? tryParseJson(record.rawJson) : null,
    };
  }

  // Update record
  const migrationId = toLong(record.migrationId ?? record.migration_id) ?? pathMigrationId;
  
  return {
    update_id: record.id || null,
    update_type: record.type || null,
    synchronizer_id: record.synchronizer || null,
    migration_id: migrationId,
    timestamp: record.recordedAt ? new Date(Number(record.recordedAt)).toISOString() : null,
    effective_at: record.effectiveAt ? new Date(Number(record.effectiveAt)).toISOString() : null,
    record_time: record.recordTime ? new Date(Number(record.recordTime)).toISOString() : null,
    command_id: record.commandId || record.command_id || null,
    workflow_id: record.workflowId || record.workflow_id || null,
    kind: record.kind || null,
    offset: record.offset || null,
    root_event_ids: record.rootEventIds || record.root_event_ids || [],
    event_count: record.eventCount || record.event_count || 0,
    // Reassignment fields
    source_synchronizer: record.sourceSynchronizer || record.source_synchronizer || null,
    target_synchronizer: record.targetSynchronizer || record.target_synchronizer || null,
    unassign_id: record.unassignId || record.unassign_id || null,
    submitter: record.submitter || null,
    reassignment_counter: record.reassignmentCounter || record.reassignment_counter || null,
    // Full update data
    update_data: record.updateDataJson ? tryParseJson(record.updateDataJson) : null,
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
  
  // Debug: log file size
  console.log(`[binary-reader] Reading ${basename}: ${fileBuffer.length} bytes`);
  
  if (fileBuffer.length === 0) {
    console.warn(`[binary-reader] File is empty: ${filePath}`);
    return { type: recordKey, count: 0, records: [] };
  }
  
  const allRecords = [];
  let offset = 0;
  let chunkIndex = 0;
  
  // Read chunks: [4-byte length][compressed data]...
  while (offset < fileBuffer.length) {
    if (offset + 4 > fileBuffer.length) {
      console.warn(`[binary-reader] Incomplete length header at offset ${offset}`);
      break;
    }
    
    const chunkLength = fileBuffer.readUInt32BE(offset);
    offset += 4;
    
    if (chunkLength === 0) {
      console.warn(`[binary-reader] Zero-length chunk at index ${chunkIndex}`);
      continue;
    }
    
    if (offset + chunkLength > fileBuffer.length) {
      console.warn(`[binary-reader] Chunk ${chunkIndex} would exceed file bounds: needs ${chunkLength} bytes at offset ${offset}, file size ${fileBuffer.length}`);
      break;
    }
    
    const compressedChunk = fileBuffer.subarray(offset, offset + chunkLength);
    offset += chunkLength;
    
    // Decompress chunk
    let decompressed;
    try {
      decompressed = await decompress(compressedChunk);
    } catch (err) {
      console.error(`[binary-reader] Failed to decompress chunk ${chunkIndex}: ${err.message}`);
      chunkIndex++;
      continue;
    }
    
    // Decode protobuf
    let message;
    try {
      message = BatchType.decode(decompressed);
    } catch (err) {
      console.error(`[binary-reader] Failed to decode protobuf chunk ${chunkIndex}: ${err.message}`);
      chunkIndex++;
      continue;
    }
    
    const records = message[recordKey] || [];
    console.log(`[binary-reader] Chunk ${chunkIndex}: decompressed ${decompressed.length} bytes, decoded ${records.length} ${recordKey}`);

    for (const r of records) {
      allRecords.push(toPlainObject(r, isEvents, filePath));
    }
    
    chunkIndex++;
  }
  
  console.log(`[binary-reader] Total: ${chunkIndex} chunks, ${allRecords.length} records from ${basename}`);
  
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

// Configurable scan limit via env var (default 500)
// This is a SOFT default - can be overridden per-call for specific endpoints
const MAX_FILES_TO_SCAN_DEFAULT = parseInt(process.env.BINARY_READER_MAX_FILES) || 500;

// Stream records with pagination (memory efficient for large datasets)
export async function streamRecords(dirPath, type = 'events', options = {}) {
  const {
    limit = 100,
    offset = 0,
    filter = null,
    sortBy = 'effective_at',
    maxDays = 30,
    maxFilesToScan: maxFilesToScanOverride,
    fullScan = false, // If true, use full directory scan instead of date-based fast scan
  } = options;
  
  // Allow override to exceed the default - important for rare event scanning like VoteRequest
  const maxFilesToScanLimit = typeof maxFilesToScanOverride === 'number' 
    ? maxFilesToScanOverride 
    : MAX_FILES_TO_SCAN_DEFAULT;
  
  // Use full scan for historical data that spans many years (like VoteRequests)
  // or fast scan for recent data
  let files;
  if (fullScan) {
    console.log(`   📂 Full scan mode: scanning all files in ${dirPath}...`);
    files = findBinaryFiles(dirPath, type);
    console.log(`   📂 Found ${files.length} total ${type} files`);
  } else {
    files = findBinaryFilesFast(dirPath, type, { maxDays, maxFiles: maxFilesToScanLimit });

    // Fallback: if date-based scan finds nothing (timezone/partition mismatch), do a capped full scan
    if (files.length === 0) {
      console.log(`   ⚠️ Fast scan found 0 ${type} files. Falling back to capped full scan...`);
      const all = findBinaryFiles(dirPath, type);
      all.sort((a, b) => extractWriteTimestampFromPath(b) - extractWriteTimestampFromPath(a));
      files = all.slice(0, maxFilesToScanLimit);
      console.log(`   📂 Fallback scan selected ${files.length}/${all.length} ${type} files`);
    }
  }

  if (files.length === 0) {
    return { records: [], total: 0, hasMore: false };
  }
  
  // Files are already sorted by data date desc from findBinaryFilesFast
  const allRecords = [];
  const maxFilesToScan = fullScan ? files.length : Math.min(files.length, maxFilesToScanLimit);
  
  let filesProcessed = 0;
  const scanStartTime = fullScan ? Date.now() : null;
  let lastLogTime = scanStartTime;
  
  for (let i = 0; i < maxFilesToScan; i++) {
    const file = files[i];
    try {
      const result = await readBinaryFile(file);
      let fileRecords = result.records;
      filesProcessed++;
      
      // Apply filter if provided
      if (filter) {
        fileRecords = fileRecords.filter(filter);
      }
      
      allRecords.push(...fileRecords);
      
      // Log progress every 250 files OR every 10 seconds for full scan
      if (fullScan) {
        const now = Date.now();
        const shouldLog = filesProcessed % 250 === 0 || (now - lastLogTime > 10000);
        
        if (shouldLog) {
          const elapsed = (now - scanStartTime) / 1000;
          const filesPerSec = filesProcessed / elapsed;
          const remaining = maxFilesToScan - filesProcessed;
          const etaSeconds = remaining / filesPerSec;
          const etaMin = Math.floor(etaSeconds / 60);
          const etaSec = Math.floor(etaSeconds % 60);
          const pct = ((filesProcessed / maxFilesToScan) * 100).toFixed(1);
          
          console.log(`   📂 [${pct}%] ${filesProcessed}/${maxFilesToScan} files | ${allRecords.length} matches | ${filesPerSec.toFixed(0)} files/s | ETA: ${etaMin}m ${etaSec}s`);
          lastLogTime = now;
        }
      }
      
      // For non-full scans, early stop once we have comfortably more than we need.
      if (!fullScan && allRecords.length >= offset + limit + 2000) {
        break;
      }
    } catch (err) {
      console.error(`Failed to read ${file}: ${err.message}`);
    }
  }
  
  if (fullScan) {
    const totalElapsed = ((Date.now() - scanStartTime) / 1000).toFixed(1);
    console.log(`   ✅ Full scan complete: ${filesProcessed} files, ${allRecords.length} matches in ${totalElapsed}s`);
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
    filesScanned: filesProcessed,
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
