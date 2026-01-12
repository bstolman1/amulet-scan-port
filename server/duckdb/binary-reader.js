/**
 * Binary/Parquet Reader for Server
 * 
 * Reads ledger data from either:
 * 1. Parquet files (preferred, via DuckDB read_parquet)
 * 2. .pb.zst files (legacy, via Protobuf + ZSTD)
 * 
 * Automatically detects available formats and uses the most efficient one.
 */

import fs from 'fs';
import path from 'path';
import protobuf from 'protobufjs';
import { decompress } from '@mongodb-js/zstd';
import duckdb from 'duckdb';

// Use process.cwd() for Vitest/Vite SSR compatibility (fileURLToPath can break under SSR)
const __dirname = path.join(process.cwd(), 'server', 'duckdb');

// DATA_DIR configuration (matches write-parquet.js)
const WIN_DEFAULT = 'C:\\ledger_raw';
const REPO_DATA_DIR = path.join(__dirname, '../../data');
const repoRawDir = path.join(REPO_DATA_DIR, 'raw');

const BASE_DATA_DIR = process.env.DATA_DIR || (fs.existsSync(repoRawDir) ? REPO_DATA_DIR : WIN_DEFAULT);
const DATA_PATH = path.join(BASE_DATA_DIR, 'raw');

// DuckDB connection for Parquet queries
const DB_FILE = process.env.DUCKDB_FILE || path.join(BASE_DATA_DIR, 'canton-explorer.duckdb');
const db = new duckdb.Database(DB_FILE);
const conn = db.connect();

/**
 * Run DuckDB query
 */
function query(sql) {
  return new Promise((resolve, reject) => {
    conn.all(sql, (err, rows) => {
      if (err) reject(err);
      else resolve(rows);
    });
  });
}

// Proto schema definition for legacy .pb.zst files
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

// ============= FILE DETECTION =============

/**
 * Check if Parquet files exist for a type
 */
export function hasParquetFiles(dirPath, type = 'events') {
  try {
    if (!fs.existsSync(dirPath)) return false;
    const stack = [dirPath];
    while (stack.length > 0) {
      const dir = stack.pop();
      const entries = fs.readdirSync(dir, { withFileTypes: true });
      for (const entry of entries) {
        if (entry.isDirectory()) {
          stack.push(path.join(dir, entry.name));
        } else if (entry.name.startsWith(`${type}-`) && entry.name.endsWith('.parquet')) {
          return true;
        }
      }
    }
    return false;
  } catch {
    return false;
  }
}

/**
 * Check if binary (.pb.zst) files exist for a type
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

/**
 * Count Parquet files
 */
export function countParquetFiles(dirPath, type = 'events') {
  let count = 0;
  
  function scanDir(dir) {
    try {
      const entries = fs.readdirSync(dir, { withFileTypes: true });
      for (const entry of entries) {
        const fullPath = path.join(dir, entry.name);
        if (entry.isDirectory()) {
          scanDir(fullPath);
        } else if (entry.name.startsWith(`${type}-`) && entry.name.endsWith('.parquet')) {
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
 * Count binary (.pb.zst) files
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
        } else if (entry.name.startsWith(`${type}-`) && entry.name.endsWith('.pb.zst')) {
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
 * Detect available format (Parquet preferred)
 */
export function detectFormat(dirPath, type = 'events') {
  if (hasParquetFiles(dirPath, type)) return 'parquet';
  if (hasBinaryFiles(dirPath, type)) return 'binary';
  return null;
}

// ============= PARQUET READING (via DuckDB) =============

/**
 * Get glob pattern for Parquet files
 */
function getParquetGlob(dirPath, type = 'events') {
  const normalizedPath = dirPath.replace(/\\/g, '/');
  return `${normalizedPath}/**/${type}-*.parquet`;
}

/**
 * Stream records from Parquet files using DuckDB
 */
async function streamParquetRecords(dirPath, type = 'events', options = {}) {
  const {
    limit = 100,
    offset = 0,
    filter = null,
    sortBy = 'effective_at',
    migrationId = null,
  } = options;
  
  const glob = getParquetGlob(dirPath, type);
  
  // Build WHERE clause
  const conditions = [];
  if (migrationId !== null) {
    conditions.push(`migration_id = ${migrationId}`);
  }
  
  // For template filtering (common use case)
  if (filter?.template_id) {
    conditions.push(`template_id = '${filter.template_id}'`);
  }
  if (filter?.event_type) {
    conditions.push(`event_type = '${filter.event_type}'`);
  }
  
  const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
  
  // Map sortBy to actual column (handle both events and updates)
  const sortColumn = sortBy === 'timestamp' ? 'recorded_at' : sortBy;
  
  try {
    // Count total matching records
    const countSql = `
      SELECT COUNT(*) as total 
      FROM read_parquet('${glob}', union_by_name=true)
      ${whereClause}
    `;
    const countResult = await query(countSql);
    const total = countResult[0]?.total || 0;
    
    if (total === 0) {
      return { records: [], total: 0, hasMore: false, source: 'parquet' };
    }
    
    // Fetch paginated records
    const dataSql = `
      SELECT * 
      FROM read_parquet('${glob}', union_by_name=true)
      ${whereClause}
      ORDER BY ${sortColumn} DESC NULLS LAST
      LIMIT ${limit} OFFSET ${offset}
    `;
    
    let records = await query(dataSql);
    
    // Apply JS filter if provided as function (for complex filters not expressible in SQL)
    if (typeof filter === 'function') {
      records = records.filter(filter);
    }
    
    // Normalize field names for consistency with legacy format
    records = records.map(r => normalizeParquetRecord(r, type));
    
    return {
      records,
      total,
      hasMore: offset + limit < total,
      source: 'parquet',
    };
  } catch (err) {
    console.error(`Parquet query error: ${err.message}`);
    // Fall back to binary if Parquet fails
    return null;
  }
}

/**
 * Normalize Parquet record field names to match expected format
 */
function normalizeParquetRecord(record, type) {
  if (type === 'events') {
    return {
      event_id: record.event_id || record.id || null,
      update_id: record.update_id || null,
      event_type: record.event_type || record.type || null,
      event_type_original: record.event_type_original || null,
      synchronizer_id: record.synchronizer_id || record.synchronizer || null,
      migration_id: record.migration_id || null,
      timestamp: record.recorded_at || null,
      effective_at: record.effective_at || null,
      created_at_ts: record.created_at_ts || null,
      contract_id: record.contract_id || null,
      template_id: record.template_id || record.template || null,
      package_name: record.package_name || null,
      payload: tryParseJson(record.payload),
      signatories: tryParseJson(record.signatories) || [],
      observers: tryParseJson(record.observers) || [],
      acting_parties: tryParseJson(record.acting_parties) || [],
      witness_parties: tryParseJson(record.witness_parties) || [],
      choice: record.choice || null,
      consuming: record.consuming || false,
      interface_id: record.interface_id || null,
      child_event_ids: tryParseJson(record.child_event_ids) || [],
      exercise_result: tryParseJson(record.exercise_result),
      contract_key: tryParseJson(record.contract_key),
      source_synchronizer: record.source_synchronizer || null,
      target_synchronizer: record.target_synchronizer || null,
      unassign_id: record.unassign_id || null,
      submitter: record.submitter || null,
      reassignment_counter: record.reassignment_counter || null,
      raw: tryParseJson(record.raw_event),
    };
  }
  
  // Update record
  return {
    update_id: record.update_id || record.id || null,
    update_type: record.update_type || record.type || null,
    synchronizer_id: record.synchronizer_id || record.synchronizer || null,
    migration_id: record.migration_id || null,
    timestamp: record.recorded_at || null,
    effective_at: record.effective_at || null,
    record_time: record.record_time || null,
    command_id: record.command_id || null,
    workflow_id: record.workflow_id || null,
    kind: record.kind || null,
    offset: record.offset || null,
    root_event_ids: tryParseJson(record.root_event_ids) || [],
    event_count: record.event_count || 0,
    source_synchronizer: record.source_synchronizer || null,
    target_synchronizer: record.target_synchronizer || null,
    unassign_id: record.unassign_id || null,
    submitter: record.submitter || null,
    reassignment_counter: record.reassignment_counter || null,
    update_data: tryParseJson(record.update_data),
  };
}

function tryParseJson(val) {
  if (!val) return null;
  if (typeof val === 'object') return val; // Already parsed
  try {
    return JSON.parse(val);
  } catch {
    return val;
  }
}

// ============= BINARY READING (legacy .pb.zst) =============

function extractMigrationIdFromPath(filePath) {
  const m = filePath.match(/migration=(\d+)/);
  return m ? parseInt(m[1], 10) : null;
}

function toLong(val) {
  if (val === null || val === undefined) return null;
  if (typeof val === 'number') return val;
  if (typeof val === 'bigint') {
    if (val > Number.MAX_SAFE_INTEGER || val < Number.MIN_SAFE_INTEGER) {
      return Number(val);
    }
    return Number(val);
  }
  if (typeof val === 'object' && 'low' in val) {
    return val.toNumber ? val.toNumber() : Number(val.low);
  }
  return Number(val);
}

function toPlainObject(record, isEvent, filePath) {
  const pathMigrationId = filePath ? extractMigrationIdFromPath(filePath) : null;

  if (isEvent) {
    const migrationId = toLong(record.migrationId ?? record.migration_id) ?? pathMigrationId;
    
    return {
      event_id: record.id || null,
      update_id: record.updateId || record.update_id || null,
      event_type: record.type || null,
      event_type_original: record.typeOriginal || record.type_original || null,
      synchronizer_id: record.synchronizer || null,
      migration_id: migrationId,
      timestamp: record.recordedAt ? new Date(toLong(record.recordedAt)).toISOString() : null,
      effective_at: record.effectiveAt ? new Date(toLong(record.effectiveAt)).toISOString() : null,
      created_at_ts: record.createdAtTs ? new Date(toLong(record.createdAtTs)).toISOString() : null,
      contract_id: record.contractId || record.contract_id || null,
      template_id: record.template || null,
      package_name: record.packageName || record.package_name || null,
      payload: record.payloadJson ? tryParseJson(record.payloadJson) : null,
      signatories: record.signatories || [],
      observers: record.observers || [],
      acting_parties: record.actingParties || record.acting_parties || [],
      witness_parties: record.witnessParties || record.witness_parties || [],
      choice: record.choice || null,
      consuming: record.consuming || false,
      interface_id: record.interfaceId || record.interface_id || null,
      child_event_ids: record.childEventIds || record.child_event_ids || [],
      exercise_result: record.exerciseResultJson ? tryParseJson(record.exerciseResultJson) : null,
      contract_key: record.contractKeyJson ? tryParseJson(record.contractKeyJson) : null,
      source_synchronizer: record.sourceSynchronizer || record.source_synchronizer || null,
      target_synchronizer: record.targetSynchronizer || record.target_synchronizer || null,
      unassign_id: record.unassignId || record.unassign_id || null,
      submitter: record.submitter || null,
      reassignment_counter: toLong(record.reassignmentCounter || record.reassignment_counter),
      raw: record.rawJson ? tryParseJson(record.rawJson) : null,
    };
  }

  const migrationId = toLong(record.migrationId ?? record.migration_id) ?? pathMigrationId;
  
  return {
    update_id: record.id || null,
    update_type: record.type || null,
    synchronizer_id: record.synchronizer || null,
    migration_id: migrationId,
    timestamp: record.recordedAt ? new Date(toLong(record.recordedAt)).toISOString() : null,
    effective_at: record.effectiveAt ? new Date(toLong(record.effectiveAt)).toISOString() : null,
    record_time: record.recordTime ? new Date(toLong(record.recordTime)).toISOString() : null,
    command_id: record.commandId || record.command_id || null,
    workflow_id: record.workflowId || record.workflow_id || null,
    kind: record.kind || null,
    offset: toLong(record.offset),
    root_event_ids: record.rootEventIds || record.root_event_ids || [],
    event_count: record.eventCount || record.event_count || 0,
    source_synchronizer: record.sourceSynchronizer || record.source_synchronizer || null,
    target_synchronizer: record.targetSynchronizer || record.target_synchronizer || null,
    unassign_id: record.unassignId || record.unassign_id || null,
    submitter: record.submitter || null,
    reassignment_counter: toLong(record.reassignmentCounter || record.reassignment_counter),
    update_data: record.updateDataJson ? tryParseJson(record.updateDataJson) : null,
  };
}

function extractWriteTimestampFromPath(filePath) {
  const basename = path.basename(filePath);
  const match = basename.match(/(?:events|updates)-(\d+)-/);
  if (match) {
    return parseInt(match[1], 10);
  }
  try {
    return fs.statSync(filePath).mtimeMs;
  } catch {
    return 0;
  }
}

function extractDataDateFromPath(filePath) {
  const yearMatch = filePath.match(/year=(\d{4})/);
  const monthMatch = filePath.match(/month=(\d{2})/);
  const dayMatch = filePath.match(/day=(\d{2})/);
  
  if (yearMatch && monthMatch && dayMatch) {
    const year = parseInt(yearMatch[1], 10);
    const month = parseInt(monthMatch[1], 10) - 1;
    const day = parseInt(dayMatch[1], 10);
    return new Date(year, month, day).getTime();
  }
  
  return extractWriteTimestampFromPath(filePath);
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

  const fileBuffer = await fs.promises.readFile(filePath);
  const allRecords = [];
  let offset = 0;
  
  while (offset < fileBuffer.length) {
    if (offset + 4 > fileBuffer.length) break;
    
    const chunkLength = fileBuffer.readUInt32BE(offset);
    offset += 4;
    
    if (offset + chunkLength > fileBuffer.length) break;
    
    const compressedChunk = fileBuffer.subarray(offset, offset + chunkLength);
    offset += chunkLength;
    
    const decompressed = await decompress(compressedChunk);
    const message = BatchType.decode(decompressed);
    const records = message[recordKey] || [];

    for (const r of records) {
      allRecords.push(toPlainObject(r, isEvents, filePath));
    }
  }
  
  return {
    type: recordKey,
    count: allRecords.length,
    records: allRecords
  };
}

/**
 * Find binary files with fast date-based scanning
 */
export function findBinaryFilesFast(dirPath, type = 'events', options = {}) {
  const { maxDays = 7, maxFiles = 1000 } = options;
  const files = [];
  
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
  
  const migrations = [];
  try {
    const entries = fs.readdirSync(dirPath, { withFileTypes: true });
    for (const entry of entries) {
      if (entry.isDirectory() && entry.name.startsWith('migration=')) {
        migrations.push(path.join(dirPath, entry.name));
      }
    }
  } catch { }
  
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
    
    if (files.length >= maxFiles) break;
  }
  
  files.sort((a, b) => {
    if (a.dataDateMs !== b.dataDateMs) return b.dataDateMs - a.dataDateMs;
    return b.writeTs - a.writeTs;
  });
  
  return files.slice(0, maxFiles).map(f => f.path);
}

/**
 * Find all binary files (full scan)
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
    } catch { }
  }
  
  if (fs.existsSync(dirPath)) {
    scanDir(dirPath);
  }
  
  return files;
}

// ============= UNIFIED STREAMING API =============

const MAX_FILES_TO_SCAN_DEFAULT = parseInt(process.env.BINARY_READER_MAX_FILES) || 500;

/**
 * Stream records from the best available format (Parquet preferred)
 */
export async function streamRecords(dirPath, type = 'events', options = {}) {
  const {
    limit = 100,
    offset = 0,
    filter = null,
    sortBy = 'effective_at',
    maxDays = 30,
    maxFilesToScan: maxFilesToScanOverride,
    fullScan = false,
    preferParquet = true, // New option: set to false to force binary
  } = options;
  
  // Try Parquet first (much faster via DuckDB SQL)
  if (preferParquet && hasParquetFiles(dirPath, type)) {
    console.log(`📊 Using Parquet format for ${type}`);
    const result = await streamParquetRecords(dirPath, type, options);
    if (result) return result;
    console.log(`⚠️ Parquet query failed, falling back to binary`);
  }
  
  // Fall back to binary (.pb.zst) files
  if (!hasBinaryFiles(dirPath, type)) {
    return { records: [], total: 0, hasMore: false, source: 'none' };
  }
  
  console.log(`📦 Using binary format for ${type}`);
  
  const maxFilesToScanLimit = typeof maxFilesToScanOverride === 'number' 
    ? maxFilesToScanOverride 
    : MAX_FILES_TO_SCAN_DEFAULT;
  
  let files;
  if (fullScan) {
    console.log(`   📂 Full scan mode: scanning all files in ${dirPath}...`);
    files = findBinaryFiles(dirPath, type);
    console.log(`   📂 Found ${files.length} total ${type} files`);
  } else {
    files = findBinaryFilesFast(dirPath, type, { maxDays, maxFiles: maxFilesToScanLimit });
  }
  
  if (files.length === 0) {
    return { records: [], total: 0, hasMore: false, source: 'binary' };
  }
  
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
      
      if (filter && typeof filter === 'function') {
        fileRecords = fileRecords.filter(filter);
      }
      
      allRecords.push(...fileRecords);
      
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
  
  allRecords.sort((a, b) => {
    const dateA = new Date(a[sortBy] || a.effective_at || a.timestamp || 0).getTime();
    const dateB = new Date(b[sortBy] || b.effective_at || b.timestamp || 0).getTime();
    return dateB - dateA;
  });
  
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

/**
 * Load all records (legacy, use streamRecords instead)
 */
export async function loadAllRecords(dirPath, type = 'events') {
  const files = findBinaryFiles(dirPath, type);
  
  if (files.length > 50) {
    console.warn(`⚠️ Too many files (${files.length}), use streamRecords() instead`);
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

// ============= CACHE =============

const recordCache = {
  events: null,
  updates: null,
  eventsTimestamp: 0,
  updatesTimestamp: 0,
};

export function invalidateCache() {
  recordCache.events = null;
  recordCache.updates = null;
  recordCache.eventsTimestamp = 0;
  recordCache.updatesTimestamp = 0;
}

// ============= EXPORTS =============

export { DATA_PATH };

export default {
  readBinaryFile,
  findBinaryFiles,
  findBinaryFilesFast,
  countBinaryFiles,
  countParquetFiles,
  hasBinaryFiles,
  hasParquetFiles,
  detectFormat,
  loadAllRecords,
  streamRecords,
  invalidateCache,
  DATA_PATH,
};
