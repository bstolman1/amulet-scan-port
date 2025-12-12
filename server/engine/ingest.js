/**
 * Ingest - Streaming ingestion of .pb.zst files into DuckDB tables
 * 
 * STREAMING-ONLY: Processes records in batches without loading entire file into memory.
 */

import { query } from '../duckdb/connection.js';
import { decodeFile } from './decoder.js';

const BATCH_SIZE = 2000; // Records per insert batch

/**
 * Insert a batch of events into events_raw
 */
async function insertEventBatch(records, fileId) {
  if (records.length === 0) return;
  
  // Build VALUES clause
  const values = records.map(r => {
    const id = sqlStr(r.id);
    const updateId = sqlStr(r.update_id);
    const type = sqlStr(r.type);
    const synchronizer = sqlStr(r.synchronizer);
    const effectiveAt = sqlTs(r.effective_at);
    const recordedAt = sqlTs(r.recorded_at);
    const contractId = sqlStr(r.contract_id);
    const party = sqlStr(r.party);
    const template = sqlStr(r.template);
    const packageName = sqlStr(r.package_name);
    const signatories = sqlArray(r.signatories);
    const observers = sqlArray(r.observers);
    const payload = sqlJson(r.payload);
    const rawJson = sqlJson(r.raw_json);
    
    return `(${id}, ${updateId}, ${type}, ${synchronizer}, ${effectiveAt}, ${recordedAt}, ${contractId}, ${party}, ${template}, ${packageName}, ${signatories}, ${observers}, ${payload}, ${rawJson}, ${fileId})`;
  }).join(',\n');
  
  await query(`
    INSERT INTO events_raw (id, update_id, type, synchronizer, effective_at, recorded_at, contract_id, party, template, package_name, signatories, observers, payload, raw_json, _file_id)
    VALUES ${values}
  `);
}

/**
 * Insert a batch of updates into updates_raw
 */
async function insertUpdateBatch(records, fileId) {
  if (records.length === 0) return;
  
  const values = records.map(r => {
    const id = sqlStr(r.id);
    const synchronizer = sqlStr(r.synchronizer);
    const effectiveAt = sqlTs(r.effective_at);
    const recordedAt = sqlTs(r.recorded_at);
    const type = sqlStr(r.type);
    const commandId = sqlStr(r.command_id);
    const workflowId = sqlStr(r.workflow_id);
    const kind = sqlStr(r.kind);
    const migrationId = r.migration_id ?? 'NULL';
    const offsetVal = r.offset_val ?? 'NULL';
    const eventCount = r.event_count ?? 0;
    
    return `(${id}, ${synchronizer}, ${effectiveAt}, ${recordedAt}, ${type}, ${commandId}, ${workflowId}, ${kind}, ${migrationId}, ${offsetVal}, ${eventCount}, ${fileId})`;
  }).join(',\n');
  
  await query(`
    INSERT INTO updates_raw (id, synchronizer, effective_at, recorded_at, type, command_id, workflow_id, kind, migration_id, offset_val, event_count, _file_id)
    VALUES ${values}
  `);
}

// SQL helpers
function sqlStr(val) {
  if (val === null || val === undefined) return 'NULL';
  return `'${String(val).replace(/'/g, "''")}'`;
}

function sqlTs(val) {
  if (!val) return 'NULL';
  return `TIMESTAMP '${val}'`;
}

function sqlArray(arr) {
  if (!arr || arr.length === 0) return 'NULL';
  const escaped = arr.map(s => `'${String(s).replace(/'/g, "''")}'`).join(', ');
  return `[${escaped}]`;
}

function sqlJson(val) {
  if (val === null || val === undefined) return 'NULL';
  try {
    const json = typeof val === 'string' ? val : JSON.stringify(val);
    return `'${json.replace(/'/g, "''")}'::JSON`;
  } catch {
    return 'NULL';
  }
}

/**
 * Ingest a single file using streaming decode
 */
async function ingestOneFile(fileRow) {
  const { file_id, file_path, file_type } = fileRow;
  
  try {
    const insertFn = file_type === 'events' ? insertEventBatch : insertUpdateBatch;
    
    let batch = [];
    let totalCount = 0;
    let minTs = null;
    let maxTs = null;
    
    // Stream records and insert in batches
    for await (const record of decodeFile(file_path)) {
      batch.push(record);
      totalCount++;
      
      // Track timestamps
      const ts = record.recorded_at || record.effective_at;
      if (ts) {
        const d = new Date(ts);
        if (!minTs || d < minTs) minTs = d;
        if (!maxTs || d > maxTs) maxTs = d;
      }
      
      // Insert when batch is full
      if (batch.length >= BATCH_SIZE) {
        await insertFn(batch, file_id);
        batch = [];
      }
    }
    
    // Insert remaining records
    if (batch.length > 0) {
      await insertFn(batch, file_id);
    }
    
    // Update file metadata
    await query(`
      UPDATE raw_files
      SET 
        record_count = ${totalCount},
        min_ts = ${minTs ? `TIMESTAMP '${minTs.toISOString()}'` : 'NULL'},
        max_ts = ${maxTs ? `TIMESTAMP '${maxTs.toISOString()}'` : 'NULL'},
        ingested = TRUE,
        ingested_at = CURRENT_TIMESTAMP
      WHERE file_id = ${file_id}
    `);
    
    return { file_id, count: totalCount, success: true };
  } catch (err) {
    console.error(`❌ Failed to ingest ${file_path}: ${err.message}`);
    return { file_id, count: 0, success: false, error: err.message };
  }
}

/**
 * Ingest up to maxFiles un-ingested files
 */
export async function ingestNewFiles(maxFiles = 5) {
  // Get files to ingest (oldest first by record_date)
  const files = await query(`
    SELECT file_id, file_path, file_type, migration_id
    FROM raw_files
    WHERE ingested = FALSE
    ORDER BY record_date ASC, file_id ASC
    LIMIT ${maxFiles}
  `);
  
  if (files.length === 0) {
    return { ingested: 0, records: 0 };
  }
  
  let totalIngested = 0;
  let totalRecords = 0;
  
  for (const file of files) {
    const result = await ingestOneFile(file);
    if (result.success) {
      totalIngested++;
      totalRecords += result.count;
      console.log(`🧩 Ingested ${result.count} records from file ${file.file_id}`);
    }
  }
  
  if (totalIngested > 0) {
    console.log(`📥 Ingested ${totalRecords} records from ${totalIngested} files`);
  }
  
  return { ingested: totalIngested, records: totalRecords };
}

/**
 * Get ingestion stats (uses efficient COUNT queries, not full scans)
 */
export async function getIngestionStats() {
  const rows = await query(`
    SELECT 
      COUNT(*) FILTER (WHERE ingested = TRUE) as ingested_files,
      COUNT(*) FILTER (WHERE ingested = FALSE) as pending_files,
      SUM(record_count) FILTER (WHERE ingested = TRUE) as total_records,
      MAX(max_ts) as latest_ts
    FROM raw_files
  `);
  
  return rows[0] || {};
}
