/**
 * Ingest - Streaming ingestion of .pb.zst files into DuckDB tables
 *
 * SAFE MODE (WSL-Optimized)
 * - Loads ZERO whole files into memory
 * - Inserts in small batches
 * - Limits to 2 files per cycle (prevents OOM)
 * - Protects against BigInt serialization errors
 */

import { query } from '../duckdb/connection.js';
import { decodeFile } from './decoder.js';

// Smaller batch size → lower peak RAM usage
const BATCH_SIZE = 1500;

// GLOBAL safety: never ingest too many files in one cycle
export const INGEST_FILE_LIMIT = 2;

/* ---------------------------------------------
 * SQL HELPERS (ESCAPING SAFE)
 * ------------------------------------------- */
function sqlStr(val) {
  if (val === null || val === undefined) return 'NULL';
  return `'${String(val).replace(/'/g, "''")}'`;
}

function sqlTs(val) {
  if (!val) return 'NULL';
  return `TIMESTAMP '${new Date(val).toISOString()}'`;
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

function sqlArray(arr) {
  if (!arr || arr.length === 0) return 'NULL';
  return `[${arr.map(v => sqlStr(v)).join(', ')}]`;
}

/* ---------------------------------------------
 * INSERT HELPERS
 * ------------------------------------------- */
async function insertEventBatch(records, fileId) {
  if (records.length === 0) return;

  const values = records.map(r => `
    (${sqlStr(r.id)},
     ${sqlStr(r.update_id)},
     ${sqlStr(r.type)},
     ${sqlStr(r.synchronizer)},
     ${sqlTs(r.effective_at)},
     ${sqlTs(r.recorded_at)},
     ${sqlStr(r.contract_id)},
     ${sqlStr(r.party)},
     ${sqlStr(r.template)},
     ${sqlStr(r.package_name)},
     ${sqlArray(r.signatories)},
     ${sqlArray(r.observers)},
     ${sqlJson(r.payload)},
     ${sqlJson(r.raw_json)},
     ${fileId})
  `).join(',');

  await query(`
    INSERT INTO events_raw
    (id, update_id, type, synchronizer, effective_at, recorded_at, contract_id, party,
     template, package_name, signatories, observers, payload, raw_json, _file_id)
    VALUES ${values}
  `);
}

async function insertUpdateBatch(records, fileId) {
  if (records.length === 0) return;

  const values = records.map(r => `
    (${sqlStr(r.id)},
     ${sqlStr(r.synchronizer)},
     ${sqlTs(r.effective_at)},
     ${sqlTs(r.recorded_at)},
     ${sqlStr(r.type)},
     ${sqlStr(r.command_id)},
     ${sqlStr(r.workflow_id)},
     ${sqlStr(r.kind)},
     ${r.migration_id ?? 'NULL'},
     ${r.offset_val ?? 'NULL'},
     ${r.event_count ?? 0},
     ${fileId})
  `).join(',');

  await query(`
    INSERT INTO updates_raw
    (id, synchronizer, effective_at, recorded_at, type, command_id, workflow_id,
     kind, migration_id, offset_val, event_count, _file_id)
    VALUES ${values}
  `);
}

/* ---------------------------------------------
 * INGEST ONE FILE (STREAMING)
 * ------------------------------------------- */
export async function ingestOneFile(fileRow) {
  const { file_id, file_path, file_type } = fileRow;

  const insertFn = file_type === 'events'
    ? insertEventBatch
    : insertUpdateBatch;

  let batch = [];
  let total = 0;
  let minTs = null;
  let maxTs = null;

  try {
    for await (const rec of decodeFile(file_path)) {
      batch.push(rec);
      total++;

      const ts = rec.recorded_at || rec.effective_at;
      if (ts) {
        const d = new Date(ts);
        if (!minTs || d < minTs) minTs = d;
        if (!maxTs || d > maxTs) maxTs = d;
      }

      if (batch.length >= BATCH_SIZE) {
        await insertFn(batch, file_id);
        batch.length = 0;
      }
    }

    if (batch.length) {
      await insertFn(batch, file_id);
    }

    await query(`
      UPDATE raw_files SET
        record_count = ${total},
        min_ts = ${minTs ? sqlTs(minTs) : 'NULL'},
        max_ts = ${maxTs ? sqlTs(maxTs) : 'NULL'},
        ingested = TRUE,
        ingested_at = CURRENT_TIMESTAMP
      WHERE file_id = ${file_id}
    `);

    console.log(`🧩 Ingested ${total} records from file ${file_id}`);
    return { success: true, count: total };

  } catch (err) {
    console.error(`❌ Ingest failed for ${file_path}:`, err);
    return { success: false, error: err.message };
  }
}

/* ---------------------------------------------
 * INGEST NEW FILES (LIMITED PER CYCLE)
 * ------------------------------------------- */
export async function ingestNewFiles(maxFiles = INGEST_FILE_LIMIT) {
  const files = await query(`
    SELECT file_id, file_path, file_type, migration_id
    FROM raw_files
    WHERE ingested = FALSE
    ORDER BY record_date ASC, file_id ASC
    LIMIT ${maxFiles}
  `);

  if (!files.length) {
    return { ingested: 0, records: 0 };
  }

  let ingested = 0;
  let totalRecords = 0;

  for (const f of files) {
    const result = await ingestOneFile(f);
    if (result.success) {
      ingested++;
      totalRecords += result.count;
    }
  }

  console.log(`📥 Ingested ${totalRecords} records from ${ingested} files`);
  return { ingested, records: totalRecords };
}

/* ---------------------------------------------
 * GET INGESTION STATS (SAFE FOR JSON)
 * ------------------------------------------- */
export async function getIngestionStats() {
  const rows = await query(`
    SELECT 
      COUNT(*) FILTER (WHERE ingested = TRUE)             AS ingested_files,
      COUNT(*) FILTER (WHERE ingested = FALSE)            AS pending_files,
      SUM(record_count) FILTER (WHERE ingested = TRUE)    AS total_records,
      MAX(max_ts)                                         AS latest_ts
    FROM raw_files
  `);

  const r = rows[0];

  return {
    ingested_files: Number(r.ingested_files || 0),
    pending_files: Number(r.pending_files || 0),
    total_records: Number(r.total_records || 0),
    latest_ts: r.latest_ts
  };
}
