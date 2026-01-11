/**
 * Parquet Worker - DuckDB In-Memory Writer
 *
 * Each worker has its own DuckDB in-memory instance for isolation.
 * Receives records from the pool, writes them to Parquet with ZSTD compression.
 *
 * Job format:
 * {
 *   type: 'events' | 'updates',
 *   filePath: string,
 *   records: object[],    // Pre-mapped records
 *   rowGroupSize: number  // DuckDB row group size
 * }
 *
 * Response format:
 * {
 *   ok: boolean,
 *   filePath: string,
 *   count: number,
 *   bytes: number,
 *   error?: string
 * }
 */

// Capture all errors
process.on('uncaughtException', (err) => {
  console.error('[PARQUET-WORKER FATAL] Uncaught exception:', err.message);
  console.error(err.stack);
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('[PARQUET-WORKER FATAL] Unhandled rejection at:', promise);
  console.error('Reason:', reason);
  process.exit(1);
});

import { parentPort, workerData, isMainThread } from 'node:worker_threads';
import fs from 'node:fs';
import path from 'node:path';

// Validate we're in a worker thread
if (isMainThread) {
  console.error('[PARQUET-WORKER] Error: parquet-worker.js must be run as a Worker');
  process.exit(1);
}

if (!workerData) {
  console.error('[PARQUET-WORKER] Error: No workerData received');
  parentPort.postMessage({ ok: false, error: 'No workerData received' });
  process.exit(1);
}

const {
  type,
  filePath,
  records,
  rowGroupSize = 100000,
} = workerData;

// Validate inputs
if (!type || !['events', 'updates'].includes(type)) {
  const msg = `Invalid type: ${type}`;
  console.error('[PARQUET-WORKER]', msg);
  parentPort.postMessage({ ok: false, error: msg, filePath });
  process.exit(1);
}

if (!filePath) {
  const msg = 'No filePath provided';
  console.error('[PARQUET-WORKER]', msg);
  parentPort.postMessage({ ok: false, error: msg });
  process.exit(1);
}

if (!records || !Array.isArray(records)) {
  const msg = `Invalid records: expected array, got ${typeof records}`;
  console.error('[PARQUET-WORKER]', msg);
  parentPort.postMessage({ ok: false, error: msg, filePath });
  process.exit(1);
}

if (records.length === 0) {
  parentPort.postMessage({ ok: true, filePath, count: 0, bytes: 0 });
  process.exit(0);
}

async function run() {
  let duckdb;
  
  try {
    // Dynamic import of duckdb
    duckdb = (await import('duckdb')).default;
  } catch (err) {
    const msg = `Failed to load duckdb: ${err.message}`;
    console.error('[PARQUET-WORKER]', msg);
    parentPort.postMessage({ ok: false, error: msg, filePath });
    return;
  }

  // Create in-memory database
  const db = new duckdb.Database(':memory:');
  const conn = db.connect();

  // Promisify query execution
  const runQuery = (sql) => {
    return new Promise((resolve, reject) => {
      conn.run(sql, (err, result) => {
        if (err) reject(err);
        else resolve(result);
      });
    });
  };

  // Promisify all() for queries that return data
  const allQuery = (sql) => {
    return new Promise((resolve, reject) => {
      conn.all(sql, (err, result) => {
        if (err) reject(err);
        else resolve(result);
      });
    });
  };

  try {
    // Ensure output directory exists
    const dir = path.dirname(filePath);
    if (dir && !fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }

    // Use forward slashes for DuckDB SQL (works on all platforms)
    const normalizedFilePath = filePath.replace(/\\/g, '/');

    // Write records as temp JSONL file
    const tempJsonlPath = normalizedFilePath.replace('.parquet', '.temp.jsonl');
    const tempNativePath = tempJsonlPath.replace(/\//g, path.sep);
    
    const lines = records.map(r => JSON.stringify(r));
    fs.writeFileSync(tempNativePath, lines.join('\n') + '\n');

    // Convert to Parquet via DuckDB
    // IMPORTANT: Force stable types for JSON-ish columns to avoid DuckDB inferring JSON and later
    // failing to parse arbitrary strings (seen in validation as "Failed to parse JSON string").
    const readFn = type === 'events'
      ? `read_json_auto('${tempJsonlPath}', columns={
          event_id: 'VARCHAR',
          update_id: 'VARCHAR',
          event_type: 'VARCHAR',
          event_type_original: 'VARCHAR',
          synchronizer_id: 'VARCHAR',
          effective_at: 'VARCHAR',
          recorded_at: 'VARCHAR',
          created_at_ts: 'VARCHAR',
          contract_id: 'VARCHAR',
          template_id: 'VARCHAR',
          package_name: 'VARCHAR',
          migration_id: 'BIGINT',
          signatories: 'VARCHAR[]',
          observers: 'VARCHAR[]',
          acting_parties: 'VARCHAR[]',
          witness_parties: 'VARCHAR[]',
          child_event_ids: 'VARCHAR[]',
          consuming: 'BOOLEAN',
          reassignment_counter: 'BIGINT',
          payload: 'VARCHAR',
          contract_key: 'VARCHAR',
          exercise_result: 'VARCHAR',
          raw_event: 'VARCHAR',
          trace_context: 'VARCHAR'
        }, union_by_name=true)`
      : `read_json_auto('${tempJsonlPath}', columns={
          update_id: 'VARCHAR',
          update_type: 'VARCHAR',
          synchronizer_id: 'VARCHAR',
          effective_at: 'VARCHAR',
          recorded_at: 'VARCHAR',
          record_time: 'VARCHAR',
          command_id: 'VARCHAR',
          workflow_id: 'VARCHAR',
          kind: 'VARCHAR',
          migration_id: 'BIGINT',
          "offset": 'BIGINT',
          event_count: 'INTEGER',
          root_event_ids: 'VARCHAR[]',
          source_synchronizer: 'VARCHAR',
          target_synchronizer: 'VARCHAR',
          unassign_id: 'VARCHAR',
          submitter: 'VARCHAR',
          reassignment_counter: 'BIGINT',
          trace_context: 'VARCHAR',
          update_data: 'VARCHAR'
        }, union_by_name=true)`;

    const sql = `
      COPY (SELECT * FROM ${readFn})
      TO '${normalizedFilePath}'
      (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE ${rowGroupSize});
    `;

    await runQuery(sql);

    // Verify file was created and get size
    const nativeFilePath = filePath;
    if (!fs.existsSync(nativeFilePath)) {
      throw new Error(`Parquet file not created: ${nativeFilePath}`);
    }

    const stats = fs.statSync(nativeFilePath);
    const bytes = stats.size;

    // ========== POST-WRITE VALIDATION ==========
    // Immediately validate the written Parquet file to catch schema issues early
    let validation = { valid: true, rowCount: 0, issues: [] };
    
    try {
      // 1. Verify file is readable and get row count
      const countResult = await allQuery(`
        SELECT COUNT(*) as cnt FROM read_parquet('${normalizedFilePath}')
      `);
      validation.rowCount = Number(countResult[0]?.cnt || 0);
      
      // Check row count matches expected
      if (validation.rowCount !== records.length) {
        validation.issues.push(`Row count mismatch: expected ${records.length}, got ${validation.rowCount}`);
      }
      
      // 2. Verify required columns exist based on type
      const schemaResult = await allQuery(`
        SELECT column_name FROM parquet_schema('${normalizedFilePath}')
      `);
      const columns = new Set(schemaResult.map(r => r.column_name));
      
      const requiredColumns = type === 'events'
        ? ['event_id', 'event_type', 'raw_event']
        : ['update_id', 'update_type', 'update_data'];
      
      for (const col of requiredColumns) {
        if (!columns.has(col)) {
          validation.issues.push(`Missing required column: ${col}`);
        }
      }
      
      // 3. Sample check: verify key columns have data
      if (type === 'events' && validation.rowCount > 0) {
        const sampleCheck = await allQuery(`
          SELECT 
            COUNT(*) FILTER (WHERE raw_event IS NOT NULL) as has_raw,
            COUNT(*) FILTER (WHERE event_type IS NOT NULL) as has_type
          FROM read_parquet('${normalizedFilePath}')
          LIMIT 100
        `);
        const sample = sampleCheck[0] || {};
        if (Number(sample.has_raw || 0) === 0) {
          validation.issues.push('No rows have raw_event data');
        }
      } else if (type === 'updates' && validation.rowCount > 0) {
        const sampleCheck = await allQuery(`
          SELECT 
            COUNT(*) FILTER (WHERE update_data IS NOT NULL) as has_data,
            COUNT(*) FILTER (WHERE record_time IS NOT NULL) as has_time
          FROM read_parquet('${normalizedFilePath}')
          LIMIT 100
        `);
        const sample = sampleCheck[0] || {};
        if (Number(sample.has_data || 0) === 0) {
          validation.issues.push('No rows have update_data');
        }
      }
      
      validation.valid = validation.issues.length === 0;
      
      if (!validation.valid) {
        console.warn(`[PARQUET-WORKER] ⚠️ Validation issues in ${path.basename(filePath)}:`, validation.issues);
      }
      
    } catch (valErr) {
      validation.valid = false;
      validation.issues.push(`Validation query failed: ${valErr.message}`);
      console.error(`[PARQUET-WORKER] ❌ Validation failed for ${path.basename(filePath)}: ${valErr.message}`);
    }

    // Clean up temp file
    if (fs.existsSync(tempNativePath)) {
      fs.unlinkSync(tempNativePath);
    }

    // Close DuckDB
    conn.close();
    db.close();

    // Report success with validation results
    parentPort.postMessage({
      ok: true,
      filePath,
      count: records.length,
      bytes,
      validation,
    });

    process.exit(0);

    process.exit(0);

  } catch (err) {
    // Clean up on error
    const tempJsonlPath = filePath.replace('.parquet', '.temp.jsonl').replace(/\\/g, '/');
    const tempNativePath = tempJsonlPath.replace(/\//g, path.sep);
    
    if (fs.existsSync(tempNativePath)) {
      try { fs.unlinkSync(tempNativePath); } catch {}
    }

    try {
      conn.close();
      db.close();
    } catch {}

    console.error(`[PARQUET-WORKER] Failed: ${err.message}`);
    parentPort.postMessage({
      ok: false,
      error: err.message,
      filePath,
    });

    process.exit(1);
  }
}

run().catch(err => {
  console.error('[PARQUET-WORKER] Fatal error:', err.message);
  parentPort.postMessage({
    ok: false,
    error: `Worker fatal: ${err.message}`,
    filePath,
  });
  process.exit(1);
});
