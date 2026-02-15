/**
 * Parquet Worker - DuckDB In-Memory Writer
 *
 * Supports two modes:
 * 1. Persistent mode (workerData === null): Stays alive, processes jobs via parentPort messages
 * 2. Legacy mode (workerData !== null): Process single job and exit (backward compatibility)
 *
 * Job format:
 * {
 *   type: 'events' | 'updates',
 *   filePath: string,
 *   records: object[],
 *   rowGroupSize: number
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

/**
 * Process a single job (shared between persistent and legacy modes)
 */
async function processJob(job) {
  const {
    type,
    filePath,
    records,
    rowGroupSize = 100000,
  } = job;

  // Validate inputs
  if (!type || !['events', 'updates'].includes(type)) {
    return { ok: false, error: `Invalid type: ${type}`, filePath };
  }

  if (!filePath) {
    return { ok: false, error: 'No filePath provided' };
  }

  if (!records || !Array.isArray(records)) {
    return { ok: false, error: `Invalid records: expected array, got ${typeof records}`, filePath };
  }

  if (records.length === 0) {
    return { ok: true, filePath, count: 0, bytes: 0 };
  }

  let duckdb;
  
  try {
    duckdb = (await import('duckdb')).default;
  } catch (err) {
    return { ok: false, error: `Failed to load duckdb: ${err.message}`, filePath };
  }

  const db = new duckdb.Database(':memory:');
  const conn = db.connect();

  const runQuery = (sql) => {
    return new Promise((resolve, reject) => {
      conn.run(sql, (err, result) => {
        if (err) reject(err);
        else resolve(result);
      });
    });
  };

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

    const normalizedFilePath = filePath.replace(/\\/g, '/');
    const tempJsonlPath = normalizedFilePath.replace('.parquet', '.temp.jsonl');
    const tempNativePath = tempJsonlPath.replace(/\//g, path.sep);
    
    const lines = records.map(r => JSON.stringify(r));
    fs.writeFileSync(tempNativePath, lines.join('\n') + '\n');

    const readFn = type === 'events'
      ? `read_json_auto('${tempJsonlPath}', columns={
          event_id: 'VARCHAR', update_id: 'VARCHAR', event_type: 'VARCHAR', event_type_original: 'VARCHAR',
          synchronizer_id: 'VARCHAR', effective_at: 'VARCHAR', recorded_at: 'VARCHAR', timestamp: 'VARCHAR',
          created_at_ts: 'VARCHAR', contract_id: 'VARCHAR', template_id: 'VARCHAR', package_name: 'VARCHAR',
          migration_id: 'BIGINT', signatories: 'VARCHAR[]', observers: 'VARCHAR[]', acting_parties: 'VARCHAR[]',
          witness_parties: 'VARCHAR[]', child_event_ids: 'VARCHAR[]', choice: 'VARCHAR', interface_id: 'VARCHAR',
          consuming: 'BOOLEAN', reassignment_counter: 'BIGINT', source_synchronizer: 'VARCHAR',
          target_synchronizer: 'VARCHAR', unassign_id: 'VARCHAR', submitter: 'VARCHAR',
          payload: 'VARCHAR', contract_key: 'VARCHAR', exercise_result: 'VARCHAR', raw_event: 'VARCHAR',
          trace_context: 'VARCHAR'
        }, union_by_name=true)`
      : `read_json_auto('${tempJsonlPath}', columns={
          update_id: 'VARCHAR', update_type: 'VARCHAR', synchronizer_id: 'VARCHAR', effective_at: 'VARCHAR',
          recorded_at: 'VARCHAR', record_time: 'VARCHAR', timestamp: 'VARCHAR', command_id: 'VARCHAR',
          workflow_id: 'VARCHAR', kind: 'VARCHAR', migration_id: 'BIGINT', "offset": 'BIGINT',
          event_count: 'INTEGER', root_event_ids: 'VARCHAR[]', source_synchronizer: 'VARCHAR',
          target_synchronizer: 'VARCHAR', unassign_id: 'VARCHAR', submitter: 'VARCHAR',
          reassignment_counter: 'BIGINT', trace_context: 'VARCHAR', update_data: 'VARCHAR'
        }, union_by_name=true)`;

    const sql = `
      COPY (SELECT * FROM ${readFn})
      TO '${normalizedFilePath}'
      (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE ${rowGroupSize});
    `;

    await runQuery(sql);

    const nativeFilePath = filePath;
    if (!fs.existsSync(nativeFilePath)) {
      throw new Error(`Parquet file not created: ${nativeFilePath}`);
    }

    const stats = fs.statSync(nativeFilePath);
    const bytes = stats.size;

    // ========== POST-WRITE VALIDATION ==========
    let validation = { valid: true, rowCount: 0, issues: [] };
    
    try {
      const countResult = await allQuery(`
        SELECT COUNT(*) as cnt FROM read_parquet('${normalizedFilePath}')
      `);
      validation.rowCount = Number(countResult[0]?.cnt || 0);
      
      if (validation.rowCount !== records.length) {
        validation.issues.push(`Row count mismatch: expected ${records.length}, got ${validation.rowCount}`);
      }
      
      const schemaResult = await allQuery(`
        DESCRIBE SELECT * FROM read_parquet('${normalizedFilePath}')
      `);
      const columns = new Set(
        schemaResult
          .map((r) => r.column_name ?? r.name ?? r.column ?? r[Object.keys(r)[0]])
          .filter(Boolean)
          .map((c) => String(c))
      );
      
      const requiredColumns = type === 'events'
        ? ['event_id', 'event_type', 'raw_event']
        : ['update_id', 'update_type', 'update_data'];
      
      for (const col of requiredColumns) {
        if (!columns.has(col)) {
          validation.issues.push(`Missing required column: ${col}`);
        }
      }
      
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

    conn.close();
    db.close();

    return {
      ok: true,
      filePath,
      count: records.length,
      bytes,
      validation,
    };

  } catch (err) {
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
    return {
      ok: false,
      error: err.message,
      filePath,
    };
  }
}

// Determine mode based on workerData
if (workerData === null || workerData === undefined) {
  // PERSISTENT MODE: Stay alive, process jobs via messages
  parentPort.on('message', async (job) => {
    const result = await processJob(job);
    parentPort.postMessage(result);
  });
} else {
  // LEGACY MODE: Process single job from workerData and exit
  processJob(workerData).then((result) => {
    parentPort.postMessage(result);
    process.exit(result.ok ? 0 : 1);
  }).catch((err) => {
    console.error('[PARQUET-WORKER] Fatal error:', err.message);
    parentPort.postMessage({
      ok: false,
      error: `Worker fatal: ${err.message}`,
      filePath: workerData.filePath,
    });
    process.exit(1);
  });
}
