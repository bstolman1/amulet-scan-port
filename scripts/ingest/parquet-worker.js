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
    const sql = `
      COPY (SELECT * FROM read_json_auto('${tempJsonlPath}'))
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

    // Clean up temp file
    if (fs.existsSync(tempNativePath)) {
      fs.unlinkSync(tempNativePath);
    }

    // Close DuckDB
    conn.close();
    db.close();

    // Report success
    parentPort.postMessage({
      ok: true,
      filePath,
      count: records.length,
      bytes,
    });

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
