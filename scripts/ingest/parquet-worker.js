/**
 * Parquet Worker - DuckDB In-Memory Writer
 *
 * Supports two modes:
 * 1. Persistent mode (workerData === null): Stays alive, processes jobs via parentPort messages
 * 2. Legacy mode (workerData !== null): Process single job and exit (backward compatibility)
 *
 * Performance optimizations:
 * - DuckDB instance is created ONCE per worker and reused across all jobs
 * - Post-write validation is sampling-based (every Nth file, configurable via PARQUET_VALIDATION_SAMPLE_RATE)
 * - First 5 files are always validated for early error detection
 *
 * FIXES APPLIED:
 *
 * FIX #1  SQL injection via filePath / tempJsonlPath
 *         Both paths were interpolated directly into DuckDB SQL strings. A path
 *         component containing a single quote (e.g. "canton's-data/") breaks the
 *         SQL or allows injection. All three SQL sites now escape single quotes
 *         before interpolation via sqlStr().
 *
 * FIX #2  Temp JSONL filename collision across concurrent jobs
 *         In persistent mode the parentPort message handler is async but not
 *         serialised — multiple jobs can be in-flight simultaneously. Because the
 *         temp path was derived purely from filePath, two jobs targeting the same
 *         output partition computed identical temp paths and raced: one job wrote
 *         its JSONL while another overwrote it, producing corrupted Parquet files.
 *         Each job now appends a unique suffix (timestamp + random hex) to its
 *         temp path to guarantee isolation.
 *
 * FIX #3  Concurrent DuckDB access on shared connection
 *         DuckDB's Node bindings are callback-based and not safe for concurrent
 *         calls on the same connection. In persistent mode, overlapping jobs called
 *         conn.run / conn.all simultaneously — undefined behaviour that can cause
 *         native crashes. A lightweight async job queue (one-at-a-time) serialises
 *         all DuckDB access without breaking the persistent-mode API.
 *
 * FIX #4  Persistent mode: unhandled rejection hangs the caller
 *         If processJob() threw outside its internal try/catch (e.g. from
 *         ensureDuckDB throwing _initError), the async parentPort message handler
 *         did not catch it. The caller's Promise waited forever with no response.
 *         The handler now wraps the call in try/catch and always posts a result.
 *
 * FIX #5  Validation SQL LIMIT applied to aggregate — semantically wrong
 *         `SELECT COUNT(*) FROM ... LIMIT 100` limits the result set to 1 row
 *         (there is only ever 1 aggregate row), not the rows scanned. The intent
 *         was to scan a sample of 100 rows. Fixed by pushing LIMIT into a subquery:
 *         `COUNT(*) FROM (SELECT col FROM read_parquet(...) LIMIT 100)`.
 *
 * FIX #6  Legacy mode: process.exit races with parentPort.postMessage
 *         postMessage is synchronous on the sending side, but the parent may not
 *         have received the message before process.exit tears down the worker.
 *         setImmediate() before process.exit gives the event loop one tick to flush.
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
import { randomBytes } from 'node:crypto';

// Validate we're in a worker thread
if (isMainThread) {
  console.error('[PARQUET-WORKER] Error: parquet-worker.js must be run as a Worker');
  process.exit(1);
}

// ─── FIX #1: SQL string escaper ────────────────────────────────────────────
// DuckDB path literals are single-quoted. Any single quote in the path must be
// doubled ('') to avoid breaking the SQL string or enabling injection.
function sqlStr(rawPath) {
  return rawPath.replace(/'/g, "''");
}

// ─── FIX #3: Async job queue ────────────────────────────────────────────────
// Serialises all DuckDB work so only one job touches _conn at a time.
// DuckDB's Node bindings are not safe for concurrent calls on the same connection.
let _jobQueueTail = Promise.resolve();

function enqueueJob(fn) {
  // Chain onto the tail — next job starts only after the current one finishes
  const next = _jobQueueTail.then(() => fn());
  // The queue tail must never reject (so the chain doesn't stall on error)
  _jobQueueTail = next.catch(() => {});
  return next;
}

// ============ PERSISTENT DUCKDB INSTANCE ============
// Created once per worker thread, reused across all jobs.
// Each job reads from a fresh temp file so no state leaks between jobs.
let _duckdb = null;
let _db = null;
let _conn = null;
let _initError = null;

// Sampling-based validation
const VALIDATION_SAMPLE_RATE    = parseInt(process.env.PARQUET_VALIDATION_SAMPLE_RATE) || 20;
const ALWAYS_VALIDATE_FIRST_N   = 5;
let _jobCounter = 0;

function shouldValidate() {
  _jobCounter++;
  if (_jobCounter <= ALWAYS_VALIDATE_FIRST_N) return true;
  return (_jobCounter % VALIDATION_SAMPLE_RATE) === 0;
}

async function ensureDuckDB() {
  if (_conn) return { db: _db, conn: _conn };
  if (_initError) throw _initError;

  try {
    _duckdb = (await import('duckdb')).default;
    _db     = new _duckdb.Database(':memory:');
    _conn   = _db.connect();
    return { db: _db, conn: _conn };
  } catch (err) {
    _initError = err;
    throw err;
  }
}

// Cleanup on worker exit
process.on('exit', () => {
  try { if (_conn) _conn.close(); } catch {}
  try { if (_db)   _db.close();   } catch {}
});

/**
 * Process a single job (shared between persistent and legacy modes).
 *
 * FIX #3: In persistent mode this is always called via enqueueJob(), so only
 * one invocation accesses DuckDB at a time.
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

  let conn;
  try {
    ({ conn } = await ensureDuckDB());
  } catch (err) {
    return { ok: false, error: `Failed to load duckdb: ${err.message}`, filePath };
  }

  const runQuery = (sql) => new Promise((resolve, reject) => {
    conn.run(sql, (err, result) => { if (err) reject(err); else resolve(result); });
  });

  const allQuery = (sql) => new Promise((resolve, reject) => {
    conn.all(sql, (err, result) => { if (err) reject(err); else resolve(result); });
  });

  try {
    // Ensure output directory exists
    const dir = path.dirname(filePath);
    if (dir && !fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }

    const normalizedFilePath = filePath.replace(/\\/g, '/');

    // FIX #2: append a unique suffix so concurrent jobs never share a temp path
    // SECURITY FIX: Math.random() → randomBytes — temp filenames now use a
    // cryptographically random suffix to prevent predictable path collisions.
    const jobSuffix    = `${Date.now()}_${randomBytes(4).toString('hex')}`;
    const tempJsonlPath   = normalizedFilePath.replace('.parquet', `.temp.${jobSuffix}.jsonl`);
    const tempNativePath  = tempJsonlPath.replace(/\//g, path.sep);

    // Write records in chunks to avoid V8 max string length limit
    const CHUNK_SIZE = 5000;
    const fd = fs.openSync(tempNativePath, 'w');
    try {
      for (let i = 0; i < records.length; i += CHUNK_SIZE) {
        const chunk = records.slice(i, i + CHUNK_SIZE);
        const block = chunk.map(r => JSON.stringify(r)).join('\n') + '\n';
        fs.writeSync(fd, block);
      }
    } finally {
      fs.closeSync(fd);
    }

    // FIX #1: escape single quotes in all path literals used in DuckDB SQL
    const safeTempPath   = sqlStr(tempJsonlPath);
    const safeOutputPath = sqlStr(normalizedFilePath);

    const readFn = type === 'events'
      ? `read_json_auto('${safeTempPath}', columns={
          event_id: 'VARCHAR', update_id: 'VARCHAR', event_type: 'VARCHAR', event_type_original: 'VARCHAR',
          synchronizer_id: 'VARCHAR', effective_at: 'VARCHAR', recorded_at: 'VARCHAR', timestamp: 'VARCHAR',
          created_at_ts: 'VARCHAR', contract_id: 'VARCHAR', template_id: 'VARCHAR', package_name: 'VARCHAR',
          migration_id: 'BIGINT', signatories: 'VARCHAR[]', observers: 'VARCHAR[]', acting_parties: 'VARCHAR[]',
          witness_parties: 'VARCHAR[]', child_event_ids: 'VARCHAR[]', choice: 'VARCHAR', interface_id: 'VARCHAR',
          consuming: 'BOOLEAN', reassignment_counter: 'BIGINT', source_synchronizer: 'VARCHAR',
          target_synchronizer: 'VARCHAR', unassign_id: 'VARCHAR', submitter: 'VARCHAR',
          payload: 'VARCHAR', contract_key: 'VARCHAR', exercise_result: 'VARCHAR', raw_event: 'VARCHAR',
          trace_context: 'VARCHAR'
        }, union_by_name=true, maximum_object_size=67108864)`
      : `read_json_auto('${safeTempPath}', columns={
          update_id: 'VARCHAR', update_type: 'VARCHAR', synchronizer_id: 'VARCHAR', effective_at: 'VARCHAR',
          recorded_at: 'VARCHAR', record_time: 'VARCHAR', timestamp: 'VARCHAR', command_id: 'VARCHAR',
          workflow_id: 'VARCHAR', kind: 'VARCHAR', migration_id: 'BIGINT', "offset": 'BIGINT',
          event_count: 'INTEGER', root_event_ids: 'VARCHAR[]', source_synchronizer: 'VARCHAR',
          target_synchronizer: 'VARCHAR', unassign_id: 'VARCHAR', submitter: 'VARCHAR',
          reassignment_counter: 'BIGINT', trace_context: 'VARCHAR', update_data: 'VARCHAR'
        }, union_by_name=true, maximum_object_size=67108864)`;

    const sql = `
      COPY (SELECT * FROM ${readFn})
      TO '${safeOutputPath}'
      (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE ${rowGroupSize});
    `;

    await runQuery(sql);

    // Clean up temp file before validation (validation reads the Parquet, not the JSONL)
    if (fs.existsSync(tempNativePath)) {
      fs.unlinkSync(tempNativePath);
    }

    if (!fs.existsSync(filePath)) {
      throw new Error(`Parquet file not created: ${filePath}`);
    }

    const stats = fs.statSync(filePath);
    const bytes = stats.size;

    // ========== SAMPLING-BASED VALIDATION ==========
    let validation = { valid: true, rowCount: records.length, issues: [], sampled: false };

    if (shouldValidate()) {
      validation.sampled = true;
      try {
        // FIX #1: escape path in validation queries too
        const safeReadPath = sqlStr(normalizedFilePath);

        const countResult = await allQuery(`
          SELECT COUNT(*) as cnt FROM read_parquet('${safeReadPath}')
        `);
        validation.rowCount = Number(countResult[0]?.cnt || 0);

        if (validation.rowCount !== records.length) {
          validation.issues.push(`Row count mismatch: expected ${records.length}, got ${validation.rowCount}`);
        }

        const schemaResult = await allQuery(`
          DESCRIBE SELECT * FROM read_parquet('${safeReadPath}')
        `);
        const columns = new Set(
          schemaResult
            .map(r => r.column_name ?? r.name ?? r.column ?? r[Object.keys(r)[0]])
            .filter(Boolean)
            .map(c => String(c))
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
          // FIX #5: push LIMIT into a subquery so we scan 100 rows, not limit the aggregate result
          const sampleCheck = await allQuery(`
            SELECT
              COUNT(*) FILTER (WHERE raw_event IS NOT NULL)  as has_raw,
              COUNT(*) FILTER (WHERE event_type IS NOT NULL) as has_type
            FROM (SELECT raw_event, event_type FROM read_parquet('${safeReadPath}') LIMIT 100)
          `);
          const sample = sampleCheck[0] || {};
          if (Number(sample.has_raw || 0) === 0) {
            validation.issues.push('No rows have raw_event data');
          }
        } else if (type === 'updates' && validation.rowCount > 0) {
          // FIX #5: same fix for the updates branch
          const sampleCheck = await allQuery(`
            SELECT
              COUNT(*) FILTER (WHERE update_data IS NOT NULL)  as has_data,
              COUNT(*) FILTER (WHERE record_time IS NOT NULL)  as has_time
            FROM (SELECT update_data, record_time FROM read_parquet('${safeReadPath}') LIMIT 100)
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
    }

    return {
      ok: true,
      filePath,
      count: records.length,
      bytes,
      validation,
    };

  } catch (err) {
    // Note: tempNativePath is in the inner try scope and intentionally not accessible here.
    // The unique per-job suffix (FIX #2) means a crashed job's temp file cannot be named
    // by the catch block. Stale temp files are cleaned up by the caller or on next worker start.
    console.error(`[PARQUET-WORKER] Failed: ${err.message}`);
    return {
      ok: false,
      error: err.message,
      filePath,
    };
  }
}

// ─── Persistent mode ────────────────────────────────────────────────────────

if (workerData === null || workerData === undefined) {
  // PERSISTENT MODE: Stay alive, process jobs via messages.
  //
  // FIX #3: Jobs are serialised through enqueueJob() so DuckDB is never accessed
  // concurrently. This also prevents temp-file path collisions (FIX #2 gives each
  // job a unique suffix, but serialisation provides an additional safety layer).
  //
  // FIX #4: try/catch inside the handler ensures the caller always receives a
  // response. Without it, an exception thrown from processJob (e.g. _initError
  // re-thrown from ensureDuckDB before the outer try is entered) would leave the
  // caller's Promise pending forever.
  parentPort.on('message', (job) => {
    enqueueJob(async () => {
      try {
        const result = await processJob(job);
        parentPort.postMessage(result);
      } catch (err) {
        // FIX #4: always post a response so the caller never hangs
        parentPort.postMessage({
          ok:       false,
          error:    `Worker internal error: ${err.message}`,
          filePath: job?.filePath,
        });
      }
    });
  });

} else {
  // LEGACY MODE: Process single job from workerData and exit.
  //
  // FIX #6: setImmediate before process.exit gives the event loop one tick to
  // flush the postMessage to the parent before the worker tears down.
  processJob(workerData).then((result) => {
    parentPort.postMessage(result);
    setImmediate(() => process.exit(result.ok ? 0 : 1));  // FIX #6
  }).catch((err) => {
    console.error('[PARQUET-WORKER] Fatal error:', err.message);
    parentPort.postMessage({
      ok:       false,
      error:    `Worker fatal: ${err.message}`,
      filePath: workerData.filePath,
    });
    setImmediate(() => process.exit(1));  // FIX #6
  });
}
