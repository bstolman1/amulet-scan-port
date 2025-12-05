/**
 * DuckDB Writer Module - Direct Parquet (B3 Hybrid)
 *
 * Drop-in replacement for write-parquet.js.
 * Same exported API:
 *   bufferUpdates, bufferEvents, flushAll, getBufferStats, waitForWrites
 *
 * Differences:
 *   - No JSONL files
 *   - Uses DuckDB in-memory tables
 *   - Flushes directly to Parquet when row/time thresholds hit
 */

import duckdb from 'duckdb';
import { mkdirSync } from 'fs';
import { join, resolve } from 'path';
import { fileURLToPath } from 'url';
import { getPartitionPath } from './parquet-schema.js';

// ---------- Paths / Constants ----------

const __filename = fileURLToPath(import.meta.url);
const __dirname = resolve(__filename, '..');

// project root: .../amulet-scan-port
const ROOT_DIR = resolve(__dirname, '../../');
const DATA_ROOT = join(ROOT_DIR, 'data', 'raw');

// thresholds (can tweak via env vars)
const ROW_FLUSH_THRESHOLD = parseInt(process.env.FLUSH_ROWS || '250000', 10);
const TIME_FLUSH_MS = parseInt(process.env.FLUSH_MS || '30000', 10);

// ---------- DuckDB setup ----------

let db = null;
let conn = null;
let initialized = false;

async function initDuckDB() {
  if (initialized) return;
  db = new duckdb.Database(':memory:');
  conn = db.connect();

  // helper to promisify conn.run
  conn.runAsync = (sql, params = []) =>
    new Promise((resolve, reject) => {
      conn.run(sql, params, err => (err ? reject(err) : resolve()));
    });

  await conn.runAsync(`
    CREATE TABLE IF NOT EXISTS updates_mem (
      update_id       VARCHAR,
      update_type     VARCHAR,
      migration_id    BIGINT,
      synchronizer_id VARCHAR,
      workflow_id     VARCHAR,
      offset          BIGINT,
      record_time     TIMESTAMP,
      effective_at    TIMESTAMP,
      timestamp       TIMESTAMP,
      kind            VARCHAR,
      update_data     VARCHAR
    );
  `);

  await conn.runAsync(`
    CREATE TABLE IF NOT EXISTS events_mem (
      event_id      VARCHAR,
      update_id     VARCHAR,
      event_type    VARCHAR,
      contract_id   VARCHAR,
      template_id   VARCHAR,
      package_name  VARCHAR,
      migration_id  BIGINT,
      timestamp     TIMESTAMP,
      created_at_ts TIMESTAMP,
      signatories   VARCHAR[],
      observers     VARCHAR[],
      payload       VARCHAR
    );
  `);

  initialized = true;
  console.log('🦆 DuckDB writer initialized (in-memory tables ready).');
}

// ---------- In-memory JS buffers (before DuckDB insert) ----------

let updatesBuffer = [];
let eventsBuffer = [];
let lastFlushTs = Date.now();
let pendingWrites = [];
let flushing = false;

function totalBufferedRows() {
  return updatesBuffer.length + eventsBuffer.length;
}

// ---------- Public API ----------

export async function bufferUpdates(rows) {
  if (!rows || !rows.length) return;
  await initDuckDB();

  updatesBuffer.push(...rows);
  await maybeFlush(rows[rows.length - 1].timestamp);
}

export async function bufferEvents(rows) {
  if (!rows || !rows.length) return;
  await initDuckDB();

  eventsBuffer.push(...rows);
  await maybeFlush(rows[rows.length - 1].timestamp);
}

export async function flushAll() {
  await initDuckDB();
  await flushToParquet(new Date());
  return []; // Return empty array for compatibility
}

export function getBufferStats() {
  return {
    updates: updatesBuffer.length,
    events: eventsBuffer.length,
    total: totalBufferedRows(),
    queuedWrites: pendingWrites.length,
    activeWrites: flushing ? 1 : 0,
  };
}

export async function waitForWrites() {
  await Promise.all(pendingWrites);
}

// ---------- Internal helpers ----------

async function maybeFlush(latestTs) {
  const now = Date.now();
  const needsRowFlush = totalBufferedRows() >= ROW_FLUSH_THRESHOLD;
  const needsTimeFlush = now - lastFlushTs >= TIME_FLUSH_MS;

  if ((needsRowFlush || needsTimeFlush) && !flushing) {
    flushing = true;
    const ts = latestTs || new Date();
    const p = flushToParquet(ts)
      .catch(err => {
        console.error('❌ Error flushing to Parquet:', err);
      })
      .finally(() => {
        flushing = false;
        lastFlushTs = Date.now();
      });
    pendingWrites.push(p);
  }
}

async function flushToParquet(latestTs) {
  if (!totalBufferedRows()) return;

  const updateCount = updatesBuffer.length;
  const eventCount = eventsBuffer.length;
  
  console.log(
    `💾 Flushing ${updateCount} updates, ${eventCount} events to Parquet...`
  );

  // Insert current JS buffers into DuckDB tables
  await bulkInsertUpdates(updatesBuffer);
  await bulkInsertEvents(eventsBuffer);

  // Clear JS buffers (data now lives in DuckDB mem tables)
  updatesBuffer = [];
  eventsBuffer = [];

  // Partition path from timestamp
  const partitionPath = getPartitionPath(latestTs || new Date());
  const baseDir = join(DATA_ROOT, partitionPath);
  mkdirSync(baseDir, { recursive: true });

  const tsSuffix = Date.now();
  const updatesFile = join(baseDir, `updates-${tsSuffix}.parquet`);
  const eventsFile = join(baseDir, `events-${tsSuffix}.parquet`);

  // COPY to parquet with big row groups for speed
  if (updateCount > 0) {
    await conn.runAsync(`
      COPY (
        SELECT * FROM updates_mem
      ) TO '${updatesFile}'
      (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 250000);
    `);
  }

  if (eventCount > 0) {
    await conn.runAsync(`
      COPY (
        SELECT * FROM events_mem
      ) TO '${eventsFile}'
      (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 250000);
    `);
  }

  // Truncate mem tables; keep schema
  await conn.runAsync(`DELETE FROM updates_mem;`);
  await conn.runAsync(`DELETE FROM events_mem;`);

  console.log(`✅ Wrote Parquet:
  - ${updatesFile}
  - ${eventsFile}`);
}

// Insert helpers: keep it simple with many VALUES clauses
async function bulkInsertUpdates(rows) {
  if (!rows.length) return;

  const chunks = chunkArray(rows, 5000); // avoid massive single SQL
  for (const chunk of chunks) {
    const values = chunk
      .map(r => {
        const esc = s =>
          s === null || s === undefined
            ? 'NULL'
            : `'${String(s).replace(/'/g, "''")}'`;

        return `(
          ${esc(r.update_id)},
          ${esc(r.update_type)},
          ${r.migration_id ?? 'NULL'},
          ${esc(r.synchronizer_id)},
          ${esc(r.workflow_id)},
          ${r.offset ?? 'NULL'},
          ${r.record_time ? esc(r.record_time.toISOString()) : 'NULL'},
          ${r.effective_at ? esc(r.effective_at.toISOString()) : 'NULL'},
          ${r.timestamp ? esc(r.timestamp.toISOString()) : 'NULL'},
          ${esc(r.kind)},
          ${esc(r.update_data)}
        )`;
      })
      .join(',');

    const sql = `
      INSERT INTO updates_mem (
        update_id,
        update_type,
        migration_id,
        synchronizer_id,
        workflow_id,
        offset,
        record_time,
        effective_at,
        timestamp,
        kind,
        update_data
      ) VALUES ${values};
    `;

    await conn.runAsync(sql);
  }
}

async function bulkInsertEvents(rows) {
  if (!rows.length) return;

  const chunks = chunkArray(rows, 5000);
  for (const chunk of chunks) {
    const values = chunk
      .map(r => {
        const esc = s =>
          s === null || s === undefined
            ? 'NULL'
            : `'${String(s).replace(/'/g, "''")}'`;

        const escArr = arr => {
          if (!arr || !arr.length) return "ARRAY[]::VARCHAR[]";
          const inner = arr
            .map(v => `'${String(v).replace(/'/g, "''")}'`)
            .join(',');
          return `ARRAY[${inner}]::VARCHAR[]`;
        };

        return `(
          ${esc(r.event_id)},
          ${esc(r.update_id)},
          ${esc(r.event_type)},
          ${esc(r.contract_id)},
          ${esc(r.template_id)},
          ${esc(r.package_name)},
          ${r.migration_id ?? 'NULL'},
          ${r.timestamp ? esc(r.timestamp.toISOString()) : 'NULL'},
          ${r.created_at_ts ? esc(r.created_at_ts.toISOString()) : 'NULL'},
          ${escArr(r.signatories)},
          ${escArr(r.observers)},
          ${esc(r.payload)}
        )`;
      })
      .join(',');

    const sql = `
      INSERT INTO events_mem (
        event_id,
        update_id,
        event_type,
        contract_id,
        template_id,
        package_name,
        migration_id,
        timestamp,
        created_at_ts,
        signatories,
        observers,
        payload
      ) VALUES ${values};
    `;

    await conn.runAsync(sql);
  }
}

function chunkArray(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) {
    out.push(arr.slice(i, i + size));
  }
  return out;
}

export default {
  bufferUpdates,
  bufferEvents,
  flushAll,
  getBufferStats,
  waitForWrites,
};
