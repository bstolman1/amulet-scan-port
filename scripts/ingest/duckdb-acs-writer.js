/**
 * DuckDB ACS Writer Module - Direct Parquet
 *
 * Drop-in replacement for write-acs-parquet.js.
 * Same exported API:
 *   setSnapshotTime, bufferContracts, flushAll, getBufferStats, clearBuffers
 *
 * Differences:
 *   - No JSONL files
 *   - Uses DuckDB in-memory tables
 *   - Flushes directly to Parquet with ZSTD compression
 */

import duckdb from 'duckdb';
import { mkdirSync } from 'fs';
import { join, resolve } from 'path';
import { fileURLToPath } from 'url';
import { getACSPartitionPath } from './acs-schema.js';

// ---------- Paths / Constants ----------

const __filename = fileURLToPath(import.meta.url);
const __dirname = resolve(__filename, '..');

const ROOT_DIR = resolve(__dirname, '../../');
const DATA_ROOT = join(ROOT_DIR, 'data', 'raw');

// Thresholds (env configurable)
const ROW_FLUSH_THRESHOLD = parseInt(process.env.ACS_FLUSH_ROWS || '50000', 10);
const TIME_FLUSH_MS = parseInt(process.env.ACS_FLUSH_MS || '30000', 10);

// ---------- DuckDB setup ----------

let db = null;
let conn = null;
let initialized = false;

async function initDuckDB() {
  if (initialized) return;
  db = new duckdb.Database(':memory:');
  conn = db.connect();

  conn.runAsync = (sql, params = []) =>
    new Promise((resolve, reject) => {
      conn.run(sql, params, err => (err ? reject(err) : resolve()));
    });

  await conn.runAsync(`
    CREATE TABLE IF NOT EXISTS contracts_mem (
      contract_id    VARCHAR,
      template_id    VARCHAR,
      package_name   VARCHAR,
      module_name    VARCHAR,
      entity_name    VARCHAR,
      migration_id   BIGINT,
      record_time    TIMESTAMP,
      snapshot_time  TIMESTAMP,
      signatories    VARCHAR[],
      observers      VARCHAR[],
      payload        VARCHAR
    );
  `);

  initialized = true;
  console.log('🦆 DuckDB ACS writer initialized.');
}

// ---------- State ----------

let contractsBuffer = [];
let currentSnapshotTime = null;
let lastFlushTs = Date.now();
let pendingWrites = [];
let flushing = false;

// ---------- Public API ----------

export function setSnapshotTime(time) {
  currentSnapshotTime = time;
}

export async function bufferContracts(contracts) {
  if (!contracts || !contracts.length) return;
  await initDuckDB();

  contractsBuffer.push(...contracts);
  await maybeFlush();
}

export async function flushAll() {
  await initDuckDB();
  await flushToParquet();
  return [];
}

export function getBufferStats() {
  return {
    contracts: contractsBuffer.length,
    total: contractsBuffer.length,
    queuedWrites: pendingWrites.length,
    activeWrites: flushing ? 1 : 0,
  };
}

export function clearBuffers() {
  contractsBuffer = [];
  currentSnapshotTime = null;
}

export async function waitForWrites() {
  await Promise.all(pendingWrites);
}

// ---------- Internal helpers ----------

async function maybeFlush() {
  const now = Date.now();
  const needsRowFlush = contractsBuffer.length >= ROW_FLUSH_THRESHOLD;
  const needsTimeFlush = now - lastFlushTs >= TIME_FLUSH_MS;

  if ((needsRowFlush || needsTimeFlush) && !flushing) {
    flushing = true;
    const p = flushToParquet()
      .catch(err => {
        console.error('❌ Error flushing ACS to Parquet:', err);
      })
      .finally(() => {
        flushing = false;
        lastFlushTs = Date.now();
      });
    pendingWrites.push(p);
  }
}

async function flushToParquet() {
  if (contractsBuffer.length === 0) return;

  const count = contractsBuffer.length;
  console.log(`💾 Flushing ${count} ACS contracts to Parquet...`);

  // Insert into DuckDB
  await bulkInsertContracts(contractsBuffer);
  contractsBuffer = [];

  // Partition path
  const timestamp = currentSnapshotTime || new Date();
  const partitionPath = getACSPartitionPath(timestamp);
  const baseDir = join(DATA_ROOT, partitionPath);
  mkdirSync(baseDir, { recursive: true });

  const tsSuffix = Date.now();
  const contractsFile = join(baseDir, `contracts-${tsSuffix}.parquet`);

  // COPY to parquet
  await conn.runAsync(`
    COPY (
      SELECT * FROM contracts_mem
    ) TO '${contractsFile}'
    (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000);
  `);

  // Truncate mem table
  await conn.runAsync(`DELETE FROM contracts_mem;`);

  console.log(`✅ Wrote ${count} contracts to ${contractsFile}`);
}

async function bulkInsertContracts(rows) {
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
          ${esc(r.contract_id)},
          ${esc(r.template_id)},
          ${esc(r.package_name)},
          ${esc(r.module_name)},
          ${esc(r.entity_name)},
          ${r.migration_id ?? 'NULL'},
          ${r.record_time ? esc(r.record_time.toISOString()) : 'NULL'},
          ${r.snapshot_time ? esc(r.snapshot_time.toISOString()) : 'NULL'},
          ${escArr(r.signatories)},
          ${escArr(r.observers)},
          ${esc(r.payload)}
        )`;
      })
      .join(',');

    const sql = `
      INSERT INTO contracts_mem (
        contract_id,
        template_id,
        package_name,
        module_name,
        entity_name,
        migration_id,
        record_time,
        snapshot_time,
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
  setSnapshotTime,
  bufferContracts,
  flushAll,
  getBufferStats,
  clearBuffers,
  waitForWrites,
};
