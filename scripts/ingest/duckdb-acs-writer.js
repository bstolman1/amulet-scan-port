/**
 * DuckDB ACS Writer Module - Direct Parquet (Optimized)
 *
 * Writes directly to Parquet via DuckDB without in-memory tables.
 * Flow: JS buffer → temp JSONL → DuckDB COPY to Parquet
 */

import duckdb from 'duckdb';
import { mkdirSync, writeFileSync, unlinkSync } from 'fs';
import { join, resolve } from 'path';
import { tmpdir } from 'os';
import { fileURLToPath } from 'url';
import { getACSPartitionPath } from './acs-schema.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = resolve(__filename, '..');

const ROOT_DIR = resolve(__dirname, '../../');
const DATA_ROOT = join(ROOT_DIR, 'data', 'raw');

const ROW_FLUSH_THRESHOLD = parseInt(process.env.ACS_FLUSH_ROWS || '50000', 10);
const TIME_FLUSH_MS = parseInt(process.env.ACS_FLUSH_MS || '30000', 10);

// Single DuckDB connection
let db = null;
let conn = null;

function getConn() {
  if (!conn) {
    db = new duckdb.Database(':memory:');
    conn = db.connect();
    conn.runAsync = (sql) =>
      new Promise((resolve, reject) => {
        conn.run(sql, (err) => (err ? reject(err) : resolve()));
      });
    console.log('🦆 DuckDB ACS writer initialized.');
  }
  return conn;
}

// State
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
  contractsBuffer.push(...contracts);
  await maybeFlush();
}

export async function flushAll() {
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

// ---------- Internal ----------

async function maybeFlush() {
  const now = Date.now();
  const needsRowFlush = contractsBuffer.length >= ROW_FLUSH_THRESHOLD;
  const needsTimeFlush = now - lastFlushTs >= TIME_FLUSH_MS;

  if ((needsRowFlush || needsTimeFlush) && !flushing) {
    flushing = true;
    const p = flushToParquet()
      .catch(err => console.error('❌ Error flushing ACS to Parquet:', err))
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

  const timestamp = currentSnapshotTime || new Date();
  const partitionPath = getACSPartitionPath(timestamp);
  const baseDir = join(DATA_ROOT, partitionPath);
  mkdirSync(baseDir, { recursive: true });

  const tsSuffix = Date.now();
  const tmpFile = join(tmpdir(), `acs-${tsSuffix}.jsonl`);
  const outFile = join(baseDir, `contracts-${tsSuffix}.parquet`);

  // Fast JSONL write
  writeFileSync(tmpFile, contractsBuffer.map(r => JSON.stringify(r)).join('\n'), 'utf8');
  contractsBuffer = [];

  const c = getConn();
  await c.runAsync(`
    COPY (
      SELECT * FROM read_json_auto('${tmpFile.replace(/\\/g, '/')}')
    ) TO '${outFile.replace(/\\/g, '/')}'
    (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000);
  `);

  safeUnlink(tmpFile);
  console.log(`✅ Wrote ${count} contracts to ${outFile}`);
}

function safeUnlink(filePath) {
  try {
    unlinkSync(filePath);
  } catch (err) {
    // Ignore - Windows will clean up
  }
}

export default {
  setSnapshotTime,
  bufferContracts,
  flushAll,
  getBufferStats,
  clearBuffers,
  waitForWrites,
};
