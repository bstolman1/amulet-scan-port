/**
 * DuckDB Writer Module - Direct Parquet (Optimized)
 *
 * Writes directly to Parquet via DuckDB without in-memory tables.
 * Flow: JS buffer → temp JSONL → DuckDB COPY to Parquet
 */

import duckdb from 'duckdb';
import { mkdirSync, writeFileSync, unlinkSync } from 'fs';
import { join, resolve } from 'path';
import { tmpdir } from 'os';
import { fileURLToPath } from 'url';
import { getPartitionPath } from './parquet-schema.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = resolve(__filename, '..');

const ROOT_DIR = resolve(__dirname, '../../');
const DATA_ROOT = join(ROOT_DIR, 'data', 'raw');

const ROW_FLUSH_THRESHOLD = parseInt(process.env.FLUSH_ROWS || '100000', 10);
const TIME_FLUSH_MS = parseInt(process.env.FLUSH_MS || '30000', 10);

// Single DuckDB connection for COPY commands
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
    console.log('🦆 DuckDB writer initialized.');
  }
  return conn;
}

// Buffers
let updatesBuffer = [];
let eventsBuffer = [];
let lastFlushTs = Date.now();
let flushing = false;
let pendingWrites = [];

function totalBufferedRows() {
  return updatesBuffer.length + eventsBuffer.length;
}

// ---------- Public API ----------

export async function bufferUpdates(rows) {
  if (!rows || !rows.length) return;
  updatesBuffer.push(...rows);
  await maybeFlush(rows[rows.length - 1].timestamp);
}

export async function bufferEvents(rows) {
  if (!rows || !rows.length) return;
  eventsBuffer.push(...rows);
  await maybeFlush(rows[rows.length - 1].timestamp);
}

export async function flushAll() {
  await flushToParquet(new Date());
  return [];
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

// ---------- Internal ----------

async function maybeFlush(latestTs) {
  const now = Date.now();
  const needsRowFlush = totalBufferedRows() >= ROW_FLUSH_THRESHOLD;
  const needsTimeFlush = now - lastFlushTs >= TIME_FLUSH_MS;

  if ((needsRowFlush || needsTimeFlush) && !flushing) {
    flushing = true;
    const ts = latestTs || new Date();
    const p = flushToParquet(ts)
      .catch(err => console.error('❌ Error flushing to Parquet:', err))
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
  console.log(`💾 Flushing ${updateCount} updates, ${eventCount} events to Parquet...`);

  const partitionPath = getPartitionPath(latestTs || new Date());
  const baseDir = join(DATA_ROOT, partitionPath);
  mkdirSync(baseDir, { recursive: true });

  const tsSuffix = Date.now();
  const c = getConn();

  // Write updates
  if (updateCount > 0) {
    const tmpFile = join(tmpdir(), `upd-${tsSuffix}.jsonl`);
    const outFile = join(baseDir, `updates-${tsSuffix}.parquet`);
    
    // Fast JSONL write
    writeFileSync(tmpFile, updatesBuffer.map(r => JSON.stringify(r)).join('\n'), 'utf8');
    updatesBuffer = [];

    await c.runAsync(`
      COPY (
        SELECT * FROM read_json_auto('${tmpFile.replace(/\\/g, '/')}')
      ) TO '${outFile.replace(/\\/g, '/')}'
      (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000);
    `);

    safeUnlink(tmpFile);
  }

  // Write events
  if (eventCount > 0) {
    const tmpFile = join(tmpdir(), `evt-${tsSuffix}.jsonl`);
    const outFile = join(baseDir, `events-${tsSuffix}.parquet`);

    writeFileSync(tmpFile, eventsBuffer.map(r => JSON.stringify(r)).join('\n'), 'utf8');
    eventsBuffer = [];

    await c.runAsync(`
      COPY (
        SELECT * FROM read_json_auto('${tmpFile.replace(/\\/g, '/')}')
      ) TO '${outFile.replace(/\\/g, '/')}'
      (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000);
    `);

    safeUnlink(tmpFile);
  }

  console.log(`✅ Wrote Parquet to ${baseDir}`);
}

function safeUnlink(filePath) {
  try {
    unlinkSync(filePath);
  } catch (err) {
    // Ignore - Windows will clean up
  }
}

export default {
  bufferUpdates,
  bufferEvents,
  flushAll,
  getBufferStats,
  waitForWrites,
};
