/**
 * ACS Parquet Writer Module with GCS Upload
 *
 * Writes ACS snapshot data directly to Parquet files using DuckDB Node.js bindings.
 * This eliminates the need for a separate materialization step.
 *
 * GCS Mode (when GCS_BUCKET is set):
 * 1. Writes Parquet files to /tmp/ledger_raw/acs (ephemeral scratch space)
 * 2. Enqueues each file for async upload via gcs-upload-queue.js (non-blocking)
 * 3. Queue deletes local file after successful upload
 * 4. Keeps disk usage flat regardless of total data volume
 *
 * Local Mode (default):
 * - Writes to DATA_DIR/raw/acs
 *
 * Drop-in replacement for write-acs-jsonl.js with same API surface.
 */

import { mkdirSync, existsSync, readdirSync, rmSync, statSync, writeFileSync, unlinkSync } from 'fs';
import { join, dirname, sep } from 'path';
import { fileURLToPath } from 'url';
import { randomBytes } from 'crypto';
import duckdb from 'duckdb';
import { getACSPartitionPath } from './acs-schema.js';
import {
  isGCSMode,
  getTmpRawDir,
  getRawDir,
  getBaseDataDir,
  ensureTmpDir,
} from './path-utils.js';
import {
  initGCS,
  getGCSPath,
} from './gcs-upload.js';
// FIX: replaced uploadAndCleanupSync (blocking sync upload) with the async
// upload queue. uploadAndCleanupSync blocked the Node.js event loop for the
// entire duration of each GCS upload — stalling cron triggers, status logs,
// and signal handling during large snapshots. The queue uploads concurrently
// in the background without blocking the write loop.
import {
  enqueueUpload,
  getUploadQueue,
  drainUploads,
} from './gcs-upload-queue.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Configuration — defer GCS_MODE check to avoid errors during module import.
// FIX: removed BASE_DATA_DIR — it was assigned in ensureModuleInitialized()
// but never read anywhere in the module. Keeping a dead variable is misleading.
let GCS_MODE = null;
let DATA_DIR = null;
const MAX_ROWS_PER_FILE = parseInt(process.env.ACS_MAX_ROWS_PER_FILE) || 10000;
const MAX_SNAPSHOTS_PER_MIGRATION = parseInt(process.env.MAX_SNAPSHOTS_PER_MIGRATION) || 2;
// FIX: ROW_GROUP_SIZE was hardcoded to 100000. Making it env-configurable
// matches the pattern used in parquet-worker.js (PARQUET_ROW_GROUP) and
// lets operators tune memory/read performance without code changes.
const ROW_GROUP_SIZE = parseInt(process.env.ACS_ROW_GROUP_SIZE) || 100000;

let moduleInitialized = false;

/**
 * Initialize module configuration (called lazily on first use)
 */
function ensureModuleInitialized() {
  if (moduleInitialized) return;
  try {
    GCS_MODE = isGCSMode();
    DATA_DIR = GCS_MODE ? getTmpRawDir() : getRawDir();
    if (GCS_MODE) {
      initGCS();
      ensureTmpDir();
      console.log(`☁️ [ACS-Parquet] GCS mode enabled`);
      console.log(`☁️ [ACS-Parquet] Local scratch: ${DATA_DIR}`);
      console.log(`☁️ [ACS-Parquet] GCS destination: gs://${process.env.GCS_BUCKET}/raw/`);
    } else {
      console.log(`📂 [ACS-Parquet] Local mode - output directory: ${DATA_DIR}`);
    }
    moduleInitialized = true;
  } catch (err) {
    console.error(`❌ [ACS-Parquet] Initialization failed: ${err.message}`);
    console.error(err.stack);
    throw err;
  }
}

// In-memory buffer for batching
let contractsBuffer = [];
let currentSnapshotTime = null;
let currentMigrationId = null;
let fileCounter = 0;

// Stats tracking
let totalContractsWritten = 0;
let totalFilesWritten = 0;
let totalFilesEnqueued = 0;   // renamed from totalFilesUploaded: reflects queue semantics

// DuckDB instance for Parquet writing
let db = null;
let conn = null;

/**
 * Initialize DuckDB connection
 */
function ensureDbInitialized() {
  ensureModuleInitialized();
  if (db && conn) return;
  db = new duckdb.Database(':memory:');
  conn = db.connect();
  console.log(`🦆 [ACS-Parquet] DuckDB initialized for Parquet writing`);
}

/**
 * Ensure directory exists (Windows-safe manual fallback)
 */
function ensureDir(dirPath) {
  const normalizedPath = dirPath.split('/').join(sep);
  try {
    if (!existsSync(normalizedPath)) {
      mkdirSync(normalizedPath, { recursive: true });
    }
  } catch (err) {
    if (err.code !== 'EEXIST') {
      const parts = normalizedPath.split(sep).filter(Boolean);
      let current = parts[0].includes(':') ? parts[0] + sep : sep;
      for (let i = parts[0].includes(':') ? 1 : 0; i < parts.length; i++) {
        current = join(current, parts[i]);
        try {
          if (!existsSync(current)) mkdirSync(current);
        } catch (e) {
          if (e.code !== 'EEXIST') throw e;
        }
      }
    }
  }
}

/**
 * Enqueue a completed Parquet file for async GCS upload.
 *
 * FIX: replaced the old uploadToGCSIfEnabled() which called uploadAndCleanupSync()
 * — a blocking synchronous upload that held the Node event loop for the full upload
 * duration. The queue returns immediately and handles upload + local deletion in
 * the background, allowing the write loop to continue.
 */
function enqueueGCSUpload(localPath, partition, fileName) {
  if (!moduleInitialized || !GCS_MODE) return;
  const relativePath = join(partition, fileName).replace(/\\/g, '/');
  const gcsPath = getGCSPath(relativePath);
  enqueueUpload(localPath, gcsPath);
  totalFilesEnqueued++;
}

/**
 * Escape single quotes in a filesystem path for safe DuckDB SQL interpolation.
 *
 * DuckDB does not support parameterized queries for filenames in COPY/read_json_auto.
 * The paths used here are always internally constructed (DATA_DIR + partition + filename),
 * so injection via external input is not possible in normal operation. However, if
 * DATA_DIR contains a single quote (unusual but possible on some systems), the SQL
 * would silently break or behave incorrectly.
 *
 * This escaping is the standard SQL literal escape: ' → ''
 */
function escapeSqlPath(p) {
  return p.replace(/'/g, "''");
}

/**
 * Write contracts to Parquet file via DuckDB Node.js bindings.
 */
async function writeToParquet(contracts, filePath, partition, fileName) {
  if (contracts.length === 0) return null;
  ensureDbInitialized();

  // FIX: build tempJsonlPath from dirname + random name instead of
  // filePath.replace('.parquet', ...). The replace() substitutes the FIRST
  // occurrence of '.parquet' in the string — if the partition path itself
  // ever contained '.parquet' (unlikely but possible), the directory portion
  // of the path would be corrupted, placing the temp file in the wrong location
  // or causing a write error.
  const rand = randomBytes(4).toString('hex');
  const tempJsonlPath = join(dirname(filePath), `temp-${rand}.jsonl`);

  // DuckDB requires forward-slash paths on all platforms
  const parquetPath = filePath.replace(/\\/g, '/');
  const jsonlPath   = tempJsonlPath.replace(/\\/g, '/');

  try {
    const t0 = Date.now();
    console.log(`🧾 [ACS-Parquet] Writing temp JSONL (${contracts.length} rows) -> ${tempJsonlPath}`);

    const lines = contracts.map(c => JSON.stringify(c));
    writeFileSync(tempJsonlPath, lines.join('\n') + '\n');

    const jsonlBytes = statSync(tempJsonlPath).size;
    console.log(`🧾 [ACS-Parquet] Temp JSONL size: ${(jsonlBytes / 1024 / 1024).toFixed(2)} MB`);

    // FIX: escape single quotes in both paths before SQL interpolation.
    // DuckDB has no native bind-parameter API for COPY filenames; escaping
    // is the correct mitigation. Paths are always internally constructed so
    // real injection risk is very low, but this defends against DATA_DIR
    // values that happen to contain a quote character.
    const safJsonl   = escapeSqlPath(jsonlPath);
    const safParquet = escapeSqlPath(parquetPath);

    const sql = `
      COPY (
        SELECT * FROM read_json_auto('${safJsonl}', ignore_errors=true)
      ) TO '${safParquet}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE ${ROW_GROUP_SIZE});
    `;

    const duckdbTimeoutMs = parseInt(process.env.DUCKDB_COPY_TIMEOUT_MS) || 10 * 60 * 1000;
    console.log(`🦆 [ACS-Parquet] DuckDB COPY -> ${filePath} (timeout: ${duckdbTimeoutMs}ms)`);

    // FIX: the original code let the timeout timer run forever after a successful
    // DuckDB COPY — it fired reject() on an already-resolved Promise (harmless
    // to correctness) but kept a live timer in Node's event loop for up to 10
    // minutes per file, preventing clean process exit until all timers elapsed.
    // Fix: store the handle and clear it in both the success and failure paths.
    await new Promise((resolve, reject) => {
      let timedOut = false;
      const timer = setTimeout(() => {
        timedOut = true;
        reject(new Error(`DuckDB COPY timed out after ${duckdbTimeoutMs}ms`));
      }, duckdbTimeoutMs);

      conn.run(sql, (err) => {
        clearTimeout(timer);
        if (timedOut) return; // reject already fired; ignore late callback
        if (err) reject(err);
        else resolve();
      });
    });

    console.log(`🦆 [ACS-Parquet] DuckDB COPY complete in ${Date.now() - t0}ms`);

    // Delete temp JSONL now that Parquet is written
    if (existsSync(tempJsonlPath)) unlinkSync(tempJsonlPath);

    totalContractsWritten += contracts.length;
    totalFilesWritten++;
    console.log(`📝 Wrote ${contracts.length} contracts to ${filePath}`);

    // Async GCS upload — returns immediately, upload happens in background
    if (GCS_MODE) enqueueGCSUpload(filePath, partition, fileName);

    return { file: filePath, count: contracts.length };

  } catch (err) {
    // Always clean up temp JSONL on any error
    if (existsSync(tempJsonlPath)) {
      try { unlinkSync(tempJsonlPath); } catch {}
    }
    // In GCS mode, delete the incomplete/corrupted Parquet file — it was not
    // successfully written, so uploading it would corrupt the dataset.
    if (GCS_MODE && existsSync(filePath)) {
      try { unlinkSync(filePath); } catch {}
    }
    console.error(`❌ Parquet write failed for ${filePath}:`, err.message);
    throw err;
  }
}

/**
 * Delete old snapshots for a migration, keeping only the most recent ones.
 * Note: In GCS mode this only affects local /tmp files (which should be empty
 * after successful uploads).
 */
export function cleanupOldSnapshots(migrationId, keepCount = MAX_SNAPSHOTS_PER_MIGRATION) {
  ensureModuleInitialized();
  if (GCS_MODE) {
    console.log(`[ACS] ⚠️ cleanupOldSnapshots not applicable in GCS mode. Use gsutil for GCS cleanup.`);
    return { deleted: 0, kept: 0 };
  }
  const migrationDir = join(DATA_DIR, 'acs', `migration=${migrationId}`);
  if (!existsSync(migrationDir)) {
    console.log(`[ACS] No existing snapshots for migration ${migrationId}`);
    return { deleted: 0, kept: 0 };
  }

  const snapshots = [];
  try {
    const years = readdirSync(migrationDir).filter(f => f.startsWith('year='));
    for (const year of years) {
      const yearPath = join(migrationDir, year);
      if (!statSync(yearPath).isDirectory()) continue;

      const months = readdirSync(yearPath).filter(f => f.startsWith('month='));
      for (const month of months) {
        const monthPath = join(yearPath, month);
        if (!statSync(monthPath).isDirectory()) continue;

        const days = readdirSync(monthPath).filter(f => f.startsWith('day='));
        for (const day of days) {
          const dayPath = join(monthPath, day);
          if (!statSync(dayPath).isDirectory()) continue;

          // Support both legacy snapshot= and current snapshot_id= formats
          const snapshotDirs = readdirSync(dayPath).filter(f =>
            f.startsWith('snapshot_id=') || f.startsWith('snapshot=')
          );

          for (const snapshot of snapshotDirs) {
            const snapshotPath = join(dayPath, snapshot);
            if (!statSync(snapshotPath).isDirectory()) continue;

            // FIX: strip the partition key prefix, then parseInt() before
            // constructing the ISO string. The original code used raw string
            // substitution: new Date(`${y}-${m}-${d}T...Z`) where m and d were
            // taken directly from directory names like 'month=1' → '1'.
            // new Date('2026-1-5T00:00:00Z') is not valid ISO 8601 — V8/Node
            // throws RangeError ("Invalid time value") on .toISOString(), making
            // the timestamp NaN and the sort order for cleanup undefined.
            // Fix: parseInt() + padStart(2,'0') produces '2026-01-05' which V8
            // always parses correctly. Works for both old unpadded paths
            // ('month=1') and new zero-padded paths ('month=01').
            const yNum = parseInt(year.replace('year=', ''), 10);
            const mNum = parseInt(month.replace('month=', ''), 10);
            const dNum = parseInt(day.replace('day=', ''), 10);
            const s = snapshot.replace('snapshot_id=', '').replace('snapshot=', '');

            // snapshot_id is HHMMSSmmm (new) or HHMMSS (legacy) — extract time components
            const hh = s.substring(0, 2);
            const mm = s.substring(2, 4);
            const ss = s.substring(4, 6) || '00';

            const isoString = [
              `${yNum}-`,
              `${String(mNum).padStart(2, '0')}-`,
              `${String(dNum).padStart(2, '0')}`,
              `T${hh}:${mm}:${ss}Z`,
            ].join('');

            const timestamp = new Date(isoString);
            // Guard: if the directory name is malformed, skip rather than sort incorrectly
            if (isNaN(timestamp.getTime())) {
              console.warn(`[ACS] Skipping snapshot with unparseable timestamp: ${snapshotPath}`);
              continue;
            }

            snapshots.push({
              path: snapshotPath,
              timestamp,
              isComplete: existsSync(join(snapshotPath, '_COMPLETE')),
            });
          }
        }
      }
    }
  } catch (err) {
    console.error(`[ACS] Error scanning snapshots: ${err.message}`);
    return { deleted: 0, kept: 0, error: err.message };
  }

  if (snapshots.length === 0) {
    console.log(`[ACS] No snapshots found for migration ${migrationId}`);
    return { deleted: 0, kept: 0 };
  }

  // Sort newest first (NaN timestamps already filtered above)
  snapshots.sort((a, b) => b.timestamp - a.timestamp);

  const toKeep = [];
  const toDelete = [];
  for (const snap of snapshots) {
    if (toKeep.length < keepCount && snap.isComplete) {
      toKeep.push(snap);
    } else {
      toDelete.push(snap);
    }
  }

  let deletedCount = 0;
  for (const snap of toDelete) {
    try {
      console.log(`[ACS] 🗑️ Deleting old snapshot: ${snap.path}`);
      rmSync(snap.path, { recursive: true, force: true });
      deletedCount++;
    } catch (err) {
      console.error(`[ACS] Failed to delete ${snap.path}: ${err.message}`);
    }
  }

  cleanupEmptyDirs(migrationDir);
  console.log(`[ACS] Cleanup complete: deleted ${deletedCount} snapshots, keeping ${toKeep.length}`);
  return { deleted: deletedCount, kept: toKeep.length };
}

/**
 * Remove empty directories recursively
 */
function cleanupEmptyDirs(dirPath) {
  if (!existsSync(dirPath) || !statSync(dirPath).isDirectory()) return;
  const entries = readdirSync(dirPath);
  for (const entry of entries) {
    const fullPath = join(dirPath, entry);
    if (statSync(fullPath).isDirectory()) cleanupEmptyDirs(fullPath);
  }
  const remainingEntries = readdirSync(dirPath);
  if (remainingEntries.length === 0) {
    try { rmSync(dirPath, { recursive: true }); } catch {}
  }
}

/**
 * Set current snapshot time and migration ID (for partitioning).
 * Must be called before bufferContracts/flushContracts for each new snapshot.
 */
export function setSnapshotTime(time, migrationId = null) {
  currentSnapshotTime = time;
  currentMigrationId = migrationId;
  fileCounter = 0;
}

/**
 * Add contracts to in-memory buffer, flushing to Parquet when full.
 */
export async function bufferContracts(contracts) {
  if (!contracts || contracts.length === 0) return null;
  contractsBuffer.push(...contracts);
  if (contractsBuffer.length >= MAX_ROWS_PER_FILE) return await flushContracts();
  return null;
}

/**
 * Flush the contracts buffer to a Parquet file.
 *
 * FIX: removed the `|| new Date()` fallback for timestamp. If setSnapshotTime()
 * was never called AND snapshot_time on the records is null (as normalizeACSContract
 * now correctly returns when snapshotTime is omitted), the fallback created a
 * different partition per flush batch. Each batch would land in a distinct
 * directory named after the moment flush ran, making the snapshot unfindable
 * by any query using the expected partition path.
 * Fix: throw if no timestamp can be determined, forcing the caller to always
 * call setSnapshotTime() before writing.
 */
export async function flushContracts() {
  if (contractsBuffer.length === 0) return null;
  ensureModuleInitialized();

  const timestamp = currentSnapshotTime ?? contractsBuffer[0]?.snapshot_time ?? null;
  if (timestamp == null) {
    throw new Error(
      '[ACS-Parquet] flushContracts: no snapshot timestamp available. ' +
      'Call setSnapshotTime(time, migrationId) before writing contracts.'
    );
  }
  const migrationId = currentMigrationId ?? contractsBuffer[0]?.migration_id ?? null;

  const partition    = getACSPartitionPath(timestamp, migrationId);
  const partitionDir = join(DATA_DIR, partition);
  ensureDir(partitionDir);

  fileCounter++;
  const rand     = randomBytes(4).toString('hex');
  const fileName = `contracts-${String(fileCounter).padStart(5, '0')}-${rand}.parquet`;
  const filePath = join(partitionDir, fileName);

  // Snapshot the buffer and reset synchronously before the first await.
  // This ensures a concurrent call to flushContracts() sees an empty buffer
  // and returns null immediately rather than racing on the same data.
  const contractsToWrite = contractsBuffer;
  contractsBuffer = [];

  return await writeToParquet(contractsToWrite, filePath, partition, fileName);
}

/**
 * Flush all remaining buffered contracts to Parquet.
 */
export async function flushAll() {
  const results = [];
  const contractsResult = await flushContracts();
  if (contractsResult) results.push(contractsResult);
  return results;
}

/**
 * Get current buffer and upload statistics.
 * Maps gcs-upload-queue stats to the same field names callers expect.
 */
export function getBufferStats() {
  const gcsMode = moduleInitialized ? GCS_MODE : false;
  // FIX: switched from getUploadStats() (from the old gcs-upload.js sync path,
  // which tracked gsutil process results) to the async queue's stats. Field
  // names are remapped to preserve the caller-facing API:
  //   gcsStats.totalUploads       → queue.queued   (jobs accepted)
  //   gcsStats.successfulUploads  → queue.completed (uploads confirmed)
  //   gcsStats.failedUploads      → queue.failed
  //   gcsStats.totalBytesUploaded → queue.bytesUploaded
  const queueStats = gcsMode ? getUploadQueue().getStats() : null;

  return {
    contracts: contractsBuffer.length,
    maxRowsPerFile: MAX_ROWS_PER_FILE,
    totalContractsWritten,
    totalFilesWritten,
    totalFilesEnqueued,
    gcsMode,
    ...(queueStats && {
      gcsUploads:        queueStats.queued,
      gcsSuccessful:     queueStats.completed,
      gcsFailed:         queueStats.failed,
      gcsBytesUploaded:  queueStats.bytesUploaded,
      gcsPending:        queueStats.pending,
      gcsActive:         queueStats.active,
    }),
  };
}

/**
 * Clear all in-memory buffers (called before a new snapshot begins).
 */
export function clearBuffers() {
  contractsBuffer = [];
  currentSnapshotTime = null;
  currentMigrationId = null;
  fileCounter = 0;
}

/**
 * Write a _COMPLETE marker file for a snapshot partition.
 * In GCS mode the marker is also enqueued for async upload.
 *
 * FIX: removed `|| new Date()` fallback — same reasoning as flushContracts.
 * If no timestamp is available the marker would be written to a random partition
 * that doesn't match the actual data partition. Throw instead.
 */
export async function writeCompletionMarker(snapshotTime, migrationId, stats = {}) {
  ensureModuleInitialized();

  const timestamp = snapshotTime ?? currentSnapshotTime ?? null;
  if (timestamp == null) {
    throw new Error(
      '[ACS-Parquet] writeCompletionMarker: no snapshot timestamp available. ' +
      'Pass snapshotTime explicitly or call setSnapshotTime() first.'
    );
  }
  const migration = migrationId ?? currentMigrationId;

  const partition    = getACSPartitionPath(timestamp, migration);
  const partitionDir = join(DATA_DIR, partition);
  ensureDir(partitionDir);

  const markerPath = join(partitionDir, '_COMPLETE');
  const markerData = {
    completed_at:       new Date().toISOString(),
    snapshot_time:      timestamp instanceof Date ? timestamp.toISOString() : timestamp,
    migration_id:       migration,
    files_written:      totalFilesWritten,
    contracts_written:  totalContractsWritten,
    files_enqueued:     totalFilesEnqueued,
    gcs_mode:           GCS_MODE,
    ...stats,
  };

  writeFileSync(markerPath, JSON.stringify(markerData, null, 2));
  console.log(`✅ Wrote completion marker to ${markerPath}`);
  console.log(`   📊 Summary: ${totalFilesWritten} files, ${totalContractsWritten} contracts`);

  // Enqueue the marker for async GCS upload (fire-and-forget, tiny file)
  if (GCS_MODE) {
    const relativePath = join(partition, '_COMPLETE').replace(/\\/g, '/');
    const gcsPath = getGCSPath(relativePath);
    enqueueUpload(markerPath, gcsPath);
  }

  return markerPath;
}

/**
 * Check if a snapshot partition has been marked complete.
 * Note: In GCS mode this only checks local scratch files.
 */
export function isSnapshotComplete(snapshotTime, migrationId) {
  ensureModuleInitialized();
  const partition  = getACSPartitionPath(snapshotTime, migrationId);
  const markerPath = join(DATA_DIR, partition, '_COMPLETE');
  return existsSync(markerPath);
}

/**
 * Drain pending GCS uploads then shut down DuckDB.
 * Call before process.exit() to ensure all enqueued files are uploaded.
 */
export async function shutdown() {
  // Drain any in-flight or queued GCS uploads before closing DuckDB.
  // This replaces the old sync-upload-and-exit pattern: because uploads are
  // now async, we must await the queue drain before tearing down.
  if (moduleInitialized && GCS_MODE) {
    console.log(`☁️ [ACS-Parquet] Draining GCS upload queue before shutdown...`);
    await drainUploads();
    const queueStats = getUploadQueue().getStats();
    console.log(
      `☁️ [ACS-Parquet] GCS drain complete — ` +
      `${queueStats.completed} uploaded, ${queueStats.failed} failed`
    );
  }

  if (conn) { try { conn.close(); } catch {} conn = null; }
  if (db)   { try { db.close();   } catch {} db   = null; }
  console.log(`🦆 [ACS-Parquet] Shutdown complete`);
}

export default {
  setSnapshotTime,
  bufferContracts,
  flushContracts,
  flushAll,
  getBufferStats,
  clearBuffers,
  writeCompletionMarker,
  isSnapshotComplete,
  cleanupOldSnapshots,
  shutdown,
};
