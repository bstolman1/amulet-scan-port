/**
 * ACS Parquet Writer Module with GCS Upload
 * 
 * Writes ACS snapshot data directly to Parquet files using DuckDB Node.js bindings.
 * This eliminates the need for a separate materialization step.
 * 
 * GCS Mode (when GCS_BUCKET is set):
 * 1. Writes Parquet files to /tmp/ledger_raw/acs (ephemeral scratch space)
 * 2. Uploads each file immediately to GCS using gsutil
 * 3. Deletes local file after upload
 * 4. Keeps disk usage flat regardless of total data volume
 * 
 * Local Mode (default):
 * - Writes to DATA_DIR/raw/acs like before
 * 
 * Drop-in replacement for write-acs-jsonl.js with same API surface.
 */

import { mkdirSync, existsSync, readdirSync, rmSync, statSync, writeFileSync, unlinkSync } from 'fs';
import { join, dirname, sep, isAbsolute, resolve } from 'path';
import { fileURLToPath } from 'url';
import { randomBytes } from 'crypto';
import duckdb from 'duckdb';
import { getACSPartitionPath } from './acs-schema.js';
import { 
  isGCSMode, 
  getTmpRawDir, 
  getRawDir, 
  getBaseDataDir,
  ensureTmpDir 
} from './path-utils.js';
import {
  initGCS,
  isGCSEnabled,
  getGCSPath,
  uploadAndCleanupSync,
  getUploadStats,
} from './gcs-upload.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Configuration - use /tmp in GCS mode, DATA_DIR otherwise
// Defer GCS_MODE check to avoid errors during module import
let GCS_MODE = null;
let BASE_DATA_DIR = null;
let DATA_DIR = null;
const MAX_ROWS_PER_FILE = parseInt(process.env.ACS_MAX_ROWS_PER_FILE) || 10000;

// Keep at least 2 snapshots per migration
const MAX_SNAPSHOTS_PER_MIGRATION = parseInt(process.env.MAX_SNAPSHOTS_PER_MIGRATION) || 2;

// Deferred initialization flag
let moduleInitialized = false;

/**
 * Initialize module configuration (called lazily on first use)
 */
function ensureModuleInitialized() {
  if (moduleInitialized) return;
  
  try {
    GCS_MODE = isGCSMode();
    BASE_DATA_DIR = GCS_MODE ? '/tmp/ledger_raw' : getBaseDataDir();
    DATA_DIR = GCS_MODE ? getTmpRawDir() : getRawDir();
    
    if (GCS_MODE) {
      initGCS();
      ensureTmpDir();
      console.log(`☁️ [ACS-Parquet] GCS mode enabled`);
      console.log(`☁️ [ACS-Parquet] Local scratch: ${DATA_DIR}`);
      console.log(`☁️ [ACS-Parquet] GCS destination: gs://${process.env.GCS_BUCKET}/raw/acs/`);
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
let totalFilesUploaded = 0;

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
 * Ensure directory exists (Windows-safe)
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
          if (!existsSync(current)) {
            mkdirSync(current);
          }
        } catch (e) {
          if (e.code !== 'EEXIST') throw e;
        }
      }
    }
  }
}

/**
 * Delete old snapshots for a migration, keeping only the most recent ones
 * Note: In GCS mode, this only affects local /tmp files (which should be empty anyway)
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
          
          const snapshotDirs = readdirSync(dayPath).filter(f => f.startsWith('snapshot='));
          
          for (const snapshot of snapshotDirs) {
            const snapshotPath = join(dayPath, snapshot);
            if (!statSync(snapshotPath).isDirectory()) continue;
            
            const y = year.replace('year=', '');
            const m = month.replace('month=', '');
            const d = day.replace('day=', '');
            const s = snapshot.replace('snapshot=', '');
            const hh = s.substring(0, 2);
            const mm = s.substring(2, 4);
            const ss = s.substring(4, 6) || '00';
            
            const timestamp = new Date(`${y}-${m}-${d}T${hh}:${mm}:${ss}Z`);
            
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
    if (statSync(fullPath).isDirectory()) {
      cleanupEmptyDirs(fullPath);
    }
  }
  
  const remainingEntries = readdirSync(dirPath);
  if (remainingEntries.length === 0) {
    try {
      rmSync(dirPath, { recursive: true });
    } catch {
      // Ignore errors
    }
  }
}

/**
 * Upload file to GCS if in GCS mode
 */
function uploadToGCSIfEnabled(localPath, partition, fileName) {
  // GCS_MODE is already checked before calling this function
  // but check again for safety
  if (!moduleInitialized || !GCS_MODE) return null;
  
  const relativePath = join('acs', partition, fileName).replace(/\\/g, '/');
  const gcsPath = getGCSPath(relativePath);
  
  const result = uploadAndCleanupSync(localPath, gcsPath, { quiet: false });
  
  if (result.ok) {
    totalFilesUploaded++;
  }
  
  return result;
}

/**
 * Write contracts to Parquet file via DuckDB Node.js bindings
 */
async function writeToParquet(contracts, filePath, partition, fileName) {
  if (contracts.length === 0) return null;
  
  ensureDbInitialized();
  
  const tempJsonlPath = filePath.replace('.parquet', `.temp-${randomBytes(4).toString('hex')}.jsonl`);
  const parquetPath = filePath.replace(/\\/g, '/');
  const jsonlPath = tempJsonlPath.replace(/\\/g, '/');
  
  try {
    // Write contracts to temp JSONL file
    const lines = contracts.map(c => JSON.stringify(c));
    writeFileSync(tempJsonlPath, lines.join('\n') + '\n');
    
    // Use DuckDB Node.js bindings to convert to Parquet
    const sql = `
      COPY (
        SELECT * FROM read_json_auto('${jsonlPath}', ignore_errors=true)
      ) TO '${parquetPath}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000);
    `;
    
    await new Promise((resolve, reject) => {
      conn.run(sql, (err) => {
        if (err) reject(err);
        else resolve();
      });
    });
    
    // Clean up temp file
    if (existsSync(tempJsonlPath)) {
      unlinkSync(tempJsonlPath);
    }
    
    totalContractsWritten += contracts.length;
    totalFilesWritten++;
    
    console.log(`📝 Wrote ${contracts.length} contracts to ${filePath}`);
    
    // Upload to GCS if enabled (this also deletes the local file)
    if (GCS_MODE) {
      uploadToGCSIfEnabled(filePath, partition, fileName);
    }
    
    return { file: filePath, count: contracts.length };
  } catch (err) {
    // Clean up temp file on error
    if (existsSync(tempJsonlPath)) {
      try {
        unlinkSync(tempJsonlPath);
      } catch {
        // Ignore cleanup errors
      }
    }
    // In GCS mode, always clean up local file on error
    if (GCS_MODE && existsSync(filePath)) {
      try {
        unlinkSync(filePath);
      } catch {
        // Ignore cleanup errors
      }
    }
    console.error(`❌ Parquet write failed for ${filePath}:`, err.message);
    throw err;
  }
}

/**
 * Set current snapshot time and migration ID (for partitioning)
 */
export function setSnapshotTime(time, migrationId = null) {
  currentSnapshotTime = time;
  currentMigrationId = migrationId;
  fileCounter = 0;
}

/**
 * Add contracts to buffer
 */
export async function bufferContracts(contracts) {
  if (!contracts || contracts.length === 0) return null;
  
  contractsBuffer.push(...contracts);
  
  if (contractsBuffer.length >= MAX_ROWS_PER_FILE) {
    return await flushContracts();
  }
  return null;
}

/**
 * Flush contracts buffer to Parquet file
 */
export async function flushContracts() {
  if (contractsBuffer.length === 0) return null;
  
  ensureModuleInitialized();
  
  const timestamp = currentSnapshotTime || contractsBuffer[0]?.snapshot_time || new Date();
  const migrationId = currentMigrationId ?? contractsBuffer[0]?.migration_id ?? null;
  const partition = getACSPartitionPath(timestamp, migrationId);
  const partitionDir = join(DATA_DIR, 'acs', partition);
  
  ensureDir(partitionDir);
  
  fileCounter++;
  const rand = randomBytes(4).toString('hex');
  const fileName = `contracts-${String(fileCounter).padStart(5, '0')}-${rand}.parquet`;
  const filePath = join(partitionDir, fileName);
  
  const contractsToWrite = contractsBuffer;
  contractsBuffer = [];
  
  return await writeToParquet(contractsToWrite, filePath, partition, fileName);
}

/**
 * Flush all buffers
 */
export async function flushAll() {
  const results = [];
  
  const contractsResult = await flushContracts();
  if (contractsResult) results.push(contractsResult);
  
  return results;
}

/**
 * Get buffer stats
 */
export function getBufferStats() {
  // Safely check GCS_MODE - may not be initialized yet
  const gcsMode = moduleInitialized ? GCS_MODE : false;
  const gcsStats = gcsMode ? getUploadStats() : null;
  
  return {
    contracts: contractsBuffer.length,
    maxRowsPerFile: MAX_ROWS_PER_FILE,
    totalContractsWritten,
    totalFilesWritten,
    totalFilesUploaded,
    gcsMode: gcsMode,
    // GCS-specific stats
    ...(gcsStats && {
      gcsUploads: gcsStats.totalUploads,
      gcsSuccessful: gcsStats.successfulUploads,
      gcsFailed: gcsStats.failedUploads,
      gcsBytesUploaded: gcsStats.totalBytesUploaded,
    }),
  };
}

/**
 * Clear buffers (for new snapshot)
 */
export function clearBuffers() {
  contractsBuffer = [];
  currentSnapshotTime = null;
  currentMigrationId = null;
  fileCounter = 0;
}

/**
 * Write a completion marker file for a snapshot
 * In GCS mode, this marker is also uploaded to GCS
 */
export async function writeCompletionMarker(snapshotTime, migrationId, stats = {}) {
  ensureModuleInitialized();
  
  const timestamp = snapshotTime || currentSnapshotTime || new Date();
  const migration = migrationId ?? currentMigrationId;
  const partition = getACSPartitionPath(timestamp, migration);
  const partitionDir = join(DATA_DIR, 'acs', partition);
  
  ensureDir(partitionDir);
  
  const markerPath = join(partitionDir, '_COMPLETE');
  const markerData = {
    completed_at: new Date().toISOString(),
    snapshot_time: timestamp instanceof Date ? timestamp.toISOString() : timestamp,
    migration_id: migration,
    files_written: totalFilesWritten,
    contracts_written: totalContractsWritten,
    files_uploaded: totalFilesUploaded,
    gcs_mode: GCS_MODE,
    ...stats,
  };
  
  writeFileSync(markerPath, JSON.stringify(markerData, null, 2));
  console.log(`✅ Wrote completion marker to ${markerPath}`);
  console.log(`   📊 Summary: ${totalFilesWritten} files, ${totalContractsWritten} contracts`);
  
  // Upload marker to GCS if enabled
  if (GCS_MODE) {
    const relativePath = join('acs', partition, '_COMPLETE').replace(/\\/g, '/');
    const gcsPath = getGCSPath(relativePath);
    uploadAndCleanupSync(markerPath, gcsPath, { quiet: true });
  }
  
  return markerPath;
}

/**
 * Check if a snapshot partition is complete
 * Note: In GCS mode, this only checks local files
 */
export function isSnapshotComplete(snapshotTime, migrationId) {
  ensureModuleInitialized();
  
  const partition = getACSPartitionPath(snapshotTime, migrationId);
  const markerPath = join(DATA_DIR, 'acs', partition, '_COMPLETE');
  return existsSync(markerPath);
}

/**
 * Shutdown and cleanup
 */
export function shutdown() {
  if (conn) {
    try {
      conn.close();
    } catch {
      // Ignore
    }
    conn = null;
  }
  if (db) {
    try {
      db.close();
    } catch {
      // Ignore
    }
    db = null;
  }
  
  // Safely check GCS_MODE - may not be initialized
  if (moduleInitialized && GCS_MODE) {
    const gcsStats = getUploadStats();
    console.log(`☁️ [ACS-Parquet] GCS shutdown - ${gcsStats.successfulUploads} files uploaded`);
  }
  
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
