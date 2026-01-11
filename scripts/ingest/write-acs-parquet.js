/**
 * ACS Parquet Writer Module
 * 
 * Writes ACS snapshot data directly to Parquet files using DuckDB Node.js library.
 * This eliminates the need for a separate materialization step.
 * 
 * Drop-in replacement for write-acs-jsonl.js with same API surface.
 */

import { mkdirSync, existsSync, readdirSync, rmSync, statSync, writeFileSync, unlinkSync } from 'fs';
import { join, dirname, sep } from 'path';
import { fileURLToPath } from 'url';
import { randomBytes } from 'crypto';
import duckdb from 'duckdb';
import { getACSPartitionPath } from './acs-schema.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Configuration - Default Windows path: C:\ledger_raw
const WIN_DEFAULT = 'C:\\ledger_raw';
const BASE_DATA_DIR = process.env.DATA_DIR || WIN_DEFAULT;
const DATA_DIR = join(BASE_DATA_DIR, 'raw');
const MAX_ROWS_PER_FILE = parseInt(process.env.ACS_MAX_ROWS_PER_FILE) || 10000;

// Keep at least 2 snapshots per migration
const MAX_SNAPSHOTS_PER_MIGRATION = parseInt(process.env.MAX_SNAPSHOTS_PER_MIGRATION) || 2;

// Create a single DuckDB instance for the module (in-memory, just for writing)
const db = new duckdb.Database(':memory:');

// In-memory buffer for batching
let contractsBuffer = [];
let currentSnapshotTime = null;
let currentMigrationId = null;
let fileCounter = 0;

/**
 * Ensure directory exists
 */
function ensureDir(dirPath) {
  if (!existsSync(dirPath)) {
    mkdirSync(dirPath, { recursive: true });
  }
}

/**
 * Delete old snapshots for a migration, keeping only the most recent ones
 */
export function cleanupOldSnapshots(migrationId, keepCount = MAX_SNAPSHOTS_PER_MIGRATION) {
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
 * Run a DuckDB exec (no results) as a promise
 */
function runExec(sql) {
  return new Promise((resolve, reject) => {
    db.exec(sql, (err) => {
      if (err) reject(err);
      else resolve();
    });
  });
}

/**
 * Escape a string value for SQL
 */
function escapeStr(val) {
  if (val === null || val === undefined) return 'NULL';
  const str = String(val).replace(/'/g, "''");
  return `'${str}'`;
}

/**
 * Write contracts to Parquet file via DuckDB Node.js library (optimized with temp JSONL)
 */
async function writeToParquet(contracts, filePath) {
  if (contracts.length === 0) return null;
  
  // Normalize path for DuckDB (forward slashes work on all platforms)
  const normalizedFilePath = filePath.replace(/\\/g, '/');
  const tempJsonlPath = normalizedFilePath.replace('.parquet', '.temp.jsonl');
  const tempNativePath = tempJsonlPath.replace(/\//g, sep);
  const parentDir = dirname(filePath);
  
  try {
    // Ensure parent directory exists
    ensureDir(parentDir);
    
    // Write temp JSONL (fastest way to bulk load into DuckDB)
    const lines = contracts.map(c => JSON.stringify(c));
    writeFileSync(tempNativePath, lines.join('\n') + '\n');
    
    // Use DuckDB to read JSONL and write Parquet
    const sql = `COPY (SELECT * FROM read_json_auto('${tempJsonlPath}')) TO '${normalizedFilePath}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)`;
    await runExec(sql);
    
    // Verify file was created
    if (!existsSync(filePath)) {
      throw new Error(`DuckDB completed but parquet file not created: ${filePath}`);
    }
    
    // Clean up temp file
    if (existsSync(tempNativePath)) {
      unlinkSync(tempNativePath);
    }
    
    console.log(`📝 Wrote ${contracts.length} contracts to ${filePath}`);
    return { file: filePath, count: contracts.length };
  } catch (err) {
    // Clean up temp file on error
    if (existsSync(tempNativePath)) {
      try { unlinkSync(tempNativePath); } catch {}
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
  
  const timestamp = currentSnapshotTime || contractsBuffer[0]?.snapshot_time || new Date();
  const migrationId = currentMigrationId || contractsBuffer[0]?.migration_id || null;
  const partition = getACSPartitionPath(timestamp, migrationId);
  const partitionDir = join(DATA_DIR, partition);
  
  ensureDir(partitionDir);
  
  fileCounter++;
  const fileName = `contracts-${String(fileCounter).padStart(5, '0')}.parquet`;
  const filePath = join(partitionDir, fileName);
  
  const contractsToWrite = contractsBuffer;
  contractsBuffer = [];
  
  return await writeToParquet(contractsToWrite, filePath);
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
  return {
    contracts: contractsBuffer.length,
    maxRowsPerFile: MAX_ROWS_PER_FILE,
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
 */
export async function writeCompletionMarker(snapshotTime, migrationId, stats = {}) {
  const timestamp = snapshotTime || currentSnapshotTime || new Date();
  const migration = migrationId ?? currentMigrationId;
  const partition = getACSPartitionPath(timestamp, migration);
  const partitionDir = join(DATA_DIR, partition);
  
  ensureDir(partitionDir);
  
  const markerPath = join(partitionDir, '_COMPLETE');
  const markerData = {
    completed_at: new Date().toISOString(),
    snapshot_time: timestamp instanceof Date ? timestamp.toISOString() : timestamp,
    migration_id: migration,
    ...stats,
  };
  
  writeFileSync(markerPath, JSON.stringify(markerData, null, 2));
  console.log(`✅ Wrote completion marker to ${markerPath}`);
  return markerPath;
}

/**
 * Check if a snapshot partition is complete
 */
export function isSnapshotComplete(snapshotTime, migrationId) {
  const partition = getACSPartitionPath(snapshotTime, migrationId);
  const markerPath = join(DATA_DIR, partition, '_COMPLETE');
  return existsSync(markerPath);
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
};
