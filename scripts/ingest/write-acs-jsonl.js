/**
 * ACS JSONL Writer Module
 * 
 * Handles writing ACS snapshot data to partitioned JSONL files.
 * Uses streaming writes to avoid memory limits.
 * 
 * IMPORTANT: Each new snapshot REPLACES the previous snapshot for the same migration.
 * This prevents data accumulation and corruption from stale snapshots.
 */

import { createWriteStream, mkdirSync, existsSync, readdirSync, rmSync, statSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import { getACSPartitionPath, ACS_COLUMNS } from './acs-schema.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Configuration - Default Windows path: C:\ledger_raw
// ACS data goes in DATA_DIR/raw subdirectory (alongside ledger data)
const WIN_DEFAULT = 'C:\\ledger_raw';
const BASE_DATA_DIR = process.env.DATA_DIR || WIN_DEFAULT;
const DATA_DIR = join(BASE_DATA_DIR, 'raw');
const MAX_ROWS_PER_FILE = parseInt(process.env.ACS_MAX_ROWS_PER_FILE) || 10000;

// Keep at least 2 snapshots per migration so that during a new snapshot fetch,
// the previous complete snapshot is still available for queries.
// Set to 1 only if you don't care about availability during snapshot updates.
const MAX_SNAPSHOTS_PER_MIGRATION = parseInt(process.env.MAX_SNAPSHOTS_PER_MIGRATION) || 2;

// In-memory buffer for batching
let contractsBuffer = [];
let currentSnapshotTime = null;
let currentMigrationId = null;

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
 * @param {number} migrationId - The migration ID to clean up
 * @param {number} keepCount - Number of snapshots to keep (default: MAX_SNAPSHOTS_PER_MIGRATION)
 */
export function cleanupOldSnapshots(migrationId, keepCount = MAX_SNAPSHOTS_PER_MIGRATION) {
  const migrationDir = join(DATA_DIR, 'acs', `migration=${migrationId}`);
  
  if (!existsSync(migrationDir)) {
    console.log(`[ACS] No existing snapshots for migration ${migrationId}`);
    return { deleted: 0, kept: 0 };
  }
  
  // Collect all snapshot directories with their timestamps
  const snapshots = [];
  
  try {
    // Walk through year/month/day/snapshot structure
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
            
            // Parse timestamp from path structure
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
  
  // Sort by timestamp descending (newest first)
  snapshots.sort((a, b) => b.timestamp - a.timestamp);
  
  // Keep the most recent 'keepCount' complete snapshots, delete the rest
  const toKeep = [];
  const toDelete = [];
  
  for (const snap of snapshots) {
    if (toKeep.length < keepCount && snap.isComplete) {
      toKeep.push(snap);
    } else {
      toDelete.push(snap);
    }
  }
  
  // Delete old snapshots
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
  
  // Clean up empty parent directories
  cleanupEmptyDirs(migrationDir);
  
  console.log(`[ACS] Cleanup complete: deleted ${deletedCount} snapshots, keeping ${toKeep.length}`);
  return { deleted: deletedCount, kept: toKeep.length };
}

/**
 * Remove empty directories recursively (bottom-up)
 */
function cleanupEmptyDirs(dirPath) {
  if (!existsSync(dirPath) || !statSync(dirPath).isDirectory()) return;
  
  const entries = readdirSync(dirPath);
  
  // First, recurse into subdirectories
  for (const entry of entries) {
    const fullPath = join(dirPath, entry);
    if (statSync(fullPath).isDirectory()) {
      cleanupEmptyDirs(fullPath);
    }
  }
  
  // Check again after recursion - directory may now be empty
  const remainingEntries = readdirSync(dirPath);
  if (remainingEntries.length === 0) {
    try {
      rmSync(dirPath, { recursive: true });
    } catch {
      // Ignore errors - may be the root acs directory
    }
  }
}

/**
 * Get next file number for a partition
 */
function getNextFileNumber(partitionDir, prefix) {
  ensureDir(partitionDir);
  
  const files = readdirSync(partitionDir)
    .filter(f => f.startsWith(prefix) && f.endsWith('.jsonl'));
  
  if (files.length === 0) return 1;
  
  const numbers = files.map(f => {
    const match = f.match(/-(\d+)\.jsonl$/);
    return match ? parseInt(match[1]) : 0;
  });
  
  return Math.max(...numbers) + 1;
}

/**
 * Write rows to a JSON-lines file using streaming
 */
function writeJsonLines(rows, filePath) {
  return new Promise((resolve, reject) => {
    const stream = createWriteStream(filePath, { encoding: 'utf8' });
    
    stream.on('error', reject);
    stream.on('finish', resolve);
    
    for (const row of rows) {
      stream.write(JSON.stringify(row) + '\n');
    }
    
    stream.end();
  });
}

/**
 * Set current snapshot time and migration ID (for partitioning)
 */
export function setSnapshotTime(time, migrationId = null) {
  currentSnapshotTime = time;
  currentMigrationId = migrationId;
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
 * Flush contracts buffer to file
 */
export async function flushContracts() {
  if (contractsBuffer.length === 0) return null;
  
  const timestamp = currentSnapshotTime || contractsBuffer[0]?.snapshot_time || new Date();
  const migrationId = currentMigrationId || contractsBuffer[0]?.migration_id || null;
  const partition = getACSPartitionPath(timestamp, migrationId);
  const partitionDir = join(DATA_DIR, partition);
  
  ensureDir(partitionDir);
  
  const fileNum = getNextFileNumber(partitionDir, 'contracts');
  const fileName = `contracts-${String(fileNum).padStart(5, '0')}.jsonl`;
  const filePath = join(partitionDir, fileName);
  
  await writeJsonLines(contractsBuffer, filePath);
  
  const count = contractsBuffer.length;
  contractsBuffer = [];
  
  console.log(`📝 Wrote ${count} contracts to ${filePath}`);
  return { file: filePath, count };
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
}

/**
 * Write a completion marker file for a snapshot
 * This file indicates the snapshot is complete and safe to use
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
  
  await writeJsonLines([markerData], markerPath);
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
