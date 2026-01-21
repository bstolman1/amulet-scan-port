#!/usr/bin/env node
/**
 * GCS Reconciliation - Startup Safety Check
 * 
 * Compares cursor positions against actual GCS file timestamps
 * to detect any gaps from previous VM crashes.
 * 
 * This script should be run BEFORE starting backfill to ensure
 * the cursor reflects what actually made it to GCS.
 * 
 * Usage:
 *   node reconcile-gcs.js              # Check all migrations
 *   node reconcile-gcs.js --fix        # Reset cursors to GCS-confirmed positions
 *   node reconcile-gcs.js --migration=0  # Check specific migration
 */

import dotenv from 'dotenv';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { execSync } from 'child_process';
import { existsSync, readdirSync, readFileSync, writeFileSync } from 'fs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
dotenv.config({ path: join(__dirname, '.env') });

import { getCursorDir, isGCSMode } from './path-utils.js';
import { loadCursorLegacy, atomicWriteFile, getCursorPath } from './atomic-cursor.js';

const GCS_BUCKET = process.env.GCS_BUCKET;
const CURSOR_DIR = getCursorDir();

// Parse args
const args = process.argv.slice(2);
const FIX_MODE = args.includes('--fix');
const MIGRATION_ARG = args.find(a => a.startsWith('--migration='));
const TARGET_MIGRATION = MIGRATION_ARG ? parseInt(MIGRATION_ARG.split('=')[1]) : null;

/**
 * List files in GCS bucket for a migration
 */
function listGCSFiles(migrationId) {
  if (!GCS_BUCKET) {
    throw new Error('GCS_BUCKET not set');
  }
  
  const prefix = `raw/migration=${migrationId}/`;
  
  try {
    // List all parquet files for this migration
    const output = execSync(
      `gsutil ls -r "gs://${GCS_BUCKET}/${prefix}**/*.parquet" 2>/dev/null || true`,
      { encoding: 'utf8', maxBuffer: 50 * 1024 * 1024 }
    );
    
    return output.trim().split('\n').filter(line => line.includes('.parquet'));
  } catch (err) {
    console.error(`❌ Failed to list GCS files: ${err.message}`);
    return [];
  }
}

/**
 * Extract timestamp from parquet filename
 * Format: updates-{timestamp}-{count}.parquet or events-{timestamp}-{count}.parquet
 */
function extractTimestampFromPath(filePath) {
  const match = filePath.match(/(?:updates|events)-(\d+)-/);
  if (match) {
    return parseInt(match[1]);
  }
  return null;
}

/**
 * Find the latest timestamp in GCS for a migration
 */
function findLatestGCSTimestamp(migrationId) {
  const files = listGCSFiles(migrationId);
  
  if (files.length === 0) {
    return { timestamp: null, fileCount: 0 };
  }
  
  let minTimestamp = Infinity;  // Backfill goes backwards in time
  
  for (const file of files) {
    const ts = extractTimestampFromPath(file);
    if (ts !== null && ts < minTimestamp) {
      minTimestamp = ts;
    }
  }
  
  return {
    timestamp: minTimestamp === Infinity ? null : minTimestamp,
    fileCount: files.length,
  };
}

/**
 * Load all cursors for a migration
 */
function loadCursorsForMigration(migrationId) {
  if (!existsSync(CURSOR_DIR)) {
    return [];
  }
  
  const files = readdirSync(CURSOR_DIR);
  const cursors = [];
  
  for (const file of files) {
    if (!file.startsWith(`cursor-${migrationId}-`) || !file.endsWith('.json')) {
      continue;
    }
    
    try {
      const content = readFileSync(join(CURSOR_DIR, file), 'utf8');
      const cursor = JSON.parse(content);
      cursors.push({ file, cursor });
    } catch (err) {
      console.warn(`⚠️ Failed to read cursor ${file}: ${err.message}`);
    }
  }
  
  return cursors;
}

/**
 * Reconcile a single migration
 */
function reconcileMigration(migrationId) {
  console.log(`\n🔍 Reconciling migration ${migrationId}...`);
  
  // Find latest GCS timestamp
  const gcsResult = findLatestGCSTimestamp(migrationId);
  
  if (gcsResult.timestamp === null) {
    console.log(`   ℹ️ No GCS files found for migration ${migrationId}`);
    return { ok: true, message: 'No GCS files', migrationId };
  }
  
  console.log(`   ☁️ GCS: ${gcsResult.fileCount} files, earliest timestamp: ${new Date(gcsResult.timestamp).toISOString()}`);
  
  // Load cursors
  const cursors = loadCursorsForMigration(migrationId);
  
  if (cursors.length === 0) {
    console.log(`   ℹ️ No cursors found for migration ${migrationId}`);
    return { ok: true, message: 'No cursors', migrationId };
  }
  
  let hasGap = false;
  const gaps = [];
  
  for (const { file, cursor } of cursors) {
    const cursorTimestamp = cursor.last_before ? new Date(cursor.last_before).getTime() : null;
    const gcsConfirmedTimestamp = cursor.last_gcs_confirmed ? new Date(cursor.last_gcs_confirmed).getTime() : null;
    
    // Check if cursor claims a position beyond what's in GCS
    if (cursorTimestamp && cursorTimestamp < gcsResult.timestamp) {
      hasGap = true;
      const gapSize = cursor.total_updates - (cursor.gcs_confirmed_updates || 0);
      
      gaps.push({
        file,
        cursorPosition: cursor.last_before,
        gcsPosition: new Date(gcsResult.timestamp).toISOString(),
        gapUpdates: gapSize,
      });
      
      console.log(`   ⚠️ GAP DETECTED in ${file}:`);
      console.log(`      Cursor claims: ${cursor.last_before} (${cursor.total_updates} updates)`);
      console.log(`      GCS has: ${new Date(gcsResult.timestamp).toISOString()}`);
      console.log(`      Gap: ~${gapSize} updates may be lost`);
      
      if (FIX_MODE) {
        // Reset cursor to GCS-confirmed position
        console.log(`   🔧 Fixing: resetting cursor to GCS-confirmed position...`);
        
        const fixedCursor = {
          ...cursor,
          last_before: new Date(gcsResult.timestamp).toISOString(),
          last_confirmed_before: new Date(gcsResult.timestamp).toISOString(),
          last_gcs_confirmed: new Date(gcsResult.timestamp).toISOString(),
          total_updates: cursor.gcs_confirmed_updates || 0,
          confirmed_updates: cursor.gcs_confirmed_updates || 0,
          gcs_confirmed_updates: cursor.gcs_confirmed_updates || 0,
          total_events: cursor.gcs_confirmed_events || 0,
          confirmed_events: cursor.gcs_confirmed_events || 0,
          gcs_confirmed_events: cursor.gcs_confirmed_events || 0,
          complete: false,  // Not complete since we're resuming
          reconciled_at: new Date().toISOString(),
          reconciled_from: cursor.last_before,
        };
        
        atomicWriteFile(join(CURSOR_DIR, file), fixedCursor);
        console.log(`   ✅ Cursor reset. Will re-fetch from ${new Date(gcsResult.timestamp).toISOString()}`);
      }
    } else if (gcsConfirmedTimestamp && cursorTimestamp) {
      // Check if GCS-confirmed is behind local cursor (expected during normal operation)
      const pendingUpdates = cursor.total_updates - (cursor.gcs_confirmed_updates || 0);
      if (pendingUpdates > 0) {
        console.log(`   ℹ️ ${file}: ${pendingUpdates} updates pending GCS confirmation`);
      } else {
        console.log(`   ✅ ${file}: GCS and local cursor are in sync`);
      }
    } else {
      console.log(`   ✅ ${file}: OK (${cursor.total_updates} updates)`);
    }
  }
  
  return {
    ok: !hasGap,
    migrationId,
    gcsFileCount: gcsResult.fileCount,
    cursorCount: cursors.length,
    gaps,
    fixed: FIX_MODE && hasGap,
  };
}

/**
 * Discover migrations from cursors
 */
function discoverMigrationsFromCursors() {
  if (!existsSync(CURSOR_DIR)) {
    return [];
  }
  
  const files = readdirSync(CURSOR_DIR);
  const migrations = new Set();
  
  for (const file of files) {
    const match = file.match(/cursor-(\d+)-/);
    if (match) {
      migrations.add(parseInt(match[1]));
    }
  }
  
  return Array.from(migrations).sort((a, b) => a - b);
}

/**
 * Main reconciliation function
 */
async function main() {
  console.log("\n" + "═".repeat(80));
  console.log("🔍 GCS RECONCILIATION CHECK");
  console.log("═".repeat(80));
  
  if (!isGCSMode()) {
    console.log("ℹ️ GCS mode not enabled. Reconciliation not needed for local mode.");
    return;
  }
  
  console.log(`GCS Bucket: ${GCS_BUCKET}`);
  console.log(`Cursor Dir: ${CURSOR_DIR}`);
  console.log(`Fix Mode: ${FIX_MODE ? 'ENABLED (will reset cursors)' : 'disabled (dry run)'}`);
  console.log("═".repeat(80));
  
  // Discover or use specified migration
  const migrations = TARGET_MIGRATION !== null 
    ? [TARGET_MIGRATION] 
    : discoverMigrationsFromCursors();
  
  if (migrations.length === 0) {
    console.log("\nNo migrations found in cursor directory.");
    return;
  }
  
  console.log(`\nFound ${migrations.length} migration(s): ${migrations.join(', ')}`);
  
  const results = [];
  
  for (const migrationId of migrations) {
    const result = reconcileMigration(migrationId);
    results.push(result);
  }
  
  // Summary
  console.log("\n" + "═".repeat(80));
  console.log("📊 RECONCILIATION SUMMARY");
  console.log("═".repeat(80));
  
  const hasGaps = results.some(r => !r.ok);
  const totalGaps = results.reduce((sum, r) => sum + (r.gaps?.length || 0), 0);
  
  for (const result of results) {
    const status = result.ok ? '✅' : (result.fixed ? '🔧' : '⚠️');
    console.log(`${status} Migration ${result.migrationId}: ${result.ok ? 'OK' : `${result.gaps?.length || 0} gap(s)`}`);
  }
  
  if (hasGaps && !FIX_MODE) {
    console.log("\n⚠️ Gaps detected! Run with --fix to reset cursors to GCS-confirmed positions.");
    console.log("   This will cause affected batches to be re-fetched on next run.");
    process.exit(1);
  } else if (hasGaps && FIX_MODE) {
    console.log("\n✅ Gaps fixed. Cursors reset to GCS-confirmed positions.");
    console.log("   Affected data will be re-fetched on next run.");
  } else {
    console.log("\n✅ All cursors are in sync with GCS.");
  }
  
  console.log("═".repeat(80) + "\n");
}

// Export for use as module
export { reconcileMigration, findLatestGCSTimestamp, loadCursorsForMigration };

// Run if called directly
main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
