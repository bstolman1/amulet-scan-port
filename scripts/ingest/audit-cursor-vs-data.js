#!/usr/bin/env node
/**
 * Cursor vs Data Audit Tool
 * 
 * Scans GCS partitions to find the actual latest record_time in stored data,
 * then compares against local and GCS cursor positions to detect drift.
 * 
 * Usage:
 *   node audit-cursor-vs-data.js
 *   node audit-cursor-vs-data.js --verbose
 */

import { Storage } from '@google-cloud/storage';
import { Database } from 'duckdb-async';
import { readFileSync, existsSync } from 'fs';
import { join } from 'path';
import dotenv from 'dotenv';

dotenv.config();

const BUCKET = process.env.GCS_BUCKET || 'canton-bucket';
const CURSOR_DIR = process.env.CURSOR_DIR || '/tmp/ledger_raw/cursors';
const VERBOSE = process.argv.includes('--verbose');

const storage = new Storage();

/**
 * List recent Parquet files from a GCS prefix
 */
async function listRecentFiles(prefix, limit = 50) {
  const [files] = await storage.bucket(BUCKET).getFiles({
    prefix,
    maxResults: 1000,
  });
  
  // Filter to .parquet files and sort by name (descending for most recent)
  const parquetFiles = files
    .filter(f => f.name.endsWith('.parquet'))
    .sort((a, b) => b.name.localeCompare(a.name))
    .slice(0, limit);
  
  return parquetFiles.map(f => `gs://${BUCKET}/${f.name}`);
}

/**
 * Query max record_time from a set of Parquet files using DuckDB
 */
async function getMaxRecordTime(files) {
  if (files.length === 0) return null;
  
  const db = await Database.create(':memory:');
  
  // Install and load httpfs for GCS access
  await db.run("INSTALL httpfs; LOAD httpfs;");
  await db.run("SET s3_region='auto';");
  
  // Build a UNION query across all files
  const fileList = files.map(f => `'${f}'`).join(', ');
  
  try {
    const result = await db.all(`
      SELECT MAX(record_time) as max_record_time
      FROM read_parquet([${fileList}])
      WHERE record_time IS NOT NULL
    `);
    
    await db.close();
    return result[0]?.max_record_time || null;
  } catch (err) {
    if (VERBOSE) console.error(`  DuckDB error: ${err.message}`);
    await db.close();
    return null;
  }
}

/**
 * Read local cursor file
 */
function readLocalCursor(name) {
  const path = join(CURSOR_DIR, `${name}.json`);
  if (!existsSync(path)) return null;
  
  try {
    const data = JSON.parse(readFileSync(path, 'utf8'));
    return data;
  } catch {
    return null;
  }
}

/**
 * Read GCS cursor backup
 */
async function readGCSCursor(name) {
  try {
    const file = storage.bucket(BUCKET).file(`cursors/${name}.json`);
    const [exists] = await file.exists();
    if (!exists) return null;
    
    const [contents] = await file.download();
    return JSON.parse(contents.toString());
  } catch {
    return null;
  }
}

/**
 * Format timestamp for display
 */
function formatTime(ts) {
  if (!ts) return 'N/A';
  const date = new Date(typeof ts === 'string' ? ts : ts);
  return date.toISOString();
}

/**
 * Calculate time difference in human-readable format
 */
function timeDiff(ts1, ts2) {
  if (!ts1 || !ts2) return 'N/A';
  const d1 = new Date(ts1);
  const d2 = new Date(ts2);
  const diffMs = Math.abs(d1 - d2);
  
  const hours = Math.floor(diffMs / (1000 * 60 * 60));
  const minutes = Math.floor((diffMs % (1000 * 60 * 60)) / (1000 * 60));
  const seconds = Math.floor((diffMs % (1000 * 60)) / 1000);
  
  if (hours > 0) return `${hours}h ${minutes}m`;
  if (minutes > 0) return `${minutes}m ${seconds}s`;
  return `${seconds}s`;
}

async function main() {
  console.log('═══════════════════════════════════════════════════════════════');
  console.log('  CURSOR vs DATA AUDIT');
  console.log(`  Bucket: gs://${BUCKET}`);
  console.log(`  Local Cursor Dir: ${CURSOR_DIR}`);
  console.log('═══════════════════════════════════════════════════════════════\n');

  // --- Check for GCS cursor backup ---
  console.log('📂 GCS Cursor Backup Status:');
  const gcsCursor = await readGCSCursor('live-cursor');
  if (gcsCursor) {
    console.log(`  ✓ Found: gs://${BUCKET}/cursors/live-cursor.json`);
    console.log(`    after_offset: ${gcsCursor.after || 'N/A'}`);
    console.log(`    record_time: ${formatTime(gcsCursor.record_time)}`);
  } else {
    console.log(`  ✗ Not found: gs://${BUCKET}/cursors/live-cursor.json`);
    console.log('    (Backup created after first successful ingestion batch)');
  }
  console.log();

  // --- Check local cursor ---
  console.log('📂 Local Cursor Status:');
  const localCursor = readLocalCursor('live-cursor');
  if (localCursor) {
    console.log(`  ✓ Found: ${CURSOR_DIR}/live-cursor.json`);
    console.log(`    after_offset: ${localCursor.after || 'N/A'}`);
    console.log(`    record_time: ${formatTime(localCursor.record_time)}`);
  } else {
    console.log(`  ✗ Not found: ${CURSOR_DIR}/live-cursor.json`);
  }
  console.log();

  // --- Check backfill cursor (fallback boundary) ---
  console.log('📂 Backfill Cursor (Migration 4 boundary):');
  const backfillCursor = readLocalCursor('backfill-cursor');
  if (backfillCursor) {
    console.log(`  ✓ Found: ${CURSOR_DIR}/backfill-cursor.json`);
    console.log(`    after_offset: ${backfillCursor.after || 'N/A'}`);
    console.log(`    record_time: ${formatTime(backfillCursor.record_time)}`);
  } else {
    console.log(`  ✗ Not found`);
  }
  console.log();

  // --- Scan GCS for latest data ---
  console.log('🔍 Scanning GCS for latest record_time in data...');
  
  // Check updates partition
  console.log('\n  [updates/] partition:');
  const updateFiles = await listRecentFiles('raw/updates/', 30);
  if (VERBOSE) {
    console.log(`    Found ${updateFiles.length} recent Parquet files`);
    updateFiles.slice(0, 5).forEach(f => console.log(`      ${f}`));
  }
  
  const updatesMaxTime = await getMaxRecordTime(updateFiles);
  if (updatesMaxTime) {
    console.log(`    Latest record_time: ${formatTime(updatesMaxTime)}`);
  } else if (updateFiles.length === 0) {
    console.log('    No Parquet files found (partition may be empty)');
  } else {
    console.log('    Could not determine max record_time');
  }

  // Check backfill/updates partition
  console.log('\n  [backfill/updates/] partition:');
  const backfillFiles = await listRecentFiles('raw/backfill/updates/', 30);
  if (VERBOSE) {
    console.log(`    Found ${backfillFiles.length} recent Parquet files`);
  }
  
  const backfillMaxTime = await getMaxRecordTime(backfillFiles);
  if (backfillMaxTime) {
    console.log(`    Latest record_time: ${formatTime(backfillMaxTime)}`);
  } else if (backfillFiles.length === 0) {
    console.log('    No Parquet files found');
  } else {
    console.log('    Could not determine max record_time');
  }

  // --- Summary ---
  console.log('\n═══════════════════════════════════════════════════════════════');
  console.log('  SUMMARY');
  console.log('═══════════════════════════════════════════════════════════════');
  
  const effectiveCursor = localCursor || gcsCursor || backfillCursor;
  const cursorTime = effectiveCursor?.record_time;
  const latestDataTime = updatesMaxTime || backfillMaxTime;
  
  console.log(`  Cursor position:     ${formatTime(cursorTime)}`);
  console.log(`  Latest data in GCS:  ${formatTime(latestDataTime)}`);
  
  if (cursorTime && latestDataTime) {
    const cursorDate = new Date(cursorTime);
    const dataDate = new Date(latestDataTime);
    
    if (cursorDate > dataDate) {
      console.log(`  Status: ⚠️  Cursor AHEAD of data by ${timeDiff(cursorTime, latestDataTime)}`);
      console.log('           (This is normal - cursor tracks API position, data may lag)');
    } else if (cursorDate < dataDate) {
      console.log(`  Status: ⚠️  Cursor BEHIND data by ${timeDiff(cursorTime, latestDataTime)}`);
      console.log('           (Unusual - cursor should be at or ahead of written data)');
    } else {
      console.log('  Status: ✓ Cursor matches latest data');
    }
  } else if (!cursorTime && !latestDataTime) {
    console.log('  Status: 🆕 Fresh start - no cursor or data found');
  } else if (!cursorTime) {
    console.log('  Status: ⚠️  No cursor found but data exists');
    console.log('           Run fetch-updates.js to create cursor from backfill boundary');
  } else {
    console.log('  Status: 📭 Cursor exists but no live update data in GCS yet');
  }
  
  console.log('═══════════════════════════════════════════════════════════════\n');
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
