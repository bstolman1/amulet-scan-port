#!/usr/bin/env node
/**
 * Phase 2: GCS Partition Repair Script
 * 
 * Scans existing GCS Parquet files and re-layouts them into correct UTC-based
 * Hive partitions. Handles backfill, live updates, and ACS snapshot streams.
 * 
 * The problem: historical files may have been partitioned using local time
 * instead of UTC, placing files in wrong day/snapshot_id folders.
 * 
 * Usage:
 *   node repair-partitions.js                        # dry-run, all streams
 *   node repair-partitions.js --execute              # actually move files
 *   node repair-partitions.js --verify               # verify-only (check existing files)
 *   node repair-partitions.js --execute --verify      # move files, then verify destinations
 *   node repair-partitions.js --stream=acs            # only ACS
 *   node repair-partitions.js --stream=backfill       # only backfill (updates + events)
 *   node repair-partitions.js --stream=updates        # only live updates (updates + events)
 *   node repair-partitions.js --migration=4           # only migration 4
 */

import { execSync } from 'child_process';
import { mkdirSync, existsSync, appendFileSync, writeFileSync, unlinkSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import duckdb from 'duckdb';

import { getUtcPartition, getPartitionPath } from './data-schema.js';
import { getACSPartitionPath } from './acs-schema.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// ── CLI flags ──────────────────────────────────────────────────────────────────

const args = process.argv.slice(2);
const EXECUTE = args.includes('--execute');
const VERIFY_FLAG = args.includes('--verify');
const STREAM_ARG = args.find(a => a.startsWith('--stream='))?.split('=')[1] || 'all';
const MIGRATION_ARG = args.find(a => a.startsWith('--migration='))?.split('=')[1];
const MIGRATION_FILTER = MIGRATION_ARG != null ? parseInt(MIGRATION_ARG) : null;

const GCS_BUCKET = process.env.GCS_BUCKET;
const IS_MAIN = process.argv[1]?.endsWith('repair-partitions.js');

if (IS_MAIN && !GCS_BUCKET) {
  console.error('❌ GCS_BUCKET environment variable is required');
  process.exit(1);
}

const TMP_DIR = '/tmp/repair-partitions';
const LOG_FILE = join(TMP_DIR, 'repair-log.jsonl');

// ── Helpers ────────────────────────────────────────────────────────────────────

function exec(cmd) {
  return execSync(cmd, { stdio: 'pipe', timeout: 60_000 }).toString().trim();
}

function log(entry) {
  const line = JSON.stringify({ ...entry, ts: new Date().toISOString() });
  console.log(`  ${entry.action === 'verify_failed' ? '❌' : entry.action === 'skip' ? '✅' : '🔧'} ${entry.action}: ${entry.file || entry.from || ''}`);
  appendFileSync(LOG_FILE, line + '\n');
}

function ensureDir(dir) {
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
}

/**
 * Read a column from a local Parquet file via DuckDB.
 * Returns array of distinct string values.
 */
function readParquetColumn(db, localPath, column) {
  return new Promise((resolve, reject) => {
    const safePath = localPath.replace(/\\/g, '/');
    const sql = `SELECT DISTINCT "${column}"::VARCHAR AS val FROM read_parquet('${safePath}') WHERE "${column}" IS NOT NULL`;
    db.all(sql, (err, rows) => {
      if (err) reject(err);
      else resolve(rows.map(r => r.val));
    });
  });
}

/**
 * Extract rows matching a filter into a new local Parquet file via DuckDB.
 */
function extractToParquet(db, srcPath, destPath, whereClause) {
  return new Promise((resolve, reject) => {
    const src = srcPath.replace(/\\/g, '/');
    const dst = destPath.replace(/\\/g, '/');
    const sql = `COPY (SELECT * FROM read_parquet('${src}') WHERE ${whereClause}) TO '${dst}' (FORMAT PARQUET, COMPRESSION ZSTD)`;
    db.run(sql, (err) => {
      if (err) reject(err);
      else resolve();
    });
  });
}

/**
 * Count rows in a local Parquet file.
 */
function countRows(db, localPath) {
  return new Promise((resolve, reject) => {
    const safePath = localPath.replace(/\\/g, '/');
    db.all(`SELECT count(*)::INTEGER AS cnt FROM read_parquet('${safePath}')`, (err, rows) => {
      if (err) reject(err);
      else resolve(rows[0]?.cnt ?? 0);
    });
  });
}

// ── Stream definitions ─────────────────────────────────────────────────────────

/**
 * @typedef {Object} StreamConfig
 * @property {string} name - Human readable name
 * @property {string} prefix - GCS prefix under raw/
 * @property {string} timestampCol - Column to check for UTC correctness
 * @property {boolean} isACS - Whether this is an ACS stream (different partition structure)
 * @property {function} computeCorrectPath - Given timestamp + migrationId, return correct GCS prefix
 */

function getStreams() {
  const streams = [];

  if (STREAM_ARG === 'all' || STREAM_ARG === 'backfill') {
    streams.push(
      {
        name: 'backfill/updates',
        prefix: `gs://${GCS_BUCKET}/raw/backfill/updates/`,
        timestampCol: 'effective_at',
        isACS: false,
        computeCorrectPath: (ts, mig) => getPartitionPath(ts, mig, 'updates', 'backfill'),
      },
      {
        name: 'backfill/events',
        prefix: `gs://${GCS_BUCKET}/raw/backfill/events/`,
        timestampCol: 'effective_at',
        isACS: false,
        computeCorrectPath: (ts, mig) => getPartitionPath(ts, mig, 'events', 'backfill'),
      },
    );
  }

  if (STREAM_ARG === 'all' || STREAM_ARG === 'updates') {
    streams.push(
      {
        name: 'updates/updates',
        prefix: `gs://${GCS_BUCKET}/raw/updates/updates/`,
        timestampCol: 'effective_at',
        isACS: false,
        computeCorrectPath: (ts, mig) => getPartitionPath(ts, mig, 'updates', 'updates'),
      },
      {
        name: 'updates/events',
        prefix: `gs://${GCS_BUCKET}/raw/updates/events/`,
        timestampCol: 'effective_at',
        isACS: false,
        computeCorrectPath: (ts, mig) => getPartitionPath(ts, mig, 'events', 'updates'),
      },
    );
  }

  if (STREAM_ARG === 'all' || STREAM_ARG === 'acs') {
    streams.push({
      name: 'acs',
      prefix: `gs://${GCS_BUCKET}/raw/acs/`,
      timestampCol: 'snapshot_time',
      isACS: true,
      computeCorrectPath: (ts, mig) => getACSPartitionPath(ts, mig),
    });
  }

  return streams;
}

// ── GCS scanning ───────────────────────────────────────────────────────────────

/**
 * List all Parquet-compatible files under a GCS prefix, recursively.
 * Matches both .parquet extension files AND extensionless files (application/octet-stream)
 * that live inside Hive partition folders (day=X/).
 * Returns array of gs:// URIs.
 */
function listGCSParquetFiles(prefix) {
  try {
    const output = exec(`gsutil ls -r "${prefix}" 2>/dev/null || true`);
    if (!output) return [];
    return output.split('\n').filter(l => {
      if (!l.startsWith('gs://')) return false;
      // Skip directory listings (end with /:)
      if (l.endsWith('/:') || l.endsWith('/')) return false;
      // Accept .parquet files
      if (l.endsWith('.parquet')) return true;
      // Accept extensionless files inside day= partition folders
      if (/\/day=\d+\/[^/]+$/.test(l) && !l.includes('.jsonl') && !l.includes('.json') && !l.includes('.zst')) return true;
      return false;
    });
  } catch {
    return [];
  }
}

/**
 * Parse migration ID from a GCS path like .../migration=4/...
 */
export function parseMigrationFromPath(gcsPath) {
  const match = gcsPath.match(/migration=(\d+)/);
  return match ? parseInt(match[1]) : null;
}

/**
 * Parse the current partition from a GCS path.
 * Returns { year, month, day } for ledger streams, or { year, month, day, snapshotId } for ACS.
 */
export function parseCurrentPartition(gcsPath, isACS) {
  const yearMatch = gcsPath.match(/year=(\d+)/);
  const monthMatch = gcsPath.match(/month=(\d+)/);
  const dayMatch = gcsPath.match(/day=(\d+)/);

  const result = {
    year: yearMatch ? parseInt(yearMatch[1]) : null,
    month: monthMatch ? parseInt(monthMatch[1]) : null,
    day: dayMatch ? parseInt(dayMatch[1]) : null,
  };

  if (isACS) {
    const snapMatch = gcsPath.match(/snapshot_id=(\d+)/);
    result.snapshotId = snapMatch ? snapMatch[1] : null;
  }

  return result;
}

/**
 * Determine the repair action for a file given its GCS path and the timestamps it contains.
 * Pure function — no side effects.
 * 
 * @param {string} gcsFile - Full gs:// URI of the file
 * @param {string[]} timestamps - Distinct timestamp values from the file
 * @param {object} stream - Stream config with computeCorrectPath
 * @param {string} bucket - GCS bucket name
 * @returns {{ action: string, from?: string, to?: string, splits?: object[], reason?: string }}
 */
export function determineRepairAction(gcsFile, timestamps, stream, bucket) {
  if (!timestamps || timestamps.length === 0) {
    return { action: 'skip', reason: `no ${stream.timestampCol} values` };
  }

  const migrationId = parseMigrationFromPath(gcsFile);

  // Group timestamps by their correct UTC partition
  const partitionGroups = {};
  for (const ts of timestamps) {
    const correctPath = stream.computeCorrectPath(ts, migrationId);
    if (!partitionGroups[correctPath]) partitionGroups[correctPath] = [];
    partitionGroups[correctPath].push(ts);
  }

  const correctPaths = Object.keys(partitionGroups);
  const currentGCSDir = gcsFile.substring(0, gcsFile.lastIndexOf('/') + 1);
  const fileName = gcsFile.substring(gcsFile.lastIndexOf('/') + 1);

  if (correctPaths.length === 1) {
    const correctGCSDir = `gs://${bucket}/raw/${correctPaths[0]}/`;

    if (currentGCSDir === correctGCSDir) {
      return { action: 'skip', reason: 'already correct' };
    }

    return {
      action: 'move',
      from: gcsFile,
      to: `${correctGCSDir}${fileName}`,
      rows: timestamps.length,
    };
  }

  // Multiple partitions → split
  const splits = Object.entries(partitionGroups).map(([correctPath, tsGroup]) => {
    const correctGCSDir = `gs://${bucket}/raw/${correctPath}/`;
    return {
      partition: correctPath,
      to: `${correctGCSDir}${fileName}`,
      timestamps: tsGroup,
    };
  });

  return { action: 'split', from: gcsFile, splits };
}

/**
 * Check whether a file's timestamps all match its current GCS partition.
 * Pure function — no side effects.
 * 
 * @param {string} gcsFile - Full gs:// URI
 * @param {string[]} timestamps - Distinct timestamp values
 * @param {object} stream - Stream config
 * @param {string} bucket - GCS bucket name
 * @returns {{ passed: boolean, failedTimestamp?: string, expected?: string, actual?: string }}
 */
export function checkVerification(gcsFile, timestamps, stream, bucket) {
  if (!timestamps || timestamps.length === 0) {
    return { passed: true, skipped: true };
  }

  const migrationId = parseMigrationFromPath(gcsFile);
  const currentGCSDir = gcsFile.substring(0, gcsFile.lastIndexOf('/') + 1);

  for (const ts of timestamps) {
    const correctPath = stream.computeCorrectPath(ts, migrationId);
    const correctGCSDir = `gs://${bucket}/raw/${correctPath}/`;

    if (currentGCSDir !== correctGCSDir) {
      return {
        passed: false,
        failedTimestamp: ts,
        expected: correctGCSDir,
        actual: currentGCSDir,
      };
    }
  }

  return { passed: true };
}

// ── Core repair logic ──────────────────────────────────────────────────────────

/**
 * Process a single Parquet file: download, check partition, move if wrong.
 */
async function processFile(db, gcsFile, stream, stats) {
  const migrationId = parseMigrationFromPath(gcsFile);
  if (MIGRATION_FILTER != null && migrationId !== MIGRATION_FILTER) return;

  const currentPartition = parseCurrentPartition(gcsFile, stream.isACS);
  const localFile = join(TMP_DIR, 'current.parquet');

  // Download
  try {
    exec(`gsutil cp "${gcsFile}" "${localFile}" 2>/dev/null`);
  } catch (err) {
    log({ action: 'download_error', file: gcsFile, error: err.message });
    stats.errors++;
    return;
  }

  try {
    // Read distinct timestamps
    const timestamps = await readParquetColumn(db, localFile, stream.timestampCol);

    if (timestamps.length === 0) {
      log({ action: 'skip', file: gcsFile, reason: `no ${stream.timestampCol} values` });
      stats.skipped++;
      return;
    }

    // Group timestamps by their correct UTC partition
    const partitionGroups = {};
    for (const ts of timestamps) {
      const correctPath = stream.computeCorrectPath(ts, migrationId);
      if (!partitionGroups[correctPath]) partitionGroups[correctPath] = [];
      partitionGroups[correctPath].push(ts);
    }

    const correctPaths = Object.keys(partitionGroups);

    // Check if file is already in the correct single partition
    if (correctPaths.length === 1) {
      const correctGCSDir = `gs://${GCS_BUCKET}/raw/${correctPaths[0]}/`;
      const currentGCSDir = gcsFile.substring(0, gcsFile.lastIndexOf('/') + 1);

      if (currentGCSDir === correctGCSDir) {
        log({ action: 'skip', file: gcsFile, reason: 'already correct' });
        stats.correct++;
        return;
      }

      // Single partition but wrong folder → move
      const fileName = gcsFile.substring(gcsFile.lastIndexOf('/') + 1);
      const destGCS = `${correctGCSDir}${fileName}`;

      if (EXECUTE) {
        exec(`gsutil cp "${gcsFile}" "${destGCS}"`);
        exec(`gsutil rm "${gcsFile}"`);
        log({ action: 'move', from: gcsFile, to: destGCS, rows: timestamps.length });
      } else {
        log({ action: 'would_move', from: gcsFile, to: destGCS, rows: timestamps.length });
      }
      stats.moved++;
      return;
    }

    // Multiple partitions → split
    const fileName = gcsFile.substring(gcsFile.lastIndexOf('/') + 1);
    const baseName = fileName.replace('.parquet', '');

    for (const [correctPath, tsGroup] of Object.entries(partitionGroups)) {
      const splitFile = join(TMP_DIR, `split-${baseName}-${correctPath.replace(/\//g, '_')}.parquet`);
      const correctGCSDir = `gs://${GCS_BUCKET}/raw/${correctPath}/`;
      const destGCS = `${correctGCSDir}${fileName}`;

      // Build WHERE clause for this partition's timestamps
      const { year, month, day } = getUtcPartition(tsGroup[0]);
      const dayStart = `${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}T00:00:00`;
      const nextDay = new Date(Date.UTC(year, month - 1, day + 1));
      const dayEnd = nextDay.toISOString().replace(/\.\d{3}Z$/, '');

      const whereClause = `"${stream.timestampCol}" >= '${dayStart}' AND "${stream.timestampCol}" < '${dayEnd}'`;

      if (EXECUTE) {
        await extractToParquet(db, localFile, splitFile, whereClause);
        const splitRows = await countRows(db, splitFile);
        exec(`gsutil cp "${splitFile}" "${destGCS}"`);
        if (existsSync(splitFile)) unlinkSync(splitFile);
        log({ action: 'split', from: gcsFile, to: destGCS, rows: splitRows, partition: correctPath });
      } else {
        log({ action: 'would_split', from: gcsFile, to: destGCS, partition: correctPath });
      }
    }

    // Delete original after all splits uploaded
    if (EXECUTE) {
      exec(`gsutil rm "${gcsFile}"`);
    }
    stats.split++;

  } catch (err) {
    log({ action: 'process_error', file: gcsFile, error: err.message });
    stats.errors++;
  } finally {
    if (existsSync(localFile)) unlinkSync(localFile);
  }
}

/**
 * Verify a single file is in the correct UTC partition.
 */
async function verifyFile(db, gcsFile, stream, stats) {
  const migrationId = parseMigrationFromPath(gcsFile);
  if (MIGRATION_FILTER != null && migrationId !== MIGRATION_FILTER) return;

  const localFile = join(TMP_DIR, 'verify.parquet');

  try {
    exec(`gsutil cp "${gcsFile}" "${localFile}" 2>/dev/null`);
  } catch (err) {
    log({ action: 'verify_download_error', file: gcsFile, error: err.message });
    stats.verifyErrors++;
    return;
  }

  try {
    const timestamps = await readParquetColumn(db, localFile, stream.timestampCol);
    if (timestamps.length === 0) {
      stats.verifySkipped++;
      return;
    }

    // Check every timestamp maps to the current folder
    const currentGCSDir = gcsFile.substring(0, gcsFile.lastIndexOf('/') + 1);

    for (const ts of timestamps) {
      const correctPath = stream.computeCorrectPath(ts, migrationId);
      const correctGCSDir = `gs://${GCS_BUCKET}/raw/${correctPath}/`;

      if (currentGCSDir !== correctGCSDir) {
        log({
          action: 'verify_failed',
          file: gcsFile,
          timestamp: ts,
          expectedPartition: correctGCSDir,
          actualPartition: currentGCSDir,
        });
        stats.verifyFailed++;
        return; // One failure per file is enough
      }
    }

    stats.verifyPassed++;

  } catch (err) {
    log({ action: 'verify_error', file: gcsFile, error: err.message });
    stats.verifyErrors++;
  } finally {
    if (existsSync(localFile)) unlinkSync(localFile);
  }
}

// ── Main ───────────────────────────────────────────────────────────────────────

async function main() {
  console.log('🔧 Phase 2: GCS Partition Repair');
  console.log(`   Mode: ${EXECUTE ? '🚀 EXECUTE' : '👀 DRY-RUN'}`);
  console.log(`   Verify: ${VERIFY_FLAG ? 'YES' : 'NO'}`);
  console.log(`   Stream: ${STREAM_ARG}`);
  console.log(`   Migration filter: ${MIGRATION_FILTER ?? 'all'}`);
  console.log(`   Bucket: ${GCS_BUCKET}`);
  console.log(`   Log: ${LOG_FILE}\n`);

  ensureDir(TMP_DIR);
  writeFileSync(LOG_FILE, ''); // Clear log

  // Init DuckDB
  const database = new duckdb.Database(':memory:');
  const db = database.connect();

  const streams = getStreams();
  const totalStats = {
    correct: 0, moved: 0, split: 0, skipped: 0, errors: 0,
    verifyPassed: 0, verifyFailed: 0, verifyErrors: 0, verifySkipped: 0,
  };

  for (const stream of streams) {
    console.log(`\n📂 Scanning stream: ${stream.name}`);
    console.log(`   Prefix: ${stream.prefix}`);

    const files = listGCSParquetFiles(stream.prefix);
    console.log(`   Found ${files.length} Parquet files`);

    if (files.length === 0) continue;

    // Phase A: Repair (unless verify-only)
    if (!VERIFY_FLAG || EXECUTE) {
      console.log(`\n   🔍 Checking partitions...`);
      for (let i = 0; i < files.length; i++) {
        if ((i + 1) % 50 === 0 || i === files.length - 1) {
          console.log(`   Progress: ${i + 1}/${files.length}`);
        }
        await processFile(db, files[i], stream, totalStats);
      }
    }

    // Phase B: Verify
    if (VERIFY_FLAG) {
      // If we executed moves, re-scan to pick up new locations
      const verifyFiles = EXECUTE ? listGCSParquetFiles(stream.prefix) : files;
      console.log(`\n   🔎 Verifying ${verifyFiles.length} files...`);

      for (let i = 0; i < verifyFiles.length; i++) {
        if ((i + 1) % 50 === 0 || i === verifyFiles.length - 1) {
          console.log(`   Verify progress: ${i + 1}/${verifyFiles.length}`);
        }
        await verifyFile(db, verifyFiles[i], stream, totalStats);
      }
    }
  }

  // Summary
  console.log('\n' + '═'.repeat(60));
  console.log('📊 Repair Summary');
  console.log('═'.repeat(60));
  console.log(`   ✅ Already correct: ${totalStats.correct}`);
  console.log(`   🔧 ${EXECUTE ? 'Moved' : 'Would move'}: ${totalStats.moved}`);
  console.log(`   ✂️  ${EXECUTE ? 'Split' : 'Would split'}: ${totalStats.split}`);
  console.log(`   ⏭️  Skipped (no timestamps): ${totalStats.skipped}`);
  console.log(`   ❌ Errors: ${totalStats.errors}`);

  if (VERIFY_FLAG) {
    console.log(`   ──────────────────────────`);
    console.log(`   ✅ Verify passed: ${totalStats.verifyPassed}`);
    console.log(`   ❌ Verify failed: ${totalStats.verifyFailed}`);
    console.log(`   ⚠️  Verify errors: ${totalStats.verifyErrors}`);
  }

  console.log(`\n   📝 Full log: ${LOG_FILE}`);

  if (!EXECUTE && (totalStats.moved > 0 || totalStats.split > 0)) {
    console.log(`\n   ⚠️  This was a dry run. Re-run with --execute to apply changes.`);
  }

  if (totalStats.verifyFailed > 0) {
    process.exit(1);
  }
}

if (IS_MAIN) {
  main().catch(err => {
    console.error('💥 Fatal error:', err.message);
    console.error(err.stack);
    process.exit(1);
  });
}
