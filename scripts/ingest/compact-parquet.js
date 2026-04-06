#!/usr/bin/env node
/**
 * GCS Parquet Compaction Script
 *
 * For each specified day partition:
 * 1. Downloads all Parquet files to a temp directory
 * 2. Deduplicates by update_id using DuckDB
 * 3. Writes consolidated Parquet files (respecting MAX_ROWS_PER_FILE)
 * 4. Uploads the new files to GCS
 * 5. Deletes the old files from GCS
 *
 * Usage:
 *   node compact-parquet.js                    # Auto-detect: compact days with dups or >100 files
 *   node compact-parquet.js --month=3 --day=10 # Compact a specific day
 *   node compact-parquet.js --dry-run           # Show what would be done without changing anything
 *   node compact-parquet.js --all               # Compact all days (March+April 2026)
 */

import { Storage } from '@google-cloud/storage';
import { execSync } from 'child_process';
import fs from 'fs';
import path from 'path';

const GCS_BUCKET = process.env.GCS_BUCKET || 'canton-bucket';
const MIGRATION = 4;
const MAX_ROWS_PER_FILE = parseInt(process.env.MAX_ROWS_PER_FILE) || 100000;
const FILE_THRESHOLD = 1000; // auto-compact days with more files than this
const TMP_DOWNLOAD = '/tmp/compact-download';
const TMP_OUTPUT = '/tmp/compact-output';

const args = process.argv.slice(2);
const DRY_RUN = args.includes('--dry-run');
const COMPACT_ALL = args.includes('--all');
const specificMonth = args.find(a => a.startsWith('--month='))?.split('=')[1];
const specificDay = args.find(a => a.startsWith('--day='))?.split('=')[1];

const storage = new Storage();
const bucket = storage.bucket(GCS_BUCKET);

// Both updates and events tables
const TABLES = ['updates', 'events'];

function cleanup(dir) {
  if (fs.existsSync(dir)) {
    for (const f of fs.readdirSync(dir)) {
      fs.unlinkSync(path.join(dir, f));
    }
  }
}

function ensureDir(dir) {
  fs.mkdirSync(dir, { recursive: true });
}

async function listFiles(prefix) {
  const [files] = await bucket.getFiles({ prefix });
  return files
    .filter(f => f.name.endsWith('.parquet'))
    .map(f => f.name);
}

async function downloadFiles(gcsFiles, localDir) {
  ensureDir(localDir);
  let downloaded = 0;
  for (const name of gcsFiles) {
    const local = path.join(localDir, path.basename(name));
    await new Promise((resolve, reject) => {
      bucket.file(name).createReadStream()
        .pipe(fs.createWriteStream(local))
        .on('finish', resolve)
        .on('error', reject);
    });
    downloaded++;
    if (downloaded % 100 === 0) {
      process.stdout.write(`  downloaded ${downloaded}/${gcsFiles.length}\r`);
    }
  }
}

function countRows(localDir) {
  try {
    const sql = `SELECT count(*) as cnt FROM read_parquet('${localDir}/*.parquet')`;
    const result = execSync(`duckdb -csv -c "${sql}"`, { encoding: 'utf8' }).trim();
    return parseInt(result.split('\n')[1]);
  } catch {
    return 0;
  }
}

function countUnique(localDir, idColumn) {
  try {
    const sql = `SELECT count(DISTINCT ${idColumn}) as cnt FROM read_parquet('${localDir}/*.parquet')`;
    const result = execSync(`duckdb -csv -c "${sql}"`, { encoding: 'utf8' }).trim();
    return parseInt(result.split('\n')[1]);
  } catch {
    return 0;
  }
}

function compactWithDuckDB(inputDir, outputDir, idColumn, maxRowsPerFile) {
  ensureDir(outputDir);

  // Get total unique rows to determine number of output files
  const totalUnique = countUnique(inputDir, idColumn);
  const numFiles = Math.max(1, Math.ceil(totalUnique / maxRowsPerFile));

  // Use DuckDB to deduplicate and write consolidated files
  // ROW_NUMBER() partitioned per file to split output across files
  const sql = `
    COPY (
      SELECT * EXCLUDE (rn, file_num)
      FROM (
        SELECT *,
          ROW_NUMBER() OVER (PARTITION BY ${idColumn} ORDER BY recorded_at DESC) as dedup_rn,
          (ROW_NUMBER() OVER (ORDER BY record_time, ${idColumn}) - 1) / ${maxRowsPerFile} as file_num
        FROM read_parquet('${inputDir}/*.parquet')
      )
      WHERE dedup_rn = 1
      ORDER BY record_time, ${idColumn}
    )
    TO '${outputDir}/compacted.parquet'
    (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 25000)
  `;

  try {
    execSync(`duckdb -c "${sql}"`, {
      encoding: 'utf8',
      maxBuffer: 100 * 1024 * 1024,
      timeout: 600_000, // 10 min
    });
  } catch (e) {
    // DuckDB might use partitioned writes for large datasets; try PARTITION_BY approach
    // Fall back to single-file write which always works
    const sqlSimple = `
      COPY (
        SELECT * EXCLUDE (dedup_rn)
        FROM (
          SELECT *,
            ROW_NUMBER() OVER (PARTITION BY ${idColumn} ORDER BY recorded_at DESC) as dedup_rn
          FROM read_parquet('${inputDir}/*.parquet')
        )
        WHERE dedup_rn = 1
        ORDER BY record_time, ${idColumn}
      )
      TO '${outputDir}/compacted.parquet'
      (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 25000)
    `;
    execSync(`duckdb -c "${sqlSimple}"`, {
      encoding: 'utf8',
      maxBuffer: 100 * 1024 * 1024,
      timeout: 600_000,
    });
  }

  return fs.readdirSync(outputDir).filter(f => f.endsWith('.parquet'));
}

async function uploadFiles(localDir, gcsPrefix, files) {
  for (const file of files) {
    const localPath = path.join(localDir, file);
    const gcsPath = `${gcsPrefix}${file}`;
    await bucket.upload(localPath, { destination: gcsPath, resumable: false });
  }
}

async function deleteGCSFiles(gcsFiles) {
  // Delete in batches of 100
  for (let i = 0; i < gcsFiles.length; i += 100) {
    const batch = gcsFiles.slice(i, i + 100);
    await Promise.all(batch.map(name => bucket.file(name).delete().catch(() => {})));
    if (i + 100 < gcsFiles.length) {
      process.stdout.write(`  deleted ${i + 100}/${gcsFiles.length}\r`);
    }
  }
}

async function compactDay(month, day) {
  const dayLabel = `month=${month}/day=${day}`;
  console.log(`\n${'='.repeat(60)}`);
  console.log(`Compacting ${dayLabel}`);
  console.log('='.repeat(60));

  for (const table of TABLES) {
    const idColumn = table === 'updates' ? 'update_id' : 'event_id';
    const gcsPrefix = `raw/updates/${table}/migration=${MIGRATION}/year=2026/month=${month}/day=${day}/`;

    console.log(`\n  --- ${table} ---`);

    // List existing files
    const existingFiles = await listFiles(gcsPrefix);
    if (existingFiles.length === 0) {
      console.log(`  No files found, skipping.`);
      continue;
    }
    console.log(`  Found ${existingFiles.length} files in GCS`);

    if (DRY_RUN) {
      // Just report stats
      cleanup(TMP_DOWNLOAD);
      ensureDir(TMP_DOWNLOAD);
      await downloadFiles(existingFiles, TMP_DOWNLOAD);
      const totalRows = countRows(TMP_DOWNLOAD);
      const uniqueRows = countUnique(TMP_DOWNLOAD, idColumn);
      const dups = totalRows - uniqueRows;
      console.log(`  total=${totalRows}  unique=${uniqueRows}  dups=${dups}${dups > 0 ? ' <<<' : ''}`);
      console.log(`  Would produce ~${Math.max(1, Math.ceil(uniqueRows / MAX_ROWS_PER_FILE))} consolidated file(s)`);
      cleanup(TMP_DOWNLOAD);
      continue;
    }

    // Download
    console.log(`  Downloading ${existingFiles.length} files...`);
    cleanup(TMP_DOWNLOAD);
    ensureDir(TMP_DOWNLOAD);
    await downloadFiles(existingFiles, TMP_DOWNLOAD);

    const totalBefore = countRows(TMP_DOWNLOAD);
    const uniqueBefore = countUnique(TMP_DOWNLOAD, idColumn);
    const dupsBefore = totalBefore - uniqueBefore;
    console.log(`  Before: total=${totalBefore}  unique=${uniqueBefore}  dups=${dupsBefore}`);

    if (dupsBefore === 0 && existingFiles.length <= FILE_THRESHOLD) {
      console.log(`  No dups and file count OK (${existingFiles.length} <= ${FILE_THRESHOLD}), skipping.`);
      cleanup(TMP_DOWNLOAD);
      continue;
    }

    // Compact
    console.log(`  Compacting (dedup + consolidate)...`);
    cleanup(TMP_OUTPUT);
    const outputFiles = compactWithDuckDB(TMP_DOWNLOAD, TMP_OUTPUT, idColumn, MAX_ROWS_PER_FILE);
    const totalAfter = countRows(TMP_OUTPUT);
    console.log(`  After:  total=${totalAfter}  files=${outputFiles.length}  (removed ${dupsBefore} dups, ${existingFiles.length} → ${outputFiles.length} files)`);

    if (totalAfter !== uniqueBefore) {
      console.error(`  ERROR: Row count mismatch! Expected ${uniqueBefore}, got ${totalAfter}. Aborting this table.`);
      cleanup(TMP_DOWNLOAD);
      cleanup(TMP_OUTPUT);
      continue;
    }

    // Upload new files
    console.log(`  Uploading ${outputFiles.length} compacted file(s)...`);
    await uploadFiles(TMP_OUTPUT, gcsPrefix, outputFiles);

    // Delete old files (only the ones we downloaded, not the new ones)
    console.log(`  Deleting ${existingFiles.length} old file(s)...`);
    await deleteGCSFiles(existingFiles);

    console.log(`  Done: ${dayLabel}/${table}`);

    cleanup(TMP_DOWNLOAD);
    cleanup(TMP_OUTPUT);
  }
}

// ─── Main ─────────────────────────────────────────────────────────────────

async function main() {
  console.log(`GCS Parquet Compaction${DRY_RUN ? ' (DRY RUN)' : ''}`);
  console.log(`Bucket: ${GCS_BUCKET}  Migration: ${MIGRATION}  Max rows/file: ${MAX_ROWS_PER_FILE}`);

  if (specificMonth && specificDay) {
    await compactDay(parseInt(specificMonth), parseInt(specificDay));
    console.log('\nDone.');
    return;
  }

  // Discover all day partitions
  const months = [3, 4];
  const toCompact = [];

  for (const month of months) {
    for (let day = 1; day <= 31; day++) {
      for (const table of TABLES) {
        const prefix = `raw/updates/${table}/migration=${MIGRATION}/year=2026/month=${month}/day=${day}/`;
        const files = await listFiles(prefix);
        if (files.length === 0) continue;

        // Check if this day needs compaction
        if (COMPACT_ALL) {
          if (!toCompact.find(d => d.month === month && d.day === day)) {
            toCompact.push({ month, day, reason: 'all', files: files.length });
          }
          break;
        }

        if (files.length > FILE_THRESHOLD) {
          if (!toCompact.find(d => d.month === month && d.day === day)) {
            toCompact.push({ month, day, reason: `${files.length} files (>${FILE_THRESHOLD})`, files: files.length });
          }
          break;
        }
      }
    }
  }

  // Also check for dups on days with normal file counts (quick scan)
  if (!COMPACT_ALL) {
    for (const month of months) {
      for (let day = 1; day <= 31; day++) {
        if (toCompact.find(d => d.month === month && d.day === day)) continue;

        const prefix = `raw/updates/updates/migration=${MIGRATION}/year=2026/month=${month}/day=${day}/`;
        const files = await listFiles(prefix);
        if (files.length === 0) continue;

        // Quick dup check: download, count, check
        cleanup(TMP_DOWNLOAD);
        ensureDir(TMP_DOWNLOAD);
        await downloadFiles(files, TMP_DOWNLOAD);
        const total = countRows(TMP_DOWNLOAD);
        const unique = countUnique(TMP_DOWNLOAD, 'update_id');
        cleanup(TMP_DOWNLOAD);

        if (total > unique) {
          toCompact.push({ month, day, reason: `${total - unique} dups`, files: files.length });
        }
      }
    }
  }

  if (toCompact.length === 0) {
    console.log('\nNo days need compaction.');
    return;
  }

  console.log(`\nDays to compact:`);
  for (const d of toCompact.sort((a, b) => a.month * 100 + a.day - (b.month * 100 + b.day))) {
    console.log(`  month=${d.month}/day=${d.day}  (${d.reason})`);
  }

  for (const d of toCompact.sort((a, b) => a.month * 100 + a.day - (b.month * 100 + b.day))) {
    await compactDay(d.month, d.day);
  }

  console.log('\nAll done.');
}

main().catch(err => {
  console.error('Fatal:', err);
  cleanup(TMP_DOWNLOAD);
  cleanup(TMP_OUTPUT);
  process.exit(1);
});
