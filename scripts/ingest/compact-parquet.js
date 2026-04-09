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
 *   node compact-parquet.js                        # Auto-detect: compact days with dups or >100 files
 *   node compact-parquet.js --month=3 --day=10     # Compact a specific day
 *   node compact-parquet.js --days=3/6,3/10,4/4    # Compact a list of days
 *   node compact-parquet.js --dry-run              # Show what would be done without changing anything
 *   node compact-parquet.js --all                  # Compact all days (March+April 2026)
 *   node compact-parquet.js --inspect --days=3/10  # List files + per-file row counts (no changes)
 *
 * Correctness guarantees:
 * - Pass 1 dup-ID detection failures are fatal (not silently swallowed).
 * - If count(*) - count(DISTINCT id) disagrees with GROUP BY HAVING count(*)>1,
 *   the compaction aborts for that table (inconsistency check).
 * - Post-compaction the output is re-checked for duplicates before any upload.
 * - Output file names include a per-run ID, so re-running compaction on the
 *   same day cannot clobber its own new files during the delete-old-files step.
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
const SKIP_DUP_SCAN = args.includes('--skip-dup-scan');
const INSPECT = args.includes('--inspect');
const specificMonth = args.find(a => a.startsWith('--month='))?.split('=')[1];
const specificDay = args.find(a => a.startsWith('--day='))?.split('=')[1];
// --days=3/6,3/10,3/30,4/4,4/5  — compact specific days without scanning
const specificDays = args.find(a => a.startsWith('--days='))?.split('=')[1]
  ?.split(',').map(d => {
    const [m, dy] = d.split('/').map(Number);
    return { month: m, day: dy };
  }) || [];

// Unique per-run prefix to guarantee output file names can't collide with
// files from a previous compaction run. Without this, running compaction
// twice on the same day would upload part-0000.parquet (overwriting the
// previous part-0000.parquet) and then delete it again as part of the
// "existing files" cleanup — wiping the new data.
const RUN_ID = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

const storage = new Storage();
const bucket = storage.bucket(GCS_BUCKET);

// Both updates and events tables
const TABLES = ['updates', 'events'];

function cleanup(dir) {
  if (fs.existsSync(dir)) {
    fs.rmSync(dir, { recursive: true, force: true });
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

function compactWithDuckDB(inputDir, outputDir, idColumn, maxRowsPerFile, expectedDupCount) {
  ensureDir(outputDir);

  // Two-pass approach:
  // Pass 1: Find duplicate IDs (tiny query — just IDs + count)
  // Pass 2: Write consolidated output
  const sqlFile = path.join(outputDir, '_compact.sql');
  const outFile = path.join(outputDir, 'compacted.parquet');

  // Pass 1: find dup IDs.
  // Write the query to a SQL file and pipe it in, so we get predictable
  // DuckDB settings (memory_limit) and the query never hits the shell
  // command-line. Errors must propagate — silently swallowing them and
  // falling into the "no dups" path was the root cause of compacted output
  // still containing duplicates.
  const dupSqlFile = path.join(outputDir, '_find_dups.sql');
  const dupOutFile = path.join(outputDir, '_dup_ids.csv');
  const dupSql = [
    "SET memory_limit='8GB';",
    "SET threads=2;",
    "SET preserve_insertion_order=false;",
    `COPY (SELECT ${idColumn} FROM read_parquet('${inputDir}/*.parquet') GROUP BY ${idColumn} HAVING count(*) > 1) TO '${dupOutFile}' (FORMAT CSV, HEADER false);`,
  ].join('\n');
  fs.writeFileSync(dupSqlFile, dupSql);
  execSync(`cat ${dupSqlFile} | duckdb`, {
    encoding: 'utf8',
    maxBuffer: 10 * 1024 * 1024,
    timeout: 600_000, // 10 min
    stdio: ['pipe', 'pipe', 'inherit'],
  });
  try { fs.unlinkSync(dupSqlFile); } catch {}

  let dupIds = [];
  if (fs.existsSync(dupOutFile)) {
    const raw = fs.readFileSync(dupOutFile, 'utf8').trim();
    if (raw) {
      dupIds = raw.split('\n').filter(l => l.trim());
    }
    try { fs.unlinkSync(dupOutFile); } catch {}
  }

  // Consistency check: if the caller detected dups via countRows-countUnique
  // but Pass 1 returns zero dup IDs, something is wrong. Fail loud rather
  // than silently consolidating and losing the dedup guarantee.
  if (expectedDupCount > 0 && dupIds.length === 0) {
    throw new Error(
      `Dup detection mismatch: expected >0 duplicate IDs (saw ${expectedDupCount} dup rows via count(*)-count(DISTINCT)) ` +
      `but Pass 1 GROUP BY HAVING count(*)>1 returned 0 IDs. Aborting to avoid writing unchanged data.`
    );
  }

  if (dupIds.length === 0) {
    // No dups — just consolidate files into one
    console.log(`  No dups, consolidating files...`);
    const sql = [
      "SET memory_limit='8GB';",
      "SET threads=2;",
      "SET preserve_insertion_order=false;",
      `COPY (SELECT * FROM read_parquet('${inputDir}/*.parquet'))`,
      `TO '${outFile}'`,
      "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 25000);",
    ].join('\n');
    fs.writeFileSync(sqlFile, sql);
  } else {
    // Build a filter list for the small number of dup IDs
    const idList = dupIds.map(id => `'${id.replace(/'/g, "''")}'`).join(',');
    console.log(`  Found ${dupIds.length} duplicate ID(s), deduplicating...`);

    const sql = [
      "SET memory_limit='8GB';",
      "SET threads=2;",
      "SET preserve_insertion_order=false;",
      "COPY (",
      `  SELECT * FROM read_parquet('${inputDir}/*.parquet')`,
      `  WHERE ${idColumn} NOT IN (${idList})`,
      "  UNION ALL",
      "  SELECT * EXCLUDE (_rn) FROM (",
      `    SELECT *, ROW_NUMBER() OVER (PARTITION BY ${idColumn} ORDER BY recorded_at DESC) as _rn`,
      `    FROM read_parquet('${inputDir}/*.parquet')`,
      `    WHERE ${idColumn} IN (${idList})`,
      "  ) WHERE _rn = 1",
      ")",
      `TO '${outFile}'`,
      "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 25000);",
    ].join('\n');
    fs.writeFileSync(sqlFile, sql);
  }

  execSync(`cat ${sqlFile} | duckdb`, {
    encoding: 'utf8',
    maxBuffer: 100 * 1024 * 1024,
    timeout: 1_800_000, // 30 min
  });

  // Clean up the SQL file
  try { fs.unlinkSync(sqlFile); } catch {}

  // Split the single output file into chunks of maxRowsPerFile.
  // countRows reads ONLY the single compacted.parquet at this point — no
  // other .parquet files exist in outputDir yet.
  const totalRows = countRows(outputDir);
  if (totalRows > maxRowsPerFile) {
    const numChunks = Math.ceil(totalRows / maxRowsPerFile);
    console.log(`  Splitting into ${numChunks} files (${maxRowsPerFile} rows each)...`);
    const splitSql = [
      "SET memory_limit='8GB';",
      "SET threads=2;",
      "SET preserve_insertion_order=false;",
    ];
    for (let i = 0; i < numChunks; i++) {
      const offset = i * maxRowsPerFile;
      const chunkFile = path.join(outputDir, `compact-${RUN_ID}-${String(i).padStart(4, '0')}.parquet`);
      splitSql.push(
        `COPY (SELECT * FROM read_parquet('${outFile}') LIMIT ${maxRowsPerFile} OFFSET ${offset})`,
        `TO '${chunkFile}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 25000);`
      );
    }
    const splitFile = path.join(outputDir, '_split.sql');
    fs.writeFileSync(splitFile, splitSql.join('\n'));
    execSync(`cat ${splitFile} | duckdb`, {
      encoding: 'utf8',
      maxBuffer: 100 * 1024 * 1024,
      timeout: 1_800_000,
    });
    // Remove the single big file and the SQL file
    fs.unlinkSync(outFile);
    try { fs.unlinkSync(splitFile); } catch {}
  } else {
    // Small dataset fits in a single file — rename from the generic
    // 'compacted.parquet' to a unique name so a future re-run can't collide
    // with it on upload/delete.
    const uniqueFile = path.join(outputDir, `compact-${RUN_ID}-0000.parquet`);
    fs.renameSync(outFile, uniqueFile);
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

async function inspectDay(month, day) {
  const dayLabel = `month=${month}/day=${day}`;
  console.log(`\n${'='.repeat(60)}`);
  console.log(`Inspecting ${dayLabel}`);
  console.log('='.repeat(60));

  for (const table of TABLES) {
    const idColumn = table === 'updates' ? 'update_id' : 'event_id';
    const gcsPrefix = `raw/updates/${table}/migration=${MIGRATION}/year=2026/month=${month}/day=${day}/`;

    console.log(`\n  --- ${table} ---`);

    const existingFiles = await listFiles(gcsPrefix);
    if (existingFiles.length === 0) {
      console.log(`  No files found.`);
      continue;
    }

    console.log(`  Found ${existingFiles.length} files in GCS:`);
    for (const name of existingFiles) {
      console.log(`    ${path.basename(name)}`);
    }

    // Download and analyze
    cleanup(TMP_DOWNLOAD);
    ensureDir(TMP_DOWNLOAD);
    await downloadFiles(existingFiles, TMP_DOWNLOAD);

    const totalRows = countRows(TMP_DOWNLOAD);
    const uniqueRows = countUnique(TMP_DOWNLOAD, idColumn);
    const dups = totalRows - uniqueRows;
    console.log(`  total=${totalRows}  unique=${uniqueRows}  dups=${dups}${dups > 0 ? ' <<<' : ''}`);

    // Per-file row count breakdown
    console.log(`  Per-file row counts:`);
    for (const name of existingFiles) {
      const local = path.join(TMP_DOWNLOAD, path.basename(name));
      try {
        const sql = `SELECT count(*) FROM read_parquet('${local}')`;
        const result = execSync(`duckdb -csv -c "${sql}"`, { encoding: 'utf8' }).trim();
        const count = parseInt(result.split('\n')[1]);
        console.log(`    ${path.basename(name).padEnd(50)} ${count}`);
      } catch {
        console.log(`    ${path.basename(name).padEnd(50)} ERROR`);
      }
    }

    cleanup(TMP_DOWNLOAD);
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

    if (dupsBefore === 0 && existingFiles.length <= FILE_THRESHOLD && totalBefore <= MAX_ROWS_PER_FILE) {
      console.log(`  No dups, file count OK (${existingFiles.length} <= ${FILE_THRESHOLD}), rows OK (${totalBefore} <= ${MAX_ROWS_PER_FILE}), skipping.`);
      cleanup(TMP_DOWNLOAD);
      continue;
    }

    // Compact
    console.log(`  Compacting (dedup + consolidate)...`);
    cleanup(TMP_OUTPUT);
    let outputFiles;
    try {
      outputFiles = compactWithDuckDB(TMP_DOWNLOAD, TMP_OUTPUT, idColumn, MAX_ROWS_PER_FILE, dupsBefore);
    } catch (err) {
      console.error(`  ERROR: compactWithDuckDB failed: ${err.message}`);
      cleanup(TMP_DOWNLOAD);
      cleanup(TMP_OUTPUT);
      continue;
    }
    const totalAfter = countRows(TMP_OUTPUT);
    const uniqueAfter = countUnique(TMP_OUTPUT, idColumn);
    const dupsAfter = totalAfter - uniqueAfter;
    console.log(`  After:  total=${totalAfter}  unique=${uniqueAfter}  dups=${dupsAfter}  files=${outputFiles.length}  (${existingFiles.length} → ${outputFiles.length} files)`);

    if (totalAfter !== uniqueBefore) {
      console.error(`  ERROR: Row count mismatch! Expected ${uniqueBefore}, got ${totalAfter}. Aborting this table.`);
      cleanup(TMP_DOWNLOAD);
      cleanup(TMP_OUTPUT);
      continue;
    }

    // Post-compaction dup verification. Even if total row counts match,
    // paranoia: make sure the output is actually deduplicated before we
    // replace the existing GCS files.
    if (dupsAfter !== 0) {
      console.error(`  ERROR: Output still has ${dupsAfter} duplicate row(s) after compaction. Aborting this table.`);
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
  console.log(`GCS Parquet Compaction${DRY_RUN ? ' (DRY RUN)' : ''}${INSPECT ? ' (INSPECT)' : ''}`);
  console.log(`Bucket: ${GCS_BUCKET}  Migration: ${MIGRATION}  Max rows/file: ${MAX_ROWS_PER_FILE}  Run: ${RUN_ID}`);

  // --inspect mode: list files + per-file row counts, no compaction
  if (INSPECT) {
    if (specificDays.length === 0 && !(specificMonth && specificDay)) {
      console.error('--inspect requires --days=... or --month= --day=');
      process.exit(1);
    }
    if (specificMonth && specificDay) {
      await inspectDay(parseInt(specificMonth), parseInt(specificDay));
    }
    for (const d of specificDays) {
      await inspectDay(d.month, d.day);
    }
    console.log('\nDone.');
    return;
  }

  if (specificMonth && specificDay) {
    await compactDay(parseInt(specificMonth), parseInt(specificDay));
    console.log('\nDone.');
    return;
  }

  if (specificDays.length > 0) {
    console.log(`\nDays to compact (from --days flag):`);
    for (const d of specificDays) {
      console.log(`  month=${d.month}/day=${d.day}`);
    }
    for (const d of specificDays) {
      await compactDay(d.month, d.day);
    }
    console.log('\nAll done.');
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
  if (!COMPACT_ALL && !SKIP_DUP_SCAN) {
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
