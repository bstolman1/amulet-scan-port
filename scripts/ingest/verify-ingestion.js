#!/usr/bin/env node
/**
 * verify-ingestion.js
 *
 * Single comprehensive verification script that replaces:
 *   - validate-backfill.js    (read wrong file format — .pb.zst instead of Parquet)
 *   - validate-acs.js         (read local JSONL — you're in GCS mode)
 *   - verify-gcs-parquet.js   (broken BigQuery SQL, half-finished approaches)
 *   - audit-cursor-vs-data.js (wrong cursor filenames)
 *
 * What this does:
 *   1. CURSOR AUDIT     — reads all cursor files, reports state per migration
 *   2. GCS FILE COUNT   — counts actual Parquet files in GCS per migration
 *   3. ROW COUNT        — samples files with DuckDB to estimate total rows
 *   4. RECONCILIATION   — compares cursor claims vs actual GCS row counts
 *   5. DATE GAP CHECK   — detects missing calendar days per migration
 *   6. SCHEMA CHECK     — validates column names in sampled files
 *   7. ACS CHECK        — verifies ACS snapshot files if present
 *   8. BIGQUERY LOAD    — optionally creates BQ external tables for full counts
 *
 * Usage:
 *   node verify-ingestion.js                        # Full verification
 *   node verify-ingestion.js --migration 3          # Single migration
 *   node verify-ingestion.js --quick                # Skip DuckDB row sampling
 *   node verify-ingestion.js --bq                   # Also create BQ external tables
 *   node verify-ingestion.js --gaps                 # Only check for date gaps
 *   node verify-ingestion.js --verbose              # Show per-file details
 *
 * Requirements:
 *   - GCS_BUCKET env var set
 *   - gsutil authenticated (service account key)
 *   - duckdb CLI installed (for row counts)
 *   - bq CLI installed (optional, only with --bq flag)
 */

import dotenv from 'dotenv';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { execFile as execFileCb } from 'child_process';
import { promisify } from 'util';
import { existsSync, readdirSync, readFileSync, mkdirSync, unlinkSync, writeFileSync } from 'fs';
import { randomBytes } from 'crypto';
import { tmpdir } from 'os';

const execFileAsync = promisify(execFileCb);

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
dotenv.config({ path: join(__dirname, '.env') });

import { getCursorDir } from './path-utils.js';

// ─────────────────────────────────────────────────────────────
// Config
// ─────────────────────────────────────────────────────────────

const BUCKET        = process.env.GCS_BUCKET;
const GCP_PROJECT   = process.env.GCP_PROJECT || process.env.quota_project_id || null;
const CURSOR_DIR    = getCursorDir();
const TMP_DIR       = '/tmp/verify-ingestion';

const args          = process.argv.slice(2);
const TARGET_MIG    = args.includes('--migration') ? parseInt(args[args.indexOf('--migration') + 1]) : null;
const QUICK         = args.includes('--quick');
const WITH_BQ       = args.includes('--bq');
const GAPS_ONLY     = args.includes('--gaps');
const VERBOSE       = args.includes('--verbose') || args.includes('-v');

// How many files to DuckDB-sample per migration per type
const SAMPLE_FILES_PER_MIG = QUICK ? 0 : 5;

// Expected schema columns (from data-schema.js / acs-schema.js)
const EXPECTED_UPDATES_COLS = [
  'update_id','migration_id','record_time','update_type','synchronizer_id',
  'effective_at','update_data','offset','domain_id',
];
const EXPECTED_EVENTS_COLS = [
  'event_id','update_id','migration_id','contract_id','template_id',
  'event_type','effective_at','synchronizer_id','package_name','raw_event',
];
const EXPECTED_ACS_COLS = [
  'contract_id','template_id','migration_id','record_time',
  'signatories','payload','raw',
];

// ─────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────

function banner(text) {
  const line = '═'.repeat(70);
  console.log(`\n${line}\n  ${text}\n${line}`);
}

function section(text) {
  console.log(`\n${'─'.repeat(60)}\n  ${text}\n${'─'.repeat(60)}`);
}

function fmt(n) {
  if (n == null) return 'N/A';
  return Number(n).toLocaleString();
}

function fmtDate(ts) {
  if (!ts) return 'N/A';
  return new Date(ts).toISOString().replace('T', ' ').substring(0, 19) + ' UTC';
}

function elapsed(ms) {
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60000) return `${(ms/1000).toFixed(1)}s`;
  const m = Math.floor(ms / 60000);
  return `${m}m ${Math.floor((ms % 60000)/1000)}s`;
}

async function gsutil(...args) {
  try {
    const { stdout } = await execFileAsync('gsutil', args, { timeout: 60000 });
    return stdout.trim();
  } catch (err) {
    const stderr = err.stderr?.toString() || '';
    // "No URLs matched" or "CommandException" = genuinely empty, not an error
    if (stderr.includes('CommandException') ||
        stderr.includes('No URLs matched') ||
        stderr.includes('matched no objects')) {
      return '';
    }
    throw err;
  }
}

// SECURITY FIX: replaced execSync (spawns shell, injection risk) with execFileAsync
// via a temp SQL file. Passing SQL inline with -c required shell interpolation;
// writing to a temp file and passing the path as a positional arg avoids the shell entirely.
async function duckdb(sql) {
  const tmpFile = join(tmpdir(), `duckdb_${randomBytes(6).toString('hex')}.sql`);
  try {
    writeFileSync(tmpFile, sql, 'utf8');
    const { stdout } = await execFileAsync('duckdb', ['-noheader', '-list', tmpFile], {
      encoding: 'utf8',
      timeout: 30000,
    });
    return stdout.trim();
  } catch (err) {
    throw new Error(`DuckDB: ${err.stderr?.toString().trim() || err.message}`);
  } finally {
    try { unlinkSync(tmpFile); } catch {}
  }
}

function ensureTmp() {
  if (!existsSync(TMP_DIR)) mkdirSync(TMP_DIR, { recursive: true });
}

function cleanTmp() {
  if (!existsSync(TMP_DIR)) return;
  for (const f of readdirSync(TMP_DIR)) {
    try { unlinkSync(join(TMP_DIR, f)); } catch {}
  }
}

// ─────────────────────────────────────────────────────────────
// 1. CURSOR AUDIT
// ─────────────────────────────────────────────────────────────

function readAllCursors() {
  if (!existsSync(CURSOR_DIR)) {
    console.warn(`  ⚠️  Cursor directory not found: ${CURSOR_DIR}`);
    return [];
  }

  const files = readdirSync(CURSOR_DIR).filter(f => f.endsWith('.json') && !f.endsWith('.bak'));
  const cursors = [];

  for (const file of files) {
    try {
      const raw = readFileSync(join(CURSOR_DIR, file), 'utf8');
      const data = JSON.parse(raw);
      // Skip .bak files that sneak in
      if (data.migration_id == null) continue;
      cursors.push({ file, ...data });
    } catch (err) {
      console.warn(`  ⚠️  Failed to read cursor ${file}: ${err.message}`);
    }
  }

  return cursors.sort((a, b) => (a.migration_id ?? 0) - (b.migration_id ?? 0));
}

function printCursorSummary(cursors) {
  section('1. CURSOR STATE');

  if (cursors.length === 0) {
    console.log('  ❌ No cursors found');
    return;
  }

  let grandUpdates = 0, grandEvents = 0;

  for (const c of cursors) {
    const mig    = c.migration_id;
    const status = c.complete ? '✅ complete' : '🔄 in progress';
    const gcsOk  = c.gcs_confirmed_updates > 0 ? '☁️ GCS confirmed' : '⚠️  GCS unconfirmed';

    console.log(`\n  Migration ${mig} — ${status} | ${gcsOk}`);
    console.log(`    Updates:      ${fmt(c.total_updates)}  (GCS confirmed: ${fmt(c.gcs_confirmed_updates)})`);
    console.log(`    Events:       ${fmt(c.total_events)}  (GCS confirmed: ${fmt(c.gcs_confirmed_events)})`);
    console.log(`    Range:        ${fmtDate(c.min_time)} → ${fmtDate(c.max_time)}`);
    console.log(`    Last before:  ${fmtDate(c.last_before)}`);
    console.log(`    Last GCS:     ${fmtDate(c.last_gcs_confirmed)}`);
    console.log(`    Updated:      ${fmtDate(c.updated_at)}`);

    if (c.error) {
      console.log(`    ❌ Last error: ${c.error}`);
    }

    grandUpdates += c.total_updates || 0;
    grandEvents  += c.total_events  || 0;
  }

  console.log(`\n  TOTAL across all migrations:`);
  console.log(`    Updates: ${fmt(grandUpdates)}`);
  console.log(`    Events:  ${fmt(grandEvents)}`);
}

// ─────────────────────────────────────────────────────────────
// 2. GCS FILE COUNT
// ─────────────────────────────────────────────────────────────

async function countGCSFiles(migration, type, source = 'backfill') {
  // e.g. gs://bucket/raw/backfill/updates/migration=3/
  const prefix = `gs://${BUCKET}/raw/${source}/${type}/migration=${migration}/`;
  const out = await gsutil('ls', '-r', prefix);
  if (!out) return { files: 0, bytes: 0 };

  const lines = out.split('\n').filter(l => l.endsWith('.parquet'));
  return { files: lines.length, bytes: 0 };
}

async function getGCSMigrations(source = 'backfill') {
  // Discover which migration= folders exist
  const migrations = new Set();
  for (const type of ['updates', 'events']) {
    const prefix = `gs://${BUCKET}/raw/${source}/${type}/`;
    const out = await gsutil('ls', prefix);
    if (!out) continue;
    for (const line of out.split('\n')) {
      const m = line.match(/migration=(\d+)/);
      if (m) migrations.add(parseInt(m[1]));
    }
  }
  return [...migrations].sort((a, b) => a - b);
}

async function printGCSFileCounts(migrations) {
  section('2. GCS FILE COUNTS');

  for (const mig of migrations) {
    const [u, e] = await Promise.all([
      countGCSFiles(mig, 'updates'),
      countGCSFiles(mig, 'events'),
    ]);
    const status = u.files > 0 ? '✅' : '⚠️ ';
    console.log(`  ${status} Migration ${mig}: ${fmt(u.files)} update files | ${fmt(e.files)} event files`);
  }

  // ACS
  const acsOut = await gsutil('ls', '-r', `gs://${BUCKET}/raw/acs/`).catch(() => '');
  const acsFiles = acsOut ? acsOut.split('\n').filter(l => l.endsWith('.parquet')).length : 0;
  console.log(`  ${ acsFiles > 0 ? '✅' : '⚠️ '} ACS: ${fmt(acsFiles)} snapshot files`);
}

// ─────────────────────────────────────────────────────────────
// 3. ROW COUNT SAMPLING (DuckDB)
// ─────────────────────────────────────────────────────────────

async function sampleParquetFiles(migration, type, source = 'backfill', n = 5) {
  if (n === 0) return null;

  ensureTmp();

  const prefix = `gs://${BUCKET}/raw/${source}/${type}/migration=${migration}/`;
  const out = await gsutil('ls', '-r', prefix);
  if (!out) return null;

  const allFiles = out.split('\n').filter(l => l.endsWith('.parquet'));
  if (allFiles.length === 0) return null;

  // Pick evenly spaced sample
  const step  = Math.max(1, Math.floor(allFiles.length / n));
  const picks = [];
  for (let i = 0; i < allFiles.length && picks.length < n; i += step) {
    picks.push(allFiles[i].trim());
  }

  let totalRows  = 0;
  let minTime    = null;
  let maxTime    = null;
  let schemaCols = null;
  let badFiles   = 0;

  for (const gcsPath of picks) {
    const localName = `sample-m${migration}-${type}-${picks.indexOf(gcsPath)}.parquet`;
    const localPath = join(TMP_DIR, localName);

    try {
      await gsutil('cp', gcsPath, localPath);

      const rowStr  = await duckdb(`SELECT COUNT(*) FROM '${localPath}'`);
      const rows    = parseInt(rowStr) || 0;
      totalRows    += rows;

      // Get time range
      const timeCol = type === 'updates' ? 'record_time' : 'effective_at';
      try {
        const rangeStr = await duckdb(
          `SELECT MIN(${timeCol}), MAX(${timeCol}) FROM '${localPath}'`
        );
        const [mn, mx] = rangeStr.split('|');
        if (mn && (!minTime || mn < minTime)) minTime = mn.trim();
        if (mx && (!maxTime || mx > maxTime)) maxTime = mx.trim();
      } catch {}

      // Get schema from first file only
      if (!schemaCols) {
        try {
          const schemaOut = await duckdb(`SELECT column_name FROM (DESCRIBE SELECT * FROM '${localPath}')`);
          schemaCols = schemaOut.split('\n').map(s => s.trim()).filter(Boolean);
        } catch {}
      }

      if (VERBOSE) {
        console.log(`    📄 ${gcsPath.split('/').pop()}: ${fmt(rows)} rows`);
      }
    } catch (err) {
      badFiles++;
      if (VERBOSE) console.warn(`    ⚠️  Failed to sample ${gcsPath.split('/').pop()}: ${err.message}`);
    } finally {
      try { unlinkSync(localPath); } catch {}
    }
  }

  const avgRows = picks.length > badFiles
    ? Math.round(totalRows / (picks.length - badFiles))
    : 0;

  return {
    sampled: picks.length,
    badFiles,
    totalSampledRows: totalRows,
    avgRowsPerFile: avgRows,
    estimatedTotalRows: avgRows * allFiles.length,
    totalFiles: allFiles.length,
    minTime,
    maxTime,
    schemaCols,
  };
}

async function printRowCountEstimates(migrations, cursors) {
  section('3. ROW COUNT ESTIMATES (DuckDB sampling)');

  if (QUICK) {
    console.log('  ⏭️  Skipped (--quick mode). Remove --quick for row count sampling.');
    return;
  }

  for (const mig of migrations) {
    console.log(`\n  Migration ${mig}:`);
    const cursor = cursors.find(c => c.migration_id === mig);

    for (const type of ['updates', 'events']) {
      process.stdout.write(`    Sampling ${type}... `);
      const t0     = Date.now();
      const result = await sampleParquetFiles(mig, type, 'backfill', SAMPLE_FILES_PER_MIG);

      if (!result) {
        console.log('no files found');
        continue;
      }

      const estimated = result.estimatedTotalRows;
      const claimed   = type === 'updates'
        ? (cursor?.total_updates || 0)
        : (cursor?.total_events  || 0);

      const diff    = claimed - estimated;
      const diffPct = claimed > 0 ? ((diff / claimed) * 100).toFixed(1) : '?';
      const match   = Math.abs(diff) / Math.max(claimed, 1) < 0.10; // within 10%

      console.log(`done (${elapsed(Date.now() - t0)})`);
      console.log(`      Files:     ${fmt(result.totalFiles)} total, ${result.sampled} sampled`);
      console.log(`      Avg rows/file: ${fmt(result.avgRowsPerFile)}`);
      console.log(`      Estimated rows: ~${fmt(estimated)}`);
      console.log(`      Cursor claims:   ${fmt(claimed)}`);
      console.log(`      Difference:  ${diff >= 0 ? '+' : ''}${fmt(diff)} (${diffPct}%)  ${match ? '✅ within 10%' : '⚠️  check gap detection'}`);

      if (result.minTime) {
        console.log(`      Time range:  ${fmtDate(result.minTime)} → ${fmtDate(result.maxTime)}`);
      }

      // Schema check
      if (result.schemaCols) {
        const expected = type === 'updates' ? EXPECTED_UPDATES_COLS : EXPECTED_EVENTS_COLS;
        const missing  = expected.filter(c => !result.schemaCols.includes(c));
        const extra    = result.schemaCols.filter(c => !expected.includes(c));

        if (missing.length === 0) {
          console.log(`      Schema: ✅ all ${expected.length} expected columns present`);
        } else {
          console.log(`      Schema: ❌ missing columns: ${missing.join(', ')}`);
        }
        if (VERBOSE && extra.length > 0) {
          console.log(`      Schema: ➕ extra columns: ${extra.join(', ')}`);
        }
      }
    }
  }
}

// ─────────────────────────────────────────────────────────────
// 4. DATE GAP DETECTION
// ─────────────────────────────────────────────────────────────

async function getDayPartitions(migration, type, source = 'backfill') {
  const prefix = `gs://${BUCKET}/raw/${source}/${type}/migration=${migration}/`;
  const out    = await gsutil('ls', '-r', prefix);
  if (!out) return new Set();

  const days = new Set();
  for (const line of out.split('\n')) {
    const m = line.match(/year=(\d+)\/month=(\d+)\/day=(\d+)/);
    if (m) {
      // Pad to ISO date for comparison
      const y  = m[1];
      const mo = m[2].padStart(2, '0');
      const d  = m[3].padStart(2, '0');
      days.add(`${y}-${mo}-${d}`);
    }
  }
  return days;
}

function findMissingDays(days, minTime, maxTime) {
  if (!minTime || !maxTime || days.size === 0) return [];

  const missing = [];
  const start   = new Date(minTime);
  const end     = new Date(maxTime);
  start.setUTCHours(0, 0, 0, 0);
  end.setUTCHours(0, 0, 0, 0);

  const cur = new Date(start);
  while (cur <= end) {
    const key = cur.toISOString().substring(0, 10);
    if (!days.has(key)) missing.push(key);
    cur.setUTCDate(cur.getUTCDate() + 1);
  }
  return missing;
}

async function printGapDetection(migrations, cursors) {
  section('4. DATE GAP DETECTION');

  for (const mig of migrations) {
    const cursor  = cursors.find(c => c.migration_id === mig);
    const minTime = cursor?.min_time;
    const maxTime = cursor?.max_time || cursor?.last_before;

    console.log(`\n  Migration ${mig}:`);

    if (!minTime || !maxTime) {
      console.log('    ⚠️  No time range in cursor — cannot check gaps');
      continue;
    }

    for (const type of ['updates', 'events']) {
      process.stdout.write(`    Scanning ${type} day partitions... `);
      const t0   = Date.now();
      const days = await getDayPartitions(mig, type);
      console.log(`${days.size} days (${elapsed(Date.now() - t0)})`);

      if (days.size === 0) {
        console.log('    ⚠️  No day partitions found in GCS');
        continue;
      }

      const missing = findMissingDays(days, minTime, maxTime);

      if (missing.length === 0) {
        console.log(`    ✅ No missing days (${fmtDate(minTime)} → ${fmtDate(maxTime)})`);
      } else {
        // Consolidate into ranges for readability
        const ranges = [];
        let rangeStart = missing[0], rangePrev = missing[0];
        for (let i = 1; i < missing.length; i++) {
          const prev = new Date(rangePrev);
          const curr = new Date(missing[i]);
          const gap  = (curr - prev) / 86400000;
          if (gap === 1) {
            rangePrev = missing[i];
          } else {
            ranges.push(rangeStart === rangePrev ? rangeStart : `${rangeStart} → ${rangePrev}`);
            rangeStart = missing[i];
            rangePrev  = missing[i];
          }
        }
        ranges.push(rangeStart === rangePrev ? rangeStart : `${rangeStart} → ${rangePrev}`);

        console.log(`    ⚠️  ${missing.length} missing day(s):`);
        for (const r of ranges.slice(0, 20)) {
          console.log(`       ❌ ${r}`);
        }
        if (ranges.length > 20) console.log(`       ... and ${ranges.length - 20} more`);
      }
    }

    // Cross-check: updates days vs events days alignment
    const [uDays, eDays] = await Promise.all([
      getDayPartitions(mig, 'updates'),
      getDayPartitions(mig, 'events'),
    ]);
    const uOnly = [...uDays].filter(d => !eDays.has(d)).sort();
    const eOnly = [...eDays].filter(d => !uDays.has(d)).sort();

    if (uOnly.length === 0 && eOnly.length === 0) {
      console.log(`    ✅ Updates and events day partitions are aligned`);
    } else {
      if (uOnly.length > 0) console.log(`    ⚠️  ${uOnly.length} day(s) in updates but not events: ${uOnly.slice(0,5).join(', ')}${uOnly.length > 5 ? '...' : ''}`);
      if (eOnly.length > 0) console.log(`    ⚠️  ${eOnly.length} day(s) in events but not updates: ${eOnly.slice(0,5).join(', ')}${eOnly.length > 5 ? '...' : ''}`);
    }
  }
}

// ─────────────────────────────────────────────────────────────
// 5. ACS VERIFICATION
// ─────────────────────────────────────────────────────────────

async function verifyACS() {
  section('5. ACS SNAPSHOT VERIFICATION');

  const out = await gsutil('ls', '-r', `gs://${BUCKET}/raw/acs/`);
  if (!out) {
    console.log('  📭 No ACS data found in GCS');
    return;
  }

  const allFiles    = out.split('\n').filter(l => l.endsWith('.parquet'));
  const completeMarkers = out.split('\n').filter(l => l.includes('_COMPLETE'));

  console.log(`  Total ACS Parquet files: ${fmt(allFiles.length)}`);
  console.log(`  Completion markers:      ${fmt(completeMarkers.length)}`);

  // Discover migrations in ACS
  const acsMigs = new Set();
  for (const line of out.split('\n')) {
    const m = line.match(/migration=(\d+)/);
    if (m) acsMigs.add(parseInt(m[1]));
  }

  console.log(`  Migrations with ACS:     ${[...acsMigs].sort().join(', ') || 'none'}`);

  if (QUICK || allFiles.length === 0) return;

  // Sample one ACS file for schema check
  ensureTmp();
  const sampleFile = allFiles[Math.floor(allFiles.length / 2)]?.trim();
  if (!sampleFile) return;

  const localPath = join(TMP_DIR, 'acs-sample.parquet');
  try {
    await gsutil('cp', sampleFile, localPath);

    const rowStr   = await duckdb(`SELECT COUNT(*) FROM '${localPath}'`);
    const schemaOut = await duckdb(`SELECT column_name FROM (DESCRIBE SELECT * FROM '${localPath}')`);
    const cols     = schemaOut.split('\n').map(s => s.trim()).filter(Boolean);

    console.log(`\n  Sample file: ${sampleFile.split('/').pop()}`);
    console.log(`  Rows: ${fmt(parseInt(rowStr))}`);

    const missing = EXPECTED_ACS_COLS.filter(c => !cols.includes(c));
    if (missing.length === 0) {
      console.log(`  Schema: ✅ all ${EXPECTED_ACS_COLS.length} expected columns present`);
    } else {
      console.log(`  Schema: ❌ missing: ${missing.join(', ')}`);
    }

    if (VERBOSE) {
      console.log(`  All columns: ${cols.join(', ')}`);
    }
  } catch (err) {
    console.warn(`  ⚠️  Failed to sample ACS file: ${err.message}`);
  } finally {
    try { unlinkSync(localPath); } catch {}
  }
}

// ─────────────────────────────────────────────────────────────
// 6. BIGQUERY EXTERNAL TABLES (optional)
// ─────────────────────────────────────────────────────────────

// SECURITY FIX: replaced execSync (shell=true) with execFileAsync.
// SQL is passed as a discrete CLI argument — no shell spawning or interpolation.
async function bqQuery(sql) {
  const args = ['query', '--use_legacy_sql=false', '--format=prettyjson'];
  if (GCP_PROJECT) args.push(`--project_id=${GCP_PROJECT}`);
  args.push(sql);
  try {
    const { stdout } = await execFileAsync('bq', args, {
      encoding: 'utf8',
      timeout: 120000,
    });
    return stdout.trim();
  } catch (err) {
    throw new Error(err.stderr?.toString().trim() || err.message);
  }
}

async function createBQExternalTable(dataset, tableName, gcsPattern, description) {
  const projectFlag = GCP_PROJECT ? `--project_id=${GCP_PROJECT}` : '';
  const fullTable   = GCP_PROJECT ? `${GCP_PROJECT}:${dataset}.${tableName}` : `${dataset}.${tableName}`;

  try {
    // SECURITY FIX: execFileAsync replaces execSync — table definition passed as discrete args
  const tableDef = JSON.stringify({
    sourceFormat: 'PARQUET',
    sourceUris: [gcsPattern],
    autodetect: true,
  });
  const mkArgs = [`--external_table_definition=${tableDef}`];
  if (GCP_PROJECT) mkArgs.push(`--project_id=${GCP_PROJECT}`);
  mkArgs.push(fullTable);
  await execFileAsync('bq', ['mk', ...mkArgs], {
    encoding: 'utf8',
  });
    console.log(`  ✅ Created external table: ${fullTable}`);
    return fullTable.replace(':', '.');
  } catch (err) {
    const msg = err.stderr?.toString() || '';
    if (msg.includes('already exists')) {
      console.log(`  ℹ️  Table already exists: ${fullTable}`);
      return fullTable.replace(':', '.');
    }
    throw new Error(msg.trim() || err.message);
  }
}

async function printBQCounts(migrations) {
  section('6. BIGQUERY EXACT ROW COUNTS');

  const dataset = 'canton_verify';

  // Create dataset if needed
  try {
    const projectFlag = GCP_PROJECT ? `--project_id=${GCP_PROJECT}` : '';
    // SECURITY FIX: execFileAsync replaces execSync
    const dsArgs = GCP_PROJECT ? [`--project_id=${GCP_PROJECT}`, '--dataset', dataset] : ['--dataset', dataset];
    await execFileAsync('bq', ['mk', ...dsArgs], {
      encoding: 'utf8',
    });
    console.log(`  ✅ Created dataset: ${dataset}`);
  } catch (err) {
    const msg = err.stderr?.toString() || '';
    if (!msg.includes('already exists')) {
      console.warn(`  ⚠️  Could not create dataset: ${msg.trim()}`);
    }
  }

  for (const mig of migrations) {
    console.log(`\n  Migration ${mig}:`);

    for (const type of ['updates', 'events']) {
      const tableName = `backfill_${type}_m${mig}`;
      const gcsPattern = `gs://${BUCKET}/raw/backfill/${type}/migration=${mig}/**`;

      try {
        const fullTable = await createBQExternalTable(dataset, tableName, gcsPattern, `${type} migration ${mig}`);

        // Query exact count and time range
        const timeCol = type === 'updates' ? 'record_time' : 'effective_at';
        const result  = await bqQuery(
          `SELECT COUNT(*) as total, MIN(${timeCol}) as earliest, MAX(${timeCol}) as latest FROM \`${fullTable}\``
        );

        const parsed = JSON.parse(result);
        if (parsed && parsed[0]) {
          const row = parsed[0];
          console.log(`    ${type}: ${fmt(parseInt(row.total?.v || row.total))} rows`);
          console.log(`      Range: ${fmtDate(row.earliest?.v || row.earliest)} → ${fmtDate(row.latest?.v || row.latest)}`);
        }
      } catch (err) {
        console.warn(`    ⚠️  BQ ${type}: ${err.message}`);
      }
    }
  }
}

// ─────────────────────────────────────────────────────────────
// 7. RECONCILIATION SUMMARY
// ─────────────────────────────────────────────────────────────

function printReconciliation(cursors, migrations) {
  section('7. RECONCILIATION SUMMARY');

  let allGood = true;

  for (const mig of migrations) {
    const cursor = cursors.find(c => c.migration_id === mig);
    if (!cursor) {
      console.log(`\n  Migration ${mig}: ⚠️  no cursor found`);
      allGood = false;
      continue;
    }

    console.log(`\n  Migration ${mig}:`);

    // GCS confirmation gap
    const updateGap = (cursor.total_updates || 0) - (cursor.gcs_confirmed_updates || 0);
    const eventGap  = (cursor.total_events  || 0) - (cursor.gcs_confirmed_events  || 0);

    if (updateGap === 0 && eventGap === 0) {
      console.log(`    ✅ All fetched records confirmed in GCS`);
    } else {
      if (updateGap > 0) {
        console.log(`    ⚠️  ${fmt(updateGap)} updates fetched but not yet GCS-confirmed`);
        allGood = false;
      }
      if (eventGap > 0) {
        console.log(`    ⚠️  ${fmt(eventGap)} events fetched but not yet GCS-confirmed`);
        allGood = false;
      }
      console.log(`    (Normal if backfill is in progress — check again when complete)`);
    }

    // Completeness
    if (cursor.complete) {
      console.log(`    ✅ Migration marked complete`);
    } else {
      const pct = cursor.max_time && cursor.min_time && cursor.last_before
        ? (() => {
            const total = new Date(cursor.max_time) - new Date(cursor.min_time);
            const done  = new Date(cursor.max_time) - new Date(cursor.last_before);
            return total > 0 ? ((done / total) * 100).toFixed(1) : '?';
          })()
        : '?';
      console.log(`    🔄 In progress (~${pct}% of time range covered)`);
    }
  }

  if (allGood && migrations.length > 0) {
    console.log(`\n  ✅ All migrations reconciled cleanly`);
  }
}

// ─────────────────────────────────────────────────────────────
// MAIN
// ─────────────────────────────────────────────────────────────

async function main() {
  const t0 = Date.now();

  banner('CANTON LEDGER INGESTION VERIFICATION');
  console.log(`  Bucket:     gs://${BUCKET}`);
  console.log(`  Cursor dir: ${CURSOR_DIR}`);
  console.log(`  Mode:       ${QUICK ? 'quick (no DuckDB sampling)' : 'full'}`);
  if (TARGET_MIG != null) console.log(`  Migration:  ${TARGET_MIG} only`);
  if (WITH_BQ)  console.log(`  BigQuery:   enabled`);

  if (!BUCKET) {
    console.error('\n❌ GCS_BUCKET not set in .env');
    process.exit(1);
  }

  // 1. Read cursors
  const allCursors = readAllCursors();
  const cursors    = TARGET_MIG != null
    ? allCursors.filter(c => c.migration_id === TARGET_MIG)
    : allCursors;

  printCursorSummary(cursors);

  if (GAPS_ONLY) {
    // Discover migrations from GCS directly
    const gcsMigs = TARGET_MIG != null ? [TARGET_MIG] : await getGCSMigrations();
    await printGapDetection(gcsMigs, cursors);
    console.log(`\n  ⏱️  Completed in ${elapsed(Date.now() - t0)}\n`);
    return;
  }

  // 2. Discover GCS migrations
  const gcsMigs = TARGET_MIG != null
    ? [TARGET_MIG]
    : await getGCSMigrations();

  if (gcsMigs.length === 0) {
    console.log('\n  ⚠️  No migrations found in GCS');
  }

  // 3–7 in order
  await printGCSFileCounts(gcsMigs);
  await printRowCountEstimates(gcsMigs, cursors);
  await printGapDetection(gcsMigs, cursors);
  await verifyACS();

  if (WITH_BQ) {
    await printBQCounts(gcsMigs);
  } else {
    section('6. BIGQUERY EXACT ROW COUNTS');
    console.log('  ⏭️  Skipped. Run with --bq for exact BigQuery row counts.');
    console.log('  Tip: Use after backfill completes for definitive reconciliation.');
  }

  printReconciliation(cursors, gcsMigs);

  // Final verdict
  banner('VERIFICATION COMPLETE');
  console.log(`  Time elapsed: ${elapsed(Date.now() - t0)}`);

  const incomplete = cursors.filter(c => !c.complete);
  const unconfirmed = cursors.filter(c =>
    (c.total_updates || 0) > (c.gcs_confirmed_updates || 0) ||
    (c.total_events  || 0) > (c.gcs_confirmed_events  || 0)
  );

  if (incomplete.length > 0) {
    console.log(`\n  🔄 ${incomplete.length} migration(s) still in progress: ${incomplete.map(c => `M${c.migration_id}`).join(', ')}`);
  }
  if (unconfirmed.length > 0) {
    console.log(`  ⚠️  ${unconfirmed.length} migration(s) have unconfirmed GCS writes`);
    console.log(`     (Expected — GCS confirmation catches up at each checkpoint)`);
  }
  if (incomplete.length === 0 && unconfirmed.length === 0) {
    console.log(`\n  ✅ All migrations complete and GCS-confirmed`);
  }

  console.log();
  cleanTmp();
}

main().catch(err => {
  console.error('\n❌ Fatal error:', err.message);
  if (VERBOSE) console.error(err.stack);
  cleanTmp();
  process.exit(1);
});
