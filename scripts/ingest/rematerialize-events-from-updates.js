#!/usr/bin/env node
/**
 * rematerialize-events-from-updates.js
 *
 * Recovery tool for partitions where updates exist but events are missing
 * (e.g. the 2026-04-02 M4 gap caused by a partial reingest that advanced
 * the cursor past a day without writing its events).
 *
 * For a given --migration=N --date=YYYY-MM-DD, this reads every existing
 * updates parquet file for that day, extracts events from the canonical
 * `update_data` column using the SAME normalization path as fetch-updates.js
 * and reingest-updates.js (normalizeUpdate → flattenEventsInTreeOrder →
 * normalizeEvent → groupByPartition), and writes new events parquet files
 * to the correct partition(s) in GCS.
 *
 * Guarantees
 *   - Byte-equivalent normalization to the live writer (shared data-schema.js)
 *   - Idempotent: same inputs → same output filenames → re-running overwrites
 *   - Safe-by-default: refuses to write if any events already exist for the
 *     target date (use --force to override)
 *   - Dry-run by default
 *   - Post-upload verification: compares row count to SUM(event_count) of
 *     the source updates, fails hard on mismatch
 *   - Non-colliding filename prefix `events-remat-` (distinct from `-live-`
 *     and `-ri-` so future reingests don't confuse provenance)
 *
 * Usage
 *   node rematerialize-events-from-updates.js --migration=4 --date=2026-04-02 --dry-run
 *   node rematerialize-events-from-updates.js --migration=4 --date=2026-04-02 --source=updates
 *   node rematerialize-events-from-updates.js --migration=4 --date=2026-04-02 --execute
 *
 * Requirements
 *   - GCS_BUCKET env var
 *   - GCS_HMAC_KEY_ID / GCS_HMAC_SECRET env vars (DuckDB auth)
 *   - gsutil + duckdb CLIs in PATH
 */

import { execSync, execFile as execFileCb, spawn } from 'child_process';
import { promisify } from 'util';
import { createInterface } from 'readline';
import { createReadStream, createWriteStream, existsSync, mkdirSync, unlinkSync, readdirSync, writeFileSync, statfsSync } from 'fs';
import { createHash } from 'crypto';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';

import {
  normalizeUpdate,
  normalizeEvent,
  flattenEventsInTreeOrder,
  groupByPartition,
  EVENTS_COLUMNS,
} from './data-schema.js';

const execFile = promisify(execFileCb);
const __filename = fileURLToPath(import.meta.url);
const __dirname  = dirname(__filename);
dotenv.config({ path: join(__dirname, '.env') });

// ─────────────────────────────────────────────────────────────
// Config
// ─────────────────────────────────────────────────────────────

const BUCKET           = process.env.GCS_BUCKET || 'canton-bucket';
const HMAC_KEY_ID      = process.env.GCS_HMAC_KEY_ID;
const HMAC_SECRET      = process.env.GCS_HMAC_SECRET;

// DuckDB tuning — safe on 31 GiB VM
const DUCKDB_MEMORY    = process.env.REMAT_DUCKDB_MEMORY || '6GB';
const DUCKDB_THREADS   = parseInt(process.env.REMAT_DUCKDB_THREADS || '2', 10);
const TMP_BASE         = process.env.REMAT_TMP_DIR || '/tmp/remat-events';
// Per-(migration, date) subdir so concurrent runs for different days don't
// collide and so a safe cleanup at startup only touches this run's own files.
// Filled in below once CLI args are parsed.
let TMP_ROOT;
let DUCKDB_SPILL;

// Output chunking. 100k events per file ≈ ~5-10 MB compressed; matches live cadence.
const MAX_EVENTS_PER_FILE = parseInt(process.env.REMAT_MAX_EVENTS_PER_FILE || '100000', 10);

// How many source parquet files to stream concurrently.
const SOURCE_CONCURRENCY = parseInt(process.env.REMAT_SOURCE_CONCURRENCY || '1', 10);

// Minimum free disk space to allow operation.
const MIN_FREE_TMP_GB = 15;

// ─────────────────────────────────────────────────────────────
// CLI
// ─────────────────────────────────────────────────────────────

function argVal(name, def = null) {
  const args = process.argv.slice(2);
  const i = args.findIndex(a => a === `--${name}` || a.startsWith(`--${name}=`));
  if (i === -1) return def;
  const a = args[i];
  if (a.includes('=')) return a.slice(a.indexOf('=') + 1);
  return (i + 1 < args.length && !args[i + 1].startsWith('--')) ? args[i + 1] : def;
}
function hasFlag(name) {
  return process.argv.slice(2).some(a => a === `--${name}`);
}

const OPTS = {
  migration:  argVal('migration') !== null ? parseInt(argVal('migration'), 10) : null,
  date:       argVal('date'),                   // YYYY-MM-DD
  source:     argVal('source', 'updates'),      // 'updates' (live) or 'backfill'
  execute:    hasFlag('execute'),
  force:      hasFlag('force'),
  verbose:    hasFlag('verbose') || hasFlag('v'),
  help:       hasFlag('help') || hasFlag('h'),
};
const DRY_RUN = !OPTS.execute;  // explicit opt-in to write

function printHelp() {
  console.log(`
rematerialize-events-from-updates.js — rebuild missing events from update_data

Required:
  --migration=N          migration ID (e.g. 4)
  --date=YYYY-MM-DD      target day (UTC)

Optional:
  --source=updates|backfill   which source partition to read updates from (default: updates)
  --execute              actually write; default is dry-run
  --force                proceed even if events already exist at the target
  --verbose, -v
  --help, -h

Examples:
  # inspect what would be written, no side effects
  node rematerialize-events-from-updates.js --migration=4 --date=2026-04-02

  # commit
  source ~/.gcs_hmac_env
  node rematerialize-events-from-updates.js --migration=4 --date=2026-04-02 --execute
`);
}
if (OPTS.help) { printHelp(); process.exit(0); }

if (OPTS.migration === null || !OPTS.date) {
  console.error('ERROR: --migration and --date are required\n');
  printHelp();
  process.exit(2);
}
if (!/^\d{4}-\d{2}-\d{2}$/.test(OPTS.date)) {
  console.error(`ERROR: --date must be YYYY-MM-DD, got "${OPTS.date}"`);
  process.exit(2);
}
if (!['updates', 'backfill'].includes(OPTS.source)) {
  console.error(`ERROR: --source must be 'updates' or 'backfill', got "${OPTS.source}"`);
  process.exit(2);
}

// ─────────────────────────────────────────────────────────────
// Shared state
// ─────────────────────────────────────────────────────────────

const [YEAR, MONTH, DAY] = OPTS.date.split('-').map(Number);
const SOURCE  = OPTS.source;
const TARGET_MIG = OPTS.migration;

// Partition path for the *target day* — used in deterministic filename hash so
// a re-run with the same (date, migration, source) produces stable names.
const TARGET_PARTITION = `${SOURCE}/events/migration=${TARGET_MIG}/year=${YEAR}/month=${MONTH}/day=${DAY}`;

// Finalize TMP_ROOT now that we know the target — isolates each (mig,date,source).
TMP_ROOT     = join(TMP_BASE, `m${TARGET_MIG}-${OPTS.date}-${SOURCE}`);
DUCKDB_SPILL = join(TMP_ROOT, 'duckdb_spill');

const REPORT = {
  started_at:  new Date().toISOString(),
  migration:   TARGET_MIG,
  date:        OPTS.date,
  source:      SOURCE,
  dry_run:     DRY_RUN,
  source_files: [],
  source_updates: 0,
  transactions: 0,
  reassignments: 0,
  expected_events_min: 0,        // SUM(event_count)
  expected_events_max: 0,        // + 2 * reassignments
  generated_events: 0,
  events_by_partition: {},
  output_files: [],
  errors: [],
  finished_at: null,
};

// ─────────────────────────────────────────────────────────────
// Shell helpers
// ─────────────────────────────────────────────────────────────

function sh(cmd, { timeout = 120000, maxBuffer = 256 * 1024 * 1024 } = {}) {
  try {
    return execSync(cmd, { stdio: ['ignore', 'pipe', 'pipe'], timeout, maxBuffer }).toString();
  } catch (err) {
    const stderr = err.stderr?.toString() || '';
    const msg = (stderr + ' ' + (err.message || '')).toLowerCase();
    if (msg.includes('no urls matched') || msg.includes('matched no objects')) return '';
    throw new Error(`Command failed: ${cmd.slice(0, 120)} — ${stderr.trim().slice(0, 300) || err.message}`);
  }
}

function checkPrereqs() {
  try { execSync('gsutil version', { stdio: 'pipe' }); }
  catch { throw new Error('gsutil not found — install Google Cloud SDK'); }
  try { execSync('duckdb --version', { stdio: 'pipe' }); }
  catch { throw new Error('duckdb CLI not found — install duckdb'); }
  if (!HMAC_KEY_ID || !HMAC_SECRET) {
    throw new Error(
      'GCS_HMAC_KEY_ID and GCS_HMAC_SECRET must be set — DuckDB GCS auth requires HMAC. ' +
      'Create with `gcloud storage hmac create <sa-email>` and `source ~/.gcs_hmac_env`.'
    );
  }
  // Probe bucket access early
  sh(`gsutil ls "gs://${BUCKET}/" > /dev/null`);
  // Disk guard
  try {
    const s = statfsSync('/tmp');
    const freeGB = Math.floor((Number(s.bavail) * Number(s.bsize)) / (1024 ** 3));
    if (freeGB < MIN_FREE_TMP_GB) {
      throw new Error(`Only ${freeGB} GB free on /tmp (need ${MIN_FREE_TMP_GB} GB). Free space before running.`);
    }
  } catch (err) {
    if (err.message.startsWith('Only ')) throw err;
    console.warn(`⚠️  Could not check /tmp free space: ${err.message}`);
  }
}

function ensureTmpDirs() {
  // Clear stale staging files from any prior failed run for this SAME
  // (mig, date, source). TMP_ROOT is scoped to those so this cannot touch
  // a concurrent run for a different day. Report files live at the
  // TMP_ROOT top level (not in the cleared subdirs), so they persist.
  for (const sub of ['src', 'jsonl', 'parquet', 'duckdb_spill']) {
    const dir = join(TMP_ROOT, sub);
    if (existsSync(dir)) {
      for (const f of readdirSync(dir)) {
        try { unlinkSync(join(dir, f)); } catch {}
      }
    }
  }
  for (const d of [TMP_BASE, TMP_ROOT, DUCKDB_SPILL, join(TMP_ROOT, 'src'), join(TMP_ROOT, 'jsonl'), join(TMP_ROOT, 'parquet')]) {
    if (!existsSync(d)) mkdirSync(d, { recursive: true });
  }
}

function cleanupTmp() {
  // Remove per-run artifacts but keep TMP_ROOT around so the report file
  // (written at the TMP_BASE level above) persists for later inspection.
  for (const sub of ['src', 'jsonl', 'parquet']) {
    const dir = join(TMP_ROOT, sub);
    if (!existsSync(dir)) continue;
    for (const f of readdirSync(dir)) {
      try { unlinkSync(join(dir, f)); } catch {}
    }
  }
}

// SQL string-literal escape
function sqlStr(s) { return String(s).replace(/'/g, "''"); }

// ─────────────────────────────────────────────────────────────
// GCS discovery + pre-flight
// ─────────────────────────────────────────────────────────────

function sourceUpdatesPrefix() {
  return `gs://${BUCKET}/raw/${SOURCE}/updates/migration=${TARGET_MIG}/year=${YEAR}/month=${MONTH}/day=${DAY}/`;
}
function targetEventsPrefix(year = YEAR, month = MONTH, day = DAY) {
  return `gs://${BUCKET}/raw/${SOURCE}/events/migration=${TARGET_MIG}/year=${year}/month=${month}/day=${day}/`;
}

function listParquet(prefix) {
  const out = sh(`gsutil ls "${prefix}" `, { timeout: 120000 });
  return out.split('\n').map(s => s.trim()).filter(p => p.endsWith('.parquet'));
}

function preflight() {
  // 1) Source must exist and contain parquet files
  const sources = listParquet(sourceUpdatesPrefix()).sort();
  if (sources.length === 0) {
    throw new Error(`No source updates parquet files under ${sourceUpdatesPrefix()}`);
  }
  REPORT.source_files = sources;
  console.log(`  source: ${sources.length} parquet file(s) under ${sourceUpdatesPrefix().replace(`gs://${BUCKET}/`, '')}`);

  // 2) Target should be empty (or --force). Check the target day AND ±1 neighbors because
  //    events for a day's updates can legitimately drift across midnight.
  const neighbors = [-1, 0, 1].map(offset => {
    const dt = new Date(Date.UTC(YEAR, MONTH - 1, DAY + offset));
    return { y: dt.getUTCFullYear(), m: dt.getUTCMonth() + 1, d: dt.getUTCDate(), offset };
  });

  for (const n of neighbors) {
    const prefix = targetEventsPrefix(n.y, n.m, n.d);
    const existing = listParquet(prefix);
    const label = `day=${n.d} (${n.offset === 0 ? 'TARGET' : n.offset > 0 ? '+1' : '-1'})`;
    if (existing.length > 0 && n.offset === 0) {
      if (!OPTS.force) {
        throw new Error(
          `Target events partition is NOT empty — ${existing.length} file(s) already exist under ${prefix}. ` +
          `Refusing to run without --force to avoid creating duplicate events.`
        );
      }
      console.log(`  ⚠️  target ${label}: ${existing.length} file(s) exist — proceeding due to --force`);
    } else if (existing.length > 0) {
      console.log(`  ℹ️  neighbor ${label}: ${existing.length} file(s) exist — any drifted events will collide. Safe: our output uses distinct 'remat-' prefix, but rows will duplicate if an event truly belongs here and was already written.`);
    } else {
      console.log(`  ✅ ${label}: empty`);
    }
  }

  // 3) Concurrent ingest guard — refuse to step on an in-flight fetch-updates.js
  //    or reingest-updates.js. `grep -v pgrep` drops pgrep's self-match (the
  //    shell command itself contains the pattern strings and matches).
  const running = sh(`pgrep -fa 'fetch-updates.js|reingest-updates.js' | grep -v pgrep || true`)
    .split('\n').filter(Boolean);
  if (running.length > 0 && !OPTS.force) {
    console.error('\nERROR: ingestion process(es) running — would share /tmp, RAM, and GCS egress:');
    running.slice(0, 5).forEach(l => console.error(`  ${l}`));
    console.error(`\nThese processes write to DIFFERENT partitions than the target (day=${DAY}),`);
    console.error(`and event-remat filenames never collide with -live-/-ri-, so there is no`);
    console.error(`correctness risk. But concurrent DuckDB jobs compete for resources.`);
    console.error('\nOptions:');
    console.error('  (a) SAFEST: pause the live ingest, run this, restart live ingest');
    console.error('  (b) Proceed with --force (accepts the resource contention risk)');
    process.exit(3);
  }
}

// ─────────────────────────────────────────────────────────────
// Oracle: SUM(event_count) across source updates
// We use this post-upload to verify the rematerialized row count matches.
// ─────────────────────────────────────────────────────────────

/**
 * Run a DuckDB query that returns a single JSON array, via COPY TO file.
 *
 * We cannot use `duckdb -json -c "setup; select ..."` and parse stdout
 * because multi-statement runs emit ONE JSON array per result-producing
 * statement (e.g. CREATE SECRET returns [{"Success":true}] before the
 * actual SELECT result), which doesn't parse as a single JSON value.
 * Writing the result to a temp file sidesteps the issue entirely and
 * mirrors the pattern streamUpdatesFromGCS already uses.
 */
async function duckdbQueryToJson(sql, label) {
  const outFile = join(TMP_ROOT, `query-${label}-${Date.now()}.json`);
  const wrappedSql = [
    `SET memory_limit='${DUCKDB_MEMORY}';`,
    `SET threads=${DUCKDB_THREADS};`,
    `SET preserve_insertion_order=false;`,
    `SET temp_directory='${sqlStr(DUCKDB_SPILL)}';`,
    `SET enable_progress_bar=false;`,
    `INSTALL httpfs; LOAD httpfs;`,
    `CREATE OR REPLACE SECRET s (TYPE GCS, KEY_ID '${sqlStr(HMAC_KEY_ID)}', SECRET '${sqlStr(HMAC_SECRET)}');`,
    `COPY (${sql}) TO '${sqlStr(outFile)}' (FORMAT JSON, ARRAY TRUE);`,
  ].join(' ');
  try {
    await execFile('duckdb', ['-c', wrappedSql], { maxBuffer: 16 * 1024 * 1024, timeout: 600000 });
  } catch (err) {
    throw new Error(`DuckDB query [${label}] failed: ${(err.stderr?.toString() || err.message).slice(0, 500)}`);
  }
  const { readFileSync } = await import('fs');
  const text = readFileSync(outFile, 'utf8').trim();
  try { unlinkSync(outFile); } catch {}
  return text ? JSON.parse(text) : [];
}

/**
 * Compute an oracle for post-extraction verification.
 *
 * Caveat: normalizeUpdate sets `event_count = Object.keys(events_by_id).length`,
 * which is ZERO for reassignments (their events live in a separate
 * `reassignment.event.{created,archived}_event` path, not in events_by_id).
 * But a reassignment can still produce 1 or 2 events in output. So
 * SUM(event_count) is a LOWER BOUND and the true expected value is
 *   [sum_event_count, sum_event_count + 2 * reassignments].
 */
async function queryExpectedEvents() {
  const glob = `${sourceUpdatesPrefix()}*.parquet`;
  const rows = await duckdbQueryToJson(
    [
      `SELECT`,
      `  COUNT(*)::BIGINT AS updates_count,`,
      `  COUNT_IF(update_type = 'reassignment')::BIGINT AS reassignments,`,
      `  COUNT_IF(update_type = 'transaction')::BIGINT  AS transactions,`,
      `  SUM(event_count)::BIGINT AS sum_event_count`,
      `FROM read_parquet('${sqlStr(glob)}', union_by_name=true)`,
    ].join(' '),
    'oracle'
  );
  const r = rows[0] || {};
  const updatesCount   = Number(r.updates_count    || 0);
  const reassignments  = Number(r.reassignments    || 0);
  const transactions   = Number(r.transactions     || 0);
  const sumEventCount  = Number(r.sum_event_count  || 0);
  return {
    updates_count:   updatesCount,
    transactions,
    reassignments,
    expected_events_min: sumEventCount,
    expected_events_max: sumEventCount + 2 * reassignments,
  };
}

// ─────────────────────────────────────────────────────────────
// Source reader: one source parquet file → one ndjson temp file → readline.
//
// We deliberately avoid `COPY TO '/dev/stdout'` — DuckDB versions and
// progress output make stdout fragile for strict JSON parsing.  Writing a
// per-source temp file is cheap (~5-50 MB each) given we have 200 GB of disk
// headroom and a spinning stream of files that get deleted as we consume them.
//
// Timestamp columns in the source parquet are stored as VARCHAR (matches the
// live writer in write-parquet.js:459) — calling strftime() on a VARCHAR
// errors, so we just SELECT the columns as-is.  update_data is also VARCHAR
// (JSON-stringified inner update).
// ─────────────────────────────────────────────────────────────

async function dumpSourceToNdjson(gcsPath, ndjsonPath) {
  const sql = [
    `SET memory_limit='${DUCKDB_MEMORY}';`,
    `SET threads=${DUCKDB_THREADS};`,
    `SET preserve_insertion_order=false;`,
    `SET temp_directory='${sqlStr(DUCKDB_SPILL)}';`,
    `SET enable_progress_bar=false;`,
    `INSTALL httpfs; LOAD httpfs;`,
    `CREATE OR REPLACE SECRET s (TYPE GCS, KEY_ID '${sqlStr(HMAC_KEY_ID)}', SECRET '${sqlStr(HMAC_SECRET)}');`,
    `COPY (SELECT`,
    `  update_id,`,
    `  update_type,`,
    `  migration_id,`,
    `  synchronizer_id,`,
    `  record_time,`,
    `  effective_at,`,
    `  recorded_at,`,
    `  event_count,`,
    `  update_data`,
    `FROM read_parquet('${sqlStr(gcsPath)}', union_by_name=true))`,
    `TO '${sqlStr(ndjsonPath)}' (FORMAT JSON, ARRAY FALSE);`,
  ].join(' ');

  try {
    await execFile('duckdb', ['-c', sql], { maxBuffer: 16 * 1024 * 1024, timeout: 600000 });
  } catch (err) {
    throw new Error(
      `duckdb read failed for ${gcsPath}: ${(err.stderr?.toString() || err.message).slice(0, 500)}`
    );
  }
}

async function* streamUpdatesFromGCS(gcsPath) {
  // Unique temp file per source parquet; deleted after the generator is consumed.
  const tag = createHash('sha256').update(gcsPath).digest('hex').slice(0, 12);
  const ndjsonPath = join(TMP_ROOT, 'src', `src-${tag}.ndjson`);
  if (!existsSync(dirname(ndjsonPath))) mkdirSync(dirname(ndjsonPath), { recursive: true });

  await dumpSourceToNdjson(gcsPath, ndjsonPath);

  try {
    const rl = createInterface({ input: createReadStream(ndjsonPath, { encoding: 'utf8' }), crlfDelay: Infinity });
    let lineCount = 0;
    for await (const raw of rl) {
      const line = raw.trim();
      if (!line) continue;
      try {
        yield JSON.parse(line);
        lineCount++;
      } catch (err) {
        throw new Error(`JSON parse failed on line ${lineCount + 1} of ${ndjsonPath}: ${err.message}`);
      }
    }
  } finally {
    // Always clean up the temp ndjson, even if the caller aborted.
    try { unlinkSync(ndjsonPath); } catch {}
  }
}

// ─────────────────────────────────────────────────────────────
// Event extraction — MIRRORS reingest-updates.js:1160-1230 exactly
// ─────────────────────────────────────────────────────────────

/**
 * Given a single row from the updates parquet (columns listed in
 * streamUpdatesFromGCS), reconstruct the normalized events for that update.
 *
 * The canonical logic lives in normalizeEvent() / flattenEventsInTreeOrder()
 * in data-schema.js — this function is the thin adapter that reverses what
 * normalizeUpdate() did to pack the data into `update_data`.
 */
function extractEventsFromUpdateRow(row) {
  // Parse the stringified inner update.  For transactions, `u` contains
  // events_by_id + root_event_ids; for reassignments it contains `event`
  // with a single created_event or archived_event.
  let u;
  try {
    u = typeof row.update_data === 'string' ? JSON.parse(row.update_data) : row.update_data;
  } catch (err) {
    throw new Error(`update_id=${row.update_id}: failed to parse update_data — ${err.message}`);
  }

  // Reuse the original normalization's view of per-update context, built
  // from the inner `u` (not the raw wrapper, which we no longer have).
  const updateInfo = {
    record_time:     u?.record_time     ?? row.record_time,
    effective_at:    u?.effective_at    ?? row.effective_at,
    synchronizer_id: u?.synchronizer_id ?? row.synchronizer_id,
    source:          u?.source          ?? null,
    target:          u?.target          ?? null,
    unassign_id:     u?.unassign_id     ?? null,
    submitter:       u?.submitter       ?? null,
    counter:         u?.counter         ?? null,
  };

  // Preserve the original batch timestamp so rematerialized events carry
  // the same recorded_at as their parent updates — not "now".
  const batchTimestamp = row.recorded_at ? new Date(row.recorded_at) : new Date();

  const migId    = Number(row.migration_id ?? TARGET_MIG);
  const updateId = row.update_id;
  const events   = [];

  if (row.update_type === 'reassignment') {
    // Matches reingest-updates.js:1184-1197
    const ce = u?.event?.created_event;
    const ae = u?.event?.archived_event;
    if (ce) {
      const ev = normalizeEvent(ce, updateId, migId, ce, updateInfo, { batchTimestamp });
      ev.event_type = 'reassign_create';
      events.push(ev);
    }
    if (ae) {
      const ev = normalizeEvent(ae, updateId, migId, ae, updateInfo, { batchTimestamp });
      ev.event_type = 'reassign_archive';
      events.push(ev);
    }
  } else {
    // Transactions — matches reingest-updates.js:1198-1210
    const eventsById   = u?.events_by_id  || u?.eventsById  || {};
    const rootEventIds = u?.root_event_ids || u?.rootEventIds || [];
    const flattened = flattenEventsInTreeOrder(eventsById, rootEventIds);

    for (const rawEvent of flattened) {
      const ev = normalizeEvent(rawEvent, updateId, migId, rawEvent, updateInfo, { batchTimestamp });
      // Preserve original event_id from the events_by_id map key when it differs from
      // what's inside the event body (matches reingest-updates.js:1206-1209)
      const mapKeyId = rawEvent.event_id;
      if (mapKeyId && ev.event_id && mapKeyId !== ev.event_id) ev.event_id = mapKeyId;
      else if (mapKeyId && !ev.event_id) ev.event_id = mapKeyId;
      events.push(ev);
    }
  }

  return events;
}

// ─────────────────────────────────────────────────────────────
// Output writer — deterministic filenames, chunk-flush on threshold
// ─────────────────────────────────────────────────────────────

// Accumulator: partition_path → { events: [], chunkIdx: int }
const partitionBuffers = new Map();

function partitionKey(partitionPath) {
  // partition_path looks like 'updates/events/migration=4/year=2026/month=4/day=2'
  if (!partitionBuffers.has(partitionPath)) {
    partitionBuffers.set(partitionPath, { events: [], chunkIdx: 0 });
  }
  return partitionBuffers.get(partitionPath);
}

function chunkFilename(partitionPath, chunkIdx) {
  // Deterministic: same (date, migration, partition, chunkIdx) → same name.
  // Source-file ordering is stable (sorted) so chunk boundaries are reproducible.
  const hash = createHash('sha256')
    .update(`${OPTS.date}|${TARGET_MIG}|${partitionPath}|${chunkIdx}`)
    .digest('hex')
    .slice(0, 16);
  return `events-remat-${hash}.parquet`;
}

// Explicit events-parquet schema — MIRRORS write-parquet.js:447-457.
// By using the SAME columns={...} dict the live writer uses, our output
// parquet has the same schema (column names, types, nullability) including
// the always-NULL `trace_context` column.  Without this, read_json_auto
// would infer the schema from the data and drop trace_context (and risk
// inferring different types for sparse columns).
const EVENTS_PARQUET_SCHEMA = `{
  event_id: 'VARCHAR', update_id: 'VARCHAR', event_type: 'VARCHAR', event_type_original: 'VARCHAR',
  synchronizer_id: 'VARCHAR', effective_at: 'VARCHAR', recorded_at: 'VARCHAR', created_at_ts: 'VARCHAR',
  timestamp: 'VARCHAR',
  contract_id: 'VARCHAR', template_id: 'VARCHAR', package_name: 'VARCHAR', migration_id: 'BIGINT',
  signatories: 'VARCHAR[]', observers: 'VARCHAR[]', acting_parties: 'VARCHAR[]', witness_parties: 'VARCHAR[]',
  child_event_ids: 'VARCHAR[]', consuming: 'BOOLEAN', reassignment_counter: 'BIGINT',
  choice: 'VARCHAR', interface_id: 'VARCHAR',
  source_synchronizer: 'VARCHAR', target_synchronizer: 'VARCHAR', unassign_id: 'VARCHAR', submitter: 'VARCHAR',
  payload: 'VARCHAR', contract_key: 'VARCHAR', exercise_result: 'VARCHAR', raw_event: 'VARCHAR', trace_context: 'VARCHAR'
}`;

// Pending uploads — populated by flushPartition() during the main loop,
// drained by uploadStagedChunks() AFTER the oracle check passes. Keeping
// GCS uploads entirely behind the integrity gate means a failed run leaves
// GCS untouched: we can delete the /tmp staging dir and retry cleanly.
const stagedChunks = [];

/**
 * Flush a partition's accumulated events to a LOCAL Parquet file and record
 * it for later upload.  Does NOT touch GCS — uploads are deferred to
 * uploadStagedChunks() which only runs after the oracle check passes.
 *
 * Idempotent filename: re-running with the same (date, migration, partition,
 * chunkIdx) produces the same name.  Safe for retry after a failed run.
 */
async function flushPartition(partitionPath) {
  const buf = partitionBuffers.get(partitionPath);
  if (!buf || buf.events.length === 0) return;

  const events = buf.events;
  const chunkIdx = buf.chunkIdx;
  buf.events = [];
  buf.chunkIdx = chunkIdx + 1;

  const fname = chunkFilename(partitionPath, chunkIdx);
  const jsonlPath   = join(TMP_ROOT, 'jsonl',   fname.replace('.parquet', '.jsonl'));
  const parquetPath = join(TMP_ROOT, 'parquet', fname);
  const gcsPath     = `gs://${BUCKET}/raw/${partitionPath}/${fname}`;

  // Dry-run: record the plan, don't touch disk.
  if (DRY_RUN) {
    REPORT.output_files.push({ partition: partitionPath, chunk: chunkIdx, events: events.length, gcs: gcsPath, dry_run: true });
    console.log(`    [dry-run] would stage ${events.length} events → ${gcsPath}`);
    return;
  }

  // Serialize events to ndjson, then convert to parquet via DuckDB.
  await writeJsonlFile(jsonlPath, events);

  const copySql = [
    `SET memory_limit='${DUCKDB_MEMORY}'; SET threads=${DUCKDB_THREADS};`,
    `SET preserve_insertion_order=false; SET temp_directory='${sqlStr(DUCKDB_SPILL)}';`,
    `COPY (SELECT * FROM read_json_auto('${sqlStr(jsonlPath)}', columns=${EVENTS_PARQUET_SCHEMA}, union_by_name=true, maximum_object_size=67108864))`,
    `TO '${sqlStr(parquetPath)}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 5000);`,
  ].join(' ');

  try {
    await execFile('duckdb', ['-c', copySql], { maxBuffer: 16 * 1024 * 1024, timeout: 600000 });
  } catch (err) {
    throw new Error(`DuckDB COPY failed for ${fname}: ${err.stderr?.toString()?.slice(0, 400) || err.message}`);
  }

  // Delete the intermediate jsonl immediately — the parquet carries the data.
  try { unlinkSync(jsonlPath); } catch {}

  stagedChunks.push({ partitionPath, chunkIdx, fname, parquetPath, gcsPath, eventCount: events.length });
  console.log(`    📦 staged ${events.length} events → ${parquetPath} (will upload after verify)`);
}

/**
 * Upload all staged chunks to GCS in order. Called only after the oracle
 * check confirms generated == expected. If an upload fails mid-batch, the
 * caller must manually inspect GCS and either delete the partial uploads
 * (filenames are logged) or rerun — the filenames are deterministic so a
 * rerun overwrites the same objects.
 */
async function uploadStagedChunks() {
  if (stagedChunks.length === 0) {
    console.log('\n  (no staged chunks to upload)');
    return;
  }
  console.log(`\n  Uploading ${stagedChunks.length} staged chunk(s) to GCS…`);
  let uploaded = 0;
  for (const c of stagedChunks) {
    try {
      execSync(`gsutil -q cp "${c.parquetPath}" "${c.gcsPath}"`, {
        stdio: ['ignore', 'pipe', 'pipe'],
        timeout: 180000,
      });
    } catch (err) {
      throw new Error(
        `gsutil upload failed for ${c.gcsPath} (after ${uploaded}/${stagedChunks.length} successful uploads): ` +
        (err.stderr?.toString() || err.message).slice(0, 300) +
        `\n\nTo recover: inspect gs://${BUCKET}/raw/${c.partitionPath}/ for any events-remat-* files already ` +
        `written, decide whether to delete them (gsutil rm) or let a rerun overwrite them.`
      );
    }
    REPORT.output_files.push({
      partition: c.partitionPath, chunk: c.chunkIdx, events: c.eventCount, gcs: c.gcsPath,
    });
    uploaded++;
    console.log(`    ✅ [${uploaded}/${stagedChunks.length}] ${c.gcsPath}`);
    try { unlinkSync(c.parquetPath); } catch {}
  }
}

/**
 * Write an array of event objects to a JSONL file, one line per event.
 * Large arrays (hundreds of thousands of rows × many-KB each) don't fit in
 * a single Buffer, so we stream through an fs write stream instead of
 * writeFileSync.
 */
async function writeJsonlFile(path, events) {
  const { createWriteStream } = await import('fs');
  return new Promise((resolve, reject) => {
    const stream = createWriteStream(path, { encoding: 'utf8' });
    stream.on('error', reject);
    stream.on('finish', resolve);
    for (const ev of events) {
      if (!stream.write(JSON.stringify(ev) + '\n')) {
        // Backpressure — pause briefly.  Each write is ~KB; drain handler below.
      }
    }
    stream.end();
  });
}

// ─────────────────────────────────────────────────────────────
// Post-upload verification
// ─────────────────────────────────────────────────────────────

async function verifyWrittenEvents() {
  // Count rows + check for duplicates across ALL partitions we wrote to
  // (target day ±1 catches any drift).  If any mismatch, fail loudly.
  const partitions = new Set();
  for (const o of REPORT.output_files) partitions.add(o.partition);

  if (partitions.size === 0) {
    console.log('\n  (no output partitions — nothing to verify)');
    return { ok: true, rows: 0, distinct: 0 };
  }

  const globs = [...partitions].map(p => `'gs://${BUCKET}/raw/${p}/*.parquet'`).join(', ');
  const rows = await duckdbQueryToJson(
    [
      `SELECT`,
      `  COUNT(*)::BIGINT AS rows,`,
      `  COUNT(DISTINCT event_id)::BIGINT AS distinct_event_id,`,
      `  COUNT(DISTINCT update_id)::BIGINT AS distinct_update_id,`,
      `  SUM(CASE WHEN event_id IS NULL THEN 1 ELSE 0 END)::BIGINT AS null_event_id,`,
      `  SUM(CASE WHEN update_id IS NULL THEN 1 ELSE 0 END)::BIGINT AS null_update_id`,
      `FROM read_parquet([${globs}], union_by_name=true)`,
    ].join(' '),
    'post-verify'
  );
  return rows[0] || {};
}

// ─────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────

async function main() {
  const banner = '═'.repeat(75);
  console.log(banner);
  console.log(`  REMATERIALIZE EVENTS FROM UPDATES`);
  console.log(`  Target:    gs://${BUCKET}/raw/${TARGET_PARTITION}/`);
  console.log(`  Migration: ${TARGET_MIG}    Date: ${OPTS.date}    Source: ${SOURCE}`);
  console.log(`  Mode:      ${DRY_RUN ? 'DRY RUN (add --execute to commit)' : 'EXECUTE'}`);
  console.log(banner);

  checkPrereqs();
  ensureTmpDirs();

  console.log('\n── Pre-flight ──');
  preflight();

  console.log('\n── Counting source (oracle for post-verify) ──');
  const oracle = await queryExpectedEvents();
  REPORT.source_updates         = oracle.updates_count;
  REPORT.transactions           = oracle.transactions;
  REPORT.reassignments          = oracle.reassignments;
  REPORT.expected_events_min    = oracle.expected_events_min;
  REPORT.expected_events_max    = oracle.expected_events_max;
  console.log(`  updates:          ${oracle.updates_count.toLocaleString()} ` +
              `(transactions=${oracle.transactions.toLocaleString()}, reassignments=${oracle.reassignments.toLocaleString()})`);
  console.log(`  expected events:  ${oracle.expected_events_min.toLocaleString()}` +
              (oracle.reassignments > 0 ? ` – ${oracle.expected_events_max.toLocaleString()} (range due to reassignments; see oracle caveat)` : ` (exact)`));
  if (oracle.updates_count === 0 || oracle.expected_events_max === 0) {
    throw new Error('Source oracle returned zero updates or zero expected events — cannot proceed.');
  }

  console.log('\n── Extracting and writing ──');
  const start = Date.now();
  let rowsSeen = 0;

  // Iterate source files in sorted order so chunk boundaries (and thus filenames)
  // are reproducible across runs.
  for (let i = 0; i < REPORT.source_files.length; i++) {
    const srcFile = REPORT.source_files[i];
    const label = `[${i + 1}/${REPORT.source_files.length}] ${srcFile.split('/').pop()}`;
    if (OPTS.verbose) console.log(`  ${label}`);

    for await (const row of streamUpdatesFromGCS(srcFile)) {
      rowsSeen++;
      let events;
      try {
        events = extractEventsFromUpdateRow(row);
      } catch (err) {
        REPORT.errors.push({ update_id: row.update_id, error: err.message });
        if (REPORT.errors.length <= 10) console.warn(`    ⚠️  ${err.message}`);
        continue;
      }
      if (events.length === 0) continue;

      // Route each event to its own effective_at-based partition (events can
      // drift across midnight; groupByPartition handles per-record routing).
      const groups = groupByPartition(events, 'events', SOURCE, TARGET_MIG);
      for (const [partitionPath, partEvents] of Object.entries(groups)) {
        const buf = partitionKey(partitionPath);
        REPORT.events_by_partition[partitionPath] =
          (REPORT.events_by_partition[partitionPath] || 0) + partEvents.length;
        REPORT.generated_events += partEvents.length;

        buf.events.push(...partEvents);
        if (buf.events.length >= MAX_EVENTS_PER_FILE) {
          await flushPartition(partitionPath);
        }
      }
    }

    if (!OPTS.verbose && (i + 1) % 50 === 0) {
      console.log(`  [${i + 1}/${REPORT.source_files.length}] updates=${rowsSeen.toLocaleString()} events=${REPORT.generated_events.toLocaleString()}`);
    }
  }

  // Flush any residual buffers
  console.log('\n── Final flush ──');
  for (const partitionPath of partitionBuffers.keys()) {
    await flushPartition(partitionPath);
  }

  const elapsedS = Math.round((Date.now() - start) / 1000);
  console.log(`\n  updates processed:  ${rowsSeen.toLocaleString()}`);
  console.log(`  events generated:   ${REPORT.generated_events.toLocaleString()}`);
  console.log(`  expected range:     [${REPORT.expected_events_min.toLocaleString()}, ${REPORT.expected_events_max.toLocaleString()}]`);
  console.log(`  elapsed:            ${elapsedS}s`);

  // Sanity check #1: generated events must fall within the oracle's range.
  //
  // GATE FOR GCS UPLOADS: by design nothing has been uploaded to GCS yet —
  // all flushPartition() calls have only staged chunks locally in /tmp.
  // If this check fails we abort WITHOUT touching GCS; the user can delete
  // /tmp/remat-events/m<mig>-<date>-<source>/ and retry. --force does NOT
  // override this: a count outside the oracle range is an integrity failure
  // (dropped events, duplicates, or a bad update_data row).
  //
  // The range accounts for reassignments: SUM(event_count) undercounts them
  // (see queryExpectedEvents comment). Any reassignment contributes 1-2
  // events, so the actual count is bounded by [min, min + 2*reassignments].
  const gen = REPORT.generated_events;
  if (gen < REPORT.expected_events_min || gen > REPORT.expected_events_max) {
    console.error(`\n  ❌ OUT OF RANGE: generated ${gen} not in [${REPORT.expected_events_min}, ${REPORT.expected_events_max}]`);
    if (REPORT.errors.length > 0) {
      console.error(`  (${REPORT.errors.length} per-update error(s) recorded — see the report file)`);
    }
    throw new Error(
      `Event count out of oracle range — nothing uploaded to GCS. Inspect the ` +
      `report in /tmp/remat-events/m${TARGET_MIG}-${OPTS.date}-${SOURCE}/ for ` +
      `per-update errors, fix the underlying cause, and re-run. Staged chunks ` +
      `will be cleaned up automatically on retry.`
    );
  } else if (gen === REPORT.expected_events_min && REPORT.expected_events_min === REPORT.expected_events_max) {
    console.log(`\n  ✅ event count matches oracle exactly`);
  } else {
    console.log(`\n  ✅ event count ${gen} within oracle range [${REPORT.expected_events_min}, ${REPORT.expected_events_max}] (reassignments present)`);
  }

  // Now that integrity is verified, upload the staged chunks.
  if (!DRY_RUN) {
    console.log('\n── Uploading staged chunks ──');
    await uploadStagedChunks();
  }

  // Post-upload verification (skipped for dry-run — nothing was written)
  if (!DRY_RUN) {
    console.log('\n── Post-upload verification ──');
    const v = await verifyWrittenEvents();
    console.log(`  rows in GCS:        ${Number(v.rows || 0).toLocaleString()}`);
    console.log(`  distinct event_id:  ${Number(v.distinct_event_id || 0).toLocaleString()}`);
    console.log(`  distinct update_id: ${Number(v.distinct_update_id || 0).toLocaleString()}`);

    const problems = [];
    const rows = Number(v.rows || 0);
    if (rows !== REPORT.generated_events) {
      problems.push(`rows in GCS ${rows} != generated ${REPORT.generated_events} (upload dropped events)`);
    }
    if (rows < REPORT.expected_events_min || rows > REPORT.expected_events_max) {
      problems.push(`rows ${rows} outside oracle range [${REPORT.expected_events_min}, ${REPORT.expected_events_max}]`);
    }
    if (rows !== Number(v.distinct_event_id || 0)) {
      problems.push(`rows ${rows} != distinct event_id ${v.distinct_event_id} (duplicates)`);
    }
    if (Number(v.null_event_id || 0) > 0)   problems.push(`${v.null_event_id} null event_id(s)`);
    if (Number(v.null_update_id || 0) > 0)  problems.push(`${v.null_update_id} null update_id(s)`);

    if (problems.length > 0) {
      console.error(`\n  ❌ POST-VERIFY FAILED:\n    - ${problems.join('\n    - ')}`);
      REPORT.verify = { ok: false, problems, ...v };
      throw new Error('Post-upload verification failed — see problems above. Inspect GCS and consider `gsutil rm` on the events-remat-* files before retrying.');
    } else {
      console.log(`\n  ✅ post-upload verification passed`);
      REPORT.verify = { ok: true, ...v };
    }
  }

  REPORT.finished_at = new Date().toISOString();

  // Persist the report (useful on failure too — catch-all in the catch below)
  const reportPath = join(TMP_ROOT, `report-${OPTS.date}-m${TARGET_MIG}-${Date.now()}.json`);
  writeFileSync(reportPath, JSON.stringify(REPORT, null, 2));
  console.log(`\n📄 Report saved to ${reportPath}`);

  cleanupTmp();
  return 0;
}

main()
  .then(code => process.exit(code))
  .catch(err => {
    REPORT.finished_at = new Date().toISOString();
    REPORT.errors.push({ fatal: true, message: err.message });
    try {
      const p = join(TMP_ROOT, `report-ERROR-${OPTS.date}-m${TARGET_MIG}-${Date.now()}.json`);
      writeFileSync(p, JSON.stringify(REPORT, null, 2));
      console.error(`\n📄 Error report saved to ${p}`);
    } catch {}
    console.error(`\n❌ FATAL: ${err.message}`);
    if (OPTS.verbose && err.stack) console.error(err.stack);
    process.exit(1);
  });
