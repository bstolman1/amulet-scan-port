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
import { createReadStream, existsSync, mkdirSync, unlinkSync, readdirSync, writeFileSync, statfsSync } from 'fs';
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
const TMP_ROOT         = process.env.REMAT_TMP_DIR || '/tmp/remat-events';
const DUCKDB_SPILL     = join(TMP_ROOT, 'duckdb_spill');

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

const REPORT = {
  started_at:  new Date().toISOString(),
  migration:   TARGET_MIG,
  date:        OPTS.date,
  source:      SOURCE,
  dry_run:     DRY_RUN,
  source_files: [],
  source_updates: 0,
  expected_events: 0,          // SUM(event_count) from source updates — our row-count oracle
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
  for (const d of [TMP_ROOT, DUCKDB_SPILL, join(TMP_ROOT, 'jsonl'), join(TMP_ROOT, 'parquet')]) {
    if (!existsSync(d)) mkdirSync(d, { recursive: true });
  }
}

function cleanupTmp() {
  // Remove per-run artifacts but keep TMP_ROOT around so it isn't re-created every step.
  for (const sub of ['jsonl', 'parquet']) {
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
  const running = sh(`pgrep -fa 'fetch-updates.js|reingest-updates.js' || true`)
    .split('\n').filter(Boolean);
  if (running.length > 0 && !OPTS.force) {
    console.error('\nERROR: ingestion process(es) running — would race on GCS writes:');
    running.slice(0, 5).forEach(l => console.error(`  ${l}`));
    console.error('\nStop them (or pass --force) before running a rematerialization.');
    process.exit(3);
  }
}

// ─────────────────────────────────────────────────────────────
// Oracle: SUM(event_count) across source updates
// We use this post-upload to verify the rematerialized row count matches.
// ─────────────────────────────────────────────────────────────

async function queryExpectedEvents() {
  const glob = `${sourceUpdatesPrefix()}*.parquet`;
  const sql = [
    `SET memory_limit='${DUCKDB_MEMORY}'; SET threads=${DUCKDB_THREADS};`,
    `SET preserve_insertion_order=false; SET temp_directory='${sqlStr(DUCKDB_SPILL)}';`,
    `INSTALL httpfs; LOAD httpfs;`,
    `CREATE OR REPLACE SECRET s (TYPE GCS, KEY_ID '${sqlStr(HMAC_KEY_ID)}', SECRET '${sqlStr(HMAC_SECRET)}');`,
    `SELECT COUNT(*)::BIGINT AS updates_count, SUM(event_count)::BIGINT AS expected_events`,
    `FROM read_parquet('${sqlStr(glob)}', union_by_name=true);`,
  ].join(' ');
  const { stdout } = await execFile('duckdb', ['-json', '-c', sql], { maxBuffer: 16 * 1024 * 1024, timeout: 600000 });
  const rows = JSON.parse(stdout.trim() || '[]');
  const r = rows[0] || {};
  return {
    updates_count:   Number(r.updates_count   || 0),
    expected_events: Number(r.expected_events || 0),
  };
}

// ─────────────────────────────────────────────────────────────
// Streaming reader: DuckDB dumps newline-delimited JSON; Node streams it line-by-line.
// One DuckDB invocation per source parquet file keeps memory bounded and lets us
// pipeline with event extraction.
// ─────────────────────────────────────────────────────────────

/**
 * Spawn a DuckDB process that reads one source parquet file and emits
 * newline-delimited JSON on stdout with only the columns we need for
 * event reconstruction.  The returned iterator yields already-parsed rows.
 */
async function* streamUpdatesFromGCS(gcsPath) {
  const sql = [
    `SET memory_limit='${DUCKDB_MEMORY}';`,
    `SET threads=${DUCKDB_THREADS};`,
    `SET preserve_insertion_order=false;`,
    `SET temp_directory='${sqlStr(DUCKDB_SPILL)}';`,
    `INSTALL httpfs; LOAD httpfs;`,
    `CREATE OR REPLACE SECRET s (TYPE GCS, KEY_ID '${sqlStr(HMAC_KEY_ID)}', SECRET '${sqlStr(HMAC_SECRET)}');`,
    // NB: cast timestamps to ISO strings so round-tripping through JSON preserves precision.
    // update_data is ALREADY a JSON string column, so emit as-is.
    `COPY (SELECT`,
    `  update_id,`,
    `  update_type,`,
    `  migration_id,`,
    `  synchronizer_id,`,
    `  strftime(record_time, '%Y-%m-%dT%H:%M:%S.%fZ') AS record_time,`,
    `  strftime(effective_at, '%Y-%m-%dT%H:%M:%S.%fZ') AS effective_at,`,
    `  strftime(recorded_at, '%Y-%m-%dT%H:%M:%S.%fZ') AS recorded_at,`,
    `  event_count,`,
    `  update_data`,
    `FROM read_parquet('${sqlStr(gcsPath)}', union_by_name=true))`,
    `TO '/dev/stdout' (FORMAT JSON, ARRAY false);`,
  ].join(' ');

  const child = spawn('duckdb', ['-c', sql], { stdio: ['ignore', 'pipe', 'pipe'] });
  const stderrBuf = [];
  child.stderr.on('data', chunk => stderrBuf.push(chunk));

  const rl = createInterface({ input: child.stdout, crlfDelay: Infinity });
  let exitErr = null;
  const exitPromise = new Promise(resolve => {
    child.on('exit', code => {
      if (code !== 0) {
        exitErr = new Error(
          `duckdb exited ${code} reading ${gcsPath}: ${Buffer.concat(stderrBuf).toString().slice(0, 400)}`
        );
      }
      resolve();
    });
  });

  let lineCount = 0;
  for await (const raw of rl) {
    const line = raw.trim();
    if (!line || line.startsWith('[') || line.startsWith(']')) continue;
    const clean = line.endsWith(',') ? line.slice(0, -1) : line;
    try {
      yield JSON.parse(clean);
      lineCount++;
    } catch (err) {
      throw new Error(`JSON parse failed on line ${lineCount + 1} of ${gcsPath}: ${err.message}`);
    }
  }

  await exitPromise;
  if (exitErr) throw exitErr;
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

/**
 * Flush a partition's accumulated events to a Parquet file, upload to GCS,
 * clean up tmp files.  Idempotent filename: overwrites on re-run.
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

  // Write JSONL line-by-line (avoid loading all as one string).
  await writeJsonlFile(jsonlPath, events);

  if (DRY_RUN) {
    REPORT.output_files.push({ partition: partitionPath, chunk: chunkIdx, events: events.length, gcs: gcsPath, dry_run: true });
    console.log(`    [dry-run] would write ${events.length} events → ${gcsPath}`);
    unlinkSync(jsonlPath);
    return;
  }

  // JSONL → Parquet via DuckDB CLI. Uses the same memory knobs as the live writer.
  const columnTypes = EVENTS_COLUMNS.map(c => `"${c}": 'JSON'`).join(', ');
  // We build the schema explicitly so column order + types are stable.
  const copySql = [
    `SET memory_limit='${DUCKDB_MEMORY}'; SET threads=${DUCKDB_THREADS};`,
    `SET preserve_insertion_order=false; SET temp_directory='${sqlStr(DUCKDB_SPILL)}';`,
    `COPY (SELECT ${EVENTS_COLUMNS.map(c => `"${c}"`).join(', ')}`,
    `      FROM read_json_auto('${sqlStr(jsonlPath)}', format='newline_delimited', union_by_name=true, maximum_object_size=67108864))`,
    `TO '${sqlStr(parquetPath)}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 5000);`,
  ].join(' ');

  try {
    await execFile('duckdb', ['-c', copySql], { maxBuffer: 16 * 1024 * 1024, timeout: 600000 });
  } catch (err) {
    throw new Error(`DuckDB COPY failed for ${fname}: ${err.stderr?.toString()?.slice(0, 400) || err.message}`);
  }

  // Upload to GCS via gsutil cp. -n would skip if exists; we want overwrite
  // (idempotent re-runs), so plain cp.
  try {
    execSync(`gsutil -q cp "${parquetPath}" "${gcsPath}"`, {
      stdio: ['ignore', 'pipe', 'pipe'],
      timeout: 120000,
    });
  } catch (err) {
    throw new Error(`gsutil upload failed for ${gcsPath}: ${(err.stderr?.toString() || err.message).slice(0, 300)}`);
  }

  REPORT.output_files.push({ partition: partitionPath, chunk: chunkIdx, events: events.length, gcs: gcsPath });
  console.log(`    ✅ wrote ${events.length} events → ${gcsPath}`);

  // Cleanup local files immediately.
  try { unlinkSync(jsonlPath); } catch {}
  try { unlinkSync(parquetPath); } catch {}
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
  const sql = [
    `SET memory_limit='${DUCKDB_MEMORY}'; SET threads=${DUCKDB_THREADS};`,
    `SET preserve_insertion_order=false; SET temp_directory='${sqlStr(DUCKDB_SPILL)}';`,
    `INSTALL httpfs; LOAD httpfs;`,
    `CREATE OR REPLACE SECRET s (TYPE GCS, KEY_ID '${sqlStr(HMAC_KEY_ID)}', SECRET '${sqlStr(HMAC_SECRET)}');`,
    `SELECT`,
    `  COUNT(*)::BIGINT AS rows,`,
    `  COUNT(DISTINCT event_id)::BIGINT AS distinct_event_id,`,
    `  COUNT(DISTINCT update_id)::BIGINT AS distinct_update_id,`,
    `  SUM(CASE WHEN event_id IS NULL THEN 1 ELSE 0 END)::BIGINT AS null_event_id,`,
    `  SUM(CASE WHEN update_id IS NULL THEN 1 ELSE 0 END)::BIGINT AS null_update_id`,
    `FROM read_parquet([${globs}], union_by_name=true);`,
  ].join(' ');

  const { stdout } = await execFile('duckdb', ['-json', '-c', sql], { maxBuffer: 16 * 1024 * 1024, timeout: 600000 });
  const rows = JSON.parse(stdout.trim() || '[]');
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
  REPORT.source_updates   = oracle.updates_count;
  REPORT.expected_events  = oracle.expected_events;
  console.log(`  updates: ${oracle.updates_count.toLocaleString()}`);
  console.log(`  expected events (SUM(event_count)): ${oracle.expected_events.toLocaleString()}`);
  if (oracle.updates_count === 0 || oracle.expected_events === 0) {
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
  console.log(`  expected (oracle):  ${REPORT.expected_events.toLocaleString()}`);
  console.log(`  elapsed:            ${elapsedS}s`);

  // Sanity check #1: local count matches oracle
  if (REPORT.generated_events !== REPORT.expected_events) {
    const diff = REPORT.generated_events - REPORT.expected_events;
    console.error(`\n  ❌ MISMATCH: generated ${REPORT.generated_events} vs expected ${REPORT.expected_events} (diff ${diff > 0 ? '+' : ''}${diff})`);
    if (!OPTS.force) {
      throw new Error(`Event count mismatch — refusing to continue without --force. See /tmp/remat-events/last-report.json for the per-update errors.`);
    }
    console.error('  ⚠️  Continuing due to --force');
  } else {
    console.log(`\n  ✅ event count matches oracle exactly`);
  }

  // Post-upload verification (skipped for dry-run — nothing was written)
  if (!DRY_RUN) {
    console.log('\n── Post-upload verification ──');
    const v = await verifyWrittenEvents();
    console.log(`  rows in GCS:        ${Number(v.rows || 0).toLocaleString()}`);
    console.log(`  distinct event_id:  ${Number(v.distinct_event_id || 0).toLocaleString()}`);
    console.log(`  distinct update_id: ${Number(v.distinct_update_id || 0).toLocaleString()}`);

    const problems = [];
    if (Number(v.rows || 0) !== REPORT.expected_events) {
      problems.push(`row count ${v.rows} != expected ${REPORT.expected_events}`);
    }
    if (Number(v.rows || 0) !== Number(v.distinct_event_id || 0)) {
      problems.push(`rows ${v.rows} != distinct event_id ${v.distinct_event_id} (duplicates)`);
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
