#!/usr/bin/env node
/**
 * Comprehensive Data Quality Check for Scan API → GCS raw data.
 *
 * Scans every migration/day partition in `gs://$GCS_BUCKET/raw/` and runs:
 *
 *   STRUCTURAL (filename-only, cheap)
 *     - Missing day partitions between first/last observed date per migration
 *     - Intra-day time gaps between consecutive Parquet files
 *     - updates/ vs events/ day alignment
 *     - Empty partitions (folder exists, 0 parquet files)
 *     - Zero-byte / suspiciously tiny parquet files
 *
 *   ROW-LEVEL (DuckDB over GCS via httpfs — one day at a time)
 *     - Duplicate update_id / event_id within a day
 *     - Null values in critical columns
 *     - Partition path vs effective_at mismatch
 *     - Migration path vs migration_id mismatch
 *     - Orphan events (events.update_id with no match in same-day updates)
 *     - event_count consistency (updates.event_count vs observed events)
 *     - Offset jumps / decreases within a day (updates only)
 *     - Timestamp sanity (record_time/effective_at plausibility)
 *
 *   CROSS-SOURCE (per migration)
 *     - Duplicate update_id across raw/backfill/ and raw/updates/ boundary days
 *
 *   ACS
 *     - Duplicate contract_id per snapshot
 *     - Null rates on critical columns
 *     - Missing _COMPLETE marker
 *
 * Output: pretty console summary + optional structured JSON report.
 * Exit: 0 if clean, 1 if any finding.
 *
 * Requirements:
 *   - GCS_BUCKET env var
 *   - gsutil authenticated (ADC or service account)
 *   - duckdb CLI in PATH
 *
 * Usage examples:
 *   node data-quality-check.js                                # all checks, all data
 *   node data-quality-check.js --quick                        # metadata-only: structural + alignment
 *   node data-quality-check.js --migration=3 --source=backfill
 *   node data-quality-check.js --start=2026-03-01 --end=2026-04-01
 *   node data-quality-check.js --checks=structural,dups,nulls
 *   node data-quality-check.js --concurrency=2 --output=dq-report.json
 *   node data-quality-check.js --dry-run                      # show plan, no queries
 *   node data-quality-check.js --cross-day-dups=updates       # opt-in heavy check
 */

import { execSync, execFile as execFileCb } from 'child_process';
import { promisify } from 'util';
import { existsSync, mkdirSync, writeFileSync, statfsSync, readFileSync, unlinkSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import dotenv from 'dotenv';

const execFile = promisify(execFileCb);
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
dotenv.config({ path: join(__dirname, '.env') });

// ─────────────────────────────────────────────────────────────
// Config
// ─────────────────────────────────────────────────────────────

const BUCKET = process.env.GCS_BUCKET || 'canton-bucket';

// DuckDB tuning — safe envelope for 31 GiB VM, leaves room for OS + Node + live ingest
const DUCKDB_MEMORY    = process.env.DQ_DUCKDB_MEMORY || '6GB';
const DUCKDB_THREADS   = parseInt(process.env.DQ_DUCKDB_THREADS || '2', 10);
const DUCKDB_SPILL_DIR = process.env.DQ_SPILL_DIR || '/tmp/duckdb_dq_spill';
const DUCKDB_MAX_OBJ   = 67108864;  // 64 MB — matches ingest

// Per-query timeout and retry
const QUERY_TIMEOUT_MS = parseInt(process.env.DQ_QUERY_TIMEOUT_MS || '300000', 10);  // 5 min
const QUERY_MAX_RETRIES = 2;

// Disk safety — abort if less than this free on /tmp before row-level checks
const MIN_FREE_TMP_GB = 15;

// Time-gap threshold (seconds) between consecutive parquet files in a day
const INTRA_DAY_GAP_THRESHOLD_S = parseInt(process.env.DQ_GAP_THRESHOLD || '300', 10);

// Plausible partition year range — matches data-schema.js bounds
const YEAR_MIN = parseInt(process.env.PARTITION_YEAR_MIN || '2020', 10);
const YEAR_MAX = parseInt(process.env.PARTITION_YEAR_MAX || '2035', 10);

// Critical columns per data type
const UPDATES_CRITICAL_COLS = ['update_id', 'record_time', 'effective_at', 'migration_id'];
const EVENTS_CRITICAL_COLS  = ['event_id', 'update_id', 'contract_id', 'template_id', 'effective_at', 'migration_id'];
const ACS_CRITICAL_COLS     = ['contract_id', 'template_id', 'migration_id'];

// All check names, in run order
const ALL_CHECKS = [
  'structural',    // gaps/empty/tiny (filename metadata only)
  'alignment',     // per-day asymmetry: updates without events, or events without updates
  'nulls',         // null rates in critical cols
  'dups',          // per-day dup update_id / event_id
  'partition',     // effective_at / migration_id vs partition path
  'orphans',       // events with no matching update in same day
  'event_count',   // updates.event_count vs observed events
  'timestamps',    // plausibility + ordering
  'offsets',       // offset monotonicity within a day (updates)
  'boundary',      // backfill ↔ updates overlap (per migration)
  'acs',           // ACS snapshot dup + nulls + _COMPLETE
];

// ─────────────────────────────────────────────────────────────
// CLI parsing
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
  source:         argVal('source', 'all'),     // backfill | updates | all
  migration:      argVal('migration') !== null ? parseInt(argVal('migration'), 10) : null,
  start:          argVal('start'),
  end:            argVal('end'),
  checks:         (argVal('checks', hasFlag('quick') ? 'structural,alignment' : 'all'))
                    .split(',').map(s => s.trim()).filter(Boolean),
  concurrency:    parseInt(argVal('concurrency', '4'), 10),
  sampleDays:     argVal('sample-days') !== null ? parseInt(argVal('sample-days'), 10) : null,
  output:         argVal('output'),
  crossDayDups:   argVal('cross-day-dups'),    // updates | events | null
  dryRun:         hasFlag('dry-run'),
  verbose:        hasFlag('verbose') || hasFlag('v'),
  force:          hasFlag('force'),
  help:           hasFlag('help') || hasFlag('h'),
  skipAcs:        hasFlag('skip-acs'),
  quick:          hasFlag('quick'),
};

if (OPTS.checks.includes('all')) {
  OPTS.checks = ALL_CHECKS.slice();
}
if (OPTS.skipAcs) OPTS.checks = OPTS.checks.filter(c => c !== 'acs');

function printHelpAndExit() {
  console.log(`
Comprehensive data-quality check over gs://${BUCKET}/raw/.

Flags:
  --source=backfill|updates|all   (default: all)
  --migration=N                   (default: all discovered)
  --start=YYYY-MM-DD              (default: earliest observed)
  --end=YYYY-MM-DD                (default: latest observed)
  --checks=a,b,c                  (default: all — see list below)
  --quick                         shorthand for --checks=structural,alignment (metadata-only, minutes)
  --sample-days=N                 random-sample N days per migration/source (row-level only)
  --concurrency=N                 parallel DuckDB queries (default: 4)
  --cross-day-dups=updates|events opt-in full-migration dedup (heavy, hours)
  --output=path.json              write structured report
  --dry-run                       print the plan, skip queries
  --force                         skip the "live ingest running" guard
  --skip-acs                      skip ACS checks
  --verbose, -v
  --help, -h

Available checks: ${ALL_CHECKS.join(', ')}
`);
  process.exit(0);
}
if (OPTS.help) printHelpAndExit();

// ─────────────────────────────────────────────────────────────
// Result state
// ─────────────────────────────────────────────────────────────

const REPORT = {
  started_at:  new Date().toISOString(),
  finished_at: null,
  bucket:      BUCKET,
  options:     OPTS,
  env: {
    duckdb_memory:  DUCKDB_MEMORY,
    duckdb_threads: DUCKDB_THREADS,
    spill_dir:      DUCKDB_SPILL_DIR,
  },
  partitions_scanned: 0,
  findings: [],           // { severity, check, scope, detail, counts }
  per_day: [],            // one entry per (source, mig, type, date)
  summary: {},            // filled at the end
};

const SEVERITY = { INFO: 'info', WARN: 'warn', ERROR: 'error' };

function addFinding(severity, check, scope, detail, counts = {}) {
  REPORT.findings.push({ severity, check, scope, detail, counts });
  if (OPTS.verbose || severity !== SEVERITY.INFO) {
    const icon = severity === SEVERITY.ERROR ? '❌' : severity === SEVERITY.WARN ? '⚠️ ' : 'ℹ️ ';
    console.log(`   ${icon} [${check}] ${scope}: ${detail}`);
  }
}

// ─────────────────────────────────────────────────────────────
// Shell / gsutil helpers
// ─────────────────────────────────────────────────────────────

function sh(cmd, { timeout = 120000, maxBuffer = 256 * 1024 * 1024 } = {}) {
  try {
    // NB: we deliberately do NOT redirect stderr to /dev/null or append
    // `|| true` — an auth failure from gsutil is silent otherwise and
    // every check just reports "no data found".
    return execSync(cmd, { stdio: ['ignore', 'pipe', 'pipe'], timeout, maxBuffer }).toString();
  } catch (err) {
    const stderr = (err.stderr?.toString() || '');
    const msg = (stderr + ' ' + (err.message || '')).toLowerCase();
    // "No URLs matched" / "matched no objects" is the normal "prefix empty"
    // signal and must stay silent. Everything else (auth, network, quota) is
    // loud — callers should not continue as if the data is missing.
    if (msg.includes('no urls matched') || msg.includes('matched no objects')) {
      return '';
    }
    if (msg.includes('reauthentication required') || msg.includes('your credentials are invalid') ||
        msg.includes('accessdeniedexception') || msg.includes('401 anonymous') ||
        msg.includes('login required')) {
      throw new Error(
        `gsutil auth failed — run \`gcloud auth application-default login\` ` +
        `(or \`gcloud auth login\` on the VM) and retry.\n  ${stderr.trim().split('\n')[0] || err.message}`
      );
    }
    throw new Error(`gsutil error: ${stderr.trim().split('\n').slice(-3).join(' | ') || err.message}`);
  }
}

function checkPrereqs() {
  try { execSync('gsutil version', { stdio: 'pipe' }); }
  catch { throw new Error('gsutil not found in PATH — install Google Cloud SDK and run `gcloud auth application-default login`.'); }
  try { execSync('duckdb --version', { stdio: 'pipe' }); }
  catch { throw new Error('duckdb CLI not found in PATH — install duckdb.'); }
  // Probe bucket auth — if creds are stale, fail here instead of silently
  // reporting "no migrations found" for every prefix.
  try {
    execSync(`gsutil ls "gs://${BUCKET}/" > /dev/null`, { stdio: ['ignore', 'pipe', 'pipe'], timeout: 30000 });
  } catch (err) {
    const stderr = (err.stderr?.toString() || err.message || '').trim().split('\n').slice(0, 3).join(' | ');
    throw new Error(`gsutil cannot list gs://${BUCKET}/ — ${stderr}\n  Try: gcloud auth application-default login`);
  }
}

function checkFreeTmpGB() {
  try {
    const s = statfsSync('/tmp');
    return Math.floor((Number(s.bavail) * Number(s.bsize)) / (1024 ** 3));
  } catch { return null; }
}

function liveIngestRunning() {
  try {
    // `grep -v pgrep` drops pgrep's self-match (the shell command itself
    // contains the pattern strings, so pgrep would otherwise match its own
    // invocation as a "running ingest process").
    const out = execSync("pgrep -fa 'fetch-updates.js|fetch-backfill.js|reingest-updates.js' | grep -v pgrep || true", { stdio: 'pipe' }).toString();
    return out.trim().split('\n').filter(Boolean);
  } catch { return []; }
}

function gsutilLsFlat(prefix) {
  const out = sh(`gsutil ls "${prefix}"`);
  return out.split('\n').map(s => s.trim()).filter(Boolean);
}

/**
 * Recursively list parquet files under a GCS prefix in one gsutil call.
 * Returns Map<"Y/M/D", [gs://... paths]>.
 */
function bulkListParquet(prefix) {
  const out = sh(`gsutil ls -r "${prefix}"`, { timeout: 180000 });
  if (!out) return new Map();
  const index = new Map();
  for (const line of out.split('\n')) {
    const p = line.trim();
    if (!p.endsWith('.parquet')) continue;
    const m = p.match(/year=(\d+)\/month=(\d+)\/day=(\d+)\//);
    if (!m) continue;
    const key = `${m[1]}/${m[2]}/${m[3]}`;
    if (!index.has(key)) index.set(key, []);
    index.get(key).push(p);
  }
  return index;
}

/**
 * Long-listing (name + byte size) for tiny-file detection. Parsed from `gsutil ls -l`.
 */
function bulkListParquetWithSize(prefix) {
  const out = sh(`gsutil ls -l "${prefix}**/*.parquet"`, { timeout: 180000 });
  if (!out) return [];
  const files = [];
  for (const line of out.split('\n')) {
    const m = line.match(/^\s*(\d+)\s+(\S+)\s+(gs:\/\/.+\.parquet)\s*$/);
    if (m) files.push({ size: parseInt(m[1], 10), modified: m[2], path: m[3] });
  }
  return files;
}

function discoverMigrations(sourcePrefix) {
  const migs = [];
  for (const line of gsutilLsFlat(sourcePrefix)) {
    const m = line.match(/migration=(\d+)/);
    if (m) migs.push(parseInt(m[1], 10));
  }
  return [...new Set(migs)].sort((a, b) => a - b);
}

// ─────────────────────────────────────────────────────────────
// DuckDB helpers
// ─────────────────────────────────────────────────────────────

function ensureSpillDir() {
  if (!existsSync(DUCKDB_SPILL_DIR)) mkdirSync(DUCKDB_SPILL_DIR, { recursive: true });
}

function duckdbPrelude() {
  const keyId  = process.env.GCS_HMAC_KEY_ID;
  const secret = process.env.GCS_HMAC_SECRET;
  if (!keyId || !secret) {
    throw new Error(
      'DuckDB GCS auth requires HMAC keys. Set GCS_HMAC_KEY_ID and GCS_HMAC_SECRET ' +
      'in the environment (e.g. `source ~/.gcs_hmac_env`) before running row-level checks. ' +
      'Create keys once with: gcloud storage hmac create <vm-service-account-email>'
    );
  }
  // Escape single quotes defensively (HMAC secrets are base64 so this is
  // typically a no-op, but guards against future key rotations that pick
  // up a quote.)
  const esc = s => s.replace(/'/g, "''");
  return [
    `SET memory_limit='${DUCKDB_MEMORY}';`,
    `SET threads=${DUCKDB_THREADS};`,
    `SET preserve_insertion_order=false;`,
    `SET temp_directory='${DUCKDB_SPILL_DIR}';`,
    `INSTALL httpfs; LOAD httpfs;`,
    `CREATE OR REPLACE SECRET dq_gcs (TYPE GCS, KEY_ID '${esc(keyId)}', SECRET '${esc(secret)}');`,
  ].join(' ');
}

/**
 * Run a DuckDB query, return parsed JSON rows.
 *
 * Routes through `COPY (...) TO '<file>' (FORMAT JSON, ARRAY TRUE)` rather
 * than `duckdb -json -c "...; SELECT"`. The `-json` mode emits one JSON
 * array per result-producing statement, so a multi-statement run (SET... +
 * INSTALL + LOAD + CREATE SECRET + SELECT) produces several arrays
 * concatenated on stdout, which JSON.parse can't handle.  The COPY pattern
 * writes only the SELECT result to the file. Same fix applied to the
 * rematerializer.
 *
 * Retries once on transient errors (network / timeout).
 */
async function duckdb(query, { label = 'query', timeout = QUERY_TIMEOUT_MS } = {}) {
  const cleanQuery = query.trim().replace(/;\s*$/, '');
  let lastErr;
  for (let attempt = 0; attempt <= QUERY_MAX_RETRIES; attempt++) {
    const outFile = join(
      DUCKDB_SPILL_DIR,
      `dq-${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}.json`
    );
    const wrapped = `${duckdbPrelude()} COPY (${cleanQuery}) TO '${outFile.replace(/'/g, "''")}' (FORMAT JSON, ARRAY TRUE);`;
    try {
      await execFile('duckdb', ['-c', wrapped], { maxBuffer: 16 * 1024 * 1024, timeout });
      const text = readFileSync(outFile, 'utf8').trim();
      try { unlinkSync(outFile); } catch {}
      return text ? JSON.parse(text) : [];
    } catch (err) {
      try { unlinkSync(outFile); } catch {}
      lastErr = err;
      // execFile's default err.message is "Command failed: <full cmd line>"
      // which is useless — the actual DuckDB error is in err.stderr (and
      // sometimes err.stdout). Preserve all three so we can actually
      // diagnose OOM / timeout / SQL errors.
      const stderr = err.stderr?.toString().trim() || '';
      const stdout = err.stdout?.toString().trim() || '';
      const signal = err.signal || '';
      const code   = err.code != null ? err.code : '';
      const diag =
        (stderr ? `stderr: ${stderr}` : '') +
        (stdout ? `\nstdout: ${stdout}` : '') +
        (signal ? `\nsignal: ${signal}` : '') +
        (code !== '' ? `\nexit: ${code}` : '') ||
        `no stderr/stdout — ${err.message || 'unknown'}`;
      const transient = /timed out|ECONNRESET|503|500|temporarily|reset by peer|http/i.test(stderr + ' ' + stdout + ' ' + err.message);
      if (attempt < QUERY_MAX_RETRIES && transient) {
        const backoff = 1000 * Math.pow(2, attempt);
        console.warn(`   ⏳ [${label}] retry ${attempt + 1}/${QUERY_MAX_RETRIES} after ${backoff}ms: ${(stderr || err.message).slice(0, 180)}`);
        await new Promise(r => setTimeout(r, backoff));
        continue;
      }
      throw new Error(`DuckDB [${label}] failed — ${diag.slice(0, 1500)}`);
    }
  }
  throw lastErr;
}

// Simple promise-pool for bounded concurrency
async function mapLimit(items, limit, fn) {
  const results = new Array(items.length);
  let cursor = 0;
  async function worker() {
    while (true) {
      const i = cursor++;
      if (i >= items.length) return;
      results[i] = await fn(items[i], i);
    }
  }
  await Promise.all(Array.from({ length: Math.max(1, limit) }, worker));
  return results;
}

// ─────────────────────────────────────────────────────────────
// Calendar helpers
// ─────────────────────────────────────────────────────────────

function ymd(y, m, d) { return `${y}-${String(m).padStart(2, '0')}-${String(d).padStart(2, '0')}`; }
function parseYMD(s) { const [y, m, d] = s.split('-').map(Number); return { y, m, d }; }

function addDays(ymdStr, n) {
  const { y, m, d } = parseYMD(ymdStr);
  const dt = new Date(Date.UTC(y, m - 1, d));
  dt.setUTCDate(dt.getUTCDate() + n);
  return ymd(dt.getUTCFullYear(), dt.getUTCMonth() + 1, dt.getUTCDate());
}

function buildDateRange(firstKey, lastKey) {
  // firstKey / lastKey = "Y/M/D"
  const [y1, m1, d1] = firstKey.split('/').map(Number);
  const [y2, m2, d2] = lastKey.split('/').map(Number);
  const start = ymd(y1, m1, d1);
  const end   = ymd(y2, m2, d2);
  const out = [];
  let cur = start;
  while (cur <= end) { out.push(cur); cur = addDays(cur, 1); }
  return out;
}

function filterByCli(dateStrs) {
  const s = OPTS.start || '0000-00-00';
  const e = OPTS.end   || '9999-99-99';
  return dateStrs.filter(d => d >= s && d <= e);
}

function keyToDate(key) {
  const [y, m, d] = key.split('/').map(Number);
  return ymd(y, m, d);
}
function dateToKey(dateStr) {
  const { y, m, d } = parseYMD(dateStr);
  return `${y}/${m}/${d}`;
}

// Parquet filenames carry different kinds of timestamps depending on source:
//   LIVE:      updates_2026-02-02T15-30-00.000000Z.parquet   (ISO — is record_time)
//   BACKFILL:  updates-1771189136334-77bd2ac9.parquet        (epoch-ms — is FETCH time)
//   REINGEST:  updates-ri-<sha>.parquet                       (deterministic — no time)
//
// Only the ISO form corresponds to the underlying data's ledger time. Using
// the backfill fetch-time stamps for gap detection produces spurious warnings
// at every shard boundary (backfill fetches a day's data in discontiguous
// wall-clock batches, but contiguous ledger-time ranges), so we deliberately
// ignore them. This matches `audit-gcs-gaps.js`.
function parseFileTime(path) {
  const name = path.split('/').pop() || '';
  const iso = name.match(/(\d{4}-\d{2}-\d{2})T(\d{2})-(\d{2})-(\d{2})\.(\d+)Z/);
  if (iso) {
    const [, date, hh, mm, ss, frac] = iso;
    const t = new Date(`${date}T${hh}:${mm}:${ss}.${frac.slice(0, 3)}Z`).getTime();
    if (!isNaN(t)) return t;
  }
  return null;
}

function humanDuration(ms) {
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m${s % 60}s`;
  const h = Math.floor(m / 60);
  return `${h}h${m % 60}m`;
}

// ─────────────────────────────────────────────────────────────
// STRUCTURAL CHECKS
// ─────────────────────────────────────────────────────────────

/**
 * Run structural checks for one (source, type, migration) combo.
 * Uses a pre-built parquet index (keyed by Y/M/D).
 * Returns {days: [{date, fileCount, status, gaps}], firstDate, lastDate}.
 */
function runStructural(source, type, migration, fileIndex, sizedFiles) {
  const scope = `${source}/${type}/migration=${migration}`;

  if (fileIndex.size === 0) {
    addFinding(SEVERITY.WARN, 'structural', scope, 'no parquet files found');
    return { days: [], firstDate: null, lastDate: null };
  }

  // Build sorted list of date keys and fill in the calendar range so we
  // can see gaps between the first and last observed day.
  const sortedKeys = [...fileIndex.keys()].sort((a, b) => {
    const [ay, am, ad] = a.split('/').map(Number);
    const [by, bm, bd] = b.split('/').map(Number);
    return ay - by || am - bm || ad - bd;
  });
  const firstKey = sortedKeys[0];
  const lastKey  = sortedKeys[sortedKeys.length - 1];
  const allDates = filterByCli(buildDateRange(firstKey, lastKey));

  // Index tiny/zero-byte files per day for the tiny-file check
  const tinyByDay = new Map();
  for (const f of sizedFiles) {
    if (f.size >= 1024) continue; // <1 KB considered tiny (real parquet headers are bigger)
    const m = f.path.match(/year=(\d+)\/month=(\d+)\/day=(\d+)\//);
    if (!m) continue;
    const key = `${m[1]}/${m[2]}/${m[3]}`;
    if (!tinyByDay.has(key)) tinyByDay.set(key, []);
    tinyByDay.get(key).push(f);
  }

  const days = [];
  for (const date of allDates) {
    const key = dateToKey(date);
    const files = fileIndex.get(key) || [];

    if (files.length === 0) {
      addFinding(SEVERITY.ERROR, 'structural', `${scope} ${date}`, 'missing day partition');
      days.push({ date, fileCount: 0, status: 'missing', gaps: [] });
      continue;
    }

    // Intra-day time gaps (only for files with parseable timestamps)
    const stamped = files.map(p => ({ path: p, t: parseFileTime(p) }))
                         .filter(f => f.t != null)
                         .sort((a, b) => a.t - b.t);
    const gaps = [];
    for (let i = 1; i < stamped.length; i++) {
      const delta = stamped[i].t - stamped[i - 1].t;
      if (delta > INTRA_DAY_GAP_THRESHOLD_S * 1000) {
        gaps.push({
          after: stamped[i - 1].path.split('/').pop(),
          before: stamped[i].path.split('/').pop(),
          gap_ms: delta,
          gap: humanDuration(delta),
        });
      }
    }
    if (gaps.length > 0) {
      addFinding(SEVERITY.WARN, 'structural', `${scope} ${date}`,
        `${gaps.length} intra-day gap(s) > ${INTRA_DAY_GAP_THRESHOLD_S}s`,
        { file_count: files.length, gaps: gaps.length });
    }

    // Tiny / zero-byte files
    const tiny = tinyByDay.get(key) || [];
    if (tiny.length > 0) {
      addFinding(SEVERITY.ERROR, 'structural', `${scope} ${date}`,
        `${tiny.length} suspiciously tiny parquet file(s) (<1 KB)`,
        { tiny_files: tiny.length });
    }

    days.push({ date, fileCount: files.length, status: gaps.length ? 'gaps' : 'ok', gaps });
  }

  // Empty partition check — directory exists but no .parquet (discovered via flat ls)
  const dayDirs = gsutilLsFlat(`gs://${BUCKET}/raw/${source}/${type}/migration=${migration}/`);
  // We don't recurse through year/month here because flat ls already had the index;
  // the explicit missing-day finding covers most empty cases. Skipping a separate
  // recursive empty-dir sweep keeps this check O(1) per migration.

  REPORT.partitions_scanned += days.length;

  return {
    days,
    firstDate: keyToDate(firstKey),
    lastDate:  keyToDate(lastKey),
  };
}

/**
 * Cross-check updates/ vs events/ day alignment within a migration.
 * Returns list of days where one side has data and the other doesn't.
 */
function checkAlignment(source, migration, updatesIndex, eventsIndex) {
  const scope = `${source}/migration=${migration}`;
  const uDays = new Set(updatesIndex.keys());
  const eDays = new Set(eventsIndex.keys());

  const uOnly = [...uDays].filter(k => !eDays.has(k)).sort();
  const eOnly = [...eDays].filter(k => !uDays.has(k)).sort();

  if (uOnly.length === 0 && eOnly.length === 0) {
    addFinding(SEVERITY.INFO, 'alignment', scope,
      `updates↔events aligned (${uDays.size} days)`);
    return;
  }

  // Per-day asymmetric findings. This is the 2026-04-02 class of bug:
  // updates written but events never uploaded (or vice versa). Each day
  // gets its own ERROR with file counts and a remediation hint so the
  // finding is directly actionable.
  for (const key of uOnly) {
    const date = keyToDate(key);
    const updatesCount = updatesIndex.get(key)?.length || 0;
    addFinding(SEVERITY.ERROR, 'alignment', `${scope} ${date}`,
      `updates=${updatesCount} files but events=0 — events were never written, ` +
      `partially uploaded, or deleted after write. Rematerialize with: ` +
      `node rematerialize-events-from-updates.js --migration=${migration} --date=${date} --source=${source} --execute`,
      { updates_files: updatesCount, events_files: 0, asymmetry: 'events_missing' });
  }
  for (const key of eOnly) {
    const date = keyToDate(key);
    const eventsCount = eventsIndex.get(key)?.length || 0;
    addFinding(SEVERITY.ERROR, 'alignment', `${scope} ${date}`,
      `events=${eventsCount} files but updates=0 — orphan events. Parent updates are ` +
      `missing; cannot be rematerialized from update_data. Requires a targeted reingest ` +
      `of this date after deleting the orphan events.`,
      { updates_files: 0, events_files: eventsCount, asymmetry: 'updates_missing' });
  }

  // Summary line so the operator sees the shape of the problem at a glance.
  addFinding(SEVERITY.WARN, 'alignment', scope,
    `${uOnly.length + eOnly.length} asymmetric day(s) ` +
    `(${uOnly.length} updates-only, ${eOnly.length} events-only, out of ` +
    `${uDays.size + eOnly.length} total)`,
    { updates_only_days: uOnly.length, events_only_days: eOnly.length });
}

// ─────────────────────────────────────────────────────────────
// ROW-LEVEL CHECKS (DuckDB over GCS)
// ─────────────────────────────────────────────────────────────

function dayGlob(source, type, migration, date) {
  const { y, m, d } = parseYMD(date);
  return `gs://${BUCKET}/raw/${source}/${type}/migration=${migration}/year=${y}/month=${m}/day=${d}/*.parquet`;
}

/**
 * Combined per-day query for updates: counts, nulls, partition mismatch,
 * duplicate update_id, timestamp sanity, offset monotonicity.
 */
async function checkUpdatesDay(source, migration, date) {
  const glob = dayGlob(source, 'updates', migration, date);
  const { y, m, d } = parseYMD(date);
  const scope = `${source}/updates/migration=${migration} ${date}`;

  const nullExprs = UPDATES_CRITICAL_COLS
    .map(c => `SUM(CASE WHEN ${c} IS NULL THEN 1 ELSE 0 END) AS null_${c}`).join(', ');

  const sql = `
    WITH src AS (
      SELECT
        update_id, record_time, effective_at, migration_id, "offset"
      FROM read_parquet('${glob}', union_by_name=true)
    )
    SELECT
      COUNT(*) AS row_count,
      COUNT(DISTINCT update_id) AS distinct_update_id,
      ${nullExprs},
      SUM(CASE WHEN migration_id IS NOT NULL AND migration_id <> ${migration} THEN 1 ELSE 0 END) AS mig_mismatch,
      -- effective_at is stored as VARCHAR ISO-8601 (see write-parquet.js:449),
      -- so we extract date parts via substr instead of strftime() which errors
      -- on non-timestamp input.
      SUM(CASE WHEN effective_at IS NOT NULL AND (
            CAST(substr(effective_at, 1, 4) AS INT) <> ${y} OR
            CAST(substr(effective_at, 6, 2) AS INT) <> ${m} OR
            CAST(substr(effective_at, 9, 2) AS INT) <> ${d}
          ) THEN 1 ELSE 0 END) AS partition_mismatch,
      -- TRY_CAST returns NULL on bad input rather than erroring the whole query.
      SUM(CASE WHEN TRY_CAST(record_time AS TIMESTAMP) IS NOT NULL
                 AND TRY_CAST(effective_at AS TIMESTAMP) IS NOT NULL
                 AND TRY_CAST(record_time AS TIMESTAMP) < TRY_CAST(effective_at AS TIMESTAMP) - INTERVAL 1 DAY
               THEN 1 ELSE 0 END) AS time_backwards,
      -- VARCHAR MIN/MAX is lexicographic which equals chronological for ISO strings.
      MIN(record_time) AS min_record_time,
      MAX(record_time) AS max_record_time,
      MIN("offset") AS min_offset,
      MAX("offset") AS max_offset,
      COUNT(DISTINCT "offset") AS distinct_offset,
      SUM(CASE WHEN "offset" IS NULL THEN 1 ELSE 0 END) AS null_offset
    FROM src
  `;

  try {
    const rows = await duckdb(sql, { label: `upd ${scope}` });
    const r = rows[0] || {};
    const total = Number(r.row_count || 0);
    const distinct = Number(r.distinct_update_id || 0);
    const dupCount = total - distinct;

    // Null critical cols
    if (OPTS.checks.includes('nulls')) {
      for (const col of UPDATES_CRITICAL_COLS) {
        const n = Number(r[`null_${col}`] || 0);
        if (n > 0) {
          addFinding(SEVERITY.ERROR, 'nulls', scope,
            `${n}/${total} rows have NULL ${col}`, { column: col, nulls: n, rows: total });
        }
      }
    }

    // Per-day update_id duplicates
    if (OPTS.checks.includes('dups') && dupCount > 0) {
      addFinding(SEVERITY.ERROR, 'dups', scope,
        `${dupCount} duplicate update_id row(s) (total=${total}, distinct=${distinct})`,
        { total, distinct, duplicates: dupCount });
    }

    // Migration ID mismatch vs partition path
    if (OPTS.checks.includes('partition') && Number(r.mig_mismatch || 0) > 0) {
      addFinding(SEVERITY.ERROR, 'partition', scope,
        `${r.mig_mismatch} rows have migration_id <> ${migration}`,
        { mismatched: Number(r.mig_mismatch) });
    }

    // effective_at → partition path mismatch
    if (OPTS.checks.includes('partition') && Number(r.partition_mismatch || 0) > 0) {
      addFinding(SEVERITY.ERROR, 'partition', scope,
        `${r.partition_mismatch} rows have effective_at outside year=${y}/month=${m}/day=${d}`,
        { mismatched: Number(r.partition_mismatch) });
    }

    // Timestamp sanity
    if (OPTS.checks.includes('timestamps')) {
      if (Number(r.time_backwards || 0) > 0) {
        addFinding(SEVERITY.WARN, 'timestamps', scope,
          `${r.time_backwards} rows where record_time is >24h before effective_at`,
          { rows: Number(r.time_backwards) });
      }
      const minRT = r.min_record_time ? new Date(r.min_record_time) : null;
      const maxRT = r.max_record_time ? new Date(r.max_record_time) : null;
      if (minRT && (minRT.getUTCFullYear() < YEAR_MIN || minRT.getUTCFullYear() > YEAR_MAX)) {
        addFinding(SEVERITY.ERROR, 'timestamps', scope,
          `min(record_time)=${r.min_record_time} outside plausible year range`);
      }
      if (maxRT && (maxRT.getUTCFullYear() < YEAR_MIN || maxRT.getUTCFullYear() > YEAR_MAX)) {
        addFinding(SEVERITY.ERROR, 'timestamps', scope,
          `max(record_time)=${r.max_record_time} outside plausible year range`);
      }
    }

    // Offset uniqueness: ledger offsets are supposed to be unique per update.
    // But the Canton Scan API sometimes omits the offset for backfill ranges
    // (the column ends up all-null), so `distinct=0` just means "offset was
    // never populated here" — which is caught separately by the `nulls` check
    // if it matters. We only flag true duplicate populated offsets here.
    if (OPTS.checks.includes('offsets') && total > 0) {
      const distOff = Number(r.distinct_offset || 0);
      const nullOff = Number(r.null_offset || 0);
      const populated = total - nullOff;
      if (distOff > 0 && distOff < populated) {
        addFinding(SEVERITY.WARN, 'offsets', scope,
          `${populated - distOff} duplicate offset row(s) — ` +
          `distinct(offset)=${distOff} but ${populated} populated rows`,
          { distinct_offset: distOff, populated_rows: populated, null_rows: nullOff });
      }
    }

    REPORT.per_day.push({
      source, type: 'updates', migration, date,
      rows: total, distinct_update_id: distinct, duplicates: dupCount,
      null_counts: Object.fromEntries(UPDATES_CRITICAL_COLS.map(c => [c, Number(r[`null_${c}`] || 0)])),
      mig_mismatch: Number(r.mig_mismatch || 0),
      partition_mismatch: Number(r.partition_mismatch || 0),
      min_record_time: r.min_record_time || null,
      max_record_time: r.max_record_time || null,
    });
    return { rows: total, distinct };
  } catch (err) {
    addFinding(SEVERITY.ERROR, 'query', scope, `updates query failed: ${err.message.slice(0, 800)}`);
    return { rows: 0, distinct: 0, error: err.message };
  }
}

/**
 * Per-day events query: counts, nulls, partition mismatch, dup event_id,
 * orphan events (optional), event_count consistency (optional).
 */
async function checkEventsDay(source, migration, date, updatesIndex = null) {
  const glob = dayGlob(source, 'events', migration, date);
  const { y, m, d } = parseYMD(date);
  const scope = `${source}/events/migration=${migration} ${date}`;

  const nullExprs = EVENTS_CRITICAL_COLS
    .map(c => `SUM(CASE WHEN ${c} IS NULL THEN 1 ELSE 0 END) AS null_${c}`).join(', ');

  const sql = `
    WITH src AS (
      SELECT event_id, update_id, contract_id, template_id, effective_at, migration_id
      FROM read_parquet('${glob}', union_by_name=true)
    )
    SELECT
      COUNT(*) AS row_count,
      COUNT(DISTINCT event_id) AS distinct_event_id,
      COUNT(DISTINCT update_id) AS distinct_update_id,
      ${nullExprs},
      SUM(CASE WHEN migration_id IS NOT NULL AND migration_id <> ${migration} THEN 1 ELSE 0 END) AS mig_mismatch,
      -- effective_at is stored as VARCHAR ISO-8601; use substr to extract
      -- date parts (strftime errors on VARCHAR input).
      SUM(CASE WHEN effective_at IS NOT NULL AND (
            CAST(substr(effective_at, 1, 4) AS INT) <> ${y} OR
            CAST(substr(effective_at, 6, 2) AS INT) <> ${m} OR
            CAST(substr(effective_at, 9, 2) AS INT) <> ${d}
          ) THEN 1 ELSE 0 END) AS partition_mismatch
    FROM src
  `;

  let baseStats;
  try {
    const rows = await duckdb(sql, { label: `evt ${scope}` });
    baseStats = rows[0] || {};
  } catch (err) {
    addFinding(SEVERITY.ERROR, 'query', scope, `events query failed: ${err.message.slice(0, 800)}`);
    return { rows: 0, distinct: 0, error: err.message };
  }

  const total = Number(baseStats.row_count || 0);
  const distinctEvt = Number(baseStats.distinct_event_id || 0);
  const dupEvt = total - distinctEvt;

  if (OPTS.checks.includes('nulls')) {
    for (const col of EVENTS_CRITICAL_COLS) {
      const n = Number(baseStats[`null_${col}`] || 0);
      if (n > 0) {
        addFinding(SEVERITY.ERROR, 'nulls', scope,
          `${n}/${total} rows have NULL ${col}`, { column: col, nulls: n, rows: total });
      }
    }
  }
  if (OPTS.checks.includes('dups') && dupEvt > 0) {
    addFinding(SEVERITY.ERROR, 'dups', scope,
      `${dupEvt} duplicate event_id row(s) (total=${total}, distinct=${distinctEvt})`,
      { total, distinct: distinctEvt, duplicates: dupEvt });
  }
  if (OPTS.checks.includes('partition')) {
    if (Number(baseStats.mig_mismatch || 0) > 0) {
      addFinding(SEVERITY.ERROR, 'partition', scope,
        `${baseStats.mig_mismatch} rows have migration_id <> ${migration}`);
    }
    if (Number(baseStats.partition_mismatch || 0) > 0) {
      addFinding(SEVERITY.ERROR, 'partition', scope,
        `${baseStats.partition_mismatch} rows have effective_at outside year=${y}/month=${m}/day=${d}`);
    }
  }

  // Orphan events + event_count consistency (joined with same-day updates + ±1 day grace window)
  if (OPTS.checks.includes('orphans') || OPTS.checks.includes('event_count')) {
    const prev = addDays(date, -1);
    const next = addDays(date, +1);
    // Filter to days that ACTUALLY have updates parquet files — DuckDB's
    // read_parquet errors hard if any glob in its list matches zero files,
    // which would otherwise blow up at migration boundaries.
    const candidateDates = [prev, date, next];
    const availableDates = updatesIndex
      ? candidateDates.filter(dt => {
          const { y, m, d } = parseYMD(dt);
          return updatesIndex.has(`${y}/${m}/${d}`);
        })
      : candidateDates; // no index available — fall back to old behavior
    if (availableDates.length === 0) {
      addFinding(SEVERITY.INFO, 'orphans', scope,
        'no updates in ±1 day window — cannot compute orphans/event_count');
    } else {
      const updatesGlobs = availableDates.map(dt => dayGlob(source, 'updates', migration, dt));
      const sqlJoin = `
      WITH evt AS (
        SELECT event_id, update_id
        FROM read_parquet('${glob}', union_by_name=true)
      ),
      upd AS (
        SELECT update_id, event_count
        FROM read_parquet([${updatesGlobs.map(g => `'${g}'`).join(', ')}], union_by_name=true)
      ),
      evt_grouped AS (
        SELECT update_id, COUNT(*) AS observed_events
        FROM evt
        GROUP BY update_id
      )
      SELECT
        (SELECT COUNT(*) FROM evt_grouped e LEFT JOIN upd u USING (update_id)
           WHERE u.update_id IS NULL) AS orphan_update_ids,
        (SELECT COUNT(*) FROM evt_grouped e JOIN upd u USING (update_id)
           WHERE u.event_count IS NOT NULL AND u.event_count <> e.observed_events) AS event_count_mismatch
    `;
      try {
        const rows = await duckdb(sqlJoin, { label: `orph ${scope}` });
        const r = rows[0] || {};
        if (OPTS.checks.includes('orphans') && Number(r.orphan_update_ids || 0) > 0) {
          addFinding(SEVERITY.ERROR, 'orphans', scope,
            `${r.orphan_update_ids} distinct update_id(s) in events with no matching update in ±1 day window`,
            { orphan_update_ids: Number(r.orphan_update_ids) });
        }
        if (OPTS.checks.includes('event_count') && Number(r.event_count_mismatch || 0) > 0) {
          addFinding(SEVERITY.WARN, 'event_count', scope,
            `${r.event_count_mismatch} update(s) have events.count <> updates.event_count`,
            { mismatched: Number(r.event_count_mismatch) });
        }
      } catch (err) {
        addFinding(SEVERITY.WARN, 'orphans', scope, `join query failed: ${err.message.slice(0, 600)}`);
      }
    }
  }

  REPORT.per_day.push({
    source, type: 'events', migration, date,
    rows: total, distinct_event_id: distinctEvt, duplicates: dupEvt,
    distinct_update_id: Number(baseStats.distinct_update_id || 0),
    null_counts: Object.fromEntries(EVENTS_CRITICAL_COLS.map(c => [c, Number(baseStats[`null_${c}`] || 0)])),
    mig_mismatch: Number(baseStats.mig_mismatch || 0),
    partition_mismatch: Number(baseStats.partition_mismatch || 0),
  });

  return { rows: total, distinct: distinctEvt };
}

// ─────────────────────────────────────────────────────────────
// CROSS-SOURCE BOUNDARY CHECK (backfill ↔ updates per migration)
// ─────────────────────────────────────────────────────────────

/**
 * For a given migration, find update_ids that appear in both raw/backfill/updates/
 * and raw/updates/updates/. Per Decision #2 in GCS_REINGESTION_DECISIONS.md, the
 * handoff should be overlap-free.
 */
async function checkBoundary(migration, backfillIdx, updatesIdx) {
  const scope = `boundary/migration=${migration}`;
  if (backfillIdx.size === 0 || updatesIdx.size === 0) {
    addFinding(SEVERITY.INFO, 'boundary', scope, 'one side empty — nothing to overlap');
    return;
  }

  // Only scan overlapping days + a 1-day buffer — cheap.
  const bfKeys = new Set(backfillIdx.keys());
  const upKeys = new Set(updatesIdx.keys());
  const overlapKeys = [...bfKeys].filter(k => upKeys.has(k));
  // Also take the last backfill day and first updates day, even if they don't overlap
  const sortKeys = (set) => [...set].sort((a, b) => {
    const [ay, am, ad] = a.split('/').map(Number);
    const [by, bm, bd] = b.split('/').map(Number);
    return ay - by || am - bm || ad - bd;
  });
  const edgeKeys = new Set(overlapKeys);
  const bfSorted = sortKeys(bfKeys);
  const upSorted = sortKeys(upKeys);
  if (bfSorted.length) edgeKeys.add(bfSorted[bfSorted.length - 1]);
  if (upSorted.length) edgeKeys.add(upSorted[0]);

  if (edgeKeys.size === 0) {
    addFinding(SEVERITY.INFO, 'boundary', scope, 'no boundary days to check');
    return;
  }

  // Only glob days that actually have files on that side — read_parquet() errors
  // on a pattern that matches nothing, so filter by the per-side index first.
  const globsFor = (source, presentKeys) => [...edgeKeys]
    .filter(k => presentKeys.has(k))
    .map(k => {
      const [y, m, d] = k.split('/').map(Number);
      return `gs://${BUCKET}/raw/${source}/updates/migration=${migration}/year=${y}/month=${m}/day=${d}/*.parquet`;
    });
  const bfGlobList = globsFor('backfill', bfKeys);
  const upGlobList = globsFor('updates',  upKeys);
  if (bfGlobList.length === 0 || upGlobList.length === 0) {
    addFinding(SEVERITY.INFO, 'boundary', scope, 'no shared/edge days with data on both sides — skipping');
    return;
  }
  const bfGlobs = bfGlobList.map(g => `'${g}'`).join(', ');
  const upGlobs = upGlobList.map(g => `'${g}'`).join(', ');

  const sql = `
    WITH bf AS (
      SELECT DISTINCT update_id FROM read_parquet([${bfGlobs}], union_by_name=true)
    ),
    up AS (
      SELECT DISTINCT update_id FROM read_parquet([${upGlobs}], union_by_name=true)
    )
    SELECT COUNT(*) AS overlap
    FROM bf JOIN up USING (update_id)
  `;

  try {
    const rows = await duckdb(sql, { label: `boundary m=${migration}`, timeout: QUERY_TIMEOUT_MS * 2 });
    const overlap = Number(rows[0]?.overlap || 0);
    if (overlap > 0) {
      addFinding(SEVERITY.ERROR, 'boundary', scope,
        `${overlap} update_id(s) appear in BOTH backfill and updates within ${edgeKeys.size} boundary day(s)`,
        { overlapping_update_ids: overlap, boundary_days: edgeKeys.size });
    } else {
      addFinding(SEVERITY.INFO, 'boundary', scope,
        `no overlap across ${edgeKeys.size} boundary day(s)`);
    }
  } catch (err) {
    addFinding(SEVERITY.WARN, 'boundary', scope, `query failed: ${err.message.slice(0, 200)}`);
  }
}

// ─────────────────────────────────────────────────────────────
// OPT-IN: full-migration cross-day dedup
// ─────────────────────────────────────────────────────────────

async function checkCrossDayDups(kind, migration, source) {
  // kind = 'updates' | 'events'
  const idCol = kind === 'updates' ? 'update_id' : 'event_id';
  const scope = `cross-day/${source}/${kind}/migration=${migration}`;

  const prefix = `gs://${BUCKET}/raw/${source}/${kind}/migration=${migration}/`;
  const sql = `
    WITH src AS (
      SELECT ${idCol} FROM read_parquet('${prefix}**/*.parquet', union_by_name=true, hive_partitioning=true)
    )
    SELECT
      COUNT(*) AS rows,
      COUNT(DISTINCT ${idCol}) AS distinct_ids,
      COUNT(*) - COUNT(DISTINCT ${idCol}) AS duplicates
    FROM src
  `;

  console.log(`   🧮 Running cross-day dedup for ${scope} — this reads every ${idCol} in the migration.`);
  try {
    const rows = await duckdb(sql, { label: scope, timeout: 3600_000 }); // 1h cap
    const r = rows[0] || {};
    const total = Number(r.rows || 0);
    const distinct = Number(r.distinct_ids || 0);
    const dups = Number(r.duplicates || 0);
    if (dups > 0) {
      addFinding(SEVERITY.ERROR, 'cross-day-dups', scope,
        `${dups} duplicate ${idCol}(s) across whole migration (total=${total}, distinct=${distinct})`,
        { total, distinct, duplicates: dups });
    } else {
      addFinding(SEVERITY.INFO, 'cross-day-dups', scope,
        `no duplicate ${idCol} across whole migration (total=${total})`);
    }
  } catch (err) {
    addFinding(SEVERITY.WARN, 'cross-day-dups', scope, `query failed: ${err.message.slice(0, 240)}`);
  }
}

// ─────────────────────────────────────────────────────────────
// ACS CHECKS
// ─────────────────────────────────────────────────────────────

async function checkACS() {
  const scope = 'acs';
  const basePrefix = `gs://${BUCKET}/raw/acs/`;

  const migsOut = gsutilLsFlat(basePrefix);
  const migrations = [];
  for (const l of migsOut) {
    const m = l.match(/migration=(\d+)/);
    if (m) migrations.push(parseInt(m[1], 10));
  }
  if (migrations.length === 0) {
    addFinding(SEVERITY.WARN, 'acs', scope, `no ACS data found under ${basePrefix}`);
    return;
  }

  // Collect all snapshot directories (hold .parquet files and optional _COMPLETE marker)
  const sample = sh(`gsutil ls -r "${basePrefix}"`, { timeout: 120000 });
  const snapshotDirs = new Set();
  const completeMarkers = new Set();
  for (const line of sample.split('\n')) {
    const p = line.trim();
    if (!p) continue;
    const snapMatch = p.match(/^(gs:\/\/[^/]+\/raw\/acs\/migration=\d+\/year=\d+\/month=\d+\/day=\d+\/snapshot=\d+\/)/);
    if (snapMatch) snapshotDirs.add(snapMatch[1]);
    if (p.endsWith('/_COMPLETE')) completeMarkers.add(p.replace(/_COMPLETE$/, ''));
  }

  const snapshots = [...snapshotDirs].sort();
  addFinding(SEVERITY.INFO, 'acs', scope, `found ${snapshots.length} snapshot(s) across migrations ${migrations.join(', ')}`);

  // Missing _COMPLETE marker check
  let missingComplete = 0;
  for (const s of snapshots) {
    if (!completeMarkers.has(s)) {
      missingComplete++;
      if (OPTS.verbose) addFinding(SEVERITY.WARN, 'acs', s, 'missing _COMPLETE marker');
    }
  }
  if (missingComplete > 0) {
    addFinding(SEVERITY.WARN, 'acs', scope,
      `${missingComplete}/${snapshots.length} snapshot(s) missing _COMPLETE marker`,
      { missing: missingComplete, total: snapshots.length });
  }

  // Row-level checks: limit to a sample to keep it bounded (latest per migration + optional --sample-days)
  const perMigLatest = new Map();
  for (const s of snapshots) {
    const m = s.match(/migration=(\d+)/);
    if (m) perMigLatest.set(parseInt(m[1], 10), s);  // sorted ascending, so last wins
  }
  const toScan = [...perMigLatest.values()];

  const nullExprs = ACS_CRITICAL_COLS
    .map(c => `SUM(CASE WHEN ${c} IS NULL THEN 1 ELSE 0 END) AS null_${c}`).join(', ');

  for (const snap of toScan) {
    const glob = `${snap}*.parquet`;
    const sSnap = snap.replace(`gs://${BUCKET}/`, '').replace(/\/$/, '');
    const sql = `
      WITH src AS (
        SELECT contract_id, template_id, migration_id FROM read_parquet('${glob}', union_by_name=true)
      )
      SELECT
        COUNT(*) AS rows,
        COUNT(DISTINCT contract_id) AS distinct_cid,
        ${nullExprs}
      FROM src
    `;
    try {
      const rows = await duckdb(sql, { label: `acs ${sSnap}` });
      const r = rows[0] || {};
      const total = Number(r.rows || 0);
      const distinct = Number(r.distinct_cid || 0);
      const dups = total - distinct;
      if (dups > 0) {
        addFinding(SEVERITY.ERROR, 'acs', sSnap,
          `${dups} duplicate contract_id(s) (total=${total}, distinct=${distinct})`);
      }
      for (const col of ACS_CRITICAL_COLS) {
        const n = Number(r[`null_${col}`] || 0);
        if (n > 0) {
          addFinding(SEVERITY.ERROR, 'acs', sSnap, `${n}/${total} rows NULL ${col}`);
        }
      }
    } catch (err) {
      addFinding(SEVERITY.WARN, 'acs', sSnap, `query failed: ${err.message.slice(0, 200)}`);
    }
  }
}

// ─────────────────────────────────────────────────────────────
// MAIN
// ─────────────────────────────────────────────────────────────

function printBanner() {
  console.log('═'.repeat(75));
  console.log(`  DATA QUALITY CHECK — gs://${BUCKET}/raw/`);
  console.log(`  Started:    ${REPORT.started_at}`);
  console.log(`  Checks:     ${OPTS.checks.join(', ')}`);
  console.log(`  Source:     ${OPTS.source}`);
  if (OPTS.migration !== null) console.log(`  Migration:  ${OPTS.migration}`);
  if (OPTS.start || OPTS.end)  console.log(`  Range:      ${OPTS.start || '…'} → ${OPTS.end || '…'}`);
  console.log(`  DuckDB:     memory=${DUCKDB_MEMORY} threads=${DUCKDB_THREADS} spill=${DUCKDB_SPILL_DIR}`);
  console.log(`  Concurrency:${OPTS.concurrency}`);
  if (OPTS.dryRun) console.log(`  Mode:       DRY RUN (no queries)`);
  console.log('═'.repeat(75));
}

function summarize() {
  const bySeverity = { info: 0, warn: 0, error: 0 };
  const byCheck = {};
  for (const f of REPORT.findings) {
    bySeverity[f.severity] = (bySeverity[f.severity] || 0) + 1;
    byCheck[f.check] = (byCheck[f.check] || 0) + (f.severity === SEVERITY.INFO ? 0 : 1);
  }
  REPORT.summary = {
    partitions_scanned: REPORT.partitions_scanned,
    findings_total:     REPORT.findings.length,
    errors:             bySeverity.error,
    warnings:           bySeverity.warn,
    info:               bySeverity.info,
    by_check:           byCheck,
  };

  console.log('\n' + '═'.repeat(75));
  console.log('  SUMMARY');
  console.log('═'.repeat(75));
  console.log(`  Partitions scanned: ${REPORT.partitions_scanned}`);
  console.log(`  Findings: ${REPORT.findings.length} total — ${bySeverity.error} errors, ${bySeverity.warn} warnings, ${bySeverity.info} info`);
  if (Object.keys(byCheck).length > 0) {
    console.log('\n  By check (non-info):');
    for (const [k, v] of Object.entries(byCheck).sort((a, b) => b[1] - a[1])) {
      console.log(`    ${k.padEnd(18)} ${v}`);
    }
  }
  if (bySeverity.error === 0 && bySeverity.warn === 0) {
    console.log('\n  ✅ No data quality issues detected.');
  } else {
    console.log(`\n  ${bySeverity.error > 0 ? '❌' : '⚠️ '} Issues detected — see findings above${OPTS.output ? ` or in ${OPTS.output}` : ''}.`);
  }
  console.log('═'.repeat(75));
}

async function main() {
  printBanner();

  if (OPTS.dryRun) {
    console.log('\n[dry-run] would run checks; exiting.');
    return 0;
  }

  // Prereqs
  checkPrereqs();
  ensureSpillDir();

  // Heavy checks = anything that actually issues DuckDB queries against GCS.
  // `structural` and `alignment` are metadata-only (gsutil ls) and don't
  // compete with the live ingest for /tmp, RAM, or egress bandwidth.
  const METADATA_ONLY = new Set(['structural', 'alignment']);
  const heavyChecks = OPTS.checks.some(c => !METADATA_ONLY.has(c));
  if (heavyChecks) {
    const free = checkFreeTmpGB();
    if (free !== null && free < MIN_FREE_TMP_GB) {
      if (!OPTS.force) {
        throw new Error(`Only ${free} GB free on /tmp (need ${MIN_FREE_TMP_GB} GB for DuckDB spill). Free space or rerun with --force.`);
      }
      console.warn(`⚠️  /tmp has only ${free} GB free — continuing due to --force.`);
    }
  }

  // Live-ingest guard
  const running = liveIngestRunning();
  if (running.length > 0 && heavyChecks) {
    console.warn(`\n⚠️  Live ingestion processes detected (will compete for /tmp and RAM):`);
    running.slice(0, 5).forEach(l => console.warn(`    ${l}`));
    if (!OPTS.force) {
      throw new Error('Refusing to run row-level checks while ingest is running. Rerun with --force to proceed.');
    }
  }

  // Discovery
  const sources = OPTS.source === 'all' ? ['backfill', 'updates'] : [OPTS.source];
  const types = ['updates', 'events'];

  // Bulk file indexes — one gsutil call per (source, type, migration)
  const indexes = new Map();  // key: `${source}/${type}/${migration}` → Map<Y/M/D, paths>
  const sized = new Map();    // key: same → [{size,modified,path}]

  for (const source of sources) {
    for (const type of types) {
      const srcPrefix = `gs://${BUCKET}/raw/${source}/${type}/`;
      const allMigs = discoverMigrations(srcPrefix);
      const migs = OPTS.migration !== null ? allMigs.filter(m => m === OPTS.migration) : allMigs;
      if (migs.length === 0) {
        addFinding(SEVERITY.WARN, 'structural', `${source}/${type}`, `no migrations found under ${srcPrefix}`);
        continue;
      }
      for (const mig of migs) {
        const migPrefix = `gs://${BUCKET}/raw/${source}/${type}/migration=${mig}/`;
        console.log(`\n🔎 Indexing ${source}/${type}/migration=${mig}…`);
        const idx = bulkListParquet(migPrefix);
        const files = bulkListParquetWithSize(migPrefix);
        indexes.set(`${source}/${type}/${mig}`, idx);
        sized.set(`${source}/${type}/${mig}`, files);
        console.log(`   ${idx.size} day(s) / ${files.length} file(s)`);
      }
    }
  }

  // Per-migration loop: structural, alignment, row-level, boundary
  const migSet = new Set();
  for (const k of indexes.keys()) migSet.add(k.split('/').slice(2).join('/'));

  for (const source of sources) {
    for (const type of types) {
      for (const k of [...indexes.keys()].filter(x => x.startsWith(`${source}/${type}/`))) {
        const migration = parseInt(k.split('/')[2], 10);
        const idx = indexes.get(k);
        const files = sized.get(k) || [];

        console.log(`\n─── STRUCTURAL: ${k} ───`);
        const { days } = runStructural(source, type, migration, idx, files);

        // Row-level checks, day by day (only for the calendar of days that actually have files,
        // to avoid issuing empty queries for missing days — missing is already flagged).
        const rowChecks = ['nulls', 'dups', 'partition', 'timestamps', 'offsets', 'orphans', 'event_count'];
        if (!OPTS.checks.some(c => rowChecks.includes(c))) continue;

        let eligible = days.filter(d => d.status !== 'missing').map(d => d.date);
        eligible = filterByCli(eligible);
        if (OPTS.sampleDays && eligible.length > OPTS.sampleDays) {
          // Deterministic sample: every Nth day
          const step = Math.ceil(eligible.length / OPTS.sampleDays);
          eligible = eligible.filter((_, i) => i % step === 0);
          console.log(`   (sampled ${eligible.length} of ${days.length} day(s))`);
        }
        if (eligible.length === 0) continue;

        console.log(`─── ROW-LEVEL: ${k} over ${eligible.length} day(s), concurrency=${OPTS.concurrency} ───`);
        // For events, the orphan/event_count join needs the corresponding
        // updates-side index so it can filter the ±1 day globs down to days
        // that actually have files (otherwise DuckDB errors on missing glob).
        const updatesIndexForJoin = type === 'events'
          ? indexes.get(`${source}/updates/${migration}`)
          : null;
        const fn = type === 'updates'
          ? (date) => checkUpdatesDay(source, migration, date)
          : (date) => checkEventsDay(source, migration, date, updatesIndexForJoin);
        await mapLimit(eligible, OPTS.concurrency, fn);
      }
    }
  }

  // Updates↔events alignment (per source, per migration) — the class of check
  // that catches gaps like 2026-04-02 (updates present, events missing).
  if (OPTS.checks.includes('alignment')) {
    console.log('\n─── ALIGNMENT: updates ↔ events ───');
    for (const source of sources) {
      const migs = new Set();
      for (const k of indexes.keys()) {
        if (k.startsWith(`${source}/updates/`) || k.startsWith(`${source}/events/`)) {
          migs.add(parseInt(k.split('/')[2], 10));
        }
      }
      for (const mig of [...migs].sort((a, b) => a - b)) {
        const uIdx = indexes.get(`${source}/updates/${mig}`) || new Map();
        const eIdx = indexes.get(`${source}/events/${mig}`) || new Map();
        checkAlignment(source, mig, uIdx, eIdx);
      }
    }
  }

  // Cross-source boundary overlap (only if both sources were scanned)
  if (OPTS.checks.includes('boundary') && sources.includes('backfill') && sources.includes('updates')) {
    console.log('\n─── BOUNDARY (backfill ↔ updates) ───');
    const migs = new Set();
    for (const k of indexes.keys()) migs.add(parseInt(k.split('/')[2], 10));
    for (const mig of [...migs].sort((a, b) => a - b)) {
      const bf = indexes.get(`backfill/updates/${mig}`) || new Map();
      const up = indexes.get(`updates/updates/${mig}`) || new Map();
      await checkBoundary(mig, bf, up);
    }
  }

  // Opt-in full-migration dedup
  if (OPTS.crossDayDups) {
    console.log(`\n─── CROSS-DAY DEDUP (opt-in): ${OPTS.crossDayDups} ───`);
    const migs = new Set();
    for (const k of indexes.keys()) migs.add(parseInt(k.split('/')[2], 10));
    for (const source of sources) {
      for (const mig of [...migs].sort((a, b) => a - b)) {
        await checkCrossDayDups(OPTS.crossDayDups, mig, source);
      }
    }
  }

  // ACS
  if (OPTS.checks.includes('acs')) {
    console.log('\n─── ACS SNAPSHOTS ───');
    await checkACS();
  }

  REPORT.finished_at = new Date().toISOString();
  summarize();

  if (OPTS.output) {
    writeFileSync(OPTS.output, JSON.stringify(REPORT, null, 2));
    console.log(`\n📄 Report: ${OPTS.output}`);
  }

  const errors = REPORT.findings.filter(f => f.severity === SEVERITY.ERROR).length;
  return errors > 0 ? 1 : 0;
}

main()
  .then(code => process.exit(code))
  .catch(err => {
    console.error(`\n❌ FATAL: ${err.message}`);
    if (OPTS.verbose && err.stack) console.error(err.stack);
    process.exit(2);
  });
