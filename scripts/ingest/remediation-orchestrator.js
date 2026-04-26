#!/usr/bin/env node
/**
 * Backfill Remediation Orchestrator
 *
 * Drives per-day remediation of fetch-backfill.js's ms-truncation data loss
 * (see DEPRECATED.md). For each day in the requested (migration, range):
 *
 *   1. reingest:  spawn `reingest-updates.js --start=D --end=D --migration=N --clean`.
 *                 Wait for the script's persistent cursor file to be deleted —
 *                 that's the only reliable "completed cleanly" signal, since
 *                 reingest's SIGINT handler also exits 0.
 *   2. verify:    spawn `verify-scan-completeness.js --migration=N --date=D`.
 *                 Read its output ndjson. Halt on any drift.
 *   3. cleanup:   SDK-delete `raw/backfill/{updates,events}/migration=N/.../day=D/`.
 *                 The day's data now lives only in raw/updates/, which the
 *                 verify step proved matches the Scan API.
 *
 * Halt semantics: any phase failure leaves the day in a known partial state
 * (recorded in the ndjson state file) and aborts the run. Re-running the same
 * command resumes from the next phase that hasn't completed for the next day —
 * each phase is idempotent, so a resume after a state-write gap costs at most
 * one phase's worth of duplicated work.
 *
 * No skip-on-MATCH: every day in the range is reingested, even if pre-existing
 * data already matches Scan. The point of this tool is to standardize all
 * archive data on the proven-good reingest path, not just to fill gaps.
 *
 * Usage:
 *   node remediation-orchestrator.js --migration=0 --start=2024-09-17 --end=2024-09-17
 *   node remediation-orchestrator.js --migration=4 --start=2025-12-19 --end=2025-12-19
 *   node remediation-orchestrator.js --migration=2 --start=2024-12-12 --end=2025-06-24 --max-days=5
 *   node remediation-orchestrator.js --migration=0 --start=... --end=... --dry-run
 *
 * Files written (defaults; overridable via flags):
 *   ~/remediation-m<N>.ndjson        per-phase state stream (resume-friendly)
 *   ~/remediation-m<N>.log           combined subprocess stdout/stderr
 *
 * Environment:
 *   GCS_BUCKET, GCS_HMAC_KEY_ID, GCS_HMAC_SECRET — same as verify:scan
 *   plus standard Scan API / GCS env from .env (used by the child scripts)
 */

import { spawn } from 'child_process';
import { existsSync, mkdirSync, appendFileSync, readFileSync, writeFileSync, unlinkSync, openSync, closeSync, createReadStream } from 'fs';
import { createInterface } from 'readline';
import { dirname, join, resolve } from 'path';
import { fileURLToPath } from 'url';
import { homedir } from 'os';
import dotenv from 'dotenv';

const __filename = fileURLToPath(import.meta.url);
const __dirname  = dirname(__filename);
dotenv.config({ path: join(__dirname, '.env') });

// ─────────────────────────────────────────────────────────────
// Config
// ─────────────────────────────────────────────────────────────

const BUCKET     = process.env.GCS_BUCKET || 'canton-bucket';
const CURSOR_DIR = process.env.CURSOR_DIR
  || join(process.env.DATA_DIR || '/var/lib/ledger_raw', 'cursors');

// Where verify writes its result ndjson — we want a unique path per day so a
// failed orchestrator run doesn't pollute the next day's parse. Lives under
// /tmp because it's purely transient (the authoritative record is the
// orchestrator state file).
const VERIFY_TMP_DIR = process.env.ORCH_VERIFY_TMP_DIR || '/tmp/remediation-verify';

// Child-script paths — siblings to this file
const REINGEST_SCRIPT = join(__dirname, 'reingest-updates.js');
const VERIFY_SCRIPT   = join(__dirname, 'verify-scan-completeness.js');

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
function hasFlag(name) { return process.argv.slice(2).some(a => a === `--${name}`); }

const OPTS = {
  migration:  argVal('migration') !== null ? parseInt(argVal('migration'), 10) : null,
  start:      argVal('start'),
  end:        argVal('end'),
  state:      argVal('state'),
  log:        argVal('log'),
  maxDays:    argVal('max-days') !== null ? parseInt(argVal('max-days'), 10) : null,
  dryRun:     hasFlag('dry-run'),
  verbose:    hasFlag('verbose') || hasFlag('v'),
  help:       hasFlag('help') || hasFlag('h'),
};

function printHelp() {
  console.log(`
remediation-orchestrator.js — per-day remediation of fetch-backfill.js loss.

Required:
  --migration=N            migration ID (0-4)
  --start=YYYY-MM-DD       inclusive
  --end=YYYY-MM-DD         inclusive

Options:
  --state=<path>           state ndjson (default: ~/remediation-m<N>.ndjson)
  --log=<path>             combined log (default: ~/remediation-m<N>.log)
  --max-days=N             stop after N days have completed (resumable)
  --dry-run                plan only — list days that would be processed; no subprocess work
  --verbose, -v

Per-day flow:
  reingest --clean  →  verify (must MATCH)  →  cleanup raw/backfill/<day>

Halt-on-mismatch:
  Any DRIFT after reingest, any cursor-file persistence after reingest, or any
  cleanup error exits non-zero with the day left in its halted phase. Re-run
  the same command to resume from the failed phase.

Environment:
  GCS_BUCKET, GCS_HMAC_KEY_ID, GCS_HMAC_SECRET (used by verify subprocess)
  Scan API config from scripts/ingest/.env (used by reingest subprocess)
`);
}
if (OPTS.help) { printHelp(); process.exit(0); }

if (OPTS.migration === null || !OPTS.start || !OPTS.end) {
  console.error('ERROR: --migration, --start, and --end are all required\n');
  printHelp();
  process.exit(2);
}
if (!/^\d{4}-\d{2}-\d{2}$/.test(OPTS.start) || !/^\d{4}-\d{2}-\d{2}$/.test(OPTS.end)) {
  console.error('ERROR: --start and --end must be YYYY-MM-DD\n');
  process.exit(2);
}
if (OPTS.start > OPTS.end) {
  console.error(`ERROR: --start (${OPTS.start}) is after --end (${OPTS.end})\n`);
  process.exit(2);
}

// Defaults derived from migration
OPTS.state = OPTS.state ? resolve(OPTS.state) : join(homedir(), `remediation-m${OPTS.migration}.ndjson`);
OPTS.log   = OPTS.log   ? resolve(OPTS.log)   : join(homedir(), `remediation-m${OPTS.migration}.log`);

// ─────────────────────────────────────────────────────────────
// Date helpers (local — same conventions as verify-scan)
// ─────────────────────────────────────────────────────────────

function parseYMD(s) { const [y, m, d] = s.split('-').map(Number); return { y, m, d }; }
function ymd(y, m, d) { return `${y}-${String(m).padStart(2, '0')}-${String(d).padStart(2, '0')}`; }
function addDays(dateStr, n) {
  const { y, m, d } = parseYMD(dateStr);
  const dt = new Date(Date.UTC(y, m - 1, d));
  dt.setUTCDate(dt.getUTCDate() + n);
  return ymd(dt.getUTCFullYear(), dt.getUTCMonth() + 1, dt.getUTCDate());
}
function buildDateRange(start, end) {
  const out = [];
  let cur = start;
  while (cur <= end) { out.push(cur); cur = addDays(cur, 1); }
  return out;
}

// ─────────────────────────────────────────────────────────────
// State file: ndjson, one record per (date, phase) transition
//
// Each line: {date, phase, status, ts, ...details}
//   phase: 'reingest' | 'verify' | 'cleanup'
//   status: 'ok' | 'failed'
// On resume we read every line, then for each day determine the latest
// completed phase. Phases are linear: reingest → verify → cleanup → done.
// ─────────────────────────────────────────────────────────────

const PHASES = ['reingest', 'verify', 'cleanup'];

function ensureStateFile(path) {
  if (!existsSync(path)) {
    mkdirSync(dirname(path), { recursive: true });
    closeSync(openSync(path, 'a'));
  }
}

async function loadState(path) {
  // Returns Map<date, {lastPhaseOk: string|null, lastFailed: {phase, ts, ...}|null}>
  const byDate = new Map();
  if (!existsSync(path)) return byDate;
  const rl = createInterface({ input: createReadStream(path), crlfDelay: Infinity });
  for await (const raw of rl) {
    const line = raw.trim();
    if (!line) continue;
    let rec;
    try { rec = JSON.parse(line); } catch { continue; }
    if (!rec.date || !rec.phase || !rec.status) continue;
    const e = byDate.get(rec.date) || { lastPhaseOk: null, lastFailed: null };
    if (rec.status === 'ok') {
      // Track the highest-numbered phase that completed ok.
      const idxNew = PHASES.indexOf(rec.phase);
      const idxCur = e.lastPhaseOk ? PHASES.indexOf(e.lastPhaseOk) : -1;
      if (idxNew > idxCur) e.lastPhaseOk = rec.phase;
      // A successful retry clears any prior failure on the same phase.
      if (e.lastFailed && e.lastFailed.phase === rec.phase) e.lastFailed = null;
    } else if (rec.status === 'failed') {
      e.lastFailed = { phase: rec.phase, ts: rec.ts, ...rec };
    }
    byDate.set(rec.date, e);
  }
  return byDate;
}

function nextPhaseFor(state, dateStr) {
  // Returns the next phase to run for `dateStr`, or null if the day is done.
  // A failed phase blocks progression even if a later phase coincidentally ran;
  // operator must clear the failure (re-run, which is idempotent for all
  // three phases) before progressing.
  const e = state.get(dateStr);
  if (!e) return PHASES[0];
  if (e.lastFailed) return e.lastFailed.phase;  // retry the failed phase
  if (!e.lastPhaseOk) return PHASES[0];
  const idx = PHASES.indexOf(e.lastPhaseOk);
  return idx < 0 ? PHASES[0] : (idx + 1 < PHASES.length ? PHASES[idx + 1] : null);
}

function appendState(path, rec) {
  appendFileSync(path, JSON.stringify({ ts: new Date().toISOString(), ...rec }) + '\n');
}

// ─────────────────────────────────────────────────────────────
// Combined log file — every subprocess line is prefixed with [date phase]
// so the operator can grep a single day's worth of output cleanly.
// ─────────────────────────────────────────────────────────────

function logLine(path, line) {
  appendFileSync(path, line.endsWith('\n') ? line : line + '\n');
}

function logHeader(path, dateStr, phase, msg) {
  logLine(path, `\n[${dateStr} ${phase}] ${msg}`);
}

// ─────────────────────────────────────────────────────────────
// Cleanup: SDK-based delete of raw/backfill/{updates,events}/.../day=D/
//
// The SDK is used (not gsutil) for the same reason reingest-updates.js uses
// it: gsutil silently fails when ADC expires, whereas the SDK surfaces the
// error. Each `getFiles({prefix})` enumerates the day partition exhaustively;
// `Promise.all(... .delete())` blasts them in parallel.
// ─────────────────────────────────────────────────────────────

let _gcsBucket = null;
async function getBucket() {
  if (_gcsBucket) return _gcsBucket;
  const { Storage } = await import('@google-cloud/storage');
  _gcsBucket = new Storage().bucket(BUCKET);
  return _gcsBucket;
}

async function deletePrefix(prefix) {
  // Returns {count, errors:[]}. Treats per-file 404 as success (idempotent).
  const bucket = await getBucket();
  const [files] = await bucket.getFiles({ prefix });
  if (files.length === 0) return { count: 0, errors: [] };
  const errors = [];
  await Promise.all(files.map(async (f) => {
    try { await f.delete({ ignoreNotFound: true }); }
    catch (err) { errors.push({ name: f.name, error: err.message }); }
  }));
  return { count: files.length - errors.length, errors };
}

async function cleanupBackfillDay(migration, dateStr, logPath) {
  const { y, m, d } = parseYMD(dateStr);
  const prefixes = [
    `raw/backfill/updates/migration=${migration}/year=${y}/month=${m}/day=${d}/`,
    `raw/backfill/events/migration=${migration}/year=${y}/month=${m}/day=${d}/`,
  ];
  let totalDeleted = 0;
  const allErrors = [];
  for (const prefix of prefixes) {
    logHeader(logPath, dateStr, 'cleanup', `deleting gs://${BUCKET}/${prefix}`);
    const { count, errors } = await deletePrefix(prefix);
    logLine(logPath, `  deleted ${count} files` + (errors.length ? `, ${errors.length} errors` : ''));
    totalDeleted += count;
    for (const e of errors) {
      logLine(logPath, `  ERROR: ${e.name}: ${e.error}`);
      allErrors.push(e);
    }
  }
  return { deleted: totalDeleted, errors: allErrors };
}

// ─────────────────────────────────────────────────────────────
// Subprocess runners (TODO — next commit)
// Per-day driver + outer loop (TODO — next commit)
// ─────────────────────────────────────────────────────────────

async function main() {
  const dates = buildDateRange(OPTS.start, OPTS.end);

  console.log('═'.repeat(75));
  console.log(`  REMEDIATION ORCHESTRATOR`);
  console.log(`  Migration:   ${OPTS.migration}`);
  console.log(`  Days:        ${dates.length}  (${dates[0]} → ${dates[dates.length - 1]})`);
  console.log(`  State:       ${OPTS.state}`);
  console.log(`  Log:         ${OPTS.log}`);
  console.log(`  Max days:    ${OPTS.maxDays || '(no limit)'}`);
  console.log(`  Mode:        ${OPTS.dryRun ? 'DRY RUN' : 'EXECUTE'}`);
  console.log('═'.repeat(75));

  ensureStateFile(OPTS.state);
  ensureStateFile(OPTS.log);
  if (!existsSync(VERIFY_TMP_DIR)) mkdirSync(VERIFY_TMP_DIR, { recursive: true });

  const state = await loadState(OPTS.state);

  // Build the per-day plan
  const plan = [];
  for (const date of dates) {
    const next = nextPhaseFor(state, date);
    const failure = state.get(date)?.lastFailed || null;
    plan.push({ date, nextPhase: next, hasFailure: !!failure });
  }

  const todo = plan.filter(p => p.nextPhase !== null);
  const done = plan.filter(p => p.nextPhase === null);
  console.log(`\nPlan: ${todo.length} day(s) to process, ${done.length} already complete.`);
  if (todo.length === 0) {
    console.log('\n✅ Nothing to do — all days in range are already done.\n');
    return 0;
  }
  for (const p of todo.slice(0, 10)) {
    const tag = p.hasFailure ? `RETRY ${p.nextPhase}` : `next: ${p.nextPhase}`;
    console.log(`  ${p.date}  ${tag}`);
  }
  if (todo.length > 10) console.log(`  … and ${todo.length - 10} more`);

  if (OPTS.dryRun) {
    console.log('\n[dry-run] no subprocess work performed; exiting.\n');
    return 0;
  }

  // TODO (next commit): per-day driver + outer loop
  console.error('\nERROR: subprocess runners not yet implemented in this commit.\n');
  return 3;
}

main()
  .then(code => process.exit(code))
  .catch(err => {
    console.error(`\n❌ FATAL: ${err.message}`);
    if (OPTS.verbose && err.stack) console.error(err.stack);
    process.exit(2);
  });
