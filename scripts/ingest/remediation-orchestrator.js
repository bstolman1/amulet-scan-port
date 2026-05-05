#!/usr/bin/env node
/**
 * Backfill Remediation Orchestrator
 *
 * Drives per-day remediation of fetch-backfill.js's ms-truncation data loss
 * (see DEPRECATED.md). For each day in the requested (migration, range):
 *
 *   1. reingest:  spawn `reingest-updates.js --start=D --end=D --migration=N --clean
 *                              --after=DT00:00:00.000000Z`.
 *                 `--after` at midnight forces the walk to start from the
 *                 beginning of the day, bypassing findBackfillBoundary(). Without
 *                 it, boundary days (the last day of a migration's backfill) would
 *                 start mid-day at the backfill cursor, dropping the first half.
 *                 Wait for the script's persistent cursor file to be deleted —
 *                 that's the only reliable "completed cleanly" signal, since
 *                 reingest's SIGINT handler also exits 0.
 *   2. verify:    spawn `verify-scan-completeness.js --migration=N --date=D --scope=updates`.
 *                 --scope=updates is load-bearing: verify reads ONLY raw/updates/<day>,
 *                 not the union with raw/backfill/. A MATCH proves raw/updates/ alone
 *                 is self-sufficient before cleanup destroys raw/backfill/. Halt on drift.
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
  resetDay:   argVal('reset-day'),  // YYYY-MM-DD — erase all state for this day so it re-runs from scratch
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
  --reset-day=YYYY-MM-DD   erase all state entries for this day so it re-runs from reingest;
                           exits immediately after rewriting the state file
  --verbose, -v

Per-day flow:
  reingest --clean --after=<day>T00:00:00Z  →  verify (must MATCH)  →  cleanup raw/backfill/<day>

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
// --reset-day: erase all state entries for a specific day so the
// orchestrator re-runs it from reingest on the next invocation.
// Operates on the state file and exits immediately.
// ─────────────────────────────────────────────────────────────

if (OPTS.resetDay) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(OPTS.resetDay)) {
    console.error(`ERROR: --reset-day must be YYYY-MM-DD, got: ${OPTS.resetDay}`);
    process.exit(2);
  }
  if (!existsSync(OPTS.state)) {
    console.log(`State file not found: ${OPTS.state}`);
    console.log('Nothing to reset.');
    process.exit(0);
  }
  const original = readFileSync(OPTS.state, 'utf8');
  const kept = original.split('\n').filter(line => {
    const t = line.trim();
    if (!t) return false;
    try { return JSON.parse(t).date !== OPTS.resetDay; } catch { return true; }
  });
  writeFileSync(OPTS.state, kept.map(l => l + '\n').join(''));
  const removed = original.split('\n').filter(l => l.trim()).length - kept.length;
  console.log(`Removed ${removed} state line(s) for ${OPTS.resetDay} from ${OPTS.state}`);
  console.log(`Re-run without --reset-day to process from reingest.`);
  process.exit(0);
}

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
  const bucket = await getBucket();

  // Defense-in-depth preflight: refuse cleanup if raw/updates/<day> is empty
  // on either type. Verify can pass with raw/updates/ empty when raw/backfill/
  // alone has the full record set (verify reads the union via union_by_name).
  // Deleting raw/backfill/ in that case would destroy the only copy. The
  // ~50 ms cost per day is negligible relative to the safety it buys.
  for (const type of ['updates', 'events']) {
    const updatesPrefix = `raw/updates/${type}/migration=${migration}/year=${y}/month=${m}/day=${d}/`;
    const [files] = await bucket.getFiles({ prefix: updatesPrefix });
    if (files.length === 0) {
      logHeader(logPath, dateStr, 'cleanup',
        `REFUSING — preflight: gs://${BUCKET}/${updatesPrefix} has 0 files ` +
        `(reingest cursor was cleared but no ${type} landed)`);
      return { deleted: 0, errors: [{ name: '<preflight>', error: `raw/updates/${type}/<day> empty after reingest` }] };
    }
  }

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
// Subprocess runner — spawns a child Node script, streams stdout/stderr
// line-by-line into the orchestrator log with a [date phase] prefix, and
// resolves to {code, signal}. Never rejects on non-zero exit; the caller
// inspects the returned code to decide success vs failure.
// ─────────────────────────────────────────────────────────────

function spawnWithLog(cmd, args, dateStr, phase, logPath) {
  return new Promise((resolveP, rejectP) => {
    const child = spawn(cmd, args, {
      stdio: ['ignore', 'pipe', 'pipe'],
      env: { ...process.env },
    });
    const prefix    = `[${dateStr} ${phase}]`;
    const errPrefix = `[${dateStr} ${phase}!]`;
    const outRl = createInterface({ input: child.stdout, crlfDelay: Infinity });
    const errRl = createInterface({ input: child.stderr, crlfDelay: Infinity });
    outRl.on('line', (l) => logLine(logPath, `${prefix} ${l}`));
    errRl.on('line', (l) => logLine(logPath, `${errPrefix} ${l}`));
    child.on('error', rejectP);
    child.on('exit', (code, signal) => resolveP({ code, signal }));
  });
}

// ─────────────────────────────────────────────────────────────
// Reingest phase
//
// Cursor-file path mirrors reingestCursorPath() in reingest-updates.js:
//   ${CURSOR_DIR}/reingest-${migration}-${start}-${end}.json   (dates unpadded YYYYMMDD)
//
// Resume detection: if the cursor file exists when we start, a prior run
// crashed mid-day and reingest's safety guard refuses --clean. Drop --clean
// in that case so reingest auto-resumes from the saved cursor.
//
// Success requires BOTH:
//   - subprocess exit code 0
//   - cursor file absent after exit
// reingest's gracefulShutdown saves cursor and exits 0 on SIGINT/SIGTERM,
// so exit 0 alone isn't enough.
// ─────────────────────────────────────────────────────────────

function reingestCursorPath(migration, startStr, endStr) {
  const s = startStr.replace(/-/g, '');
  const e = endStr.replace(/-/g, '');
  return join(CURSOR_DIR, `reingest-${migration}-${s}-${e}.json`);
}

async function runReingest(migration, dateStr, logPath) {
  const cursorPath = reingestCursorPath(migration, dateStr, dateStr);
  const resuming   = existsSync(cursorPath);

  const args = [
    '--max-old-space-size=8192',
    REINGEST_SCRIPT,
    `--start=${dateStr}`,
    `--end=${dateStr}`,
    `--migration=${migration}`,
  ];
  if (!resuming) {
    args.push('--clean');
    // Force the walk to start from midnight even when a backfill boundary cursor
    // exists mid-day (e.g. the last day of a migration's backfill). Without this,
    // reingest's priority order picks up findBackfillBoundary() and uses it as
    // rangeStart, fetching only the tail of the day. --after at 00:00:00 has the
    // same effect as the default start-of-day cursor for non-boundary days, so
    // this is safe to pass unconditionally.
    // --clean wipes raw/updates/<day> and deletes the cursor file first, so the
    // validateSafeResume check (triggered by --after) sees symmetric empty state
    // and passes. Priority in reingest: saved cursor > --after > backfill boundary.
    args.push(`--after=${dateStr}T00:00:00.000000Z`);
  } else {
    // On resume, backfill data coexists with partial updates data (cleanup hasn't
    // run yet). Reingest's overlap check would reject this — --force bypasses it.
    args.push('--force');
  }

  logHeader(logPath, dateStr, 'reingest',
    `starting (${resuming ? 'RESUME from cursor' : 'fresh --clean'}) — cmd: node ${args.map(a => a.replace(REINGEST_SCRIPT, 'reingest-updates.js')).join(' ')}`);

  const t0 = Date.now();
  let res;
  try {
    res = await spawnWithLog('node', args, dateStr, 'reingest', logPath);
  } catch (err) {
    logHeader(logPath, dateStr, 'reingest', `subprocess spawn error: ${err.message}`);
    return { ok: false, reason: 'spawn_error', details: { error: err.message } };
  }
  const elapsed_s = Math.round((Date.now() - t0) / 1000);

  const cursorAfter = existsSync(cursorPath);
  if (res.code === 0 && !cursorAfter) {
    logHeader(logPath, dateStr, 'reingest', `OK (exit=0, cursor cleared) — ${elapsed_s}s`);
    return { ok: true, details: { elapsed_s, was_resume: resuming } };
  }
  // Anything else is failure.
  const reason = res.signal              ? `signal_${res.signal}`
               : (res.code !== 0)        ? `exit_${res.code}`
               : cursorAfter             ? 'cursor_persists'
               :                           'unknown';
  logHeader(logPath, dateStr, 'reingest',
    `FAILED (exit=${res.code}, signal=${res.signal || 'none'}, cursor_present=${cursorAfter}) — ${elapsed_s}s`);
  return {
    ok: false,
    reason,
    details: { exit: res.code, signal: res.signal, cursor_present: cursorAfter, elapsed_s, was_resume: resuming },
  };
}

// ─────────────────────────────────────────────────────────────
// Verify phase
//
// Runs verify-scan-completeness.js with --output=<temp>, then reads the
// single ndjson result line for this day. Authoritative outcome is the
// `status` field of the result, not the subprocess exit code:
//   MATCH → ok; DRIFT → halt; ERROR → halt; missing line → halt.
// ─────────────────────────────────────────────────────────────

function verifyOutputPath(migration, dateStr) {
  return join(VERIFY_TMP_DIR, `m${migration}-${dateStr}.ndjson`);
}

async function runVerify(migration, dateStr, logPath) {
  if (!existsSync(VERIFY_TMP_DIR)) mkdirSync(VERIFY_TMP_DIR, { recursive: true });
  const outFile = verifyOutputPath(migration, dateStr);
  // Always start fresh — old content from a prior failed run would parse first.
  try { if (existsSync(outFile)) unlinkSync(outFile); } catch {}

  const args = [
    '--max-old-space-size=4096',
    VERIFY_SCRIPT,
    `--migration=${migration}`,
    `--date=${dateStr}`,
    `--output=${outFile}`,
    // verify's default scope is 'updates' — reads only raw/updates/<day>.
    `--scope=updates`,
  ];
  logHeader(logPath, dateStr, 'verify', `starting — scope=updates output=${outFile}`);

  const t0 = Date.now();
  let res;
  try {
    res = await spawnWithLog('node', args, dateStr, 'verify', logPath);
  } catch (err) {
    logHeader(logPath, dateStr, 'verify', `subprocess spawn error: ${err.message}`);
    return { ok: false, reason: 'spawn_error', details: { error: err.message } };
  }
  const elapsed_s = Math.round((Date.now() - t0) / 1000);

  // Parse the result ndjson — should have exactly one line for our day.
  let parsed = null;
  if (existsSync(outFile)) {
    const text = readFileSync(outFile, 'utf8');
    for (const line of text.split('\n')) {
      const t = line.trim();
      if (!t) continue;
      try {
        const r = JSON.parse(t);
        if (r.date === dateStr && r.migration === migration) { parsed = r; break; }
      } catch { /* ignore */ }
    }
  }

  if (!parsed) {
    logHeader(logPath, dateStr, 'verify',
      `FAILED — no result line in ${outFile} (exit=${res.code}, signal=${res.signal || 'none'}) — ${elapsed_s}s`);
    return {
      ok: false,
      reason: 'no_result',
      details: { exit: res.code, signal: res.signal, elapsed_s },
    };
  }

  const summary = `gcs=${parsed.gcs} scan=${parsed.scan} status=${parsed.status}`;
  logHeader(logPath, dateStr, 'verify',
    parsed.status === 'MATCH'
      ? `OK ${summary} — ${elapsed_s}s`
      : `FAILED ${summary} — ${elapsed_s}s (exit=${res.code})`);

  if (parsed.status === 'MATCH') {
    return { ok: true, details: { gcs: parsed.gcs, scan: parsed.scan, elapsed_s } };
  }
  return {
    ok: false,
    reason: parsed.status === 'DRIFT' ? 'drift' : 'verify_error',
    details: { gcs: parsed.gcs, scan: parsed.scan, status: parsed.status, elapsed_s, error: parsed.error },
  };
}

// ─────────────────────────────────────────────────────────────
// Per-day driver — runs phases starting from `startPhase`, appends a state
// record after each one, and stops at the first failure. Returns
// {ok: true} on full success, or {ok: false, phase, reason, details} when
// a phase fails (in which case the orchestrator halts the whole run).
//
// Each phase is idempotent so a state-write gap is recoverable: re-running
// the same command at most repeats the last unrecorded phase.
// ─────────────────────────────────────────────────────────────

async function processDay(migration, dateStr, startPhase, logPath, statePath) {
  const startIdx = PHASES.indexOf(startPhase);
  if (startIdx < 0) {
    return { ok: false, phase: 'driver', reason: 'unknown_start_phase', details: { startPhase } };
  }

  const stats = { gcs: null, scan: null, deleted: null };

  for (let i = startIdx; i < PHASES.length; i++) {
    const phase = PHASES[i];
    let result;
    try {
      if (phase === 'reingest') {
        result = await runReingest(migration, dateStr, logPath);
      } else if (phase === 'verify') {
        result = await runVerify(migration, dateStr, logPath);
        if (result.ok) {
          stats.gcs  = result.details.gcs;
          stats.scan = result.details.scan;
        }
      } else if (phase === 'cleanup') {
        const r = await cleanupBackfillDay(migration, dateStr, logPath);
        result = {
          ok: r.errors.length === 0,
          reason: r.errors.length ? 'cleanup_errors' : undefined,
          details: { deleted: r.deleted, errors: r.errors },
        };
        if (result.ok) stats.deleted = r.deleted;
      } else {
        result = { ok: false, reason: 'unknown_phase', details: { phase } };
      }
    } catch (err) {
      result = { ok: false, reason: 'exception', details: { error: err.message } };
    }

    appendState(statePath, {
      date:      dateStr,
      migration,
      phase,
      status:    result.ok ? 'ok' : 'failed',
      reason:    result.reason,
      ...result.details,
    });

    if (!result.ok) {
      return { ok: false, phase, reason: result.reason, details: result.details };
    }
  }

  return { ok: true, stats };
}

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

  // Marker line in the log so a multi-run log file is easy to navigate.
  logLine(OPTS.log,
    `\n══ orchestrator run starting ${new Date().toISOString()} ` +
    `mig=${OPTS.migration} ${OPTS.start}..${OPTS.end} ` +
    `todo=${todo.length} max-days=${OPTS.maxDays || '∞'} ══`);

  let processed = 0;
  let halted = null;
  const completed = [];  // {date, gcs, scan, deleted, elapsed_s}
  const overallStart = Date.now();

  for (const p of todo) {
    if (OPTS.maxDays && processed >= OPTS.maxDays) {
      console.log(`\n  Reached --max-days=${OPTS.maxDays}; stopping (resumable on re-run).`);
      break;
    }
    const tag = p.hasFailure ? `RETRY at ${p.nextPhase}` : `from ${p.nextPhase}`;
    console.log(`\n──── ${p.date} (${tag}) ────`);
    const t0 = Date.now();
    const r = await processDay(OPTS.migration, p.date, p.nextPhase, OPTS.log, OPTS.state);
    const elapsed_s = Math.round((Date.now() - t0) / 1000);
    processed += 1;

    if (!r.ok) {
      halted = { date: p.date, ...r, elapsed_s };
      console.error(`❌ HALTED  ${p.date}  phase=${r.phase}  reason=${r.reason}  (${elapsed_s}s)`);
      console.error(`   See ${OPTS.log} for subprocess output.`);
      console.error(`   Re-run the same command to retry from this phase (each phase is idempotent).`);
      break;
    }
    completed.push({ date: p.date, elapsed_s, ...r.stats });
    console.log(`✅ DONE    ${p.date}  (${elapsed_s}s)`);
  }

  const totalElapsedS = Math.round((Date.now() - overallStart) / 1000);
  const totalUpdates  = completed.reduce((s, d) => s + (d.scan || 0), 0);
  const totalDeleted  = completed.reduce((s, d) => s + (d.deleted || 0), 0);

  console.log('\n' + '═'.repeat(75));
  console.log(`  REMEDIATION ${halted ? 'HALTED' : 'RUN COMPLETE'}`);
  console.log('═'.repeat(75));
  console.log(`  Migration:       ${OPTS.migration}`);
  console.log(`  Days processed:  ${processed}  (${completed.length} completed, ${done.length} previously done)`);
  console.log(`  Updates verified: ${totalUpdates.toLocaleString()}  (all MATCH)`);
  console.log(`  Backfill deleted: ${totalDeleted.toLocaleString()} files`);
  console.log(`  Elapsed:         ${Math.floor(totalElapsedS/3600)}h ${Math.floor((totalElapsedS%3600)/60)}m ${totalElapsedS%60}s`);
  console.log(`  Avg per day:     ${completed.length ? Math.round(totalElapsedS / completed.length) : 0}s`);
  console.log(`  State:           ${OPTS.state}`);
  console.log(`  Log:             ${OPTS.log}`);
  if (halted) {
    console.log(`  Halted at:       ${halted.date}  phase=${halted.phase}  reason=${halted.reason}`);
  }

  if (completed.length > 0 && !halted) {
    console.log('\n  Per-day results:');
    console.log('  date          updates   backfill-deleted  time');
    console.log('  ──────────    ───────   ────────────────  ────');
    for (const d of completed) {
      const u = d.scan != null ? String(d.scan).padStart(7) : '      ?';
      const del = d.deleted != null ? String(d.deleted).padStart(16) : '               ?';
      console.log(`  ${d.date}    ${u}   ${del}  ${d.elapsed_s}s`);
    }
  }

  console.log('═'.repeat(75) + '\n');

  return halted ? 1 : 0;
}

main()
  .then(code => process.exit(code))
  .catch(err => {
    console.error(`\n❌ FATAL: ${err.message}`);
    if (OPTS.verbose && err.stack) console.error(err.stack);
    process.exit(2);
  });
