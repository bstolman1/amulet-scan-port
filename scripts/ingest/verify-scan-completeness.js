#!/usr/bin/env node
/**
 * verify-scan-completeness.js
 *
 * Strict completeness verifier: for a given (migration, day), compares
 *
 *   COUNT(DISTINCT update_id) in GCS where record_time ∈ [D, D+1) and migration_id = N
 *    vs
 *   COUNT of updates returned by the Scan API with the same filter
 *
 * Any drift is treated as an issue — the point of this tool is to provide a
 * 100% completeness guarantee, not a statistical one. Together with the DQ
 * script's no-dup / no-orphan / structural-aligned invariants, an exact
 * match is sufficient evidence that the archive and the Scan API agree.
 *
 * Design notes
 *   - Scan API pagination uses `record_time` (Canton's canonical ordering key).
 *     We filter on record_time on the GCS side too, reading ±1 day partitions
 *     to catch any records whose effective_at drifted across midnight.
 *   - On migration boundaries we filter by migration_id as well, so records
 *     belonging to other migrations in the same time window are excluded.
 *   - Endpoint failover: rotate through 13 SV Scan endpoints on error. We DO
 *     NOT round-robin by default — first healthy endpoint is used until an
 *     error triggers failover (matches fetch-updates.js behavior).
 *   - Resume support: output file is an ndjson stream (one JSON per line).
 *     On re-run with --resume, skip days already present.
 *
 * Usage examples
 *   node verify-scan-completeness.js --migration=4 --date=2026-04-02 --output=/tmp/check.ndjson
 *   node verify-scan-completeness.js --migration=4 --start=2026-03-01 --end=2026-04-20 --output=...
 *   node verify-scan-completeness.js --migration=4 --start=2025-12-16 --end=2026-04-22 --sample=30 --output=...
 *   node verify-scan-completeness.js --migration=4 --start=... --end=... --resume --output=...
 *
 * Requirements
 *   - GCS_BUCKET, GCS_HMAC_KEY_ID, GCS_HMAC_SECRET env vars
 *   - duckdb CLI
 *   - Internet access to Scan API endpoints
 */

import { execSync, execFile as execFileCb } from 'child_process';
import { promisify } from 'util';
import { existsSync, mkdirSync, unlinkSync, readFileSync, appendFileSync, writeFileSync } from 'fs';
import { createInterface } from 'readline';
import { createReadStream } from 'fs';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';
import axios from 'axios';
import http from 'http';
import https from 'https';
import dotenv from 'dotenv';

const execFile = promisify(execFileCb);
const __filename = fileURLToPath(import.meta.url);
const __dirname  = dirname(__filename);
dotenv.config({ path: join(__dirname, '.env') });

// ─────────────────────────────────────────────────────────────
// Config
// ─────────────────────────────────────────────────────────────

const BUCKET        = process.env.GCS_BUCKET || 'canton-bucket';
const HMAC_KEY_ID   = process.env.GCS_HMAC_KEY_ID;
const HMAC_SECRET   = process.env.GCS_HMAC_SECRET;

const DUCKDB_MEMORY = process.env.VSC_DUCKDB_MEMORY || '6GB';
const DUCKDB_THREADS = parseInt(process.env.VSC_DUCKDB_THREADS || '2', 10);
const TMP_DIR       = process.env.VSC_TMP_DIR || '/tmp/verify-scan';

// Page size for Scan API /v2/updates — smaller = more calls, larger = heavier per-call.
// 100 matches live ingest default; empirically stable across endpoints.
const PAGE_SIZE     = parseInt(process.env.VSC_PAGE_SIZE || '100', 10);

// Per-request timeout. Scan endpoints sometimes stall — a generous timeout
// avoids false failovers, and AbortController enforces it reliably.
const REQUEST_TIMEOUT_MS = parseInt(process.env.VSC_REQUEST_TIMEOUT_MS || '30000', 10);

// Failover: rotate to the next healthy endpoint after N consecutive errors.
const ENDPOINT_ROTATE_AFTER_ERRORS = parseInt(process.env.VSC_ROTATE_AFTER || '3', 10);

// Global retry cap for a single day before we mark it ERROR and move on.
const MAX_DAY_ATTEMPTS = parseInt(process.env.VSC_MAX_DAY_ATTEMPTS || '3', 10);

// Max consecutive fully-exhausted-all-endpoints cycles before we bail.
const MAX_COOLDOWN_CYCLES = parseInt(process.env.VSC_MAX_COOLDOWN_CYCLES || '6', 10);
const COOLDOWN_MS         = parseInt(process.env.VSC_COOLDOWN_MS || '300000', 10); // 5 min

// Scan endpoints — copied from fetch-updates.js:397-411 (stays in sync here deliberately;
// this tool is standalone so we don't drag in fetch-updates.js's heavy machinery).
const ALL_SCAN_ENDPOINTS = [
  { name: 'Global-Synchronizer-Foundation',  url: 'https://scan.sv-1.global.canton.network.sync.global/api/scan' },
  { name: 'Digital-Asset-1',                 url: 'https://scan.sv-1.global.canton.network.digitalasset.com/api/scan' },
  { name: 'Digital-Asset-2',                 url: 'https://scan.sv-2.global.canton.network.digitalasset.com/api/scan' },
  { name: 'Cumberland-1',                    url: 'https://scan.sv-1.global.canton.network.cumberland.io/api/scan' },
  { name: 'Cumberland-2',                    url: 'https://scan.sv-2.global.canton.network.cumberland.io/api/scan' },
  { name: 'Five-North-1',                    url: 'https://scan.sv-1.global.canton.network.fivenorth.io/api/scan' },
  { name: 'Tradeweb-Markets-1',              url: 'https://scan.sv-1.global.canton.network.tradeweb.com/api/scan' },
  { name: 'Proof-Group-1',                   url: 'https://scan.sv-1.global.canton.network.proofgroup.xyz/api/scan' },
  { name: 'Liberty-City-Ventures-1',         url: 'https://scan.sv-1.global.canton.network.lcv.mpch.io/api/scan' },
  { name: 'MPC-Holding-Inc',                 url: 'https://scan.sv-1.global.canton.network.mpch.io/api/scan' },
  { name: 'Orb-1-LP-1',                      url: 'https://scan.sv-1.global.canton.network.orb1lp.mpch.io/api/scan' },
  { name: 'SV-Nodeops-Limited',              url: 'https://scan.sv.global.canton.network.sv-nodeops.com/api/scan' },
  { name: 'C7-Technology-Services-Limited',  url: 'https://scan.sv-1.global.canton.network.c7.digital/api/scan' },
];

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
  migration:   argVal('migration') !== null ? parseInt(argVal('migration'), 10) : null,
  date:        argVal('date'),
  start:       argVal('start'),
  end:         argVal('end'),
  sample:      argVal('sample') !== null ? parseInt(argVal('sample'), 10) : null,
  concurrency: parseInt(argVal('concurrency', '1'), 10),
  output:      argVal('output'),
  resume:      hasFlag('resume'),
  dryRun:      hasFlag('dry-run'),
  verbose:     hasFlag('verbose') || hasFlag('v'),
  help:        hasFlag('help') || hasFlag('h'),
};

function printHelp() {
  console.log(`
verify-scan-completeness.js — compare GCS update counts to Scan API, per day.

Required:
  --migration=N        migration ID
  --output=path.ndjson  ndjson stream (one result per line — resume-friendly)

Target selection (one of):
  --date=YYYY-MM-DD
  --start=YYYY-MM-DD --end=YYYY-MM-DD
  --start / --end + --sample=N    (random sample N days from range)

Options:
  --concurrency=N      parallel days (default 1 — Scan API rate-limits to watch)
  --resume             skip days already present in --output
  --dry-run            plan only, no API or GCS calls
  --verbose, -v

Environment:
  GCS_BUCKET, GCS_HMAC_KEY_ID, GCS_HMAC_SECRET
  VSC_PAGE_SIZE, VSC_REQUEST_TIMEOUT_MS, VSC_ROTATE_AFTER, …
`);
}
if (OPTS.help) { printHelp(); process.exit(0); }

if (OPTS.migration === null || !OPTS.output) {
  console.error('ERROR: --migration and --output are required\n');
  printHelp();
  process.exit(2);
}
if (!OPTS.date && !(OPTS.start && OPTS.end)) {
  console.error('ERROR: specify either --date or both --start and --end\n');
  printHelp();
  process.exit(2);
}

// ─────────────────────────────────────────────────────────────
// Date helpers
// ─────────────────────────────────────────────────────────────

function parseYMD(s) { const [y, m, d] = s.split('-').map(Number); return { y, m, d }; }
function ymd(y, m, d) { return `${y}-${String(m).padStart(2, '0')}-${String(d).padStart(2, '0')}`; }
function addDays(dateStr, n) {
  const { y, m, d } = parseYMD(dateStr);
  const dt = new Date(Date.UTC(y, m - 1, d));
  dt.setUTCDate(dt.getUTCDate() + n);
  return ymd(dt.getUTCFullYear(), dt.getUTCMonth() + 1, dt.getUTCDate());
}
function dayBoundsISO(dateStr) {
  return {
    // start-of-day is the INCLUSIVE lower bound for the day; we pass
    // `after = dayStart - 1us` so Scan API returns records AT start-of-day.
    dayStart:   `${dateStr}T00:00:00.000000Z`,
    dayEnd:     `${addDays(dateStr, 1)}T00:00:00.000000Z`,
    afterCursor: `${dateStr}T00:00:00.000000Z`, // strictly greater cursor; see fetchCount
  };
}

function buildDateRange() {
  if (OPTS.date) return [OPTS.date];
  const dates = [];
  let cur = OPTS.start;
  while (cur <= OPTS.end) { dates.push(cur); cur = addDays(cur, 1); }
  if (OPTS.sample && dates.length > OPTS.sample) {
    // Deterministic sample: shuffle with a seed derived from migration+range
    const rng = mulberry32(hash32(`${OPTS.migration}|${OPTS.start}|${OPTS.end}|${OPTS.sample}`));
    const arr = dates.slice();
    for (let i = arr.length - 1; i > 0; i--) {
      const j = Math.floor(rng() * (i + 1));
      [arr[i], arr[j]] = [arr[j], arr[i]];
    }
    return arr.slice(0, OPTS.sample).sort();
  }
  return dates;
}

function hash32(s) { let h = 2166136261 >>> 0; for (const c of s) h = Math.imul(h ^ c.charCodeAt(0), 16777619); return h >>> 0; }
function mulberry32(a) { return function() { a |= 0; a = a + 0x6D2B79F5 | 0; let t = a; t = Math.imul(t ^ t >>> 15, t | 1); t ^= t + Math.imul(t ^ t >>> 7, t | 61); return ((t ^ t >>> 14) >>> 0) / 4294967296; }; }

// ─────────────────────────────────────────────────────────────
// Axios client + endpoint management
//
// A fresh client on each failover — per GCS_REINGESTION_DECISIONS §11:
// "Fresh HTTP Client on Failover. Recreate axios client on every endpoint
//  failover, instead of just changing baseURL. Axios reuses TCP via
//  keep-alive; a hung endpoint's stuck socket blocks new requests."
// ─────────────────────────────────────────────────────────────

const httpAgent  = new http.Agent({ keepAlive: true, maxSockets: 8 });
const httpsAgent = new https.Agent({ keepAlive: true, maxSockets: 8, rejectUnauthorized: process.env.INSECURE_TLS !== 'true' });

function createClient(baseURL) {
  return axios.create({
    baseURL,
    timeout: REQUEST_TIMEOUT_MS,  // fallback; AbortController is the primary enforcement
    httpAgent,
    httpsAgent,
    headers: { 'content-type': 'application/json' },
  });
}

let activeEndpointIdx = 0;
let activeClient = createClient(ALL_SCAN_ENDPOINTS[0].url);
const endpointHealth = ALL_SCAN_ENDPOINTS.map(() => ({ errors: 0, healthy: true }));

function currentEndpointName() { return ALL_SCAN_ENDPOINTS[activeEndpointIdx].name; }

async function probeEndpoint(ep) {
  try {
    const client = createClient(ep.url);
    await client.get('/v0/dso', { timeout: 8000 });
    return true;
  } catch {
    return false;
  }
}

async function failoverToNextHealthy(reason) {
  const startIdx = activeEndpointIdx;
  for (let step = 1; step <= ALL_SCAN_ENDPOINTS.length; step++) {
    const idx = (startIdx + step) % ALL_SCAN_ENDPOINTS.length;
    if (!endpointHealth[idx].healthy) continue;
    const ep = ALL_SCAN_ENDPOINTS[idx];
    console.warn(`  ↪  failover: ${currentEndpointName()} → ${ep.name} (reason: ${reason})`);
    activeEndpointIdx = idx;
    activeClient = createClient(ep.url);
    endpointHealth[idx].errors = 0;
    return true;
  }
  // All unhealthy — try a full re-probe.
  console.warn(`  ↪  all endpoints marked unhealthy — re-probing…`);
  const probed = await Promise.all(ALL_SCAN_ENDPOINTS.map(probeEndpoint));
  probed.forEach((ok, i) => { endpointHealth[i] = { errors: 0, healthy: ok }; });
  const firstOk = probed.indexOf(true);
  if (firstOk === -1) return false;
  activeEndpointIdx = firstOk;
  activeClient = createClient(ALL_SCAN_ENDPOINTS[firstOk].url);
  console.warn(`  ↪  using ${ALL_SCAN_ENDPOINTS[firstOk].name} after re-probe`);
  return true;
}

async function initialProbe() {
  console.log(`🔍 Probing ${ALL_SCAN_ENDPOINTS.length} Scan endpoints…`);
  const results = await Promise.all(ALL_SCAN_ENDPOINTS.map(async (ep, i) => {
    const ok = await probeEndpoint(ep);
    endpointHealth[i].healthy = ok;
    console.log(`  ${ok ? '✅' : '❌'} ${ep.name.padEnd(38)} ${ep.url}`);
    return ok;
  }));
  const firstOk = results.indexOf(true);
  if (firstOk === -1) throw new Error('All Scan endpoints unreachable');
  activeEndpointIdx = firstOk;
  activeClient = createClient(ALL_SCAN_ENDPOINTS[firstOk].url);
  console.log(`  active: ${currentEndpointName()}\n`);
}

// ─────────────────────────────────────────────────────────────
// Single /v2/updates POST with abort-based timeout + failover on error
// ─────────────────────────────────────────────────────────────

async function postUpdatesPage(afterMigrationId, afterRecordTime) {
  let cooldowns = 0;
  while (true) {
    const controller = new AbortController();
    const timeoutId  = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
    try {
      const payload = { page_size: PAGE_SIZE, daml_value_encoding: 'compact_json' };
      if (afterMigrationId !== null && afterRecordTime) {
        payload.after = { after_migration_id: afterMigrationId, after_record_time: afterRecordTime };
      }
      const resp = await activeClient.post('/v2/updates', payload, { signal: controller.signal });
      clearTimeout(timeoutId);
      // success — reset error count for this endpoint
      endpointHealth[activeEndpointIdx].errors = 0;
      return resp.data?.transactions || [];
    } catch (err) {
      clearTimeout(timeoutId);
      const isTimeout = err.name === 'AbortError' || err.code === 'ERR_CANCELED' || err.code === 'ECONNABORTED';
      const status = err.response?.status;
      // Record as error on current endpoint
      endpointHealth[activeEndpointIdx].errors += 1;
      const errLabel = isTimeout ? 'timeout' : (status ? `http_${status}` : (err.code || 'network'));
      if (endpointHealth[activeEndpointIdx].errors >= ENDPOINT_ROTATE_AFTER_ERRORS) {
        endpointHealth[activeEndpointIdx].healthy = false;
        const ok = await failoverToNextHealthy(errLabel);
        if (!ok) {
          cooldowns += 1;
          if (cooldowns > MAX_COOLDOWN_CYCLES) {
            throw new Error(`All endpoints unhealthy after ${cooldowns} cooldowns — aborting`);
          }
          console.warn(`  💤 all endpoints unhealthy — cooling down ${Math.round(COOLDOWN_MS/1000)}s before retry…`);
          await new Promise(r => setTimeout(r, COOLDOWN_MS));
          continue;
        }
      } else {
        // Small backoff on same endpoint before retry
        await new Promise(r => setTimeout(r, 500 * endpointHealth[activeEndpointIdx].errors));
      }
      // loop continues — retries on current or new endpoint
    }
  }
}

// ─────────────────────────────────────────────────────────────
// Count updates for (migration, date) via paginated Scan API
//
// Walks forward from `after = (migrationId, dayStart)` and stops when either:
//   - record_time >= dayEnd  (we've crossed into the next day), OR
//   - migration_id > target  (we've crossed into the next migration), OR
//   - empty response (reached the head)
//
// Counts only records where migration_id == target AND record_time < dayEnd.
// ─────────────────────────────────────────────────────────────

async function scanCountForDay(migrationId, dateStr) {
  const { dayEnd, afterCursor } = dayBoundsISO(dateStr);
  let afterMigrationId = migrationId;
  let afterRecordTime  = afterCursor;
  let counted = 0;
  let pages   = 0;
  let firstRT = null;
  let lastRT  = null;
  const seenIds = new Set();  // dedupe in case Scan API ever returns the same update twice in one walk

  while (true) {
    const txs = await postUpdatesPage(afterMigrationId, afterRecordTime);
    pages += 1;
    if (txs.length === 0) break;

    for (const tx of txs) {
      const mig = tx.migration_id;
      const rt  = tx.record_time;
      const uid = tx.update_id;
      if (!firstRT) firstRT = rt;
      lastRT = rt;
      // Stop condition: crossed into next migration or next day.
      if (mig > migrationId) return finalize();
      if (rt >= dayEnd)      return finalize();
      if (mig === migrationId && rt < dayEnd && uid && !seenIds.has(uid)) {
        seenIds.add(uid);
        counted += 1;
      }
    }

    // Advance cursor — use MAX record_time across the page (matches fetch-updates.js
    // correctness fix: last element isn't always the max; using max avoids rewinding).
    const maxRT = txs.reduce((m, t) => t.record_time > m ? t.record_time : m, txs[0].record_time);
    if (maxRT === afterRecordTime) {
      // No forward progress — stalled; break to avoid infinite loop.
      break;
    }
    afterRecordTime = maxRT;
    // migration_id can only go up or stay equal in paginated order; we keep passing target.
    afterMigrationId = migrationId;

    if (pages % 50 === 0) {
      console.log(`    · [${dateStr} m=${migrationId}] paged ${pages} calls, counted ${counted} so far (cursor=${afterRecordTime})`);
    }
  }

  function finalize() {
    return { count: counted, pages, firstRT, lastRT };
  }
  return finalize();
}

// ─────────────────────────────────────────────────────────────
// Count updates in GCS via DuckDB (reads ±1 day partitions to catch
// midnight-drift; filters strictly by record_time and migration_id)
// ─────────────────────────────────────────────────────────────

function sqlStr(s) { return String(s).replace(/'/g, "''"); }

// Enumerate which day-partition globs ACTUALLY have files. Avoids DuckDB's
// "No files found" error when one of the candidate globs is empty (e.g. the
// `updates/updates/` path doesn't exist in Dec 2025 because live ingest
// hadn't started yet). Cheap — 6 gsutil ls calls, ~2-3 s total per day.
function listExistingGlobs(migrationId, dateStr) {
  const candidates = [];
  for (const offset of [-1, 0, 1]) {
    const dt = addDays(dateStr, offset);
    const { y, m, d } = parseYMD(dt);
    candidates.push(`gs://${BUCKET}/raw/backfill/updates/migration=${migrationId}/year=${y}/month=${m}/day=${d}/*.parquet`);
    candidates.push(`gs://${BUCKET}/raw/updates/updates/migration=${migrationId}/year=${y}/month=${m}/day=${d}/*.parquet`);
  }
  const existing = [];
  for (const glob of candidates) {
    try {
      const out = execSync(`gsutil ls "${glob}" 2>/dev/null || true`, {
        stdio: ['ignore', 'pipe', 'ignore'],
        timeout: 30000,
        maxBuffer: 32 * 1024 * 1024,
      }).toString();
      if (out.split('\n').some(l => l.trim().endsWith('.parquet'))) {
        existing.push(glob);
      }
    } catch { /* treat as empty */ }
  }
  return existing;
}

async function gcsCountForDay(migrationId, dateStr) {
  if (!existsSync(TMP_DIR)) mkdirSync(TMP_DIR, { recursive: true });

  const globs = listExistingGlobs(migrationId, dateStr);
  if (globs.length === 0) {
    // Genuinely no files for this day in GCS — return 0, let the comparison
    // surface it as DRIFT (if Scan has rows) or MATCH (if Scan also empty).
    return 0;
  }

  const { dayStart, dayEnd } = dayBoundsISO(dateStr);
  const outFile = join(TMP_DIR, `vsc-${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}.json`);

  const sql = `
    WITH src AS (
      SELECT DISTINCT update_id
      FROM read_parquet([${globs.map(g => `'${g}'`).join(', ')}], union_by_name=true)
      WHERE record_time >= '${sqlStr(dayStart)}'
        AND record_time <  '${sqlStr(dayEnd)}'
        AND migration_id = ${migrationId}
    )
    SELECT COUNT(*)::BIGINT AS gcs_count FROM src
  `;
  const wrapped = [
    `SET memory_limit='${DUCKDB_MEMORY}';`,
    `SET threads=${DUCKDB_THREADS};`,
    `SET preserve_insertion_order=false;`,
    `SET temp_directory='${sqlStr(join(TMP_DIR, 'spill'))}';`,
    `INSTALL httpfs; LOAD httpfs;`,
    `CREATE OR REPLACE SECRET s (TYPE GCS, KEY_ID '${sqlStr(HMAC_KEY_ID)}', SECRET '${sqlStr(HMAC_SECRET)}');`,
    `COPY (${sql}) TO '${sqlStr(outFile)}' (FORMAT JSON, ARRAY TRUE);`,
  ].join(' ');

  try {
    await execFile('duckdb', ['-c', wrapped], { maxBuffer: 8 * 1024 * 1024, timeout: 900_000 });
    const text = readFileSync(outFile, 'utf8').trim();
    try { unlinkSync(outFile); } catch {}
    const rows = text ? JSON.parse(text) : [];
    return Number(rows[0]?.gcs_count || 0);
  } catch (err) {
    try { unlinkSync(outFile); } catch {}
    const stderr = err.stderr?.toString() || '';
    throw new Error(`GCS count failed for ${dateStr} m=${migrationId}: ${stderr.slice(0, 400) || err.message}`);
  }
}

// ─────────────────────────────────────────────────────────────
// Pre-flight + output management
// ─────────────────────────────────────────────────────────────

function checkPrereqs() {
  if (!BUCKET) throw new Error('GCS_BUCKET env var required');
  if (!HMAC_KEY_ID || !HMAC_SECRET) {
    throw new Error('GCS_HMAC_KEY_ID and GCS_HMAC_SECRET required — `source ~/.gcs_hmac_env`');
  }
  try { execSync('duckdb --version', { stdio: 'pipe' }); }
  catch { throw new Error('duckdb CLI not found in PATH'); }
  if (!existsSync(TMP_DIR)) mkdirSync(TMP_DIR, { recursive: true });
}

async function loadAlreadyDone(outPath) {
  const done = new Set();
  if (!existsSync(outPath)) return done;
  const rl = createInterface({ input: createReadStream(outPath), crlfDelay: Infinity });
  for await (const raw of rl) {
    const line = raw.trim();
    if (!line) continue;
    try {
      const obj = JSON.parse(line);
      if (obj.date && obj.migration != null) {
        done.add(`${obj.migration}|${obj.date}`);
      }
    } catch { /* ignore malformed lines */ }
  }
  return done;
}

function classifyResult(gcs, scan) {
  if (gcs === scan) return 'MATCH';
  return 'DRIFT';
}

function fmtResultLine(r) {
  const ico = r.status === 'MATCH' ? '✓' : r.status === 'DRIFT' ? '✗' : '⚠';
  const diffPart = r.status === 'DRIFT'
    ? `  DRIFT  gcs−scan=${r.gcs - r.scan} (${(100 * (r.gcs - r.scan) / Math.max(1, r.scan)).toFixed(4)}%)`
    : r.status === 'ERROR' ? `  ERROR: ${r.error}` : '  MATCH';
  return `  ${ico} ${r.date}  mig=${r.migration}  gcs=${r.gcs}  scan=${r.scan}${diffPart}  (${r.elapsed_s}s, ${r.pages} pages)`;
}

// ─────────────────────────────────────────────────────────────
// Concurrency pool
// ─────────────────────────────────────────────────────────────

async function mapLimit(items, limit, fn) {
  const out = new Array(items.length);
  let cursor = 0;
  async function worker() {
    while (true) {
      const i = cursor++;
      if (i >= items.length) return;
      out[i] = await fn(items[i], i);
    }
  }
  await Promise.all(Array.from({ length: Math.max(1, limit) }, worker));
  return out;
}

// ─────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────

async function main() {
  const dates = buildDateRange();

  console.log('═'.repeat(75));
  console.log(`  SCAN COMPLETENESS VERIFIER`);
  console.log(`  Migration:   ${OPTS.migration}`);
  console.log(`  Days:        ${dates.length}  (${dates[0]} → ${dates[dates.length - 1]})${OPTS.sample ? `  [sampled from range]` : ''}`);
  console.log(`  Concurrency: ${OPTS.concurrency}`);
  console.log(`  Output:      ${OPTS.output}${OPTS.resume ? '  [resume mode]' : ''}`);
  console.log(`  Scan page:   ${PAGE_SIZE}   Req timeout: ${REQUEST_TIMEOUT_MS/1000}s   Failover: ${ENDPOINT_ROTATE_AFTER_ERRORS} errors`);
  console.log('═'.repeat(75));

  if (OPTS.dryRun) {
    console.log('\n[dry-run] would verify the dates above; exiting.');
    return 0;
  }

  checkPrereqs();

  // Resume: filter out already-done days
  const already = OPTS.resume ? await loadAlreadyDone(OPTS.output) : new Set();
  const todo = dates.filter(d => !already.has(`${OPTS.migration}|${d}`));
  if (already.size > 0) {
    console.log(`\n📋 Resume: skipping ${already.size} already-done day(s); ${todo.length} remaining`);
  }
  if (todo.length === 0) {
    console.log('\n✅ Nothing to do — all days already verified.\n');
    return 0;
  }

  // Ensure output file exists (append mode below)
  if (!existsSync(OPTS.output)) writeFileSync(OPTS.output, '');

  await initialProbe();

  const startedAt = Date.now();
  console.log(`\n🏃 Running ${todo.length} day(s) at concurrency=${OPTS.concurrency}…\n`);

  let matches = 0, drifts = 0, errors = 0;

  await mapLimit(todo, OPTS.concurrency, async (date) => {
    const t0 = Date.now();
    let result;
    try {
      // Run GCS + Scan counts in parallel — they don't share resources
      const [gcsCount, scanResult] = await Promise.all([
        gcsCountForDay(OPTS.migration, date),
        scanCountForDay(OPTS.migration, date),
      ]);
      const elapsed = Math.round((Date.now() - t0) / 1000);
      const status = classifyResult(gcsCount, scanResult.count);
      result = {
        date, migration: OPTS.migration,
        gcs: gcsCount, scan: scanResult.count,
        status,
        pages: scanResult.pages,
        elapsed_s: elapsed,
        scan_first_record_time: scanResult.firstRT,
        scan_last_record_time:  scanResult.lastRT,
        endpoint_at_end: currentEndpointName(),
        ts: new Date().toISOString(),
      };
      if (status === 'MATCH') matches += 1; else drifts += 1;
    } catch (err) {
      const elapsed = Math.round((Date.now() - t0) / 1000);
      result = {
        date, migration: OPTS.migration,
        gcs: null, scan: null, status: 'ERROR',
        pages: 0, elapsed_s: elapsed,
        error: (err.message || String(err)).slice(0, 600),
        endpoint_at_end: currentEndpointName(),
        ts: new Date().toISOString(),
      };
      errors += 1;
    }
    // Stream result: append immediately so crash mid-run doesn't lose progress
    appendFileSync(OPTS.output, JSON.stringify(result) + '\n');
    console.log(fmtResultLine(result));
  });

  const totalElapsedS = Math.round((Date.now() - startedAt) / 1000);
  console.log('\n' + '═'.repeat(75));
  console.log(`  VERIFICATION SUMMARY`);
  console.log('═'.repeat(75));
  console.log(`  Total days:  ${todo.length}`);
  console.log(`  MATCH:       ${matches}`);
  console.log(`  DRIFT:       ${drifts}`);
  console.log(`  ERROR:       ${errors}`);
  console.log(`  Elapsed:     ${Math.floor(totalElapsedS/3600)}h ${Math.floor((totalElapsedS%3600)/60)}m ${totalElapsedS%60}s`);
  if (drifts === 0 && errors === 0) {
    console.log('\n  ✅ Archive and Scan API agree on every day checked.');
    return 0;
  }
  if (errors > 0) console.log(`\n  ⚠️  ${errors} day(s) errored — rerun with --resume to retry them.`);
  if (drifts > 0) console.log(`\n  ❌ ${drifts} day(s) DRIFTED — inspect ${OPTS.output} for details.`);
  return drifts > 0 ? 1 : 0;
}

main()
  .then(code => process.exit(code))
  .catch(err => {
    console.error(`\n❌ FATAL: ${err.message}`);
    if (OPTS.verbose && err.stack) console.error(err.stack);
    process.exit(2);
  });

