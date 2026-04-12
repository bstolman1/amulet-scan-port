#!/usr/bin/env node
/**
 * Targeted Re-Ingestion Script for raw/updates/ Data
 *
 * Carefully re-fetches data from the Scan API for a specific date range and
 * writes it to canton-bucket/raw/updates/{updates,events}/ in GCS.
 *
 * Designed to fix gaps and corrupted data in the updates partition without
 * touching the backfill partition or introducing duplicates.
 *
 * SAFETY FEATURES — EXACTLY-ONCE SEMANTICS:
 * 1. Verifies the target date range does NOT overlap with backfill data
 * 2. Optionally deletes existing bad data in the target range before writing
 * 3. DETERMINISTIC FILENAMES: Each batch writes Parquet files with filenames
 *    derived from sha256(cursorPosition + partition). Same cursor → same API
 *    response → same records → same filename → GCS overwrite (not new file)
 *    → zero duplicates. No downstream dedup needed.
 * 4. PER-BATCH WRITES: Each API batch is written to Parquet and uploaded to
 *    GCS synchronously before the cursor advances. No async buffering.
 * 5. Dry-run mode to preview what would happen
 * 6. Persistent cursor: progress is saved to disk after every batch.
 *    On restart (crash, SIGINT, SIGTERM), re-running the same command
 *    auto-resumes from where it stopped — no manual --after needed.
 * 7. Graceful shutdown: SIGINT/SIGTERM saves the cursor before exiting.
 *    Since each batch is fully written to GCS before cursor advances,
 *    no buffered data can be lost.
 * 8. ZERO GAPS: Cursor is saved AFTER successful GCS upload. If crash
 *    occurs mid-batch, restart re-fetches the same batch → same deterministic
 *    filename → overwrites in GCS → no gap, no dup.
 *
 * Usage:
 *   # Preview what would be re-ingested (dry run)
 *   node reingest-updates.js --start=2026-03-03 --end=2026-03-21 --dry-run
 *
 *   # Re-ingest March 3-21 data, cleaning existing updates data first
 *   node reingest-updates.js --start=2026-03-03 --end=2026-03-21 --clean
 *
 *   # Full clean re-ingest: also remove partial backfill data, start from scratch
 *   node reingest-updates.js --start=2026-03-03 --end=2026-04-09 --migration=4 --clean-backfill
 *
 *   # Re-ingest only March 20-21 (missing events)
 *   node reingest-updates.js --start=2026-03-20 --end=2026-03-21 --clean
 *
 *   # Re-ingest specific migration only
 *   node reingest-updates.js --start=2026-03-03 --end=2026-03-21 --migration=4 --clean
 *
 *   # Resume from a specific cursor after interruption (e.g. disk full)
 *   node reingest-updates.js --start=2026-03-03 --end=2026-03-30 --migration=4 --after=2026-03-03T18:12:09.953575Z
 *
 *   # Check what exists in backfill vs updates for overlap detection
 *   node reingest-updates.js --start=2026-03-03 --end=2026-03-21 --audit-only
 *
 * Environment: Same .env as fetch-updates.js (GCS_BUCKET, SCAN_URL, etc.)
 */

import dotenv from 'dotenv';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { promisify } from 'util';
import { execFile as execFileCb, exec as execCb } from 'child_process';
import { existsSync, mkdirSync, readFileSync, readdirSync, writeFileSync, unlinkSync } from 'fs';
import axios from 'axios';
import https from 'https';
import { createHash } from 'crypto';
import { atomicWriteFile } from './atomic-cursor.js';

const execFileAsync = promisify(execFileCb);
const execAsync = promisify(execCb);

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
dotenv.config({ path: join(__dirname, '.env') });

import { normalizeUpdate, normalizeEvent, flattenEventsInTreeOrder, groupByPartition } from './data-schema.js';
import { mapUpdateRecord, mapEventRecord } from './write-parquet.js';

// ─── CLI args ─────────────────────────────────────────────────────────────
const args = process.argv.slice(2);

function argVal(name) {
  const idx = args.findIndex(a => a === `--${name}` || a.startsWith(`--${name}=`));
  if (idx === -1) return null;
  const arg = args[idx];
  if (arg.includes('=')) return arg.split('=').slice(1).join('=');
  return (idx + 1 < args.length && !args[idx + 1].startsWith('--')) ? args[idx + 1] : null;
}

const START_DATE = argVal('start');
const END_DATE = argVal('end');
const TARGET_MIGRATION = argVal('migration') ? parseInt(argVal('migration')) : null;
const RESUME_AFTER = argVal('after'); // Resume from cursor, e.g. --after=2026-03-03T18:12:09.953575Z
const DRY_RUN = args.includes('--dry-run');
const CLEAN = args.includes('--clean');
const CLEAN_BACKFILL = args.includes('--clean-backfill');
const AUDIT_ONLY = args.includes('--audit-only');
const VERBOSE = args.includes('--verbose') || args.includes('-v');
const FORCE = args.includes('--force');
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 1000;

const GCS_BUCKET = process.env.GCS_BUCKET;
const SCAN_URL = process.env.SCAN_URL || 'https://scan.sv-1.global.canton.network.sync.global/api/scan';
const ENDPOINT_ROTATE_AFTER_ERRORS = parseInt(process.env.ENDPOINT_ROTATE_AFTER_ERRORS) || 3;

// ─── Persistent reingest cursor ──────────────────────────────────────────
// Prevents duplicates on restart: on crash, the cursor records exactly where
// we stopped so re-running the same command auto-resumes from that point.
// Cursor file is keyed to (start, end, migration) so concurrent reingest
// runs for different ranges don't collide.

const CURSOR_DIR = process.env.CURSOR_DIR || join(process.env.DATA_DIR || '/var/lib/ledger_raw', 'cursors');

function reingestCursorPath(migrationId) {
  const s = START_DATE.replace(/-/g, '');
  const e = END_DATE.replace(/-/g, '');
  return join(CURSOR_DIR, `reingest-${migrationId}-${s}-${e}.json`);
}

function loadReingestCursor(migrationId) {
  const p = reingestCursorPath(migrationId);
  if (!existsSync(p)) return null;
  try {
    const data = JSON.parse(readFileSync(p, 'utf8'));
    if (data.start_date !== START_DATE || data.end_date !== END_DATE) return null;
    if (data.migration_id !== migrationId) return null;
    return data;
  } catch {
    return null;
  }
}

function saveReingestCursor(migrationId, afterRecordTime, afterMigrationId, stats) {
  mkdirSync(CURSOR_DIR, { recursive: true });
  atomicWriteFile(reingestCursorPath(migrationId), {
    migration_id:       migrationId,
    after_record_time:  afterRecordTime,
    after_migration_id: afterMigrationId,
    start_date:         START_DATE,
    end_date:           END_DATE,
    updates_written:    stats.updates,
    events_written:     stats.events,
    batches_processed:  stats.batches,
    updated_at:         new Date().toISOString(),
  });
}

function deleteReingestCursor(migrationId) {
  const p = reingestCursorPath(migrationId);
  try { unlinkSync(p); } catch {}
}

// Track in-flight state for graceful shutdown
let _shutdownState = null;

if (!START_DATE || !END_DATE) {
  console.error('Usage: node reingest-updates.js --start=YYYY-MM-DD --end=YYYY-MM-DD [--clean] [--dry-run] [--audit-only]');
  process.exit(1);
}

if (!GCS_BUCKET) {
  console.error('GCS_BUCKET environment variable is required');
  process.exit(1);
}

const INSECURE_TLS = process.env.INSECURE_TLS === 'true';
const FETCH_TIMEOUT_MS = parseInt(process.env.FETCH_TIMEOUT_MS) || 30000;

// Adaptive fetch parameters for stuck cursors (mirrors fetch-updates.js FIX #13).
// When the same cursor times out repeatedly, the API is likely struggling with
// a large/complex response at that position. Progressively reduce page_size
// and increase timeout to break through.
let _adaptivePageSize  = BATCH_SIZE;       // current effective page_size
let _adaptiveTimeoutMs = FETCH_TIMEOUT_MS; // current effective timeout
let _stuckCursor       = null;             // cursor value that is repeatedly failing
let _stuckCursorHits   = 0;                // consecutive errors at the same cursor

// ─── Multi-node failover (mirrors fetch-updates.js) ─────────────────────
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

let activeScanUrl = SCAN_URL;
let activeEndpointName = ALL_SCAN_ENDPOINTS.find(e => e.url === SCAN_URL)?.name || 'Custom';

const client = axios.create({
  baseURL: activeScanUrl,
  timeout: FETCH_TIMEOUT_MS,
  httpsAgent: new https.Agent({ rejectUnauthorized: !INSECURE_TLS }),
});

/**
 * Probe all endpoints and switch to the fastest healthy one.
 */
async function tryFailover() {
  const candidates = ALL_SCAN_ENDPOINTS.filter(ep => ep.url !== activeScanUrl);
  if (candidates.length === 0) return false;

  console.log(`   🔄 Probing ${candidates.length} alternative Scan endpoints...`);
  const results = await Promise.allSettled(
    candidates.map(async (ep) => {
      const start = Date.now();
      try {
        const resp = await axios.get(`${ep.url}/v0/dso`, {
          timeout: 10000,
          httpsAgent: new https.Agent({ rejectUnauthorized: !INSECURE_TLS }),
          headers: { Accept: 'application/json' },
        });
        if (resp.status >= 200 && resp.status < 300) {
          return { name: ep.name, url: ep.url, latencyMs: Date.now() - start };
        }
        return null;
      } catch { return null; }
    })
  );

  const healthy = results
    .filter(r => r.status === 'fulfilled' && r.value !== null)
    .map(r => r.value)
    .sort((a, b) => a.latencyMs - b.latencyMs);

  if (healthy.length > 0) {
    const best = healthy[0];
    console.log(`   ✅ Switching to ${best.name} (${best.latencyMs}ms) — ${healthy.length} healthy endpoints found`);
    activeScanUrl = best.url;
    activeEndpointName = best.name;
    client.defaults.baseURL = best.url;
    return true;
  }
  console.log(`   ⚠️  No healthy alternative endpoints found`);
  return false;
}

/**
 * Probe all configured endpoints once at startup and switch to the fastest healthy
 * one if the configured endpoint is unreachable. Mirrors fetch-updates.js
 * probeAllScanEndpoints — prevents starting the run on a dead endpoint.
 */
async function probeAllScanEndpoints() {
  console.log(`\n🔍 Probing ${ALL_SCAN_ENDPOINTS.length} Scan API endpoints (GET /v0/dso)...`);
  const results = await Promise.allSettled(
    ALL_SCAN_ENDPOINTS.map(async (ep) => {
      const start = Date.now();
      try {
        const resp = await axios.get(`${ep.url}/v0/dso`, {
          timeout: 10000,
          httpsAgent: new https.Agent({ rejectUnauthorized: !INSECURE_TLS }),
          headers: { Accept: 'application/json' },
        });
        if (resp.status >= 200 && resp.status < 300) {
          return { name: ep.name, url: ep.url, healthy: true, latencyMs: Date.now() - start };
        }
        return { name: ep.name, url: ep.url, healthy: false, latencyMs: Date.now() - start };
      } catch (err) {
        return { name: ep.name, url: ep.url, healthy: false, error: err.code || err.message, latencyMs: Date.now() - start };
      }
    })
  );

  const healthy = results
    .filter(r => r.status === 'fulfilled' && r.value.healthy)
    .map(r => r.value)
    .sort((a, b) => a.latencyMs - b.latencyMs);

  for (const r of results) {
    const ep = r.status === 'fulfilled' ? r.value : { name: '?', healthy: false, error: 'rejected', latencyMs: 0 };
    const icon = ep.healthy ? '✅' : '❌';
    const active = ep.name === activeEndpointName ? ' ← ACTIVE' : '';
    const detail = ep.healthy ? `${ep.latencyMs}ms` : `${ep.error || 'unhealthy'} (${ep.latencyMs}ms)`;
    console.log(`   ${icon}  ${ep.name} — ${detail}${active}`);
  }
  console.log(`   ${healthy.length}/${ALL_SCAN_ENDPOINTS.length} endpoints reachable`);

  if (healthy.length === 0) {
    console.error(`\n🔴 No Scan API endpoints are reachable! Check network/DNS.`);
    return;
  }
  // If active endpoint is not healthy, auto-failover to fastest reachable
  const activeHealthy = healthy.some(ep => ep.url === activeScanUrl);
  if (!activeHealthy) {
    const best = healthy[0];
    console.log(`\n⚠️  Active endpoint "${activeEndpointName}" is NOT reachable.`);
    console.log(`   🔄 Auto-failover to ${best.name} (${best.latencyMs}ms)`);
    activeScanUrl = best.url;
    activeEndpointName = best.name;
    client.defaults.baseURL = best.url;
  }
}

/**
 * Fetch a batch from the Scan API with AbortController-based timeout.
 * Uses the adaptive page_size and timeout for stuck cursors.
 * Returns { items, lastMigrationId, lastRecordTime }.
 * Throws an Error with code 'FETCH_TIMEOUT' on abort.
 *
 * Mirrors fetch-updates.js fetchUpdates() — this gives reingest the same
 * abort-on-timeout safety as the live pipeline (axios timeout alone is
 * unreliable when the server holds the connection open).
 */
async function fetchUpdatesAPI(afterMigrationId, afterRecordTime) {
  const effectiveTimeout  = _adaptiveTimeoutMs;
  const effectivePageSize = _adaptivePageSize;

  const controller = new AbortController();
  const timeoutId  = setTimeout(() => controller.abort(), effectiveTimeout);

  try {
    const payload = {
      page_size: effectivePageSize,
      daml_value_encoding: 'compact_json',
      after: { after_migration_id: afterMigrationId, after_record_time: afterRecordTime },
    };

    const response = await client.post('/v2/updates', payload, { signal: controller.signal });
    const transactions = response.data?.transactions || [];

    return {
      items:           transactions,
      lastMigrationId: transactions.length > 0 ? transactions[transactions.length - 1].migration_id : null,
      // Use MAX record_time across the batch (consistent with fetch-updates.js)
      lastRecordTime: transactions.length > 0
        ? transactions.reduce((max, tx) => tx.record_time > max ? tx.record_time : max, transactions[0].record_time)
        : null,
    };
  } catch (err) {
    if (err.name === 'AbortError' || err.code === 'ERR_CANCELED') {
      const e = new Error(`Fetch timed out after ${effectiveTimeout}ms (page_size=${effectivePageSize})`);
      e.code = 'FETCH_TIMEOUT';
      throw e;
    }
    throw err;
  } finally {
    clearTimeout(timeoutId);
  }
}

// ─── GCS helpers (SDK-based — gsutil silently fails when auth expires) ────

/**
 * List .parquet files under a GCS prefix using the SDK.
 * Returns array of full GCS paths like 'raw/updates/updates/migration=4/year=2026/month=3/day=3/file.parquet'
 */
async function gcsListParquet(prefix) {
  try {
    const bucket = await getGCSBucket();
    // prefix should NOT include gs://bucket/ — just the object path prefix
    const cleanPrefix = prefix
      .replace(`gs://${GCS_BUCKET}/`, '')
      .replace(/^\/*/, '');
    const [files] = await bucket.getFiles({ prefix: cleanPrefix });
    return files
      .map(f => f.name)
      .filter(name => name.endsWith('.parquet'));
  } catch (err) {
    console.error(`   ❌ GCS list failed for ${prefix}: ${err.message}`);
    return [];
  }
}

/**
 * Delete all objects under a GCS prefix using the SDK.
 * Returns number of files deleted.
 */
async function gcsDeletePrefix(prefix) {
  try {
    const bucket = await getGCSBucket();
    const cleanPrefix = prefix
      .replace(`gs://${GCS_BUCKET}/`, '')
      .replace(/^\/*/, '');
    const [files] = await bucket.getFiles({ prefix: cleanPrefix });
    if (files.length === 0) return 0;
    await Promise.all(files.map(f => f.delete()));
    return files.length;
  } catch (err) {
    console.error(`   ❌ GCS delete failed for ${prefix}: ${err.message}`);
    return 0;
  }
}

// ─── Date range helpers ───────────────────────────────────────────────────

function dateRange(startStr, endStr) {
  const dates = [];
  const start = new Date(startStr + 'T00:00:00Z');
  const end = new Date(endStr + 'T00:00:00Z');
  for (let d = new Date(start); d <= end; d.setUTCDate(d.getUTCDate() + 1)) {
    // Partition paths use UNPADDED numeric values (e.g. month=3, day=5)
    // to match getUtcPartition() which returns raw integers
    const year = d.getUTCFullYear();
    const month = d.getUTCMonth() + 1;
    const day = d.getUTCDate();
    dates.push({
      dateStr: `${year}-${month}-${day}`,  // unpadded key for matching bulkListGCS
      displayStr: d.toISOString().split('T')[0],  // padded for display
      year,
      month,
      day,
    });
  }
  return dates;
}

function dayPartitionPath(source, type, migrationId, year, month, day) {
  // Partition paths use unpadded values to match getUtcPartition()
  return `gs://${GCS_BUCKET}/raw/${source}/${type}/migration=${migrationId}/year=${year}/month=${month}/day=${day}/`;
}

// ─── Bulk GCS listing (one call per migration+source+type) ────────────────

async function bulkListGCS(source, type, migrationId) {
  const prefix = `raw/${source}/${type}/migration=${migrationId}/`;
  const files = await gcsListParquet(prefix);
  // Parse day from each file path: .../year=YYYY/month=M/day=D/...
  // Keys use unpadded values to match dateRange() dateStr format
  const byDay = {};
  for (const f of files) {
    const m = f.match(/year=(\d+)\/month=(\d+)\/day=(\d+)/);
    if (m) {
      const key = `${parseInt(m[1])}-${parseInt(m[2])}-${parseInt(m[3])}`;
      byDay[key] = (byDay[key] || 0) + 1;
    }
  }
  return byDay;
}

// ─── Audit: Check what exists ─────────────────────────────────────────────

async function auditDateRange(dates, migrations) {
  console.log('\n' + '═'.repeat(80));
  console.log('📊 AUDIT: Checking existing data in GCS');
  console.log('═'.repeat(80));

  for (const source of ['backfill', 'updates']) {
    console.log(`\n── ${source.toUpperCase()} ──`);
    for (const mig of migrations) {
      console.log(`  Migration ${mig}:`);
      // Bulk list: 2 calls instead of 2 * numDays
      const [updatesMap, eventsMap] = await Promise.all([
        bulkListGCS(source, 'updates', mig),
        bulkListGCS(source, 'events', mig),
      ]);

      let hasAny = false;
      for (const { dateStr, displayStr } of dates) {
        const updatesCount = updatesMap[dateStr] || 0;
        const eventsCount = eventsMap[dateStr] || 0;

        if (updatesCount > 0 || eventsCount > 0) {
          hasAny = true;
          const status = updatesCount > 0 && eventsCount > 0 ? '✅' :
                         updatesCount > 0 && eventsCount === 0 ? '⚠️  EVENTS MISSING' :
                         '⚠️  UPDATES MISSING';
          console.log(`    ${displayStr}: ${status}  (${updatesCount} update files, ${eventsCount} event files)`);
        } else if (VERBOSE) {
          console.log(`    ${displayStr}: ❌ NO DATA`);
        }
      }
      if (!hasAny) console.log('    (no data in this date range)');
    }
  }
}

// ─── Check for backfill overlap ───────────────────────────────────────────

async function checkBackfillOverlap(dates, migrations) {
  console.log('\n🔍 Checking for backfill/updates overlap...');
  const overlaps = [];

  for (const mig of migrations) {
    const [bUpdatesMap, bEventsMap] = await Promise.all([
      bulkListGCS('backfill', 'updates', mig),
      bulkListGCS('backfill', 'events', mig),
    ]);
    const [uUpdatesMap, uEventsMap] = await Promise.all([
      bulkListGCS('updates', 'updates', mig),
      bulkListGCS('updates', 'events', mig),
    ]);

    for (const { dateStr, displayStr } of dates) {
      const backfillHas = (bUpdatesMap[dateStr] || 0) + (bEventsMap[dateStr] || 0);
      const updatesHas = (uUpdatesMap[dateStr] || 0) + (uEventsMap[dateStr] || 0);

      if (backfillHas > 0 && updatesHas > 0) {
        overlaps.push({ dateStr: displayStr, migration: mig,
          backfillUpdates: bUpdatesMap[dateStr] || 0,
          backfillEvents: bEventsMap[dateStr] || 0,
        });
      }
    }
  }

  if (overlaps.length > 0) {
    console.log('\n⚠️  OVERLAP DETECTED: The following dates have data in BOTH backfill and updates:');
    for (const o of overlaps) {
      console.log(`   ${o.dateStr} (migration=${o.migration}): backfill has ${o.backfillUpdates} update files, ${o.backfillEvents} event files`);
    }
    console.log('\n   These dates already have backfill data. Re-ingesting into updates/ would create duplicates.');
    console.log('   Options:');
    console.log('   1. Adjust --start to skip dates that already have complete backfill data');
    console.log('   2. Use --force to proceed anyway (downstream dedup must handle it)');
    console.log('   3. Delete the backfill data for these dates first if updates data should replace it');

    if (!FORCE) {
      console.log('\n   Use --force to override this safety check.');
      return false;
    }
    console.log('\n   --force specified, proceeding despite overlap.');
  } else {
    console.log('   ✅ No overlap with backfill data.');
  }
  return true;
}

// ─── Clean existing bad data ──────────────────────────────────────────────

async function cleanUpdatesData(dates, migrations) {
  // Sources to clean: always 'updates', and optionally 'backfill' when
  // --clean-backfill is given (e.g. to remove partial backfill data before
  // a full re-ingestion that replaces it).
  const sourcesToClean = ['updates'];
  if (CLEAN_BACKFILL) sourcesToClean.push('backfill');

  console.log(`\n🗑️  CLEANING existing data for target date range (sources: ${sourcesToClean.join(', ')})...`);
  let totalDeleted = 0;

  for (const source of sourcesToClean) {
    for (const mig of migrations) {
      // Bulk list to find which days have data
      const [updatesMap, eventsMap] = await Promise.all([
        bulkListGCS(source, 'updates', mig),
        bulkListGCS(source, 'events', mig),
      ]);

      for (const { dateStr, year, month, day } of dates) {
        for (const [type, countMap] of [['updates', updatesMap], ['events', eventsMap]]) {
          const count = countMap[dateStr] || 0;
          if (count > 0) {
            const gcsPrefix = `raw/${source}/${type}/migration=${mig}/year=${year}/month=${month}/day=${day}/`;
            if (DRY_RUN) {
              console.log(`   [DRY RUN] Would delete ${count} ${source}/${type} files at ${gcsPrefix}`);
            } else {
              console.log(`   Deleting ${count} ${source}/${type} files at ${gcsPrefix}...`);
              const deleted = await gcsDeletePrefix(gcsPrefix);
              totalDeleted += deleted;
            }
          }
        }
      }
    }
  }

  if (DRY_RUN) {
    console.log('   [DRY RUN] No files actually deleted.');
  } else {
    console.log(`   ✅ Deleted ${totalDeleted} files total.`);
  }
}

// ─── Discover migrations ──────────────────────────────────────────────────

async function discoverMigrations() {
  if (TARGET_MIGRATION !== null) return [TARGET_MIGRATION];

  console.log('🔍 Discovering migrations from Scan API...');
  const migrations = [];
  const startMig = parseInt(process.env.START_MIGRATION) || 0;
  const endMig = parseInt(process.env.END_MIGRATION) || 4;

  for (let id = startMig; id <= endMig; id++) {
    try {
      const res = await client.post('/v0/backfilling/migration-info', { migration_id: id });
      if (res.data) {
        migrations.push(id);
        if (VERBOSE) {
          console.log(`   Migration ${id}: found`);
        }
      }
    } catch (err) {
      if (VERBOSE) {
        console.log(`   Migration ${id}: not found (${err.response?.status || err.message})`);
      }
    }
  }

  console.log(`   Found migrations: [${migrations.join(', ')}]`);
  return migrations;
}

// ─── Find backfill boundary ───────────────────────────────────────────────

async function findBackfillBoundary(migrationId) {
  // Strategy 1: Check local cursor files
  const CURSOR_DIR = process.env.CURSOR_DIR || join(process.env.DATA_DIR || '/home/ben/ledger_data', 'cursors');
  console.log(`   🔍 Looking for backfill cursors in ${CURSOR_DIR}...`);

  let bestTime = null;
  try {
    if (existsSync(CURSOR_DIR)) {
      const files = readdirSync(CURSOR_DIR).filter(f => f.endsWith('.json'));
      for (const file of files) {
        try {
          const cursor = JSON.parse(readFileSync(join(CURSOR_DIR, file), 'utf8'));
          if (cursor.migration_id === migrationId || file.includes(`cursor-${migrationId}`)) {
            const maxTime = cursor.max_time || cursor.last_confirmed_before;
            if (maxTime && (!bestTime || new Date(maxTime) > new Date(bestTime))) {
              bestTime = maxTime;
              console.log(`   📍 Cursor ${file}: max_time=${maxTime}`);
            }
          }
        } catch {}
      }
    }
  } catch (err) {
    console.log(`   ⚠️  Could not read local cursors: ${err.message}`);
  }

  // Strategy 2: Check GCS cursor backup
  try {
    const { stdout } = await execAsync(
      `gsutil cat "gs://${GCS_BUCKET}/cursors/cursor-${migrationId}-*.json" 2>/dev/null || true`,
      { timeout: 15000 }
    );
    if (stdout.trim()) {
      try {
        const cursor = JSON.parse(stdout.trim());
        const maxTime = cursor.max_time || cursor.last_confirmed_before;
        if (maxTime && (!bestTime || new Date(maxTime) > new Date(bestTime))) {
          bestTime = maxTime;
          console.log(`   📍 GCS cursor: max_time=${maxTime}`);
        }
      } catch {}
    }
  } catch {}

  // Strategy 3: Check GCS live cursor
  try {
    const { stdout } = await execAsync(
      `gsutil cat "gs://${GCS_BUCKET}/cursors/live-cursor.json" 2>/dev/null || true`,
      { timeout: 15000 }
    );
    if (stdout.trim()) {
      try {
        const cursor = JSON.parse(stdout.trim());
        const rt = cursor.record_time;
        if (rt) {
          console.log(`   📍 Live cursor: record_time=${rt}, migration=${cursor.migration_id}`);
        }
      } catch {}
    }
  } catch {}

  return bestTime;
}

// ─── Deterministic write path (exactly-once semantics) ───────────────────
// Instead of buffering records across batches and writing with random filenames
// (which creates duplicates on restart), this path writes each API batch directly
// to GCS with deterministic filenames derived from the cursor position.
//
// Guarantees:
//   Same cursor position → same API response → same records → same filename
//   → GCS overwrite (not new file) → zero duplicates
//   Cursor saved AFTER GCS upload → zero gaps

// DuckDB SQL path escaper — doubles single quotes to prevent SQL injection
function sqlStr(rawPath) {
  return rawPath.replace(/'/g, "''");
}

// DuckDB column definitions — must match write-parquet.js writeToParquetCLI exactly
const UPDATES_DUCKDB_COLUMNS = [
  "update_id: 'VARCHAR'", "update_type: 'VARCHAR'", "synchronizer_id: 'VARCHAR'",
  "effective_at: 'VARCHAR'", "recorded_at: 'VARCHAR'", "record_time: 'VARCHAR'",
  "timestamp: 'VARCHAR'", "command_id: 'VARCHAR'", "workflow_id: 'VARCHAR'", "kind: 'VARCHAR'",
  "migration_id: 'BIGINT'", '"offset": \'BIGINT\'', "event_count: 'INTEGER'",
  "root_event_ids: 'VARCHAR[]'", "source_synchronizer: 'VARCHAR'",
  "target_synchronizer: 'VARCHAR'", "unassign_id: 'VARCHAR'", "submitter: 'VARCHAR'",
  "reassignment_counter: 'BIGINT'", "trace_context: 'VARCHAR'", "update_data: 'VARCHAR'",
].join(', ');

const EVENTS_DUCKDB_COLUMNS = [
  "event_id: 'VARCHAR'", "update_id: 'VARCHAR'", "event_type: 'VARCHAR'",
  "event_type_original: 'VARCHAR'", "synchronizer_id: 'VARCHAR'", "effective_at: 'VARCHAR'",
  "recorded_at: 'VARCHAR'", "created_at_ts: 'VARCHAR'", "timestamp: 'VARCHAR'",
  "contract_id: 'VARCHAR'", "template_id: 'VARCHAR'", "package_name: 'VARCHAR'",
  "migration_id: 'BIGINT'", "signatories: 'VARCHAR[]'", "observers: 'VARCHAR[]'",
  "acting_parties: 'VARCHAR[]'", "witness_parties: 'VARCHAR[]'", "child_event_ids: 'VARCHAR[]'",
  "consuming: 'BOOLEAN'", "reassignment_counter: 'BIGINT'", "choice: 'VARCHAR'",
  "interface_id: 'VARCHAR'", "source_synchronizer: 'VARCHAR'", "target_synchronizer: 'VARCHAR'",
  "unassign_id: 'VARCHAR'", "submitter: 'VARCHAR'", "payload: 'VARCHAR'",
  "contract_key: 'VARCHAR'", "exercise_result: 'VARCHAR'", "raw_event: 'VARCHAR'",
  "trace_context: 'VARCHAR'",
].join(', ');

const REINGEST_TMP_DIR = '/tmp/reingest';

// DuckDB temp directory for spilling intermediate state to disk when the
// in-memory 256 MiB cap is tight. Without this, `:memory:` databases can't
// spill and a wide batch will OOM the COPY.
const DUCKDB_SPILL_DIR = join(REINGEST_TMP_DIR, 'duckdb_spill');

// Maximum JSONL payload size handed to a single DuckDB invocation. Above
// this, the partition's records are split into multiple Parquet chunks
// (deterministic byte-based greedy packing). Sized to leave comfortable
// headroom under the 256 MiB DuckDB memory limit: DuckDB's working-set
// inflation on wide VARCHAR rows is roughly 3-4x the source JSONL, so
// 20 MiB JSONL → ~60-80 MiB RAM + writer buffers + engine overhead
// stays well under 244 MiB.
const MAX_JSONL_BYTES_PER_CHUNK = 20 * 1024 * 1024; // 20 MiB

// Largest single JSON object DuckDB will accept. Must be ≥ the biggest
// individual record we ever see — a single oversized record gets its own
// chunk, and that chunk's one line has to fit inside this buffer.
// 48 MiB is enormous for any real ledger record while still saving 16 MiB
// of static allocation vs the original 64 MiB setting.
const DUCKDB_MAX_OBJECT_SIZE = 48 * 1024 * 1024; // 48 MiB

// ─── GCS SDK client (uses VM service account / ADC, not gsutil user creds) ──
let _gcsStorage = null;
let _gcsBucket = null;

async function getGCSBucket() {
  if (_gcsBucket) return _gcsBucket;
  const { Storage } = await import('@google-cloud/storage');
  _gcsStorage = new Storage();
  _gcsBucket = _gcsStorage.bucket(GCS_BUCKET);
  return _gcsBucket;
}

async function uploadFileToGCS(localPath, gcsObjectPath) {
  const bucket = await getGCSBucket();
  await bucket.upload(localPath, {
    destination: gcsObjectPath,
    metadata: { contentType: 'application/octet-stream' },
  });
}

/**
 * Deterministic filename based on cursor position and partition.
 * Same cursor → same data → same filename → GCS overwrite → no dup.
 *
 * When a single-partition batch exceeds MAX_JSONL_BYTES_PER_CHUNK, it is
 * split into multiple Parquet chunks. Each chunk gets a `-c{i}of{N}`
 * suffix. Because the chunk decision is purely a function of the input
 * records' serialized bytes (and the API returns the same records for the
 * same cursor), the split is reproducible across retries → GCS overwrite
 * semantics still hold.
 *
 * @param {string} type            - 'updates' or 'events'
 * @param {string} afterRecordTime - Cursor position that produced this batch
 * @param {string} partition       - Partition path (e.g. 'updates/updates/migration=4/year=2026/month=3/day=5')
 * @param {number} chunkIdx        - 0-indexed chunk number (only used when chunkCount > 1)
 * @param {number} chunkCount      - Total number of chunks for this partition
 * @returns {string} Deterministic filename like 'updates-ri-a1b2c3d4e5f6a7b8.parquet'
 *                   or 'updates-ri-a1b2c3d4e5f6a7b8-c0of3.parquet' when chunked
 */
function deterministicFileName(type, afterRecordTime, partition, chunkIdx = 0, chunkCount = 1) {
  const hash = createHash('sha256')
    .update(`${afterRecordTime}|${partition}`)
    .digest('hex')
    .slice(0, 16);
  return chunkCount > 1
    ? `${type}-ri-${hash}-c${chunkIdx}of${chunkCount}.parquet`
    : `${type}-ri-${hash}.parquet`;
}

/**
 * Run DuckDB CLI to convert a single JSONL file to a single Parquet file.
 *
 * Memory knobs (tuned for 256 MiB cap on memory-constrained VMs):
 *   * memory_limit='256MB'
 *   * threads=1
 *   * preserve_insertion_order=false — lets DuckDB stream rows through the
 *     pipeline without holding the whole input in RAM just to preserve order.
 *     Safe here because downstream consumers query by column, not row index,
 *     and the per-file row order does not affect our deterministic-filename
 *     exactly-once semantics.
 *   * temp_directory — DuckDB can spill intermediate state to disk when
 *     memory is tight. Without this, `:memory:` databases have nowhere to
 *     spill and a wide COPY will OOM.
 *   * maximum_object_size=48 MiB — single-JSON-object read buffer (was 64
 *     MiB). Must be ≥ the biggest individual record we see, because a
 *     record oversized beyond MAX_JSONL_BYTES_PER_CHUNK gets its own chunk
 *     and that one line must fit inside this buffer.
 *   * ROW_GROUP_SIZE 5000 — more frequent row-group flushes keep the
 *     Parquet writer buffer small. Combined with byte-based chunking,
 *     the per-chunk row-group footprint is bounded by the chunk size.
 */
async function jsonlToParquetViaDuckDB(jsonlPath, parquetPath, sqlFilePath, type) {
  const columns = type === 'events' ? EVENTS_DUCKDB_COLUMNS : UPDATES_DUCKDB_COLUMNS;
  mkdirSync(DUCKDB_SPILL_DIR, { recursive: true });
  const sql = [
    "SET memory_limit='256MB';",
    "SET threads=1;",
    "SET preserve_insertion_order=false;",
    `SET temp_directory='${sqlStr(DUCKDB_SPILL_DIR)}';`,
    `COPY (SELECT * FROM read_json_auto('${sqlStr(jsonlPath)}', columns={${columns}}, union_by_name=true, maximum_object_size=${DUCKDB_MAX_OBJECT_SIZE}))`,
    `TO '${sqlStr(parquetPath)}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 5000);`,
  ].join('\n');
  writeFileSync(sqlFilePath, sql);

  await execAsync(`duckdb :memory: < "${sqlFilePath}"`, {
    timeout: 180000,
    maxBuffer: 10 * 1024 * 1024,
  });
}

/**
 * Greedy byte-based deterministic chunker. Walks the serialized JSONL lines
 * and packs them into contiguous ranges of at most MAX_JSONL_BYTES_PER_CHUNK
 * bytes each. A single line larger than the threshold gets its own chunk
 * (so we never silently drop data). Same input → same chunks, so the
 * chunk-aware deterministic filename scheme round-trips cleanly on retries.
 *
 * @param {string[]} lines - already-serialized JSON lines (no trailing '\n')
 * @returns {{start:number,end:number}[]} non-empty, contiguous, ordered ranges
 */
function chunkLinesByBytes(lines) {
  const ranges = [];
  let chunkStart = 0;
  let chunkBytes = 0;
  for (let i = 0; i < lines.length; i++) {
    const lineBytes = Buffer.byteLength(lines[i], 'utf8') + 1; // +1 for '\n'
    if (chunkBytes > 0 && chunkBytes + lineBytes > MAX_JSONL_BYTES_PER_CHUNK) {
      ranges.push({ start: chunkStart, end: i });
      chunkStart = i;
      chunkBytes = 0;
    }
    chunkBytes += lineBytes;
  }
  if (chunkStart < lines.length) {
    ranges.push({ start: chunkStart, end: lines.length });
  }
  return ranges;
}

/**
 * Write a single partition's records to Parquet and upload to GCS.
 *
 * For small partitions this writes one file (keeping the legacy single-file
 * naming for backwards compat with already-uploaded Parquets). For large
 * partitions whose serialized JSONL would exceed MAX_JSONL_BYTES_PER_CHUNK,
 * the records are split into N byte-balanced chunks and written as N
 * Parquet files — each through its own DuckDB invocation, so each stays
 * well under the 256 MiB memory cap.
 *
 * Exactly-once semantics are preserved because the chunk count and chunk
 * boundaries are a pure function of the input records' serialized bytes.
 */
async function writePartitionToGCS(records, type, partition, afterRecordTime) {
  if (records.length === 0) return;

  const mapFn = type === 'updates' ? mapUpdateRecord : mapEventRecord;
  const mapped = records.map(mapFn);
  const lines = mapped.map(r => JSON.stringify(r));

  const chunkRanges = chunkLinesByBytes(lines);
  const chunkCount = chunkRanges.length;

  if (chunkCount > 1) {
    let totalBytes = 0;
    for (const line of lines) totalBytes += Buffer.byteLength(line, 'utf8') + 1;
    const mib = (totalBytes / (1024 * 1024)).toFixed(1);
    console.log(`   ✂️  ${type} partition ${partition} split into ${chunkCount} chunks (${records.length} records, ${mib} MiB JSONL)`);
  }

  mkdirSync(REINGEST_TMP_DIR, { recursive: true });

  for (let chunkIdx = 0; chunkIdx < chunkRanges.length; chunkIdx++) {
    const { start, end } = chunkRanges[chunkIdx];
    const chunkLines = lines.slice(start, end);
    const chunkRecordCount = end - start;

    const fileName    = deterministicFileName(type, afterRecordTime, partition, chunkIdx, chunkCount);
    const jsonlPath   = join(REINGEST_TMP_DIR, fileName.replace('.parquet', '.jsonl'));
    const parquetPath = join(REINGEST_TMP_DIR, fileName);
    const sqlFilePath = join(REINGEST_TMP_DIR, fileName.replace('.parquet', '.sql'));

    try {
      // 1. Write this chunk's JSONL slice
      writeFileSync(jsonlPath, chunkLines.join('\n') + '\n');

      // 2. Convert JSONL → Parquet via memory-tuned DuckDB CLI
      await jsonlToParquetViaDuckDB(jsonlPath, parquetPath, sqlFilePath, type);

      // 3. Upload to GCS via SDK (deterministic path → overwrite = idempotent, zero dups)
      //    Uses VM service account / ADC — not gsutil user credentials.
      const gcsObjectPath = `raw/${partition}/${fileName}`;
      await uploadFileToGCS(parquetPath, gcsObjectPath);

      const chunkNote = chunkCount > 1 ? ` [chunk ${chunkIdx + 1}/${chunkCount}]` : '';
      console.log(`   📤 ${type}: ${chunkRecordCount} records${chunkNote} → ${partition}/${fileName}`);
    } finally {
      // Clean up temp files regardless of outcome
      for (const p of [jsonlPath, parquetPath, sqlFilePath]) {
        try { if (existsSync(p)) unlinkSync(p); } catch {}
      }
    }
  }
}

/**
 * Write a full API batch (updates + events) to GCS with deterministic filenames.
 * Each partition is written and uploaded serially to minimize memory on constrained VMs.
 *
 * @param {object[]} updates        - Normalized update records
 * @param {object[]} events         - Normalized event records
 * @param {number}   migrationId    - Migration ID for partitioning
 * @param {string}   afterRecordTime - Cursor position that produced this batch
 */
async function writeBatchToGCS(updates, events, migrationId, afterRecordTime) {
  // Write updates partitioned by effective_at day
  if (updates.length > 0) {
    const groups = groupByPartition(updates, 'updates', 'updates', migrationId);
    for (const [partition, records] of Object.entries(groups)) {
      await writePartitionToGCS(records, 'updates', partition, afterRecordTime);
    }
  }
  // Write events partitioned by effective_at day
  if (events.length > 0) {
    const groups = groupByPartition(events, 'events', 'updates', migrationId);
    for (const [partition, records] of Object.entries(groups)) {
      await writePartitionToGCS(records, 'events', partition, afterRecordTime);
    }
  }
}

// ─── Fetch and re-ingest ──────────────────────────────────────────────────

async function reingestDateRange(dates, migrations) {
  console.log('\n' + '═'.repeat(80));
  console.log(`📥 RE-INGESTING: ${START_DATE} to ${END_DATE}`);
  console.log('═'.repeat(80));

  let totalUpdates = 0;
  let totalEvents = 0;

  for (const mig of migrations) {
    console.log(`\n── Migration ${mig} ──`);

    // Find where backfill ends to avoid overlap.
    // Skip when --clean-backfill is used: the backfill data for this range
    // has been (or will be) deleted, so there's no boundary to honour.
    const backfillEnd = CLEAN_BACKFILL ? null : await findBackfillBoundary(mig);

    // Build the full time range for this re-ingestion
    const rangeStartDefault = START_DATE + 'T00:00:00.000000Z';
    const rangeEnd = new Date(new Date(END_DATE + 'T00:00:00Z').getTime() + 86400000).toISOString();

    // Resume priority: saved cursor > --after flag > backfill boundary > start date
    let rangeStart = rangeStartDefault;
    let migUpdates = 0;
    let migEvents = 0;
    let batchNum = 0;

    const savedCursor = loadReingestCursor(mig);
    if (savedCursor && savedCursor.after_record_time) {
      rangeStart = savedCursor.after_record_time;
      migUpdates = savedCursor.updates_written || 0;
      migEvents = savedCursor.events_written || 0;
      batchNum = savedCursor.batches_processed || 0;
      console.log(`   📍 Resuming from saved cursor: ${savedCursor.after_record_time}`);
      console.log(`      (${migUpdates.toLocaleString()} updates, ${migEvents.toLocaleString()} events already written in previous run)`);
    } else if (RESUME_AFTER) {
      rangeStart = RESUME_AFTER;
      console.log(`   📍 Resuming from --after cursor: ${RESUME_AFTER}`);
      console.log(`      (skipping already-ingested data)`);
    } else if (backfillEnd && new Date(backfillEnd) >= new Date(rangeStartDefault) &&
        new Date(backfillEnd) < new Date(rangeEnd)) {
      rangeStart = backfillEnd;
      console.log(`   📍 Using backfill boundary as start: ${backfillEnd}`);
      console.log(`      (prevents duplicate data with raw/backfill/)`);
    } else {
      console.log(`   📍 Starting from beginning of range: ${rangeStartDefault}`);
    }

    console.log(`   Time range: ${rangeStart} → ${rangeEnd}`);

    // Use the v2/updates API with after semantics to walk forward through the range
    let afterRecordTime = rangeStart;
    let afterMigrationId = savedCursor?.after_migration_id ?? mig;
    let consecutiveEmpty = 0;
    let consecutiveErrors = 0;
    let totalFailovers = 0;
    let cooldowns = 0;
    const MAX_CONSECUTIVE_ERRORS = 20;

    // Expose state for graceful shutdown
    _shutdownState = { mig, afterRecordTime, afterMigrationId, migUpdates, migEvents, batchNum };

    while (true) {
      batchNum++;

      try {
        // Use AbortController-based fetch with adaptive page_size/timeout
        const data = await fetchUpdatesAPI(afterMigrationId, afterRecordTime);
        const transactions = data.items;
        consecutiveErrors = 0;
        totalFailovers = 0;
        cooldowns = 0;

        // Reset adaptive params on successful fetch with data — the stuck cursor
        // has been passed, so restore normal page_size and timeout.
        if (transactions.length > 0) {
          if (_adaptivePageSize !== BATCH_SIZE || _adaptiveTimeoutMs !== FETCH_TIMEOUT_MS) {
            console.log(`   🔧 Adaptive params reset: page_size → ${BATCH_SIZE}, timeout → ${FETCH_TIMEOUT_MS / 1000}s (cursor unstuck)`);
            _adaptivePageSize  = BATCH_SIZE;
            _adaptiveTimeoutMs = FETCH_TIMEOUT_MS;
          }
          _stuckCursor     = null;
          _stuckCursorHits = 0;
        }

        if (transactions.length === 0) {
          consecutiveEmpty++;
          if (consecutiveEmpty >= 10) {
            console.log(`   ✅ Migration ${mig}: No more data (${consecutiveEmpty} empty responses)`);
            break;
          }
          await new Promise(r => setTimeout(r, 2000));
          continue;
        }
        consecutiveEmpty = 0;

        // Last record_time across batch (already computed by fetchUpdatesAPI)
        const lastRecordTime = data.lastRecordTime;

        if (new Date(lastRecordTime) > new Date(rangeEnd)) {
          const inRange = transactions.filter(tx =>
            new Date(tx.record_time) <= new Date(rangeEnd)
          );
          if (inRange.length === 0) {
            console.log(`   ✅ Migration ${mig}: Reached end of date range`);
            break;
          }
          const result = await processAndWrite(inRange, mig, afterRecordTime);
          migUpdates += result.updates;
          migEvents += result.events;
          saveReingestCursor(mig, afterRecordTime, afterMigrationId, { updates: migUpdates, events: migEvents, batches: batchNum });
          console.log(`   ✅ Migration ${mig}: Reached end of date range (partial batch: ${inRange.length})`);
          break;
        }

        if (!DRY_RUN) {
          const result = await processAndWrite(transactions, mig, afterRecordTime);
          migUpdates += result.updates;
          migEvents += result.events;
        } else {
          migUpdates += transactions.length;
        }

        // Advance cursor in memory
        afterRecordTime = lastRecordTime;
        afterMigrationId = transactions[transactions.length - 1].migration_id ?? mig;

        // Update shutdown state
        _shutdownState = { mig, afterRecordTime, afterMigrationId, migUpdates, migEvents, batchNum };

        // Save cursor after every batch — data is already in GCS (written
        // synchronously by writeBatchToGCS before cursor advances).
        // On crash, worst case: re-fetch one batch → same deterministic
        // filename → GCS overwrite → zero dups, zero gaps.
        if (!DRY_RUN) {
          saveReingestCursor(mig, afterRecordTime, afterMigrationId, { updates: migUpdates, events: migEvents, batches: batchNum });
        }

        // Progress logging
        if (batchNum % 10 === 0) {
          const cursorDate = afterRecordTime.split('T')[0];
          console.log(`   📥 Batch ${batchNum}: ${migUpdates.toLocaleString()} updates, ${migEvents.toLocaleString()} events | date=${cursorDate} | cursor=${afterRecordTime}`);
        }

      } catch (err) {
        if (err.response?.status === 404) {
          console.log(`   ℹ️  Migration ${mig}: 404 - no data for this migration in this range`);
          break;
        }
        consecutiveErrors++;
        console.error(`   ❌ Batch ${batchNum} failed (${consecutiveErrors}/${MAX_CONSECUTIVE_ERRORS}): ${err.message}`);

        // Adaptive page_size / timeout when stuck at the same cursor (FIX #13).
        // If the API consistently times out for a cursor, the response at that
        // position is likely very large. Halve page_size (min 1) and increase
        // timeout (up to 3x base) to break through.
        const isTimeout = err.code === 'FETCH_TIMEOUT'
          || err.code === 'ECONNABORTED'
          || err.code === 'ETIMEDOUT'
          || err.code === 'ECONNRESET'
          || err.message?.includes('timeout');

        if (isTimeout) {
          if (_stuckCursor === afterRecordTime) {
            _stuckCursorHits++;
          } else {
            _stuckCursor     = afterRecordTime;
            _stuckCursorHits = 1;
          }
          if (_stuckCursorHits > 0 && _stuckCursorHits % ENDPOINT_ROTATE_AFTER_ERRORS === 0) {
            const oldPageSize = _adaptivePageSize;
            _adaptivePageSize  = Math.max(1, Math.floor(_adaptivePageSize / 2));
            _adaptiveTimeoutMs = Math.min(FETCH_TIMEOUT_MS * 3, Math.round(_adaptiveTimeoutMs * 1.5));
            if (_adaptivePageSize !== oldPageSize) {
              console.log(
                `   🔧 Adaptive retry: page_size ${oldPageSize} → ${_adaptivePageSize}, ` +
                `timeout ${Math.round(_adaptiveTimeoutMs / 1000)}s ` +
                `(stuck at same cursor for ${_stuckCursorHits} errors)`
              );
            }
          }
        }

        // Try failover to another SV node after ENDPOINT_ROTATE_AFTER_ERRORS consecutive errors
        if (consecutiveErrors % ENDPOINT_ROTATE_AFTER_ERRORS === 0 && totalFailovers < 5) {
          const switched = await tryFailover();
          if (switched) {
            totalFailovers++;
            console.log(`   🔄 Retrying with ${activeEndpointName} (failover ${totalFailovers}/5)...`);
            continue; // Don't reset counter — let it keep climbing toward MAX
          }
        }

        if (consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
          cooldowns = (cooldowns || 0) + 1;
          if (cooldowns > 6) {
            throw new Error(`Too many consecutive errors after ${cooldowns} cooldowns, aborting migration ${mig}. Last cursor: ${afterRecordTime}`);
          }
          // Wait 5 minutes in case it's a temporary full-network outage
          console.log(`   ⏳ All endpoints failing. Waiting 5 minutes before retry (cooldown ${cooldowns}/6)... Last cursor: ${afterRecordTime}`);
          await new Promise(r => setTimeout(r, 300000));
          consecutiveErrors = 0;
          totalFailovers = 0;
          console.log(`   🔄 Resuming after cooldown...`);
          continue;
        }
        // Exponential backoff: 2s, 4s, 8s, 16s, 32s
        const delay = Math.min(2000 * Math.pow(2, consecutiveErrors - 1), 32000);
        await new Promise(r => setTimeout(r, delay));
      }
    }

    // Final cursor save for this migration
    if (!DRY_RUN) {
      saveReingestCursor(mig, afterRecordTime, afterMigrationId, { updates: migUpdates, events: migEvents, batches: batchNum });
    }

    totalUpdates += migUpdates;
    totalEvents += migEvents;
    console.log(`   Migration ${mig} complete: ${migUpdates.toLocaleString()} updates, ${migEvents.toLocaleString()} events`);

    // Migration finished — remove cursor so re-running doesn't skip
    deleteReingestCursor(mig);
    console.log(`   Cursor cleared (migration ${mig} fully ingested)`);
  }

  _shutdownState = null;
  return { totalUpdates, totalEvents };
}

/**
 * Normalize an API batch and write directly to GCS with deterministic filenames.
 *
 * @param {object[]} transactions    - Raw API response transactions
 * @param {number}   migrationId     - Migration ID
 * @param {string}   afterRecordTime - Cursor position that produced this batch (for deterministic filename)
 */
async function processAndWrite(transactions, migrationId, afterRecordTime) {
  const updates = [];
  const events = [];
  const batchTimestamp = new Date();

  for (const item of transactions) {
    try {
      const update = normalizeUpdate({ ...item, migration_id: migrationId }, { batchTimestamp });
      updates.push(update);

      const isReassignment = !!item.reassignment;
      const u = item.transaction || item.reassignment || item;

      const updateInfo = {
        record_time:     u.record_time,
        effective_at:    u.effective_at,
        synchronizer_id: u.synchronizer_id,
        source:          u.source     || null,
        target:          u.target     || null,
        unassign_id:     u.unassign_id || null,
        submitter:       u.submitter  || null,
        counter:         u.counter    ?? null,
      };

      if (isReassignment) {
        const ce = item.reassignment?.event?.created_event;
        const ae = item.reassignment?.event?.archived_event;

        if (ce) {
          const ev = normalizeEvent(ce, update.update_id, migrationId, item, updateInfo, { batchTimestamp });
          ev.event_type = 'reassign_create';
          events.push(ev);
        }
        if (ae) {
          const ev = normalizeEvent(ae, update.update_id, migrationId, item, updateInfo, { batchTimestamp });
          ev.event_type = 'reassign_archive';
          events.push(ev);
        }
      } else {
        const eventsById   = u?.events_by_id  || u?.eventsById   || {};
        const rootEventIds = u?.root_event_ids || u?.rootEventIds || [];

        const flattened = flattenEventsInTreeOrder(eventsById, rootEventIds);
        for (const rawEvent of flattened) {
          const ev = normalizeEvent(rawEvent, update.update_id, migrationId, rawEvent, updateInfo, { batchTimestamp });

          const mapKeyId = rawEvent.event_id;
          if (mapKeyId && ev.event_id && mapKeyId !== ev.event_id) {
            ev.event_id = mapKeyId;
          } else if (mapKeyId && !ev.event_id) {
            ev.event_id = mapKeyId;
          }

          events.push(ev);
        }
      }
    } catch (err) {
      const txId = item?.update_id || item?.transaction?.update_id || 'UNKNOWN';
      console.warn(`   ⚠️ Transaction ${txId} decode failed: ${err.message}`);
    }
  }

  // Filter out stragglers with effective_at before the re-ingestion start date.
  // These records have record_time within our cursor range but effective_at in an
  // earlier period — they already exist in the backfill/earlier data for that date.
  // Writing them here would create partial out-of-range partitions (e.g. March 2
  // files when re-ingesting from March 3).
  const rangeStartDate = new Date(START_DATE + 'T00:00:00Z');
  const filteredUpdates = updates.filter(u => u.effective_at >= rangeStartDate);
  const filteredEvents  = events.filter(e => e.effective_at >= rangeStartDate);

  const droppedUpdates = updates.length - filteredUpdates.length;
  const droppedEvents  = events.length - filteredEvents.length;
  if (droppedUpdates > 0 || droppedEvents > 0) {
    console.log(`   ⏭️  Filtered ${droppedUpdates} updates + ${droppedEvents} events with effective_at before ${START_DATE}`);
  }

  // Write directly to GCS with deterministic filenames — no buffering.
  // writeBatchToGCS completes synchronously (all uploads confirmed)
  // before this function returns, so the cursor can safely advance.
  if (!DRY_RUN) {
    await writeBatchToGCS(filteredUpdates, filteredEvents, migrationId, afterRecordTime);
  }

  return { updates: filteredUpdates.length, events: filteredEvents.length };
}

// ─── Main ─────────────────────────────────────────────────────────────────

async function main() {
  const startTime = Date.now();

  console.log('\n' + '═'.repeat(80));
  console.log('🔧 TARGETED RE-INGESTION TOOL');
  console.log('═'.repeat(80));
  console.log(`   Date range:    ${START_DATE} → ${END_DATE}`);
  console.log(`   GCS bucket:    ${GCS_BUCKET}`);
  console.log(`   Scan URL:      ${activeScanUrl} (${activeEndpointName})`);
  console.log(`   Migration:     ${TARGET_MIGRATION !== null ? TARGET_MIGRATION : 'all'}`);
  const modeStr = DRY_RUN ? 'DRY RUN' : AUDIT_ONLY ? 'AUDIT ONLY' : CLEAN_BACKFILL ? 'CLEAN ALL (incl. backfill) + RE-INGEST' : CLEAN ? 'CLEAN + RE-INGEST' : 'RE-INGEST ONLY';
  console.log(`   Mode:          ${modeStr}`);
  if (RESUME_AFTER) console.log(`   Resume after:  ${RESUME_AFTER}`);
  console.log(`   Batch size:    ${BATCH_SIZE}`);
  console.log('═'.repeat(80));

  const dates = dateRange(START_DATE, END_DATE);
  console.log(`   Days in range: ${dates.length}`);

  // Step 0: Probe all Scan endpoints; auto-failover if active is unreachable.
  // Mirrors fetch-updates.js — prevents starting the run on a dead endpoint.
  await probeAllScanEndpoints();

  // Step 1: Discover migrations
  const migrations = await discoverMigrations();
  if (migrations.length === 0) {
    console.error('❌ No migrations found');
    process.exit(1);
  }

  // Safety guard: refuse to destroy in-progress data on a resume run.
  //
  // `--clean` / `--clean-backfill` wipe every Parquet file under the target
  // date range and delete the saved reingest cursor. That is the correct
  // behaviour for the FIRST run of a fresh ingestion, but on a resume run
  // it silently destroys the cumulative progress of the previous run
  // (potentially days of GCS writes).
  //
  // A saved cursor is a strong signal that "a prior run wrote data to GCS
  // under this exact (start, end, migration) key". If one exists, we refuse
  // `--clean` / `--clean-backfill` unless the user passes `--force` to
  // confirm they really do mean to wipe and restart.
  if ((CLEAN || CLEAN_BACKFILL) && !FORCE) {
    const existingCursors = [];
    for (const mig of migrations) {
      const cur = loadReingestCursor(mig);
      if (cur) existingCursors.push({ mig, cursor: cur });
    }
    if (existingCursors.length > 0) {
      console.error('\n' + '═'.repeat(80));
      console.error('❌ SAFETY GUARD: --clean / --clean-backfill is destructive on a resume run');
      console.error('═'.repeat(80));
      console.error('   A saved reingest cursor already exists for this (start, end, migration):');
      for (const { mig, cursor } of existingCursors) {
        console.error(`     migration=${mig}:  cursor=${cursor.after_record_time}`);
        console.error(`                    already written: ${(cursor.updates_written || 0).toLocaleString()} updates, ${(cursor.events_written || 0).toLocaleString()} events, ${(cursor.batches_processed || 0).toLocaleString()} batches`);
      }
      console.error('');
      console.error('   Proceeding with --clean / --clean-backfill would delete all data previously');
      console.error('   written by this resume key AND remove the cursor — destroying the prior run.');
      console.error('');
      console.error('   What you probably want:');
      console.error('     • TO RESUME: re-run the same command WITHOUT --clean / --clean-backfill.');
      console.error('       The script will pick up automatically from the saved cursor.');
      console.error('');
      console.error('   If you really do want to wipe everything and start from scratch:');
      console.error('     • Add --force to bypass this guard.');
      console.error('═'.repeat(80));
      process.exit(1);
    }
  }

  // Step 2: Audit what exists
  await auditDateRange(dates, migrations);

  if (AUDIT_ONLY) {
    console.log('\n✅ Audit complete. Use --clean to clean and re-ingest.');
    return;
  }

  // Step 3: Check for backfill overlap
  // When --clean-backfill is used, backfill data is deleted so no overlap.
  // When --clean is used (without --clean-backfill), overlap is handled by
  // starting from the backfill boundary cursor.
  if (CLEAN_BACKFILL) {
    console.log('\n🔍 --clean-backfill: backfill data will be deleted, no overlap possible');
  } else if (!CLEAN) {
    const overlapOk = await checkBackfillOverlap(dates, migrations);
    if (!overlapOk) {
      process.exit(1);
    }
  } else {
    console.log('\n🔍 --clean mode: backfill overlap will be handled automatically');
    console.log('   Re-ingestion will start from the backfill cursor boundary');
  }

  // Step 4: Clean existing bad data (if --clean or --clean-backfill)
  if (CLEAN || CLEAN_BACKFILL) {
    await cleanUpdatesData(dates, migrations);
    // Also clear any saved reingest cursor so we start truly fresh
    for (const mig of migrations) {
      deleteReingestCursor(mig);
    }
  }

  if (DRY_RUN) {
    console.log('\n✅ [DRY RUN] Would re-ingest data for the above date range.');
    console.log('   Remove --dry-run to actually re-ingest.');
    return;
  }

  // Step 5: Re-ingest
  const { totalUpdates, totalEvents } = await reingestDateRange(dates, migrations);

  const duration = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  console.log('\n' + '═'.repeat(80));
  console.log(`✅ RE-INGESTION COMPLETE (${duration} minutes)`);
  console.log(`   Total updates: ${totalUpdates.toLocaleString()}`);
  console.log(`   Total events:  ${totalEvents.toLocaleString()}`);
  console.log('═'.repeat(80));

  // Step 6: Post-ingestion audit
  console.log('\n📊 POST-INGESTION AUDIT:');
  await auditDateRange(dates, migrations);
}

// ─── Graceful shutdown ───────────────────────────────────────────────────
// On SIGINT/SIGTERM: save cursor, then exit. No buffered data to flush since
// each batch writes directly to GCS before the cursor advances.
// On restart the cursor auto-resumes from where we stopped.

let _isShuttingDown = false;

async function gracefulShutdown(signal) {
  if (_isShuttingDown) return;
  _isShuttingDown = true;
  console.log(`\n⚠️  ${signal} received — shutting down gracefully...`);

  try {
    // No buffered data to flush — each batch writes directly to GCS
    // before the cursor advances. Just save cursor for clean resume.
    if (_shutdownState) {
      const { mig, afterRecordTime, afterMigrationId, migUpdates, migEvents, batchNum } = _shutdownState;
      saveReingestCursor(mig, afterRecordTime, afterMigrationId,
        { updates: migUpdates, events: migEvents, batches: batchNum });
      console.log(`   Cursor saved: migration=${mig}, cursor=${afterRecordTime}`);
      console.log(`   Re-run the same command to resume automatically.`);
    }
    console.log('   Shutdown complete.');
  } catch (err) {
    console.error(`   Shutdown error: ${err.message}`);
  }
  process.exit(0);
}

process.on('SIGINT',  () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));

// ─── Unhandled error safety net ───────────────────────────────────────────
// Catch truly unexpected errors, save the cursor, then exit. Mirrors
// fetch-updates.js — prevents an uncaught error from losing in-flight progress.
process.on('uncaughtException', (err) => {
  console.error('\n[uncaughtException]', err);
  if (_shutdownState && !_isShuttingDown) {
    try {
      const { mig, afterRecordTime, afterMigrationId, migUpdates, migEvents, batchNum } = _shutdownState;
      saveReingestCursor(mig, afterRecordTime, afterMigrationId,
        { updates: migUpdates, events: migEvents, batches: batchNum });
      console.error(`   Cursor saved at ${afterRecordTime} — re-run to resume.`);
    } catch {}
  }
  process.exit(1);
});

process.on('unhandledRejection', (reason) => {
  console.error('\n[unhandledRejection]', reason);
  if (_shutdownState && !_isShuttingDown) {
    try {
      const { mig, afterRecordTime, afterMigrationId, migUpdates, migEvents, batchNum } = _shutdownState;
      saveReingestCursor(mig, afterRecordTime, afterMigrationId,
        { updates: migUpdates, events: migEvents, batches: batchNum });
      console.error(`   Cursor saved at ${afterRecordTime} — re-run to resume.`);
    } catch {}
  }
  process.exit(1);
});

main().catch(async (err) => {
  console.error(`\n❌ FATAL: ${err.message}`);
  if (VERBOSE) console.error(err.stack);

  // Save cursor even on fatal errors so progress isn't lost
  if (_shutdownState && !_isShuttingDown) {
    try {
      const { mig, afterRecordTime, afterMigrationId, migUpdates, migEvents, batchNum } = _shutdownState;
      saveReingestCursor(mig, afterRecordTime, afterMigrationId,
        { updates: migUpdates, events: migEvents, batches: batchNum });
      console.error(`   Cursor saved at ${afterRecordTime} — re-run to resume.`);
    } catch {}
  }
  process.exit(1);
});
