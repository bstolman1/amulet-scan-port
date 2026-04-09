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
 * SAFETY FEATURES:
 * 1. Verifies the target date range does NOT overlap with backfill data
 * 2. Optionally deletes existing bad data in the target range before writing
 * 3. Uses the same Parquet writer pipeline as fetch-updates.js
 * 4. Deduplicates within each batch using update_id
 * 5. Dry-run mode to preview what would happen
 * 6. Persistent cursor: progress is saved to disk after every flush cycle.
 *    On restart (crash, SIGINT, SIGTERM), re-running the same command
 *    auto-resumes from where it stopped — no manual --after needed.
 * 7. Graceful shutdown: SIGINT/SIGTERM flushes buffered data to GCS and
 *    saves the cursor before exiting.
 * 8. GCS drain: parquetWriter.shutdown() is called on completion to ensure
 *    all async GCS uploads finish before the process exits.
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
import { atomicWriteFile } from './atomic-cursor.js';

const execFileAsync = promisify(execFileCb);
const execAsync = promisify(execCb);

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
dotenv.config({ path: join(__dirname, '.env') });

// ─── Memory-safe overrides for re-ingestion ──────────────────────────────
// The default .env is tuned for the live pipeline on larger machines.
// Re-ingestion is a long-running bulk process; prioritize stability over speed.
if (!process.env.REINGEST_USE_DEFAULT_SETTINGS) {
  process.env.PARQUET_WORKERS        = '6';
  process.env.GCS_UPLOAD_CONCURRENCY = '16';
  process.env.MAX_ROWS_PER_FILE      = '50000';
  process.env.MIN_ROWS_PER_FILE      = '10000';
}

import { normalizeUpdate, normalizeEvent, flattenEventsInTreeOrder } from './data-schema.js';
import * as parquetWriter from './write-parquet.js';

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
const FLUSH_EVERY = 5; // flush + save cursor every N batches

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

// ─── GCS helpers ──────────────────────────────────────────────────────────

async function gsutilLs(gcsPath, opts = {}) {
  try {
    const { stdout } = await execAsync(
      `gsutil ls -r "${gcsPath}" 2>/dev/null || true`,
      { timeout: opts.timeout || 60000, maxBuffer: 50 * 1024 * 1024 }
    );
    return stdout.trim().split('\n').filter(l => l && l.endsWith('.parquet'));
  } catch (err) {
    return [];
  }
}

async function gsutilRm(gcsPath) {
  try {
    await execAsync(`gsutil -m rm -r "${gcsPath}" 2>/dev/null || true`, { timeout: 120000 });
    return true;
  } catch (err) {
    // Treat any failure as non-fatal for cleanup
    return false;
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
  const prefix = `gs://${GCS_BUCKET}/raw/${source}/${type}/migration=${migrationId}/`;
  const files = await gsutilLs(prefix + '**/*.parquet');
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
            const gcsPath = dayPartitionPath(source, type, mig, year, month, day);
            if (DRY_RUN) {
              console.log(`   [DRY RUN] Would delete ${count} ${source}/${type} files at ${gcsPath}`);
            } else {
              console.log(`   Deleting ${count} ${source}/${type} files at ${gcsPath}...`);
              await gsutilRm(gcsPath);
              totalDeleted += count;
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

// ─── Fetch and re-ingest ──────────────────────────────────────────────────

async function reingestDateRange(dates, migrations) {
  console.log('\n' + '═'.repeat(80));
  console.log(`📥 RE-INGESTING: ${START_DATE} to ${END_DATE}`);
  console.log('═'.repeat(80));

  // Set writer to 'updates' source (writes to raw/updates/)
  parquetWriter.setDataSource('updates');

  let totalUpdates = 0;
  let totalEvents = 0;

  for (const mig of migrations) {
    parquetWriter.setMigrationId(mig);
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
        const payload = {
          page_size: BATCH_SIZE,
          daml_value_encoding: 'compact_json',
          after: { after_migration_id: afterMigrationId, after_record_time: afterRecordTime },
        };

        const response = await client.post('/v2/updates', payload);
        const transactions = response.data?.transactions || [];
        consecutiveErrors = 0;
        totalFailovers = 0;
        cooldowns = 0;

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

        // Check if we've gone past our end date
        const lastRecordTime = transactions.reduce((max, tx) =>
          tx.record_time > max ? tx.record_time : max,
          transactions[0].record_time
        );

        if (new Date(lastRecordTime) > new Date(rangeEnd)) {
          const inRange = transactions.filter(tx =>
            new Date(tx.record_time) <= new Date(rangeEnd)
          );
          if (inRange.length === 0) {
            console.log(`   ✅ Migration ${mig}: Reached end of date range`);
            break;
          }
          const result = await processAndWrite(inRange, mig);
          migUpdates += result.updates;
          migEvents += result.events;
          // Flush + save cursor for the final partial batch
          await flushAndSaveCursor(mig, afterRecordTime, afterMigrationId, migUpdates, migEvents, batchNum);
          console.log(`   ✅ Migration ${mig}: Reached end of date range (partial batch: ${inRange.length})`);
          break;
        }

        if (!DRY_RUN) {
          const result = await processAndWrite(transactions, mig);
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

        // Progress logging
        if (batchNum % 10 === 0) {
          const cursorDate = afterRecordTime.split('T')[0];
          console.log(`   📥 Batch ${batchNum}: ${migUpdates.toLocaleString()} updates, ${migEvents.toLocaleString()} events | date=${cursorDate} | cursor=${afterRecordTime}`);
        }

        // Periodic flush: write data to GCS, then save cursor.
        // Cursor is saved AFTER data is confirmed written — this guarantees
        // the cursor never advances past actually-persisted data. On crash,
        // worst case: up to FLUSH_EVERY batches get re-fetched (harmless dups
        // at the cursor boundary), but no data is skipped.
        if (!DRY_RUN && batchNum % FLUSH_EVERY === 0) {
          await flushAndSaveCursor(mig, afterRecordTime, afterMigrationId, migUpdates, migEvents, batchNum);
        }

      } catch (err) {
        if (err.response?.status === 404) {
          console.log(`   ℹ️  Migration ${mig}: 404 - no data for this migration in this range`);
          break;
        }
        consecutiveErrors++;
        console.error(`   ❌ Batch ${batchNum} failed (${consecutiveErrors}/${MAX_CONSECUTIVE_ERRORS}): ${err.message}`);

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

    // Final flush for this migration
    if (!DRY_RUN) {
      await flushAndSaveCursor(mig, afterRecordTime, afterMigrationId, migUpdates, migEvents, batchNum);
    }

    totalUpdates += migUpdates;
    totalEvents += migEvents;
    console.log(`   Migration ${mig} complete: ${migUpdates.toLocaleString()} updates, ${migEvents.toLocaleString()} events`);

    // Migration finished — remove cursor so re-running doesn't skip
    deleteReingestCursor(mig);
    console.log(`   Cursor cleared (migration ${mig} fully ingested)`);
  }

  _shutdownState = null;

  // Shut down the writer (flushes + drains GCS upload queue)
  if (!DRY_RUN) {
    console.log('\n⏳ Shutting down writer (draining GCS uploads)...');
    await parquetWriter.shutdown();
    console.log('   ✅ All data flushed and uploaded.');
  }

  return { totalUpdates, totalEvents };
}

/**
 * Flush buffered data to Parquet, wait for GCS uploads, then save cursor.
 *
 * Order is critical for crash safety:
 *   1. flushAll()       → writes buffered records to local Parquet files
 *   2. waitForWrites()  → waits for Parquet file creation to finish
 *   3. waitForUploads() → waits for GCS upload queue to drain
 *   4. saveCursor()     → persists the position AFTER data is in GCS
 *
 * If we crash between 1–3, cursor is behind data → restart re-fetches
 * some records that are already in GCS → small dups (dedup handles it).
 * If we crash after 4, cursor matches data → clean resume.
 *
 * The WRONG order (save cursor before upload confirms) would risk gaps:
 * cursor could advance past data that never made it to GCS.
 */
async function flushAndSaveCursor(mig, afterRecordTime, afterMigrationId, updates, events, batches) {
  await parquetWriter.flushAll();
  await parquetWriter.waitForWrites();
  await parquetWriter.waitForUploads();
  saveReingestCursor(mig, afterRecordTime, afterMigrationId, { updates, events, batches });
}

async function processAndWrite(transactions, migrationId) {
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

  if (updates.length > 0) await parquetWriter.bufferUpdates(updates);
  if (events.length > 0) await parquetWriter.bufferEvents(events);

  return { updates: updates.length, events: events.length };
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

  // Step 1: Discover migrations
  const migrations = await discoverMigrations();
  if (migrations.length === 0) {
    console.error('❌ No migrations found');
    process.exit(1);
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
// On SIGINT/SIGTERM: flush buffered data, drain GCS uploads, save cursor,
// then exit. On restart the cursor auto-resumes from where we stopped.

let _isShuttingDown = false;

async function gracefulShutdown(signal) {
  if (_isShuttingDown) return;
  _isShuttingDown = true;
  console.log(`\n⚠️  ${signal} received — shutting down gracefully...`);

  try {
    // Flush whatever is buffered and wait for GCS uploads
    await parquetWriter.flushAll();
    await parquetWriter.waitForWrites();
    await parquetWriter.waitForUploads();

    // Save cursor so restart resumes from here
    if (_shutdownState) {
      const { mig, afterRecordTime, afterMigrationId, migUpdates, migEvents, batchNum } = _shutdownState;
      saveReingestCursor(mig, afterRecordTime, afterMigrationId,
        { updates: migUpdates, events: migEvents, batches: batchNum });
      console.log(`   Cursor saved: migration=${mig}, cursor=${afterRecordTime}`);
      console.log(`   Re-run the same command to resume automatically.`);
    }

    // Drain GCS upload queue
    await parquetWriter.shutdown();
    console.log('   Shutdown complete.');
  } catch (err) {
    console.error(`   Shutdown error: ${err.message}`);
  }
  process.exit(0);
}

process.on('SIGINT',  () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));

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
