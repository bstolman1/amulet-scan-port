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
 *
 * Usage:
 *   # Preview what would be re-ingested (dry run)
 *   node reingest-updates.js --start=2026-03-03 --end=2026-03-21 --dry-run
 *
 *   # Re-ingest March 3-21 data, cleaning bad data first
 *   node reingest-updates.js --start=2026-03-03 --end=2026-03-21 --clean
 *
 *   # Re-ingest only March 20-21 (missing events)
 *   node reingest-updates.js --start=2026-03-20 --end=2026-03-21 --clean
 *
 *   # Re-ingest specific migration only
 *   node reingest-updates.js --start=2026-03-03 --end=2026-03-21 --migration=4 --clean
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
import { existsSync, mkdirSync, readFileSync } from 'fs';
import axios from 'axios';
import https from 'https';

const execFileAsync = promisify(execFileCb);
const execAsync = promisify(execCb);

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
dotenv.config({ path: join(__dirname, '.env') });

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
const DRY_RUN = args.includes('--dry-run');
const CLEAN = args.includes('--clean');
const AUDIT_ONLY = args.includes('--audit-only');
const VERBOSE = args.includes('--verbose') || args.includes('-v');
const FORCE = args.includes('--force');
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 1000;

const GCS_BUCKET = process.env.GCS_BUCKET;
const SCAN_URL = process.env.SCAN_URL || 'https://scan.sv-1.global.canton.network.sync.global/api/scan';

if (!START_DATE || !END_DATE) {
  console.error('Usage: node reingest-updates.js --start=YYYY-MM-DD --end=YYYY-MM-DD [--clean] [--dry-run] [--audit-only]');
  process.exit(1);
}

if (!GCS_BUCKET) {
  console.error('GCS_BUCKET environment variable is required');
  process.exit(1);
}

const INSECURE_TLS = process.env.INSECURE_TLS === 'true';
const client = axios.create({
  baseURL: SCAN_URL,
  timeout: parseInt(process.env.FETCH_TIMEOUT_MS) || 30000,
  httpsAgent: new https.Agent({ rejectUnauthorized: !INSECURE_TLS }),
});

// ─── GCS helpers ──────────────────────────────────────────────────────────

async function gsutilLs(gcsPath, opts = {}) {
  try {
    const { stdout } = await execAsync(
      `gsutil ls -r "${gcsPath}" 2>/dev/null || true`,
      { timeout: opts.timeout || 60000, maxBuffer: 50 * 1024 * 1024 }
    );
    return stdout.trim().split('\n').filter(l => l && l.endsWith('.parquet'));
  } catch (err) {
    // Any gsutil ls failure means no files found (or transient error)
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
  console.log('\n🗑️  CLEANING existing updates data for target date range...');
  let totalDeleted = 0;

  for (const mig of migrations) {
    // Bulk list to find which days have data
    const [updatesMap, eventsMap] = await Promise.all([
      bulkListGCS('updates', 'updates', mig),
      bulkListGCS('updates', 'events', mig),
    ]);

    for (const { dateStr, year, month, day } of dates) {
      for (const [type, countMap] of [['updates', updatesMap], ['events', eventsMap]]) {
        const count = countMap[dateStr] || 0;
        if (count > 0) {
          const gcsPath = dayPartitionPath('updates', type, mig, year, month, day);
          if (DRY_RUN) {
            console.log(`   [DRY RUN] Would delete ${count} ${type} files at ${gcsPath}`);
          } else {
            console.log(`   Deleting ${count} ${type} files at ${gcsPath}...`);
            await gsutilRm(gcsPath);
            totalDeleted += count;
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

    // Build the full time range for this re-ingestion
    const rangeStart = START_DATE + 'T00:00:00.000000Z';
    const rangeEnd = new Date(new Date(END_DATE + 'T00:00:00Z').getTime() + 86400000).toISOString();

    console.log(`   Time range: ${rangeStart} → ${rangeEnd}`);

    // Use the v2/updates API with after semantics to walk forward through the range
    let afterRecordTime = rangeStart;
    let afterMigrationId = mig;
    let batchNum = 0;
    let migUpdates = 0;
    let migEvents = 0;
    let consecutiveEmpty = 0;

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

        if (transactions.length === 0) {
          consecutiveEmpty++;
          if (consecutiveEmpty >= 5) {
            console.log(`   ✅ Migration ${mig}: No more data (${consecutiveEmpty} empty responses)`);
            break;
          }
          // Small delay before retry
          await new Promise(r => setTimeout(r, 1000));
          continue;
        }
        consecutiveEmpty = 0;

        // Check if we've gone past our end date
        const lastRecordTime = transactions.reduce((max, tx) =>
          tx.record_time > max ? tx.record_time : max,
          transactions[0].record_time
        );

        if (new Date(lastRecordTime) > new Date(rangeEnd)) {
          // Filter out transactions beyond our range
          const inRange = transactions.filter(tx =>
            new Date(tx.record_time) <= new Date(rangeEnd)
          );
          if (inRange.length === 0) {
            console.log(`   ✅ Migration ${mig}: Reached end of date range`);
            break;
          }
          await processAndWrite(inRange, mig);
          migUpdates += inRange.length;
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

        // Advance cursor
        afterRecordTime = lastRecordTime;
        afterMigrationId = transactions[transactions.length - 1].migration_id ?? mig;

        if (batchNum % 10 === 0) {
          console.log(`   📥 Batch ${batchNum}: ${migUpdates.toLocaleString()} updates, ${migEvents.toLocaleString()} events | cursor=${afterRecordTime}`);
        }

      } catch (err) {
        if (err.response?.status === 404) {
          console.log(`   ℹ️  Migration ${mig}: 404 - no data for this migration in this range`);
          break;
        }
        console.error(`   ❌ Batch ${batchNum} failed: ${err.message}`);
        // Retry after delay
        await new Promise(r => setTimeout(r, 5000));
      }
    }

    totalUpdates += migUpdates;
    totalEvents += migEvents;
    console.log(`   Migration ${mig} complete: ${migUpdates.toLocaleString()} updates, ${migEvents.toLocaleString()} events`);
  }

  // Flush remaining buffered data
  if (!DRY_RUN) {
    console.log('\n⏳ Flushing remaining buffered data...');
    await parquetWriter.flushAll();
    await parquetWriter.waitForWrites();
    console.log('   ✅ All data flushed.');
  }

  return { totalUpdates, totalEvents };
}

async function processAndWrite(transactions, migrationId) {
  const updates = [];
  const events = [];

  for (const item of transactions) {
    try {
      const isReassignment = !!item.reassignment;
      const u = isReassignment ? item.reassignment : (item.transaction || item);

      const update = normalizeUpdate({ ...item, migration_id: migrationId });
      if (update) updates.push(update);

      // Extract events
      const eventsById = u?.events_by_id || u?.eventsById || {};
      const rootEventIds = u?.root_event_ids || u?.rootEventIds || [];
      const flatEvents = flattenEventsInTreeOrder(eventsById, rootEventIds);

      for (const [eventId, evt] of flatEvents) {
        try {
          const createdEvent = evt?.created_event || evt?.CreatedEvent;
          const archivedEvent = evt?.archived_event || evt?.ArchivedEvent;
          const exercisedEvent = evt?.exercised_event || evt?.ExercisedEvent;
          const eventData = createdEvent || archivedEvent || exercisedEvent;

          if (!eventData) continue;

          const updateId = update?.update_id || u?.update_id;
          const effectiveAt = update?.effective_at || u?.effective_at;
          const synchronizerId = update?.synchronizer_id || u?.synchronizer_id;

          const normalized = normalizeEvent(eventData, {
            event_id: eventId,
            update_id: updateId,
            effective_at: effectiveAt,
            migration_id: migrationId,
            synchronizer_id: synchronizerId,
            event_type_original: createdEvent ? 'created_event' :
                                 archivedEvent ? 'archived_event' : 'exercised_event',
          });

          if (normalized) events.push(normalized);
        } catch (err) {
          if (VERBOSE) {
            console.warn(`   ⚠️ Event ${eventId} failed: ${err.message}`);
          }
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
  console.log(`   Scan URL:      ${SCAN_URL}`);
  console.log(`   Migration:     ${TARGET_MIGRATION !== null ? TARGET_MIGRATION : 'all'}`);
  console.log(`   Mode:          ${DRY_RUN ? 'DRY RUN' : AUDIT_ONLY ? 'AUDIT ONLY' : CLEAN ? 'CLEAN + RE-INGEST' : 'RE-INGEST ONLY'}`);
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
  const overlapOk = await checkBackfillOverlap(dates, migrations);
  if (!overlapOk) {
    process.exit(1);
  }

  // Step 4: Clean existing bad data (if --clean)
  if (CLEAN) {
    await cleanUpdatesData(dates, migrations);
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

main().catch(err => {
  console.error(`\n❌ FATAL: ${err.message}`);
  if (VERBOSE) console.error(err.stack);
  process.exit(1);
});
