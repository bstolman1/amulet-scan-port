#!/usr/bin/env node
/**
 * Cursor vs Data Audit Tool
 * 
 * Lightweight diagnostic that compares cursor positions against actual
 * data in GCS across ALL 4 Hive partition paths:
 *   - raw/updates/updates/   (live transactions)
 *   - raw/updates/events/    (live events)
 *   - raw/backfill/updates/  (historical transactions)
 *   - raw/backfill/events/   (historical events)
 * 
 * Uses gcs-scanner.js (gsutil-based) instead of DuckDB for speed.
 * 
 * Usage:
 *   node audit-cursor-vs-data.js
 *   node audit-cursor-vs-data.js --verbose
 */

import { execSync } from 'child_process';
import { readFileSync, existsSync } from 'fs';
import { join } from 'path';
import dotenv from 'dotenv';
import { getCursorDir } from './path-utils.js';
import {
  getGCSScanPrefixes,
  scanGCSHivePartition,
} from './gcs-scanner.js';

dotenv.config();

const BUCKET = process.env.GCS_BUCKET || 'canton-bucket';
const CURSOR_DIR = getCursorDir();
const VERBOSE = process.argv.includes('--verbose');

// ─────────────────────────────────────────────────────────────
// Cursor helpers
// ─────────────────────────────────────────────────────────────

function readLocalCursor(name) {
  const path = join(CURSOR_DIR, `${name}.json`);
  if (!existsSync(path)) return null;
  try {
    return JSON.parse(readFileSync(path, 'utf8'));
  } catch {
    return null;
  }
}

async function readGCSCursor(name) {
  try {
    const raw = execSync(
      `gsutil cat gs://${BUCKET}/cursors/${name}.json 2>/dev/null`,
      { stdio: 'pipe', timeout: 10000 }
    ).toString().trim();
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

// ─────────────────────────────────────────────────────────────
// Display helpers
// ─────────────────────────────────────────────────────────────

function fmt(ts) {
  if (!ts) return 'N/A';
  return new Date(ts).toISOString();
}

function timeDiff(ts1, ts2) {
  if (!ts1 || !ts2) return 'N/A';
  const diffMs = Math.abs(new Date(ts1) - new Date(ts2));
  const h = Math.floor(diffMs / 3600000);
  const m = Math.floor((diffMs % 3600000) / 60000);
  const s = Math.floor((diffMs % 60000) / 1000);
  if (h > 0) return `${h}h ${m}m`;
  if (m > 0) return `${m}m ${s}s`;
  return `${s}s`;
}

function label(prefix) {
  if (prefix.includes('/updates/updates/')) return 'live/updates';
  if (prefix.includes('/updates/events/'))  return 'live/events';
  if (prefix.includes('/backfill/updates/')) return 'backfill/updates';
  if (prefix.includes('/backfill/events/'))  return 'backfill/events';
  return prefix;
}

// ─────────────────────────────────────────────────────────────
// Main audit
// ─────────────────────────────────────────────────────────────

async function main() {
  console.log('═══════════════════════════════════════════════════════════════');
  console.log('  CURSOR vs DATA AUDIT');
  console.log(`  Bucket: gs://${BUCKET}`);
  console.log(`  Cursor Dir: ${CURSOR_DIR}`);
  console.log('═══════════════════════════════════════════════════════════════\n');

  // ── 1. Read all cursors ──────────────────────────────────────
  console.log('📂 CURSOR STATUS:\n');

  const localLive = readLocalCursor('live-cursor');
  const gcsLive = await readGCSCursor('live-cursor');
  const backfill = readLocalCursor('backfill-cursor');

  const cursors = [
    { name: 'Local live-cursor', data: localLive, path: `${CURSOR_DIR}/live-cursor.json` },
    { name: 'GCS live-cursor',   data: gcsLive,   path: `gs://${BUCKET}/cursors/live-cursor.json` },
    { name: 'Local backfill-cursor', data: backfill, path: `${CURSOR_DIR}/backfill-cursor.json` },
  ];

  for (const c of cursors) {
    if (c.data) {
      console.log(`  ✅ ${c.name}`);
      console.log(`     Path: ${c.path}`);
      console.log(`     record_time: ${fmt(c.data.record_time)}`);
      console.log(`     after_offset: ${c.data.after || 'N/A'}`);
      if (c.data.migration !== undefined) console.log(`     migration: ${c.data.migration}`);
    } else {
      console.log(`  ❌ ${c.name} — not found`);
    }
    console.log();
  }

  // Check local vs GCS cursor consistency
  if (localLive && gcsLive) {
    const localTs = new Date(localLive.record_time).getTime();
    const gcsTs = new Date(gcsLive.record_time).getTime();
    if (localTs !== gcsTs) {
      console.log(`  ⚠️  LOCAL vs GCS CURSOR MISMATCH!`);
      console.log(`     Local: ${fmt(localLive.record_time)}`);
      console.log(`     GCS:   ${fmt(gcsLive.record_time)}`);
      console.log(`     Drift: ${timeDiff(localLive.record_time, gcsLive.record_time)}`);
      console.log(`     (The more recent one is authoritative)\n`);
    } else {
      console.log(`  ✅ Local and GCS cursors are in sync\n`);
    }
  }

  // ── 2. Scan ALL 4 GCS prefixes ──────────────────────────────
  console.log('═══════════════════════════════════════════════════════════════');
  console.log('  GCS DATA SCAN (all 4 Hive partition paths)');
  console.log('═══════════════════════════════════════════════════════════════\n');

  const exec = (cmd) => execSync(cmd, { stdio: 'pipe', timeout: 15000 }).toString().trim();
  const prefixes = getGCSScanPrefixes(BUCKET);
  const results = [];

  for (const prefix of prefixes) {
    const tag = label(prefix);
    try {
      const result = scanGCSHivePartition(prefix, exec);
      if (result) {
        results.push({ prefix, tag, ...result });
        console.log(`  ✅ ${tag.padEnd(20)} migration=${result.migrationId}  latest=${fmt(result.timestamp)}`);
        if (VERBOSE) console.log(`     source: ${result.source}`);
      } else {
        console.log(`  📭 ${tag.padEnd(20)} (no data found)`);
      }
    } catch (err) {
      console.log(`  ❌ ${tag.padEnd(20)} scan error: ${err.message}`);
    }
  }

  // ── 3. Find overall latest data timestamp ────────────────────
  let latestData = null;
  for (const r of results) {
    if (!latestData || new Date(r.timestamp) > new Date(latestData.timestamp)) {
      latestData = r;
    }
  }

  // ── 4. Summary ───────────────────────────────────────────────
  const effectiveCursor = localLive || gcsLive || backfill;
  const cursorTime = effectiveCursor?.record_time;
  const dataTime = latestData?.timestamp;

  console.log('\n═══════════════════════════════════════════════════════════════');
  console.log('  SUMMARY');
  console.log('═══════════════════════════════════════════════════════════════\n');

  console.log(`  Active cursor:       ${fmt(cursorTime)}`);
  console.log(`  Latest data in GCS:  ${fmt(dataTime)}  (${latestData?.tag || 'none'})`);

  if (cursorTime && dataTime) {
    const cursorMs = new Date(cursorTime).getTime();
    const dataMs = new Date(dataTime).getTime();
    const drift = timeDiff(cursorTime, dataTime);

    if (cursorMs > dataMs) {
      console.log(`\n  Status: ⚠️  Cursor AHEAD of data by ${drift}`);
      console.log('           (Normal — cursor tracks API offset, data may lag slightly)');
    } else if (cursorMs < dataMs) {
      console.log(`\n  Status: 🚨 Cursor BEHIND data by ${drift}`);
      console.log('           (Unusual — cursor should be at or ahead of data)');
      console.log('           This can happen if cursor was not saved on crash.');
      console.log('           May cause DUPLICATE ingestion of recent records.');
    } else {
      console.log('\n  Status: ✅ Cursor matches latest data');
    }
  } else if (!cursorTime && !dataTime) {
    console.log('\n  Status: 🆕 Fresh start — no cursor or data found');
  } else if (!cursorTime) {
    console.log('\n  Status: ⚠️  No cursor but data exists in GCS');
    console.log('           Run fetch-updates.js — it will auto-recover from GCS data');
  } else {
    console.log('\n  Status: 📭 Cursor exists but no data in GCS yet');
  }

  // ── 5. Check events vs updates consistency ───────────────────
  const liveUpdates = results.find(r => r.tag === 'live/updates');
  const liveEvents = results.find(r => r.tag === 'live/events');

  if (liveUpdates && liveEvents) {
    const uMs = new Date(liveUpdates.timestamp).getTime();
    const eMs = new Date(liveEvents.timestamp).getTime();
    if (uMs !== eMs) {
      const drift = timeDiff(liveUpdates.timestamp, liveEvents.timestamp);
      console.log(`\n  ⚠️  Live updates vs events timestamp drift: ${drift}`);
      console.log(`     updates: ${fmt(liveUpdates.timestamp)}`);
      console.log(`     events:  ${fmt(liveEvents.timestamp)}`);
      console.log('     (Small drift is normal; large drift indicates partial write failure)');
    } else {
      console.log('\n  ✅ Live updates and events partitions are in sync');
    }
  }

  console.log('\n═══════════════════════════════════════════════════════════════\n');
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
