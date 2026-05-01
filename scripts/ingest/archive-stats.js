#!/usr/bin/env node
/**
 * Archive size & row count summary across all migrations.
 *
 * Reports per-migration:
 *   - Total file count (updates + events)
 *   - Total bytes (raw Parquet on disk)
 *   - Estimated row counts (from verify state files where available)
 *
 * Uses @google-cloud/storage SDK (ADC). For DuckDB-based exact row counts,
 * use verify-scan-completeness.js — this script reports file-level metrics.
 *
 * Usage:
 *   node archive-stats.js
 */

import { Storage } from '@google-cloud/storage';
import { existsSync, readFileSync } from 'fs';
import { homedir } from 'os';
import { join } from 'path';

const BUCKET = process.env.GCS_BUCKET || 'canton-bucket';
const storage = new Storage();
const bucket = storage.bucket(BUCKET);

const MIGRATIONS = [
  { id: 0, start: '2024-06-24', end: '2024-10-16' },
  { id: 1, start: '2024-10-16', end: '2024-12-11' },
  { id: 2, start: '2024-12-11', end: '2025-06-25' },
  { id: 3, start: '2025-06-25', end: '2025-12-10' },
  { id: 4, start: '2025-12-10', end: '2026-03-02' },
];

function fmtBytes(n) {
  if (n < 1024) return `${n} B`;
  const units = ['KiB', 'MiB', 'GiB', 'TiB'];
  let v = n / 1024;
  for (const u of units) {
    if (v < 1024) return `${v.toFixed(2)} ${u}`;
    v /= 1024;
  }
  return `${v.toFixed(2)} PiB`;
}

async function sumPrefix(prefix) {
  let count = 0;
  let bytes = 0;
  let pageToken = undefined;
  do {
    const [files, nextQuery] = await bucket.getFiles({
      prefix,
      autoPaginate: false,
      maxResults: 1000,
      pageToken,
    });
    for (const f of files) {
      count++;
      bytes += parseInt(f.metadata.size || 0);
    }
    pageToken = nextQuery?.pageToken;
  } while (pageToken);
  return { count, bytes };
}

function readVerifiedUpdates(migrationId) {
  const state = join(homedir(), `remediation-m${migrationId}.ndjson`);
  if (!existsSync(state)) return null;
  let total = 0;
  let dayCount = 0;
  for (const line of readFileSync(state, 'utf8').split('\n')) {
    const t = line.trim();
    if (!t) continue;
    try {
      const r = JSON.parse(t);
      if (r.phase === 'verify' && r.status === 'ok' && typeof r.scan === 'number') {
        total += r.scan;
        dayCount += 1;
      }
    } catch {}
  }
  return { total, dayCount };
}

async function main() {
  const totals = { files: 0, bytes: 0, updateRecords: 0 };
  const rows = [];

  console.log('Scanning GCS… (this may take a minute)\n');

  for (const mig of MIGRATIONS) {
    const updatesPrefix = `raw/updates/updates/migration=${mig.id}/`;
    const eventsPrefix  = `raw/updates/events/migration=${mig.id}/`;

    const [u, e] = await Promise.all([
      sumPrefix(updatesPrefix),
      sumPrefix(eventsPrefix),
    ]);
    const verified = readVerifiedUpdates(mig.id);

    rows.push({
      mig: mig.id,
      range: `${mig.start} → ${mig.end}`,
      updateFiles: u.count,
      eventFiles:  e.count,
      bytes:       u.bytes + e.bytes,
      verifiedUpdates: verified ? verified.total : null,
      verifiedDays:    verified ? verified.dayCount : null,
    });

    totals.files += u.count + e.count;
    totals.bytes += u.bytes + e.bytes;
    if (verified) totals.updateRecords += verified.total;
  }

  console.log('Migration  Range                       Files (u/e)         Size           Updates verified');
  console.log('─────────  ──────────────────────────  ──────────────────  ─────────────  ────────────────');
  for (const r of rows) {
    const filesStr = `${r.updateFiles.toLocaleString()} / ${r.eventFiles.toLocaleString()}`;
    const updStr = r.verifiedUpdates != null
      ? `${r.verifiedUpdates.toLocaleString()} (${r.verifiedDays}d)`
      : '—';
    console.log(
      `M${r.mig}         ${r.range.padEnd(26)}  ${filesStr.padEnd(18)}  ${fmtBytes(r.bytes).padEnd(13)}  ${updStr}`
    );
  }
  console.log('─────────  ──────────────────────────  ──────────────────  ─────────────  ────────────────');
  console.log(`Total                                  ${totals.files.toLocaleString()} files          ${fmtBytes(totals.bytes).padEnd(13)}  ${totals.updateRecords.toLocaleString()} updates`);
  console.log();
}

main().catch(err => {
  console.error(`FATAL: ${err.message}`);
  process.exit(1);
});
