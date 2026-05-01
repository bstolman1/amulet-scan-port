#!/usr/bin/env node
/**
 * Quick partition coverage check: lists all day partitions under
 * raw/updates/updates/migration=N/ and reports any gaps vs the expected
 * date range. Uses @google-cloud/storage SDK (ADC) — no gsutil needed.
 *
 * Usage:
 *   node check-partition-coverage.js
 *   node check-partition-coverage.js --migration=0
 */

import { Storage } from '@google-cloud/storage';

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

const targetMig = process.argv.find(a => a.startsWith('--migration='));
const filterMig = targetMig ? parseInt(targetMig.split('=')[1]) : null;

function buildDateRange(startStr, endStr) {
  const dates = [];
  const start = new Date(startStr + 'T00:00:00Z');
  const end = new Date(endStr + 'T00:00:00Z');
  for (let d = new Date(start); d <= end; d.setUTCDate(d.getUTCDate() + 1)) {
    dates.push({
      str: d.toISOString().split('T')[0],
      year: d.getUTCFullYear(),
      month: d.getUTCMonth() + 1,
      day: d.getUTCDate(),
    });
  }
  return dates;
}

async function listDayPartitions(migrationId) {
  const prefix = `raw/updates/updates/migration=${migrationId}/`;
  const [files] = await bucket.getFiles({ prefix, delimiter: '/' });
  // getFiles with delimiter returns prefixes (subdirectories) via apiResponse
  // but we need to list deeper. Instead, list all files and extract unique day paths.
  const [allFiles] = await bucket.getFiles({ prefix, maxResults: 10000 });

  const days = new Set();
  for (const f of allFiles) {
    const match = f.name.match(/year=(\d+)\/month=(\d+)\/day=(\d+)\//);
    if (match) {
      const y = parseInt(match[1]);
      const m = parseInt(match[2]);
      const d = parseInt(match[3]);
      const dateStr = `${y}-${String(m).padStart(2, '0')}-${String(d).padStart(2, '0')}`;
      days.add(dateStr);
    }
  }
  return days;
}

async function main() {
  const migrations = filterMig !== null
    ? MIGRATIONS.filter(m => m.id === filterMig)
    : MIGRATIONS;

  for (const mig of migrations) {
    console.log(`\n${'═'.repeat(60)}`);
    console.log(`  Migration ${mig.id}  (${mig.start} → ${mig.end})`);
    console.log('═'.repeat(60));

    const expected = buildDateRange(mig.start, mig.end);
    const existing = await listDayPartitions(mig.id);

    const missing = [];
    const found = [];
    for (const d of expected) {
      if (existing.has(d.str)) {
        found.push(d.str);
      } else {
        missing.push(d.str);
      }
    }

    const extra = [...existing].filter(d => !expected.some(e => e.str === d)).sort();

    console.log(`  Expected days:  ${expected.length}`);
    console.log(`  Found in GCS:   ${found.length}`);
    console.log(`  Missing:        ${missing.length}`);
    if (extra.length > 0) {
      console.log(`  Extra (outside range): ${extra.length}`);
    }

    if (missing.length > 0) {
      console.log(`\n  Missing days:`);
      for (const d of missing) {
        console.log(`    ${d}`);
      }
    } else {
      console.log(`\n  ✅ All days covered`);
    }

    if (extra.length > 0) {
      console.log(`\n  Extra days (outside expected range):`);
      for (const d of extra) {
        console.log(`    ${d}`);
      }
    }
  }

  console.log('\n');
}

main().catch(err => {
  console.error(`FATAL: ${err.message}`);
  process.exit(1);
});
