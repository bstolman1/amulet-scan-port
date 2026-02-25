#!/usr/bin/env node
/**
 * Check backfill progress by scanning GCS date partitions
 * and comparing against the known M3 date range.
 * 
 * Usage: node check-backfill-progress.js [--migration 3] [--bucket canton-bucket]
 */

import { execSync } from 'child_process';

const args = process.argv.slice(2);
const migration = args.includes('--migration') ? args[args.indexOf('--migration') + 1] : '3';
const bucket = args.includes('--bucket') ? args[args.indexOf('--bucket') + 1] : process.env.GCS_BUCKET || 'canton-bucket';

// Known M3 boundaries (from migration-info)
const MIGRATION_RANGES = {
  '3': { start: '2025-06-25', end: '2025-12-10' },
  '4': { start: '2025-12-10', end: null }, // ongoing
};

const range = MIGRATION_RANGES[migration];
if (!range) {
  console.error(`Unknown migration ${migration}. Known: ${Object.keys(MIGRATION_RANGES).join(', ')}`);
  process.exit(1);
}

// Generate all expected dates in range
function generateDateRange(startStr, endStr) {
  const dates = [];
  const start = new Date(startStr + 'T00:00:00Z');
  const end = new Date(endStr + 'T00:00:00Z');
  for (let d = new Date(start); d <= end; d.setUTCDate(d.getUTCDate() + 1)) {
    dates.push({
      year: d.getUTCFullYear(),
      month: d.getUTCMonth() + 1, // unpadded
      day: d.getUTCDate(),         // unpadded
    });
  }
  return dates;
}

const expectedDates = generateDateRange(range.start, range.end);
console.log(`\n📊 Backfill Progress Check — Migration ${migration}`);
console.log(`   Date range: ${range.start} → ${range.end} (${expectedDates.length} days)\n`);

// Scan GCS for existing partitions
const prefix = `gs://${bucket}/raw/backfill/events/migration=${migration}/`;
console.log(`🔍 Scanning ${prefix} ...`);

let gcsOutput;
try {
  gcsOutput = execSync(`gsutil ls "${prefix}"`, { encoding: 'utf-8', timeout: 30000 });
} catch (e) {
  console.error('Failed to list GCS. Check gsutil auth and bucket name.');
  process.exit(1);
}

// Parse year/month/day from GCS paths
const foundPartitions = new Set();
const yearDirs = gcsOutput.trim().split('\n').filter(Boolean);

for (const yearDir of yearDirs) {
  const yearMatch = yearDir.match(/year=(\d+)/);
  if (!yearMatch) continue;
  const year = yearMatch[1];

  let monthOutput;
  try {
    monthOutput = execSync(`gsutil ls "${yearDir}"`, { encoding: 'utf-8', timeout: 30000 });
  } catch { continue; }

  for (const monthDir of monthOutput.trim().split('\n').filter(Boolean)) {
    const monthMatch = monthDir.match(/month=(\d+)/);
    if (!monthMatch) continue;
    const month = monthMatch[1];

    let dayOutput;
    try {
      dayOutput = execSync(`gsutil ls "${monthDir}"`, { encoding: 'utf-8', timeout: 30000 });
    } catch { continue; }

    for (const dayDir of dayOutput.trim().split('\n').filter(Boolean)) {
      const dayMatch = dayDir.match(/day=(\d+)/);
      if (!dayMatch) continue;
      foundPartitions.add(`${year}-${month}-${dayMatch[1]}`);
    }
  }
}

// Compare
let covered = 0;
const missing = [];

for (const d of expectedDates) {
  const key = `${d.year}-${d.month}-${d.day}`;
  if (foundPartitions.has(key)) {
    covered++;
  } else {
    missing.push(`${d.year}-${String(d.month).padStart(2, '0')}-${String(d.day).padStart(2, '0')}`);
  }
}

const pct = ((covered / expectedDates.length) * 100).toFixed(1);

console.log(`\n✅ Days with data: ${covered} / ${expectedDates.length} (${pct}%)`);

if (missing.length > 0 && missing.length <= 30) {
  console.log(`\n❌ Missing days (${missing.length}):`);
  missing.forEach(d => console.log(`   ${d}`));
} else if (missing.length > 30) {
  console.log(`\n❌ Missing days: ${missing.length}`);
  console.log(`   First: ${missing[0]}`);
  console.log(`   Last:  ${missing[missing.length - 1]}`);
}

// Also get file counts for a few sample days
console.log(`\n📁 Sample file counts:`);
const sampleDays = [expectedDates[0], expectedDates[Math.floor(expectedDates.length / 2)], expectedDates[expectedDates.length - 1]];
for (const d of sampleDays) {
  const dayPath = `${prefix}year=${d.year}/month=${d.month}/day=${d.day}/`;
  try {
    const count = execSync(`gsutil ls "${dayPath}" 2>/dev/null | wc -l`, { encoding: 'utf-8', timeout: 10000 }).trim();
    console.log(`   ${d.year}-${String(d.month).padStart(2, '0')}-${String(d.day).padStart(2, '0')}: ${count} files`);
  } catch {
    console.log(`   ${d.year}-${String(d.month).padStart(2, '0')}-${String(d.day).padStart(2, '0')}: (no data)`);
  }
}

console.log('');
