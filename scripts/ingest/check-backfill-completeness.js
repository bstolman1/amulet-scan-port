#!/usr/bin/env node
/**
 * Backfill Completeness Checker
 * 
 * Uses DuckDB to count actual rows in GCS Parquet files per day,
 * then compares against the total expected for the migration.
 * 
 * Usage: node check-backfill-completeness.js [--bucket canton-bucket] [--migration 3]
 */

import { execSync } from 'child_process';

const args = process.argv.slice(2);
const migration = args.includes('--migration') ? args[args.indexOf('--migration') + 1] : '3';
const bucket = args.includes('--bucket') ? args[args.indexOf('--bucket') + 1] : process.env.GCS_BUCKET || 'canton-bucket';

// Known M3 expected totals (from migration-info API)
// Update these if you have better numbers
const EXPECTED_EVENTS = {
  '3': 717_000_000,  // ~717M events for M3
};

const expected = EXPECTED_EVENTS[migration];
const gcsPath = `gs://${bucket}/raw/backfill/events/migration=${migration}/**/*.parquet`;

console.log(`\n📊 Backfill Completeness Check — Migration ${migration}`);
console.log(`   GCS path: ${gcsPath}`);
console.log(`   Expected: ~${(expected / 1_000_000).toFixed(0)}M events`);
console.log(`\n⏳ Counting rows via DuckDB (this may take a few minutes)...\n`);

// DuckDB query to count rows per month/day with hive partitioning
const query = `
INSTALL httpfs; LOAD httpfs;
CREATE SECRET (TYPE GCS, PROVIDER CONFIG);

SELECT 
  year,
  month, 
  day,
  COUNT(*) as event_count,
  COUNT(DISTINCT day) as days
FROM read_parquet('${gcsPath}', hive_partitioning=true)
GROUP BY year, month, day
ORDER BY year, month, day;
`;

const summaryQuery = `
INSTALL httpfs; LOAD httpfs;
CREATE SECRET (TYPE GCS, PROVIDER CONFIG);

SELECT 
  COUNT(*) as total_events,
  MIN(year || '-' || LPAD(month::VARCHAR, 2, '0') || '-' || LPAD(day::VARCHAR, 2, '0')) as earliest_day,
  MAX(year || '-' || LPAD(month::VARCHAR, 2, '0') || '-' || LPAD(day::VARCHAR, 2, '0')) as latest_day,
  COUNT(DISTINCT year || '-' || month || '-' || day) as days_covered
FROM read_parquet('${gcsPath}', hive_partitioning=true);
`;

try {
  // First get the summary
  console.log('📈 Overall summary:');
  const summary = execSync(`duckdb -json -c "${summaryQuery.replace(/"/g, '\\"')}"`, {
    encoding: 'utf-8',
    timeout: 600_000, // 10 min timeout
    maxBuffer: 50 * 1024 * 1024,
  });

  const summaryData = JSON.parse(summary);
  if (summaryData.length > 0) {
    const s = summaryData[0];
    const totalEvents = Number(s.total_events);
    const pct = expected ? ((totalEvents / expected) * 100).toFixed(1) : '?';
    
    console.log(`   Total events in GCS:  ${totalEvents.toLocaleString()}`);
    console.log(`   Expected events:      ~${expected ? expected.toLocaleString() : 'unknown'}`);
    console.log(`   Progress:             ${pct}%`);
    console.log(`   Date range covered:   ${s.earliest_day} → ${s.latest_day}`);
    console.log(`   Days with data:       ${s.days_covered}`);
    console.log('');
  }

  // Then per-day breakdown
  console.log('📅 Per-day breakdown:');
  const perDay = execSync(`duckdb -json -c "${query.replace(/"/g, '\\"')}"`, {
    encoding: 'utf-8',
    timeout: 600_000,
    maxBuffer: 50 * 1024 * 1024,
  });

  const dayData = JSON.parse(perDay);
  
  let currentMonth = null;
  let monthTotal = 0;
  
  for (const row of dayData) {
    const month = `${row.year}-${String(row.month).padStart(2, '0')}`;
    if (month !== currentMonth) {
      if (currentMonth) {
        console.log(`   ${currentMonth} total: ${monthTotal.toLocaleString()} events\n`);
      }
      currentMonth = month;
      monthTotal = 0;
    }
    const count = Number(row.event_count);
    monthTotal += count;
    const day = String(row.day).padStart(2, '0');
    const bar = '█'.repeat(Math.min(50, Math.round(count / 100_000)));
    console.log(`   ${month}-${day}: ${count.toLocaleString().padStart(12)} ${bar}`);
  }
  if (currentMonth) {
    console.log(`   ${currentMonth} total: ${monthTotal.toLocaleString()} events`);
  }

} catch (e) {
  console.error('❌ DuckDB query failed:', e.message);
  console.error('\nMake sure:');
  console.error('  1. DuckDB is installed: brew install duckdb / apt install duckdb');
  console.error('  2. GCS auth is set up: gcloud auth application-default login');
  process.exit(1);
}

console.log('');
