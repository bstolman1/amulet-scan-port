#!/usr/bin/env node
/**
 * GCS Gap Audit Tool
 * 
 * Walks every day partition in GCS for a given date range and detects:
 *   1. Missing calendar days (no partition exists)
 *   2. Intra-day time gaps between consecutive Parquet files
 *   3. Updates vs Events partition mismatches (one exists but not the other)
 *   4. Empty partitions (directory exists but no Parquet files)
 * 
 * Scans both live (raw/updates/) and backfill (raw/backfill/) paths.
 * 
 * Usage:
 *   node audit-gcs-gaps.js                          # Audit current month
 *   node audit-gcs-gaps.js --month=2 --year=2026    # Audit Feb 2026
 *   node audit-gcs-gaps.js --start=2024-07-01 --end=2025-12-10  # Date range
 *   node audit-gcs-gaps.js --migration=3
 *   node audit-gcs-gaps.js --verbose                # Show per-file details
 *   node audit-gcs-gaps.js --source=updates         # Only scan live data
 *   node audit-gcs-gaps.js --source=backfill        # Only scan backfill data
 */

import { execSync } from 'child_process';
import dotenv from 'dotenv';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
dotenv.config({ path: join(__dirname, '.env') });

// ─────────────────────────────────────────────────────────────
// CLI args
// ─────────────────────────────────────────────────────────────

const args = process.argv.slice(2);

function argVal(name) {
  const idx = args.findIndex(a => a === `--${name}` || a.startsWith(`--${name}=`));
  if (idx === -1) return null;
  const arg = args[idx];
  if (arg.includes('=')) return arg.split('=').slice(1).join('=');
  // Space-separated: --start 2024-07-01
  return (idx + 1 < args.length && !args[idx + 1].startsWith('--')) ? args[idx + 1] : null;
}

const now = new Date();
const START_DATE = argVal('start');  // e.g. 2024-07-01
const END_DATE = argVal('end');      // e.g. 2025-12-10
const TARGET_MIGRATION = argVal('migration') ? parseInt(argVal('migration')) : null;
const VERBOSE = args.includes('--verbose') || args.includes('-v');
const SOURCE_FILTER = argVal('source');
const GAP_THRESHOLD_S = parseInt(argVal('gap-threshold') || '300');
const BUCKET = process.env.GCS_BUCKET || 'canton-bucket';

/**
 * Build list of {year, month, startDay, endDay} objects to audit.
 * Supports --start/--end range or legacy --month/--year single month.
 */
function buildMonthRanges() {
  if (START_DATE && END_DATE) {
    const s = new Date(START_DATE + 'T00:00:00Z');
    const e = new Date(END_DATE + 'T00:00:00Z');
    const ranges = [];
    let cur = new Date(Date.UTC(s.getUTCFullYear(), s.getUTCMonth(), 1));
    while (cur <= e) {
      const y = cur.getUTCFullYear();
      const m = cur.getUTCMonth() + 1;
      const firstDay = (y === s.getUTCFullYear() && m === s.getUTCMonth() + 1) ? s.getUTCDate() : 1;
      const lastOfMonth = new Date(y, m, 0).getDate();
      const lastDay = (y === e.getUTCFullYear() && m === e.getUTCMonth() + 1) ? e.getUTCDate() : lastOfMonth;
      ranges.push({ year: y, month: m, startDay: firstDay, endDay: lastDay });
      cur = new Date(Date.UTC(y, m, 1)); // next month
    }
    return ranges;
  }
  // Legacy single-month mode
  const y = parseInt(argVal('year') || now.getUTCFullYear());
  const m = parseInt(argVal('month') || (now.getUTCMonth() + 1));
  const isCurrentMonth = y === now.getUTCFullYear() && m === (now.getUTCMonth() + 1);
  const lastDay = isCurrentMonth ? now.getUTCDate() : new Date(y, m, 0).getDate();
  return [{ year: y, month: m, startDay: 1, endDay: lastDay }];
}

// ─────────────────────────────────────────────────────────────
// GCS helpers
// ─────────────────────────────────────────────────────────────

function exec(cmd) {
  try {
    return execSync(cmd, { stdio: 'pipe', timeout: 30000, maxBuffer: 10 * 1024 * 1024 }).toString().trim();
  } catch (err) {
    if (err.stderr?.toString().includes('CommandException') ||
        err.stderr?.toString().includes('No URLs matched') ||
        err.message?.includes('CommandException') ||
        err.message?.includes('No URLs matched')) {
      return '';
    }
    throw err;
  }
}

function gsutilLs(prefix) {
  const output = exec(`gsutil ls "${prefix}" 2>/dev/null || true`);
  if (!output) return [];
  return output.split('\n').filter(l => l.trim().length > 0);
}

// ─────────────────────────────────────────────────────────────
// Timestamp extraction from Parquet filenames
// ─────────────────────────────────────────────────────────────

/**
 * Extract ISO timestamp from Parquet filename.
 * Expected: updates_2026-02-02T15-30-00.000000Z.parquet
 *      or:  events_2026-02-02T15-30-00.000000Z.parquet
 */
function extractTimestamp(filename) {
  const match = filename.match(/(\d{4}-\d{2}-\d{2}T[\d-]+\.\d+Z)/);
  if (!match) return null;
  // Convert filename dashes back to colons: 15-30-00 → 15:30:00
  const ts = match[1].replace(/(\d{2})-(\d{2})-(\d{2})\./, '$1:$2:$3.');
  return ts;
}

/**
 * Parse all Parquet files in a day partition, extract and sort timestamps.
 */
function getFilesWithTimestamps(dayPath) {
  const lines = gsutilLs(dayPath);
  const files = lines.filter(f => f.endsWith('.parquet'));
  
  const parsed = files.map(f => {
    const ts = extractTimestamp(f);
    return { path: f, timestamp: ts, epochMs: ts ? new Date(ts).getTime() : null };
  }).filter(f => f.epochMs !== null);
  
  parsed.sort((a, b) => a.epochMs - b.epochMs);
  return parsed;
}

// ─────────────────────────────────────────────────────────────
// Calendar helpers
// ─────────────────────────────────────────────────────────────

function daysInMonth(year, month) {
  return new Date(year, month, 0).getDate();
}

function formatDate(y, m, d) {
  return `${y}-${String(m).padStart(2, '0')}-${String(d).padStart(2, '0')}`;
}

function formatDuration(ms) {
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${s % 60}s`;
  const h = Math.floor(m / 60);
  return `${h}h ${m % 60}m`;
}

// ─────────────────────────────────────────────────────────────
// Core audit logic
// ─────────────────────────────────────────────────────────────

/**
 * Discover which migrations exist under a given GCS source prefix.
 * Returns sorted descending (newest first).
 */
function discoverMigrations(sourcePrefix) {
  const lines = gsutilLs(sourcePrefix);
  const migrations = [];
  for (const l of lines) {
    const match = l.match(/migration=(\d+)/);
    if (match) migrations.push({ path: l.trim(), id: parseInt(match[1]) });
  }
  migrations.sort((a, b) => b.id - a.id);
  return migrations;
}

/**
 * Audit a single type (updates or events) under a migration for the target month.
 * Returns { daysFound, dayResults, missingDays }.
 */
function auditMigrationMonth(migPath, migrationId, type, range) {
  const { year, month, startDay, endDay } = range;
  const dayResults = [];
  const missingDays = [];
  
  // Check which day partitions exist for this month
  // Path: migPath/year=YYYY/month=M/
  const monthPrefix = `${migPath}year=${year}/month=${month}/`;
  const dayLines = gsutilLs(monthPrefix);
  
  // Parse existing day partitions
  const existingDays = new Set();
  const dayPaths = {};
  for (const l of dayLines) {
    const match = l.match(/day=(\d+)/);
    if (match) {
      const d = parseInt(match[1]);
      existingDays.add(d);
      dayPaths[d] = l.trim();
    }
  }
  
  // Check each calendar day in range
  for (let d = startDay; d <= endDay; d++) {
    const dateStr = formatDate(year, month, d);
    
    if (!existingDays.has(d)) {
      missingDays.push(d);
      dayResults.push({ day: d, date: dateStr, status: 'missing', fileCount: 0, gaps: [] });
      continue;
    }
    
    // Scan files in this day partition
    const files = getFilesWithTimestamps(dayPaths[d]);
    
    if (files.length === 0) {
      dayResults.push({ day: d, date: dateStr, status: 'empty', fileCount: 0, gaps: [] });
      continue;
    }
    
    // Detect intra-day gaps
    const gaps = [];
    for (let i = 1; i < files.length; i++) {
      const prev = files[i - 1];
      const curr = files[i];
      const gapMs = curr.epochMs - prev.epochMs;
      
      if (gapMs > GAP_THRESHOLD_S * 1000) {
        gaps.push({
          afterFile: prev.path.split('/').pop(),
          beforeFile: curr.path.split('/').pop(),
          afterTs: prev.timestamp,
          beforeTs: curr.timestamp,
          gapMs,
          gapFormatted: formatDuration(gapMs),
        });
      }
    }
    
    const earliest = files[0].timestamp;
    const latest = files[files.length - 1].timestamp;
    
    dayResults.push({
      day: d,
      date: dateStr,
      status: gaps.length > 0 ? 'gaps' : 'ok',
      fileCount: files.length,
      earliest,
      latest,
      gaps,
    });
  }
  
  return { daysFound: existingDays.size, dayResults, missingDays, lastDayChecked: endDay };
}

// ─────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────

function main() {
  const ranges = buildMonthRanges();
  const rangeLabel = START_DATE && END_DATE
    ? `${START_DATE} → ${END_DATE}`
    : (() => {
        const r = ranges[0];
        const monthName = new Date(r.year, r.month - 1).toLocaleString('en', { month: 'long' });
        return `${monthName} ${r.year}`;
      })();
  
  console.log('═══════════════════════════════════════════════════════════════');
  console.log(`  GCS GAP AUDIT — ${rangeLabel}`);
  console.log(`  Bucket: gs://${BUCKET}`);
  console.log(`  Gap threshold: ${GAP_THRESHOLD_S}s`);
  if (TARGET_MIGRATION !== null) console.log(`  Migration filter: ${TARGET_MIGRATION}`);
  if (SOURCE_FILTER) console.log(`  Source filter: ${SOURCE_FILTER}`);
  console.log('═══════════════════════════════════════════════════════════════\n');
  
  const sources = SOURCE_FILTER 
    ? [SOURCE_FILTER] 
    : ['updates', 'backfill'];
  const types = ['updates', 'events'];
  
  let totalGaps = 0;
  let totalMissingDays = 0;
  let totalEmptyDays = 0;
  const summaryRows = [];
  
  for (const source of sources) {
    for (const type of types) {
      const sourcePrefix = `gs://${BUCKET}/raw/${source}/${type}/`;
      const tag = `${source}/${type}`;
      
      console.log(`─── Scanning ${tag} ───`);
      
      // Discover migrations
      const migrations = discoverMigrations(sourcePrefix);
      
      if (migrations.length === 0) {
        console.log(`  📭 No migrations found\n`);
        continue;
      }
      
      // Filter to target migration if specified
      const migsToScan = TARGET_MIGRATION !== null 
        ? migrations.filter(m => m.id === TARGET_MIGRATION) 
        : migrations;
      
      if (migsToScan.length === 0) {
        console.log(`  📭 Migration ${TARGET_MIGRATION} not found (available: ${migrations.map(m => m.id).join(', ')})\n`);
        continue;
      }
      
      for (const mig of migsToScan) {
        console.log(`\n  📂 migration=${mig.id}`);
        
        let migOk = 0, migGaps = 0, migMissing = 0, migEmpty = 0, migIntraGaps = 0, migChecked = 0;
        
        for (const range of ranges) {
          const result = auditMigrationMonth(mig.path, mig.id, type, range);
          
          const okDays = result.dayResults.filter(d => d.status === 'ok').length;
          const gapDays = result.dayResults.filter(d => d.status === 'gaps').length;
          const missingDays = result.dayResults.filter(d => d.status === 'missing').length;
          const emptyDays = result.dayResults.filter(d => d.status === 'empty').length;
          const dayGaps = result.dayResults.reduce((sum, d) => sum + d.gaps.length, 0);
          
          totalGaps += dayGaps;
          totalMissingDays += missingDays;
          totalEmptyDays += emptyDays;
          migOk += okDays;
          migGaps += gapDays;
          migMissing += missingDays;
          migEmpty += emptyDays;
          migIntraGaps += dayGaps;
          migChecked += result.lastDayChecked;
          
          // Print day-by-day results
          for (const day of result.dayResults) {
            if (day.status === 'ok') {
              if (VERBOSE) {
                console.log(`     ✅ ${day.date}  ${day.fileCount} files  [${day.earliest} → ${day.latest}]`);
              }
            } else if (day.status === 'missing') {
              console.log(`     ❌ ${day.date}  MISSING — no partition exists`);
            } else if (day.status === 'empty') {
              console.log(`     ⚠️  ${day.date}  EMPTY — partition exists but no .parquet files`);
            } else if (day.status === 'gaps') {
              console.log(`     ⚠️  ${day.date}  ${day.fileCount} files, ${day.gaps.length} gap(s):`);
              for (const gap of day.gaps) {
                console.log(`        🕳️  ${gap.gapFormatted} gap: ${gap.afterTs} → ${gap.beforeTs}`);
                if (VERBOSE) {
                  console.log(`           after: ${gap.afterFile}`);
                  console.log(`           before: ${gap.beforeFile}`);
                }
              }
            }
          }
        }
        
        // Compact summary for this migration
        if (!VERBOSE && migOk > 0) {
          console.log(`     ✅ ${migOk} day(s) OK (use --verbose to see details)`);
        }
        
        summaryRows.push({
          path: `${tag}/migration=${mig.id}`,
          checked: migChecked,
          ok: migOk,
          gaps: migGaps,
          missing: migMissing,
          empty: migEmpty,
          intraGaps: migIntraGaps,
        });
      }
      console.log();
    }
  }
  
  // ── Cross-check: updates vs events partition alignment ─────
  console.log('═══════════════════════════════════════════════════════════════');
  console.log('  UPDATES vs EVENTS PARTITION ALIGNMENT');
  console.log('═══════════════════════════════════════════════════════════════\n');
  
  for (const source of sources) {
    const updatesPrefix = `gs://${BUCKET}/raw/${source}/updates/`;
    const eventsPrefix = `gs://${BUCKET}/raw/${source}/events/`;
    
    const updateMigs = discoverMigrations(updatesPrefix);
    const eventMigs = discoverMigrations(eventsPrefix);
    
    if (updateMigs.length === 0 && eventMigs.length === 0) continue;
    
    const migsToCheck = TARGET_MIGRATION !== null 
      ? [TARGET_MIGRATION] 
      : [...new Set([...updateMigs.map(m => m.id), ...eventMigs.map(m => m.id)])];
    
    for (const migId of migsToCheck) {
      const uMig = updateMigs.find(m => m.id === migId);
      const eMig = eventMigs.find(m => m.id === migId);
      
      if (!uMig || !eMig) {
        console.log(`  ⚠️  ${source}/migration=${migId}: ${!uMig ? 'updates MISSING' : 'events MISSING'}`);
        continue;
      }
      
      // Compare day partitions across all months in range
      let totalUDays = 0, totalEDays = 0, allUOnly = [], allEOnly = [];
      for (const range of ranges) {
        const monthPrefixU = `${uMig.path}year=${range.year}/month=${range.month}/`;
        const monthPrefixE = `${eMig.path}year=${range.year}/month=${range.month}/`;
        
        const uDays = new Set(gsutilLs(monthPrefixU)
          .map(l => l.match(/day=(\d+)/)?.[1])
          .filter(Boolean)
          .map(Number));
        const eDays = new Set(gsutilLs(monthPrefixE)
          .map(l => l.match(/day=(\d+)/)?.[1])
          .filter(Boolean)
          .map(Number));
        
        totalUDays += uDays.size;
        totalEDays += eDays.size;
        const uOnly = [...uDays].filter(d => !eDays.has(d)).map(d => formatDate(range.year, range.month, d));
        const eOnly = [...eDays].filter(d => !uDays.has(d)).map(d => formatDate(range.year, range.month, d));
        allUOnly.push(...uOnly);
        allEOnly.push(...eOnly);
      }
      
      if (allUOnly.length === 0 && allEOnly.length === 0) {
        console.log(`  ✅ ${source}/migration=${migId}: updates and events aligned (${totalUDays} day partitions)`);
      } else {
        if (allUOnly.length > 0) {
          console.log(`  ⚠️  ${source}/migration=${migId}: days in updates but NOT events: ${allUOnly.join(', ')}`);
        }
        if (allEOnly.length > 0) {
          console.log(`  ⚠️  ${source}/migration=${migId}: days in events but NOT updates: ${allEOnly.join(', ')}`);
        }
      }
    }
  }
  
  // ── Final summary ──────────────────────────────────────────
  console.log('\n═══════════════════════════════════════════════════════════════');
  console.log(`  SUMMARY — ${rangeLabel}`);
  console.log('═══════════════════════════════════════════════════════════════\n');
  
  console.log('  Path                                  Days  OK  Gaps  Missing  Empty  IntraGaps');
  console.log('  ' + '─'.repeat(85));
  
  for (const r of summaryRows) {
    console.log(
      `  ${r.path.padEnd(40)} ${String(r.checked).padStart(4)}  ` +
      `${String(r.ok).padStart(2)}  ${String(r.gaps).padStart(4)}  ` +
      `${String(r.missing).padStart(7)}  ${String(r.empty).padStart(5)}  ` +
      `${String(r.intraGaps).padStart(9)}`
    );
  }
  
  console.log();
  
  if (totalGaps === 0 && totalMissingDays === 0 && totalEmptyDays === 0) {
    console.log('  ✅ No gaps detected — all day partitions present and continuous');
  } else {
    if (totalMissingDays > 0) console.log(`  ❌ ${totalMissingDays} missing day partition(s)`);
    if (totalEmptyDays > 0)   console.log(`  ⚠️  ${totalEmptyDays} empty day partition(s)`);
    if (totalGaps > 0)        console.log(`  ⚠️  ${totalGaps} intra-day time gap(s) exceeding ${GAP_THRESHOLD_S}s`);
    console.log('\n  To investigate further, run with --verbose');
  }
  
  console.log('\n═══════════════════════════════════════════════════════════════\n');
  
  // Exit with error code if gaps found
  if (totalGaps > 0 || totalMissingDays > 0) {
    process.exit(1);
  }
}

main();
