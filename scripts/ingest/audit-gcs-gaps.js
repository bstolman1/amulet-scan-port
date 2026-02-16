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
const MERGED = args.includes('--merged') || (START_DATE && END_DATE && !args.includes('--per-migration'));
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

function exec(cmd, opts = {}) {
  try {
    return execSync(cmd, { stdio: 'pipe', timeout: opts.timeout || 30000, maxBuffer: 50 * 1024 * 1024 }).toString().trim();
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

/**
 * Recursively list all .parquet files under a migration prefix in ONE gsutil call.
 * Returns a Map<string, string[]> keyed by "year/month/day" → [file paths].
 */
function bulkListParquet(migPath) {
  process.stderr.write(`    ⏳ Listing all files under ${migPath.replace(`gs://${BUCKET}/`, '')}...`);
  const output = exec(`gsutil ls -r "${migPath}**/*.parquet" 2>/dev/null || true`, { timeout: 120000 });
  if (!output) {
    process.stderr.write(' 0 files\n');
    return new Map();
  }
  const lines = output.split('\n').filter(l => l.endsWith('.parquet'));
  process.stderr.write(` ${lines.length} files\n`);
  
  // Index by year/month/day
  const index = new Map();
  for (const line of lines) {
    const m = line.match(/year=(\d+)\/month=(\d+)\/day=(\d+)\//);
    if (!m) continue;
    const key = `${m[1]}/${m[2]}/${m[3]}`;
    if (!index.has(key)) index.set(key, []);
    index.get(key).push(line.trim());
  }
  return index;
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
 * Parse file paths, extract and sort timestamps.
 */
function parseFilesWithTimestamps(filePaths) {
  const parsed = filePaths.map(f => {
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
 * Audit a migration using a pre-built file index (from bulkListParquet).
 * Checks all calendar days in the given ranges.
 * Returns array of { day, date, status, fileCount, gaps, earliest, latest }.
 */
function auditMigrationFromIndex(fileIndex, ranges) {
  const dayResults = [];
  
  for (const range of ranges) {
    const { year, month, startDay, endDay } = range;
    
    for (let d = startDay; d <= endDay; d++) {
      const dateStr = formatDate(year, month, d);
      const key = `${year}/${month}/${d}`;
      const filePaths = fileIndex.get(key);
      
      if (!filePaths || filePaths.length === 0) {
        // Check if it's truly missing vs empty — if key exists with 0 files it's empty
        dayResults.push({ day: d, date: dateStr, status: filePaths ? 'empty' : 'missing', fileCount: 0, gaps: [] });
        continue;
      }
      
      const files = parseFilesWithTimestamps(filePaths);
      
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
  }
  
  return dayResults;
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
  if (MERGED) console.log(`  Mode: MERGED (any migration covers a day → OK)`);
  console.log('═══════════════════════════════════════════════════════════════\n');
  
  const sources = SOURCE_FILTER 
    ? [SOURCE_FILTER] 
    : ['updates', 'backfill'];
  const types = ['updates', 'events'];
  
  let totalGaps = 0;
  let totalMissingDays = 0;
  let totalEmptyDays = 0;
  const summaryRows = [];
  
  // Cache bulk indexes per migration path to reuse for alignment check
  const bulkIndexCache = new Map();
  
  for (const source of sources) {
    for (const type of types) {
      const sourcePrefix = `gs://${BUCKET}/raw/${source}/${type}/`;
      const tag = `${source}/${type}`;
      
      console.log(`─── Scanning ${tag} ───`);
      
      const migrations = discoverMigrations(sourcePrefix);
      
      if (migrations.length === 0) {
        console.log(`  📭 No migrations found\n`);
        continue;
      }
      
      const migsToScan = TARGET_MIGRATION !== null 
        ? migrations.filter(m => m.id === TARGET_MIGRATION) 
        : migrations;
      
      if (migsToScan.length === 0) {
        console.log(`  📭 Migration ${TARGET_MIGRATION} not found (available: ${migrations.map(m => m.id).join(', ')})\n`);
        continue;
      }
      
      console.log(`  📂 Migrations found: ${migsToScan.map(m => m.id).join(', ')}\n`);
      
      // Bulk-list all files per migration (ONE gsutil call each)
      const migIndexes = new Map();
      for (const mig of migsToScan) {
        const idx = bulkListParquet(mig.path);
        migIndexes.set(mig.id, idx);
        bulkIndexCache.set(`${source}/${type}/migration=${mig.id}`, idx);
      }
      
      if (MERGED) {
        // ── MERGED MODE ──
        const merged = new Map();
        
        for (const mig of migsToScan) {
          const dayResults = auditMigrationFromIndex(migIndexes.get(mig.id), ranges);
          for (const day of dayResults) {
            const existing = merged.get(day.date);
            const rank = { ok: 3, gaps: 2, empty: 1, missing: 0 };
            if (!existing || rank[day.status] > rank[existing.status]) {
              merged.set(day.date, { ...day, fromMig: mig.id });
            }
          }
        }
        
        const sorted = [...merged.entries()].sort((a, b) => a[0].localeCompare(b[0]));
        let okCount = 0, gapCount = 0, missingCount = 0, emptyCount = 0, intraGaps = 0;
        
        for (const [dateStr, day] of sorted) {
          if (day.status === 'ok') {
            okCount++;
            if (VERBOSE) {
              console.log(`     ✅ ${dateStr}  mig=${day.fromMig}  ${day.fileCount} files  [${day.earliest} → ${day.latest}]`);
            }
          } else if (day.status === 'missing') {
            missingCount++;
            console.log(`     ❌ ${dateStr}  MISSING — no migration has data`);
          } else if (day.status === 'empty') {
            emptyCount++;
            console.log(`     ⚠️  ${dateStr}  EMPTY — partition exists (mig=${day.fromMig}) but no .parquet files`);
          } else if (day.status === 'gaps') {
            gapCount++;
            intraGaps += day.gaps.length;
            console.log(`     ⚠️  ${dateStr}  mig=${day.fromMig}  ${day.fileCount} files, ${day.gaps.length} gap(s):`);
            for (const gap of day.gaps) {
              console.log(`        🕳️  ${gap.gapFormatted} gap: ${gap.afterTs} → ${gap.beforeTs}`);
            }
          }
        }
        
        if (!VERBOSE && okCount > 0) {
          console.log(`     ✅ ${okCount} day(s) OK (use --verbose to see details)`);
        }
        
        totalGaps += intraGaps;
        totalMissingDays += missingCount;
        totalEmptyDays += emptyCount;
        
        summaryRows.push({
          path: `${tag} (merged)`,
          checked: sorted.length,
          ok: okCount,
          gaps: gapCount,
          missing: missingCount,
          empty: emptyCount,
          intraGaps,
        });
      } else {
        // ── PER-MIGRATION MODE ──
        for (const mig of migsToScan) {
          console.log(`\n  📂 migration=${mig.id}`);
          
          const dayResults = auditMigrationFromIndex(migIndexes.get(mig.id), ranges);
          
          let migOk = 0, migGaps = 0, migMissing = 0, migEmpty = 0, migIntraGaps = 0;
          
          for (const day of dayResults) {
            if (day.status === 'ok') {
              migOk++;
              if (VERBOSE) {
                console.log(`     ✅ ${day.date}  ${day.fileCount} files  [${day.earliest} → ${day.latest}]`);
              }
            } else if (day.status === 'missing') {
              migMissing++;
              console.log(`     ❌ ${day.date}  MISSING — no partition exists`);
            } else if (day.status === 'empty') {
              migEmpty++;
              console.log(`     ⚠️  ${day.date}  EMPTY — partition exists but no .parquet files`);
            } else if (day.status === 'gaps') {
              migGaps++;
              migIntraGaps += day.gaps.length;
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
          
          totalGaps += migIntraGaps;
          totalMissingDays += migMissing;
          totalEmptyDays += migEmpty;
          
          if (!VERBOSE && migOk > 0) {
            console.log(`     ✅ ${migOk} day(s) OK (use --verbose to see details)`);
          }
          
          summaryRows.push({
            path: `${tag}/migration=${mig.id}`,
            checked: dayResults.length,
            ok: migOk,
            gaps: migGaps,
            missing: migMissing,
            empty: migEmpty,
            intraGaps: migIntraGaps,
          });
        }
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
      // Use cached indexes if available, otherwise discover from bulk listing
      const uKey = `${source}/updates/migration=${migId}`;
      const eKey = `${source}/events/migration=${migId}`;
      const uIndex = bulkIndexCache.get(uKey);
      const eIndex = bulkIndexCache.get(eKey);
      
      if (!uIndex && !eIndex) {
        console.log(`  ⚠️  ${source}/migration=${migId}: no data indexed`);
        continue;
      }
      
      const uDayKeys = uIndex ? new Set(uIndex.keys()) : new Set();
      const eDayKeys = eIndex ? new Set(eIndex.keys()) : new Set();
      
      const uOnly = [...uDayKeys].filter(k => !eDayKeys.has(k)).sort();
      const eOnly = [...eDayKeys].filter(k => !uDayKeys.has(k)).sort();
      
      if (uOnly.length === 0 && eOnly.length === 0) {
        console.log(`  ✅ ${source}/migration=${migId}: updates and events aligned (${uDayKeys.size} day partitions)`);
      } else {
        if (uOnly.length > 0) {
          console.log(`  ⚠️  ${source}/migration=${migId}: ${uOnly.length} day(s) in updates but NOT events`);
          if (VERBOSE) console.log(`       ${uOnly.join(', ')}`);
        }
        if (eOnly.length > 0) {
          console.log(`  ⚠️  ${source}/migration=${migId}: ${eOnly.length} day(s) in events but NOT updates`);
          if (VERBOSE) console.log(`       ${eOnly.join(', ')}`);
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
  
  if (totalGaps > 0 || totalMissingDays > 0) {
    process.exit(1);
  }
}

main();
