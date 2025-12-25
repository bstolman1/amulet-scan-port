#!/usr/bin/env node
/**
 * Migration 4 Data Completeness Validation Script
 * 
 * Validates that data from migration 4 (Dec 10, 2025) to current date is complete.
 * 
 * Checks performed:
 * 1. Time coverage - verifies continuous data from Dec 10, 2025 to now
 * 2. Gap detection - identifies missing time ranges
 * 3. File integrity - validates all data files can be read
 * 4. Daily coverage - checks each day has data
 * 5. Record counts - verifies expected data volume per day
 * 
 * Usage:
 *   node validate-migration-4.js                    # Quick validation (filename-based)
 *   node validate-migration-4.js --full             # Full decode validation (slow)
 *   node validate-migration-4.js --verbose          # Detailed output
 *   node validate-migration-4.js --end-date 2025-12-20  # Custom end date
 *   node validate-migration-4.js --sample 100       # Sample N files for stats
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Configuration
const BASE_DATA_DIR = process.env.DATA_DIR || 'C:\\ledger_raw';
const RAW_DIR = process.env.RAW_DIR || path.join(BASE_DATA_DIR, 'raw');
const CURSOR_DIR = process.env.CURSOR_DIR || path.join(BASE_DATA_DIR, 'cursors');

// Migration 4 boundaries
const MIGRATION_ID = 4;
const MIGRATION_START = new Date('2025-12-10T00:00:00Z');
const GAP_THRESHOLD_MS = parseInt(process.env.GAP_THRESHOLD_MS) || 300000; // 5 minutes

// Parse CLI args
const args = process.argv.slice(2);
const fullMode = args.includes('--full');
const verbose = args.includes('--verbose') || args.includes('-v');
const sampleSize = args.includes('--sample') 
  ? parseInt(args[args.indexOf('--sample') + 1]) 
  : 50;
const customEndDate = args.includes('--end-date') 
  ? new Date(args[args.indexOf('--end-date') + 1] + 'T23:59:59Z')
  : new Date();

// Lazy load heavy dependencies only when needed
let readBinaryFile, getFileStats;
async function loadBinaryReader() {
  if (!readBinaryFile) {
    const module = await import('./read-binary.js');
    readBinaryFile = module.readBinaryFile;
    getFileStats = module.getFileStats;
  }
}

/**
 * Results accumulator
 */
const results = {
  startTime: Date.now(),
  migrationId: MIGRATION_ID,
  expectedRange: {
    start: MIGRATION_START.toISOString(),
    end: customEndDate.toISOString(),
  },
  // File stats
  files: {
    updates: { total: 0, valid: 0, corrupted: 0, records: 0 },
    events: { total: 0, valid: 0, corrupted: 0, records: 0 },
  },
  // Time coverage
  coverage: {
    actualStart: null,
    actualEnd: null,
    expectedDays: 0,
    coveredDays: 0,
    missingDays: [],
    coveragePercent: 0,
  },
  // Gap detection
  gaps: [],
  totalGapTime: 0,
  // Daily breakdown
  dailyStats: {},
  // Errors
  errors: [],
};

/**
 * Find all data files for migration 4
 */
function findMigration4Files() {
  const files = { updates: [], events: [] };
  
  if (!fs.existsSync(RAW_DIR)) {
    console.log(`⚠️ No raw data directory found: ${RAW_DIR}`);
    return files;
  }
  
  function scanDir(dir) {
    let entries;
    try {
      entries = fs.readdirSync(dir, { withFileTypes: true });
    } catch (err) {
      return;
    }
    
    for (const entry of entries) {
      const fullPath = path.join(dir, entry.name);
      
      if (entry.isDirectory()) {
        // Check for migration directory or partition
        const isMigration4Dir = entry.name === 'migration-4' || 
                               entry.name === 'migration_4' ||
                               entry.name === 'migration=4';
        const isMigrationDir = entry.name.match(/migration[-_=]?(\d+)/i);
        
        if (isMigrationDir && !isMigration4Dir) {
          // Skip other migrations
          continue;
        }
        
        scanDir(fullPath);
      } else if (entry.isFile() && entry.name.endsWith('.pb.zst')) {
        // Check if filename contains migration-4 marker
        if (entry.name.includes('mig-4') || 
            entry.name.includes('migration-4') ||
            fullPath.includes('migration-4') ||
            fullPath.includes('migration=4') ||
            fullPath.includes('migration_4')) {
          
          if (entry.name.startsWith('updates-')) {
            files.updates.push(fullPath);
          } else if (entry.name.startsWith('events-')) {
            files.events.push(fullPath);
          }
        }
      }
    }
  }
  
  // Also scan for files without migration prefix but check content
  function scanAllFiles(dir) {
    let entries;
    try {
      entries = fs.readdirSync(dir, { withFileTypes: true });
    } catch (err) {
      return;
    }
    
    for (const entry of entries) {
      const fullPath = path.join(dir, entry.name);
      
      if (entry.isDirectory()) {
        scanAllFiles(fullPath);
      } else if (entry.isFile() && entry.name.endsWith('.pb.zst')) {
        // Extract timestamp from filename to filter by date
        const match = entry.name.match(/(\d{4}-\d{2}-\d{2})/);
        if (match) {
          const fileDate = new Date(match[1] + 'T00:00:00Z');
          if (fileDate >= MIGRATION_START && fileDate <= customEndDate) {
            if (entry.name.startsWith('updates-') && !files.updates.includes(fullPath)) {
              files.updates.push(fullPath);
            } else if (entry.name.startsWith('events-') && !files.events.includes(fullPath)) {
              files.events.push(fullPath);
            }
          }
        }
      }
    }
  }
  
  scanDir(RAW_DIR);
  
  // If no migration-specific files found, scan all files by date
  if (files.updates.length === 0 && files.events.length === 0) {
    console.log('   No migration-4 specific directories found, scanning by date...');
    scanAllFiles(RAW_DIR);
  }
  
  // Debug: show sample paths and extracted dates
  console.log('   Sample files with extracted dates:');
  for (const f of files.updates.slice(0, 5)) {
    const extracted = extractTimestampFromFilename(f);
    const dateStr = extracted ? extracted.toISOString().substring(0, 10) : 'FAILED';
    console.log(`      ${dateStr} ← ${f.replace(RAW_DIR, '...')}`);
  }
  
  // Count files per day directory
  const dayDirs = {};
  for (const f of [...files.updates, ...files.events]) {
    const dayMatch = f.match(/day=(\d{1,2})/);
    const monthMatch = f.match(/month=(\d{1,2})/);
    if (dayMatch && monthMatch) {
      const key = `12-${dayMatch[1].padStart(2, '0')}`;
      dayDirs[key] = (dayDirs[key] || 0) + 1;
    }
  }
  if (Object.keys(dayDirs).length > 0) {
    console.log('   Files per day directory:');
    for (const [day, count] of Object.entries(dayDirs).sort()) {
      console.log(`      day=${day}: ${count.toLocaleString()} files`);
    }
  }
  
  return files;
}

/**
 * Extract timestamp from filename - supports multiple formats
 * Priority: path-based (most reliable) > ISO date > Unix timestamp
 */
function extractTimestampFromFilename(filePath) {
  // PRIORITY 1: Path-based date (most reliable for Hive-style partitioning)
  // Matches: .../year=2025/month=12/day=10/...
  const yearMatch = filePath.match(/year=(\d{4})/);
  const monthMatch = filePath.match(/month=(\d{1,2})/);
  const dayMatch = filePath.match(/day=(\d{1,2})/);
  if (yearMatch && monthMatch && dayMatch) {
    const y = yearMatch[1];
    const m = monthMatch[1].padStart(2, '0');
    const d = dayMatch[1].padStart(2, '0');
    return new Date(`${y}-${m}-${d}T00:00:00Z`);
  }
  
  const basename = path.basename(filePath);
  
  // PRIORITY 2: ISO timestamp in filename
  // Matches: updates-2025-12-10T12-30-00Z.pb.zst
  const isoMatch = basename.match(/(\d{4}-\d{2}-\d{2}T[\d-]+Z)/);
  if (isoMatch) {
    const ts = isoMatch[1].replace(/-(\d{2})-(\d{2})-(\d{2})Z/, ':$1:$2:$3Z');
    return new Date(ts);
  }
  
  // PRIORITY 3: Date only in filename
  // Matches: updates-2025-12-10.pb.zst
  const dateMatch = basename.match(/(\d{4}-\d{2}-\d{2})/);
  if (dateMatch) {
    return new Date(dateMatch[1] + 'T00:00:00Z');
  }
  
  // PRIORITY 4: Unix timestamp (milliseconds)
  // Matches: updates-1733875200000.pb.zst
  const unixMatch = basename.match(/(\d{13})/);
  if (unixMatch) {
    const ts = parseInt(unixMatch[1], 10);
    if (ts > 1700000000000 && ts < 1800000000000) {
      return new Date(ts);
    }
  }
  
  // PRIORITY 5: Unix timestamp (seconds)
  // Matches: updates-1733875200.pb.zst
  const unixSecsMatch = basename.match(/-(\d{10})\./);
  if (unixSecsMatch) {
    const ts = parseInt(unixSecsMatch[1], 10) * 1000;
    if (ts > 1700000000000 && ts < 1800000000000) {
      return new Date(ts);
    }
  }
  
  return null;
}

/**
 * Validate a single file (quick mode uses filename, full mode decodes)
 */
async function validateFile(filePath, fullScan = false) {
  const basename = path.basename(filePath);
  const fileType = basename.startsWith('updates-') ? 'updates' : 'events';
  
  // Quick mode: just extract date from filename
  if (!fullScan) {
    const ts = extractTimestampFromFilename(filePath);
    return {
      valid: true,
      file: basename,
      path: filePath,
      count: 0, // Unknown without decode
      type: fileType,
      minTime: ts ? ts.toISOString() : null,
      maxTime: ts ? ts.toISOString() : null,
      fromFilename: true,
    };
  }
  
  // Full mode: decode file
  try {
    await loadBinaryReader();
    const data = await readBinaryFile(filePath);
    
    let minTime = null;
    let maxTime = null;
    
    for (const record of data.records) {
      const time = record.effective_at || record.recorded_at || record.record_time;
      if (time) {
        const ts = new Date(time);
        if (!minTime || ts < minTime) minTime = ts;
        if (!maxTime || ts > maxTime) maxTime = ts;
      }
    }
    
    // Clear records from memory
    data.records = null;
    
    return {
      valid: true,
      file: basename,
      path: filePath,
      count: data.count,
      type: fileType,
      minTime: minTime?.toISOString() || null,
      maxTime: maxTime?.toISOString() || null,
    };
    
  } catch (err) {
    results.errors.push({ type: 'file_corrupt', file: basename, error: err.message });
    return {
      valid: false,
      file: basename,
      path: filePath,
      type: fileType,
      error: err.message,
    };
  }
}

/**
 * Sample files to get actual record counts (for quick mode)
 */
async function sampleFiles(files, count = 50) {
  await loadBinaryReader();
  
  const samples = [];
  const indices = new Set();
  const maxSamples = Math.min(count, files.length);
  
  // Pick random indices
  while (indices.size < maxSamples) {
    indices.add(Math.floor(Math.random() * files.length));
  }
  
  let processed = 0;
  for (const idx of indices) {
    try {
      const stats = await getFileStats(files[idx]);
      samples.push(stats);
      processed++;
      process.stdout.write(`\r   Sampling: ${processed}/${maxSamples} files`);
    } catch (err) {
      // Skip corrupt files
    }
  }
  console.log('');
  
  return samples;
}

/**
 * Analyze time coverage
 */
function analyzeTimeCoverage(fileResults) {
  const timeRanges = [];
  const daysCovered = new Set();
  
  for (const result of fileResults) {
    if (!result.valid) continue;
    
    if (result.minTime && result.maxTime) {
      timeRanges.push({
        file: result.file,
        start: new Date(result.minTime),
        end: new Date(result.maxTime),
        count: result.count,
      });
      
      // Track days covered
      const startDate = result.minTime.substring(0, 10);
      const endDate = result.maxTime.substring(0, 10);
      daysCovered.add(startDate);
      if (startDate !== endDate) {
        daysCovered.add(endDate);
      }
    } else if (result.file) {
      // Try to extract date from filename
      const ts = extractTimestampFromFilename(result.path || result.file);
      if (ts && ts >= MIGRATION_START && ts <= customEndDate) {
        const dateStr = ts.toISOString().substring(0, 10);
        daysCovered.add(dateStr);
      }
    }
  }
  
  // Calculate expected days
  const expectedDays = [];
  const current = new Date(MIGRATION_START);
  while (current <= customEndDate) {
    expectedDays.push(current.toISOString().substring(0, 10));
    current.setDate(current.getDate() + 1);
  }
  
  // Find missing days
  const missingDays = expectedDays.filter(day => !daysCovered.has(day));
  
  // Calculate actual range
  let actualStart = null;
  let actualEnd = null;
  
  for (const range of timeRanges) {
    if (!actualStart || range.start < actualStart) actualStart = range.start;
    if (!actualEnd || range.end > actualEnd) actualEnd = range.end;
  }
  
  results.coverage = {
    actualStart: actualStart?.toISOString() || null,
    actualEnd: actualEnd?.toISOString() || null,
    expectedDays: expectedDays.length,
    coveredDays: daysCovered.size,
    missingDays,
    coveragePercent: expectedDays.length > 0 
      ? ((daysCovered.size / expectedDays.length) * 100).toFixed(2) 
      : 0,
  };
  
  return timeRanges;
}

/**
 * Detect gaps in time coverage
 */
function detectGaps(timeRanges) {
  if (timeRanges.length < 2) return [];
  
  // Sort by start time
  const sorted = [...timeRanges].sort((a, b) => a.start.getTime() - b.start.getTime());
  
  const gaps = [];
  
  for (let i = 1; i < sorted.length; i++) {
    const prev = sorted[i - 1];
    const curr = sorted[i];
    
    const gapMs = curr.start.getTime() - prev.end.getTime();
    
    if (gapMs > GAP_THRESHOLD_MS) {
      gaps.push({
        afterFile: prev.file,
        beforeFile: curr.file,
        gapStart: prev.end.toISOString(),
        gapEnd: curr.start.toISOString(),
        gapMs,
        gapDuration: formatDuration(gapMs),
      });
    }
  }
  
  results.gaps = gaps;
  results.totalGapTime = gaps.reduce((sum, g) => sum + g.gapMs, 0);
  
  return gaps;
}

/**
 * Build daily statistics
 */
function buildDailyStats(fileResults) {
  const daily = {};
  
  for (const result of fileResults) {
    if (!result.valid) continue;
    
    let dateStr = null;
    
    if (result.minTime) {
      dateStr = result.minTime.substring(0, 10);
    } else {
      const ts = extractTimestampFromFilename(result.path || result.file);
      if (ts) dateStr = ts.toISOString().substring(0, 10);
    }
    
    if (dateStr) {
      if (!daily[dateStr]) {
        daily[dateStr] = { updates: 0, events: 0, files: 0, records: 0 };
      }
      
      daily[dateStr].files++;
      daily[dateStr].records += result.count || 0;
      
      if (result.type === 'updates') {
        daily[dateStr].updates += result.count || 0;
      } else {
        daily[dateStr].events += result.count || 0;
      }
    }
  }
  
  results.dailyStats = daily;
  return daily;
}

/**
 * Format duration
 */
function formatDuration(ms) {
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
  if (ms < 3600000) return `${(ms / 60000).toFixed(1)}m`;
  if (ms < 86400000) return `${(ms / 3600000).toFixed(1)}h`;
  return `${(ms / 86400000).toFixed(1)}d`;
}

/**
 * Print validation report
 */
function printReport() {
  const elapsed = ((Date.now() - results.startTime) / 1000).toFixed(1);
  
  console.log('\n' + '═'.repeat(80));
  console.log('📊 MIGRATION 4 DATA COMPLETENESS REPORT');
  console.log('═'.repeat(80));
  
  // Expected range
  console.log('\n📅 VALIDATION RANGE:');
  console.log(`   Migration:     ${results.migrationId}`);
  console.log(`   Expected start: ${results.expectedRange.start}`);
  console.log(`   Expected end:   ${results.expectedRange.end}`);
  console.log(`   Actual start:   ${results.coverage.actualStart || 'N/A'}`);
  console.log(`   Actual end:     ${results.coverage.actualEnd || 'N/A'}`);
  
  // File stats
  console.log('\n📁 FILE STATISTICS:');
  console.log(`   Updates files:  ${results.files.updates.total.toLocaleString()} (${results.files.updates.valid.toLocaleString()} valid, ${results.files.updates.corrupted} corrupted)`);
  console.log(`   Events files:   ${results.files.events.total.toLocaleString()} (${results.files.events.valid.toLocaleString()} valid, ${results.files.events.corrupted} corrupted)`);
  const recordNote = fullMode ? '' : ' (estimated from sample)';
  console.log(`   Update records: ${results.files.updates.records.toLocaleString()}${recordNote}`);
  console.log(`   Event records:  ${results.files.events.records.toLocaleString()}${recordNote}`);
  console.log(`   Total records:  ${(results.files.updates.records + results.files.events.records).toLocaleString()}${recordNote}`);
  // Coverage
  console.log('\n📈 COVERAGE ANALYSIS:');
  const cov = results.coverage;
  console.log(`   Expected days:  ${cov.expectedDays}`);
  console.log(`   Covered days:   ${cov.coveredDays}`);
  console.log(`   Coverage:       ${cov.coveragePercent}%`);
  
  if (cov.missingDays.length > 0) {
    console.log(`\n   ⚠️ MISSING DAYS (${cov.missingDays.length}):`);
    for (const day of cov.missingDays.slice(0, 10)) {
      console.log(`      • ${day}`);
    }
    if (cov.missingDays.length > 10) {
      console.log(`      ... and ${cov.missingDays.length - 10} more`);
    }
  } else {
    console.log(`   ✅ All expected days have data`);
  }
  
  // Gaps
  console.log('\n🔍 GAP ANALYSIS:');
  if (results.gaps.length === 0) {
    console.log('   ✅ No significant gaps detected');
  } else {
    console.log(`   ⚠️ Found ${results.gaps.length} gap(s), total: ${formatDuration(results.totalGapTime)}`);
    for (const gap of results.gaps.slice(0, 10)) {
      console.log(`      • ${gap.gapDuration}: ${gap.gapStart} → ${gap.gapEnd}`);
    }
    if (results.gaps.length > 10) {
      console.log(`      ... and ${results.gaps.length - 10} more`);
    }
  }
  
  // Daily stats
  if (verbose) {
    console.log('\n📋 DAILY BREAKDOWN:');
    const days = Object.keys(results.dailyStats).sort();
    for (const day of days) {
      const stats = results.dailyStats[day];
      console.log(`   ${day}: ${stats.files} files, ${stats.records.toLocaleString()} records (${stats.updates.toLocaleString()} updates, ${stats.events.toLocaleString()} events)`);
    }
  }
  
  // Errors
  if (results.errors.length > 0) {
    console.log('\n❌ ERRORS:');
    for (const err of results.errors.slice(0, 10)) {
      console.log(`   • ${err.type}: ${err.file} - ${err.error}`);
    }
    if (results.errors.length > 10) {
      console.log(`   ... and ${results.errors.length - 10} more`);
    }
  }
  
  // Summary
  console.log('\n' + '─'.repeat(80));
  const isComplete = cov.coveragePercent >= 99 && results.gaps.length === 0 && results.errors.length === 0;
  if (isComplete) {
    console.log('✅ VALIDATION PASSED: Migration 4 data is complete');
  } else {
    console.log('⚠️ VALIDATION ISSUES DETECTED:');
    if (cov.coveragePercent < 99) {
      console.log(`   • Coverage only ${cov.coveragePercent}% (expected ≥99%)`);
    }
    if (results.gaps.length > 0) {
      console.log(`   • ${results.gaps.length} time gap(s) detected`);
    }
    if (cov.missingDays.length > 0) {
      console.log(`   • ${cov.missingDays.length} missing day(s)`);
    }
    if (results.errors.length > 0) {
      console.log(`   • ${results.errors.length} file error(s)`);
    }
  }
  
  console.log(`⏱️ Validation completed in ${elapsed}s`);
  console.log('═'.repeat(80) + '\n');
  
  return isComplete ? 0 : 1;
}

/**
 * Main validation function
 */
async function runValidation() {
  console.log('\n' + '═'.repeat(80));
  console.log('🔍 MIGRATION 4 DATA COMPLETENESS VALIDATION');
  console.log('═'.repeat(80));
  console.log(`   Data directory:  ${BASE_DATA_DIR}`);
  console.log(`   Raw directory:   ${RAW_DIR}`);
  console.log(`   Mode:            ${fullMode ? 'Full decode (slow)' : 'Quick filename-based'}`);
  console.log(`   Date range:      ${MIGRATION_START.toISOString().substring(0, 10)} → ${customEndDate.toISOString().substring(0, 10)}`);
  console.log('═'.repeat(80) + '\n');
  
  // Find files
  console.log('📁 Scanning for migration 4 files...');
  const files = findMigration4Files();
  
  results.files.updates.total = files.updates.length;
  results.files.events.total = files.events.length;
  
  console.log(`   Found ${files.updates.length} update files`);
  console.log(`   Found ${files.events.length} event files`);
  
  if (files.updates.length === 0 && files.events.length === 0) {
    console.log('\n⚠️ No files found for migration 4');
    console.log('   Check that files exist in:', RAW_DIR);
    console.log('   Expected patterns: updates-*.pb.zst, events-*.pb.zst');
    process.exit(1);
  }
  
  // Quick validation: process files by filename only
  console.log('\n🔄 Processing files...');
  const allFiles = [...files.updates, ...files.events];
  const fileResults = [];
  let processed = 0;
  
  const batchSize = 1000;
  for (let i = 0; i < allFiles.length; i += batchSize) {
    const batch = allFiles.slice(i, i + batchSize);
    
    for (const filePath of batch) {
      try {
        const result = await validateFile(filePath, fullMode);
        fileResults.push(result);
        
        if (result.valid) {
          if (result.type === 'updates') {
            results.files.updates.valid++;
            results.files.updates.records += result.count || 0;
          } else {
            results.files.events.valid++;
            results.files.events.records += result.count || 0;
          }
        } else {
          if (result.type === 'updates') {
            results.files.updates.corrupted++;
          } else {
            results.files.events.corrupted++;
          }
        }
      } catch (err) {
        results.errors.push({ type: 'validation_error', file: path.basename(filePath), error: err.message });
      }
      
      processed++;
    }
    
    process.stdout.write(`\r   Progress: ${processed.toLocaleString()}/${allFiles.length.toLocaleString()} files`);
  }
  console.log('');
  
  // Sample files to estimate record counts (quick mode only)
  if (!fullMode && sampleSize > 0) {
    console.log(`\n📊 Sampling ${sampleSize} files for record count estimates...`);
    try {
      const updateSamples = await sampleFiles(files.updates, Math.floor(sampleSize / 2));
      const eventSamples = await sampleFiles(files.events, Math.floor(sampleSize / 2));
      
      // Estimate total records
      if (updateSamples.length > 0) {
        const avgUpdates = updateSamples.reduce((s, f) => s + f.count, 0) / updateSamples.length;
        results.files.updates.records = Math.round(avgUpdates * files.updates.length);
        console.log(`   Updates: ~${avgUpdates.toFixed(0)} records/file → ~${results.files.updates.records.toLocaleString()} total (estimated)`);
      }
      
      if (eventSamples.length > 0) {
        const avgEvents = eventSamples.reduce((s, f) => s + f.count, 0) / eventSamples.length;
        results.files.events.records = Math.round(avgEvents * files.events.length);
        console.log(`   Events: ~${avgEvents.toFixed(0)} records/file → ~${results.files.events.records.toLocaleString()} total (estimated)`);
      }
    } catch (err) {
      console.log(`   ⚠️ Sampling failed: ${err.message}`);
    }
  }
  
  // Analyze coverage
  console.log('\n📈 Analyzing time coverage...');
  const timeRanges = analyzeTimeCoverage(fileResults);
  
  // Detect gaps (only meaningful in full mode with actual timestamps)
  if (fullMode && timeRanges.length >= 2) {
    console.log('🔍 Detecting gaps...');
    detectGaps(timeRanges);
  }
  
  // Build daily stats
  console.log('📋 Building daily statistics...');
  buildDailyStats(fileResults);
  
  // Print report
  const exitCode = printReport();
  process.exit(exitCode);
}

// Run validation
runValidation().catch(err => {
  console.error('\n❌ FATAL:', err.message);
  console.error(err.stack);
  process.exit(1);
});
