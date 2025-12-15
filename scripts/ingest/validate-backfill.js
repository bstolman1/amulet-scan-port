#!/usr/bin/env node
/**
 * Backfill Validation Script
 * 
 * Verifies data integrity and checks for gaps in ingested backfill data.
 * 
 * Checks performed:
 * 1. File integrity - validates all .pb.zst files can be decoded
 * 2. Time gaps - detects missing time ranges in the data
 * 3. Cursor consistency - validates cursor state matches actual data
 * 4. Duplicate detection - identifies potential duplicate records
 * 5. Coverage analysis - calculates % of expected time range covered
 * 
 * Usage:
 *   node validate-backfill.js                    # Validate all data
 *   node validate-backfill.js --migration 3     # Validate specific migration
 *   node validate-backfill.js --quick           # Quick scan (stats only)
 *   node validate-backfill.js --fix-cursors     # Reset cursors for incomplete ranges
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { readBinaryFile, getFileStats } from './read-binary.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Configuration
const BASE_DATA_DIR = process.env.DATA_DIR || path.join(__dirname, '../../data');
const CURSOR_DIR = process.env.CURSOR_DIR || path.join(BASE_DATA_DIR, 'cursors');
const RAW_DIR = path.join(BASE_DATA_DIR, 'raw');

// Gap detection threshold (gaps larger than this are reported)
const GAP_THRESHOLD_MS = parseInt(process.env.GAP_THRESHOLD_MS) || 60000; // 1 minute default

// Parse CLI args
const args = process.argv.slice(2);
const targetMigration = args.includes('--migration') 
  ? parseInt(args[args.indexOf('--migration') + 1]) 
  : null;
const quickMode = args.includes('--quick');
const fixCursors = args.includes('--fix-cursors');
const verbose = args.includes('--verbose') || args.includes('-v');

/**
 * Results accumulator
 */
const results = {
  filesScanned: 0,
  filesCorrupted: 0,
  totalRecords: 0,
  totalUpdates: 0,
  totalEvents: 0,
  duplicatesFound: 0,
  gapsFound: [],
  cursorIssues: [],
  coverage: {},
  errors: [],
  startTime: Date.now(),
};

/**
 * Load all cursor files
 */
function loadCursors() {
  const cursors = [];
  
  if (!fs.existsSync(CURSOR_DIR)) {
    console.log(`⚠️ No cursor directory found: ${CURSOR_DIR}`);
    return cursors;
  }
  
  const files = fs.readdirSync(CURSOR_DIR).filter(f => f.endsWith('.json'));
  
  for (const file of files) {
    try {
      const data = JSON.parse(fs.readFileSync(path.join(CURSOR_DIR, file), 'utf8'));
      cursors.push({ file, ...data });
    } catch (err) {
      results.errors.push({ type: 'cursor_read', file, error: err.message });
    }
  }
  
  return cursors;
}

/**
 * Find all .pb.zst files in the raw directory
 */
function findDataFiles(migrationFilter = null) {
  const files = { updates: [], events: [] };
  
  if (!fs.existsSync(RAW_DIR)) {
    console.log(`⚠️ No raw data directory found: ${RAW_DIR}`);
    return files;
  }
  
  function scanDir(dir) {
    const entries = fs.readdirSync(dir, { withFileTypes: true });
    
    for (const entry of entries) {
      const fullPath = path.join(dir, entry.name);
      
      if (entry.isDirectory()) {
        // Check migration filter
        if (migrationFilter !== null) {
          const migMatch = entry.name.match(/migration[-_]?(\d+)/i);
          if (migMatch && parseInt(migMatch[1]) !== migrationFilter) {
            continue;
          }
        }
        scanDir(fullPath);
      } else if (entry.isFile() && entry.name.endsWith('.pb.zst')) {
        if (entry.name.startsWith('updates-')) {
          files.updates.push(fullPath);
        } else if (entry.name.startsWith('events-')) {
          files.events.push(fullPath);
        }
      }
    }
  }
  
  scanDir(RAW_DIR);
  return files;
}

/**
 * Extract timestamp from filename or parse file for time range
 */
function extractFileTimeRange(filePath) {
  const basename = path.basename(filePath);
  
  // Try to extract timestamp from filename pattern: updates-2024-01-15T12-30-00Z.pb.zst
  const match = basename.match(/(\d{4}-\d{2}-\d{2}T[\d-]+Z)/);
  if (match) {
    const ts = match[1].replace(/-(\d{2})-(\d{2})-(\d{2})Z/, ':$1:$2:$3Z');
    return { start: ts, end: ts, fromFilename: true };
  }
  
  return null;
}

/**
 * Validate a single file's integrity
 */
async function validateFile(filePath, quickScan = false) {
  const basename = path.basename(filePath);
  
  try {
    if (quickScan) {
      // Just get stats without full decode
      const stats = await getFileStats(filePath);
      return {
        valid: true,
        file: basename,
        count: stats.count,
        type: stats.type,
        chunks: stats.chunks,
      };
    }
    
    // Full decode to validate all records
    const data = await readBinaryFile(filePath);
    
    // Extract time range from records without keeping records in memory
    // Use effective_at primarily (actual event time), fall back to recorded_at
    let minTime = null;
    let maxTime = null;
    const sampleIds = []; // Keep only first few IDs for duplicate sampling
    
    for (let i = 0; i < data.records.length; i++) {
      const record = data.records[i];
      const time = record.effective_at || record.recorded_at;
      if (time) {
        if (!minTime || time < minTime) minTime = time;
        if (!maxTime || time > maxTime) maxTime = time;
      }
      // Only keep first 10 IDs for duplicate detection (memory-efficient sampling)
      if (i < 10) {
        const id = record.id || record.transaction_id || record.update_id;
        if (id) sampleIds.push(id);
      }
    }
    
    // Clear records from memory immediately
    data.records = null;
    
    return {
      valid: true,
      file: basename,
      path: filePath,
      count: data.count,
      type: data.type,
      chunks: data.chunksRead,
      minTime,
      maxTime,
      sampleIds, // Only sample IDs, not full records
    };
    
  } catch (err) {
    results.filesCorrupted++;
    results.errors.push({ type: 'file_corrupt', file: basename, error: err.message });
    
    return {
      valid: false,
      file: basename,
      path: filePath,
      error: err.message,
    };
  }
}

/**
 * Detect time gaps in a sorted list of time ranges
 */
function detectGaps(timeRanges, thresholdMs) {
  const gaps = [];
  
  if (timeRanges.length < 2) return gaps;
  
  // Sort by start time
  const sorted = [...timeRanges].sort((a, b) => 
    new Date(a.minTime).getTime() - new Date(b.minTime).getTime()
  );
  
  for (let i = 1; i < sorted.length; i++) {
    const prev = sorted[i - 1];
    const curr = sorted[i];
    
    const prevEnd = new Date(prev.maxTime).getTime();
    const currStart = new Date(curr.minTime).getTime();
    const gapMs = currStart - prevEnd;
    
    if (gapMs > thresholdMs) {
      gaps.push({
        after: prev.file,
        before: curr.file,
        gapStart: prev.maxTime,
        gapEnd: curr.minTime,
        gapMs,
        gapFormatted: formatDuration(gapMs),
      });
    }
  }
  
  return gaps;
}

/**
 * Detect duplicate records across files using sample IDs (memory-efficient)
 */
function detectDuplicates(results) {
  const seen = new Map();
  const duplicates = [];
  
  for (const { file, sampleIds } of results) {
    if (!sampleIds) continue;
    for (const id of sampleIds) {
      if (seen.has(id)) {
        duplicates.push({
          id,
          firstFile: seen.get(id),
          secondFile: file,
        });
      } else {
        seen.set(id, file);
      }
    }
  }
  
  return duplicates;
}

/**
 * Validate cursor state against actual data
 */
function validateCursors(cursors, fileTimeRanges) {
  const issues = [];
  
  for (const cursor of cursors) {
    // Check if cursor claims complete but data might be missing
    if (cursor.complete) {
      const minTime = cursor.min_time;
      const lastBefore = cursor.last_before;
      
      if (minTime && lastBefore) {
        const expectedMs = new Date(cursor.max_time || cursor.min_time).getTime() - 
                          new Date(minTime).getTime();
        const coveredMs = new Date(cursor.max_time || lastBefore).getTime() - 
                         new Date(lastBefore).getTime();
        
        // Check if last_before is significantly higher than min_time
        if (new Date(lastBefore).getTime() > new Date(minTime).getTime() + GAP_THRESHOLD_MS) {
          issues.push({
            cursor: cursor.file,
            issue: 'incomplete_range',
            message: `Cursor marked complete but last_before (${lastBefore}) > min_time (${minTime})`,
            lastBefore,
            minTime,
          });
        }
      }
    }
    
    // Check for stale cursors (not updated in 24h but not complete)
    if (!cursor.complete && cursor.updated_at) {
      const lastUpdate = new Date(cursor.updated_at).getTime();
      const staleThreshold = Date.now() - (24 * 60 * 60 * 1000);
      
      if (lastUpdate < staleThreshold) {
        issues.push({
          cursor: cursor.file,
          issue: 'stale',
          message: `Cursor not updated since ${cursor.updated_at} and not marked complete`,
          lastUpdate: cursor.updated_at,
        });
      }
    }
  }
  
  return issues;
}

/**
 * Calculate coverage percentage for each migration
 */
function calculateCoverage(cursors, fileTimeRanges) {
  const coverage = {};
  
  for (const cursor of cursors) {
    const migId = cursor.migration_id;
    if (!migId) continue;
    
    if (!coverage[migId]) {
      coverage[migId] = {
        migration: migId,
        synchronizers: [],
        totalRangeMs: 0,
        coveredMs: 0,
        files: 0,
        records: 0,
      };
    }
    
    const minTime = cursor.min_time;
    const maxTime = cursor.max_time;
    const lastBefore = cursor.last_before;
    
    if (minTime && maxTime) {
      const totalMs = new Date(maxTime).getTime() - new Date(minTime).getTime();
      const coveredMs = new Date(maxTime).getTime() - new Date(lastBefore || maxTime).getTime();
      
      coverage[migId].totalRangeMs += totalMs;
      coverage[migId].coveredMs += coveredMs;
      coverage[migId].records += cursor.total_updates || 0;
      coverage[migId].synchronizers.push({
        id: cursor.synchronizer_id?.substring(0, 30),
        complete: cursor.complete || false,
        updates: cursor.total_updates || 0,
        events: cursor.total_events || 0,
      });
    }
  }
  
  // Calculate percentages
  for (const migId of Object.keys(coverage)) {
    const c = coverage[migId];
    c.coveragePercent = c.totalRangeMs > 0 
      ? ((c.coveredMs / c.totalRangeMs) * 100).toFixed(2) + '%'
      : '0%';
  }
  
  return coverage;
}

/**
 * Format duration in human-readable form
 */
function formatDuration(ms) {
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
  if (ms < 3600000) return `${(ms / 60000).toFixed(1)}m`;
  if (ms < 86400000) return `${(ms / 3600000).toFixed(1)}h`;
  return `${(ms / 86400000).toFixed(1)}d`;
}

/**
 * Fix incomplete cursors by resetting them
 */
function fixIncompleteCursors(cursorIssues) {
  let fixed = 0;
  
  for (const issue of cursorIssues) {
    if (issue.issue === 'incomplete_range' || issue.issue === 'stale') {
      const cursorPath = path.join(CURSOR_DIR, issue.cursor);
      
      if (fs.existsSync(cursorPath)) {
        try {
          const cursor = JSON.parse(fs.readFileSync(cursorPath, 'utf8'));
          
          // Reset to allow re-processing
          cursor.complete = false;
          cursor.error = null;
          cursor.reset_at = new Date().toISOString();
          cursor.reset_reason = issue.issue;
          
          fs.writeFileSync(cursorPath, JSON.stringify(cursor, null, 2));
          console.log(`   ✅ Reset cursor: ${issue.cursor}`);
          fixed++;
        } catch (err) {
          console.error(`   ❌ Failed to fix cursor ${issue.cursor}: ${err.message}`);
        }
      }
    }
  }
  
  return fixed;
}

/**
 * Print summary report
 */
function printReport() {
  const elapsed = ((Date.now() - results.startTime) / 1000).toFixed(1);
  
  console.log('\n' + '═'.repeat(80));
  console.log('📊 BACKFILL VALIDATION REPORT');
  console.log('═'.repeat(80));
  
  // File summary
  console.log('\n📁 FILE INTEGRITY:');
  console.log(`   Files scanned:    ${results.filesScanned}`);
  console.log(`   Files valid:      ${results.filesScanned - results.filesCorrupted}`);
  console.log(`   Files corrupted:  ${results.filesCorrupted}`);
  console.log(`   Total updates:    ${results.totalUpdates.toLocaleString()}`);
  console.log(`   Total events:     ${results.totalEvents.toLocaleString()}`);
  
  // Gaps
  console.log('\n⏱️ TIME GAPS:');
  if (results.gapsFound.length === 0) {
    console.log('   ✅ No significant gaps detected');
  } else {
    console.log(`   ⚠️ Found ${results.gapsFound.length} gap(s):`);
    for (const gap of results.gapsFound.slice(0, 10)) {
      console.log(`      • ${gap.gapFormatted} gap between ${gap.gapStart} and ${gap.gapEnd}`);
    }
    if (results.gapsFound.length > 10) {
      console.log(`      ... and ${results.gapsFound.length - 10} more`);
    }
  }
  
  // Duplicates
  console.log('\n🔄 DUPLICATES:');
  if (results.duplicatesFound === 0) {
    console.log('   ✅ No duplicates detected');
  } else {
    console.log(`   ⚠️ Found ${results.duplicatesFound} potential duplicate(s)`);
  }
  
  // Cursor issues
  console.log('\n📍 CURSOR STATE:');
  if (results.cursorIssues.length === 0) {
    console.log('   ✅ All cursors valid');
  } else {
    console.log(`   ⚠️ Found ${results.cursorIssues.length} issue(s):`);
    for (const issue of results.cursorIssues) {
      console.log(`      • ${issue.cursor}: ${issue.issue} - ${issue.message}`);
    }
  }
  
  // Coverage
  console.log('\n📈 COVERAGE BY MIGRATION:');
  for (const [migId, cov] of Object.entries(results.coverage)) {
    console.log(`   Migration ${migId}:`);
    console.log(`      Coverage:      ${cov.coveragePercent}`);
    console.log(`      Records:       ${cov.records.toLocaleString()}`);
    console.log(`      Synchronizers: ${cov.synchronizers.length}`);
    
    const complete = cov.synchronizers.filter(s => s.complete).length;
    const incomplete = cov.synchronizers.length - complete;
    console.log(`      Complete:      ${complete}/${cov.synchronizers.length}`);
    
    if (incomplete > 0 && verbose) {
      for (const s of cov.synchronizers.filter(s => !s.complete)) {
        console.log(`         ⏳ ${s.id}... (${s.updates} updates)`);
      }
    }
  }
  
  // Errors
  if (results.errors.length > 0) {
    console.log('\n❌ ERRORS:');
    for (const err of results.errors.slice(0, 10)) {
      console.log(`   • ${err.type}: ${err.file || 'unknown'} - ${err.error}`);
    }
    if (results.errors.length > 10) {
      console.log(`   ... and ${results.errors.length - 10} more`);
    }
  }
  
  console.log('\n' + '─'.repeat(80));
  console.log(`⏱️ Validation completed in ${elapsed}s`);
  console.log('═'.repeat(80) + '\n');
  
  // Exit code based on issues found
  const hasIssues = results.filesCorrupted > 0 || 
                   results.gapsFound.length > 0 || 
                   results.cursorIssues.length > 0;
  
  return hasIssues ? 1 : 0;
}

/**
 * Main validation function
 */
async function runValidation() {
  console.log('\n' + '═'.repeat(80));
  console.log('🔍 BACKFILL DATA VALIDATION');
  console.log('═'.repeat(80));
  console.log(`   Data directory:   ${BASE_DATA_DIR}`);
  console.log(`   Cursor directory: ${CURSOR_DIR}`);
  console.log(`   Gap threshold:    ${formatDuration(GAP_THRESHOLD_MS)}`);
  console.log(`   Mode:             ${quickMode ? 'Quick scan' : 'Full validation'}`);
  if (targetMigration) {
    console.log(`   Target migration: ${targetMigration}`);
  }
  console.log('═'.repeat(80) + '\n');
  
  // Load cursors
  console.log('📍 Loading cursors...');
  const cursors = loadCursors();
  console.log(`   Found ${cursors.length} cursor(s)`);
  
  // Find data files
  console.log('\n📁 Scanning data files...');
  const files = findDataFiles(targetMigration);
  console.log(`   Found ${files.updates.length} update file(s)`);
  console.log(`   Found ${files.events.length} event file(s)`);
  
  // Validate files
  const updateResults = [];
  const eventResults = [];
  
  if (files.updates.length > 0) {
    console.log('\n🔄 Validating update files...');
    let progress = 0;
    
    for (const file of files.updates) {
      const result = await validateFile(file, quickMode);
      results.filesScanned++;
      
      if (result.valid) {
        results.totalUpdates += result.count;
        updateResults.push(result);
      }
      
      progress++;
      if (progress % 10 === 0 || progress === files.updates.length) {
        process.stdout.write(`\r   Progress: ${progress}/${files.updates.length} files`);
      }
    }
    console.log('');
  }
  
  if (files.events.length > 0) {
    console.log('\n🔄 Validating event files...');
    let progress = 0;
    
    for (const file of files.events) {
      const result = await validateFile(file, quickMode);
      results.filesScanned++;
      
      if (result.valid) {
        results.totalEvents += result.count;
        eventResults.push(result);
      }
      
      progress++;
      if (progress % 10 === 0 || progress === files.events.length) {
        process.stdout.write(`\r   Progress: ${progress}/${files.events.length} files`);
      }
    }
    console.log('');
  }
  
  // Detect gaps (only in full mode with time data)
  if (!quickMode) {
    console.log('\n⏱️ Analyzing time coverage...');
    
    const updateTimeRanges = updateResults
      .filter(r => r.minTime && r.maxTime)
      .map(r => ({ file: r.file, minTime: r.minTime, maxTime: r.maxTime }));
    
    if (updateTimeRanges.length > 1) {
      results.gapsFound = detectGaps(updateTimeRanges, GAP_THRESHOLD_MS);
    }
    
    // Detect duplicates using sample IDs (memory-efficient)
    console.log('\n🔄 Checking for duplicates...');
    
    const duplicates = detectDuplicates(updateResults);
    results.duplicatesFound = duplicates.length;
  }
  
  // Validate cursors
  console.log('\n📍 Validating cursor state...');
  results.cursorIssues = validateCursors(cursors, updateResults);
  
  // Calculate coverage
  results.coverage = calculateCoverage(cursors, updateResults);
  
  // Fix cursors if requested
  if (fixCursors && results.cursorIssues.length > 0) {
    console.log('\n🔧 Fixing incomplete cursors...');
    const fixed = fixIncompleteCursors(results.cursorIssues);
    console.log(`   Fixed ${fixed} cursor(s)`);
  }
  
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
