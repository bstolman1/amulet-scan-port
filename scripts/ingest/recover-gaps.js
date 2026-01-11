#!/usr/bin/env node
/**
 * Gap Recovery Script
 * 
 * Automatically detects and re-fetches missing time ranges in backfill data.
 * Works by analyzing existing data files for time gaps, then fetching the
 * missing ranges from the backfill API.
 * 
 * Usage:
 *   node recover-gaps.js                     # Detect and recover all gaps
 *   node recover-gaps.js --migration 3       # Only recover gaps in migration 3
 *   node recover-gaps.js --dry-run           # Detect gaps without fetching
 *   node recover-gaps.js --threshold 300000  # Set gap threshold to 5 minutes
 */

import path from 'path';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';

// Load .env from the script's directory, not cwd
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.join(__dirname, '.env') });

import axios from 'axios';
import { Agent as HttpAgent } from 'http';
import { Agent as HttpsAgent } from 'https';
import fs from 'fs';
import Piscina from 'piscina';
import { readBinaryFile } from './read-binary.js';
import { normalizeUpdate, normalizeEvent } from './data-schema.js';
import { bufferUpdates, bufferEvents, flushAll, waitForWrites, shutdown } from './write-binary.js';

// TLS config (secure by default)
// Set INSECURE_TLS=1 only in controlled environments with self-signed certs.
const INSECURE_TLS = ['1', 'true', 'yes'].includes(String(process.env.INSECURE_TLS || '').toLowerCase());
if (INSECURE_TLS) {
  process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";
}

// Configuration
const SCAN_URL = process.env.SCAN_URL || 'https://scan.sv-1.global.canton.network.sync.global/api/scan';
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 1000;
// Default Windows path: C:\ledger_raw
const WIN_DEFAULT = 'C:\\ledger_raw';
const BASE_DATA_DIR = process.env.DATA_DIR || WIN_DEFAULT;
const CURSOR_DIR = process.env.CURSOR_DIR || path.join(BASE_DATA_DIR, 'cursors');
const RAW_DIR = path.join(BASE_DATA_DIR, 'raw');

// Gap detection threshold (default 2 minutes)
const DEFAULT_GAP_THRESHOLD_MS = 120000;

// Parse CLI args
const args = process.argv.slice(2);
const targetMigration = args.includes('--migration') 
  ? parseInt(args[args.indexOf('--migration') + 1]) 
  : null;
const dryRun = args.includes('--dry-run');
const thresholdArg = args.includes('--threshold')
  ? parseInt(args[args.indexOf('--threshold') + 1])
  : null;
const GAP_THRESHOLD_MS = thresholdArg || parseInt(process.env.GAP_THRESHOLD_MS) || DEFAULT_GAP_THRESHOLD_MS;
const verbose = args.includes('--verbose') || args.includes('-v');
const maxGaps = args.includes('--max-gaps')
  ? parseInt(args[args.indexOf('--max-gaps') + 1])
  : 50;

// HTTP client
const client = axios.create({
  baseURL: SCAN_URL,
  httpAgent: new HttpAgent({
    keepAlive: true,
    keepAliveMsecs: 60000,
    maxSockets: 8,
  }),
  httpsAgent: new HttpsAgent({
    keepAlive: true,
    keepAliveMsecs: 60000,
    rejectUnauthorized: !INSECURE_TLS,
    maxSockets: 8,
  }),
  timeout: 180000,
});

// Decode worker pool
let decodePool = null;

function getDecodePool() {
  if (!decodePool) {
    decodePool = new Piscina({
      filename: new URL('./decode-worker.js', import.meta.url).href,
      minThreads: 2,
      maxThreads: 4,
    });
  }
  return decodePool;
}

/**
 * Sleep helper
 */
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Retry with exponential backoff
 */
async function retryWithBackoff(fn, maxRetries = 5) {
  let lastError;
  
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;
      const status = error.response?.status;
      const retryable = [429, 500, 502, 503, 504].includes(status) ||
                       ['ECONNRESET', 'ETIMEDOUT', 'ECONNREFUSED'].includes(error.code);
      
      if (attempt === maxRetries || !retryable) {
        throw error;
      }
      
      const delay = Math.min(1000 * Math.pow(2, attempt), 30000);
      await sleep(delay + Math.random() * 1000);
    }
  }
  
  throw lastError;
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
 * Find all .pb.zst files in the raw directory
 */
function findDataFiles(migrationFilter = null) {
  const files = [];
  
  if (!fs.existsSync(RAW_DIR)) {
    return files;
  }
  
  function scanDir(dir, currentMigration = null) {
    const entries = fs.readdirSync(dir, { withFileTypes: true });
    
    for (const entry of entries) {
      const fullPath = path.join(dir, entry.name);
      
      if (entry.isDirectory()) {
        // Extract migration ID from directory name
        const migMatch = entry.name.match(/migration[-_]?(\d+)/i);
        const migId = migMatch ? parseInt(migMatch[1]) : currentMigration;
        
        if (migrationFilter !== null && migId !== null && migId !== migrationFilter) {
          continue;
        }
        
        scanDir(fullPath, migId);
      } else if (entry.isFile() && entry.name.endsWith('.pb.zst') && entry.name.startsWith('updates-')) {
        files.push({ path: fullPath, migration: currentMigration });
      }
    }
  }
  
  scanDir(RAW_DIR);
  return files;
}

/**
 * Extract time range from a data file
 */
async function extractFileTimeRange(filePath) {
  try {
    const data = await readBinaryFile(filePath);
    
    let minTime = null;
    let maxTime = null;
    let synchronizer = null;
    
    for (const record of data.records) {
      // Use effective_at primarily (actual event time) for accurate gap detection
      const time = record.effective_at || record.recorded_at;
      if (time) {
        if (!minTime || time < minTime) minTime = time;
        if (!maxTime || time > maxTime) maxTime = time;
      }
      if (!synchronizer && record.synchronizer) {
        synchronizer = record.synchronizer;
      }
    }
    
    return {
      file: path.basename(filePath),
      path: filePath,
      minTime,
      maxTime,
      synchronizer,
      recordCount: data.count,
    };
  } catch (err) {
    console.warn(`   ⚠️ Failed to read ${path.basename(filePath)}: ${err.message}`);
    return null;
  }
}

/**
 * Detect gaps in time coverage
 */
function detectGaps(timeRanges, thresholdMs) {
  const gaps = [];
  
  if (timeRanges.length < 2) return gaps;
  
  // Group by synchronizer
  const bySynchronizer = {};
  for (const range of timeRanges) {
    if (!range.synchronizer) continue;
    if (!bySynchronizer[range.synchronizer]) {
      bySynchronizer[range.synchronizer] = [];
    }
    bySynchronizer[range.synchronizer].push(range);
  }
  
  // Find gaps within each synchronizer
  for (const [syncId, ranges] of Object.entries(bySynchronizer)) {
    // Sort by start time
    const sorted = [...ranges].sort((a, b) => 
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
          synchronizer: syncId,
          gapStart: prev.maxTime,
          gapEnd: curr.minTime,
          gapMs,
          afterFile: prev.file,
          beforeFile: curr.file,
        });
      }
    }
  }
  
  // Sort gaps by size (largest first)
  gaps.sort((a, b) => b.gapMs - a.gapMs);
  
  return gaps;
}

/**
 * Load cursor to get migration info
 */
function loadCursors() {
  const cursors = [];
  
  if (!fs.existsSync(CURSOR_DIR)) return cursors;
  
  const files = fs.readdirSync(CURSOR_DIR).filter(f => f.endsWith('.json'));
  
  for (const file of files) {
    try {
      const data = JSON.parse(fs.readFileSync(path.join(CURSOR_DIR, file), 'utf8'));
      cursors.push(data);
    } catch (err) {
      // Ignore invalid cursors
    }
  }
  
  return cursors;
}

/**
 * Find migration ID for a synchronizer
 */
function findMigrationForSynchronizer(synchronizerId, cursors) {
  const cursor = cursors.find(c => c.synchronizer_id === synchronizerId);
  return cursor?.migration_id || null;
}

/**
 * Fetch backfill data for a gap with deduplication
 */
async function fetchGapData(migrationId, synchronizerId, gapStart, gapEnd) {
  const allTransactions = [];
  const seenUpdateIds = new Set(); // Deduplication
  let currentBefore = gapEnd;
  const atOrAfter = gapStart;
  let consecutiveEmpty = 0;
  
  while (true) {
    if (new Date(currentBefore).getTime() <= new Date(atOrAfter).getTime()) {
      break;
    }
    
    const payload = {
      migration_id: migrationId,
      synchronizer_id: synchronizerId,
      before: currentBefore,
      at_or_after: atOrAfter,
      count: BATCH_SIZE,
    };
    
    let response;
    try {
      response = await retryWithBackoff(async () => {
        const res = await client.post('/v0/backfilling/updates-before', payload);
        return res.data;
      });
    } catch (err) {
      console.error(`      ❌ Fetch failed: ${err.message}`);
      break;
    }
    
    const txs = response?.transactions || [];
    
    if (txs.length === 0) {
      consecutiveEmpty++;
      if (consecutiveEmpty >= 3) break;
      
      const d = new Date(currentBefore);
      d.setTime(d.getTime() - 1000);
      if (d.getTime() <= new Date(atOrAfter).getTime()) break;
      currentBefore = d.toISOString();
      continue;
    }
    
    consecutiveEmpty = 0;
    
    // Deduplicate transactions
    for (const tx of txs) {
      const updateId = tx.update_id || tx.transaction?.update_id || tx.reassignment?.update_id;
      if (updateId) {
        if (!seenUpdateIds.has(updateId)) {
          seenUpdateIds.add(updateId);
          allTransactions.push(tx);
        }
      } else {
        allTransactions.push(tx);
      }
    }
    
    // Find oldest timestamp
    let oldestTime = null;
    for (const tx of txs) {
      const t = tx.record_time || tx.event?.record_time || tx.effective_at;
      if (t && (!oldestTime || t < oldestTime)) {
        oldestTime = t;
      }
    }
    
    if (oldestTime && new Date(oldestTime).getTime() <= new Date(atOrAfter).getTime()) {
      break;
    }
    
    if (oldestTime) {
      const d = new Date(oldestTime);
      d.setMilliseconds(d.getMilliseconds() - 1);
      currentBefore = d.toISOString();
    } else {
      const d = new Date(currentBefore);
      d.setMilliseconds(d.getMilliseconds() - 1);
      currentBefore = d.toISOString();
    }
  }
  
  return allTransactions;
}

/**
 * Process and write recovered transactions
 */
async function processTransactions(transactions, migrationId) {
  const pool = getDecodePool();
  
  const tasks = transactions.map(tx => 
    pool.run({ tx, migrationId }).catch(err => {
      // Fallback to main thread decode
      return decodeInMainThread(tx, migrationId);
    })
  );
  
  const results = await Promise.all(tasks);
  
  const updates = [];
  const events = [];
  
  for (const r of results) {
    if (!r) continue;
    if (r.update) updates.push(r.update);
    if (Array.isArray(r.events) && r.events.length > 0) {
      events.push(...r.events);
    }
  }
  
  await bufferUpdates(updates);
  await bufferEvents(events);
  
  return { updates: updates.length, events: events.length };
}

/**
 * Fallback decode
 */
function decodeInMainThread(tx, migrationId) {
  const isReassignment = !!tx.event;
  const update = normalizeUpdate(tx);
  update.migration_id = migrationId;

  const events = [];
  const txData = tx.transaction || tx.reassignment || tx;

  const updateInfo = {
    record_time: txData.record_time,
    effective_at: txData.effective_at,
    synchronizer_id: txData.synchronizer_id,
    source: txData.source || null,
    target: txData.target || null,
    unassign_id: txData.unassign_id || null,
    submitter: txData.submitter || null,
    counter: txData.counter ?? null,
  };

  if (isReassignment) {
    const ce = tx.event?.created_event;
    const ae = tx.event?.archived_event;

    if (ce) {
      const ev = normalizeEvent(ce, update.update_id, migrationId, tx, updateInfo);
      ev.event_type = 'reassign_create';
      events.push(ev);
    }
    if (ae) {
      const ev = normalizeEvent(ae, update.update_id, migrationId, tx, updateInfo);
      ev.event_type = 'reassign_archive';
      events.push(ev);
    }
  } else {
    const eventsById = txData.events_by_id || tx.events_by_id || {};
    for (const [eventId, rawEvent] of Object.entries(eventsById)) {
      const ev = normalizeEvent(rawEvent, update.update_id, migrationId, rawEvent, updateInfo);
      ev.event_id = eventId;
      events.push(ev);
    }
  }

  return { update, events };
}

/**
 * Recover a single gap
 */
async function recoverGap(gap, migrationId, gapIndex, totalGaps) {
  const syncShort = gap.synchronizer.substring(0, 30);
  
  console.log(`\n   📍 Gap ${gapIndex + 1}/${totalGaps}: ${formatDuration(gap.gapMs)}`);
  console.log(`      Synchronizer: ${syncShort}...`);
  console.log(`      Range: ${gap.gapStart} → ${gap.gapEnd}`);
  
  if (dryRun) {
    console.log(`      ⏭️ Skipping (dry-run mode)`);
    return { recovered: 0, events: 0 };
  }
  
  // Fetch missing data
  console.log(`      🔄 Fetching missing data...`);
  const transactions = await fetchGapData(
    migrationId, 
    gap.synchronizer, 
    gap.gapStart, 
    gap.gapEnd
  );
  
  if (transactions.length === 0) {
    console.log(`      ℹ️ No transactions found in gap (may be legitimate empty period)`);
    return { recovered: 0, events: 0 };
  }
  
  // Process and write
  console.log(`      📝 Processing ${transactions.length} transactions...`);
  const result = await processTransactions(transactions, migrationId);
  
  console.log(`      ✅ Recovered: ${result.updates} updates, ${result.events} events`);
  
  return { recovered: result.updates, events: result.events };
}

/**
 * Main recovery function
 */
async function runRecovery() {
  console.log('\n' + '═'.repeat(80));
  console.log('🔧 GAP RECOVERY');
  console.log('═'.repeat(80));
  console.log(`   Data directory:   ${BASE_DATA_DIR}`);
  console.log(`   Gap threshold:    ${formatDuration(GAP_THRESHOLD_MS)}`);
  console.log(`   Max gaps:         ${maxGaps}`);
  console.log(`   Mode:             ${dryRun ? 'Dry run (no changes)' : 'Recovery mode'}`);
  if (targetMigration) {
    console.log(`   Target migration: ${targetMigration}`);
  }
  console.log('═'.repeat(80));
  
  // Load cursors for migration info
  const cursors = loadCursors();
  
  // Find all data files
  console.log('\n📁 Scanning data files...');
  const files = findDataFiles(targetMigration);
  console.log(`   Found ${files.length} update file(s)`);
  
  if (files.length === 0) {
    console.log('\n⚠️ No data files found. Nothing to recover.');
    return;
  }
  
  // Extract time ranges from all files
  console.log('\n⏱️ Analyzing time coverage...');
  const timeRanges = [];
  let progress = 0;
  
  for (const file of files) {
    const range = await extractFileTimeRange(file.path);
    if (range && range.minTime && range.maxTime) {
      range.migration = file.migration;
      timeRanges.push(range);
    }
    
    progress++;
    if (progress % 10 === 0 || progress === files.length) {
      process.stdout.write(`\r   Progress: ${progress}/${files.length} files analyzed`);
    }
  }
  console.log('');
  
  // Detect gaps
  console.log('\n🔍 Detecting gaps...');
  const gaps = detectGaps(timeRanges, GAP_THRESHOLD_MS);
  
  if (gaps.length === 0) {
    console.log('   ✅ No gaps detected! Data coverage is complete.');
    return;
  }
  
  console.log(`   ⚠️ Found ${gaps.length} gap(s)`);
  
  // Limit gaps to recover
  const gapsToRecover = gaps.slice(0, maxGaps);
  if (gaps.length > maxGaps) {
    console.log(`   📊 Recovering ${maxGaps} largest gaps (use --max-gaps to change)`);
  }
  
  // Summary of gaps
  console.log('\n📊 Gap Summary:');
  let totalGapMs = 0;
  for (let i = 0; i < Math.min(10, gapsToRecover.length); i++) {
    const gap = gapsToRecover[i];
    console.log(`   ${i + 1}. ${formatDuration(gap.gapMs)} in ${gap.synchronizer.substring(0, 30)}...`);
    totalGapMs += gap.gapMs;
  }
  if (gapsToRecover.length > 10) {
    console.log(`   ... and ${gapsToRecover.length - 10} more`);
    for (const gap of gapsToRecover.slice(10)) {
      totalGapMs += gap.gapMs;
    }
  }
  console.log(`\n   Total gap time: ${formatDuration(totalGapMs)}`);
  
  // Recover each gap
  console.log('\n' + '─'.repeat(80));
  console.log('🔄 RECOVERING GAPS');
  console.log('─'.repeat(80));
  
  let totalRecovered = 0;
  let totalEvents = 0;
  let gapsFixed = 0;
  const startTime = Date.now();
  
  for (let i = 0; i < gapsToRecover.length; i++) {
    const gap = gapsToRecover[i];
    
    // Find migration ID
    let migrationId = findMigrationForSynchronizer(gap.synchronizer, cursors);
    
    if (!migrationId) {
      // Try to extract from file path
      const fileRange = timeRanges.find(r => r.synchronizer === gap.synchronizer);
      migrationId = fileRange?.migration || targetMigration;
    }
    
    if (!migrationId) {
      console.log(`\n   ⚠️ Gap ${i + 1}: Cannot determine migration ID, skipping`);
      continue;
    }
    
    try {
      const result = await recoverGap(gap, migrationId, i, gapsToRecover.length);
      totalRecovered += result.recovered;
      totalEvents += result.events;
      if (result.recovered > 0) gapsFixed++;
    } catch (err) {
      console.error(`\n   ❌ Gap ${i + 1} failed: ${err.message}`);
    }
    
    // Flush periodically
    if ((i + 1) % 5 === 0) {
      await flushAll();
    }
  }
  
  // Final flush
  await flushAll();
  await waitForWrites();
  
  // Report
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  
  console.log('\n' + '═'.repeat(80));
  console.log('📊 RECOVERY COMPLETE');
  console.log('═'.repeat(80));
  console.log(`   Gaps analyzed:    ${gapsToRecover.length}`);
  console.log(`   Gaps recovered:   ${gapsFixed}`);
  console.log(`   Updates recovered: ${totalRecovered.toLocaleString()}`);
  console.log(`   Events recovered:  ${totalEvents.toLocaleString()}`);
  console.log(`   Time elapsed:      ${elapsed}s`);
  console.log('═'.repeat(80) + '\n');
  
  // Cleanup
  if (decodePool) {
    await decodePool.destroy();
  }
  await shutdown();
}

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('\n🛑 Shutting down...');
  await flushAll();
  await waitForWrites();
  await shutdown();
  if (decodePool) {
    await decodePool.destroy();
  }
  process.exit(0);
});

// Run
runRecovery().catch(async err => {
  console.error('\n❌ FATAL:', err.message);
  console.error(err.stack);
  await shutdown();
  if (decodePool) {
    await decodePool.destroy();
  }
  process.exit(1);
});
