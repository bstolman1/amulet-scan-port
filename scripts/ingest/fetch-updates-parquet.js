#!/usr/bin/env node
/**
 * Canton Ledger Ingestion Script - Parquet Version
 * 
 * Fetches ledger updates from Canton Scan API and writes to partitioned parquet files.
 * This replaces the Supabase/Postgres ingestion with local file storage.
 * 
 * Usage:
 *   node fetch-updates-parquet.js          # Resume from backfill cursor
 *   node fetch-updates-parquet.js --live   # Start from current API time (live mode)
 */

import dotenv from 'dotenv';
dotenv.config();

import axios from 'axios';
import https from 'https';
import { normalizeUpdate, normalizeEvent, flattenEventsInTreeOrder } from './parquet-schema.js';
// Use binary writer (Protobuf + ZSTD) for consistency with backfill and to capture raw_json
import { bufferUpdates, bufferEvents, flushAll, getBufferStats, setMigrationId } from './write-binary.js';

// Parse command line arguments
const args = process.argv.slice(2);
const LIVE_MODE = args.includes('--live') || args.includes('-l');

// Configuration
const SCAN_URL = process.env.SCAN_URL || 'https://scan.sv-2.us.cip-testing.network.canton.global/api';
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 100;
const POLL_INTERVAL = parseInt(process.env.POLL_INTERVAL) || 5000;

// Axios client with retry logic
const client = axios.create({
  baseURL: SCAN_URL,
  timeout: 60000,
  httpsAgent: new https.Agent({ rejectUnauthorized: false }),
});

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
// Default Windows path: C:\ledger_raw
const WIN_DEFAULT = 'C:\\ledger_raw';
const DATA_DIR = process.env.DATA_DIR || WIN_DEFAULT;

// Track state
let lastTimestamp = null;
let lastMigrationId = null;
let migrationId = null;
let isRunning = true;

// Cursor directory (same as backfill script)
const CURSOR_DIR = path.join(DATA_DIR, 'cursors');
const LIVE_CURSOR_FILE = path.join(CURSOR_DIR, 'live-cursor.json');

/**
 * Get current time from the Scan API (latest available record time)
 */
async function getCurrentAPITime() {
  try {
    const response = await client.post('/v2/updates', {
      page_size: 1,
      daml_value_encoding: 'compact_json',
    });
    
    const transactions = response.data?.transactions || [];
    if (transactions.length > 0) {
      const latest = transactions[0];
      return {
        recordTime: latest.record_time,
        migrationId: latest.migration_id
      };
    }
    return null;
  } catch (err) {
    console.error('Failed to get current API time:', err.message);
    return null;
  }
}

/**
 * Load live cursor if it exists
 */
function loadLiveCursor() {
  if (!fs.existsSync(LIVE_CURSOR_FILE)) {
    return null;
  }
  try {
    const data = JSON.parse(fs.readFileSync(LIVE_CURSOR_FILE, 'utf8'));
    console.log(`📍 Loaded live cursor: migration=${data.migration_id}, record_time=${data.record_time}`);
    return data;
  } catch (err) {
    console.warn(`⚠️ Failed to read live cursor: ${err.message}`);
    return null;
  }
}

/**
 * Save live cursor state
 */
function saveLiveCursor(migrationId, recordTime) {
  if (!fs.existsSync(CURSOR_DIR)) {
    fs.mkdirSync(CURSOR_DIR, { recursive: true });
  }
  const cursor = {
    migration_id: migrationId,
    record_time: recordTime,
    updated_at: new Date().toISOString(),
    mode: 'live'
  };
  fs.writeFileSync(LIVE_CURSOR_FILE, JSON.stringify(cursor, null, 2));
}

/**
 * Find the latest timestamp from backfill cursor files
 * This is the authoritative source for where backfill stopped
 */
async function findLatestTimestamp() {
  // In live mode, try to resume from live cursor, but never go backwards behind backfill.
  if (LIVE_MODE) {
    const liveCursor = loadLiveCursor();
    const liveMigration = liveCursor?.migration_id ?? null;
    const liveTime = liveCursor?.record_time ?? null;

    // Always compute the best-known backfill resume point too.
    const backfillTime = findLatestFromCursors();
    const backfillMigration = lastMigrationId;

    // If we have both, prefer whichever is newer.
    if (liveCursor && backfillTime) {
      const liveTs = new Date(liveTime).getTime();
      const backfillTs = new Date(backfillTime).getTime();

      const useBackfill =
        backfillMigration > liveMigration ||
        (backfillMigration === liveMigration && backfillTs > liveTs);

      if (useBackfill) {
        console.log(
          `⚠️ Live cursor is behind backfill (live m${liveMigration}@${liveTime}); continuing from backfill (m${backfillMigration}@${backfillTime})`
        );
        return backfillTime;
      }

      lastMigrationId = liveMigration;
      return liveTime;
    }

    // Only live cursor exists
    if (liveCursor) {
      lastMigrationId = liveMigration;
      return liveTime;
    }

    // Only backfill cursor exists
    if (backfillTime) {
      return backfillTime;
    }

    console.log('🔴 LIVE MODE: No live/backfill cursor found, falling back to raw files...');
  } else {
    // Non-live mode: cursor files from backfill are authoritative
    const backfillTime = findLatestFromCursors();
    if (backfillTime) return backfillTime;
  }

  // Fallback: check raw data directory for binary files
  const rawDir = path.join(DATA_DIR, 'raw');
  if (fs.existsSync(rawDir)) {
    const result = await findLatestFromRawData(rawDir);
    if (result) return result;
  }

  console.log('📁 No existing backfill data found, starting fresh');
  return null;
}

/**
 * Find latest timestamp from backfill cursor files
 * For LIVE updates, we want to continue FORWARD from the LATEST timestamp (max_time/last_before)
 * NOT backward from min_time
 */
function findLatestFromCursors() {
  if (!fs.existsSync(CURSOR_DIR)) {
    console.log('📁 No cursor directory found');
    return null;
  }
  
  const cursorFiles = fs.readdirSync(CURSOR_DIR)
    .filter(f => f.endsWith('.json'));
  
  if (cursorFiles.length === 0) {
    console.log('📁 No cursor files found');
    return null;
  }
  
  console.log(`📁 Found ${cursorFiles.length} cursor file(s)`);
  
  // For live updates, find the LATEST timestamp to continue FORWARD from
  // We want max_time (the newest point in the backfill) or last_before if backfill incomplete
  let latestTimestamp = null;
  let latestMigration = null;
  let selectedCursor = null;
  
  for (const file of cursorFiles) {
    try {
      const cursorPath = path.join(CURSOR_DIR, file);
      const cursor = JSON.parse(fs.readFileSync(cursorPath, 'utf8'));
      
      // Skip non-cursor files
      if (!cursor.migration_id && !cursor.max_time && !cursor.min_time) continue;
      
      const migration = cursor.migration_id;
      
      // For live updates: use max_time (the newest data point reached)
      // If backfill is complete, max_time is where we should continue from
      // If incomplete, last_before tells us where the cursor was during backfill
      const maxTime = cursor.max_time;
      
      if (maxTime) {
        const timestamp = new Date(maxTime).getTime();
        const currentBest = latestTimestamp ? new Date(latestTimestamp).getTime() : 0;
        
        // Prefer the highest migration with the latest max_time
        if (!latestTimestamp || 
            migration > latestMigration ||
            (migration === latestMigration && timestamp > currentBest)) {
          latestTimestamp = maxTime;
          latestMigration = migration;
          selectedCursor = cursor;
        }
      }
      
      console.log(`   • ${file}: migration=${migration}, max_time=${maxTime}, complete=${cursor.complete || false}`);
    } catch (err) {
      console.warn(`   ⚠️ Failed to read cursor ${file}: ${err.message}`);
    }
  }
  
  if (latestTimestamp && selectedCursor) {
    console.log(`📍 Live updates will continue from: migration=${latestMigration}, timestamp=${latestTimestamp}`);
    lastMigrationId = latestMigration;
    return latestTimestamp;
  }
  
  return null;
}

/**
 * Fallback: Find latest timestamp from raw binary data files
 */
async function findLatestFromRawData(rawDir) {
  // Check events subdirectory
  const eventsDir = path.join(rawDir, 'events');
  if (!fs.existsSync(eventsDir)) {
    return null;
  }
  
  // Get migration directories
  const migrationDirs = fs.readdirSync(eventsDir)
    .filter(d => d.startsWith('migration-'))
    .sort()
    .reverse();
  
  if (migrationDirs.length === 0) {
    return null;
  }
  
  // Check most recent migration for date directories
  for (const migDir of migrationDirs.slice(0, 2)) {
    const migPath = path.join(eventsDir, migDir);
    const dateDirs = fs.readdirSync(migPath)
      .filter(d => /^\d{4}-\d{2}-\d{2}$/.test(d))
      .sort()
      .reverse();
    
    if (dateDirs.length > 0) {
      // Use the earliest date directory as the resume point
      const earliestDate = dateDirs[dateDirs.length - 1];
      const timestamp = `${earliestDate}T00:00:00Z`;
      console.log(`📍 Found data in ${migDir}/${earliestDate}, resuming from ${timestamp}`);
      return timestamp;
    }
  }
  
  return null;
}

/**
 * Detect latest migration from the scan API
 * Uses backfill cursor if available, otherwise queries API
 */
async function detectLatestMigration() {
  // If we found a migration from backfill cursor, use it
  if (lastMigrationId !== null) {
    migrationId = lastMigrationId;
    console.log(`📍 Using migration_id from backfill cursor: ${migrationId}`);
    setMigrationId(migrationId);
    return migrationId;
  }
  
  try {
    // Query the API to detect current migration
    const response = await client.post('/v2/updates', {
      page_size: 1
    });
    
    // v2/updates response contains transactions array with migration_id
    const transactions = response.data?.transactions || [];
    if (transactions.length > 0 && transactions[0].migration_id !== undefined) {
      migrationId = transactions[0].migration_id;
      console.log(`📍 Detected migration_id from API: ${migrationId}`);
      setMigrationId(migrationId);
      return migrationId;
    }
    
    // Fallback: use default migration ID 1
    console.warn('⚠️ Could not detect migration_id, using default: 1');
    migrationId = 1;
    setMigrationId(migrationId);
    return migrationId;
  } catch (err) {
    console.error('Failed to detect migration:', err.message);
    console.warn('⚠️ Using fallback migration_id: 1');
    migrationId = 1;
    setMigrationId(migrationId);
    return migrationId;
  }
}

/**
 * Fetch updates from the scan API using v2/updates
 * Uses proper pagination with after.after_migration_id and after.after_record_time
 */
async function fetchUpdates(afterMigrationId = null, afterRecordTime = null) {
  try {
    const payload = {
      page_size: BATCH_SIZE,
      daml_value_encoding: 'compact_json',
    };
    
    // Use the "after" object for proper pagination (v2 API format)
    if (afterMigrationId !== null && afterRecordTime) {
      payload.after = {
        after_migration_id: afterMigrationId,
        after_record_time: afterRecordTime
      };
    }
    
    const response = await client.post('/v2/updates', payload);
    
    // v2/updates returns { transactions: [...] }
    const transactions = response.data?.transactions || [];
    return { 
      items: transactions,
      // Track the last item for next pagination
      lastMigrationId: transactions.length > 0 ? transactions[transactions.length - 1].migration_id : null,
      lastRecordTime: transactions.length > 0 ? transactions[transactions.length - 1].record_time : null
    };
  } catch (err) {
    if (err.response?.status === 404) {
      return { items: [], lastMigrationId: null, lastRecordTime: null };
    }
    throw err;
  }
}

/**
 * Process a batch of updates
 */
async function processUpdates(items) {
  const updates = [];
  const events = [];

  for (const item of items) {
    // Normalize update (handles {transaction}, {reassignment}, or already-flat)
    const update = normalizeUpdate(item);
    updates.push(update);

    // v2/updates returns a Transaction/Reassignment shape with events_by_id + root_event_ids
    // (NOT an array at item.transaction.events)
    const u = item.transaction || item.reassignment || item;
    const eventsById = u?.events_by_id || u?.eventsById || {};
    const rootEventIds = u?.root_event_ids || u?.rootEventIds || [];

    const flattened = flattenEventsInTreeOrder(eventsById, rootEventIds);
    for (const ev of flattened) {
      // Preserve raw event (includes inner created_event/exercised_event, etc.)
      const normalizedEvent = normalizeEvent(ev, update.update_id, migrationId, ev, u);
      events.push(normalizedEvent);
    }

    // Reassignment shape (if present) may not have events_by_id; keep 0 events in that case.
  }

  // Buffer for batch writing (async)
  await bufferUpdates(updates);
  await bufferEvents(events);

  return { updates: updates.length, events: events.length };
}

/**
 * Main ingestion loop
 */
async function runIngestion() {
  const modeLabel = LIVE_MODE ? '🔴 LIVE MODE' : '📜 RESUME MODE';
  console.log(`🚀 Starting Canton ledger ingestion (v2/updates mode) - ${modeLabel}\n`);
  
  // Check for existing data from backfill first (sets lastMigrationId if found)
  lastTimestamp = await findLatestTimestamp();
  
  // Then detect/confirm migration
  await detectLatestMigration();
  
  // Track pagination state for v2/updates
  let afterMigrationId = lastMigrationId || migrationId;
  let afterRecordTime = lastTimestamp;
  
  if (afterRecordTime) {
    console.log(`📍 ${LIVE_MODE ? 'Live ingestion' : 'Resuming'} from: migration=${afterMigrationId}, record_time=${afterRecordTime}`);
  } else {
    console.log('📍 Starting fresh (no existing data found)');
    afterMigrationId = null; // Start from beginning
  }
  
  let totalUpdates = 0;
  let totalEvents = 0;
  let emptyPolls = 0;
  
  while (isRunning) {
    try {
      // Fetch using v2/updates with proper (migration_id, record_time) pagination
      const data = await fetchUpdates(afterMigrationId, afterRecordTime);
      
      if (!data.items || data.items.length === 0) {
        emptyPolls++;
        
        // Flush any remaining buffered data
        if (emptyPolls === 1) {
          const flushed = await flushAll();
          if (flushed.length > 0) {
            console.log(`💾 Flushed ${flushed.length} files`);
          }
          // Save live cursor after flush
          if (LIVE_MODE && afterRecordTime) {
            saveLiveCursor(afterMigrationId, afterRecordTime);
          }
        }
        
        // Log status periodically
        if (emptyPolls % 12 === 0) { // Every minute at 5s intervals
          const stats = getBufferStats();
          console.log(`⏳ ${LIVE_MODE ? '[LIVE]' : ''} Waiting for new updates... (buffered: ${stats.updates} updates, ${stats.events} events)`);
        }
        
        await sleep(POLL_INTERVAL);
        continue;
      }
      
      emptyPolls = 0;
      
      const { updates, events } = await processUpdates(data.items);
      totalUpdates += updates;
      totalEvents += events;
      
      // Update pagination cursor from response
      if (data.lastMigrationId !== null) {
        afterMigrationId = data.lastMigrationId;
      }
      if (data.lastRecordTime) {
        afterRecordTime = data.lastRecordTime;
      }
      
      // Periodically save live cursor (every 10 batches)
      if (LIVE_MODE && totalUpdates % (BATCH_SIZE * 10) < BATCH_SIZE) {
        saveLiveCursor(afterMigrationId, afterRecordTime);
      }
      
      const stats = getBufferStats();
      const modePrefix = LIVE_MODE ? '🔴' : '📦';
      console.log(`${modePrefix} Processed ${updates} updates, ${events} events | Total: ${totalUpdates} updates, ${totalEvents} events | Cursor: m${afterMigrationId}@${afterRecordTime?.substring(0, 19) || 'start'}`);
      
    } catch (err) {
      console.error('❌ Error during ingestion:', err.message);
      await sleep(10000); // Wait 10s on error
    }
  }
}

/**
 * Sleep helper
 */
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Graceful shutdown
 */
async function shutdown() {
  console.log('\n🛑 Shutting down...');
  isRunning = false;
  
  // Flush remaining data
  const flushed = await flushAll();
  if (flushed.length > 0) {
    console.log(`💾 Flushed ${flushed.length} files on shutdown`);
  }
  
  // Save final live cursor state
  if (LIVE_MODE && lastTimestamp) {
    saveLiveCursor(lastMigrationId || migrationId, lastTimestamp);
    console.log('💾 Saved live cursor state');
  }
  
  process.exit(0);
}

process.on('SIGINT', () => shutdown());
process.on('SIGTERM', () => shutdown());

// Run
runIngestion().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
