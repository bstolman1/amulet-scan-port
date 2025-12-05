#!/usr/bin/env node
/**
 * Canton Ledger Backfill Script - Parquet Version
 * 
 * Fetches historical ledger data using the backfilling API
 * and writes to partitioned parquet files.
 * 
 * Uses the same API endpoints and configuration as the working
 * fetch-backfill-history.js script.
 */

import dotenv from 'dotenv';
dotenv.config();

import axios from 'axios';
import { Agent as HttpAgent } from 'http';
import { Agent as HttpsAgent } from 'https';
import { existsSync, readFileSync, writeFileSync, mkdirSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import { normalizeUpdate, normalizeEvent, getPartitionPath } from './parquet-schema.js';
import { bufferUpdates, bufferEvents, flushAll, getBufferStats, waitForWrites } from './write-parquet.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// TLS config - must be set before any requests
process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

// Configuration - use absolute paths relative to project root
const SCAN_URL = process.env.SCAN_URL || 'https://scan.sv-1.global.canton.network.sync.global/api/scan';
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 500;
const CURSOR_DIR = process.env.CURSOR_DIR || join(__dirname, '../../data/cursors');

// Axios client
const client = axios.create({
  baseURL: SCAN_URL,
  httpAgent: new HttpAgent({ keepAlive: true, keepAliveMsecs: 30000 }),
  httpsAgent: new HttpsAgent({
    keepAlive: true,
    keepAliveMsecs: 30000,
    rejectUnauthorized: false,
  }),
  timeout: 120000,
});

/**
 * Sleep helper
 */
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Retry with exponential backoff
 */
async function retryWithBackoff(fn, options = {}) {
  const {
    maxRetries = 5,
    baseDelay = 1000,
    maxDelay = 30000,
    shouldRetry = (error) => {
      const retryableCodes = ['ECONNRESET', 'ETIMEDOUT', 'ECONNREFUSED', 'EPIPE'];
      const retryableStatuses = [429, 500, 502, 503, 504];
      
      if (error.code && retryableCodes.includes(error.code)) return true;
      if (error.response?.status && retryableStatuses.includes(error.response.status)) return true;
      return false;
    }
  } = options;

  let lastError;
  
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;
      
      if (attempt === maxRetries || !shouldRetry(error)) {
        throw error;
      }

      const exponentialDelay = Math.min(baseDelay * Math.pow(2, attempt), maxDelay);
      const jitter = Math.random() * exponentialDelay * 0.3;
      const delay = exponentialDelay + jitter;
      
      console.log(`   ⏳ Retry attempt ${attempt + 1}/${maxRetries} after ${Math.round(delay)}ms (error: ${error.code || error.message})`);
      await sleep(delay);
    }
  }
  
  throw lastError;
}

/**
 * Load cursor from file
 */
function loadCursor(migrationId, synchronizerId) {
  const cursorFile = join(CURSOR_DIR, `cursor-${migrationId}-${sanitize(synchronizerId)}.json`);
  
  if (existsSync(cursorFile)) {
    return JSON.parse(readFileSync(cursorFile, 'utf8'));
  }
  
  return null;
}

/**
 * Save cursor to file
 */
function saveCursor(migrationId, synchronizerId, cursor, minTime, maxTime) {
  mkdirSync(CURSOR_DIR, { recursive: true });
  
  const cursorFile = join(CURSOR_DIR, `cursor-${migrationId}-${sanitize(synchronizerId)}.json`);
  const cursorData = {
    ...cursor,
    migration_id: migrationId,
    synchronizer_id: synchronizerId,
    id: `cursor-${migrationId}-${sanitize(synchronizerId)}`,
    cursor_name: `migration-${migrationId}-${synchronizerId.substring(0, 20)}`,
    min_time: minTime || cursor.min_time,
    max_time: maxTime || cursor.max_time,
    last_processed_round: cursor.last_processed_round || 0,
  };
  writeFileSync(cursorFile, JSON.stringify(cursorData, null, 2));
}

/**
 * Sanitize string for filename
 */
function sanitize(str) {
  return str.replace(/[^a-zA-Z0-9-_]/g, '_').substring(0, 50);
}

/**
 * Detect all available migrations via /v0/state/acs/snapshot-timestamp
 * This matches the working fetch-acs-data.js approach
 */
async function detectMigrations() {
  console.log("🔎 Detecting available migrations via /v0/state/acs/snapshot-timestamp");

  const migrations = [];
  let id = 1;

  while (true) {
    try {
      const res = await client.get("/v0/state/acs/snapshot-timestamp", {
        params: { migration_id: id, before: new Date().toISOString() },
      });

      if (res.data?.record_time) {
        migrations.push(id);
        console.log(`  • migration_id=${id} record_time=${res.data.record_time}`);
        id++;
      } else {
        break;
      }
    } catch (err) {
      if (err.response && err.response.status === 404) {
        break;
      }
      console.error(`❌ Error probing migration_id=${id}:`, err.response?.status, err.message);
      break;
    }
  }

  console.log(`✅ Found migrations: ${migrations.join(", ")}`);
  return migrations;
}

/**
 * Get migration info via POST /v0/backfilling/migration-info
 * Returns synchronizer ranges for backfilling
 */
async function getMigrationInfo(migrationId) {
  try {
    const res = await client.post("/v0/backfilling/migration-info", {
      migration_id: migrationId,
    });
    return res.data;
  } catch (err) {
    if (err.response && err.response.status === 404) {
      console.log(`ℹ️  No backfilling info for migration_id=${migrationId} (404)`);
      return null;
    }
    throw err;
  }
}

/**
 * Fetch backfill data before a timestamp
 */
async function fetchBackfillBefore(migrationId, synchronizerId, before, atOrAfter) {
  const payload = {
    migration_id: migrationId,
    synchronizer_id: synchronizerId,
    before,
    count: BATCH_SIZE,
  };
  
  if (atOrAfter) {
    payload.at_or_after = atOrAfter;
  }
  
  return await retryWithBackoff(async () => {
    const response = await client.post('/v0/backfilling/updates-before', payload);
    return response.data;
  });
}

/**
 * Get event time from transaction or reassignment
 */
function getEventTime(txOrReassign) {
  return (
    txOrReassign.record_time ||
    txOrReassign.event?.record_time ||
    txOrReassign.effective_at
  );
}

/**
 * Process backfill items (transactions array from API response)
 */
async function processBackfillItems(transactions, migrationId) {
  const updates = [];
  const events = [];
  
  for (const tx of transactions) {
    const isReassignment = !!tx.event;
    const update = normalizeUpdate(tx);
    update.migration_id = migrationId;
    updates.push(update);
    
    // Extract events based on type
    if (isReassignment) {
      const ce = tx.event?.created_event;
      if (ce) {
        const normalizedEvent = normalizeEvent(ce, update.update_id, migrationId);
        normalizedEvent.event_type = 'reassign_create';
        events.push(normalizedEvent);
      }
    } else {
      const eventsById = tx.events_by_id || {};
      for (const [eventId, ev] of Object.entries(eventsById)) {
        const normalizedEvent = normalizeEvent(ev, update.update_id, migrationId);
        normalizedEvent.event_id = eventId;
        events.push(normalizedEvent);
      }
    }
  }
  
  await bufferUpdates(updates);
  await bufferEvents(events);
  
  return { updates: updates.length, events: events.length };
}

/**
 * Backfill a single synchronizer
 */
async function backfillSynchronizer(migrationId, synchronizerId, minTime, maxTime) {
  console.log(`\n📍 Backfilling migration ${migrationId}, synchronizer ${synchronizerId.substring(0, 30)}...`);
  console.log(`   Range: ${minTime} to ${maxTime}`);
  
  // Load existing cursor
  let cursor = loadCursor(migrationId, synchronizerId);
  let before = cursor?.last_before || maxTime;
  const atOrAfter = minTime;
  
  let totalUpdates = 0;
  let totalEvents = 0;
  let pageCount = 0;
  
  while (true) {
    try {
      console.log(`   ➜ Requesting updates-before: before=${before}`);
      
      const data = await fetchBackfillBefore(migrationId, synchronizerId, before, atOrAfter);
      const txs = data?.transactions || [];
      
      if (!txs.length) {
        console.log(`   ✅ No more transactions. Marking complete.`);
        break;
      }
      
      const { updates, events } = processBackfillItems(txs, migrationId);
      totalUpdates += updates;
      totalEvents += events;
      pageCount++;
      
      // Get earliest timestamp from batch
      let earliest = null;
      for (const tx of txs) {
        const t = getEventTime(tx);
        if (!t) continue;
        if (!earliest || t < earliest) earliest = t;
      }
      
      // Check if we've reached the lower bound
      if (!earliest || earliest <= atOrAfter) {
        console.log(`   ✅ Reached lower bound of range. Complete.`);
        saveCursor(migrationId, synchronizerId, {
          last_before: earliest,
          total_updates: totalUpdates,
          total_events: totalEvents,
          complete: true,
          updated_at: new Date().toISOString(),
        }, minTime, maxTime);
        break;
      }
      
      before = earliest;
      
      // Save cursor periodically
      if (pageCount % 10 === 0) {
        saveCursor(migrationId, synchronizerId, {
          last_before: before,
          total_updates: totalUpdates,
          total_events: totalEvents,
          updated_at: new Date().toISOString(),
        }, minTime, maxTime);
        
        const stats = getBufferStats();
        console.log(`   📦 Page ${pageCount}: ${totalUpdates} updates, ${totalEvents} events | Buffer: ${stats.updates}/${stats.events}`);
      }
      
      console.log(`   📥 Stored ${txs.length} updates, new before=${before}`);
      
    } catch (err) {
      const status = err.response?.status;
      const msg = err.response?.data?.error || err.message;
      console.error(`   ❌ Error at page ${pageCount} (status ${status || "n/a"}): ${msg}`);
      
      // Save cursor and wait before retry for transient errors
      if ([429, 500, 502, 503, 504].includes(status)) {
        console.log("   ⏳ Transient error, backing off...");
        saveCursor(migrationId, synchronizerId, {
          last_before: before,
          total_updates: totalUpdates,
          total_events: totalEvents,
          error: msg,
          updated_at: new Date().toISOString(),
        }, minTime, maxTime);
        await sleep(5000);
        continue;
      }
      
      throw err;
    }
  }
  
  // Flush remaining data
  flushAll();
  
  // Mark as complete
  saveCursor(migrationId, synchronizerId, {
    last_before: before,
    total_updates: totalUpdates,
    total_events: totalEvents,
    complete: true,
    updated_at: new Date().toISOString(),
  }, minTime, maxTime);
  
  return { updates: totalUpdates, events: totalEvents };
}

/**
 * Main backfill function
 */
async function runBackfill() {
  console.log("\n" + "=".repeat(80));
  console.log("🚀 Starting Canton ledger backfill (Parquet mode)");
  console.log("   SCAN_URL:", SCAN_URL);
  console.log("   BATCH_SIZE:", BATCH_SIZE);
  console.log("=".repeat(80));
  
  // Detect migrations
  const migrations = await detectMigrations();
  
  if (!migrations.length) {
    console.log("⚠️ No migrations found. Exiting.");
    return;
  }
  
  let grandTotalUpdates = 0;
  let grandTotalEvents = 0;
  
  for (const migrationId of migrations) {
    console.log(`\n${"─".repeat(80)}`);
    console.log(`📘 Migration ${migrationId}: fetching backfilling metadata`);
    console.log(`${"─".repeat(80)}`);
    
    const info = await getMigrationInfo(migrationId);
    
    if (!info) {
      console.log("   ℹ️  No backfilling info; skipping this migration.");
      continue;
    }
    
    // API returns record_time_range array (not synchronizer_ranges object)
    const ranges = info.record_time_range || [];
    
    if (!ranges.length) {
      console.log("   ℹ️  No synchronizer ranges; skipping.");
      continue;
    }
    
    console.log(`   Found ${ranges.length} synchronizer ranges for migration ${migrationId}`);
    
    for (const range of ranges) {
      const synchronizerId = range.synchronizer_id;
      const minTime = range.min;
      const maxTime = range.max;
      
      // Check if already complete
      const cursor = loadCursor(migrationId, synchronizerId);
      if (cursor?.complete) {
        console.log(`   ⏭️ Skipping ${synchronizerId.substring(0, 30)}... (already complete)`);
        continue;
      }
      
      const { updates, events } = await backfillSynchronizer(migrationId, synchronizerId, minTime, maxTime);
      grandTotalUpdates += updates;
      grandTotalEvents += events;
    }
    
    console.log(`✅ Completed migration ${migrationId}`);
  }
  
  console.log(`\n${"═".repeat(80)}`);
  console.log(`🎉 Backfill complete!`);
  console.log(`   Total updates: ${grandTotalUpdates.toLocaleString()}`);
  console.log(`   Total events: ${grandTotalEvents.toLocaleString()}`);
  console.log(`${"═".repeat(80)}\n`);
}

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('\n🛑 Shutting down... waiting for writes to complete');
  await flushAll();
  await waitForWrites();
  console.log('✅ All writes complete');
  process.exit(0);
});

// Run
runBackfill().catch(async err => {
  console.error('\n❌ FATAL:', err.message);
  console.error(err.stack);
  await flushAll();
  await waitForWrites();
  process.exit(1);
});
