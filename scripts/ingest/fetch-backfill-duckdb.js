#!/usr/bin/env node
/**
 * Canton Ledger Backfill Script - DuckDB B3 Mode
 * 
 * Fetches historical ledger data using the backfilling API
 * and writes directly to Parquet using DuckDB (no intermediate JSONL).
 * 
 * Optimizations:
 * - Parallel API fetching (configurable concurrency)
 * - Prefetch queue for continuous data flow
 * - Direct DuckDB → Parquet writes (faster than JSONL conversion)
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
import { bufferUpdates, bufferEvents, flushAll, getBufferStats, waitForWrites } from './duckdb-writer.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// TLS config - must be set before any requests
process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

// Configuration
const SCAN_URL = process.env.SCAN_URL || 'https://scan.sv-1.global.canton.network.sync.global/api/scan';
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 500;
const CURSOR_DIR = process.env.CURSOR_DIR || join(__dirname, '../../data/cursors');
const PARALLEL_FETCHES = parseInt(process.env.PARALLEL_FETCHES) || 4; // Concurrent API requests

// Axios client with connection pooling
const client = axios.create({
  baseURL: SCAN_URL,
  httpAgent: new HttpAgent({ keepAlive: true, keepAliveMsecs: 30000, maxSockets: PARALLEL_FETCHES + 2 }),
  httpsAgent: new HttpsAgent({
    keepAlive: true,
    keepAliveMsecs: 30000,
    rejectUnauthorized: false,
    maxSockets: PARALLEL_FETCHES + 2,
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
  try {
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
  } catch (err) {
    console.error(`   [saveCursor] ❌ FAILED: ${err.message}`);
  }
}

/**
 * Sanitize string for filename
 */
function sanitize(str) {
  return str.replace(/[^a-zA-Z0-9-_]/g, '_').substring(0, 50);
}

/**
 * Detect all available migrations via /v0/state/acs/snapshot-timestamp
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
 * Fetch backfill data before a timestamp (single request)
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
 * Parallel fetch with sliding window - fetches multiple pages concurrently
 * Returns results in order as they complete
 */
async function parallelFetchBatch(migrationId, synchronizerId, startBefore, atOrAfter, count) {
  const results = [];
  const pending = new Map();
  let nextBefore = startBefore;
  let fetched = 0;
  let reachedEnd = false;
  
  // Start initial parallel fetches
  for (let i = 0; i < Math.min(count, PARALLEL_FETCHES); i++) {
    if (reachedEnd) break;
    
    const fetchId = fetched;
    const before = nextBefore;
    
    pending.set(fetchId, {
      before,
      promise: fetchBackfillBefore(migrationId, synchronizerId, before, atOrAfter)
        .then(data => ({ fetchId, before, data, error: null }))
        .catch(error => ({ fetchId, before, data: null, error }))
    });
    
    fetched++;
    // Estimate next 'before' by subtracting time (will be corrected by actual results)
    const d = new Date(nextBefore);
    d.setHours(d.getHours() - 1);
    nextBefore = d.toISOString();
  }
  
  // Process results as they complete
  while (pending.size > 0) {
    // Wait for any fetch to complete
    const completed = await Promise.race(
      Array.from(pending.values()).map(p => p.promise)
    );
    
    pending.delete(completed.fetchId);
    
    if (completed.error) {
      // Re-throw errors to be handled by caller
      throw completed.error;
    }
    
    const txs = completed.data?.transactions || [];
    
    if (txs.length === 0) {
      reachedEnd = true;
      continue;
    }
    
    results.push({ transactions: txs, before: completed.before });
    
    // Find earliest timestamp from this batch for next fetch
    let earliest = null;
    for (const tx of txs) {
      const t = getEventTime(tx);
      if (!t) continue;
      if (!earliest || t < earliest) earliest = t;
    }
    
    if (earliest && earliest <= atOrAfter) {
      reachedEnd = true;
      continue;
    }
    
    // Start a new fetch if we haven't reached the limit
    if (!reachedEnd && fetched < count && pending.size < PARALLEL_FETCHES) {
      const fetchId = fetched;
      const before = earliest || nextBefore;
      
      pending.set(fetchId, {
        before,
        promise: fetchBackfillBefore(migrationId, synchronizerId, before, atOrAfter)
          .then(data => ({ fetchId, before, data, error: null }))
          .catch(error => ({ fetchId, before, data: null, error }))
      });
      
      fetched++;
      nextBefore = before;
    }
  }
  
  return { results, reachedEnd };
}

/**
 * Backfill a single synchronizer with parallel fetching
 */
async function backfillSynchronizer(migrationId, synchronizerId, minTime, maxTime) {
  console.log(`\n📍 Backfilling migration ${migrationId}, synchronizer ${synchronizerId.substring(0, 30)}...`);
  console.log(`   Range: ${minTime} to ${maxTime}`);
  console.log(`   Parallel fetches: ${PARALLEL_FETCHES}`);
  
  // Load existing cursor
  let cursor = loadCursor(migrationId, synchronizerId);
  let before = cursor?.last_before || maxTime;
  const atOrAfter = minTime;
  
  let totalUpdates = 0;
  let totalEvents = 0;
  let batchCount = 0;
  const startTime = Date.now();
  
  // Save initial cursor
  saveCursor(migrationId, synchronizerId, {
    last_before: before,
    total_updates: 0,
    total_events: 0,
    started_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
  }, minTime, maxTime);
  
  while (true) {
    try {
      // Fetch multiple pages in parallel
      const { results, reachedEnd } = await parallelFetchBatch(
        migrationId, synchronizerId, before, atOrAfter, PARALLEL_FETCHES * 3
      );
      
      if (results.length === 0) {
        console.log(`   ✅ No more transactions. Marking complete.`);
        break;
      }
      
      // Process all fetched batches
      let batchUpdates = 0;
      let batchEvents = 0;
      let earliestTime = before;
      
      for (const { transactions } of results) {
        const { updates, events } = await processBackfillItems(transactions, migrationId);
        batchUpdates += updates;
        batchEvents += events;
        
        // Track earliest timestamp
        for (const tx of transactions) {
          const t = getEventTime(tx);
          if (t && t < earliestTime) earliestTime = t;
        }
      }
      
      totalUpdates += batchUpdates;
      totalEvents += batchEvents;
      batchCount++;
      
      // Update cursor position
      before = earliestTime;
      
      // Calculate throughput
      const elapsed = (Date.now() - startTime) / 1000;
      const throughput = Math.round(totalUpdates / elapsed);
      
      // Save cursor and log progress
      saveCursor(migrationId, synchronizerId, {
        last_before: before,
        total_updates: totalUpdates,
        total_events: totalEvents,
        updated_at: new Date().toISOString(),
      }, minTime, maxTime);
      
      const stats = getBufferStats();
      console.log(`   📦 Batch ${batchCount}: +${batchUpdates} updates, +${batchEvents} events | Total: ${totalUpdates.toLocaleString()} | ${throughput}/s | Queue: ${stats.queuedWrites}/${stats.activeWrites}`);
      
      if (reachedEnd || earliestTime <= atOrAfter) {
        console.log(`   ✅ Reached lower bound. Complete.`);
        break;
      }
      
    } catch (err) {
      const status = err.response?.status;
      const msg = err.response?.data?.error || err.message;
      console.error(`   ❌ Error at batch ${batchCount} (status ${status || "n/a"}): ${msg}`);
      
      // Save cursor and retry for transient errors
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
  await flushAll();
  
  // Mark as complete
  saveCursor(migrationId, synchronizerId, {
    last_before: before,
    total_updates: totalUpdates,
    total_events: totalEvents,
    complete: true,
    updated_at: new Date().toISOString(),
  }, minTime, maxTime);
  
  const totalTime = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`   ⏱️ Completed in ${totalTime}s (${Math.round(totalUpdates / parseFloat(totalTime))}/s avg)`);
  
  return { updates: totalUpdates, events: totalEvents };
}

/**
 * Main backfill function
 */
async function runBackfill() {
  console.log("\n" + "=".repeat(80));
  console.log("🚀 Starting Canton ledger backfill (DuckDB B3 mode → direct Parquet)");
  console.log("   SCAN_URL:", SCAN_URL);
  console.log("   BATCH_SIZE:", BATCH_SIZE);
  console.log("   PARALLEL_FETCHES:", PARALLEL_FETCHES);
  console.log("   CURSOR_DIR:", CURSOR_DIR);
  console.log("   FLUSH_ROWS:", process.env.FLUSH_ROWS || '250000 (default)');
  console.log("   FLUSH_MS:", process.env.FLUSH_MS || '30000 (default)');
  console.log("=".repeat(80));
  
  // Ensure cursor directory exists
  mkdirSync(CURSOR_DIR, { recursive: true });
  
  // Detect migrations
  const migrations = await detectMigrations();
  
  if (!migrations.length) {
    console.log("⚠️ No migrations found. Exiting.");
    return;
  }
  
  let grandTotalUpdates = 0;
  let grandTotalEvents = 0;
  const grandStartTime = Date.now();
  
  for (const migrationId of migrations) {
    console.log(`\n${"─".repeat(80)}`);
    console.log(`📘 Migration ${migrationId}: fetching backfilling metadata`);
    console.log(`${"─".repeat(80)}`);
    
    const info = await getMigrationInfo(migrationId);
    
    if (!info) {
      console.log("   ℹ️  No backfilling info; skipping this migration.");
      continue;
    }
    
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
  
  const grandTotalTime = ((Date.now() - grandStartTime) / 1000).toFixed(1);
  
  console.log(`\n${"═".repeat(80)}`);
  console.log(`🎉 Backfill complete!`);
  console.log(`   Total updates: ${grandTotalUpdates.toLocaleString()}`);
  console.log(`   Total events: ${grandTotalEvents.toLocaleString()}`);
  console.log(`   Total time: ${grandTotalTime}s`);
  console.log(`   Average throughput: ${Math.round(grandTotalUpdates / parseFloat(grandTotalTime))}/s`);
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
