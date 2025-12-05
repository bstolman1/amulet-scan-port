#!/usr/bin/env node
/**
 * Canton Ledger Backfill Script - Parquet Version
 * 
 * Fetches historical ledger data using the backfilling API
 * and writes to partitioned parquet files.
 */

import dotenv from 'dotenv';
dotenv.config();

import axios from 'axios';
import https from 'https';
import { existsSync, readFileSync, writeFileSync, mkdirSync } from 'fs';
import { join } from 'path';
import { normalizeUpdate, normalizeEvent, getPartitionPath } from './parquet-schema.js';
import { bufferUpdates, bufferEvents, flushAll, getBufferStats } from './write-parquet.js';

// Configuration
const SCAN_URL = process.env.SCAN_URL || 'https://scan.sv-2.us.cip-testing.network.canton.global/api';
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 500;
const CURSOR_DIR = process.env.CURSOR_DIR || './data/cursors';

// Axios client
const client = axios.create({
  baseURL: SCAN_URL,
  timeout: 120000,
  httpsAgent: new https.Agent({ rejectUnauthorized: false }),
});

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
function saveCursor(migrationId, synchronizerId, cursor) {
  mkdirSync(CURSOR_DIR, { recursive: true });
  
  const cursorFile = join(CURSOR_DIR, `cursor-${migrationId}-${sanitize(synchronizerId)}.json`);
  writeFileSync(cursorFile, JSON.stringify(cursor, null, 2));
}

/**
 * Sanitize string for filename
 */
function sanitize(str) {
  return str.replace(/[^a-zA-Z0-9-_]/g, '_').substring(0, 50);
}

/**
 * Detect all available migrations
 */
async function detectMigrations() {
  const migrations = [];
  
  for (let id = 0; id <= 10; id++) {
    try {
      const response = await client.post('/v0/backfilling/updates-before', {
        migration_id: id,
        page_size: 1,
      });
      
      if (response.data) {
        migrations.push(id);
        console.log(`✅ Found migration ${id}`);
      }
    } catch (err) {
      // Migration doesn't exist
    }
  }
  
  return migrations;
}

/**
 * Get migration info (synchronizer ranges)
 */
async function getMigrationInfo(migrationId) {
  try {
    const response = await client.get(`/v0/backfilling/synchronizer-ranges/${migrationId}`);
    return response.data;
  } catch (err) {
    console.error(`Failed to get migration info for ${migrationId}:`, err.message);
    return null;
  }
}

/**
 * Fetch backfill data before a timestamp
 */
async function fetchBackfillBefore(migrationId, synchronizerId, before) {
  const payload = {
    migration_id: migrationId,
    synchronizer_id: synchronizerId,
    page_size: BATCH_SIZE,
  };
  
  if (before) {
    payload.before = before;
  }
  
  const response = await client.post('/v0/backfilling/updates-before', payload);
  return response.data;
}

/**
 * Process backfill items
 */
function processBackfillItems(items, migrationId) {
  const updates = [];
  const events = [];
  
  for (const item of items) {
    const update = normalizeUpdate(item);
    update.migration_id = migrationId;
    updates.push(update);
    
    // Extract events
    const tx = item.transaction;
    if (tx?.events) {
      for (const event of tx.events) {
        const normalizedEvent = normalizeEvent(event, update.update_id, migrationId);
        events.push(normalizedEvent);
      }
    }
  }
  
  bufferUpdates(updates);
  bufferEvents(events);
  
  return { updates: updates.length, events: events.length };
}

/**
 * Backfill a single synchronizer
 */
async function backfillSynchronizer(migrationId, synchronizerId, range) {
  console.log(`\n📍 Backfilling migration ${migrationId}, synchronizer ${synchronizerId}`);
  console.log(`   Range: ${range.min_time} to ${range.max_time}`);
  
  // Load existing cursor
  let cursor = loadCursor(migrationId, synchronizerId);
  let lastBefore = cursor?.last_before || range.max_time;
  const minTime = new Date(range.min_time).getTime();
  
  let totalUpdates = 0;
  let totalEvents = 0;
  let pageCount = 0;
  
  while (true) {
    try {
      const data = await fetchBackfillBefore(migrationId, synchronizerId, lastBefore);
      
      if (!data.items || data.items.length === 0) {
        console.log(`   ✅ Completed synchronizer ${synchronizerId}`);
        break;
      }
      
      const { updates, events } = processBackfillItems(data.items, migrationId);
      totalUpdates += updates;
      totalEvents += events;
      pageCount++;
      
      // Get earliest timestamp from batch
      const timestamps = data.items
        .map(i => i.transaction?.record_time || i.reassignment?.record_time)
        .filter(Boolean)
        .map(t => new Date(t).getTime());
      
      const earliestTime = Math.min(...timestamps);
      lastBefore = new Date(earliestTime).toISOString();
      
      // Save cursor periodically
      if (pageCount % 10 === 0) {
        saveCursor(migrationId, synchronizerId, {
          last_before: lastBefore,
          total_updates: totalUpdates,
          total_events: totalEvents,
          updated_at: new Date().toISOString(),
        });
        
        const stats = getBufferStats();
        console.log(`   📦 Page ${pageCount}: ${totalUpdates} updates, ${totalEvents} events | Buffer: ${stats.updates}/${stats.events}`);
      }
      
      // Check if we've reached the beginning
      if (earliestTime <= minTime) {
        console.log(`   ✅ Reached min_time for synchronizer ${synchronizerId}`);
        break;
      }
      
    } catch (err) {
      console.error(`   ❌ Error at page ${pageCount}:`, err.message);
      
      // Save cursor and wait before retry
      saveCursor(migrationId, synchronizerId, {
        last_before: lastBefore,
        total_updates: totalUpdates,
        total_events: totalEvents,
        error: err.message,
        updated_at: new Date().toISOString(),
      });
      
      await sleep(5000);
    }
  }
  
  // Flush remaining data
  flushAll();
  
  // Mark as complete
  saveCursor(migrationId, synchronizerId, {
    last_before: lastBefore,
    total_updates: totalUpdates,
    total_events: totalEvents,
    complete: true,
    updated_at: new Date().toISOString(),
  });
  
  return { updates: totalUpdates, events: totalEvents };
}

/**
 * Sleep helper
 */
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Main backfill function
 */
async function runBackfill() {
  console.log('🚀 Starting Canton ledger backfill (Parquet mode)\n');
  
  // Detect migrations
  console.log('🔍 Detecting migrations...');
  const migrations = await detectMigrations();
  console.log(`Found ${migrations.length} migrations: ${migrations.join(', ')}\n`);
  
  let grandTotalUpdates = 0;
  let grandTotalEvents = 0;
  
  for (const migrationId of migrations) {
    console.log(`\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
    console.log(`📋 Processing migration ${migrationId}`);
    console.log(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
    
    const info = await getMigrationInfo(migrationId);
    
    if (!info?.synchronizer_ranges) {
      console.log(`   ⚠️ No synchronizer ranges found`);
      continue;
    }
    
    for (const [synchronizerId, range] of Object.entries(info.synchronizer_ranges)) {
      // Check if already complete
      const cursor = loadCursor(migrationId, synchronizerId);
      if (cursor?.complete) {
        console.log(`   ⏭️ Skipping ${synchronizerId} (already complete)`);
        continue;
      }
      
      const { updates, events } = await backfillSynchronizer(migrationId, synchronizerId, range);
      grandTotalUpdates += updates;
      grandTotalEvents += events;
    }
  }
  
  console.log(`\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
  console.log(`✅ Backfill complete!`);
  console.log(`   Total updates: ${grandTotalUpdates.toLocaleString()}`);
  console.log(`   Total events: ${grandTotalEvents.toLocaleString()}`);
  console.log(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n`);
}

// Graceful shutdown
process.on('SIGINT', () => {
  console.log('\n🛑 Shutting down...');
  flushAll();
  process.exit(0);
});

// Run
runBackfill().catch(err => {
  console.error('Fatal error:', err);
  flushAll();
  process.exit(1);
});
