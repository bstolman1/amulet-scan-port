#!/usr/bin/env node
/**
 * Canton Ledger Ingestion Script - Parquet Version
 * 
 * Fetches ledger updates from Canton Scan API and writes to partitioned parquet files.
 * This replaces the Supabase/Postgres ingestion with local file storage.
 */

import dotenv from 'dotenv';
dotenv.config();

import axios from 'axios';
import https from 'https';
import { normalizeUpdate, normalizeEvent } from './parquet-schema.js';
// Use binary writer (Protobuf + ZSTD) for consistency with backfill and to capture raw_json
import { bufferUpdates, bufferEvents, flushAll, getBufferStats, setMigrationId } from './write-binary.js';

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
// Default WSL path: /home/bstolz/canton-explorer/data
const WSL_DEFAULT = '/home/bstolz/canton-explorer/data';
const DATA_DIR = process.env.DATA_DIR || WSL_DEFAULT;

// Track state
let lastTimestamp = null;
let lastMigrationId = null;
let migrationId = null;
let isRunning = true;

// Cursor directory (same as backfill script)
const CURSOR_DIR = path.join(DATA_DIR, 'cursors');

/**
 * Find the latest timestamp from backfill cursor files
 * This is the authoritative source for where backfill stopped
 */
async function findLatestTimestamp() {
  // First, check cursor files from backfill (most reliable)
  const cursorResult = findLatestFromCursors();
  if (cursorResult) {
    return cursorResult;
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
 */
function findLatestFromCursors() {
  if (!fs.existsSync(CURSOR_DIR)) {
    console.log('📁 No cursor directory found');
    return null;
  }
  
  const cursorFiles = fs.readdirSync(CURSOR_DIR)
    .filter(f => f.startsWith('cursor-') && f.endsWith('.json'));
  
  if (cursorFiles.length === 0) {
    console.log('📁 No cursor files found');
    return null;
  }
  
  console.log(`📁 Found ${cursorFiles.length} cursor file(s)`);
  
  // Find the cursor with the latest timestamp (min_time is the earliest point reached)
  // We want to continue from min_time (where backfill stopped going backward)
  let latestMinTime = null;
  let latestMigration = null;
  let selectedCursor = null;
  
  for (const file of cursorFiles) {
    try {
      const cursorPath = path.join(CURSOR_DIR, file);
      const cursor = JSON.parse(fs.readFileSync(cursorPath, 'utf8'));
      
      // min_time is where the backfill reached (going backward)
      // We want to start v2/updates from this point forward
      const minTime = cursor.min_time || cursor.last_before;
      const migration = cursor.migration_id;
      
      if (minTime) {
        // Prefer cursors from higher migrations, then earlier min_time
        if (!latestMinTime || 
            (migration > latestMigration) ||
            (migration === latestMigration && minTime < latestMinTime)) {
          latestMinTime = minTime;
          latestMigration = migration;
          selectedCursor = cursor;
        }
      }
      
      console.log(`   • ${file}: migration=${migration}, min_time=${minTime}, total=${cursor.total_updates || 0}`);
    } catch (err) {
      console.warn(`   ⚠️ Failed to read cursor ${file}: ${err.message}`);
    }
  }
  
  if (latestMinTime && selectedCursor) {
    console.log(`📍 Resuming from backfill cursor: migration=${latestMigration}, timestamp=${latestMinTime}`);
    lastMigrationId = latestMigration;
    return latestMinTime;
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
    // Normalize update
    const update = normalizeUpdate(item);
    updates.push(update);
    
    // Extract events from transaction
    const tx = item.transaction;
    if (tx?.events) {
      for (const event of tx.events) {
        // Pass complete event as raw to preserve all original data
        const normalizedEvent = normalizeEvent(event, update.update_id, migrationId, event);
        events.push(normalizedEvent);
      }
    }
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
  console.log('🚀 Starting Canton ledger ingestion (v2/updates mode)\n');
  
  // Check for existing data from backfill first (sets lastMigrationId if found)
  lastTimestamp = await findLatestTimestamp();
  
  // Then detect/confirm migration
  await detectLatestMigration();
  
  // Track pagination state for v2/updates
  let afterMigrationId = lastMigrationId || migrationId;
  let afterRecordTime = lastTimestamp;
  
  if (afterRecordTime) {
    console.log(`📍 Resuming from: migration=${afterMigrationId}, record_time=${afterRecordTime}`);
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
        }
        
        // Log status periodically
        if (emptyPolls % 12 === 0) { // Every minute at 5s intervals
          const stats = getBufferStats();
          console.log(`⏳ Waiting for new updates... (buffered: ${stats.updates} updates, ${stats.events} events)`);
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
      
      const stats = getBufferStats();
      console.log(`📦 Processed ${updates} updates, ${events} events | Total: ${totalUpdates} updates, ${totalEvents} events | Cursor: m${afterMigrationId}@${afterRecordTime?.substring(0, 19) || 'start'}`);
      
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
  
  process.exit(0);
}

process.on('SIGINT', () => shutdown());
process.on('SIGTERM', () => shutdown());

// Run
runIngestion().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
