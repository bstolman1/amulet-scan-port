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
import { bufferUpdates, bufferEvents, flushAll, getBufferStats, setMigrationId } from './write-parquet.js';

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
import readline from 'readline';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const DATA_DIR = path.join(__dirname, '../../data');

// Track state
let lastOffset = null;
let lastTimestamp = null;
let migrationId = null;
let isRunning = true;

/**
 * Find the latest timestamp from existing data files (backfill or previous runs)
 */
async function findLatestTimestamp() {
  const eventsDir = path.join(DATA_DIR, 'events');
  
  if (!fs.existsSync(eventsDir)) {
    console.log('📁 No existing data directory found, starting fresh');
    return null;
  }
  
  // Get all date directories, sorted descending
  const dateDirs = fs.readdirSync(eventsDir)
    .filter(d => /^\d{4}-\d{2}-\d{2}$/.test(d))
    .sort()
    .reverse();
  
  if (dateDirs.length === 0) {
    console.log('📁 No existing data files found, starting fresh');
    return null;
  }
  
  // Check most recent directories for latest timestamp
  for (const dateDir of dateDirs.slice(0, 3)) {
    const dirPath = path.join(eventsDir, dateDir);
    const files = fs.readdirSync(dirPath)
      .filter(f => f.endsWith('.jsonl') || f.endsWith('.jsonl.gz'))
      .sort()
      .reverse();
    
    for (const file of files.slice(0, 3)) {
      const filePath = path.join(dirPath, file);
      const latest = await getLatestTimestampFromFile(filePath);
      if (latest) {
        console.log(`📍 Found latest timestamp: ${latest} from ${dateDir}/${file}`);
        return latest;
      }
    }
  }
  
  return null;
}

/**
 * Read the last few lines of a JSONL file to find latest timestamp
 */
async function getLatestTimestampFromFile(filePath) {
  return new Promise((resolve) => {
    const lines = [];
    const rl = readline.createInterface({
      input: fs.createReadStream(filePath),
      crlfDelay: Infinity
    });
    
    rl.on('line', (line) => {
      if (line.trim()) {
        lines.push(line);
        if (lines.length > 100) lines.shift(); // Keep last 100 lines
      }
    });
    
    rl.on('close', () => {
      // Find latest timestamp from last lines
      let latest = null;
      for (const line of lines.reverse()) {
        try {
          const obj = JSON.parse(line);
          const ts = obj.timestamp || obj.record_time;
          if (ts && (!latest || ts > latest)) {
            latest = ts;
          }
        } catch (e) {
          // Skip invalid JSON
        }
      }
      resolve(latest);
    });
    
    rl.on('error', () => resolve(null));
  });
}

/**
 * Detect latest migration from the scan API
 * Uses the updates endpoint with a minimal request to get migration_id
 */
async function detectLatestMigration() {
  try {
    // Use the updates endpoint to detect migration - it returns migration_id in response
    const response = await client.post('/v0/updates', {
      page_size: 1
    });
    
    // The response should contain migration_id
    if (response.data && response.data.migration_id !== undefined) {
      migrationId = response.data.migration_id;
      console.log(`📍 Detected migration_id: ${migrationId}`);
      setMigrationId(migrationId);
      return migrationId;
    }
    
    // Fallback: use default migration ID 0
    console.warn('⚠️ Could not detect migration_id from updates, using default: 0');
    migrationId = 0;
    setMigrationId(migrationId);
    return migrationId;
  } catch (err) {
    console.error('Failed to detect migration:', err.message);
    // Fallback to migration 0 instead of crashing
    console.warn('⚠️ Using fallback migration_id: 0');
    migrationId = 0;
    setMigrationId(migrationId);
    return migrationId;
  }
}

/**
 * Fetch updates from the scan API
 * Can use either offset-based or timestamp-based pagination
 */
async function fetchUpdates(afterOffset = null, afterTimestamp = null) {
  try {
    const payload = {
      migration_id: migrationId,
      page_size: BATCH_SIZE,
    };
    
    if (afterOffset) {
      payload.after = afterOffset;
    } else if (afterTimestamp) {
      // Use begin_exclusive for timestamp-based fetching (after backfill)
      payload.begin_exclusive = afterTimestamp;
    }
    
    const response = await client.post('/v2/updates', payload);
    return response.data;
  } catch (err) {
    if (err.response?.status === 404) {
      return { items: [] };
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
        const normalizedEvent = normalizeEvent(event, update.update_id, migrationId);
        events.push(normalizedEvent);
      }
    }
    
    // Track offset
    const offset = tx?.offset || item.reassignment?.offset;
    if (offset) lastOffset = offset;
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
  console.log('🚀 Starting Canton ledger ingestion (Parquet mode)\n');
  
  // Detect migration
  await detectLatestMigration();
  
  // Check for existing data from backfill
  lastTimestamp = await findLatestTimestamp();
  
  if (lastTimestamp) {
    console.log(`📍 Resuming from backfill timestamp: ${lastTimestamp}`);
  } else {
    console.log('📍 Starting fresh (no existing data found)');
  }
  
  let totalUpdates = 0;
  let totalEvents = 0;
  let emptyPolls = 0;
  let usingTimestamp = !!lastTimestamp; // Start with timestamp if we have backfill data
  
  while (isRunning) {
    try {
      // Use timestamp-based fetching until we get an offset, then switch to offset-based
      const data = usingTimestamp && !lastOffset
        ? await fetchUpdates(null, lastTimestamp)
        : await fetchUpdates(lastOffset, null);
      
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
      
      // Once we have an offset, we're caught up and can use offset-based pagination
      if (lastOffset && usingTimestamp) {
        console.log(`✅ Caught up with live updates, switching to offset-based pagination`);
        usingTimestamp = false;
      }
      
      const stats = getBufferStats();
      console.log(`📦 Processed ${updates} updates, ${events} events | Total: ${totalUpdates} updates, ${totalEvents} events | Buffer: ${stats.updates}/${stats.events}`);
      
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
