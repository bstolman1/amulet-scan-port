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
import { bufferUpdates, bufferEvents, flushAll, getBufferStats } from './write-parquet.js';

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

// Track state
let lastOffset = null;
let migrationId = null;
let isRunning = true;

/**
 * Detect latest migration from the scan API
 */
async function detectLatestMigration() {
  try {
    const response = await client.get('/v0/state/acs/snapshot-timestamp');
    migrationId = response.data.migration_id;
    console.log(`📍 Detected migration_id: ${migrationId}`);
    return migrationId;
  } catch (err) {
    console.error('Failed to detect migration:', err.message);
    throw err;
  }
}

/**
 * Fetch updates from the scan API
 */
async function fetchUpdates(afterOffset = null) {
  try {
    const payload = {
      migration_id: migrationId,
      page_size: BATCH_SIZE,
    };
    
    if (afterOffset) {
      payload.after = afterOffset;
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
  
  let totalUpdates = 0;
  let totalEvents = 0;
  let emptyPolls = 0;
  
  while (isRunning) {
    try {
      const data = await fetchUpdates(lastOffset);
      
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
