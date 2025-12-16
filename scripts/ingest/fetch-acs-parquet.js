#!/usr/bin/env node
/**
 * ACS Snapshot Fetcher - Parquet Version
 * 
 * Fetches current Active Contract Set from Canton Scan API
 * and writes to local partitioned files for DuckDB.
 */

import dotenv from 'dotenv';
dotenv.config();

import axios from 'axios';
import { Agent as HttpAgent } from 'http';
import { Agent as HttpsAgent } from 'https';
import BigNumber from 'bignumber.js';
import { normalizeACSContract, isTemplate, parseTemplateId } from './acs-schema.js';
import { setSnapshotTime, bufferContracts, flushAll, getBufferStats, clearBuffers, writeCompletionMarker } from './write-acs-parquet.js';

// TLS config (secure by default)
// Set INSECURE_TLS=1 only in controlled environments with self-signed certs.
const INSECURE_TLS = ['1', 'true', 'yes'].includes(String(process.env.INSECURE_TLS || '').toLowerCase());
if (INSECURE_TLS) {
  process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";
}

// Configuration
const SCAN_URL = process.env.SCAN_URL || 'https://scan.sv-1.global.canton.network.sync.global/api/scan';
const PAGE_SIZE = parseInt(process.env.PAGE_SIZE) || 500;

// Axios client with keepalive
const client = axios.create({
  baseURL: SCAN_URL,
  httpAgent: new HttpAgent({ keepAlive: true, keepAliveMsecs: 30000 }),
  httpsAgent: new HttpsAgent({
    keepAlive: true,
    keepAliveMsecs: 30000,
    rejectUnauthorized: !INSECURE_TLS,
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
async function retryWithBackoff(fn, maxRetries = 5) {
  let lastError;
  
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;
      
      const retryable = ['ECONNRESET', 'ETIMEDOUT', 'ECONNREFUSED'].includes(error.code) ||
        [429, 500, 502, 503, 504].includes(error.response?.status);
      
      if (attempt === maxRetries || !retryable) throw error;
      
      const delay = Math.min(1000 * Math.pow(2, attempt), 30000);
      console.log(`   ⏳ Retry ${attempt + 1}/${maxRetries} in ${delay}ms...`);
      await sleep(delay);
    }
  }
  
  throw lastError;
}

/**
 * Detect all valid migrations
 */
async function detectMigrations() {
  console.log('🔎 Detecting migrations...');
  const migrations = [];
  let id = 1;
  
  while (true) {
    try {
      const res = await client.get('/v0/state/acs/snapshot-timestamp', {
        params: { before: new Date().toISOString(), migration_id: id },
      });
      
      if (res.data?.record_time) {
        migrations.push(id);
        id++;
      } else {
        break;
      }
    } catch {
      break;
    }
  }
  
  if (migrations.length === 0) throw new Error('No valid migrations found');
  console.log(`📘 Found migrations: [${migrations.join(', ')}]`);
  return migrations;
}

/**
 * Get snapshot timestamp for a migration
 */
async function getSnapshotTimestamp(migrationId) {
  const res = await client.get('/v0/state/acs/snapshot-timestamp', {
    params: { before: new Date().toISOString(), migration_id: migrationId },
  });
  return res.data.record_time;
}

/**
 * Fetch ACS page
 */
async function fetchACSPage(migrationId, recordTime, after = null) {
  return await retryWithBackoff(async () => {
    const payload = {
      migration_id: migrationId,
      record_time: recordTime,
      page_size: PAGE_SIZE,
      daml_value_encoding: 'compact_json',
    };
    
    if (after) payload.after = after;
    
    const res = await client.post('/v0/state/acs', payload);
    return res.data;
  });
}

/**
 * Run ACS snapshot for a single migration
 */
async function runMigrationSnapshot(migrationId) {
  console.log(`\n📍 Starting ACS snapshot for migration ${migrationId}`);
  
  const recordTime = await getSnapshotTimestamp(migrationId);
  console.log(`   Record time: ${recordTime}`);
  
  // Use current time for partitioning to ensure unique snapshot folders per run
  // recordTime is the ledger state time which may be the same across multiple runs
  const snapshotRunTime = new Date();
  console.log(`   Snapshot run time: ${snapshotRunTime.toISOString()}`);
  
  setSnapshotTime(snapshotRunTime, migrationId);
  
  // Stats
  let totalContracts = 0;
  let page = 0;
  let after = null;
  const seen = new Set();
  
  // Totals by template type
  let amuletTotal = new BigNumber(0);
  let lockedTotal = new BigNumber(0);
  const templateCounts = {};
  
  while (true) {
    page++;
    console.log(`   📄 Fetching page ${page}...`);
    
    const data = await fetchACSPage(migrationId, recordTime, after);
    const events = data.created_events || [];
    
    if (!events.length) {
      console.log(`   ✅ No more events - finished migration ${migrationId}`);
      break;
    }
    
    const contracts = [];
    
    for (const event of events) {
      const id = event.contract_id || event.event_id;
      if (seen.has(id)) continue;
      seen.add(id);
      
      // Normalize for storage
      const contract = normalizeACSContract(event, migrationId, recordTime, recordTime);
      contracts.push(contract);
      
      // Count by template
      const templateId = event.template_id || 'unknown';
      templateCounts[templateId] = (templateCounts[templateId] || 0) + 1;
      
      // Calculate totals for Amulet/LockedAmulet
      if (isTemplate(event, 'Splice.Amulet', 'Amulet')) {
        const amt = new BigNumber(event.create_arguments?.amount?.initialAmount ?? '0');
        amuletTotal = amuletTotal.plus(amt);
      } else if (isTemplate(event, 'Splice.Amulet', 'LockedAmulet')) {
        const amt = new BigNumber(event.create_arguments?.amulet?.amount?.initialAmount ?? '0');
        lockedTotal = lockedTotal.plus(amt);
      }
    }
    
    await bufferContracts(contracts);
    totalContracts += contracts.length;
    
    const stats = getBufferStats();
    console.log(`   📦 Processed ${contracts.length} contracts (total: ${totalContracts}, buffer: ${stats.contracts})`);
    
    // Get cursor for next page - use next_page_token from API response
    after = data.next_page_token;
    
    if (after === undefined || after === null) break;
  }
  
  // Flush remaining
  await flushAll();
  
  // Write completion marker to indicate this snapshot is complete
  const stats = {
    totalContracts,
    amuletTotal: amuletTotal.toString(),
    lockedTotal: lockedTotal.toString(),
    circulatingTotal: amuletTotal.plus(lockedTotal).toString(),
    templateCount: Object.keys(templateCounts).length,
  };
  await writeCompletionMarker(snapshotRunTime, migrationId, stats);
  
  return {
    migrationId,
    recordTime,
    totalContracts,
    amuletTotal: amuletTotal.toString(),
    lockedTotal: lockedTotal.toString(),
    circulatingTotal: amuletTotal.plus(lockedTotal).toString(),
    templateCounts,
  };
}

/**
 * Main function
 */
async function runACSSnapshot() {
  console.log('🚀 Starting ACS Snapshot (Parquet mode)\n');
  console.log(`⚙️  Configuration:`);
  console.log(`   - Scan URL: ${SCAN_URL}`);
  console.log(`   - Page Size: ${PAGE_SIZE}`);
  console.log('');
  
  const migrations = await detectMigrations();
  const results = [];
  
  for (const migrationId of migrations) {
    clearBuffers();
    const result = await runMigrationSnapshot(migrationId);
    results.push(result);
  }
  
  // Print summary
  console.log('\n' + '='.repeat(80));
  console.log('📊 ACS SNAPSHOT SUMMARY');
  console.log('='.repeat(80));
  
  for (const r of results) {
    console.log(`\nMigration ${r.migrationId}:`);
    console.log(`   - Record Time: ${r.recordTime}`);
    console.log(`   - Total Contracts: ${r.totalContracts.toLocaleString()}`);
    console.log(`   - Amulet Total: ${r.amuletTotal}`);
    console.log(`   - Locked Total: ${r.lockedTotal}`);
    console.log(`   - Circulating: ${r.circulatingTotal}`);
  }
  
  console.log('\n' + '='.repeat(80));
  console.log('✅ ACS Snapshot Complete');
  console.log('='.repeat(80));
}

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('\n🛑 Shutting down...');
  await flushAll();
  process.exit(0);
});

// Run
runACSSnapshot().catch(err => {
  console.error('❌ Fatal error:', err);
  process.exit(1);
});
