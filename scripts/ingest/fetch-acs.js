#!/usr/bin/env node
/**
 * ACS Snapshot Fetcher
 * 
 * Fetches current Active Contract Set from Canton Scan API
 * and writes directly to Parquet files (default) or JSONL with --keep-raw.
 * 
 * Usage:
 *   node fetch-acs.js            # Write to Parquet (default)
 *   node fetch-acs.js --keep-raw # Also write to .jsonl files
 *   node fetch-acs.js --local    # Force local disk mode (ignore GCS_BUCKET)
 */

import dotenv from 'dotenv';
dotenv.config();

import axios from 'axios';
import { Agent as HttpAgent } from 'http';
import { Agent as HttpsAgent } from 'https';
import BigNumber from 'bignumber.js';
import { normalizeACSContract, isTemplate, parseTemplateId, validateTemplates, validateContractFields, detectTemplateFormat } from './acs-schema.js';

// Parse command line arguments
const args = process.argv.slice(2);
const KEEP_RAW = args.includes('--keep-raw') || args.includes('--raw');
const RAW_ONLY = args.includes('--raw-only') || args.includes('--legacy');
const LOCAL_MODE = args.includes('--local') || args.includes('--local-disk');
const USE_PARQUET = !RAW_ONLY;
const USE_JSONL = KEEP_RAW || RAW_ONLY;

// If --local flag is set, force local disk mode by setting GCS_ENABLED=false
if (LOCAL_MODE) {
  process.env.GCS_ENABLED = 'false';
}

// Use Parquet writer by default, JSONL writer only if --keep-raw or --raw-only
import * as parquetWriter from './write-acs-parquet.js';
import * as jsonlWriter from './write-acs-jsonl.js';

// Unified writer functions
function setSnapshotTime(time, migrationId = null) {
  if (USE_PARQUET) {
    parquetWriter.setSnapshotTime(time, migrationId);
  }
  if (USE_JSONL) {
    jsonlWriter.setSnapshotTime(time, migrationId);
  }
}

async function bufferContracts(contracts) {
  if (USE_JSONL) {
    await jsonlWriter.bufferContracts(contracts);
  }
  if (USE_PARQUET) {
    return parquetWriter.bufferContracts(contracts);
  }
}

async function flushAll() {
  const results = [];
  if (USE_JSONL) {
    const jsonlResults = await jsonlWriter.flushAll();
    results.push(...jsonlResults);
  }
  if (USE_PARQUET) {
    const parquetResults = await parquetWriter.flushAll();
    results.push(...parquetResults);
  }
  return results;
}

function getBufferStats() {
  const stats = USE_PARQUET ? parquetWriter.getBufferStats() : { contracts: 0, maxRowsPerFile: 15000 };
  if (USE_JSONL) {
    stats.jsonlContracts = jsonlWriter.getBufferStats().contracts;
  }
  return stats;
}

function clearBuffers() {
  if (USE_PARQUET) {
    parquetWriter.clearBuffers();
  }
  if (USE_JSONL) {
    jsonlWriter.clearBuffers();
  }
}

async function writeCompletionMarker(time, migrationId, stats) {
  if (USE_JSONL) {
    await jsonlWriter.writeCompletionMarker(time, migrationId, stats);
  }
  if (USE_PARQUET) {
    return parquetWriter.writeCompletionMarker(time, migrationId, stats);
  }
}

function isSnapshotComplete(time, migrationId) {
  if (USE_PARQUET) {
    return parquetWriter.isSnapshotComplete(time, migrationId);
  }
  return jsonlWriter.isSnapshotComplete(time, migrationId);
}

function cleanupOldSnapshots(migrationId) {
  if (KEEP_RAW) {
    jsonlWriter.cleanupOldSnapshots(migrationId);
  }
  return parquetWriter.cleanupOldSnapshots(migrationId);
}

// TLS config (secure by default)
// Set INSECURE_TLS=1 only in controlled environments with self-signed certs.
const INSECURE_TLS = ['1', 'true', 'yes'].includes(String(process.env.INSECURE_TLS || '').toLowerCase());
if (INSECURE_TLS) {
  process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";
}

// Configuration
const SCAN_URL = process.env.SCAN_URL || 'https://scan.sv-1.global.canton.network.sync.global/api/scan';
const PAGE_SIZE = parseInt(process.env.PAGE_SIZE) || 500;
// Skip migrations that already have complete snapshots (set to false to force re-fetch all)
const SKIP_COMPLETE = process.env.SKIP_COMPLETE !== 'false';
// Only fetch the latest migration by default (set FETCH_ALL=true to fetch all)
const FETCH_ALL = process.env.FETCH_ALL === 'true';

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
  console.log(`   API base URL: ${SCAN_URL}`);
  const migrations = [];
  let id = 0;
  
  while (true) {
    const endpoint = '/v0/state/acs/snapshot-timestamp';
    const params = { before: new Date().toISOString(), migration_id: id };
    console.log(`   🌐 [migration ${id}] GET ${endpoint} with params:`, JSON.stringify(params));
    const t0 = Date.now();
    
    try {
      const res = await client.get(endpoint, { params });
      const latency = Date.now() - t0;
      console.log(`   ✅ [migration ${id}] Response in ${latency}ms, status=${res.status}`);
      
      if (res.data?.record_time) {
        migrations.push(id);
        console.log(`   Found migration ${id} with record_time: ${res.data.record_time}`);
        id++;
      } else {
        console.log(`   Migration ${id} returned no record_time, stopping detection`);
        break;
      }
    } catch (err) {
      const latency = Date.now() - t0;
      console.log(`   ❌ [migration ${id}] Failed after ${latency}ms: ${err.code || err.message}`);
      if (err.response) {
        console.log(`      HTTP ${err.response.status}: ${JSON.stringify(err.response.data).substring(0, 200)}`);
      }
      break;
    }
  }
  
  if (migrations.length === 0) {
    throw new Error('No valid migrations found. Check SCAN_URL and network connectivity.');
  }
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
  
  // Field validation tracking
  const fieldIssues = {
    criticalMissing: {},  // field -> count
    importantMissing: {}, // field -> count
    contractsWithCritical: 0,
    contractsWithImportant: 0,
  };
  
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
      
      // Validate contract fields
      const { missingCritical, missingImportant } = validateContractFields(contract);
      if (missingCritical.length > 0) {
        fieldIssues.contractsWithCritical++;
        for (const field of missingCritical) {
          fieldIssues.criticalMissing[field] = (fieldIssues.criticalMissing[field] || 0) + 1;
        }
      }
      if (missingImportant.length > 0) {
        fieldIssues.contractsWithImportant++;
        for (const field of missingImportant) {
          fieldIssues.importantMissing[field] = (fieldIssues.importantMissing[field] || 0) + 1;
        }
      }
      
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
  
  // Validate templates against expected registry
  const validation = validateTemplates(templateCounts);
  
  // Print validation report
  console.log(`\n   📋 Template Validation Report:`);
  console.log(`   ├─ Found: ${validation.found.length} expected templates`);
  console.log(`   ├─ Missing: ${validation.missing.length} templates (${validation.missing.filter(t => t.required).length} required)`);
  console.log(`   ├─ Unexpected: ${validation.unexpected.length} templates`);
  console.log(`   └─ Format variations: ${JSON.stringify(validation.formatVariations)}`);
  
  // Print field validation report
  console.log(`\n   🔍 Field Validation Report:`);
  const criticalFields = Object.entries(fieldIssues.criticalMissing);
  const importantFields = Object.entries(fieldIssues.importantMissing);
  
  if (criticalFields.length === 0 && importantFields.length === 0) {
    console.log(`   ✅ All fields populated correctly`);
  } else {
    if (criticalFields.length > 0) {
      console.log(`   ❌ CRITICAL MISSING (${fieldIssues.contractsWithCritical} contracts):`);
      for (const [field, count] of criticalFields) {
        console.log(`      - ${field}: ${count} contracts`);
      }
    }
    if (importantFields.length > 0) {
      console.log(`   ⚠️  Important missing (${fieldIssues.contractsWithImportant} contracts):`);
      for (const [field, count] of importantFields) {
        console.log(`      - ${field}: ${count} contracts`);
      }
    }
  }
  
  // Print warnings
  if (validation.warnings.length > 0) {
    console.log(`\n   ⚠️  VALIDATION WARNINGS:`);
    for (const warning of validation.warnings) {
      console.log(`      ${warning}`);
    }
  }
  
  // Log unexpected templates (may be new templates we should add to registry)
  if (validation.unexpected.length > 0) {
    console.log(`\n   🔍 Unexpected templates (consider adding to registry):`);
    for (const t of validation.unexpected.slice(0, 10)) {
      console.log(`      - ${t.key} (${t.count} contracts, formats: ${t.formats.join(', ')})`);
    }
    if (validation.unexpected.length > 10) {
      console.log(`      ... and ${validation.unexpected.length - 10} more`);
    }
  }
  
  // Write completion marker to indicate this snapshot is complete
  const stats = {
    totalContracts,
    amuletTotal: amuletTotal.toString(),
    lockedTotal: lockedTotal.toString(),
    circulatingTotal: amuletTotal.plus(lockedTotal).toString(),
    templateCount: Object.keys(templateCounts).length,
    validation: {
      foundCount: validation.found.length,
      missingCount: validation.missing.length,
      unexpectedCount: validation.unexpected.length,
      warnings: validation.warnings,
    },
    fieldValidation: {
      criticalMissing: fieldIssues.criticalMissing,
      importantMissing: fieldIssues.importantMissing,
      contractsWithCriticalIssues: fieldIssues.contractsWithCritical,
      contractsWithImportantIssues: fieldIssues.contractsWithImportant,
    },
  };
  await writeCompletionMarker(snapshotRunTime, migrationId, stats);
  
  // Clean up old snapshots AFTER the new one is complete
  // This ensures there's always valid data available during the snapshot process
  console.log(`\n   🗑️ Cleaning up old snapshots for migration ${migrationId}...`);
  const cleanupResult = cleanupOldSnapshots(migrationId);
  if (cleanupResult.deleted > 0) {
    console.log(`   ✅ Deleted ${cleanupResult.deleted} old snapshot(s), keeping ${cleanupResult.kept}`);
  }
  
  return {
    migrationId,
    recordTime,
    totalContracts,
    amuletTotal: amuletTotal.toString(),
    lockedTotal: lockedTotal.toString(),
    circulatingTotal: amuletTotal.plus(lockedTotal).toString(),
    templateCounts,
    validation,
    fieldIssues,
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
  console.log(`   - Skip Complete: ${SKIP_COMPLETE}`);
  console.log(`   - Fetch All Migrations: ${FETCH_ALL}`);
  console.log(`   - DATA_DIR env: ${process.env.DATA_DIR || '(not set)'}`);
  console.log(`   - GCS_BUCKET env: ${process.env.GCS_BUCKET || '(not set)'}`);
  console.log('');
  
  console.log('🔄 Calling detectMigrations()...');
  let migrations;
  try {
    migrations = await detectMigrations();
    console.log(`✅ detectMigrations() returned: [${migrations.join(', ')}]`);
  } catch (err) {
    console.error(`❌ detectMigrations() failed: ${err.message}`);
    console.error(err.stack);
    throw err;
  }
  
  const results = [];
  
  // Determine which migrations to process
  let migrationsToProcess;
  if (FETCH_ALL) {
    migrationsToProcess = migrations;
    console.log(`📋 Processing all migrations: [${migrationsToProcess.join(', ')}]`);
  } else {
    // Only process the latest migration
    migrationsToProcess = [Math.max(...migrations)];
    console.log(`📋 Processing only latest migration: [${migrationsToProcess[0]}]`);
  }
  
  for (const migrationId of migrationsToProcess) {
    // Check if we should skip this migration
    if (SKIP_COMPLETE) {
      // We need to check with a dummy timestamp - the isSnapshotComplete checks filesystem
      // For now, we'll run regardless since each run creates a new snapshot_time
      // The optimization here is just skipping older migrations entirely
    }
    
    clearBuffers();
    try {
      const result = await runMigrationSnapshot(migrationId);
      results.push(result);
    } catch (err) {
      console.error(`❌ runMigrationSnapshot(${migrationId}) failed: ${err.message}`);
      console.error(err.stack);
      throw err;
    }
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
    
    if (r.validation) {
      console.log(`   - Templates Found: ${r.validation.found.length}`);
      console.log(`   - Templates Missing: ${r.validation.missing.length}`);
      if (r.validation.warnings.length > 0) {
        console.log(`   - Warnings: ${r.validation.warnings.length}`);
      }
    }
    
    if (r.fieldIssues) {
      const criticalCount = r.fieldIssues.contractsWithCritical;
      const importantCount = r.fieldIssues.contractsWithImportant;
      if (criticalCount > 0) {
        console.log(`   - ❌ Critical field issues: ${criticalCount} contracts`);
      }
      if (importantCount > 0) {
        console.log(`   - ⚠️  Important field issues: ${importantCount} contracts`);
      }
    }
  }
  
  // Print overall validation summary
  const allWarnings = results.flatMap(r => r.validation?.warnings || []);
  if (allWarnings.length > 0) {
    console.log('\n⚠️  VALIDATION WARNINGS SUMMARY:');
    for (const w of [...new Set(allWarnings)]) {
      console.log(`   ${w}`);
    }
  }
  
  // Check for critical field issues
  const hasCriticalIssues = results.some(r => r.fieldIssues?.contractsWithCritical > 0);
  
  console.log('\n' + '='.repeat(80));
  if (hasCriticalIssues) {
    console.log('⚠️  ACS Snapshot Complete (with critical field issues)');
  } else {
    console.log('✅ ACS Snapshot Complete');
  }
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
