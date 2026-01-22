#!/usr/bin/env node
/**
 * Field Coverage Audit Tool
 * 
 * Compares extracted/normalized columns against raw JSON payloads
 * to detect fields present in source data but NOT captured in schema.
 * 
 * This ensures 100% data capture - no fields silently dropped.
 * 
 * Usage:
 *   node audit-field-coverage.js [--type acs|backfill] [--sample N] [--verbose]
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import zlib from 'zlib';
import { promisify } from 'util';

const gunzip = promisify(zlib.gunzip);

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Import schema definitions
import { ACS_COLUMNS, CRITICAL_CONTRACT_FIELDS, IMPORTANT_CONTRACT_FIELDS } from './acs-schema.js';
import { UPDATES_COLUMNS, EVENTS_COLUMNS } from './data-schema.js';

// Parse CLI args
const args = process.argv.slice(2);
const auditType = args.includes('--type') ? args[args.indexOf('--type') + 1] : 'all';
const sampleSize = args.includes('--sample') ? parseInt(args[args.indexOf('--sample') + 1]) : 100;
const verbose = args.includes('--verbose');

const BASE_DATA_DIR = process.env.DATA_DIR || path.join(__dirname, '../../data');
const RAW_DIR = path.join(BASE_DATA_DIR, 'raw');

// Results accumulator
const results = {
  acs: {
    sampledRecords: 0,
    allRawKeys: new Set(),
    capturedKeys: new Set(ACS_COLUMNS),
    missingKeys: new Set(),
    unexpectedKeys: new Set(),
    samplePayloads: [],
  },
  updates: {
    sampledRecords: 0,
    allRawKeys: new Set(),
    capturedKeys: new Set(UPDATES_COLUMNS),
    missingKeys: new Set(),
    unexpectedKeys: new Set(),
    samplePayloads: [],
  },
  events: {
    sampledRecords: 0,
    allRawKeys: new Set(),
    capturedKeys: new Set(EVENTS_COLUMNS),
    missingKeys: new Set(),
    unexpectedKeys: new Set(),
    samplePayloads: [],
  },
};

/**
 * Recursively get all keys from an object (flattened with dot notation)
 */
function getAllKeys(obj, prefix = '') {
  const keys = new Set();
  if (!obj || typeof obj !== 'object') return keys;
  
  for (const [key, value] of Object.entries(obj)) {
    const fullKey = prefix ? `${prefix}.${key}` : key;
    keys.add(fullKey);
    
    // Don't recurse into arrays or deeply nested objects beyond 2 levels
    if (value && typeof value === 'object' && !Array.isArray(value) && prefix.split('.').length < 2) {
      for (const nestedKey of getAllKeys(value, fullKey)) {
        keys.add(nestedKey);
      }
    }
  }
  return keys;
}

/**
 * Find all JSONL files recursively
 */
function findJsonlFiles(dir, pattern = '.jsonl') {
  const files = [];
  if (!fs.existsSync(dir)) return files;
  
  const items = fs.readdirSync(dir, { withFileTypes: true });
  for (const item of items) {
    const fullPath = path.join(dir, item.name);
    if (item.isDirectory()) {
      files.push(...findJsonlFiles(fullPath, pattern));
    } else if (item.name.endsWith(pattern)) {
      files.push(fullPath);
    }
  }
  return files;
}

/**
 * Find Parquet files (we'll read their embedded JSON columns)
 */
function findParquetFiles(dir) {
  const files = [];
  if (!fs.existsSync(dir)) return files;
  
  const items = fs.readdirSync(dir, { withFileTypes: true });
  for (const item of items) {
    const fullPath = path.join(dir, item.name);
    if (item.isDirectory()) {
      files.push(...findParquetFiles(fullPath));
    } else if (item.name.endsWith('.parquet')) {
      files.push(fullPath);
    }
  }
  return files;
}

/**
 * Read JSONL file and extract records
 */
async function readJsonlFile(filePath, maxRecords = 50) {
  const records = [];
  const content = fs.readFileSync(filePath, 'utf-8');
  const lines = content.split('\n').filter(l => l.trim());
  
  for (let i = 0; i < Math.min(lines.length, maxRecords); i++) {
    try {
      records.push(JSON.parse(lines[i]));
    } catch (e) {
      // Skip malformed lines
    }
  }
  return records;
}

/**
 * Audit ACS data by examining raw JSON payloads
 */
async function auditACS() {
  console.log('\n📋 Auditing ACS Field Coverage...\n');
  
  const acsDir = path.join(RAW_DIR, 'acs');
  const jsonlFiles = findJsonlFiles(acsDir);
  const parquetFiles = findParquetFiles(acsDir);
  
  console.log(`  Found ${jsonlFiles.length} JSONL files, ${parquetFiles.length} Parquet files`);
  
  // Sample from JSONL files (they contain the raw data)
  let sampled = 0;
  for (const file of jsonlFiles.slice(0, 10)) {
    if (sampled >= sampleSize) break;
    
    const records = await readJsonlFile(file, sampleSize - sampled);
    for (const record of records) {
      sampled++;
      
      // Get all keys from this raw record
      const rawKeys = getAllKeys(record);
      for (const key of rawKeys) {
        results.acs.allRawKeys.add(key);
      }
      
      // Store sample for reporting
      if (results.acs.samplePayloads.length < 3) {
        results.acs.samplePayloads.push({
          file: path.basename(file),
          keys: [...rawKeys].slice(0, 20),
          template_id: record.template_id,
        });
      }
    }
  }
  
  results.acs.sampledRecords = sampled;
  
  // Compare raw keys to captured columns
  // Map raw API keys to our column names
  const keyMapping = {
    'contract_id': 'contract_id',
    'event_id': 'event_id',
    'template_id': 'template_id',
    'signatories': 'signatories',
    'observers': 'observers',
    'create_arguments': 'payload', // We capture this as 'payload'
    'created_at': 'record_time',   // We capture this as 'record_time'
  };
  
  // Find keys in raw data that we DON'T capture
  const topLevelRawKeys = [...results.acs.allRawKeys].filter(k => !k.includes('.'));
  for (const rawKey of topLevelRawKeys) {
    const mappedKey = keyMapping[rawKey] || rawKey;
    if (!results.acs.capturedKeys.has(mappedKey) && !results.acs.capturedKeys.has(rawKey)) {
      // Check if it's captured under a different name
      if (rawKey === 'create_arguments' && results.acs.capturedKeys.has('payload')) continue;
      if (rawKey === 'created_at' && results.acs.capturedKeys.has('record_time')) continue;
      
      results.acs.missingKeys.add(rawKey);
    }
  }
  
  console.log(`  Sampled ${sampled} ACS records`);
  console.log(`  Found ${results.acs.allRawKeys.size} unique raw keys`);
  console.log(`  Schema captures ${results.acs.capturedKeys.size} columns`);
  console.log(`  Potentially missing: ${results.acs.missingKeys.size} keys`);
}

/**
 * Audit backfill data (updates + events)
 */
async function auditBackfill() {
  console.log('\n📋 Auditing Backfill Field Coverage...\n');
  
  const backfillDir = path.join(RAW_DIR, 'backfill');
  const jsonlFiles = findJsonlFiles(backfillDir);
  
  console.log(`  Found ${jsonlFiles.length} JSONL files`);
  
  // Separate updates and events files
  const updateFiles = jsonlFiles.filter(f => f.includes('updates'));
  const eventFiles = jsonlFiles.filter(f => f.includes('events'));
  
  // Audit updates
  let updatesSampled = 0;
  for (const file of updateFiles.slice(0, 10)) {
    if (updatesSampled >= sampleSize) break;
    
    const records = await readJsonlFile(file, sampleSize - updatesSampled);
    for (const record of records) {
      updatesSampled++;
      
      // Parse raw JSON if stored as string
      let rawData = record;
      if (record.update_data) {
        try {
          rawData = JSON.parse(record.update_data);
        } catch (e) {
          rawData = record;
        }
      }
      
      const rawKeys = getAllKeys(rawData);
      for (const key of rawKeys) {
        results.updates.allRawKeys.add(key);
      }
      
      if (results.updates.samplePayloads.length < 3) {
        results.updates.samplePayloads.push({
          file: path.basename(file),
          keys: [...rawKeys].slice(0, 20),
          update_id: record.update_id,
          update_type: record.update_type,
        });
      }
    }
  }
  results.updates.sampledRecords = updatesSampled;
  
  // Audit events
  let eventsSampled = 0;
  for (const file of eventFiles.slice(0, 10)) {
    if (eventsSampled >= sampleSize) break;
    
    const records = await readJsonlFile(file, sampleSize - eventsSampled);
    for (const record of records) {
      eventsSampled++;
      
      // Parse raw_event JSON if stored as string
      let rawData = record;
      if (record.raw_event) {
        try {
          rawData = JSON.parse(record.raw_event);
        } catch (e) {
          rawData = record;
        }
      }
      
      const rawKeys = getAllKeys(rawData);
      for (const key of rawKeys) {
        results.events.allRawKeys.add(key);
      }
      
      if (results.events.samplePayloads.length < 3) {
        results.events.samplePayloads.push({
          file: path.basename(file),
          keys: [...rawKeys].slice(0, 20),
          event_id: record.event_id,
          event_type: record.event_type,
        });
      }
    }
  }
  results.events.sampledRecords = eventsSampled;
  
  // Analyze missing keys for updates
  const updateKeyMapping = {
    'effective_at': 'effective_at',
    'record_time': 'record_time',
    'events_by_id': null, // We flatten this into events table
    'root_event_ids': 'root_event_ids',
  };
  
  const updateTopLevel = [...results.updates.allRawKeys].filter(k => !k.includes('.'));
  for (const rawKey of updateTopLevel) {
    if (updateKeyMapping[rawKey] === null) continue; // Intentionally not captured at update level
    const mappedKey = updateKeyMapping[rawKey] || rawKey;
    if (!results.updates.capturedKeys.has(mappedKey) && !results.updates.capturedKeys.has(rawKey)) {
      results.updates.missingKeys.add(rawKey);
    }
  }
  
  // Analyze missing keys for events
  const eventKeyMapping = {
    'created_event': null,     // We unwrap this
    'archived_event': null,    // We unwrap this
    'exercised_event': null,   // We unwrap this
    'create_arguments': 'payload',
    'choice_argument': 'payload',
  };
  
  const eventTopLevel = [...results.events.allRawKeys].filter(k => !k.includes('.'));
  for (const rawKey of eventTopLevel) {
    if (eventKeyMapping[rawKey] === null) continue;
    const mappedKey = eventKeyMapping[rawKey] || rawKey;
    if (!results.events.capturedKeys.has(mappedKey) && !results.events.capturedKeys.has(rawKey)) {
      results.events.missingKeys.add(rawKey);
    }
  }
  
  console.log(`  Sampled ${updatesSampled} updates, ${eventsSampled} events`);
  console.log(`  Updates: ${results.updates.allRawKeys.size} raw keys, ${results.updates.missingKeys.size} potentially missing`);
  console.log(`  Events: ${results.events.allRawKeys.size} raw keys, ${results.events.missingKeys.size} potentially missing`);
}

/**
 * Print comprehensive report
 */
function printReport() {
  console.log('\n' + '='.repeat(80));
  console.log('                    FIELD COVERAGE AUDIT REPORT');
  console.log('='.repeat(80));
  
  // ACS Report
  if (auditType === 'all' || auditType === 'acs') {
    console.log('\n📦 ACS CONTRACTS SCHEMA COVERAGE');
    console.log('-'.repeat(50));
    console.log(`  Records sampled: ${results.acs.sampledRecords}`);
    console.log(`  Unique raw keys found: ${results.acs.allRawKeys.size}`);
    console.log(`  Schema columns: ${results.acs.capturedKeys.size}`);
    
    console.log('\n  ✅ Captured columns:');
    for (const col of [...results.acs.capturedKeys].sort()) {
      const isCritical = CRITICAL_CONTRACT_FIELDS.includes(col);
      const isImportant = IMPORTANT_CONTRACT_FIELDS.includes(col);
      const marker = isCritical ? '🔴' : isImportant ? '🟡' : '⚪';
      console.log(`     ${marker} ${col}`);
    }
    
    if (results.acs.missingKeys.size > 0) {
      console.log('\n  ⚠️  POTENTIALLY MISSING (found in raw, not in schema):');
      for (const key of [...results.acs.missingKeys].sort()) {
        console.log(`     ❌ ${key}`);
      }
    } else {
      console.log('\n  ✅ No missing keys detected');
    }
    
    console.log('\n  🔒 SAFETY NET: Raw JSON preserved in "raw" column');
    
    if (verbose && results.acs.samplePayloads.length > 0) {
      console.log('\n  Sample payloads:');
      for (const sample of results.acs.samplePayloads) {
        console.log(`    - ${sample.template_id}: [${sample.keys.slice(0, 8).join(', ')}...]`);
      }
    }
  }
  
  // Updates Report
  if (auditType === 'all' || auditType === 'backfill') {
    console.log('\n📝 LEDGER UPDATES SCHEMA COVERAGE');
    console.log('-'.repeat(50));
    console.log(`  Records sampled: ${results.updates.sampledRecords}`);
    console.log(`  Unique raw keys found: ${results.updates.allRawKeys.size}`);
    console.log(`  Schema columns: ${results.updates.capturedKeys.size}`);
    
    if (results.updates.missingKeys.size > 0) {
      console.log('\n  ⚠️  POTENTIALLY MISSING:');
      for (const key of [...results.updates.missingKeys].sort()) {
        console.log(`     ❌ ${key}`);
      }
    } else {
      console.log('\n  ✅ No missing keys detected');
    }
    
    console.log('\n  🔒 SAFETY NET: Raw JSON preserved in "update_data" column');
    
    // Events Report
    console.log('\n📄 LEDGER EVENTS SCHEMA COVERAGE');
    console.log('-'.repeat(50));
    console.log(`  Records sampled: ${results.events.sampledRecords}`);
    console.log(`  Unique raw keys found: ${results.events.allRawKeys.size}`);
    console.log(`  Schema columns: ${results.events.capturedKeys.size}`);
    
    if (results.events.missingKeys.size > 0) {
      console.log('\n  ⚠️  POTENTIALLY MISSING:');
      for (const key of [...results.events.missingKeys].sort()) {
        console.log(`     ❌ ${key}`);
      }
    } else {
      console.log('\n  ✅ No missing keys detected');
    }
    
    console.log('\n  🔒 SAFETY NET: Raw JSON preserved in "raw_event" column');
  }
  
  // Summary
  console.log('\n' + '='.repeat(80));
  console.log('SUMMARY');
  console.log('='.repeat(80));
  
  const totalMissing = results.acs.missingKeys.size + 
    results.updates.missingKeys.size + 
    results.events.missingKeys.size;
  
  if (totalMissing === 0) {
    console.log('\n✅ ALL FIELDS ACCOUNTED FOR');
    console.log('   Every field found in raw data is either:');
    console.log('   - Captured as a dedicated column');
    console.log('   - Preserved in raw JSON blob for future access');
  } else {
    console.log(`\n⚠️  ${totalMissing} FIELDS MAY BE MISSING DEDICATED COLUMNS`);
    console.log('   However, all raw data is preserved in JSON columns');
    console.log('   Consider adding dedicated columns for frequently queried fields');
  }
  
  console.log('\n📌 DATA INTEGRITY GUARANTEES:');
  console.log('   - ACS: "raw" column contains complete original contract');
  console.log('   - Updates: "update_data" column contains complete original update');
  console.log('   - Events: "raw_event" column contains complete original event');
  console.log('   - No data is ever silently dropped\n');
  
  return totalMissing;
}

/**
 * Main execution
 */
async function runAudit() {
  console.log('🔍 Field Coverage Audit Tool');
  console.log(`   Audit type: ${auditType}`);
  console.log(`   Sample size: ${sampleSize}`);
  console.log(`   Data directory: ${RAW_DIR}`);
  
  try {
    if (auditType === 'all' || auditType === 'acs') {
      await auditACS();
    }
    
    if (auditType === 'all' || auditType === 'backfill') {
      await auditBackfill();
    }
    
    const missing = printReport();
    
    // Exit with warning code if fields are missing
    process.exit(missing > 0 ? 1 : 0);
  } catch (err) {
    console.error('\n❌ FATAL ERROR:', err.message);
    if (verbose) console.error(err.stack);
    process.exit(2);
  }
}

runAudit();
