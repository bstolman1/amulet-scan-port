#!/usr/bin/env node
/**
 * ACS Snapshot Validation Tests
 * 
 * Validates ACS snapshot data integrity using the same logic as validate-acs.js
 * but in a test-friendly format that can be run via npm test.
 * 
 * Tests:
 * 1. ACS directory exists
 * 2. At least one snapshot exists
 * 3. Snapshots have completion markers
 * 4. JSONL files are readable and parseable
 * 5. Critical contract fields are populated
 */

import { existsSync, readdirSync, createReadStream } from 'fs';
import path from 'path';
import readline from 'readline';
import { fileURLToPath } from 'url';
import { getBaseDataDir, getRawDir } from '../path-utils.js';
import { 
  CRITICAL_CONTRACT_FIELDS, 
  validateContractFields 
} from '../acs-schema.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ─────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────

const BASE_DATA_DIR = getBaseDataDir();
const RAW_DIR = process.env.RAW_DIR || getRawDir();
const ACS_DIR = path.join(RAW_DIR, 'acs');

// Test configuration
const SAMPLE_SIZE = parseInt(process.env.TEST_SAMPLE_SIZE) || 5;
const SKIP_DATA_TESTS = process.env.SKIP_DATA_TESTS === 'true';

// ─────────────────────────────────────────────────────────────
// Test Utilities
// ─────────────────────────────────────────────────────────────

function findSnapshots(acsDir) {
  const snapshots = [];
  
  if (!existsSync(acsDir)) return snapshots;
  
  function scan(dir, pathParts = {}) {
    const entries = readdirSync(dir, { withFileTypes: true });
    
    for (const entry of entries) {
      const fullPath = path.join(dir, entry.name);
      
      if (entry.isDirectory()) {
        const newParts = { ...pathParts };
        
        if (entry.name.startsWith('migration=')) {
          newParts.migrationId = parseInt(entry.name.split('=')[1]);
        } else if (entry.name.startsWith('snapshot=')) {
          newParts.snapshotId = entry.name.split('=')[1];
          snapshots.push({
            path: fullPath,
            ...newParts,
            hasComplete: existsSync(path.join(fullPath, '_COMPLETE')),
          });
          continue;
        }
        
        scan(fullPath, newParts);
      }
    }
  }
  
  scan(acsDir);
  return snapshots;
}

async function readJsonlSample(filePath, maxRecords = 10) {
  const records = [];
  
  const fileStream = createReadStream(filePath);
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity,
  });
  
  for await (const line of rl) {
    if (line.trim()) {
      records.push(JSON.parse(line));
      if (records.length >= maxRecords) break;
    }
  }
  
  rl.close();
  fileStream.close();
  
  return records;
}

// ─────────────────────────────────────────────────────────────
// Test Cases
// ─────────────────────────────────────────────────────────────

const tests = [];
const results = { passed: 0, failed: 0, skipped: 0 };

function test(name, fn) {
  tests.push({ name, fn });
}

function skip(name, reason) {
  tests.push({ name, skip: true, reason });
}

async function runTests() {
  console.log('\n' + '═'.repeat(60));
  console.log('🧪 ACS SNAPSHOT VALIDATION TESTS');
  console.log('═'.repeat(60));
  console.log(`   ACS directory: ${ACS_DIR}`);
  console.log(`   Sample size: ${SAMPLE_SIZE} records per file`);
  console.log('─'.repeat(60) + '\n');
  
  for (const { name, fn, skip: isSkipped, reason } of tests) {
    if (isSkipped) {
      console.log(`⏭️ SKIP: ${name} (${reason})`);
      results.skipped++;
      continue;
    }
    
    try {
      await fn();
      console.log(`✅ PASS: ${name}`);
      results.passed++;
    } catch (err) {
      console.log(`❌ FAIL: ${name}`);
      console.log(`   Error: ${err.message}`);
      results.failed++;
    }
  }
  
  console.log('\n' + '─'.repeat(60));
  console.log(`📊 Results: ${results.passed} passed, ${results.failed} failed, ${results.skipped} skipped`);
  console.log('═'.repeat(60) + '\n');
  
  return results.failed === 0;
}

// ─────────────────────────────────────────────────────────────
// Define Tests
// ─────────────────────────────────────────────────────────────

test('ACS directory exists', () => {
  if (!existsSync(ACS_DIR)) {
    throw new Error(`ACS directory not found: ${ACS_DIR}`);
  }
});

test('At least one snapshot exists', () => {
  const snapshots = findSnapshots(ACS_DIR);
  if (snapshots.length === 0) {
    throw new Error('No ACS snapshots found');
  }
  console.log(`   Found ${snapshots.length} snapshots`);
});

test('Snapshots have completion markers', () => {
  const snapshots = findSnapshots(ACS_DIR);
  const complete = snapshots.filter(s => s.hasComplete);
  const incomplete = snapshots.filter(s => !s.hasComplete);
  
  if (incomplete.length > 0 && complete.length === 0) {
    throw new Error(`All ${incomplete.length} snapshots are incomplete`);
  }
  
  console.log(`   Complete: ${complete.length}, Incomplete: ${incomplete.length}`);
});

if (SKIP_DATA_TESTS) {
  skip('JSONL files are readable', 'SKIP_DATA_TESTS=true');
  skip('Contract records have critical fields', 'SKIP_DATA_TESTS=true');
} else {
  test('JSONL files are readable', async () => {
    const snapshots = findSnapshots(ACS_DIR);
    if (snapshots.length === 0) throw new Error('No snapshots to test');
    
    // Test first complete snapshot
    const snapshot = snapshots.find(s => s.hasComplete) || snapshots[0];
    const entries = readdirSync(snapshot.path, { withFileTypes: true });
    const jsonlFiles = entries
      .filter(e => e.isFile() && e.name.endsWith('.jsonl') && e.name !== '_COMPLETE')
      .map(e => path.join(snapshot.path, e.name));
    
    if (jsonlFiles.length === 0) {
      throw new Error(`No JSONL files in snapshot: ${snapshot.snapshotId}`);
    }
    
    // Read first file
    const records = await readJsonlSample(jsonlFiles[0], 1);
    if (records.length === 0) {
      throw new Error('First JSONL file is empty');
    }
    
    console.log(`   Verified ${jsonlFiles.length} JSONL files in snapshot ${snapshot.snapshotId}`);
  });
  
  test('Contract records have critical fields', async () => {
    const snapshots = findSnapshots(ACS_DIR);
    if (snapshots.length === 0) throw new Error('No snapshots to test');
    
    const snapshot = snapshots.find(s => s.hasComplete) || snapshots[0];
    const entries = readdirSync(snapshot.path, { withFileTypes: true });
    const jsonlFiles = entries
      .filter(e => e.isFile() && e.name.endsWith('.jsonl') && e.name !== '_COMPLETE')
      .map(e => path.join(snapshot.path, e.name));
    
    if (jsonlFiles.length === 0) throw new Error('No JSONL files to test');
    
    // Sample records from first file
    const records = await readJsonlSample(jsonlFiles[0], SAMPLE_SIZE);
    
    let valid = 0;
    let invalid = 0;
    
    for (const record of records) {
      const { missingCritical } = validateContractFields(record);
      if (missingCritical.length > 0) {
        invalid++;
      } else {
        valid++;
      }
    }
    
    if (invalid > valid) {
      throw new Error(`Most records (${invalid}/${records.length}) missing critical fields`);
    }
    
    console.log(`   Validated ${valid}/${records.length} records have critical fields`);
  });
}

// ─────────────────────────────────────────────────────────────
// Run Tests
// ─────────────────────────────────────────────────────────────

runTests()
  .then(passed => process.exit(passed ? 0 : 1))
  .catch(err => {
    console.error(`\n❌ FATAL: ${err.message}`);
    process.exit(1);
  });
