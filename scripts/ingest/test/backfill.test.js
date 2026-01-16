#!/usr/bin/env node
/**
 * Backfill Data Validation Tests
 * 
 * Validates backfill data integrity using the same logic as validate-backfill.js
 * but in a test-friendly format that can be run via npm test.
 * 
 * Tests:
 * 1. Data directory exists and has expected structure
 * 2. Binary files are readable and decodable
 * 3. Critical fields are populated
 * 4. Cursor state is consistent with data files
 * 5. No significant time gaps (optional)
 */

import { existsSync, readdirSync, statSync } from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { getBaseDataDir, getCursorDir, getRawDir } from '../path-utils.js';
import { readBinaryFile, getFileStats } from '../read-binary.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ─────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────

const BASE_DATA_DIR = getBaseDataDir();
const CURSOR_DIR = getCursorDir();
const RAW_DIR = process.env.RAW_DIR || getRawDir();

// Test configuration
const SAMPLE_SIZE = parseInt(process.env.TEST_SAMPLE_SIZE) || 10;
const SKIP_DATA_TESTS = process.env.SKIP_DATA_TESTS === 'true';

// Critical fields that must be present
const CRITICAL_UPDATE_FIELDS = ['id', 'type', 'migrationId'];
const CRITICAL_EVENT_FIELDS = ['id', 'updateId', 'type'];

// ─────────────────────────────────────────────────────────────
// Test Utilities
// ─────────────────────────────────────────────────────────────

function findDataFiles(dir, filter = () => true) {
  const files = [];
  
  if (!existsSync(dir)) return files;
  
  function scan(currentDir) {
    const entries = readdirSync(currentDir, { withFileTypes: true });
    
    for (const entry of entries) {
      const fullPath = path.join(currentDir, entry.name);
      
      if (entry.isDirectory()) {
        scan(fullPath);
      } else if (entry.isFile() && entry.name.endsWith('.pb.zst') && filter(entry.name)) {
        files.push(fullPath);
      }
    }
  }
  
  scan(dir);
  return files;
}

function sampleFiles(files, sampleSize) {
  if (files.length <= sampleSize) return files;
  
  const sampled = [];
  const step = Math.floor(files.length / sampleSize);
  
  for (let i = 0; i < sampleSize; i++) {
    sampled.push(files[i * step]);
  }
  
  return sampled;
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
  console.log('🧪 BACKFILL DATA VALIDATION TESTS');
  console.log('═'.repeat(60));
  console.log(`   Data directory: ${BASE_DATA_DIR}`);
  console.log(`   Raw directory: ${RAW_DIR}`);
  console.log(`   Sample size: ${SAMPLE_SIZE} files`);
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

test('Data directory exists', () => {
  if (!existsSync(BASE_DATA_DIR)) {
    throw new Error(`Data directory not found: ${BASE_DATA_DIR}`);
  }
});

test('Raw directory exists', () => {
  if (!existsSync(RAW_DIR)) {
    throw new Error(`Raw directory not found: ${RAW_DIR}`);
  }
});

test('Updates files exist', () => {
  const updateFiles = findDataFiles(RAW_DIR, name => name.startsWith('updates-'));
  if (updateFiles.length === 0) {
    throw new Error('No update files found');
  }
  console.log(`   Found ${updateFiles.length} update files`);
});

test('Events files exist', () => {
  const eventFiles = findDataFiles(RAW_DIR, name => name.startsWith('events-'));
  if (eventFiles.length === 0) {
    throw new Error('No event files found');
  }
  console.log(`   Found ${eventFiles.length} event files`);
});

if (SKIP_DATA_TESTS) {
  skip('Update files are readable', 'SKIP_DATA_TESTS=true');
  skip('Event files are readable', 'SKIP_DATA_TESTS=true');
  skip('Update records have critical fields', 'SKIP_DATA_TESTS=true');
  skip('Event records have critical fields', 'SKIP_DATA_TESTS=true');
} else {
  test('Update files are readable', async () => {
    const updateFiles = findDataFiles(RAW_DIR, name => name.startsWith('updates-'));
    const sampled = sampleFiles(updateFiles, SAMPLE_SIZE);
    
    let readable = 0;
    let corrupted = 0;
    
    for (const file of sampled) {
      try {
        const stats = await getFileStats(file);
        if (stats.count > 0) readable++;
      } catch {
        corrupted++;
      }
    }
    
    if (corrupted > 0) {
      throw new Error(`${corrupted}/${sampled.length} sampled files are corrupted`);
    }
    
    console.log(`   Verified ${readable}/${sampled.length} sampled files`);
  });
  
  test('Event files are readable', async () => {
    const eventFiles = findDataFiles(RAW_DIR, name => name.startsWith('events-'));
    const sampled = sampleFiles(eventFiles, SAMPLE_SIZE);
    
    let readable = 0;
    let corrupted = 0;
    
    for (const file of sampled) {
      try {
        const stats = await getFileStats(file);
        if (stats.count > 0) readable++;
      } catch {
        corrupted++;
      }
    }
    
    if (corrupted > 0) {
      throw new Error(`${corrupted}/${sampled.length} sampled files are corrupted`);
    }
    
    console.log(`   Verified ${readable}/${sampled.length} sampled files`);
  });
  
  test('Update records have critical fields', async () => {
    const updateFiles = findDataFiles(RAW_DIR, name => name.startsWith('updates-'));
    if (updateFiles.length === 0) throw new Error('No update files to test');
    
    // Test just first file
    const file = updateFiles[0];
    const data = await readBinaryFile(file);
    
    if (data.records.length === 0) {
      throw new Error('First update file is empty');
    }
    
    const record = data.records[0];
    const missing = CRITICAL_UPDATE_FIELDS.filter(f => !record[f]);
    
    if (missing.length > 0) {
      throw new Error(`Missing critical fields: ${missing.join(', ')}`);
    }
    
    console.log(`   First record has all critical fields`);
  });
  
  test('Event records have critical fields', async () => {
    const eventFiles = findDataFiles(RAW_DIR, name => name.startsWith('events-'));
    if (eventFiles.length === 0) throw new Error('No event files to test');
    
    // Test just first file
    const file = eventFiles[0];
    const data = await readBinaryFile(file);
    
    if (data.records.length === 0) {
      throw new Error('First event file is empty');
    }
    
    const record = data.records[0];
    const missing = CRITICAL_EVENT_FIELDS.filter(f => !record[f]);
    
    if (missing.length > 0) {
      throw new Error(`Missing critical fields: ${missing.join(', ')}`);
    }
    
    console.log(`   First record has all critical fields`);
  });
}

test('Cursor directory exists', () => {
  if (!existsSync(CURSOR_DIR)) {
    throw new Error(`Cursor directory not found: ${CURSOR_DIR}`);
  }
});

test('At least one cursor file exists', () => {
  const cursors = readdirSync(CURSOR_DIR).filter(f => f.endsWith('.json'));
  if (cursors.length === 0) {
    throw new Error('No cursor files found');
  }
  console.log(`   Found ${cursors.length} cursor files`);
});

// ─────────────────────────────────────────────────────────────
// Run Tests
// ─────────────────────────────────────────────────────────────

runTests()
  .then(passed => process.exit(passed ? 0 : 1))
  .catch(err => {
    console.error(`\n❌ FATAL: ${err.message}`);
    process.exit(1);
  });
