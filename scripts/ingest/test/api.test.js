#!/usr/bin/env node
/**
 * Scan API Connectivity Tests
 * 
 * Validates that the Scan API is accessible and returns expected data.
 * Tests the same endpoints used by fetch-backfill.js and fetch-updates.js.
 * 
 * Tests:
 * 1. API endpoint is reachable
 * 2. Round data endpoint returns valid data
 * 3. Updates endpoint returns expected structure
 * 4. ACS snapshot endpoint is accessible
 */

import { fileURLToPath } from 'url';
import path from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ─────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────

const SCAN_URL = process.env.SCAN_URL || 'https://scan.sv-1.global.canton.network.sync.global/api/scan';
const TIMEOUT_MS = parseInt(process.env.API_TIMEOUT_MS) || 30000;

// ─────────────────────────────────────────────────────────────
// Test Utilities
// ─────────────────────────────────────────────────────────────

async function fetchWithTimeout(url, options = {}) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), TIMEOUT_MS);
  
  try {
    const response = await fetch(url, {
      ...options,
      signal: controller.signal,
    });
    clearTimeout(timeout);
    return response;
  } catch (err) {
    clearTimeout(timeout);
    throw err;
  }
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
  console.log('🧪 SCAN API CONNECTIVITY TESTS');
  console.log('═'.repeat(60));
  console.log(`   API URL: ${SCAN_URL}`);
  console.log(`   Timeout: ${TIMEOUT_MS}ms`);
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

test('Scan API base URL is reachable', async () => {
  const response = await fetchWithTimeout(`${SCAN_URL}/v0/round-of-latest-data`);
  
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
  }
  
  console.log(`   Status: ${response.status} OK`);
});

test('Round data endpoint returns valid JSON', async () => {
  const response = await fetchWithTimeout(`${SCAN_URL}/v0/round-of-latest-data`);
  const data = await response.json();
  
  if (typeof data.round !== 'number' && typeof data.round !== 'string') {
    throw new Error(`Invalid round data: ${JSON.stringify(data).substring(0, 100)}`);
  }
  
  console.log(`   Latest round: ${data.round}`);
});

test('Updates endpoint returns expected structure', async () => {
  // Get a recent timestamp
  const before = new Date().toISOString();
  const url = `${SCAN_URL}/v0/updates?before=${encodeURIComponent(before)}&page_size=1`;
  
  const response = await fetchWithTimeout(url);
  
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
  }
  
  const data = await response.json();
  
  // Check for expected structure
  if (!Array.isArray(data.updates) && !data.items && !data.transactions) {
    throw new Error(`Unexpected response structure: ${Object.keys(data).join(', ')}`);
  }
  
  const updates = data.updates || data.items || data.transactions || [];
  console.log(`   Returned ${updates.length} update(s)`);
});

test('ACS snapshot timestamp endpoint is accessible', async () => {
  const before = new Date().toISOString();
  const url = `${SCAN_URL}/v0/state/acs/snapshot-timestamp?before=${encodeURIComponent(before)}&migration_id=0`;
  
  const response = await fetchWithTimeout(url);
  
  // May return 404 if no snapshots, but endpoint should be reachable
  if (response.status === 404) {
    console.log(`   Endpoint accessible (no snapshots for migration 0)`);
    return;
  }
  
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
  }
  
  const data = await response.json();
  console.log(`   Snapshot timestamp available`);
});

test('Migration detection works', async () => {
  // Try to detect migrations by checking snapshot timestamps for multiple migration IDs
  const before = new Date().toISOString();
  const migrations = [];
  
  for (let migId = 0; migId <= 5; migId++) {
    try {
      const url = `${SCAN_URL}/v0/state/acs/snapshot-timestamp?before=${encodeURIComponent(before)}&migration_id=${migId}`;
      const response = await fetchWithTimeout(url);
      
      if (response.ok) {
        migrations.push(migId);
      }
    } catch {
      // Ignore errors for individual migrations
    }
  }
  
  console.log(`   Found ${migrations.length} accessible migrations: [${migrations.join(', ')}]`);
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
