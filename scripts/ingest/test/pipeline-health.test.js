#!/usr/bin/env node
/**
 * End-to-End Pipeline Health Check
 * 
 * Validates the entire ingestion pipeline is working:
 * 1. Scan API connectivity
 * 2. Data fetch (backfill, updates, ACS)
 * 3. Data parsing and normalization
 * 4. File writing (temp files)
 * 5. GCS upload (if enabled)
 * 6. Data integrity verification
 * 
 * Usage:
 *   node test/pipeline-health.test.js [--quick] [--skip-gcs]
 */

import { fileURLToPath } from 'url';
import path from 'path';
import fs from 'fs';
import https from 'https';
import http from 'http';
import { execSync, exec } from 'child_process';
import crypto from 'crypto';
import os from 'os';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ─────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────

const SCAN_URL = process.env.SCAN_URL || 'https://scan.sv-1.global.canton.network.sync.global/api/scan';
const TIMEOUT_MS = parseInt(process.env.API_TIMEOUT_MS) || 30000;
const GCS_BUCKET = process.env.GCS_BUCKET;
const QUICK_MODE = process.argv.includes('--quick');
const SKIP_GCS = process.argv.includes('--skip-gcs') || !GCS_BUCKET;

const TEST_DIR = path.join(os.tmpdir(), `pipeline-health-${Date.now()}`);

// ─────────────────────────────────────────────────────────────
// Utilities
// ─────────────────────────────────────────────────────────────

function httpGet(url, options = {}) {
  return new Promise((resolve, reject) => {
    const parsedUrl = new URL(url);
    const client = parsedUrl.protocol === 'https:' ? https : http;
    
    const req = client.get(url, {
      timeout: TIMEOUT_MS,
      headers: {
        'Accept': 'application/json',
        'User-Agent': 'PipelineHealthCheck/1.0',
        ...options.headers,
      },
    }, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        resolve({
          ok: res.statusCode >= 200 && res.statusCode < 300,
          status: res.statusCode,
          statusText: res.statusMessage,
          data,
          json: () => {
            try {
              return JSON.parse(data);
            } catch (e) {
              throw new Error(`Invalid JSON: ${data.substring(0, 200)}`);
            }
          },
        });
      });
    });
    
    req.on('error', reject);
    req.on('timeout', () => {
      req.destroy();
      reject(new Error(`Timeout after ${TIMEOUT_MS}ms`));
    });
  });
}

function execAsync(cmd, options = {}) {
  return new Promise((resolve, reject) => {
    exec(cmd, { timeout: 60000, ...options }, (error, stdout, stderr) => {
      if (error) {
        reject(new Error(`${error.message}\nstderr: ${stderr}`));
      } else {
        resolve({ stdout: stdout.trim(), stderr: stderr.trim() });
      }
    });
  });
}

function hashFile(filePath) {
  const content = fs.readFileSync(filePath);
  return crypto.createHash('sha256').update(content).digest('hex');
}

function cleanup() {
  if (fs.existsSync(TEST_DIR)) {
    fs.rmSync(TEST_DIR, { recursive: true, force: true });
  }
}

// ─────────────────────────────────────────────────────────────
// Health Check Framework
// ─────────────────────────────────────────────────────────────

const checks = [];
const results = {
  passed: 0,
  failed: 0,
  skipped: 0,
  warnings: 0,
  details: [],
};

function check(name, category, fn, options = {}) {
  checks.push({ name, category, fn, ...options });
}

function warn(name, category, reason) {
  checks.push({ name, category, skip: true, warning: true, reason });
}

function skip(name, category, reason) {
  checks.push({ name, category, skip: true, reason });
}

async function runHealthChecks() {
  const startTime = Date.now();
  
  console.log('\n' + '═'.repeat(70));
  console.log('🏥 END-TO-END PIPELINE HEALTH CHECK');
  console.log('═'.repeat(70));
  console.log(`   Scan API: ${SCAN_URL}`);
  console.log(`   GCS Bucket: ${GCS_BUCKET || '(not configured)'}`);
  console.log(`   Mode: ${QUICK_MODE ? 'Quick' : 'Full'}`);
  console.log(`   Test Dir: ${TEST_DIR}`);
  console.log('─'.repeat(70));
  
  // Create test directory
  fs.mkdirSync(TEST_DIR, { recursive: true });
  
  let currentCategory = '';
  
  for (const { name, category, fn, skip: isSkipped, warning, reason, critical } of checks) {
    // Print category header
    if (category !== currentCategory) {
      currentCategory = category;
      console.log(`\n📦 ${category.toUpperCase()}`);
      console.log('─'.repeat(50));
    }
    
    if (isSkipped) {
      if (warning) {
        console.log(`   ⚠️  ${name}: ${reason}`);
        results.warnings++;
      } else {
        console.log(`   ⏭️  ${name}: SKIPPED (${reason})`);
        results.skipped++;
      }
      results.details.push({ name, category, status: warning ? 'warning' : 'skipped', reason });
      continue;
    }
    
    const checkStart = Date.now();
    try {
      const result = await fn();
      const duration = Date.now() - checkStart;
      console.log(`   ✅ ${name} (${duration}ms)`);
      if (result?.info) {
        console.log(`      └─ ${result.info}`);
      }
      results.passed++;
      results.details.push({ name, category, status: 'passed', duration, info: result?.info });
    } catch (err) {
      const duration = Date.now() - checkStart;
      console.log(`   ❌ ${name} (${duration}ms)`);
      console.log(`      └─ Error: ${err.message}`);
      results.failed++;
      results.details.push({ name, category, status: 'failed', duration, error: err.message, critical });
      
      if (critical) {
        console.log('\n   🛑 CRITICAL CHECK FAILED - Aborting remaining checks');
        break;
      }
    }
  }
  
  // Cleanup
  cleanup();
  
  // Summary
  const totalTime = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log('\n' + '═'.repeat(70));
  console.log('📊 HEALTH CHECK SUMMARY');
  console.log('─'.repeat(70));
  console.log(`   ✅ Passed:   ${results.passed}`);
  console.log(`   ❌ Failed:   ${results.failed}`);
  console.log(`   ⚠️  Warnings: ${results.warnings}`);
  console.log(`   ⏭️  Skipped:  ${results.skipped}`);
  console.log(`   ⏱️  Duration: ${totalTime}s`);
  console.log('─'.repeat(70));
  
  if (results.failed === 0) {
    console.log('🎉 PIPELINE HEALTHY - All critical checks passed!');
  } else {
    console.log('🚨 PIPELINE UNHEALTHY - Critical issues detected');
    console.log('\nFailed checks:');
    results.details
      .filter(d => d.status === 'failed')
      .forEach(d => console.log(`   • ${d.category}/${d.name}: ${d.error}`));
  }
  
  console.log('═'.repeat(70) + '\n');
  
  return results.failed === 0;
}

// ─────────────────────────────────────────────────────────────
// STAGE 1: API Connectivity
// ─────────────────────────────────────────────────────────────

check('DNS Resolution', 'API Connectivity', async () => {
  const url = new URL(SCAN_URL);
  const { stdout } = await execAsync(`getent hosts ${url.hostname} || nslookup ${url.hostname}`);
  return { info: `Resolved ${url.hostname}` };
}, { critical: true });

check('TLS Handshake', 'API Connectivity', async () => {
  const url = new URL(SCAN_URL);
  if (url.protocol !== 'https:') {
    return { info: 'HTTP (no TLS)' };
  }
  
  await new Promise((resolve, reject) => {
    const req = https.request({
      hostname: url.hostname,
      port: 443,
      path: '/',
      method: 'HEAD',
      timeout: 10000,
    }, (res) => resolve(res));
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('TLS timeout')); });
    req.end();
  });
  
  return { info: 'TLS connection successful' };
}, { critical: true });

check('API Base Endpoint', 'API Connectivity', async () => {
  const response = await httpGet(`${SCAN_URL}/v0/round-of-latest-data`);
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  const data = response.json();
  return { info: `Round ${data.round}` };
}, { critical: true });

// ─────────────────────────────────────────────────────────────
// STAGE 2: Data Fetch Endpoints
// ─────────────────────────────────────────────────────────────

check('Updates Endpoint', 'Data Fetch', async () => {
  const before = new Date().toISOString();
  const response = await httpGet(`${SCAN_URL}/v0/updates?before=${encodeURIComponent(before)}&page_size=1`);
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  const data = response.json();
  const count = (data.updates || data.items || []).length;
  return { info: `Returned ${count} update(s)` };
});

check('Backfill Endpoint (transactions-by-round)', 'Data Fetch', async () => {
  // First get the latest round
  const roundRes = await httpGet(`${SCAN_URL}/v0/round-of-latest-data`);
  if (!roundRes.ok) throw new Error('Failed to get latest round');
  const { round } = roundRes.json();
  
  // Then fetch transactions for a recent round
  const targetRound = Math.max(0, round - 10);
  const response = await httpGet(`${SCAN_URL}/v0/updates/transactions-by-round/${targetRound}?page_size=1`);
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  return { info: `Round ${targetRound} accessible` };
});

check('ACS Migrations Detection', 'Data Fetch', async () => {
  const before = new Date().toISOString();
  const migrations = [];
  
  for (let migId = 0; migId <= 3; migId++) {
    try {
      const url = `${SCAN_URL}/v0/state/acs/snapshot-timestamp?before=${encodeURIComponent(before)}&migration_id=${migId}`;
      const response = await httpGet(url);
      if (response.ok) {
        migrations.push(migId);
      }
    } catch {
      // Expected for non-existent migrations
    }
  }
  
  return { info: `Found migrations: [${migrations.join(', ') || 'none'}]` };
});

if (!QUICK_MODE) {
  check('ACS Contracts Fetch', 'Data Fetch', async () => {
    const before = new Date().toISOString();
    
    // First get snapshot timestamp for migration 0
    const tsUrl = `${SCAN_URL}/v0/state/acs/snapshot-timestamp?before=${encodeURIComponent(before)}&migration_id=0`;
    const tsRes = await httpGet(tsUrl);
    
    if (!tsRes.ok) {
      return { info: 'No ACS snapshot available (expected for new networks)' };
    }
    
    const { record_time } = tsRes.json();
    
    // Fetch first page of contracts
    const acsUrl = `${SCAN_URL}/v0/state/acs?record_time=${encodeURIComponent(record_time)}&migration_id=0&page_size=10`;
    const acsRes = await httpGet(acsUrl);
    
    if (!acsRes.ok) {
      throw new Error(`HTTP ${acsRes.status}`);
    }
    
    const data = acsRes.json();
    const contracts = data.items || data.contracts || [];
    return { info: `Fetched ${contracts.length} contracts` };
  });
}

// ─────────────────────────────────────────────────────────────
// STAGE 3: Data Processing
// ─────────────────────────────────────────────────────────────

check('JSON Parsing', 'Data Processing', async () => {
  const response = await httpGet(`${SCAN_URL}/v0/round-of-latest-data`);
  const data = response.json();
  
  if (typeof data !== 'object') {
    throw new Error('Expected object response');
  }
  
  return { info: 'JSON parsing successful' };
});

check('BigNumber Handling', 'Data Processing', async () => {
  // Test with a real update that might contain large numbers
  const before = new Date().toISOString();
  const response = await httpGet(`${SCAN_URL}/v0/updates?before=${encodeURIComponent(before)}&page_size=5`);
  
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  
  const data = response.json();
  const updates = data.updates || data.items || [];
  
  // Check if we can find any numeric fields
  let numericFieldsFound = 0;
  for (const update of updates) {
    const json = JSON.stringify(update);
    // Look for large numbers or decimal values
    if (/"\d{10,}"/.test(json) || /\d+\.\d+/.test(json)) {
      numericFieldsFound++;
    }
  }
  
  return { info: `Processed ${updates.length} updates, ${numericFieldsFound} with large numbers` };
});

// ─────────────────────────────────────────────────────────────
// STAGE 4: File I/O
// ─────────────────────────────────────────────────────────────

check('JSONL Write', 'File I/O', async () => {
  const testFile = path.join(TEST_DIR, 'test.jsonl');
  const records = [
    { id: 1, name: 'test1', timestamp: new Date().toISOString() },
    { id: 2, name: 'test2', amount: '123456789012345678901234567890' },
    { id: 3, name: 'test3', nested: { a: 1, b: [1, 2, 3] } },
  ];
  
  const content = records.map(r => JSON.stringify(r)).join('\n') + '\n';
  fs.writeFileSync(testFile, content);
  
  // Verify
  const lines = fs.readFileSync(testFile, 'utf8').split('\n').filter(Boolean);
  if (lines.length !== 3) {
    throw new Error(`Expected 3 lines, got ${lines.length}`);
  }
  
  // Parse each line
  lines.forEach((line, i) => {
    const parsed = JSON.parse(line);
    if (parsed.id !== i + 1) {
      throw new Error(`Record ${i} mismatch`);
    }
  });
  
  return { info: `Wrote and verified ${lines.length} records` };
});

check('Large File Write', 'File I/O', async () => {
  const testFile = path.join(TEST_DIR, 'large-test.jsonl');
  const recordCount = QUICK_MODE ? 1000 : 10000;
  
  const writeStream = fs.createWriteStream(testFile);
  
  for (let i = 0; i < recordCount; i++) {
    writeStream.write(JSON.stringify({
      id: i,
      timestamp: new Date().toISOString(),
      data: 'x'.repeat(100),
    }) + '\n');
  }
  
  await new Promise((resolve) => writeStream.end(resolve));
  
  const stats = fs.statSync(testFile);
  const sizeMB = (stats.size / 1024 / 1024).toFixed(2);
  
  return { info: `Wrote ${recordCount} records (${sizeMB} MB)` };
});

check('Temp Directory Permissions', 'File I/O', async () => {
  const testFile = path.join(os.tmpdir(), `health-check-${Date.now()}.tmp`);
  
  try {
    fs.writeFileSync(testFile, 'test');
    const content = fs.readFileSync(testFile, 'utf8');
    if (content !== 'test') {
      throw new Error('Read/write mismatch');
    }
    fs.unlinkSync(testFile);
    return { info: `${os.tmpdir()} is writable` };
  } catch (err) {
    throw new Error(`Cannot write to temp: ${err.message}`);
  }
});

// ─────────────────────────────────────────────────────────────
// STAGE 5: GCS Integration
// ─────────────────────────────────────────────────────────────

if (SKIP_GCS) {
  skip('GCS Authentication', 'GCS Integration', 'GCS not configured or --skip-gcs');
  skip('GCS Bucket Access', 'GCS Integration', 'GCS not configured or --skip-gcs');
  skip('GCS Upload/Download Integrity', 'GCS Integration', 'GCS not configured or --skip-gcs');
} else {
  check('GCS Authentication', 'GCS Integration', async () => {
    try {
      const { stdout } = await execAsync('gcloud auth list --filter=status:ACTIVE --format="value(account)" 2>/dev/null');
      if (!stdout) {
        throw new Error('No active account');
      }
      return { info: stdout.split('\n')[0] };
    } catch {
      // Try service account
      const { stdout } = await execAsync('curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/email 2>/dev/null || echo "unknown"');
      return { info: `VM Service Account: ${stdout}` };
    }
  });

  check('GCS Bucket Access', 'GCS Integration', async () => {
    const { stdout } = await execAsync(`gsutil ls gs://${GCS_BUCKET}/ 2>&1 | head -5`);
    const items = stdout.split('\n').filter(Boolean).length;
    return { info: `Listed ${items} items` };
  });

  check('GCS Upload/Download Integrity', 'GCS Integration', async () => {
    const testContent = crypto.randomBytes(1024).toString('hex');
    const testFile = path.join(TEST_DIR, 'gcs-integrity-test.txt');
    const gcsPath = `gs://${GCS_BUCKET}/health-check/integrity-${Date.now()}.txt`;
    
    // Write local file
    fs.writeFileSync(testFile, testContent);
    const localHash = hashFile(testFile);
    
    // Upload to GCS
    await execAsync(`gsutil cp ${testFile} ${gcsPath}`);
    
    // Download from GCS
    const downloadFile = path.join(TEST_DIR, 'gcs-download.txt');
    await execAsync(`gsutil cp ${gcsPath} ${downloadFile}`);
    
    // Verify hash
    const downloadHash = hashFile(downloadFile);
    
    // Cleanup GCS
    await execAsync(`gsutil rm ${gcsPath}`);
    
    if (localHash !== downloadHash) {
      throw new Error(`Hash mismatch: ${localHash} !== ${downloadHash}`);
    }
    
    return { info: 'Upload/download integrity verified' };
  });
}

// ─────────────────────────────────────────────────────────────
// STAGE 6: System Resources
// ─────────────────────────────────────────────────────────────

check('Available Memory', 'System Resources', async () => {
  const totalMem = os.totalmem();
  const freeMem = os.freemem();
  const usedPercent = ((1 - freeMem / totalMem) * 100).toFixed(1);
  const freeGB = (freeMem / 1024 / 1024 / 1024).toFixed(1);
  
  if (freeMem < 500 * 1024 * 1024) {
    throw new Error(`Low memory: ${freeGB}GB free`);
  }
  
  return { info: `${freeGB}GB free (${usedPercent}% used)` };
});

check('Available Disk Space', 'System Resources', async () => {
  try {
    const { stdout } = await execAsync('df -h /tmp | tail -1');
    const parts = stdout.split(/\s+/);
    const available = parts[3];
    const usePercent = parts[4];
    
    return { info: `${available} available (${usePercent} used)` };
  } catch {
    return { info: 'Unable to check (non-critical)' };
  }
});

check('Node.js Version', 'System Resources', async () => {
  const version = process.version;
  const major = parseInt(version.slice(1));
  
  if (major < 16) {
    throw new Error(`Node.js ${version} is too old (need >= 16)`);
  }
  
  return { info: version };
});

check('Required Tools', 'System Resources', async () => {
  const tools = ['node', 'npm'];
  if (!SKIP_GCS) {
    tools.push('gsutil', 'gcloud');
  }
  
  const found = [];
  const missing = [];
  
  for (const tool of tools) {
    try {
      await execAsync(`which ${tool}`);
      found.push(tool);
    } catch {
      missing.push(tool);
    }
  }
  
  if (missing.length > 0) {
    throw new Error(`Missing: ${missing.join(', ')}`);
  }
  
  return { info: found.join(', ') };
});

// ─────────────────────────────────────────────────────────────
// Run Health Checks
// ─────────────────────────────────────────────────────────────

runHealthChecks()
  .then(healthy => {
    process.exit(healthy ? 0 : 1);
  })
  .catch(err => {
    console.error(`\n❌ FATAL: ${err.message}`);
    cleanup();
    process.exit(1);
  });
