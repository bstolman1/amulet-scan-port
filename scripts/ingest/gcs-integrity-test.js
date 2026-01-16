#!/usr/bin/env node
/**
 * GCS Integrity Test
 * 
 * Verifies that files written to /tmp are successfully uploaded to GCS 1:1.
 * 
 * Tests:
 * 1. Write test files to /tmp with known content
 * 2. Upload to GCS using the same mechanism as ingestion
 * 3. Download and verify content matches exactly
 * 4. Verify file sizes match
 * 5. Clean up test files
 * 
 * Usage:
 *   node gcs-integrity-test.js               # Run full test
 *   node gcs-integrity-test.js --keep        # Don't delete test files
 *   node gcs-integrity-test.js --size 1000   # Test with 1000 records
 */

import { execSync } from 'child_process';
import { writeFileSync, readFileSync, existsSync, unlinkSync, mkdirSync, statSync } from 'fs';
import { randomBytes, createHash } from 'crypto';
import path from 'path';
import { runPreflightChecks } from './gcs-preflight.js';
import { initGCS, uploadAndCleanupSync, getTmpPath, getGCSPath, isGCSEnabled } from './gcs-upload.js';

// ─────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────

const GCS_BUCKET = process.env.GCS_BUCKET;
const TEST_PREFIX = '_integrity_test';

// ─────────────────────────────────────────────────────────────
// CLI Argument Parsing
// ─────────────────────────────────────────────────────────────

function parseArgs() {
  const args = process.argv.slice(2);
  return {
    keep: args.includes('--keep'),
    size: parseInt(args.find((_, i) => args[i - 1] === '--size') || '100'),
    verbose: args.includes('--verbose') || args.includes('-v'),
    help: args.includes('--help') || args.includes('-h'),
  };
}

// ─────────────────────────────────────────────────────────────
// Test Utilities
// ─────────────────────────────────────────────────────────────

/**
 * Generate test data with known content
 */
function generateTestData(numRecords) {
  const records = [];
  for (let i = 0; i < numRecords; i++) {
    records.push({
      id: `test_${i}_${randomBytes(8).toString('hex')}`,
      timestamp: new Date().toISOString(),
      data: randomBytes(100).toString('base64'),
      index: i,
    });
  }
  return JSON.stringify(records);
}

/**
 * Calculate SHA256 hash of content
 */
function sha256(content) {
  return createHash('sha256').update(content).digest('hex');
}

/**
 * Download file from GCS and return content
 */
function downloadFromGCS(gcsPath) {
  const tempDownloadPath = `/tmp/gcs_integrity_download_${Date.now()}.tmp`;
  
  try {
    execSync(`gsutil cp "${gcsPath}" "${tempDownloadPath}"`, {
      encoding: 'utf8',
      stdio: ['pipe', 'pipe', 'pipe'],
      timeout: 60000,
    });
    
    const content = readFileSync(tempDownloadPath);
    unlinkSync(tempDownloadPath);
    return content;
  } catch (err) {
    if (existsSync(tempDownloadPath)) {
      unlinkSync(tempDownloadPath);
    }
    throw err;
  }
}

/**
 * Get file size from GCS
 */
function getGCSFileSize(gcsPath) {
  try {
    const output = execSync(`gsutil stat "${gcsPath}"`, {
      encoding: 'utf8',
      stdio: ['pipe', 'pipe', 'pipe'],
      timeout: 30000,
    });
    
    const sizeMatch = output.match(/Content-Length:\s*(\d+)/);
    return sizeMatch ? parseInt(sizeMatch[1]) : null;
  } catch {
    return null;
  }
}

/**
 * Delete file from GCS
 */
function deleteFromGCS(gcsPath) {
  try {
    execSync(`gsutil rm "${gcsPath}"`, {
      encoding: 'utf8',
      stdio: ['pipe', 'pipe', 'pipe'],
      timeout: 30000,
    });
    return true;
  } catch {
    return false;
  }
}

// ─────────────────────────────────────────────────────────────
// Test Cases
// ─────────────────────────────────────────────────────────────

/**
 * Test basic upload and download integrity
 */
async function testBasicIntegrity(options = {}) {
  const { size = 100, verbose = false } = options;
  
  console.log(`\n📝 Test: Basic Upload/Download Integrity (${size} records)`);
  
  const testFileName = `${TEST_PREFIX}_basic_${Date.now()}.json`;
  const relativePath = `test/${testFileName}`;
  const localPath = getTmpPath(relativePath);
  const gcsPath = getGCSPath(relativePath);
  
  // Generate and write test data
  const originalContent = generateTestData(size);
  const originalHash = sha256(originalContent);
  const originalSize = Buffer.byteLength(originalContent);
  
  if (verbose) {
    console.log(`   Local path: ${localPath}`);
    console.log(`   GCS path: ${gcsPath}`);
    console.log(`   Content size: ${originalSize} bytes`);
    console.log(`   Content hash: ${originalHash.substring(0, 16)}...`);
  }
  
  // Write to local tmp
  const dir = path.dirname(localPath);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
  }
  writeFileSync(localPath, originalContent);
  
  // Upload to GCS (this also deletes local file)
  const uploadResult = uploadAndCleanupSync(localPath, gcsPath, { deleteOnFailure: false });
  
  if (!uploadResult.ok) {
    return {
      name: 'Basic Integrity',
      passed: false,
      error: `Upload failed: ${uploadResult.error}`,
    };
  }
  
  // Verify local file was deleted
  if (existsSync(localPath)) {
    return {
      name: 'Basic Integrity',
      passed: false,
      error: 'Local file was not deleted after upload',
    };
  }
  
  // Download from GCS
  let downloadedContent;
  try {
    downloadedContent = downloadFromGCS(gcsPath);
  } catch (err) {
    return {
      name: 'Basic Integrity',
      passed: false,
      error: `Download failed: ${err.message}`,
    };
  }
  
  // Verify content matches
  const downloadedHash = sha256(downloadedContent);
  const downloadedSize = downloadedContent.length;
  
  if (verbose) {
    console.log(`   Downloaded size: ${downloadedSize} bytes`);
    console.log(`   Downloaded hash: ${downloadedHash.substring(0, 16)}...`);
  }
  
  // Clean up GCS file
  if (!options.keep) {
    deleteFromGCS(gcsPath);
  }
  
  if (originalHash !== downloadedHash) {
    return {
      name: 'Basic Integrity',
      passed: false,
      error: `Hash mismatch: original=${originalHash.substring(0, 16)}... downloaded=${downloadedHash.substring(0, 16)}...`,
    };
  }
  
  if (originalSize !== downloadedSize) {
    return {
      name: 'Basic Integrity',
      passed: false,
      error: `Size mismatch: original=${originalSize} downloaded=${downloadedSize}`,
    };
  }
  
  console.log(`   ✅ Content verified (${originalSize} bytes, hash matches)`);
  
  return {
    name: 'Basic Integrity',
    passed: true,
    bytesVerified: originalSize,
  };
}

/**
 * Test multiple concurrent uploads
 */
async function testConcurrentUploads(options = {}) {
  const { verbose = false } = options;
  const numFiles = 5;
  
  console.log(`\n📝 Test: Concurrent Uploads (${numFiles} files)`);
  
  const testFiles = [];
  
  // Generate test files
  for (let i = 0; i < numFiles; i++) {
    const testFileName = `${TEST_PREFIX}_concurrent_${i}_${Date.now()}.json`;
    const relativePath = `test/${testFileName}`;
    const localPath = getTmpPath(relativePath);
    const gcsPath = getGCSPath(relativePath);
    const content = generateTestData(50 + i * 10);
    const hash = sha256(content);
    
    const dir = path.dirname(localPath);
    if (!existsSync(dir)) {
      mkdirSync(dir, { recursive: true });
    }
    writeFileSync(localPath, content);
    
    testFiles.push({ localPath, gcsPath, hash, size: Buffer.byteLength(content) });
  }
  
  // Upload all files
  const uploadResults = testFiles.map(f => 
    uploadAndCleanupSync(f.localPath, f.gcsPath, { deleteOnFailure: false })
  );
  
  // Check all uploads succeeded
  const failedUploads = uploadResults.filter(r => !r.ok);
  if (failedUploads.length > 0) {
    return {
      name: 'Concurrent Uploads',
      passed: false,
      error: `${failedUploads.length}/${numFiles} uploads failed`,
    };
  }
  
  // Verify all files in GCS
  let verified = 0;
  for (const file of testFiles) {
    const gcsSize = getGCSFileSize(file.gcsPath);
    
    if (gcsSize === null) {
      return {
        name: 'Concurrent Uploads',
        passed: false,
        error: `File not found in GCS: ${file.gcsPath}`,
      };
    }
    
    if (gcsSize !== file.size) {
      return {
        name: 'Concurrent Uploads',
        passed: false,
        error: `Size mismatch for ${path.basename(file.gcsPath)}: expected=${file.size} actual=${gcsSize}`,
      };
    }
    
    verified++;
    
    // Clean up
    if (!options.keep) {
      deleteFromGCS(file.gcsPath);
    }
  }
  
  console.log(`   ✅ All ${verified} files verified in GCS`);
  
  return {
    name: 'Concurrent Uploads',
    passed: true,
    filesVerified: verified,
  };
}

/**
 * Test large file upload
 */
async function testLargeFile(options = {}) {
  const { verbose = false } = options;
  const largeSize = 10000; // 10k records
  
  console.log(`\n📝 Test: Large File Upload (${largeSize} records)`);
  
  const testFileName = `${TEST_PREFIX}_large_${Date.now()}.json`;
  const relativePath = `test/${testFileName}`;
  const localPath = getTmpPath(relativePath);
  const gcsPath = getGCSPath(relativePath);
  
  // Generate large content
  const content = generateTestData(largeSize);
  const originalSize = Buffer.byteLength(content);
  const originalHash = sha256(content);
  
  if (verbose) {
    console.log(`   Content size: ${(originalSize / 1024).toFixed(1)} KB`);
  }
  
  const dir = path.dirname(localPath);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
  }
  writeFileSync(localPath, content);
  
  // Upload
  const startTime = Date.now();
  const uploadResult = uploadAndCleanupSync(localPath, gcsPath, { deleteOnFailure: false });
  const uploadTime = Date.now() - startTime;
  
  if (!uploadResult.ok) {
    return {
      name: 'Large File Upload',
      passed: false,
      error: `Upload failed: ${uploadResult.error}`,
    };
  }
  
  // Verify in GCS
  const gcsSize = getGCSFileSize(gcsPath);
  
  // Clean up
  if (!options.keep) {
    deleteFromGCS(gcsPath);
  }
  
  if (gcsSize !== originalSize) {
    return {
      name: 'Large File Upload',
      passed: false,
      error: `Size mismatch: expected=${originalSize} actual=${gcsSize}`,
    };
  }
  
  const throughput = (originalSize / 1024) / (uploadTime / 1000);
  console.log(`   ✅ Verified ${(originalSize / 1024).toFixed(1)} KB in ${uploadTime}ms (${throughput.toFixed(1)} KB/s)`);
  
  return {
    name: 'Large File Upload',
    passed: true,
    bytesVerified: originalSize,
    uploadTimeMs: uploadTime,
  };
}

// ─────────────────────────────────────────────────────────────
// Main Test Runner
// ─────────────────────────────────────────────────────────────

async function runIntegrityTests() {
  const options = parseArgs();
  
  if (options.help) {
    console.log(`
GCS Integrity Test - Verifies /tmp → GCS upload integrity

Usage:
  node gcs-integrity-test.js [options]

Options:
  --keep              Don't delete test files after verification
  --size <n>          Number of records for basic test (default: 100)
  --verbose, -v       Show detailed output
  --help, -h          Show this help
`);
    process.exit(0);
  }
  
  console.log('\n' + '═'.repeat(60));
  console.log('🧪 GCS INTEGRITY TEST');
  console.log('═'.repeat(60));
  
  // Run preflight checks first
  try {
    runPreflightChecks({ quick: false, throwOnFail: true });
  } catch (err) {
    console.error(`\n❌ Preflight checks failed: ${err.message}`);
    process.exit(1);
  }
  
  // Initialize GCS
  try {
    initGCS();
  } catch (err) {
    console.error(`\n❌ GCS initialization failed: ${err.message}`);
    process.exit(1);
  }
  
  console.log(`\n📊 Test Configuration:`);
  console.log(`   GCS Bucket: ${GCS_BUCKET}`);
  console.log(`   Test size: ${options.size} records`);
  console.log(`   Keep files: ${options.keep}`);
  
  // Run tests
  const results = [];
  
  results.push(await testBasicIntegrity(options));
  results.push(await testConcurrentUploads(options));
  results.push(await testLargeFile(options));
  
  // Print summary
  console.log('\n' + '─'.repeat(60));
  console.log('📊 TEST RESULTS');
  console.log('─'.repeat(60));
  
  let passed = 0;
  let failed = 0;
  
  for (const result of results) {
    if (result.passed) {
      console.log(`✅ ${result.name}`);
      passed++;
    } else {
      console.log(`❌ ${result.name}: ${result.error}`);
      failed++;
    }
  }
  
  console.log('─'.repeat(60));
  console.log(`Total: ${passed} passed, ${failed} failed`);
  console.log('═'.repeat(60) + '\n');
  
  process.exit(failed > 0 ? 1 : 0);
}

// Run tests
runIntegrityTests().catch(err => {
  console.error(`\n❌ FATAL: ${err.message}`);
  console.error(err.stack);
  process.exit(1);
});
