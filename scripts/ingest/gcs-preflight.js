#!/usr/bin/env node
/**
 * GCS Preflight Validation
 * 
 * Validates GCS configuration before starting ingestion:
 * 1. gsutil is installed and accessible
 * 2. GCS_BUCKET environment variable is set
 * 3. Service account has read/write access to bucket
 * 4. Network connectivity to GCS endpoints
 * 
 * Call runPreflightChecks() at the start of any ingestion script.
 * 
 * Usage:
 *   node gcs-preflight.js           # Run all checks
 *   node gcs-preflight.js --quick   # Skip write test
 */

import { execSync } from 'child_process';
import { randomBytes } from 'crypto';

// ─────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────

const PREFLIGHT_TIMEOUT_MS = parseInt(process.env.PREFLIGHT_TIMEOUT_MS) || 30000;

// ─────────────────────────────────────────────────────────────
// Check Functions
// ─────────────────────────────────────────────────────────────

/**
 * Check if gsutil is installed and accessible
 */
function checkGsutilInstalled() {
  try {
    const output = execSync('gsutil version', {
      encoding: 'utf8',
      stdio: ['pipe', 'pipe', 'pipe'],
      timeout: 10000,
    });
    
    const versionMatch = output.match(/gsutil version:\s*([\d.]+)/);
    const version = versionMatch ? versionMatch[1] : 'unknown';
    
    return {
      ok: true,
      name: 'gsutil installed',
      version,
      message: `gsutil version ${version}`,
    };
  } catch (err) {
    return {
      ok: false,
      name: 'gsutil installed',
      error: err.message,
      message: 'gsutil not found. Install Google Cloud SDK: https://cloud.google.com/sdk/docs/install',
    };
  }
}

/**
 * Check if GCS_BUCKET environment variable is set
 */
function checkBucketEnvVar() {
  const bucket = process.env.GCS_BUCKET;
  
  if (!bucket) {
    return {
      ok: false,
      name: 'GCS_BUCKET env var',
      message: 'GCS_BUCKET environment variable is not set. Add to .env file.',
    };
  }
  
  // Basic bucket name validation
  const bucketPattern = /^[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]$/;
  if (!bucketPattern.test(bucket)) {
    return {
      ok: false,
      name: 'GCS_BUCKET env var',
      message: `Invalid bucket name format: "${bucket}". Bucket names must be 3-63 chars, lowercase letters, numbers, hyphens.`,
    };
  }
  
  return {
    ok: true,
    name: 'GCS_BUCKET env var',
    bucket,
    message: `Bucket: ${bucket}`,
  };
}

/**
 * Check if we can list the bucket (read access)
 */
function checkBucketReadAccess() {
  const bucket = process.env.GCS_BUCKET;
  if (!bucket) {
    return { ok: false, name: 'bucket read access', message: 'GCS_BUCKET not set' };
  }
  
  try {
    execSync(`gsutil ls "gs://${bucket}/" 2>&1 | head -5`, {
      encoding: 'utf8',
      stdio: ['pipe', 'pipe', 'pipe'],
      timeout: PREFLIGHT_TIMEOUT_MS,
    });
    
    return {
      ok: true,
      name: 'bucket read access',
      message: `Can list gs://${bucket}/`,
    };
  } catch (err) {
    const errorMsg = err.message || '';
    
    if (errorMsg.includes('AccessDenied') || errorMsg.includes('403')) {
      return {
        ok: false,
        name: 'bucket read access',
        message: `Access denied to gs://${bucket}/. Check service account permissions.`,
      };
    }
    
    if (errorMsg.includes('BucketNotFound') || errorMsg.includes('404')) {
      return {
        ok: false,
        name: 'bucket read access',
        message: `Bucket not found: gs://${bucket}/. Verify bucket exists.`,
      };
    }
    
    if (errorMsg.includes('timeout') || errorMsg.includes('ETIMEDOUT')) {
      return {
        ok: false,
        name: 'bucket read access',
        message: `Network timeout connecting to GCS. Check firewall/proxy settings.`,
      };
    }
    
    return {
      ok: false,
      name: 'bucket read access',
      error: errorMsg,
      message: `Failed to access bucket: ${errorMsg.substring(0, 100)}`,
    };
  }
}

/**
 * Check if we can write to the bucket (write access)
 */
function checkBucketWriteAccess(skipWrite = false) {
  if (skipWrite) {
    return {
      ok: true,
      name: 'bucket write access',
      message: 'Skipped (--quick mode)',
      skipped: true,
    };
  }
  
  const bucket = process.env.GCS_BUCKET;
  if (!bucket) {
    return { ok: false, name: 'bucket write access', message: 'GCS_BUCKET not set' };
  }
  
  const testFileName = `_preflight_test_${Date.now()}_${randomBytes(4).toString('hex')}.txt`;
  const testPath = `gs://${bucket}/raw/${testFileName}`;
  const testContent = `Preflight test at ${new Date().toISOString()}`;
  
  try {
    // Write test file
    execSync(`echo "${testContent}" | gsutil cp - "${testPath}"`, {
      encoding: 'utf8',
      stdio: ['pipe', 'pipe', 'pipe'],
      timeout: PREFLIGHT_TIMEOUT_MS,
    });
    
    // Verify it exists
    execSync(`gsutil stat "${testPath}"`, {
      encoding: 'utf8',
      stdio: ['pipe', 'pipe', 'pipe'],
      timeout: 10000,
    });
    
    // Clean up
    try {
      execSync(`gsutil rm "${testPath}"`, {
        encoding: 'utf8',
        stdio: ['pipe', 'pipe', 'pipe'],
        timeout: 10000,
      });
    } catch {
      // Ignore cleanup errors
    }
    
    return {
      ok: true,
      name: 'bucket write access',
      message: `Can write to gs://${bucket}/raw/`,
    };
  } catch (err) {
    const errorMsg = err.message || '';
    
    if (errorMsg.includes('AccessDenied') || errorMsg.includes('403')) {
      return {
        ok: false,
        name: 'bucket write access',
        message: `Write access denied to gs://${bucket}/raw/. Check service account permissions (roles/storage.objectCreator).`,
      };
    }
    
    return {
      ok: false,
      name: 'bucket write access',
      error: errorMsg,
      message: `Failed to write to bucket: ${errorMsg.substring(0, 100)}`,
    };
  }
}

/**
 * Check GCS endpoint connectivity
 */
function checkGCSConnectivity() {
  try {
    // Test DNS resolution and TCP connectivity to GCS
    execSync('curl -s --connect-timeout 5 -o /dev/null -w "%{http_code}" https://storage.googleapis.com/', {
      encoding: 'utf8',
      stdio: ['pipe', 'pipe', 'pipe'],
      timeout: 10000,
    });
    
    return {
      ok: true,
      name: 'GCS connectivity',
      message: 'Can reach storage.googleapis.com',
    };
  } catch (err) {
    return {
      ok: false,
      name: 'GCS connectivity',
      message: 'Cannot reach storage.googleapis.com. Check network/firewall.',
      error: err.message,
    };
  }
}

/**
 * Check service account authentication
 */
function checkAuthentication() {
  try {
    const output = execSync('gcloud auth list --filter=status:ACTIVE --format="value(account)"', {
      encoding: 'utf8',
      stdio: ['pipe', 'pipe', 'pipe'],
      timeout: 10000,
    });
    
    const account = output.trim().split('\n')[0];
    
    if (!account) {
      return {
        ok: false,
        name: 'authentication',
        message: 'No active gcloud account. Run: gcloud auth login',
      };
    }
    
    return {
      ok: true,
      name: 'authentication',
      account,
      message: `Authenticated as: ${account}`,
    };
  } catch (err) {
    // gcloud might not be installed, but gsutil could still work via service account
    return {
      ok: true,
      name: 'authentication',
      message: 'gcloud not available (VM service account likely in use)',
      skipped: true,
    };
  }
}

// ─────────────────────────────────────────────────────────────
// Main Preflight Function
// ─────────────────────────────────────────────────────────────

/**
 * Run all preflight checks
 * 
 * @param {object} options - Options
 * @param {boolean} options.quick - Skip write test
 * @param {boolean} options.throwOnFail - Throw error if any check fails
 * @param {boolean} options.silent - Don't print to console
 * @returns {object} Results with ok (boolean) and checks (array)
 */
export function runPreflightChecks(options = {}) {
  const { quick = false, throwOnFail = true, silent = false } = options;
  
  const results = {
    ok: true,
    timestamp: new Date().toISOString(),
    checks: [],
  };
  
  const log = silent ? () => {} : console.log;
  const logError = silent ? () => {} : console.error;
  
  log('\n' + '═'.repeat(60));
  log('🔍 GCS PREFLIGHT CHECKS');
  log('═'.repeat(60));
  
  // Run checks in order
  const checks = [
    () => checkGsutilInstalled(),
    () => checkBucketEnvVar(),
    () => checkGCSConnectivity(),
    () => checkAuthentication(),
    () => checkBucketReadAccess(),
    () => checkBucketWriteAccess(quick),
  ];
  
  for (const check of checks) {
    const result = check();
    results.checks.push(result);
    
    const icon = result.ok ? '✅' : '❌';
    const status = result.skipped ? '⏭️' : icon;
    log(`${status} ${result.name}: ${result.message}`);
    
    if (!result.ok && !result.skipped) {
      results.ok = false;
    }
  }
  
  log('─'.repeat(60));
  
  if (results.ok) {
    log('✅ All preflight checks passed\n');
  } else {
    logError('❌ Preflight checks FAILED\n');
    
    if (throwOnFail) {
      const failures = results.checks.filter(c => !c.ok && !c.skipped);
      throw new Error(
        `GCS preflight failed:\n` +
        failures.map(f => `  - ${f.name}: ${f.message}`).join('\n')
      );
    }
  }
  
  return results;
}

/**
 * Validate GCS is ready (simple boolean check)
 */
export function isGCSReady(options = {}) {
  try {
    const results = runPreflightChecks({ ...options, throwOnFail: false, silent: true });
    return results.ok;
  } catch {
    return false;
  }
}

// ─────────────────────────────────────────────────────────────
// CLI Entry Point
// ─────────────────────────────────────────────────────────────

const isMainModule = process.argv[1]?.endsWith('gcs-preflight.js');

if (isMainModule) {
  const args = process.argv.slice(2);
  const quick = args.includes('--quick') || args.includes('-q');
  
  try {
    runPreflightChecks({ quick, throwOnFail: true });
    process.exit(0);
  } catch (err) {
    console.error(`\n${err.message}\n`);
    process.exit(1);
  }
}
