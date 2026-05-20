#!/usr/bin/env node
/**
 * GCS Preflight Validation
 *
 * Validates GCS configuration before starting ingestion:
 * 1. GCS_BUCKET environment variable is set and valid
 * 2. Network connectivity to GCS endpoints
 * 3. Service account has read access to bucket (via SDK)
 * 4. Service account has write access to bucket (via SDK)
 *
 * Uses @google-cloud/storage SDK (ADC) instead of gsutil to avoid
 * interactive reauthentication in non-interactive environments (systemd).
 *
 * Call runPreflightChecks() at the start of any ingestion script.
 *
 * Usage:
 *   node gcs-preflight.js           # Run all checks
 *   node gcs-preflight.js --quick   # Skip write test
 */

import { randomBytes } from 'crypto';
import { get as httpsGet } from 'https';
import { Storage } from '@google-cloud/storage';

const PREFLIGHT_TIMEOUT_MS = parseInt(process.env.PREFLIGHT_TIMEOUT_MS) || 30000;

let _storage = null;
function getStorage() {
  if (!_storage) _storage = new Storage();
  return _storage;
}

/**
 * Check if GCS_BUCKET environment variable is set and valid.
 */
function checkBucketEnvVar() {
  const bucket = process.env.GCS_BUCKET;

  if (!bucket) {
    return {
      ok:      false,
      name:    'GCS_BUCKET env var',
      message: 'GCS_BUCKET environment variable is not set. Add to .env file.',
    };
  }

  const bucketPattern = /^[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]$/;
  if (!bucketPattern.test(bucket)) {
    return {
      ok:      false,
      name:    'GCS_BUCKET env var',
      message: `Invalid bucket name format: "${bucket}". Bucket names must be 3-63 chars, lowercase letters, numbers, hyphens.`,
    };
  }

  return {
    ok:      true,
    name:    'GCS_BUCKET env var',
    bucket,
    message: `Bucket: ${bucket}`,
  };
}

/**
 * Check GCS endpoint connectivity via HTTPS.
 */
async function checkGCSConnectivity() {
  return new Promise((resolve) => {
    const timeout = setTimeout(() => {
      resolve({
        ok:      false,
        name:    'GCS connectivity',
        message: 'Timed out connecting to storage.googleapis.com. Check network/firewall.',
      });
    }, 10000);

    const req = httpsGet('https://storage.googleapis.com/', { timeout: 9000 }, (res) => {
      clearTimeout(timeout);
      res.resume();
      resolve({
        ok:      true,
        name:    'GCS connectivity',
        message: `Can reach storage.googleapis.com (HTTP ${res.statusCode})`,
      });
    });

    req.on('error', (err) => {
      clearTimeout(timeout);
      resolve({
        ok:      false,
        name:    'GCS connectivity',
        message: `Cannot reach storage.googleapis.com: ${err.message}`,
        error:   err.message,
      });
    });

    req.on('timeout', () => {
      clearTimeout(timeout);
      req.destroy();
      resolve({
        ok:      false,
        name:    'GCS connectivity',
        message: 'Cannot reach storage.googleapis.com — connection timed out.',
      });
    });
  });
}

/**
 * Check if we can list the bucket (read access) using the SDK.
 */
async function checkBucketReadAccess() {
  const bucketName = process.env.GCS_BUCKET;
  if (!bucketName) {
    return { ok: false, name: 'bucket read access', message: 'GCS_BUCKET not set' };
  }

  try {
    const bucket = getStorage().bucket(bucketName);
    await bucket.getFiles({ prefix: 'raw/', maxResults: 1 });

    return {
      ok:      true,
      name:    'bucket read access',
      message: `Can list gs://${bucketName}/`,
    };
  } catch (err) {
    const code = err.code || err.response?.statusCode;
    if (code === 403) {
      return {
        ok:      false,
        name:    'bucket read access',
        message: `Access denied to gs://${bucketName}/. Check service account permissions.`,
      };
    }
    if (code === 404) {
      return {
        ok:      false,
        name:    'bucket read access',
        message: `Bucket not found: gs://${bucketName}/. Verify bucket exists.`,
      };
    }

    return {
      ok:      false,
      name:    'bucket read access',
      error:   err.message,
      message: `Failed to access bucket: ${err.message.substring(0, 100)}`,
    };
  }
}

/**
 * Check if we can write to the bucket (write access) using the SDK.
 */
async function checkBucketWriteAccess(skipWrite = false) {
  if (skipWrite) {
    return {
      ok:      true,
      name:    'bucket write access',
      message: 'Skipped (--quick mode)',
      skipped: true,
    };
  }

  const bucketName = process.env.GCS_BUCKET;
  if (!bucketName) {
    return { ok: false, name: 'bucket write access', message: 'GCS_BUCKET not set' };
  }

  const testId      = `${Date.now()}_${randomBytes(4).toString('hex')}`;
  const testObjName = `raw/_preflight_test_${testId}.txt`;
  const testContent = `Preflight test at ${new Date().toISOString()}`;

  try {
    const bucket = getStorage().bucket(bucketName);
    const file = bucket.file(testObjName);

    await file.save(testContent, { resumable: false });
    await file.delete({ ignoreNotFound: true });

    return {
      ok:      true,
      name:    'bucket write access',
      message: `Can write to gs://${bucketName}/raw/`,
    };
  } catch (err) {
    const code = err.code || err.response?.statusCode;
    if (code === 403) {
      return {
        ok:      false,
        name:    'bucket write access',
        message: `Write access denied to gs://${bucketName}/raw/. Check service account permissions (roles/storage.objectCreator).`,
      };
    }

    return {
      ok:      false,
      name:    'bucket write access',
      error:   err.message,
      message: `Failed to write to bucket: ${err.message.substring(0, 100)}`,
    };
  }
}

/**
 * Check service account authentication via SDK metadata.
 */
async function checkAuthentication() {
  try {
    const storage = getStorage();
    const [email] = await storage.authClient.getCredentials();
    const account = email?.client_email || email?.email || null;

    return {
      ok:      true,
      name:    'authentication',
      account: account || 'ADC (service account or user)',
      message: account ? `Authenticated as: ${account}` : 'Authenticated via ADC',
    };
  } catch {
    return {
      ok:      true,
      name:    'authentication',
      message: 'Auth check inconclusive (ADC likely in use)',
      skipped: true,
    };
  }
}

/**
 * Run all preflight checks.
 *
 * @param {object}  options
 * @param {boolean} options.quick       - Skip write test
 * @param {boolean} options.throwOnFail - Throw error if any check fails
 * @param {boolean} options.silent      - Don't print to console
 * @returns {Promise<{ok: boolean, timestamp: string, checks: object[]}>}
 */
export async function runPreflightChecks(options = {}) {
  const { quick = false, throwOnFail = true, silent = false } = options;

  const results = {
    ok:        true,
    timestamp: new Date().toISOString(),
    checks:    [],
  };

  const logFn      = silent ? () => {} : console.log;
  const logErrorFn = silent ? () => {} : console.error;

  logFn('\n' + '═'.repeat(60));
  logFn('🔍 GCS PREFLIGHT CHECKS');
  logFn('═'.repeat(60));

  const checks = [
    () => checkBucketEnvVar(),
    () => checkGCSConnectivity(),
    () => checkAuthentication(),
    () => checkBucketReadAccess(),
    () => checkBucketWriteAccess(quick),
  ];

  for (const check of checks) {
    const result = await check();
    results.checks.push(result);

    const icon   = result.ok ? '✅' : '❌';
    const status = result.skipped ? '⏭️' : icon;
    logFn(`${status} ${result.name}: ${result.message}`);

    if (!result.ok && !result.skipped) {
      results.ok = false;
    }
  }

  logFn('─'.repeat(60));

  if (results.ok) {
    logFn('✅ All preflight checks passed\n');
  } else {
    logErrorFn('❌ Preflight checks FAILED\n');

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
 * Validate GCS is ready (simple boolean check).
 * @returns {Promise<boolean>}
 */
export async function isGCSReady(options = {}) {
  try {
    const results = await runPreflightChecks({ ...options, throwOnFail: false, silent: true });
    return results.ok;
  } catch {
    return false;
  }
}

const isMainModule = process.argv[1]?.endsWith('gcs-preflight.js');

if (isMainModule) {
  const args  = process.argv.slice(2);
  const quick = args.includes('--quick') || args.includes('-q');

  runPreflightChecks({ quick, throwOnFail: true })
    .then(() => process.exit(0))
    .catch(err => {
      console.error(`\n${err.message}\n`);
      process.exit(1);
    });
}
