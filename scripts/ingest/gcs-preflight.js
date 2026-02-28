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
 *
 * FIXES APPLIED:
 *
 * FIX #1  All execSync calls replaced with execFileAsync
 *         execSync blocked the event loop for up to PREFLIGHT_TIMEOUT_MS (30s)
 *         per check. Every check function is now async.
 *
 * FIX #2  checkBucketWriteAccess: shell injection removed
 *         Old: execSync(`echo "${testContent}" | gsutil cp - ...`)
 *         The echo|pipe pattern goes through a shell and can misbehave if
 *         testContent contains shell-special characters. New: write content
 *         to a local temp file first, then upload with execFileAsync (no shell).
 *
 * FIX #3  checkGCSConnectivity: curl replaced with Node https.get
 *         curl may not be installed in container environments. A pure-Node
 *         connectivity check is more portable and doesn't need execSync/exec.
 *
 * FIX #4  checkBucketReadAccess: shell pipeline removed
 *         Old: execSync(`gsutil ls "..." 2>&1 | head -5`)
 *         Pipeline requires a shell. New: execFileAsync(['gsutil','ls',...])
 *         and truncate output in JS.
 *
 * FIX #5  checkAuthentication: execSync replaced with execFileAsync
 *         gcloud is optional; failure is still handled gracefully.
 *
 * FIX #6  runPreflightChecks made async
 *         With all check functions now async, the loop must await each result.
 *         Previously `const result = check()` would have returned a Promise
 *         object rather than the check result.
 *
 * FIX #7  isGCSReady made async
 *         It calls the now-async runPreflightChecks; callers must await it.
 *
 * FIX #8  CLI entry point awaits runPreflightChecks
 *         The top-level call was not awaited; with async functions it would
 *         exit before checks completed.
 *
 * FIX #9  checkBucketWriteAccess cleanup failure is logged
 *         Previously the catch block was empty and swallowed cleanup errors,
 *         leaving orphaned test objects in GCS silently.
 */

import { promisify } from 'util';
import { execFile as execFileCb } from 'child_process';
import { randomBytes } from 'crypto';
import { writeFileSync, unlinkSync, existsSync } from 'fs';
import { get as httpsGet } from 'https';
import { tmpdir } from 'os';
import { join } from 'path';

// FIX #1: promisified execFile — async, no shell, no injection
const execFileAsync = promisify(execFileCb);

// ─────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────

const PREFLIGHT_TIMEOUT_MS = parseInt(process.env.PREFLIGHT_TIMEOUT_MS) || 30000;

// ─────────────────────────────────────────────────────────────
// Check Functions (all async — FIX #1)
// ─────────────────────────────────────────────────────────────

/**
 * Check if gsutil is installed and accessible.
 *
 * FIX #1: execSync → execFileAsync.
 */
async function checkGsutilInstalled() {
  try {
    // FIX #1: execFileAsync — no shell, no event loop block
    const { stdout } = await execFileAsync('gsutil', ['version'], { timeout: 10000 });

    const versionMatch = stdout.match(/gsutil version:\s*([\d.]+)/);
    const version = versionMatch ? versionMatch[1] : 'unknown';

    return {
      ok:      true,
      name:    'gsutil installed',
      version,
      message: `gsutil version ${version}`,
    };
  } catch (err) {
    return {
      ok:      false,
      name:    'gsutil installed',
      error:   err.message,
      message: 'gsutil not found. Install Google Cloud SDK: https://cloud.google.com/sdk/docs/install',
    };
  }
}

/**
 * Check if GCS_BUCKET environment variable is set.
 * Synchronous — no I/O needed.
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
 * Check if we can list the bucket (read access).
 *
 * FIX #1: execSync → execFileAsync.
 * FIX #4: Shell pipeline (`gsutil ls ... | head -5`) replaced with
 *   execFileAsync + JS truncation. Pipelines require a shell and are
 *   not portable across all container environments.
 */
async function checkBucketReadAccess() {
  const bucket = process.env.GCS_BUCKET;
  if (!bucket) {
    return { ok: false, name: 'bucket read access', message: 'GCS_BUCKET not set' };
  }

  try {
    // FIX #4: no shell — execFileAsync with args array
    await execFileAsync(
      'gsutil', ['ls', `gs://${bucket}/`],
      { timeout: PREFLIGHT_TIMEOUT_MS }
    );

    return {
      ok:      true,
      name:    'bucket read access',
      message: `Can list gs://${bucket}/`,
    };
  } catch (err) {
    const errorMsg = err.stderr?.toString() || err.message || '';

    if (errorMsg.includes('AccessDenied') || errorMsg.includes('403')) {
      return {
        ok:      false,
        name:    'bucket read access',
        message: `Access denied to gs://${bucket}/. Check service account permissions.`,
      };
    }
    if (errorMsg.includes('BucketNotFound') || errorMsg.includes('404')) {
      return {
        ok:      false,
        name:    'bucket read access',
        message: `Bucket not found: gs://${bucket}/. Verify bucket exists.`,
      };
    }
    if (errorMsg.includes('timeout') || errorMsg.includes('ETIMEDOUT')) {
      return {
        ok:      false,
        name:    'bucket read access',
        message: 'Network timeout connecting to GCS. Check firewall/proxy settings.',
      };
    }

    return {
      ok:      false,
      name:    'bucket read access',
      error:   errorMsg,
      message: `Failed to access bucket: ${errorMsg.substring(0, 100)}`,
    };
  }
}

/**
 * Check if we can write to the bucket (write access).
 *
 * FIX #1: execSync → execFileAsync.
 * FIX #2: Shell injection via `echo "${testContent}" | gsutil cp - ...` removed.
 *   testContent is safe today, but the echo|pipe pattern is brittle (any shell-
 *   special character in the string silently corrupts it or causes an error).
 *   New approach: write content to a local temp file, then upload it — no shell.
 * FIX #9: Cleanup failure is now logged rather than silently swallowed.
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

  const bucket = process.env.GCS_BUCKET;
  if (!bucket) {
    return { ok: false, name: 'bucket write access', message: 'GCS_BUCKET not set' };
  }

  const testId       = `${Date.now()}_${randomBytes(4).toString('hex')}`;
  const testFileName = `_preflight_test_${testId}.txt`;
  const testGcsPath  = `gs://${bucket}/raw/${testFileName}`;
  const testContent  = `Preflight test at ${new Date().toISOString()}`;
  // FIX #2: write to a local temp file so we never need a shell pipeline
  const tmpFile = join(tmpdir(), testFileName);

  try {
    writeFileSync(tmpFile, testContent, 'utf8');

    // FIX #1/#2: execFileAsync — no shell, testContent never touches the shell
    await execFileAsync(
      'gsutil', ['cp', tmpFile, testGcsPath],
      { timeout: PREFLIGHT_TIMEOUT_MS }
    );

    // Verify it exists
    await execFileAsync(
      'gsutil', ['stat', testGcsPath],
      { timeout: 10000 }
    );

    return {
      ok:      true,
      name:    'bucket write access',
      message: `Can write to gs://${bucket}/raw/`,
    };
  } catch (err) {
    const errorMsg = err.stderr?.toString() || err.message || '';

    if (errorMsg.includes('AccessDenied') || errorMsg.includes('403')) {
      return {
        ok:      false,
        name:    'bucket write access',
        message: `Write access denied to gs://${bucket}/raw/. Check service account permissions (roles/storage.objectCreator).`,
      };
    }

    return {
      ok:      false,
      name:    'bucket write access',
      error:   errorMsg,
      message: `Failed to write to bucket: ${errorMsg.substring(0, 100)}`,
    };
  } finally {
    // Clean up local temp file regardless of outcome
    try { if (existsSync(tmpFile)) unlinkSync(tmpFile); } catch {}

    // Clean up GCS test object
    // FIX #9: log cleanup failures rather than swallowing them silently
    try {
      await execFileAsync('gsutil', ['rm', testGcsPath], { timeout: 10000 });
    } catch (cleanupErr) {
      // Cleanup failure is non-fatal but should be visible to operators
      console.warn(`  ⚠️ Failed to remove preflight test object ${testGcsPath}: ${cleanupErr.message}`);
    }
  }
}

/**
 * Check GCS endpoint connectivity.
 *
 * FIX #3: Replaced execSync(`curl ...`) with a pure-Node https.get call.
 *   curl may not be installed in container environments (distroless images,
 *   minimal Alpine builds, etc.). Node's built-in https module is always
 *   available and does not require a shell.
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
      res.resume(); // consume response body so socket is released
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
 * Check service account authentication.
 *
 * FIX #1/#5: execSync → execFileAsync.
 *   gcloud is optional; failure is still handled gracefully — the function
 *   returns ok:true with a note when gcloud is unavailable (VM service account).
 */
async function checkAuthentication() {
  try {
    // FIX #5: execFileAsync — async, no shell
    const { stdout } = await execFileAsync(
      'gcloud',
      ['auth', 'list', '--filter=status:ACTIVE', '--format=value(account)'],
      { timeout: 10000 }
    );

    const account = stdout.trim().split('\n')[0];

    if (!account) {
      return {
        ok:      false,
        name:    'authentication',
        message: 'No active gcloud account. Run: gcloud auth login',
      };
    }

    return {
      ok:      true,
      name:    'authentication',
      account,
      message: `Authenticated as: ${account}`,
    };
  } catch {
    // gcloud not installed — VM service account is likely in use via metadata server
    return {
      ok:      true,
      name:    'authentication',
      message: 'gcloud not available (VM service account likely in use)',
      skipped: true,
    };
  }
}

// ─────────────────────────────────────────────────────────────
// Main Preflight Function
// ─────────────────────────────────────────────────────────────

/**
 * Run all preflight checks.
 *
 * FIX #6: Now async — check functions are async so the loop must await each
 *   result. Previously `const result = check()` would have stored a Promise
 *   object in results.checks and logged "[object Promise]" as the check message.
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

  // FIX #6: checks array contains async functions — each must be awaited
  const checks = [
    () => checkGsutilInstalled(),
    () => checkBucketEnvVar(),       // sync — still works fine in an async context
    () => checkGCSConnectivity(),
    () => checkAuthentication(),
    () => checkBucketReadAccess(),
    () => checkBucketWriteAccess(quick),
  ];

  for (const check of checks) {
    // FIX #6: await the async check so result is the resolved value, not a Promise
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
 *
 * FIX #7: Now async — must be awaited by callers.
 *   Previously it called the async runPreflightChecks without await,
 *   so results.ok was always undefined (Promise, not boolean).
 *
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

// ─────────────────────────────────────────────────────────────
// CLI Entry Point
// ─────────────────────────────────────────────────────────────

const isMainModule = process.argv[1]?.endsWith('gcs-preflight.js');

if (isMainModule) {
  const args  = process.argv.slice(2);
  const quick = args.includes('--quick') || args.includes('-q');

  // FIX #8: await the now-async runPreflightChecks so the process does not
  // exit before checks complete. Without await, the Promise is fire-and-forget
  // and process.exit(0) runs immediately.
  runPreflightChecks({ quick, throwOnFail: true })
    .then(() => process.exit(0))
    .catch(err => {
      console.error(`\n${err.message}\n`);
      process.exit(1);
    });
}
