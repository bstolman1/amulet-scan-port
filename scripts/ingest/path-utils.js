/**
 * Cross-Platform Path Utilities
 *
 * Provides consistent path handling across Windows and Linux environments.
 * Automatically detects platform mismatches and uses appropriate defaults.
 *
 * Includes support for:
 * - GCS (Google Cloud Storage) configuration via GCS_BUCKET env var
 * - /tmp/ledger_raw as ephemeral scratch space for GCP VMs
 *
 * FIXES APPLIED:
 *
 * FIX #1  LINUX_DEFAULT hardcoded developer home directory
 *         '/home/ben/ledger_data' was the fallback path when DATA_DIR is unset
 *         or contains a platform-mismatched path. On any production or CI system
 *         this path either does not exist (mkdirSync fails) or points to a random
 *         user's home directory. Replaced with '/var/lib/ledger_raw', a conventional
 *         system data directory. Callers are always expected to set DATA_DIR; the
 *         default is a last-resort safety net, not a production value.
 *
 * FIX #2  getGCSBucket() returns null instead of throwing
 *         getGCSBucket() called validateGCSBucket() with no arguments (required=false).
 *         When GCS_BUCKET is unset and GCS_ENABLED !== 'true', validateGCSBucket(false)
 *         returns null silently — directly contradicting the JSDoc that promised
 *         "@throws {Error} If GCS_BUCKET is not set". Any caller passing this null
 *         to a gsutil command or URL constructor got silent corruption.
 *         Fixed: getGCSBucket() now calls validateGCSBucket(true), which always
 *         throws when GCS_BUCKET is missing.
 *
 * FIX #3  TMP_DIR not configurable — hardcoded '/tmp/ledger_raw'
 *         The tmp scratch path was a module-level constant with no env override.
 *         On systems with a non-standard tmp mount, or in test environments where
 *         /tmp isolation is needed, there was no way to redirect it.
 *         Now reads from process.env.TMP_LEDGER_DIR at call time, falling back to
 *         '/tmp/ledger_raw'. All functions that referenced TMP_DIR now call
 *         getTmpDir() to get the (possibly overridden) value.
 *
 * FIX #4  ensureTmpDir creates TMP_DIR but not the 'raw' subdirectory
 *         getTmpRawDir() returns join(TMP_DIR, 'raw'). write-parquet.js calls
 *         ensureTmpDir() and then immediately uses getTmpRawDir() as its dataDir.
 *         ensureTmpDir() only created TMP_DIR itself — the 'raw' subdirectory was
 *         not created, relying on ensureDir() in write-parquet.js to fill the gap
 *         silently. ensureTmpDir() now creates both TMP_DIR and TMP_DIR/raw.
 *
 * FIX #5  normalizeCrossPlatform logs to console.warn unconditionally
 *         console.warn calls inside a utility function cannot be suppressed in
 *         tests or in callers that manage their own logging. Changed signature to
 *         accept an optional `logFn` (default: console.warn) so callers and tests
 *         can redirect or suppress path-mismatch warnings.
 *
 * FIX #6  Default export removed
 *         The default export duplicated all named exports as a plain object,
 *         consistent with the same fix applied across every module in this session.
 */

import { resolve, join } from 'path';
import { existsSync, mkdirSync } from 'fs';

// Platform detection
export const isWindows    = process.platform === 'win32';
export const isWindowsPath = (p) => /^[A-Za-z]:[\\/]/.test(p);
export const isLinuxPath   = (p) => !!p && p.startsWith('/');

// FIX #1: replaced '/home/ben/ledger_data' with a conventional system data directory.
// DATA_DIR should always be set explicitly in production. This default is a
// last-resort safety net — it will produce an obvious path rather than silently
// writing to a developer's home directory on a server where that user doesn't exist.
export const WIN_DEFAULT   = 'C:\\ledger_raw';
export const LINUX_DEFAULT = '/var/lib/ledger_raw';

// FIX #3: TMP_DIR is no longer a module-level constant — use getTmpDir() instead.
// Kept as a named export for backward compatibility, but callers should prefer getTmpDir().
/** @deprecated Use getTmpDir() to respect TMP_LEDGER_DIR env override */
export const TMP_DIR = '/tmp/ledger_raw';

/**
 * Normalize a path for the current platform.
 *
 * If running on Linux but given a Windows path (or vice versa),
 * falls back to the appropriate default for the current platform.
 *
 * FIX #5: accepts an optional logFn for testability — callers and tests can
 *   suppress or redirect the platform-mismatch warning.
 *
 * @param {string}   inputPath          - The path from environment or config
 * @param {string}   [customLinuxDefault] - Optional custom Linux default
 * @param {string}   [customWinDefault]   - Optional custom Windows default
 * @param {function} [logFn]              - Warning logger (default: console.warn)
 * @returns {string} - Normalized path for current platform
 */
export function normalizeCrossPlatform(
  inputPath,
  customLinuxDefault = LINUX_DEFAULT,
  customWinDefault   = WIN_DEFAULT,
  logFn              = console.warn,     // FIX #5: injectable for testing
) {
  if (!inputPath) {
    return isWindows ? customWinDefault : customLinuxDefault;
  }

  if (!isWindows && isWindowsPath(inputPath)) {
    logFn(`⚠️ [path-utils] Windows path detected on Linux: "${inputPath}"`);
    logFn(`   Using Linux default: "${customLinuxDefault}"`);
    return customLinuxDefault;
  }

  if (isWindows && isLinuxPath(inputPath)) {
    logFn(`⚠️ [path-utils] Linux path detected on Windows: "${inputPath}"`);
    logFn(`   Using Windows default: "${customWinDefault}"`);
    return customWinDefault;
  }

  return inputPath;
}

/**
 * Get the base data directory, with cross-platform normalisation.
 *
 * @param {string} [envVar] - Environment variable value (process.env.DATA_DIR)
 * @returns {string} - Resolved absolute path
 */
export function getBaseDataDir(envVar = process.env.DATA_DIR) {
  const normalized = normalizeCrossPlatform(envVar);
  return resolve(normalized);
}

/**
 * Get the raw data directory (DATA_DIR/raw).
 *
 * @param {string} [envVar] - Environment variable value (process.env.DATA_DIR)
 * @returns {string} - Resolved absolute path to raw directory
 */
export function getRawDir(envVar = process.env.DATA_DIR) {
  return join(getBaseDataDir(envVar), 'raw');
}

/**
 * Get the cursor directory, with cross-platform normalisation.
 *
 * @param {string} [cursorEnv] - CURSOR_DIR environment variable
 * @param {string} [dataEnv]   - DATA_DIR environment variable (fallback)
 * @returns {string} - Resolved absolute path to cursor directory
 */
export function getCursorDir(
  cursorEnv = process.env.CURSOR_DIR,
  dataEnv   = process.env.DATA_DIR,
) {
  if (cursorEnv) {
    return resolve(normalizeCrossPlatform(cursorEnv));
  }
  return join(getBaseDataDir(dataEnv), 'cursors');
}

/**
 * Convert a path to use forward slashes (for DuckDB SQL queries).
 *
 * @param {string} filePath - Path to normalise
 * @returns {string} - Path with forward slashes
 */
export function toDuckDBPath(filePath) {
  return filePath.replace(/\\/g, '/');
}

/**
 * Get the tmp directory for ephemeral scratch space.
 *
 * FIX #3: reads TMP_LEDGER_DIR env var at call time, falling back to
 *   '/tmp/ledger_raw'. This allows test environments and non-standard
 *   deployments to redirect the scratch path without code changes.
 *
 * @returns {string} - Path to tmp directory
 */
export function getTmpDir() {
  // FIX #3: env var override replaces the hardcoded module constant
  return process.env.TMP_LEDGER_DIR || '/tmp/ledger_raw';
}

/**
 * Get the tmp raw directory for Parquet files.
 *
 * @returns {string} - Path to <tmpDir>/raw directory
 */
export function getTmpRawDir() {
  return join(getTmpDir(), 'raw');
}

/**
 * Ensure the tmp directory AND its 'raw' subdirectory exist.
 *
 * FIX #4: previously only created TMP_DIR ('/tmp/ledger_raw'), not
 *   TMP_DIR/raw. write-parquet.js calls ensureTmpDir() then immediately uses
 *   getTmpRawDir() (which returns TMP_DIR/raw) as its dataDir. The 'raw'
 *   subdirectory was silently created later by ensureDir() — a latent failure
 *   mode if any code tried to stat or list the directory before the first write.
 *   Now creates both levels explicitly.
 */
export function ensureTmpDir() {
  const tmpDir    = getTmpDir();
  const tmpRawDir = getTmpRawDir();
  if (!existsSync(tmpDir))    mkdirSync(tmpDir,    { recursive: true });
  if (!existsSync(tmpRawDir)) mkdirSync(tmpRawDir, { recursive: true });
}

/**
 * Check if GCS uploads are enabled.
 *
 * GCS mode is enabled when:
 *   1. GCS_BUCKET is set, AND
 *   2. GCS_ENABLED is not explicitly 'false'
 *
 * Safe default: local disk (no bucket → no GCS).
 *
 * @returns {boolean}
 */
export function isGCSMode() {
  if (!process.env.GCS_BUCKET) return false;
  return process.env.GCS_ENABLED !== 'false';
}

/**
 * Validate GCS_BUCKET is set when GCS mode is required.
 *
 * @param {boolean} required - If true, throws when bucket is not set
 * @returns {string | null}  - Bucket name, or null if not set and not required
 * @throws {Error} If required=true (or GCS_ENABLED='true') and GCS_BUCKET is not set
 */
export function validateGCSBucket(required = false) {
  if (!process.env.GCS_BUCKET) {
    if (required || process.env.GCS_ENABLED === 'true') {
      throw new Error(
        'GCS_BUCKET environment variable is required but not set.\n' +
        'Set GCS_BUCKET=your-bucket-name in your .env file.\n' +
        'Or remove GCS_ENABLED=true to use local disk instead.'
      );
    }
    return null;
  }
  return process.env.GCS_BUCKET;
}

/**
 * Get the GCS bucket name.
 *
 * FIX #2: now calls validateGCSBucket(true) so it always throws when
 *   GCS_BUCKET is not set, matching the JSDoc contract. Previously it called
 *   validateGCSBucket() (required=false), which silently returned null —
 *   directly contradicting "@throws {Error} If GCS_BUCKET is not set".
 *
 * @returns {string} Bucket name
 * @throws {Error}   If GCS_BUCKET is not set
 */
export function getGCSBucket() {
  // FIX #2: required=true — always throws on missing bucket
  return validateGCSBucket(true);
}

/**
 * Log the current path configuration (for debugging).
 */
export function logPathConfig(moduleName = 'path-utils') {
  console.log(`📂 [${moduleName}] Platform: ${isWindows ? 'Windows' : 'Linux'}`);
  console.log(`📂 [${moduleName}] DATA_DIR env: ${process.env.DATA_DIR || '(not set)'}`);
  console.log(`📂 [${moduleName}] Base data dir: ${getBaseDataDir()}`);
  console.log(`📂 [${moduleName}] Raw dir: ${getRawDir()}`);
  console.log(`📂 [${moduleName}] Cursor dir: ${getCursorDir()}`);
  console.log(`📂 [${moduleName}] GCS_BUCKET: ${process.env.GCS_BUCKET || '(not set - local mode)'}`);
  console.log(`📂 [${moduleName}] GCS_ENABLED: ${process.env.GCS_ENABLED || '(not set)'}`);
  if (isGCSMode()) {
    console.log(`📂 [${moduleName}] Mode: Write to /tmp → upload to GCS → delete local`);
  } else {
    console.log(`📂 [${moduleName}] Mode: Write to DATA_DIR (local disk only)`);
  }
}

// FIX #6: Default export removed — consistent with every other module fixed in
// this session. Named exports are the canonical interface.
// Use: import { getBaseDataDir, isGCSMode, ... } from './path-utils.js';
