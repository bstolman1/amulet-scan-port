/**
 * Cross-Platform Path Utilities
 * 
 * Provides consistent path handling across Windows and Linux environments.
 * Automatically detects platform mismatches and uses appropriate defaults.
 * 
 * Includes support for:
 * - GCS (Google Cloud Storage) configuration via GCS_BUCKET env var
 * - /tmp/ledger_raw as ephemeral scratch space for GCP VMs
 */

import { resolve, join } from 'path';
import { existsSync, mkdirSync } from 'fs';

// Tmp directory for ephemeral scratch space (GCP VMs)
export const TMP_DIR = '/tmp/ledger_raw';

// Platform detection
export const isWindows = process.platform === 'win32';
export const isWindowsPath = (p) => /^[A-Za-z]:[\\/]/.test(p);
export const isLinuxPath = (p) => p && p.startsWith('/');

// Default paths per platform
export const WIN_DEFAULT = 'C:\\ledger_raw';
export const LINUX_DEFAULT = '/home/ben/ledger_data';

/**
 * Normalize a path for the current platform.
 * 
 * If running on Linux but given a Windows path (or vice versa),
 * falls back to the appropriate default for the current platform.
 * 
 * @param {string} inputPath - The path from environment or config
 * @param {string} [customLinuxDefault] - Optional custom Linux default
 * @param {string} [customWinDefault] - Optional custom Windows default
 * @returns {string} - Normalized path for current platform
 */
export function normalizeCrossPlatform(inputPath, customLinuxDefault = LINUX_DEFAULT, customWinDefault = WIN_DEFAULT) {
  if (!inputPath) {
    return isWindows ? customWinDefault : customLinuxDefault;
  }
  
  // If we're on Linux but got a Windows path, use Linux default
  if (!isWindows && isWindowsPath(inputPath)) {
    console.warn(`⚠️ [path-utils] Windows path detected on Linux: "${inputPath}"`);
    console.warn(`   Using Linux default: "${customLinuxDefault}"`);
    return customLinuxDefault;
  }
  
  // If we're on Windows but got a Linux path, use Windows default
  if (isWindows && isLinuxPath(inputPath)) {
    console.warn(`⚠️ [path-utils] Linux path detected on Windows: "${inputPath}"`);
    console.warn(`   Using Windows default: "${customWinDefault}"`);
    return customWinDefault;
  }
  
  return inputPath;
}

/**
 * Get the base data directory, with cross-platform normalization.
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
 * Get the cursor directory, with cross-platform normalization.
 * 
 * @param {string} [cursorEnv] - CURSOR_DIR environment variable
 * @param {string} [dataEnv] - DATA_DIR environment variable (fallback)
 * @returns {string} - Resolved absolute path to cursor directory
 */
export function getCursorDir(cursorEnv = process.env.CURSOR_DIR, dataEnv = process.env.DATA_DIR) {
  if (cursorEnv) {
    return resolve(normalizeCrossPlatform(cursorEnv));
  }
  return join(getBaseDataDir(dataEnv), 'cursors');
}

/**
 * Convert a path to use forward slashes (for DuckDB SQL queries).
 * 
 * @param {string} filePath - Path to normalize
 * @returns {string} - Path with forward slashes
 */
export function toDuckDBPath(filePath) {
  return filePath.replace(/\\/g, '/');
}

/**
 * Get the tmp directory for ephemeral scratch space.
 * Used on GCP VMs where data is written to /tmp then uploaded to GCS.
 * 
 * @returns {string} - Path to tmp directory
 */
export function getTmpDir() {
  return TMP_DIR;
}

/**
 * Get the tmp raw directory for Parquet files.
 * 
 * @returns {string} - Path to tmp/raw directory
 */
export function getTmpRawDir() {
  return join(TMP_DIR, 'raw');
}

/**
 * Ensure the tmp directory exists.
 */
export function ensureTmpDir() {
  if (!existsSync(TMP_DIR)) {
    mkdirSync(TMP_DIR, { recursive: true });
  }
}

/**
 * Check if GCS uploads are enabled.
 * GCS_BUCKET must always be set. GCS_ENABLED controls whether to upload.
 * 
 * @returns {boolean} - True if GCS uploads are enabled
 */
export function isGCSMode() {
  // GCS_ENABLED defaults to true, set to 'false' to write to disk only
  return process.env.GCS_ENABLED !== 'false';
}

/**
 * Validate that GCS_BUCKET is always set (required).
 * Call at startup to fail fast.
 * 
 * @throws {Error} If GCS_BUCKET is not set
 */
export function validateGCSBucket() {
  if (!process.env.GCS_BUCKET) {
    throw new Error(
      'GCS_BUCKET environment variable is required but not set.\n' +
      'Set GCS_BUCKET=your-bucket-name in your .env file.\n' +
      'Use GCS_ENABLED=false to write to local disk instead of uploading.'
    );
  }
  return process.env.GCS_BUCKET;
}

/**
 * Get the GCS bucket name.
 * @returns {string} Bucket name
 * @throws {Error} If GCS_BUCKET is not set
 */
export function getGCSBucket() {
  return validateGCSBucket();
}

/**
 * Log the current path configuration (for debugging)
 */
export function logPathConfig(moduleName = 'path-utils') {
  console.log(`📂 [${moduleName}] Platform: ${isWindows ? 'Windows' : 'Linux'}`);
  console.log(`📂 [${moduleName}] DATA_DIR env: ${process.env.DATA_DIR || '(not set)'}`);
  console.log(`📂 [${moduleName}] Base data dir: ${getBaseDataDir()}`);
  console.log(`📂 [${moduleName}] Raw dir: ${getRawDir()}`);
  console.log(`📂 [${moduleName}] Cursor dir: ${getCursorDir()}`);
  console.log(`📂 [${moduleName}] GCS_BUCKET: ${process.env.GCS_BUCKET || '(NOT SET - REQUIRED)'}`);
  console.log(`📂 [${moduleName}] GCS_ENABLED: ${process.env.GCS_ENABLED !== 'false' ? 'true (uploading to GCS)' : 'false (disk only)'}`);
  if (isGCSMode()) {
    console.log(`📂 [${moduleName}] Mode: Write to /tmp → upload to GCS → delete local`);
  } else {
    console.log(`📂 [${moduleName}] Mode: Write to DATA_DIR (no GCS upload)`);
  }
}

export default {
  isWindows,
  isWindowsPath,
  isLinuxPath,
  WIN_DEFAULT,
  LINUX_DEFAULT,
  TMP_DIR,
  normalizeCrossPlatform,
  getBaseDataDir,
  getRawDir,
  getCursorDir,
  getTmpDir,
  getTmpRawDir,
  ensureTmpDir,
  isGCSMode,
  validateGCSBucket,
  getGCSBucket,
  toDuckDBPath,
  logPathConfig,
};
