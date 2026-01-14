/**
 * Cross-Platform Path Utilities
 * 
 * Provides consistent path handling across Windows and Linux environments.
 * Automatically detects platform mismatches and uses appropriate defaults.
 */

import { resolve, join } from 'path';

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
 * Log the current path configuration (for debugging)
 */
export function logPathConfig(moduleName = 'path-utils') {
  console.log(`📂 [${moduleName}] Platform: ${isWindows ? 'Windows' : 'Linux'}`);
  console.log(`📂 [${moduleName}] DATA_DIR env: ${process.env.DATA_DIR || '(not set)'}`);
  console.log(`📂 [${moduleName}] Base data dir: ${getBaseDataDir()}`);
  console.log(`📂 [${moduleName}] Raw dir: ${getRawDir()}`);
  console.log(`📂 [${moduleName}] Cursor dir: ${getCursorDir()}`);
}

export default {
  isWindows,
  isWindowsPath,
  isLinuxPath,
  WIN_DEFAULT,
  LINUX_DEFAULT,
  normalizeCrossPlatform,
  getBaseDataDir,
  getRawDir,
  getCursorDir,
  toDuckDBPath,
  logPathConfig,
};
