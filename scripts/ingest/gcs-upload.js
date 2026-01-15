/**
 * GCS Upload Module
 * 
 * Handles uploading Parquet files from /tmp to Google Cloud Storage.
 * Uses gsutil for uploads (relies on VM service account authentication).
 * 
 * Key behaviors:
 * - Writes to /tmp/ledger_raw as ephemeral scratch space
 * - Uploads each file immediately to GCS after creation
 * - Deletes local file after upload (even on failure)
 * - Configured via GCS_BUCKET environment variable
 * 
 * Requirements:
 * - gsutil CLI available in PATH
 * - VM has service account with GCS write permissions
 * - GCS_BUCKET environment variable set
 */

import { execSync } from 'child_process';
import { existsSync, mkdirSync, unlinkSync } from 'fs';
import path from 'path';

// Constants
const TMP_DIR = '/tmp/ledger_raw';

// GCS bucket configuration (fail fast if not set)
let GCS_BUCKET = null;
let gcsEnabled = false;

/**
 * Initialize GCS configuration.
 * Call this before any uploads. Will throw if GCS_BUCKET not set.
 */
export function initGCS() {
  GCS_BUCKET = process.env.GCS_BUCKET;
  
  if (!GCS_BUCKET) {
    throw new Error(
      'GCS_BUCKET environment variable not set. ' +
      'Set GCS_BUCKET to enable GCS uploads, or run with GCS_ENABLED=false for local-only mode.'
    );
  }
  
  gcsEnabled = true;
  console.log(`☁️ [gcs-upload] GCS enabled, bucket: ${GCS_BUCKET}`);
  console.log(`☁️ [gcs-upload] Tmp directory: ${TMP_DIR}`);
  
  return GCS_BUCKET;
}

/**
 * Check if GCS uploads are enabled.
 */
export function isGCSEnabled() {
  // Allow explicit disable via environment
  if (process.env.GCS_ENABLED === 'false') {
    return false;
  }
  return gcsEnabled || !!process.env.GCS_BUCKET;
}

/**
 * Get the GCS bucket name.
 * @returns {string|null} Bucket name or null if not configured
 */
export function getGCSBucket() {
  return GCS_BUCKET || process.env.GCS_BUCKET || null;
}

/**
 * Ensure the tmp directory exists.
 */
export function ensureTmpDir() {
  if (!existsSync(TMP_DIR)) {
    mkdirSync(TMP_DIR, { recursive: true });
    console.log(`📁 [gcs-upload] Created tmp directory: ${TMP_DIR}`);
  }
}

/**
 * Get a local tmp path for a file.
 * 
 * @param {string} relativePath - Relative path within the data structure
 * @returns {string} Full path in /tmp/ledger_raw/
 */
export function getTmpPath(relativePath) {
  ensureTmpDir();
  const fullPath = path.join(TMP_DIR, relativePath);
  
  // Ensure parent directory exists
  const parentDir = path.dirname(fullPath);
  if (!existsSync(parentDir)) {
    mkdirSync(parentDir, { recursive: true });
  }
  
  return fullPath;
}

/**
 * Get the GCS path for a file.
 * 
 * @param {string} relativePath - Relative path within the data structure
 * @returns {string} GCS URI (gs://bucket/path)
 */
export function getGCSPath(relativePath) {
  const bucket = getGCSBucket();
  if (!bucket) {
    throw new Error('GCS_BUCKET not configured');
  }
  
  // Normalize path separators to forward slashes
  const normalized = relativePath.replace(/\\/g, '/');
  
  // Prefix with 'raw/' if not already present
  const prefix = normalized.startsWith('raw/') ? '' : 'raw/';
  
  return `gs://${bucket}/${prefix}${normalized}`;
}

/**
 * Upload a file to GCS and delete the local copy.
 * 
 * Uses gsutil for upload (relies on VM service account).
 * Always deletes local file after upload attempt (success or failure).
 * 
 * @param {string} localPath - Full path to local file
 * @param {string} gcsPath - GCS URI (gs://bucket/path)
 * @param {object} [options] - Upload options
 * @param {number} [options.timeout=300000] - Timeout in ms (default 5 minutes)
 * @param {boolean} [options.quiet=false] - Suppress gsutil output
 * @returns {object} Upload result { ok, localPath, gcsPath, bytes?, error? }
 */
export function uploadAndCleanup(localPath, gcsPath, options = {}) {
  const { timeout = 300000, quiet = false } = options;
  
  const result = {
    ok: false,
    localPath,
    gcsPath,
    bytes: 0,
    error: null,
  };
  
  try {
    // Verify local file exists
    if (!existsSync(localPath)) {
      throw new Error(`Local file not found: ${localPath}`);
    }
    
    // Get file size before upload
    const fs = await import('fs');
    const stats = fs.statSync(localPath);
    result.bytes = stats.size;
    
    // Build gsutil command
    const gsutilArgs = quiet ? '-q' : '';
    const cmd = `gsutil ${gsutilArgs} cp "${localPath}" "${gcsPath}"`;
    
    // Execute upload
    execSync(cmd, {
      stdio: quiet ? 'pipe' : 'inherit',
      timeout,
      encoding: 'utf8',
    });
    
    result.ok = true;
    console.log(`☁️ Uploaded ${path.basename(localPath)} to ${gcsPath} (${(result.bytes / 1024).toFixed(1)}KB)`);
    
  } catch (err) {
    result.error = err.message;
    console.error(`❌ [gcs-upload] Failed to upload ${path.basename(localPath)}: ${err.message}`);
    
  } finally {
    // ALWAYS delete local file to prevent disk accumulation
    if (existsSync(localPath)) {
      try {
        unlinkSync(localPath);
        console.log(`🗑️ Deleted local file: ${path.basename(localPath)}`);
      } catch (deleteErr) {
        console.error(`❌ [gcs-upload] Failed to delete ${localPath}: ${deleteErr.message}`);
      }
    }
  }
  
  return result;
}

/**
 * Synchronous version of uploadAndCleanup for use in workers.
 * Uses require() to avoid top-level await issues.
 */
export function uploadAndCleanupSync(localPath, gcsPath, options = {}) {
  const { timeout = 300000, quiet = false } = options;
  const fs = require('fs');
  
  const result = {
    ok: false,
    localPath,
    gcsPath,
    bytes: 0,
    error: null,
  };
  
  try {
    // Verify local file exists
    if (!fs.existsSync(localPath)) {
      throw new Error(`Local file not found: ${localPath}`);
    }
    
    // Get file size before upload
    const stats = fs.statSync(localPath);
    result.bytes = stats.size;
    
    // Build gsutil command
    const gsutilArgs = quiet ? '-q' : '';
    const cmd = `gsutil ${gsutilArgs} cp "${localPath}" "${gcsPath}"`;
    
    // Execute upload
    execSync(cmd, {
      stdio: quiet ? 'pipe' : 'inherit',
      timeout,
      encoding: 'utf8',
    });
    
    result.ok = true;
    console.log(`☁️ Uploaded ${path.basename(localPath)} to ${gcsPath} (${(result.bytes / 1024).toFixed(1)}KB)`);
    
  } catch (err) {
    result.error = err.message;
    console.error(`❌ [gcs-upload] Failed to upload ${path.basename(localPath)}: ${err.message}`);
    
  } finally {
    // ALWAYS delete local file to prevent disk accumulation
    if (fs.existsSync(localPath)) {
      try {
        fs.unlinkSync(localPath);
        console.log(`🗑️ Deleted local file: ${path.basename(localPath)}`);
      } catch (deleteErr) {
        console.error(`❌ [gcs-upload] Failed to delete ${localPath}: ${deleteErr.message}`);
      }
    }
  }
  
  return result;
}

/**
 * Get the tmp directory path.
 */
export function getTmpDir() {
  return TMP_DIR;
}

// Stats tracking
let uploadStats = {
  totalUploads: 0,
  successfulUploads: 0,
  failedUploads: 0,
  totalBytesUploaded: 0,
};

/**
 * Get upload statistics.
 */
export function getUploadStats() {
  return { ...uploadStats };
}

/**
 * Reset upload statistics.
 */
export function resetUploadStats() {
  uploadStats = {
    totalUploads: 0,
    successfulUploads: 0,
    failedUploads: 0,
    totalBytesUploaded: 0,
  };
}

export default {
  TMP_DIR,
  initGCS,
  isGCSEnabled,
  getGCSBucket,
  ensureTmpDir,
  getTmpPath,
  getTmpDir,
  getGCSPath,
  uploadAndCleanup,
  uploadAndCleanupSync,
  getUploadStats,
  resetUploadStats,
};
