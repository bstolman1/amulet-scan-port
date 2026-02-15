/**
 * GCS Upload Module with Retry Logic
 * 
 * Handles uploading Parquet files from /tmp to Google Cloud Storage.
 * Uses gsutil for uploads (relies on VM service account authentication).
 * 
 * Features:
 * - Exponential backoff retry for transient failures
 * - Configurable retry attempts and delays
 * - Writes to /tmp/ledger_raw as ephemeral scratch space
 * - Uploads each file immediately to GCS after creation
 * - Deletes local file after upload (even on failure after all retries)
 * - Configured via GCS_BUCKET environment variable
 * 
 * Requirements:
 * - gsutil CLI available in PATH
 * - VM has service account with GCS write permissions
 * - GCS_BUCKET environment variable set
 */

import { execSync } from 'child_process';
import { existsSync, mkdirSync, unlinkSync, statSync } from 'fs';
import path from 'path';

// Constants
const TMP_DIR = '/tmp/ledger_raw';

// Retry configuration (can be overridden via environment)
const DEFAULT_MAX_RETRIES = parseInt(process.env.GCS_MAX_RETRIES) || 3;
const DEFAULT_BASE_DELAY_MS = parseInt(process.env.GCS_RETRY_BASE_DELAY_MS) || 1000;
const DEFAULT_MAX_DELAY_MS = parseInt(process.env.GCS_RETRY_MAX_DELAY_MS) || 30000;

// Transient error patterns that should trigger retry
const TRANSIENT_ERROR_PATTERNS = [
  /timeout/i,
  /timed out/i,
  /connection reset/i,
  /connection refused/i,
  /network unreachable/i,
  /temporary failure/i,
  /service unavailable/i,
  /503/,
  /502/,
  /500/,
  /ECONNRESET/,
  /ETIMEDOUT/,
  /ENOTFOUND/,
  /ENETUNREACH/,
  /socket hang up/i,
  /rate limit/i,
  /too many requests/i,
  /429/,
  /try again/i,
  /retryable/i,
];

// GCS bucket configuration
let GCS_BUCKET = null;

/**
 * Check if an error is transient and should be retried.
 * 
 * @param {string} errorMessage - The error message to check
 * @returns {boolean} True if the error is transient
 */
function isTransientError(errorMessage) {
  if (!errorMessage) return false;
  return TRANSIENT_ERROR_PATTERNS.some(pattern => pattern.test(errorMessage));
}

/**
 * Calculate delay with exponential backoff and jitter.
 * 
 * @param {number} attempt - Current attempt number (0-indexed)
 * @param {number} baseDelay - Base delay in milliseconds
 * @param {number} maxDelay - Maximum delay in milliseconds
 * @returns {number} Delay in milliseconds
 */
function calculateBackoffDelay(attempt, baseDelay = DEFAULT_BASE_DELAY_MS, maxDelay = DEFAULT_MAX_DELAY_MS) {
  // Exponential backoff: baseDelay * 2^attempt
  const exponentialDelay = baseDelay * Math.pow(2, attempt);
  
  // Add jitter (±25% randomness) to prevent thundering herd
  const jitter = exponentialDelay * 0.25 * (Math.random() * 2 - 1);
  
  // Clamp to maxDelay
  return Math.min(exponentialDelay + jitter, maxDelay);
}

/**
 * Sleep for a specified duration.
 * 
 * @param {number} ms - Duration in milliseconds
 */
function sleep(ms) {
  // Use Atomics.wait for synchronous sleep without busy-waiting the CPU.
  // SharedArrayBuffer + Atomics.wait blocks the thread for the specified
  // duration without spinning, keeping the function synchronous (required
  // by uploadAndCleanupSync) while not pinning the CPU at 100%.
  const sharedBuf = new SharedArrayBuffer(4);
  const view = new Int32Array(sharedBuf);
  Atomics.wait(view, 0, 0, ms);
}

/**
 * Initialize GCS configuration.
 * Only required when GCS mode is enabled.
 * 
 * @param {boolean} required - If true, throws when bucket is not set
 * @returns {string|null} Bucket name or null if not configured
 * @throws {Error} If required=true and GCS_BUCKET is not set
 */
export function initGCS(required = false) {
  GCS_BUCKET = process.env.GCS_BUCKET;
  
  if (!GCS_BUCKET) {
    if (required || process.env.GCS_ENABLED === 'true') {
      throw new Error(
        'GCS_BUCKET environment variable is required but not set.\n' +
        'Set GCS_BUCKET=your-bucket-name in your .env file.\n' +
        'Or remove GCS_ENABLED=true to use local disk instead.'
      );
    }
    console.log(`📂 [gcs-upload] GCS_BUCKET not set - using local disk mode`);
    return null;
  }
  
  const gcsEnabled = isGCSEnabled();
  
  console.log(`☁️ [gcs-upload] GCS_BUCKET: ${GCS_BUCKET}`);
  console.log(`☁️ [gcs-upload] GCS_ENABLED: ${gcsEnabled}`);
  
  if (gcsEnabled) {
    console.log(`☁️ [gcs-upload] Mode: Write to /tmp → upload to GCS → delete local`);
    console.log(`☁️ [gcs-upload] Tmp directory: ${TMP_DIR}`);
    console.log(`☁️ [gcs-upload] Retry config: max ${DEFAULT_MAX_RETRIES} retries, base delay ${DEFAULT_BASE_DELAY_MS}ms`);
  } else {
    console.log(`☁️ [gcs-upload] Mode: Write to DATA_DIR (no GCS upload)`);
  }
  
  return GCS_BUCKET;
}

/**
 * Check if GCS uploads are enabled.
 * GCS is enabled when GCS_BUCKET is set AND GCS_ENABLED is not 'false'.
 * 
 * @returns {boolean} True if GCS uploads should happen
 */
export function isGCSEnabled() {
  // Require GCS_BUCKET to be set for GCS mode
  if (!process.env.GCS_BUCKET) {
    return false;
  }
  return process.env.GCS_ENABLED !== 'false';
}

/**
 * Get the GCS bucket name.
 * @returns {string} Bucket name
 * @throws {Error} If GCS_BUCKET is not configured
 */
export function getGCSBucket() {
  const bucket = GCS_BUCKET || process.env.GCS_BUCKET;
  if (!bucket) {
    throw new Error('GCS_BUCKET not configured. Call initGCS() first or set GCS_BUCKET env var.');
  }
  return bucket;
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
 * Execute a single upload attempt.
 * 
 * @param {string} localPath - Full path to local file
 * @param {string} gcsPath - GCS URI
 * @param {object} options - Upload options
 * @returns {object} Result with ok, error properties
 */
function executeUpload(localPath, gcsPath, options) {
  const { timeout = 300000, quiet = false } = options;
  
  const gsutilArgs = quiet ? '-q' : '';
  const cmd = `gsutil ${gsutilArgs} cp "${localPath}" "${gcsPath}"`;
  
  execSync(cmd, {
    stdio: quiet ? 'pipe' : 'inherit',
    timeout,
    encoding: 'utf8',
  });
  
  return { ok: true };
}

/**
 * Upload a file to GCS with retry logic and delete the local copy.
 * 
 * Uses gsutil for upload (relies on VM service account).
 * Retries transient failures with exponential backoff.
 * Always deletes local file after all attempts (success or failure).
 * 
 * @param {string} localPath - Full path to local file
 * @param {string} gcsPath - GCS URI (gs://bucket/path)
 * @param {object} [options] - Upload options
 * @param {number} [options.timeout=300000] - Timeout per attempt in ms (default 5 minutes)
 * @param {boolean} [options.quiet=false] - Suppress gsutil output
 * @param {number} [options.maxRetries=3] - Maximum retry attempts
 * @param {number} [options.baseDelay=1000] - Base delay for exponential backoff in ms
 * @param {number} [options.maxDelay=30000] - Maximum delay between retries in ms
 * @param {boolean} [options.deleteOnFailure=true] - Delete local file even if upload fails
 * @returns {object} Upload result { ok, localPath, gcsPath, bytes?, error?, attempts?, retried? }
 */
export function uploadAndCleanupSync(localPath, gcsPath, options = {}) {
  const { 
    timeout = 300000, 
    quiet = false,
    maxRetries = DEFAULT_MAX_RETRIES,
    baseDelay = DEFAULT_BASE_DELAY_MS,
    maxDelay = DEFAULT_MAX_DELAY_MS,
    deleteOnFailure = true,
  } = options;
  
  const result = {
    ok: false,
    localPath,
    gcsPath,
    bytes: 0,
    error: null,
    attempts: 0,
    retried: false,
  };
  
  // Track stats
  uploadStats.totalUploads++;
  
  try {
    // Verify local file exists
    if (!existsSync(localPath)) {
      throw new Error(`Local file not found: ${localPath}`);
    }
    
    // Get file size before upload
    const stats = statSync(localPath);
    result.bytes = stats.size;
    
    let lastError = null;
    
    // Retry loop with exponential backoff
    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      result.attempts = attempt + 1;
      
      if (attempt > 0) {
        result.retried = true;
        const delay = calculateBackoffDelay(attempt - 1, baseDelay, maxDelay);
        console.log(`🔄 [gcs-upload] Retry ${attempt}/${maxRetries} for ${path.basename(localPath)} after ${Math.round(delay)}ms...`);
        sleep(delay);
      }
      
      try {
        executeUpload(localPath, gcsPath, { timeout, quiet });
        
        // gsutil's built-in CRC32C check handles transport integrity
        
        // Success!
        result.ok = true;
        uploadStats.successfulUploads++;
        uploadStats.totalBytesUploaded += result.bytes;
        
        const retryInfo = result.retried ? ` (after ${result.attempts} attempts)` : '';
        console.log(`☁️ Uploaded ${path.basename(localPath)} to ${gcsPath} (${(result.bytes / 1024).toFixed(1)}KB)${retryInfo}`);
        
        break; // Exit retry loop on success
        
      } catch (err) {
        lastError = err;
        
        // Check if error is transient and we have retries left
        if (attempt < maxRetries && isTransientError(err.message)) {
          console.warn(`⚠️ [gcs-upload] Transient error (attempt ${attempt + 1}/${maxRetries + 1}): ${err.message}`);
          continue; // Retry
        }
        
        // Non-transient error or out of retries
        if (attempt === maxRetries) {
          console.error(`❌ [gcs-upload] Failed after ${result.attempts} attempts: ${err.message}`);
        }
      }
    }
    
    // If we exited the loop without success, record the error
    if (!result.ok) {
      result.error = lastError?.message || 'Unknown error';
      uploadStats.failedUploads++;
      console.error(`❌ [gcs-upload] Final failure for ${path.basename(localPath)}: ${result.error}`);
    }
    
  } catch (err) {
    // Error before retry loop (e.g., file not found)
    result.error = err.message;
    uploadStats.failedUploads++;
    console.error(`❌ [gcs-upload] Failed to upload ${path.basename(localPath)}: ${err.message}`);
    
  } finally {
    // ALWAYS delete local file to prevent disk accumulation (unless explicitly disabled)
    if (deleteOnFailure || result.ok) {
      if (existsSync(localPath)) {
        try {
          unlinkSync(localPath);
          if (!quiet) {
            console.log(`🗑️ Deleted local file: ${path.basename(localPath)}`);
          }
        } catch (deleteErr) {
          console.error(`❌ [gcs-upload] Failed to delete ${localPath}: ${deleteErr.message}`);
        }
      }
    }
  }
  
  return result;
}

/**
 * Async version of uploadAndCleanupSync with retry logic.
 * Uses setTimeout for non-blocking delays.
 * 
 * @param {string} localPath - Full path to local file
 * @param {string} gcsPath - GCS URI (gs://bucket/path)
 * @param {object} [options] - Upload options (same as uploadAndCleanupSync)
 * @returns {Promise<object>} Upload result
 */
export async function uploadAndCleanup(localPath, gcsPath, options = {}) {
  const { 
    timeout = 300000, 
    quiet = false,
    maxRetries = DEFAULT_MAX_RETRIES,
    baseDelay = DEFAULT_BASE_DELAY_MS,
    maxDelay = DEFAULT_MAX_DELAY_MS,
    deleteOnFailure = true,
  } = options;
  
  const result = {
    ok: false,
    localPath,
    gcsPath,
    bytes: 0,
    error: null,
    attempts: 0,
    retried: false,
  };
  
  // Track stats
  uploadStats.totalUploads++;
  
  try {
    // Verify local file exists
    if (!existsSync(localPath)) {
      throw new Error(`Local file not found: ${localPath}`);
    }
    
    // Get file size before upload
    const stats = statSync(localPath);
    result.bytes = stats.size;
    
    let lastError = null;
    
    // Retry loop with exponential backoff
    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      result.attempts = attempt + 1;
      
      if (attempt > 0) {
        result.retried = true;
        const delay = calculateBackoffDelay(attempt - 1, baseDelay, maxDelay);
        console.log(`🔄 [gcs-upload] Retry ${attempt}/${maxRetries} for ${path.basename(localPath)} after ${Math.round(delay)}ms...`);
        await new Promise(resolve => setTimeout(resolve, delay));
      }
      
      try {
        executeUpload(localPath, gcsPath, { timeout, quiet });
        
        // gsutil's built-in CRC32C check handles transport integrity
        
        // Success!
        result.ok = true;
        uploadStats.successfulUploads++;
        uploadStats.totalBytesUploaded += result.bytes;
        
        const retryInfo = result.retried ? ` (after ${result.attempts} attempts)` : '';
        console.log(`☁️ Uploaded ${path.basename(localPath)} to ${gcsPath} (${(result.bytes / 1024).toFixed(1)}KB)${retryInfo}`);
        
        break; // Exit retry loop on success
        
      } catch (err) {
        lastError = err;
        
        // Check if error is transient and we have retries left
        if (attempt < maxRetries && isTransientError(err.message)) {
          console.warn(`⚠️ [gcs-upload] Transient error (attempt ${attempt + 1}/${maxRetries + 1}): ${err.message}`);
          continue; // Retry
        }
        
        // Non-transient error or out of retries
        if (attempt === maxRetries) {
          console.error(`❌ [gcs-upload] Failed after ${result.attempts} attempts: ${err.message}`);
        }
      }
    }
    
    // If we exited the loop without success, record the error
    if (!result.ok) {
      result.error = lastError?.message || 'Unknown error';
      uploadStats.failedUploads++;
      console.error(`❌ [gcs-upload] Final failure for ${path.basename(localPath)}: ${result.error}`);
    }
    
  } catch (err) {
    // Error before retry loop (e.g., file not found)
    result.error = err.message;
    uploadStats.failedUploads++;
    console.error(`❌ [gcs-upload] Failed to upload ${path.basename(localPath)}: ${err.message}`);
    
  } finally {
    // ALWAYS delete local file to prevent disk accumulation (unless explicitly disabled)
    if (deleteOnFailure || result.ok) {
      if (existsSync(localPath)) {
        try {
          unlinkSync(localPath);
          if (!quiet) {
            console.log(`🗑️ Deleted local file: ${path.basename(localPath)}`);
          }
        } catch (deleteErr) {
          console.error(`❌ [gcs-upload] Failed to delete ${localPath}: ${deleteErr.message}`);
        }
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
  totalRetries: 0,
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
    totalRetries: 0,
  };
}

/**
 * Get retry configuration.
 */
export function getRetryConfig() {
  return {
    maxRetries: DEFAULT_MAX_RETRIES,
    baseDelayMs: DEFAULT_BASE_DELAY_MS,
    maxDelayMs: DEFAULT_MAX_DELAY_MS,
  };
}

// Export internals for testing
export { sleep, calculateBackoffDelay, isTransientError };

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
  getRetryConfig,
  isTransientError,
  sleep,
  calculateBackoffDelay,
};
