/**
 * GCS Upload Queue - Background Async Upload Manager
 * 
 * Provides a non-blocking upload queue that decouples file writing from 
 * GCS uploads. Files are queued and uploaded in parallel background workers.
 * 
 * Key Features:
 * - Non-blocking: write operations return immediately after queuing
 * - Parallel uploads: configurable concurrent upload limit
 * - Backpressure: pauses writes if queue grows too large
 * - Graceful shutdown: drains queue before exit
 * - Progress tracking: real-time stats on throughput
 * 
 * Usage:
 *   const queue = getUploadQueue();
 *   queue.enqueue(localPath, gcsPath);  // Returns immediately
 *   await queue.drain();                // Wait for all uploads to complete
 */

import { spawn, execSync } from 'child_process';
import { existsSync, readFileSync, unlinkSync, statSync, appendFileSync, mkdirSync } from 'fs';
import { createHash } from 'crypto';
import path from 'path';
// LAZY env var reading - called at queue creation time, not module load time
// This is critical because ESM hoists imports before dotenv.config() runs
function getConfigFromEnv() {
  return {
    maxConcurrent: parseInt(process.env.GCS_UPLOAD_CONCURRENCY) || 8,
    queueHighWater: parseInt(process.env.GCS_QUEUE_HIGH_WATER) || 100,
    queueLowWater: parseInt(process.env.GCS_QUEUE_LOW_WATER) || 20,
    maxRetries: parseInt(process.env.GCS_MAX_RETRIES) || 3,
    baseDelayMs: parseInt(process.env.GCS_RETRY_BASE_DELAY_MS) || 1000,
    // Byte-aware backpressure: pause when queued bytes exceed this (default 512MB)
    byteHighWater: parseInt(process.env.GCS_BYTE_HIGH_WATER) || 512 * 1024 * 1024,
    byteLowWater: parseInt(process.env.GCS_BYTE_LOW_WATER) || 128 * 1024 * 1024,
  };
}

// Transient error patterns
const TRANSIENT_ERROR_PATTERNS = [
  /timeout/i, /timed out/i, /connection reset/i, /connection refused/i,
  /network unreachable/i, /temporary failure/i, /service unavailable/i,
  /503/, /502/, /500/, /ECONNRESET/, /ETIMEDOUT/, /ENOTFOUND/, /ENETUNREACH/,
  /socket hang up/i, /rate limit/i, /too many requests/i, /429/, /try again/i,
];

function isTransientError(msg) {
  if (!msg) return false;
  return TRANSIENT_ERROR_PATTERNS.some(p => p.test(msg));
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function calculateBackoffDelay(attempt, baseDelay = 1000) {
  const exponentialDelay = baseDelay * Math.pow(2, attempt);
  const jitter = exponentialDelay * 0.25 * (Math.random() * 2 - 1);
  return Math.min(exponentialDelay + jitter, 30000);
}

// Dead-letter log directory
const DEAD_LETTER_DIR = '/tmp/ledger_raw';
const DEAD_LETTER_FILE = path.join(DEAD_LETTER_DIR, 'failed-uploads.jsonl');

/**
 * Log a failed upload to the dead-letter file for later retry.
 * Each line is a JSON object with localPath, gcsPath, error, and timestamp.
 */
export function logFailedUpload(localPath, gcsPath, error, keepFile = true) {
  try {
    if (!existsSync(DEAD_LETTER_DIR)) {
      mkdirSync(DEAD_LETTER_DIR, { recursive: true });
    }
    const entry = {
      localPath,
      gcsPath,
      error: error || 'Unknown error',
      timestamp: new Date().toISOString(),
      fileExists: existsSync(localPath),
    };
    appendFileSync(DEAD_LETTER_FILE, JSON.stringify(entry) + '\n');
    console.error(`📋 [dead-letter] Logged failed upload: ${path.basename(localPath)} → ${gcsPath}`);
  } catch (logErr) {
    console.error(`❌ [dead-letter] Failed to write dead-letter log: ${logErr.message}`);
  }
}

/**
 * Get path to the dead-letter file.
 */
export function getDeadLetterPath() {
  return DEAD_LETTER_FILE;
}

/**
 * Compute MD5 hash of a local file (Base64-encoded, matching GCS format).
 */
export function computeLocalMD5(localPath) {
  const data = readFileSync(localPath);
  return createHash('md5').update(data).digest('base64');
}

/**
 * Get the MD5 hash of a GCS object using gsutil stat.
 * Returns null if the hash cannot be retrieved.
 */
export function getGCSObjectMD5(gcsPath, timeout = 30000) {
  try {
    const output = execSync(`gsutil stat "${gcsPath}"`, {
      encoding: 'utf8',
      stdio: ['pipe', 'pipe', 'pipe'],
      timeout,
    });
    // gsutil stat outputs "Hash (md5):    <base64hash>"
    const match = output.match(/Hash \(md5\):\s+(\S+)/);
    return match ? match[1] : null;
  } catch {
    return null;
  }
}

/**
 * Verify upload integrity by comparing local and remote MD5 hashes.
 * 
 * @param {string} localPath - Local file path
 * @param {string} gcsPath - GCS URI
 * @returns {{ ok: boolean, localMD5?: string, remoteMD5?: string, error?: string }}
 */
export function verifyUploadIntegrity(localPath, gcsPath) {
  try {
    if (!existsSync(localPath)) {
      return { ok: false, error: 'Local file no longer exists for verification' };
    }
    
    const localMD5 = computeLocalMD5(localPath);
    const remoteMD5 = getGCSObjectMD5(gcsPath);
    
    if (!remoteMD5) {
      return { ok: false, localMD5, error: 'Could not retrieve GCS object hash' };
    }
    
    if (localMD5 !== remoteMD5) {
      return { ok: false, localMD5, remoteMD5, error: `Hash mismatch: local=${localMD5} remote=${remoteMD5}` };
    }
    
    return { ok: true, localMD5, remoteMD5 };
  } catch (err) {
    return { ok: false, error: `Verification failed: ${err.message}` };
  }
}

function gsutilUpload(localPath, gcsPath, timeout = 300000) {
  return new Promise((resolve, reject) => {
    const proc = spawn('gsutil', ['-q', 'cp', localPath, gcsPath], {
      stdio: ['ignore', 'pipe', 'pipe'],
    });

    let stderr = '';
    let timeoutId = null;

    if (timeout) {
      timeoutId = setTimeout(() => {
        proc.kill('SIGTERM');
        reject(new Error(`Upload timed out after ${timeout}ms`));
      }, timeout);
    }

    proc.stderr.on('data', (chunk) => {
      stderr += chunk.toString();
    });

    proc.on('close', (code) => {
      if (timeoutId) clearTimeout(timeoutId);
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(stderr || `gsutil exited with code ${code}`));
      }
    });

    proc.on('error', (err) => {
      if (timeoutId) clearTimeout(timeoutId);
      reject(err);
    });
  });
}

class GCSUploadQueue {
  constructor(maxConcurrent) {
    // Read config lazily at construction time (after dotenv has loaded)
    const config = getConfigFromEnv();
    this.maxConcurrent = maxConcurrent || config.maxConcurrent;
    this.queueHighWater = config.queueHighWater;
    this.queueLowWater = config.queueLowWater;
    this.maxRetries = config.maxRetries;
    this.baseDelayMs = config.baseDelayMs;
    this.byteHighWater = config.byteHighWater;
    this.byteLowWater = config.byteLowWater;
    this.queue = [];
    this.activeUploads = 0;
    this.isPaused = false;
    this.isShuttingDown = false;
    this.queuedBytes = 0; // Track total queued bytes

    // Stats
    this.stats = {
      queued: 0,
      completed: 0,
      failed: 0,
      bytesUploaded: 0,
      startTime: Date.now(),
      peakQueueSize: 0,
      peakQueueBytes: 0,
      totalRetries: 0,
    };

    // Event callbacks
    this.onDrain = null;
    this.drainPromise = null;
    this.drainResolve = null;

    console.log(`☁️ [upload-queue] Initialized with ${this.maxConcurrent} concurrent uploads`);
    console.log(`☁️ [upload-queue] Backpressure: pause at ${this.queueHighWater} items or ${(this.byteHighWater / 1024 / 1024).toFixed(0)}MB`);
  }

  /**
   * Enqueue a file for upload to GCS.
   * Returns immediately - upload happens in background.
   * 
   * @param {string} localPath - Local file path
   * @param {string} gcsPath - GCS destination URI
   * @param {object} options - Upload options
   * @returns {boolean} True if queued, false if backpressure applied
   */
  enqueue(localPath, gcsPath, options = {}) {
    if (this.isShuttingDown) {
      console.warn(`⚠️ [upload-queue] Rejecting upload during shutdown: ${path.basename(localPath)}`);
      return false;
    }

    // Track file size for byte-aware backpressure
    let fileSize = 0;
    try {
      fileSize = existsSync(localPath) ? statSync(localPath).size : 0;
    } catch { fileSize = 0; }

    this.queue.push({ localPath, gcsPath, options, attempts: 0, fileSize });
    this.stats.queued++;
    this.queuedBytes += fileSize;
    this.stats.peakQueueSize = Math.max(this.stats.peakQueueSize, this.queue.length);
    this.stats.peakQueueBytes = Math.max(this.stats.peakQueueBytes || 0, this.queuedBytes);

    // Check for backpressure (count-based OR byte-based)
    if (!this.isPaused && (this.queue.length >= this.queueHighWater || this.queuedBytes >= this.byteHighWater)) {
      this.isPaused = true;
      const reason = this.queuedBytes >= this.byteHighWater
        ? `${(this.queuedBytes / 1024 / 1024).toFixed(1)}MB queued`
        : `${this.queue.length} items queued`;
      console.warn(`⚠️ [upload-queue] Backpressure ON: ${reason}`);
    }

    // Pump the queue
    this._pump();

    return true;
  }

  /**
   * Check if writes should pause due to backpressure.
   */
  shouldPause() {
    return this.isPaused;
  }

  /**
   * Get current queue depth for monitoring.
   */
  getQueueDepth() {
    return this.queue.length + this.activeUploads;
  }

  /**
   * Process queued uploads.
   */
  _pump() {
    while (this.activeUploads < this.maxConcurrent && this.queue.length > 0) {
      const job = this.queue.shift();
      this.queuedBytes -= (job.fileSize || 0);
      this.activeUploads++;
      this._processUpload(job);
    }

    // Check for low water mark (both count AND bytes must be below threshold)
    if (this.isPaused && this.queue.length <= this.queueLowWater && this.queuedBytes <= this.byteLowWater) {
      this.isPaused = false;
      console.log(`✅ [upload-queue] Backpressure OFF: ${this.queue.length} items, ${(this.queuedBytes / 1024 / 1024).toFixed(1)}MB`);
    }

    // Check for drain completion
    if (this.drainResolve && this.activeUploads === 0 && this.queue.length === 0) {
      this.drainResolve();
      this.drainResolve = null;
      this.drainPromise = null;
    }
  }

  /**
   * Process a single upload with retry logic.
   */
  async _processUpload(job) {
    const { localPath, gcsPath, options } = job;
    const maxRetries = options.maxRetries ?? this.maxRetries;

    try {
      // Verify file exists
      if (!existsSync(localPath)) {
        throw new Error(`File not found: ${localPath}`);
      }

      const fileSize = statSync(localPath).size;
      let lastError = null;

      for (let attempt = 0; attempt <= maxRetries; attempt++) {
        if (attempt > 0) {
          this.stats.totalRetries++;
          const delay = calculateBackoffDelay(attempt - 1, this.baseDelayMs);
          console.log(`🔄 [upload-queue] Retry ${attempt}/${maxRetries} for ${path.basename(localPath)}`);
          await sleep(delay);
        }

        try {
          await gsutilUpload(localPath, gcsPath, options.timeout || 300000);

          // gsutil's built-in CRC32C check handles transport integrity
          this.stats.completed++;
          this.stats.bytesUploaded += fileSize;

          const retryInfo = attempt > 0 ? ` (retry ${attempt})` : '';
          console.log(`☁️ Uploaded ${path.basename(localPath)} (${(fileSize / 1024).toFixed(1)}KB)${retryInfo}`);

          // Delete local file — safe now that integrity is verified
          try {
            unlinkSync(localPath);
            console.log(`🗑️ Deleted ${path.basename(localPath)}`);
          } catch (e) {
            console.error(`⚠️ Failed to delete ${localPath}: ${e.message}`);
          }

          return; // Success, exit
        } catch (err) {
          lastError = err;
          if (attempt < maxRetries && isTransientError(err.message)) {
            continue; // Retry
          }
          break; // Give up
        }
      }

      // All retries failed
      this.stats.failed++;
      console.error(`❌ [upload-queue] Upload failed: ${path.basename(localPath)}: ${lastError?.message}`);

      // Log to dead-letter file for later retry
      logFailedUpload(localPath, gcsPath, lastError?.message);

      // Keep local file if it exists so retry script can re-upload it
      // Only delete if explicitly configured to free disk space
      const deleteOnFailure = options.deleteOnFailure !== undefined ? options.deleteOnFailure : false;
      if (deleteOnFailure && existsSync(localPath)) {
        try {
          unlinkSync(localPath);
          console.warn(`⚠️ [upload-queue] Deleted failed file (deleteOnFailure=true): ${path.basename(localPath)}`);
        } catch (e) {
          console.error(`⚠️ Failed to delete ${localPath}: ${e.message}`);
        }
      } else if (existsSync(localPath)) {
        console.log(`📂 [upload-queue] Keeping local file for retry: ${path.basename(localPath)}`);
      }

    } finally {
      this.activeUploads--;
      this._pump();
    }
  }

  /**
   * Wait for all queued uploads to complete.
   */
  async drain() {
    if (this.activeUploads === 0 && this.queue.length === 0) {
      return;
    }

    if (!this.drainPromise) {
      this.drainPromise = new Promise(resolve => {
        this.drainResolve = resolve;
      });
    }

    console.log(`⏳ [upload-queue] Draining ${this.queue.length} queued + ${this.activeUploads} active uploads...`);
    await this.drainPromise;
    console.log(`✅ [upload-queue] Drain complete`);
  }

  /**
   * Shutdown the queue (waits for pending uploads).
   */
  async shutdown() {
    this.isShuttingDown = true;
    await this.drain();
    this.printStats();
  }

  /**
   * Get current stats.
   */
  getStats() {
    const elapsed = (Date.now() - this.stats.startTime) / 1000;
    const throughputMBps = (this.stats.bytesUploaded / 1024 / 1024) / elapsed;

    return {
      ...this.stats,
      pending: this.queue.length,
      active: this.activeUploads,
      elapsedSeconds: elapsed.toFixed(1),
      throughputMBps: throughputMBps.toFixed(2),
    };
  }

  /**
   * Print stats summary.
   */
  printStats() {
    const stats = this.getStats();
    console.log(`\n📊 [upload-queue] Final Statistics:`);
    console.log(`   Completed: ${stats.completed}`);
    console.log(`   Failed: ${stats.failed}`);
    console.log(`   Total Retries: ${stats.totalRetries}`);
    console.log(`   Peak Queue: ${stats.peakQueueSize}`);
    console.log(`   Data Uploaded: ${(stats.bytesUploaded / 1024 / 1024).toFixed(2)} MB`);
    console.log(`   Throughput: ${stats.throughputMBps} MB/s`);
    console.log(`   Duration: ${stats.elapsedSeconds}s\n`);
  }
}

// Singleton instance
let queueInstance = null;

/**
 * Get the singleton upload queue instance.
 */
export function getUploadQueue(maxConcurrent) {
  if (!queueInstance) {
    queueInstance = new GCSUploadQueue(maxConcurrent);
  }
  return queueInstance;
}

/**
 * Shutdown the upload queue.
 */
export async function shutdownUploadQueue() {
  if (queueInstance) {
    await queueInstance.shutdown();
    queueInstance = null;
  }
}

/**
 * Non-blocking enqueue for GCS upload.
 * Returns immediately - upload happens in background.
 */
export function enqueueUpload(localPath, gcsPath, options = {}) {
  const queue = getUploadQueue();
  return queue.enqueue(localPath, gcsPath, options);
}

/**
 * Wait for all uploads to complete.
 */
export async function drainUploads() {
  if (queueInstance) {
    await queueInstance.drain();
  }
}

/**
 * Check if writes should pause for backpressure.
 */
export function shouldPauseWrites() {
  return queueInstance?.shouldPause() ?? false;
}

export default GCSUploadQueue;
