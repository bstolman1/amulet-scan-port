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

import { spawn } from 'child_process';
import { existsSync, unlinkSync, statSync } from 'fs';
import path from 'path';

// Configuration from environment
const MAX_CONCURRENT = parseInt(process.env.GCS_UPLOAD_CONCURRENCY) || 8;
const QUEUE_HIGH_WATER = parseInt(process.env.GCS_QUEUE_HIGH_WATER) || 100;
const QUEUE_LOW_WATER = parseInt(process.env.GCS_QUEUE_LOW_WATER) || 20;
const DEFAULT_MAX_RETRIES = parseInt(process.env.GCS_MAX_RETRIES) || 3;
const DEFAULT_BASE_DELAY_MS = parseInt(process.env.GCS_RETRY_BASE_DELAY_MS) || 1000;

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

function calculateBackoffDelay(attempt, baseDelay = DEFAULT_BASE_DELAY_MS) {
  const exponentialDelay = baseDelay * Math.pow(2, attempt);
  const jitter = exponentialDelay * 0.25 * (Math.random() * 2 - 1);
  return Math.min(exponentialDelay + jitter, 30000);
}

/**
 * Execute gsutil cp using spawn (non-blocking)
 */
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
  constructor(maxConcurrent = MAX_CONCURRENT) {
    this.maxConcurrent = maxConcurrent;
    this.queue = [];
    this.activeUploads = 0;
    this.isPaused = false;
    this.isShuttingDown = false;

    // Stats
    this.stats = {
      queued: 0,
      completed: 0,
      failed: 0,
      bytesUploaded: 0,
      startTime: Date.now(),
      peakQueueSize: 0,
      totalRetries: 0,
    };

    // Event callbacks
    this.onDrain = null;
    this.drainPromise = null;
    this.drainResolve = null;

    console.log(`☁️ [upload-queue] Initialized with ${maxConcurrent} concurrent uploads`);
    console.log(`☁️ [upload-queue] Backpressure: pause at ${QUEUE_HIGH_WATER}, resume at ${QUEUE_LOW_WATER}`);
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

    this.queue.push({ localPath, gcsPath, options, attempts: 0 });
    this.stats.queued++;
    this.stats.peakQueueSize = Math.max(this.stats.peakQueueSize, this.queue.length);

    // Check for backpressure
    if (this.queue.length >= QUEUE_HIGH_WATER && !this.isPaused) {
      this.isPaused = true;
      console.warn(`⚠️ [upload-queue] Backpressure ON: queue at ${this.queue.length} items`);
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
      this.activeUploads++;
      this._processUpload(job);
    }

    // Check for low water mark
    if (this.isPaused && this.queue.length <= QUEUE_LOW_WATER) {
      this.isPaused = false;
      console.log(`✅ [upload-queue] Backpressure OFF: queue at ${this.queue.length} items`);
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
    const maxRetries = options.maxRetries ?? DEFAULT_MAX_RETRIES;

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
          const delay = calculateBackoffDelay(attempt - 1);
          console.log(`🔄 [upload-queue] Retry ${attempt}/${maxRetries} for ${path.basename(localPath)}`);
          await sleep(delay);
        }

        try {
          await gsutilUpload(localPath, gcsPath, options.timeout || 300000);

          // Success
          this.stats.completed++;
          this.stats.bytesUploaded += fileSize;

          const retryInfo = attempt > 0 ? ` (retry ${attempt})` : '';
          console.log(`☁️ Uploaded ${path.basename(localPath)} (${(fileSize / 1024).toFixed(1)}KB)${retryInfo}`);

          // Delete local file
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

      // Still delete local file to prevent disk fill
      if (existsSync(localPath)) {
        try {
          unlinkSync(localPath);
        } catch (e) {
          console.error(`⚠️ Failed to delete ${localPath}: ${e.message}`);
        }
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
