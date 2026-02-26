/**
 * GCS Upload Queue - Background Async Upload Manager (SDK Version)
 * 
 * Uses @google-cloud/storage SDK for uploads instead of spawning gsutil processes.
 * This gives connection pooling, HTTP/2 multiplexing, and eliminates process spawn overhead.
 * 
 * Key Features:
 * - Non-blocking: write operations return immediately after queuing
 * - Parallel uploads: configurable concurrent upload limit
 * - Backpressure: pauses writes if queue grows too large (count-based AND byte-based)
 * - Graceful shutdown: drains queue before exit
 * - Progress tracking: real-time stats on throughput
 * - SDK-based: no child process spawning, CRC32C integrity checks built-in
 * 
 * Usage:
 *   const queue = getUploadQueue();
 *   queue.enqueue(localPath, gcsPath);  // Returns immediately
 *   await queue.drain();                // Wait for all uploads to complete
 */

import { existsSync, unlinkSync, statSync, appendFileSync, mkdirSync, createReadStream } from 'fs';
import path from 'path';

// Lazy-load the GCS SDK to avoid import-time errors if not installed
let _storage = null;
let _bucket = null;

async function getStorageClient() {
  if (_storage) return _storage;
  try {
    const { Storage } = await import('@google-cloud/storage');
    _storage = new Storage();
    return _storage;
  } catch (err) {
    throw new Error(`@google-cloud/storage not installed. Run: npm install @google-cloud/storage\n${err.message}`);
  }
}

async function getBucket() {
  if (_bucket) return _bucket;
  const bucketName = process.env.GCS_BUCKET;
  if (!bucketName) throw new Error('GCS_BUCKET environment variable not set');
  const storage = await getStorageClient();
  _bucket = storage.bucket(bucketName);
  return _bucket;
}

// LAZY env var reading - called at queue creation time, not module load time
function getConfigFromEnv() {
  return {
    maxConcurrent: parseInt(process.env.GCS_UPLOAD_CONCURRENCY) || 8,
    queueHighWater: parseInt(process.env.GCS_QUEUE_HIGH_WATER) || 100,
    queueLowWater: parseInt(process.env.GCS_QUEUE_LOW_WATER) || 20,
    maxRetries: parseInt(process.env.GCS_MAX_RETRIES) || 3,
    baseDelayMs: parseInt(process.env.GCS_RETRY_BASE_DELAY_MS) || 1000,
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

export function getDeadLetterPath() {
  return DEAD_LETTER_FILE;
}

/**
 * Upload a file to GCS using the SDK with streaming.
 * SDK handles CRC32C integrity automatically.
 */
async function sdkUpload(localPath, gcsPath, timeout = 300000) {
  const bucket = await getBucket();
  
  // Parse gcsPath: gs://bucket/path → path
  const objectName = gcsPath.replace(/^gs:\/\/[^/]+\//, '');
  
  const file = bucket.file(objectName);
  
  await new Promise((resolve, reject) => {
    const timeoutId = setTimeout(() => {
      reject(new Error(`Upload timed out after ${timeout}ms`));
    }, timeout);

    const readStream = createReadStream(localPath);
    const writeStream = file.createWriteStream({
      resumable: false, // Small files don't need resumable uploads
      validation: 'crc32c', // SDK validates integrity automatically
      metadata: {
        contentType: 'application/octet-stream',
      },
    });

    readStream.on('error', (err) => {
      clearTimeout(timeoutId);
      reject(err);
    });

    writeStream.on('error', (err) => {
      clearTimeout(timeoutId);
      reject(err);
    });

    writeStream.on('finish', () => {
      clearTimeout(timeoutId);
      resolve();
    });

    readStream.pipe(writeStream);
  });
}

class GCSUploadQueue {
  constructor(maxConcurrent) {
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
    this.queuedBytes = 0;

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

    this.onDrain = null;
    this.drainPromise = null;
    this.drainResolve = null;

    console.log(`☁️ [upload-queue] Initialized with ${this.maxConcurrent} concurrent uploads (SDK mode)`);
    console.log(`☁️ [upload-queue] Backpressure: pause at ${this.queueHighWater} items or ${(this.byteHighWater / 1024 / 1024).toFixed(0)}MB`);
  }

  enqueue(localPath, gcsPath, options = {}) {
    if (this.isShuttingDown) {
      console.warn(`⚠️ [upload-queue] Rejecting upload during shutdown: ${path.basename(localPath)}`);
      return false;
    }

    let fileSize = 0;
    try {
      fileSize = existsSync(localPath) ? statSync(localPath).size : 0;
    } catch { fileSize = 0; }

    this.queue.push({ localPath, gcsPath, options, attempts: 0, fileSize });
    this.stats.queued++;
    this.queuedBytes += fileSize;
    this.stats.peakQueueSize = Math.max(this.stats.peakQueueSize, this.queue.length);
    this.stats.peakQueueBytes = Math.max(this.stats.peakQueueBytes || 0, this.queuedBytes);

    if (!this.isPaused && (this.queue.length >= this.queueHighWater || this.queuedBytes >= this.byteHighWater)) {
      this.isPaused = true;
      const reason = this.queuedBytes >= this.byteHighWater
        ? `${(this.queuedBytes / 1024 / 1024).toFixed(1)}MB queued`
        : `${this.queue.length} items queued`;
      console.warn(`⚠️ [upload-queue] Backpressure ON: ${reason}`);
    }

    this._pump();
    return true;
  }

  shouldPause() {
    return this.isPaused;
  }

  getQueueDepth() {
    return this.queue.length + this.activeUploads;
  }

  _pump() {
    while (this.activeUploads < this.maxConcurrent && this.queue.length > 0) {
      const job = this.queue.shift();
      this.queuedBytes -= (job.fileSize || 0);
      this.activeUploads++;
      this._processUpload(job);
    }

    if (this.isPaused && this.queue.length <= this.queueLowWater && this.queuedBytes <= this.byteLowWater) {
      this.isPaused = false;
      console.log(`✅ [upload-queue] Backpressure OFF: ${this.queue.length} items, ${(this.queuedBytes / 1024 / 1024).toFixed(1)}MB`);
    }

    if (this.drainResolve && this.activeUploads === 0 && this.queue.length === 0) {
      this.drainResolve();
      this.drainResolve = null;
      this.drainPromise = null;
    }
  }

  async _processUpload(job) {
    const { localPath, gcsPath, options } = job;
    const maxRetries = options.maxRetries ?? this.maxRetries;

    try {
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
          await sdkUpload(localPath, gcsPath, options.timeout || 300000);

          this.stats.completed++;
          this.stats.bytesUploaded += fileSize;

          const retryInfo = attempt > 0 ? ` (retry ${attempt})` : '';
          console.log(`☁️ Uploaded ${path.basename(localPath)} (${(fileSize / 1024).toFixed(1)}KB)${retryInfo}`);

          try {
            unlinkSync(localPath);
            console.log(`🗑️ Deleted ${path.basename(localPath)}`);
          } catch (e) {
            console.error(`⚠️ Failed to delete ${localPath}: ${e.message}`);
          }

          return;
        } catch (err) {
          lastError = err;
          if (attempt < maxRetries && isTransientError(err.message)) {
            continue;
          }
          break;
        }
      }

      this.stats.failed++;
      console.error(`❌ [upload-queue] Upload failed: ${path.basename(localPath)}: ${lastError?.message}`);
      logFailedUpload(localPath, gcsPath, lastError?.message);

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

  async shutdown() {
    this.isShuttingDown = true;
    await this.drain();
    this.printStats();
  }

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

export function getUploadQueue(maxConcurrent) {
  if (!queueInstance) {
    queueInstance = new GCSUploadQueue(maxConcurrent);
  }
  return queueInstance;
}

export async function shutdownUploadQueue() {
  if (queueInstance) {
    await queueInstance.shutdown();
    queueInstance = null;
  }
}

export function enqueueUpload(localPath, gcsPath, options = {}) {
  const queue = getUploadQueue();
  return queue.enqueue(localPath, gcsPath, options);
}

export async function drainUploads() {
  if (queueInstance) {
    await queueInstance.drain();
  }
}

export function shouldPauseWrites() {
  return queueInstance?.shouldPause() ?? false;
}

export async function waitForBackpressureRelief(timeoutMs = 30000) {
  if (!queueInstance || !queueInstance.shouldPause()) return;
  const start = Date.now();
  while (queueInstance.shouldPause() && (Date.now() - start) < timeoutMs) {
    await new Promise(r => setTimeout(r, 200));
  }
  if (queueInstance.shouldPause()) {
    console.warn(`⚠️ [upload-queue] Backpressure relief timeout after ${timeoutMs}ms — continuing anyway`);
  }
}

export default GCSUploadQueue;
