/**
 * Binary Writer Pool (Enhanced Version)
 *
 * Adds support for:
 *   - MAX_WORKERS
 *   - MAX_CONCURRENT_WRITES
 *   - WORKER_POOL_SIZE
 *   - CHUNK_SIZE
 *   - ZSTD_LEVEL
 *
 * Fully user-configurable performance tuning.
 */

import { Worker } from 'node:worker_threads';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const WORKER_SCRIPT = path.join(__dirname, 'worker-writer.js');

// -------------------------------
// 🔥 NEW: TRUE PERFORMANCE OVERRIDES
// -------------------------------

// User can set ANY of these:
const ENV_MAX_WORKERS =
  parseInt(process.env.MAX_WORKERS) ||
  parseInt(process.env.MAX_CONCURRENT_WRITES) ||
  parseInt(process.env.WORKER_POOL_SIZE);

// Default: ALL CPU threads minus 1 (best for 12700K)
const CPU_THREADS = os.cpus().length;

const DEFAULT_MAX_WORKERS = ENV_MAX_WORKERS || Math.max(2, CPU_THREADS - 1);

// Chunk size (passed to worker)
const CHUNK_SIZE = parseInt(process.env.CHUNK_SIZE) || 4096;

// Compression level override
const ZSTD_LEVEL = parseInt(process.env.ZSTD_LEVEL) || 1;


export class BinaryWriterPool {
  constructor(maxWorkers = DEFAULT_MAX_WORKERS) {
    this.maxWorkers = maxWorkers;
    this.slots = maxWorkers;
    this.activeWorkers = new Set();
    this.queue = [];
    this.startTime = Date.now();
    this.stats = {
      totalJobs: 0,
      completedJobs: 0,
      failedJobs: 0,
      totalRecords: 0,
      totalOriginalBytes: 0,
      totalCompressedBytes: 0,
    };
    this.initialized = false;
  }

  async init() {
    if (this.initialized) return;
    console.log(
      `🔧 Initializing binary writer pool with ${this.maxWorkers} threads ` +
      `(CPU: ${CPU_THREADS}, CHUNK_SIZE: ${CHUNK_SIZE}, ZSTD_LEVEL: ${ZSTD_LEVEL})`
    );
    this.initialized = true;
  }

  /**
   * Enqueue job
   */
  writeJob(job) {
    this.stats.totalJobs++;

    // Attach chunk size + compression level
    job.chunkSize = CHUNK_SIZE;
    job.zstdLevel = ZSTD_LEVEL;

    return new Promise((resolve, reject) => {
      this.queue.push({ job, resolve, reject });
      this._pump();
    });
  }

  /**
   * Pump execution queue
   */
  _pump() {
    while (this.slots > 0 && this.queue.length > 0) {
      this.slots--;
      const { job, resolve, reject } = this.queue.shift();

      const worker = new Worker(WORKER_SCRIPT, { workerData: job });
      this.activeWorkers.add(worker);

      let jobCompleted = false;

      const cleanup = () => {
        this.activeWorkers.delete(worker);
        this.slots++;
        this._pump();
      };

      worker.once('message', (msg) => {
        jobCompleted = true;

        if (msg.ok) {
          this.stats.completedJobs++;
          this.stats.totalRecords += msg.count;
          this.stats.totalOriginalBytes += msg.originalSize || 0;
          this.stats.totalCompressedBytes += msg.compressedSize || 0;
          resolve(msg);
        } else {
          this.stats.failedJobs++;
          reject(new Error(msg.error || "Worker error"));
        }

        cleanup();
      });

      worker.once('error', (err) => {
        this.stats.failedJobs++;
        reject(err);
        cleanup();
      });

      worker.once('exit', (code) => {
        if (!jobCompleted && code !== 0) {
          console.error(`❌ Worker crashed with exit code ${code}`);
          this.stats.failedJobs++;
          reject(new Error(`Worker crashed with code ${code}`));
        }
        cleanup();
      });
    }
  }

  getStats() {
    const elapsed = (Date.now() - this.startTime) / 1000;
    const ratio =
      this.stats.totalOriginalBytes > 0
        ? ((this.stats.totalCompressedBytes / this.stats.totalOriginalBytes) * 100).toFixed(1)
        : "0.0";

    const mbWritten = (this.stats.totalCompressedBytes / (1024 * 1024)).toFixed(2);
    const mbPerSec = elapsed > 0 ? (mbWritten / elapsed).toFixed(2) : "0.00";

    return {
      ...this.stats,
      activeWorkers: this.activeWorkers.size,
      queuedJobs: this.queue.length,
      availableSlots: this.slots,
      compressionRatio: `${ratio}%`,
      mbWritten,
      mbPerSec,
      elapsedSec: elapsed.toFixed(1),
    };
  }

  async drain() {
    while (this.queue.length > 0 || this.activeWorkers.size > 0) {
      await new Promise((r) => setTimeout(r, 60));
    }
  }

  async shutdown() {
    await this.drain();
    console.log("🔧 Binary writer pool shut down");
    console.log("📊 Final writer stats:", this.getStats());
  }
}

// Singleton
let poolInstance = null;

export function getBinaryWriterPool(sizeOverride) {
  if (!poolInstance) {
    const finalSize = sizeOverride || DEFAULT_MAX_WORKERS;
    poolInstance = new BinaryWriterPool(finalSize);
  }
  return poolInstance;
}

export async function shutdownBinaryPool() {
  if (poolInstance) {
    await poolInstance.shutdown();
    poolInstance = null;
  }
}

export default BinaryWriterPool;
