/**
 * Binary Writer Pool
 * 
 * Manages a pool of worker threads for parallel Protobuf encoding + ZSTD compression.
 * Each worker has its own heap, avoiding the 4GB limit of a single process.
 */

import { Worker } from 'node:worker_threads';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const WORKER_SCRIPT = path.join(__dirname, 'worker-writer.js');
const DEFAULT_MAX_WORKERS = parseInt(process.env.MAX_WORKERS) || 
                            parseInt(process.env.MAX_CONCURRENT_WRITES) || 
                            parseInt(process.env.WORKER_POOL_SIZE) || 
                            Math.max(2, os.cpus().length - 1);

export class BinaryWriterPool {
  constructor(maxWorkers = DEFAULT_MAX_WORKERS) {
    this.maxWorkers = maxWorkers;
    this.slots = maxWorkers; // available slots
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
    console.log(`🔧 Initializing binary writer pool with ${this.maxWorkers} threads`);
    this.initialized = true;
  }

  /**
   * Queue a write job
   * @param {Object} job - { type: 'events'|'updates', filePath, records, zstdLevel? }
   * @returns {Promise<Object>} - { ok, filePath, count, originalSize, compressedSize }
   */
  writeJob(job) {
    this.stats.totalJobs++;
    return new Promise((resolve, reject) => {
      this.queue.push({ job, resolve, reject });
      this._pump();
    });
  }

  _pump() {
    while (this.slots > 0 && this.queue.length > 0) {
      this.slots--;
      const { job, resolve, reject } = this.queue.shift();

      const worker = new Worker(WORKER_SCRIPT, { workerData: job });
      this.activeWorkers.add(worker);
      
      // Track if job completed successfully (to ignore exit code)
      let jobCompleted = false;

      const cleanup = () => {
        this.activeWorkers.delete(worker);
        this.slots++;
        // Don't call terminate - let worker exit naturally
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
          reject(new Error(msg.error || 'Unknown worker error'));
        }
        cleanup();
      });

      worker.once('error', (err) => {
        this.stats.failedJobs++;
        reject(err);
        cleanup();
      });

      worker.once('exit', (code) => {
        // Only log if job didn't complete successfully AND code is non-zero
        if (!jobCompleted && code !== 0 && code !== null) {
          console.error(`Worker exited unexpectedly with code ${code}`);
          this.stats.failedJobs++;
          reject(new Error(`Worker exited with code ${code}`));
          cleanup();
        }
      });
    }
  }

  /**
   * Get pool statistics with throughput
   */
  getStats() {
    const elapsed = (Date.now() - this.startTime) / 1000;
    const ratio = this.stats.totalOriginalBytes > 0 
      ? (this.stats.totalCompressedBytes / this.stats.totalOriginalBytes * 100).toFixed(1)
      : 0;
    
    const recordsPerSec = elapsed > 0 ? Math.round(this.stats.totalRecords / elapsed) : 0;
    const mbWritten = this.stats.totalCompressedBytes / (1024 * 1024);
    const mbPerSec = elapsed > 0 ? (mbWritten / elapsed).toFixed(2) : 0;
    
    return {
      ...this.stats,
      activeWorkers: this.activeWorkers.size,
      queuedJobs: this.queue.length,
      availableSlots: this.slots,
      compressionRatio: `${ratio}%`,
      elapsedSec: elapsed.toFixed(1),
      recordsPerSec,
      mbWritten: mbWritten.toFixed(2),
      mbPerSec,
    };
  }

  /**
   * Wait for all pending jobs to complete
   */
  async drain() {
    while (this.queue.length > 0 || this.activeWorkers.size > 0) {
      await new Promise(r => setTimeout(r, 100));
    }
  }

  /**
   * Shutdown the pool
   */
  async shutdown() {
    await this.drain();
    console.log('🔧 Binary writer pool shut down');
    console.log('📊 Final stats:', JSON.stringify(this.getStats(), null, 2));
  }
}

// Singleton instance
let poolInstance = null;

export function getBinaryWriterPool(size) {
  if (!poolInstance) {
    poolInstance = new BinaryWriterPool(size);
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
