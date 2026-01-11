/**
 * Parquet Writer Pool (Parallel Version)
 *
 * Manages a pool of worker threads for parallel Parquet file writing.
 * Each worker uses its own in-memory DuckDB instance for isolation.
 * 
 * Adds support for:
 *   - PARQUET_WORKERS (dedicated env var for parquet parallelism)
 *   - MAX_WORKERS / WORKER_POOL_SIZE (shared with binary writer)
 *   - Throughput tracking (files/sec, MB/sec)
 *   - Backpressure handling
 *
 * Drop-in replacement for the synchronous CLI-based approach.
 */

import { Worker } from 'node:worker_threads';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const WORKER_SCRIPT = path.join(__dirname, 'parquet-worker.js');

// -------------------------------
// Performance Configuration
// -------------------------------

// Priority: PARQUET_WORKERS > MAX_WORKERS > MAX_CONCURRENT_WRITES > WORKER_POOL_SIZE
const ENV_MAX_WORKERS =
  parseInt(process.env.PARQUET_WORKERS) ||
  parseInt(process.env.MAX_WORKERS) ||
  parseInt(process.env.MAX_CONCURRENT_WRITES) ||
  parseInt(process.env.WORKER_POOL_SIZE);

// Default: CPU threads minus 1 (leave 1 core for main thread)
const CPU_THREADS = os.cpus().length;
const DEFAULT_MAX_WORKERS = ENV_MAX_WORKERS || Math.max(2, CPU_THREADS - 1);

// Row group size for Parquet files (affects read performance)
const ROW_GROUP_SIZE = parseInt(process.env.PARQUET_ROW_GROUP) || 100000;


export class ParquetWriterPool {
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
      totalBytes: 0,
      validatedFiles: 0,
      validationFailures: 0,
      validationIssues: [],
    };
    this.initialized = false;
  }

  async init() {
    if (this.initialized) return;
    console.log(
      `🔧 Initializing Parquet writer pool with ${this.maxWorkers} threads ` +
      `(CPU: ${CPU_THREADS}, ROW_GROUP_SIZE: ${ROW_GROUP_SIZE})`
    );
    this.initialized = true;
  }

  /**
   * Enqueue a write job
   * 
   * @param {object} job - { type, filePath, records }
   * @returns {Promise<object>} - { ok, filePath, count, bytes }
   */
  writeJob(job) {
    this.stats.totalJobs++;

    // Attach row group size config
    job.rowGroupSize = ROW_GROUP_SIZE;

    return new Promise((resolve, reject) => {
      this.queue.push({ job, resolve, reject });
      this._pump();
    });
  }

  /**
   * Process queued jobs with available workers
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
          this.stats.totalRecords += msg.count || 0;
          this.stats.totalBytes += msg.bytes || 0;
          
          // Track validation results
          if (msg.validation) {
            this.stats.validatedFiles++;
            if (!msg.validation.valid) {
              this.stats.validationFailures++;
              // Keep last 10 validation issues for debugging
              if (this.stats.validationIssues.length < 10) {
                this.stats.validationIssues.push({
                  file: path.basename(job.filePath),
                  issues: msg.validation.issues,
                });
              }
              console.warn(`⚠️ Parquet validation failed: ${path.basename(job.filePath)} - ${msg.validation.issues.join(', ')}`);
            }
          }
          
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
          console.error(`❌ Parquet worker crashed with exit code ${code}`);
          this.stats.failedJobs++;
          reject(new Error(`Worker crashed with code ${code}`));
        }
        cleanup();
      });
    }
  }

  getStats() {
    const elapsed = (Date.now() - this.startTime) / 1000;
    const mbWritten = (this.stats.totalBytes / (1024 * 1024)).toFixed(2);
    const mbPerSec = elapsed > 0 ? (mbWritten / elapsed).toFixed(2) : "0.00";
    const filesPerSec = elapsed > 0 ? (this.stats.completedJobs / elapsed).toFixed(2) : "0.00";

    return {
      ...this.stats,
      activeWorkers: this.activeWorkers.size,
      queuedJobs: this.queue.length,
      availableSlots: this.slots,
      mbWritten,
      mbPerSec,
      filesPerSec,
      elapsedSec: elapsed.toFixed(1),
      validationRate: this.stats.validatedFiles > 0 
        ? ((this.stats.validatedFiles - this.stats.validationFailures) / this.stats.validatedFiles * 100).toFixed(1) + '%'
        : 'N/A',
    };
  }

  /**
   * Get recent validation issues (for debugging)
   */
  getValidationIssues() {
    return this.stats.validationIssues;
  }

  async drain() {
    while (this.queue.length > 0 || this.activeWorkers.size > 0) {
      await new Promise((r) => setTimeout(r, 50));
    }
  }

  async shutdown() {
    await this.drain();
    console.log("🔧 Parquet writer pool shut down");
    console.log("📊 Final Parquet stats:", this.getStats());
  }
}

// Singleton instance
let poolInstance = null;

export function getParquetWriterPool(sizeOverride) {
  if (!poolInstance) {
    const finalSize = sizeOverride || DEFAULT_MAX_WORKERS;
    poolInstance = new ParquetWriterPool(finalSize);
  }
  return poolInstance;
}

export async function shutdownParquetPool() {
  if (poolInstance) {
    await poolInstance.shutdown();
    poolInstance = null;
  }
}

export default ParquetWriterPool;
