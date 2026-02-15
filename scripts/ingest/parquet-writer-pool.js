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

// CPU threads (static - doesn't change at runtime)
const CPU_THREADS = os.cpus().length;

// LAZY env var reading - called at pool creation time, not module load time
// This is critical because ESM hoists imports before dotenv.config() runs
function getMaxWorkersFromEnv() {
  // Priority: PARQUET_WORKERS > MAX_WORKERS > MAX_CONCURRENT_WRITES > WORKER_POOL_SIZE
  const envValue =
    parseInt(process.env.PARQUET_WORKERS) ||
    parseInt(process.env.MAX_WORKERS) ||
    parseInt(process.env.MAX_CONCURRENT_WRITES) ||
    parseInt(process.env.WORKER_POOL_SIZE);
  
  // Default: CPU threads minus 1 (leave 1 core for main thread)
  return envValue || Math.max(2, CPU_THREADS - 1);
}

// Row group size for Parquet files (affects read performance)
const ROW_GROUP_SIZE = parseInt(process.env.PARQUET_ROW_GROUP) || 100000;


export class ParquetWriterPool {
  constructor(maxWorkers, workerScript) {
    this.maxWorkers = maxWorkers;
    this.slots = maxWorkers;
    this.activeWorkers = new Set();
    this._persistentWorkers = [];   // Persistent worker pool
    this._idleWorkers = [];         // Workers ready for a job
    this._workerScript = workerScript || WORKER_SCRIPT;
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
      workersSpawned: 0,
    };
    this.initialized = false;
  }

  async init() {
    if (this.initialized) return;
    console.log(
      `🔧 Initializing Parquet writer pool with ${this.maxWorkers} persistent threads ` +
      `(CPU: ${CPU_THREADS}, ROW_GROUP_SIZE: ${ROW_GROUP_SIZE})`
    );
    
    // Spawn persistent workers
    for (let i = 0; i < this.maxWorkers; i++) {
      this._spawnPersistentWorker();
    }
    
    this.initialized = true;
  }

  /**
   * Spawn a single persistent worker that stays alive and processes jobs via messages.
   */
  _spawnPersistentWorker() {
    const worker = new Worker(this._workerScript, { workerData: null });
    this.stats.workersSpawned++;
    
    worker._busy = false;
    worker._resolve = null;
    worker._reject = null;
    
    worker.on('message', (msg) => {
      const resolve = worker._resolve;
      const reject = worker._reject;
      worker._resolve = null;
      worker._reject = null;
      worker._busy = false;
      this.activeWorkers.delete(worker);
      this._idleWorkers.push(worker);
      this.slots++;
      
      if (msg.ok) {
        this.stats.completedJobs++;
        this.stats.totalRecords += msg.count || 0;
        this.stats.totalBytes += msg.bytes || 0;
        
        if (msg.validation) {
          this.stats.validatedFiles++;
          if (!msg.validation.valid) {
            this.stats.validationFailures++;
            if (this.stats.validationIssues.length < 10) {
              this.stats.validationIssues.push({
                file: path.basename(msg.filePath || ''),
                issues: msg.validation.issues,
              });
            }
            console.warn(`⚠️ Parquet validation failed: ${path.basename(msg.filePath || '')} - ${msg.validation.issues.join(', ')}`);
          }
        }
        
        resolve?.(msg);
      } else {
        this.stats.failedJobs++;
        reject?.(new Error(msg.error || 'Worker error'));
      }
      
      this._pump();
    });
    
    worker.on('error', (err) => {
      const reject = worker._reject;
      worker._resolve = null;
      worker._reject = null;
      worker._busy = false;
      
      // Remove crashed worker and replace it
      this._removePersistentWorker(worker);
      this.stats.failedJobs++;
      reject?.(err);
      
      // Spawn replacement
      this._spawnPersistentWorker();
      this._pump();
    });
    
    worker.on('exit', (code) => {
      if (code !== 0 && !this._shuttingDown) {
        console.error(`❌ Persistent parquet worker exited with code ${code}, replacing...`);
        this._removePersistentWorker(worker);
        
        const reject = worker._reject;
        if (reject) {
          this.stats.failedJobs++;
          reject(new Error(`Worker crashed with code ${code}`));
        }
        
        // Spawn replacement
        this._spawnPersistentWorker();
        this._pump();
      }
    });
    
    this._persistentWorkers.push(worker);
    this._idleWorkers.push(worker);
  }

  _removePersistentWorker(worker) {
    const idx = this._persistentWorkers.indexOf(worker);
    if (idx !== -1) this._persistentWorkers.splice(idx, 1);
    const idleIdx = this._idleWorkers.indexOf(worker);
    if (idleIdx !== -1) this._idleWorkers.splice(idleIdx, 1);
  }

  /**
   * Enqueue a write job
   * 
   * @param {object} job - { type, filePath, records }
   * @returns {Promise<object>} - { ok, filePath, count, bytes }
   */
  /**
   * Enqueue a write job with retry logic
   * 
   * @param {object} job - { type, filePath, records }
   * @param {number} maxRetries - Maximum retry attempts (default: 3)
   * @returns {Promise<object>} - { ok, filePath, count, bytes }
   */
  async writeJob(job, maxRetries = 3) {
    this.stats.totalJobs++;

    // Attach row group size config
    job.rowGroupSize = ROW_GROUP_SIZE;

    let lastError;
    for (let attempt = 0; attempt < maxRetries; attempt++) {
      try {
        return await this._executeJob(job);
      } catch (err) {
        lastError = err;
        const isTransient = this._isTransientError(err);
        
        if (!isTransient || attempt >= maxRetries - 1) {
          // Non-transient error or exhausted retries
          throw err;
        }
        
        const delay = Math.min(1000 * Math.pow(2, attempt), 10000) + Math.random() * 500;
        console.log(`   ⏳ Parquet write retry (attempt ${attempt + 1}/${maxRetries}): ${err.message}. Retrying in ${Math.round(delay)}ms...`);
        await new Promise(r => setTimeout(r, delay));
      }
    }
    throw lastError;
  }

  /**
   * Check if an error is transient and should be retried
   */
  _isTransientError(err) {
    const msg = err.message || '';
    const transientPatterns = [
      /resource busy/i,
      /disk full/i,
      /no space left/i,
      /ENOSPC/i,
      /EMFILE/i,           // Too many open files
      /ENFILE/i,           // File table overflow
      /EAGAIN/i,           // Resource temporarily unavailable
      /EBUSY/i,            // Device or resource busy
      /timeout/i,
      /timed out/i,
      /worker crashed/i,
    ];
    return transientPatterns.some(p => p.test(msg));
  }

  /**
   * Execute a single write job (no retry)
   */
  _executeJob(job) {
    return new Promise((resolve, reject) => {
      this.queue.push({ job, resolve, reject });
      this._pump();
    });
  }

  /**
   * Process queued jobs with available workers
   */
  _pump() {
    while (this._idleWorkers.length > 0 && this.queue.length > 0) {
      const worker = this._idleWorkers.shift();
      this.slots--;
      const { job, resolve, reject } = this.queue.shift();
      
      worker._busy = true;
      worker._resolve = resolve;
      worker._reject = reject;
      this.activeWorkers.add(worker);
      
      // Send job to persistent worker via message passing
      worker.postMessage(job);
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
    this._shuttingDown = true;
    await this.drain();
    
    // Terminate all persistent workers
    for (const worker of this._persistentWorkers) {
      try { worker.terminate(); } catch {}
    }
    this._persistentWorkers = [];
    this._idleWorkers = [];
    this.activeWorkers.clear();
    
    console.log("🔧 Parquet writer pool shut down");
    console.log("📊 Final Parquet stats:", this.getStats());
  }
}

// Singleton instance
let poolInstance = null;

export function getParquetWriterPool(sizeOverride) {
  if (!poolInstance) {
    // Read env vars NOW (after dotenv has loaded), not at module import time
    const finalSize = sizeOverride || getMaxWorkersFromEnv();
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
