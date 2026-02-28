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
 *
 * FIXES APPLIED:
 *
 * FIX #1  Double-respawn on worker crash
 *         Both 'error' and 'exit' handlers called _spawnPersistentWorker() on every
 *         crash, growing the pool by 2 workers instead of 1. Over time the pool
 *         grew unboundedly. Fixed by setting worker._crashed = true in 'error' and
 *         checking it in 'exit' so only one handler spawns the replacement.
 *
 * FIX #2  'slots' counter never restored on worker crash
 *         slots was decremented in _pump() but only incremented in the 'message'
 *         handler. On crash, the slot was permanently lost — slots went negative
 *         after enough crashes, causing incorrect backpressure reporting to callers.
 *         The 'error' and 'exit' handlers now restore the slot when a busy worker
 *         crashes.
 *
 * FIX #3  writeJob mutates the caller's job object
 *         `job.rowGroupSize = ROW_GROUP_SIZE` modified the object passed in by the
 *         caller. Retried or reused job objects got rowGroupSize stamped permanently.
 *         Now assigns to a shallow copy before mutating.
 *
 * FIX #4  drain() has no timeout — hangs shutdown() on stuck worker
 *         If a worker deadlocked (DuckDB hang, GCS stall), drain() polled forever.
 *         shutdown() called drain() before terminate(), so a single hung worker
 *         blocked clean process exit indefinitely. drain() now accepts a timeout
 *         (default 30s) and resolves when either the queue drains or the deadline
 *         passes. shutdown() always proceeds to terminate() regardless.
 *
 * FIX #5  ENOSPC / disk full classified as transient (retryable)
 *         Retrying on a full disk wastes up to ~11s before failing — disk space
 *         does not free itself. ENOSPC, 'disk full', and 'no space left' are now
 *         classified as fatal and thrown immediately without retry.
 *
 * FIX #6  stats.validationIssues keeps first 10, not most recent 10
 *         Once 10 validation failures accumulated, all subsequent failures were
 *         silently dropped from the detail log. getValidationIssues() returned a
 *         frozen snapshot of the first 10 failures rather than recent ones. Now
 *         keeps the most recent 10 (shift oldest when at capacity).
 *
 * FIX #7  mbWritten computed as string, then divided as number
 *         `const mbWritten = (...).toFixed(2)` returned a string. The subsequent
 *         `mbWritten / elapsed` coerced it back to a number implicitly — a type
 *         hazard. mbWritten is now computed as a number and stringified only in
 *         the returned object.
 *
 * FIX #8  _respawnCrashes and _shuttingDown not initialised in constructor
 *         Both were implicit fields set lazily or in other methods. Moved to
 *         constructor for explicit class invariants.
 *
 * FIX #9  Duplicate JSDoc block before writeJob removed
 *         A stale "Enqueue a write job" comment (without retry docs) immediately
 *         preceded the correct JSDoc. Dead documentation removed.
 *
 * FIX #10 Default export removed
 *         Exporting the class as default alongside getParquetWriterPool() allowed
 *         callers to bypass the singleton and spawn duplicate worker pools.
 *         Named exports only; use getParquetWriterPool() as the entry point.
 *
 * FIX #11 _pump() called at end of init()
 *         Jobs enqueued before init() resolved (rare but possible) would sit in
 *         the queue until the next external _pump() trigger. init() now calls
 *         _pump() after all workers are spawned.
 */

import { Worker } from 'node:worker_threads';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

const WORKER_SCRIPT = path.join(__dirname, 'parquet-worker.js');

// ─── Performance Configuration ─────────────────────────────────────────────

const CPU_THREADS = os.cpus().length;

/**
 * Read worker count from environment at pool-creation time, not module load time.
 * ESM hoists imports before dotenv.config() runs, so env vars must be read lazily.
 */
function getMaxWorkersFromEnv() {
  // Priority: PARQUET_WORKERS > MAX_WORKERS > MAX_CONCURRENT_WRITES > WORKER_POOL_SIZE
  const envValue =
    parseInt(process.env.PARQUET_WORKERS) ||
    parseInt(process.env.MAX_WORKERS) ||
    parseInt(process.env.MAX_CONCURRENT_WRITES) ||
    parseInt(process.env.WORKER_POOL_SIZE);

  // Default: half of CPU threads, capped at 6 to avoid DuckDB concurrency issues
  return envValue || Math.min(6, Math.max(2, Math.floor(CPU_THREADS / 2)));
}

const ROW_GROUP_SIZE = parseInt(process.env.PARQUET_ROW_GROUP) || 100000;

// ─── Pool ──────────────────────────────────────────────────────────────────

export class ParquetWriterPool {
  constructor(maxWorkers, workerScript) {
    this.maxWorkers         = maxWorkers;
    this.slots              = maxWorkers;
    this.activeWorkers      = new Set();
    this._persistentWorkers = [];
    this._idleWorkers       = [];
    this._workerScript      = workerScript || WORKER_SCRIPT;
    this.queue              = [];
    this.startTime          = Date.now();

    // FIX #8: initialise all fields explicitly in the constructor
    this._respawnCrashes = [];
    this._shuttingDown   = false;

    this.stats = {
      totalJobs:          0,
      completedJobs:      0,
      failedJobs:         0,
      totalRecords:       0,
      totalBytes:         0,
      validatedFiles:     0,
      validationFailures: 0,
      validationIssues:   [],   // rolling buffer of last 10 (FIX #6)
      workersSpawned:     0,
    };
    this.initialized = false;
  }

  async init() {
    if (this.initialized) return;
    console.log(
      `🔧 Initializing Parquet writer pool with ${this.maxWorkers} persistent threads ` +
      `(CPU: ${CPU_THREADS}, ROW_GROUP_SIZE: ${ROW_GROUP_SIZE})`
    );

    for (let i = 0; i < this.maxWorkers; i++) {
      this._spawnPersistentWorker();
    }

    this.initialized = true;
    // FIX #11: drain any jobs queued before init() completed
    this._pump();
  }

  /**
   * Spawn a single persistent worker.
   * Includes respawn guard: if a slot crashes >5 times in 60s, stop respawning.
   *
   * FIX #1: worker._crashed flag prevents both 'error' and 'exit' from each
   *   spawning a replacement — only the first handler to fire does the respawn.
   * FIX #2: slots counter restored in error/exit handlers so it never goes negative.
   */
  _spawnPersistentWorker() {
    // Respawn guard — only enforced during respawns, not initial pool boot
    if (this.initialized) {
      const now = Date.now();
      this._respawnCrashes = this._respawnCrashes.filter(t => now - t < 60000);
      if (this._respawnCrashes.length >= 5) {
        console.error(
          `🚨 FATAL: Worker respawn loop detected (${this._respawnCrashes.length} crashes in 60s). Stopping respawn.`
        );
        return;
      }
      this._respawnCrashes.push(now);
    }

    const worker = new Worker(this._workerScript, { workerData: null });
    this.stats.workersSpawned++;

    worker._busy    = false;
    worker._resolve = null;
    worker._reject  = null;
    // FIX #1: flag set by whichever crash handler fires first
    worker._crashed = false;

    worker.on('message', (msg) => {
      const resolve = worker._resolve;
      const reject  = worker._reject;
      worker._resolve = null;
      worker._reject  = null;
      worker._busy    = false;
      this.activeWorkers.delete(worker);
      this._idleWorkers.push(worker);
      this.slots++;

      if (msg.ok) {
        this.stats.completedJobs++;
        this.stats.totalRecords += msg.count || 0;
        this.stats.totalBytes   += msg.bytes  || 0;

        if (msg.validation) {
          this.stats.validatedFiles++;
          if (!msg.validation.valid) {
            this.stats.validationFailures++;
            // FIX #6: rolling buffer — keep most recent 10, not first 10
            if (this.stats.validationIssues.length >= 10) {
              this.stats.validationIssues.shift();
            }
            this.stats.validationIssues.push({
              file:   path.basename(msg.filePath || ''),
              issues: msg.validation.issues,
            });
            console.warn(
              `⚠️ Parquet validation failed: ${path.basename(msg.filePath || '')} - ` +
              msg.validation.issues.join(', ')
            );
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
      // FIX #1: mark crashed so 'exit' does not also spawn a replacement
      if (worker._crashed) return;
      worker._crashed = true;

      const reject = worker._reject;
      worker._resolve = null;
      worker._reject  = null;

      // FIX #2: restore the slot if this worker was busy when it crashed
      if (worker._busy) {
        this.slots++;
        this.activeWorkers.delete(worker);
      }
      worker._busy = false;

      this._removePersistentWorker(worker);
      this.stats.failedJobs++;
      reject?.(err);

      if (!this._shuttingDown) {
        this._spawnPersistentWorker();
        this._pump();
      }
    });

    worker.on('exit', (code) => {
      if (this._shuttingDown) return;
      if (code === 0) return;

      // FIX #1: if 'error' already handled this crash, skip respawn
      if (worker._crashed) return;
      worker._crashed = true;

      console.error(`❌ Persistent parquet worker exited with code ${code}, replacing...`);
      this._removePersistentWorker(worker);

      // FIX #2: restore slot if worker was busy when it exited unexpectedly
      if (worker._busy) {
        this.slots++;
        this.activeWorkers.delete(worker);
      }
      worker._busy = false;

      const reject = worker._reject;
      if (reject) {
        worker._resolve = null;
        worker._reject  = null;
        this.stats.failedJobs++;
        reject(new Error(`Worker crashed with exit code ${code}`));
      }

      this._spawnPersistentWorker();
      this._pump();
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
   * Enqueue a write job with retry logic.
   *
   * FIX #3: job is shallow-copied before mutation so the caller's object is
   *   never modified (previously `job.rowGroupSize = ...` stamped the caller's obj).
   * FIX #5: ENOSPC / disk-full errors are classified as fatal and thrown immediately
   *   rather than retried — disk space does not free itself between attempts.
   *
   * @param {object} job        - { type, filePath, records }
   * @param {number} maxRetries - Maximum retry attempts (default: 3)
   * @returns {Promise<object>} - { ok, filePath, count, bytes }
   */
  async writeJob(job, maxRetries = 3) {
    this.stats.totalJobs++;

    // FIX #3: shallow-copy before mutating — do not stamp caller's object
    const jobCopy = { ...job, rowGroupSize: ROW_GROUP_SIZE };

    let lastError;
    for (let attempt = 0; attempt < maxRetries; attempt++) {
      try {
        return await this._executeJob(jobCopy);
      } catch (err) {
        lastError = err;

        // FIX #5: fatal errors must not be retried
        if (this._isFatalError(err)) throw err;

        const isTransient = this._isTransientError(err);
        if (!isTransient || attempt >= maxRetries - 1) throw err;

        const delay = Math.min(1000 * Math.pow(2, attempt), 10000) + Math.random() * 500;
        console.log(
          `   ⏳ Parquet write retry (attempt ${attempt + 1}/${maxRetries}): ` +
          `${err.message}. Retrying in ${Math.round(delay)}ms...`
        );
        await new Promise(r => setTimeout(r, delay));
      }
    }
    throw lastError;
  }

  /**
   * Errors that should NEVER be retried — retrying cannot fix them.
   *
   * FIX #5: ENOSPC / disk full was previously classified as transient,
   *   wasting up to ~11s of retries before failing. Disk space doesn't free
   *   itself between attempts.
   */
  _isFatalError(err) {
    const msg = err.message || '';
    return (
      /ENOSPC/i.test(msg)       ||
      /disk full/i.test(msg)    ||
      /no space left/i.test(msg)
    );
  }

  /**
   * Transient errors that are safe to retry.
   * NOTE: disk-full patterns deliberately excluded here (checked in _isFatalError first).
   */
  _isTransientError(err) {
    const msg = err.message || '';
    const transientPatterns = [
      /resource busy/i,
      /EMFILE/i,       // Too many open files
      /ENFILE/i,       // File table overflow
      /EAGAIN/i,       // Resource temporarily unavailable
      /EBUSY/i,        // Device or resource busy
      /timeout/i,
      /timed out/i,
      /worker crashed/i,
    ];
    return transientPatterns.some(p => p.test(msg));
  }

  /**
   * Execute a single write job (no retry).
   */
  _executeJob(job) {
    return new Promise((resolve, reject) => {
      this.queue.push({ job, resolve, reject });
      this._pump();
    });
  }

  /**
   * Dispatch queued jobs to idle workers.
   */
  _pump() {
    while (this._idleWorkers.length > 0 && this.queue.length > 0) {
      const worker             = this._idleWorkers.shift();
      const { job, resolve, reject } = this.queue.shift();
      this.slots--;

      worker._busy    = true;
      worker._resolve = resolve;
      worker._reject  = reject;
      this.activeWorkers.add(worker);

      worker.postMessage(job);
    }
  }

  getStats() {
    const elapsed = (Date.now() - this.startTime) / 1000;

    // FIX #7: compute mbWritten as a number first; stringify only in the return object
    const mbWritten  = this.stats.totalBytes / (1024 * 1024);
    const mbPerSec   = elapsed > 0 ? mbWritten / elapsed : 0;
    const filesPerSec = elapsed > 0 ? this.stats.completedJobs / elapsed : 0;

    return {
      ...this.stats,
      activeWorkers:  this.activeWorkers.size,
      queuedJobs:     this.queue.length,
      availableSlots: this.slots,
      mbWritten:      mbWritten.toFixed(2),
      mbPerSec:       mbPerSec.toFixed(2),
      filesPerSec:    filesPerSec.toFixed(2),
      elapsedSec:     elapsed.toFixed(1),
      validationRate: this.stats.validatedFiles > 0
        ? (
            (this.stats.validatedFiles - this.stats.validationFailures) /
            this.stats.validatedFiles * 100
          ).toFixed(1) + '%'
        : 'N/A',
    };
  }

  /**
   * Get recent validation issues (rolling last-10 buffer).
   */
  getValidationIssues() {
    return this.stats.validationIssues;
  }

  /**
   * Wait for all queued and active jobs to complete.
   *
   * FIX #4: now accepts a timeout (default 30s). Resolves when the queue is
   *   empty OR when the deadline passes — whichever comes first. shutdown()
   *   always proceeds to terminate() regardless of whether drain timed out.
   *
   * @param {number} timeoutMs - Maximum ms to wait (default: 30000)
   * @returns {Promise<boolean>} - true if fully drained, false if timed out
   */
  async drain(timeoutMs = 30000) {
    const deadline = Date.now() + timeoutMs;
    while (this.queue.length > 0 || this.activeWorkers.size > 0) {
      if (Date.now() >= deadline) {
        console.warn(
          `⚠️ drain() timed out after ${timeoutMs}ms ` +
          `(${this.queue.length} queued, ${this.activeWorkers.size} active)`
        );
        return false;
      }
      await new Promise(r => setTimeout(r, 50));
    }
    return true;
  }

  async shutdown() {
    this._shuttingDown = true;

    // FIX #4: drain with a deadline — never block terminate() on a stuck worker
    const drained = await this.drain(30000);
    if (!drained) {
      console.warn('⚠️ Shutdown proceeding with active workers (drain timed out).');
    }

    for (const worker of this._persistentWorkers) {
      try { worker.terminate(); } catch {}
    }
    this._persistentWorkers = [];
    this._idleWorkers       = [];
    this.activeWorkers.clear();

    console.log('🔧 Parquet writer pool shut down');
    console.log('📊 Final Parquet stats:', this.getStats());
  }
}

// ─── Singleton ─────────────────────────────────────────────────────────────

let poolInstance = null;

/**
 * Get (or create) the singleton ParquetWriterPool.
 * Reads env vars at call time (after dotenv.config() has run).
 *
 * Callers must await pool.init() before calling writeJob().
 */
export function getParquetWriterPool(sizeOverride) {
  if (!poolInstance) {
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

// FIX #10: Default export removed — it allowed callers to bypass the singleton
// and spawn duplicate worker pools. Use named imports only:
//   import { getParquetWriterPool } from './parquet-writer-pool.js';
