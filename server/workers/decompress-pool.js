/**
 * Decompress Worker Pool
 * 
 * Manages a pool of worker threads for parallel ZSTD decompression.
 * Distributes file processing across workers for true CPU parallelism.
 */

import { Worker } from 'worker_threads';
import path from 'path';
import { fileURLToPath } from 'url';
import os from 'os';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

class DecompressPool {
  constructor(options = {}) {
    // Default to CPU cores - 1, minimum 2, maximum 16
    const cpuCount = os.cpus().length;
    this.size = Math.min(Math.max(options.size || cpuCount - 1, 2), 16);
    this.workers = [];
    this.available = [];
    this.pending = new Map(); // id -> { resolve, reject }
    this.taskId = 0;
    this.initialized = false;
    this.initPromise = null;
    this.filterTemplate = options.filterTemplate || null;
    
    // Stats
    this.stats = {
      filesProcessed: 0,
      totalEvents: 0,
      errors: 0,
    };
  }
  
  async init() {
    if (this.initialized) return;
    if (this.initPromise) return this.initPromise;
    
    this.initPromise = this._init();
    return this.initPromise;
  }
  
  async _init() {
    const workerPath = path.join(__dirname, 'decompress-worker.js');
    
    const workerPromises = [];
    
    for (let i = 0; i < this.size; i++) {
      workerPromises.push(this._createWorker(workerPath, i));
    }
    
    await Promise.all(workerPromises);
    this.initialized = true;
    console.log(`[DecompressPool] Initialized with ${this.size} workers`);
  }
  
  async _createWorker(workerPath, index) {
    return new Promise((resolve, reject) => {
      const worker = new Worker(workerPath);
      
      worker.on('message', (msg) => {
        if (msg.ready) {
          this.workers.push(worker);
          this.available.push(worker);
          resolve();
          return;
        }
        
        // Handle task completion
        const { id, success, events, error, filePath } = msg;
        const pending = this.pending.get(id);
        
        if (pending) {
          this.pending.delete(id);
          this.available.push(worker);
          
          if (success) {
            this.stats.filesProcessed++;
            this.stats.totalEvents += events.length;
            pending.resolve({ events, filePath });
          } else {
            this.stats.errors++;
            pending.resolve({ events: [], filePath, error }); // Don't reject, just return empty
          }
          
          // Process next in queue if any
          this._processQueue();
        }
      });
      
      worker.on('error', (err) => {
        console.error(`[DecompressPool] Worker ${index} error:`, err.message);
        // Remove from available and try to recover
        const availIdx = this.available.indexOf(worker);
        if (availIdx !== -1) this.available.splice(availIdx, 1);
      });
      
      worker.on('exit', (code) => {
        if (code !== 0) {
          console.error(`[DecompressPool] Worker ${index} exited with code ${code}`);
        }
        // Remove from all lists
        const wIdx = this.workers.indexOf(worker);
        if (wIdx !== -1) this.workers.splice(wIdx, 1);
        const aIdx = this.available.indexOf(worker);
        if (aIdx !== -1) this.available.splice(aIdx, 1);
      });
    });
  }
  
  // Queue for tasks when all workers are busy
  _queue = [];
  
  _processQueue() {
    while (this._queue.length > 0 && this.available.length > 0) {
      const task = this._queue.shift();
      this._dispatchTask(task);
    }
  }
  
  _dispatchTask(task) {
    const worker = this.available.pop();
    if (!worker) {
      this._queue.push(task);
      return;
    }
    
    worker.postMessage({
      id: task.id,
      filePath: task.filePath,
      filterTemplate: this.filterTemplate,
    });
  }
  
  /**
   * Process a single file and return events
   */
  async processFile(filePath) {
    await this.init();
    
    return new Promise((resolve, reject) => {
      const id = ++this.taskId;
      
      this.pending.set(id, { resolve, reject });
      
      const task = { id, filePath };
      
      if (this.available.length > 0) {
        this._dispatchTask(task);
      } else {
        this._queue.push(task);
      }
    });
  }
  
  /**
   * Process multiple files with bounded in-flight work (prevents 60k+ promises)
   * Calls onProgress as each file completes.
   */
  async processFiles(filePaths, onProgress) {
    await this.init();

    const results = [];
    const total = filePaths.length;
    let completed = 0;

    // Keep a small multiple of worker count in-flight to avoid huge memory usage.
    const maxInFlight = Math.max(this.size * 2, 4);

    let idx = 0;
    const inFlight = new Set();

    const launchOne = (filePath) => {
      const p = this.processFile(filePath)
        .then((result) => {
          completed++;
          if (onProgress) {
            onProgress({
              completed,
              total,
              percent: Math.round((completed / total) * 100),
              ...result,
            });
          }
          results.push(result);
          return result;
        })
        .finally(() => {
          inFlight.delete(p);
        });

      inFlight.add(p);
    };

    // Prime the pump
    while (idx < total && inFlight.size < maxInFlight) {
      launchOne(filePaths[idx++]);
    }

    // Drain
    while (inFlight.size > 0) {
      await Promise.race(inFlight);
      while (idx < total && inFlight.size < maxInFlight) {
        launchOne(filePaths[idx++]);
      }
    }

    return results;
  }
  
  /**
   * Get pool statistics
   */
  getStats() {
    return {
      ...this.stats,
      poolSize: this.size,
      activeWorkers: this.workers.length,
      availableWorkers: this.available.length,
      pendingTasks: this.pending.size,
      queuedTasks: this._queue.length,
    };
  }
  
  /**
   * Reset statistics
   */
  resetStats() {
    this.stats = {
      filesProcessed: 0,
      totalEvents: 0,
      errors: 0,
    };
  }
  
  /**
   * Shutdown all workers
   */
  async shutdown() {
    console.log('[DecompressPool] Shutting down...');
    
    // Reject all pending tasks
    for (const [id, { reject }] of this.pending) {
      reject(new Error('Pool shutdown'));
    }
    this.pending.clear();
    this._queue = [];
    
    // Terminate all workers
    const terminatePromises = this.workers.map(w => w.terminate());
    await Promise.all(terminatePromises);
    
    this.workers = [];
    this.available = [];
    this.initialized = false;
    this.initPromise = null;
    
    console.log('[DecompressPool] Shutdown complete');
  }
}

// Singleton instance for VoteRequest scanning
let voteRequestPool = null;

export function getVoteRequestPool() {
  if (!voteRequestPool) {
    voteRequestPool = new DecompressPool({
      filterTemplate: 'VoteRequest',
    });
  }
  return voteRequestPool;
}

export function createPool(options = {}) {
  return new DecompressPool(options);
}

export default DecompressPool;
