/**
 * Worker Pool for Compression
 * 
 * Manages a pool of worker threads for parallel compression.
 * Distributes work across threads and handles backpressure.
 */

import { Worker } from 'worker_threads';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import os from 'os';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const WORKER_SCRIPT = join(__dirname, 'compression-worker.js');

// Default pool size = CPU cores - 1 (leave one for main thread)
// Can be overridden with WORKER_POOL_SIZE or MAX_CONCURRENT_WRITES env vars
const DEFAULT_POOL_SIZE = parseInt(process.env.WORKER_POOL_SIZE) || parseInt(process.env.MAX_CONCURRENT_WRITES) || Math.max(2, os.cpus().length - 1);

class WorkerPool {
  constructor(size = DEFAULT_POOL_SIZE) {
    this.size = size;
    this.workers = [];
    this.taskQueue = [];
    this.pendingTasks = new Map(); // id -> { resolve, reject }
    this.taskIdCounter = 0;
    this.busyWorkers = new Set();
    this.initialized = false;
  }

  /**
   * Initialize the worker pool
   */
  async init() {
    if (this.initialized) return;
    
    console.log(`🔧 Initializing worker pool with ${this.size} threads`);
    
    for (let i = 0; i < this.size; i++) {
      const worker = new Worker(WORKER_SCRIPT);
      
      worker.on('message', (result) => {
        this.handleResult(worker, result);
      });
      
      worker.on('error', (err) => {
        console.error(`Worker error:`, err.message);
        this.handleWorkerError(worker, err);
      });
      
      worker.on('exit', (code) => {
        if (code !== 0) {
          console.error(`Worker exited with code ${code}`);
        }
        this.removeWorker(worker);
      });
      
      this.workers.push(worker);
    }
    
    this.initialized = true;
  }

  /**
   * Handle completed task result
   */
  handleResult(worker, result) {
    const { id, success, data, error, originalRows } = result;
    const pending = this.pendingTasks.get(id);
    
    if (pending) {
      this.pendingTasks.delete(id);
      
      if (success) {
        pending.resolve({ data, originalRows });
      } else {
        pending.reject(new Error(error));
      }
    }
    
    // Mark worker as available
    this.busyWorkers.delete(worker);
    
    // Process next task if any
    this.processQueue();
  }

  /**
   * Handle worker error
   */
  handleWorkerError(worker, err) {
    // Reject all pending tasks for this worker
    for (const [id, pending] of this.pendingTasks) {
      pending.reject(err);
    }
    this.busyWorkers.delete(worker);
  }

  /**
   * Remove dead worker
   */
  removeWorker(worker) {
    const index = this.workers.indexOf(worker);
    if (index > -1) {
      this.workers.splice(index, 1);
    }
    this.busyWorkers.delete(worker);
  }

  /**
   * Get an available worker
   */
  getAvailableWorker() {
    for (const worker of this.workers) {
      if (!this.busyWorkers.has(worker)) {
        return worker;
      }
    }
    return null;
  }

  /**
   * Process queued tasks
   */
  processQueue() {
    while (this.taskQueue.length > 0) {
      const worker = this.getAvailableWorker();
      if (!worker) break;
      
      const task = this.taskQueue.shift();
      this.busyWorkers.add(worker);
      worker.postMessage(task);
    }
  }

  /**
   * Compress rows using worker thread
   * Returns Promise<{ data: Buffer, originalRows: number }>
   */
  async compress(rows, level = 1) {
    if (!this.initialized) {
      await this.init();
    }
    
    const id = ++this.taskIdCounter;
    
    return new Promise((resolve, reject) => {
      this.pendingTasks.set(id, { resolve, reject });
      
      const task = { id, rows, level };
      
      // Try to assign to available worker immediately
      const worker = this.getAvailableWorker();
      if (worker) {
        this.busyWorkers.add(worker);
        worker.postMessage(task);
      } else {
        // Queue the task
        this.taskQueue.push(task);
      }
    });
  }

  /**
   * Get pool statistics
   */
  getStats() {
    return {
      totalWorkers: this.workers.length,
      busyWorkers: this.busyWorkers.size,
      queuedTasks: this.taskQueue.length,
      pendingTasks: this.pendingTasks.size
    };
  }

  /**
   * Wait for all pending tasks to complete
   */
  async drain() {
    while (this.pendingTasks.size > 0 || this.taskQueue.length > 0) {
      await new Promise(r => setTimeout(r, 50));
    }
  }

  /**
   * Shutdown all workers
   */
  async shutdown() {
    await this.drain();
    
    for (const worker of this.workers) {
      await worker.terminate();
    }
    
    this.workers = [];
    this.initialized = false;
    console.log('🔧 Worker pool shut down');
  }
}

// Singleton instance
let poolInstance = null;

export function getWorkerPool(size) {
  if (!poolInstance) {
    poolInstance = new WorkerPool(size);
  }
  return poolInstance;
}

export async function shutdownPool() {
  if (poolInstance) {
    await poolInstance.shutdown();
    poolInstance = null;
  }
}

export default WorkerPool;
