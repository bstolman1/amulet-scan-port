/**
 * Engine Worker - Background loop for indexing, ingestion, and aggregation
 * 
 * Runs in small batches to avoid OOM:
 * 1. Scan for new files → index them
 * 2. Ingest a few files
 * 3. Update aggregations if there's new data
 */

import { scanAndIndexFiles, getPendingFileCount } from './file-index.js';
import { ingestNewFiles, getIngestionStats } from './ingest.js';
import { updateAllAggregations, hasNewData } from './aggregations.js';
import { initEngineSchema } from './schema.js';

const WORKER_INTERVAL_MS = parseInt(process.env.ENGINE_INTERVAL_MS || '30000', 10);
const FILES_PER_CYCLE = parseInt(process.env.ENGINE_FILES_PER_CYCLE || '3', 10);

let running = false;
let workerInterval = null;
let lastStats = null;

/**
 * Single worker cycle
 */
async function runCycle() {
  if (running) {
    console.log('⏭️ Engine cycle already running, skipping...');
    return;
  }
  
  running = true;
  const startTime = Date.now();
  
  try {
    // Step 1: Index new files
    const { newFiles } = await scanAndIndexFiles();
    
    // Step 2: Ingest files (small batch)
    const { ingested, records } = await ingestNewFiles(FILES_PER_CYCLE);
    
    // Step 3: Update aggregations if new data
    if (ingested > 0) {
      const aggResults = await updateAllAggregations();
      lastStats = {
        ...aggResults,
        lastCycle: new Date().toISOString(),
        cycleDuration: Date.now() - startTime,
      };
    }
    
    // Log progress
    const pending = await getPendingFileCount();
    if (ingested > 0 || newFiles > 0) {
      console.log(`⚙️ Engine cycle: indexed=${newFiles}, ingested=${ingested} (${records} records), pending=${pending}, ${Date.now() - startTime}ms`);
    }
    
  } catch (err) {
    console.error('❌ Engine worker error:', err.message);
  } finally {
    running = false;
  }
}

/**
 * Start the background engine worker
 */
export async function startEngineWorker() {
  console.log('⚙️ Starting warehouse engine worker...');
  console.log(`   Interval: ${WORKER_INTERVAL_MS}ms, Files/cycle: ${FILES_PER_CYCLE}`);
  
  // Initialize schema
  await initEngineSchema();
  
  // Run initial cycle
  await runCycle();
  
  // Schedule periodic runs
  workerInterval = setInterval(runCycle, WORKER_INTERVAL_MS);
  
  console.log('✅ Engine worker started');
}

/**
 * Stop the engine worker
 */
export function stopEngineWorker() {
  if (workerInterval) {
    clearInterval(workerInterval);
    workerInterval = null;
    console.log('🛑 Engine worker stopped');
  }
}

/**
 * Manually trigger a cycle
 */
export async function triggerCycle() {
  return runCycle();
}

/**
 * Get engine status
 */
export async function getEngineStatus() {
  const stats = await getIngestionStats();
  const pending = await getPendingFileCount();
  
  return {
    running: !!workerInterval,
    processing: running,
    pendingFiles: pending,
    ...stats,
    lastStats,
  };
}
