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
import { runGapDetection, getLastGapDetection } from './gap-detector.js';
import { buildVoteRequestIndex, isIndexingInProgress as isVoteIndexing, isIndexPopulated as isVoteIndexPopulated } from './vote-request-indexer.js';
import { 
  buildTemplateFileIndex, 
  isTemplateIndexingInProgress, 
  isTemplateIndexPopulated,
  getTemplateIndexStats,
} from './template-file-index.js';

const WORKER_INTERVAL_MS = parseInt(process.env.ENGINE_INTERVAL_MS || '30000', 10);
const FILES_PER_CYCLE = parseInt(process.env.ENGINE_FILES_PER_CYCLE || '3', 10);
const GAP_CHECK_INTERVAL = parseInt(process.env.GAP_CHECK_INTERVAL || '10', 10); // Check gaps every N cycles
const AUTO_RECOVER_GAPS = process.env.AUTO_RECOVER_GAPS !== 'false'; // Enable by default
const VOTE_INDEX_BUILD_ON_STARTUP = process.env.VOTE_INDEX_BUILD_ON_STARTUP !== 'false'; // Build on startup by default
const TEMPLATE_INDEX_BUILD_ON_STARTUP = process.env.TEMPLATE_INDEX_BUILD_ON_STARTUP !== 'false'; // Build on startup by default

let running = false;
let workerInterval = null;
let lastStats = null;
let cycleCount = 0;
let templateIndexBuildPromise = null; // Track background template index build
let voteIndexBuildPromise = null; // Track background vote index build

const CYCLE_TIMEOUT_MS = parseInt(process.env.ENGINE_CYCLE_TIMEOUT_MS || '300000', 10); // 5 min default

/**
 * Promise with timeout wrapper
 */
function withTimeout(promise, ms, label) {
  return Promise.race([
    promise,
    new Promise((_, reject) => 
      setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms)
    )
  ]);
}

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
    // Step 1: Index new files (with timeout)
    console.log('🔍 Scanning for new files...');
    const { newFiles } = await withTimeout(
      scanAndIndexFiles(), 
      CYCLE_TIMEOUT_MS, 
      'File scan'
    );
    console.log(`📁 Found ${newFiles} new files`);
    
    // Step 2: Ingest files (small batch, with timeout)
    console.log(`📥 Ingesting up to ${FILES_PER_CYCLE} files...`);
    const { ingested, records } = await withTimeout(
      ingestNewFiles(FILES_PER_CYCLE),
      CYCLE_TIMEOUT_MS,
      'File ingestion'
    );
    
    // Step 3: Update aggregations if new data
    if (ingested > 0) {
      console.log('📊 Updating aggregations...');
      const aggResults = await withTimeout(
        updateAllAggregations(),
        CYCLE_TIMEOUT_MS,
        'Aggregation update'
      );
      lastStats = {
        ...aggResults,
        lastCycle: new Date().toISOString(),
        cycleDuration: Date.now() - startTime,
      };
    }
    
    // Step 4: Gap detection (periodic)
    cycleCount++;
    if (cycleCount % GAP_CHECK_INTERVAL === 0) {
      console.log('🔍 Running periodic gap detection...');
      try {
        const gapResult = await withTimeout(
          runGapDetection(AUTO_RECOVER_GAPS),
          CYCLE_TIMEOUT_MS,
          'Gap detection'
        );
        if (gapResult.gaps?.length > 0) {
          console.log(`   ⚠️ TIME GAPS: Found ${gapResult.totalGaps} gap(s), total: ${gapResult.totalGapTime}`);
        }
      } catch (err) {
        console.warn(`   ⚠️ Gap detection failed: ${err.message}`);
      }
    }
    
    // Log progress
    const pending = await getPendingFileCount();
    console.log(`✅ Engine cycle complete: indexed=${newFiles}, ingested=${ingested} (${records} records), pending=${pending}, ${Date.now() - startTime}ms`);
    
  } catch (err) {
    console.error('❌ Engine worker error:', err.message);
    if (err.stack) console.error(err.stack);
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
  console.log(`   Gap check: every ${GAP_CHECK_INTERVAL} cycles, Auto-recover: ${AUTO_RECOVER_GAPS}`);
  
  // Initialize schema
  await initEngineSchema();
  
  // Run initial cycle
  await runCycle();
  
  // Schedule periodic runs
  workerInterval = setInterval(runCycle, WORKER_INTERVAL_MS);
  
  console.log('✅ Engine worker started');
  
  // Start template index build first (vote index depends on it)
  if (TEMPLATE_INDEX_BUILD_ON_STARTUP) {
    startTemplateIndexBuild();
  }
  
  // Vote request index will be started after template index completes (if needed)
}

/**
 * Start template index build as a background task
 * Once complete, starts vote index build if configured
 */
async function startTemplateIndexBuild() {
  if (templateIndexBuildPromise || isTemplateIndexingInProgress()) {
    console.log('⏭️ Template index build already in progress');
    return;
  }
  
  const populated = await isTemplateIndexPopulated();
  if (populated) {
    console.log('✅ Template index already populated, skipping build');
    // Start vote index build if configured
    if (VOTE_INDEX_BUILD_ON_STARTUP) {
      startVoteIndexBuild();
    }
    return;
  }
  
  console.log('📑 Starting background template index build...');
  console.log('   This will dramatically speed up VoteRequest index builds');
  
  templateIndexBuildPromise = buildTemplateFileIndex({ force: false, incremental: true })
    .then(result => {
      console.log(`✅ Template index complete: ${result.filesIndexed} files, ${result.templatesFound} mappings in ${result.elapsedSeconds}s`);
      templateIndexBuildPromise = null;
      
      // Now start vote index build if configured
      if (VOTE_INDEX_BUILD_ON_STARTUP) {
        startVoteIndexBuild();
      }
    })
    .catch(err => {
      console.error('❌ Template index build failed:', err.message);
      templateIndexBuildPromise = null;
    });
}

/**
 * Start vote request index build as a background task (no timeout)
 * This runs independently of the main worker cycle
 */
async function startVoteIndexBuild() {
  // Check if already running
  if (voteIndexBuildPromise || isVoteIndexing()) {
    console.log('⏭️ Vote request index build already in progress');
    return;
  }
  
  // Check if index is already populated
  const populated = await isVoteIndexPopulated();
  if (populated) {
    console.log('✅ Vote request index already populated, skipping build');
    return;
  }
  
  console.log('🗳️ Starting background vote request index build (no timeout)...');
  console.log('   This may take 15-30 minutes for large datasets');
  
  voteIndexBuildPromise = buildVoteRequestIndex({ force: false })
    .then(result => {
      console.log(`✅ Vote request index build complete: ${result.totalIndexed} records indexed`);
      voteIndexBuildPromise = null;
    })
    .catch(err => {
      console.error('❌ Vote request index build failed:', err.message);
      voteIndexBuildPromise = null;
    });
}

/**
 * Manually trigger vote request index rebuild
 */
export async function triggerVoteIndexRebuild() {
  if (voteIndexBuildPromise || isVoteIndexing()) {
    return { success: false, message: 'Index build already in progress' };
  }
  
  console.log('🗳️ Manually triggered vote request index rebuild...');
  
  voteIndexBuildPromise = buildVoteRequestIndex({ force: true })
    .then(result => {
      console.log(`✅ Vote request index rebuild complete: ${result.totalIndexed} records indexed`);
      voteIndexBuildPromise = null;
      return result;
    })
    .catch(err => {
      console.error('❌ Vote request index rebuild failed:', err.message);
      voteIndexBuildPromise = null;
      throw err;
    });
  
  return { success: true, message: 'Index rebuild started in background' };
}

/**
 * Check if vote index is currently building
 */
export function isVoteIndexBuilding() {
  return !!voteIndexBuildPromise || isVoteIndexing();
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
  const gapInfo = getLastGapDetection();
  
  return {
    running: !!workerInterval,
    processing: running,
    pendingFiles: pending,
    ...stats,
    lastStats,
    gapDetection: gapInfo,
    voteIndexBuilding: isVoteIndexBuilding(),
  };
}

/**
 * Manually trigger gap detection
 */
export async function triggerGapDetection(autoRecover = false) {
  return runGapDetection(autoRecover);
}
