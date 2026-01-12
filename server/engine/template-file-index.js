/**
 * Template-to-File Index
 * 
 * Builds and maintains a persistent mapping of template names to the files that contain them.
 * This dramatically speeds up template-specific queries (like VoteRequest) by scanning only
 * relevant files instead of all 35K+ files.
 * 
 * Uses parallel worker threads for fast initial indexing (~10-15 min vs ~70 min sequential).
 * 
 * The index is stored in DuckDB and survives server restarts.
 */

import fs from 'fs';
import path from 'path';
import os from 'os';
import { Worker } from 'worker_threads';
import { query, queryOne, DATA_PATH } from '../duckdb/connection.js';
import { readBinaryFile, findBinaryFiles } from '../duckdb/binary-reader.js';

// Use process.cwd() for Vitest/Vite SSR compatibility
const WORKER_SCRIPT = path.join(process.cwd(), 'server', 'engine', 'template-index-worker.js');

// Worker pool size - default to CPU cores - 1, min 2, max 8
const WORKER_POOL_SIZE = Math.min(8, Math.max(2, parseInt(process.env.TEMPLATE_INDEX_WORKERS) || os.cpus().length - 1));
const FILES_PER_WORKER_BATCH = 100; // Files to process per worker message

let indexingInProgress = false;
let indexingProgress = { current: 0, total: 0, startTime: null };

/**
 * Ensure index tables exist
 */
async function ensureIndexTables() {
  // Template-to-file mapping table
  await query(`
    CREATE TABLE IF NOT EXISTS template_file_index (
      file_path VARCHAR NOT NULL,
      template_name VARCHAR NOT NULL,
      event_count INTEGER DEFAULT 0,
      first_event_at TIMESTAMP,
      last_event_at TIMESTAMP,
      PRIMARY KEY (file_path, template_name)
    )
  `);

  // DuckDB requires a UNIQUE index for ON CONFLICT composite targets
  await query(`
    CREATE UNIQUE INDEX IF NOT EXISTS uq_template_file_index_pk
    ON template_file_index(file_path, template_name)
  `);

  // Index state tracking table
  await query(`
    CREATE TABLE IF NOT EXISTS template_file_index_state (
      id INTEGER PRIMARY KEY,
      last_indexed_file VARCHAR,
      last_indexed_at TIMESTAMP,
      total_files_indexed INTEGER DEFAULT 0,
      total_templates_found INTEGER DEFAULT 0,
      build_duration_seconds REAL
    )
  `);

  // DuckDB requires a UNIQUE index for ON CONFLICT (id)
  await query(`
    CREATE UNIQUE INDEX IF NOT EXISTS uq_template_file_index_state_id
    ON template_file_index_state(id)
  `);

  // Create index for fast template lookups
  await query(`
    CREATE INDEX IF NOT EXISTS idx_template_file_template 
    ON template_file_index(template_name)
  `);
}

/**
 * Get current index state
 */
export async function getTemplateIndexState() {
  try {
    const state = await queryOne(`
      SELECT last_indexed_file, last_indexed_at, total_files_indexed, 
             total_templates_found, build_duration_seconds 
      FROM template_file_index_state 
      WHERE id = 1
    `);
    return state || { 
      last_indexed_file: null, 
      last_indexed_at: null, 
      total_files_indexed: 0,
      total_templates_found: 0,
      build_duration_seconds: null
    };
  } catch (err) {
    return { 
      last_indexed_file: null, 
      last_indexed_at: null, 
      total_files_indexed: 0,
      total_templates_found: 0,
      build_duration_seconds: null
    };
  }
}

/**
 * Normalize stored path to current OS format.
 * The template_file_index may store Unix-style paths (/home/...) even when running on Windows.
 * This function converts them relative to DATA_PATH so fs.readFile works correctly.
 */
function normalizePath(storedPath) {
  // If running on Windows and storedPath is Unix-style, convert it relative to DATA_PATH
  if (process.platform === 'win32' && storedPath.startsWith('/')) {
    // Extract the relative portion after "data/raw/" (or equivalent subdirectory)
    const rawIdx = storedPath.indexOf('/data/raw/');
    if (rawIdx !== -1) {
      const relative = storedPath.slice(rawIdx + '/data/raw/'.length);
      return path.join(DATA_PATH, relative);
    }
    // Fallback: try extracting after "migration="
    const migrationIdx = storedPath.indexOf('migration=');
    if (migrationIdx !== -1) {
      const relative = storedPath.slice(migrationIdx);
      return path.join(DATA_PATH, relative);
    }
  }
  return storedPath;
}

/**
 * Get files containing a specific template (or template pattern)
 */
export async function getFilesForTemplate(templatePattern) {
  try {
    const rows = await query(`
      SELECT DISTINCT file_path, event_count, first_event_at, last_event_at
      FROM template_file_index
      WHERE template_name LIKE '%' || ? || '%'
      ORDER BY last_event_at DESC
    `, [templatePattern]);
    return rows.map(r => normalizePath(r.file_path));
  } catch (err) {
    console.error('Error querying template index:', err);
    return [];
  }
}

/**
 * Get files containing a specific template with metadata
 */
export async function getFilesForTemplateWithMeta(templatePattern) {
  try {
    const rows = await query(`
      SELECT file_path, template_name, event_count, first_event_at, last_event_at
      FROM template_file_index
      WHERE template_name LIKE '%' || ? || '%'
      ORDER BY last_event_at DESC
    `, [templatePattern]);
    return rows.map(r => ({ ...r, file_path: normalizePath(r.file_path) }));
  } catch (err) {
    console.error('Error querying template index:', err);
    return [];
  }
}

/**
 * Get unique template names in the index
 */
export async function getIndexedTemplates() {
  try {
    const rows = await query(`
      SELECT template_name, SUM(event_count) as total_events, COUNT(DISTINCT file_path) as file_count
      FROM template_file_index
      GROUP BY template_name
      ORDER BY total_events DESC
    `);
    return rows;
  } catch (err) {
    console.error('Error getting indexed templates:', err);
    return [];
  }
}

/**
 * Check if index is populated
 */
export async function isTemplateIndexPopulated() {
  try {
    const result = await queryOne(`SELECT COUNT(*) as count FROM template_file_index`);
    return (result?.count || 0) > 0;
  } catch {
    return false;
  }
}

/**
 * Check if indexing is in progress
 */
export function isTemplateIndexingInProgress() {
  return indexingInProgress;
}

/**
 * Get current indexing progress
 */
export function getTemplateIndexingProgress() {
  if (!indexingInProgress) {
    return { inProgress: false };
  }
  
  const elapsed = indexingProgress.startTime ? (Date.now() - indexingProgress.startTime) / 1000 : 0;
  const filesPerSec = elapsed > 0 ? indexingProgress.current / elapsed : 0;
  const remaining = indexingProgress.total - indexingProgress.current;
  const etaSeconds = filesPerSec > 0 ? remaining / filesPerSec : 0;
  
  return {
    inProgress: true,
    current: indexingProgress.current,
    total: indexingProgress.total,
    percent: indexingProgress.total > 0 
      ? ((indexingProgress.current / indexingProgress.total) * 100).toFixed(1) 
      : 0,
    filesPerSec: filesPerSec.toFixed(1),
    etaMinutes: (etaSeconds / 60).toFixed(1),
    elapsedSeconds: elapsed.toFixed(0),
  };
}

/**
 * Extract simple template name from full template_id
 * e.g., "Splice.DsoRules:VoteRequest" -> "VoteRequest"
 */
function extractTemplateName(templateId) {
  if (!templateId) return null;
  // Get the part after the last colon or the whole string
  const parts = templateId.split(':');
  return parts[parts.length - 1];
}

/**
 * Build or update the template-to-file index
 * @param {Object} options - Options for building the index
 * @param {boolean} options.force - Force rebuild from scratch
 * @param {boolean} options.incremental - Only index new files
 */
export async function buildTemplateFileIndex({ force = false, incremental = true, parallel = true } = {}) {
  if (indexingInProgress) {
    console.log('⏳ Template file indexing already in progress');
    return { status: 'in_progress', progress: getTemplateIndexingProgress() };
  }
  
  indexingInProgress = true;
  console.log('\n📑 Starting template-to-file index build...');
  
  try {
    const startTime = Date.now();
    indexingProgress.startTime = startTime;
    
    // Ensure tables exist
    await ensureIndexTables();
    
    // Find all event files
    console.log('   Scanning for event files...');
    const allFiles = findBinaryFiles(DATA_PATH, 'events');
    console.log(`   📂 Found ${allFiles.length} total event files`);
    
    // Get already-indexed files if incremental
    let indexedFilesSet = new Set();
    if (incremental && !force) {
      try {
        const indexedFiles = await query(`SELECT DISTINCT file_path FROM template_file_index`);
        indexedFilesSet = new Set(indexedFiles.map(r => r.file_path));
        console.log(`   📋 ${indexedFilesSet.size} files already indexed`);
      } catch {
        // Table might not exist yet
      }
    }
    
    // Clear existing data if force rebuild
    if (force) {
      try {
        await query('DELETE FROM template_file_index');
        console.log('   Cleared existing index');
        indexedFilesSet.clear();
      } catch (err) {
        // Table might not exist yet
      }
    }
    
    // Filter to only unindexed files
    const filesToIndex = allFiles.filter(f => !indexedFilesSet.has(f));
    const filesSkipped = allFiles.length - filesToIndex.length;
    
    if (filesToIndex.length === 0) {
      console.log('   ✅ All files already indexed');
      indexingInProgress = false;
      return {
        status: 'complete',
        filesIndexed: 0,
        filesSkipped,
        templatesFound: 0,
        elapsedSeconds: 0,
      };
    }
    
    indexingProgress.total = filesToIndex.length;
    indexingProgress.current = 0;
    
    let filesProcessed = 0;
    let templatesFound = 0;
    
    // Use parallel processing if enabled and we have many files
    // NOTE: parallel=false by default because @mongodb-js/zstd (native addon) doesn't work reliably in worker threads
    const useParallel = parallel && filesToIndex.length > 500 && process.env.TEMPLATE_INDEX_PARALLEL === 'true';
    
    if (useParallel) {
      console.log(`   🚀 Using ${WORKER_POOL_SIZE} parallel workers (experimental)`);
      const result = await processFilesParallel(filesToIndex, startTime);
      filesProcessed = result.filesProcessed;
      templatesFound = result.templatesFound;
    } else {
      console.log(`   📝 Using sequential processing (${filesToIndex.length} files)`);
      const result = await processFilesSequential(filesToIndex, startTime);
      filesProcessed = result.filesProcessed;
      templatesFound = result.templatesFound;
    }
    
    // Update state
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    
    await query(`
      INSERT INTO template_file_index_state (id, last_indexed_at, total_files_indexed, total_templates_found, build_duration_seconds)
      VALUES (1, now(), ?, ?, ?)
      ON CONFLICT (id) DO UPDATE SET
        last_indexed_at = now(),
        total_files_indexed = ?,
        total_templates_found = ?,
        build_duration_seconds = ?
    `, [filesProcessed, templatesFound, parseFloat(elapsed), filesProcessed, templatesFound, parseFloat(elapsed)]);
    
    console.log(`✅ Template index built: ${filesProcessed} files indexed, ${filesSkipped} skipped, ${templatesFound} template mappings in ${elapsed}s`);
    
    indexingInProgress = false;
    
    return {
      status: 'complete',
      filesIndexed: filesProcessed,
      filesSkipped,
      templatesFound,
      elapsedSeconds: parseFloat(elapsed),
    };
    
  } catch (err) {
    console.error('❌ Template index build failed:', err);
    indexingInProgress = false;
    throw err;
  }
}

/**
 * Process files using parallel worker threads
 * Falls back to sequential if workers fail to start
 */
async function processFilesParallel(files, startTime) {
  return new Promise((resolve, reject) => {
    const workers = [];
    let taskIdCounter = 0;
    let filesProcessed = 0;
    let templatesFound = 0;
    let completedBatches = 0;
    let lastLogTime = startTime;
    let workersFailed = 0;
    
    // Split files into batches
    const batches = [];
    for (let i = 0; i < files.length; i += FILES_PER_WORKER_BATCH) {
      batches.push(files.slice(i, i + FILES_PER_WORKER_BATCH));
    }
    
    const totalBatches = batches.length;
    let nextBatchIndex = 0;
    
    console.log(`   📦 Split ${files.length} files into ${totalBatches} batches of ~${FILES_PER_WORKER_BATCH} files`);
    
    // Timeout watchdog - if no progress in 2 minutes, fall back to sequential
    let lastProgress = Date.now();
    const watchdog = setInterval(() => {
      if (Date.now() - lastProgress > 120000) {
        console.warn('   ⚠️ Worker pool stalled for 2 minutes, falling back to sequential processing');
        clearInterval(watchdog);
        for (const w of workers) {
          try { w.terminate(); } catch {}
        }
        // Fall back to sequential for remaining files
        const remainingFiles = files.slice(filesProcessed);
        processFilesSequential(remainingFiles, startTime)
          .then(seqResult => {
            resolve({
              filesProcessed: filesProcessed + seqResult.filesProcessed,
              templatesFound: templatesFound + seqResult.templatesFound,
            });
          })
          .catch(reject);
      }
    }, 10000);
    
    const finishIfDone = () => {
      if (completedBatches >= totalBatches) {
        clearInterval(watchdog);
        for (const w of workers) {
          try { w.terminate(); } catch {}
        }
        resolve({ filesProcessed, templatesFound });
      }
    };
    
    // Create workers
    for (let i = 0; i < WORKER_POOL_SIZE; i++) {
      try {
        const worker = new Worker(WORKER_SCRIPT);
        
        worker.on('message', async (result) => {
          lastProgress = Date.now();
          const { id, success, results, processed, errors } = result;
          
          if (success && results) {
            await flushBatch(results);
            filesProcessed += processed;
            templatesFound += results.length;
          } else if (!success) {
            console.warn(`   ⚠️ Worker batch failed: ${result.error}`);
          }
          
          completedBatches++;
          indexingProgress.current = filesProcessed;
          
          // Log progress
          const now = Date.now();
          if (now - lastLogTime > 5000 || completedBatches === totalBatches) {
            const elapsed = (now - startTime) / 1000;
            const filesPerSec = elapsed > 0 ? filesProcessed / elapsed : 0;
            const pct = ((completedBatches / totalBatches) * 100).toFixed(1);
            const remaining = files.length - filesProcessed;
            const etaSeconds = filesPerSec > 0 ? remaining / filesPerSec : 0;
            const etaMin = Math.floor(etaSeconds / 60);
            const etaSec = Math.floor(etaSeconds % 60);
            
            console.log(`   📑 [${pct}%] ${filesProcessed}/${files.length} files | ${templatesFound} templates | ${filesPerSec.toFixed(0)} files/s | ETA: ${etaMin}m ${etaSec}s`);
            lastLogTime = now;
          }
          
          // Send next batch to this worker
          if (nextBatchIndex < batches.length) {
            const batch = batches[nextBatchIndex++];
            worker.postMessage({ id: ++taskIdCounter, files: batch });
          } else {
            finishIfDone();
          }
        });
        
        worker.on('error', (err) => {
          console.error(`   ⚠️ Worker error:`, err.message);
          workersFailed++;
          // If all workers failed, fall back to sequential
          if (workersFailed >= WORKER_POOL_SIZE) {
            console.warn('   ⚠️ All workers failed, falling back to sequential');
            clearInterval(watchdog);
            processFilesSequential(files, startTime).then(resolve).catch(reject);
          }
        });
        
        worker.on('exit', (code) => {
          if (code !== 0 && completedBatches < totalBatches) {
            console.warn(`   ⚠️ Worker exited with code ${code}`);
          }
        });
        
        workers.push(worker);
        
        // Assign initial batch to this worker
        if (nextBatchIndex < batches.length) {
          const batch = batches[nextBatchIndex++];
          worker.postMessage({ id: ++taskIdCounter, files: batch });
        }
      } catch (err) {
        console.error(`   ⚠️ Failed to create worker:`, err.message);
        workersFailed++;
      }
    }
    
    // If no workers started, fall back immediately
    if (workers.length === 0) {
      console.warn('   ⚠️ No workers started, using sequential processing');
      clearInterval(watchdog);
      processFilesSequential(files, startTime).then(resolve).catch(reject);
    }
  });
}

/**
 * Process files concurrently in the main thread.
 * This avoids worker-thread native addon issues, but still parallelizes IO + decompression.
 */
async function processFilesSequential(files, startTime) {
  // Kept function name for compatibility with callers; this is actually concurrent.
  const concurrency = Math.min(
    12,
    Math.max(2, parseInt(process.env.TEMPLATE_INDEX_CONCURRENCY || '6', 10))
  );

  let filesProcessed = 0;
  let templatesFound = 0;
  let lastLogTime = startTime;

  // Buffer DB writes to reduce per-row overhead
  const FLUSH_BATCH_SIZE = 1000;
  let pendingRows = [];

  let nextIndex = 0;

  const processOne = async () => {
    while (true) {
      const i = nextIndex++;
      if (i >= files.length) return;

      const file = files[i];
      indexingProgress.current = Math.min(filesProcessed + 1, files.length);

      try {
        const result = await readBinaryFile(file);
        const records = result.records || [];

        // Extract unique templates from this file
        const templateStats = new Map();
        for (const record of records) {
          const templateName = extractTemplateName(record.template_id);
          if (!templateName) continue;

          const effectiveAt = record.effective_at ? new Date(record.effective_at) : null;
          if (!templateStats.has(templateName)) {
            templateStats.set(templateName, {
              count: 0,
              firstEventAt: effectiveAt,
              lastEventAt: effectiveAt,
            });
          }

          const stats = templateStats.get(templateName);
          stats.count++;
          if (effectiveAt) {
            if (!stats.firstEventAt || effectiveAt < stats.firstEventAt) stats.firstEventAt = effectiveAt;
            if (!stats.lastEventAt || effectiveAt > stats.lastEventAt) stats.lastEventAt = effectiveAt;
          }
        }

        for (const [templateName, stats] of templateStats) {
          pendingRows.push([
            file,
            templateName,
            stats.count,
            stats.firstEventAt?.toISOString() || null,
            stats.lastEventAt?.toISOString() || null,
          ]);
          templatesFound++;
        }

        filesProcessed++;

        if (pendingRows.length >= FLUSH_BATCH_SIZE) {
          const toFlush = pendingRows;
          pendingRows = [];
          await flushBatch(toFlush);
        }

        const now = Date.now();
        if (filesProcessed % 500 === 0 || now - lastLogTime > 10000) {
          const elapsed = (now - startTime) / 1000;
          const filesPerSec = elapsed > 0 ? filesProcessed / elapsed : 0;
          const remaining = files.length - filesProcessed;
          const etaSeconds = filesPerSec > 0 ? remaining / filesPerSec : 0;
          const etaMin = Math.floor(etaSeconds / 60);
          const etaSec = Math.floor(etaSeconds % 60);
          const pct = ((filesProcessed / files.length) * 100).toFixed(1);

          console.log(
            `   📑 [${pct}%] ${filesProcessed}/${files.length} files | ${templatesFound} templates | ${filesPerSec.toFixed(0)} files/s | ETA: ${etaMin}m ${etaSec}s | conc=${concurrency}`
          );
          lastLogTime = now;
        }
      } catch (err) {
        console.warn(`   ⚠️ Skipped ${path.basename(file)}: ${err.message}`);
      }
    }
  };

  console.log(`   ⚡ Concurrent processing enabled: concurrency=${concurrency}`);
  await Promise.all(Array.from({ length: concurrency }, () => processOne()));

  if (pendingRows.length > 0) {
    await flushBatch(pendingRows);
  }

  return { filesProcessed, templatesFound };
}

/**
 * Flush a batch of template mappings to the database (bulk insert)
 * batch: Array<[file_path, template_name, event_count, first_event_at, last_event_at]>
 */
async function flushBatch(batch) {
  if (!batch || batch.length === 0) return;

  // Chunk large batches to keep SQL size reasonable
  const CHUNK_SIZE = 500;

  for (let i = 0; i < batch.length; i += CHUNK_SIZE) {
    const chunk = batch.slice(i, i + CHUNK_SIZE);

    const valuesSql = chunk.map(() => '(?, ?, ?, ?, ?)').join(', ');
    const params = chunk.flat();

    try {
      await query(
        `
        INSERT INTO template_file_index (file_path, template_name, event_count, first_event_at, last_event_at)
        VALUES ${valuesSql}
        ON CONFLICT (file_path, template_name) DO UPDATE SET
          event_count = EXCLUDED.event_count,
          first_event_at = EXCLUDED.first_event_at,
          last_event_at = EXCLUDED.last_event_at
        `,
        params
      );
    } catch (err) {
      // If a bulk insert fails, fall back to row-by-row so we still make progress
      for (const row of chunk) {
        try {
          await query(
            `
            INSERT INTO template_file_index (file_path, template_name, event_count, first_event_at, last_event_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (file_path, template_name) DO UPDATE SET
              event_count = EXCLUDED.event_count,
              first_event_at = EXCLUDED.first_event_at,
              last_event_at = EXCLUDED.last_event_at
            `,
            row
          );
        } catch {
          // ignore
        }
      }
    }
  }
}

/**
 * Get index statistics
 */
export async function getTemplateIndexStats() {
  try {
    const fileCount = await queryOne(`SELECT COUNT(DISTINCT file_path) as count FROM template_file_index`);
    const templateCount = await queryOne(`SELECT COUNT(DISTINCT template_name) as count FROM template_file_index`);
    const totalEvents = await queryOne(`SELECT SUM(event_count) as count FROM template_file_index`);
    const state = await getTemplateIndexState();
    
    return {
      totalFiles: Number(fileCount?.count || 0),
      uniqueTemplates: Number(templateCount?.count || 0),
      totalEventMappings: Number(totalEvents?.count || 0),
      lastIndexedAt: state.last_indexed_at,
      buildDurationSeconds: state.build_duration_seconds,
      isPopulated: Number(fileCount?.count || 0) > 0,
      inProgress: indexingInProgress,
      progress: indexingInProgress ? getTemplateIndexingProgress() : null,
    };
  } catch (err) {
    console.error('Error getting template index stats:', err);
    return {
      totalFiles: 0,
      uniqueTemplates: 0,
      totalEventMappings: 0,
      lastIndexedAt: null,
      buildDurationSeconds: null,
      isPopulated: false,
      inProgress: indexingInProgress,
      progress: indexingInProgress ? getTemplateIndexingProgress() : null,
    };
  }
}
