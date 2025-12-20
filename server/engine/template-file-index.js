/**
 * Template-to-File Index
 * 
 * Builds and maintains a persistent mapping of template names to the files that contain them.
 * This dramatically speeds up template-specific queries (like VoteRequest) by scanning only
 * relevant files instead of all 35K+ files.
 * 
 * The index is stored in DuckDB and survives server restarts.
 */

import fs from 'fs';
import path from 'path';
import { query, queryOne, DATA_PATH } from '../duckdb/connection.js';
import { readBinaryFile, findBinaryFiles } from '../duckdb/binary-reader.js';

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
    return rows.map(r => r.file_path);
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
    return rows;
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
export async function buildTemplateFileIndex({ force = false, incremental = true } = {}) {
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
    const files = findBinaryFiles(DATA_PATH, 'events');
    console.log(`   📂 Found ${files.length} total event files`);
    
    indexingProgress.total = files.length;
    indexingProgress.current = 0;
    
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
    
    let filesProcessed = 0;
    let templatesFound = 0;
    let filesSkipped = 0;
    let lastLogTime = startTime;
    
    // Process files in batches for better performance
    const BATCH_SIZE = 50;
    let batch = [];
    
    for (let i = 0; i < files.length; i++) {
      const file = files[i];
      indexingProgress.current = i + 1;
      
      // Skip already-indexed files in incremental mode
      if (indexedFilesSet.has(file)) {
        filesSkipped++;
        continue;
      }
      
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
            if (!stats.firstEventAt || effectiveAt < stats.firstEventAt) {
              stats.firstEventAt = effectiveAt;
            }
            if (!stats.lastEventAt || effectiveAt > stats.lastEventAt) {
              stats.lastEventAt = effectiveAt;
            }
          }
        }
        
        // Queue batch inserts
        for (const [templateName, stats] of templateStats) {
          batch.push({
            file_path: file,
            template_name: templateName,
            event_count: stats.count,
            first_event_at: stats.firstEventAt?.toISOString() || null,
            last_event_at: stats.lastEventAt?.toISOString() || null,
          });
          templatesFound++;
        }
        
        // Flush batch when it gets large enough
        if (batch.length >= BATCH_SIZE) {
          await flushBatch(batch);
          batch = [];
        }
        
        filesProcessed++;
        
        // Log progress every 500 files or every 10 seconds
        const now = Date.now();
        const shouldLog = filesProcessed % 500 === 0 || (now - lastLogTime > 10000);
        
        if (shouldLog) {
          const elapsed = (now - startTime) / 1000;
          const filesPerSec = filesProcessed / elapsed;
          const remaining = files.length - i - 1 - filesSkipped;
          const etaSeconds = remaining / filesPerSec;
          const etaMin = Math.floor(etaSeconds / 60);
          const etaSec = Math.floor(etaSeconds % 60);
          const pct = ((i / files.length) * 100).toFixed(1);
          
          console.log(`   📑 [${pct}%] ${filesProcessed} indexed, ${filesSkipped} skipped | ${templatesFound} templates | ${filesPerSec.toFixed(0)} files/s | ETA: ${etaMin}m ${etaSec}s`);
          lastLogTime = now;
        }
        
      } catch (err) {
        // Skip files that can't be read
        console.warn(`   ⚠️ Skipped ${path.basename(file)}: ${err.message}`);
      }
    }
    
    // Flush remaining batch
    if (batch.length > 0) {
      await flushBatch(batch);
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
 * Flush a batch of template mappings to the database
 */
async function flushBatch(batch) {
  if (batch.length === 0) return;
  
  // Use INSERT OR REPLACE for upsert behavior
  for (const item of batch) {
    try {
      await query(`
        INSERT INTO template_file_index (file_path, template_name, event_count, first_event_at, last_event_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (file_path, template_name) DO UPDATE SET
          event_count = EXCLUDED.event_count,
          first_event_at = EXCLUDED.first_event_at,
          last_event_at = EXCLUDED.last_event_at
      `, [
        item.file_path,
        item.template_name,
        item.event_count,
        item.first_event_at,
        item.last_event_at,
      ]);
    } catch (err) {
      // Ignore individual insert errors
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
