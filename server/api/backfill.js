/**
 * Backfill API - DuckDB Parquet Only
 * 
 * Data Authority: All queries use DuckDB over Parquet files.
 * See docs/architecture.md for the Data Authority Contract.
 */

import { Router } from 'express';
import { readFileSync, existsSync, readdirSync, unlinkSync, rmSync, statSync } from 'fs';
import { join, basename as pathBasename } from 'path';
import db, { safeQuery, hasFileType, DATA_PATH } from '../duckdb/connection.js';
import { getLastGapDetection } from '../engine/gap-detector.js';
import { triggerGapDetection } from '../engine/worker.js';
import { requireAuth } from '../lib/auth.js';

// Use process.cwd() for Vitest/Vite SSR compatibility
const __dirname = join(process.cwd(), 'server', 'api');

const router = Router();

// Path to data directory - configurable via env var for cross-platform support
// Prefer repo-local ./data if present (matches server/duckdb/connection.js)
const WIN_DEFAULT = 'C:\\ledger_raw';
const REPO_DATA_DIR = join(__dirname, '../../data');
const repoCursorDir = join(REPO_DATA_DIR, 'cursors');

// Final selection order:
// 1) process.env.DATA_DIR (explicit override)
// 2) repo-local data/ (if present)
// 3) Windows default path
const DATA_DIR = process.env.DATA_DIR || (existsSync(repoCursorDir) ? REPO_DATA_DIR : WIN_DEFAULT);
const CURSOR_DIR = process.env.CURSOR_DIR || join(DATA_DIR, 'cursors');

/**
 * Read all cursor files from the cursors directory
 */
function readAllCursors() {
  if (!existsSync(CURSOR_DIR)) {
    return [];
  }

  // Cursor files may be named differently across environments (e.g. cursor-*.json, backfill-*.json).
  // Read all JSON files and only keep those that look like backfill cursors.
  const files = readdirSync(CURSOR_DIR).filter((f) => f.endsWith('.json'));
  const cursors = [];
  for (const file of files) {
    try {
       const filePath = join(CURSOR_DIR, file);
       const data = JSON.parse(readFileSync(filePath, 'utf-8'));

       // Skip non-cursor JSON files
       const looksLikeCursor =
         typeof data === 'object' &&
         data &&
         (data.migration_id !== undefined || data.cursor_name !== undefined || data.last_before !== undefined);
       if (!looksLikeCursor) continue;

       // Detect if cursor is stale (not updated in 5+ minutes but marked complete with old timestamp)
       const updatedAt = data.updated_at ? new Date(data.updated_at).getTime() : 0;
       const isRecentlyUpdated = Date.now() - updatedAt < 5 * 60 * 1000; // 5 minutes

       // If marked complete but has pending writes, it's still finalizing
       const hasPendingWork = (data.pending_writes || 0) > 0 || (data.buffered_records || 0) > 0;
       const effectiveComplete = data.complete && !hasPendingWork;

       cursors.push({
        id: data.id || file.replace('.json', ''),
        cursor_name: data.cursor_name || `migration-${data.migration_id}-${data.synchronizer_id?.substring(0, 20)}`,
        migration_id: data.migration_id,
        synchronizer_id: data.synchronizer_id,
        min_time: data.min_time,
        max_time: data.max_time,
        last_before: data.last_before,
        complete: effectiveComplete,
        last_processed_round: data.last_processed_round || 0,
        updated_at: data.updated_at || new Date().toISOString(),
        started_at: data.started_at || data.updated_at || new Date().toISOString(),
        total_updates: data.total_updates || 0,
        total_events: data.total_events || 0,
        pending_writes: data.pending_writes || 0,
        buffered_records: data.buffered_records || 0,
        is_recently_updated: isRecentlyUpdated,
        error: data.error,
      });
    } catch (err) {
      console.error(`Error reading cursor file ${file}:`, err.message);
    }
  }

  return cursors;
}

// Track file counts over time to detect active writes
let lastFileCounts = { events: 0, updates: 0, timestamp: 0 };

function countRawFiles() {
  const rawDir = join(DATA_DIR, 'raw');
  let events = 0;
  let updates = 0;
  let parquetEvents = 0;
  let parquetUpdates = 0;
  
  function scanDir(dir) {
    try {
      const entries = readdirSync(dir, { withFileTypes: true });
      for (const entry of entries) {
        if (entry.isDirectory()) {
          scanDir(join(dir, entry.name));
        } else if (entry.name.endsWith('.pb.zst')) {
          if (entry.name.startsWith('events-')) events++;
          else if (entry.name.startsWith('updates-')) updates++;
        } else if (entry.name.endsWith('.parquet')) {
          if (entry.name.startsWith('events-')) parquetEvents++;
          else if (entry.name.startsWith('updates-')) parquetUpdates++;
        }
      }
    } catch {}
  }
  
  if (existsSync(rawDir)) {
    scanDir(rawDir);
  }
  
  // Return combined counts - prefer Parquet if available
  return { 
    events: parquetEvents || events, 
    updates: parquetUpdates || updates,
    format: (parquetEvents > 0 || parquetUpdates > 0) ? 'parquet' : (events > 0 || updates > 0) ? 'pb.zst' : 'none',
    parquetEvents,
    parquetUpdates,
    binaryEvents: events,
    binaryUpdates: updates,
  };
}

// GET /api/backfill/debug - Debug endpoint to check paths
router.get('/debug', (req, res) => {
  const cursorExists = existsSync(CURSOR_DIR);
  const cursorFiles = cursorExists ? readdirSync(CURSOR_DIR).filter(f => f.endsWith('.json')) : [];
  const fileCounts = countRawFiles();
  
  res.json({
    cursorDir: CURSOR_DIR,
    cursorDirExists: cursorExists,
    cursorFiles,
    rawFileCounts: fileCounts,
    dataDir: DATA_DIR,
  });
});

// GET /api/backfill/write-activity - Check if files are actively being written
router.get('/write-activity', (req, res) => {
  try {
    const now = Date.now();
    const currentCounts = countRawFiles();
    
    // Compare with last counts (if within last 30 seconds)
    const isActive = now - lastFileCounts.timestamp < 30000 && 
      (currentCounts.events > lastFileCounts.events || currentCounts.updates > lastFileCounts.updates);
    
    const delta = {
      events: currentCounts.events - (lastFileCounts.events || 0),
      updates: currentCounts.updates - (lastFileCounts.updates || 0),
      seconds: Math.round((now - lastFileCounts.timestamp) / 1000),
    };
    
    // Update last counts
    lastFileCounts = { ...currentCounts, timestamp: now };
    
    res.json({
      isWriting: isActive,
      currentCounts,
      delta,
      message: isActive 
        ? `+${delta.events} event files, +${delta.updates} update files in last ${delta.seconds}s` 
        : 'No new files detected since last check',
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/backfill/cursors - Get all backfill cursors
router.get('/cursors', (req, res) => {
  try {
    const cursors = readAllCursors();
    console.log(`[backfill] Found ${cursors.length} cursors in ${CURSOR_DIR}`);
    res.json({ data: cursors, count: cursors.length });
  } catch (err) {
    console.error('Error reading cursors:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/backfill/stats - Get backfill statistics from data files
router.get('/stats', async (req, res) => {
  try {
    const basePath = db.DATA_PATH.replace(/\\/g, '/');
    
    // Check if parquet files exist using hasFileType
    const hasParquetUpdates = db.hasFileType('updates', '.parquet');
    const hasParquetEvents = db.hasFileType('events', '.parquet');
    
    // Also check for JSONL files as fallback
    const hasJsonlUpdates = db.hasDataFiles('updates');
    const hasJsonlEvents = db.hasDataFiles('events');
    
    let updatesCount = 0;
    let eventsCount = 0;
    let activeMigrations = 0;
    
    // First, check for migrations in the raw directory structure (binary files)
    // This handles the case where data is in migration=X/year=YYYY/... format
    const rawDir = join(DATA_DIR, 'raw');
    const migrationsFromDirs = new Set();
    
    if (existsSync(rawDir)) {
      try {
        const entries = readdirSync(rawDir, { withFileTypes: true });
        for (const entry of entries) {
          if (entry.isDirectory()) {
            // Check for migration=X format
            const migrationMatch = entry.name.match(/^migration[=_]?(\d+)$/i);
            if (migrationMatch) {
              migrationsFromDirs.add(parseInt(migrationMatch[1]));
            }
          }
        }
      } catch (e) {
        console.warn('Error scanning raw directory for migrations:', e.message);
      }
    }
    
    // Use directory-based migration count as primary source
    if (migrationsFromDirs.size > 0) {
      activeMigrations = migrationsFromDirs.size;
    }

    // Try Parquet first, then JSONL for counts
    if (hasParquetUpdates) {
      try {
        const result = await db.safeQuery(`
          SELECT COUNT(*) as count FROM read_parquet('${basePath}/**/updates-*.parquet', union_by_name=true)
        `);
        updatesCount = Number(result[0]?.count || 0);
        
        // Only query parquet for migrations if we don't have directory-based count
        if (activeMigrations === 0) {
          const migrationsResult = await db.safeQuery(`
            SELECT COUNT(DISTINCT migration_id) as count 
            FROM read_parquet('${basePath}/**/updates-*.parquet', union_by_name=true)
            WHERE migration_id IS NOT NULL
          `).catch(() => [{ count: 0 }]);
          activeMigrations = Number(migrationsResult[0]?.count || 0);
        }
      } catch (e) {
        console.warn('Parquet updates query failed:', e.message);
      }
    } else if (hasJsonlUpdates) {
      try {
        const result = await db.safeQuery(`SELECT COUNT(*) as count FROM ${db.readJsonlGlob('updates')}`);
        updatesCount = Number(result[0]?.count || 0);
      } catch (e) {
        console.warn('JSONL updates query failed:', e.message);
      }
    }

    if (hasParquetEvents) {
      try {
        const result = await db.safeQuery(`
          SELECT COUNT(*) as count FROM read_parquet('${basePath}/**/events-*.parquet', union_by_name=true)
        `);
        eventsCount = Number(result[0]?.count || 0);
      } catch (e) {
        console.warn('Parquet events query failed:', e.message);
      }
    } else if (hasJsonlEvents) {
      try {
        const result = await db.safeQuery(`SELECT COUNT(*) as count FROM ${db.readJsonlGlob('events')}`);
        eventsCount = Number(result[0]?.count || 0);
      } catch (e) {
        console.warn('JSONL events query failed:', e.message);
      }
    }

    // Read cursor stats
    const cursors = readAllCursors();
    const totalCursors = cursors.length;
    const completedCursors = cursors.filter(c => c.complete).length;
    
    // Also count from raw binary files if we have them
    const fileCounts = countRawFiles();

    res.json({
      totalUpdates: updatesCount,
      totalEvents: eventsCount,
      activeMigrations,
      migrationsFromDirs: Array.from(migrationsFromDirs).sort((a, b) => a - b),
      totalCursors,
      completedCursors,
      rawFileCounts: fileCounts,
    });
  } catch (err) {
    console.error('Error getting backfill stats:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/backfill/shards - Get shard progress data
router.get('/shards', (req, res) => {
  try {
    const cursors = readAllCursors();
    
    // Group cursors by migration and synchronizer, identifying shards
    const groups = {};
    
    for (const cursor of cursors) {
      const migrationId = cursor.migration_id || 0;
      const synchronizerId = cursor.synchronizer_id || 'unknown';
      
      // Extract shard info from cursor name or file
      const shardMatch = cursor.cursor_name?.match(/-shard(\d+)$/) || cursor.id?.match(/-shard(\d+)$/);
      const shardIndex = shardMatch ? parseInt(shardMatch[1]) : null;
      const isSharded = shardIndex !== null;
      
      // Create base key (without shard suffix)
      const baseKey = `${migrationId}-${synchronizerId}`;
      
      if (!groups[baseKey]) {
        groups[baseKey] = {
          migrationId,
          synchronizerId,
          shards: [],
          totalUpdates: 0,
          totalEvents: 0,
        };
      }
      
      // Calculate progress
      const hasPendingWork = (cursor.pending_writes || 0) > 0 || (cursor.buffered_records || 0) > 0;
      let progress = 0;
      if (cursor.complete) {
        // If cursor is marked complete but we still have pending writes/buffers, treat as "finalizing"
        progress = hasPendingWork ? 99.9 : 100;
      } else if (cursor.min_time && cursor.max_time && cursor.last_before) {
        const minMs = new Date(cursor.min_time).getTime();
        const maxMs = new Date(cursor.max_time).getTime();
        const currentMs = new Date(cursor.last_before).getTime();
        const totalRange = maxMs - minMs;
        if (totalRange > 0) {
          let rawProgress = ((maxMs - currentMs) / totalRange) * 100;
          // Cap at 99.9% if not marked complete OR has pending writes
          if ((rawProgress >= 99.5 || hasPendingWork) && !cursor.complete) {
            rawProgress = Math.min(rawProgress, 99.9);
          }
          progress = Math.min(100, Math.max(0, rawProgress));
        }
      }
      
      // Calculate throughput and ETA
      let throughput = null;
      let eta = null;
      if (cursor.started_at && cursor.updated_at && !cursor.complete) {
        const startedAt = new Date(cursor.started_at).getTime();
        const updatedAt = new Date(cursor.updated_at).getTime();
        const elapsed = updatedAt - startedAt;
        
        if (elapsed > 0 && cursor.total_updates) {
          throughput = Math.round(cursor.total_updates / (elapsed / 1000));
        }
        
        if (elapsed > 0 && progress > 0) {
          const totalEstimate = (elapsed / progress) * 100;
          const remaining = totalEstimate - elapsed;
          if (remaining > 0) {
            eta = remaining;
          }
        }
      }
      
      groups[baseKey].shards.push({
        shardIndex,
        isSharded,
        progress,
        throughput,
        eta,
        complete: cursor.complete,
        totalUpdates: cursor.total_updates || 0,
        totalEvents: cursor.total_events || 0,
        pendingWrites: cursor.pending_writes || 0,
        bufferedRecords: cursor.buffered_records || 0,
        minTime: cursor.min_time,
        maxTime: cursor.max_time,
        lastBefore: cursor.last_before,
        updatedAt: cursor.updated_at,
        startedAt: cursor.started_at,
        error: cursor.error,
      });
      
      groups[baseKey].totalUpdates += cursor.total_updates || 0;
      groups[baseKey].totalEvents += cursor.total_events || 0;
    }
    
    // Calculate aggregate stats for each group
    const result = Object.values(groups).map(group => {
      const sortedShards = group.shards.sort((a, b) => (a.shardIndex || 0) - (b.shardIndex || 0));
      const totalShards = sortedShards.length;
      const completedShards = sortedShards.filter(s => s.complete).length;
      const overallProgress = totalShards > 0 
        ? sortedShards.reduce((sum, s) => sum + s.progress, 0) / totalShards 
        : 0;
      
      // Calculate combined ETA (use slowest shard)
      const activeShards = sortedShards.filter(s => !s.complete && s.eta);
      const combinedEta = activeShards.length > 0 
        ? Math.max(...activeShards.map(s => s.eta))
        : null;
      
      // Check for active shards (updated in last minute)
      const now = Date.now();
      const activeCount = sortedShards.filter(s => {
        if (s.complete) return false;
        if (!s.updatedAt) return false;
        return now - new Date(s.updatedAt).getTime() < 60000;
      }).length;
      
      return {
        ...group,
        shards: sortedShards,
        totalShards,
        completedShards,
        activeShards: activeCount,
        overallProgress,
        combinedEta,
      };
    });
    
    res.json({ data: result, count: result.length });
  } catch (err) {
    console.error('Error reading shard progress:', err);
    res.status(500).json({ error: err.message });
  }
});

// DELETE /api/backfill/purge - Purge all backfill data (local files) - PROTECTED
router.delete('/purge', requireAuth, (req, res) => {
  try {
    const dataDir = DATA_DIR;
    const rawDir = join(dataDir, 'raw');
    let deletedCursors = 0;
    let deletedDataFiles = 0;

    // Delete cursor files
    if (existsSync(CURSOR_DIR)) {
      const cursorFiles = readdirSync(CURSOR_DIR).filter(f => f.endsWith('.json'));
      for (const file of cursorFiles) {
        try {
          unlinkSync(join(CURSOR_DIR, file));
          deletedCursors++;
        } catch (err) {
          console.error(`Error deleting cursor file ${file}:`, err.message);
        }
      }
    }

    // Delete raw data directory recursively
    if (existsSync(rawDir)) {
      try {
        rmSync(rawDir, { recursive: true, force: true });
        deletedDataFiles = 1; // Directory deleted
      } catch (err) {
        console.error('Error deleting raw data directory:', err.message);
      }
    }

    console.log(`[backfill] Purged ${deletedCursors} cursors, deleted raw data: ${deletedDataFiles > 0}`);
    
    res.json({
      success: true,
      deleted_cursors: deletedCursors,
      deleted_data_dir: deletedDataFiles > 0,
    });
  } catch (err) {
    console.error('Error purging backfill data:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/backfill/gaps - Get detected time gaps
router.get('/gaps', (req, res) => {
  try {
    const gapInfo = getLastGapDetection();
    res.json({
      data: gapInfo?.gaps || [],
      totalGaps: gapInfo?.totalGaps || 0,
      totalGapTime: gapInfo?.totalGapTime || '0ms',
      detectedAt: gapInfo?.detectedAt || null,
      autoRecoverEnabled: gapInfo?.autoRecoverEnabled || false,
      recoveryAttempted: gapInfo?.recoveryAttempted || false,
      transactionsRecovered: gapInfo?.transactionsRecovered || 0,
    });
  } catch (err) {
    console.error('Error getting gap info:', err);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/backfill/gaps/detect - Trigger gap detection manually - PROTECTED
router.post('/gaps/detect', requireAuth, async (req, res) => {
  try {
    const autoRecover = req.body?.autoRecover === true;
    const result = await triggerGapDetection(autoRecover);
    res.json({
      success: true,
      gaps: result.gaps?.length || 0,
      totalGapTime: result.totalGapTime || '0ms',
      message: result.gaps?.length > 0 
        ? `Found ${result.gaps.length} gap(s)` 
        : 'No gaps detected',
    });
  } catch (err) {
    console.error('Error triggering gap detection:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/backfill/reconciliation - Get cursor vs file reconciliation data
router.get('/reconciliation', async (req, res) => {
  try {
    const cursors = readAllCursors();
    const fileCounts = countRawFiles();
    
    // Sum up cursor totals
    let cursorUpdates = 0;
    let cursorEvents = 0;
    
    for (const cursor of cursors) {
      cursorUpdates += cursor.total_updates || 0;
      cursorEvents += cursor.total_events || 0;
    }
    
    // Estimate file totals using actual batch sizes from bulletproof-backfill.js
    // Updates use BATCH_SIZE=5000
    // Events vary significantly - use higher estimate based on validation sampling:
    //   avg ~8,400, min ~5,000, max ~19,000
    const UPDATES_PER_FILE = 5000;
    const EVENTS_PER_FILE = 8500; // Adjusted based on actual validation sampling
    const estimatedFileUpdates = fileCounts.updates * UPDATES_PER_FILE;
    const estimatedFileEvents = fileCounts.events * EVENTS_PER_FILE;
    
    // Calculate differences - can be negative (more in files than cursors = good, means re-fetching worked)
    const updatesDiff = cursorUpdates - estimatedFileUpdates;
    const eventsDiff = cursorEvents - estimatedFileEvents;
    
    // Determine if data is missing or if there's extra data (from re-fetching)
    const updatesStatus = updatesDiff > 0 ? 'missing' : (updatesDiff < 0 ? 'extra' : 'match');
    const eventsStatus = eventsDiff > 0 ? 'missing' : (eventsDiff < 0 ? 'extra' : 'match');
    
    res.json({
      updates: {
        cursorTotal: cursorUpdates,
        fileTotal: estimatedFileUpdates,
        fileCount: fileCounts.updates,
        difference: Math.abs(updatesDiff),
        status: updatesStatus,
        percentDiff: cursorUpdates > 0 ? (Math.abs(updatesDiff) / cursorUpdates) * 100 : 0,
      },
      events: {
        cursorTotal: cursorEvents,
        fileTotal: estimatedFileEvents,
        fileCount: fileCounts.events,
        difference: Math.abs(eventsDiff),
        status: eventsStatus,
        percentDiff: cursorEvents > 0 ? (Math.abs(eventsDiff) / cursorEvents) * 100 : 0,
      },
      note: 'File totals are estimates (~5000 updates/file, ~8500 events/file). Run validate-backfill.js for exact counts. "Extra" data from gap traversal re-fetching is normal.',
    });
  } catch (err) {
    console.error('Error getting reconciliation data:', err);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/backfill/validate-integrity - Validate data integrity by sampling Parquet files - PROTECTED
// Checks schema requirements: event_type_original, root_event_ids, child_event_ids, record_time, canonical event_id
router.post('/validate-integrity', requireAuth, async (req, res) => {
  const sampleSize = Math.min(req.body?.sampleSize || 100, 500);
  
  try {
    const basePath = DATA_PATH.replace(/\\/g, '/');
    const hasParquetEvents = hasFileType('events', '.parquet');
    const hasParquetUpdates = hasFileType('updates', '.parquet');
    
    if (!hasParquetEvents && !hasParquetUpdates) {
      return res.json({ success: false, error: 'No Parquet data files found' });
    }
    
    const results = {
      dataFormat: 'parquet',
      sampledRecords: sampleSize,
      eventFiles: { checked: 0, valid: 0, missingRawJson: 0, emptyRecords: 0 },
      updateFiles: { checked: 0, valid: 0, missingUpdateDataJson: 0, emptyRecords: 0 },
      schemaCompliance: {
        eventsWithTypeOriginal: 0,
        eventsWithoutTypeOriginal: 0,
        eventsWithCanonicalId: 0,
        eventsWithSynthesizedId: 0,
        updatesWithRootEventIds: 0,
        updatesWithoutRootEventIds: 0,
        updatesWithRecordTime: 0,
        updatesWithoutRecordTime: 0,
        eventsWithChildEventIds: 0,
        eventsWithContractId: 0,
        eventsWithoutContractId: 0,
      },
      errors: [],
      sampleDetails: [],
    };
    
    // Validate events via DuckDB sampling
    if (hasParquetEvents) {
      try {
        const eventSample = await safeQuery(`
          SELECT 
            event_id,
            raw_event IS NOT NULL as has_raw_json,
            event_type_original IS NOT NULL as has_type_original,
            contract_id IS NOT NULL as has_contract_id,
            child_event_ids IS NOT NULL as has_child_event_ids
          FROM read_parquet('${basePath}/**/events-*.parquet', union_by_name=true)
          USING SAMPLE ${sampleSize} ROWS
        `);
        
        results.eventFiles.checked = eventSample.length;
        
        for (const row of eventSample) {
          if (row.has_raw_json) {
            results.eventFiles.valid++;
          } else {
            results.eventFiles.missingRawJson++;
          }
          
          if (row.has_type_original) {
            results.schemaCompliance.eventsWithTypeOriginal++;
          } else {
            results.schemaCompliance.eventsWithoutTypeOriginal++;
          }
          
          if (row.event_id && String(row.event_id).includes(':')) {
            results.schemaCompliance.eventsWithCanonicalId++;
          } else if (row.event_id) {
            results.schemaCompliance.eventsWithSynthesizedId++;
          }
          
          if (row.has_contract_id) {
            results.schemaCompliance.eventsWithContractId++;
          } else {
            results.schemaCompliance.eventsWithoutContractId++;
          }
          
          if (row.has_child_event_ids) {
            results.schemaCompliance.eventsWithChildEventIds++;
          }
        }
        
        results.sampleDetails.push({
          type: 'events',
          recordCount: eventSample.length,
          hasRequiredFields: results.eventFiles.missingRawJson === 0,
          missingFields: results.eventFiles.missingRawJson > 0 ? ['raw_json'] : [],
        });
        
      } catch (err) {
        results.errors.push({ file: 'events-*.parquet', error: err.message });
      }
    }
    
    // Validate updates via DuckDB sampling
    if (hasParquetUpdates) {
      try {
        const updateSample = await safeQuery(`
          SELECT 
            update_id,
            update_data IS NOT NULL as has_update_data,
            root_event_ids IS NOT NULL as has_root_event_ids,
            record_time IS NOT NULL as has_record_time
          FROM read_parquet('${basePath}/**/updates-*.parquet', union_by_name=true)
          USING SAMPLE ${sampleSize} ROWS
        `);
        
        results.updateFiles.checked = updateSample.length;
        
        for (const row of updateSample) {
          if (row.has_update_data) {
            results.updateFiles.valid++;
          } else {
            results.updateFiles.missingUpdateDataJson++;
          }
          
          if (row.has_root_event_ids) {
            results.schemaCompliance.updatesWithRootEventIds++;
          } else {
            results.schemaCompliance.updatesWithoutRootEventIds++;
          }
          
          if (row.has_record_time) {
            results.schemaCompliance.updatesWithRecordTime++;
          } else {
            results.schemaCompliance.updatesWithoutRecordTime++;
          }
        }
        
        results.sampleDetails.push({
          type: 'updates',
          recordCount: updateSample.length,
          hasRequiredFields: results.updateFiles.missingUpdateDataJson === 0,
          missingFields: results.updateFiles.missingUpdateDataJson > 0 ? ['update_data'] : [],
        });
        
      } catch (err) {
        results.errors.push({ file: 'updates-*.parquet', error: err.message });
      }
    }
    
    // Calculate integrity score
    const totalChecked = results.eventFiles.checked + results.updateFiles.checked;
    const totalValid = results.eventFiles.valid + results.updateFiles.valid;
    const baseScore = totalChecked > 0 ? (totalValid / totalChecked) * 100 : 0;
    
    const sc = results.schemaCompliance;
    const totalEvents = sc.eventsWithTypeOriginal + sc.eventsWithoutTypeOriginal;
    const totalUpdates = sc.updatesWithRecordTime + sc.updatesWithoutRecordTime;
    
    let schemaScore = 100;
    if (totalEvents > 0) {
      const typeOriginalRatio = sc.eventsWithTypeOriginal / totalEvents;
      const canonicalIdRatio = sc.eventsWithCanonicalId / (sc.eventsWithCanonicalId + sc.eventsWithSynthesizedId || 1);
      schemaScore = schemaScore * (0.5 + 0.25 * typeOriginalRatio + 0.25 * canonicalIdRatio);
    }
    if (totalUpdates > 0) {
      const recordTimeRatio = sc.updatesWithRecordTime / totalUpdates;
      const rootEventIdsRatio = sc.updatesWithRootEventIds / (sc.updatesWithRootEventIds + sc.updatesWithoutRootEventIds || 1);
      schemaScore = schemaScore * (0.5 + 0.25 * recordTimeRatio + 0.25 * rootEventIdsRatio);
    }
    
    results.integrityScore = Math.round(baseScore * 0.7 + schemaScore * 0.3);
    results.schemaComplianceScore = Math.round(schemaScore);
    results.success = true;
    
    res.json(results);
  } catch (err) {
    console.error('Error validating integrity:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// POST /api/backfill/gaps/recover - Recover gaps with streaming progress - PROTECTED
router.post('/gaps/recover', requireAuth, async (req, res) => {
  const maxGaps = req.body?.maxGaps || 10;
  const stream = req.body?.stream === true;
  
  // Set up SSE for streaming progress
  if (stream) {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.flushHeaders();
  }
  
  const sendProgress = (data) => {
    if (stream) {
      res.write(`data: ${JSON.stringify(data)}\n\n`);
    }
  };
  
  try {
    // First detect gaps
    sendProgress({ type: 'progress', message: '🔍 Detecting gaps...' });
    const gapResult = await triggerGapDetection(false);
    
    if (!gapResult.gaps || gapResult.gaps.length === 0) {
      sendProgress({ type: 'progress', message: '✅ No gaps detected' });
      if (stream) {
        res.end();
      } else {
        res.json({ success: true, message: 'No gaps to recover', recovered: 0 });
      }
      return;
    }
    
    const gapsToRecover = gapResult.gaps.slice(0, maxGaps);
    sendProgress({ 
      type: 'progress', 
      message: `📋 Found ${gapResult.gaps.length} gaps, recovering ${gapsToRecover.length}...`,
      totalGaps: gapsToRecover.length,
    });
    
    let totalUpdates = 0;
    let totalEvents = 0;
    
    // Recovery would require importing the actual recovery functions
    // For now, trigger the detection with autoRecover=true
    sendProgress({ type: 'progress', message: '🔄 Starting auto-recovery...' });
    
    const recoveryResult = await triggerGapDetection(true);
    
    totalUpdates = recoveryResult.transactionsRecovered || 0;
    
    sendProgress({ 
      type: 'progress', 
      message: `✅ Recovery complete: ${totalUpdates} transactions found`,
      updatesRecovered: totalUpdates,
      eventsRecovered: totalEvents,
    });
    
    if (stream) {
      res.end();
    } else {
      res.json({ 
        success: true, 
        message: 'Recovery complete',
        updatesRecovered: totalUpdates,
        eventsRecovered: totalEvents,
      });
    }
  } catch (err) {
    console.error('Error recovering gaps:', err);
    sendProgress({ type: 'error', message: err.message });
    
    if (stream) {
      res.end();
    } else {
      res.status(500).json({ error: err.message });
    }
  }
});

/**
 * GET /api/backfill/sample-raw
 * Sample records from Parquet files to verify data integrity
 * Query params:
 *   - limit: max records to return (default 10)
 *   - type: 'events' or 'updates' (default both)
 *   - template: filter by template (for events)
 */
router.get('/sample-raw', async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 10, 100);
    const typeFilter = req.query.type; // 'events', 'updates', or undefined for both
    const templateFilter = req.query.template;
    
    const basePath = DATA_PATH.replace(/\\/g, '/');
    const hasParquetEvents = hasFileType('events', '.parquet');
    const hasParquetUpdates = hasFileType('updates', '.parquet');
    
    if (!hasParquetEvents && !hasParquetUpdates) {
      return res.json({ 
        error: 'No Parquet files found',
        path: basePath,
        typeFilter,
      });
    }
    
    const results = {
      dataFormat: 'parquet',
      typeFilter,
      templateFilter,
      samples: [],
    };
    
    // Sample events
    if (hasParquetEvents && (!typeFilter || typeFilter === 'events')) {
      try {
        let eventQuery = `
          SELECT 
            event_id,
            event_type,
            template_id,
            contract_id,
            COALESCE(timestamp, effective_at) as timestamp,
            payload
          FROM read_parquet('${basePath}/**/events-*.parquet', union_by_name=true)
        `;
        
        if (templateFilter) {
          eventQuery += ` WHERE template_id LIKE '%${templateFilter}%'`;
        }
        
        eventQuery += ` LIMIT ${limit}`;
        
        const eventRecords = await safeQuery(eventQuery);
        results.samples.push({
          type: 'events',
          recordCount: eventRecords.length,
          records: eventRecords,
          error: null,
        });
      } catch (err) {
        results.samples.push({
          type: 'events',
          recordCount: 0,
          records: [],
          error: err.message,
        });
      }
    }
    
    // Sample updates
    if (hasParquetUpdates && (!typeFilter || typeFilter === 'updates')) {
      try {
        const updateQuery = `
          SELECT 
            update_id,
            migration_id,
            record_time,
            root_event_ids
          FROM read_parquet('${basePath}/**/updates-*.parquet', union_by_name=true)
          LIMIT ${limit}
        `;
        
        const updateRecords = await safeQuery(updateQuery);
        results.samples.push({
          type: 'updates',
          recordCount: updateRecords.length,
          records: updateRecords,
          error: null,
        });
      } catch (err) {
        results.samples.push({
          type: 'updates',
          recordCount: 0,
          records: [],
          error: err.message,
        });
      }
    }
    
    // Summary stats
    const allRecords = results.samples.flatMap(s => s.records);
    results.summary = {
      totalRecordsReturned: allRecords.length,
      eventTypes: [...new Set(allRecords.map(r => r.event_type).filter(Boolean))],
      templates: [...new Set(allRecords.map(r => r.template_id).filter(Boolean))].slice(0, 20),
    };
    
    res.json(results);
  } catch (err) {
    console.error('Error sampling raw files:', err);
    res.status(500).json({ error: err.message });
  }
});

export default router;
