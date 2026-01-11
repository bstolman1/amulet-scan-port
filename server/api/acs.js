import { Router } from 'express';
import db from '../duckdb/connection.js';
import path from 'path';
import fs from 'fs';
import { getCached, setCache, getCacheStats, invalidateCache } from '../cache/stats-cache.js';
import {
  sanitizeNumber,
  sanitizeIdentifier,
  escapeLikePattern,
  escapeString,
} from '../lib/sql-sanitize.js';

const router = Router();

// Cache TTL for different endpoints
const CACHE_TTL = {
  RICH_LIST: 5 * 60 * 1000,     // 5 minutes
  SUPPLY: 5 * 60 * 1000,        // 5 minutes  
  MINING_ROUNDS: 5 * 60 * 1000, // 5 minutes
  ALLOCATIONS: 5 * 60 * 1000,   // 5 minutes
  TEMPLATES: 10 * 60 * 1000,    // 10 minutes
  STATS: 10 * 60 * 1000,        // 10 minutes
  SNAPSHOTS: 10 * 60 * 1000,    // 10 minutes
};

// Helper to convert BigInt to Number for JSON serialization
function serializeBigInt(obj) {
  return JSON.parse(JSON.stringify(obj, (_, v) => typeof v === 'bigint' ? Number(v) : v));
}

// ACS data path - use the centralized path from duckdb connection
const ACS_DATA_PATH = db.ACS_DATA_PATH;

// Find ACS files and return their paths (supports both JSONL and Parquet)
function findACSFiles() {
  try {
    if (!fs.existsSync(ACS_DATA_PATH)) {
      console.log(`[ACS] findACSFiles: ACS_DATA_PATH does not exist: ${ACS_DATA_PATH}`);
      return { jsonl: [], parquet: [] };
    }
    const allFiles = fs.readdirSync(ACS_DATA_PATH, { recursive: true });
    const jsonlFiles = allFiles
      .map(f => String(f))
      .filter(f => f.endsWith('.jsonl') || f.endsWith('.jsonl.gz') || f.endsWith('.jsonl.zst'))
      .map(f => path.join(ACS_DATA_PATH, f).replace(/\\/g, '/')); // Normalize for DuckDB
    
    const parquetFiles = allFiles
      .map(f => String(f))
      .filter(f => f.endsWith('.parquet'))
      .map(f => path.join(ACS_DATA_PATH, f).replace(/\\/g, '/')); // Normalize for DuckDB
    
    if (jsonlFiles.length > 0 || parquetFiles.length > 0) {
      console.log(`[ACS] findACSFiles: Found ${parquetFiles.length} parquet, ${jsonlFiles.length} jsonl files`);
    }
    return { jsonl: jsonlFiles, parquet: parquetFiles };
  } catch (err) {
    console.error(`[ACS] findACSFiles error: ${err.message}`);
    return { jsonl: [], parquet: [] };
  }
}

// Helper to get ACS source - builds query from actual files found
// IMPORTANT: Prefers Parquet files for better performance, falls back to JSONL
// Uses UNION (not UNION ALL) to prevent duplicate records
const getACSSource = () => {
  const { jsonl: jsonlFiles, parquet: parquetFiles } = findACSFiles();
  
  // Prefer Parquet files for faster queries
  if (parquetFiles.length > 0) {
    const uniqueParquet = [...new Set(parquetFiles)];
    
    if (uniqueParquet.length <= 100) {
      const selects = uniqueParquet.map(f => 
        `SELECT * FROM read_parquet('${f}')`
      );
      console.log(`[ACS] Using ${uniqueParquet.length} Parquet files (optimized)`);
      return `(${selects.join(' UNION ')})`;
    }
    
    // For large counts, use glob pattern
    const acsPath = ACS_DATA_PATH.replace(/\\/g, '/');
    console.log(`[ACS] Using Parquet glob pattern for ${uniqueParquet.length} files`);
    return `(SELECT * FROM read_parquet('${acsPath}/**/*.parquet', union_by_name=true))`;
  }
  
  // Fall back to JSONL if no Parquet files
  if (jsonlFiles.length === 0) {
    return `(SELECT NULL as placeholder WHERE false)`;
  }
  
  // Deduplicate files to prevent double-counting
  const uniqueFiles = [...new Set(jsonlFiles)];
  
  // For small file counts, use explicit list with UNION to deduplicate
  if (uniqueFiles.length <= 100) {
    const selects = uniqueFiles.map(f => 
      `SELECT * FROM read_json_auto('${f}', union_by_name=true, ignore_errors=true)`
    );
    return `(${selects.join(' UNION ')})`;
  }
  
  // For large counts, use glob but only for file types that exist
  // Group by extension type to avoid overlap
  const acsPath = ACS_DATA_PATH.replace(/\\/g, '/');
  
  // Separate files by extension type
  const plainJsonl = uniqueFiles.filter(f => f.endsWith('.jsonl') && !f.endsWith('.jsonl.gz') && !f.endsWith('.jsonl.zst'));
  const gzFiles = uniqueFiles.filter(f => f.endsWith('.jsonl.gz'));
  const zstFiles = uniqueFiles.filter(f => f.endsWith('.jsonl.zst'));
  
  const parts = [];
  if (plainJsonl.length > 0) {
    parts.push(`SELECT * FROM read_json_auto('${acsPath}/**/*.jsonl', union_by_name=true, ignore_errors=true)`);
  }
  if (gzFiles.length > 0) {
    parts.push(`SELECT * FROM read_json_auto('${acsPath}/**/*.jsonl.gz', union_by_name=true, ignore_errors=true)`);
  }
  if (zstFiles.length > 0) {
    parts.push(`SELECT * FROM read_json_auto('${acsPath}/**/*.jsonl.zst', union_by_name=true, ignore_errors=true)`);
  }
  
  // Use UNION to deduplicate across all sources
  return parts.length > 0 ? `(${parts.join(' UNION ')})` : `(SELECT NULL as placeholder WHERE false)`;
};

// Check if ACS data exists (Parquet or JSONL)
function hasACSData() {
  const { jsonl, parquet } = findACSFiles();
  return jsonl.length > 0 || parquet.length > 0;
}

// Find completion marker files to identify complete snapshots
function findCompleteSnapshots() {
  try {
    if (!fs.existsSync(ACS_DATA_PATH)) return [];
    const allFiles = fs.readdirSync(ACS_DATA_PATH, { recursive: true });
    return allFiles
      .map(f => String(f))
      .filter(f => f.endsWith('_COMPLETE'))
      .map(f => {
        // Parse partition path - supports both old and new formats:
        // Old: migration_id=X/snapshot_time=YYYY-MM-DDTHH-MM-SS/_COMPLETE
        // New: migration=X/year=YYYY/month=MM/day=DD/snapshot=HHMMSS/_COMPLETE
        const parts = f.split(/[\/\\]/);
        let migrationId = null;
        let snapshotTime = null;
        let year = null, month = null, day = null, snapshot = null;
        
        for (const part of parts) {
          if (part.startsWith('migration_id=')) {
            migrationId = parseInt(part.replace('migration_id=', ''));
          } else if (part.startsWith('migration=')) {
            migrationId = parseInt(part.replace('migration=', ''));
          } else if (part.startsWith('snapshot_time=')) {
            snapshotTime = part.replace('snapshot_time=', '').replace(/-/g, (m, offset) => 
              offset > 9 ? ':' : m
            );
            const match = snapshotTime.match(/^(\d{4}-\d{2}-\d{2}T)(\d{2})-(\d{2})-(\d{2})(.*)$/);
            if (match) {
              snapshotTime = `${match[1]}${match[2]}:${match[3]}:${match[4]}${match[5]}`;
            }
          } else if (part.startsWith('year=')) {
            year = part.replace('year=', '');
          } else if (part.startsWith('month=')) {
            month = part.replace('month=', '');
          } else if (part.startsWith('day=')) {
            day = part.replace('day=', '');
          } else if (part.startsWith('snapshot=')) {
            snapshot = part.replace('snapshot=', '');
          }
        }
        
        // Build snapshot time from year/month/day/snapshot if not already set
        if (!snapshotTime && year && month && day && snapshot) {
          // snapshot format: HHMMSS -> HH:MM:SS
          const hh = snapshot.substring(0, 2);
          const mm = snapshot.substring(2, 4);
          const ss = snapshot.substring(4, 6) || '00';
          snapshotTime = `${year}-${month}-${day}T${hh}:${mm}:${ss}`;
        }
        
        // Get the directory path (parent of _COMPLETE file) as absolute path
        const relativeDirPath = f.replace(/[\/\\]_COMPLETE$/, '');
        const absoluteDirPath = path.join(ACS_DATA_PATH, relativeDirPath);
        
        return { migrationId, snapshotTime, path: absoluteDirPath };
      })
      .filter(s => s.migrationId !== null && s.snapshotTime !== null);
  } catch {
    return [];
  }
}

// Scan filesystem for available snapshots (even without _COMPLETE markers)
// Supports both old format (migration_id=/snapshot_time=) and new format (migration=/year=/month=/day=/snapshot=)
function findAvailableSnapshots() {
  try {
    if (!fs.existsSync(ACS_DATA_PATH)) {
      console.log(`[ACS] ACS_DATA_PATH does not exist: ${ACS_DATA_PATH}`);
      return [];
    }
    
    const snapshots = [];
    const entries = fs.readdirSync(ACS_DATA_PATH, { withFileTypes: true });
    
    console.log(`[ACS] Scanning ACS_DATA_PATH: ${ACS_DATA_PATH}`);
    console.log(`[ACS] Found top-level entries: ${entries.map(e => e.name).join(', ')}`);
    
    for (const entry of entries) {
      if (!entry.isDirectory()) continue;
      
      let migrationId = null;
      
      // Support both formats
      if (entry.name.startsWith('migration_id=')) {
        migrationId = parseInt(entry.name.replace('migration_id=', ''));
      } else if (entry.name.startsWith('migration=')) {
        migrationId = parseInt(entry.name.replace('migration=', ''));
      } else {
        continue;
      }
      
      const migrationPath = path.join(ACS_DATA_PATH, entry.name);
      
      // Check for old format: direct snapshot_time= directories
      const migrationContents = fs.readdirSync(migrationPath, { withFileTypes: true });
      
      for (const subEntry of migrationContents) {
        if (!subEntry.isDirectory()) continue;
        
        if (subEntry.name.startsWith('snapshot_time=')) {
          // Old format: migration_id=X/snapshot_time=YYYY-MM-DDTHH-MM-SS/
          let snapshotTime = subEntry.name.replace('snapshot_time=', '');
          const match = snapshotTime.match(/^(\d{4}-\d{2}-\d{2}T)(\d{2})-(\d{2})-(\d{2})(.*)$/);
          if (match) {
            snapshotTime = `${match[1]}${match[2]}:${match[3]}:${match[4]}${match[5]}`;
          }
          
        const snapshotPath = path.join(migrationPath, subEntry.name);
        const files = fs.readdirSync(snapshotPath);
        const hasJsonl = files.some(f => f.endsWith('.jsonl') || f.endsWith('.jsonl.gz') || f.endsWith('.jsonl.zst'));
        const hasParquet = files.some(f => f.endsWith('.parquet'));
        const hasData = hasJsonl || hasParquet;
        const isComplete = files.includes('_COMPLETE');
        
        if (hasData) {
          snapshots.push({ migrationId, snapshotTime, isComplete, path: snapshotPath, format: hasParquet ? 'parquet' : 'jsonl' });
        }
        } else if (subEntry.name.startsWith('year=')) {
          // New format: migration=X/year=YYYY/month=MM/day=DD/snapshot=HHMMSS/
          const year = subEntry.name.replace('year=', '');
          const yearPath = path.join(migrationPath, subEntry.name);
          
          const monthDirs = fs.readdirSync(yearPath, { withFileTypes: true })
            .filter(d => d.isDirectory() && d.name.startsWith('month='));
          
          for (const monthDir of monthDirs) {
            const month = monthDir.name.replace('month=', '');
            const monthPath = path.join(yearPath, monthDir.name);
            
            const dayDirs = fs.readdirSync(monthPath, { withFileTypes: true })
              .filter(d => d.isDirectory() && d.name.startsWith('day='));
            
            for (const dayDir of dayDirs) {
              const day = dayDir.name.replace('day=', '');
              const dayPath = path.join(monthPath, dayDir.name);
              
              const snapshotDirs = fs.readdirSync(dayPath, { withFileTypes: true })
                .filter(d => d.isDirectory() && d.name.startsWith('snapshot='));
              
              for (const snapDir of snapshotDirs) {
                const snapshot = snapDir.name.replace('snapshot=', '');
                const snapshotPath = path.join(dayPath, snapDir.name);
                
                // Build ISO timestamp from components
                const hh = snapshot.substring(0, 2) || '00';
                const mm = snapshot.substring(2, 4) || '00';
                const ss = snapshot.substring(4, 6) || '00';
                const snapshotTime = `${year}-${month}-${day}T${hh}:${mm}:${ss}`;
                
            const files = fs.readdirSync(snapshotPath);
            const hasJsonl = files.some(f => f.endsWith('.jsonl') || f.endsWith('.jsonl.gz') || f.endsWith('.jsonl.zst'));
            const hasParquet = files.some(f => f.endsWith('.parquet'));
            const hasData = hasJsonl || hasParquet;
            const isComplete = files.includes('_COMPLETE');
            
            if (hasData) {
              console.log(`[ACS] Found snapshot: migration=${migrationId}, time=${snapshotTime}, format=${hasParquet ? 'parquet' : 'jsonl'}, path=${snapshotPath}`);
              snapshots.push({ migrationId, snapshotTime, isComplete, path: snapshotPath, format: hasParquet ? 'parquet' : 'jsonl' });
            }
              }
            }
          }
        }
      }
    }
    
    console.log(`[ACS] Total snapshots found: ${snapshots.length}`);
    return snapshots;
  } catch (err) {
    console.error('[ACS] Error scanning snapshots:', err.message);
    return [];
  }
}

// Get ACS source for a specific snapshot path (much more efficient than reading all files)
// Prefers Parquet files for better query performance
function getSnapshotFilesSource(snapshotPath) {
  if (!snapshotPath || !fs.existsSync(snapshotPath)) {
    return null;
  }
  
  const normalizedPath = snapshotPath.replace(/\\/g, '/');
  
  // Check what file types exist in this snapshot
  const files = fs.readdirSync(snapshotPath);
  const hasParquet = files.some(f => f.endsWith('.parquet'));
  const hasJsonl = files.some(f => f.endsWith('.jsonl') && !f.endsWith('.jsonl.gz') && !f.endsWith('.jsonl.zst'));
  const hasGz = files.some(f => f.endsWith('.jsonl.gz'));
  const hasZst = files.some(f => f.endsWith('.jsonl.zst'));
  
  // Prefer Parquet files for better performance
  if (hasParquet) {
    const parquetCount = files.filter(f => f.endsWith('.parquet')).length;
    console.log(`[ACS] Using optimized Parquet source for snapshot: ${normalizedPath} (${parquetCount} files)`);
    return `(SELECT * FROM read_parquet('${normalizedPath}/*.parquet', union_by_name=true))`;
  }
  
  // Fall back to JSONL
  const parts = [];
  if (hasJsonl) {
    parts.push(`SELECT * FROM read_json_auto('${normalizedPath}/*.jsonl', union_by_name=true, ignore_errors=true)`);
  }
  if (hasGz) {
    parts.push(`SELECT * FROM read_json_auto('${normalizedPath}/*.jsonl.gz', union_by_name=true, ignore_errors=true)`);
  }
  if (hasZst) {
    parts.push(`SELECT * FROM read_json_auto('${normalizedPath}/*.jsonl.zst', union_by_name=true, ignore_errors=true)`);
  }
  
  if (parts.length === 0) return null;
  
  console.log(`[ACS] Using JSONL source for snapshot: ${normalizedPath} (${files.filter(f => f.endsWith('.jsonl') || f.endsWith('.jsonl.gz') || f.endsWith('.jsonl.zst')).length} files)`);
  // Use UNION (not UNION ALL) to prevent duplicate records across file types
  return `(${parts.join(' UNION ')})`;
}

// Get the best snapshot and its source (returns both snapshot info and optimized DuckDB source)
function getBestSnapshotAndSource() {
  // First try: Find complete snapshots from filesystem
  const completeSnapshots = findCompleteSnapshots();
  
  if (completeSnapshots.length > 0) {
    completeSnapshots.sort((a, b) => {
      if (b.migrationId !== a.migrationId) return b.migrationId - a.migrationId;
      return new Date(b.snapshotTime) - new Date(a.snapshotTime);
    });
    
    const latest = completeSnapshots[0];
    const source = getSnapshotFilesSource(latest.path);
    if (source) {
      console.log(`[ACS] Using complete snapshot: migration_id=${latest.migrationId}, snapshot_time=${latest.snapshotTime}`);
      return { snapshot: latest, source, type: 'complete' };
    }
  }
  
  // Second try: Use available snapshots from filesystem
  const availableSnapshots = findAvailableSnapshots();
  
  if (availableSnapshots.length > 0) {
    availableSnapshots.sort((a, b) => {
      if (b.migrationId !== a.migrationId) return b.migrationId - a.migrationId;
      return new Date(b.snapshotTime) - new Date(a.snapshotTime);
    });
    
    const latest = availableSnapshots[0];
    const source = getSnapshotFilesSource(latest.path);
    if (source) {
      console.log(`[ACS] Using filesystem snapshot: migration_id=${latest.migrationId}, snapshot_time=${latest.snapshotTime}`);
      return { snapshot: latest, source, type: 'available' };
    }
  }
  
  // Final fallback: Use all ACS files (expensive!)
  console.log('[ACS] WARNING: No snapshots found, falling back to all files');
  return { snapshot: null, source: getACSSource(), type: 'fallback' };
}

// Helper function to get CTE for latest COMPLETE snapshot (filters by latest migration_id first)
// If snapshotTime is provided, uses that specific snapshot instead of latest
// IMPORTANT: Only uses snapshots that have a _COMPLETE marker file
function getSnapshotCTE(acsSource, snapshotTime = null, migrationId = null) {
  if (snapshotTime) {
    // Use specific snapshot
    return `
      snapshot_params AS (
        SELECT 
          '${snapshotTime}'::TIMESTAMP as snapshot_time,
          ${migrationId || `(SELECT MAX(migration_id) FROM ${acsSource} WHERE snapshot_time = '${snapshotTime}'::TIMESTAMP)`} as migration_id
      ),
      latest_migration AS (SELECT migration_id FROM snapshot_params),
      latest_snapshot AS (SELECT snapshot_time, migration_id FROM snapshot_params)
    `;
  }
  
  // Use getBestSnapshotAndSource for filesystem-based detection
  const { snapshot } = getBestSnapshotAndSource();
  
  if (snapshot) {
    return `
      latest_migration AS (
        SELECT ${snapshot.migrationId} as migration_id
      ),
      latest_snapshot AS (
        SELECT '${snapshot.snapshotTime}'::TIMESTAMP as snapshot_time, ${snapshot.migrationId} as migration_id
      )
    `;
  }
  
  // Final fallback: Query the data (may be slow/fail for large datasets)
  console.log('[ACS] WARNING: No snapshots found via filesystem scan, falling back to data query');
  return `
    latest_migration AS (
      SELECT MAX(migration_id) as migration_id FROM ${acsSource}
    ),
    latest_snapshot AS (
      SELECT MAX(snapshot_time) as snapshot_time, (SELECT migration_id FROM latest_migration) as migration_id
      FROM ${acsSource}
      WHERE migration_id = (SELECT migration_id FROM latest_migration)
    )
  `;
}

// Backwards compatible alias
function getLatestSnapshotCTE(acsSource) {
  return getSnapshotCTE(acsSource);
}

// GET /api/acs/debug - Debug endpoint showing paths, snapshots, and file counts
router.get('/debug', (req, res) => {
  try {
    const completeSnapshots = findCompleteSnapshots();
    const availableSnapshots = findAvailableSnapshots();
    const files = findACSFiles();
    
    // Get selected snapshot info
    let selectedSnapshot = null;
    if (completeSnapshots.length > 0) {
      completeSnapshots.sort((a, b) => {
        if (b.migrationId !== a.migrationId) return b.migrationId - a.migrationId;
        return new Date(b.snapshotTime) - new Date(a.snapshotTime);
      });
      selectedSnapshot = { ...completeSnapshots[0], source: 'complete' };
    } else if (availableSnapshots.length > 0) {
      availableSnapshots.sort((a, b) => {
        if (b.migrationId !== a.migrationId) return b.migrationId - a.migrationId;
        return new Date(b.snapshotTime) - new Date(a.snapshotTime);
      });
      selectedSnapshot = { ...availableSnapshots[0], source: 'available' };
    }
    
    res.json({
      acsDataPath: ACS_DATA_PATH,
      pathExists: fs.existsSync(ACS_DATA_PATH),
      totalFiles: files.length,
      sampleFiles: files.slice(0, 5),
      completeSnapshots: completeSnapshots.length,
      availableSnapshots: availableSnapshots.length,
      allSnapshots: availableSnapshots.map(s => ({
        migrationId: s.migrationId,
        snapshotTime: s.snapshotTime,
        isComplete: s.isComplete,
        path: s.path,
      })),
      selectedSnapshot,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/acs/sample - Get sample raw contracts to verify schema and data
router.get('/sample', async (req, res) => {
  try {
    if (!hasACSData()) {
      return res.json({ data: [], message: 'No ACS data available' });
    }

    const limit = sanitizeNumber(req.query.limit, { min: 1, max: 100, defaultValue: 10 });
    const { snapshot, source: acsSource } = getBestSnapshotAndSource();

    // Get sample rows with all columns
    const sql = `
      SELECT *
      FROM ${acsSource}
      LIMIT ${limit}
    `;

    const rows = await db.safeQuery(sql);

    // Also get distinct template_ids to show what's available
    const templateSql = `
      SELECT DISTINCT 
        COALESCE(template_id, 'NULL') as template_id,
        COALESCE(entity_name, 'NULL') as entity_name,
        COALESCE(module_name, 'NULL') as module_name
      FROM ${acsSource}
      ORDER BY template_id
      LIMIT 50
    `;
    
    const templates = await db.safeQuery(templateSql);

    // Get total count
    const countSql = `SELECT COUNT(*) as cnt FROM ${acsSource}`;
    const countRows = await db.safeQuery(countSql);
    const totalCount = Number(countRows?.[0]?.cnt || 0);

    // Check for governance-related templates specifically
    const governanceSql = `
      SELECT 
        COALESCE(template_id, 'NULL') as template_id,
        COUNT(*) as cnt
      FROM ${acsSource}
      WHERE template_id LIKE '%Vote%'
         OR template_id LIKE '%Dso%'
         OR template_id LIKE '%Confirmation%'
         OR template_id LIKE '%Governance%'
         OR entity_name LIKE '%Vote%'
         OR entity_name LIKE '%Dso%'
      GROUP BY template_id
      ORDER BY cnt DESC
      LIMIT 20
    `;
    
    let governanceTemplates = [];
    try {
      governanceTemplates = await db.safeQuery(governanceSql);
    } catch {}

    res.json(serializeBigInt({
      snapshot: snapshot ? {
        migration_id: snapshot.migrationId,
        snapshot_time: snapshot.snapshotTime,
        path: snapshot.path,
      } : null,
      totalContracts: totalCount,
      sampleContracts: rows,
      availableTemplates: templates,
      governanceTemplates,
      columns: rows.length > 0 ? Object.keys(rows[0]) : [],
    }));
  } catch (err) {
    console.error('ACS sample error:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/acs/cache - Get cache statistics (for debugging)
router.get('/cache', (req, res) => {
  res.json(getCacheStats());
});

// POST /api/acs/cache/invalidate - Invalidate cache
router.post('/cache/invalidate', (req, res) => {
  const { prefix } = req.body || {};
  invalidateCache(prefix || 'acs:');
  res.json({ status: 'ok', message: 'Cache invalidated' });
});

// GET /api/acs/status - Get ACS availability status (for graceful degradation during snapshots)
router.get('/status', (req, res) => {
  try {
    const completeSnapshots = findCompleteSnapshots();
    const availableSnapshots = findAvailableSnapshots();
    
    // Find in-progress snapshots (available but not complete)
    const completeSet = new Set(
      completeSnapshots.map(s => `${s.migrationId}:${s.snapshotTime}`)
    );
    const inProgressSnapshots = availableSnapshots.filter(s => 
      !s.isComplete && !completeSet.has(`${s.migrationId}:${s.snapshotTime}`)
    );
    
    const hasCompleteSnapshot = completeSnapshots.length > 0;
    const hasInProgressSnapshot = inProgressSnapshots.length > 0;
    
    // Get info about the best available snapshot
    const bestSnapshot = completeSnapshots.length > 0 
      ? completeSnapshots.sort((a, b) => {
          if (b.migrationId !== a.migrationId) return b.migrationId - a.migrationId;
          return new Date(b.snapshotTime) - new Date(a.snapshotTime);
        })[0]
      : null;
    
    res.json({
      available: hasCompleteSnapshot,
      snapshotInProgress: hasInProgressSnapshot,
      completeSnapshotCount: completeSnapshots.length,
      inProgressSnapshotCount: inProgressSnapshots.length,
      latestComplete: bestSnapshot ? {
        migrationId: bestSnapshot.migrationId,
        snapshotTime: bestSnapshot.snapshotTime,
      } : null,
      message: hasCompleteSnapshot 
        ? (hasInProgressSnapshot ? 'Data available, snapshot update in progress' : 'Data available')
        : (hasInProgressSnapshot ? 'Snapshot in progress, please wait...' : 'No ACS data available'),
    });
  } catch (err) {
    console.error('ACS status error:', err);
    res.status(500).json({ 
      available: false, 
      snapshotInProgress: false,
      error: err.message,
      message: 'Error checking ACS status',
    });
  }
});

// GET /api/acs/snapshots - List all available snapshots (uses filesystem scan, not data query)
router.get('/snapshots', async (req, res) => {
  try {
    // Check cache first
    const cacheKey = 'acs:v2:snapshots';
    const cached = getCached(cacheKey);
    if (cached) {
      return res.json(cached);
    }

    // Use filesystem-based snapshot discovery (fast, no data query)
    const availableSnapshots = findAvailableSnapshots();
    const completeSnapshots = findCompleteSnapshots();
    
    if (availableSnapshots.length === 0) {
      return res.json({ data: [], message: 'No ACS snapshots found' });
    }
    
    // Build complete set for status marking
    const completeSet = new Set(
      completeSnapshots.map(s => `${s.migrationId}:${s.snapshotTime}`)
    );
    
    // Sort by migration DESC, then snapshot_time DESC
    availableSnapshots.sort((a, b) => {
      if (b.migrationId !== a.migrationId) return b.migrationId - a.migrationId;
      return new Date(b.snapshotTime) - new Date(a.snapshotTime);
    });
    
    // Transform to match the UI's expected format
    // Check which snapshot has actual data files (not just markers)
    const snapshots = availableSnapshots.slice(0, 50).map((s) => {
      const snapshotTimeStr = new Date(s.snapshotTime).toISOString();
      const isComplete = s.isComplete || completeSet.has(`${s.migrationId}:${s.snapshotTime}`);
      
      // Check if this snapshot has actual data files
      let hasDataFiles = false;
      let fileCount = 0;
      try {
        if (s.path && fs.existsSync(s.path)) {
          const files = fs.readdirSync(s.path);
          const dataFiles = files.filter(f => f.endsWith('.jsonl') || f.endsWith('.jsonl.gz') || f.endsWith('.jsonl.zst'));
          fileCount = dataFiles.length;
          hasDataFiles = fileCount > 0;
        }
      } catch {}
      
      return {
        id: `local-m${s.migrationId}-${snapshotTimeStr}`,
        timestamp: s.snapshotTime,
        migration_id: s.migrationId,
        record_time: s.snapshotTime,
        entry_count: hasDataFiles ? null : 0, // null means "has data, count unknown"; 0 means archived
        template_count: 0,
        status: isComplete ? 'completed' : 'in_progress',
        source: 'local',
        path: s.path,
        has_data: hasDataFiles,
        file_count: fileCount,
      };
    });

    console.log(`[ACS] Returning ${snapshots.length} snapshots from filesystem scan`);
    const result = serializeBigInt({ data: snapshots });
    setCache(cacheKey, result, CACHE_TTL.SNAPSHOTS);
    res.json(result);
  } catch (err) {
    console.error('ACS snapshots error:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/acs/latest - Get latest snapshot summary with supply metrics (optimized)
router.get('/latest', async (req, res) => {
  try {
    if (!hasACSData()) {
      return res.json({ data: null, message: 'No ACS data available' });
    }

    // Check cache
    const cacheKey = 'acs:v2:latest';
    const cached = getCached(cacheKey);
    if (cached) {
      return res.json(cached);
    }

    // Use optimized snapshot source
    const { snapshot, source: acsSource } = getBestSnapshotAndSource();
    
    if (!snapshot) {
      return res.json({ data: null, message: 'No snapshot found' });
    }

    // Get basic counts from the optimized source
    const basicSql = `
      SELECT 
        COUNT(DISTINCT contract_id) as contract_count,
        COUNT(DISTINCT template_id) as template_count,
        MIN(record_time) as record_time
      FROM ${acsSource}
    `;

    const basicRows = await db.safeQuery(basicSql);
    const row = basicRows[0] || {};

    // Calculate supply metrics from Amulet and LockedAmulet contracts
    const supplySql = `
      WITH latest_contracts AS (
        SELECT
          contract_id,
          any_value(template_id) as template_id,
          any_value(entity_name) as entity_name,
          any_value(payload) as payload
        FROM ${acsSource}
        GROUP BY contract_id
      ),
      amulet_totals AS (
        SELECT 
          COALESCE(SUM(
            CAST(
              COALESCE(
                payload->>'$.amount.initialAmount',
                payload->'amount'->>'initialAmount',
                '0'
              ) AS DOUBLE
            )
          ), 0) as amulet_total
        FROM latest_contracts
        WHERE entity_name = 'Amulet'
           OR (template_id LIKE '%:Amulet:%' AND template_id NOT LIKE '%:LockedAmulet:%')
       ),
       locked_totals AS (
         SELECT 
           COALESCE(SUM(
             CAST(
               COALESCE(
                 payload->>'$.amulet.amount.initialAmount',
                 payload->'amulet'->'amount'->>'initialAmount',
                 '0'
               ) AS DOUBLE
             )
           ), 0) as locked_total
         FROM latest_contracts
         WHERE entity_name = 'LockedAmulet' OR template_id LIKE '%:LockedAmulet:%'
      )
      SELECT 
        amulet_totals.amulet_total,
        locked_totals.locked_total
      FROM amulet_totals, locked_totals
    `;

    let amuletTotal = 0;
    let lockedTotal = 0;
    
    try {
      const supplyRows = await db.safeQuery(supplySql);
      if (supplyRows.length > 0) {
        amuletTotal = supplyRows[0].amulet_total || 0;
        lockedTotal = supplyRows[0].locked_total || 0;
      }
    } catch (supplyErr) {
      console.warn('Could not calculate supply metrics:', supplyErr.message);
    }

    const circulatingSupply = amuletTotal - lockedTotal;

    const result = serializeBigInt({
      data: {
        id: 'local-latest',
        timestamp: snapshot.snapshotTime,
        migration_id: snapshot.migrationId,
        record_time: row.record_time || snapshot.snapshotTime,
        entry_count: row.contract_count || 0,
        template_count: row.template_count || 0,
        amulet_total: amuletTotal,
        locked_total: lockedTotal,
        circulating_supply: circulatingSupply,
        status: snapshot.isComplete ? 'completed' : 'in_progress',
        source: 'local',
      }
    });
    
    setCache(cacheKey, result, CACHE_TTL.SUPPLY);
    res.json(result);
  } catch (err) {
    console.error('ACS latest error:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/acs/templates - Get template statistics from latest snapshot
router.get('/templates', async (req, res) => {
  try {
    if (!hasACSData()) {
      return res.json({ data: [] });
    }

    const limit = sanitizeNumber(req.query.limit, { min: 1, max: 500, defaultValue: 100 });
    
    // Check cache
    const cacheKey = `acs:v2:templates:${limit}`;
    const cached = getCached(cacheKey);
    if (cached) {
      return res.json(cached);
    }

    // Use optimized snapshot source
    const { snapshot, source: acsSource } = getBestSnapshotAndSource();
    
    const sql = `
      SELECT 
        template_id,
        entity_name,
        module_name,
        COUNT(DISTINCT contract_id) as contract_count,
        COUNT(DISTINCT contract_id) as unique_contracts
      FROM ${acsSource} acs
      GROUP BY template_id, entity_name, module_name
      ORDER BY contract_count DESC
      LIMIT ${limit}
    `;

    const rows = await db.safeQuery(sql);
    const result = serializeBigInt({ data: rows });
    setCache(cacheKey, result, CACHE_TTL.TEMPLATES);
    res.json(result);
  } catch (err) {
    console.error('ACS templates error:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/acs/templates/search - Search for templates by suffix/pattern
router.get('/templates/search', async (req, res) => {
  try {
    const { q } = req.query;
    if (!q) {
      return res.status(400).json({ error: 'Missing query parameter "q"' });
    }
    
    // Validate query length and characters to prevent injection
    const query = String(q).slice(0, 500);
    if (!/^[\w.:@-]+$/i.test(query)) {
      return res.status(400).json({ error: 'Invalid query format. Only alphanumeric, dots, colons, underscores, hyphens and @ allowed.' });
    }

    if (!hasACSData()) {
      return res.json({ data: [], found: false, query: q });
    }

    const { snapshot, source: acsSource } = getBestSnapshotAndSource();

    // Normalize query for flexible matching - handle all separator formats
    const variants = new Set([
      query,
      query.replaceAll(':', '.'),  // Splice:DsoRules:VoteRequest -> Splice.DsoRules.VoteRequest
      query.replaceAll('.', ':'),  // Splice.DsoRules.VoteRequest -> Splice:DsoRules:VoteRequest
      query.replaceAll(':', '_'),  // Splice:DsoRules:VoteRequest -> Splice_DsoRules_VoteRequest
      query.replaceAll('.', '_'),  // Splice.DsoRules.VoteRequest -> Splice_DsoRules_VoteRequest
      query.replaceAll('_', ':'),  // Splice_DsoRules_VoteRequest -> Splice:DsoRules:VoteRequest
      query.replaceAll('_', '.'),  // Splice_DsoRules_VoteRequest -> Splice.DsoRules.VoteRequest
    ]);

    // Use escapeLikePattern for safe LIKE queries
    const like = (col, v) => `${col} LIKE '%${escapeLikePattern(v)}%' ESCAPE '\\\\'`;
    const where = [...variants]
      .flatMap((v) => [
        like('tid', v),
        like('entity_name', v.split(/[:._]/).pop() || v), // Also match just the entity name
      ])
      .join(' OR ');

    const sql = `
      WITH base AS (
        SELECT
          COALESCE(
            template_id,
            json_extract_string(payload, '$.template_id'),
            json_extract_string(payload, '$.templateId')
          ) AS tid,
          entity_name,
          module_name,
          contract_id
        FROM ${acsSource}
      )
      SELECT
        tid as template_id,
        entity_name,
        module_name,
        COUNT(DISTINCT contract_id) as contract_count
      FROM base
      WHERE tid IS NOT NULL AND (${where})
      GROUP BY tid, entity_name, module_name
      ORDER BY contract_count DESC
      LIMIT 20
    `;

    const rows = await db.safeQuery(sql);

    // Add quick snapshot context and fallback hints when no match
    let totalTemplates = 0;
    let sampleTemplates = [];
    if (rows.length === 0) {
      try {
        const tRows = await db.safeQuery(`
          WITH base AS (
            SELECT COALESCE(template_id, json_extract_string(payload, '$.template_id'), json_extract_string(payload, '$.templateId')) AS tid
            FROM ${acsSource}
          )
          SELECT COUNT(DISTINCT tid) as cnt
          FROM base
          WHERE tid IS NOT NULL
        `);
        totalTemplates = Number(tRows?.[0]?.cnt || 0);

        const sRows = await db.safeQuery(`
          WITH base AS (
            SELECT COALESCE(template_id, json_extract_string(payload, '$.template_id'), json_extract_string(payload, '$.templateId')) AS tid
            FROM ${acsSource}
          )
          SELECT DISTINCT tid
          FROM base
          WHERE tid IS NOT NULL
          ORDER BY tid
          LIMIT 20
        `);
        sampleTemplates = sRows.map(r => r.tid).filter(Boolean);
      } catch {
        // ignore
      }
    }

    console.log(`[ACS] Template search for "${q}": found ${rows.length} matching templates`);

    res.json(serializeBigInt({
      data: rows,
      found: rows.length > 0,
      query: q,
      snapshot: snapshot ? {
        migration_id: snapshot.migrationId,
        snapshot_time: snapshot.snapshotTime,
        path: snapshot.path,
      } : null,
      debug: rows.length === 0 ? { totalTemplates, sampleTemplates } : undefined,
    }));
  } catch (err) {
    console.error('ACS template search error:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/acs/snapshot-info - Get info about the currently selected snapshot
router.get('/snapshot-info', (req, res) => {
  try {
    const { snapshot, type } = getBestSnapshotAndSource();
    
    if (!snapshot) {
      return res.json({ data: null, message: 'No snapshot available' });
    }
    
    // Count files in the snapshot directory
    let fileCount = 0;
    try {
      const files = fs.readdirSync(snapshot.path);
      fileCount = files.filter(f => 
        f.endsWith('.jsonl') || f.endsWith('.jsonl.gz') || f.endsWith('.jsonl.zst')
      ).length;
    } catch {}
    
    res.json({
      data: {
        migration_id: snapshot.migrationId,
        snapshot_time: snapshot.snapshotTime,
        path: snapshot.path,
        type, // 'complete', 'available', or 'fallback'
        file_count: fileCount,
      },
    });
  } catch (err) {
    console.error('ACS snapshot-info error:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/acs/contracts - Get contracts by template with parsed payload
// Note: Not cached because it depends on template/entity query params and is paginated
router.get('/contracts', async (req, res) => {
  try {
    if (!hasACSData()) {
      console.log('[ACS] No ACS data available');
      return res.json({ data: [], count: 0 });
    }

    const { template, entity } = req.query;
    const limit = sanitizeNumber(req.query.limit, { min: 1, max: 100000, defaultValue: 100 });
    const offset = sanitizeNumber(req.query.offset, { min: 0, defaultValue: 0 });

    console.log(`[ACS] Contracts request: template=${template}, entity=${entity}, limit=${limit}`);

    let whereClause = '1=1';
    if (template) {
      // Validate template format
      const t = String(template).slice(0, 500);
      if (!/^[\w.:@-]+$/i.test(t)) {
        return res.status(400).json({ error: 'Invalid template format' });
      }
      
      // Normalize template query to handle all separator formats
      const parts = t.split(/[:._]/);
      const entityName = parts.pop() || t;
      const moduleName = parts.length >= 1 ? parts[parts.length - 1] : null;
      
      // Build more precise matching patterns using escaped values
      const likeClauses = [];
      const escapedEntity = escapeLikePattern(entityName);
      const escapedModule = moduleName ? escapeLikePattern(moduleName) : null;
      
      // Match full qualified names (ending with :EntityName or .EntityName or _EntityName)
      if (escapedModule && escapedEntity) {
        likeClauses.push(`template_id ILIKE '%${escapedModule}.${escapedEntity}' ESCAPE '\\\\'`);
        likeClauses.push(`template_id ILIKE '%${escapedModule}:${escapedEntity}' ESCAPE '\\\\'`);
        likeClauses.push(`template_id ILIKE '%.${escapedModule}:${escapedEntity}' ESCAPE '\\\\'`);
        likeClauses.push(`template_id ILIKE '%:${escapedModule}:${escapedEntity}' ESCAPE '\\\\'`);
      }
      
      // Match by entity_name column exactly (case-insensitive)
      likeClauses.push(`LOWER(entity_name) = LOWER('${escapeString(entityName)}')`);
      
      // Match by module_name + entity_name if both available
      if (escapedModule) {
        likeClauses.push(`(LOWER(module_name) ILIKE '%${escapedModule}' ESCAPE '\\\\' AND LOWER(entity_name) = LOWER('${escapeString(entityName)}'))`);
      }

      whereClause = `(${likeClauses.join(' OR ')})`;
      console.log(`[ACS] Template parts: module=${moduleName}, entity=${entityName}`);
    } else if (entity) {
      // Validate entity format
      const e = String(entity).slice(0, 200);
      if (!/^[\w.:@-]+$/i.test(e)) {
        return res.status(400).json({ error: 'Invalid entity format' });
      }
      // Match by entity_name exactly (case-insensitive)
      whereClause = `LOWER(entity_name) = LOWER('${escapeString(e)}')`;
    }

    console.log(`[ACS] WHERE clause: ${whereClause}`);

    // Use optimized snapshot source (reads only files from selected snapshot)
    const { snapshot, source: acsSource, type } = getBestSnapshotAndSource();
    
    if (!snapshot) {
      console.log('[ACS] No snapshot found, returning empty result');
      return res.json({ data: [], count: 0, message: 'No snapshot found' });
    }
    
    console.log(`[ACS] Using snapshot: migration=${snapshot.migrationId}, time=${snapshot.snapshotTime}, type=${type}`);
    
    // First get total count (deduplicated by contract_id)
    const countSql = `
      SELECT COUNT(DISTINCT contract_id) as total_count
      FROM ${acsSource}
      WHERE ${whereClause}
    `;
    
    const countResult = await db.safeQuery(countSql);
    const totalCount = Number(countResult[0]?.total_count || 0);
    
    console.log(`[ACS] Count query returned: ${totalCount} for template=${template}`);
    
    // If no results, try to debug by checking what templates exist
    if (totalCount === 0 && template) {
      try {
        const debugSql = `
          SELECT DISTINCT entity_name, COUNT(*) as cnt
          FROM ${acsSource}
          GROUP BY entity_name
          ORDER BY cnt DESC
          LIMIT 20
        `;
        const debugRows = await db.safeQuery(debugSql);
        console.log(`[ACS] Available entity_names in snapshot:`, debugRows.map(r => `${r.entity_name}(${r.cnt})`).join(', '));
      } catch (debugErr) {
        console.warn('[ACS] Debug query failed:', debugErr.message);
      }
    }
    
    // Use GROUP BY contract_id to deduplicate (handles any duplicate records)
    const sql = `
      SELECT 
        contract_id,
        any_value(template_id) as template_id,
        any_value(entity_name) as entity_name,
        any_value(module_name) as module_name,
        any_value(signatories) as signatories,
        any_value(observers) as observers,
        any_value(payload) as payload,
        any_value(record_time) as record_time
      FROM ${acsSource}
      WHERE ${whereClause}
      GROUP BY contract_id
      ORDER BY contract_id
      LIMIT ${limit}
      OFFSET ${offset}
    `;

    const rows = await db.safeQuery(sql);
    console.log(`[ACS] Found ${rows.length} contracts (total: ${totalCount}) for template=${template}`);
    
    // Parse payload JSON and flatten for frontend consumption
    const parsedRows = rows.map(row => {
      let parsedPayload = row.payload;
      if (typeof row.payload === 'string') {
        try {
          parsedPayload = JSON.parse(row.payload);
        } catch {
          // Keep as string if parsing fails
        }
      }
      
      // Return the parsed payload fields at the top level for frontend compatibility
      return {
        ...row,
        ...parsedPayload, // Spread payload fields (owner, amount, amulet, etc.)
        payload: parsedPayload, // Keep original payload too
      };
    });
    
    res.json(serializeBigInt({ data: parsedRows, count: totalCount }));
  } catch (err) {
    console.error('ACS contracts error:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/acs/rich-list - Get aggregated holder balances (server-side calculation)
router.get('/rich-list', async (req, res) => {
  try {
    if (!hasACSData()) {
      return res.json({ data: [], totalSupply: 0, holderCount: 0 });
    }

    const limit = Math.min(parseInt(req.query.limit) || 100, 1000);
    const search = req.query.search || '';

    // Use optimized snapshot source
    const { snapshot, source: acsSource } = getBestSnapshotAndSource();
    
    if (!snapshot) {
      return res.json({ data: [], totalSupply: 0, holderCount: 0, message: 'No snapshot found' });
    }

    // Check cache - use search-specific key
    const cacheKey = `acs:v3:rich-list:m${snapshot.migrationId}:t${snapshot.snapshotTime}:${limit}:${search}`;
    const cached = getCached(cacheKey);
    if (cached) {
      console.log(`[ACS] Rich list cache HIT: ${cacheKey}`);
      return res.json(cached);
    }

    console.log(`[ACS] Rich list cache MISS: ${cacheKey}`);

    // Try to use pre-computed aggregation first
    const aggregation = getCached('aggregation:v2:holder-balances');
    if (aggregation && !search) {
      // Use pre-computed data
      const holders = aggregation.holders.slice(0, limit).map(row => ({
        owner: row.owner,
        amount: row.unlocked_balance,
        locked: row.locked_balance,
        total: row.total_balance,
      }));

      const result = serializeBigInt({
        data: holders,
        totalSupply: aggregation.totalSupply,
        unlockedSupply: aggregation.unlockedSupply,
        lockedSupply: aggregation.lockedSupply,
        holderCount: aggregation.holderCount,
        cached: true,
        refreshedAt: aggregation.refreshedAt,
      });

      setCache(cacheKey, result, CACHE_TTL.RICH_LIST);
      return res.json(result);
    }

    // Fall back to query (for search or if no pre-computed data)
    // NOTE: acsSource already reads only the best snapshot's files, no need to filter by migration_id/snapshot_time
    const sql = `
      WITH latest_contracts AS (
        SELECT
          contract_id,
          any_value(template_id) as template_id,
          any_value(entity_name) as entity_name,
          any_value(payload) as payload
        FROM ${acsSource}
        GROUP BY contract_id
      ),
      amulet_balances AS (
        SELECT 
          json_extract_string(payload, '$.owner') as owner,
          CAST(COALESCE(
            json_extract_string(payload, '$.amount.initialAmount'),
            '0'
          ) AS DOUBLE) as amount
        FROM latest_contracts
        WHERE (
          entity_name = 'Amulet'
          OR (
            (template_id LIKE '%:Amulet:%' OR template_id LIKE '%:Amulet')
            AND template_id NOT LIKE '%:LockedAmulet:%'
            AND template_id NOT LIKE '%:LockedAmulet'
          )
        )
          AND json_extract_string(payload, '$.owner') IS NOT NULL
      ),
      locked_balances AS (
        SELECT 
          COALESCE(
            json_extract_string(payload, '$.amulet.owner'),
            json_extract_string(payload, '$.owner')
          ) as owner,
          CAST(COALESCE(
            json_extract_string(payload, '$.amulet.amount.initialAmount'),
            json_extract_string(payload, '$.amount.initialAmount'),
            '0'
          ) AS DOUBLE) as amount
        FROM latest_contracts
        WHERE (entity_name = 'LockedAmulet' OR template_id LIKE '%:LockedAmulet:%' OR template_id LIKE '%:LockedAmulet')
          AND (json_extract_string(payload, '$.amulet.owner') IS NOT NULL 
               OR json_extract_string(payload, '$.owner') IS NOT NULL)
      ),
      combined AS (
        SELECT owner, amount, 0.0 as locked FROM amulet_balances
        UNION ALL
        SELECT owner, 0.0 as amount, amount as locked FROM locked_balances
      ),
      aggregated AS (
        SELECT 
          owner,
          SUM(amount) as unlocked_balance,
          SUM(locked) as locked_balance,
          SUM(amount) + SUM(locked) as total_balance
        FROM combined
        WHERE owner IS NOT NULL AND owner != ''
        GROUP BY owner
      )
      SELECT * FROM aggregated
      ${search ? `WHERE owner ILIKE '%${search.replace(/'/g, "''")}%'` : ''}
      ORDER BY total_balance DESC
      LIMIT ${limit}
    `;

    const rows = await db.safeQuery(sql);
    console.log(`[ACS] Rich list returned ${rows.length} holders`);

    // Get total supply and holder count
    const statsSql = `
      WITH latest_contracts AS (
        SELECT
          contract_id,
          any_value(template_id) as template_id,
          any_value(entity_name) as entity_name,
          any_value(payload) as payload
        FROM ${acsSource}
        GROUP BY contract_id
      ),
      amulet_total AS (
        SELECT COALESCE(SUM(
          CAST(COALESCE(
            json_extract_string(payload, '$.amount.initialAmount'),
            '0'
          ) AS DOUBLE)
        ), 0) as total
        FROM latest_contracts
        WHERE (
          entity_name = 'Amulet'
          OR (
            template_id LIKE '%:Amulet:%'
            AND template_id NOT LIKE '%:LockedAmulet:%'
          )
        )
      ),
      locked_total AS (
        SELECT COALESCE(SUM(
          CAST(COALESCE(
            json_extract_string(payload, '$.amulet.amount.initialAmount'),
            json_extract_string(payload, '$.amount.initialAmount'),
            '0'
          ) AS DOUBLE)
        ), 0) as total
        FROM latest_contracts
        WHERE (entity_name = 'LockedAmulet' OR template_id LIKE '%:LockedAmulet:%')
      ),
      holder_count AS (
        SELECT COUNT(DISTINCT COALESCE(
          json_extract_string(payload, '$.amulet.owner'),
          json_extract_string(payload, '$.owner')
        )) as count
        FROM latest_contracts
        WHERE (entity_name IN ('Amulet', 'LockedAmulet') 
             OR (template_id LIKE '%:Amulet:%' AND template_id NOT LIKE '%:LockedAmulet:%')
             OR template_id LIKE '%:LockedAmulet:%')
      )
      SELECT 
        amulet_total.total + locked_total.total as total_supply,
        amulet_total.total as unlocked_supply,
        locked_total.total as locked_supply,
        holder_count.count as holder_count
      FROM amulet_total, locked_total, holder_count
    `;

    const stats = await db.safeQuery(statsSql);
    const totalSupply = stats[0]?.total_supply || 0;
    const unlockedSupply = stats[0]?.unlocked_supply || 0;
    const lockedSupply = stats[0]?.locked_supply || 0;
    const holderCount = stats[0]?.holder_count || 0;

    const result = serializeBigInt({
      data: rows.map(row => ({
        owner: row.owner,
        amount: row.unlocked_balance,
        locked: row.locked_balance,
        total: row.total_balance,
      })),
      totalSupply,
      unlockedSupply,
      lockedSupply,
      holderCount,
    });
    
    setCache(cacheKey, result, CACHE_TTL.RICH_LIST);
    res.json(result);
  } catch (err) {
    console.error('ACS rich-list error:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/acs/supply - Get supply statistics (Amulet contracts)
router.get('/supply', async (req, res) => {
  try {
    if (!hasACSData()) {
      return res.json({ data: null });
    }

    // Use optimized snapshot source
    const { snapshot, source: acsSource } = getBestSnapshotAndSource();
    
    if (!snapshot) {
      return res.json({ data: null, message: 'No snapshot found' });
    }

    // Check cache
    const cacheKey = `acs:v3:supply:m${snapshot.migrationId}:t${snapshot.snapshotTime}`;
    const cached = getCached(cacheKey);
    if (cached) {
      return res.json(cached);
    }

    const sql = `
      SELECT 
        COUNT(*) as amulet_count
      FROM ${acsSource}
      WHERE (entity_name = 'Amulet' OR template_id LIKE '%Amulet%')
    `;

    const rows = await db.safeQuery(sql);
    const result = serializeBigInt({ 
      data: {
        ...rows[0],
        snapshot_time: snapshot.snapshotTime,
        migration_id: snapshot.migrationId,
      }
    });
    setCache(cacheKey, result, CACHE_TTL.SUPPLY);
    res.json(result);
  } catch (err) {
    console.error('ACS supply error:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/acs/allocations - Get amulet allocations with server-side aggregation
router.get('/allocations', async (req, res) => {
  try {
    if (!hasACSData()) {
      return res.json({ data: [], totalCount: 0, totalAmount: 0 });
    }

    const limit = Math.min(parseInt(req.query.limit) || 100, 1000);
    const offset = parseInt(req.query.offset) || 0;
    const search = (req.query.search || '').trim();

    // Use optimized snapshot source
    const { snapshot, source: acsSource } = getBestSnapshotAndSource();
    
    if (!snapshot) {
      return res.json({ data: [], totalCount: 0, totalAmount: 0, message: 'No snapshot found' });
    }

    // Check cache for paginated data
    const cacheKey = `acs:v3:allocations:m${snapshot.migrationId}:t${snapshot.snapshotTime}:${limit}:${offset}:${search}`;
    const cached = getCached(cacheKey);
    if (cached) {
      return res.json(cached);
    }

    console.log(`[ACS] Allocations request: limit=${limit}, offset=${offset}, search=${search}`);

    const sql = `
      SELECT 
        contract_id,
        json_extract_string(payload, '$.allocation.settlement.executor') as executor,
        json_extract_string(payload, '$.allocation.transferLeg.sender') as sender,
        json_extract_string(payload, '$.allocation.transferLeg.receiver') as receiver,
        CAST(COALESCE(json_extract_string(payload, '$.allocation.transferLeg.amount'), '0') AS DOUBLE) as amount,
        json_extract_string(payload, '$.allocation.settlement.requestedAt') as requested_at,
        json_extract_string(payload, '$.allocation.transferLegId') as transfer_leg_id,
        payload
      FROM ${acsSource}
      WHERE (entity_name = 'AmuletAllocation' OR template_id LIKE '%:AmuletAllocation:%' OR template_id LIKE '%:AmuletAllocation')
        ${search ? `AND (
          json_extract_string(payload, '$.allocation.settlement.executor') ILIKE '%${search.replace(/'/g, "''")}%'
          OR json_extract_string(payload, '$.allocation.transferLeg.sender') ILIKE '%${search.replace(/'/g, "''")}%'
          OR json_extract_string(payload, '$.allocation.transferLeg.receiver') ILIKE '%${search.replace(/'/g, "''")}%'
        )` : ''}
      ORDER BY amount DESC
      LIMIT ${limit} OFFSET ${offset}
    `;

    const rows = await db.safeQuery(sql);

    // Get totals (cached separately from paginated data to avoid refetching on page change)
    const statsCacheKey = `acs:v3:allocations-stats:m${snapshot.migrationId}:t${snapshot.snapshotTime}:${search}`;
    let stats = getCached(statsCacheKey);
    
    if (!stats) {
      const statsSql = `
        SELECT 
          COUNT(*) as total_count,
          COALESCE(SUM(CAST(COALESCE(json_extract_string(payload, '$.allocation.transferLeg.amount'), '0') AS DOUBLE)), 0) as total_amount,
          COUNT(DISTINCT json_extract_string(payload, '$.allocation.settlement.executor')) as unique_executors
        FROM ${acsSource}
        WHERE (entity_name = 'AmuletAllocation' OR template_id LIKE '%:AmuletAllocation:%' OR template_id LIKE '%:AmuletAllocation')
          ${search ? `AND (
            json_extract_string(payload, '$.allocation.settlement.executor') ILIKE '%${search.replace(/'/g, "''")}%'
            OR json_extract_string(payload, '$.allocation.transferLeg.sender') ILIKE '%${search.replace(/'/g, "''")}%'
            OR json_extract_string(payload, '$.allocation.transferLeg.receiver') ILIKE '%${search.replace(/'/g, "''")}%'
          )` : ''}
      `;

      const statsRows = await db.safeQuery(statsSql);
      stats = statsRows[0] || { total_count: 0, total_amount: 0, unique_executors: 0 };
      setCache(statsCacheKey, stats, CACHE_TTL.ALLOCATIONS);
    }

    const result = serializeBigInt({
      data: rows,
      totalCount: stats.total_count || 0,
      totalAmount: stats.total_amount || 0,
      uniqueExecutors: stats.unique_executors || 0,
    });
    
    setCache(cacheKey, result, CACHE_TTL.ALLOCATIONS);
    res.json(result);
  } catch (err) {
    console.error('ACS allocations error:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/acs/mining-rounds - Get mining rounds with server-side aggregation
// Supports optional `snapshot` query param to query a specific snapshot time
router.get('/mining-rounds', async (req, res) => {
  try {
    if (!hasACSData()) {
      return res.json({ openRounds: [], issuingRounds: [], closedRounds: [], counts: {} });
    }

    const closedLimit = Math.min(parseInt(req.query.closedLimit) || 20, 100);

    // Use optimized snapshot source
    const { snapshot, source: acsSource } = getBestSnapshotAndSource();
    
    if (!snapshot) {
      return res.json({ openRounds: [], issuingRounds: [], closedRounds: [], counts: {}, message: 'No snapshot found' });
    }

    // Check cache
    const cacheKey = `acs:v3:mining-rounds:m${snapshot.migrationId}:t${snapshot.snapshotTime}:${closedLimit}`;
    const cached = getCached(cacheKey);
    if (cached) {
      console.log(`[ACS] Mining rounds cache HIT: ${cacheKey}`);
      return res.json(cached);
    }

    console.log(`[ACS] Mining rounds cache MISS: ${cacheKey}`);

    // Skip pre-computed aggregation for now (use direct query)
    const aggregation = getCached('aggregation:mining-rounds');
    if (aggregation) {
      const result = serializeBigInt({
        openRounds: aggregation.openRounds,
        issuingRounds: aggregation.issuingRounds,
        closedRounds: aggregation.closedRounds.slice(0, closedLimit),
        counts: aggregation.counts,
        cached: true,
        refreshedAt: aggregation.refreshedAt,
      });
      
      setCache(cacheKey, result, CACHE_TTL.MINING_ROUNDS);
      return res.json(result);
    }

    const sql = `
      SELECT 
        contract_id,
        entity_name,
        template_id,
        -- Try multiple extraction paths for round number
        COALESCE(
          NULLIF(json_extract_string(payload, '$.round.number'), ''),
          NULLIF(CAST(json_extract(payload, '$.round.number') AS VARCHAR), ''),
          NULLIF(json_extract_string(payload, '$.round'), ''),
          NULLIF(CAST(json_extract(payload, '$.round') AS VARCHAR), '')
        ) as round_number,
        json_extract_string(payload, '$.opensAt') as opens_at,
        json_extract_string(payload, '$.targetClosesAt') as target_closes_at,
        json_extract_string(payload, '$.amuletPrice') as amulet_price,
        payload
      FROM ${acsSource}
      WHERE (entity_name IN ('OpenMiningRound', 'IssuingMiningRound', 'ClosedMiningRound')
             OR template_id LIKE '%MiningRound%')
      ORDER BY entity_name, CAST(COALESCE(NULLIF(
        COALESCE(
          NULLIF(json_extract_string(payload, '$.round.number'), ''),
          NULLIF(json_extract_string(payload, '$.round'), ''),
          '0'
        ), ''), '0') AS BIGINT) DESC
    `;

    const rows = await db.safeQuery(sql);

    // Separate by type
    const openRounds = rows.filter(r => r.entity_name === 'OpenMiningRound' || r.template_id?.includes('OpenMiningRound'));
    const issuingRounds = rows.filter(r => r.entity_name === 'IssuingMiningRound' || r.template_id?.includes('IssuingMiningRound'));
    const allClosedRounds = rows.filter(r => r.entity_name === 'ClosedMiningRound' || r.template_id?.includes('ClosedMiningRound'));
    const closedRounds = allClosedRounds.slice(0, closedLimit);

    const result = serializeBigInt({
      openRounds,
      issuingRounds,
      closedRounds,
      counts: {
        open: openRounds.length,
        issuing: issuingRounds.length,
        closed: allClosedRounds.length,
      },
      snapshotTime: snapshot.snapshotTime,
    });
    
    setCache(cacheKey, result, CACHE_TTL.MINING_ROUNDS);
    res.json(result);
  } catch (err) {
    console.error('ACS mining-rounds error:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/acs/stats - Overview statistics (uses optimized snapshot source)
router.get('/stats', async (req, res) => {
  try {
    if (!hasACSData()) {
      return res.json({ 
        data: {
          total_contracts: 0,
          total_templates: 0,
          total_snapshots: 0,
          latest_snapshot: null,
        }
      });
    }

    // Check cache
    const cacheKey = 'acs:v2:stats';
    const cached = getCached(cacheKey);
    if (cached) {
      return res.json(cached);
    }

    // Use optimized snapshot source
    const { snapshot, source: acsSource } = getBestSnapshotAndSource();
    const availableSnapshots = findAvailableSnapshots();

    const sql = `
      SELECT 
        COUNT(DISTINCT contract_id) as total_contracts,
        COUNT(DISTINCT template_id) as total_templates
      FROM ${acsSource}
    `;

    const rows = await db.safeQuery(sql);
    const result = serializeBigInt({ 
      data: {
        ...rows[0],
        total_snapshots: availableSnapshots.length,
        latest_snapshot: snapshot?.snapshotTime || null,
        latest_record_time: snapshot?.snapshotTime || null,
        migration_id: snapshot?.migrationId || null,
      }
    });
    setCache(cacheKey, result, CACHE_TTL.STATS);
    res.json(result);
  } catch (err) {
    console.error('ACS stats error:', err);
    res.status(500).json({ error: err.message });
  }
});


// ─────────────────────────────────────────────────────────────────────────────
// REAL-TIME SUPPLY: Snapshot + v2/updates delta calculation
// ─────────────────────────────────────────────────────────────────────────────

// Helper to get events source for delta calculation
function getEventsSource() {
  const hasParquet = db.hasFileType('events', '.parquet');
  if (hasParquet) {
    return `read_parquet('${db.DATA_PATH.replace(/\\/g, '/')}/**/events-*.parquet', union_by_name=true)`;
  }

  const hasJsonl = db.hasFileType('events', '.jsonl');
  const hasGzip = db.hasFileType('events', '.jsonl.gz');
  const hasZstd = db.hasFileType('events', '.jsonl.zst');

  if (!hasJsonl && !hasGzip && !hasZstd) {
    // Return empty table with expected schema
    return `(
      SELECT
        NULL::VARCHAR as event_id,
        NULL::VARCHAR as event_type,
        NULL::VARCHAR as template_id,
        NULL::JSON as payload,
        NULL::TIMESTAMP as effective_at,
        NULL::TIMESTAMP as timestamp
      WHERE false
    )`;
  }

  const basePath = db.DATA_PATH.replace(/\\/g, '/');
  const queries = [];
  if (hasJsonl) queries.push(`SELECT * FROM read_json_auto('${basePath}/**/events-*.jsonl', union_by_name=true, ignore_errors=true)`);
  if (hasGzip) queries.push(`SELECT * FROM read_json_auto('${basePath}/**/events-*.jsonl.gz', union_by_name=true, ignore_errors=true)`);
  if (hasZstd) queries.push(`SELECT * FROM read_json_auto('${basePath}/**/events-*.jsonl.zst', union_by_name=true, ignore_errors=true)`);

  return `(${queries.join(' UNION ')})`;
}

// GET /api/acs/realtime-supply - Get real-time supply (snapshot + delta from v2/updates)
router.get('/realtime-supply', async (req, res) => {
  try {
    if (!hasACSData()) {
      return res.json({ 
        data: null, 
        message: 'No ACS data available',
        source: 'none'
      });
    }

    // Use optimized snapshot source (reads only the best snapshot, not all files)
    const { snapshot, source: acsSource } = getBestSnapshotAndSource();
    
    if (!snapshot) {
      return res.json({ data: null, message: 'No snapshot found' });
    }

    // Check cache (shorter TTL for real-time)
    const cacheKey = `acs:v3:realtime-supply:m${snapshot.migrationId}:t${snapshot.snapshotTime}`;
    const cached = getCached(cacheKey);
    if (cached) {
      return res.json(cached);
    }

    const eventsSource = getEventsSource();

    // Step 1: Get snapshot totals and record_time
    // NOTE: Since we use getBestSnapshotAndSource(), the acsSource already reads only the
    // best snapshot's files. No need to filter by migration_id/snapshot_time columns.
    const snapshotSql = `
      WITH latest_contracts AS (
        SELECT
          contract_id,
          any_value(template_id) as template_id,
          any_value(entity_name) as entity_name,
          any_value(payload) as payload
        FROM ${acsSource}
        GROUP BY contract_id
      ),
      snapshot_info AS (
        SELECT MIN(record_time) as record_time
        FROM ${acsSource}
      ),
      amulet_total AS (
        SELECT COALESCE(SUM(
          CAST(COALESCE(json_extract_string(payload, '$.amount.initialAmount'), '0') AS DOUBLE)
        ), 0) as total
        FROM latest_contracts
        WHERE (
          entity_name = 'Amulet'
          OR (template_id LIKE '%:Amulet:%' AND template_id NOT LIKE '%:LockedAmulet:%')
        )
      ),
      locked_total AS (
        SELECT COALESCE(SUM(
          CAST(COALESCE(
            json_extract_string(payload, '$.amulet.amount.initialAmount'),
            json_extract_string(payload, '$.amount.initialAmount'),
            '0'
          ) AS DOUBLE)
        ), 0) as total
        FROM latest_contracts
        WHERE (entity_name = 'LockedAmulet' OR template_id LIKE '%:LockedAmulet:%')
      )
      SELECT 
        '${snapshot.snapshotTime}'::TIMESTAMP as snapshot_time,
        ${snapshot.migrationId} as migration_id,
        snapshot_info.record_time,
        amulet_total.total as snapshot_unlocked,
        locked_total.total as snapshot_locked,
        amulet_total.total + locked_total.total as snapshot_total
      FROM snapshot_info, amulet_total, locked_total
    `;

    const snapshotResult = await db.safeQuery(snapshotSql);
    
    if (snapshotResult.length === 0) {
      return res.json({ data: null, message: 'No snapshot data found' });
    }

    const snapshotRow = snapshotResult[0];
    const recordTime = snapshotRow.record_time;

    // Step 2: Calculate delta from events since record_time
    // Created events ADD to supply, Archived events SUBTRACT
    let deltaUnlocked = 0;
    let deltaLocked = 0;
    let createdCount = 0;
    let archivedCount = 0;

    if (recordTime) {
      const deltaSql = `
        WITH amulet_events AS (
          SELECT 
            event_type,
            template_id,
            CAST(COALESCE(
              json_extract_string(payload, '$.amount.initialAmount'),
              '0'
            ) AS DOUBLE) as amount
          FROM ${eventsSource}
          WHERE COALESCE(effective_at, timestamp) > '${recordTime}'::TIMESTAMP
            AND (
              template_id LIKE '%:Amulet:%' 
              AND template_id NOT LIKE '%:LockedAmulet:%'
            )
            AND event_type IN ('created', 'archived')
        ),
        locked_events AS (
          SELECT 
            event_type,
            template_id,
            CAST(COALESCE(
              json_extract_string(payload, '$.amulet.amount.initialAmount'),
              json_extract_string(payload, '$.amount.initialAmount'),
              '0'
            ) AS DOUBLE) as amount
          FROM ${eventsSource}
          WHERE COALESCE(effective_at, timestamp) > '${recordTime}'::TIMESTAMP
            AND template_id LIKE '%:LockedAmulet:%'
            AND event_type IN ('created', 'archived')
        ),
        amulet_delta AS (
          SELECT 
            SUM(CASE WHEN event_type = 'created' THEN amount ELSE 0 END) as created_sum,
            SUM(CASE WHEN event_type = 'archived' THEN amount ELSE 0 END) as archived_sum,
            COUNT(CASE WHEN event_type = 'created' THEN 1 END) as created_count,
            COUNT(CASE WHEN event_type = 'archived' THEN 1 END) as archived_count
          FROM amulet_events
        ),
        locked_delta AS (
          SELECT 
            SUM(CASE WHEN event_type = 'created' THEN amount ELSE 0 END) as created_sum,
            SUM(CASE WHEN event_type = 'archived' THEN amount ELSE 0 END) as archived_sum,
            COUNT(CASE WHEN event_type = 'created' THEN 1 END) as created_count,
            COUNT(CASE WHEN event_type = 'archived' THEN 1 END) as archived_count
          FROM locked_events
        )
        SELECT 
          COALESCE(amulet_delta.created_sum, 0) - COALESCE(amulet_delta.archived_sum, 0) as delta_unlocked,
          COALESCE(locked_delta.created_sum, 0) - COALESCE(locked_delta.archived_sum, 0) as delta_locked,
          COALESCE(amulet_delta.created_count, 0) + COALESCE(locked_delta.created_count, 0) as created_count,
          COALESCE(amulet_delta.archived_count, 0) + COALESCE(locked_delta.archived_count, 0) as archived_count
        FROM amulet_delta, locked_delta
      `;

      try {
        const deltaResult = await db.safeQuery(deltaSql);
        if (deltaResult.length > 0) {
          deltaUnlocked = deltaResult[0].delta_unlocked || 0;
          deltaLocked = deltaResult[0].delta_locked || 0;
          createdCount = deltaResult[0].created_count || 0;
          archivedCount = deltaResult[0].archived_count || 0;
        }
      } catch (deltaErr) {
        console.warn('Could not calculate delta from events:', deltaErr.message);
        // Continue with snapshot-only values
      }
    }

    // Step 3: Combine snapshot + delta for real-time values
    const realtimeUnlocked = snapshotRow.snapshot_unlocked + deltaUnlocked;
    const realtimeLocked = snapshotRow.snapshot_locked + deltaLocked;
    const realtimeTotal = realtimeUnlocked + realtimeLocked;

    const result = serializeBigInt({
      data: {
        // Snapshot values (point-in-time)
        snapshot: {
          timestamp: snapshot.snapshotTime,
          migration_id: snapshot.migrationId,
          record_time: recordTime,
          unlocked: snapshotRow.snapshot_unlocked,
          locked: snapshotRow.snapshot_locked,
          total: snapshotRow.snapshot_total,
        },
        // Delta from v2/updates since snapshot
        delta: {
          since: recordTime,
          unlocked: deltaUnlocked,
          locked: deltaLocked,
          total: deltaUnlocked + deltaLocked,
          events: {
            created: createdCount,
            archived: archivedCount,
          }
        },
        // Combined real-time values
        realtime: {
          unlocked: realtimeUnlocked,
          locked: realtimeLocked,
          total: realtimeTotal,
          circulating: realtimeUnlocked, // Unlocked = circulating
        },
        // Metadata
        calculated_at: new Date().toISOString(),
      }
    });

    // Short cache for real-time data (30 seconds)
    setCache(cacheKey, result, 30 * 1000);
    res.json(result);
  } catch (err) {
    console.error('Realtime supply error:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/acs/realtime-rich-list - Get real-time rich list (snapshot + delta)
router.get('/realtime-rich-list', async (req, res) => {
  try {
    if (!hasACSData()) {
      return res.json({ data: [], message: 'No ACS data available' });
    }

    const limit = Math.min(parseInt(req.query.limit) || 100, 1000);
    const search = req.query.search || '';

    // Use optimized snapshot source (reads only the best snapshot, not all files)
    const { snapshot, source: acsSource } = getBestSnapshotAndSource();
    
    if (!snapshot) {
      return res.json({ data: [], message: 'No snapshot found' });
    }

    // Check cache
    const cacheKey = `acs:v3:realtime-rich-list:m${snapshot.migrationId}:t${snapshot.snapshotTime}:${limit}:${search}`;
    const cached = getCached(cacheKey);
    if (cached) {
      return res.json(cached);
    }

    const eventsSource = getEventsSource();

    // Step 1: Get snapshot record_time
    const recordTimeSql = `SELECT MIN(record_time) as record_time FROM ${acsSource}`;
    const recordTimeResult = await db.safeQuery(recordTimeSql);
    const recordTime = recordTimeResult[0]?.record_time;

    // Step 2: Get snapshot balances per owner
    // NOTE: Since acsSource already reads only the best snapshot's files, no need to filter by migration_id/snapshot_time
    const snapshotSql = `
      WITH latest_contracts AS (
        SELECT
          contract_id,
          any_value(template_id) as template_id,
          any_value(entity_name) as entity_name,
          any_value(payload) as payload
        FROM ${acsSource}
        GROUP BY contract_id
      ),
      amulet_balances AS (
        SELECT 
          json_extract_string(payload, '$.owner') as owner,
          CAST(COALESCE(json_extract_string(payload, '$.amount.initialAmount'), '0') AS DOUBLE) as amount
        FROM latest_contracts
        WHERE (
          entity_name = 'Amulet'
          OR (template_id LIKE '%:Amulet:%' AND template_id NOT LIKE '%:LockedAmulet:%')
        )
          AND json_extract_string(payload, '$.owner') IS NOT NULL
      ),
      locked_balances AS (
        SELECT 
          COALESCE(json_extract_string(payload, '$.amulet.owner'), json_extract_string(payload, '$.owner')) as owner,
          CAST(COALESCE(
            json_extract_string(payload, '$.amulet.amount.initialAmount'),
            json_extract_string(payload, '$.amount.initialAmount'),
            '0'
          ) AS DOUBLE) as amount
        FROM latest_contracts
        WHERE (entity_name = 'LockedAmulet' OR template_id LIKE '%:LockedAmulet:%')
          AND (json_extract_string(payload, '$.amulet.owner') IS NOT NULL OR json_extract_string(payload, '$.owner') IS NOT NULL)
      ),
      combined AS (
        SELECT owner, amount, 0.0 as locked FROM amulet_balances
        UNION ALL
        SELECT owner, 0.0 as amount, amount as locked FROM locked_balances
      )
      SELECT 
        owner,
        SUM(amount) as unlocked_balance,
        SUM(locked) as locked_balance
      FROM combined
      WHERE owner IS NOT NULL AND owner != ''
      GROUP BY owner
    `;

    const snapshotBalances = await db.safeQuery(snapshotSql);
    
    // Build a map of owner -> balances
    const balanceMap = new Map();
    for (const row of snapshotBalances) {
      balanceMap.set(row.owner, {
        unlocked: row.unlocked_balance || 0,
        locked: row.locked_balance || 0,
      });
    }

    // Step 3: Apply delta from events since record_time
    if (recordTime) {
      const deltaSql = `
        WITH amulet_events AS (
          SELECT 
            event_type,
            json_extract_string(payload, '$.owner') as owner,
            CAST(COALESCE(json_extract_string(payload, '$.amount.initialAmount'), '0') AS DOUBLE) as amount
          FROM ${eventsSource}
          WHERE COALESCE(effective_at, timestamp) > '${recordTime}'::TIMESTAMP
            AND (template_id LIKE '%:Amulet:%' AND template_id NOT LIKE '%:LockedAmulet:%')
            AND event_type IN ('created', 'archived')
            AND json_extract_string(payload, '$.owner') IS NOT NULL
        ),
        locked_events AS (
          SELECT 
            event_type,
            COALESCE(json_extract_string(payload, '$.amulet.owner'), json_extract_string(payload, '$.owner')) as owner,
            CAST(COALESCE(
              json_extract_string(payload, '$.amulet.amount.initialAmount'),
              json_extract_string(payload, '$.amount.initialAmount'),
              '0'
            ) AS DOUBLE) as amount
          FROM ${eventsSource}
          WHERE COALESCE(effective_at, timestamp) > '${recordTime}'::TIMESTAMP
            AND template_id LIKE '%:LockedAmulet:%'
            AND event_type IN ('created', 'archived')
        )
        SELECT 
          owner,
          SUM(CASE WHEN event_type = 'created' THEN amount ELSE -amount END) as delta,
          'unlocked' as type
        FROM amulet_events
        WHERE owner IS NOT NULL
        GROUP BY owner
        UNION ALL
        SELECT 
          owner,
          SUM(CASE WHEN event_type = 'created' THEN amount ELSE -amount END) as delta,
          'locked' as type
        FROM locked_events
        WHERE owner IS NOT NULL
        GROUP BY owner
      `;

      try {
        const deltaRows = await db.safeQuery(deltaSql);
        for (const row of deltaRows) {
          const existing = balanceMap.get(row.owner) || { unlocked: 0, locked: 0 };
          if (row.type === 'unlocked') {
            existing.unlocked += row.delta;
          } else {
            existing.locked += row.delta;
          }
          balanceMap.set(row.owner, existing);
        }
      } catch (deltaErr) {
        console.warn('Could not apply event deltas to rich list:', deltaErr.message);
      }
    }

    // Step 4: Convert to array and sort
    let holders = Array.from(balanceMap.entries())
      .map(([owner, bal]) => ({
        owner,
        amount: Math.max(0, bal.unlocked), // Clamp to 0 (archived more than created = 0)
        locked: Math.max(0, bal.locked),
        total: Math.max(0, bal.unlocked) + Math.max(0, bal.locked),
      }))
      .filter(h => h.total > 0);

    // Apply search filter
    if (search) {
      holders = holders.filter(h => h.owner.toLowerCase().includes(search.toLowerCase()));
    }

    // Sort by total descending
    holders.sort((a, b) => b.total - a.total);

    // Limit
    holders = holders.slice(0, limit);

    // Calculate totals
    const totalSupply = holders.reduce((sum, h) => sum + h.total, 0);
    const unlockedSupply = holders.reduce((sum, h) => sum + h.amount, 0);
    const lockedSupply = holders.reduce((sum, h) => sum + h.locked, 0);

    const result = serializeBigInt({
      data: holders,
      totalSupply,
      unlockedSupply,
      lockedSupply,
      holderCount: balanceMap.size,
      snapshotRecordTime: recordTime,
      isRealtime: !!recordTime,
    });

    // Short cache (30 seconds)
    setCache(cacheKey, result, 30 * 1000);
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/acs/realtime-debug - Debug info for snapshot+delta wiring
router.get('/realtime-debug', async (req, res) => {
  try {
    const acsSource = getACSSource();
    const eventsSource = getEventsSource();

    // Snapshot side
    let snapshot = null;
    try {
      const rows = await db.safeQuery(`
        WITH ${getLatestSnapshotCTE(acsSource)}
        SELECT
          (SELECT migration_id FROM latest_migration) as migration_id,
          (SELECT snapshot_time FROM latest_snapshot) as snapshot_time,
          MIN(record_time) as record_time,
          COUNT(DISTINCT contract_id) as distinct_contracts
        FROM ${acsSource}
        WHERE migration_id = (SELECT migration_id FROM latest_migration)
          AND snapshot_time = (SELECT snapshot_time FROM latest_snapshot)
      `);
      snapshot = rows?.[0] || null;
    } catch (e) {
      snapshot = { error: e.message };
    }

    // Events side (schema sample)
    let eventSample = null;
    let eventColumns = [];
    try {
      const sampleRows = await db.safeQuery(`SELECT * FROM ${eventsSource} LIMIT 1`);
      eventSample = sampleRows?.[0] || null;
      eventColumns = eventSample ? Object.keys(eventSample) : [];
    } catch (e) {
      eventSample = { error: e.message };
      eventColumns = [];
    }

    const result = serializeBigInt({
      data: {
        snapshot,
        events: {
          columns: eventColumns,
          has_effective_at: eventColumns.includes('effective_at'),
          has_timestamp: eventColumns.includes('timestamp'),
          has_event_type: eventColumns.includes('event_type'),
          sample: eventSample,
        },
      },
    });

    res.json(result);
  } catch (err) {
    console.error('ACS realtime-debug error:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/acs/aggregate - Aggregate template data (sum amounts)
router.get('/aggregate', async (req, res) => {
  try {
    const { template, mode = 'circulating' } = req.query;
    
    if (!template) {
      return res.status(400).json({ error: 'template parameter required' });
    }

    const cacheKey = `acs:aggregate:${template}:${mode}`;
    const cached = getCached(cacheKey);
    if (cached) return res.json(cached);

    if (!hasACSData()) {
      return res.json({ sum: 0, count: 0, templateCount: 0 });
    }

    const acsSource = getSnapshotCTE();
    
    // Query for the template, summing the amount field
    // mode=circulating sums all amounts, mode=locked sums only locked amounts
    const amountField = mode === 'locked' 
      ? "COALESCE(CAST(json_extract_string(payload, '$.lockedAmount') AS DOUBLE), 0)"
      : "COALESCE(CAST(json_extract_string(payload, '$.amount') AS DOUBLE), 0)";

    const sql = `
      WITH filtered AS (
        SELECT 
          ${amountField} as amount
        FROM ${acsSource}
        WHERE template_id LIKE '%${template.replace(/'/g, "''")}'
      )
      SELECT 
        SUM(amount) as sum,
        COUNT(*) as count
      FROM filtered
    `;

    const rows = await db.safeQuery(sql);
    const result = {
      sum: rows[0]?.sum || 0,
      count: rows[0]?.count || 0,
      templateCount: 1,
    };

    setCache(cacheKey, result, CACHE_TTL.SUPPLY);
    res.json(result);
  } catch (err) {
    console.error('ACS aggregate error:', err);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/acs/trigger-snapshot - Trigger a new ACS snapshot
router.post('/trigger-snapshot', async (req, res) => {
  try {
    // This endpoint triggers the local snapshot script
    // For now, just return a message - the actual trigger would be implemented
    // based on how your snapshot process works (e.g., spawn a child process)
    res.json({ 
      success: true, 
      message: 'Snapshot trigger endpoint - run `node scripts/ingest/fetch-acs-parquet.js` manually' 
    });
  } catch (err) {
    console.error('ACS trigger-snapshot error:', err);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/acs/purge - Purge all local ACS data
router.post('/purge', async (req, res) => {
  try {
    console.log('[ACS] Purge requested');
    
    if (!fs.existsSync(ACS_DATA_PATH)) {
      return res.json({ 
        success: true, 
        message: 'No ACS data directory found - nothing to purge',
        deletedFiles: 0,
        deletedDirs: 0
      });
    }

    let deletedFiles = 0;
    let deletedDirs = 0;

    // Recursively delete all files and directories in the ACS data path
    const deleteRecursive = (dirPath) => {
      if (!fs.existsSync(dirPath)) return;
      
      const entries = fs.readdirSync(dirPath, { withFileTypes: true });
      
      for (const entry of entries) {
        const fullPath = path.join(dirPath, entry.name);
        
        if (entry.isDirectory()) {
          deleteRecursive(fullPath);
          try {
            fs.rmdirSync(fullPath);
            deletedDirs++;
          } catch (e) {
            console.warn(`[ACS] Could not remove dir ${fullPath}: ${e.message}`);
          }
        } else {
          try {
            fs.unlinkSync(fullPath);
            deletedFiles++;
          } catch (e) {
            console.warn(`[ACS] Could not remove file ${fullPath}: ${e.message}`);
          }
        }
      }
    };

    deleteRecursive(ACS_DATA_PATH);

    // Invalidate all ACS-related caches
    invalidateCache('acs');
    invalidateCache('supply');
    invalidateCache('rich-list');

    console.log(`[ACS] Purge complete: ${deletedFiles} files, ${deletedDirs} directories deleted`);

    res.json({ 
      success: true, 
      message: `Purged ${deletedFiles} files and ${deletedDirs} directories`,
      deletedFiles,
      deletedDirs
    });
  } catch (err) {
    console.error('[ACS] Purge error:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

export default router;
