import duckdb from 'duckdb';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// Prefer the repository-local data directory if it exists (common in Lovable + WSL setups)
// Repo layout: server/duckdb/connection.js -> ../../data
const REPO_DATA_DIR = path.join(__dirname, '../../data');
const repoRawDir = path.join(REPO_DATA_DIR, 'raw');

// DATA_DIR should point to the base directory
// Default Windows path: C:/ledger_raw
const WIN_DEFAULT_DATA_DIR = 'C:/ledger_raw';

// Final selection order:
// 1) process.env.DATA_DIR (explicit override)
// 2) repo-local data/ (if present)
// 3) WSL default path
const BASE_DATA_DIR = process.env.DATA_DIR || (fs.existsSync(repoRawDir) ? REPO_DATA_DIR : WIN_DEFAULT_DATA_DIR);
// Ledger events/updates live under: <BASE_DATA_DIR>/raw
const DATA_PATH = path.join(BASE_DATA_DIR, 'raw');
// ACS snapshots live under: <BASE_DATA_DIR>/raw/acs
const ACS_DATA_PATH = path.join(BASE_DATA_DIR, 'raw', 'acs');

// Persistent DuckDB instance
const DB_FILE = process.env.DUCKDB_FILE || path.join(BASE_DATA_DIR, 'canton-explorer.duckdb');
console.log(`🦆 DuckDB database: ${DB_FILE}`);
console.log(`📦 DuckDB base data dir: ${BASE_DATA_DIR}`);

let db = null;
let conn = null;
let recoveryAttempted = false;

function logDuckDBDiagnostics(prefix = 'ℹ️') {
  try {
    const exists = fs.existsSync(DB_FILE);
    const walExists = fs.existsSync(`${DB_FILE}.wal`);
    let size = null;
    try {
      size = exists ? fs.statSync(DB_FILE).size : null;
    } catch {}

    console.log(`${prefix} DuckDB diagnostics:`);
    console.log(`   platform=${process.platform}`);
    console.log(`   db_file_exists=${exists}${size != null ? ` size_bytes=${size}` : ''}`);
    console.log(`   wal_exists=${walExists}`);
  } catch (e) {
    console.warn(`⚠️ Failed to log DuckDB diagnostics: ${e?.message || e}`);
  }
}

function openDuckDBConnection() {
  if (conn) return;

  // If the DB file doesn't exist, DuckDB will create a new empty DB.
  // That's almost never what we want for this app, so fail loudly.
  if (!fs.existsSync(DB_FILE)) {
    throw new Error(`DuckDB file not found at ${DB_FILE} (check DATA_DIR/DUCKDB_FILE)`);
  }

  db = new duckdb.Database(DB_FILE);
  conn = db.connect();
}

function closeDuckDBConnection() {
  try { conn?.close?.(); } catch {}
  try { db?.close?.(); } catch {}
  conn = null;
  db = null;
}

function pingDuckDB() {
  return new Promise((resolve, reject) => {
    if (!conn) return reject(new Error('DuckDB connection not initialized'));
    conn.all('SELECT 1 AS ok', (err, rows) => {
      if (err) reject(err);
      else resolve(rows);
    });
  });
}

function cleanupDuckDBArtifacts() {
  // On Windows, stale .wal/.lock files are a common cause of "connection closed" issues.
  try {
    const walPath = `${DB_FILE}.wal`;
    if (fs.existsSync(walPath)) {
      fs.unlinkSync(walPath);
      console.log(`🧹 Deleted WAL: ${walPath}`);
    }
  } catch (e) {
    console.warn(`⚠️ Failed deleting WAL: ${e?.message || e}`);
  }

  try {
    if (!fs.existsSync(BASE_DATA_DIR)) return;
    const entries = fs.readdirSync(BASE_DATA_DIR);
    const lockFiles = entries.filter((f) => f.endsWith('.lock'));
    for (const f of lockFiles) {
      const lockPath = path.join(BASE_DATA_DIR, f);
      try {
        fs.unlinkSync(lockPath);
        console.log(`🧹 Deleted lock: ${lockPath}`);
      } catch (e) {
        console.warn(`⚠️ Failed deleting lock ${lockPath}: ${e?.message || e}`);
      }
    }
  } catch (e) {
    console.warn(`⚠️ Failed scanning lock files: ${e?.message || e}`);
  }
}

export async function ensureDuckDBReady({ allowRecovery = true } = {}) {
  try {
    openDuckDBConnection();
    await pingDuckDB();
  } catch (err) {
    const msg = err?.message || String(err);
    logDuckDBDiagnostics('❗');

    // Only try recovery once per process to avoid loops.
    if (allowRecovery && process.platform === 'win32' && !recoveryAttempted) {
      recoveryAttempted = true;
      console.warn(`⚠️ DuckDB connection ping failed: ${msg}`);
      console.warn('🧹 Attempting Windows DuckDB recovery (remove WAL/lock, reconnect once)...');

      cleanupDuckDBArtifacts();
      closeDuckDBConnection();

      openDuckDBConnection();
      await pingDuckDB();
      return;
    }

    throw err;
  }
}


/**
 * Check if any files of a given extension exist for a type (lazy check, no memory accumulation)
 */
function hasFileType(type, extension) {
  try {
    if (!fs.existsSync(DATA_PATH)) return false;
    const stack = [DATA_PATH];
    while (stack.length > 0) {
      const dir = stack.pop();
      const entries = fs.readdirSync(dir, { withFileTypes: true });
      for (const entry of entries) {
        if (entry.isDirectory()) {
          stack.push(path.join(dir, entry.name));
        } else if (entry.name.includes(`${type}-`) && entry.name.endsWith(extension)) {
          return true; // Found one, stop immediately
        }
      }
    }
    return false;
  } catch {
    return false;
  }
}

/**
 * Count files matching pattern (for logging) - limited scan
 */
function countDataFiles(type = 'events', maxScan = 10000) {
  try {
    if (!fs.existsSync(DATA_PATH)) return 0;
    let count = 0;
    const stack = [DATA_PATH];
    while (stack.length > 0 && count < maxScan) {
      const dir = stack.pop();
      const entries = fs.readdirSync(dir, { withFileTypes: true });
      for (const entry of entries) {
        if (entry.isDirectory()) {
          stack.push(path.join(dir, entry.name));
        } else if (
          entry.name.includes(`${type}-`) && 
          (entry.name.endsWith('.jsonl') || 
           entry.name.endsWith('.jsonl.gz') || 
           entry.name.endsWith('.jsonl.zst') ||
           entry.name.endsWith('.pb.zst'))
        ) {
          count++;  // Fixed: was missing this increment
          if (count >= maxScan) break;
        }
      }
    }
    return count;
  } catch {
    return 0;
  }
}

// Legacy function - now uses lazy detection
function findDataFiles(type = 'events') {
  console.warn('⚠️ findDataFiles() is deprecated for large datasets');
  return []; // Return empty, force callers to use glob patterns
}

function hasDataFiles(type = 'events') {
  return hasFileType(type, '.jsonl') || 
         hasFileType(type, '.jsonl.gz') || 
         hasFileType(type, '.jsonl.zst') ||
         hasFileType(type, '.pb.zst');
}

// Helper to run queries
export async function query(sql, params = []) {
  await ensureDuckDBReady();
  return new Promise((resolve, reject) => {
    conn.all(sql, ...params, (err, rows) => {
      if (err) reject(err);
      else resolve(rows);
    });
  });
}

// Helper to get a single row
export async function queryOne(sql, params = []) {
  const rows = await query(sql, params);
  return rows[0] || null;
}

// Helper to get file glob pattern (supports both jsonl and parquet)
export function getFileGlob(type = 'events', dateFilter = null, format = 'jsonl') {
  const ext = format === 'parquet' ? 'parquet' : 'jsonl';
  
  if (dateFilter) {
    const { year, month, day } = dateFilter;
    let pattern = `${DATA_PATH}/year=${year}`;
    if (month) pattern += `/month=${String(month).padStart(2, '0')}`;
    if (day) pattern += `/day=${String(day).padStart(2, '0')}`;
    pattern += `/${type}-*.${ext}`;
    return pattern;
  }
  return `${DATA_PATH}/**/${type}-*.${ext}`;
}

// Legacy function for backward compatibility
export function getParquetGlob(type = 'events', dateFilter = null) {
  return getFileGlob(type, dateFilter, 'parquet');
}

// Helper for safe table reads with error handling
export async function safeQuery(sql) {
  try {
    return await query(sql);
  } catch (err) {
    console.error('Query error:', err.message);
    console.error('SQL:', sql);
    throw err;
  }
}

// Read JSON-lines files using DuckDB glob patterns (more efficient for large datasets)
// Uses forward slashes which DuckDB handles on all platforms
// Uses lazy file detection to avoid memory issues with large file counts
export function readJsonlGlob(type = 'events') {
  const basePath = DATA_PATH.replace(/\\/g, '/');
  
  // Lazy check - stops as soon as one file of each type is found
  const hasJsonl = hasFileType(type, '.jsonl');
  const hasGzip = hasFileType(type, '.jsonl.gz');
  const hasZstd = hasFileType(type, '.jsonl.zst');
  
  if (!hasJsonl && !hasGzip && !hasZstd) {
    return `(SELECT NULL as placeholder WHERE false)`;
  }
  
  const queries = [];
  if (hasJsonl) {
    queries.push(`SELECT * FROM read_json_auto('${basePath}/**/${type}-*.jsonl', union_by_name=true, ignore_errors=true)`);
  }
  if (hasGzip) {
    queries.push(`SELECT * FROM read_json_auto('${basePath}/**/${type}-*.jsonl.gz', union_by_name=true, ignore_errors=true)`);
  }
  if (hasZstd) {
    queries.push(`SELECT * FROM read_json_auto('${basePath}/**/${type}-*.jsonl.zst', union_by_name=true, ignore_errors=true)`);
  }
  
  return `(${queries.join(' UNION ALL ')})`;
}

// Read from explicit file list - use only for small lists (<100 files)
export function readJsonlFiles(filePaths) {
  if (filePaths.length === 0) {
    return `(SELECT NULL as placeholder WHERE false)`;
  }
  // For large file counts, fall back to glob patterns
  if (filePaths.length > 100) {
    console.warn(`⚠️ Too many files (${filePaths.length}), using glob pattern instead`);
    // Detect type from first file path
    const type = filePaths[0].includes('events-') ? 'events' : 'updates';
    return readJsonlGlob(type);
  }
  if (filePaths.length === 1) {
    return `read_json_auto('${filePaths[0]}', union_by_name=true, ignore_errors=true)`;
  }
  const selects = filePaths.map(f => 
    `SELECT * FROM read_json_auto('${f}', union_by_name=true, ignore_errors=true)`
  );
  return `(${selects.join(' UNION ALL ')})`;
}

// Legacy function using glob patterns
export function readJsonl(globPattern) {
  const gzPattern = globPattern.replace('.jsonl', '.jsonl.gz');
  const zstPattern = globPattern.replace('.jsonl', '.jsonl.zst');
  return `(
    SELECT * FROM read_json_auto('${globPattern}', union_by_name=true, ignore_errors=true)
    UNION ALL
    SELECT * FROM read_json_auto('${gzPattern}', union_by_name=true, ignore_errors=true)
    UNION ALL
    SELECT * FROM read_json_auto('${zstPattern}', union_by_name=true, ignore_errors=true)
  )`;
}

// Read parquet file using DuckDB  
export function readParquet(globPattern) {
  return `read_parquet('${globPattern}', union_by_name=true)`;
}

// Initialize views for common queries
// For large datasets, we skip view creation and use direct queries instead
export async function initializeViews() {
  // Use lazy counting with a cap to avoid memory issues
  const eventCount = countDataFiles('events', 10000);
  const updateCount = countDataFiles('updates', 10000);
  
  console.log(`📂 Found ~${eventCount}${eventCount >= 10000 ? '+' : ''} event files, ~${updateCount}${updateCount >= 10000 ? '+' : ''} update files`);
  
  // For large datasets (>1000 files), skip view creation to avoid OOM
  const MAX_FILES_FOR_VIEWS = 1000;
  
  if (eventCount > MAX_FILES_FOR_VIEWS || updateCount > MAX_FILES_FOR_VIEWS) {
    console.log(`⚠️ Dataset too large for views`);
    console.log('ℹ️ Using direct queries instead of materialized views');
    
    // Create empty placeholder views
    await query(`CREATE OR REPLACE VIEW all_events AS SELECT NULL as placeholder WHERE false`);
    await query(`CREATE OR REPLACE VIEW all_updates AS SELECT NULL as placeholder WHERE false`);
    return;
  }
  
  if (eventCount === 0 && updateCount === 0) {
    console.log('ℹ️ No data files found yet');
    await query(`CREATE OR REPLACE VIEW all_events AS SELECT NULL as placeholder WHERE false`);
    await query(`CREATE OR REPLACE VIEW all_updates AS SELECT NULL as placeholder WHERE false`);
    return;
  }
  
  try {
    // For small datasets, create views using glob patterns (no file list accumulation)
    if (eventCount > 0 && eventCount <= MAX_FILES_FOR_VIEWS) {
      await query(`
        CREATE OR REPLACE VIEW all_events AS
        SELECT * FROM ${readJsonlGlob('events')}
      `);
      console.log(`✅ Created events view (~${eventCount} files)`);
    } else {
      await query(`CREATE OR REPLACE VIEW all_events AS SELECT NULL as placeholder WHERE false`);
    }
    
    if (updateCount > 0 && updateCount <= MAX_FILES_FOR_VIEWS) {
      await query(`
        CREATE OR REPLACE VIEW all_updates AS
        SELECT * FROM ${readJsonlGlob('updates')}
      `);
      console.log(`✅ Created updates view (~${updateCount} files)`);
    } else {
      await query(`CREATE OR REPLACE VIEW all_updates AS SELECT NULL as placeholder WHERE false`);
    }
    
    console.log('✅ DuckDB views initialized');
  } catch (err) {
    console.error('❌ View creation failed:', err.message);
    // Create empty placeholders as fallback
    try {
      await query(`CREATE OR REPLACE VIEW all_events AS SELECT NULL as placeholder WHERE false`);
      await query(`CREATE OR REPLACE VIEW all_updates AS SELECT NULL as placeholder WHERE false`);
    } catch (e) {
      console.error('❌ Failed to create placeholder views:', e.message);
    }
  }
}

// Initialize on import (best-effort only). Never crash the server on init failures.
initializeViews().catch((err) => {
  console.error('❌ DuckDB view initialization failed during startup:', err?.message || err);
});

export { hasFileType, countDataFiles, hasDataFiles, DATA_PATH, ACS_DATA_PATH };

export default { query, safeQuery, getFileGlob, getParquetGlob, readJsonl, readJsonlFiles, readJsonlGlob, readParquet, findDataFiles, hasFileType, countDataFiles, hasDataFiles, DATA_PATH, ACS_DATA_PATH };
