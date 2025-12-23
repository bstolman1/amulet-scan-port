import duckdb from 'duckdb';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// Prefer the repository-local data directory by default (works cross-platform)
// Repo layout: server/duckdb/connection.js -> ../../data
const REPO_DATA_DIR = path.join(__dirname, '../../data');

// Legacy Windows default path (kept as last-resort fallback)
const WIN_DEFAULT_DATA_DIR = 'C:\\ledger_raw';

// Final selection order:
// 1) process.env.DATA_DIR (explicit override)
// 2) repo-local data/ (default)
// 3) Windows legacy default path (last resort)
const BASE_DATA_DIR = process.env.DATA_DIR || REPO_DATA_DIR || WIN_DEFAULT_DATA_DIR;
console.log(`📁 BASE_DATA_DIR: ${BASE_DATA_DIR}`);

// Ensure directories exist (DuckDB will fail to create the DB file if the parent dir is missing)
try {
  fs.mkdirSync(BASE_DATA_DIR, { recursive: true });
} catch {}

// Ledger events/updates live under: <BASE_DATA_DIR>/raw
const DATA_PATH = path.join(BASE_DATA_DIR, 'raw');
// ACS snapshots live under: <BASE_DATA_DIR>/raw/acs
const ACS_DATA_PATH = path.join(BASE_DATA_DIR, 'raw', 'acs');

try {
  fs.mkdirSync(DATA_PATH, { recursive: true });
  fs.mkdirSync(ACS_DATA_PATH, { recursive: true });
} catch {}

// Database file path (persistent storage)
export const DB_FILE = process.env.DUCKDB_FILE || path.join(BASE_DATA_DIR, 'canton-explorer.duckdb');
console.log(`🦆 DuckDB database: ${DB_FILE}`);

// ✅ Single-process singleton DB handle (Windows safe)
// Rule: new duckdb.Database() must be called once per process.
let _db = null;

export function getDB() {
  if (!_db) {
    _db = new duckdb.Database(DB_FILE);
  }
  return _db;
}

export function closeDB() {
  if (_db) {
    try {
      _db.close();
    } catch {}
    _db = null;
  }
}

// Query helper: open a short-lived connection for each query, but reuse the single DB handle.
export function query(sql, params = []) {
  return new Promise((resolve, reject) => {
    const db = getDB();
    const conn = db.connect();

    const cleanup = () => {
      try {
        conn.close();
      } catch {}
    };

    const done = (err, rows) => {
      cleanup();
      if (err) {
        console.error('❌ DuckDB query error:', err?.message || err);
        console.error('   DB_FILE:', DB_FILE);
        console.error('   SQL (first 200 chars):', String(sql).slice(0, 200));
        reject(err);
        return;
      }
      resolve(rows ?? []);
    };

    // Use conn.run for DDL (CREATE/DROP/ALTER/INSERT/UPDATE/DELETE) and conn.all for queries
    const isDDL = /^\s*(CREATE|DROP|ALTER|INSERT|UPDATE|DELETE)/i.test(sql);

    try {
      if (isDDL) {
        conn.run(sql, done);
      } else if (params && params.length > 0) {
        conn.all(sql, params, done);
      } else {
        conn.all(sql, done);
      }
    } catch (err) {
      cleanup();
      console.error('❌ DuckDB threw:', err?.message || err);
      reject(err);
    }
  });
}

// Helper to get a single row
export async function queryOne(sql, params = []) {
  const rows = await query(sql, params);
  return rows[0] || null;
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
          count++;
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

// NOTE: Do not auto-initialize views at import time.
// It can crash the process on startup (unhandled promise rejection) and should be invoked explicitly by the server.
// initializeViews();

export { hasFileType, countDataFiles, hasDataFiles, DATA_PATH, ACS_DATA_PATH, getDB, closeDB }; 

export default { query, queryOne, safeQuery, getFileGlob, getParquetGlob, readJsonl, readJsonlFiles, readJsonlGlob, readParquet, findDataFiles, hasFileType, countDataFiles, hasDataFiles, DATA_PATH, ACS_DATA_PATH, DB_FILE, getDB, closeDB };
