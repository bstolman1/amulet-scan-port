import duckdb from 'duckdb';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DATA_PATH = path.resolve(__dirname, '../../data/raw');

// In-memory DuckDB instance
const db = new duckdb.Database(':memory:');
const conn = db.connect();

/**
 * Find all data files matching patterns (for graceful handling when no data)
 * Returns array of full file paths
 */
function findDataFiles(type = 'events') {
  try {
    if (!fs.existsSync(DATA_PATH)) return [];
    const allFiles = fs.readdirSync(DATA_PATH, { recursive: true });
    return allFiles
      .map(f => String(f))
      .filter(f => f.includes(`${type}-`) && (f.endsWith('.jsonl') || f.endsWith('.jsonl.gz')))
      .map(f => path.join(DATA_PATH, f).replace(/\\/g, '/')); // Normalize to forward slashes for DuckDB
  } catch {
    return [];
  }
}

function hasDataFiles(type = 'events') {
  return findDataFiles(type).length > 0;
}

// Helper to run queries
export function query(sql, params = []) {
  return new Promise((resolve, reject) => {
    conn.all(sql, ...params, (err, rows) => {
      if (err) reject(err);
      else resolve(rows);
    });
  });
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
export function readJsonlGlob(type = 'events') {
  const basePath = DATA_PATH.replace(/\\/g, '/');
  return `(
    SELECT * FROM read_json_auto('${basePath}/**/${type}-*.jsonl', union_by_name=true, ignore_errors=true)
    UNION ALL
    SELECT * FROM read_json_auto('${basePath}/**/${type}-*.jsonl.gz', union_by_name=true, ignore_errors=true)
  )`;
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
  return `(
    SELECT * FROM read_json_auto('${globPattern}', union_by_name=true, ignore_errors=true)
    UNION ALL
    SELECT * FROM read_json_auto('${gzPattern}', union_by_name=true, ignore_errors=true)
  )`;
}

// Read parquet file using DuckDB  
export function readParquet(globPattern) {
  return `read_parquet('${globPattern}', union_by_name=true)`;
}

// Initialize views for common queries
export async function initializeViews() {
  // Check if data files exist first to avoid DuckDB errors
  const hasEvents = hasDataFiles('events');
  const hasUpdates = hasDataFiles('updates');
  
  if (!hasEvents && !hasUpdates) {
    console.log('ℹ️ No data files found yet - views will be created when data arrives');
    // Create empty placeholder views
    await query(`CREATE OR REPLACE VIEW all_events AS SELECT NULL as placeholder WHERE false`);
    await query(`CREATE OR REPLACE VIEW all_updates AS SELECT NULL as placeholder WHERE false`);
    return;
  }
  
  try {
    // Use glob patterns directly - more efficient for large datasets
    const basePath = DATA_PATH.replace(/\\/g, '/');
    
    if (hasEvents) {
      await query(`
        CREATE OR REPLACE VIEW all_events AS
        SELECT * FROM ${readJsonlGlob('events')}
      `);
      console.log('✅ Created events view using glob pattern');
    } else {
      await query(`CREATE OR REPLACE VIEW all_events AS SELECT NULL as placeholder WHERE false`);
    }
    
    if (hasUpdates) {
      await query(`
        CREATE OR REPLACE VIEW all_updates AS
        SELECT * FROM ${readJsonlGlob('updates')}
      `);
      console.log('✅ Created updates view using glob pattern');
    } else {
      await query(`CREATE OR REPLACE VIEW all_updates AS SELECT NULL as placeholder WHERE false`);
    }
    
    console.log('✅ DuckDB views initialized');
  } catch (err) {
    console.error('❌ Failed to initialize views:', err.message);
    // Create empty placeholder views as fallback
    try {
      await query(`CREATE OR REPLACE VIEW all_events AS SELECT NULL as placeholder WHERE false`);
      await query(`CREATE OR REPLACE VIEW all_updates AS SELECT NULL as placeholder WHERE false`);
    } catch (e) {
      console.error('❌ Failed to create placeholder views:', e.message);
    }
  }
}

// Initialize on import
initializeViews();

export default { query, safeQuery, getFileGlob, getParquetGlob, readJsonl, readJsonlFiles, readParquet, findDataFiles, DATA_PATH };
