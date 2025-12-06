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

// Read JSON-lines files using DuckDB from explicit file list
// This avoids glob pattern issues on Windows
export function readJsonlFiles(filePaths) {
  if (filePaths.length === 0) {
    return `(SELECT NULL as placeholder WHERE false)`;
  }
  if (filePaths.length === 1) {
    return `read_json_auto('${filePaths[0]}', union_by_name=true, ignore_errors=true)`;
  }
  // Union all files together
  const selects = filePaths.map(f => 
    `SELECT * FROM read_json_auto('${f}', union_by_name=true, ignore_errors=true)`
  );
  return `(${selects.join(' UNION ALL ')})`;
}

// Legacy function using glob patterns (may fail on Windows with no files)
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
    // Find actual files and create views from them (avoids glob issues on Windows)
    const eventFiles = findDataFiles('events');
    const updateFiles = findDataFiles('updates');
    
    console.log(`📂 Found ${eventFiles.length} event files, ${updateFiles.length} update files`);
    
    if (eventFiles.length > 0) {
      await query(`
        CREATE OR REPLACE VIEW all_events AS
        SELECT * FROM ${readJsonlFiles(eventFiles)}
      `);
    } else {
      await query(`CREATE OR REPLACE VIEW all_events AS SELECT NULL as placeholder WHERE false`);
    }
    
    if (updateFiles.length > 0) {
      await query(`
        CREATE OR REPLACE VIEW all_updates AS
        SELECT * FROM ${readJsonlFiles(updateFiles)}
      `);
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
