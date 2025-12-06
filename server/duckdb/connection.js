import duckdb from 'duckdb';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DATA_PATH = path.resolve(__dirname, '../../data/raw');

// In-memory DuckDB instance
const db = new duckdb.Database(':memory:');
const conn = db.connect();

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

// Read JSON-lines file using DuckDB (supports both .jsonl and .jsonl.gz)
// Uses UNION to work cross-platform (Windows doesn't support brace expansion)
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
  try {
    // Try JSONL first (our primary format), then fallback to parquet
    const jsonlEventsGlob = getFileGlob('events', null, 'jsonl');
    const jsonlUpdatesGlob = getFileGlob('updates', null, 'jsonl');
    
    // Create views for easier querying - using JSON-lines format
    await query(`
      CREATE OR REPLACE VIEW all_events AS
      SELECT * FROM ${readJsonl(jsonlEventsGlob)}
    `);
    
    await query(`
      CREATE OR REPLACE VIEW all_updates AS
      SELECT * FROM ${readJsonl(jsonlUpdatesGlob)}
    `);
    
    console.log('✅ DuckDB views initialized (JSONL format)');
  } catch (err) {
    console.warn('⚠️ Could not initialize views (data may not exist yet):', err.message);
    
    // Try parquet as fallback
    try {
      const parquetEventsGlob = getFileGlob('events', null, 'parquet');
      const parquetUpdatesGlob = getFileGlob('updates', null, 'parquet');
      
      await query(`
        CREATE OR REPLACE VIEW all_events AS
        SELECT * FROM ${readParquet(parquetEventsGlob)}
      `);
      
      await query(`
        CREATE OR REPLACE VIEW all_updates AS
        SELECT * FROM ${readParquet(parquetUpdatesGlob)}
      `);
      
      console.log('✅ DuckDB views initialized (Parquet format)');
    } catch (e) {
      console.warn('⚠️ No data files found yet');
    }
  }
}

// Initialize on import
initializeViews();

export default { query, safeQuery, getFileGlob, getParquetGlob, readJsonl, readParquet, DATA_PATH };
