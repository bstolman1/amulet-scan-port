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

// Helper to get parquet glob pattern
export function getParquetGlob(type = 'events', dateFilter = null) {
  if (dateFilter) {
    const { year, month, day } = dateFilter;
    let pattern = `${DATA_PATH}/year=${year}`;
    if (month) pattern += `/month=${String(month).padStart(2, '0')}`;
    if (day) pattern += `/day=${String(day).padStart(2, '0')}`;
    pattern += `/${type}-*.parquet`;
    return pattern;
  }
  return `${DATA_PATH}/**/${type}-*.parquet`;
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

// Initialize views for common queries
export async function initializeViews() {
  try {
    // Create views for easier querying
    await query(`
      CREATE OR REPLACE VIEW all_events AS
      SELECT * FROM read_parquet('${DATA_PATH}/**/*events*.parquet', union_by_name=true)
    `);
    
    await query(`
      CREATE OR REPLACE VIEW all_updates AS
      SELECT * FROM read_parquet('${DATA_PATH}/**/*updates*.parquet', union_by_name=true)
    `);
    
    console.log('✅ DuckDB views initialized');
  } catch (err) {
    console.warn('⚠️ Could not initialize views (data may not exist yet):', err.message);
  }
}

// Initialize on import
initializeViews();

export default { query, safeQuery, getParquetGlob, DATA_PATH };
