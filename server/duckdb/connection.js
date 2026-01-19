import duckdb from 'duckdb';
import path from 'path';
import fs from 'fs';
import os from 'os';

// Use process.cwd() for Vitest compatibility (fileURLToPath breaks under Vite SSR)
const __dirname = path.join(process.cwd(), 'server', 'duckdb');

// ============================================================
// PLATFORM DETECTION
// Windows requires per-query connections due to exclusive file locking
// ============================================================
const IS_WINDOWS = os.platform() === 'win32';
if (IS_WINDOWS) {
  console.log('🪟 Windows detected - using per-query DuckDB connections');
}

// ============================================================
// TEST MODE DETECTION
// In test mode, use small fixture dataset instead of scanning hundreds of raw files
// ============================================================
const IS_TEST = process.env.NODE_ENV === 'test';
const TEST_FIXTURES_PATH = path.join(process.cwd(), 'data', 'test-fixtures').replace(/\\/g, '/');

// Prefer the repository-local data directory if it exists (common in Lovable + WSL setups)
// Repo layout: server/duckdb/connection.js -> ../../data
const REPO_DATA_DIR = path.join(__dirname, '../../data');
const repoRawDir = path.join(REPO_DATA_DIR, 'raw');

// DATA_DIR should point to the base directory
// Default Windows path: C:\ledger_raw
const WIN_DEFAULT_DATA_DIR = 'C:\\ledger_raw';

// Final selection order:
// 1) In test mode, use test-fixtures (skip raw data scan entirely)
// 2) process.env.DATA_DIR (explicit override)
// 3) repo-local data/ (if present)
// 4) Windows default path
const BASE_DATA_DIR = process.env.DATA_DIR || (fs.existsSync(repoRawDir) ? REPO_DATA_DIR : WIN_DEFAULT_DATA_DIR);
// Ledger events/updates live under: <BASE_DATA_DIR>/raw
// In test mode, point to test-fixtures instead
const DATA_PATH = IS_TEST ? TEST_FIXTURES_PATH : path.join(BASE_DATA_DIR, 'raw');
// ACS snapshots live under: <BASE_DATA_DIR>/raw/acs
const ACS_DATA_PATH = path.join(BASE_DATA_DIR, 'raw', 'acs');

// Persistent DuckDB instance (survives restarts, shareable between processes)
// In test mode, always use an in-memory DB to avoid file locking / cross-test interference.
const DB_FILE = IS_TEST
  ? ':memory:'
  : (process.env.DUCKDB_FILE || path.join(BASE_DATA_DIR, 'canton-explorer.duckdb'));
console.log(`🦆 DuckDB database: ${DB_FILE}`);

// ============================================================
// CONNECTION STRATEGY
// Linux: Connection pool (shared Database instance, multiple connections)
// Windows: Per-query connections (create db + conn, execute, close both)
// ============================================================

const POOL_SIZE = IS_TEST ? 1 : parseInt(process.env.DUCKDB_POOL_SIZE || '4', 10);
const POOL_TIMEOUT_MS = parseInt(process.env.DUCKDB_POOL_TIMEOUT_MS || '30000', 10);

// Only create persistent db instance on Linux
const db = IS_WINDOWS ? null : new duckdb.Database(DB_FILE);

// Pool of connections
const pool = [];
const waiting = []; // Queue of pending requests for connections
let poolInitialized = false;

// ============================================================
// POOL HEALTH METRICS
// ============================================================
const poolMetrics = {
  // Counters
  totalQueries: 0,
  totalErrors: 0,
  timeoutCount: 0,
  
  // Wait time tracking
  totalWaitTimeMs: 0,
  waitCount: 0, // Number of times a query had to wait
  maxWaitTimeMs: 0,
  
  // Query time tracking
  totalQueryTimeMs: 0,
  maxQueryTimeMs: 0,
  
  // Peak usage
  peakInUse: 0,
  peakWaiting: 0,
  
  // Start time for uptime calculation
  startedAt: Date.now(),
  
  // Recent wait times (circular buffer for percentile calculations)
  recentWaitTimes: [],
  recentQueryTimes: [],
  maxRecentSamples: 1000,
};

/**
 * Record a wait time sample
 */
function recordWaitTime(waitMs) {
  poolMetrics.totalWaitTimeMs += waitMs;
  poolMetrics.waitCount++;
  if (waitMs > poolMetrics.maxWaitTimeMs) {
    poolMetrics.maxWaitTimeMs = waitMs;
  }
  
  // Add to circular buffer
  poolMetrics.recentWaitTimes.push(waitMs);
  if (poolMetrics.recentWaitTimes.length > poolMetrics.maxRecentSamples) {
    poolMetrics.recentWaitTimes.shift();
  }
}

/**
 * Record a query time sample
 */
function recordQueryTime(queryMs) {
  poolMetrics.totalQueryTimeMs += queryMs;
  if (queryMs > poolMetrics.maxQueryTimeMs) {
    poolMetrics.maxQueryTimeMs = queryMs;
  }
  
  // Add to circular buffer
  poolMetrics.recentQueryTimes.push(queryMs);
  if (poolMetrics.recentQueryTimes.length > poolMetrics.maxRecentSamples) {
    poolMetrics.recentQueryTimes.shift();
  }
}

/**
 * Calculate percentile from sorted array
 */
function percentile(sortedArr, p) {
  if (sortedArr.length === 0) return 0;
  const idx = Math.ceil((p / 100) * sortedArr.length) - 1;
  return sortedArr[Math.max(0, idx)];
}

/**
 * Update peak usage metrics
 */
function updatePeakMetrics() {
  const inUse = pool.filter(c => c.inUse).length;
  if (inUse > poolMetrics.peakInUse) {
    poolMetrics.peakInUse = inUse;
  }
  if (waiting.length > poolMetrics.peakWaiting) {
    poolMetrics.peakWaiting = waiting.length;
  }
}

/**
 * Create a fresh connection for the pool (Linux only)
 */
function createConnection(id) {
  if (IS_WINDOWS) {
    throw new Error('createConnection() should not be called on Windows');
  }
  return {
    id,
    conn: db.connect(),
    inUse: false,
    createdAt: Date.now(),
  };
}

/**
 * Test if a connection is healthy by running a simple query (Linux only)
 */
function testConnection(connWrapper) {
  return new Promise((resolve) => {
    try {
      connWrapper.conn.all('SELECT 1 AS ok', (err, rows) => {
        resolve(!err && rows && rows.length > 0);
      });
    } catch {
      resolve(false);
    }
  });
}

/**
 * Initialize the connection pool (Linux only)
 */
function initPool() {
  if (IS_WINDOWS) {
    console.log(`🦆 DuckDB per-query mode (Windows) - no connection pool`);
    poolInitialized = true;
    return;
  }
  
  if (poolInitialized) return;
  
  for (let i = 0; i < POOL_SIZE; i++) {
    pool.push(createConnection(i));
  }
  
  console.log(`🦆 DuckDB connection pool initialized: ${POOL_SIZE} connections`);
  poolInitialized = true;
}

// Initialize pool on module load
initPool();

/**
 * Acquire a connection from the pool (Linux only)
 * Returns a promise that resolves with a connection wrapper
 * Automatically tests and recreates stale connections
 */
async function acquireConnection() {
  if (IS_WINDOWS) {
    throw new Error('acquireConnection() should not be called on Windows');
  }
  
  const acquireStart = Date.now();
  
  // Try to find an available connection
  const available = pool.find(c => !c.inUse);
  if (available) {
    available.inUse = true;
    updatePeakMetrics();
    
    // Test connection health - recreate if stale
    const isHealthy = await testConnection(available);
    if (!isHealthy) {
      console.log(`🔄 Recreating stale DuckDB connection ${available.id}`);
      try {
        available.conn.close?.();
      } catch { /* ignore close errors */ }
      available.conn = db.connect();
      available.createdAt = Date.now();
    }
    
    return { connWrapper: available, waitMs: 0 };
  }
  
  // No available connection, add to waiting queue with timeout
  return new Promise((resolve, reject) => {
    const timeoutId = setTimeout(() => {
      const idx = waiting.findIndex(w => w.reject === reject);
      if (idx !== -1) {
        waiting.splice(idx, 1);
        poolMetrics.timeoutCount++;
        reject(new Error(`Connection pool timeout after ${POOL_TIMEOUT_MS}ms`));
      }
    }, POOL_TIMEOUT_MS);
    
    waiting.push({ 
      resolve: async (connWrapper) => {
        const waitMs = Date.now() - acquireStart;
        recordWaitTime(waitMs);
        
        // Test connection health - recreate if stale
        const isHealthy = await testConnection(connWrapper);
        if (!isHealthy) {
          console.log(`🔄 Recreating stale DuckDB connection ${connWrapper.id}`);
          try {
            connWrapper.conn.close?.();
          } catch { /* ignore close errors */ }
          connWrapper.conn = db.connect();
          connWrapper.createdAt = Date.now();
        }
        
        resolve({ connWrapper, waitMs });
      }, 
      reject, 
      timeoutId,
      enqueuedAt: acquireStart,
    });
    
    updatePeakMetrics();
  });
}

/**
 * Release a connection back to the pool (Linux only)
 */
function releaseConnection(connWrapper) {
  if (IS_WINDOWS) return; // No-op on Windows
  
  connWrapper.inUse = false;
  
  // If there are waiting requests, give them this connection
  if (waiting.length > 0) {
    const next = waiting.shift();
    clearTimeout(next.timeoutId);
    connWrapper.inUse = true;
    next.resolve(connWrapper);
  }
}

/**
 * Execute a query using per-query connection (Windows)
 * Creates fresh db + connection, executes, closes both immediately
 */
function queryWindows(sql, params = []) {
  const queryStart = Date.now();
  poolMetrics.totalQueries++;
  
  return new Promise((resolve, reject) => {
    const localDb = new duckdb.Database(DB_FILE);
    const conn = localDb.connect();
    
    conn.all(sql, ...params, (err, rows) => {
      const queryMs = Date.now() - queryStart;
      recordQueryTime(queryMs);
      
      // Always close connection and db
      try { conn.close(); } catch { /* ignore */ }
      try { localDb.close(); } catch { /* ignore */ }
      
      if (err) {
        poolMetrics.totalErrors++;
        reject(err);
      } else {
        resolve(rows);
      }
    });
  });
}

/**
 * Execute a query using a pooled connection (Linux)
 * Automatically acquires and releases connections
 */
async function queryLinux(sql, params = []) {
  const { connWrapper, waitMs } = await acquireConnection();
  const queryStart = Date.now();
  
  try {
    poolMetrics.totalQueries++;
    
    return await new Promise((resolve, reject) => {
      connWrapper.conn.all(sql, ...params, (err, rows) => {
        if (err) {
          poolMetrics.totalErrors++;
          reject(err);
        } else {
          resolve(rows);
        }
      });
    });
  } finally {
    const queryMs = Date.now() - queryStart;
    recordQueryTime(queryMs);
    releaseConnection(connWrapper);
  }
}

/**
 * Execute a query - automatically chooses strategy based on platform
 */
export async function query(sql, params = []) {
  if (IS_WINDOWS) {
    return queryWindows(sql, params);
  }
  return queryLinux(sql, params);
}

/**
 * Execute multiple queries in parallel safely
 * Each query gets its own connection from the pool
 */
export async function queryParallel(queries) {
  return Promise.all(queries.map(({ sql, params = [] }) => query(sql, params)));
}

/**
 * Execute a query and return a single row
 */
export async function queryOne(sql, params = []) {
  const rows = await query(sql, params);
  return rows[0] || null;
}

/**
 * Get pool statistics including health metrics
 */
export function getPoolStats() {
  const inUse = pool.filter(c => c.inUse).length;
  const available = pool.filter(c => !c.inUse).length;
  const uptimeMs = Date.now() - poolMetrics.startedAt;
  
  // Calculate averages
  const avgWaitTimeMs = poolMetrics.waitCount > 0 
    ? poolMetrics.totalWaitTimeMs / poolMetrics.waitCount 
    : 0;
  const avgQueryTimeMs = poolMetrics.totalQueries > 0 
    ? poolMetrics.totalQueryTimeMs / poolMetrics.totalQueries 
    : 0;
  
  // Calculate percentiles from recent samples
  const sortedWaitTimes = [...poolMetrics.recentWaitTimes].sort((a, b) => a - b);
  const sortedQueryTimes = [...poolMetrics.recentQueryTimes].sort((a, b) => a - b);
  
  return {
    // Current state
    size: POOL_SIZE,
    inUse,
    available,
    waiting: waiting.length,
    timeoutMs: POOL_TIMEOUT_MS,
    
    // Health metrics
    health: {
      uptimeMs,
      uptimeFormatted: formatDuration(uptimeMs),
      
      // Query stats
      totalQueries: poolMetrics.totalQueries,
      totalErrors: poolMetrics.totalErrors,
      errorRate: poolMetrics.totalQueries > 0 
        ? (poolMetrics.totalErrors / poolMetrics.totalQueries * 100).toFixed(2) + '%'
        : '0%',
      queriesPerSecond: uptimeMs > 0 
        ? (poolMetrics.totalQueries / (uptimeMs / 1000)).toFixed(2)
        : 0,
      
      // Wait time stats
      timeoutCount: poolMetrics.timeoutCount,
      waitCount: poolMetrics.waitCount,
      avgWaitTimeMs: Math.round(avgWaitTimeMs * 100) / 100,
      maxWaitTimeMs: poolMetrics.maxWaitTimeMs,
      p50WaitTimeMs: percentile(sortedWaitTimes, 50),
      p95WaitTimeMs: percentile(sortedWaitTimes, 95),
      p99WaitTimeMs: percentile(sortedWaitTimes, 99),
      
      // Query time stats
      avgQueryTimeMs: Math.round(avgQueryTimeMs * 100) / 100,
      maxQueryTimeMs: poolMetrics.maxQueryTimeMs,
      p50QueryTimeMs: percentile(sortedQueryTimes, 50),
      p95QueryTimeMs: percentile(sortedQueryTimes, 95),
      p99QueryTimeMs: percentile(sortedQueryTimes, 99),
      
      // Peak usage
      peakInUse: poolMetrics.peakInUse,
      peakWaiting: poolMetrics.peakWaiting,
      peakUtilization: ((poolMetrics.peakInUse / POOL_SIZE) * 100).toFixed(1) + '%',
      
      // Sample sizes
      recentWaitSamples: poolMetrics.recentWaitTimes.length,
      recentQuerySamples: poolMetrics.recentQueryTimes.length,
    },
  };
}

/**
 * Reset pool metrics (useful for testing or after config changes)
 */
export function resetPoolMetrics() {
  poolMetrics.totalQueries = 0;
  poolMetrics.totalErrors = 0;
  poolMetrics.timeoutCount = 0;
  poolMetrics.totalWaitTimeMs = 0;
  poolMetrics.waitCount = 0;
  poolMetrics.maxWaitTimeMs = 0;
  poolMetrics.totalQueryTimeMs = 0;
  poolMetrics.maxQueryTimeMs = 0;
  poolMetrics.peakInUse = 0;
  poolMetrics.peakWaiting = 0;
  poolMetrics.startedAt = Date.now();
  poolMetrics.recentWaitTimes = [];
  poolMetrics.recentQueryTimes = [];
  
  console.log('🦆 Pool metrics reset');
}

/**
 * Format duration in human-readable format
 */
function formatDuration(ms) {
  const seconds = Math.floor(ms / 1000);
  const minutes = Math.floor(seconds / 60);
  const hours = Math.floor(minutes / 60);
  const days = Math.floor(hours / 24);
  
  if (days > 0) return `${days}d ${hours % 24}h ${minutes % 60}m`;
  if (hours > 0) return `${hours}h ${minutes % 60}m ${seconds % 60}s`;
  if (minutes > 0) return `${minutes}m ${seconds % 60}s`;
  return `${seconds}s`;
}

// ============================================================
// FILE DETECTION UTILITIES
// ============================================================

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
           entry.name.endsWith('.pb.zst') ||
           entry.name.endsWith('.parquet'))
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
  return hasFileType(type, '.parquet') ||
         hasFileType(type, '.jsonl') || 
         hasFileType(type, '.jsonl.gz') || 
         hasFileType(type, '.jsonl.zst') ||
         hasFileType(type, '.pb.zst');
}

function hasParquetFiles(type = 'events') {
  return hasFileType(type, '.parquet');
}

// ============================================================
// QUERY HELPERS
// ============================================================

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

// Read from Parquet files (preferred format)
export function readParquetGlob(type = 'events') {
  const basePath = DATA_PATH.replace(/\\/g, '/');
  
  if (!hasParquetFiles(type)) {
    return `(SELECT NULL as placeholder WHERE false)`;
  }
  
  return `read_parquet('${basePath}/**/${type}-*.parquet', union_by_name=true)`;
}

// Read JSON-lines files using DuckDB glob patterns (more efficient for large datasets)
// Uses forward slashes which DuckDB handles on all platforms
// Uses lazy file detection to avoid memory issues with large file counts
export function readJsonlGlob(type = 'events') {
  // In test mode, use small fixture dataset to avoid scanning 415+ raw files
  if (IS_TEST) {
    return `(SELECT * FROM read_json_auto('${TEST_FIXTURES_PATH}/${type}-*.jsonl', union_by_name=true, ignore_errors=true))`;
  }
  
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

// Auto-detect best format and return appropriate read expression
export function readDataGlob(type = 'events') {
  // Prefer Parquet (fastest)
  if (hasParquetFiles(type)) {
    return readParquetGlob(type);
  }
  // Fall back to JSONL
  return readJsonlGlob(type);
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

// ============================================================
// VIEW INITIALIZATION
// ============================================================

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
    // Prefer Parquet format when available
    if (eventCount > 0 && eventCount <= MAX_FILES_FOR_VIEWS) {
      await query(`
        CREATE OR REPLACE VIEW all_events AS
        SELECT * FROM ${readDataGlob('events')}
      `);
      const format = hasParquetFiles('events') ? 'Parquet' : 'JSONL';
      console.log(`✅ Created events view (~${eventCount} ${format} files)`);
    } else {
      await query(`CREATE OR REPLACE VIEW all_events AS SELECT NULL as placeholder WHERE false`);
    }
    
    if (updateCount > 0 && updateCount <= MAX_FILES_FOR_VIEWS) {
      await query(`
        CREATE OR REPLACE VIEW all_updates AS
        SELECT * FROM ${readDataGlob('updates')}
      `);
      const format = hasParquetFiles('updates') ? 'Parquet' : 'JSONL';
      console.log(`✅ Created updates view (~${updateCount} ${format} files)`);
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

// Initialize on import
initializeViews();

export { hasFileType, countDataFiles, hasDataFiles, hasParquetFiles, DATA_PATH, ACS_DATA_PATH, IS_TEST, TEST_FIXTURES_PATH };

export default { 
  query, 
  queryParallel,
  queryOne,
  safeQuery, 
  getPoolStats,
  resetPoolMetrics,
  getFileGlob, 
  getParquetGlob, 
  readJsonl, 
  readJsonlFiles, 
  readJsonlGlob, 
  readParquetGlob, 
  readDataGlob, 
  readParquet, 
  findDataFiles, 
  hasFileType, 
  countDataFiles, 
  hasDataFiles, 
  hasParquetFiles, 
  DATA_PATH, 
  ACS_DATA_PATH,
  IS_TEST,
  TEST_FIXTURES_PATH,
};
