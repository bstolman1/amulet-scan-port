/**
 * Structured JSON Logger for Backfill Operations
 * 
 * Provides machine-readable logs for debugging long runs.
 * All logs are valid JSON for easy parsing and analysis.
 * 
 * Usage:
 *   import { log, logBatch, logCursor, logError, logMetrics } from './structured-logger.js';
 *   log('info', 'Starting backfill', { migration: 1 });
 */

const LOG_LEVEL = process.env.LOG_LEVEL || 'info';
const LOG_FORMAT = process.env.LOG_FORMAT || 'json'; // 'json' or 'pretty'

const LEVELS = {
  debug: 0,
  info: 1,
  warn: 2,
  error: 3,
  fatal: 4,
};

const currentLevel = LEVELS[LOG_LEVEL] ?? LEVELS.info;

/**
 * Format a log entry
 */
function formatEntry(entry) {
  if (LOG_FORMAT === 'pretty') {
    const { level, message, ...rest } = entry;
    const prefix = level === 'error' || level === 'fatal' ? '❌' :
                   level === 'warn' ? '⚠️' :
                   level === 'debug' ? '🔍' : '📋';
    const extra = Object.keys(rest).length > 0 ? ` ${JSON.stringify(rest)}` : '';
    return `${prefix} [${level.toUpperCase()}] ${message}${extra}`;
  }
  return JSON.stringify(entry);
}

/**
 * Core log function
 */
export function log(level, message, data = {}) {
  if (LEVELS[level] < currentLevel) return;
  
  const entry = {
    ts: new Date().toISOString(),
    level,
    message,
    ...data,
  };
  
  const output = formatEntry(entry);
  
  if (level === 'error' || level === 'fatal') {
    console.error(output);
  } else {
    console.log(output);
  }
  
  return entry;
}

/**
 * Log batch processing event
 */
export function logBatch(data) {
  return log('info', 'batch_processed', {
    migration: data.migrationId,
    synchronizer: data.synchronizerId?.substring(0, 30),
    shard: data.shardIndex ?? null,
    batch: data.batchCount,
    updates: data.updates,
    events: data.events,
    total_updates: data.totalUpdates,
    total_events: data.totalEvents,
    cursor_before: data.cursorBefore,
    cursor_after: data.cursorAfter,
    throughput: data.throughput,
    latency_ms: data.latencyMs,
    parallel_fetches: data.parallelFetches,
    decode_workers: data.decodeWorkers,
    queued_jobs: data.queuedJobs,
    active_workers: data.activeWorkers,
  });
}

/**
 * Log cursor state change
 */
export function logCursor(action, data) {
  return log('info', `cursor_${action}`, {
    migration: data.migrationId,
    synchronizer: data.synchronizerId?.substring(0, 30),
    shard: data.shardIndex ?? null,
    last_before: data.lastBefore,
    total_updates: data.totalUpdates,
    total_events: data.totalEvents,
    complete: data.complete ?? false,
    pending_writes: data.pendingWrites ?? 0,
  });
}

/**
 * Log error event
 */
export function logError(context, error, data = {}) {
  return log('error', `error_${context}`, {
    error_code: error.code || error.response?.status || 'UNKNOWN',
    error_message: error.message,
    ...data,
  });
}

/**
 * Log fatal error (non-recoverable)
 */
export function logFatal(context, error, data = {}) {
  return log('fatal', `fatal_${context}`, {
    error_code: error.code || error.response?.status || 'UNKNOWN',
    error_message: error.message,
    stack: error.stack?.split('\n').slice(0, 5).join('\n'),
    ...data,
  });
}

/**
 * Log auto-tuning event
 */
export function logTune(component, action, data) {
  return log('info', `tune_${component}`, {
    action, // 'up', 'down', 'stable'
    ...data,
  });
}

/**
 * Log metrics snapshot
 */
export function logMetrics(data) {
  return log('info', 'metrics', {
    migration: data.migrationId,
    shard: data.shardIndex ?? null,
    elapsed_s: data.elapsedSeconds,
    total_updates: data.totalUpdates,
    total_events: data.totalEvents,
    avg_throughput: data.avgThroughput,
    current_throughput: data.currentThroughput,
    parallel_fetches: data.parallelFetches,
    decode_workers: data.decodeWorkers,
    avg_latency_ms: data.avgLatencyMs,
    p95_latency_ms: data.p95LatencyMs,
    error_count: data.errorCount,
    retry_count: data.retryCount,
    memory_mb: Math.round(process.memoryUsage().heapUsed / 1024 / 1024),
  });
}

/**
 * Log migration start/complete
 */
export function logMigration(action, data) {
  return log('info', `migration_${action}`, {
    migration: data.migrationId,
    shard: data.shardIndex ?? null,
    synchronizer_count: data.synchronizerCount,
    min_time: data.minTime,
    max_time: data.maxTime,
    ...data.extra,
  });
}

/**
 * Log synchronizer start/complete
 */
export function logSynchronizer(action, data) {
  return log('info', `synchronizer_${action}`, {
    migration: data.migrationId,
    synchronizer: data.synchronizerId?.substring(0, 30),
    shard: data.shardIndex ?? null,
    min_time: data.minTime,
    max_time: data.maxTime,
    total_updates: data.totalUpdates,
    total_events: data.totalEvents,
    elapsed_s: data.elapsedSeconds,
    ...data.extra,
  });
}

/**
 * Log fetch result (success, empty, or failure)
 */
export function logFetch(result, data) {
  return log('debug', `fetch_${result}`, {
    migration: data.migrationId,
    synchronizer: data.synchronizerId?.substring(0, 30),
    before: data.before,
    transactions: data.transactions ?? 0,
    latency_ms: data.latencyMs,
    attempt: data.attempt,
  });
}

/**
 * Log run summary (at end of backfill)
 */
export function logSummary(data) {
  return log('info', 'run_summary', {
    success: data.success,
    total_updates: data.totalUpdates,
    total_events: data.totalEvents,
    total_time_s: data.totalTimeSeconds,
    avg_throughput: data.avgThroughput,
    migrations_processed: data.migrationsProcessed,
    all_complete: data.allComplete,
    pending_count: data.pendingCount,
  });
}

export default {
  log,
  logBatch,
  logCursor,
  logError,
  logFatal,
  logTune,
  logMetrics,
  logMigration,
  logSynchronizer,
  logFetch,
  logSummary,
};
