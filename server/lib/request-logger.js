/**
 * Request Logger for Express Server
 * 
 * Provides structured JSON logging for API requests, responses, and errors.
 * Integrates with the existing structured-logger pattern for consistency.
 */

const LOG_LEVEL = process.env.LOG_LEVEL || 'info';
const LOG_FORMAT = process.env.LOG_FORMAT || 'json';

const LEVELS = {
  debug: 0,
  info: 1,
  warn: 2,
  error: 3,
};

const currentLevel = LEVELS[LOG_LEVEL] ?? LEVELS.info;

/**
 * Format a log entry
 */
function formatEntry(entry) {
  if (LOG_FORMAT === 'pretty') {
    const { level, message, ...rest } = entry;
    const prefix = level === 'error' ? '❌' :
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
  if (LEVELS[level] < currentLevel) return null;
  
  const entry = {
    ts: new Date().toISOString(),
    level,
    message,
    ...data,
  };
  
  const output = formatEntry(entry);
  
  if (level === 'error') {
    console.error(output);
  } else {
    console.log(output);
  }
  
  return entry;
}

/**
 * Log an API request completion
 */
export function logRequest(req, res, data = {}) {
  return log('info', 'api_request', {
    method: req.method,
    path: req.path,
    query: Object.keys(req.query || {}).length > 0 ? req.query : undefined,
    status: res.statusCode,
    duration_ms: data.durationMs,
    user_agent: req.get('user-agent')?.substring(0, 100),
    ip: req.ip || req.connection?.remoteAddress,
    ...data,
  });
}

/**
 * Log an API error
 */
export function logApiError(req, error, data = {}) {
  return log('error', 'api_error', {
    method: req.method,
    path: req.path,
    query: Object.keys(req.query || {}).length > 0 ? req.query : undefined,
    error_message: error.message,
    error_code: error.code || 'UNKNOWN',
    error_name: error.name,
    stack: error.stack?.split('\n').slice(0, 5).join('\n'),
    ...data,
  });
}

/**
 * Log a slow request warning
 */
export function logSlowRequest(req, durationMs, threshold = 1000) {
  return log('warn', 'slow_request', {
    method: req.method,
    path: req.path,
    duration_ms: durationMs,
    threshold_ms: threshold,
  });
}

/**
 * Log validation failure
 */
export function logValidationError(req, errors) {
  return log('warn', 'validation_error', {
    method: req.method,
    path: req.path,
    errors,
  });
}

/**
 * Log database query
 */
export function logQuery(query, durationMs, rowCount) {
  return log('debug', 'db_query', {
    query: query.substring(0, 500),
    duration_ms: durationMs,
    row_count: rowCount,
  });
}

/**
 * Express middleware for request logging
 * Logs all requests with timing information
 */
export function requestLoggerMiddleware(options = {}) {
  const {
    slowThreshold = 1000,
    skipPaths = ['/health'],
    logBody = false,
  } = options;

  return (req, res, next) => {
    // Skip logging for certain paths
    if (skipPaths.some(path => req.path.startsWith(path))) {
      return next();
    }

    const start = Date.now();
    
    // Store original end function
    const originalEnd = res.end;
    
    res.end = function(...args) {
      const durationMs = Date.now() - start;
      
      // Log the request
      logRequest(req, res, {
        durationMs,
        body: logBody && req.body ? JSON.stringify(req.body).substring(0, 200) : undefined,
      });
      
      // Log slow requests as warnings
      if (durationMs > slowThreshold) {
        logSlowRequest(req, durationMs, slowThreshold);
      }
      
      // Call original end
      return originalEnd.apply(this, args);
    };
    
    next();
  };
}

/**
 * Express error handling middleware with logging
 */
export function errorLoggerMiddleware() {
  return (err, req, res, next) => {
    logApiError(req, err);
    
    // Don't expose internal error details in production
    const isDev = process.env.NODE_ENV === 'development';
    
    res.status(err.status || 500).json({
      error: isDev ? err.message : 'Internal server error',
      ...(isDev && { stack: err.stack }),
    });
  };
}

export default {
  log,
  logRequest,
  logApiError,
  logSlowRequest,
  logValidationError,
  logQuery,
  requestLoggerMiddleware,
  errorLoggerMiddleware,
};
