/**
 * Request Logger for Express Server
 * 
 * Provides structured JSON logging for API requests, responses, and errors.
 * Integrates with the existing structured-logger pattern for consistency.
 */
// @ts-nocheck
function stryNS_9fa48() {
  var g = typeof globalThis === 'object' && globalThis && globalThis.Math === Math && globalThis || new Function("return this")();
  var ns = g.__stryker__ || (g.__stryker__ = {});
  if (ns.activeMutant === undefined && g.process && g.process.env && g.process.env.__STRYKER_ACTIVE_MUTANT__) {
    ns.activeMutant = g.process.env.__STRYKER_ACTIVE_MUTANT__;
  }
  function retrieveNS() {
    return ns;
  }
  stryNS_9fa48 = retrieveNS;
  return retrieveNS();
}
stryNS_9fa48();
function stryCov_9fa48() {
  var ns = stryNS_9fa48();
  var cov = ns.mutantCoverage || (ns.mutantCoverage = {
    static: {},
    perTest: {}
  });
  function cover() {
    var c = cov.static;
    if (ns.currentTestId) {
      c = cov.perTest[ns.currentTestId] = cov.perTest[ns.currentTestId] || {};
    }
    var a = arguments;
    for (var i = 0; i < a.length; i++) {
      c[a[i]] = (c[a[i]] || 0) + 1;
    }
  }
  stryCov_9fa48 = cover;
  cover.apply(null, arguments);
}
function stryMutAct_9fa48(id) {
  var ns = stryNS_9fa48();
  function isActive(id) {
    if (ns.activeMutant === id) {
      if (ns.hitCount !== void 0 && ++ns.hitCount > ns.hitLimit) {
        throw new Error('Stryker: Hit count limit reached (' + ns.hitCount + ')');
      }
      return true;
    }
    return false;
  }
  stryMutAct_9fa48 = isActive;
  return isActive(id);
}
const LOG_LEVEL = stryMutAct_9fa48("151") ? process.env.LOG_LEVEL && 'info' : stryMutAct_9fa48("150") ? false : stryMutAct_9fa48("149") ? true : (stryCov_9fa48("149", "150", "151"), process.env.LOG_LEVEL || 'info');
const LOG_FORMAT = stryMutAct_9fa48("155") ? process.env.LOG_FORMAT && 'json' : stryMutAct_9fa48("154") ? false : stryMutAct_9fa48("153") ? true : (stryCov_9fa48("153", "154", "155"), process.env.LOG_FORMAT || 'json');
const LEVELS = {
  debug: 0,
  info: 1,
  warn: 2,
  error: 3
};
const currentLevel = stryMutAct_9fa48("158") ? LEVELS[LOG_LEVEL] && LEVELS.info : (stryCov_9fa48("158"), LEVELS[LOG_LEVEL] ?? LEVELS.info);

/**
 * Format a log entry
 */
function formatEntry(entry) {
  if (stryMutAct_9fa48("159")) {
    {}
  } else {
    stryCov_9fa48("159");
    if (stryMutAct_9fa48("162") ? LOG_FORMAT !== 'pretty' : stryMutAct_9fa48("161") ? false : stryMutAct_9fa48("160") ? true : (stryCov_9fa48("160", "161", "162"), LOG_FORMAT === 'pretty')) {
      if (stryMutAct_9fa48("164")) {
        {}
      } else {
        stryCov_9fa48("164");
        const {
          level,
          message,
          ...rest
        } = entry;
        const prefix = (stryMutAct_9fa48("167") ? level !== 'error' : stryMutAct_9fa48("166") ? false : stryMutAct_9fa48("165") ? true : (stryCov_9fa48("165", "166", "167"), level === 'error')) ? '❌' : (stryMutAct_9fa48("172") ? level !== 'warn' : stryMutAct_9fa48("171") ? false : stryMutAct_9fa48("170") ? true : (stryCov_9fa48("170", "171", "172"), level === 'warn')) ? '⚠️' : (stryMutAct_9fa48("177") ? level !== 'debug' : stryMutAct_9fa48("176") ? false : stryMutAct_9fa48("175") ? true : (stryCov_9fa48("175", "176", "177"), level === 'debug')) ? '🔍' : '📋';
        const extra = (stryMutAct_9fa48("184") ? Object.keys(rest).length <= 0 : stryMutAct_9fa48("183") ? Object.keys(rest).length >= 0 : stryMutAct_9fa48("182") ? false : stryMutAct_9fa48("181") ? true : (stryCov_9fa48("181", "182", "183", "184"), Object.keys(rest).length > 0)) ? ` ${JSON.stringify(rest)}` : '';
        return `${prefix} [${stryMutAct_9fa48("188") ? level.toLowerCase() : (stryCov_9fa48("188"), level.toUpperCase())}] ${message}${extra}`;
      }
    }
    return JSON.stringify(entry);
  }
}

/**
 * Core log function
 */
export function log(level, message, data = {}) {
  if (stryMutAct_9fa48("189")) {
    {}
  } else {
    stryCov_9fa48("189");
    if (stryMutAct_9fa48("193") ? LEVELS[level] >= currentLevel : stryMutAct_9fa48("192") ? LEVELS[level] <= currentLevel : stryMutAct_9fa48("191") ? false : stryMutAct_9fa48("190") ? true : (stryCov_9fa48("190", "191", "192", "193"), LEVELS[level] < currentLevel)) return null;
    const entry = {
      ts: new Date().toISOString(),
      level,
      message,
      ...data
    };
    const output = formatEntry(entry);
    if (stryMutAct_9fa48("197") ? level !== 'error' : stryMutAct_9fa48("196") ? false : stryMutAct_9fa48("195") ? true : (stryCov_9fa48("195", "196", "197"), level === 'error')) {
      if (stryMutAct_9fa48("199")) {
        {}
      } else {
        stryCov_9fa48("199");
        console.error(output);
      }
    } else {
      if (stryMutAct_9fa48("200")) {
        {}
      } else {
        stryCov_9fa48("200");
        console.log(output);
      }
    }
    return entry;
  }
}

/**
 * Log an API request completion
 */
export function logRequest(req, res, data = {}) {
  if (stryMutAct_9fa48("201")) {
    {}
  } else {
    stryCov_9fa48("201");
    return log('info', 'api_request', {
      method: req.method,
      path: req.path,
      query: (stryMutAct_9fa48("208") ? Object.keys(req.query || {}).length <= 0 : stryMutAct_9fa48("207") ? Object.keys(req.query || {}).length >= 0 : stryMutAct_9fa48("206") ? false : stryMutAct_9fa48("205") ? true : (stryCov_9fa48("205", "206", "207", "208"), Object.keys(stryMutAct_9fa48("211") ? req.query && {} : stryMutAct_9fa48("210") ? false : stryMutAct_9fa48("209") ? true : (stryCov_9fa48("209", "210", "211"), req.query || {})).length > 0)) ? req.query : undefined,
      status: res.statusCode,
      duration_ms: data.durationMs,
      user_agent: stryMutAct_9fa48("213") ? req.get('user-agent').substring(0, 100) : stryMutAct_9fa48("212") ? req.get('user-agent') : (stryCov_9fa48("212", "213"), req.get('user-agent')?.substring(0, 100)),
      ip: stryMutAct_9fa48("217") ? req.ip && req.connection?.remoteAddress : stryMutAct_9fa48("216") ? false : stryMutAct_9fa48("215") ? true : (stryCov_9fa48("215", "216", "217"), req.ip || (stryMutAct_9fa48("218") ? req.connection.remoteAddress : (stryCov_9fa48("218"), req.connection?.remoteAddress))),
      ...data
    });
  }
}

/**
 * Log an API error
 */
export function logApiError(req, error, data = {}) {
  if (stryMutAct_9fa48("219")) {
    {}
  } else {
    stryCov_9fa48("219");
    return log('error', 'api_error', {
      method: req.method,
      path: req.path,
      query: (stryMutAct_9fa48("226") ? Object.keys(req.query || {}).length <= 0 : stryMutAct_9fa48("225") ? Object.keys(req.query || {}).length >= 0 : stryMutAct_9fa48("224") ? false : stryMutAct_9fa48("223") ? true : (stryCov_9fa48("223", "224", "225", "226"), Object.keys(stryMutAct_9fa48("229") ? req.query && {} : stryMutAct_9fa48("228") ? false : stryMutAct_9fa48("227") ? true : (stryCov_9fa48("227", "228", "229"), req.query || {})).length > 0)) ? req.query : undefined,
      error_message: error.message,
      error_code: stryMutAct_9fa48("232") ? error.code && 'UNKNOWN' : stryMutAct_9fa48("231") ? false : stryMutAct_9fa48("230") ? true : (stryCov_9fa48("230", "231", "232"), error.code || 'UNKNOWN'),
      error_name: error.name,
      stack: stryMutAct_9fa48("235") ? error.stack.split('\n').slice(0, 5).join('\n') : stryMutAct_9fa48("234") ? error.stack?.split('\n').join('\n') : (stryCov_9fa48("234", "235"), error.stack?.split('\n').slice(0, 5).join('\n')),
      ...data
    });
  }
}

/**
 * Log a slow request warning
 */
export function logSlowRequest(req, durationMs, threshold = 1000) {
  if (stryMutAct_9fa48("238")) {
    {}
  } else {
    stryCov_9fa48("238");
    return log('warn', 'slow_request', {
      method: req.method,
      path: req.path,
      duration_ms: durationMs,
      threshold_ms: threshold
    });
  }
}

/**
 * Log validation failure
 */
export function logValidationError(req, errors) {
  if (stryMutAct_9fa48("242")) {
    {}
  } else {
    stryCov_9fa48("242");
    return log('warn', 'validation_error', {
      method: req.method,
      path: req.path,
      errors
    });
  }
}

/**
 * Log database query
 */
export function logQuery(query, durationMs, rowCount) {
  if (stryMutAct_9fa48("246")) {
    {}
  } else {
    stryCov_9fa48("246");
    return log('debug', 'db_query', {
      query: stryMutAct_9fa48("250") ? query : (stryCov_9fa48("250"), query.substring(0, 500)),
      duration_ms: durationMs,
      row_count: rowCount
    });
  }
}

/**
 * Express middleware for request logging
 * Logs all requests with timing information
 */
export function requestLoggerMiddleware(options = {}) {
  if (stryMutAct_9fa48("251")) {
    {}
  } else {
    stryCov_9fa48("251");
    const {
      slowThreshold = 1000,
      skipPaths = stryMutAct_9fa48("252") ? [] : (stryCov_9fa48("252"), ['/health']),
      logBody = stryMutAct_9fa48("254") ? true : (stryCov_9fa48("254"), false)
    } = options;
    return (req, res, next) => {
      if (stryMutAct_9fa48("255")) {
        {}
      } else {
        stryCov_9fa48("255");
        // Skip logging for certain paths
        if (stryMutAct_9fa48("258") ? skipPaths.every(path => req.path.startsWith(path)) : stryMutAct_9fa48("257") ? false : stryMutAct_9fa48("256") ? true : (stryCov_9fa48("256", "257", "258"), skipPaths.some(stryMutAct_9fa48("259") ? () => undefined : (stryCov_9fa48("259"), path => stryMutAct_9fa48("260") ? req.path.endsWith(path) : (stryCov_9fa48("260"), req.path.startsWith(path)))))) {
          if (stryMutAct_9fa48("261")) {
            {}
          } else {
            stryCov_9fa48("261");
            return next();
          }
        }
        const start = Date.now();

        // Store original end function
        const originalEnd = res.end;
        res.end = function (...args) {
          if (stryMutAct_9fa48("262")) {
            {}
          } else {
            stryCov_9fa48("262");
            const durationMs = stryMutAct_9fa48("263") ? Date.now() + start : (stryCov_9fa48("263"), Date.now() - start);

            // Log the request
            logRequest(req, res, {
              durationMs,
              body: (stryMutAct_9fa48("267") ? logBody || req.body : stryMutAct_9fa48("266") ? false : stryMutAct_9fa48("265") ? true : (stryCov_9fa48("265", "266", "267"), logBody && req.body)) ? stryMutAct_9fa48("268") ? JSON.stringify(req.body) : (stryCov_9fa48("268"), JSON.stringify(req.body).substring(0, 200)) : undefined
            });

            // Log slow requests as warnings
            if (stryMutAct_9fa48("272") ? durationMs <= slowThreshold : stryMutAct_9fa48("271") ? durationMs >= slowThreshold : stryMutAct_9fa48("270") ? false : stryMutAct_9fa48("269") ? true : (stryCov_9fa48("269", "270", "271", "272"), durationMs > slowThreshold)) {
              if (stryMutAct_9fa48("273")) {
                {}
              } else {
                stryCov_9fa48("273");
                logSlowRequest(req, durationMs, slowThreshold);
              }
            }

            // Call original end
            return originalEnd.apply(this, args);
          }
        };
        next();
      }
    };
  }
}

/**
 * Express error handling middleware with logging
 */
export function errorLoggerMiddleware() {
  if (stryMutAct_9fa48("274")) {
    {}
  } else {
    stryCov_9fa48("274");
    return (err, req, res, next) => {
      if (stryMutAct_9fa48("275")) {
        {}
      } else {
        stryCov_9fa48("275");
        logApiError(req, err);

        // Don't expose internal error details in production
        const isDev = stryMutAct_9fa48("278") ? process.env.NODE_ENV !== 'development' : stryMutAct_9fa48("277") ? false : stryMutAct_9fa48("276") ? true : (stryCov_9fa48("276", "277", "278"), process.env.NODE_ENV === 'development');
        res.status(stryMutAct_9fa48("282") ? err.status && 500 : stryMutAct_9fa48("281") ? false : stryMutAct_9fa48("280") ? true : (stryCov_9fa48("280", "281", "282"), err.status || 500)).json({
          error: isDev ? err.message : 'Internal server error',
          ...(stryMutAct_9fa48("287") ? isDev || {
            stack: err.stack
          } : stryMutAct_9fa48("286") ? false : stryMutAct_9fa48("285") ? true : (stryCov_9fa48("285", "286", "287"), isDev && {
            stack: err.stack
          }))
        });
      }
    };
  }
}
export default {
  log,
  logRequest,
  logApiError,
  logSlowRequest,
  logValidationError,
  logQuery,
  requestLoggerMiddleware,
  errorLoggerMiddleware
};