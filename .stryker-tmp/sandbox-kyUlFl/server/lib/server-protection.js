/**
 * Server Protection Middleware
 * Provides rate limiting, memory monitoring, request timeouts, and error boundaries
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
import rateLimit from 'express-rate-limit';
import helmet from 'helmet';
import { logError } from './crash-logger.js';

// ============= Rate Limiting =============

// General API rate limiter - 100 requests per minute per IP
export const apiLimiter = rateLimit({
  windowMs: stryMutAct_9fa48("291") ? 60 / 1000 : (stryCov_9fa48("291"), 60 * 1000),
  // 1 minute
  max: 100,
  message: {
    error: 'Too many requests, please try again later'
  },
  standardHeaders: stryMutAct_9fa48("294") ? false : (stryCov_9fa48("294"), true),
  legacyHeaders: stryMutAct_9fa48("295") ? true : (stryCov_9fa48("295"), false),
  handler: (req, res, next, options) => {
    if (stryMutAct_9fa48("296")) {
      {}
    } else {
      stryCov_9fa48("296");
      console.warn(`⚠️ Rate limit exceeded for IP: ${req.ip}`);
      res.status(429).json(options.message);
    }
  }
});

// Strict limiter for expensive operations (search, aggregations)
export const expensiveLimiter = rateLimit({
  windowMs: stryMutAct_9fa48("299") ? 60 / 1000 : (stryCov_9fa48("299"), 60 * 1000),
  max: 20,
  // 20 requests per minute
  message: {
    error: 'Too many expensive requests, please slow down'
  },
  standardHeaders: stryMutAct_9fa48("302") ? false : (stryCov_9fa48("302"), true),
  legacyHeaders: stryMutAct_9fa48("303") ? true : (stryCov_9fa48("303"), false)
});

// ============= Security Headers =============

export const securityHeaders = helmet({
  contentSecurityPolicy: stryMutAct_9fa48("305") ? true : (stryCov_9fa48("305"), false),
  // Disable CSP for API-only server
  crossOriginResourcePolicy: {
    policy: 'cross-origin'
  }
});

// ============= Memory Monitoring =============

const MEMORY_CHECK_INTERVAL_MS = 30000; // Check every 30 seconds
const MEMORY_WARN_THRESHOLD = 0.75; // Warn at 75% heap usage
const MEMORY_CRITICAL_THRESHOLD = 0.90; // Critical at 90%

let memoryMonitorInterval = null;
let isMemoryCritical = stryMutAct_9fa48("308") ? true : (stryCov_9fa48("308"), false);
export function startMemoryMonitor() {
  if (stryMutAct_9fa48("309")) {
    {}
  } else {
    stryCov_9fa48("309");
    if (stryMutAct_9fa48("311") ? false : stryMutAct_9fa48("310") ? true : (stryCov_9fa48("310", "311"), memoryMonitorInterval)) return;
    memoryMonitorInterval = setInterval(() => {
      if (stryMutAct_9fa48("312")) {
        {}
      } else {
        stryCov_9fa48("312");
        const usage = process.memoryUsage();
        const heapUsedMB = Math.round(stryMutAct_9fa48("313") ? usage.heapUsed / 1024 * 1024 : (stryCov_9fa48("313"), (stryMutAct_9fa48("314") ? usage.heapUsed * 1024 : (stryCov_9fa48("314"), usage.heapUsed / 1024)) / 1024));
        const heapTotalMB = Math.round(stryMutAct_9fa48("315") ? usage.heapTotal / 1024 * 1024 : (stryCov_9fa48("315"), (stryMutAct_9fa48("316") ? usage.heapTotal * 1024 : (stryCov_9fa48("316"), usage.heapTotal / 1024)) / 1024));
        const heapPercent = stryMutAct_9fa48("317") ? usage.heapUsed * usage.heapTotal : (stryCov_9fa48("317"), usage.heapUsed / usage.heapTotal);
        if (stryMutAct_9fa48("321") ? heapPercent < MEMORY_CRITICAL_THRESHOLD : stryMutAct_9fa48("320") ? heapPercent > MEMORY_CRITICAL_THRESHOLD : stryMutAct_9fa48("319") ? false : stryMutAct_9fa48("318") ? true : (stryCov_9fa48("318", "319", "320", "321"), heapPercent >= MEMORY_CRITICAL_THRESHOLD)) {
          if (stryMutAct_9fa48("322")) {
            {}
          } else {
            stryCov_9fa48("322");
            if (stryMutAct_9fa48("325") ? false : stryMutAct_9fa48("324") ? true : stryMutAct_9fa48("323") ? isMemoryCritical : (stryCov_9fa48("323", "324", "325"), !isMemoryCritical)) {
              if (stryMutAct_9fa48("326")) {
                {}
              } else {
                stryCov_9fa48("326");
                console.error(`🚨 CRITICAL: Memory at ${(stryMutAct_9fa48("328") ? heapPercent / 100 : (stryCov_9fa48("328"), heapPercent * 100)).toFixed(1)}% (${heapUsedMB}/${heapTotalMB}MB)`);
                logError(new Error(`Critical memory pressure: ${stryMutAct_9fa48("330") ? heapPercent / 100 : (stryCov_9fa48("330"), heapPercent * 100)}%`), 'MEMORY_CRITICAL');
                isMemoryCritical = stryMutAct_9fa48("332") ? false : (stryCov_9fa48("332"), true);

                // Attempt garbage collection if available
                if (stryMutAct_9fa48("334") ? false : stryMutAct_9fa48("333") ? true : (stryCov_9fa48("333", "334"), global.gc)) {
                  if (stryMutAct_9fa48("335")) {
                    {}
                  } else {
                    stryCov_9fa48("335");
                    console.log('🧹 Forcing garbage collection...');
                    global.gc();
                  }
                }
              }
            }
          }
        } else if (stryMutAct_9fa48("340") ? heapPercent < MEMORY_WARN_THRESHOLD : stryMutAct_9fa48("339") ? heapPercent > MEMORY_WARN_THRESHOLD : stryMutAct_9fa48("338") ? false : stryMutAct_9fa48("337") ? true : (stryCov_9fa48("337", "338", "339", "340"), heapPercent >= MEMORY_WARN_THRESHOLD)) {
          if (stryMutAct_9fa48("341")) {
            {}
          } else {
            stryCov_9fa48("341");
            console.warn(`⚠️ Memory warning: ${(stryMutAct_9fa48("343") ? heapPercent / 100 : (stryCov_9fa48("343"), heapPercent * 100)).toFixed(1)}% (${heapUsedMB}/${heapTotalMB}MB)`);
            isMemoryCritical = stryMutAct_9fa48("344") ? true : (stryCov_9fa48("344"), false);
          }
        } else {
          if (stryMutAct_9fa48("345")) {
            {}
          } else {
            stryCov_9fa48("345");
            isMemoryCritical = stryMutAct_9fa48("346") ? true : (stryCov_9fa48("346"), false);
          }
        }
      }
    }, MEMORY_CHECK_INTERVAL_MS);
    console.log('📊 Memory monitor started (checks every 30s)');
  }
}
export function stopMemoryMonitor() {
  if (stryMutAct_9fa48("348")) {
    {}
  } else {
    stryCov_9fa48("348");
    if (stryMutAct_9fa48("350") ? false : stryMutAct_9fa48("349") ? true : (stryCov_9fa48("349", "350"), memoryMonitorInterval)) {
      if (stryMutAct_9fa48("351")) {
        {}
      } else {
        stryCov_9fa48("351");
        clearInterval(memoryMonitorInterval);
        memoryMonitorInterval = null;
      }
    }
  }
}
export function getMemoryStatus() {
  if (stryMutAct_9fa48("352")) {
    {}
  } else {
    stryCov_9fa48("352");
    const usage = process.memoryUsage();
    return {
      heapUsedMB: Math.round(stryMutAct_9fa48("354") ? usage.heapUsed / 1024 * 1024 : (stryCov_9fa48("354"), (stryMutAct_9fa48("355") ? usage.heapUsed * 1024 : (stryCov_9fa48("355"), usage.heapUsed / 1024)) / 1024)),
      heapTotalMB: Math.round(stryMutAct_9fa48("356") ? usage.heapTotal / 1024 * 1024 : (stryCov_9fa48("356"), (stryMutAct_9fa48("357") ? usage.heapTotal * 1024 : (stryCov_9fa48("357"), usage.heapTotal / 1024)) / 1024)),
      heapPercent: (stryMutAct_9fa48("358") ? usage.heapUsed / usage.heapTotal / 100 : (stryCov_9fa48("358"), (stryMutAct_9fa48("359") ? usage.heapUsed * usage.heapTotal : (stryCov_9fa48("359"), usage.heapUsed / usage.heapTotal)) * 100)).toFixed(1),
      rssMB: Math.round(stryMutAct_9fa48("360") ? usage.rss / 1024 * 1024 : (stryCov_9fa48("360"), (stryMutAct_9fa48("361") ? usage.rss * 1024 : (stryCov_9fa48("361"), usage.rss / 1024)) / 1024)),
      externalMB: Math.round(stryMutAct_9fa48("362") ? usage.external / 1024 * 1024 : (stryCov_9fa48("362"), (stryMutAct_9fa48("363") ? usage.external * 1024 : (stryCov_9fa48("363"), usage.external / 1024)) / 1024)),
      isCritical: isMemoryCritical
    };
  }
}

// Memory check middleware - reject requests when memory is critical
export function memoryGuard(req, res, next) {
  if (stryMutAct_9fa48("364")) {
    {}
  } else {
    stryCov_9fa48("364");
    if (stryMutAct_9fa48("366") ? false : stryMutAct_9fa48("365") ? true : (stryCov_9fa48("365", "366"), isMemoryCritical)) {
      if (stryMutAct_9fa48("367")) {
        {}
      } else {
        stryCov_9fa48("367");
        console.warn(`⚠️ Rejecting request due to memory pressure: ${req.path}`);
        return res.status(503).json({
          error: 'Server under memory pressure, please retry shortly',
          retryAfter: 30
        });
      }
    }
    next();
  }
}

// ============= Request Timeout =============

const DEFAULT_TIMEOUT_MS = 30000; // 30 seconds

export function requestTimeout(timeoutMs = DEFAULT_TIMEOUT_MS) {
  if (stryMutAct_9fa48("371")) {
    {}
  } else {
    stryCov_9fa48("371");
    return (req, res, next) => {
      if (stryMutAct_9fa48("372")) {
        {}
      } else {
        stryCov_9fa48("372");
        const timer = setTimeout(() => {
          if (stryMutAct_9fa48("373")) {
            {}
          } else {
            stryCov_9fa48("373");
            if (stryMutAct_9fa48("376") ? false : stryMutAct_9fa48("375") ? true : stryMutAct_9fa48("374") ? res.headersSent : (stryCov_9fa48("374", "375", "376"), !res.headersSent)) {
              if (stryMutAct_9fa48("377")) {
                {}
              } else {
                stryCov_9fa48("377");
                console.warn(`⏱️ Request timeout: ${req.method} ${req.path}`);
                res.status(408).json({
                  error: 'Request timeout'
                });
              }
            }
          }
        }, timeoutMs);

        // Clear timeout when response finishes
        res.on('finish', stryMutAct_9fa48("382") ? () => undefined : (stryCov_9fa48("382"), () => clearTimeout(timer)));
        res.on('close', stryMutAct_9fa48("384") ? () => undefined : (stryCov_9fa48("384"), () => clearTimeout(timer)));
        next();
      }
    };
  }
}

// ============= Async Route Wrapper =============

// Wraps async route handlers to catch unhandled promise rejections
export function asyncHandler(fn) {
  if (stryMutAct_9fa48("385")) {
    {}
  } else {
    stryCov_9fa48("385");
    return (req, res, next) => {
      if (stryMutAct_9fa48("386")) {
        {}
      } else {
        stryCov_9fa48("386");
        Promise.resolve(fn(req, res, next)).catch(err => {
          if (stryMutAct_9fa48("387")) {
            {}
          } else {
            stryCov_9fa48("387");
            console.error(`❌ Async route error: ${req.method} ${req.path}`, err.message);
            logError(err, `ROUTE_ERROR: ${req.method} ${req.path}`);
            if (stryMutAct_9fa48("392") ? false : stryMutAct_9fa48("391") ? true : stryMutAct_9fa48("390") ? res.headersSent : (stryCov_9fa48("390", "391", "392"), !res.headersSent)) {
              if (stryMutAct_9fa48("393")) {
                {}
              } else {
                stryCov_9fa48("393");
                res.status(500).json({
                  error: 'Internal server error'
                });
              }
            }
          }
        });
      }
    };
  }
}

// ============= Global Error Handler =============

export function globalErrorHandler(err, req, res, next) {
  if (stryMutAct_9fa48("396")) {
    {}
  } else {
    stryCov_9fa48("396");
    console.error(`❌ Express error: ${err.message}`);
    logError(err, `EXPRESS_ERROR: ${req.method} ${req.path}`);

    // Don't expose internal errors to clients
    if (stryMutAct_9fa48("401") ? false : stryMutAct_9fa48("400") ? true : stryMutAct_9fa48("399") ? res.headersSent : (stryCov_9fa48("399", "400", "401"), !res.headersSent)) {
      if (stryMutAct_9fa48("402")) {
        {}
      } else {
        stryCov_9fa48("402");
        res.status(stryMutAct_9fa48("405") ? err.status && 500 : stryMutAct_9fa48("404") ? false : stryMutAct_9fa48("403") ? true : (stryCov_9fa48("403", "404", "405"), err.status || 500)).json({
          error: (stryMutAct_9fa48("409") ? process.env.NODE_ENV !== 'production' : stryMutAct_9fa48("408") ? false : stryMutAct_9fa48("407") ? true : (stryCov_9fa48("407", "408", "409"), process.env.NODE_ENV === 'production')) ? 'Internal server error' : err.message
        });
      }
    }
  }
}
export default {
  apiLimiter,
  expensiveLimiter,
  securityHeaders,
  startMemoryMonitor,
  stopMemoryMonitor,
  getMemoryStatus,
  memoryGuard,
  requestTimeout,
  asyncHandler,
  globalErrorHandler
};