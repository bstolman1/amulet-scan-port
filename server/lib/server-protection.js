/**
 * Server Protection Middleware
 * Provides rate limiting, memory monitoring, request timeouts, and error boundaries
 */
import rateLimit from 'express-rate-limit';
import helmet from 'helmet';
import { logError } from './crash-logger.js';

// ============= Rate Limiting =============

// General API rate limiter - 100 requests per minute per IP
export const apiLimiter = rateLimit({
  windowMs: 60 * 1000, // 1 minute
  max: 100,
  message: { error: 'Too many requests, please try again later' },
  standardHeaders: true,
  legacyHeaders: false,
  handler: (req, res, next, options) => {
    console.warn(`⚠️ Rate limit exceeded for IP: ${req.ip}`);
    res.status(429).json(options.message);
  },
});

// Strict limiter for expensive operations (search, aggregations)
export const expensiveLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 20, // 20 requests per minute
  message: { error: 'Too many expensive requests, please slow down' },
  standardHeaders: true,
  legacyHeaders: false,
});

// ============= Security Headers =============

export const securityHeaders = helmet({
  contentSecurityPolicy: false, // Disable CSP for API-only server
  crossOriginResourcePolicy: { policy: 'cross-origin' },
});

// ============= Memory Monitoring =============

const MEMORY_CHECK_INTERVAL_MS = 30000; // Check every 30 seconds
const MEMORY_WARN_THRESHOLD = 0.75; // Warn at 75% heap usage
const MEMORY_CRITICAL_THRESHOLD = 0.90; // Critical at 90%

let memoryMonitorInterval = null;
let isMemoryCritical = false;

export function startMemoryMonitor() {
  if (memoryMonitorInterval) return;
  
  memoryMonitorInterval = setInterval(() => {
    const usage = process.memoryUsage();
    const heapUsedMB = Math.round(usage.heapUsed / 1024 / 1024);
    const heapTotalMB = Math.round(usage.heapTotal / 1024 / 1024);
    const heapPercent = usage.heapUsed / usage.heapTotal;
    
    if (heapPercent >= MEMORY_CRITICAL_THRESHOLD) {
      if (!isMemoryCritical) {
        console.error(`🚨 CRITICAL: Memory at ${(heapPercent * 100).toFixed(1)}% (${heapUsedMB}/${heapTotalMB}MB)`);
        logError(new Error(`Critical memory pressure: ${heapPercent * 100}%`), 'MEMORY_CRITICAL');
        isMemoryCritical = true;
        
        // Attempt garbage collection if available
        if (global.gc) {
          console.log('🧹 Forcing garbage collection...');
          global.gc();
        }
      }
    } else if (heapPercent >= MEMORY_WARN_THRESHOLD) {
      console.warn(`⚠️ Memory warning: ${(heapPercent * 100).toFixed(1)}% (${heapUsedMB}/${heapTotalMB}MB)`);
      isMemoryCritical = false;
    } else {
      isMemoryCritical = false;
    }
  }, MEMORY_CHECK_INTERVAL_MS);
  
  console.log('📊 Memory monitor started (checks every 30s)');
}

export function stopMemoryMonitor() {
  if (memoryMonitorInterval) {
    clearInterval(memoryMonitorInterval);
    memoryMonitorInterval = null;
  }
}

export function getMemoryStatus() {
  const usage = process.memoryUsage();
  return {
    heapUsedMB: Math.round(usage.heapUsed / 1024 / 1024),
    heapTotalMB: Math.round(usage.heapTotal / 1024 / 1024),
    heapPercent: (usage.heapUsed / usage.heapTotal * 100).toFixed(1),
    rssMB: Math.round(usage.rss / 1024 / 1024),
    externalMB: Math.round(usage.external / 1024 / 1024),
    isCritical: isMemoryCritical,
  };
}

// Memory check middleware - reject requests when memory is critical
export function memoryGuard(req, res, next) {
  if (isMemoryCritical) {
    console.warn(`⚠️ Rejecting request due to memory pressure: ${req.path}`);
    return res.status(503).json({ 
      error: 'Server under memory pressure, please retry shortly',
      retryAfter: 30,
    });
  }
  next();
}

// ============= Request Timeout =============

const DEFAULT_TIMEOUT_MS = 30000; // 30 seconds

export function requestTimeout(timeoutMs = DEFAULT_TIMEOUT_MS) {
  return (req, res, next) => {
    const timer = setTimeout(() => {
      if (!res.headersSent) {
        console.warn(`⏱️ Request timeout: ${req.method} ${req.path}`);
        res.status(408).json({ error: 'Request timeout' });
      }
    }, timeoutMs);
    
    // Clear timeout when response finishes
    res.on('finish', () => clearTimeout(timer));
    res.on('close', () => clearTimeout(timer));
    
    next();
  };
}

// ============= Async Route Wrapper =============

// Wraps async route handlers to catch unhandled promise rejections
export function asyncHandler(fn) {
  return (req, res, next) => {
    Promise.resolve(fn(req, res, next)).catch((err) => {
      console.error(`❌ Async route error: ${req.method} ${req.path}`, err.message);
      logError(err, `ROUTE_ERROR: ${req.method} ${req.path}`);
      
      if (!res.headersSent) {
        res.status(500).json({ error: 'Internal server error' });
      }
    });
  };
}

// ============= Global Error Handler =============

export function globalErrorHandler(err, req, res, next) {
  console.error(`❌ Express error: ${err.message}`);
  logError(err, `EXPRESS_ERROR: ${req.method} ${req.path}`);
  
  // Don't expose internal errors to clients
  if (!res.headersSent) {
    res.status(err.status || 500).json({
      error: process.env.NODE_ENV === 'production' 
        ? 'Internal server error' 
        : err.message,
    });
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
  globalErrorHandler,
};
