// MUST be first - loads .env before other modules capture process.env
import './env.js';

// Install crash handlers early to catch startup errors
import { installCrashHandlers, LOG_PATHS } from './lib/crash-logger.js';
installCrashHandlers();

// 🔴 CRITICAL: Import app FIRST - it has trust proxy set before rate limiters load
import app from './app.js';

// NOW import everything else (rate limiters will see trust proxy already set)
import cors from 'cors';
import express from 'express';
import eventsRouter from './api/events.js';
import updatesRouter from './api/updates.js';
import partyRouter from './api/party.js';
import contractsRouter from './api/contracts.js';
import statsRouter from './api/stats.js';
import searchRouter from './api/search.js';
import backfillRouter from './api/backfill.js';
import acsRouter from './api/acs.js';
import announcementsRouter from './api/announcements.js';
import governanceLifecycleRouter from './api/governance-lifecycle.js';
import kaikoRouter from './api/kaiko.js';
import rewardsRouter from './api/rewards.js';
import scanProxyRouter from './api/scan-proxy.js';
import db, { initializeViews } from './duckdb/connection.js';
import { getCacheStats } from './cache/stats-cache.js';

// Server protection
// NOTE: This MUST be a dynamic import.
// In Node ESM, sibling static imports can be evaluated in an order that isn't guaranteed,
// which can cause express-rate-limit to initialize before app.js runs.
// Dynamic import guarantees trust proxy has already been set on the app.
const {
  apiLimiter,
  expensiveLimiter,
  securityHeaders,
  startMemoryMonitor,
  getMemoryStatus,
  memoryGuard,
  requestTimeout,
  globalErrorHandler,
} = await import('./lib/server-protection.js');

const PORT = process.env.PORT || 3001;

// Security and protection middleware
app.use(securityHeaders);
app.use(cors());
app.use(express.json({ limit: '10mb' })); // Limit request body size
app.use(requestTimeout(30000)); // 30 second timeout on all requests
app.use(memoryGuard); // Reject requests when memory is critical

// Rate limiting (nginx strips /api, so routes are at root)
app.use('/', apiLimiter); // General rate limiting for all routes

// Stricter limits for expensive operations
app.use('/search', expensiveLimiter);
app.use('/refresh-views', expensiveLimiter);

// Global BigInt serialization safety net - prevents "Do not know how to serialize a BigInt" errors
app.set('json replacer', (key, value) => 
  typeof value === 'bigint' ? Number(value) : value
);

// Root route - API info
app.get('/', (req, res) => {
  res.json({
    name: 'Amulet Scan DuckDB API',
    version: '2.0.0',
    status: 'ok',
    mode: 'read-only',
    description: 'Read-only API server. Ingestion runs separately via scripts/ingest/',
    endpoints: [
      'GET /health',
      'GET /health/detailed',
      'GET /health/config',
      'GET /events/latest',
      'GET /stats/overview',
      'GET /backfill/cursors',
      'GET /backfill/stats',
      'GET /acs/cache',
      'POST /refresh-views',
    ],
    dataPath: db.DATA_PATH,
  });
});

// Quick health check
app.get('/health', (req, res) => {
  res.json({ 
    status: 'ok', 
    timestamp: new Date().toISOString(),
  });
});

// Detailed health check
app.get('/health/detailed', (req, res) => {
  const cacheStats = getCacheStats();
  
  res.json({ 
    status: 'ok', 
    timestamp: new Date().toISOString(),
    mode: 'read-only',
    memory: getMemoryStatus(),
    cache: {
      entries: cacheStats.totalEntries,
    }
  });
});

// Config debug endpoint - shows environment configuration for debugging
app.get('/health/config', (req, res) => {
  res.json({
    timestamp: new Date().toISOString(),
    config: {
      DATA_DIR: process.env.DATA_DIR || '(not set)',
      CURSOR_DIR: process.env.CURSOR_DIR || '(not set)',
      PORT: PORT,
      LOG_LEVEL: process.env.LOG_LEVEL || '(not set)',
    },
    paths: {
      dataPath: db.DATA_PATH,
      cwd: process.cwd(),
    },
    node: {
      version: process.version,
      platform: process.platform,
    }
  });
});

// Refresh DuckDB views (call after data ingestion)
app.post('/refresh-views', async (req, res) => {
  try {
    console.log('🔄 Refreshing DuckDB views...');
    await initializeViews();
    res.json({ status: 'ok', message: 'Views refreshed successfully' });
  } catch (err) {
    console.error('Failed to refresh views:', err);
    res.status(500).json({ error: err.message });
  }
});

// API routes (nginx strips /api prefix before proxying)
app.use('/events', eventsRouter);
app.use('/updates', updatesRouter);
app.use('/party', partyRouter);
app.use('/contracts', contractsRouter);
app.use('/stats', statsRouter);
app.use('/search', searchRouter);
app.use('/backfill', backfillRouter);
app.use('/acs', acsRouter);
app.use('/announcements', announcementsRouter);
app.use('/governance-lifecycle', governanceLifecycleRouter);
app.use('/kaiko', kaikoRouter);
app.use('/rewards', rewardsRouter);
app.use('/scan-proxy', scanProxyRouter);

// Global error handler - must be last middleware
app.use(globalErrorHandler);

// Startup logic - bind to 0.0.0.0 for external access
const HOST = process.env.HOST || '0.0.0.0';
app.listen(PORT, HOST, () => {
  console.log(`🦆 DuckDB API server running on http://${HOST}:${PORT}`);
  console.log(`📁 Reading data files from ${db.DATA_PATH}`);
  console.log(`📝 Crash logs written to ${LOG_PATHS.crash}`);
  console.log(`🛡️ Rate limiting: 100 req/min general, 20 req/min for expensive ops`);
  console.log(`📖 Mode: READ-ONLY (no background ingestion)`);
  
  // Start memory monitoring (just logs warnings, no actions)
  startMemoryMonitor();
  
  // TEMPORARY: Memory allocation debugging - remove after diagnosis
  setInterval(() => {
    const m = process.memoryUsage();
    console.log('📊 MEMORY:', {
      rss: Math.round(m.rss / 1024 / 1024) + 'MB',
      heapUsed: Math.round(m.heapUsed / 1024 / 1024) + 'MB',
      heapTotal: Math.round(m.heapTotal / 1024 / 1024) + 'MB',
    });
  }, 5000);
});
