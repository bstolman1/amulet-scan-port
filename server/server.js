// MUST be first - loads .env before other modules capture process.env
import './env.js';

import express from 'express';
import cors from 'cors';
import cron from 'node-cron';
import { spawn } from 'child_process';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';

// ============= Graceful Shutdown Tracking =============
const cronJobs = [];          // Track scheduled cron jobs
const childProcesses = new Set(); // Track spawned child processes
let isShuttingDown = false;
let lockPath = null;          // Will be set after db import

function trackCronJob(job) {
  cronJobs.push(job);
  return job;
}

function trackChildProcess(child) {
  childProcesses.add(child);
  child.on('close', () => childProcesses.delete(child));
  child.on('error', () => childProcesses.delete(child));
  return child;
}

async function gracefulShutdown(signal) {
  if (isShuttingDown) return;
  isShuttingDown = true;
  
  console.log(`\n🛑 ${signal} received - starting graceful shutdown...`);
  
  // 1. Stop all cron jobs
  console.log(`   Stopping ${cronJobs.length} cron job(s)...`);
  for (const job of cronJobs) {
    try { job.stop(); } catch {}
  }
  
  // 2. Kill all child processes
  console.log(`   Killing ${childProcesses.size} child process(es)...`);
  for (const child of childProcesses) {
    try {
      child.kill('SIGTERM');
      // Force kill after 2s if still alive
      setTimeout(() => {
        try { child.kill('SIGKILL'); } catch {}
      }, 2000);
    } catch {}
  }
  
  // 3. Stop engine worker if running
  try {
    const { stopEngineWorker } = await import('./engine/worker.js');
    stopEngineWorker();
    console.log('   Engine worker stopped');
  } catch {}
  
  // 4. Remove lockfile
  if (lockPath) {
    try {
      fs.unlinkSync(lockPath);
      console.log('   Lockfile removed');
    } catch {}
  }
  
  console.log('✅ Graceful shutdown complete');
  process.exit(0);
}

// Register shutdown handlers early
process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('uncaughtException', (err) => {
  console.error('❌ Uncaught exception:', err);
  gracefulShutdown('uncaughtException');
});
process.on('unhandledRejection', (reason) => {
  console.error('❌ Unhandled rejection:', reason);
  // Don't exit on unhandled rejection, just log
});
import eventsRouter from './api/events.js';
import updatesRouter from './api/updates.js';
import partyRouter from './api/party.js';
import contractsRouter from './api/contracts.js';
import statsRouter from './api/stats.js';
import searchRouter from './api/search.js';
import backfillRouter from './api/backfill.js';
import acsRouter from './api/acs.js';
import announcementsRouter from './api/announcements.js';
import governanceLifecycleRouter, { fetchFreshData, writeCache } from './api/governance-lifecycle.js';
import kaikoRouter from './api/kaiko.js';
import rewardsRouter from './api/rewards.js';
import db, { initializeViews } from './duckdb/connection.js';
import { refreshAllAggregations, invalidateACSCache } from './cache/aggregation-worker.js';
import { getCacheStats } from './cache/stats-cache.js';
// Warehouse engine imports
import { startEngineWorker, getEngineStatus } from './engine/worker.js';
import engineRouter from './engine/api.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3001;

// Engine enabled flag - set to true to use the new warehouse engine
const ENGINE_ENABLED = process.env.ENGINE_ENABLED === 'true';

app.use(cors());
app.use(express.json());

// Root route - API info
app.get('/', async (req, res) => {
  let engineStatus = null;
  if (ENGINE_ENABLED) {
    try {
      engineStatus = await getEngineStatus();
    } catch (err) {
      engineStatus = { error: err.message };
    }
  }
  
  res.json({
    name: 'Amulet Scan DuckDB API',
    version: '1.0.0',
    status: 'ok',
    engine: ENGINE_ENABLED ? 'enabled' : 'disabled',
    engineStatus,
    endpoints: [
      'GET /health',
      'GET /api/events/latest',
      'GET /api/stats/overview',
      'GET /api/backfill/cursors',
      'GET /api/backfill/stats',
      'GET /api/acs/cache',
      'POST /api/acs/cache/invalidate',
      'POST /api/refresh-views',
      'POST /api/refresh-aggregations',
      'GET /api/engine/status',
      'GET /api/engine/stats',
      'POST /api/engine/cycle',
    ],
    dataPath: db.DATA_PATH,
  });
});

// Health check with timeout to prevent hanging
app.get('/health', async (req, res) => {
  const cacheStats = getCacheStats();
  let engineStatus = null;
  
  if (ENGINE_ENABLED) {
    try {
      // Add 2-second timeout to prevent health check from hanging
      engineStatus = await Promise.race([
        getEngineStatus(),
        new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 2000))
      ]);
    } catch (err) {
      engineStatus = { error: err.message === 'timeout' ? 'status check timed out' : err.message };
    }
  }
  
  res.json({ 
    status: 'ok', 
    timestamp: new Date().toISOString(),
    engine: ENGINE_ENABLED ? 'enabled' : 'disabled',
    engineStatus,
    cache: {
      entries: cacheStats.totalEntries,
    }
  });
});

// Refresh DuckDB views (call after data ingestion)
app.post('/api/refresh-views', async (req, res) => {
  try {
    console.log('🔄 Refreshing DuckDB views...');
    await initializeViews();
    res.json({ status: 'ok', message: 'Views refreshed successfully' });
  } catch (err) {
    console.error('Failed to refresh views:', err);
    res.status(500).json({ error: err.message });
  }
});

// Refresh aggregations manually
app.post('/api/refresh-aggregations', async (req, res) => {
  try {
    console.log('🔄 Manual aggregation refresh triggered...');
    const result = await refreshAllAggregations();
    res.json({ 
      status: 'ok', 
      message: 'Aggregations refreshed successfully',
      duration: result?.duration,
      stats: {
        holders: result?.holders?.holderCount,
        supply: result?.supply?.total_supply,
        rounds: result?.rounds?.counts,
      }
    });
  } catch (err) {
    console.error('Failed to refresh aggregations:', err);
    res.status(500).json({ error: err.message });
  }
});

// API routes
app.use('/api/events', eventsRouter);
app.use('/api/updates', updatesRouter);
app.use('/api/party', partyRouter);
app.use('/api/contracts', contractsRouter);
app.use('/api/stats', statsRouter);
app.use('/api/search', searchRouter);
app.use('/api/backfill', backfillRouter);
app.use('/api/acs', acsRouter);
app.use('/api/announcements', announcementsRouter);
app.use('/api/governance-lifecycle', governanceLifecycleRouter);
app.use('/api/kaiko', kaikoRouter);
app.use('/api/rewards', rewardsRouter);

// Engine API routes
app.use('/api/engine', engineRouter);

// Schedule governance data refresh every 4 hours
trackCronJob(cron.schedule('0 */4 * * *', async () => {
  if (isShuttingDown) return;
  console.log('⏰ Scheduled governance data refresh starting...');
  try {
    const data = await fetchFreshData();
    writeCache(data);
    console.log(`✅ Scheduled refresh complete. ${data.stats?.totalTopics || 0} topics cached.`);
  } catch (err) {
    console.error('❌ Scheduled governance refresh failed:', err.message);
  }
}));

// Schedule aggregation refresh every 15 minutes (only if engine is disabled)
if (!ENGINE_ENABLED) {
  trackCronJob(cron.schedule('*/15 * * * *', async () => {
    if (isShuttingDown) return;
    console.log('⏰ Scheduled aggregation refresh starting...');
    try {
      await refreshAllAggregations();
    } catch (err) {
      console.error('❌ Scheduled aggregation refresh failed:', err.message);
    }
  }));
}

// Schedule ACS snapshot every 3 hours starting at 00:00 UTC
// Runs at: 00:00, 03:00, 06:00, 09:00, 12:00, 15:00, 18:00, 21:00
let acsSnapshotRunning = false;
trackCronJob(cron.schedule('0 0,3,6,9,12,15,18,21 * * *', async () => {
  if (isShuttingDown) return;
  if (acsSnapshotRunning) {
    console.log('⏭️ ACS snapshot already running, skipping...');
    return;
  }
  
  acsSnapshotRunning = true;
  console.log('⏰ Scheduled ACS snapshot starting...');
  
  const scriptPath = path.resolve(__dirname, '../scripts/ingest/fetch-acs-parquet.js');
  const child = trackChildProcess(spawn('node', [scriptPath], {
    cwd: path.resolve(__dirname, '../scripts/ingest'),
    stdio: 'inherit',
    env: { ...process.env },
  }));
  
  child.on('close', async (code) => {
    acsSnapshotRunning = false;
    if (isShuttingDown) return;
    if (code === 0) {
      console.log('✅ Scheduled ACS snapshot complete');
      
      // Invalidate caches and refresh aggregations after successful snapshot
      console.log('🔄 Refreshing aggregations after snapshot...');
      invalidateACSCache();
      try {
        await refreshAllAggregations();
        console.log('✅ Post-snapshot aggregation refresh complete');
      } catch (err) {
        console.error('❌ Post-snapshot aggregation refresh failed:', err.message);
      }
    } else {
      console.error(`❌ ACS snapshot exited with code ${code}`);
    }
  });
  
  child.on('error', (err) => {
    acsSnapshotRunning = false;
    console.error('❌ Failed to start ACS snapshot:', err.message);
  });
}));

// Startup logic
app.listen(PORT, async () => {
  console.log(`🦆 DuckDB API server running on http://localhost:${PORT}`);
  console.log(`   PID: ${process.pid}`);
  console.log(`📁 Reading data files from ${db.DATA_PATH}`);
  console.log(`⏰ Governance data refresh scheduled every 4 hours`);
  console.log(`⏰ ACS snapshot scheduled every 3 hours (00:00, 03:00, 06:00, 09:00, 12:00, 15:00, 18:00, 21:00 UTC)`);

  // Prevent accidental double-start (common on Windows): create a simple lock file next to the DB.
  // If another process owns the lock and is still alive, exit early with a clear message.
  try {
    lockPath = `${db.DB_FILE}.lock`; // Set module-level lockPath for graceful shutdown
    if (fs.existsSync(lockPath)) {
      const existingPid = parseInt(String(fs.readFileSync(lockPath, 'utf8')).trim(), 10);
      if (Number.isFinite(existingPid) && existingPid !== process.pid) {
        try {
          process.kill(existingPid, 0);
          console.error(`❌ Another backend process is already running (PID ${existingPid}) and holding the DuckDB file.`);
          console.error(`   Stop PID ${existingPid} (Task Manager) or delete: ${lockPath}`);
          process.exit(1);
        } catch {
          // stale lock file - remove it
          try { fs.unlinkSync(lockPath); } catch {}
        }
      }
    }
    fs.writeFileSync(lockPath, String(process.pid), { encoding: 'utf8' });
    // Note: SIGINT/SIGTERM handlers are already registered at module level for graceful shutdown
  } catch (err) {
    console.warn('⚠️ Could not create lockfile:', err?.message);
  }

  // Initialize DuckDB views on startup, but never crash the process if this fails.
  try {
    console.log('🔧 Initializing DuckDB views...');
    await initializeViews();
    console.log('✅ DuckDB views ready');
  } catch (err) {
    console.error('⚠️ DuckDB view initialization failed (continuing):', err?.message || err);
  }

  if (ENGINE_ENABLED) {
    console.log(`⚙️ Warehouse engine ENABLED - starting background worker...`);
    try {
      await startEngineWorker();
    } catch (err) {
      console.error('❌ Failed to start engine worker:', err.message);
    }
  } else {
    console.log(`⏰ Aggregation refresh scheduled every 15 minutes (engine disabled)`);
    // Initial aggregation refresh on startup (delayed to allow server to start)
    setTimeout(async () => {
      if (isShuttingDown) return;
      console.log('🚀 Running initial aggregation refresh...');
      try {
        await refreshAllAggregations();
      } catch (err) {
        console.error('❌ Initial aggregation refresh failed:', err.message);
      }
    }, 5000);
  }
});


