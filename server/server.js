import express from 'express';
import cors from 'cors';
import cron from 'node-cron';
import { spawn } from 'child_process';
import path from 'path';
import { fileURLToPath } from 'url';
import eventsRouter from './api/events.js';
import partyRouter from './api/party.js';
import contractsRouter from './api/contracts.js';
import statsRouter from './api/stats.js';
import searchRouter from './api/search.js';
import backfillRouter from './api/backfill.js';
import acsRouter from './api/acs.js';
import announcementsRouter from './api/announcements.js';
import governanceLifecycleRouter, { fetchFreshData, writeCache } from './api/governance-lifecycle.js';
import db, { initializeViews } from './duckdb/connection.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3001;

app.use(cors());
app.use(express.json());

// Root route - API info
app.get('/', (req, res) => {
  res.json({
    name: 'Amulet Scan DuckDB API',
    version: '1.0.0',
    status: 'ok',
    endpoints: [
      'GET /health',
      'GET /api/events/latest',
      'GET /api/stats/overview',
      'GET /api/backfill/cursors',
      'GET /api/backfill/stats',
      'POST /api/refresh-views',
    ],
    dataPath: db.DATA_PATH,
  });
});

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
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

// API routes
app.use('/api/events', eventsRouter);
app.use('/api/party', partyRouter);
app.use('/api/contracts', contractsRouter);
app.use('/api/stats', statsRouter);
app.use('/api/search', searchRouter);
app.use('/api/backfill', backfillRouter);
app.use('/api/acs', acsRouter);
app.use('/api/announcements', announcementsRouter);
app.use('/api/governance-lifecycle', governanceLifecycleRouter);

// Schedule governance data refresh every 4 hours
cron.schedule('0 */4 * * *', async () => {
  console.log('⏰ Scheduled governance data refresh starting...');
  try {
    const data = await fetchFreshData();
    writeCache(data);
    console.log(`✅ Scheduled refresh complete. ${data.stats?.totalTopics || 0} topics cached.`);
  } catch (err) {
    console.error('❌ Scheduled governance refresh failed:', err.message);
  }
});

// Schedule ACS snapshot every 3 hours starting at 00:00 UTC
// Runs at: 00:00, 03:00, 06:00, 09:00, 12:00, 15:00, 18:00, 21:00
let acsSnapshotRunning = false;
cron.schedule('0 0,3,6,9,12,15,18,21 * * *', async () => {
  if (acsSnapshotRunning) {
    console.log('⏭️ ACS snapshot already running, skipping...');
    return;
  }
  
  acsSnapshotRunning = true;
  console.log('⏰ Scheduled ACS snapshot starting...');
  
  const scriptPath = path.resolve(__dirname, '../scripts/ingest/fetch-acs-parquet.js');
  const child = spawn('node', [scriptPath], {
    cwd: path.resolve(__dirname, '../scripts/ingest'),
    stdio: 'inherit',
    env: { ...process.env },
  });
  
  child.on('close', (code) => {
    acsSnapshotRunning = false;
    if (code === 0) {
      console.log('✅ Scheduled ACS snapshot complete');
    } else {
      console.error(`❌ ACS snapshot exited with code ${code}`);
    }
  });
  
  child.on('error', (err) => {
    acsSnapshotRunning = false;
    console.error('❌ Failed to start ACS snapshot:', err.message);
  });
});

app.listen(PORT, () => {
  console.log(`🦆 DuckDB API server running on http://localhost:${PORT}`);
  console.log(`📁 Reading data files from ${db.DATA_PATH}`);
  console.log(`⏰ Governance data refresh scheduled every 4 hours`);
  console.log(`⏰ ACS snapshot scheduled every 3 hours (00:00, 03:00, 06:00, 09:00, 12:00, 15:00, 18:00, 21:00 UTC)`);
});
