import express from 'express';
import cors from 'cors';
import cron from 'node-cron';
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

app.listen(PORT, () => {
  console.log(`🦆 DuckDB API server running on http://localhost:${PORT}`);
  console.log(`📁 Reading data files from ${db.DATA_PATH}`);
  console.log(`⏰ Governance data refresh scheduled every 4 hours`);
});
