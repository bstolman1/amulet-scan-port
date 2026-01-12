/**
 * Test App Factory
 * 
 * Creates an Express app instance for integration testing.
 * Isolates the app from the server startup logic (cron jobs, listening, etc.)
 */

import express from 'express';
import cors from 'cors';

// Import routers
import eventsRouter from '../api/events.js';
import updatesRouter from '../api/updates.js';
import partyRouter from '../api/party.js';
import contractsRouter from '../api/contracts.js';
import statsRouter from '../api/stats.js';
import searchRouter from '../api/search.js';
import backfillRouter from '../api/backfill.js';
import acsRouter from '../api/acs.js';
import announcementsRouter from '../api/announcements.js';
import governanceLifecycleRouter from '../api/governance-lifecycle.js';
import rewardsRouter from '../api/rewards.js';
import engineRouter from '../engine/api.js';

/**
 * Create a test-ready Express app
 * @param {Object} options - Configuration options
 * @param {Object} options.mocks - Mock implementations for dependencies
 * @returns {express.Application} Express app instance
 */
export function createTestApp(options = {}) {
  const app = express();
  
  app.use(cors());
  app.use(express.json());
  
  // Global BigInt serialization
  app.set('json replacer', (key, value) => 
    typeof value === 'bigint' ? Number(value) : value
  );
  
  // Health check
  app.get('/health', (req, res) => {
    res.json({ status: 'ok', timestamp: new Date().toISOString() });
  });
  
  // Root route
  app.get('/', (req, res) => {
    res.json({
      name: 'Amulet Scan DuckDB API',
      version: '1.0.0',
      status: 'ok',
      engine: 'disabled',
    });
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
  app.use('/api/rewards', rewardsRouter);
  app.use('/api/engine', engineRouter);
  
  // Error handler
  app.use((err, req, res, next) => {
    console.error('Test app error:', err);
    res.status(500).json({ error: err.message });
  });
  
  return app;
}

export default createTestApp;
