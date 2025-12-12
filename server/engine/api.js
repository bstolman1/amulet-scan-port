/**
 * Engine API - REST endpoints for engine status and control
 */

import { Router } from 'express';
import { getEngineStatus, triggerCycle } from './worker.js';
import { getFileStats, scanAndIndexFiles } from './file-index.js';
import { getIngestionStats, ingestNewFiles } from './ingest.js';
import { getTotalCounts, getTimeRange, getTemplateEventCounts } from './aggregations.js';
import { resetEngineSchema } from './schema.js';

const router = Router();

// GET /api/engine/status - Get engine status
router.get('/status', async (req, res) => {
  try {
    const status = await getEngineStatus();
    res.json(status);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/engine/files - Get file index stats
router.get('/files', async (req, res) => {
  try {
    const stats = await getFileStats();
    res.json({ files: stats });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/engine/stats - Get ingestion stats
router.get('/stats', async (req, res) => {
  try {
    const [ingestion, counts, timeRange] = await Promise.all([
      getIngestionStats(),
      getTotalCounts(),
      getTimeRange(),
    ]);
    
    res.json({
      ingestion,
      counts,
      timeRange,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/engine/templates - Get template event counts
router.get('/templates', async (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 100;
    const templates = await getTemplateEventCounts(limit);
    res.json({ templates });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/engine/scan - Manually trigger file scan
router.post('/scan', async (req, res) => {
  try {
    const result = await scanAndIndexFiles();
    res.json({ success: true, ...result });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/engine/ingest - Manually trigger ingestion
router.post('/ingest', async (req, res) => {
  try {
    const maxFiles = parseInt(req.query.files) || 5;
    const result = await ingestNewFiles(maxFiles);
    res.json({ success: true, ...result });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/engine/cycle - Trigger a full cycle
router.post('/cycle', async (req, res) => {
  try {
    await triggerCycle();
    const status = await getEngineStatus();
    res.json({ success: true, status });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/engine/reset - Reset engine (clear all tables)
router.post('/reset', async (req, res) => {
  try {
    await resetEngineSchema();
    res.json({ success: true, message: 'Engine schema reset' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

export default router;
