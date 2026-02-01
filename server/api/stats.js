/**
 * Stats API - SCAN-ONLY MODE
 * 
 * DuckDB is disabled. All endpoints return graceful stub responses.
 */

import { Router } from 'express';
import fs from 'fs';
import path from 'path';

const router = Router();

// Global scan-only mode - DuckDB is offline
const SCAN_ONLY = true;

// Data path for cursor files (still needed for live-status endpoint)
const DATA_PATH = process.env.DATA_DIR || './data';

// Helper: scan-only stub response
const scanOnlyStub = (data = []) => ({
  mode: 'scan-only',
  message: 'Stats disabled (DuckDB offline)',
  data,
  data_source: 'none',
});

// POST /api/stats/init-engine-schema - Initialize the engine schema
router.post('/init-engine-schema', (_req, res) => {
  res.json({ 
    success: false, 
    message: 'Engine disabled (scan-only mode)',
    mode: 'scan-only',
  });
});

// GET /api/stats/overview - Dashboard overview stats
router.get('/overview', (_req, res) => {
  res.json({
    mode: 'scan-only',
    message: 'Stats disabled (DuckDB offline)',
    total_events: null,
    unique_contracts: null,
    unique_templates: null,
    earliest_event: null,
    latest_event: null,
    data_source: 'none',
  });
});

// GET /api/stats/daily - Daily event counts
router.get('/daily', (_req, res) => {
  res.json(scanOnlyStub([]));
});

// GET /api/stats/by-type - Event counts by type
router.get('/by-type', (_req, res) => {
  res.json(scanOnlyStub([]));
});

// GET /api/stats/by-template - Event counts by template
router.get('/by-template', (_req, res) => {
  res.json(scanOnlyStub([]));
});

// GET /api/stats/hourly - Hourly activity (last 24h)
router.get('/hourly', (_req, res) => {
  res.json(scanOnlyStub([]));
});

// GET /api/stats/burn - Burn statistics
router.get('/burn', (_req, res) => {
  res.json(scanOnlyStub([]));
});

// GET /api/stats/sources - Get info about available data sources
router.get('/sources', (_req, res) => {
  res.json({
    mode: 'scan-only',
    hasParquetEvents: false,
    hasParquetUpdates: false,
    hasJsonlEvents: false,
    hasJsonlUpdates: false,
    primarySource: 'none',
    dataPath: DATA_PATH,
    engineEnabled: false,
  });
});

// GET /api/stats/ccview-comparison - CCVIEW-style counting comparison
router.get('/ccview-comparison', (_req, res) => {
  res.json({
    mode: 'scan-only',
    message: 'Stats disabled (DuckDB offline)',
    your_counts: { updates: 0, events: 0 },
    ccview_style: { total_events: 0 },
    data_source: 'none',
  });
});

// GET /api/stats/live-status - Live ingestion status
router.get('/live-status', (req, res) => {
  try {
    const dataDir = process.env.DATA_DIR || DATA_PATH;
    const cursorDir = path.join(dataDir, 'cursors');
    const liveCursorFile = path.join(cursorDir, 'live-cursor.json');
    
    let liveCursor = null;
    let backfillCursors = [];
    
    // Read live cursor if exists
    if (fs.existsSync(liveCursorFile)) {
      try {
        liveCursor = JSON.parse(fs.readFileSync(liveCursorFile, 'utf8'));
      } catch (e) { /* ignore */ }
    }
    
    // Read all backfill cursors
    if (fs.existsSync(cursorDir)) {
      const cursorFiles = fs.readdirSync(cursorDir).filter(f => f.endsWith('.json') && f !== 'live-cursor.json');
      for (const file of cursorFiles) {
        try {
          const cursor = JSON.parse(fs.readFileSync(path.join(cursorDir, file), 'utf8'));
          if (cursor.migration_id !== undefined) {
            backfillCursors.push({ file, ...cursor });
          }
        } catch (e) { /* ignore */ }
      }
    }
    
    const allBackfillComplete = backfillCursors.length > 0 && backfillCursors.every(c => c.complete === true);
    
    // Determine ingestion mode
    let mode = 'scan-only';
    let status = 'stopped';
    let currentRecordTime = null;
    
    if (liveCursor && liveCursor.updated_at) {
      const lastUpdate = new Date(liveCursor.updated_at);
      const ageMs = Date.now() - lastUpdate.getTime();
      if (ageMs < 60000) {
        status = 'running';
        currentRecordTime = liveCursor.record_time;
      } else if (ageMs < 300000) {
        status = 'idle';
        currentRecordTime = liveCursor.record_time;
      }
    }
    
    res.json({
      mode,
      status,
      live_cursor: liveCursor,
      backfill_cursors: backfillCursors,
      all_backfill_complete: allBackfillComplete,
      current_record_time: currentRecordTime,
      message: 'DuckDB offline - using SCAN API for live data',
    });
  } catch (err) {
    console.error('Error fetching live status:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/stats/aggregation-state - Aggregation progress table
router.get('/aggregation-state', (_req, res) => {
  res.json({ 
    mode: 'scan-only',
    states: [], 
    tableExists: false,
    message: 'Aggregation disabled (DuckDB offline)',
  });
});

// POST /api/stats/aggregation-state/reset - Reset aggregation state
router.post('/aggregation-state/reset', (_req, res) => {
  res.json({ 
    mode: 'scan-only',
    success: false, 
    message: 'Aggregation disabled (DuckDB offline)',
    states: [],
  });
});

// DELETE /api/stats/live-cursor - Purge live cursor
router.delete('/live-cursor', (req, res) => {
  try {
    const dataDir = process.env.DATA_DIR || DATA_PATH;
    const cursorDir = path.join(dataDir, 'cursors');
    const liveCursorFile = path.join(cursorDir, 'live-cursor.json');

    if (fs.existsSync(liveCursorFile)) {
      fs.unlinkSync(liveCursorFile);
      console.log('🗑️ Deleted live cursor file:', liveCursorFile);
      res.json({
        success: true,
        message: 'Live cursor deleted.',
        deleted_file: liveCursorFile
      });
    } else {
      res.json({
        success: true,
        message: 'No live cursor file found.',
        deleted_file: null
      });
    }
  } catch (err) {
    console.error('Error deleting live cursor:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/stats/sv-weight-history - SV weight distribution over time
router.get('/sv-weight-history', (_req, res) => {
  res.json({
    mode: 'scan-only',
    message: 'SV weight history disabled (DuckDB offline)',
    data: [],
    dailyData: [],
    stackedData: [],
    svNames: [],
    totalRules: 0,
  });
});

export default router;
