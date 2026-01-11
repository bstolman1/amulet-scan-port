/**
 * Engine API - REST endpoints for engine status and control
 * Includes streaming endpoints with cursor-based pagination
 */

import { Router } from 'express';
import { getEngineStatus, triggerCycle } from './worker.js';
import { getFileStats, scanAndIndexFiles } from './file-index.js';
import { getIngestionStats, ingestNewFiles } from './ingest.js';
import { getTotalCounts, getTimeRange, getTemplateEventCounts, streamEvents } from './aggregations.js';
import { resetEngineSchema } from './schema.js';
import { query } from '../duckdb/connection.js';
import {
  buildTemplateFileIndex,
  getTemplateIndexStats,
  getIndexedTemplates,
  getFilesForTemplateWithMeta,
  isTemplateIndexingInProgress,
  getTemplateIndexingProgress,
} from './template-file-index.js';

const router = Router();

// Helper to convert BigInt values to numbers for JSON serialization
function convertBigInts(obj) {
  if (obj === null || obj === undefined) return obj;
  if (typeof obj === 'bigint') return Number(obj);
  if (Array.isArray(obj)) return obj.map(convertBigInts);
  if (typeof obj === 'object') {
    const result = {};
    for (const [key, value] of Object.entries(obj)) {
      result[key] = convertBigInts(value);
    }
    return result;
  }
  return obj;
}

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
// NOTE: Sequential queries to avoid DuckDB transaction conflicts
router.get('/stats', async (req, res) => {
  try {
    const ingestion = await getIngestionStats();
    const counts = await getTotalCounts();
    const timeRange = await getTimeRange();
    
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

/**
 * GET /api/engine/events/stream - Streaming events with cursor-based pagination
 * 
 * Query params:
 *   - cursor: recorded_at timestamp to start after (ISO string)
 *   - limit: max records per page (default 100, max 1000)
 *   - template: filter by template name
 *   - type: filter by event type (Created, Archived)
 *   - order: 'asc' or 'desc' (default 'desc')
 */
router.get('/events/stream', async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 100, 1000);
    const cursor = req.query.cursor; // ISO timestamp
    const template = req.query.template;
    const type = req.query.type;
    const order = req.query.order === 'asc' ? 'ASC' : 'DESC';
    
    // Build WHERE clause
    const conditions = [];
    if (cursor) {
      const op = order === 'DESC' ? '<' : '>';
      conditions.push(`recorded_at ${op} TIMESTAMP '${cursor}'`);
    }
    if (template) {
      conditions.push(`template = '${template.replace(/'/g, "''")}'`);
    }
    if (type) {
      conditions.push(`type = '${type.replace(/'/g, "''")}'`);
    }
    
    const where = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
    
    const rows = await query(`
      SELECT 
        id, update_id, type, synchronizer, 
        effective_at, recorded_at, 
        contract_id, party, template, package_name,
        signatories, observers, payload
      FROM events_raw
      ${where}
      ORDER BY recorded_at ${order}
      LIMIT ${limit + 1}
    `);
    
    // Check if there are more results
    const hasMore = rows.length > limit;
    const data = hasMore ? rows.slice(0, limit) : rows;
    
    // Next cursor is the last item's recorded_at
    const nextCursor = hasMore && data.length > 0 
      ? data[data.length - 1].recorded_at 
      : null;
    
    res.json({
      data,
      pagination: {
        limit,
        count: data.length,
        hasMore,
        nextCursor,
        order,
      }
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/**
 * GET /api/engine/events/count - Get event counts with optional filters
 */
router.get('/events/count', async (req, res) => {
  try {
    const template = req.query.template;
    const type = req.query.type;
    
    const conditions = [];
    if (template) {
      conditions.push(`template = '${template.replace(/'/g, "''")}'`);
    }
    if (type) {
      conditions.push(`type = '${type.replace(/'/g, "''")}'`);
    }
    
    const where = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
    
    const rows = await query(`SELECT COUNT(*) as count FROM events_raw ${where}`);
    
    res.json({ count: rows[0]?.count || 0 });
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

// ===== Template File Index Endpoints =====

// GET /api/engine/template-index/status - Get template index status
router.get('/template-index/status', async (req, res) => {
  try {
    const stats = await getTemplateIndexStats();
    const progress = isTemplateIndexingInProgress() 
      ? getTemplateIndexingProgress() 
      : null;
    
    res.json({
      ...stats,
      inProgress: isTemplateIndexingInProgress(),
      progress,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/engine/template-index/templates - List indexed templates
router.get('/template-index/templates', async (req, res) => {
  try {
    const templates = await getIndexedTemplates();
    res.json(convertBigInts({ templates }));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/engine/template-index/files - Get files for a template
router.get('/template-index/files', async (req, res) => {
  try {
    const template = req.query.template;
    if (!template) {
      return res.status(400).json({ error: 'template query param required' });
    }
    
    const files = await getFilesForTemplateWithMeta(template);
    res.json({ 
      template, 
      fileCount: files.length,
      files 
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/engine/template-index/build - Build/update template index
router.post('/template-index/build', async (req, res) => {
  try {
    if (isTemplateIndexingInProgress()) {
      const progress = getTemplateIndexingProgress();
      return res.json({ 
        success: false, 
        message: 'Index build already in progress',
        progress,
      });
    }
    
    const force = req.query.force === 'true';
    
    // Start build in background and return immediately
    buildTemplateFileIndex({ force, incremental: !force })
      .then(result => {
        console.log(`✅ Template index build complete: ${result.filesIndexed} files, ${result.templatesFound} mappings`);
      })
      .catch(err => {
        console.error('❌ Template index build failed:', err.message);
      });
    
    res.json({ 
      success: true, 
      message: 'Template index build started in background',
      force,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

export default router;
