/**
 * governance-lifecycle.js  (refactored entry point)
 *
 * A thin router that:
 *  1. Ensures directories exist (once at startup)
 *  2. Mounts focused sub-routers
 *
 * Each sub-router owns a single concern:
 *   /                  → lifecycleRoutes    (data, refresh, cache-info)
 *   /overrides/*       → overrideRoutes     (CRUD for all override types)
 *   /audit-log/*       → auditLogRoutes     (read, stats, backfill)
 *   /learned-patterns* → learningRoutes     (patterns, rollback, training data)
 *   /llm-status etc.   → llmRoutes          (LLM/audit status and debug)
 */

import { Router } from 'express';
import { ensureDirs } from './repositories/fileRepository.js';

import lifecycleRoutes from './routes/lifecycleRoutes.js';
import overrideRoutes from './routes/overrideRoutes.js';
import auditLogRoutes from './routes/auditLogRoutes.js';
import learningRoutes from './routes/learningRoutes.js';

// Ensure cache directories exist before any route can fire
ensureDirs();

const router = Router();

// Core lifecycle data
router.use('/', lifecycleRoutes);

// Manual overrides (type, merge, move, extract)
router.use('/overrides', overrideRoutes);

// Audit log
router.use('/audit-log', auditLogRoutes);

// Pattern learning, improvements, rollback
router.use('/learned-patterns', learningRoutes);
// Legacy path aliases (keep old URLs working)
router.use('/apply-improvements', (req, res, next) => {
  req.url = '/apply-improvements' + req.url;
  learningRoutes(req, res, next);
});

export default router;
