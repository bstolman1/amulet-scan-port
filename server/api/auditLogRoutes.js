/**
 * Audit log routes.
 *
 * GET  /audit-log                  - paginated log entries
 * GET  /audit-log/stats            - summary statistics
 * GET  /audit-log/pending-backfill - overrides not yet logged
 * POST /audit-log/backfill         - create log entries for historical overrides
 */

import { Router } from 'express';
import {
  getAuditLogEntries,
  getAuditLogStats,
  getPendingBackfill,
  backfillAuditLog,
} from './auditLogService.js';

const router = Router();

router.get('/', async (req, res) => {
  const limit = parseInt(req.query.limit) || 100;
  const { actionType } = req.query;
  res.json(await getAuditLogEntries({ limit, actionType }));
});

router.get('/stats', async (req, res) => {
  res.json(await getAuditLogStats());
});

router.get('/pending-backfill', async (req, res) => {
  res.json(await getPendingBackfill());
});

router.post('/backfill', async (req, res) => {
  res.json(await backfillAuditLog());
});

export default router;
