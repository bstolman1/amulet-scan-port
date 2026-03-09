/**
 * ACS API - SCAN-ONLY MODE (FULLY STUBBED)
 * 
 * All endpoints return immediate stub responses.
 * No external network calls that could fail.
 */

import { Router } from 'express';

const router = Router();

// Helper: scan-only stub response
const scanOnlyStub = (extra = {}) => ({
  mode: 'scan-only',
  message: 'ACS disabled (DuckDB offline) - use /api/scan-proxy/* for live data',
  source: 'none',
  ...extra,
});

/**
 * GET /acs/latest
 */
router.get('/latest', (_req, res) => {
  res.json(scanOnlyStub({ data: null }));
});

/**
 * GET /acs/status
 */
router.get('/status', (_req, res) => {
  res.json({
    mode: 'scan-only',
    available: false,
    source: 'none',
    duckdb_enabled: false,
    latestRound: null,
    snapshotInProgress: false,
    completeSnapshotCount: 0,
    inProgressSnapshotCount: 0,
    message: 'ACS disabled - use /api/scan-proxy/* for live data',
  });
});

/**
 * GET /acs/cache
 */
router.get('/cache', (_req, res) => {
  res.json(scanOnlyStub({ cache: 'disabled' }));
});

/**
 * GET /acs/stats
 */
router.get('/stats', (_req, res) => {
  res.json(scanOnlyStub({
    data: {
      total_contracts: 0,
      total_templates: 0,
      total_snapshots: 0,
      latest_snapshot: null,
      latest_record_time: null,
    },
  }));
});

/**
 * GET /acs/templates
 */
router.get('/templates', (_req, res) => {
  res.json(scanOnlyStub({ data: [], count: 0 }));
});

/**
 * GET /acs/snapshots
 */
router.get('/snapshots', (_req, res) => {
  res.json(scanOnlyStub({ data: [], count: 0 }));
});

/**
 * GET /acs/contracts
 */
router.get('/contracts', (_req, res) => {
  res.json(scanOnlyStub({ data: [], count: 0 }));
});

/**
 * GET /acs/aggregate
 */
router.get('/aggregate', (_req, res) => {
  res.json(scanOnlyStub({ sum: 0, count: 0, templateCount: 0 }));
});

/**
 * GET /acs/supply
 */
router.get('/supply', (_req, res) => {
  res.json(scanOnlyStub({
    data: {
      totalSupply: 0,
      unlockedSupply: 0,
      lockedSupply: 0,
      circulatingSupply: 0,
    },
  }));
});

/**
 * GET /acs/rich-list
 */
router.get('/rich-list', (_req, res) => {
  res.json(scanOnlyStub({
    data: [],
    totalSupply: 0,
    unlockedSupply: 0,
    lockedSupply: 0,
    holderCount: 0,
  }));
});

/**
 * GET /acs/realtime-supply
 */
router.get('/realtime-supply', (_req, res) => {
  res.json(scanOnlyStub({
    data: {
      snapshot: null,
      delta: null,
      realtime: { unlocked: 0, locked: 0, total: 0, circulating: 0 },
      calculated_at: new Date().toISOString(),
    },
  }));
});

/**
 * GET /acs/realtime-rich-list
 */
router.get('/realtime-rich-list', (_req, res) => {
  res.json(scanOnlyStub({
    data: [],
    totalSupply: 0,
    unlockedSupply: 0,
    lockedSupply: 0,
    holderCount: 0,
    snapshotRecordTime: null,
    isRealtime: false,
  }));
});

/**
 * GET /acs/mining-rounds
 */
router.get('/mining-rounds', (_req, res) => {
  res.json(scanOnlyStub({
    openRounds: [],
    issuingRounds: [],
    closedRounds: [],
    counts: { open: 0, issuing: 0, closed: 0 },
    currentRound: null,
  }));
});

/**
 * GET /acs/allocations
 */
router.get('/allocations', (_req, res) => {
  res.json(scanOnlyStub({
    data: [],
    totalCount: 0,
    totalAmount: 0,
    uniqueExecutors: 0,
  }));
});

/**
 * POST /acs/cache/invalidate
 */
router.post('/cache/invalidate', (_req, res) => {
  res.json(scanOnlyStub({ success: true }));
});

/**
 * POST /acs/trigger-snapshot
 *
 * FIX: Previously returned HTTP 200 with { success: false } when the service
 * is unavailable.  HTTP semantics require a non-2xx status when the operation
 * cannot be completed.  503 + Retry-After is the correct signal for "service
 * temporarily unavailable; try again later".
 *
 * Before:
 *   res.json(scanOnlyStub({ success: false }));
 *   → 200 OK  ← clients can't distinguish success from failure
 *
 * After:
 *   res.status(503).set('Retry-After', '3600').json(...)
 *   → 503 Service Unavailable  ← unambiguous; load-balancers/monitors act correctly
 *
 * The dead `const SCAN_ONLY = true` flag was also removed — it was never read
 * anywhere in this file, so it only added noise.
 */
router.post('/trigger-snapshot', (_req, res) => {
  res
    .status(503)
    .set('Retry-After', '3600')
    .json(scanOnlyStub({ success: false, reason: 'ACS service unavailable in scan-only mode' }));
});

export default router;
