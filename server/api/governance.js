/**
 * Governance API - Proposal endpoints
 */

import express from 'express';
import { query } from '../duckdb/connection.js';
import {
  buildGovernanceIndex,
  getProposalStats,
  queryProposals,
  getProposalByKey,
  getProposalByContractId,
  getIndexingProgress,
  isGovernanceIndexingInProgress,
  invalidateCache,
} from '../engine/governance-indexer.js';

const router = express.Router();

/**
 * GET /api/governance/proposals
 * List all proposals with optional filters
 */
router.get('/proposals', async (req, res) => {
  try {
    const {
      limit = '100',
      offset = '0',
      status,
      actionType,
      requester,
      search,
      forceRefresh,
    } = req.query;

    // Force refresh if requested
    if (forceRefresh === 'true') {
      invalidateCache();
    }

    const result = await queryProposals({
      limit: parseInt(limit, 10),
      offset: parseInt(offset, 10),
      status: status || null,
      actionType: actionType || null,
      requester: requester || null,
      search: search || null,
    });

    res.json(result);
  } catch (err) {
    console.error('Error querying proposals:', err);
    res.status(500).json({ error: err.message });
  }
});

/**
 * GET /api/governance/proposals/stats
 * Get proposal statistics
 */
router.get('/proposals/stats', async (req, res) => {
  try {
    const stats = await getProposalStats();
    res.json(stats);
  } catch (err) {
    console.error('Error getting proposal stats:', err);
    res.status(500).json({ error: err.message });
  }
});

/**
 * GET /api/governance/proposals/:key
 * Get a single proposal by key (action type + reason URL)
 */
router.get('/proposals/by-key/:key', async (req, res) => {
  try {
    const { key } = req.params;
    const decodedKey = decodeURIComponent(key);
    
    const proposal = await getProposalByKey(decodedKey);
    
    if (!proposal) {
      return res.status(404).json({ error: 'Proposal not found' });
    }
    
    res.json(proposal);
  } catch (err) {
    console.error('Error getting proposal:', err);
    res.status(500).json({ error: err.message });
  }
});

/**
 * GET /api/governance/proposals/by-contract/:contractId
 * Get a proposal by contract ID
 */
router.get('/proposals/by-contract/:contractId', async (req, res) => {
  try {
    const { contractId } = req.params;
    
    const proposal = await getProposalByContractId(contractId);
    
    if (!proposal) {
      return res.status(404).json({ error: 'Proposal not found' });
    }
    
    res.json(proposal);
  } catch (err) {
    console.error('Error getting proposal by contract:', err);
    res.status(500).json({ error: err.message });
  }
});

/**
 * POST /api/governance/index/build
 * Trigger a full index rebuild (async)
 */
router.post('/index/build', async (req, res) => {
  try {
    if (isGovernanceIndexingInProgress()) {
      return res.json({ status: 'in_progress', message: 'Indexing already in progress' });
    }
    
    // Respond immediately
    res.json({ status: 'started', message: 'Governance index build started' });
    
    // Run async in background
    const { limit = '10000' } = req.query;
    invalidateCache();
    
    buildGovernanceIndex({
      limit: parseInt(limit, 10),
      forceRefresh: true,
    }).catch(err => {
      console.error('Background governance index build failed:', err);
    });
  } catch (err) {
    console.error('Error starting governance index build:', err);
    res.status(500).json({ error: err.message });
  }
});

/**
 * GET /api/governance/index/status
 * Get indexing status with stats
 */
router.get('/index/status', async (req, res) => {
  try {
    const isIndexing = isGovernanceIndexingInProgress();
    const progress = getIndexingProgress();
    
    // Get stats if available
    let stats = null;
    let cachePopulated = false;
    try {
      stats = await getProposalStats();
      cachePopulated = stats && stats.total > 0;
    } catch {
      // Stats not available yet
    }
    
    res.json({
      isIndexing,
      progress,
      stats: stats ? {
        total: stats.total || 0,
        approved: stats.approved || 0,
        rejected: stats.rejected || 0,
        pending: stats.pending || 0,
        expired: stats.expired || 0,
        expired: stats.expired || 0,
      } : null,
      cachePopulated,
      lastIndexedAt: stats?.lastIndexedAt || null,
    });
  } catch (err) {
    console.error('Error getting governance index status:', err);
    res.status(500).json({ error: err.message });
  }
});

/**
 * POST /api/governance/cache/invalidate
 * Invalidate the proposal cache
 */
router.post('/cache/invalidate', async (req, res) => {
  try {
    invalidateCache();
    res.json({ status: 'ok', message: 'Cache invalidated' });
  } catch (err) {
    console.error('Error invalidating cache:', err);
    res.status(500).json({ error: err.message });
  }
});

/**
 * GET /api/governance/action-types
 * Get list of unique action types from persistent storage
 */
router.get('/action-types', async (req, res) => {
  try {
    const rows = await query(`
      SELECT action_type, COUNT(*) as count
      FROM governance_proposals
      GROUP BY action_type
      ORDER BY count DESC
    `);
    
    const actionTypes = rows.map(row => ({
      type: row.action_type,
      count: Number(row.count),
    }));
    
    res.json(actionTypes);
  } catch (err) {
    console.error('Error getting action types:', err);
    res.status(500).json({ error: err.message });
  }
});

/**
 * POST /api/governance/index/purge
 * Purge the governance index completely (delete all proposals and reset state)
 */
router.post('/index/purge', async (req, res) => {
  try {
    // Use try-catch for each query to handle missing tables gracefully
    try {
      await query(`DELETE FROM governance_proposals`);
    } catch (err) {
      console.log('governance_proposals table may not exist, skipping:', err.message);
    }
    
    try {
      await query(`DELETE FROM governance_index_state`);
    } catch (err) {
      console.log('governance_index_state table may not exist, skipping:', err.message);
    }
    
    console.log('🗑️ Governance index purged');
    res.json({ status: 'ok', message: 'Governance index purged' });
  } catch (err) {
    console.error('Error purging governance index:', err);
    res.status(500).json({ error: err.message || 'Unknown error during purge' });
  }
});

/**
 * GET /api/governance/diagnostics
 * Get diagnostic info about vote_requests data quality (reason URL distribution)
 */
router.get('/diagnostics', async (req, res) => {
  try {
    // Count total vote requests
    const totalResult = await query(`SELECT COUNT(*) as count FROM vote_requests`);
    const total = Number(totalResult[0]?.count || 0);

    // Count by reason URL presence
    const urlDistribution = await query(`
      SELECT 
        CASE 
          WHEN reason IS NULL OR reason = '' OR reason = '""' THEN 'empty'
          WHEN reason LIKE '%"url"%' AND reason NOT LIKE '%"url":""%' AND reason NOT LIKE '%"url": ""%' THEN 'has_url'
          ELSE 'no_url_field'
        END as reason_type,
        COUNT(*) as count
      FROM vote_requests
      GROUP BY 1
      ORDER BY count DESC
    `);

    // Sample some records to show actual reason shapes
    const samples = await query(`
      SELECT 
        contract_id,
        action_tag,
        reason,
        effective_at
      FROM vote_requests
      ORDER BY effective_at DESC
      LIMIT 10
    `);

    // Count by action type to show distribution
    const actionDistribution = await query(`
      SELECT 
        action_tag,
        COUNT(*) as count
      FROM vote_requests
      GROUP BY action_tag
      ORDER BY count DESC
      LIMIT 20
    `);

    res.json({
      total,
      urlDistribution: urlDistribution.map(r => ({ type: r.reason_type, count: Number(r.count) })),
      actionDistribution: actionDistribution.map(r => ({ actionTag: r.action_tag, count: Number(r.count) })),
      samples: samples.map(s => ({
        contractId: s.contract_id,
        actionTag: s.action_tag,
        reason: s.reason,
        effectiveAt: s.effective_at,
      })),
    });
  } catch (err) {
    console.error('Error getting governance diagnostics:', err);
    res.status(500).json({ error: err.message });
  }
});

export default router;
