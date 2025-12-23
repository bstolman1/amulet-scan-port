/**
 * Governance API - Proposal endpoints
 */

import express from 'express';
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
 * Trigger a full index rebuild
 */
router.post('/index/build', async (req, res) => {
  try {
    const { limit = '10000' } = req.query;
    
    // Invalidate cache to force rebuild
    invalidateCache();
    
    const result = await buildGovernanceIndex({
      limit: parseInt(limit, 10),
      forceRefresh: true,
    });
    
    res.json({
      status: 'ok',
      summary: result.summary,
      stats: result.stats,
    });
  } catch (err) {
    console.error('Error building governance index:', err);
    res.status(500).json({ error: err.message });
  }
});

/**
 * GET /api/governance/index/status
 * Get indexing status
 */
router.get('/index/status', async (req, res) => {
  try {
    const inProgress = isGovernanceIndexingInProgress();
    const progress = getIndexingProgress();
    
    res.json({
      indexing: inProgress,
      progress,
    });
  } catch (err) {
    console.error('Error getting index status:', err);
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
 * Get list of unique action types
 */
router.get('/action-types', async (req, res) => {
  try {
    const stats = await getProposalStats();
    const actionTypes = Object.entries(stats.byActionType || {}).map(([type, count]) => ({
      type,
      count,
    }));
    
    // Sort by count descending
    actionTypes.sort((a, b) => b.count - a.count);
    
    res.json(actionTypes);
  } catch (err) {
    console.error('Error getting action types:', err);
    res.status(500).json({ error: err.message });
  }
});

export default router;
