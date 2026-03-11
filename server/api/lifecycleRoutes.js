/**
 * Core lifecycle routes.
 *
 * GET  /                  - serve from cache (or fetch fresh)
 * POST /refresh           - explicitly fetch fresh data
 * GET  /cache-info        - metadata about the cached dataset
 * GET  /enrichment-status - progress of the background message URL enrichment job
 * GET  /cip-list          - list of all CIP items
 * GET  /card-list         - list of all cards (for move-to dropdown)
 */

import { Router } from 'express';
import { readCache, readEnrichmentStatus } from './fileRepository.js';
import { applyOverrides } from './overrideService.js';
import { fetchFreshData } from './dataFetcher.js';
import { fixLifecycleItemTypes } from './lifecycleCorrelator.js';

const router = Router();

const getApiKey = () => process.env.GROUPS_IO_API_KEY;

// ── GET / ─────────────────────────────────────────────────────────────────

router.get('/', async (req, res) => {
  const forceRefresh = req.query.refresh === 'true';

  if (!forceRefresh) {
    const cached = await readCache();
    if (cached) {
      console.log(`Serving cached governance data from ${cached.cachedAt}`);
      const fixed = fixLifecycleItemTypes(cached);
      const withOverrides = await applyOverrides(fixed);
      return res.json(withOverrides);
    }
  }

  if (!getApiKey()) {
    const stale = await readCache();
    if (stale) {
      const withOverrides = await applyOverrides(stale);
      return res.json({
        ...withOverrides,
        stale: true,
        warning: 'GROUPS_IO_API_KEY not configured — showing cached data',
      });
    }
    return res.json({
      lifecycleItems: [],
      allTopics: [],
      groups: {},
      stats: { totalTopics: 0, lifecycleItems: 0, byType: {}, byStage: {}, groupCounts: {} },
      warning: 'GROUPS_IO_API_KEY not configured. Please set the API key or load cached data.',
    });
  }

  try {
    const data = await fetchFreshData();
    const withOverrides = await applyOverrides(data);
    return res.json(withOverrides);
  } catch (error) {
    console.error('Error fetching governance lifecycle:', error);
    const stale = await readCache();
    if (stale) {
      const withOverrides = await applyOverrides(stale);
      return res.json({ ...withOverrides, stale: true, error: error.message });
    }
    return res.status(500).json({ error: error.message, lifecycleItems: [], groups: {} });
  }
});

// ── POST /refresh ─────────────────────────────────────────────────────────

async function handleRefresh(req, res) {
  if (!getApiKey()) {
    return res.status(500).json({ error: 'GROUPS_IO_API_KEY not configured' });
  }
  try {
    const data = await fetchFreshData();
    return res.json({
      success: true,
      stats: data.stats,
      cachedAt: data.cachedAt,
      // Let the caller know enrichment is running in the background
      enrichment: 'started',
      enrichmentStatusUrl: '/api/governance-lifecycle/enrichment-status',
    });
  } catch (error) {
    console.error('Error refreshing governance lifecycle:', error);
    return res.status(500).json({ error: error.message });
  }
}

router.post('/refresh', handleRefresh);

// ── GET /enrichment-status ────────────────────────────────────────────────

/**
 * Returns the current state of the background message URL enrichment job.
 *
 * Poll this after triggering a refresh to know when enrichment is complete
 * and the full linkedUrls data (including /message/NNN URLs) is available.
 *
 * Response shape:
 * {
 *   state: 'running' | 'complete' | 'aborted' | 'error' | 'never_run',
 *   total: number,       // announce-group topics to enrich
 *   processed: number,   // topics attempted so far
 *   enriched: number,    // topics that got ≥1 message URL
 *   totalMessages: number,
 *   startedAt: string,
 *   completedAt: string | null,
 *   error: string | null,
 * }
 */
router.get('/enrichment-status', async (req, res) => {
  const status = await readEnrichmentStatus();
  if (!status) {
    return res.json({ state: 'never_run', total: 0, processed: 0, enriched: 0, totalMessages: 0 });
  }
  return res.json(status);
});

// ── GET /cache-info ───────────────────────────────────────────────────────

router.get('/cache-info', async (req, res) => {
  const cached = await readCache();
  if (cached) {
    return res.json({ hasCachedData: true, cachedAt: cached.cachedAt, stats: cached.stats });
  }
  return res.json({ hasCachedData: false });
});

// ── GET /cip-list ─────────────────────────────────────────────────────────

router.get('/cip-list', async (req, res) => {
  const cached = await readCache();
  if (!cached?.lifecycleItems) return res.json({ cips: [] });

  const cips = cached.lifecycleItems
    .filter(item => item.type === 'cip' && /^CIP-\d+$/i.test(item.primaryId))
    .map(item => ({
      primaryId: item.primaryId,
      firstDate: item.firstDate,
      lastDate: item.lastDate,
      topicCount: item.topics.length,
    }))
    .sort((a, b) => {
      const na = parseInt(a.primaryId.match(/\d+/)?.[0] ?? '0');
      const nb = parseInt(b.primaryId.match(/\d+/)?.[0] ?? '0');
      return nb - na;
    });

  return res.json({ cips });
});

// ── GET /card-list ────────────────────────────────────────────────────────

router.get('/card-list', async (req, res) => {
  const cached = await readCache();
  if (!cached?.lifecycleItems) return res.json({ cards: [] });

  const cards = cached.lifecycleItems
    .map(item => ({
      id: item.id,
      primaryId: item.primaryId,
      type: item.type,
      firstDate: item.firstDate,
      lastDate: item.lastDate,
      topicCount: item.topics.length,
      preview: item.topics[0]?.subject?.slice(0, 60) ?? '',
    }))
    .sort((a, b) => {
      if (a.type !== b.type) return a.type.localeCompare(b.type);
      return new Date(b.lastDate) - new Date(a.lastDate);
    });

  return res.json({ cards });
});

export default router;
