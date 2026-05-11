/**
 * Vote Results API — serves historical vote results from DuckDB
 *
 * GET  /vote-results         — read stored results (local-first for the dashboard)
 * POST /vote-results/sync    — trigger a one-off sync from Scan API → DuckDB
 * GET  /vote-results/status  — how many results are stored, last sync time
 */

import { Router } from 'express';
import { getStoredVoteResults, getVoteResultCount, upsertVoteResults } from '../lib/vote-result-store.js';
import { getCurrentEndpoint } from '../lib/endpoint-rotation.js';
import { extractHostname, createDispatcher } from '../lib/undici-dispatcher.js';

const router = Router();

/**
 * GET /vote-results
 *
 * Returns raw VoteResult objects from DuckDB so the frontend can apply its
 * existing parseVoteResults() transformation unchanged.
 *
 * Query params:
 *   limit      — max rows (default 500)
 *   status     — filter: accepted | rejected | expired
 *   actionTag  — filter by action.tag
 */
router.get('/', async (req, res) => {
  try {
    const limit = Math.min(Math.max(parseInt(req.query.limit, 10) || 500, 1), 5000);
    const status = req.query.status || undefined;
    const actionTag = req.query.actionTag || undefined;

    const results = await getStoredVoteResults({ limit, status, actionTag });

    res.json({
      dso_rules_vote_results: results,
      count: results.length,
      source: 'duckdb',
    });
  } catch (err) {
    console.error('[vote-results] GET / error:', err.message);
    res.status(500).json({ error: err.message, source: 'duckdb' });
  }
});

/**
 * GET /vote-results/status
 *
 * Quick health check: how many results are stored locally.
 */
router.get('/status', async (req, res) => {
  try {
    const count = await getVoteResultCount();
    res.json({ stored: count, source: 'duckdb' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/**
 * POST /vote-results/sync
 *
 * Fetches all vote results from the live Scan API and upserts into DuckDB.
 * Intended to be called manually or by the backfill script, not by the
 * frontend on every page load.
 */
router.post('/sync', async (req, res) => {
  try {
    const limit = parseInt(req.body?.limit, 10) || 1000;

    const endpoint = getCurrentEndpoint();
    const hostname = extractHostname(endpoint.url);
    const dispatcher = hostname ? createDispatcher(hostname) : undefined;

    const scanUrl = `${endpoint.url}/v0/admin/sv/voteresults`;
    console.log(`[vote-results] Syncing up to ${limit} results from ${endpoint.name}...`);

    const scanRes = await fetch(scanUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Accept: 'application/json',
        ...(hostname ? { Host: hostname } : {}),
      },
      body: JSON.stringify({ limit }),
      ...(dispatcher ? { dispatcher } : {}),
      signal: AbortSignal.timeout(30_000),
    });

    if (!scanRes.ok) {
      const text = await scanRes.text();
      return res.status(502).json({
        error: `Scan API returned ${scanRes.status}`,
        details: text.slice(0, 500),
      });
    }

    const data = await scanRes.json();
    const rawResults = data.dso_rules_vote_results || [];

    const { inserted, skipped } = await upsertVoteResults(rawResults);
    const total = await getVoteResultCount();

    console.log(`[vote-results] Sync complete: ${inserted} upserted, ${skipped} skipped, ${total} total stored`);

    res.json({
      synced: inserted,
      skipped,
      totalStored: total,
      fetchedFromApi: rawResults.length,
      endpoint: endpoint.name,
    });
  } catch (err) {
    console.error('[vote-results] POST /sync error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

export default router;
