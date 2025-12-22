import { Router } from 'express';
import db from '../duckdb/connection.js';
import * as partyIndexer from '../engine/party-indexer.js';

const router = Router();

// Helper to get the correct read function for JSONL files (supports .jsonl, .jsonl.gz, .jsonl.zst)
// Uses UNION for cross-platform compatibility (Windows doesn't support brace expansion)
// Use UNION (not UNION ALL) to prevent duplicate records
const getUpdatesSource = () => `(
  SELECT * FROM read_json_auto('${db.DATA_PATH}/**/updates-*.jsonl', union_by_name=true, ignore_errors=true)
  UNION
  SELECT * FROM read_json_auto('${db.DATA_PATH}/**/updates-*.jsonl.gz', union_by_name=true, ignore_errors=true)
  UNION
  SELECT * FROM read_json_auto('${db.DATA_PATH}/**/updates-*.jsonl.zst', union_by_name=true, ignore_errors=true)
)`;

// GET /api/party/index/status - Get party index status
router.get('/index/status', async (req, res) => {
  try {
    const stats = partyIndexer.getPartyIndexStats();
    const progress = partyIndexer.getIndexingProgress();
    res.json({ ...stats, indexing: progress });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/party/index/build - Build party index
router.post('/index/build', async (req, res) => {
  try {
    const { forceRebuild } = req.body || {};
    
    // Start indexing in background
    partyIndexer.buildPartyIndex({ forceRebuild }).then(result => {
      console.log('Party index build completed:', result);
    }).catch(err => {
      console.error('Party index build failed:', err);
    });
    
    res.json({ status: 'started', message: 'Party index build started in background' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/party/search - Search parties by prefix
router.get('/search', async (req, res) => {
  try {
    const { q, limit = 50 } = req.query;
    
    if (!q) {
      return res.status(400).json({ error: 'Search query required (q parameter)' });
    }
    
    const matches = partyIndexer.searchPartiesByPrefix(q, parseInt(limit));
    res.json({ data: matches, count: matches.length, indexed: partyIndexer.isIndexPopulated() });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/party/list/all - Get all unique parties
router.get('/list/all', async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 1000, 10000);
    
    // If party index is populated, use it (much faster)
    if (partyIndexer.isIndexPopulated()) {
      const index = partyIndexer.loadPartyIndex();
      const parties = Array.from(index.keys()).slice(0, limit);
      return res.json({ data: parties, count: parties.length, indexed: true });
    }
    
    // Fallback to slow JSONL scan
    const sql = `
      WITH all_parties AS (
        SELECT DISTINCT unnest(signatories) as party_id
        FROM ${getUpdatesSource()}
        UNION
        SELECT DISTINCT unnest(observers) as party_id
        FROM ${getUpdatesSource()}
      )
      SELECT party_id FROM all_parties
      WHERE party_id IS NOT NULL
      LIMIT ${limit}
    `;
    
    const rows = await db.safeQuery(sql);
    res.json({ data: rows.map(r => r.party_id), count: rows.length, indexed: false });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/party/:partyId - Get all events for a party
router.get('/:partyId', async (req, res) => {
  try {
    const { partyId } = req.params;
    const limit = Math.min(parseInt(req.query.limit) || 100, 1000);
    const useIndex = req.query.index !== 'false';
    
    // If party index is populated and requested, use it (much faster)
    if (useIndex && partyIndexer.isIndexPopulated()) {
      const result = await partyIndexer.getPartyEventsFromIndex(partyId, limit);
      return res.json({ 
        data: result.events, 
        count: result.events.length,
        total: result.total,
        party_id: partyId,
        indexed: true,
        filesScanned: result.filesScanned,
      });
    }
    
    // Fallback to slow JSONL scan
    const sql = `
      SELECT *
      FROM ${getUpdatesSource()}
      WHERE 
        list_contains(signatories, '${partyId}')
        OR list_contains(observers, '${partyId}')
      ORDER BY timestamp DESC
      LIMIT ${limit}
    `;
    
    const rows = await db.safeQuery(sql);
    res.json({ data: rows, count: rows.length, party_id: partyId, indexed: false });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/party/:partyId/summary - Get party activity summary
router.get('/:partyId/summary', async (req, res) => {
  try {
    const { partyId } = req.params;
    const useIndex = req.query.index !== 'false';
    
    // If party index is populated, get quick summary
    if (useIndex && partyIndexer.isIndexPopulated()) {
      const summary = partyIndexer.getPartySummaryFromIndex(partyId);
      if (summary) {
        return res.json({ data: summary, party_id: partyId, indexed: true });
      }
    }
    
    // Fallback to slow JSONL scan
    const sql = `
      SELECT 
        event_type,
        COUNT(*) as count,
        MIN(timestamp) as first_seen,
        MAX(timestamp) as last_seen
      FROM ${getUpdatesSource()}
      WHERE 
        list_contains(signatories, '${partyId}')
        OR list_contains(observers, '${partyId}')
      GROUP BY event_type
      ORDER BY count DESC
    `;
    
    const rows = await db.safeQuery(sql);
    res.json({ data: rows, party_id: partyId, indexed: false });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

export default router;
