import { Router } from 'express';
import db from '../duckdb/connection.js';
import * as partyIndexer from '../engine/party-indexer.js';
import * as binaryReader from '../duckdb/binary-reader.js';

const router = Router();

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
    
    // No index - return error suggesting to build the index
    res.status(503).json({ 
      error: 'Party index not built. Build the party index first for this query.',
      indexed: false,
      suggestion: 'POST /api/party/index/build to start building the index'
    });
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
    
    // Fallback: scan recent binary files (slow but works without index)
    console.log(`Party ${partyId}: No index, falling back to binary scan...`);
    const result = await binaryReader.streamRecords(db.DATA_PATH, 'events', {
      limit: limit * 10, // Get more to filter
      maxDays: 30,
      filter: (event) => {
        return (event.signatories && event.signatories.includes(partyId)) ||
               (event.observers && event.observers.includes(partyId));
      }
    });
    
    // Filter and limit
    const filtered = result.records.filter(event => 
      (event.signatories && event.signatories.includes(partyId)) ||
      (event.observers && event.observers.includes(partyId))
    ).slice(0, limit);
    
    res.json({ 
      data: filtered, 
      count: filtered.length, 
      party_id: partyId, 
      indexed: false,
      warning: 'Scanning recent files only (last 30 days). Build the party index for complete history.'
    });
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
    
    // No index - return minimal info
    res.json({ 
      data: null, 
      party_id: partyId, 
      indexed: false,
      warning: 'Party index not built. Build the index for summary data.'
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

export default router;
