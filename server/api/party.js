import { Router } from 'express';
import db from '../duckdb/connection.js';
import * as binaryReader from '../duckdb/binary-reader.js';

const router = Router();

// GET /api/party/search - Search parties (basic scan fallback)
router.get('/search', async (req, res) => {
  try {
    const { q, limit = 50 } = req.query;
    
    if (!q) {
      return res.status(400).json({ error: 'Search query required (q parameter)' });
    }
    
    // Scan recent binary files for matching parties
    const result = await binaryReader.streamRecords(db.DATA_PATH, 'events', {
      limit: 1000,
      maxDays: 30,
      filter: (event) => {
        const sigMatch = event.signatories?.some(s => s.includes(q));
        const obsMatch = event.observers?.some(o => o.includes(q));
        return sigMatch || obsMatch;
      }
    });
    
    // Extract unique parties matching the query
    const matchingParties = new Set();
    for (const event of result.records) {
      for (const sig of (event.signatories || [])) {
        if (sig.includes(q)) matchingParties.add(sig);
      }
      for (const obs of (event.observers || [])) {
        if (obs.includes(q)) matchingParties.add(obs);
      }
    }
    
    const matches = Array.from(matchingParties).slice(0, parseInt(limit));
    res.json({ data: matches, count: matches.length, indexed: false });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/party/list/all - Get unique parties (scan fallback)
router.get('/list/all', async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 1000, 10000);
    
    // Scan recent files to find parties
    const result = await binaryReader.streamRecords(db.DATA_PATH, 'events', {
      limit: 5000,
      maxDays: 30,
    });
    
    const parties = new Set();
    for (const event of result.records) {
      for (const sig of (event.signatories || [])) parties.add(sig);
      for (const obs of (event.observers || [])) parties.add(obs);
    }
    
    const partyList = Array.from(parties).slice(0, limit);
    res.json({ data: partyList, count: partyList.length, indexed: false });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/party/:partyId - Get all events for a party
router.get('/:partyId', async (req, res) => {
  try {
    const { partyId } = req.params;
    const limit = Math.min(parseInt(req.query.limit) || 100, 1000);
    
    // Scan recent binary files
    console.log(`Party ${partyId}: Scanning binary files...`);
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
      warning: 'Scanning recent files only (last 30 days).'
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/party/:partyId/summary - Get party activity summary
router.get('/:partyId/summary', async (req, res) => {
  try {
    const { partyId } = req.params;
    
    // Return minimal info without index
    res.json({ 
      data: null, 
      party_id: partyId, 
      indexed: false,
      warning: 'Party summary requires scanning historical data.'
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

export default router;
