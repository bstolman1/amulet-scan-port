/**
 * Party API - All queries use DuckDB analytical queries over Parquet files
 * No binary file scanning - Parquet is the single source of truth
 */

import { Router } from 'express';
import db from '../duckdb/connection.js';
import {
  sanitizeNumber,
  escapeString,
  escapeLikePattern,
} from '../lib/sql-sanitize.js';

const router = Router();

// Helper to convert BigInt values to numbers for JSON serialization
function convertBigInts(obj) {
  if (obj === null || obj === undefined) return obj;
  if (typeof obj === 'bigint') return Number(obj);
  if (Array.isArray(obj)) return obj.map(convertBigInts);
  if (typeof obj === 'object') {
    const result = {};
    for (const [key, value] of Object.entries(obj)) {
      result[key] = convertBigInts(value);
    }
    return result;
  }
  return obj;
}

/**
 * Get DuckDB source for events (Parquet preferred)
 */
const getEventsSource = () => {
  if (db.IS_TEST) {
    return `(SELECT * FROM read_json_auto('${db.TEST_FIXTURES_PATH}/events-*.jsonl', union_by_name=true, ignore_errors=true))`;
  }
  
  const basePath = db.DATA_PATH.replace(/\\/g, '/');
  const hasParquet = db.hasFileType('events', '.parquet');
  
  if (hasParquet) {
    return `read_parquet('${basePath}/**/events-*.parquet', union_by_name=true)`;
  }
  
  // Fallback to JSONL
  return `read_json_auto('${basePath}/**/events-*.jsonl', union_by_name=true, ignore_errors=true)`;
};

// GET /api/party/search - Search parties using DuckDB analytical query
router.get('/search', async (req, res) => {
  try {
    const { q, limit = 50 } = req.query;
    
    if (!q) {
      return res.status(400).json({ error: 'Search query required (q parameter)' });
    }
    
    const sanitizedLimit = sanitizeNumber(limit, { min: 1, max: 1000, defaultValue: 50 });
    const escaped = escapeLikePattern(q);
    
    // Use DuckDB UNNEST to search through signatories and observers arrays
    const sql = `
      WITH party_matches AS (
        SELECT DISTINCT UNNEST(signatories) as party
        FROM ${getEventsSource()}
        WHERE array_length(signatories) > 0
        
        UNION
        
        SELECT DISTINCT UNNEST(observers) as party
        FROM ${getEventsSource()}
        WHERE array_length(observers) > 0
      )
      SELECT party
      FROM party_matches
      WHERE party LIKE '%${escaped}%' ESCAPE '\\\\'
      LIMIT ${sanitizedLimit}
    `;
    
    const rows = await db.safeQuery(sql);
    const matches = rows.map(r => r.party);
    
    res.json(convertBigInts({ 
      data: matches, 
      count: matches.length, 
      source: 'parquet',
      engine: 'duckdb',
    }));
  } catch (err) {
    console.error('Party search error:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/party/list/all - Get unique parties via DuckDB analytical query
router.get('/list/all', async (req, res) => {
  try {
    const limit = sanitizeNumber(req.query.limit, { min: 1, max: 10000, defaultValue: 1000 });
    
    // Extract unique parties from signatories and observers
    const sql = `
      WITH all_parties AS (
        SELECT DISTINCT UNNEST(signatories) as party
        FROM ${getEventsSource()}
        WHERE array_length(signatories) > 0
        
        UNION
        
        SELECT DISTINCT UNNEST(observers) as party
        FROM ${getEventsSource()}
        WHERE array_length(observers) > 0
      )
      SELECT party
      FROM all_parties
      WHERE party IS NOT NULL AND party != ''
      ORDER BY party
      LIMIT ${limit}
    `;
    
    const rows = await db.safeQuery(sql);
    const partyList = rows.map(r => r.party);
    
    res.json(convertBigInts({ 
      data: partyList, 
      count: partyList.length, 
      source: 'parquet',
      engine: 'duckdb',
    }));
  } catch (err) {
    console.error('Party list error:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/party/:partyId - Get all events for a party via DuckDB
router.get('/:partyId', async (req, res) => {
  try {
    const { partyId } = req.params;
    const limit = sanitizeNumber(req.query.limit, { min: 1, max: 1000, defaultValue: 100 });
    const offset = sanitizeNumber(req.query.offset, { min: 0, max: 100000, defaultValue: 0 });
    
    const escapedPartyId = escapeString(partyId);
    
    // Query events where party appears in signatories or observers
    const sql = `
      SELECT *
      FROM ${getEventsSource()}
      WHERE list_contains(signatories, '${escapedPartyId}')
         OR list_contains(observers, '${escapedPartyId}')
      ORDER BY COALESCE(timestamp, effective_at) DESC NULLS LAST
      LIMIT ${limit}
      OFFSET ${offset}
    `;
    
    const rows = await db.safeQuery(sql);
    
    res.json(convertBigInts({ 
      data: rows, 
      count: rows.length, 
      party_id: partyId, 
      source: 'parquet',
      engine: 'duckdb',
    }));
  } catch (err) {
    console.error('Party events error:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/party/:partyId/summary - Get party activity summary via DuckDB aggregation
router.get('/:partyId/summary', async (req, res) => {
  try {
    const { partyId } = req.params;
    const escapedPartyId = escapeString(partyId);
    
    // Compute summary statistics using DuckDB aggregation
    const sql = `
      SELECT 
        COUNT(*) as total_events,
        COUNT(DISTINCT template_id) as unique_templates,
        COUNT(DISTINCT contract_id) as unique_contracts,
        MIN(COALESCE(timestamp, effective_at)) as first_seen,
        MAX(COALESCE(timestamp, effective_at)) as last_seen,
        COUNT(CASE WHEN event_type = 'Created' THEN 1 END) as created_count,
        COUNT(CASE WHEN event_type = 'Archived' THEN 1 END) as archived_count
      FROM ${getEventsSource()}
      WHERE list_contains(signatories, '${escapedPartyId}')
         OR list_contains(observers, '${escapedPartyId}')
    `;
    
    const rows = await db.safeQuery(sql);
    const summary = rows[0] || {
      total_events: 0,
      unique_templates: 0,
      unique_contracts: 0,
      first_seen: null,
      last_seen: null,
      created_count: 0,
      archived_count: 0,
    };
    
    res.json(convertBigInts({ 
      data: summary, 
      party_id: partyId, 
      source: 'parquet',
      engine: 'duckdb',
    }));
  } catch (err) {
    console.error('Party summary error:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/party/:partyId/templates - Get template breakdown for a party
router.get('/:partyId/templates', async (req, res) => {
  try {
    const { partyId } = req.params;
    const limit = sanitizeNumber(req.query.limit, { min: 1, max: 100, defaultValue: 50 });
    const escapedPartyId = escapeString(partyId);
    
    const sql = `
      SELECT 
        template_id,
        COUNT(*) as event_count,
        COUNT(CASE WHEN event_type = 'Created' THEN 1 END) as created_count,
        COUNT(CASE WHEN event_type = 'Archived' THEN 1 END) as archived_count,
        MAX(COALESCE(timestamp, effective_at)) as last_activity
      FROM ${getEventsSource()}
      WHERE list_contains(signatories, '${escapedPartyId}')
         OR list_contains(observers, '${escapedPartyId}')
      GROUP BY template_id
      ORDER BY event_count DESC
      LIMIT ${limit}
    `;
    
    const rows = await db.safeQuery(sql);
    
    res.json(convertBigInts({ 
      data: rows, 
      count: rows.length, 
      party_id: partyId, 
      source: 'parquet',
      engine: 'duckdb',
    }));
  } catch (err) {
    console.error('Party templates error:', err);
    res.status(500).json({ error: err.message });
  }
});

export default router;
