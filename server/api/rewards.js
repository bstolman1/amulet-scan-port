/**
 * Rewards API - DuckDB Parquet Only
 * 
 * Data Authority: All queries use DuckDB over Parquet files.
 * See docs/architecture.md for the Data Authority Contract.
 */

import { Router } from 'express';
import { 
  safeQuery, 
  hasFileType, 
  DATA_PATH, 
  IS_TEST, 
  TEST_FIXTURES_PATH 
} from '../duckdb/connection.js';

const router = Router();

// Reward coupon template names
const REWARD_TEMPLATES = ['AppRewardCoupon', 'ValidatorRewardCoupon', 'SvRewardCoupon'];
const ROUND_TEMPLATES = ['IssuingMiningRound', 'ClosedMiningRound', 'OpenMiningRound'];

/**
 * Get the SQL source for events data
 * Prefers Parquet, falls back to JSONL
 */
const getEventsSource = () => {
  if (IS_TEST) {
    return `(SELECT * FROM read_json_auto('${TEST_FIXTURES_PATH}/events-*.jsonl', union_by_name=true, ignore_errors=true))`;
  }
  
  const basePath = DATA_PATH.replace(/\\/g, '/');
  
  if (hasFileType('events', '.parquet')) {
    return `read_parquet('${basePath}/**/events-*.parquet', union_by_name=true)`;
  }

  const hasJsonl = hasFileType('events', '.jsonl');
  const hasGzip = hasFileType('events', '.jsonl.gz');
  const hasZstd = hasFileType('events', '.jsonl.zst');

  if (!hasJsonl && !hasGzip && !hasZstd) {
    return `(SELECT NULL::VARCHAR as event_id, NULL::VARCHAR as event_type, NULL::VARCHAR as contract_id, 
             NULL::VARCHAR as template_id, NULL::TIMESTAMP as timestamp, NULL::JSON as payload WHERE false)`;
  }

  const queries = [];
  if (hasJsonl) queries.push(`SELECT * FROM read_json_auto('${basePath}/**/events-*.jsonl', union_by_name=true, ignore_errors=true)`);
  if (hasGzip) queries.push(`SELECT * FROM read_json_auto('${basePath}/**/events-*.jsonl.gz', union_by_name=true, ignore_errors=true)`);
  if (hasZstd) queries.push(`SELECT * FROM read_json_auto('${basePath}/**/events-*.jsonl.zst', union_by_name=true, ignore_errors=true)`);

  return `(${queries.join(' UNION ')})`;
};

/**
 * GET /api/rewards/calculate
 * Calculate app rewards for a specific party ID within a date or round range
 * 
 * Query params:
 * - partyId: string (required) - The party ID to calculate rewards for
 * - startDate: ISO date string (optional) - Start of date range
 * - endDate: ISO date string (optional) - End of date range
 * - startRound: number (optional) - Start round number
 * - endRound: number (optional) - End round number
 */
router.get('/calculate', async (req, res) => {
  try {
    const { partyId, startDate, endDate, startRound, endRound } = req.query;
    
    if (!partyId) {
      return res.status(400).json({ error: 'partyId is required' });
    }
    
    console.log(`\n💰 REWARD CALCULATION: partyId=${partyId}`);
    console.log(`   Date range: ${startDate || 'none'} to ${endDate || 'none'}`);
    console.log(`   Round range: ${startRound || 'none'} to ${endRound || 'none'}`);
    
    const startTime = Date.now();
    
    // Parse round range if provided
    const startR = startRound ? parseInt(startRound, 10) : null;
    const endR = endRound ? parseInt(endRound, 10) : null;
    
    // Build template filter SQL
    const templateConditions = REWARD_TEMPLATES.map(t => `template_id LIKE '%${t}%'`).join(' OR ');
    
    // Build party filter SQL - check various fields where party might appear as beneficiary
    const partyCondition = `(
      json_extract_string(payload, '$.provider') = '${partyId}'
      OR json_extract_string(payload, '$.beneficiary') = '${partyId}'
      OR json_extract_string(payload, '$.owner') = '${partyId}'
      OR json_extract_string(payload, '$.round.provider') = '${partyId}'
      OR json_extract_string(payload, '$.dso') = '${partyId}'
    )`;
    
    // Build date filter SQL
    let dateCondition = '';
    if (startDate) {
      dateCondition += ` AND COALESCE(timestamp, effective_at) >= '${startDate}'`;
    }
    if (endDate) {
      dateCondition += ` AND COALESCE(timestamp, effective_at) <= '${endDate}'`;
    }
    
    // Build round filter SQL
    let roundCondition = '';
    if (startR !== null) {
      roundCondition += ` AND COALESCE(
        CAST(json_extract(payload, '$.round.number') AS INTEGER),
        CAST(json_extract(payload, '$.round') AS INTEGER)
      ) >= ${startR}`;
    }
    if (endR !== null) {
      roundCondition += ` AND COALESCE(
        CAST(json_extract(payload, '$.round.number') AS INTEGER),
        CAST(json_extract(payload, '$.round') AS INTEGER)
      ) <= ${endR}`;
    }
    
    // First, get issuance rates from mining rounds
    const issuanceQuery = `
      SELECT 
        COALESCE(
          CAST(json_extract(payload, '$.round.number') AS INTEGER),
          CAST(json_extract(payload, '$.round') AS INTEGER)
        ) as round_number,
        COALESCE(
          CAST(json_extract(payload, '$.issuancePerSvRewardCoupon') AS DOUBLE),
          CAST(json_extract(payload, '$.issuancePerValidatorRewardCoupon') AS DOUBLE),
          CAST(json_extract(payload, '$.issuancePerAppRewardCoupon') AS DOUBLE),
          0
        ) as issuance_rate
      FROM ${getEventsSource()}
      WHERE event_type = 'created'
        AND (${ROUND_TEMPLATES.map(t => `template_id LIKE '%${t}%'`).join(' OR ')})
      LIMIT 1000
    `;
    
    const issuanceRows = await safeQuery(issuanceQuery);
    const roundIssuanceMap = new Map();
    for (const row of issuanceRows) {
      if (row.round_number && row.issuance_rate > 0) {
        roundIssuanceMap.set(row.round_number, row.issuance_rate);
      }
    }
    console.log(`   📊 Built issuance map for ${roundIssuanceMap.size} rounds`);
    
    // Query reward events
    const rewardsQuery = `
      SELECT 
        event_id,
        template_id,
        COALESCE(timestamp, effective_at) as effective_at,
        payload,
        COALESCE(
          CAST(json_extract(payload, '$.round.number') AS INTEGER),
          CAST(json_extract(payload, '$.round') AS INTEGER),
          0
        ) as round_number,
        COALESCE(
          CAST(json_extract(payload, '$.amount') AS DOUBLE),
          CAST(json_extract(payload, '$.initialAmount') AS DOUBLE),
          0
        ) as direct_amount,
        COALESCE(CAST(json_extract(payload, '$.weight') AS DOUBLE), 0) as weight
      FROM ${getEventsSource()}
      WHERE event_type = 'created'
        AND (${templateConditions})
        AND ${partyCondition}
        ${dateCondition}
        ${roundCondition}
      ORDER BY effective_at DESC
      LIMIT 10000
    `;
    
    const records = await safeQuery(rewardsQuery);
    console.log(`   Found ${records.length} reward events for party`);
    
    // Calculate totals
    let totalRewards = 0;
    let totalWeight = 0;
    const byRound = {};
    const events = [];
    
    for (const record of records) {
      const roundNum = record.round_number || 0;
      const roundKey = String(roundNum);
      const weight = record.weight || 0;
      
      // Get issuance for this round if available
      const issuance = roundIssuanceMap.get(roundNum) || null;
      
      // Calculate amount (uses weight * issuance if available, otherwise direct amount or weight)
      let amount = record.direct_amount || 0;
      if (amount === 0 && weight > 0 && issuance) {
        amount = weight * issuance;
      } else if (amount === 0) {
        amount = weight; // Fallback to weight
      }
      
      totalRewards += amount;
      totalWeight += weight;
      
      // Group by round
      if (!byRound[roundKey]) {
        byRound[roundKey] = { count: 0, amount: 0, weight: 0 };
      }
      byRound[roundKey].count++;
      byRound[roundKey].amount += amount;
      byRound[roundKey].weight += weight;
      
      // Add to events list (limit to 500 for response size)
      if (events.length < 500) {
        events.push({
          event_id: record.event_id,
          round: roundNum,
          amount,
          weight,
          effective_at: record.effective_at,
          template_id: record.template_id,
          templateType: record.template_id?.split(':').pop() || 'Unknown',
        });
      }
    }
    
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(2);
    console.log(`   ✅ Calculation complete in ${elapsed}s: ${records.length} events, ${totalRewards.toFixed(6)} CC total`);
    
    res.json({
      partyId,
      totalRewards,
      totalWeight,
      rewardCount: records.length,
      byRound,
      events,
      queryTime: parseFloat(elapsed),
      hasIssuanceData: roundIssuanceMap.size > 0,
      note: roundIssuanceMap.size === 0 ? 'Amounts shown as weights (issuance data not available)' : null,
      data_source: 'parquet',
    });
    
  } catch (err) {
    console.error('Error calculating rewards:', err);
    res.status(500).json({ error: err.message });
  }
});

/**
 * GET /api/rewards/templates
 * List available reward templates for reference
 */
router.get('/templates', async (req, res) => {
  res.json({
    templates: [
      { name: 'AppRewardCoupon', description: 'App provider rewards' },
      { name: 'ValidatorRewardCoupon', description: 'Validator rewards' },
      { name: 'SvRewardCoupon', description: 'Super Validator rewards' },
    ],
  });
});

/**
 * GET /api/rewards/by-round
 * Get aggregate reward stats by round
 */
router.get('/by-round', async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 100, 1000);
    const templateConditions = REWARD_TEMPLATES.map(t => `template_id LIKE '%${t}%'`).join(' OR ');
    
    const sql = `
      SELECT 
        COALESCE(
          CAST(json_extract(payload, '$.round.number') AS INTEGER),
          CAST(json_extract(payload, '$.round') AS INTEGER),
          0
        ) as round_number,
        COUNT(*) as reward_count,
        SUM(COALESCE(CAST(json_extract(payload, '$.weight') AS DOUBLE), 0)) as total_weight,
        COUNT(DISTINCT json_extract_string(payload, '$.provider')) as unique_providers
      FROM ${getEventsSource()}
      WHERE event_type = 'created'
        AND (${templateConditions})
      GROUP BY round_number
      ORDER BY round_number DESC
      LIMIT ${limit}
    `;
    
    const rows = await safeQuery(sql);
    res.json({ data: rows, data_source: 'parquet' });
  } catch (err) {
    console.error('Error fetching rewards by round:', err);
    res.status(500).json({ error: err.message });
  }
});

export default router;
