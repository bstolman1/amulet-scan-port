import { Router } from 'express';
import db from '../duckdb/connection.js';
import binaryReader from '../duckdb/binary-reader.js';
import { getFilesForTemplate, isTemplateIndexPopulated } from '../engine/template-file-index.js';

const router = Router();

// Reward coupon template names
const REWARD_TEMPLATES = ['AppRewardCoupon', 'ValidatorRewardCoupon', 'SvRewardCoupon'];
const ROUND_TEMPLATES = ['IssuingMiningRound', 'ClosedMiningRound', 'OpenMiningRound'];

/**
 * Extract CC reward amount from a reward coupon payload
 * For reward coupons, the amount is: weight * issuancePerReward
 * If we don't have issuance data, we return the weight as a fallback
 */
function extractRewardAmount(payload, roundIssuance = null) {
  // Direct amount field (some coupons have this)
  if (payload?.amount) {
    return parseFloat(payload.amount);
  }
  if (payload?.initialAmount) {
    return parseFloat(payload.initialAmount);
  }
  
  // For coupons with weight, calculate using issuance rate
  const weight = parseFloat(payload?.weight || 0);
  if (weight > 0 && roundIssuance) {
    // issuance is typically in CC per weight unit
    return weight * roundIssuance;
  }
  
  // Return weight as fallback (will be multiplied by issuance later if available)
  return weight;
}

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
    
    // Parse date range if provided
    const startMs = startDate ? new Date(startDate).getTime() : null;
    const endMs = endDate ? new Date(endDate).getTime() : null;
    
    // Check if we're using date filter or round filter (not both typically needed)
    const useDateFilter = startMs !== null || endMs !== null;
    const useRoundFilter = startR !== null || endR !== null;
    
    // Build filter for reward coupons
    const isRewardForParty = (e) => {
      const templateName = e.template_id || '';
      const isRewardTemplate = REWARD_TEMPLATES.some(t => templateName.includes(t));
      if (!isRewardTemplate) return false;
      if (!e.payload) return false;
      
      const payload = e.payload;
      // Check various fields where party might appear as beneficiary
      if (payload.provider === partyId) return true;
      if (payload.beneficiary === partyId) return true;
      if (payload.owner === partyId) return true;
      if (payload.round?.provider === partyId) return true;
      if (payload.dso === partyId) return true;
      
      return false;
    };
    
    // Date filter
    const passesDateFilter = (e) => {
      if (!useDateFilter) return true;
      if (!e.effective_at) return true;
      const eventMs = new Date(e.effective_at).getTime();
      if (startMs !== null && eventMs < startMs) return false;
      if (endMs !== null && eventMs > endMs) return false;
      return true;
    };
    
    // Round filter
    const passesRoundFilter = (e) => {
      if (!useRoundFilter) return true;
      const roundNum = e.payload?.round?.number ?? e.payload?.round;
      if (roundNum === undefined || roundNum === null) return true;
      const r = typeof roundNum === 'number' ? roundNum : parseInt(roundNum, 10);
      if (startR !== null && r < startR) return false;
      if (endR !== null && r > endR) return false;
      return true;
    };
    
    // Combined filter
    const combinedFilter = (e) => isRewardForParty(e) && passesDateFilter(e) && passesRoundFilter(e);
    
    // Check if template index is available for faster scanning
    const templateIndexPopulated = await isTemplateIndexPopulated();
    let records = [];
    let roundIssuanceMap = new Map(); // round -> issuance rate
    
    if (templateIndexPopulated) {
      // Fast path: use template index - search for all reward coupon types
      console.log('   ⚡ Using template index for fast scanning');
      const allRewardFiles = new Set();
      
      for (const template of REWARD_TEMPLATES) {
        const files = await getFilesForTemplate(template);
        files.forEach(f => allRewardFiles.add(f));
      }
      
      const rewardFiles = Array.from(allRewardFiles);
      console.log(`   📂 Found ${rewardFiles.length} files with reward events`);
      
      // Also get round data for issuance rates
      const roundFiles = new Set();
      for (const template of ROUND_TEMPLATES) {
        const files = await getFilesForTemplate(template);
        files.forEach(f => roundFiles.add(f));
      }
      
      // Build issuance map from round data (sample first few files)
      const roundFileList = Array.from(roundFiles).slice(0, 100);
      for (const file of roundFileList) {
        try {
          const result = await binaryReader.readBinaryFile(file);
          for (const record of (result.records || [])) {
            if (record.event_type === 'created' && record.template_id?.includes('MiningRound')) {
              const roundNum = record.payload?.round?.number ?? record.payload?.round;
              const issuance = parseFloat(record.payload?.issuancePerSvRewardCoupon || 
                                          record.payload?.issuancePerValidatorRewardCoupon ||
                                          record.payload?.issuancePerAppRewardCoupon || 0);
              if (roundNum && issuance) {
                roundIssuanceMap.set(roundNum, issuance);
              }
            }
          }
        } catch (err) {
          // Skip files that can't be read
        }
      }
      console.log(`   📊 Built issuance map for ${roundIssuanceMap.size} rounds`);
      
      // Scan reward files with filter
      for (const file of rewardFiles) {
        try {
          const result = await binaryReader.readBinaryFile(file);
          const fileRecords = result.records || [];
          
          for (const record of fileRecords) {
            if (record.event_type === 'created' && combinedFilter(record)) {
              records.push(record);
            }
          }
        } catch (err) {
          console.warn(`   ⚠️ Error reading file ${file}: ${err.message}`);
        }
        
        // Limit to prevent timeout
        if (records.length > 10000) {
          console.log('   ⚠️ Hit 10k record limit, stopping scan');
          break;
        }
      }
    } else {
      // Slow path: full scan
      console.log('   📂 Template index not available, using full scan');
      const result = await binaryReader.streamRecords(db.DATA_PATH, 'events', {
        limit: 10000,
        offset: 0,
        maxDays: 365 * 3,
        maxFilesToScan: 5000,
        fullScan: true,
        sortBy: 'effective_at',
        filter: (e) => e.event_type === 'created' && combinedFilter(e),
      });
      records = result.records || [];
    }
    
    console.log(`   Found ${records.length} reward events for party`);
    
    // Calculate totals
    let totalRewards = 0;
    let totalWeight = 0;
    const byRound = {};
    const events = [];
    
    for (const record of records) {
      const payload = record.payload || {};
      const roundNum = payload.round?.number ?? payload.round ?? 0;
      const roundKey = String(roundNum);
      
      // Get issuance for this round if available
      const issuance = roundIssuanceMap.get(roundNum) || null;
      
      // Extract amount (uses weight * issuance if available)
      const amount = extractRewardAmount(payload, issuance);
      const weight = parseFloat(payload.weight || 0);
      
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

export default router;
