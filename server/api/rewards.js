import { Router } from 'express';
import db from '../duckdb/connection.js';
import binaryReader from '../duckdb/binary-reader.js';
import { getFilesForTemplate, isTemplateIndexPopulated } from '../engine/template-file-index.js';

const router = Router();

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
    
    // Build filters
    const filters = [];
    
    // Party filter - check in payload for provider/beneficiary
    filters.push((e) => {
      if (!e.template_id?.includes('RewardCoupon')) return false;
      if (!e.payload) return false;
      
      // Check various fields where party might appear
      const payload = e.payload;
      if (payload.provider === partyId) return true;
      if (payload.beneficiary === partyId) return true;
      if (payload.owner === partyId) return true;
      if (payload.round?.provider === partyId) return true;
      
      // Check in dso field
      if (payload.dso === partyId) return true;
      
      return false;
    });
    
    // Date filter
    if (startDate || endDate) {
      const startMs = startDate ? new Date(startDate).getTime() : 0;
      const endMs = endDate ? new Date(endDate).getTime() : Date.now();
      
      filters.push((e) => {
        if (!e.effective_at) return true; // Include if no date
        const eventMs = new Date(e.effective_at).getTime();
        return eventMs >= startMs && eventMs <= endMs;
      });
    }
    
    // Round filter
    if (startRound || endRound) {
      const startR = startRound ? parseInt(startRound, 10) : 0;
      const endR = endRound ? parseInt(endRound, 10) : Number.MAX_SAFE_INTEGER;
      
      filters.push((e) => {
        // Round is typically in payload.round.number
        const roundNum = e.payload?.round?.number ?? e.payload?.round;
        if (roundNum === undefined || roundNum === null) return true;
        const r = typeof roundNum === 'number' ? roundNum : parseInt(roundNum, 10);
        return r >= startR && r <= endR;
      });
    }
    
    // Combined filter function
    const combinedFilter = (e) => filters.every(f => f(e));
    
    // Check if template index is available for faster scanning
    const templateIndexPopulated = await isTemplateIndexPopulated();
    let records = [];
    
    if (templateIndexPopulated) {
      // Fast path: use template index
      console.log('   ⚡ Using template index for fast scanning');
      const rewardFiles = await getFilesForTemplate('RewardCoupon');
      console.log(`   📂 Found ${rewardFiles.length} files with RewardCoupon events`);
      
      // Scan files with filter
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
    const byRound = {};
    const events = [];
    
    for (const record of records) {
      // Extract amount from payload
      const amount = parseFloat(record.payload?.amount || record.payload?.initialAmount || 0);
      const roundNum = record.payload?.round?.number ?? record.payload?.round ?? 0;
      
      totalRewards += amount;
      
      // Group by round
      const roundKey = String(roundNum);
      if (!byRound[roundKey]) {
        byRound[roundKey] = { count: 0, amount: 0 };
      }
      byRound[roundKey].count++;
      byRound[roundKey].amount += amount;
      
      // Add to events list (limit to 500 for response size)
      if (events.length < 500) {
        events.push({
          event_id: record.event_id,
          round: roundNum,
          amount,
          effective_at: record.effective_at,
          template_id: record.template_id,
        });
      }
    }
    
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(2);
    console.log(`   ✅ Calculation complete in ${elapsed}s: ${records.length} events, ${totalRewards.toFixed(6)} total`);
    
    res.json({
      partyId,
      totalRewards,
      rewardCount: records.length,
      byRound,
      events,
      queryTime: parseFloat(elapsed),
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
      { name: 'RewardCoupon', description: 'General reward coupons' },
    ],
  });
});

export default router;
