/**
 * RewardCoupon Indexer - Builds persistent DuckDB index for reward coupon events
 * 
 * Scans binary files for AppRewardCoupon, ValidatorRewardCoupon, and SvRewardCoupon
 * created events and maintains a persistent table for instant reward calculations.
 * 
 * Pre-calculates CC amounts by joining with round issuance data.
 * Uses template-to-file index when available to dramatically reduce scan time.
 */

import { query, queryOne, DATA_PATH } from '../duckdb/connection.js';
import * as binaryReader from '../duckdb/binary-reader.js';
import { 
  getFilesForTemplate, 
  isTemplateIndexPopulated,
  getTemplateIndexStats
} from './template-file-index.js';

let indexingInProgress = false;
let indexingProgress = { current: 0, total: 0, startTime: null };

// Reward coupon template names
const REWARD_TEMPLATES = ['AppRewardCoupon', 'ValidatorRewardCoupon', 'SvRewardCoupon'];
const ROUND_TEMPLATES = ['IssuingMiningRound', 'ClosedMiningRound', 'OpenMiningRound'];

/**
 * Get current indexing state
 */
export async function getIndexState() {
  try {
    const state = await queryOne(`
      SELECT last_indexed_file, last_indexed_at, total_indexed 
      FROM reward_coupon_index_state 
      WHERE id = 1
    `);
    return state || { last_indexed_file: null, last_indexed_at: null, total_indexed: 0 };
  } catch (err) {
    return { last_indexed_file: null, last_indexed_at: null, total_indexed: 0 };
  }
}

/**
 * Get reward coupon counts from the index
 */
export async function getRewardCouponStats() {
  try {
    const total = await queryOne(`SELECT COUNT(*) as count FROM reward_coupons`);
    const app = await queryOne(`SELECT COUNT(*) as count FROM reward_coupons WHERE coupon_type = 'App'`);
    const validator = await queryOne(`SELECT COUNT(*) as count FROM reward_coupons WHERE coupon_type = 'Validator'`);
    const sv = await queryOne(`SELECT COUNT(*) as count FROM reward_coupons WHERE coupon_type = 'SV'`);
    const totalCC = await queryOne(`SELECT COALESCE(SUM(cc_amount), 0) as total FROM reward_coupons`);
    
    return {
      total: Number(total?.count || 0),
      app: Number(app?.count || 0),
      validator: Number(validator?.count || 0),
      sv: Number(sv?.count || 0),
      totalCC: Number(totalCC?.total || 0),
    };
  } catch (err) {
    console.error('Error getting reward coupon stats:', err);
    return { total: 0, app: 0, validator: 0, sv: 0, totalCC: 0 };
  }
}

/**
 * Query reward coupons from the persistent index
 */
export async function queryRewardCoupons({ 
  limit = 100, 
  offset = 0, 
  couponType = null,
  beneficiary = null,
  startRound = null,
  endRound = null,
  startDate = null,
  endDate = null,
} = {}) {
  let whereClause = 'WHERE 1=1';
  
  if (couponType) {
    whereClause += ` AND coupon_type = '${couponType}'`;
  }
  if (beneficiary) {
    whereClause += ` AND beneficiary = '${beneficiary.replace(/'/g, "''")}'`;
  }
  if (startRound !== null) {
    whereClause += ` AND round >= ${parseInt(startRound, 10)}`;
  }
  if (endRound !== null) {
    whereClause += ` AND round <= ${parseInt(endRound, 10)}`;
  }
  if (startDate) {
    whereClause += ` AND effective_at >= '${startDate}'`;
  }
  if (endDate) {
    whereClause += ` AND effective_at <= '${endDate}'`;
  }
  
  const results = await query(`
    SELECT 
      event_id, contract_id, template_id, effective_at,
      round, coupon_type, beneficiary, weight, cc_amount,
      has_issuance_data, payload
    FROM reward_coupons
    ${whereClause}
    ORDER BY effective_at DESC
    LIMIT ${limit} OFFSET ${offset}
  `);
  
  const safeJsonParse = (val) => {
    if (val === null || val === undefined) return null;
    if (typeof val !== 'string') return val;
    try {
      return JSON.parse(val);
    } catch {
      return val;
    }
  };

  return results.map(r => ({
    ...r,
    payload: safeJsonParse(r.payload),
  }));
}

/**
 * Get aggregated rewards by beneficiary
 */
export async function getRewardsByBeneficiary(beneficiary, { startRound, endRound, startDate, endDate } = {}) {
  let whereClause = `WHERE beneficiary = '${beneficiary.replace(/'/g, "''")}'`;
  
  if (startRound !== null && startRound !== undefined) {
    whereClause += ` AND round >= ${parseInt(startRound, 10)}`;
  }
  if (endRound !== null && endRound !== undefined) {
    whereClause += ` AND round <= ${parseInt(endRound, 10)}`;
  }
  if (startDate) {
    whereClause += ` AND effective_at >= '${startDate}'`;
  }
  if (endDate) {
    whereClause += ` AND effective_at <= '${endDate}'`;
  }
  
  const summary = await queryOne(`
    SELECT 
      COUNT(*) as reward_count,
      COALESCE(SUM(weight), 0) as total_weight,
      COALESCE(SUM(cc_amount), 0) as total_cc,
      MIN(round) as min_round,
      MAX(round) as max_round,
      bool_or(has_issuance_data) as has_issuance_data
    FROM reward_coupons
    ${whereClause}
  `);
  
  const byRound = await query(`
    SELECT 
      round,
      COUNT(*) as count,
      COALESCE(SUM(weight), 0) as weight,
      COALESCE(SUM(cc_amount), 0) as cc_amount
    FROM reward_coupons
    ${whereClause}
    GROUP BY round
    ORDER BY round DESC
    LIMIT 100
  `);
  
  const byType = await query(`
    SELECT 
      coupon_type,
      COUNT(*) as count,
      COALESCE(SUM(cc_amount), 0) as cc_amount
    FROM reward_coupons
    ${whereClause}
    GROUP BY coupon_type
  `);
  
  return {
    beneficiary,
    summary: {
      rewardCount: Number(summary?.reward_count || 0),
      totalWeight: Number(summary?.total_weight || 0),
      totalCC: Number(summary?.total_cc || 0),
      minRound: Number(summary?.min_round || 0),
      maxRound: Number(summary?.max_round || 0),
      hasIssuanceData: summary?.has_issuance_data || false,
    },
    byRound: byRound.map(r => ({
      round: Number(r.round),
      count: Number(r.count),
      weight: Number(r.weight),
      ccAmount: Number(r.cc_amount),
    })),
    byType: byType.map(r => ({
      type: r.coupon_type,
      count: Number(r.count),
      ccAmount: Number(r.cc_amount),
    })),
  };
}

/**
 * Check if index is populated
 */
export async function isIndexPopulated() {
  const stats = await getRewardCouponStats();
  return stats.total > 0;
}

/**
 * Check if indexing is in progress
 */
export function isIndexingInProgress() {
  return indexingInProgress;
}

/**
 * Get current indexing progress
 */
export function getIndexingProgress() {
  return indexingProgress;
}

/**
 * Ensure index tables exist
 */
async function ensureIndexTables() {
  // RewardCoupon index table
  await query(`
    CREATE TABLE IF NOT EXISTS reward_coupons (
      event_id            VARCHAR PRIMARY KEY,
      contract_id         VARCHAR,
      template_id         VARCHAR,
      effective_at        TIMESTAMP,
      round               BIGINT,
      coupon_type         VARCHAR,
      beneficiary         VARCHAR,
      weight              DOUBLE DEFAULT 0,
      cc_amount           DOUBLE DEFAULT 0,
      has_issuance_data   BOOLEAN DEFAULT FALSE,
      payload             VARCHAR,
      created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);

  // DuckDB requires UNIQUE indexes for ON CONFLICT targets
  await query(`
    CREATE UNIQUE INDEX IF NOT EXISTS uq_reward_coupons_event_id
    ON reward_coupons(event_id)
  `);

  // Indexes for efficient queries
  await query(`
    CREATE INDEX IF NOT EXISTS idx_reward_coupons_beneficiary ON reward_coupons(beneficiary)
  `);
  await query(`
    CREATE INDEX IF NOT EXISTS idx_reward_coupons_round ON reward_coupons(round)
  `);
  await query(`
    CREATE INDEX IF NOT EXISTS idx_reward_coupons_coupon_type ON reward_coupons(coupon_type)
  `);
  await query(`
    CREATE INDEX IF NOT EXISTS idx_reward_coupons_effective_at ON reward_coupons(effective_at)
  `);

  // Track indexing progress
  await query(`
    CREATE TABLE IF NOT EXISTS reward_coupon_index_state (
      id                  INTEGER PRIMARY KEY DEFAULT 1,
      last_indexed_file   VARCHAR,
      last_indexed_at     TIMESTAMP,
      total_indexed       BIGINT DEFAULT 0,
      CHECK (id = 1)
    )
  `);

  await query(`
    CREATE UNIQUE INDEX IF NOT EXISTS uq_reward_coupon_index_state_id
    ON reward_coupon_index_state(id)
  `);
  
  console.log('   ✓ Reward coupon index tables ensured');
}

/**
 * Extract coupon type from template name
 */
function getCouponType(templateId) {
  if (templateId?.includes('AppRewardCoupon')) return 'App';
  if (templateId?.includes('ValidatorRewardCoupon')) return 'Validator';
  if (templateId?.includes('SvRewardCoupon')) return 'SV';
  return 'Unknown';
}

/**
 * Extract beneficiary from payload
 */
function extractBeneficiary(payload) {
  return payload?.provider || payload?.beneficiary || payload?.owner || payload?.round?.provider || null;
}

/**
 * Extract round number from payload
 */
function extractRoundNumber(payload) {
  const roundNum = payload?.round?.number ?? payload?.round;
  if (roundNum === undefined || roundNum === null) return null;
  return typeof roundNum === 'number' ? roundNum : parseInt(roundNum, 10);
}

/**
 * Build issuance rate map by scanning round data
 */
async function buildIssuanceMap() {
  const roundIssuanceMap = new Map();
  const templateIndexPopulated = await isTemplateIndexPopulated();
  
  if (!templateIndexPopulated) {
    console.log('   ⚠️ Template index not available, skipping issuance map build');
    return roundIssuanceMap;
  }
  
  console.log('   📊 Building issuance rate map from round data...');
  
  const allRoundFiles = new Set();
  for (const template of ROUND_TEMPLATES) {
    const files = await getFilesForTemplate(template);
    files.forEach(f => allRoundFiles.add(f));
  }
  
  const roundFiles = Array.from(allRoundFiles);
  console.log(`   📂 Found ${roundFiles.length} files with round data`);
  
  let processed = 0;
  for (const file of roundFiles) {
    try {
      const result = await binaryReader.readBinaryFile(file);
      for (const record of (result.records || [])) {
        if (record.event_type === 'created' && record.template_id?.includes('MiningRound')) {
          const roundNum = extractRoundNumber(record.payload);
          const payload = record.payload || {};
          
          // Store issuance rates per coupon type
          const appIssuance = parseFloat(payload.issuancePerAppRewardCoupon || 0);
          const validatorIssuance = parseFloat(payload.issuancePerValidatorRewardCoupon || 0);
          const svIssuance = parseFloat(payload.issuancePerSvRewardCoupon || 0);
          
          if (roundNum !== null && (appIssuance || validatorIssuance || svIssuance)) {
            roundIssuanceMap.set(roundNum, {
              app: appIssuance,
              validator: validatorIssuance,
              sv: svIssuance,
            });
          }
        }
      }
      processed++;
      if (processed % 500 === 0) {
        console.log(`   Processed ${processed}/${roundFiles.length} round files, ${roundIssuanceMap.size} rounds found`);
      }
    } catch (err) {
      // Skip files that can't be read
    }
  }
  
  console.log(`   ✓ Built issuance map for ${roundIssuanceMap.size} rounds`);
  return roundIssuanceMap;
}

/**
 * Calculate CC amount from weight and issuance
 */
function calculateCCAmount(payload, couponType, roundIssuanceMap) {
  // Direct amount field (some coupons have this)
  if (payload?.amount) {
    return { amount: parseFloat(payload.amount), hasIssuance: true };
  }
  if (payload?.initialAmount) {
    return { amount: parseFloat(payload.initialAmount), hasIssuance: true };
  }
  
  const weight = parseFloat(payload?.weight || 0);
  const roundNum = extractRoundNumber(payload);
  
  if (weight > 0 && roundNum !== null && roundIssuanceMap.has(roundNum)) {
    const issuanceRates = roundIssuanceMap.get(roundNum);
    let issuance = 0;
    
    if (couponType === 'App') {
      issuance = issuanceRates.app;
    } else if (couponType === 'Validator') {
      issuance = issuanceRates.validator;
    } else if (couponType === 'SV') {
      issuance = issuanceRates.sv;
    }
    
    if (issuance > 0) {
      return { amount: weight * issuance, hasIssuance: true };
    }
  }
  
  // Return weight as fallback (no issuance data)
  return { amount: weight, hasIssuance: false };
}

/**
 * Build or update the RewardCoupon index by scanning binary files
 * Uses template-to-file index when available for much faster scanning
 */
export async function buildRewardCouponIndex({ force = false } = {}) {
  if (indexingInProgress) {
    console.log('⏳ RewardCoupon indexing already in progress');
    return { status: 'in_progress', progress: indexingProgress };
  }
  
  // Check if index is already populated (skip unless force=true)
  if (!force) {
    const stats = await getRewardCouponStats();
    if (stats.total > 0) {
      console.log(`✅ RewardCoupon index already populated (${stats.total} records), skipping rebuild`);
      console.log('   Use force=true to rebuild from scratch');
      return { status: 'already_populated', totalIndexed: stats.total };
    }
  }
  
  indexingInProgress = true;
  indexingProgress = { current: 0, total: 0, startTime: Date.now() };
  console.log('\n💰 Starting RewardCoupon index build...');
  
  try {
    const startTime = Date.now();
    
    // Ensure tables exist first
    await ensureIndexTables();
    
    // Build issuance map for CC calculations
    const roundIssuanceMap = await buildIssuanceMap();
    
    // Check if template index is available for faster scanning
    const templateIndexPopulated = await isTemplateIndexPopulated();
    let records = [];
    
    if (templateIndexPopulated) {
      // FAST PATH: Use template index to scan only relevant files
      const templateIndexStats = await getTemplateIndexStats();
      console.log(`   📋 Using template index (${templateIndexStats.totalFiles} files indexed)`);
      
      const allRewardFiles = new Set();
      for (const template of REWARD_TEMPLATES) {
        const files = await getFilesForTemplate(template);
        console.log(`   Found ${files.length} files for ${template}`);
        files.forEach(f => allRewardFiles.add(f));
      }
      
      const rewardFiles = Array.from(allRewardFiles);
      console.log(`   📂 Total ${rewardFiles.length} unique files to scan`);
      
      indexingProgress.total = rewardFiles.length;
      
      // Scan reward files
      for (let i = 0; i < rewardFiles.length; i++) {
        const file = rewardFiles[i];
        try {
          const result = await binaryReader.readBinaryFile(file);
          for (const record of (result.records || [])) {
            if (record.event_type === 'created' && 
                REWARD_TEMPLATES.some(t => record.template_id?.includes(t))) {
              records.push(record);
            }
          }
        } catch (err) {
          // Skip files that can't be read
        }
        
        indexingProgress.current = i + 1;
        if ((i + 1) % 500 === 0) {
          console.log(`   Scanned ${i + 1}/${rewardFiles.length} files, found ${records.length} rewards`);
        }
      }
    } else {
      // SLOW PATH: Full scan (template index not built yet)
      console.log('   ⚠️ Template index not available, using full scan (this will be slow)');
      console.log('   💡 Run template index build first for faster RewardCoupon indexing');
      
      const result = await binaryReader.streamRecords(DATA_PATH, 'events', {
        limit: 100000,
        offset: 0,
        maxDays: 365 * 5,
        maxFilesToScan: 100000,
        fullScan: true,
        sortBy: 'effective_at',
        filter: (e) => e.event_type === 'created' && 
                       REWARD_TEMPLATES.some(t => e.template_id?.includes(t)),
      });
      records = result.records || [];
    }
    
    console.log(`   Found ${records.length} reward coupon events`);
    
    // Clear existing data if force rebuild
    if (force) {
      try {
        await query('DELETE FROM reward_coupons');
        console.log('   Cleared existing index');
      } catch (err) {
        // Table might not exist yet, ignore
      }
    }
    
    // Insert reward coupons
    let inserted = 0;
    
    for (const record of records) {
      const payload = record.payload || {};
      const couponType = getCouponType(record.template_id);
      const beneficiary = extractBeneficiary(payload);
      const roundNum = extractRoundNumber(payload);
      const weight = parseFloat(payload.weight || 0);
      const { amount: ccAmount, hasIssuance } = calculateCCAmount(payload, couponType, roundIssuanceMap);
      
      const escapeStr = (val) => (val === null || val === undefined) ? null : String(val).replace(/'/g, "''");
      const payloadStr = JSON.stringify(payload);
      
      try {
        await query(`
          INSERT INTO reward_coupons (
            event_id, contract_id, template_id, effective_at,
            round, coupon_type, beneficiary, weight, cc_amount,
            has_issuance_data, payload, updated_at
          ) VALUES (
            '${escapeStr(record.event_id)}',
            ${record.contract_id ? `'${escapeStr(record.contract_id)}'` : 'NULL'},
            ${record.template_id ? `'${escapeStr(record.template_id)}'` : 'NULL'},
            ${record.effective_at ? `'${escapeStr(record.effective_at)}'` : 'NULL'},
            ${roundNum !== null ? roundNum : 'NULL'},
            '${escapeStr(couponType)}',
            ${beneficiary ? `'${escapeStr(beneficiary)}'` : 'NULL'},
            ${weight},
            ${ccAmount},
            ${hasIssuance},
            '${escapeStr(payloadStr)}',
            now()
          )
          ON CONFLICT (event_id) DO UPDATE SET
            cc_amount = EXCLUDED.cc_amount,
            has_issuance_data = EXCLUDED.has_issuance_data,
            updated_at = now()
        `);
        inserted++;
      } catch (err) {
        if (!err.message?.includes('duplicate')) {
          console.warn(`   ⚠️ Error inserting reward: ${err.message}`);
        }
      }
      
      if (inserted % 5000 === 0 && inserted > 0) {
        console.log(`   Inserted ${inserted}/${records.length} rewards...`);
      }
    }
    
    // Update index state
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    await query(`
      INSERT INTO reward_coupon_index_state (id, last_indexed_at, total_indexed)
      VALUES (1, now(), ${inserted})
      ON CONFLICT (id) DO UPDATE SET
        last_indexed_at = now(),
        total_indexed = ${inserted}
    `);
    
    console.log(`\n✅ RewardCoupon index built successfully in ${elapsed}s`);
    console.log(`   Indexed ${inserted} reward coupons`);
    console.log(`   Issuance data available for ${roundIssuanceMap.size} rounds`);
    
    const stats = await getRewardCouponStats();
    indexingInProgress = false;
    indexingProgress = { current: 0, total: 0, startTime: null };
    
    return {
      status: 'completed',
      totalIndexed: inserted,
      stats,
      durationSeconds: parseFloat(elapsed),
      issuanceRoundsAvailable: roundIssuanceMap.size,
    };
    
  } catch (err) {
    console.error('❌ Error building RewardCoupon index:', err);
    indexingInProgress = false;
    indexingProgress = { current: 0, total: 0, startTime: null };
    throw err;
  }
}
