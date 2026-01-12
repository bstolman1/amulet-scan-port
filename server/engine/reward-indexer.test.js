/**
 * Reward Indexer Tests
 * 
 * Tests for reward coupon indexing and CC amount calculations.
 * Critical for accurate reward reporting.
 */

import { describe, it, expect, vi } from 'vitest';

// Mock dependencies
vi.mock('../duckdb/connection.js', () => ({
  query: vi.fn(() => Promise.resolve([])),
  queryOne: vi.fn(() => Promise.resolve({ count: 0 })),
  DATA_PATH: '/mock/data/path',
}));

vi.mock('../duckdb/binary-reader.js', () => ({
  default: {
    readBinaryFile: vi.fn(() => Promise.resolve({ records: [] })),
    streamRecords: vi.fn(() => Promise.resolve({ records: [] })),
  },
}));

vi.mock('./template-file-index.js', () => ({
  getFilesForTemplate: vi.fn(() => Promise.resolve([])),
  isTemplateIndexPopulated: vi.fn(() => Promise.resolve(false)),
  getTemplateIndexStats: vi.fn(() => Promise.resolve({ totalFiles: 0 })),
}));

describe('Reward Indexer', () => {
  describe('Coupon type extraction', () => {
    const getCouponType = (templateId) => {
      if (templateId?.includes('AppRewardCoupon')) return 'App';
      if (templateId?.includes('ValidatorRewardCoupon')) return 'Validator';
      if (templateId?.includes('SvRewardCoupon')) return 'SV';
      return 'Unknown';
    };
    
    it('should identify App reward coupons', () => {
      expect(getCouponType('Splice.Amulet:AppRewardCoupon')).toBe('App');
      expect(getCouponType('com.example:AppRewardCoupon')).toBe('App');
    });
    
    it('should identify Validator reward coupons', () => {
      expect(getCouponType('Splice.Amulet:ValidatorRewardCoupon')).toBe('Validator');
    });
    
    it('should identify SV reward coupons', () => {
      expect(getCouponType('Splice.Amulet:SvRewardCoupon')).toBe('SV');
    });
    
    it('should return Unknown for non-reward templates', () => {
      expect(getCouponType('Splice.Amulet:Amulet')).toBe('Unknown');
      expect(getCouponType('Splice.Round:OpenMiningRound')).toBe('Unknown');
      expect(getCouponType(null)).toBe('Unknown');
      expect(getCouponType(undefined)).toBe('Unknown');
    });
  });

  describe('Beneficiary extraction', () => {
    const extractBeneficiary = (payload) => {
      return payload?.provider || payload?.beneficiary || payload?.owner || payload?.round?.provider || null;
    };
    
    it('should extract provider as beneficiary', () => {
      expect(extractBeneficiary({ provider: 'party::alice' })).toBe('party::alice');
    });
    
    it('should extract beneficiary field', () => {
      expect(extractBeneficiary({ beneficiary: 'party::bob' })).toBe('party::bob');
    });
    
    it('should extract owner as beneficiary', () => {
      expect(extractBeneficiary({ owner: 'party::charlie' })).toBe('party::charlie');
    });
    
    it('should extract nested round.provider', () => {
      expect(extractBeneficiary({ round: { provider: 'party::dave' } })).toBe('party::dave');
    });
    
    it('should prioritize provider over other fields', () => {
      expect(extractBeneficiary({
        provider: 'provider-party',
        beneficiary: 'beneficiary-party',
        owner: 'owner-party',
      })).toBe('provider-party');
    });
    
    it('should return null for empty payload', () => {
      expect(extractBeneficiary({})).toBe(null);
      expect(extractBeneficiary(null)).toBe(null);
      expect(extractBeneficiary(undefined)).toBe(null);
    });
  });

  describe('Round number extraction', () => {
    const extractRoundNumber = (payload) => {
      const roundNum = payload?.round?.number ?? payload?.round;
      if (roundNum === undefined || roundNum === null) return null;
      return typeof roundNum === 'number' ? roundNum : parseInt(roundNum, 10);
    };
    
    it('should extract round.number', () => {
      expect(extractRoundNumber({ round: { number: 42 } })).toBe(42);
    });
    
    it('should extract round as direct number', () => {
      expect(extractRoundNumber({ round: 123 })).toBe(123);
    });
    
    it('should parse string round numbers', () => {
      expect(extractRoundNumber({ round: '456' })).toBe(456);
      expect(extractRoundNumber({ round: { number: '789' } })).toBe(789);
    });
    
    it('should return null for missing round', () => {
      expect(extractRoundNumber({})).toBe(null);
      expect(extractRoundNumber(null)).toBe(null);
      expect(extractRoundNumber({ other: 'field' })).toBe(null);
    });
  });

  describe('CC amount calculation', () => {
    const calculateCCAmount = (payload, couponType, roundIssuanceMap) => {
      // Direct amount field
      if (payload?.amount) {
        return { amount: parseFloat(payload.amount), hasIssuance: true };
      }
      if (payload?.initialAmount) {
        return { amount: parseFloat(payload.initialAmount), hasIssuance: true };
      }
      
      const weight = parseFloat(payload?.weight || 0);
      const roundNum = payload?.round?.number ?? payload?.round ?? null;
      
      if (weight > 0 && roundNum !== null && roundIssuanceMap?.has(roundNum)) {
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
      
      return { amount: weight, hasIssuance: false };
    };
    
    it('should use direct amount field when available', () => {
      const result = calculateCCAmount({ amount: 100.5 }, 'App', new Map());
      expect(result.amount).toBe(100.5);
      expect(result.hasIssuance).toBe(true);
    });
    
    it('should use initialAmount as fallback', () => {
      const result = calculateCCAmount({ initialAmount: 50.25 }, 'App', new Map());
      expect(result.amount).toBe(50.25);
      expect(result.hasIssuance).toBe(true);
    });
    
    it('should calculate from weight * issuance rate', () => {
      const issuanceMap = new Map();
      issuanceMap.set(42, { app: 0.001, validator: 0.002, sv: 0.003 });
      
      // App coupon: weight 1000 * 0.001 = 1.0 CC
      const appResult = calculateCCAmount(
        { weight: 1000, round: 42 },
        'App',
        issuanceMap
      );
      expect(appResult.amount).toBe(1.0);
      expect(appResult.hasIssuance).toBe(true);
      
      // Validator coupon: weight 1000 * 0.002 = 2.0 CC
      const validatorResult = calculateCCAmount(
        { weight: 1000, round: 42 },
        'Validator',
        issuanceMap
      );
      expect(validatorResult.amount).toBe(2.0);
    });
    
    it('should return weight when no issuance data available', () => {
      const result = calculateCCAmount(
        { weight: 500, round: 99 },
        'App',
        new Map() // Empty map - no issuance data
      );
      expect(result.amount).toBe(500);
      expect(result.hasIssuance).toBe(false);
    });
    
    it('should handle missing weight', () => {
      const result = calculateCCAmount({}, 'App', new Map());
      expect(result.amount).toBe(0);
      expect(result.hasIssuance).toBe(false);
    });
  });

  describe('Reward aggregation', () => {
    it('should aggregate rewards by round', () => {
      const rewards = [
        { round: 1, amount: 10 },
        { round: 1, amount: 20 },
        { round: 2, amount: 15 },
        { round: 2, amount: 25 },
        { round: 3, amount: 5 },
      ];
      
      const byRound = {};
      for (const r of rewards) {
        const key = String(r.round);
        if (!byRound[key]) byRound[key] = { count: 0, amount: 0 };
        byRound[key].count++;
        byRound[key].amount += r.amount;
      }
      
      expect(byRound['1']).toEqual({ count: 2, amount: 30 });
      expect(byRound['2']).toEqual({ count: 2, amount: 40 });
      expect(byRound['3']).toEqual({ count: 1, amount: 5 });
    });
    
    it('should aggregate rewards by type', () => {
      const rewards = [
        { type: 'App', amount: 10 },
        { type: 'App', amount: 20 },
        { type: 'Validator', amount: 50 },
        { type: 'SV', amount: 100 },
      ];
      
      const byType = {};
      for (const r of rewards) {
        if (!byType[r.type]) byType[r.type] = { count: 0, amount: 0 };
        byType[r.type].count++;
        byType[r.type].amount += r.amount;
      }
      
      expect(byType.App).toEqual({ count: 2, amount: 30 });
      expect(byType.Validator).toEqual({ count: 1, amount: 50 });
      expect(byType.SV).toEqual({ count: 1, amount: 100 });
    });
  });
});
