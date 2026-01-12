/**
 * Rewards API Tests
 * 
 * Tests for /api/rewards/* endpoints
 */

import { describe, it, expect, vi } from 'vitest';
import { sanitizeNumber, sanitizeIdentifier } from '../lib/sql-sanitize.js';

describe('Rewards API', () => {
  describe('Reward amount extraction', () => {
    const extractRewardAmount = (payload, roundIssuance = null) => {
      if (payload?.amount) {
        return parseFloat(payload.amount);
      }
      if (payload?.initialAmount) {
        return parseFloat(payload.initialAmount);
      }
      
      const weight = parseFloat(payload?.weight || 0);
      if (weight > 0 && roundIssuance) {
        return weight * roundIssuance;
      }
      
      return weight;
    };
    
    it('should extract direct amount', () => {
      expect(extractRewardAmount({ amount: 100 })).toBe(100);
      expect(extractRewardAmount({ amount: '50.5' })).toBe(50.5);
    });
    
    it('should extract initialAmount', () => {
      expect(extractRewardAmount({ initialAmount: 75 })).toBe(75);
    });
    
    it('should calculate from weight * issuance', () => {
      expect(extractRewardAmount({ weight: 1000 }, 0.001)).toBe(1);
      expect(extractRewardAmount({ weight: 5000 }, 0.002)).toBe(10);
    });
    
    it('should return weight when no issuance available', () => {
      expect(extractRewardAmount({ weight: 500 }, null)).toBe(500);
      expect(extractRewardAmount({ weight: 500 })).toBe(500);
    });
    
    it('should handle missing data', () => {
      expect(extractRewardAmount({})).toBe(0);
      expect(extractRewardAmount(null)).toBe(0);
    });
  });

  describe('Party filter logic', () => {
    const isRewardForParty = (partyId) => (e) => {
      const payload = e.payload;
      if (!payload) return false;
      
      if (payload.provider === partyId) return true;
      if (payload.beneficiary === partyId) return true;
      if (payload.owner === partyId) return true;
      if (payload.round?.provider === partyId) return true;
      if (payload.dso === partyId) return true;
      
      return false;
    };
    
    it('should match by provider', () => {
      const filter = isRewardForParty('party::alice');
      expect(filter({ payload: { provider: 'party::alice' } })).toBe(true);
      expect(filter({ payload: { provider: 'party::bob' } })).toBe(false);
    });
    
    it('should match by beneficiary', () => {
      const filter = isRewardForParty('party::alice');
      expect(filter({ payload: { beneficiary: 'party::alice' } })).toBe(true);
    });
    
    it('should match by owner', () => {
      const filter = isRewardForParty('party::alice');
      expect(filter({ payload: { owner: 'party::alice' } })).toBe(true);
    });
    
    it('should match by nested round.provider', () => {
      const filter = isRewardForParty('party::alice');
      expect(filter({ payload: { round: { provider: 'party::alice' } } })).toBe(true);
    });
    
    it('should return false for non-matching party', () => {
      const filter = isRewardForParty('party::alice');
      expect(filter({ payload: { provider: 'party::bob', beneficiary: 'party::charlie' } })).toBe(false);
    });
    
    it('should return false for missing payload', () => {
      const filter = isRewardForParty('party::alice');
      expect(filter({})).toBe(false);
      expect(filter({ payload: null })).toBe(false);
    });
  });

  describe('Date filter logic', () => {
    const passesDateFilter = (startMs, endMs) => (e) => {
      if (startMs === null && endMs === null) return true;
      if (!e.effective_at) return true;
      const eventMs = new Date(e.effective_at).getTime();
      if (startMs !== null && eventMs < startMs) return false;
      if (endMs !== null && eventMs > endMs) return false;
      return true;
    };
    
    it('should pass all events when no filter', () => {
      const filter = passesDateFilter(null, null);
      expect(filter({ effective_at: '2024-01-01' })).toBe(true);
      expect(filter({ effective_at: '2025-12-31' })).toBe(true);
    });
    
    it('should filter by start date', () => {
      const startMs = new Date('2024-06-01').getTime();
      const filter = passesDateFilter(startMs, null);
      
      expect(filter({ effective_at: '2024-07-01' })).toBe(true);
      expect(filter({ effective_at: '2024-05-01' })).toBe(false);
    });
    
    it('should filter by end date', () => {
      const endMs = new Date('2024-06-30').getTime();
      const filter = passesDateFilter(null, endMs);
      
      expect(filter({ effective_at: '2024-06-15' })).toBe(true);
      expect(filter({ effective_at: '2024-07-15' })).toBe(false);
    });
    
    it('should filter by date range', () => {
      const startMs = new Date('2024-06-01').getTime();
      const endMs = new Date('2024-06-30').getTime();
      const filter = passesDateFilter(startMs, endMs);
      
      expect(filter({ effective_at: '2024-06-15' })).toBe(true);
      expect(filter({ effective_at: '2024-05-15' })).toBe(false);
      expect(filter({ effective_at: '2024-07-15' })).toBe(false);
    });
  });

  describe('Round filter logic', () => {
    const passesRoundFilter = (startR, endR) => (e) => {
      if (startR === null && endR === null) return true;
      const roundNum = e.payload?.round?.number ?? e.payload?.round;
      if (roundNum === undefined || roundNum === null) return true;
      const r = typeof roundNum === 'number' ? roundNum : parseInt(roundNum, 10);
      if (startR !== null && r < startR) return false;
      if (endR !== null && r > endR) return false;
      return true;
    };
    
    it('should pass all events when no filter', () => {
      const filter = passesRoundFilter(null, null);
      expect(filter({ payload: { round: 1 } })).toBe(true);
      expect(filter({ payload: { round: 1000 } })).toBe(true);
    });
    
    it('should filter by start round', () => {
      const filter = passesRoundFilter(100, null);
      expect(filter({ payload: { round: 150 } })).toBe(true);
      expect(filter({ payload: { round: 50 } })).toBe(false);
    });
    
    it('should filter by end round', () => {
      const filter = passesRoundFilter(null, 200);
      expect(filter({ payload: { round: 150 } })).toBe(true);
      expect(filter({ payload: { round: 250 } })).toBe(false);
    });
    
    it('should filter by round range', () => {
      const filter = passesRoundFilter(100, 200);
      expect(filter({ payload: { round: 150 } })).toBe(true);
      expect(filter({ payload: { round: 50 } })).toBe(false);
      expect(filter({ payload: { round: 250 } })).toBe(false);
    });
    
    it('should handle nested round.number', () => {
      const filter = passesRoundFilter(100, 200);
      expect(filter({ payload: { round: { number: 150 } } })).toBe(true);
      expect(filter({ payload: { round: { number: 50 } } })).toBe(false);
    });
  });

  describe('Parameter validation', () => {
    it('should validate partyId format', () => {
      expect(sanitizeIdentifier('party::alice')).toBe('party::alice');
      expect(sanitizeIdentifier('Digital-Asset')).toBe('Digital-Asset');
      expect(sanitizeIdentifier("party'; DROP TABLE")).toBeNull();
    });
    
    it('should validate round numbers', () => {
      expect(sanitizeNumber('100', { min: 0, max: 1000000 })).toBe(100);
      expect(sanitizeNumber('-1', { min: 0, max: 1000000 })).toBe(0);
      expect(sanitizeNumber('invalid', { min: 0, max: 1000000, defaultValue: 0 })).toBe(0);
    });
  });
});
