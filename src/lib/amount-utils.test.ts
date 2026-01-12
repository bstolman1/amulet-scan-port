/**
 * Amount Utilities Tests
 * 
 * Tests for CC amount conversion and extraction utilities
 */

import { describe, it, expect } from 'vitest';
import {
  CC_DECIMALS,
  CC_DIVISOR,
  toCC,
  pickAmount,
  pickAmountAsCC,
  pickLockedAmount,
} from './amount-utils';

describe('Constants', () => {
  it('defines correct CC decimal precision', () => {
    expect(CC_DECIMALS).toBe(10);
    expect(CC_DIVISOR).toBe(10_000_000_000);
  });
});

describe('toCC', () => {
  it('converts raw ledger amounts to CC', () => {
    expect(toCC(10_000_000_000)).toBe(1);
    expect(toCC(50_000_000_000)).toBe(5);
    expect(toCC(15_000_000_000)).toBe(1.5);
  });

  it('handles string inputs', () => {
    expect(toCC('10000000000')).toBe(1);
    expect(toCC('5000000000')).toBe(0.5);
  });

  it('handles small amounts', () => {
    expect(toCC(1)).toBe(0.0000000001);
    expect(toCC(1000)).toBe(0.0000001);
  });

  it('handles zero', () => {
    expect(toCC(0)).toBe(0);
    expect(toCC('0')).toBe(0);
  });

  it('returns 0 for invalid inputs', () => {
    expect(toCC(NaN)).toBe(0);
    expect(toCC('not a number')).toBe(0);
  });
});

describe('pickAmount', () => {
  it('extracts from amount.initialAmount path', () => {
    expect(pickAmount({ amount: { initialAmount: '1000' } })).toBe(1000);
    expect(pickAmount({ amount: { initialAmount: 2500 } })).toBe(2500);
  });

  it('extracts from amulet.amount.initialAmount path', () => {
    expect(pickAmount({ amulet: { amount: { initialAmount: '3000' } } })).toBe(3000);
  });

  it('extracts from state.amount.initialAmount path', () => {
    expect(pickAmount({ state: { amount: { initialAmount: '4000' } } })).toBe(4000);
  });

  it('extracts from create_arguments.amount.initialAmount path', () => {
    expect(pickAmount({ create_arguments: { amount: { initialAmount: '5000' } } })).toBe(5000);
  });

  it('extracts from balance.initialAmount path', () => {
    expect(pickAmount({ balance: { initialAmount: '6000' } })).toBe(6000);
  });

  it('falls back to direct amount property', () => {
    expect(pickAmount({ amount: 7000 })).toBe(7000);
    expect(pickAmount({ amount: '8000' })).toBe(8000);
  });

  it('returns 0 for missing amounts', () => {
    expect(pickAmount({})).toBe(0);
    expect(pickAmount({ other: 'value' })).toBe(0);
  });

  it('returns 0 for null/undefined', () => {
    expect(pickAmount(null)).toBe(0);
    expect(pickAmount(undefined)).toBe(0);
  });

  it('prioritizes nested paths over direct amount', () => {
    // If both exist, should pick initialAmount (nested) first
    expect(pickAmount({ 
      amount: { initialAmount: '1000' },
    })).toBe(1000);
  });
});

describe('pickAmountAsCC', () => {
  it('extracts and converts amount to CC', () => {
    expect(pickAmountAsCC({ amount: { initialAmount: '10000000000' } })).toBe(1);
    expect(pickAmountAsCC({ amount: { initialAmount: '50000000000' } })).toBe(5);
  });

  it('returns 0 for missing amounts', () => {
    expect(pickAmountAsCC({})).toBe(0);
    expect(pickAmountAsCC(null)).toBe(0);
  });
});

describe('pickLockedAmount', () => {
  it('prioritizes amulet.amount.initialAmount path', () => {
    expect(pickLockedAmount({ 
      amulet: { amount: { initialAmount: '2000' } },
      amount: { initialAmount: '1000' },
    })).toBe(2000);
  });

  it('falls back to generic pickAmount', () => {
    expect(pickLockedAmount({ amount: { initialAmount: '1500' } })).toBe(1500);
    expect(pickLockedAmount({ balance: { initialAmount: '3000' } })).toBe(3000);
  });

  it('returns 0 for missing amounts', () => {
    expect(pickLockedAmount({})).toBe(0);
    expect(pickLockedAmount(null)).toBe(0);
  });
});
