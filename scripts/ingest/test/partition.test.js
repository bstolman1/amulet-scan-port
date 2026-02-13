/**
 * Tests for UTC partitioning logic
 * 
 * Verifies:
 * - getUtcPartition returns correct UTC year/month/day
 * - getUtcPartition throws on missing or invalid timestamps
 * - getPartitionPath uses UTC (no local timezone drift)
 * - groupByPartition splits cross-midnight buffers correctly
 * - groupByPartition throws on missing effective_at
 */

import { describe, it, expect } from 'vitest';
import {
  getUtcPartition,
  getPartitionPath,
  groupByPartition,
} from '../data-schema.js';

describe('getUtcPartition', () => {
  it('should return correct UTC year/month/day for a midday timestamp', () => {
    const result = getUtcPartition('2025-04-17T14:30:00Z');
    expect(result).toEqual({ year: 2025, month: 4, day: 17 });
  });

  it('should return correct UTC day for a late-night timestamp (no off-by-one)', () => {
    // 23:50 UTC should still be April 17, not April 18
    const result = getUtcPartition('2025-04-17T23:50:00Z');
    expect(result).toEqual({ year: 2025, month: 4, day: 17 });
  });

  it('should return correct UTC day just after midnight', () => {
    const result = getUtcPartition('2025-04-18T00:00:01Z');
    expect(result).toEqual({ year: 2025, month: 4, day: 18 });
  });

  it('should handle year boundary correctly', () => {
    const result = getUtcPartition('2025-12-31T23:59:59Z');
    expect(result).toEqual({ year: 2025, month: 12, day: 31 });
  });

  it('should handle new year correctly', () => {
    const result = getUtcPartition('2026-01-01T00:00:01Z');
    expect(result).toEqual({ year: 2026, month: 1, day: 1 });
  });

  it('should use unpadded month and day values', () => {
    const result = getUtcPartition('2025-01-05T10:00:00Z');
    expect(result.month).toBe(1);  // not "01"
    expect(result.day).toBe(5);    // not "05"
  });

  it('should throw if effective_at is null', () => {
    expect(() => getUtcPartition(null)).toThrow('effective_at is required');
  });

  it('should throw if effective_at is undefined', () => {
    expect(() => getUtcPartition(undefined)).toThrow('effective_at is required');
  });

  it('should throw if effective_at is empty string', () => {
    expect(() => getUtcPartition('')).toThrow('effective_at is required');
  });

  it('should throw if effective_at is not a valid date', () => {
    expect(() => getUtcPartition('not-a-date')).toThrow('invalid timestamp');
  });
});

describe('getPartitionPath (UTC correctness)', () => {
  it('should produce correct path for a standard timestamp', () => {
    const path = getPartitionPath('2025-04-17T14:30:00Z', 4, 'events', 'backfill');
    expect(path).toBe('backfill/events/migration=4/year=2025/month=4/day=17');
  });

  it('should NOT shift day for late-night UTC timestamps', () => {
    // This is the core bug fix — previously getFullYear/getDate could shift
    const path = getPartitionPath('2025-04-17T23:50:00Z', 4, 'updates', 'backfill');
    expect(path).toBe('backfill/updates/migration=4/year=2025/month=4/day=17');
  });

  it('should handle midnight boundary correctly', () => {
    const before = getPartitionPath('2025-06-30T23:59:59Z', 4);
    const after = getPartitionPath('2025-07-01T00:00:01Z', 4);
    expect(before).toContain('month=6/day=30');
    expect(after).toContain('month=7/day=1');
  });

  it('should default migration to 0 when null', () => {
    const path = getPartitionPath('2025-04-17T10:00:00Z', null);
    expect(path).toContain('migration=0');
  });

  it('should default source to backfill', () => {
    const path = getPartitionPath('2025-04-17T10:00:00Z', 4);
    expect(path.startsWith('backfill/')).toBe(true);
  });

  it('should use updates source for live data', () => {
    const path = getPartitionPath('2025-04-17T10:00:00Z', 4, 'events', 'updates');
    expect(path.startsWith('updates/')).toBe(true);
  });
});

describe('groupByPartition', () => {
  it('should split a cross-midnight buffer into two groups', () => {
    const records = [
      { effective_at: '2025-04-17T23:50:00Z', update_id: 'upd-1', migration_id: 4 },
      { effective_at: '2025-04-17T23:55:00Z', update_id: 'upd-2', migration_id: 4 },
      { effective_at: '2025-04-18T00:05:00Z', update_id: 'upd-3', migration_id: 4 },
      { effective_at: '2025-04-18T00:10:00Z', update_id: 'upd-4', migration_id: 4 },
    ];

    const groups = groupByPartition(records, 'updates', 'backfill');
    const keys = Object.keys(groups);

    expect(keys).toHaveLength(2);
    expect(keys).toContain('backfill/updates/migration=4/year=2025/month=4/day=17');
    expect(keys).toContain('backfill/updates/migration=4/year=2025/month=4/day=18');
    expect(groups['backfill/updates/migration=4/year=2025/month=4/day=17']).toHaveLength(2);
    expect(groups['backfill/updates/migration=4/year=2025/month=4/day=18']).toHaveLength(2);
  });

  it('should keep a single-day buffer as one group', () => {
    const records = [
      { effective_at: '2025-04-17T10:00:00Z', update_id: 'upd-1', migration_id: 4 },
      { effective_at: '2025-04-17T14:00:00Z', update_id: 'upd-2', migration_id: 4 },
      { effective_at: '2025-04-17T18:00:00Z', update_id: 'upd-3', migration_id: 4 },
    ];

    const groups = groupByPartition(records, 'events', 'backfill');
    expect(Object.keys(groups)).toHaveLength(1);
    expect(Object.values(groups)[0]).toHaveLength(3);
  });

  it('should handle year boundary split', () => {
    const records = [
      { effective_at: '2025-12-31T23:58:00Z', event_id: 'evt-1', migration_id: 4 },
      { effective_at: '2026-01-01T00:01:00Z', event_id: 'evt-2', migration_id: 4 },
    ];

    const groups = groupByPartition(records, 'events', 'updates');
    const keys = Object.keys(groups);

    expect(keys).toHaveLength(2);
    expect(keys).toContain('updates/events/migration=4/year=2025/month=12/day=31');
    expect(keys).toContain('updates/events/migration=4/year=2026/month=1/day=1');
  });

  it('should use migrationId override when provided', () => {
    const records = [
      { effective_at: '2025-04-17T10:00:00Z', update_id: 'upd-1', migration_id: 3 },
    ];

    const groups = groupByPartition(records, 'updates', 'backfill', 5);
    expect(Object.keys(groups)[0]).toContain('migration=5');
  });

  it('should fall back to record.migration_id when override is null', () => {
    const records = [
      { effective_at: '2025-04-17T10:00:00Z', update_id: 'upd-1', migration_id: 3 },
    ];

    const groups = groupByPartition(records, 'updates', 'backfill', null);
    expect(Object.keys(groups)[0]).toContain('migration=3');
  });

  it('should throw if a record has no effective_at', () => {
    const records = [
      { effective_at: '2025-04-17T10:00:00Z', update_id: 'upd-1' },
      { update_id: 'upd-2' },  // missing effective_at
    ];

    expect(() => groupByPartition(records, 'updates', 'backfill'))
      .toThrow('upd-2');
  });

  it('should throw if a record has null effective_at', () => {
    const records = [
      { effective_at: null, event_id: 'evt-bad' },
    ];

    expect(() => groupByPartition(records, 'events', 'backfill'))
      .toThrow('evt-bad');
  });

  it('should handle empty array', () => {
    const groups = groupByPartition([], 'updates', 'backfill');
    expect(Object.keys(groups)).toHaveLength(0);
  });

  it('should split three-day buffer into three groups', () => {
    const records = [
      { effective_at: '2025-04-16T20:00:00Z', update_id: 'u1', migration_id: 4 },
      { effective_at: '2025-04-17T12:00:00Z', update_id: 'u2', migration_id: 4 },
      { effective_at: '2025-04-17T18:00:00Z', update_id: 'u3', migration_id: 4 },
      { effective_at: '2025-04-18T06:00:00Z', update_id: 'u4', migration_id: 4 },
    ];

    const groups = groupByPartition(records, 'updates', 'backfill');
    expect(Object.keys(groups)).toHaveLength(3);
  });
});
