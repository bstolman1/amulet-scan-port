/**
 * Unit Tests for repair-partitions.js
 * 
 * Tests the pure logic functions: partition parsing, repair action determination,
 * and verification checking — without any GCS or filesystem side effects.
 * 
 * Coverage:
 *  - parseMigrationFromPath: extraction from various path shapes
 *  - parseCurrentPartition: ledger vs ACS partition parsing
 *  - determineRepairAction: skip/move/split logic including edge cases
 *  - checkVerification: post-repair verification correctness
 *  - WHERE clause generation: date boundary correctness for splits
 *  - Filename preservation: no collisions or truncation during moves/splits
 *  - Migration 0 handling: zero is a valid migration, not null
 */

import { describe, it, expect } from 'vitest';
import {
  parseMigrationFromPath,
  parseCurrentPartition,
  determineRepairAction,
  checkVerification,
} from '../repair-partitions.js';
import { getPartitionPath, getUtcPartition } from '../data-schema.js';
import { getACSPartitionPath } from '../acs-schema.js';

// ── Test stream configs (mirrors real streams without GCS prefix) ──────────

const BUCKET = 'test-bucket';

const backfillUpdatesStream = {
  name: 'backfill/updates',
  timestampCol: 'effective_at',
  isACS: false,
  computeCorrectPath: (ts, mig) => getPartitionPath(ts, mig, 'updates', 'backfill'),
};

const backfillEventsStream = {
  name: 'backfill/events',
  timestampCol: 'effective_at',
  isACS: false,
  computeCorrectPath: (ts, mig) => getPartitionPath(ts, mig, 'events', 'backfill'),
};

const liveUpdatesStream = {
  name: 'updates/updates',
  timestampCol: 'effective_at',
  isACS: false,
  computeCorrectPath: (ts, mig) => getPartitionPath(ts, mig, 'updates', 'updates'),
};

const liveEventsStream = {
  name: 'updates/events',
  timestampCol: 'effective_at',
  isACS: false,
  computeCorrectPath: (ts, mig) => getPartitionPath(ts, mig, 'events', 'updates'),
};

const acsStream = {
  name: 'acs',
  timestampCol: 'snapshot_time',
  isACS: true,
  computeCorrectPath: (ts, mig) => getACSPartitionPath(ts, mig),
};

// ── parseMigrationFromPath ─────────────────────────────────────────────────

describe('parseMigrationFromPath', () => {
  it('should extract migration ID from path', () => {
    expect(parseMigrationFromPath('gs://bucket/raw/backfill/updates/migration=4/year=2025/month=1/day=15/file.parquet')).toBe(4);
  });

  it('should handle migration=0', () => {
    expect(parseMigrationFromPath('gs://bucket/raw/acs/migration=0/year=2025/month=6/day=1/snapshot_id=120000/file.parquet')).toBe(0);
  });

  it('should return null when no migration in path', () => {
    expect(parseMigrationFromPath('gs://bucket/raw/some/file.parquet')).toBeNull();
  });

  it('should handle double-digit migration IDs', () => {
    expect(parseMigrationFromPath('gs://bucket/raw/backfill/updates/migration=12/year=2025/file.parquet')).toBe(12);
  });
});

// ── parseCurrentPartition ──────────────────────────────────────────────────

describe('parseCurrentPartition', () => {
  it('should parse year/month/day from ledger path', () => {
    const result = parseCurrentPartition('gs://b/raw/backfill/updates/migration=4/year=2025/month=3/day=15/file.parquet', false);
    expect(result).toEqual({ year: 2025, month: 3, day: 15 });
  });

  it('should parse snapshot_id for ACS paths', () => {
    const result = parseCurrentPartition('gs://b/raw/acs/migration=0/year=2025/month=6/day=1/snapshot_id=143022/file.parquet', true);
    expect(result).toEqual({ year: 2025, month: 6, day: 1, snapshotId: '143022' });
  });

  it('should not include snapshotId for non-ACS', () => {
    const result = parseCurrentPartition('gs://b/raw/backfill/updates/migration=4/year=2025/month=1/day=1/file.parquet', false);
    expect(result.snapshotId).toBeUndefined();
  });

  it('should return nulls for missing components', () => {
    const result = parseCurrentPartition('gs://b/raw/some/file.parquet', false);
    expect(result).toEqual({ year: null, month: null, day: null });
  });

  it('should parse unpadded single-digit months and days', () => {
    const result = parseCurrentPartition('gs://b/raw/backfill/events/migration=0/year=2024/month=1/day=5/file.parquet', false);
    expect(result).toEqual({ year: 2024, month: 1, day: 5 });
  });
});

// ── determineRepairAction ──────────────────────────────────────────────────

describe('determineRepairAction', () => {
  describe('skip cases', () => {
    it('should skip when no timestamps', () => {
      const result = determineRepairAction(
        'gs://test-bucket/raw/backfill/updates/migration=4/year=2025/month=1/day=15/file.parquet',
        [],
        backfillUpdatesStream,
        BUCKET,
      );
      expect(result.action).toBe('skip');
      expect(result.reason).toContain('no effective_at');
    });

    it('should skip when null timestamps', () => {
      const result = determineRepairAction(
        'gs://test-bucket/raw/backfill/updates/migration=4/year=2025/month=1/day=15/file.parquet',
        null,
        backfillUpdatesStream,
        BUCKET,
      );
      expect(result.action).toBe('skip');
    });

    it('should skip file already in correct partition', () => {
      const gcsFile = 'gs://test-bucket/raw/backfill/updates/migration=4/year=2025/month=1/day=15/file.parquet';
      const result = determineRepairAction(
        gcsFile,
        ['2025-01-15T10:30:00Z'],
        backfillUpdatesStream,
        BUCKET,
      );
      expect(result.action).toBe('skip');
      expect(result.reason).toBe('already correct');
    });

    it('should skip when all timestamps are on the same correct day', () => {
      const gcsFile = 'gs://test-bucket/raw/backfill/updates/migration=4/year=2025/month=6/day=15/file.parquet';
      const result = determineRepairAction(
        gcsFile,
        ['2025-06-15T00:00:01Z', '2025-06-15T12:00:00Z', '2025-06-15T23:59:59Z'],
        backfillUpdatesStream,
        BUCKET,
      );
      expect(result.action).toBe('skip');
    });
  });

  describe('move cases', () => {
    it('should detect file in wrong day partition', () => {
      const gcsFile = 'gs://test-bucket/raw/backfill/updates/migration=4/year=2025/month=1/day=15/file.parquet';
      const result = determineRepairAction(
        gcsFile,
        ['2025-01-16T00:30:00Z'],
        backfillUpdatesStream,
        BUCKET,
      );
      expect(result.action).toBe('move');
      expect(result.to).toContain('day=16');
      expect(result.from).toBe(gcsFile);
    });

    it('should detect file in wrong month partition', () => {
      const gcsFile = 'gs://test-bucket/raw/backfill/updates/migration=4/year=2024/month=12/day=31/file.parquet';
      const result = determineRepairAction(
        gcsFile,
        ['2025-01-01T00:15:00Z'],
        backfillUpdatesStream,
        BUCKET,
      );
      expect(result.action).toBe('move');
      expect(result.to).toContain('year=2025');
      expect(result.to).toContain('month=1');
      expect(result.to).toContain('day=1');
    });

    it('should work for events stream', () => {
      const gcsFile = 'gs://test-bucket/raw/backfill/events/migration=4/year=2025/month=1/day=15/file.parquet';
      const result = determineRepairAction(
        gcsFile,
        ['2025-01-16T00:30:00Z'],
        backfillEventsStream,
        BUCKET,
      );
      expect(result.action).toBe('move');
      expect(result.to).toContain('events/migration=4');
      expect(result.to).toContain('day=16');
    });

    it('should work for live updates stream', () => {
      const gcsFile = 'gs://test-bucket/raw/updates/updates/migration=4/year=2025/month=6/day=30/file.parquet';
      const result = determineRepairAction(
        gcsFile,
        ['2025-07-01T00:05:00Z'],
        liveUpdatesStream,
        BUCKET,
      );
      expect(result.action).toBe('move');
      expect(result.to).toContain('updates/updates/migration=4');
      expect(result.to).toContain('month=7');
      expect(result.to).toContain('day=1');
    });

    it('should preserve filename during move', () => {
      const gcsFile = 'gs://test-bucket/raw/backfill/updates/migration=4/year=2025/month=1/day=15/updates_2025-01-16T00-30-00.parquet';
      const result = determineRepairAction(
        gcsFile,
        ['2025-01-16T00:30:00Z'],
        backfillUpdatesStream,
        BUCKET,
      );
      expect(result.to).toContain('updates_2025-01-16T00-30-00.parquet');
    });

    it('should move file off by exactly one day (local timezone +1h drift)', () => {
      // Classic bug: UTC 00:30 on day 16 was stored in day=15 (local time was still day 15)
      const gcsFile = 'gs://test-bucket/raw/backfill/updates/migration=0/year=2024/month=10/day=1/file.parquet';
      const result = determineRepairAction(
        gcsFile,
        ['2024-10-02T00:30:00Z'],
        backfillUpdatesStream,
        BUCKET,
      );
      expect(result.action).toBe('move');
      expect(result.to).toContain('day=2');
    });

    it('should handle migration=0 correctly (not treated as null)', () => {
      const gcsFile = 'gs://test-bucket/raw/backfill/updates/migration=0/year=2024/month=10/day=1/file.parquet';
      const result = determineRepairAction(
        gcsFile,
        ['2024-10-02T00:30:00Z'],
        backfillUpdatesStream,
        BUCKET,
      );
      expect(result.action).toBe('move');
      expect(result.to).toContain('migration=0');
      // Should NOT contain migration=null or migration=undefined
      expect(result.to).not.toContain('migration=null');
    });

    it('should handle live events stream move', () => {
      const gcsFile = 'gs://test-bucket/raw/updates/events/migration=4/year=2026/month=1/day=27/file.parquet';
      const result = determineRepairAction(
        gcsFile,
        ['2026-01-28T00:05:00Z'],
        liveEventsStream,
        BUCKET,
      );
      expect(result.action).toBe('move');
      expect(result.to).toContain('updates/events/migration=4');
      expect(result.to).toContain('day=28');
    });
  });

  describe('split cases', () => {
    it('should split file spanning two UTC days', () => {
      const gcsFile = 'gs://test-bucket/raw/backfill/updates/migration=4/year=2025/month=1/day=15/file.parquet';
      const result = determineRepairAction(
        gcsFile,
        ['2025-01-15T23:50:00Z', '2025-01-16T00:10:00Z'],
        backfillUpdatesStream,
        BUCKET,
      );
      expect(result.action).toBe('split');
      expect(result.splits).toHaveLength(2);

      const partitions = result.splits.map(s => s.partition);
      expect(partitions).toContain('backfill/updates/migration=4/year=2025/month=1/day=15');
      expect(partitions).toContain('backfill/updates/migration=4/year=2025/month=1/day=16');
    });

    it('should split file spanning month boundary', () => {
      const gcsFile = 'gs://test-bucket/raw/backfill/updates/migration=4/year=2025/month=1/day=31/file.parquet';
      const result = determineRepairAction(
        gcsFile,
        ['2025-01-31T23:55:00Z', '2025-02-01T00:05:00Z'],
        backfillUpdatesStream,
        BUCKET,
      );
      expect(result.action).toBe('split');
      expect(result.splits).toHaveLength(2);

      const partitions = result.splits.map(s => s.partition);
      expect(partitions).toContain('backfill/updates/migration=4/year=2025/month=1/day=31');
      expect(partitions).toContain('backfill/updates/migration=4/year=2025/month=2/day=1');
    });

    it('should split file spanning year boundary', () => {
      const gcsFile = 'gs://test-bucket/raw/backfill/updates/migration=4/year=2024/month=12/day=31/file.parquet';
      const result = determineRepairAction(
        gcsFile,
        ['2024-12-31T23:59:00Z', '2025-01-01T00:01:00Z'],
        backfillUpdatesStream,
        BUCKET,
      );
      expect(result.action).toBe('split');
      const partitions = result.splits.map(s => s.partition);
      expect(partitions).toContain('backfill/updates/migration=4/year=2024/month=12/day=31');
      expect(partitions).toContain('backfill/updates/migration=4/year=2025/month=1/day=1');
    });

    it('should split file spanning 3 consecutive UTC days', () => {
      const gcsFile = 'gs://test-bucket/raw/backfill/updates/migration=4/year=2025/month=3/day=10/file.parquet';
      const result = determineRepairAction(
        gcsFile,
        ['2025-03-10T22:00:00Z', '2025-03-11T12:00:00Z', '2025-03-12T02:00:00Z'],
        backfillUpdatesStream,
        BUCKET,
      );
      expect(result.action).toBe('split');
      expect(result.splits).toHaveLength(3);

      const partitions = result.splits.map(s => s.partition);
      expect(partitions).toContain('backfill/updates/migration=4/year=2025/month=3/day=10');
      expect(partitions).toContain('backfill/updates/migration=4/year=2025/month=3/day=11');
      expect(partitions).toContain('backfill/updates/migration=4/year=2025/month=3/day=12');
    });

    it('should split file spanning 5 days across month boundary', () => {
      const gcsFile = 'gs://test-bucket/raw/backfill/updates/migration=4/year=2025/month=1/day=29/file.parquet';
      const result = determineRepairAction(
        gcsFile,
        [
          '2025-01-29T10:00:00Z',
          '2025-01-30T10:00:00Z',
          '2025-01-31T10:00:00Z',
          '2025-02-01T10:00:00Z',
          '2025-02-02T10:00:00Z',
        ],
        backfillUpdatesStream,
        BUCKET,
      );
      expect(result.action).toBe('split');
      expect(result.splits).toHaveLength(5);

      const partitions = result.splits.map(s => s.partition);
      expect(partitions).toContain('backfill/updates/migration=4/year=2025/month=1/day=29');
      expect(partitions).toContain('backfill/updates/migration=4/year=2025/month=1/day=30');
      expect(partitions).toContain('backfill/updates/migration=4/year=2025/month=1/day=31');
      expect(partitions).toContain('backfill/updates/migration=4/year=2025/month=2/day=1');
      expect(partitions).toContain('backfill/updates/migration=4/year=2025/month=2/day=2');
    });

    it('should split file spanning 3+ days across year boundary', () => {
      const gcsFile = 'gs://test-bucket/raw/backfill/updates/migration=4/year=2024/month=12/day=30/file.parquet';
      const result = determineRepairAction(
        gcsFile,
        ['2024-12-30T08:00:00Z', '2024-12-31T16:00:00Z', '2025-01-01T04:00:00Z', '2025-01-02T12:00:00Z'],
        backfillUpdatesStream,
        BUCKET,
      );
      expect(result.action).toBe('split');
      expect(result.splits).toHaveLength(4);

      const partitions = result.splits.map(s => s.partition);
      expect(partitions).toContain('backfill/updates/migration=4/year=2024/month=12/day=30');
      expect(partitions).toContain('backfill/updates/migration=4/year=2024/month=12/day=31');
      expect(partitions).toContain('backfill/updates/migration=4/year=2025/month=1/day=1');
      expect(partitions).toContain('backfill/updates/migration=4/year=2025/month=1/day=2');
    });

    it('should group multiple timestamps into same day correctly in multi-day split', () => {
      const gcsFile = 'gs://test-bucket/raw/backfill/updates/migration=4/year=2025/month=6/day=1/file.parquet';
      const result = determineRepairAction(
        gcsFile,
        [
          '2025-06-01T10:00:00Z',
          '2025-06-01T22:00:00Z',  // same day as above
          '2025-06-02T03:00:00Z',
          '2025-06-02T15:00:00Z',  // same day as above
          '2025-06-03T01:00:00Z',
        ],
        backfillUpdatesStream,
        BUCKET,
      );
      expect(result.action).toBe('split');
      expect(result.splits).toHaveLength(3);

      const day1 = result.splits.find(s => s.partition.includes('day=1'));
      const day2 = result.splits.find(s => s.partition.includes('day=2'));
      const day3 = result.splits.find(s => s.partition.includes('day=3'));
      expect(day1.timestamps).toHaveLength(2);
      expect(day2.timestamps).toHaveLength(2);
      expect(day3.timestamps).toHaveLength(1);
    });

    it('should handle events stream multi-day split', () => {
      const gcsFile = 'gs://test-bucket/raw/backfill/events/migration=4/year=2025/month=3/day=1/file.parquet';
      const result = determineRepairAction(
        gcsFile,
        ['2025-03-01T20:00:00Z', '2025-03-02T10:00:00Z', '2025-03-03T05:00:00Z'],
        backfillEventsStream,
        BUCKET,
      );
      expect(result.action).toBe('split');
      expect(result.splits).toHaveLength(3);
      expect(result.splits[0].partition).toContain('events/migration=4');
    });

    it('should preserve same filename in each split destination', () => {
      const gcsFile = 'gs://test-bucket/raw/backfill/updates/migration=0/year=2024/month=10/day=1/updates-1769106734657-c92f15f5.parquet';
      const result = determineRepairAction(
        gcsFile,
        ['2024-10-01T23:50:00Z', '2024-10-02T00:10:00Z'],
        backfillUpdatesStream,
        BUCKET,
      );
      expect(result.action).toBe('split');
      for (const split of result.splits) {
        expect(split.to).toContain('updates-1769106734657-c92f15f5.parquet');
      }
    });

    it('should produce correct GCS URIs with gs:// prefix for each split', () => {
      const gcsFile = 'gs://test-bucket/raw/backfill/updates/migration=4/year=2025/month=1/day=15/file.parquet';
      const result = determineRepairAction(
        gcsFile,
        ['2025-01-15T23:50:00Z', '2025-01-16T00:10:00Z'],
        backfillUpdatesStream,
        BUCKET,
      );
      for (const split of result.splits) {
        expect(split.to).toMatch(/^gs:\/\/test-bucket\/raw\//);
        expect(split.to).toMatch(/\.parquet$/);
      }
    });
  });

  describe('edge cases for actual execution safety', () => {
    it('should handle February 28→March 1 (non-leap year)', () => {
      const gcsFile = 'gs://test-bucket/raw/backfill/updates/migration=4/year=2025/month=2/day=28/file.parquet';
      const result = determineRepairAction(
        gcsFile,
        ['2025-02-28T23:55:00Z', '2025-03-01T00:05:00Z'],
        backfillUpdatesStream,
        BUCKET,
      );
      expect(result.action).toBe('split');
      const partitions = result.splits.map(s => s.partition);
      expect(partitions).toContain('backfill/updates/migration=4/year=2025/month=2/day=28');
      expect(partitions).toContain('backfill/updates/migration=4/year=2025/month=3/day=1');
    });

    it('should handle February 29→March 1 (leap year 2024)', () => {
      const gcsFile = 'gs://test-bucket/raw/backfill/updates/migration=0/year=2024/month=2/day=29/file.parquet';
      const result = determineRepairAction(
        gcsFile,
        ['2024-02-29T23:55:00Z', '2024-03-01T00:05:00Z'],
        backfillUpdatesStream,
        BUCKET,
      );
      expect(result.action).toBe('split');
      const partitions = result.splits.map(s => s.partition);
      expect(partitions).toContain('backfill/updates/migration=0/year=2024/month=2/day=29');
      expect(partitions).toContain('backfill/updates/migration=0/year=2024/month=3/day=1');
    });

    it('should handle timestamp at exact midnight (00:00:00Z belongs to the new day)', () => {
      const gcsFile = 'gs://test-bucket/raw/backfill/updates/migration=4/year=2025/month=3/day=14/file.parquet';
      const result = determineRepairAction(
        gcsFile,
        ['2025-03-15T00:00:00Z'],
        backfillUpdatesStream,
        BUCKET,
      );
      expect(result.action).toBe('move');
      expect(result.to).toContain('day=15');
    });

    it('should handle timestamp at 23:59:59Z (stays in current day)', () => {
      const gcsFile = 'gs://test-bucket/raw/backfill/updates/migration=4/year=2025/month=3/day=15/file.parquet';
      const result = determineRepairAction(
        gcsFile,
        ['2025-03-15T23:59:59Z'],
        backfillUpdatesStream,
        BUCKET,
      );
      expect(result.action).toBe('skip');
    });

    it('should handle single-timestamp file that needs moving', () => {
      const gcsFile = 'gs://test-bucket/raw/backfill/updates/migration=0/year=2024/month=10/day=1/file.parquet';
      const result = determineRepairAction(
        gcsFile,
        ['2024-10-05T14:00:00Z'],
        backfillUpdatesStream,
        BUCKET,
      );
      expect(result.action).toBe('move');
      expect(result.to).toContain('day=5');
    });

    it('should handle file where ALL timestamps are in a DIFFERENT single day', () => {
      // All rows belong to day 2, but file is in day 1
      const gcsFile = 'gs://test-bucket/raw/backfill/updates/migration=0/year=2024/month=10/day=1/file.parquet';
      const result = determineRepairAction(
        gcsFile,
        ['2024-10-02T01:00:00Z', '2024-10-02T06:00:00Z', '2024-10-02T12:00:00Z'],
        backfillUpdatesStream,
        BUCKET,
      );
      expect(result.action).toBe('move');  // move, not split
      expect(result.to).toContain('day=2');
    });

    it('should handle fractional-second timestamps', () => {
      const gcsFile = 'gs://test-bucket/raw/backfill/updates/migration=4/year=2025/month=1/day=15/file.parquet';
      const result = determineRepairAction(
        gcsFile,
        ['2025-01-15T23:59:59.999Z'],
        backfillUpdatesStream,
        BUCKET,
      );
      expect(result.action).toBe('skip');
    });

    it('should handle timestamps with microsecond precision', () => {
      const gcsFile = 'gs://test-bucket/raw/backfill/updates/migration=4/year=2025/month=1/day=15/file.parquet';
      const result = determineRepairAction(
        gcsFile,
        ['2025-01-16T00:00:00.000001Z'],
        backfillUpdatesStream,
        BUCKET,
      );
      expect(result.action).toBe('move');
      expect(result.to).toContain('day=16');
    });

    it('should not confuse source prefixes (backfill file stays in backfill)', () => {
      const gcsFile = 'gs://test-bucket/raw/backfill/updates/migration=4/year=2025/month=1/day=15/file.parquet';
      const result = determineRepairAction(
        gcsFile,
        ['2025-01-16T00:30:00Z'],
        backfillUpdatesStream,
        BUCKET,
      );
      expect(result.to).toContain('/raw/backfill/');
      expect(result.to).not.toContain('/raw/updates/updates/');
    });

    it('should not confuse source prefixes (live updates file stays in updates)', () => {
      const gcsFile = 'gs://test-bucket/raw/updates/updates/migration=4/year=2025/month=1/day=15/file.parquet';
      const result = determineRepairAction(
        gcsFile,
        ['2025-01-16T00:30:00Z'],
        liveUpdatesStream,
        BUCKET,
      );
      expect(result.to).toContain('/raw/updates/updates/');
      expect(result.to).not.toContain('/raw/backfill/');
    });
  });

  describe('ACS stream', () => {
    it('should skip ACS file in correct partition', () => {
      const gcsFile = 'gs://test-bucket/raw/acs/migration=0/year=2025/month=6/day=15/snapshot_id=103045/file.parquet';
      const result = determineRepairAction(
        gcsFile,
        ['2025-06-15T10:30:45Z'],
        acsStream,
        BUCKET,
      );
      expect(result.action).toBe('skip');
      expect(result.reason).toBe('already correct');
    });

    it('should detect ACS file in wrong day (local time drift)', () => {
      const gcsFile = 'gs://test-bucket/raw/acs/migration=0/year=2025/month=6/day=14/snapshot_id=233000/file.parquet';
      const result = determineRepairAction(
        gcsFile,
        ['2025-06-15T00:30:00Z'],
        acsStream,
        BUCKET,
      );
      expect(result.action).toBe('move');
      expect(result.to).toContain('day=15');
    });

    it('should handle ACS file at month boundary', () => {
      const gcsFile = 'gs://test-bucket/raw/acs/migration=4/year=2026/month=1/day=31/snapshot_id=180001/file.parquet';
      const result = determineRepairAction(
        gcsFile,
        ['2026-02-01T00:10:00Z'],
        acsStream,
        BUCKET,
      );
      expect(result.action).toBe('move');
      expect(result.to).toContain('month=2');
      expect(result.to).toContain('day=1');
    });
  });
});

// ── checkVerification ──────────────────────────────────────────────────────

describe('checkVerification', () => {
  it('should pass for file in correct partition', () => {
    const gcsFile = 'gs://test-bucket/raw/backfill/updates/migration=4/year=2025/month=1/day=15/file.parquet';
    const result = checkVerification(
      gcsFile,
      ['2025-01-15T10:30:00Z', '2025-01-15T14:00:00Z'],
      backfillUpdatesStream,
      BUCKET,
    );
    expect(result.passed).toBe(true);
  });

  it('should fail for file in wrong partition', () => {
    const gcsFile = 'gs://test-bucket/raw/backfill/updates/migration=4/year=2025/month=1/day=15/file.parquet';
    const result = checkVerification(
      gcsFile,
      ['2025-01-16T00:30:00Z'],
      backfillUpdatesStream,
      BUCKET,
    );
    expect(result.passed).toBe(false);
    expect(result.failedTimestamp).toBe('2025-01-16T00:30:00Z');
    expect(result.expected).toContain('day=16');
    expect(result.actual).toContain('day=15');
  });

  it('should pass (skipped) for empty timestamps', () => {
    const result = checkVerification(
      'gs://test-bucket/raw/backfill/updates/migration=4/year=2025/month=1/day=15/file.parquet',
      [],
      backfillUpdatesStream,
      BUCKET,
    );
    expect(result.passed).toBe(true);
    expect(result.skipped).toBe(true);
  });

  it('should fail on first mismatched timestamp in mixed file', () => {
    const gcsFile = 'gs://test-bucket/raw/backfill/updates/migration=4/year=2025/month=1/day=15/file.parquet';
    const result = checkVerification(
      gcsFile,
      ['2025-01-15T10:00:00Z', '2025-01-16T00:05:00Z'],
      backfillUpdatesStream,
      BUCKET,
    );
    expect(result.passed).toBe(false);
    expect(result.failedTimestamp).toBe('2025-01-16T00:05:00Z');
  });

  it('should verify ACS files correctly', () => {
    const gcsFile = 'gs://test-bucket/raw/acs/migration=0/year=2025/month=6/day=15/snapshot_id=103045/file.parquet';
    const result = checkVerification(
      gcsFile,
      ['2025-06-15T10:30:45Z'],
      acsStream,
      BUCKET,
    );
    expect(result.passed).toBe(true);
  });

  it('should fail verification for ACS file in wrong partition', () => {
    const gcsFile = 'gs://test-bucket/raw/acs/migration=0/year=2025/month=6/day=14/snapshot_id=233000/file.parquet';
    const result = checkVerification(
      gcsFile,
      ['2025-06-15T00:30:00Z'],
      acsStream,
      BUCKET,
    );
    expect(result.passed).toBe(false);
  });

  it('should pass verification for file with many timestamps all on correct day', () => {
    const gcsFile = 'gs://test-bucket/raw/backfill/updates/migration=4/year=2025/month=6/day=15/file.parquet';
    const timestamps = Array.from({ length: 100 }, (_, i) => {
      const hour = String(Math.floor(i * 24 / 100)).padStart(2, '0');
      const min = String(Math.floor((i * 24 / 100 % 1) * 60)).padStart(2, '0');
      return `2025-06-15T${hour}:${min}:00Z`;
    });
    const result = checkVerification(gcsFile, timestamps, backfillUpdatesStream, BUCKET);
    expect(result.passed).toBe(true);
  });

  it('should verify migration=0 files correctly', () => {
    const gcsFile = 'gs://test-bucket/raw/backfill/updates/migration=0/year=2024/month=10/day=5/file.parquet';
    const result = checkVerification(
      gcsFile,
      ['2024-10-05T14:00:00Z'],
      backfillUpdatesStream,
      BUCKET,
    );
    expect(result.passed).toBe(true);
  });
});

// ── WHERE clause boundary tests (via getUtcPartition) ──────────────────────
// These verify the underlying partition function that drives the split WHERE clauses

describe('getUtcPartition boundary correctness (split WHERE clause safety)', () => {
  it('should assign 23:59:59.999Z to same day', () => {
    const p = getUtcPartition('2025-06-15T23:59:59.999Z');
    expect(p).toEqual({ year: 2025, month: 6, day: 15 });
  });

  it('should assign 00:00:00.000Z to new day', () => {
    const p = getUtcPartition('2025-06-16T00:00:00.000Z');
    expect(p).toEqual({ year: 2025, month: 6, day: 16 });
  });

  it('should handle DST-irrelevant UTC timestamps', () => {
    // March 9 2025 is US DST transition — should be irrelevant for UTC
    const p = getUtcPartition('2025-03-09T02:30:00Z');
    expect(p).toEqual({ year: 2025, month: 3, day: 9 });
  });

  it('should handle leap second adjacent timestamp', () => {
    const p = getUtcPartition('2024-12-31T23:59:59Z');
    expect(p).toEqual({ year: 2024, month: 12, day: 31 });
  });
});
