/**
 * Unit Tests for repair-partitions.js
 * 
 * Tests the pure logic functions: partition parsing, repair action determination,
 * and verification checking — without any GCS or filesystem side effects.
 */

import { describe, it, expect } from 'vitest';
import {
  parseMigrationFromPath,
  parseCurrentPartition,
  determineRepairAction,
  checkVerification,
} from '../repair-partitions.js';
import { getPartitionPath } from '../data-schema.js';
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
      // File is in day=15 and timestamp is Jan 15 UTC → correct
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
  });

  describe('move cases', () => {
    it('should detect file in wrong day partition', () => {
      // File is in day=15 but timestamp is Jan 16 UTC (was local time +1)
      const gcsFile = 'gs://test-bucket/raw/backfill/updates/migration=4/year=2025/month=1/day=15/file.parquet';
      const result = determineRepairAction(
        gcsFile,
        ['2025-01-16T00:30:00Z'],  // Just after midnight UTC → day 16
        backfillUpdatesStream,
        BUCKET,
      );
      expect(result.action).toBe('move');
      expect(result.to).toContain('day=16');
      expect(result.from).toBe(gcsFile);
    });

    it('should detect file in wrong month partition', () => {
      // File in month=12/day=31 but timestamp is Jan 1 UTC
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
      // File in day=14 but snapshot_time is June 15 UTC
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
});
