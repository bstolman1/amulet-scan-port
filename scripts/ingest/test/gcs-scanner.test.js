/**
 * GCS Scanner Tests
 * 
 * Comprehensive tests for GCS Hive partition scanning.
 * Verifies:
 * - ALL paths from getPartitionPath() are scanned (path contract)
 * - Migration/year/month/day parsing from gsutil output
 * - Timestamp extraction from Parquet filenames
 * - Best-result selection across multiple prefixes
 * - Edge cases: empty buckets, missing partitions, gsutil failures
 *
 * UPDATED: All scanner functions are now async (FIX #1 in gcs-scanner.js).
 * - exec mocks return Promises (mockResolvedValue / mockRejectedValue)
 * - All test functions are async with await
 * - Fallback timestamps use end-of-day-minus-5-min (FIX #3)
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
  getGCSScanPrefixes,
  findLatestFromGCS,
  scanGCSHivePartition,
  scanGCSDatePartitions,
  extractTimestampFromGCSFiles,
  parseMigrationDirs,
  parsePartitionValues,
} from '../gcs-scanner.js';
import { getPartitionPath } from '../data-schema.js';

// Helper: compute the end-of-day-minus-5-min fallback for a given date string
function expectedFallback(dateStr) {
  const d = new Date(`${dateStr}T23:59:59.999Z`);
  d.setMinutes(d.getMinutes() - 5);
  return d.toISOString();
}

// ─── Path Contract Tests ─────────────────────────────────────────────

describe('GCS Scanner - Path Contract with getPartitionPath', () => {
  const BUCKET = 'test-bucket';

  it('should produce prefixes for every valid source×type combination', () => {
    const prefixes = getGCSScanPrefixes(BUCKET);
    
    expect(prefixes).toHaveLength(4);
    expect(prefixes).toContain(`gs://${BUCKET}/raw/updates/updates/`);
    expect(prefixes).toContain(`gs://${BUCKET}/raw/updates/events/`);
    expect(prefixes).toContain(`gs://${BUCKET}/raw/backfill/updates/`);
    expect(prefixes).toContain(`gs://${BUCKET}/raw/backfill/events/`);
  });

  it('should cover every getPartitionPath output prefix', () => {
    const prefixes = getGCSScanPrefixes(BUCKET);
    
    const sources = ['backfill', 'updates'];
    const types = ['updates', 'events'];
    const testTimestamp = '2026-01-15T10:00:00Z';
    
    for (const source of sources) {
      for (const type of types) {
        const partPath = getPartitionPath(testTimestamp, 4, type, source);
        const expectedPrefix = `raw/${partPath.split('/migration=')[0]}/`;
        
        const matchingPrefix = prefixes.find(p => p.includes(expectedPrefix));
        expect(matchingPrefix).toBeTruthy();
      }
    }
  });

  it('should match the exact directory structure getPartitionPath creates', () => {
    const prefixes = getGCSScanPrefixes(BUCKET);
    
    const testCases = [
      { ts: '2025-06-15T12:00:00Z', mig: 2, type: 'updates', source: 'backfill' },
      { ts: '2026-01-27T23:59:59Z', mig: 4, type: 'updates', source: 'updates' },
      { ts: '2026-02-03T14:30:00Z', mig: 4, type: 'events', source: 'updates' },
      { ts: '2024-10-01T08:00:00Z', mig: 0, type: 'events', source: 'backfill' },
    ];
    
    for (const tc of testCases) {
      const partPath = getPartitionPath(tc.ts, tc.mig, tc.type, tc.source);
      const fullGCSPath = `gs://${BUCKET}/raw/${partPath}/data.parquet`;
      
      const coveredByPrefix = prefixes.some(prefix => fullGCSPath.startsWith(prefix));
      expect(coveredByPrefix).toBe(true);
    }
  });

  it('should return empty array for empty/null bucket', () => {
    expect(getGCSScanPrefixes('')).toEqual([]);
    expect(getGCSScanPrefixes(null)).toEqual([]);
    expect(getGCSScanPrefixes(undefined)).toEqual([]);
  });
});

// ─── Migration Directory Parsing ─────────────────────────────────────

describe('GCS Scanner - parseMigrationDirs', () => {
  it('should parse migration directories from gsutil output', () => {
    const output = [
      'gs://bucket/raw/updates/updates/migration=0/',
      'gs://bucket/raw/updates/updates/migration=2/',
      'gs://bucket/raw/updates/updates/migration=4/',
      'gs://bucket/raw/updates/updates/migration=3/',
    ].join('\n');
    
    const result = parseMigrationDirs(output);
    
    expect(result).toHaveLength(4);
    expect(result[0].id).toBe(4);
    expect(result[1].id).toBe(3);
    expect(result[2].id).toBe(2);
    expect(result[3].id).toBe(0);
    expect(result[0].path).toContain('migration=4');
  });

  it('should handle empty output', () => {
    expect(parseMigrationDirs('')).toEqual([]);
    expect(parseMigrationDirs(null)).toEqual([]);
    expect(parseMigrationDirs(undefined)).toEqual([]);
  });

  it('should skip non-migration lines', () => {
    const output = [
      'gs://bucket/raw/updates/updates/',
      'gs://bucket/raw/updates/updates/migration=4/',
      'gs://bucket/raw/updates/updates/some-other-dir/',
    ].join('\n');
    
    const result = parseMigrationDirs(output);
    expect(result).toHaveLength(1);
    expect(result[0].id).toBe(4);
  });
});

// ─── Partition Value Parsing ─────────────────────────────────────────

describe('GCS Scanner - parsePartitionValues', () => {
  it('should parse year partitions', () => {
    const output = [
      'gs://bucket/raw/.../migration=4/year=2025/',
      'gs://bucket/raw/.../migration=4/year=2026/',
    ].join('\n');
    
    const result = parsePartitionValues(output, 'year');
    expect(result).toHaveLength(2);
    expect(result[0].val).toBe(2026);
    expect(result[1].val).toBe(2025);
  });

  it('should parse month partitions (unpadded)', () => {
    const output = [
      'gs://bucket/.../year=2026/month=1/',
      'gs://bucket/.../year=2026/month=2/',
      'gs://bucket/.../year=2026/month=12/',
    ].join('\n');
    
    const result = parsePartitionValues(output, 'month');
    expect(result).toHaveLength(3);
    expect(result[0].val).toBe(12);
    expect(result[1].val).toBe(2);
    expect(result[2].val).toBe(1);
  });

  it('should parse day partitions (unpadded)', () => {
    const output = [
      'gs://bucket/.../month=1/day=1/',
      'gs://bucket/.../month=1/day=15/',
      'gs://bucket/.../month=1/day=27/',
    ].join('\n');
    
    const result = parsePartitionValues(output, 'day');
    expect(result).toHaveLength(3);
    expect(result[0].val).toBe(27);
    expect(result[1].val).toBe(15);
    expect(result[2].val).toBe(1);
  });

  it('should handle empty/null output', () => {
    expect(parsePartitionValues('', 'year')).toEqual([]);
    expect(parsePartitionValues(null, 'year')).toEqual([]);
  });
});

// ─── Timestamp Extraction (now async) ────────────────────────────────

describe('GCS Scanner - extractTimestampFromGCSFiles', () => {
  it('should extract timestamp from Parquet filename', async () => {
    const exec = vi.fn().mockResolvedValue(
      'gs://bucket/.../day=3/updates_2026-02-03T14-30-45.123456Z.parquet\n' +
      'gs://bucket/.../day=3/updates_2026-02-03T10-15-00.000000Z.parquet'
    );
    
    const result = await extractTimestampFromGCSFiles('gs://bucket/.../day=3/', '2026-02-03', exec);
    
    expect(result).toBe('2026-02-03T14:30:45.123456Z');
  });

  it('should fall back to end-of-day-minus-5-min when no parquet files exist', async () => {
    const exec = vi.fn().mockResolvedValue('');
    
    const result = await extractTimestampFromGCSFiles('gs://bucket/.../day=3/', '2026-02-03', exec);
    expect(result).toBe(expectedFallback('2026-02-03'));
  });

  it('should fall back to end-of-day-minus-5-min when filenames have no timestamps', async () => {
    const exec = vi.fn().mockResolvedValue(
      'gs://bucket/.../day=3/data-chunk-001.parquet\n' +
      'gs://bucket/.../day=3/data-chunk-002.parquet'
    );
    
    const result = await extractTimestampFromGCSFiles('gs://bucket/.../day=3/', '2026-01-27', exec);
    expect(result).toBe(expectedFallback('2026-01-27'));
  });

  it('should fall back to end-of-day-minus-5-min when gsutil fails', async () => {
    const exec = vi.fn().mockRejectedValue(new Error('No URLs matched'));
    
    const result = await extractTimestampFromGCSFiles('gs://bucket/.../day=3/', '2026-01-27', exec);
    expect(result).toBe(expectedFallback('2026-01-27'));
  });

  it('should ignore non-parquet files', async () => {
    const exec = vi.fn().mockResolvedValue(
      'gs://bucket/.../day=3/updates_2026-02-03T14-30-45.123456Z.parquet\n' +
      'gs://bucket/.../day=3/_SUCCESS\n' +
      'gs://bucket/.../day=3/updates_2026-02-03T16-00-00.000000Z.json'
    );
    
    const result = await extractTimestampFromGCSFiles('gs://bucket/.../day=3/', '2026-02-03', exec);
    expect(result).toBe('2026-02-03T14:30:45.123456Z');
  });
});

// ─── scanGCSDatePartitions (now async) ───────────────────────────────

describe('GCS Scanner - scanGCSDatePartitions', () => {
  it('should walk year/month/day and return latest partition', async () => {
    const exec = vi.fn()
      .mockResolvedValueOnce('gs://b/migration=4/year=2025/\ngs://b/migration=4/year=2026/')
      .mockResolvedValueOnce('gs://b/.../year=2026/month=1/\ngs://b/.../year=2026/month=2/')
      .mockResolvedValueOnce('gs://b/.../month=2/day=1/\ngs://b/.../month=2/day=3/')
      .mockResolvedValueOnce(
        'gs://b/.../day=3/updates_2026-02-03T14-30-00.000000Z.parquet'
      );
    
    const result = await scanGCSDatePartitions('gs://b/migration=4/', 4, exec);
    
    expect(result).not.toBeNull();
    expect(result.migrationId).toBe(4);
    expect(result.timestamp).toBe('2026-02-03T14:30:00.000000Z');
    expect(result.source).toContain('year=2026');
    expect(result.source).toContain('month=2');
    expect(result.source).toContain('day=3');
  });

  it('should return null for empty migration path', async () => {
    const exec = vi.fn().mockResolvedValue('');
    
    const result = await scanGCSDatePartitions('gs://b/migration=4/', 4, exec);
    expect(result).toBeNull();
  });

  it('should skip empty month/day directories', async () => {
    const exec = vi.fn()
      .mockResolvedValueOnce('gs://b/migration=4/year=2026/')
      .mockResolvedValueOnce('gs://b/.../year=2026/month=1/')
      .mockResolvedValueOnce('');
    
    const result = await scanGCSDatePartitions('gs://b/migration=4/', 4, exec);
    expect(result).toBeNull();
  });

  it('should return null when gsutil throws', async () => {
    const exec = vi.fn().mockRejectedValue(new Error('timeout'));
    
    const result = await scanGCSDatePartitions('gs://b/migration=4/', 4, exec);
    expect(result).toBeNull();
  });
});

// ─── scanGCSHivePartition (now async) ────────────────────────────────

describe('GCS Scanner - scanGCSHivePartition', () => {
  it('should find latest migration and scan its date partitions', async () => {
    const exec = vi.fn()
      .mockResolvedValueOnce(
        'gs://b/raw/updates/updates/migration=3/\n' +
        'gs://b/raw/updates/updates/migration=4/'
      )
      .mockResolvedValueOnce('gs://b/.../migration=4/year=2026/')
      .mockResolvedValueOnce('gs://b/.../year=2026/month=1/')
      .mockResolvedValueOnce('gs://b/.../month=1/day=27/')
      .mockResolvedValueOnce('gs://b/.../day=27/updates_2026-01-27T23-59-59.999999Z.parquet');
    
    const result = await scanGCSHivePartition('gs://b/raw/updates/updates/', exec);
    
    expect(result).not.toBeNull();
    expect(result.migrationId).toBe(4);
    expect(result.timestamp).toBe('2026-01-27T23:59:59.999999Z');
  });

  it('should return null when no migrations exist', async () => {
    const exec = vi.fn().mockResolvedValue('');
    
    const result = await scanGCSHivePartition('gs://b/raw/updates/updates/', exec);
    expect(result).toBeNull();
  });

  it('should return null when gsutil ls fails (prefix doesnt exist)', async () => {
    const exec = vi.fn().mockRejectedValue(
      new Error('CommandException: One or more URLs matched no objects')
    );
    
    const result = await scanGCSHivePartition('gs://b/raw/updates/events/', exec);
    expect(result).toBeNull();
  });
});

// ─── findLatestFromGCS (now async) ───────────────────────────────────

describe('GCS Scanner - findLatestFromGCS', () => {
  it('should scan all 4 prefixes and return the best result', async () => {
    // Use mockResolvedValueOnce for exact call ordering.
    // Prefix order from getGCSScanPrefixes: updates/updates, updates/events, backfill/updates, backfill/events
    const exec = vi.fn()
      // Prefix 1: raw/updates/updates/ — migration=4, Feb 3
      .mockResolvedValueOnce('gs://b/raw/updates/updates/migration=4/')           // ls prefix
      .mockResolvedValueOnce('gs://b/.../migration=4/year=2026/')                 // ls migration
      .mockResolvedValueOnce('gs://b/.../year=2026/month=2/')                     // ls year
      .mockResolvedValueOnce('gs://b/.../month=2/day=3/')                         // ls month
      .mockResolvedValueOnce('gs://b/.../day=3/updates_2026-02-03T14-30-00.000000Z.parquet') // ls day
      // Prefix 2: raw/updates/events/ — migration=4, Feb 3 (same)
      .mockResolvedValueOnce('gs://b/raw/updates/events/migration=4/')
      .mockResolvedValueOnce('gs://b/.../migration=4/year=2026/')
      .mockResolvedValueOnce('gs://b/.../year=2026/month=2/')
      .mockResolvedValueOnce('gs://b/.../month=2/day=3/')
      .mockResolvedValueOnce('gs://b/.../day=3/updates_2026-02-03T14-30-00.000000Z.parquet')
      // Prefix 3: raw/backfill/updates/ — migration=4, Jan 27
      .mockResolvedValueOnce('gs://b/raw/backfill/updates/migration=4/')
      .mockResolvedValueOnce('gs://b/.../migration=4/year=2026/')
      .mockResolvedValueOnce('gs://b/.../year=2026/month=1/')
      .mockResolvedValueOnce('gs://b/.../month=1/day=27/')
      .mockResolvedValueOnce('gs://b/.../day=27/updates_2026-01-27T23-59-59.999999Z.parquet')
      // Prefix 4: raw/backfill/events/ — empty
      .mockResolvedValueOnce('');
    
    const logEntries = [];
    const logFn = (level, msg, data) => logEntries.push({ level, msg, data });
    
    const result = await findLatestFromGCS({ bucket: 'b', execFn: exec, logFn: logFn });
    
    expect(result).not.toBeNull();
    // Feb 3 should win over Jan 27
    expect(result.timestamp).toBe('2026-02-03T14:30:00.000000Z');
    expect(result.migrationId).toBe(4);
  });

  it('should return null when no bucket is configured', async () => {
    const result = await findLatestFromGCS({ bucket: null });
    expect(result).toBeNull();
  });

  it('should handle all prefixes failing gracefully', async () => {
    const exec = vi.fn().mockRejectedValue(new Error('Network unreachable'));
    
    const result = await findLatestFromGCS({ bucket: 'b', execFn: exec });
    expect(result).toBeNull();
  });

  it('should prefer higher migration over more recent date', async () => {
    const exec = vi.fn()
      // Prefix 1: raw/updates/updates/ — migration=5, Jan 1
      .mockResolvedValueOnce('gs://b/raw/updates/updates/migration=5/')
      .mockResolvedValueOnce('gs://b/.../migration=5/year=2026/')
      .mockResolvedValueOnce('gs://b/.../year=2026/month=1/')
      .mockResolvedValueOnce('gs://b/.../month=1/day=1/')
      .mockResolvedValueOnce('gs://b/.../day=1/u_2026-01-01T00-00-01.000000Z.parquet')
      // Prefix 2: raw/updates/events/ — empty
      .mockResolvedValueOnce('')
      // Prefix 3: raw/backfill/updates/ — migration=4, Feb 28
      .mockResolvedValueOnce('gs://b/raw/backfill/updates/migration=4/')
      .mockResolvedValueOnce('gs://b/.../migration=4/year=2026/')
      .mockResolvedValueOnce('gs://b/.../year=2026/month=2/')
      .mockResolvedValueOnce('gs://b/.../month=2/day=28/')
      .mockResolvedValueOnce('gs://b/.../day=28/u_2026-02-28T12-00-00.000000Z.parquet')
      // Prefix 4: raw/backfill/events/ — empty
      .mockResolvedValueOnce('');
    
    const result = await findLatestFromGCS({ bucket: 'b', execFn: exec });
    
    expect(result).not.toBeNull();
    // Migration 5 should win even though migration 4 has a later date
    expect(result.migrationId).toBe(5);
  });

  it('should pick later timestamp when migration IDs are equal', async () => {
    const exec = vi.fn()
      // Prefix 1: raw/updates/updates/ — migration=4, Feb 5
      .mockResolvedValueOnce('gs://b/raw/updates/updates/migration=4/')
      .mockResolvedValueOnce('gs://b/.../migration=4/year=2026/')
      .mockResolvedValueOnce('gs://b/.../year=2026/month=2/')
      .mockResolvedValueOnce('gs://b/.../month=2/day=3/\ngs://b/.../month=2/day=5/')
      .mockResolvedValueOnce('gs://b/.../day=5/u_2026-02-05T10-00-00.000000Z.parquet')
      // Prefix 2: raw/updates/events/ — empty
      .mockResolvedValueOnce('')
      // Prefix 3: raw/backfill/updates/ — migration=4, Feb 3
      .mockResolvedValueOnce('gs://b/raw/backfill/updates/migration=4/')
      .mockResolvedValueOnce('gs://b/.../migration=4/year=2026/')
      .mockResolvedValueOnce('gs://b/.../year=2026/month=2/')
      .mockResolvedValueOnce('gs://b/.../month=2/day=3/')
      .mockResolvedValueOnce('gs://b/.../day=3/u_2026-02-03T14-30-00.000000Z.parquet')
      // Prefix 4: raw/backfill/events/ — empty
      .mockResolvedValueOnce('');
    
    const result = await findLatestFromGCS({ bucket: 'b', execFn: exec });
    
    expect(result).not.toBeNull();
    // Feb 5 should beat Feb 3 at same migration
    expect(result.timestamp).toBe('2026-02-05T10:00:00.000000Z');
    expect(result.migrationId).toBe(4);
  });
});

// ─── Regression: exact bug that was found ────────────────────────────

describe('GCS Scanner - Regression tests', () => {
  it('REGRESSION: must scan raw/updates/updates/ not raw/updates/ (the original bug)', () => {
    const prefixes = getGCSScanPrefixes('my-bucket');
    
    expect(prefixes).not.toContain('gs://my-bucket/raw/updates/');
    expect(prefixes).toContain('gs://my-bucket/raw/updates/updates/');
  });

  it('REGRESSION: must scan raw/updates/events/ (the second missing path)', () => {
    const prefixes = getGCSScanPrefixes('my-bucket');
    
    expect(prefixes).toContain('gs://my-bucket/raw/updates/events/');
  });

  it('REGRESSION: must include backfill paths', () => {
    const prefixes = getGCSScanPrefixes('my-bucket');
    
    expect(prefixes).toContain('gs://my-bucket/raw/backfill/updates/');
    expect(prefixes).toContain('gs://my-bucket/raw/backfill/events/');
  });
});
