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

// ─── Path Contract Tests ─────────────────────────────────────────────
// These are the MOST important tests: they ensure the scanner checks
// every path that getPartitionPath() can produce. If getPartitionPath
// ever adds a new source or type, these tests will catch it.

describe('GCS Scanner - Path Contract with getPartitionPath', () => {
  const BUCKET = 'test-bucket';

  it('should produce prefixes for every valid source×type combination', () => {
    const prefixes = getGCSScanPrefixes(BUCKET);
    
    // getPartitionPath has 2 sources × 2 types = 4 combinations
    expect(prefixes).toHaveLength(4);
    expect(prefixes).toContain(`gs://${BUCKET}/raw/updates/updates/`);
    expect(prefixes).toContain(`gs://${BUCKET}/raw/updates/events/`);
    expect(prefixes).toContain(`gs://${BUCKET}/raw/backfill/updates/`);
    expect(prefixes).toContain(`gs://${BUCKET}/raw/backfill/events/`);
  });

  it('should cover every getPartitionPath output prefix', () => {
    const prefixes = getGCSScanPrefixes(BUCKET);
    
    // Generate all possible getPartitionPath outputs
    const sources = ['backfill', 'updates'];
    const types = ['updates', 'events'];
    const testTimestamp = '2026-01-15T10:00:00Z';
    
    for (const source of sources) {
      for (const type of types) {
        const partPath = getPartitionPath(testTimestamp, 4, type, source);
        // partPath = "updates/events/migration=4/year=2026/month=1/day=15"
        const expectedPrefix = `raw/${partPath.split('/migration=')[0]}/`;
        
        const matchingPrefix = prefixes.find(p => p.includes(expectedPrefix));
        expect(matchingPrefix).toBeTruthy();
      }
    }
  });

  it('should match the exact directory structure getPartitionPath creates', () => {
    const prefixes = getGCSScanPrefixes(BUCKET);
    
    // Test with multiple timestamps and migrations
    const testCases = [
      { ts: '2025-06-15T12:00:00Z', mig: 2, type: 'updates', source: 'backfill' },
      { ts: '2026-01-27T23:59:59Z', mig: 4, type: 'updates', source: 'updates' },
      { ts: '2026-02-03T14:30:00Z', mig: 4, type: 'events', source: 'updates' },
      { ts: '2024-10-01T08:00:00Z', mig: 0, type: 'events', source: 'backfill' },
    ];
    
    for (const tc of testCases) {
      const partPath = getPartitionPath(tc.ts, tc.mig, tc.type, tc.source);
      // The full GCS path would be: gs://bucket/raw/{partPath}/file.parquet
      // Our prefix should cover raw/{source}/{type}/
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
    // Should be sorted descending
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
    expect(result[0].val).toBe(2026); // Descending
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

// ─── Timestamp Extraction ────────────────────────────────────────────

describe('GCS Scanner - extractTimestampFromGCSFiles', () => {
  it('should extract timestamp from Parquet filename', () => {
    const exec = vi.fn().mockReturnValue(
      'gs://bucket/.../day=3/updates_2026-02-03T14-30-45.123456Z.parquet\n' +
      'gs://bucket/.../day=3/updates_2026-02-03T10-15-00.000000Z.parquet'
    );
    
    const result = extractTimestampFromGCSFiles('gs://bucket/.../day=3/', '2026-02-03', exec);
    
    // Should pick the latest (sorted reverse), and convert dashes to colons
    expect(result).toBe('2026-02-03T14:30:45.123456Z');
  });

  it('should fall back to end-of-day when no parquet files exist', () => {
    const exec = vi.fn().mockReturnValue('');
    
    const result = extractTimestampFromGCSFiles('gs://bucket/.../day=3/', '2026-02-03', exec);
    expect(result).toBe('2026-02-03T23:59:59.999999Z');
  });

  it('should fall back to end-of-day when filenames have no timestamps', () => {
    const exec = vi.fn().mockReturnValue(
      'gs://bucket/.../day=3/data-chunk-001.parquet\n' +
      'gs://bucket/.../day=3/data-chunk-002.parquet'
    );
    
    const result = extractTimestampFromGCSFiles('gs://bucket/.../day=3/', '2026-01-27', exec);
    expect(result).toBe('2026-01-27T23:59:59.999999Z');
  });

  it('should fall back to end-of-day when gsutil fails', () => {
    const exec = vi.fn().mockImplementation(() => { throw new Error('No URLs matched'); });
    
    const result = extractTimestampFromGCSFiles('gs://bucket/.../day=3/', '2026-01-27', exec);
    expect(result).toBe('2026-01-27T23:59:59.999999Z');
  });

  it('should ignore non-parquet files', () => {
    const exec = vi.fn().mockReturnValue(
      'gs://bucket/.../day=3/updates_2026-02-03T14-30-45.123456Z.parquet\n' +
      'gs://bucket/.../day=3/_SUCCESS\n' +
      'gs://bucket/.../day=3/updates_2026-02-03T16-00-00.000000Z.json'
    );
    
    const result = extractTimestampFromGCSFiles('gs://bucket/.../day=3/', '2026-02-03', exec);
    // Only .parquet files considered
    expect(result).toBe('2026-02-03T14:30:45.123456Z');
  });
});

// ─── scanGCSDatePartitions ───────────────────────────────────────────

describe('GCS Scanner - scanGCSDatePartitions', () => {
  it('should walk year/month/day and return latest partition', () => {
    const exec = vi.fn()
      .mockReturnValueOnce('gs://b/migration=4/year=2025/\ngs://b/migration=4/year=2026/')          // years
      .mockReturnValueOnce('gs://b/.../year=2026/month=1/\ngs://b/.../year=2026/month=2/')           // months for 2026
      .mockReturnValueOnce('gs://b/.../month=2/day=1/\ngs://b/.../month=2/day=3/')                   // days for month=2
      .mockReturnValueOnce(                                                                           // files in day=3
        'gs://b/.../day=3/updates_2026-02-03T14-30-00.000000Z.parquet'
      );
    
    const result = scanGCSDatePartitions('gs://b/migration=4/', 4, exec);
    
    expect(result).not.toBeNull();
    expect(result.migrationId).toBe(4);
    expect(result.timestamp).toBe('2026-02-03T14:30:00.000000Z');
    expect(result.source).toContain('year=2026');
    expect(result.source).toContain('month=2');
    expect(result.source).toContain('day=3');
  });

  it('should return null for empty migration path', () => {
    const exec = vi.fn().mockReturnValue('');
    
    const result = scanGCSDatePartitions('gs://b/migration=4/', 4, exec);
    expect(result).toBeNull();
  });

  it('should skip empty month/day directories', () => {
    const exec = vi.fn()
      .mockReturnValueOnce('gs://b/migration=4/year=2026/')                                          // years
      .mockReturnValueOnce('gs://b/.../year=2026/month=1/')                                           // months for 2026
      .mockReturnValueOnce('');                                                                        // no days in month=1
    
    const result = scanGCSDatePartitions('gs://b/migration=4/', 4, exec);
    expect(result).toBeNull();
  });

  it('should return null when gsutil throws', () => {
    const exec = vi.fn().mockImplementation(() => { throw new Error('timeout'); });
    
    const result = scanGCSDatePartitions('gs://b/migration=4/', 4, exec);
    expect(result).toBeNull();
  });
});

// ─── scanGCSHivePartition ────────────────────────────────────────────

describe('GCS Scanner - scanGCSHivePartition', () => {
  it('should find latest migration and scan its date partitions', () => {
    const exec = vi.fn()
      // First call: list migrations
      .mockReturnValueOnce(
        'gs://b/raw/updates/updates/migration=3/\n' +
        'gs://b/raw/updates/updates/migration=4/'
      )
      // Second call: list years for migration=4
      .mockReturnValueOnce('gs://b/.../migration=4/year=2026/')
      // Third call: list months
      .mockReturnValueOnce('gs://b/.../year=2026/month=1/')
      // Fourth call: list days
      .mockReturnValueOnce('gs://b/.../month=1/day=27/')
      // Fifth call: list files in day
      .mockReturnValueOnce('gs://b/.../day=27/updates_2026-01-27T23-59-59.999999Z.parquet');
    
    const result = scanGCSHivePartition('gs://b/raw/updates/updates/', exec);
    
    expect(result).not.toBeNull();
    expect(result.migrationId).toBe(4);
    expect(result.timestamp).toBe('2026-01-27T23:59:59.999999Z');
  });

  it('should return null when no migrations exist', () => {
    const exec = vi.fn().mockReturnValue('');
    
    const result = scanGCSHivePartition('gs://b/raw/updates/updates/', exec);
    expect(result).toBeNull();
  });

  it('should return null when gsutil ls fails (prefix doesnt exist)', () => {
    const exec = vi.fn().mockImplementation(() => {
      throw new Error('CommandException: One or more URLs matched no objects');
    });
    
    const result = scanGCSHivePartition('gs://b/raw/updates/events/', exec);
    expect(result).toBeNull();
  });
});

// ─── findLatestFromGCS (Integration-style) ───────────────────────────

describe('GCS Scanner - findLatestFromGCS', () => {
  it('should scan all 4 prefixes and return the best result', () => {
    const callLog = [];
    
    const exec = vi.fn().mockImplementation((cmd) => {
      callLog.push(cmd);
      
      // raw/updates/updates/ → has migration=4 data up to Feb 3
      if (cmd.includes('raw/updates/updates/"') && !cmd.includes('migration=')) {
        return 'gs://b/raw/updates/updates/migration=4/';
      }
      if (cmd.includes('migration=4/"') && cmd.includes('raw/updates/updates')) {
        return 'gs://b/.../migration=4/year=2026/';
      }
      
      // raw/updates/events/ → has migration=4 data up to Feb 3
      if (cmd.includes('raw/updates/events/"') && !cmd.includes('migration=')) {
        return 'gs://b/raw/updates/events/migration=4/';
      }
      if (cmd.includes('migration=4/"') && cmd.includes('raw/updates/events')) {
        return 'gs://b/.../migration=4/year=2026/';
      }
      
      // raw/backfill/updates/ → has migration=4 data up to Jan 27
      if (cmd.includes('raw/backfill/updates/"') && !cmd.includes('migration=')) {
        return 'gs://b/raw/backfill/updates/migration=4/';
      }
      if (cmd.includes('migration=4/"') && cmd.includes('raw/backfill/updates')) {
        return 'gs://b/.../migration=4/year=2026/';
      }
      
      // raw/backfill/events/ → empty
      if (cmd.includes('raw/backfill/events/"')) {
        return '';
      }
      
      // Year listing → month
      if (cmd.includes('year=2026/"')) {
        // For updates/updates and updates/events → Feb
        if (callLog.some(c => c.includes('raw/updates/'))) {
          return 'gs://b/.../year=2026/month=2/';
        }
        // For backfill → Jan
        return 'gs://b/.../year=2026/month=1/';
      }
      
      // Month=2 → day=3
      if (cmd.includes('month=2/"')) {
        return 'gs://b/.../month=2/day=3/';
      }
      // Month=1 → day=27
      if (cmd.includes('month=1/"')) {
        return 'gs://b/.../month=1/day=27/';
      }
      
      // Day files
      if (cmd.includes('day=3/"')) {
        return 'gs://b/.../day=3/updates_2026-02-03T14-30-00.000000Z.parquet';
      }
      if (cmd.includes('day=27/"')) {
        return 'gs://b/.../day=27/updates_2026-01-27T23-59-59.999999Z.parquet';
      }
      
      return '';
    });
    
    const logEntries = [];
    const logFn = (level, msg, data) => logEntries.push({ level, msg, data });
    
    const result = findLatestFromGCS({ bucket: 'b', execFn: exec, logFn: logFn });
    
    expect(result).not.toBeNull();
    // Feb 3 should win over Jan 27
    expect(result.timestamp).toBe('2026-02-03T14:30:00.000000Z');
    expect(result.migrationId).toBe(4);
  });

  it('should return null when no bucket is configured', () => {
    const result = findLatestFromGCS({ bucket: null });
    expect(result).toBeNull();
  });

  it('should handle all prefixes failing gracefully', () => {
    const exec = vi.fn().mockImplementation(() => {
      throw new Error('Network unreachable');
    });
    
    const result = findLatestFromGCS({ bucket: 'b', execFn: exec });
    expect(result).toBeNull();
  });

  it('should prefer higher migration over more recent date', () => {
    const exec = vi.fn().mockImplementation((cmd) => {
      // updates/updates has migration=5, Jan 1
      if (cmd.includes('raw/updates/updates/"') && !cmd.includes('migration=')) {
        return 'gs://b/raw/updates/updates/migration=5/';
      }
      if (cmd.includes('migration=5/"')) {
        return 'gs://b/.../migration=5/year=2026/';
      }
      
      // backfill/updates has migration=4, Feb 28
      if (cmd.includes('raw/backfill/updates/"') && !cmd.includes('migration=')) {
        return 'gs://b/raw/backfill/updates/migration=4/';
      }
      if (cmd.includes('migration=4/"')) {
        return 'gs://b/.../migration=4/year=2026/';
      }
      
      // Other prefixes empty
      if (cmd.includes('raw/updates/events/"') || cmd.includes('raw/backfill/events/"')) {
        return '';
      }
      
      // Year → month branching
      if (cmd.includes('year=2026/"')) {
        // Check if we're in migration=5 context (Jan) or migration=4 (Feb)
        return 'gs://b/.../year=2026/month=1/\ngs://b/.../year=2026/month=2/';
      }
      if (cmd.includes('month=2/"')) return 'gs://b/.../month=2/day=28/';
      if (cmd.includes('month=1/"')) return 'gs://b/.../month=1/day=1/';
      if (cmd.includes('day=28/"')) return 'gs://b/.../day=28/u_2026-02-28T12-00-00.000000Z.parquet';
      if (cmd.includes('day=1/"')) return 'gs://b/.../day=1/u_2026-01-01T00-00-01.000000Z.parquet';
      
      return '';
    });
    
    const result = findLatestFromGCS({ bucket: 'b', execFn: exec });
    
    expect(result).not.toBeNull();
    // Migration 5 should win even though migration 4 has a later date
    expect(result.migrationId).toBe(5);
  });

  it('should pick later timestamp when migration IDs are equal', () => {
    const exec = vi.fn().mockImplementation((cmd) => {
      // Both prefixes have migration=4 but different dates
      if (cmd.includes('raw/updates/updates/"') && !cmd.includes('migration=')) {
        return 'gs://b/raw/updates/updates/migration=4/';
      }
      if (cmd.includes('raw/backfill/updates/"') && !cmd.includes('migration=')) {
        return 'gs://b/raw/backfill/updates/migration=4/';
      }
      if (cmd.includes('raw/updates/events/"') || cmd.includes('raw/backfill/events/"')) {
        return '';
      }
      
      if (cmd.includes('migration=4/"')) {
        return 'gs://b/.../migration=4/year=2026/';
      }
      if (cmd.includes('year=2026/"')) {
        return 'gs://b/.../year=2026/month=2/';
      }
      if (cmd.includes('month=2/"')) {
        return 'gs://b/.../month=2/day=3/\ngs://b/.../month=2/day=5/';
      }
      if (cmd.includes('day=5/"')) {
        return 'gs://b/.../day=5/u_2026-02-05T10-00-00.000000Z.parquet';
      }
      if (cmd.includes('day=3/"')) {
        return 'gs://b/.../day=3/u_2026-02-03T14-30-00.000000Z.parquet';
      }
      
      return '';
    });
    
    const result = findLatestFromGCS({ bucket: 'b', execFn: exec });
    
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
    
    // The bug was scanning gs://bucket/raw/updates/ which would find
    // migration= dirs at the wrong level. Must scan the nested path.
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
