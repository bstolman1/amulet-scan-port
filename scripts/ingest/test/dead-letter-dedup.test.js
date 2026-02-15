/**
 * Tests for dead-letter deduplication in retry-failed-uploads.js
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

const {
  mockExistsSync, mockReadFileSync, mockWriteFileSync,
  mockUnlinkSync, mockStatSync, mockExecSync,
} = vi.hoisted(() => ({
  mockExistsSync: vi.fn(),
  mockReadFileSync: vi.fn(),
  mockWriteFileSync: vi.fn(),
  mockUnlinkSync: vi.fn(),
  mockStatSync: vi.fn(),
  mockExecSync: vi.fn(),
}));

vi.mock('fs', async (importOriginal) => {
  const actual = await importOriginal();
  return {
    ...actual,
    default: { ...actual, existsSync: mockExistsSync, readFileSync: mockReadFileSync, writeFileSync: mockWriteFileSync, unlinkSync: mockUnlinkSync, statSync: mockStatSync },
    existsSync: mockExistsSync,
    readFileSync: mockReadFileSync,
    writeFileSync: mockWriteFileSync,
    unlinkSync: mockUnlinkSync,
    statSync: mockStatSync,
  };
});

vi.mock('child_process', () => ({
  execSync: mockExecSync,
  spawn: vi.fn(),
  default: { execSync: mockExecSync, spawn: vi.fn() },
}));

describe('processDeadLetterLog deduplication', () => {
  let processDeadLetterLog;

  beforeEach(async () => {
    vi.clearAllMocks();
    const mod = await import('../retry-failed-uploads.js');
    processDeadLetterLog = mod.processDeadLetterLog;
  });

  it('deduplicates entries with the same gcsPath, keeping the latest', () => {
    const entries = [
      { localPath: '/tmp/a.parquet', gcsPath: 'gs://b/a.parquet', error: 'fail1', timestamp: '2025-01-01T00:00:00Z' },
      { localPath: '/tmp/a.parquet', gcsPath: 'gs://b/a.parquet', error: 'fail2', timestamp: '2025-01-01T01:00:00Z' },
      { localPath: '/tmp/b.parquet', gcsPath: 'gs://b/b.parquet', error: 'fail3', timestamp: '2025-01-01T00:00:00Z' },
    ];
    
    // Mock file reading
    mockExistsSync.mockImplementation((p) => {
      if (typeof p === 'string' && p.endsWith('.jsonl')) return true;
      return false; // local files don't exist
    });
    mockReadFileSync.mockReturnValue(entries.map(e => JSON.stringify(e)).join('\n'));

    const stats = processDeadLetterLog('/tmp/test-dl.jsonl');

    expect(stats.deduplicated).toBe(1);
    expect(stats.total).toBe(3); // raw count
    expect(stats.unique).toBe(2); // after dedup
    // Both unique entries should have noFile since mockExistsSync returns false for parquet files
    expect(stats.noFile).toBe(2);
  });

  it('returns 0 deduplicated when all entries are unique', () => {
    const entries = [
      { localPath: '/tmp/a.parquet', gcsPath: 'gs://b/a.parquet', error: 'fail', timestamp: '2025-01-01T00:00:00Z' },
      { localPath: '/tmp/b.parquet', gcsPath: 'gs://b/b.parquet', error: 'fail', timestamp: '2025-01-01T00:00:00Z' },
    ];

    mockExistsSync.mockImplementation((p) => {
      if (typeof p === 'string' && p.endsWith('.jsonl')) return true;
      return false;
    });
    mockReadFileSync.mockReturnValue(entries.map(e => JSON.stringify(e)).join('\n'));

    const stats = processDeadLetterLog('/tmp/test-dl.jsonl');

    expect(stats.deduplicated).toBe(0);
    expect(stats.unique).toBe(2);
  });

  it('returns early with zero stats when dead-letter is empty', () => {
    mockExistsSync.mockReturnValue(false);

    const stats = processDeadLetterLog('/tmp/empty.jsonl');

    expect(stats.total).toBe(0);
    expect(stats.deduplicated).toBe(0);
  });
});
