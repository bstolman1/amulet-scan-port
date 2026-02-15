import { describe, it, expect, vi, beforeEach } from 'vitest';
import { join } from 'path';

// Mock fs with importOriginal to preserve default export
vi.mock('fs', async (importOriginal) => {
  const actual = await importOriginal();
  return {
    ...actual,
    appendFileSync: vi.fn(),
    existsSync: vi.fn(() => true),
    mkdirSync: vi.fn(),
    readFileSync: vi.fn(),
    writeFileSync: vi.fn(),
    unlinkSync: vi.fn(),
    statSync: vi.fn(() => ({ size: 1024 })),
  };
});

vi.mock('child_process', async (importOriginal) => {
  const actual = await importOriginal();
  return {
    ...actual,
    spawn: vi.fn(),
    execSync: vi.fn(),
  };
});

import { appendFileSync, existsSync, readFileSync, writeFileSync, mkdirSync } from 'fs';
import { execSync } from 'child_process';

describe('Dead Letter Upload Recovery', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    existsSync.mockReturnValue(true);
  });

  describe('logFailedUpload', () => {
    it('should append a JSON entry to the dead-letter file', async () => {
      const { logFailedUpload } = await import('../gcs-upload-queue.js');
      
      logFailedUpload('/tmp/ledger_raw/backfill/updates/test.parquet', 'gs://bucket/raw/backfill/updates/test.parquet', 'Connection timeout');
      
      expect(appendFileSync).toHaveBeenCalledTimes(1);
      const [filePath, content] = appendFileSync.mock.calls[0];
      expect(filePath).toContain('failed-uploads.jsonl');
      
      const entry = JSON.parse(content.trim());
      expect(entry.localPath).toBe('/tmp/ledger_raw/backfill/updates/test.parquet');
      expect(entry.gcsPath).toBe('gs://bucket/raw/backfill/updates/test.parquet');
      expect(entry.error).toBe('Connection timeout');
      expect(entry.timestamp).toBeDefined();
      expect(entry.fileExists).toBe(true);
    });

    it('should create dead-letter directory if it does not exist', async () => {
      existsSync.mockImplementation((p) => {
        if (p.includes('failed-uploads') || p === '/tmp/ledger_raw') return false;
        return true;
      });
      
      const { logFailedUpload } = await import('../gcs-upload-queue.js');
      logFailedUpload('/tmp/test.parquet', 'gs://bucket/test.parquet', 'error');
      
      expect(mkdirSync).toHaveBeenCalledWith('/tmp/ledger_raw', { recursive: true });
    });

    it('should not throw if logging itself fails', async () => {
      appendFileSync.mockImplementation(() => { throw new Error('Disk full'); });
      
      const { logFailedUpload } = await import('../gcs-upload-queue.js');
      
      // Should not throw
      expect(() => {
        logFailedUpload('/tmp/test.parquet', 'gs://bucket/test.parquet', 'error');
      }).not.toThrow();
    });
  });

  describe('readDeadLetterLog', () => {
    it('should parse JSONL entries', async () => {
      const entries = [
        { localPath: '/tmp/a.parquet', gcsPath: 'gs://b/a.parquet', error: 'timeout', timestamp: '2026-01-01T00:00:00Z' },
        { localPath: '/tmp/b.parquet', gcsPath: 'gs://b/b.parquet', error: '503', timestamp: '2026-01-01T00:01:00Z' },
      ];
      readFileSync.mockReturnValue(entries.map(e => JSON.stringify(e)).join('\n'));
      
      const { readDeadLetterLog } = await import('../retry-failed-uploads.js');
      const result = readDeadLetterLog('/tmp/test.jsonl');
      
      expect(result).toHaveLength(2);
      expect(result[0].localPath).toBe('/tmp/a.parquet');
      expect(result[1].error).toBe('503');
    });

    it('should return empty array if file does not exist', async () => {
      existsSync.mockReturnValue(false);
      
      const { readDeadLetterLog } = await import('../retry-failed-uploads.js');
      const result = readDeadLetterLog('/tmp/nonexistent.jsonl');
      
      expect(result).toEqual([]);
    });

    it('should skip malformed lines', async () => {
      readFileSync.mockReturnValue('{"valid": true}\nnot-json\n{"also": "valid"}');
      
      const { readDeadLetterLog } = await import('../retry-failed-uploads.js');
      const result = readDeadLetterLog('/tmp/test.jsonl');
      
      expect(result).toHaveLength(2);
    });
  });

  describe('retryUpload', () => {
    it('should return ok:true on successful gsutil upload', async () => {
      execSync.mockReturnValue('');
      
      const { retryUpload } = await import('../retry-failed-uploads.js');
      const result = retryUpload('/tmp/test.parquet', 'gs://bucket/test.parquet');
      
      expect(result.ok).toBe(true);
      expect(execSync).toHaveBeenCalledWith(
        expect.stringContaining('gsutil -q cp'),
        expect.objectContaining({ timeout: 300000 })
      );
    });

    it('should return recoverable:false if local file is missing', async () => {
      existsSync.mockReturnValue(false);
      
      const { retryUpload } = await import('../retry-failed-uploads.js');
      const result = retryUpload('/tmp/missing.parquet', 'gs://bucket/test.parquet');
      
      expect(result.ok).toBe(false);
      expect(result.recoverable).toBe(false);
    });

    it('should return recoverable:true on transient gsutil failure', async () => {
      execSync.mockImplementation(() => { throw new Error('Connection timeout'); });
      
      const { retryUpload } = await import('../retry-failed-uploads.js');
      const result = retryUpload('/tmp/test.parquet', 'gs://bucket/test.parquet');
      
      expect(result.ok).toBe(false);
      expect(result.recoverable).toBe(true);
    });
  });

  describe('processDeadLetterLog', () => {
    it('should retry uploads and rewrite log with remaining failures', async () => {
      const entries = [
        { localPath: '/tmp/a.parquet', gcsPath: 'gs://b/a.parquet', error: 'timeout', timestamp: '2026-01-01T00:00:00Z' },
        { localPath: '/tmp/b.parquet', gcsPath: 'gs://b/b.parquet', error: '503', timestamp: '2026-01-01T00:01:00Z' },
      ];
      readFileSync.mockReturnValue(entries.map(e => JSON.stringify(e)).join('\n'));
      
      // First upload succeeds, second fails
      let callCount = 0;
      execSync.mockImplementation(() => {
        callCount++;
        if (callCount === 2) throw new Error('Still failing');
      });
      
      const { processDeadLetterLog } = await import('../retry-failed-uploads.js');
      const result = await processDeadLetterLog('/tmp/test.jsonl', false);
      
      expect(result.retried).toBe(1);
      expect(result.stillFailed).toBe(1);
      
      // Should rewrite the file with only the failed entry
      expect(writeFileSync).toHaveBeenCalled();
      const writtenContent = writeFileSync.mock.calls[0][1];
      const remaining = JSON.parse(writtenContent.trim());
      expect(remaining.gcsPath).toBe('gs://b/b.parquet');
    });

    it('should clear dead-letter file when all retries succeed', async () => {
      const entries = [
        { localPath: '/tmp/a.parquet', gcsPath: 'gs://b/a.parquet', error: 'timeout', timestamp: '2026-01-01T00:00:00Z' },
      ];
      readFileSync.mockReturnValue(JSON.stringify(entries[0]));
      execSync.mockReturnValue('');
      
      const { processDeadLetterLog } = await import('../retry-failed-uploads.js');
      const result = await processDeadLetterLog('/tmp/test.jsonl', false);
      
      expect(result.retried).toBe(1);
      expect(result.stillFailed).toBe(0);
      expect(writeFileSync).toHaveBeenCalledWith('/tmp/test.jsonl', '');
    });

    it('should not modify files in dry-run mode', async () => {
      readFileSync.mockReturnValue('{"localPath":"/tmp/a.parquet","gcsPath":"gs://b/a","error":"x","timestamp":"t"}');
      
      const { processDeadLetterLog } = await import('../retry-failed-uploads.js');
      await processDeadLetterLog('/tmp/test.jsonl', true);
      
      expect(execSync).not.toHaveBeenCalled();
      expect(writeFileSync).not.toHaveBeenCalled();
    });
  });
});
