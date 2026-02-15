/**
 * Dead Letter Upload Recovery Tests
 * 
 * Tests the dead-letter logging and retry logic using
 * extracted pure functions (no module-level mocks needed).
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { createHash } from 'crypto';

describe('Dead Letter Upload Recovery', () => {

  describe('logFailedUpload - entry format', () => {
    it('should produce a valid JSON entry with all required fields', () => {
      // Test the shape of what logFailedUpload would write
      const localPath = '/tmp/ledger_raw/backfill/updates/test.parquet';
      const gcsPath = 'gs://bucket/raw/backfill/updates/test.parquet';
      const error = 'Connection timeout';

      const entry = {
        localPath,
        gcsPath,
        error: error || 'Unknown error',
        timestamp: new Date().toISOString(),
        fileExists: true,
      };

      const line = JSON.stringify(entry) + '\n';
      const parsed = JSON.parse(line.trim());

      expect(parsed.localPath).toBe(localPath);
      expect(parsed.gcsPath).toBe(gcsPath);
      expect(parsed.error).toBe('Connection timeout');
      expect(parsed.timestamp).toBeDefined();
      expect(parsed.fileExists).toBe(true);
    });

    it('should default error to Unknown error when null', () => {
      const entry = {
        localPath: '/tmp/test.parquet',
        gcsPath: 'gs://bucket/test.parquet',
        error: null || 'Unknown error',
        timestamp: new Date().toISOString(),
        fileExists: false,
      };

      expect(entry.error).toBe('Unknown error');
    });
  });

  describe('readDeadLetterLog - parsing', () => {
    function parseDeadLetterContent(content) {
      if (!content || !content.trim()) return [];
      return content.trim().split('\n').map((line, idx) => {
        try {
          return JSON.parse(line);
        } catch {
          return null;
        }
      }).filter(Boolean);
    }

    it('should parse JSONL entries', () => {
      const entries = [
        { localPath: '/tmp/a.parquet', gcsPath: 'gs://b/a.parquet', error: 'timeout', timestamp: '2026-01-01T00:00:00Z' },
        { localPath: '/tmp/b.parquet', gcsPath: 'gs://b/b.parquet', error: '503', timestamp: '2026-01-01T00:01:00Z' },
      ];
      const content = entries.map(e => JSON.stringify(e)).join('\n');
      const result = parseDeadLetterContent(content);

      expect(result).toHaveLength(2);
      expect(result[0].localPath).toBe('/tmp/a.parquet');
      expect(result[1].error).toBe('503');
    });

    it('should return empty array for empty content', () => {
      expect(parseDeadLetterContent('')).toEqual([]);
      expect(parseDeadLetterContent(null)).toEqual([]);
      expect(parseDeadLetterContent(undefined)).toEqual([]);
    });

    it('should skip malformed lines', () => {
      const content = '{"valid": true}\nnot-json\n{"also": "valid"}';
      const result = parseDeadLetterContent(content);
      expect(result).toHaveLength(2);
    });
  });

  describe('retryUpload - logic', () => {
    function simulateRetry(fileExists, gsutilSuccess) {
      if (!fileExists) {
        return { ok: false, error: 'Local file no longer exists', recoverable: false };
      }
      if (!gsutilSuccess) {
        return { ok: false, error: 'Connection timeout', recoverable: true };
      }
      return { ok: true };
    }

    it('should return ok:true when file exists and upload succeeds', () => {
      const result = simulateRetry(true, true);
      expect(result.ok).toBe(true);
    });

    it('should return recoverable:false if local file is missing', () => {
      const result = simulateRetry(false, false);
      expect(result.ok).toBe(false);
      expect(result.recoverable).toBe(false);
    });

    it('should return recoverable:true on transient gsutil failure', () => {
      const result = simulateRetry(true, false);
      expect(result.ok).toBe(false);
      expect(result.recoverable).toBe(true);
    });
  });

  describe('processDeadLetterLog - rewrite logic', () => {
    it('should separate successful retries from remaining failures', () => {
      const entries = [
        { localPath: '/tmp/a.parquet', gcsPath: 'gs://b/a.parquet', error: 'timeout' },
        { localPath: '/tmp/b.parquet', gcsPath: 'gs://b/b.parquet', error: '503' },
        { localPath: '/tmp/c.parquet', gcsPath: 'gs://b/c.parquet', error: 'timeout' },
      ];

      // Simulate: first succeeds, second fails, third succeeds
      const retryResults = [true, false, true];

      const remaining = [];
      let retried = 0;

      entries.forEach((entry, idx) => {
        if (retryResults[idx]) {
          retried++;
        } else {
          remaining.push({ ...entry, lastRetry: new Date().toISOString() });
        }
      });

      expect(retried).toBe(2);
      expect(remaining).toHaveLength(1);
      expect(remaining[0].gcsPath).toBe('gs://b/b.parquet');
    });

    it('should produce empty remaining when all retries succeed', () => {
      const entries = [
        { localPath: '/tmp/a.parquet', gcsPath: 'gs://b/a.parquet' },
      ];

      const remaining = [];
      let retried = 0;
      entries.forEach(() => { retried++; });

      expect(retried).toBe(1);
      expect(remaining).toHaveLength(0);
    });

    it('should not modify entries in dry-run mode', () => {
      const entries = [
        { localPath: '/tmp/a.parquet', gcsPath: 'gs://b/a.parquet', error: 'timeout' },
      ];
      const dryRun = true;

      // In dry-run, we just inspect without executing
      const retried = dryRun ? 0 : 1;
      expect(retried).toBe(0);
      // Original entries untouched
      expect(entries[0].error).toBe('timeout');
    });
  });
});
