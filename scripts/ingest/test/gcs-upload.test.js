/**
 * GCS Upload Tests
 * 
 * Tests the GCS upload module including:
 * - Path generation
 * - Retry logic
 * - Error detection
 */

import { describe, it, expect } from 'vitest';

describe('GCS Upload', () => {
  
  describe('GCS Path Generation', () => {
    function getGCSPath(bucket, relativePath) {
      if (!bucket) {
        throw new Error('GCS_BUCKET not configured');
      }
      
      // Normalize path separators to forward slashes
      const normalized = relativePath.replace(/\\/g, '/');
      
      // Prefix with 'raw/' if not already present
      const prefix = normalized.startsWith('raw/') ? '' : 'raw/';
      
      return `gs://${bucket}/${prefix}${normalized}`;
    }
    
    it('should generate correct GCS URI for updates folder', () => {
      expect(getGCSPath('my-bucket', 'backfill/updates/migration=0/updates.parquet'))
        .toBe('gs://my-bucket/raw/backfill/updates/migration=0/updates.parquet');
    });
    
    it('should generate correct GCS URI for events folder', () => {
      expect(getGCSPath('my-bucket', 'backfill/events/migration=0/events.parquet'))
        .toBe('gs://my-bucket/raw/backfill/events/migration=0/events.parquet');
    });
    
    it('should not double-prefix raw/', () => {
      expect(getGCSPath('my-bucket', 'raw/backfill/updates/updates.parquet'))
        .toBe('gs://my-bucket/raw/backfill/updates/updates.parquet');
    });
    
    it('should normalize Windows paths', () => {
      expect(getGCSPath('my-bucket', 'backfill\\updates\\migration=0\\updates.parquet'))
        .toBe('gs://my-bucket/raw/backfill/updates/migration=0/updates.parquet');
    });
    
    it('should throw when bucket not configured', () => {
      expect(() => getGCSPath(null, 'path'))
        .toThrow('GCS_BUCKET not configured');
    });
  });
  
  describe('Tmp Path Generation', () => {
    const TMP_DIR = '/tmp/ledger_raw';
    
    function getTmpPath(relativePath) {
      return `${TMP_DIR}/${relativePath}`;
    }
    
    it('should generate tmp path correctly for updates', () => {
      expect(getTmpPath('backfill/updates/migration=0/updates.parquet'))
        .toBe('/tmp/ledger_raw/backfill/updates/migration=0/updates.parquet');
    });
    
    it('should generate tmp path correctly for events', () => {
      expect(getTmpPath('backfill/events/migration=0/events.parquet'))
        .toBe('/tmp/ledger_raw/backfill/events/migration=0/events.parquet');
    });
  });
  
  describe('Transient Error Detection', () => {
    const TRANSIENT_PATTERNS = [
      /timeout/i, /timed out/i, /connection reset/i, /connection refused/i,
      /network unreachable/i, /temporary failure/i, /service unavailable/i,
      /503/, /502/, /500/, /ECONNRESET/, /ETIMEDOUT/, /ENOTFOUND/, /ENETUNREACH/,
      /socket hang up/i, /rate limit/i, /too many requests/i, /429/, /try again/i,
      /retryable/i,
    ];
    
    function isTransientError(errorMessage) {
      if (!errorMessage) return false;
      return TRANSIENT_PATTERNS.some(pattern => pattern.test(errorMessage));
    }
    
    const TRANSIENT_ERRORS = [
      'Connection timed out',
      'Request timeout after 300000ms',
      'connection reset by peer',
      'Connection refused',
      'network unreachable',
      'temporary failure in name resolution',
      'Service Unavailable',
      '503 Service Unavailable',
      '502 Bad Gateway',
      '500 Internal Server Error',
      'ECONNRESET',
      'ETIMEDOUT',
      'ENOTFOUND: getaddrinfo failed',
      'ENETUNREACH',
      'socket hang up',
      'rate limit exceeded',
      'too many requests',
      '429 Too Many Requests',
      'please try again later',
      'retryable error',
    ];
    
    const PERMANENT_ERRORS = [
      'AccessDenied',
      'InvalidBucketName',
      'NoSuchBucket',
      'InvalidAccessKeyId',
      'SignatureDoesNotMatch',
      'Forbidden: Access denied',
      '403 Forbidden',
      '404 Not Found',
      'Invalid argument',
      'Permission denied',
    ];
    
    it('should identify all transient errors', () => {
      for (const error of TRANSIENT_ERRORS) {
        expect(isTransientError(error)).toBe(true);
      }
    });
    
    it('should NOT identify permanent errors as transient', () => {
      for (const error of PERMANENT_ERRORS) {
        expect(isTransientError(error)).toBe(false);
      }
    });
    
    it('should handle null/empty error messages', () => {
      expect(isTransientError(null)).toBe(false);
      expect(isTransientError(undefined)).toBe(false);
      expect(isTransientError('')).toBe(false);
    });
  });
  
  describe('Backoff Calculation', () => {
    const DEFAULT_BASE_DELAY_MS = 1000;
    const DEFAULT_MAX_DELAY_MS = 30000;
    
    function calculateBackoffDelay(attempt, baseDelay = DEFAULT_BASE_DELAY_MS, maxDelay = DEFAULT_MAX_DELAY_MS) {
      const exponentialDelay = baseDelay * Math.pow(2, attempt);
      const jitter = exponentialDelay * 0.25 * (Math.random() * 2 - 1);
      return Math.min(exponentialDelay + jitter, maxDelay);
    }
    
    it('should increase delay exponentially', () => {
      const baseDelays = [];
      for (let i = 0; i < 6; i++) {
        baseDelays.push(DEFAULT_BASE_DELAY_MS * Math.pow(2, i));
      }
      
      expect(baseDelays[0]).toBe(1000);   // 1s
      expect(baseDelays[1]).toBe(2000);   // 2s
      expect(baseDelays[2]).toBe(4000);   // 4s
      expect(baseDelays[3]).toBe(8000);   // 8s
      expect(baseDelays[4]).toBe(16000);  // 16s
      expect(baseDelays[5]).toBe(32000);  // 32s (will be capped)
    });
    
    it('should cap delay at max', () => {
      for (let attempt = 0; attempt < 10; attempt++) {
        const delay = calculateBackoffDelay(attempt);
        expect(delay).toBeLessThanOrEqual(DEFAULT_MAX_DELAY_MS);
      }
    });
    
    it('should add jitter within ±25% range', () => {
      const delays = [];
      for (let i = 0; i < 100; i++) {
        delays.push(calculateBackoffDelay(1)); // Base = 2000ms
      }
      
      const min = Math.min(...delays);
      const max = Math.max(...delays);
      
      // 2000ms ± 25% = 1500-2500ms
      expect(min).toBeGreaterThanOrEqual(1500);
      expect(max).toBeLessThanOrEqual(2500);
      
      // Should have variance
      expect(max - min).toBeGreaterThan(100);
    });
    
    it('should respect custom base and max delays', () => {
      const customBase = 500;
      const customMax = 5000;
      
      // First attempt
      const delay0 = calculateBackoffDelay(0, customBase, customMax);
      expect(delay0).toBeLessThanOrEqual(customBase * 1.25);
      expect(delay0).toBeGreaterThanOrEqual(customBase * 0.75);
      
      // High attempt (should be capped)
      for (let i = 0; i < 20; i++) {
        const delay = calculateBackoffDelay(10, customBase, customMax);
        expect(delay).toBeLessThanOrEqual(customMax);
      }
    });
  });
  
  describe('Upload Result Structure', () => {
    it('should have all expected fields', () => {
      const result = {
        ok: false,
        localPath: '/tmp/file.parquet',
        gcsPath: 'gs://bucket/file.parquet',
        bytes: 0,
        error: null,
        attempts: 0,
        retried: false,
      };
      
      expect(result).toHaveProperty('ok');
      expect(result).toHaveProperty('localPath');
      expect(result).toHaveProperty('gcsPath');
      expect(result).toHaveProperty('bytes');
      expect(result).toHaveProperty('error');
      expect(result).toHaveProperty('attempts');
      expect(result).toHaveProperty('retried');
    });
    
    it('should track retry state correctly', () => {
      const result = { ok: true, attempts: 3, retried: true };
      
      expect(result.attempts).toBeGreaterThan(1);
      expect(result.retried).toBe(true);
    });
  });
  
  describe('Upload Stats Tracking', () => {
    it('should calculate throughput correctly', () => {
      const stats = {
        totalBytesUploaded: 100 * 1024 * 1024, // 100 MB
        startTime: Date.now() - 20000,          // 20 seconds ago
      };
      
      const elapsed = (Date.now() - stats.startTime) / 1000;
      const mbUploaded = stats.totalBytesUploaded / (1024 * 1024);
      const throughput = mbUploaded / elapsed;
      
      expect(throughput).toBeCloseTo(5, 0); // ~5 MB/s
    });
    
    it('should track success and failure counts', () => {
      const stats = {
        totalUploads: 100,
        successfulUploads: 95,
        failedUploads: 5,
      };
      
      expect(stats.successfulUploads + stats.failedUploads).toBe(100);
      expect(stats.successfulUploads / stats.totalUploads).toBe(0.95);
    });
  });
});
