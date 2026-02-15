import { describe, it, expect, vi, beforeEach } from 'vitest';

// Mock dependencies before imports
vi.mock('fs', async () => {
  const actual = await vi.importActual('fs');
  return {
    ...actual,
    existsSync: vi.fn(() => true),
    readFileSync: vi.fn(() => Buffer.from('test-parquet-data')),
    appendFileSync: vi.fn(),
    mkdirSync: vi.fn(),
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

import { existsSync, readFileSync } from 'fs';
import { execSync } from 'child_process';
import { createHash } from 'crypto';

describe('Upload Integrity Verification', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    existsSync.mockReturnValue(true);
    readFileSync.mockReturnValue(Buffer.from('test-parquet-data'));
    delete process.env.GCS_SKIP_VERIFY;
  });

  describe('verifyUploadIntegrity', () => {
    it('should return ok:true when hashes match', async () => {
      const expectedMD5 = createHash('md5').update(Buffer.from('test-parquet-data')).digest('base64');
      execSync.mockReturnValue(`    Hash (md5):    ${expectedMD5}\n    Hash (crc32c):  abc123==\n`);
      
      const { verifyUploadIntegrity } = await import('../gcs-upload-queue.js');
      const result = verifyUploadIntegrity('/tmp/test.parquet', 'gs://bucket/test.parquet');
      
      expect(result.ok).toBe(true);
      expect(result.localMD5).toBe(expectedMD5);
      expect(result.remoteMD5).toBe(expectedMD5);
    });

    it('should return ok:false when hashes mismatch', async () => {
      execSync.mockReturnValue('    Hash (md5):    AAAAAAAAAAAAAAAAAAAAAA==\n');
      
      const { verifyUploadIntegrity } = await import('../gcs-upload-queue.js');
      const result = verifyUploadIntegrity('/tmp/test.parquet', 'gs://bucket/test.parquet');
      
      expect(result.ok).toBe(false);
      expect(result.error).toContain('Hash mismatch');
      expect(result.localMD5).toBeDefined();
      expect(result.remoteMD5).toBe('AAAAAAAAAAAAAAAAAAAAAA==');
    });

    it('should return ok:false when local file is missing', async () => {
      existsSync.mockReturnValue(false);
      
      const { verifyUploadIntegrity } = await import('../gcs-upload-queue.js');
      const result = verifyUploadIntegrity('/tmp/missing.parquet', 'gs://bucket/test.parquet');
      
      expect(result.ok).toBe(false);
      expect(result.error).toContain('Local file no longer exists');
    });

    it('should return ok:false when gsutil stat fails', async () => {
      execSync.mockImplementation(() => { throw new Error('Not found'); });
      
      const { verifyUploadIntegrity } = await import('../gcs-upload-queue.js');
      const result = verifyUploadIntegrity('/tmp/test.parquet', 'gs://bucket/test.parquet');
      
      expect(result.ok).toBe(false);
      expect(result.error).toContain('Could not retrieve GCS object hash');
    });
  });

  describe('computeLocalMD5', () => {
    it('should compute base64-encoded MD5 hash of file contents', async () => {
      const data = Buffer.from('hello world');
      readFileSync.mockReturnValue(data);
      const expected = createHash('md5').update(data).digest('base64');
      
      const { computeLocalMD5 } = await import('../gcs-upload-queue.js');
      const result = computeLocalMD5('/tmp/test.parquet');
      
      expect(result).toBe(expected);
    });
  });

  describe('getGCSObjectMD5', () => {
    it('should parse MD5 from gsutil stat output', async () => {
      execSync.mockReturnValue(
        'gs://bucket/test.parquet:\n' +
        '    Creation time:    Mon, 01 Jan 2026 00:00:00 GMT\n' +
        '    Content-Length:   1024\n' +
        '    Hash (crc32c):    abc123==\n' +
        '    Hash (md5):       XrY7u+Ae7tCTyyK7j1rNww==\n'
      );
      
      const { getGCSObjectMD5 } = await import('../gcs-upload-queue.js');
      const result = getGCSObjectMD5('gs://bucket/test.parquet');
      
      expect(result).toBe('XrY7u+Ae7tCTyyK7j1rNww==');
    });

    it('should return null when gsutil stat fails', async () => {
      execSync.mockImplementation(() => { throw new Error('BucketNotFoundException'); });
      
      const { getGCSObjectMD5 } = await import('../gcs-upload-queue.js');
      const result = getGCSObjectMD5('gs://bucket/missing.parquet');
      
      expect(result).toBeNull();
    });

    it('should return null when output has no md5 line', async () => {
      execSync.mockReturnValue('gs://bucket/test.parquet:\n    Content-Length: 1024\n');
      
      const { getGCSObjectMD5 } = await import('../gcs-upload-queue.js');
      const result = getGCSObjectMD5('gs://bucket/test.parquet');
      
      expect(result).toBeNull();
    });
  });
});
