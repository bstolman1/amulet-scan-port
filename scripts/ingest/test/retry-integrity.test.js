/**
 * Tests for MD5 verification in retry-failed-uploads.js
 * 
 * Tests the ACTUAL retryUpload and verifyUploadIntegrity functions
 * from the real modules, using mocks only for external I/O (gsutil, fs).
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import fs from 'fs';

// Mock child_process before importing the module under test
vi.mock('child_process', async (importOriginal) => {
  const actual = await importOriginal();
  return {
    ...actual,
    default: actual,
    spawn: vi.fn(),
    execSync: vi.fn(),
  };
});

// We need to mock fs.existsSync and fs.readFileSync selectively
// so verifyUploadIntegrity can be tested with controlled inputs
const originalExistsSync = fs.existsSync;
const originalReadFileSync = fs.readFileSync;

describe('verifyUploadIntegrity (from gcs-upload-queue.js)', () => {
  let verifyUploadIntegrity, computeLocalMD5, getGCSObjectMD5;
  let execSync;

  beforeEach(async () => {
    const mod = await import('../gcs-upload-queue.js');
    verifyUploadIntegrity = mod.verifyUploadIntegrity;
    computeLocalMD5 = mod.computeLocalMD5;
    getGCSObjectMD5 = mod.getGCSObjectMD5;
    
    const cp = await import('child_process');
    execSync = cp.execSync;
    vi.clearAllMocks();
  });

  it('returns ok: true when local and remote MD5 match', () => {
    // Mock file exists
    vi.spyOn(fs, 'existsSync').mockReturnValue(true);
    // Mock file content for MD5
    vi.spyOn(fs, 'readFileSync').mockReturnValue(Buffer.from('test content'));
    // Mock gsutil stat output with matching MD5
    const crypto = require('crypto');
    const expectedMD5 = crypto.createHash('md5').update(Buffer.from('test content')).digest('base64');
    execSync.mockReturnValue(`    Hash (md5):    ${expectedMD5}\n    Size: 12\n`);

    const result = verifyUploadIntegrity('/tmp/test.parquet', 'gs://bucket/test.parquet');

    expect(result.ok).toBe(true);
    expect(result.localMD5).toBe(expectedMD5);
    expect(result.remoteMD5).toBe(expectedMD5);
  });

  it('returns ok: false when MD5 hashes mismatch', () => {
    vi.spyOn(fs, 'existsSync').mockReturnValue(true);
    vi.spyOn(fs, 'readFileSync').mockReturnValue(Buffer.from('local content'));
    execSync.mockReturnValue('    Hash (md5):    DIFFERENT_HASH\n');

    const result = verifyUploadIntegrity('/tmp/test.parquet', 'gs://bucket/test.parquet');

    expect(result.ok).toBe(false);
    expect(result.error).toContain('Hash mismatch');
  });

  it('returns ok: false when gsutil stat fails', () => {
    vi.spyOn(fs, 'existsSync').mockReturnValue(true);
    vi.spyOn(fs, 'readFileSync').mockReturnValue(Buffer.from('content'));
    execSync.mockImplementation(() => { throw new Error('gsutil not found'); });

    const result = verifyUploadIntegrity('/tmp/test.parquet', 'gs://bucket/test.parquet');

    expect(result.ok).toBe(false);
    expect(result.error).toContain('Could not retrieve');
  });

  it('returns ok: false when local file does not exist', () => {
    vi.spyOn(fs, 'existsSync').mockReturnValue(false);

    const result = verifyUploadIntegrity('/tmp/gone.parquet', 'gs://bucket/gone.parquet');

    expect(result.ok).toBe(false);
    expect(result.error).toContain('Local file no longer exists');
  });
});

describe('retryUpload (from retry-failed-uploads.js)', () => {
  let retryUpload;
  let execSync;

  beforeEach(async () => {
    const cp = await import('child_process');
    execSync = cp.execSync;
    vi.clearAllMocks();
    
    // Re-import to get fresh module
    const mod = await import('../retry-failed-uploads.js');
    retryUpload = mod.retryUpload;
  });

  it('returns ok: false with recoverable: false when file does not exist', () => {
    vi.spyOn(fs, 'existsSync').mockReturnValue(false);

    const result = retryUpload('/tmp/missing.parquet', 'gs://bucket/missing.parquet');

    expect(result.ok).toBe(false);
    expect(result.recoverable).toBe(false);
    expect(result.error).toContain('Local file no longer exists');
  });

  it('returns ok: false with recoverable: true when gsutil cp fails', () => {
    vi.spyOn(fs, 'existsSync').mockReturnValue(true);
    execSync.mockImplementation(() => { throw new Error('503 Service Unavailable'); });

    const result = retryUpload('/tmp/test.parquet', 'gs://bucket/test.parquet');

    expect(result.ok).toBe(false);
    expect(result.recoverable).toBe(true);
    expect(result.error).toContain('503');
  });

  it('calls verifyUploadIntegrity after successful gsutil cp', () => {
    vi.spyOn(fs, 'existsSync').mockReturnValue(true);
    vi.spyOn(fs, 'readFileSync').mockReturnValue(Buffer.from('data'));
    
    const crypto = require('crypto');
    const md5 = crypto.createHash('md5').update(Buffer.from('data')).digest('base64');
    
    // First call: gsutil cp (succeeds), second call: gsutil stat (returns MD5)
    execSync
      .mockReturnValueOnce('') // gsutil cp
      .mockReturnValueOnce(`    Hash (md5):    ${md5}\n`); // gsutil stat

    const result = retryUpload('/tmp/test.parquet', 'gs://bucket/test.parquet');

    expect(result.ok).toBe(true);
    expect(result.localMD5).toBe(md5);
    // Verify gsutil was called twice (cp + stat)
    expect(execSync).toHaveBeenCalledTimes(2);
  });

  it('returns ok: false when upload succeeds but integrity check fails', () => {
    vi.spyOn(fs, 'existsSync').mockReturnValue(true);
    vi.spyOn(fs, 'readFileSync').mockReturnValue(Buffer.from('data'));
    
    // gsutil cp succeeds, gsutil stat returns mismatched hash
    execSync
      .mockReturnValueOnce('') // gsutil cp
      .mockReturnValueOnce('    Hash (md5):    WRONG_HASH\n'); // gsutil stat

    const result = retryUpload('/tmp/test.parquet', 'gs://bucket/test.parquet');

    expect(result.ok).toBe(false);
    expect(result.recoverable).toBe(true);
    expect(result.error).toContain('Integrity check failed');
  });
});
