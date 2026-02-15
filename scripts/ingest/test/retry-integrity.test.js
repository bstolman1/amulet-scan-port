/**
 * Tests for MD5 verification in retry-failed-uploads.js
 * 
 * Tests the ACTUAL retryUpload and verifyUploadIntegrity functions
 * from the real modules, using mocks only for external I/O (gsutil, fs).
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { createHash } from 'crypto';

// Mock both child_process and fs via vi.mock so ESM named imports are intercepted
const mockExecSync = vi.fn();
const mockSpawn = vi.fn();

vi.mock('child_process', () => ({
  execSync: mockExecSync,
  spawn: mockSpawn,
  default: { execSync: mockExecSync, spawn: mockSpawn },
}));

const mockExistsSync = vi.fn();
const mockReadFileSync = vi.fn();
const mockStatSync = vi.fn();
const mockUnlinkSync = vi.fn();
const mockWriteFileSync = vi.fn();
const mockAppendFileSync = vi.fn();
const mockMkdirSync = vi.fn();

vi.mock('fs', async (importOriginal) => {
  const actual = await importOriginal();
  return {
    ...actual,
    default: actual,
    existsSync: mockExistsSync,
    readFileSync: mockReadFileSync,
    statSync: mockStatSync,
    unlinkSync: mockUnlinkSync,
    writeFileSync: mockWriteFileSync,
    appendFileSync: mockAppendFileSync,
    mkdirSync: mockMkdirSync,
  };
});

describe('verifyUploadIntegrity (from gcs-upload-queue.js)', () => {
  let verifyUploadIntegrity;

  beforeEach(async () => {
    vi.clearAllMocks();
    const mod = await import('../gcs-upload-queue.js');
    verifyUploadIntegrity = mod.verifyUploadIntegrity;
  });

  it('returns ok: true when local and remote MD5 match', () => {
    mockExistsSync.mockReturnValue(true);
    mockReadFileSync.mockReturnValue(Buffer.from('test content'));
    const expectedMD5 = createHash('md5').update(Buffer.from('test content')).digest('base64');
    mockExecSync.mockReturnValue(`    Hash (md5):    ${expectedMD5}\n    Size: 12\n`);

    const result = verifyUploadIntegrity('/tmp/test.parquet', 'gs://bucket/test.parquet');

    expect(result.ok).toBe(true);
    expect(result.localMD5).toBe(expectedMD5);
    expect(result.remoteMD5).toBe(expectedMD5);
  });

  it('returns ok: false when MD5 hashes mismatch', () => {
    mockExistsSync.mockReturnValue(true);
    mockReadFileSync.mockReturnValue(Buffer.from('local content'));
    mockExecSync.mockReturnValue('    Hash (md5):    DIFFERENT_HASH\n');

    const result = verifyUploadIntegrity('/tmp/test.parquet', 'gs://bucket/test.parquet');

    expect(result.ok).toBe(false);
    expect(result.error).toContain('Hash mismatch');
  });

  it('returns ok: false when gsutil stat fails', () => {
    mockExistsSync.mockReturnValue(true);
    mockReadFileSync.mockReturnValue(Buffer.from('content'));
    mockExecSync.mockImplementation(() => { throw new Error('gsutil not found'); });

    const result = verifyUploadIntegrity('/tmp/test.parquet', 'gs://bucket/test.parquet');

    expect(result.ok).toBe(false);
    expect(result.error).toContain('Could not retrieve');
  });

  it('returns ok: false when local file does not exist', () => {
    mockExistsSync.mockReturnValue(false);

    const result = verifyUploadIntegrity('/tmp/gone.parquet', 'gs://bucket/gone.parquet');

    expect(result.ok).toBe(false);
    expect(result.error).toContain('Local file no longer exists');
  });
});

describe('retryUpload (from retry-failed-uploads.js)', () => {
  let retryUpload;

  beforeEach(async () => {
    vi.clearAllMocks();
    const mod = await import('../retry-failed-uploads.js');
    retryUpload = mod.retryUpload;
  });

  it('returns ok: false with recoverable: false when file does not exist', () => {
    mockExistsSync.mockReturnValue(false);

    const result = retryUpload('/tmp/missing.parquet', 'gs://bucket/missing.parquet');

    expect(result.ok).toBe(false);
    expect(result.recoverable).toBe(false);
    expect(result.error).toContain('Local file no longer exists');
  });

  it('returns ok: false with recoverable: true when gsutil cp fails', () => {
    mockExistsSync.mockReturnValue(true);
    mockExecSync.mockImplementation(() => { throw new Error('503 Service Unavailable'); });

    const result = retryUpload('/tmp/test.parquet', 'gs://bucket/test.parquet');

    expect(result.ok).toBe(false);
    expect(result.recoverable).toBe(true);
    expect(result.error).toContain('503');
  });

  it('calls verifyUploadIntegrity after successful gsutil cp', () => {
    mockExistsSync.mockReturnValue(true);
    mockReadFileSync.mockReturnValue(Buffer.from('data'));
    
    const md5 = createHash('md5').update(Buffer.from('data')).digest('base64');
    
    // First call: gsutil cp (succeeds), second call: gsutil stat (returns MD5)
    mockExecSync
      .mockReturnValueOnce('') // gsutil cp
      .mockReturnValueOnce(`    Hash (md5):    ${md5}\n`); // gsutil stat

    const result = retryUpload('/tmp/test.parquet', 'gs://bucket/test.parquet');

    expect(result.ok).toBe(true);
    expect(result.localMD5).toBe(md5);
    expect(mockExecSync).toHaveBeenCalledTimes(2);
  });

  it('returns ok: false when upload succeeds but integrity check fails', () => {
    mockExistsSync.mockReturnValue(true);
    mockReadFileSync.mockReturnValue(Buffer.from('data'));
    
    // gsutil cp succeeds, gsutil stat returns mismatched hash
    mockExecSync
      .mockReturnValueOnce('') // gsutil cp
      .mockReturnValueOnce('    Hash (md5):    WRONG_HASH\n'); // gsutil stat

    const result = retryUpload('/tmp/test.parquet', 'gs://bucket/test.parquet');

    expect(result.ok).toBe(false);
    expect(result.recoverable).toBe(true);
    expect(result.error).toContain('Integrity check failed');
  });
});
