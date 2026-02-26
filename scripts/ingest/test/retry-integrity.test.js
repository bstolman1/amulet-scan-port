/**
 * Tests for retry-failed-uploads.js
 * 
 * Since verifyUploadIntegrity was removed from gcs-upload-queue.js 
 * (replaced by SDK CRC32C), these tests now verify the retry module's
 * own upload logic and error handling.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';

const {
  mockExecSync, mockSpawn,
  mockExistsSync, mockReadFileSync, mockStatSync,
  mockUnlinkSync, mockWriteFileSync, mockAppendFileSync, mockMkdirSync,
} = vi.hoisted(() => ({
  mockExecSync: vi.fn(),
  mockSpawn: vi.fn(),
  mockExistsSync: vi.fn(),
  mockReadFileSync: vi.fn(),
  mockStatSync: vi.fn(),
  mockUnlinkSync: vi.fn(),
  mockWriteFileSync: vi.fn(),
  mockAppendFileSync: vi.fn(),
  mockMkdirSync: vi.fn(),
}));

vi.mock('child_process', () => ({
  execSync: mockExecSync,
  spawn: mockSpawn,
  default: { execSync: mockExecSync, spawn: mockSpawn },
}));

vi.mock('fs', async (importOriginal) => {
  const actual = await importOriginal();
  return {
    ...actual,
    default: {
      ...actual,
      existsSync: mockExistsSync,
      readFileSync: mockReadFileSync,
      statSync: mockStatSync,
      unlinkSync: mockUnlinkSync,
      writeFileSync: mockWriteFileSync,
      appendFileSync: mockAppendFileSync,
      mkdirSync: mockMkdirSync,
    },
    existsSync: mockExistsSync,
    readFileSync: mockReadFileSync,
    statSync: mockStatSync,
    unlinkSync: mockUnlinkSync,
    writeFileSync: mockWriteFileSync,
    appendFileSync: mockAppendFileSync,
    mkdirSync: mockMkdirSync,
  };
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
});
