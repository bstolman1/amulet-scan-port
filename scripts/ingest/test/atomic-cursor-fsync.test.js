/**
 * Tests for atomic cursor fsync durability in atomic-cursor.js
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

const {
  mockExistsSync, mockReadFileSync, mockWriteFileSync,
  mockRenameSync, mockMkdirSync, mockUnlinkSync, mockStatSync,
  mockOpenSync, mockFsyncSync, mockCloseSync,
} = vi.hoisted(() => ({
  mockExistsSync: vi.fn(),
  mockReadFileSync: vi.fn(),
  mockWriteFileSync: vi.fn(),
  mockRenameSync: vi.fn(),
  mockMkdirSync: vi.fn(),
  mockUnlinkSync: vi.fn(),
  mockStatSync: vi.fn(),
  mockOpenSync: vi.fn(),
  mockFsyncSync: vi.fn(),
  mockCloseSync: vi.fn(),
}));

vi.mock('fs', async (importOriginal) => {
  const actual = await importOriginal();
  return {
    ...actual,
    default: {
      ...actual,
      existsSync: mockExistsSync, readFileSync: mockReadFileSync,
      writeFileSync: mockWriteFileSync, renameSync: mockRenameSync,
      mkdirSync: mockMkdirSync, unlinkSync: mockUnlinkSync, statSync: mockStatSync,
      openSync: mockOpenSync, fsyncSync: mockFsyncSync, closeSync: mockCloseSync,
    },
    existsSync: mockExistsSync,
    readFileSync: mockReadFileSync,
    writeFileSync: mockWriteFileSync,
    renameSync: mockRenameSync,
    mkdirSync: mockMkdirSync,
    unlinkSync: mockUnlinkSync,
    statSync: mockStatSync,
    openSync: mockOpenSync,
    fsyncSync: mockFsyncSync,
    closeSync: mockCloseSync,
  };
});

describe('atomicWriteFile with fsync', () => {
  let atomicWriteFile;

  beforeEach(async () => {
    vi.clearAllMocks();
    mockExistsSync.mockReturnValue(false);
    mockOpenSync.mockReturnValue(42); // fake fd
    const mod = await import('../atomic-cursor.js');
    atomicWriteFile = mod.atomicWriteFile;
  });

  it('calls fsync before rename to ensure durability (with parent dir fsync)', () => {
    const callOrder = [];
    mockWriteFileSync.mockImplementation(() => callOrder.push('write'));
    mockOpenSync.mockImplementation((path) => { callOrder.push(path.endsWith('.tmp') ? 'open' : 'open-dir'); return 42; });
    mockFsyncSync.mockImplementation(() => callOrder.push('fsync'));
    mockCloseSync.mockImplementation(() => callOrder.push('close'));
    mockRenameSync.mockImplementation(() => callOrder.push('rename'));

    atomicWriteFile('/tmp/cursor.json', { test: true });

    // New order: write tmp, fsync tmp, rename, fsync parent dir
    expect(callOrder).toEqual(['write', 'open', 'fsync', 'close', 'rename', 'open-dir', 'fsync', 'close']);
  });

  it('writes to .tmp path, then renames to final path', () => {
    atomicWriteFile('/tmp/cursor.json', { data: 'value' });

    expect(mockWriteFileSync).toHaveBeenCalledWith(
      '/tmp/cursor.json.tmp',
      expect.any(String),
      expect.objectContaining({ encoding: 'utf8' })
    );
    expect(mockRenameSync).toHaveBeenCalledWith('/tmp/cursor.json.tmp', '/tmp/cursor.json');
  });

  it('cleans up .tmp file on failure', () => {
    mockOpenSync.mockImplementation(() => { throw new Error('disk full'); });
    mockExistsSync.mockImplementation((p) => typeof p === 'string' && p.endsWith('.tmp'));

    expect(() => atomicWriteFile('/tmp/cursor.json', { data: 'value' })).toThrow('Atomic write failed');
    expect(mockUnlinkSync).toHaveBeenCalledWith('/tmp/cursor.json.tmp');
  });

  it('backs up existing valid cursor before overwriting via atomic temp+rename', () => {
    mockExistsSync.mockImplementation((p) => {
      if (typeof p === 'string' && p.endsWith('.tmp')) return false;
      if (typeof p === 'string' && p.endsWith('.json')) return true;
      return false;
    });
    mockReadFileSync.mockReturnValue('{"valid": true}');
    mockOpenSync.mockReturnValue(42);

    atomicWriteFile('/tmp/cursor.json', { new: 'data' });

    // Backup is now written atomically via temp+rename (.bak.tmp → .bak)
    expect(mockWriteFileSync).toHaveBeenCalledWith(
      '/tmp/cursor.json.bak.tmp',
      '{"valid": true}',
      expect.objectContaining({ encoding: 'utf8' })
    );
    expect(mockRenameSync).toHaveBeenCalledWith('/tmp/cursor.json.bak.tmp', '/tmp/cursor.json.bak');
  });
});
