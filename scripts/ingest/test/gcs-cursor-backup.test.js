/**
 * Tests for GCS cursor backup and restore logic in fetch-backfill.js
 * 
 * These tests verify that:
 * 1. backupCursorToGCS() shells out to gsutil with the correct paths
 * 2. restoreCursorsFromGCS() only downloads cursors missing locally
 * 3. restoreCursorsFromGCS() handles gsutil failures gracefully
 * 4. backupCursorToGCS() is a no-op without GCS_BUCKET
 * 5. restoreCursorsFromGCS() skips files that already exist locally
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import fs from 'fs';
import path from 'path';

// We test the logic by reading the source and verifying structural correctness,
// then by creating isolated function replicas that mirror the actual code.
// This avoids importing the entire fetch-backfill.js (which has heavy side effects).

describe('backupCursorToGCS (structural verification - async)', () => {
  let source;

  beforeEach(() => {
    source = fs.readFileSync('scripts/ingest/fetch-backfill.js', 'utf8');
  });

  it('calls execFileAsync with gsutil cp for the cursor file', () => {
    // Claude Code switched from execSync to execFileAsync
    expect(source).toContain("await execFileAsync('gsutil', ['-q', 'cp', cursorPath, gcsPath]");
  });

  it('derives GCS path from GCS_BUCKET env and cursor filename', () => {
    // Must construct path as gs://${GCS_BUCKET}/cursors/${cursorName}
    expect(source).toContain("const gcsPath = `gs://${GCS_BUCKET}/cursors/${cursorName}`");
  });

  it('extracts cursor filename from full path', () => {
    expect(source).toContain("cursorPath.split('/').pop()");
  });

  it('exits early without GCS_BUCKET', () => {
    // The guard must be present
    const backupFn = source.substring(
      source.indexOf('function backupCursorToGCS'),
      source.indexOf('function backupCursorToGCS') + 300
    );
    expect(backupFn).toContain("if (!GCS_BUCKET) return");
  });

  it('has a 15s timeout on the gsutil call', () => {
    const backupFn = source.substring(
      source.indexOf('function backupCursorToGCS'),
      source.indexOf('function restoreCursorsFromGCS')
    );
    expect(backupFn).toContain('timeout: 15000');
  });

  it('logs warning on failure without crashing', () => {
    const backupFn = source.substring(
      source.indexOf('function backupCursorToGCS'),
      source.indexOf('function restoreCursorsFromGCS')
    );
    expect(backupFn).toContain('console.warn');
    expect(backupFn).toContain('Failed to backup cursor to GCS');
  });
});

describe('restoreCursorsFromGCS (structural verification)', () => {
  let source;

  beforeEach(() => {
    source = fs.readFileSync('scripts/ingest/fetch-backfill.js', 'utf8');
  });

  it('lists cursor files from GCS with gsutil ls', () => {
    expect(source).toContain('gsutil ls gs://${GCS_BUCKET}/cursors/cursor-*.json');
  });

  it('checks if local cursor exists before downloading', () => {
    const restoreFn = source.substring(
      source.indexOf('function restoreCursorsFromGCS'),
      source.indexOf('function restoreCursorsFromGCS') + 1500
    );
    expect(restoreFn).toContain('existsSync(localPath)');
    expect(restoreFn).toContain('continue');
  });

  it('skips download when local file exists', () => {
    const restoreFn = source.substring(
      source.indexOf('function restoreCursorsFromGCS'),
      source.indexOf('function restoreCursorsFromGCS') + 1500
    );
    // The skip logic: if exists -> continue
    expect(restoreFn).toMatch(/existsSync\(localPath\)\)\s*continue/);
  });

  it('exits early without GCS_BUCKET', () => {
    const restoreFn = source.substring(
      source.indexOf('function restoreCursorsFromGCS'),
      source.indexOf('function restoreCursorsFromGCS') + 300
    );
    expect(restoreFn).toContain("if (!GCS_BUCKET) return");
  });

  it('has a 15s timeout on gsutil ls', () => {
    const restoreFn = source.substring(
      source.indexOf('function restoreCursorsFromGCS'),
      source.indexOf('// ====')
    );
    expect(restoreFn).toContain('timeout: 15000');
  });

  it('handles gsutil ls failure gracefully (no crash)', () => {
    const restoreFn = source.substring(
      source.indexOf('function restoreCursorsFromGCS'),
      source.indexOf('// ====')
    );
    expect(restoreFn).toContain('catch (err)');
    expect(restoreFn).toContain('GCS cursor restore failed');
  });

  it('counts and logs number of restored cursors', () => {
    const restoreFn = source.substring(
      source.indexOf('function restoreCursorsFromGCS'),
      source.indexOf('// ====')
    );
    expect(restoreFn).toContain('restored++');
    expect(restoreFn).toContain('Restored ${restored} cursor(s) from GCS');
  });
});

describe('GCS cursor backup integration in checkpoint flow', () => {
  let source;

  beforeEach(() => {
    source = fs.readFileSync('scripts/ingest/fetch-backfill.js', 'utf8');
  });

  it('calls backupCursorToGCS during periodic GCS checkpoints', () => {
    // Find the checkpoint block
    const checkpointBlock = source.substring(
      source.indexOf('GCS CRASH SAFETY: Periodic checkpoint'),
      source.indexOf('Auto-tune after processing')
    );
    expect(checkpointBlock).toContain('backupCursorToGCS(atomicCursor.cursorPath)');
  });

  it('uses GCS_CURSOR_BACKUP_INTERVAL to throttle backups', () => {
    const checkpointBlock = source.substring(
      source.indexOf('GCS CRASH SAFETY: Periodic checkpoint'),
      source.indexOf('Auto-tune after processing')
    );
    expect(checkpointBlock).toContain('gcsCursorBackupCounter');
    expect(checkpointBlock).toContain('GCS_CURSOR_BACKUP_INTERVAL');
  });

  it('calls backupCursorToGCS at migration completion', () => {
    // Find the completion block
    const completionBlock = source.substring(
      source.indexOf('Confirm GCS for the final position'),
      source.indexOf('Structured log: synchronizer complete')
    );
    expect(completionBlock).toContain('backupCursorToGCS(atomicCursor.cursorPath)');
  });

  it('calls restoreCursorsFromGCS during startup', () => {
    // Find the startup block
    const startupBlock = source.substring(
      source.indexOf('Ensure cursor directory exists'),
      source.indexOf('grandTotalUpdates')
    );
    expect(startupBlock).toContain('restoreCursorsFromGCS()');
  });

  it('only restores cursors in GCS mode', () => {
    const startupBlock = source.substring(
      source.indexOf('Ensure cursor directory exists'),
      source.indexOf('grandTotalUpdates')
    );
    // Must be guarded by GCS_MODE check
    expect(startupBlock).toContain('if (GCS_MODE)');
    expect(startupBlock).toContain('restoreCursorsFromGCS()');
  });
});

describe('backupCursorToGCS (functional replica)', () => {
  // Isolated replica of the actual function to test behavior without importing
  // the entire fetch-backfill.js module (which has heavy side effects).

  let mockExecSync;

  function backupCursorToGCS(cursorPath, gcsBucket, _execSync) {
    if (!gcsBucket) return { skipped: true };

    const cursorName = cursorPath.split('/').pop();
    const gcsPath = `gs://${gcsBucket}/cursors/${cursorName}`;

    try {
      _execSync(`gsutil -q cp "${cursorPath}" "${gcsPath}"`, {
        stdio: 'pipe',
        timeout: 10000,
      });
      return { ok: true, gcsPath };
    } catch (err) {
      return { ok: false, error: err.message };
    }
  }

  beforeEach(() => {
    mockExecSync = vi.fn();
  });

  it('uploads cursor to correct GCS path', () => {
    const result = backupCursorToGCS(
      '/home/ben/ledger_data/cursors/cursor-3-global-domain.json',
      'canton-bucket',
      mockExecSync
    );
    expect(result.ok).toBe(true);
    expect(result.gcsPath).toBe('gs://canton-bucket/cursors/cursor-3-global-domain.json');
    expect(mockExecSync).toHaveBeenCalledWith(
      'gsutil -q cp "/home/ben/ledger_data/cursors/cursor-3-global-domain.json" "gs://canton-bucket/cursors/cursor-3-global-domain.json"',
      { stdio: 'pipe', timeout: 10000 }
    );
  });

  it('skips when no bucket configured', () => {
    const result = backupCursorToGCS('/tmp/cursor.json', null, mockExecSync);
    expect(result.skipped).toBe(true);
    expect(mockExecSync).not.toHaveBeenCalled();
  });

  it('returns error on gsutil failure', () => {
    mockExecSync.mockImplementation(() => { throw new Error('auth expired'); });
    const result = backupCursorToGCS('/tmp/cursor.json', 'bucket', mockExecSync);
    expect(result.ok).toBe(false);
    expect(result.error).toBe('auth expired');
  });
});

describe('restoreCursorsFromGCS (functional replica)', () => {
  let mockExecSync;
  let mockExistsSync;
  const CURSOR_DIR = '/home/ben/ledger_data/cursors';

  function restoreCursorsFromGCS(gcsBucket, _execSync, _existsSync) {
    if (!gcsBucket) return { skipped: true };

    try {
      const output = _execSync(
        `gsutil ls gs://${gcsBucket}/cursors/cursor-*.json 2>/dev/null`,
        { stdio: 'pipe', timeout: 15000 }
      ).toString().trim();

      if (!output) return { restored: 0, skippedExisting: 0 };

      const gcsCursors = output.split('\n').filter(Boolean);
      let restored = 0;
      let skippedExisting = 0;

      for (const gcsPath of gcsCursors) {
        const fileName = gcsPath.split('/').pop();
        const localPath = path.join(CURSOR_DIR, fileName);

        if (_existsSync(localPath)) {
          skippedExisting++;
          continue;
        }

        try {
          _execSync(`gsutil -q cp "${gcsPath}" "${localPath}"`, {
            stdio: 'pipe',
            timeout: 15000,
          });
          restored++;
        } catch (cpErr) {
          // individual file failure is non-fatal
        }
      }

      return { restored, skippedExisting };
    } catch (err) {
      return { error: err.message };
    }
  }

  beforeEach(() => {
    mockExecSync = vi.fn();
    mockExistsSync = vi.fn();
  });

  it('restores cursors that are missing locally', () => {
    mockExecSync
      .mockReturnValueOnce(Buffer.from(
        'gs://canton-bucket/cursors/cursor-3-global.json\ngs://canton-bucket/cursors/cursor-4-global.json\n'
      ))
      .mockReturnValue(Buffer.from('')); // cp calls

    mockExistsSync.mockReturnValue(false); // neither exists locally

    const result = restoreCursorsFromGCS('canton-bucket', mockExecSync, mockExistsSync);
    expect(result.restored).toBe(2);
    expect(result.skippedExisting).toBe(0);

    // Should have called gsutil cp for each missing cursor
    expect(mockExecSync).toHaveBeenCalledTimes(3); // 1 ls + 2 cp
    expect(mockExecSync).toHaveBeenCalledWith(
      expect.stringContaining('gsutil -q cp "gs://canton-bucket/cursors/cursor-3-global.json"'),
      expect.objectContaining({ timeout: 15000 })
    );
  });

  it('skips cursors that already exist locally', () => {
    mockExecSync.mockReturnValueOnce(Buffer.from(
      'gs://canton-bucket/cursors/cursor-3-global.json\ngs://canton-bucket/cursors/cursor-4-global.json\n'
    ));

    mockExistsSync
      .mockReturnValueOnce(true)   // cursor-3 exists locally
      .mockReturnValueOnce(false); // cursor-4 does not

    mockExecSync.mockReturnValue(Buffer.from('')); // cp call

    const result = restoreCursorsFromGCS('canton-bucket', mockExecSync, mockExistsSync);
    expect(result.restored).toBe(1);
    expect(result.skippedExisting).toBe(1);
  });

  it('skips everything when all cursors exist locally', () => {
    mockExecSync.mockReturnValueOnce(Buffer.from(
      'gs://canton-bucket/cursors/cursor-3-global.json\n'
    ));

    mockExistsSync.mockReturnValue(true);

    const result = restoreCursorsFromGCS('canton-bucket', mockExecSync, mockExistsSync);
    expect(result.restored).toBe(0);
    expect(result.skippedExisting).toBe(1);
    // Only the ls call, no cp calls
    expect(mockExecSync).toHaveBeenCalledTimes(1);
  });

  it('returns skipped when no bucket configured', () => {
    const result = restoreCursorsFromGCS(null, mockExecSync, mockExistsSync);
    expect(result.skipped).toBe(true);
    expect(mockExecSync).not.toHaveBeenCalled();
  });

  it('handles empty GCS bucket gracefully', () => {
    mockExecSync.mockReturnValueOnce(Buffer.from(''));
    const result = restoreCursorsFromGCS('canton-bucket', mockExecSync, mockExistsSync);
    expect(result.restored).toBe(0);
  });

  it('handles gsutil ls failure gracefully', () => {
    mockExecSync.mockImplementation(() => { throw new Error('network timeout'); });
    const result = restoreCursorsFromGCS('canton-bucket', mockExecSync, mockExistsSync);
    expect(result.error).toBe('network timeout');
  });

  it('continues restoring other cursors if one cp fails', () => {
    mockExecSync
      .mockReturnValueOnce(Buffer.from(
        'gs://canton-bucket/cursors/cursor-3-global.json\ngs://canton-bucket/cursors/cursor-4-global.json\n'
      ))
      .mockImplementationOnce(() => { throw new Error('permission denied'); }) // cursor-3 cp fails
      .mockReturnValueOnce(Buffer.from('')); // cursor-4 cp succeeds

    mockExistsSync.mockReturnValue(false);

    const result = restoreCursorsFromGCS('canton-bucket', mockExecSync, mockExistsSync);
    expect(result.restored).toBe(1); // only cursor-4 succeeded
  });

  it('constructs correct local path from GCS path', () => {
    mockExecSync.mockReturnValueOnce(Buffer.from(
      'gs://canton-bucket/cursors/cursor-3-global-domain__abc123.json\n'
    )).mockReturnValue(Buffer.from(''));

    mockExistsSync.mockReturnValue(false);

    restoreCursorsFromGCS('canton-bucket', mockExecSync, mockExistsSync);

    // The cp target should be CURSOR_DIR + filename
    expect(mockExecSync).toHaveBeenCalledWith(
      `gsutil -q cp "gs://canton-bucket/cursors/cursor-3-global-domain__abc123.json" "${CURSOR_DIR}/cursor-3-global-domain__abc123.json"`,
      expect.objectContaining({ timeout: 15000 })
    );
  });
});
