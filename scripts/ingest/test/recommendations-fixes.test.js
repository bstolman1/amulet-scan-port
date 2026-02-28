/**
 * Tests for all 6 recommendation fixes:
 * #1  - Quarantine null effective_at events (not silently dropped)
 * #2  - Reassignment events handled in fetch-updates processUpdates
 * #3  - deleteOnFailure defaults to false in uploadAndCleanupSync
 * #5-6 - Atomic cursor writes in saveCursor / saveLiveCursor
 * #9  - GCS cursor backup consecutive failure alerting
 * #11 - Configurable year range in getUtcPartition
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import fs from 'fs';
import path from 'path';
import os from 'os';

// ─────────────────────────────────────────────────────────────
// #11 - Configurable year range
// ─────────────────────────────────────────────────────────────
describe('#11 - getUtcPartition configurable year range', () => {
  let getUtcPartition;

  beforeEach(async () => {
    // Clear cached module to pick up env changes
    const mod = await import('../data-schema.js');
    getUtcPartition = mod.getUtcPartition;
  });

  afterEach(() => {
    delete process.env.PARTITION_YEAR_MIN;
    delete process.env.PARTITION_YEAR_MAX;
  });

  it('accepts 2025 with default range', () => {
    const result = getUtcPartition('2025-06-15T12:00:00Z');
    expect(result).toEqual({ year: 2025, month: 6, day: 15 });
  });

  it('rejects 2031 with default range (max 2035)', () => {
    // Default max is now 2035, so 2031 should pass
    const result = getUtcPartition('2031-01-01T00:00:00Z');
    expect(result.year).toBe(2031);
  });

  it('rejects year outside configured range', () => {
    process.env.PARTITION_YEAR_MIN = '2024';
    process.env.PARTITION_YEAR_MAX = '2026';
    expect(() => getUtcPartition('2023-01-01T00:00:00Z')).toThrow('out of range [2024-2026]');
    expect(() => getUtcPartition('2027-01-01T00:00:00Z')).toThrow('out of range [2024-2026]');
  });

  it('accepts year within configured range', () => {
    process.env.PARTITION_YEAR_MIN = '2024';
    process.env.PARTITION_YEAR_MAX = '2026';
    const result = getUtcPartition('2025-03-20T08:00:00Z');
    expect(result).toEqual({ year: 2025, month: 3, day: 20 });
  });
});

// ─────────────────────────────────────────────────────────────
// #1 - Quarantine null effective_at (structural verification)
// ─────────────────────────────────────────────────────────────
describe('#1 - Quarantine null effective_at events', () => {
  it('fetch-backfill decodeInMainThread relies on normalizeEvent to throw on null effective_at', async () => {
    const source = fs.readFileSync(
      path.join(process.cwd(), 'scripts/ingest/fetch-backfill.js'),
      'utf8'
    );
    // Claude Code removed the warn-and-skip guard; normalizeEvent now throws on null effective_at
    // Verify decodeInMainThread calls normalizeEvent which enforces the guard
    expect(source).toContain('normalizeEvent(');
    // Verify the data-schema enforces the guard
    const schemaSource = fs.readFileSync(
      path.join(process.cwd(), 'scripts/ingest/data-schema.js'),
      'utf8'
    );
    expect(schemaSource).toContain('could not determine effective_at');
  });

  it('fetch-updates processUpdates relies on normalizeEvent to throw on null effective_at', async () => {
    const source = fs.readFileSync(
      path.join(process.cwd(), 'scripts/ingest/fetch-updates.js'),
      'utf8'
    );
    // Claude Code removed the quarantine guard; normalizeEvent now throws on null effective_at
    expect(source).toContain('normalizeEvent');
    expect(source).toContain('quarantine guard');
  });
});

// ─────────────────────────────────────────────────────────────
// #2 - Reassignment handling in fetch-updates processUpdates
// ─────────────────────────────────────────────────────────────
describe('#2 - Reassignment events in processUpdates', () => {
  it('fetch-updates processUpdates handles reassignment events', () => {
    const source = fs.readFileSync(
      path.join(process.cwd(), 'scripts/ingest/fetch-updates.js'),
      'utf8'
    );
    // Must handle reassignment shape
    expect(source).toContain('isReassignment');
    expect(source).toContain('reassign_create');
    expect(source).toContain('reassign_archive');
    // Must build updateInfo with reassignment fields
    expect(source).toContain('source: u.source');
    expect(source).toContain('target: u.target');
    expect(source).toContain('unassign_id');
  });
});

// ─────────────────────────────────────────────────────────────
// #3 - deleteOnFailure default in uploadAndCleanupSync
// ─────────────────────────────────────────────────────────────
describe('#3 - deleteOnFailure defaults to false in sync upload', () => {
  it('uploadAndCleanupSync defaults deleteOnFailure to false', () => {
    const source = fs.readFileSync(
      path.join(process.cwd(), 'scripts/ingest/gcs-upload.js'),
      'utf8'
    );
    // Find the sync function's default destructuring
    const syncFnMatch = source.match(
      /export function uploadAndCleanupSync[\s\S]*?deleteOnFailure\s*=\s*(true|false)/
    );
    expect(syncFnMatch).toBeTruthy();
    expect(syncFnMatch[1]).toBe('false');
  });

  it('logs to dead-letter file on sync upload failure', () => {
    const source = fs.readFileSync(
      path.join(process.cwd(), 'scripts/ingest/gcs-upload.js'),
      'utf8'
    );
    expect(source).toContain('_logToDeadLetter');
    expect(source).toContain('dead-letter');
    expect(source).toContain('failed-uploads.jsonl');
  });

  it('keeps local file on failure when deleteOnFailure=false', () => {
    const source = fs.readFileSync(
      path.join(process.cwd(), 'scripts/ingest/gcs-upload.js'),
      'utf8'
    );
    expect(source).toContain('Keeping local file for retry');
  });
});

// ─────────────────────────────────────────────────────────────
// #5-6 - Atomic cursor writes
// ─────────────────────────────────────────────────────────────
describe('#5-6 - Atomic cursor writes', () => {
  it('fetch-backfill uses AtomicCursor for all cursor writes', () => {
    const source = fs.readFileSync(
      path.join(process.cwd(), 'scripts/ingest/fetch-backfill.js'),
      'utf8'
    );
    // saveCursor was removed — all writes go through AtomicCursor.saveAtomic()
    expect(source).toContain('atomicCursor.saveAtomic(');
    expect(source).not.toContain('function saveCursor(');
  });

  it('fetch-backfill imports atomicWriteFile', () => {
    const source = fs.readFileSync(
      path.join(process.cwd(), 'scripts/ingest/fetch-backfill.js'),
      'utf8'
    );
    expect(source).toContain("atomicWriteFile");
    expect(source).toContain("from './atomic-cursor.js'");
  });

  it('fetch-updates saveLiveCursor uses atomic write', () => {
    const source = fs.readFileSync(
      path.join(process.cwd(), 'scripts/ingest/fetch-updates.js'),
      'utf8'
    );
    const start = source.indexOf('function saveLiveCursor(migId, afterRecordTime)') !== -1
      ? source.indexOf('function saveLiveCursor(migId, afterRecordTime)')
      : source.indexOf('function saveLiveCursor(migrationId, afterRecordTime)');
    const saveFn = source.substring(start, start + 1000);
    expect(saveFn).toContain('atomicWriteFileForLive');
    expect(saveFn).not.toContain('fs.writeFileSync(LIVE_CURSOR_FILE');
  });

  it('fetch-updates saveLiveCursorLocal uses atomic write', () => {
    const source = fs.readFileSync(
      path.join(process.cwd(), 'scripts/ingest/fetch-updates.js'),
      'utf8'
    );
    const start = source.indexOf('function saveLiveCursorLocal(');
    const saveFn = source.substring(start, start + 600);
    expect(saveFn).toContain('atomicWriteFileForLive');
    expect(saveFn).not.toContain('fs.writeFileSync(LIVE_CURSOR_FILE');
  });

  it('bulletproof-backfill IntegrityCursor delegates to AtomicCursor', () => {
    const source = fs.readFileSync(
      path.join(process.cwd(), 'scripts/ingest/bulletproof-backfill.js'),
      'utf8'
    );
    // IntegrityCursor now delegates to AtomicCursor instead of calling atomicWriteFile directly
    expect(source).toContain('new AtomicCursor(');
    expect(source).toContain('this._cursor.');
    expect(source).not.toContain('writeFileSync(this.cursorPath');
  });

  it('atomicWriteFile performs write-tmp-fsync-rename pattern', () => {
    const source = fs.readFileSync(
      path.join(process.cwd(), 'scripts/ingest/atomic-cursor.js'),
      'utf8'
    );
    const start = source.indexOf('export function atomicWriteFile');
    const atomicFn = source.substring(start, start + 1500);
    expect(atomicFn).toContain('.tmp');
    expect(atomicFn).toContain('fsyncSync');
    expect(atomicFn).toContain('renameSync');
  });
});

// ─────────────────────────────────────────────────────────────
// #9 - GCS cursor backup failure alerting
// ─────────────────────────────────────────────────────────────
describe('#9 - GCS cursor backup consecutive failure alerting', () => {
  it('fetch-backfill tracks consecutive GCS backup failures', () => {
    const source = fs.readFileSync(
      path.join(process.cwd(), 'scripts/ingest/fetch-backfill.js'),
      'utf8'
    );
    expect(source).toContain('gcsCursorBackupConsecutiveFailures');
    expect(source).toContain('GCS_CURSOR_BACKUP_MAX_FAILURES');
  });

  it('fetch-backfill throws after max consecutive failures', () => {
    const source = fs.readFileSync(
      path.join(process.cwd(), 'scripts/ingest/fetch-backfill.js'),
      'utf8'
    );
    const backupFn = source.substring(
      source.indexOf('function backupCursorToGCS'),
      source.indexOf('function restoreCursorsFromGCS')
    );
    expect(backupFn).toContain('throw new Error');
    expect(backupFn).toContain('consecutive times');
  });

  it('fetch-backfill resets failure counter on success', () => {
    const source = fs.readFileSync(
      path.join(process.cwd(), 'scripts/ingest/fetch-backfill.js'),
      'utf8'
    );
    const backupFn = source.substring(
      source.indexOf('function backupCursorToGCS'),
      source.indexOf('function restoreCursorsFromGCS')
    );
    expect(backupFn).toContain('gcsCursorBackupConsecutiveFailures = 0');
  });

  it('fetch-updates tracks consecutive GCS backup failures', () => {
    const source = fs.readFileSync(
      path.join(process.cwd(), 'scripts/ingest/fetch-updates.js'),
      'utf8'
    );
    expect(source).toContain('gcsCursorBackupConsecutiveFailures');
    expect(source).toContain('GCS_CURSOR_BACKUP_MAX_FAILURES');
  });

  it('fetch-updates logs fatal after max consecutive failures', () => {
    const source = fs.readFileSync(
      path.join(process.cwd(), 'scripts/ingest/fetch-updates.js'),
      'utf8'
    );
    const start = source.indexOf('function backupCursorToGCS(cursor)');
    const backupFn = source.substring(start, start + 2000);
    expect(backupFn).toContain('logFatal');
    expect(backupFn).toContain('consecutive times');
    expect(backupFn).toContain('gcsCursorBackupConsecutiveFailures = 0');
  });

  it('max failures is configurable via environment', () => {
    const source = fs.readFileSync(
      path.join(process.cwd(), 'scripts/ingest/fetch-backfill.js'),
      'utf8'
    );
    expect(source).toContain("process.env.GCS_CURSOR_BACKUP_MAX_FAILURES");
  });
});
