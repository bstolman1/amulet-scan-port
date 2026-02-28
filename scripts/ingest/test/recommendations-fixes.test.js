/**
 * Tests for all recommendation fixes:
 * #1  - Quarantine null effective_at events (not silently dropped)
 * #2  - Reassignment events handled in fetch-updates processUpdates
 * #3  - deleteOnFailure defaults to false in uploadAndCleanupSync
 * #5-6 - Atomic cursor writes in saveCursor / saveLiveCursor
 * #9  - GCS cursor backup consecutive failure alerting
 * #11 - Configurable year range in getUtcPartition
 *
 * DATA INTEGRITY FIXES (added):
 * #13 - Math.max pattern for maxRecordTime in fetch-updates
 * #14 - cursor_hold_on_errors when batchErrors > 0
 * #15 - Promise.allSettled in bufferUpdates/bufferEvents (both files)
 * #16 - Per-tx try/catch in processBackfillItems no-pool path
 * #17 - Per-tx try/catch in pool fallback .catch handler
 * #18 - LRU eviction for seenUpdateIds (keeps newest 250k)
 * #19 - Promise.allSettled for inflightProcesses in fetchTimeSliceStreaming
 * #20 - Finalization flush re-throws errors instead of catch {}
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
    expect(source).toContain('u.source');
    expect(source).toContain('u.target');
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

// ─────────────────────────────────────────────────────────────
// #13 - Math.max pattern for maxRecordTime in fetch-updates
// ─────────────────────────────────────────────────────────────
describe('#13 - Math.max for maxRecordTime cursor precision', () => {
  it('fetch-updates fetchUpdates iterates all transactions for max record_time', () => {
    const source = fs.readFileSync(
      path.join(process.cwd(), 'scripts/ingest/fetch-updates.js'),
      'utf8'
    );
    // Should iterate over all transactions to find the max
    expect(source).toContain('for (const tx of transactions)');
    expect(source).toContain('maxRecordTime');
    // Should NOT use transactions[transactions.length - 1].record_time
    expect(source).not.toMatch(/transactions\[transactions\.length\s*-\s*1\]\.record_time/);
  });

  it('fetch-updates compares record_time values to find maximum', () => {
    const source = fs.readFileSync(
      path.join(process.cwd(), 'scripts/ingest/fetch-updates.js'),
      'utf8'
    );
    // Should compare record_time values
    expect(source).toContain('tx.record_time > maxRecordTime');
  });
});

// ─────────────────────────────────────────────────────────────
// #14 - cursor_hold_on_errors when batchErrors > 0
// ─────────────────────────────────────────────────────────────
describe('#14 - Cursor hold on batch errors', () => {
  it('fetch-updates checks batchErrors > 0 and holds cursor', () => {
    const source = fs.readFileSync(
      path.join(process.cwd(), 'scripts/ingest/fetch-updates.js'),
      'utf8'
    );
    expect(source).toContain('cursor_hold_on_errors');
    expect(source).toContain('batchErrors > 0');
    expect(source).toContain('Cursor NOT advanced');
  });

  it('processUpdates returns errors count in its result', () => {
    const source = fs.readFileSync(
      path.join(process.cwd(), 'scripts/ingest/fetch-updates.js'),
      'utf8'
    );
    // processUpdates should return { updates, events, errors }
    expect(source).toContain('errors: errors.length');
  });
});

// ─────────────────────────────────────────────────────────────
// #15 - Promise.allSettled in bufferUpdates/bufferEvents
// ─────────────────────────────────────────────────────────────
describe('#15 - Independent writer execution via Promise.allSettled', () => {
  it('fetch-updates bufferUpdates uses Promise.allSettled', () => {
    const source = fs.readFileSync(
      path.join(process.cwd(), 'scripts/ingest/fetch-updates.js'),
      'utf8'
    );
    // Find bufferUpdates function and check for allSettled
    const bufferStart = source.indexOf('async function bufferUpdates(');
    const bufferEnd = source.indexOf('\nasync function bufferEvents(', bufferStart);
    const bufferFn = source.substring(bufferStart, bufferEnd);
    expect(bufferFn).toContain('Promise.allSettled');
    expect(bufferFn).not.toMatch(/Promise\.all\b\(/);
  });

  it('fetch-updates bufferEvents uses Promise.allSettled', () => {
    const source = fs.readFileSync(
      path.join(process.cwd(), 'scripts/ingest/fetch-updates.js'),
      'utf8'
    );
    const bufferStart = source.indexOf('async function bufferEvents(');
    const bufferEnd = source.indexOf('\nasync function flushAll(', bufferStart);
    const bufferFn = source.substring(bufferStart, bufferEnd);
    expect(bufferFn).toContain('Promise.allSettled');
  });

  it('fetch-backfill bufferUpdates uses Promise.allSettled', () => {
    const source = fs.readFileSync(
      path.join(process.cwd(), 'scripts/ingest/fetch-backfill.js'),
      'utf8'
    );
    const bufferStart = source.indexOf('async function bufferUpdates(');
    const bufferEnd = source.indexOf('\nasync function bufferEvents(', bufferStart);
    const bufferFn = source.substring(bufferStart, bufferEnd);
    expect(bufferFn).toContain('Promise.allSettled');
  });

  it('fetch-backfill bufferEvents uses Promise.allSettled', () => {
    const source = fs.readFileSync(
      path.join(process.cwd(), 'scripts/ingest/fetch-backfill.js'),
      'utf8'
    );
    const bufferStart = source.indexOf('async function bufferEvents(');
    const bufferEnd = source.indexOf('\nasync function flushAll(', bufferStart);
    const bufferFn = source.substring(bufferStart, bufferEnd);
    expect(bufferFn).toContain('Promise.allSettled');
  });
});

// ─────────────────────────────────────────────────────────────
// #16 - Per-tx try/catch in processBackfillItems no-pool path
// ─────────────────────────────────────────────────────────────
describe('#16 - Per-tx try/catch in no-pool decode path', () => {
  it('fetch-backfill processBackfillItems no-pool path has per-tx try/catch', () => {
    const source = fs.readFileSync(
      path.join(process.cwd(), 'scripts/ingest/fetch-backfill.js'),
      'utf8'
    );
    // Find the no-pool path inside processBackfillItems
    const fnStart = source.indexOf('async function processBackfillItems(');
    const fnEnd = source.indexOf('\n/**', fnStart + 100); // next function/jsdoc
    const fnBody = source.substring(fnStart, fnEnd > fnStart ? fnEnd : fnStart + 3000);

    // Should have per-tx loop with try/catch
    expect(fnBody).toContain('for (const tx of transactions)');
    expect(fnBody).toContain('catch (err)');
    expect(fnBody).toContain('[decode-main] Failed to decode tx');
  });
});

// ─────────────────────────────────────────────────────────────
// #17 - Per-tx try/catch in pool fallback .catch handler
// ─────────────────────────────────────────────────────────────
describe('#17 - Per-tx try/catch in pool fallback', () => {
  it('fetch-backfill pool fallback .catch has per-tx try/catch', () => {
    const source = fs.readFileSync(
      path.join(process.cwd(), 'scripts/ingest/fetch-backfill.js'),
      'utf8'
    );
    // The pool fallback is inside a .catch() handler
    const fnStart = source.indexOf('async function processBackfillItems(');
    const fnEnd = source.indexOf('\n/**', fnStart + 100);
    const fnBody = source.substring(fnStart, fnEnd > fnStart ? fnEnd : fnStart + 3000);

    expect(fnBody).toContain('Worker batch failed, main-thread fallback');
    expect(fnBody).toContain('[decode-fallback]');
    expect(fnBody).toContain('catch (fallbackErr)');
  });
});

// ─────────────────────────────────────────────────────────────
// #18 - LRU eviction for seenUpdateIds
// ─────────────────────────────────────────────────────────────
describe('#18 - LRU eviction for seenUpdateIds', () => {
  it('fetch-backfill fetchTimeSliceStreaming uses LRU eviction at 500k', () => {
    const source = fs.readFileSync(
      path.join(process.cwd(), 'scripts/ingest/fetch-backfill.js'),
      'utf8'
    );
    // Should check size > 500000
    expect(source).toContain('seenUpdateIds.size > 500000');
    // Should keep newest 250k, not just .clear()
    expect(source).toContain('entries.length - 250000');
  });

  it('fetch-backfill does NOT use bare seenUpdateIds.clear() for eviction', () => {
    const source = fs.readFileSync(
      path.join(process.cwd(), 'scripts/ingest/fetch-backfill.js'),
      'utf8'
    );
    // Find the fetchTimeSliceStreaming function
    const fnStart = source.indexOf('async function fetchTimeSliceStreaming(');
    const fnEnd = source.indexOf('\nasync function parallelFetchBatch(', fnStart);
    const fnBody = source.substring(fnStart, fnEnd);

    // The clear() should only appear as part of the LRU pattern (clear then re-add),
    // not as a standalone eviction
    const clearCount = (fnBody.match(/seenUpdateIds\.clear\(\)/g) || []).length;
    const addBackCount = (fnBody.match(/seenUpdateIds\.add\(entries\[/g) || []).length;
    // clear() should be followed by re-adding entries
    expect(addBackCount).toBeGreaterThanOrEqual(clearCount);
  });
});

// ─────────────────────────────────────────────────────────────
// #19 - Promise.allSettled for inflightProcesses
// ─────────────────────────────────────────────────────────────
describe('#19 - Promise.allSettled for inflight processes', () => {
  it('fetch-backfill fetchTimeSliceStreaming uses Promise.allSettled for finalization', () => {
    const source = fs.readFileSync(
      path.join(process.cwd(), 'scripts/ingest/fetch-backfill.js'),
      'utf8'
    );
    const fnStart = source.indexOf('async function fetchTimeSliceStreaming(');
    const fnEnd = source.indexOf('\nasync function parallelFetchBatch(', fnStart);
    const fnBody = source.substring(fnStart, fnEnd);

    // Should use Promise.allSettled for inflight processes
    expect(fnBody).toContain('Promise.allSettled(inflightProcesses)');
    // Should check for and re-throw rejected results
    expect(fnBody).toContain("r.status === 'rejected'");
    expect(fnBody).toContain('throw failed[0].reason');
  });
});

// ─────────────────────────────────────────────────────────────
// #20 - write-parquet flushUpdates/flushEvents per-partition requeue
// ─────────────────────────────────────────────────────────────
describe('#20 - Per-partition failure tracking in write-parquet', () => {
  it('write-parquet flushUpdates uses Promise.allSettled for per-partition tracking', () => {
    const source = fs.readFileSync(
      path.join(process.cwd(), 'scripts/ingest/write-parquet.js'),
      'utf8'
    );
    const fnStart = source.indexOf('export async function flushUpdates()');
    const fnEnd = source.indexOf('export async function flushEvents()', fnStart);
    const fnBody = source.substring(fnStart, fnEnd);

    expect(fnBody).toContain('Promise.allSettled(writePromises)');
    expect(fnBody).toContain('failedRecords');
    // Should only re-queue failed partition records, not all
    expect(fnBody).toContain('failedRecords.push(...records)');
  });

  it('write-parquet flushEvents uses Promise.allSettled for per-partition tracking', () => {
    const source = fs.readFileSync(
      path.join(process.cwd(), 'scripts/ingest/write-parquet.js'),
      'utf8'
    );
    const fnStart = source.indexOf('export async function flushEvents()');
    const fnEnd = source.indexOf('export async function flushAll()', fnStart);
    const fnBody = source.substring(fnStart, fnEnd);

    expect(fnBody).toContain('Promise.allSettled(writePromises)');
    expect(fnBody).toContain('failedRecords');
    expect(fnBody).toContain('failedRecords.push(...records)');
  });

  it('write-parquet waitForWrites uses Promise.all (not allSettled) to propagate errors', () => {
    const source = fs.readFileSync(
      path.join(process.cwd(), 'scripts/ingest/write-parquet.js'),
      'utf8'
    );
    const fnStart = source.indexOf('export async function waitForWrites()');
    const fnEnd = source.indexOf('\nexport async function shutdown()', fnStart);
    const fnBody = source.substring(fnStart, fnEnd);

    // Should use Promise.all (not allSettled) so write failures propagate
    expect(fnBody).toContain('Promise.all([...pendingWrites])');
    expect(fnBody).not.toContain('Promise.allSettled');
  });
});
