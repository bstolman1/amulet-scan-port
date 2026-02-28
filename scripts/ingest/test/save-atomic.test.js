/**
 * saveAtomic() Tests
 * 
 * Verifies the convenience wrapper on AtomicCursor:
 * - Merges state fields into confirmedState
 * - Persists to disk atomically
 * - Commits any pending transaction first
 * - Round-trips correctly through load()
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { writeFileSync, mkdirSync, rmSync, readFileSync } from 'fs';
import { join } from 'path';
import { tmpdir } from 'os';

const TEST_DIR = join(tmpdir(), `save-atomic-test-${Date.now()}`);
const CURSOR_DIR = join(TEST_DIR, 'cursors');

vi.mock('../path-utils.js', () => ({
  getBaseDataDir: () => TEST_DIR,
  getCursorDir: () => CURSOR_DIR,
}));

const { AtomicCursor } = await import('../atomic-cursor.js');

describe('AtomicCursor.saveAtomic()', () => {
  beforeEach(() => {
    mkdirSync(CURSOR_DIR, { recursive: true });
  });

  afterEach(() => {
    try { rmSync(TEST_DIR, { recursive: true, force: true }); } catch {}
  });

  it('should persist state and reload correctly', () => {
    const cursor = new AtomicCursor(3, 'sync-test-save');
    cursor.saveAtomic({
      last_before: '2024-08-15T10:00:00Z',
      total_updates: 5000,
      total_events: 25000,
      min_time: '2024-06-25T00:00:00Z',
      max_time: '2024-12-10T00:00:00Z',
    });

    // Reload from disk
    const cursor2 = new AtomicCursor(3, 'sync-test-save');
    const state = cursor2.load();

    expect(state.lastBefore).toBe('2024-08-15T10:00:00Z');
    expect(state.totalUpdates).toBe(5000);
    expect(state.totalEvents).toBe(25000);
    expect(state.minTime).toBe('2024-06-25T00:00:00Z');
    expect(state.maxTime).toBe('2024-12-10T00:00:00Z');
  });

  it('should throw if transaction is open (must commit/rollback first)', () => {
    const cursor = new AtomicCursor(0, 'sync-pending-save');
    cursor.beginTransaction(10, 20, '2024-01-15T10:00:00Z');

    // saveAtomic now throws if transaction is open
    expect(() => cursor.saveAtomic({
      last_before: '2024-01-15T09:00:00Z',
      total_updates: 100,
      total_events: 500,
    })).toThrow('saveAtomic() called while a transaction is open');
  });

  it('should mark complete via saveAtomic', () => {
    const cursor = new AtomicCursor(0, 'sync-complete-save');
    cursor.saveAtomic({
      last_before: '2024-01-01T00:00:00Z',
      total_updates: 999,
      total_events: 4999,
      complete: true,
    });

    const cursor2 = new AtomicCursor(0, 'sync-complete-save');
    const state = cursor2.load();

    expect(state.complete).toBe(true);
    expect(state.totalUpdates).toBe(999);
  });

  it('should preserve GCS checkpoint after saveAtomic', () => {
    const cursor = new AtomicCursor(0, 'sync-gcs-save');
    cursor.beginTransaction(100, 500, '2024-06-15T10:00:00Z');
    cursor.commit();
    cursor.confirmGCS('2024-06-15T09:00:00Z', 80, 400);

    // saveAtomic should not overwrite GCS state
    cursor.saveAtomic({
      last_before: '2024-06-15T08:00:00Z',
      total_updates: 200,
      total_events: 1000,
    });

    const cursor2 = new AtomicCursor(0, 'sync-gcs-save');
    cursor2.load();
    const gcsStatus = cursor2.getGCSStatus();

    expect(gcsStatus.hasGCSCheckpoint).toBe(true);
    expect(gcsStatus.lastGCSConfirmed).toBe('2024-06-15T09:00:00Z');
  });

  it('should handle multiple sequential saveAtomic calls', () => {
    const cursor = new AtomicCursor(0, 'sync-multi-save');

    cursor.saveAtomic({ last_before: '2024-01-01T10:00:00Z', total_updates: 100, total_events: 500 });
    cursor.saveAtomic({ last_before: '2024-01-01T09:00:00Z', total_updates: 200, total_events: 1000 });
    cursor.saveAtomic({ last_before: '2024-01-01T08:00:00Z', total_updates: 300, total_events: 1500 });

    const cursor2 = new AtomicCursor(0, 'sync-multi-save');
    const state = cursor2.load();

    expect(state.lastBefore).toBe('2024-01-01T08:00:00Z');
    expect(state.totalUpdates).toBe(300);
    expect(state.totalEvents).toBe(1500);
  });

  it('should create backup file on second save', () => {
    const cursor = new AtomicCursor(0, 'sync-backup-save');
    cursor.saveAtomic({ last_before: '2024-01-01T10:00:00Z', total_updates: 100, total_events: 500 });
    cursor.saveAtomic({ last_before: '2024-01-01T09:00:00Z', total_updates: 200, total_events: 1000 });

    // Backup should exist and contain first save's state
    const backupPath = cursor.cursorPath + '.bak';
    const backup = JSON.parse(readFileSync(backupPath, 'utf8'));
    expect(backup.confirmed_updates).toBe(100);
  });
});
