/**
 * Multi-Migration Sequencing Tests
 * 
 * Verifies cursor isolation between migrations:
 * - Each migration gets its own cursor file
 * - Completing migration 0 doesn't affect migration 1
 * - loadCursorLegacy reads correct migration-specific cursor
 * - isCursorComplete checks per-migration status
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { mkdirSync, rmSync } from 'fs';
import { join } from 'path';
import { tmpdir } from 'os';

const TEST_DIR = join(tmpdir(), `multi-mig-test-${Date.now()}`);
const CURSOR_DIR = join(TEST_DIR, 'cursors');

vi.mock('../path-utils.js', () => ({
  getBaseDataDir: () => TEST_DIR,
  getCursorDir: () => CURSOR_DIR,
}));

const { AtomicCursor, loadCursorLegacy, isCursorComplete } = await import('../atomic-cursor.js');

describe('Multi-Migration Cursor Isolation', () => {
  beforeEach(() => {
    mkdirSync(CURSOR_DIR, { recursive: true });
  });

  afterEach(() => {
    try { rmSync(TEST_DIR, { recursive: true, force: true }); } catch {}
  });

  it('should isolate cursors between migrations', () => {
    const sync = 'global-domain-sync-001';

    const c0 = new AtomicCursor(0, sync);
    c0.beginTransaction(100, 500, '2024-03-01T00:00:00Z');
    c0.commit();
    c0.confirmGCS('2024-03-01T00:00:00Z', 100, 500);
    c0.markComplete();

    const c1 = new AtomicCursor(1, sync);
    c1.beginTransaction(200, 1000, '2024-06-01T00:00:00Z');
    c1.commit();

    // Verify isolation
    const reload0 = new AtomicCursor(0, sync);
    const state0 = reload0.load();
    expect(state0.complete).toBe(true);
    expect(state0.totalUpdates).toBe(100);

    const reload1 = new AtomicCursor(1, sync);
    const state1 = reload1.load();
    expect(state1.complete).toBe(false);
    expect(state1.totalUpdates).toBe(200);
  });

  it('should check completion per migration via isCursorComplete', () => {
    const sync = 'sync-complete-check';

    const c0 = new AtomicCursor(0, sync);
    c0.beginTransaction(50, 100, '2024-01-01T00:00:00Z');
    c0.commit();
    c0.confirmGCS('2024-01-01T00:00:00Z', 50, 100);
    c0.markComplete();

    const c1 = new AtomicCursor(1, sync);
    c1.beginTransaction(10, 20, '2024-04-01T00:00:00Z');
    c1.commit();
    // NOT marked complete

    expect(isCursorComplete(0, sync)).toBe(true);
    expect(isCursorComplete(1, sync)).toBe(false);
    expect(isCursorComplete(2, sync)).toBe(false); // doesn't exist
  });

  it('should load correct migration via loadCursorLegacy', () => {
    const sync = 'sync-legacy-load';

    const c0 = new AtomicCursor(0, sync);
    c0.saveAtomic({ last_before: '2024-01-01T00:00:00Z', total_updates: 1000, total_events: 5000 });

    const c3 = new AtomicCursor(3, sync);
    c3.saveAtomic({ last_before: '2024-08-15T00:00:00Z', total_updates: 50000, total_events: 250000 });

    const loaded0 = loadCursorLegacy(0, sync);
    expect(loaded0.confirmed_updates).toBe(1000);

    const loaded3 = loadCursorLegacy(3, sync);
    expect(loaded3.confirmed_updates).toBe(50000);

    const loaded2 = loadCursorLegacy(2, sync);
    expect(loaded2).toBeNull();
  });

  it('should isolate sharded cursors within same migration', () => {
    const sync = 'sync-sharded';

    const shard0 = new AtomicCursor(3, sync, 0, 4);
    shard0.saveAtomic({ last_before: '2024-07-01T00:00:00Z', total_updates: 100, total_events: 500 });

    const shard1 = new AtomicCursor(3, sync, 1, 4);
    shard1.saveAtomic({ last_before: '2024-08-01T00:00:00Z', total_updates: 200, total_events: 1000 });

    const reload0 = new AtomicCursor(3, sync, 0, 4);
    expect(reload0.load().totalUpdates).toBe(100);

    const reload1 = new AtomicCursor(3, sync, 1, 4);
    expect(reload1.load().totalUpdates).toBe(200);

    // Non-sharded cursor for same migration should be separate
    const nonSharded = new AtomicCursor(3, sync);
    expect(nonSharded.load().totalUpdates).toBe(0);
  });

  it('should handle sequential migration processing pattern', () => {
    const sync = 'sync-sequential';
    const migrations = [0, 1, 2, 3];

    // Process each migration sequentially
    for (const migId of migrations) {
      const cursor = new AtomicCursor(migId, sync);
      const state = cursor.load();

      if (state.complete) continue;

      // Simulate processing
      const updates = 1000 * (migId + 1);
      const events = 5000 * (migId + 1);
      const ts = `2024-0${migId + 1}-01T00:00:00Z`;
      cursor.beginTransaction(updates, events, ts);
      cursor.commit();
      cursor.confirmGCS(ts, updates, events);
      cursor.markComplete();
    }

    // Verify all complete and isolated
    for (const migId of migrations) {
      expect(isCursorComplete(migId, sync)).toBe(true);
      const data = loadCursorLegacy(migId, sync);
      expect(data.confirmed_updates).toBe(1000 * (migId + 1));
    }
  });
});
