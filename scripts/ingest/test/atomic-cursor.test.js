/**
 * AtomicCursor Tests
 * 
 * Tests crash-safe cursor management including:
 * - Atomic file writes
 * - Transaction commit/rollback
 * - GCS checkpoint tracking
 * - Recovery from corrupted state
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { existsSync, readFileSync, writeFileSync, mkdirSync, rmSync, unlinkSync } from 'fs';
import { join } from 'path';
import { tmpdir } from 'os';

// Mock path-utils to use temp directory
const TEST_DIR = join(tmpdir(), `cursor-test-${Date.now()}`);
const CURSOR_DIR = join(TEST_DIR, 'cursors');

vi.mock('../path-utils.js', () => ({
  getBaseDataDir: () => TEST_DIR,
  getCursorDir: () => CURSOR_DIR,
}));

// Import after mocking
const { AtomicCursor } = await import('../atomic-cursor.js');

describe('AtomicCursor', () => {
  beforeEach(() => {
    mkdirSync(CURSOR_DIR, { recursive: true });
  });

  afterEach(() => {
    try {
      rmSync(TEST_DIR, { recursive: true, force: true });
    } catch (e) {
      // Ignore cleanup errors
    }
  });

  describe('Basic Operations', () => {
    it('should create cursor with correct initial state', () => {
      const cursor = new AtomicCursor(0, 'sync-123');
      
      expect(cursor.migrationId).toBe(0);
      expect(cursor.synchronizerId).toBe('sync-123');
      expect(cursor.confirmedState.lastBefore).toBeNull();
      expect(cursor.confirmedState.totalUpdates).toBe(0);
      expect(cursor.confirmedState.complete).toBe(false);
      expect(cursor.inTransaction).toBe(false);
    });

    it('should load empty state when no cursor file exists', () => {
      const cursor = new AtomicCursor(0, 'sync-new');
      const state = cursor.load();
      
      expect(state.lastBefore).toBeNull();
      expect(state.totalUpdates).toBe(0);
    });

    it('should persist and reload cursor state', () => {
      const cursor1 = new AtomicCursor(0, 'sync-persist');
      cursor1.beginTransaction(100, 500, '2024-01-15T10:00:00Z');
      cursor1.commit();
      
      // Create new cursor and load
      const cursor2 = new AtomicCursor(0, 'sync-persist');
      const state = cursor2.load();
      
      expect(state.totalUpdates).toBe(100);
      expect(state.totalEvents).toBe(500);
      expect(state.lastBefore).toBe('2024-01-15T10:00:00Z');
    });
  });

  describe('Transaction Management', () => {
    it('should begin transaction and track pending state', () => {
      const cursor = new AtomicCursor(0, 'sync-tx');
      
      cursor.beginTransaction(50, 200, '2024-01-15T12:00:00Z');
      
      expect(cursor.inTransaction).toBe(true);
      expect(cursor.pendingState.updates).toBe(50);
      expect(cursor.pendingState.events).toBe(200);
      expect(cursor.pendingState.lastBefore).toBe('2024-01-15T12:00:00Z');
    });

    it('should throw if beginning transaction while already in one', () => {
      const cursor = new AtomicCursor(0, 'sync-double-tx');
      cursor.beginTransaction(10, 20, '2024-01-15T12:00:00Z');
      
      expect(() => {
        cursor.beginTransaction(30, 40, '2024-01-15T13:00:00Z');
      }).toThrow('Already in transaction');
    });

    it('should commit transaction and update confirmed state', () => {
      const cursor = new AtomicCursor(0, 'sync-commit');
      
      cursor.beginTransaction(100, 500, '2024-01-15T10:00:00Z');
      const state = cursor.commit();
      
      expect(cursor.inTransaction).toBe(false);
      expect(state.totalUpdates).toBe(100);
      expect(state.totalEvents).toBe(500);
      expect(state.lastBefore).toBe('2024-01-15T10:00:00Z');
      expect(cursor.pendingState.updates).toBe(0);
    });

    it('should throw if committing without active transaction', () => {
      const cursor = new AtomicCursor(0, 'sync-no-tx');
      
      expect(() => {
        cursor.commit();
      }).toThrow('No transaction in progress');
    });

    it('should rollback transaction and restore previous state', () => {
      const cursor = new AtomicCursor(0, 'sync-rollback');
      
      // First transaction - committed
      cursor.beginTransaction(50, 100, '2024-01-15T09:00:00Z');
      cursor.commit();
      
      // Second transaction - rolled back
      cursor.beginTransaction(100, 200, '2024-01-15T10:00:00Z');
      cursor.rollback();
      
      expect(cursor.inTransaction).toBe(false);
      expect(cursor.confirmedState.totalUpdates).toBe(50);
      expect(cursor.confirmedState.lastBefore).toBe('2024-01-15T09:00:00Z');
    });

    it('should accumulate pending data with addPending', () => {
      const cursor = new AtomicCursor(0, 'sync-add');
      
      cursor.beginTransaction(10, 20, '2024-01-15T12:00:00Z');
      cursor.addPending(15, 30, '2024-01-15T11:00:00Z');
      
      expect(cursor.pendingState.updates).toBe(25);
      expect(cursor.pendingState.events).toBe(50);
      // Should keep earlier timestamp
      expect(cursor.pendingState.lastBefore).toBe('2024-01-15T11:00:00Z');
    });
  });

  describe('GCS Checkpoint Tracking', () => {
    it('should track GCS confirmed position separately', () => {
      const cursor = new AtomicCursor(0, 'sync-gcs');
      
      // Write some data locally
      cursor.beginTransaction(100, 500, '2024-01-15T10:00:00Z');
      cursor.commit();
      
      // GCS confirm at earlier position
      cursor.confirmGCS('2024-01-15T09:00:00Z', 80, 400);
      
      const status = cursor.getGCSStatus();
      
      expect(status.hasGCSCheckpoint).toBe(true);
      expect(status.lastGCSConfirmed).toBe('2024-01-15T09:00:00Z');
      expect(status.gcsConfirmedUpdates).toBe(80);
      expect(status.localUpdates).toBe(100);
      expect(status.pendingGCSUpdates).toBe(20);
      expect(status.isSynced).toBe(false);
    });

    it('should return GCS position for crash-safe resume', () => {
      const cursor = new AtomicCursor(0, 'sync-resume');
      
      cursor.beginTransaction(100, 500, '2024-01-15T10:00:00Z');
      cursor.commit();
      cursor.confirmGCS('2024-01-15T09:00:00Z', 80, 400);
      
      // Default resume should use GCS position
      const resumePos = cursor.getResumePosition();
      
      expect(resumePos.lastBefore).toBe('2024-01-15T09:00:00Z');
      expect(resumePos.totalUpdates).toBe(80);
      expect(resumePos.isGCSConfirmed).toBe(true);
    });

    it('should allow unsafe local resume position', () => {
      const cursor = new AtomicCursor(0, 'sync-unsafe');
      
      cursor.beginTransaction(100, 500, '2024-01-15T10:00:00Z');
      cursor.commit();
      cursor.confirmGCS('2024-01-15T09:00:00Z', 80, 400);
      
      const unsafePos = cursor.getResumePosition(true);
      
      expect(unsafePos.lastBefore).toBe('2024-01-15T10:00:00Z');
      expect(unsafePos.totalUpdates).toBe(100);
      expect(unsafePos.isGCSConfirmed).toBe(false);
    });

    it('should sync GCS position on confirmGCS with no args', () => {
      const cursor = new AtomicCursor(0, 'sync-auto');
      
      cursor.beginTransaction(100, 500, '2024-01-15T10:00:00Z');
      cursor.commit();
      cursor.confirmGCS(); // No args = sync to current local position
      
      const status = cursor.getGCSStatus();
      
      expect(status.isSynced).toBe(true);
      expect(status.pendingGCSUpdates).toBe(0);
    });
  });

  describe('Completion Marking', () => {
    it('should mark cursor as complete', () => {
      const cursor = new AtomicCursor(0, 'sync-complete');
      
      cursor.beginTransaction(100, 500, '2024-01-15T10:00:00Z');
      cursor.commit();
      cursor.markComplete();
      
      expect(cursor.confirmedState.complete).toBe(true);
    });

    it('should throw if marking complete with pending data', () => {
      const cursor = new AtomicCursor(0, 'sync-pending');
      
      cursor.beginTransaction(100, 500, '2024-01-15T10:00:00Z');
      // Don't commit
      
      expect(() => {
        cursor.markComplete();
      }).toThrow('Cannot mark complete with pending data');
    });
  });

  describe('Crash Recovery', () => {
    it('should recover from main file with pending state', () => {
      // Simulate crash during transaction by writing cursor with pending data
      const cursorPath = join(CURSOR_DIR, 'cursor-0-sync-crash.json');
      const crashState = {
        migration_id: 0,
        synchronizer_id: 'sync-crash',
        last_confirmed_before: '2024-01-15T09:00:00Z',
        confirmed_updates: 80,
        confirmed_events: 400,
        pending_updates: 20,
        pending_events: 100,
        in_transaction: true,
      };
      writeFileSync(cursorPath, JSON.stringify(crashState));
      
      // Load should resume from confirmed position, ignoring pending
      const cursor = new AtomicCursor(0, 'sync-crash');
      const state = cursor.load();
      
      expect(state.totalUpdates).toBe(80);
      expect(state.lastBefore).toBe('2024-01-15T09:00:00Z');
    });

    it('should recover from backup if main is corrupted', () => {
      const cursorPath = join(CURSOR_DIR, 'cursor-0-sync-corrupt.json');
      const backupPath = cursorPath + '.bak';
      
      // Write corrupted main file
      writeFileSync(cursorPath, '{ invalid json');
      
      // Write valid backup
      const backupState = {
        migration_id: 0,
        synchronizer_id: 'sync-corrupt',
        last_confirmed_before: '2024-01-15T08:00:00Z',
        confirmed_updates: 50,
        confirmed_events: 200,
      };
      writeFileSync(backupPath, JSON.stringify(backupState));
      
      const cursor = new AtomicCursor(0, 'sync-corrupt');
      const state = cursor.load();
      
      expect(state.totalUpdates).toBe(50);
      expect(state.lastBefore).toBe('2024-01-15T08:00:00Z');
    });

    it('should return null if both main and backup are corrupted', () => {
      const cursorPath = join(CURSOR_DIR, 'cursor-0-sync-both-bad.json');
      const backupPath = cursorPath + '.bak';
      
      writeFileSync(cursorPath, '{ invalid');
      writeFileSync(backupPath, 'also invalid }');
      
      const cursor = new AtomicCursor(0, 'sync-both-bad');
      const state = cursor.load();
      
      // Should return initial empty state
      expect(state.lastBefore).toBeNull();
      expect(state.totalUpdates).toBe(0);
    });
  });

  describe('Shard Support', () => {
    it('should create shard-specific cursor files', () => {
      const cursor1 = new AtomicCursor(0, 'sync-shard', 0, 4);
      const cursor2 = new AtomicCursor(0, 'sync-shard', 1, 4);
      
      cursor1.beginTransaction(10, 20, '2024-01-15T10:00:00Z');
      cursor1.commit();
      
      cursor2.beginTransaction(30, 40, '2024-01-15T11:00:00Z');
      cursor2.commit();
      
      // Reload and verify isolation
      const reload1 = new AtomicCursor(0, 'sync-shard', 0, 4);
      const reload2 = new AtomicCursor(0, 'sync-shard', 1, 4);
      
      expect(reload1.load().totalUpdates).toBe(10);
      expect(reload2.load().totalUpdates).toBe(30);
    });
  });

  describe('Time Bounds', () => {
    it('should track min/max time bounds', () => {
      const cursor = new AtomicCursor(0, 'sync-bounds');
      
      cursor.setTimeBounds('2024-01-01T00:00:00Z', '2024-12-31T23:59:59Z');
      
      expect(cursor.confirmedState.minTime).toBe('2024-01-01T00:00:00Z');
      expect(cursor.confirmedState.maxTime).toBe('2024-12-31T23:59:59Z');
    });
  });
});
