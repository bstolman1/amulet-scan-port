/**
 * Atomic Cursor Coverage Tests
 * 
 * Tests for AtomicCursor that exercise REAL functionality:
 * - Transaction lifecycle (begin, commit, rollback)
 * - addPending accumulation
 * - File persistence (writing and loading cursor state)
 * - GCS checkpoint tracking
 * - Edge cases and error conditions
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { existsSync, writeFileSync, readFileSync, mkdirSync, rmSync } from 'fs';
import { join } from 'path';
import { tmpdir } from 'os';

// Create unique temp directory per test run to avoid collisions
const TEST_DIR = join(tmpdir(), `cursor-test-${Date.now()}-${Math.random().toString(36).slice(2)}`);
const CURSOR_DIR = join(TEST_DIR, 'cursors');

// Mock path-utils to redirect file operations to temp directory
vi.mock('../path-utils.js', () => ({
  getBaseDataDir: () => TEST_DIR,
  getCursorDir: () => CURSOR_DIR,
}));

// Import AFTER mocking
const { AtomicCursor, getCursorPath, loadCursorLegacy, isCursorComplete } = await import('../atomic-cursor.js');

describe('Atomic Cursor Coverage', () => {
  beforeEach(() => {
    mkdirSync(CURSOR_DIR, { recursive: true });
  });

  afterEach(() => {
    try {
      rmSync(TEST_DIR, { recursive: true, force: true });
    } catch {
      // Ignore cleanup errors
    }
  });

  describe('Transaction Lifecycle', () => {
    it('beginTransaction should set inTransaction flag', () => {
      const cursor = new AtomicCursor(0, 'test-begin');
      
      expect(cursor.inTransaction).toBe(false);
      
      cursor.beginTransaction(10, 20, '2024-01-15T10:00:00Z');
      
      expect(cursor.inTransaction).toBe(true);
    });
    
    it('beginTransaction should throw if already in transaction', () => {
      const cursor = new AtomicCursor(0, 'test-double-begin');
      
      cursor.beginTransaction(10, 20, '2024-01-15T10:00:00Z');
      
      expect(() => {
        cursor.beginTransaction(5, 10, '2024-01-15T11:00:00Z');
      }).toThrow('Already in transaction');
    });
    
    it('commit should add pending to confirmed totals', () => {
      const cursor = new AtomicCursor(0, 'test-commit');
      
      cursor.beginTransaction(100, 500, '2024-01-15T10:00:00Z');
      cursor.commit();
      
      expect(cursor.confirmedState.totalUpdates).toBe(100);
      expect(cursor.confirmedState.totalEvents).toBe(500);
      expect(cursor.confirmedState.lastBefore).toBe('2024-01-15T10:00:00Z');
    });
    
    it('commit should throw if not in transaction', () => {
      const cursor = new AtomicCursor(0, 'test-no-commit');
      
      expect(() => cursor.commit()).toThrow('No transaction in progress');
    });
    
    it('commit should accumulate across multiple transactions', () => {
      const cursor = new AtomicCursor(0, 'test-accumulate');
      
      cursor.beginTransaction(50, 100, '2024-01-15T08:00:00Z');
      cursor.commit();
      
      cursor.beginTransaction(50, 100, '2024-01-15T10:00:00Z');
      cursor.commit();
      
      expect(cursor.confirmedState.totalUpdates).toBe(100);
      expect(cursor.confirmedState.totalEvents).toBe(200);
    });
  });
  
  describe('addPending', () => {
    it('should auto-start transaction if not in one', () => {
      const cursor = new AtomicCursor(0, 'test-auto-tx');
      
      expect(cursor.inTransaction).toBe(false);
      
      cursor.addPending(10, 20, '2024-01-15T10:00:00Z');
      
      expect(cursor.inTransaction).toBe(true);
      expect(cursor.pendingState.updates).toBe(10);
    });
    
    it('should accumulate when already in transaction', () => {
      const cursor = new AtomicCursor(0, 'test-add');
      
      cursor.beginTransaction(10, 20, '2024-01-15T10:00:00Z');
      cursor.addPending(5, 10, '2024-01-15T09:00:00Z');
      
      expect(cursor.pendingState.updates).toBe(15);
      expect(cursor.pendingState.events).toBe(30);
    });
    
    it('should keep earlier timestamp when adding', () => {
      const cursor = new AtomicCursor(0, 'test-earlier');
      
      cursor.beginTransaction(10, 20, '2024-01-15T10:00:00Z');
      cursor.addPending(5, 10, '2024-01-15T08:00:00Z'); // Earlier
      
      expect(cursor.pendingState.lastBefore).toBe('2024-01-15T08:00:00Z');
    });
    
    it('should not change to later timestamp', () => {
      const cursor = new AtomicCursor(0, 'test-later');
      
      cursor.beginTransaction(10, 20, '2024-01-15T08:00:00Z');
      cursor.addPending(5, 10, '2024-01-15T12:00:00Z'); // Later
      
      expect(cursor.pendingState.lastBefore).toBe('2024-01-15T08:00:00Z');
    });
  });
  
  describe('rollback', () => {
    it('should do nothing if not in transaction', () => {
      const cursor = new AtomicCursor(0, 'test-rollback-noop');
      
      expect(() => cursor.rollback()).not.toThrow();
      expect(cursor.inTransaction).toBe(false);
    });
    
    it('should restore to pre-transaction state', () => {
      const cursor = new AtomicCursor(0, 'test-rollback');
      
      // First transaction
      cursor.beginTransaction(50, 100, '2024-01-15T08:00:00Z');
      cursor.commit();
      
      // Second transaction - then rollback
      cursor.beginTransaction(25, 50, '2024-01-15T10:00:00Z');
      cursor.rollback();
      
      // Should be back to first commit state
      expect(cursor.confirmedState.totalUpdates).toBe(50);
      expect(cursor.confirmedState.totalEvents).toBe(100);
      expect(cursor.inTransaction).toBe(false);
    });
    
    it('should clear pending state on rollback', () => {
      const cursor = new AtomicCursor(0, 'test-clear-pending');
      
      cursor.beginTransaction(100, 500, '2024-01-15T10:00:00Z');
      cursor.rollback();
      
      expect(cursor.pendingState.updates).toBe(0);
      expect(cursor.pendingState.events).toBe(0);
      expect(cursor.pendingState.lastBefore).toBe(null);
    });
  });
  
  describe('File Persistence', () => {
    it('should write cursor to disk on commit', () => {
      const cursor = new AtomicCursor(0, 'test-persist');
      
      cursor.beginTransaction(100, 500, '2024-01-15T10:00:00Z');
      cursor.commit();
      
      const path = getCursorPath(0, 'test-persist');
      expect(existsSync(path)).toBe(true);
      
      const saved = JSON.parse(readFileSync(path, 'utf8'));
      expect(saved.confirmed_updates).toBe(100);
      expect(saved.confirmed_events).toBe(500);
      expect(saved.last_confirmed_before).toBe('2024-01-15T10:00:00Z');
    });
    
    it('should load cursor state correctly', () => {
      // Create and save
      const cursor1 = new AtomicCursor(0, 'test-load');
      cursor1.beginTransaction(100, 500, '2024-01-15T10:00:00Z');
      cursor1.commit();
      
      // Load in new instance
      const cursor2 = new AtomicCursor(0, 'test-load');
      const loaded = cursor2.load();
      
      expect(loaded.totalUpdates).toBe(100);
      expect(loaded.totalEvents).toBe(500);
      expect(loaded.lastBefore).toBe('2024-01-15T10:00:00Z');
    });
    
    it('should recover from backup if main file corrupted', () => {
      const path = getCursorPath(0, 'test-backup');
      const backupPath = path + '.bak';
      
      // Write valid backup
      const validState = {
        migration_id: 0,
        synchronizer_id: 'test-backup',
        last_confirmed_before: '2024-01-15T08:00:00Z',
        confirmed_updates: 50,
        confirmed_events: 250,
      };
      writeFileSync(backupPath, JSON.stringify(validState));
      
      // Write corrupted main file
      writeFileSync(path, 'not valid json {{{');
      
      // Should recover from backup
      const cursor = new AtomicCursor(0, 'test-backup');
      const loaded = cursor.load();
      
      expect(loaded.totalUpdates).toBe(50);
      expect(loaded.lastBefore).toBe('2024-01-15T08:00:00Z');
    });
  });
  
  describe('getState', () => {
    it('should return complete state snapshot', () => {
      const cursor = new AtomicCursor(0, 'test-getstate');
      
      cursor.beginTransaction(100, 500, '2024-01-15T10:00:00Z');
      
      const state = cursor.getState();
      
      expect(state).toHaveProperty('confirmed');
      expect(state).toHaveProperty('pending');
      expect(state).toHaveProperty('inTransaction');
      expect(state).toHaveProperty('gcsStatus');
    });
    
    it('should reflect current transaction state', () => {
      const cursor = new AtomicCursor(0, 'test-state-tx');
      
      expect(cursor.getState().inTransaction).toBe(false);
      
      cursor.beginTransaction(100, 500, '2024-01-15T10:00:00Z');
      
      expect(cursor.getState().inTransaction).toBe(true);
      expect(cursor.getState().pending.updates).toBe(100);
    });
  });
  
  describe('saveAtomic', () => {
    it('should update confirmed state directly', () => {
      const cursor = new AtomicCursor(0, 'test-save');
      
      cursor.saveAtomic({
        last_before: '2024-01-15T10:00:00Z',
        total_updates: 100,
        total_events: 500,
      });
      
      expect(cursor.confirmedState.lastBefore).toBe('2024-01-15T10:00:00Z');
      expect(cursor.confirmedState.totalUpdates).toBe(100);
    });
    
    it('should auto-commit pending transaction', () => {
      const cursor = new AtomicCursor(0, 'test-save-commit');
      
      cursor.beginTransaction(50, 200, '2024-01-15T09:00:00Z');
      
      cursor.saveAtomic({
        last_before: '2024-01-15T10:00:00Z',
        total_updates: 100,
      });
      
      expect(cursor.inTransaction).toBe(false);
    });
    
    it('should update time bounds', () => {
      const cursor = new AtomicCursor(0, 'test-bounds');
      
      cursor.saveAtomic({
        min_time: '2024-01-01T00:00:00Z',
        max_time: '2024-12-31T23:59:59Z',
      });
      
      expect(cursor.confirmedState.minTime).toBe('2024-01-01T00:00:00Z');
      expect(cursor.confirmedState.maxTime).toBe('2024-12-31T23:59:59Z');
    });
    
    it('should set complete flag', () => {
      const cursor = new AtomicCursor(0, 'test-complete');
      
      cursor.saveAtomic({ complete: true });
      
      expect(cursor.confirmedState.complete).toBe(true);
    });
  });
  
  describe('GCS Checkpoint', () => {
    it('confirmGCS should record GCS-confirmed position', () => {
      const cursor = new AtomicCursor(0, 'test-gcs');
      
      cursor.beginTransaction(100, 500, '2024-01-15T10:00:00Z');
      cursor.commit();
      
      cursor.confirmGCS('2024-01-15T09:00:00Z', 80, 400);
      
      const status = cursor.getGCSStatus();
      expect(status.lastGCSConfirmed).toBe('2024-01-15T09:00:00Z');
      expect(status.gcsConfirmedUpdates).toBe(80);
      expect(status.pendingGCSUpdates).toBe(20);
    });
    
    it('confirmGCS with no args should sync to local', () => {
      const cursor = new AtomicCursor(0, 'test-gcs-sync');
      
      cursor.beginTransaction(100, 500, '2024-01-15T10:00:00Z');
      cursor.commit();
      
      cursor.confirmGCS();
      
      const status = cursor.getGCSStatus();
      expect(status.isSynced).toBe(true);
      expect(status.pendingGCSUpdates).toBe(0);
    });
    
    it('getResumePosition should prefer GCS-confirmed', () => {
      const cursor = new AtomicCursor(0, 'test-resume');
      
      cursor.beginTransaction(100, 500, '2024-01-15T10:00:00Z');
      cursor.commit();
      cursor.confirmGCS('2024-01-15T08:00:00Z', 50, 250);
      
      const pos = cursor.getResumePosition();
      
      expect(pos.lastBefore).toBe('2024-01-15T08:00:00Z');
      expect(pos.isGCSConfirmed).toBe(true);
    });
    
    it('getResumePosition with useLocalPosition returns local', () => {
      const cursor = new AtomicCursor(0, 'test-resume-local');
      
      cursor.beginTransaction(100, 500, '2024-01-15T10:00:00Z');
      cursor.commit();
      cursor.confirmGCS('2024-01-15T08:00:00Z', 50, 250);
      
      const pos = cursor.getResumePosition(true);
      
      expect(pos.lastBefore).toBe('2024-01-15T10:00:00Z');
      expect(pos.isGCSConfirmed).toBe(false);
    });
  });
  
  describe('loadCursorLegacy', () => {
    it('should load existing cursor', () => {
      const path = getCursorPath(0, 'test-legacy');
      writeFileSync(path, JSON.stringify({
        migration_id: 0,
        synchronizer_id: 'test-legacy',
        confirmed_updates: 100,
        confirmed_events: 500,
      }));
      
      const loaded = loadCursorLegacy(0, 'test-legacy');
      
      expect(loaded).not.toBeNull();
      expect(loaded.confirmed_updates).toBe(100);
    });
    
    it('should return null for non-existent cursor', () => {
      const loaded = loadCursorLegacy(0, 'nonexistent-xyz-123');
      expect(loaded).toBeNull();
    });
  });
  
  describe('isCursorComplete', () => {
    it('should return true if complete flag is set', () => {
      const path = getCursorPath(0, 'test-done');
      writeFileSync(path, JSON.stringify({
        migration_id: 0,
        synchronizer_id: 'test-done',
        complete: true,
      }));
      
      expect(isCursorComplete(0, 'test-done')).toBe(true);
    });
    
    it('should return false if complete flag is false', () => {
      const path = getCursorPath(0, 'test-not-done');
      writeFileSync(path, JSON.stringify({
        migration_id: 0,
        synchronizer_id: 'test-not-done',
        complete: false,
      }));
      
      expect(isCursorComplete(0, 'test-not-done')).toBe(false);
    });
    
    it('should return false for non-existent cursor', () => {
      expect(isCursorComplete(0, 'missing-xyz-456')).toBe(false);
    });
  });
});
