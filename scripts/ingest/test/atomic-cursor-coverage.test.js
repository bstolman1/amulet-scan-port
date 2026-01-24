/**
 * Atomic Cursor Coverage Tests
 * 
 * Additional tests to cover lines not hit by existing tests:
 * - addPending when not in transaction (lines 286-288)
 * - rollback edge case (line 336)
 * - getState() method (lines 508-515)
 * - saveAtomic() method (lines 523-553)
 * - loadCursorLegacy (lines 559-561)
 * - isCursorComplete (lines 567-569)
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { existsSync, writeFileSync, mkdirSync, rmSync } from 'fs';
import { join } from 'path';
import { tmpdir } from 'os';

// Mock path-utils to use temp directory
const TEST_DIR = join(tmpdir(), `cursor-coverage-test-${Date.now()}`);
const CURSOR_DIR = join(TEST_DIR, 'cursors');

vi.mock('../path-utils.js', () => ({
  getBaseDataDir: () => TEST_DIR,
  getCursorDir: () => CURSOR_DIR,
}));

// Import after mocking
const { AtomicCursor, loadCursorLegacy, isCursorComplete } = await import('../atomic-cursor.js');

describe('Atomic Cursor Coverage', () => {
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

  describe('addPending when not in transaction', () => {
    it('should start transaction automatically when addPending called without active transaction', () => {
      const cursor = new AtomicCursor(0, 'sync-auto-tx');
      
      expect(cursor.inTransaction).toBe(false);
      
      // Call addPending without beginTransaction
      cursor.addPending(10, 20, '2024-01-15T10:00:00Z');
      
      // Should have auto-started transaction
      expect(cursor.inTransaction).toBe(true);
      expect(cursor.pendingState.updates).toBe(10);
      expect(cursor.pendingState.events).toBe(20);
    });
    
    it('should accumulate when already in transaction', () => {
      const cursor = new AtomicCursor(0, 'sync-accumulate');
      
      cursor.beginTransaction(10, 20, '2024-01-15T10:00:00Z');
      cursor.addPending(5, 10, '2024-01-15T09:00:00Z');
      
      expect(cursor.pendingState.updates).toBe(15);
      expect(cursor.pendingState.events).toBe(30);
      // Should keep earlier timestamp
      expect(cursor.pendingState.lastBefore).toBe('2024-01-15T09:00:00Z');
    });
    
    it('should keep existing timestamp if new one is later', () => {
      const cursor = new AtomicCursor(0, 'sync-keep-earlier');
      
      cursor.beginTransaction(10, 20, '2024-01-15T08:00:00Z');
      cursor.addPending(5, 10, '2024-01-15T10:00:00Z');
      
      // Should keep the earlier timestamp
      expect(cursor.pendingState.lastBefore).toBe('2024-01-15T08:00:00Z');
    });
  });
  
  describe('rollback edge cases', () => {
    it('should do nothing when rollback called without transaction', () => {
      const cursor = new AtomicCursor(0, 'sync-no-rollback');
      
      // No transaction started
      expect(cursor.inTransaction).toBe(false);
      
      // Should not throw
      expect(() => cursor.rollback()).not.toThrow();
      
      expect(cursor.inTransaction).toBe(false);
    });
    
    it('should restore state after rollback', () => {
      const cursor = new AtomicCursor(0, 'sync-restore');
      
      // First commit
      cursor.beginTransaction(50, 100, '2024-01-15T08:00:00Z');
      cursor.commit();
      
      // Second transaction - then rollback
      cursor.beginTransaction(25, 50, '2024-01-15T09:00:00Z');
      expect(cursor.pendingState.updates).toBe(25);
      
      cursor.rollback();
      
      // Should restore to first commit state
      expect(cursor.confirmedState.totalUpdates).toBe(50);
      expect(cursor.confirmedState.totalEvents).toBe(100);
      expect(cursor.inTransaction).toBe(false);
    });
  });
  
  describe('getState', () => {
    it('should return complete state object', () => {
      const cursor = new AtomicCursor(0, 'sync-getstate');
      
      cursor.beginTransaction(100, 500, '2024-01-15T10:00:00Z');
      
      const state = cursor.getState();
      
      expect(state).toHaveProperty('confirmed');
      expect(state).toHaveProperty('pending');
      expect(state).toHaveProperty('inTransaction');
      expect(state).toHaveProperty('gcsStatus');
    });
    
    it('should return confirmed state copy', () => {
      const cursor = new AtomicCursor(0, 'sync-confirmed');
      
      cursor.beginTransaction(100, 500, '2024-01-15T10:00:00Z');
      cursor.commit();
      
      const state = cursor.getState();
      
      expect(state.confirmed.totalUpdates).toBe(100);
      expect(state.confirmed.totalEvents).toBe(500);
    });
    
    it('should return pending state copy', () => {
      const cursor = new AtomicCursor(0, 'sync-pending');
      
      cursor.beginTransaction(50, 200, '2024-01-15T11:00:00Z');
      
      const state = cursor.getState();
      
      expect(state.pending.updates).toBe(50);
      expect(state.pending.events).toBe(200);
      expect(state.pending.lastBefore).toBe('2024-01-15T11:00:00Z');
    });
    
    it('should return transaction status', () => {
      const cursor = new AtomicCursor(0, 'sync-tx-status');
      
      expect(cursor.getState().inTransaction).toBe(false);
      
      cursor.beginTransaction(10, 20, '2024-01-15T12:00:00Z');
      
      expect(cursor.getState().inTransaction).toBe(true);
    });
  });
  
  describe('saveAtomic', () => {
    it('should update confirmed state with provided values', () => {
      const cursor = new AtomicCursor(0, 'sync-save-atomic');
      
      cursor.saveAtomic({
        last_before: '2024-01-15T10:00:00Z',
        total_updates: 100,
        total_events: 500,
      });
      
      expect(cursor.confirmedState.lastBefore).toBe('2024-01-15T10:00:00Z');
      expect(cursor.confirmedState.totalUpdates).toBe(100);
      expect(cursor.confirmedState.totalEvents).toBe(500);
    });
    
    it('should commit pending transaction before saving', () => {
      const cursor = new AtomicCursor(0, 'sync-commit-before');
      
      cursor.beginTransaction(50, 200, '2024-01-15T09:00:00Z');
      
      expect(cursor.inTransaction).toBe(true);
      
      cursor.saveAtomic({
        last_before: '2024-01-15T10:00:00Z',
        total_updates: 100,
      });
      
      expect(cursor.inTransaction).toBe(false);
      // Should have committed 50 updates + set 100 = 150 (commit adds, then set overwrites)
      expect(cursor.confirmedState.totalUpdates).toBe(100);
    });
    
    it('should update min_time and max_time', () => {
      const cursor = new AtomicCursor(0, 'sync-time-bounds');
      
      cursor.saveAtomic({
        min_time: '2024-01-01T00:00:00Z',
        max_time: '2024-12-31T23:59:59Z',
      });
      
      expect(cursor.confirmedState.minTime).toBe('2024-01-01T00:00:00Z');
      expect(cursor.confirmedState.maxTime).toBe('2024-12-31T23:59:59Z');
    });
    
    it('should update complete flag', () => {
      const cursor = new AtomicCursor(0, 'sync-complete');
      
      cursor.saveAtomic({
        complete: true,
      });
      
      expect(cursor.confirmedState.complete).toBe(true);
    });
    
    it('should only update provided fields', () => {
      const cursor = new AtomicCursor(0, 'sync-partial');
      
      // Set initial state
      cursor.saveAtomic({
        last_before: '2024-01-15T10:00:00Z',
        total_updates: 100,
        total_events: 500,
      });
      
      // Update only one field
      cursor.saveAtomic({
        total_updates: 200,
      });
      
      // Other fields should remain
      expect(cursor.confirmedState.lastBefore).toBe('2024-01-15T10:00:00Z');
      expect(cursor.confirmedState.totalUpdates).toBe(200);
      expect(cursor.confirmedState.totalEvents).toBe(500);
    });
    
    it('should return confirmed state', () => {
      const cursor = new AtomicCursor(0, 'sync-return');
      
      const result = cursor.saveAtomic({
        total_updates: 100,
      });
      
      expect(result).toBe(cursor.confirmedState);
    });
  });
  
  describe('loadCursorLegacy', () => {
    it('should load cursor from file path', () => {
      // Create cursor file
      const cursorPath = join(CURSOR_DIR, 'cursor-0-sync-legacy.json');
      const state = {
        migration_id: 0,
        synchronizer_id: 'sync-legacy',
        last_confirmed_before: '2024-01-15T10:00:00Z',
        confirmed_updates: 100,
        confirmed_events: 500,
      };
      writeFileSync(cursorPath, JSON.stringify(state));
      
      const loaded = loadCursorLegacy(0, 'sync-legacy');
      
      expect(loaded).toBeDefined();
      expect(loaded.confirmed_updates).toBe(100);
    });
    
    it('should return null for non-existent cursor', () => {
      const loaded = loadCursorLegacy(0, 'sync-nonexistent-xyz');
      
      expect(loaded).toBeNull();
    });
  });
  
  describe('isCursorComplete', () => {
    it('should return true if cursor is marked complete', () => {
      const cursorPath = join(CURSOR_DIR, 'cursor-0-sync-done.json');
      const state = {
        migration_id: 0,
        synchronizer_id: 'sync-done',
        complete: true,
      };
      writeFileSync(cursorPath, JSON.stringify(state));
      
      const isComplete = isCursorComplete(0, 'sync-done');
      
      expect(isComplete).toBe(true);
    });
    
    it('should return false if cursor is not complete', () => {
      const cursorPath = join(CURSOR_DIR, 'cursor-0-sync-incomplete.json');
      const state = {
        migration_id: 0,
        synchronizer_id: 'sync-incomplete',
        complete: false,
      };
      writeFileSync(cursorPath, JSON.stringify(state));
      
      const isComplete = isCursorComplete(0, 'sync-incomplete');
      
      expect(isComplete).toBe(false);
    });
    
    it('should return false if cursor does not exist', () => {
      const isComplete = isCursorComplete(0, 'sync-missing-xyz');
      
      expect(isComplete).toBe(false);
    });
  });
  
  describe('GCS checkpoint edge cases', () => {
    it('should handle confirmGCS with all arguments', () => {
      const cursor = new AtomicCursor(0, 'sync-gcs-full');
      
      cursor.beginTransaction(100, 500, '2024-01-15T10:00:00Z');
      cursor.commit();
      
      cursor.confirmGCS('2024-01-15T09:00:00Z', 80, 400);
      
      const status = cursor.getGCSStatus();
      
      expect(status.lastGCSConfirmed).toBe('2024-01-15T09:00:00Z');
      expect(status.gcsConfirmedUpdates).toBe(80);
    });
    
    it('should handle confirmGCS with no arguments (sync to local)', () => {
      const cursor = new AtomicCursor(0, 'sync-gcs-sync');
      
      cursor.beginTransaction(100, 500, '2024-01-15T10:00:00Z');
      cursor.commit();
      
      cursor.confirmGCS();
      
      const status = cursor.getGCSStatus();
      
      expect(status.isSynced).toBe(true);
      expect(status.pendingGCSUpdates).toBe(0);
    });
  });
});
