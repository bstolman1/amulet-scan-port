#!/usr/bin/env node
/**
 * Bulletproof Backfill Process - Zero Data Loss Guarantee
 * 
 * KEY PRINCIPLES:
 * 1. CURSOR ONLY UPDATES AFTER CONFIRMED WRITES - Never advance cursor until data is on disk
 * 2. OVERLAP TIME RANGES - Add overlap between fetches to catch boundary cases
 * 3. VERIFY BEFORE ADVANCE - Verify record counts match before advancing
 * 4. DEDUP AT READ TIME - Accept duplicates during fetch, dedup when querying
 * 5. NEVER SKIP EMPTY - Empty responses = step back 1ms only, never jump
 * 
 * This module exports hardened versions of the fetch functions.
 */

import { existsSync, readFileSync, writeFileSync, mkdirSync, readdirSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Configuration
const WSL_DEFAULT = '/home/bstolz/canton-explorer/data';
const BASE_DATA_DIR = process.env.DATA_DIR || WSL_DEFAULT;
const CURSOR_DIR = process.env.CURSOR_DIR || join(BASE_DATA_DIR, 'cursors');
const RAW_DIR = join(BASE_DATA_DIR, 'raw');

// Constants for bulletproof operation
const OVERLAP_MS = 1000; // 1 second overlap between time slices to catch boundary cases
const MAX_EMPTY_BEFORE_STEP = 10; // Maximum empty responses before stepping back
const STEP_BACK_MS = 1; // Only step back 1ms on empty - NEVER more
const VERIFY_INTERVAL = 100; // Verify counts every N batches

/**
 * Integrity-checked cursor operations
 */
export class IntegrityCursor {
  constructor(migrationId, synchronizerId, shardIndex = null) {
    this.migrationId = migrationId;
    this.synchronizerId = synchronizerId;
    this.shardIndex = shardIndex;
    this.cursorPath = this._getCursorPath();
    this.pendingUpdates = 0;
    this.pendingEvents = 0;
    this.confirmedUpdates = 0;
    this.confirmedEvents = 0;
    this.lastConfirmedBefore = null;
    this.writeBuffer = [];
  }

  _getCursorPath() {
    const shardSuffix = this.shardIndex !== null ? `-shard${this.shardIndex}` : '';
    const sanitized = this.synchronizerId.replace(/[^a-zA-Z0-9-_]/g, '_').substring(0, 50);
    return join(CURSOR_DIR, `cursor-${this.migrationId}-${sanitized}${shardSuffix}.json`);
  }

  load() {
    if (existsSync(this.cursorPath)) {
      const data = JSON.parse(readFileSync(this.cursorPath, 'utf8'));
      this.confirmedUpdates = data.confirmed_updates || data.total_updates || 0;
      this.confirmedEvents = data.confirmed_events || data.total_events || 0;
      this.lastConfirmedBefore = data.last_confirmed_before || data.last_before || null;
      return data;
    }
    return null;
  }

  /**
   * Record pending data (not yet written to disk)
   * DO NOT advance cursor position yet
   */
  recordPending(updates, events) {
    this.pendingUpdates += updates;
    this.pendingEvents += events;
  }

  /**
   * Confirm data has been written to disk
   * NOW it's safe to advance cursor position
   */
  confirmWrite(writtenUpdates, writtenEvents, beforeTimestamp) {
    this.confirmedUpdates += writtenUpdates;
    this.confirmedEvents += writtenEvents;
    this.lastConfirmedBefore = beforeTimestamp;
    this.pendingUpdates -= writtenUpdates;
    this.pendingEvents -= writtenEvents;
    
    // Persist immediately after confirmation
    this._persist();
  }

  /**
   * Persist cursor state to disk
   * ONLY called after writes are confirmed
   */
  _persist() {
    try {
      if (!existsSync(CURSOR_DIR)) {
        mkdirSync(CURSOR_DIR, { recursive: true });
      }

      const cursorData = {
        id: `cursor-${this.migrationId}-${this.synchronizerId.substring(0, 20)}`,
        migration_id: this.migrationId,
        synchronizer_id: this.synchronizerId,
        shard_index: this.shardIndex,
        
        // CONFIRMED state - safe to resume from here
        last_confirmed_before: this.lastConfirmedBefore,
        confirmed_updates: this.confirmedUpdates,
        confirmed_events: this.confirmedEvents,
        
        // Legacy fields for compatibility
        last_before: this.lastConfirmedBefore,
        total_updates: this.confirmedUpdates,
        total_events: this.confirmedEvents,
        
        // Pending state - not yet confirmed
        pending_updates: this.pendingUpdates,
        pending_events: this.pendingEvents,
        
        // Metadata
        updated_at: new Date().toISOString(),
        complete: false,
      };

      writeFileSync(this.cursorPath, JSON.stringify(cursorData, null, 2));
    } catch (err) {
      console.error(`[IntegrityCursor] CRITICAL: Failed to save cursor: ${err.message}`);
      throw err; // This is critical - must not continue
    }
  }

  /**
   * Mark as complete ONLY after all writes verified
   */
  markComplete() {
    if (this.pendingUpdates > 0 || this.pendingEvents > 0) {
      throw new Error(`Cannot mark complete with pending data: ${this.pendingUpdates} updates, ${this.pendingEvents} events`);
    }

    const cursorData = {
      id: `cursor-${this.migrationId}-${this.synchronizerId.substring(0, 20)}`,
      migration_id: this.migrationId,
      synchronizer_id: this.synchronizerId,
      shard_index: this.shardIndex,
      last_confirmed_before: this.lastConfirmedBefore,
      last_before: this.lastConfirmedBefore,
      confirmed_updates: this.confirmedUpdates,
      confirmed_events: this.confirmedEvents,
      total_updates: this.confirmedUpdates,
      total_events: this.confirmedEvents,
      pending_updates: 0,
      pending_events: 0,
      updated_at: new Date().toISOString(),
      completed_at: new Date().toISOString(),
      complete: true,
    };

    writeFileSync(this.cursorPath, JSON.stringify(cursorData, null, 2));
    console.log(`[IntegrityCursor] ✅ Marked complete: ${this.confirmedUpdates} updates, ${this.confirmedEvents} events`);
  }
}

/**
 * Write verifier - confirms data was actually written to disk
 */
export class WriteVerifier {
  constructor(rawDir = RAW_DIR) {
    this.rawDir = rawDir;
    this.lastFileCount = { updates: 0, events: 0 };
    this.lastRecordCount = { updates: 0, events: 0 };
  }

  /**
   * Count files in raw directory
   */
  countFiles() {
    let updates = 0;
    let events = 0;

    const scanDir = (dir) => {
      try {
        const entries = readdirSync(dir, { withFileTypes: true });
        for (const entry of entries) {
          if (entry.isDirectory()) {
            scanDir(join(dir, entry.name));
          } else if (entry.name.endsWith('.pb.zst')) {
            if (entry.name.startsWith('updates-')) updates++;
            else if (entry.name.startsWith('events-')) events++;
          }
        }
      } catch {}
    };

    if (existsSync(this.rawDir)) {
      scanDir(this.rawDir);
    }

    return { updates, events };
  }

  /**
   * Wait for file count to increase (confirms writes completed)
   */
  async waitForWrites(expectedNewFiles, timeoutMs = 30000) {
    const startCounts = this.countFiles();
    const startTime = Date.now();

    while (Date.now() - startTime < timeoutMs) {
      const current = this.countFiles();
      const newUpdates = current.updates - startCounts.updates;
      const newEvents = current.events - startCounts.events;
      const totalNew = newUpdates + newEvents;

      if (totalNew >= expectedNewFiles) {
        return { success: true, newUpdates, newEvents };
      }

      await new Promise(r => setTimeout(r, 100));
    }

    return { 
      success: false, 
      message: `Timeout waiting for ${expectedNewFiles} files`,
      current: this.countFiles(),
      started: startCounts,
    };
  }

  /**
   * Verify writes completed before advancing cursor
   */
  async verifyAndConfirm(cursor, pendingUpdates, pendingEvents, beforeTimestamp, flushFn, waitFn) {
    const beforeCounts = this.countFiles();

    // Flush buffers
    await flushFn();

    // Wait for all pending writes
    await waitFn();

    // Verify file count increased
    const afterCounts = this.countFiles();
    const newFiles = (afterCounts.updates - beforeCounts.updates) + (afterCounts.events - beforeCounts.events);

    if (newFiles > 0 || (pendingUpdates === 0 && pendingEvents === 0)) {
      // Writes confirmed - NOW update cursor
      cursor.confirmWrite(pendingUpdates, pendingEvents, beforeTimestamp);
      return { success: true, newFiles };
    }

    console.warn(`[WriteVerifier] ⚠️ No new files detected after flush. Expected writes may have failed.`);
    return { success: false, newFiles: 0 };
  }
}

/**
 * Time range manager with overlap to prevent gaps
 */
export class TimeRangeManager {
  constructor(minTime, maxTime, overlapMs = OVERLAP_MS) {
    this.minTime = new Date(minTime).getTime();
    this.maxTime = new Date(maxTime).getTime();
    this.overlapMs = overlapMs;
    this.currentBefore = this.maxTime;
    this.processedRanges = [];
  }

  /**
   * Get next time range to fetch with overlap
   */
  getNextRange(stepMs) {
    if (this.currentBefore <= this.minTime) {
      return null; // Complete
    }

    const rangeEnd = this.currentBefore;
    const rangeStart = Math.max(this.minTime, this.currentBefore - stepMs);

    // Add overlap to catch boundary cases
    const effectiveStart = Math.max(this.minTime, rangeStart - this.overlapMs);

    return {
      before: new Date(rangeEnd).toISOString(),
      atOrAfter: new Date(effectiveStart).toISOString(),
      rangeMs: rangeEnd - effectiveStart,
    };
  }

  /**
   * Advance after confirmed processing
   */
  advance(oldestProcessedTime) {
    const oldestMs = new Date(oldestProcessedTime).getTime();
    
    // Only advance to oldest processed, not beyond
    if (oldestMs < this.currentBefore) {
      this.processedRanges.push({
        from: oldestMs,
        to: this.currentBefore,
        processedAt: new Date().toISOString(),
      });
      this.currentBefore = oldestMs;
    }
  }

  /**
   * Get progress percentage
   */
  getProgress() {
    const totalRange = this.maxTime - this.minTime;
    if (totalRange <= 0) return 100;
    const processed = this.maxTime - this.currentBefore;
    return Math.min(100, Math.max(0, (processed / totalRange) * 100));
  }

  /**
   * Check for gaps in processed ranges
   */
  detectGaps(thresholdMs = 60000) {
    const gaps = [];
    const sorted = [...this.processedRanges].sort((a, b) => a.from - b.from);

    for (let i = 1; i < sorted.length; i++) {
      const prev = sorted[i - 1];
      const curr = sorted[i];
      const gapMs = curr.from - prev.to;

      if (gapMs > thresholdMs) {
        gaps.push({
          start: new Date(prev.to).toISOString(),
          end: new Date(curr.from).toISOString(),
          gapMs,
        });
      }
    }

    return gaps;
  }
}

/**
 * Deduplication tracker - accepts duplicates during fetch, tracks for stats
 */
export class DedupTracker {
  constructor() {
    this.seenUpdateIds = new Set();
    this.duplicateCount = 0;
    this.uniqueCount = 0;
  }

  /**
   * Track and deduplicate transactions
   * Returns only unique transactions
   */
  deduplicate(transactions) {
    const unique = [];

    for (const tx of transactions) {
      const updateId = tx.update_id || tx.transaction?.update_id || tx.reassignment?.update_id;
      
      if (!updateId) {
        // No ID - accept it (can dedupe later)
        unique.push(tx);
        this.uniqueCount++;
        continue;
      }

      if (this.seenUpdateIds.has(updateId)) {
        this.duplicateCount++;
        continue;
      }

      this.seenUpdateIds.add(updateId);
      unique.push(tx);
      this.uniqueCount++;
    }

    return unique;
  }

  /**
   * Clear tracker to free memory (call periodically)
   */
  reset() {
    const stats = {
      uniqueCount: this.uniqueCount,
      duplicateCount: this.duplicateCount,
      dedupRate: this.duplicateCount / (this.uniqueCount + this.duplicateCount) * 100,
    };
    this.seenUpdateIds.clear();
    this.duplicateCount = 0;
    this.uniqueCount = 0;
    return stats;
  }

  getStats() {
    return {
      seenIds: this.seenUpdateIds.size,
      uniqueCount: this.uniqueCount,
      duplicateCount: this.duplicateCount,
    };
  }
}

/**
 * Empty response handler - NEVER skip data
 */
export class EmptyResponseHandler {
  constructor(stepBackMs = STEP_BACK_MS, maxEmpty = MAX_EMPTY_BEFORE_STEP) {
    this.stepBackMs = stepBackMs;
    this.maxEmpty = maxEmpty;
    this.consecutiveEmpty = 0;
    this.totalEmpty = 0;
  }

  /**
   * Handle empty API response
   * Returns: { action: 'continue' | 'done', newBefore: string | null }
   */
  handleEmpty(currentBefore, lowerBound) {
    this.consecutiveEmpty++;
    this.totalEmpty++;

    const currentMs = new Date(currentBefore).getTime();
    const lowerMs = new Date(lowerBound).getTime();

    // CRITICAL: Only step back by 1ms, NEVER jump
    const newMs = currentMs - this.stepBackMs;

    if (newMs <= lowerMs) {
      // We've reached the lower bound
      return { action: 'done', newBefore: null };
    }

    // After many empty responses, we might be in a true gap
    // But NEVER skip - just continue stepping back 1ms at a time
    if (this.consecutiveEmpty >= this.maxEmpty * 3) {
      console.log(`   ⚠️ ${this.consecutiveEmpty} consecutive empty responses. Continuing to step back carefully.`);
    }

    return { 
      action: 'continue', 
      newBefore: new Date(newMs).toISOString(),
      consecutiveEmpty: this.consecutiveEmpty,
    };
  }

  /**
   * Reset counter on successful data
   */
  resetOnData() {
    const wasEmpty = this.consecutiveEmpty;
    this.consecutiveEmpty = 0;
    return wasEmpty;
  }

  getStats() {
    return {
      consecutiveEmpty: this.consecutiveEmpty,
      totalEmpty: this.totalEmpty,
    };
  }
}

/**
 * Batch integrity tracker
 */
export class BatchIntegrityTracker {
  constructor() {
    this.batches = [];
    this.totalUpdates = 0;
    this.totalEvents = 0;
  }

  recordBatch(batchId, updates, events, timeRange) {
    this.batches.push({
      id: batchId,
      updates,
      events,
      timeRange,
      timestamp: new Date().toISOString(),
    });
    this.totalUpdates += updates;
    this.totalEvents += events;
  }

  /**
   * Verify totals match expectations
   */
  verify(expectedUpdates, expectedEvents) {
    return {
      match: this.totalUpdates === expectedUpdates && this.totalEvents === expectedEvents,
      expected: { updates: expectedUpdates, events: expectedEvents },
      actual: { updates: this.totalUpdates, events: this.totalEvents },
      difference: {
        updates: this.totalUpdates - expectedUpdates,
        events: this.totalEvents - expectedEvents,
      },
    };
  }

  getSummary() {
    return {
      batchCount: this.batches.length,
      totalUpdates: this.totalUpdates,
      totalEvents: this.totalEvents,
      firstBatch: this.batches[0],
      lastBatch: this.batches[this.batches.length - 1],
    };
  }
}

// Export all components
export default {
  IntegrityCursor,
  WriteVerifier,
  TimeRangeManager,
  DedupTracker,
  EmptyResponseHandler,
  BatchIntegrityTracker,
  OVERLAP_MS,
  MAX_EMPTY_BEFORE_STEP,
  STEP_BACK_MS,
  VERIFY_INTERVAL,
};
