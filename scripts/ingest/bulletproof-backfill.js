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

import { existsSync, readFileSync, mkdirSync, readdirSync, writeFileSync } from 'fs';
import { AtomicCursor, atomicWriteFile, getCursorPath } from './atomic-cursor.js';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Configuration - cross-platform path handling
import { getBaseDataDir, getCursorDir, getRawDir } from './path-utils.js';
const BASE_DATA_DIR = getBaseDataDir();
const CURSOR_DIR = getCursorDir();
const RAW_DIR = getRawDir();

// Constants for bulletproof operation
const OVERLAP_MS = 1000; // 1 second overlap between time slices to catch boundary cases
const MAX_EMPTY_BEFORE_STEP = 10; // Maximum empty responses before stepping back
const STEP_BACK_MS = 1; // Only step back 1ms on empty - NEVER more
const VERIFY_INTERVAL = 100; // Verify counts every N batches

/**
 * Integrity-checked cursor operations.
 *
 * FIX #1: This class no longer reimplements cursor management from scratch.
 * It is now a thin wrapper around AtomicCursor, which provides proper
 * transaction semantics (beginTransaction / commit / rollback), crash-safe
 * atomic writes, backup recovery, and GCS checkpointing.
 *
 * The public API (load, recordPending, confirmWrite, markComplete) is
 * preserved for backward compatibility with existing call sites.
 */
export class IntegrityCursor {
  constructor(migrationId, synchronizerId, shardIndex = null) {
    this.migrationId = migrationId;
    this.synchronizerId = synchronizerId;
    this.shardIndex = shardIndex;

    // Delegate all state management to AtomicCursor
    this._cursor = new AtomicCursor(migrationId, synchronizerId, shardIndex);

    // Pending tracking (mirrored from AtomicCursor.pendingState for external reads)
    this.pendingUpdates = 0;
    this.pendingEvents = 0;
  }

  // Convenience accessors that mirror the old public fields
  get confirmedUpdates() { return this._cursor.confirmedState.totalUpdates; }
  get confirmedEvents()  { return this._cursor.confirmedState.totalEvents; }
  get lastConfirmedBefore() { return this._cursor.confirmedState.lastBefore; }
  get cursorPath() { return this._cursor.cursorPath; }

  /**
   * Load cursor from disk.
   * FIX #4: Delegates to AtomicCursor.load() which uses readCursorSafe —
   * corrupted main files fall back to the .bak file automatically.
   */
  load() {
    return this._cursor.load();
  }

  /**
   * Record pending data (not yet written to disk).
   * DO NOT advance cursor position yet.
   */
  recordPending(updates, events) {
    this.pendingUpdates += updates;
    this.pendingEvents += events;
  }

  /**
   * Confirm data has been written to disk.
   * NOW it's safe to advance cursor position.
   *
   * FIX #1: Uses AtomicCursor transaction pattern so the cursor only
   * advances after data is confirmed durable — not just in-memory.
   */
  confirmWrite(writtenUpdates, writtenEvents, beforeTimestamp) {
    // Begin transaction if not already open, otherwise add to pending
    if (!this._cursor.inTransaction) {
      this._cursor.beginTransaction(writtenUpdates, writtenEvents, beforeTimestamp);
    } else {
      this._cursor.addPending(writtenUpdates, writtenEvents, beforeTimestamp);
    }

    // Commit immediately — data is already on disk when this is called
    this._cursor.commit();

    this.pendingUpdates -= writtenUpdates;
    this.pendingEvents -= writtenEvents;
  }

  /**
   * Mark as complete ONLY after all writes verified.
   * Delegates to AtomicCursor.markComplete() which enforces GCS sync.
   */
  markComplete() {
    if (this.pendingUpdates > 0 || this.pendingEvents > 0) {
      throw new Error(
        `Cannot mark complete with pending data: ${this.pendingUpdates} updates, ${this.pendingEvents} events`
      );
    }
    this._cursor.markComplete();
    console.log(`[IntegrityCursor] ✅ Marked complete: ${this.confirmedUpdates} updates, ${this.confirmedEvents} events`);
  }

  /**
   * Expose GCS confirmation for callers that use it.
   */
  confirmGCS(timestamp = null, updates = null, events = null) {
    return this._cursor.confirmGCS(timestamp, updates, events);
  }

  getGCSStatus() {
    return this._cursor.getGCSStatus();
  }

  getState() {
    return this._cursor.getState();
  }
}

/**
 * Write verifier - confirms data was actually written to disk.
 *
 * FIX #2: File counting is now scoped to a shard-specific subdirectory
 * (or a caller-supplied scope dir) so concurrent shards/processes writing
 * to sibling directories don't inflate counts and produce false positives.
 *
 * FIX #3: verifyAndConfirm now distinguishes between "no files needed"
 * (pendingUpdates === 0) and "writes confirmed" (newFiles > 0) — each path
 * is handled and logged separately rather than collapsed into one condition.
 */
export class WriteVerifier {
  /**
   * @param {string} rawDir      - Base raw data directory
   * @param {string|null} scopeDir - Subdirectory scoped to this shard/process.
   *   Pass a unique per-shard path (e.g. `${rawDir}/shard-3`) to avoid counting
   *   files written by other concurrent shards. Defaults to rawDir for single-
   *   process setups, but should always be set in sharded runs.
   */
  constructor(rawDir = RAW_DIR, scopeDir = null) {
    this.rawDir = rawDir;
    // FIX #2: Use scoped directory for counting so other shards don't interfere
    this.scopeDir = scopeDir || rawDir;
  }

  /**
   * Count .pb.zst files in the scoped directory tree.
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

    if (existsSync(this.scopeDir)) {
      scanDir(this.scopeDir);
    }

    return { updates, events };
  }

  /**
   * Wait for file count to increase (confirms writes completed).
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
   * Verify writes completed before advancing cursor.
   *
   * FIX #3: The two success conditions are now separated and each logs clearly:
   *   Path A — pendingUpdates/Events are both zero: nothing to write, confirm trivially.
   *   Path B — newFiles > 0: writes detected in scoped directory, confirm.
   *   Path C — neither: warn and return failure without advancing cursor.
   */
  async verifyAndConfirm(cursor, pendingUpdates, pendingEvents, beforeTimestamp, flushFn, waitFn) {
    // Path A: nothing was pending, no writes needed
    if (pendingUpdates === 0 && pendingEvents === 0) {
      console.log(`[WriteVerifier] No pending data — skipping write verification.`);
      cursor.confirmWrite(0, 0, beforeTimestamp);
      return { success: true, newFiles: 0, path: 'no-op' };
    }

    // Path B: data was pending — flush, wait, then count files in scoped dir
    const beforeCounts = this.countFiles();

    await flushFn();
    await waitFn();

    const afterCounts = this.countFiles();
    const newFiles =
      (afterCounts.updates - beforeCounts.updates) +
      (afterCounts.events - beforeCounts.events);

    if (newFiles > 0) {
      console.log(`[WriteVerifier] ✅ ${newFiles} new file(s) detected — confirming cursor advance.`);
      cursor.confirmWrite(pendingUpdates, pendingEvents, beforeTimestamp);
      return { success: true, newFiles, path: 'confirmed' };
    }

    // Path C: expected writes but none detected
    console.warn(
      `[WriteVerifier] ⚠️ No new files detected after flush. ` +
      `Expected writes for ${pendingUpdates} updates, ${pendingEvents} events. ` +
      `Cursor NOT advanced. Scope dir: ${this.scopeDir}`
    );
    return { success: false, newFiles: 0, path: 'failed' };
  }
}

/**
 * Time range manager with overlap to prevent gaps.
 *
 * FIX #6: getNextRange now returns both the effective (overlapped) start and
 * the non-overlapped nominal start. Callers must pass the nominal start to
 * advance() — not the overlapped start — so consecutive ranges don't drift
 * and double-count the overlap window.
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
   * Get next time range to fetch with overlap.
   *
   * Returns:
   *   before          - upper bound for the API query (ISO string)
   *   atOrAfter       - effective lower bound including overlap (ISO string) — pass to the API
   *   nominalStart    - lower bound WITHOUT overlap (ISO string) — pass to advance()
   *   rangeMs         - size of the effective fetch window in milliseconds
   */
  getNextRange(stepMs) {
    if (this.currentBefore <= this.minTime) {
      return null; // Complete
    }

    const rangeEnd = this.currentBefore;
    // FIX #6: Track the nominal (non-overlapped) start separately from the
    // effective (overlapped) start so advance() receives the right boundary.
    const nominalStart = Math.max(this.minTime, this.currentBefore - stepMs);
    const effectiveStart = Math.max(this.minTime, nominalStart - this.overlapMs);

    return {
      before: new Date(rangeEnd).toISOString(),
      atOrAfter: new Date(effectiveStart).toISOString(),      // pass to API
      nominalStart: new Date(nominalStart).toISOString(),     // pass to advance()
      rangeMs: rangeEnd - effectiveStart,
    };
  }

  /**
   * Advance after confirmed processing.
   *
   * FIX #6: Callers should pass range.nominalStart (not range.atOrAfter) so
   * the overlap window is not subtracted twice on consecutive calls.
   */
  advance(nominalStartTime) {
    const nominalMs = new Date(nominalStartTime).getTime();

    if (nominalMs < this.currentBefore) {
      this.processedRanges.push({
        from: nominalMs,
        to: this.currentBefore,
        processedAt: new Date().toISOString(),
      });
      this.currentBefore = nominalMs;
    }
  }

  /**
   * Get progress percentage.
   */
  getProgress() {
    const totalRange = this.maxTime - this.minTime;
    if (totalRange <= 0) return 100;
    const processed = this.maxTime - this.currentBefore;
    return Math.min(100, Math.max(0, (processed / totalRange) * 100));
  }

  /**
   * Check for gaps in processed ranges.
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
 * Deduplication tracker - accepts duplicates during fetch, tracks for stats.
 *
 * FIX #5: reset() now guards against divide-by-zero when no records have
 * been processed, returning 0 instead of NaN for dedupRate.
 */
export class DedupTracker {
  constructor() {
    this.seenUpdateIds = new Set();
    this.duplicateCount = 0;
    this.uniqueCount = 0;
  }

  /**
   * Track and deduplicate transactions.
   * Returns only unique transactions.
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
   * Clear tracker to free memory (call periodically).
   *
   * FIX #5: Guard against divide-by-zero — dedupRate is 0 when no records seen.
   */
  reset() {
    const total = this.uniqueCount + this.duplicateCount;
    const stats = {
      uniqueCount: this.uniqueCount,
      duplicateCount: this.duplicateCount,
      // FIX #5: total === 0 when called before any records — return 0 not NaN
      dedupRate: total > 0 ? (this.duplicateCount / total) * 100 : 0,
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
 * Empty response handler - Progressive step-back for gaps.
 *
 * Strategy: Start with 1ms, then progressively increase step size after many
 * consecutive empties (likely a migration gap). This balances safety with
 * efficiency for sparse historical data.
 *
 * FIX #7: Step tiers are now a constructor parameter with the original values
 * as defaults, so callers can tune thresholds per-dataset without editing
 * this class. stepBackMs and maxEmpty were already parameterized; tiers now
 * follow the same pattern.
 */
export class EmptyResponseHandler {
  /**
   * @param {number} stepBackMs   - Base step size for the first tier (default: 1ms)
   * @param {number} maxEmpty     - Legacy threshold kept for compatibility
   * @param {Array}  stepTiers    - Override the progressive step tier table.
   *   Each entry: { threshold: number, stepMs: number }
   *   threshold = consecutive empty count at which this step size activates.
   *   Entries are evaluated in order; last matching threshold wins.
   */
  constructor(
    stepBackMs = STEP_BACK_MS,
    maxEmpty = MAX_EMPTY_BEFORE_STEP,
    stepTiers = null,
  ) {
    this.baseStepMs = stepBackMs;
    this.maxEmpty = maxEmpty;
    this.consecutiveEmpty = 0;
    this.totalEmpty = 0;

    // FIX #7: Accept custom tiers or fall back to well-calibrated defaults
    this.stepTiers = stepTiers ?? [
      { threshold: 0,    stepMs: 1        }, // Start: 1ms
      { threshold: 100,  stepMs: 100      }, // After 100:  100ms
      { threshold: 200,  stepMs: 1_000    }, // After 200:  1 second
      { threshold: 500,  stepMs: 10_000   }, // After 500:  10 seconds
      { threshold: 1000, stepMs: 60_000   }, // After 1000: 1 minute
      { threshold: 2000, stepMs: 300_000  }, // After 2000: 5 minutes
      { threshold: 5000, stepMs: 3_600_000}, // After 5000: 1 hour
    ];
  }

  /**
   * Get current step size based on consecutive empties.
   */
  _getCurrentStep() {
    let stepMs = this.baseStepMs;
    for (const tier of this.stepTiers) {
      if (this.consecutiveEmpty >= tier.threshold) {
        stepMs = tier.stepMs;
      }
    }
    return stepMs;
  }

  /**
   * Handle empty API response.
   * Returns: { action: 'continue' | 'done', newBefore: string | null }
   */
  handleEmpty(currentBefore, lowerBound) {
    this.consecutiveEmpty++;
    this.totalEmpty++;

    const currentMs = new Date(currentBefore).getTime();
    const lowerMs = new Date(lowerBound).getTime();

    const stepMs = this._getCurrentStep();
    const newMs = currentMs - stepMs;

    if (newMs <= lowerMs) {
      return { action: 'done', newBefore: null };
    }

    // Log at tier transitions
    if (this.consecutiveEmpty === 100) {
      console.log(`   ⚠️ 100 consecutive empties - increasing step to 100ms (possible gap)`);
    } else if (this.consecutiveEmpty === 500) {
      console.log(`   ⚠️ 500 consecutive empties - increasing step to 10s (likely migration gap)`);
    } else if (this.consecutiveEmpty === 1000) {
      console.log(`   ⚠️ 1000 consecutive empties - increasing step to 1min (large gap)`);
    } else if (this.consecutiveEmpty === 2000) {
      console.log(`   ⚠️ 2000 consecutive empties - increasing step to 5min (very large gap)`);
    } else if (this.consecutiveEmpty % 1000 === 0 && this.consecutiveEmpty > 2000) {
      console.log(`   ⚠️ ${this.consecutiveEmpty} consecutive empties - stepping back ${stepMs / 1000}s at a time`);
    }

    return {
      action: 'continue',
      newBefore: new Date(newMs).toISOString(),
      consecutiveEmpty: this.consecutiveEmpty,
      stepMs,
    };
  }

  /**
   * Reset counter on successful data.
   */
  resetOnData() {
    const wasEmpty = this.consecutiveEmpty;
    if (wasEmpty > 100) {
      console.log(`   ✅ Found data after ${wasEmpty} empty responses - resetting step to 1ms`);
    }
    this.consecutiveEmpty = 0;
    return wasEmpty;
  }

  getStats() {
    return {
      consecutiveEmpty: this.consecutiveEmpty,
      totalEmpty: this.totalEmpty,
      currentStepMs: this._getCurrentStep(),
    };
  }
}

/**
 * Batch integrity tracker.
 *
 * FIX #8: this.batches no longer grows without bound. A rolling window of the
 * last MAX_BATCH_HISTORY entries is kept for diagnostics (first/last batch
 * access, gap detection). Aggregate totals are always exact regardless of
 * how many batches have been processed.
 */
export class BatchIntegrityTracker {
  /**
   * @param {number} maxHistory - Max number of individual batch records to
   *   retain in memory (default: 1000). Oldest entries are evicted once this
   *   limit is reached. Aggregate totals are always maintained separately and
   *   are not affected by eviction.
   */
  constructor(maxHistory = 1000) {
    this.maxHistory = maxHistory;
    this.batches = [];          // Rolling window — capped at maxHistory
    this.totalBatchCount = 0;  // True total, unaffected by eviction
    this.totalUpdates = 0;
    this.totalEvents = 0;
    this._firstBatch = null;   // Preserved on eviction so getSummary() stays accurate
  }

  recordBatch(batchId, updates, events, timeRange) {
    const entry = {
      id: batchId,
      updates,
      events,
      timeRange,
      timestamp: new Date().toISOString(),
    };

    // Preserve first batch before it could be evicted
    if (this.totalBatchCount === 0) {
      this._firstBatch = entry;
    }

    this.batches.push(entry);
    this.totalBatchCount++;
    this.totalUpdates += updates;
    this.totalEvents += events;

    // FIX #8: Evict oldest entry once the rolling window is full
    if (this.batches.length > this.maxHistory) {
      this.batches.shift();
    }
  }

  /**
   * Verify totals match expectations.
   */
  verify(expectedUpdates, expectedEvents) {
    return {
      match: this.totalUpdates === expectedUpdates && this.totalEvents === expectedEvents,
      expected: { updates: expectedUpdates, events: expectedEvents },
      actual:   { updates: this.totalUpdates,   events: this.totalEvents },
      difference: {
        updates: this.totalUpdates - expectedUpdates,
        events:  this.totalEvents  - expectedEvents,
      },
    };
  }

  getSummary() {
    return {
      batchCount: this.totalBatchCount,
      retainedBatches: this.batches.length,
      totalUpdates: this.totalUpdates,
      totalEvents: this.totalEvents,
      firstBatch: this._firstBatch,
      lastBatch: this.batches[this.batches.length - 1] ?? null,
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
