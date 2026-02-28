/**
 * Behavioral tests for data integrity fixes.
 *
 * Tests isolated logic patterns introduced by the 10 data integrity fixes
 * without importing the full pipeline modules (which have heavy Node deps).
 *
 * - LRU eviction logic (Fix #18)
 * - Per-partition requeue semantics (Fix #10/write-parquet)
 * - Cursor hold on errors (Fix #14)
 * - Max record_time cursor precision (Fix #13)
 * - Per-tx error isolation (Fix #9/#16/#17)
 */

import { describe, it, expect } from 'vitest';

// ─────────────────────────────────────────────────────────────
// LRU eviction logic (Fix #18)
// ─────────────────────────────────────────────────────────────
describe('LRU eviction for seenUpdateIds', () => {
  /**
   * Replica of the LRU eviction pattern from fetchTimeSliceStreaming.
   * When size exceeds threshold, keep the newest half.
   */
  function applyLRUEviction(seenSet, threshold = 500000, keepCount = 250000) {
    if (seenSet.size > threshold) {
      const entries = [...seenSet];
      seenSet.clear();
      for (let i = entries.length - keepCount; i < entries.length; i++) {
        seenSet.add(entries[i]);
      }
    }
  }

  it('should not evict when below threshold', () => {
    const seen = new Set(['a', 'b', 'c']);
    applyLRUEviction(seen, 5, 2);
    expect(seen.size).toBe(3);
  });

  it('should keep newest entries when above threshold', () => {
    const seen = new Set();
    for (let i = 0; i < 10; i++) seen.add(`id-${i}`);

    applyLRUEviction(seen, 5, 3);

    expect(seen.size).toBe(3);
    // Should keep the NEWEST 3 (last added)
    expect(seen.has('id-7')).toBe(true);
    expect(seen.has('id-8')).toBe(true);
    expect(seen.has('id-9')).toBe(true);
    // Should NOT have the oldest
    expect(seen.has('id-0')).toBe(false);
    expect(seen.has('id-4')).toBe(false);
  });

  it('should evict exactly oldest half at default thresholds', () => {
    const seen = new Set();
    // Simulate 500001 entries (just above threshold)
    const totalEntries = 600;
    const threshold = 500;
    const keepCount = 250;
    for (let i = 0; i < totalEntries; i++) seen.add(`upd-${i}`);

    applyLRUEviction(seen, threshold, keepCount);

    expect(seen.size).toBe(keepCount);
    // Newest should remain
    expect(seen.has(`upd-${totalEntries - 1}`)).toBe(true);
    expect(seen.has(`upd-${totalEntries - keepCount}`)).toBe(true);
    // Oldest should be gone
    expect(seen.has('upd-0')).toBe(false);
    expect(seen.has(`upd-${totalEntries - keepCount - 1}`)).toBe(false);
  });
});

// ─────────────────────────────────────────────────────────────
// Per-partition requeue (Fix #10 in write-parquet)
// ─────────────────────────────────────────────────────────────
describe('Per-partition failure requeue', () => {
  /**
   * Simulates the Promise.allSettled per-partition requeue pattern.
   */
  async function simulateFlush(partitionGroups, failPartitions = new Set()) {
    const writePromises = Object.entries(partitionGroups).map(([partition, records]) => {
      if (failPartitions.has(partition)) {
        return Promise.reject(new Error(`Write failed for ${partition}`));
      }
      return Promise.resolve({ partition, count: records.length });
    });

    const settled = await Promise.allSettled(writePromises);
    const results = [];
    const failedRecords = [];
    const entries = Object.entries(partitionGroups);

    for (let i = 0; i < settled.length; i++) {
      if (settled[i].status === 'fulfilled') {
        results.push(settled[i].value);
      } else {
        const [, records] = entries[i];
        failedRecords.push(...records);
      }
    }

    return { results, failedRecords };
  }

  it('should re-queue only records from failed partitions', async () => {
    const groups = {
      'year=2024/month=1/day=15': [{ id: 1 }, { id: 2 }],
      'year=2024/month=1/day=16': [{ id: 3 }],
      'year=2024/month=1/day=17': [{ id: 4 }, { id: 5 }],
    };

    const { results, failedRecords } = await simulateFlush(
      groups,
      new Set(['year=2024/month=1/day=16'])
    );

    // Only the failed partition's records should be re-queued
    expect(failedRecords).toHaveLength(1);
    expect(failedRecords[0].id).toBe(3);
    // Successful partitions should be in results
    expect(results).toHaveLength(2);
  });

  it('should not re-queue any records when all partitions succeed', async () => {
    const groups = {
      'partition-a': [{ id: 1 }],
      'partition-b': [{ id: 2 }],
    };

    const { results, failedRecords } = await simulateFlush(groups);

    expect(failedRecords).toHaveLength(0);
    expect(results).toHaveLength(2);
  });

  it('should re-queue all records when all partitions fail', async () => {
    const groups = {
      'partition-a': [{ id: 1 }],
      'partition-b': [{ id: 2 }, { id: 3 }],
    };

    const { results, failedRecords } = await simulateFlush(
      groups,
      new Set(['partition-a', 'partition-b'])
    );

    expect(failedRecords).toHaveLength(3);
    expect(results).toHaveLength(0);
  });
});

// ─────────────────────────────────────────────────────────────
// Cursor hold on errors (Fix #14)
// ─────────────────────────────────────────────────────────────
describe('Cursor hold on batch errors', () => {
  /**
   * Simulates processUpdates returning errors count.
   */
  function simulateProcessUpdates(items, failIndices = new Set()) {
    const updates = [];
    const events = [];
    const errors = [];

    for (let i = 0; i < items.length; i++) {
      if (failIndices.has(i)) {
        errors.push({ tx_id: items[i].id, error: 'decode failed' });
      } else {
        updates.push({ update_id: items[i].id });
        events.push({ event_id: `${items[i].id}:0` });
      }
    }

    return { updates: updates.length, events: events.length, errors: errors.length };
  }

  it('should return errors count when some items fail', () => {
    const items = [{ id: 'tx-1' }, { id: 'tx-2' }, { id: 'tx-3' }];
    const result = simulateProcessUpdates(items, new Set([1]));

    expect(result.updates).toBe(2);
    expect(result.events).toBe(2);
    expect(result.errors).toBe(1);
  });

  it('should hold cursor when errors > 0', () => {
    const items = [{ id: 'tx-1' }, { id: 'tx-bad' }];
    const result = simulateProcessUpdates(items, new Set([1]));

    let cursorAdvanced = false;
    // Simulates the cursor_hold_on_errors logic
    if (result.errors > 0) {
      // Do NOT advance cursor
      cursorAdvanced = false;
    } else {
      cursorAdvanced = true;
    }

    expect(cursorAdvanced).toBe(false);
  });

  it('should advance cursor when errors = 0', () => {
    const items = [{ id: 'tx-1' }, { id: 'tx-2' }];
    const result = simulateProcessUpdates(items);

    let cursorAdvanced = false;
    if (result.errors > 0) {
      cursorAdvanced = false;
    } else {
      cursorAdvanced = true;
    }

    expect(cursorAdvanced).toBe(true);
  });
});

// ─────────────────────────────────────────────────────────────
// Max record_time cursor (Fix #13)
// ─────────────────────────────────────────────────────────────
describe('Max record_time cursor precision', () => {
  /**
   * Simulates the max(record_time) pattern from fetchUpdates.
   */
  function findMaxRecordTime(transactions) {
    let maxRecordTime = null;
    for (const tx of transactions) {
      if (tx.record_time && (!maxRecordTime || tx.record_time > maxRecordTime)) {
        maxRecordTime = tx.record_time;
      }
    }
    return maxRecordTime;
  }

  it('should find the max record_time regardless of array order', () => {
    const txs = [
      { record_time: '2024-01-15T10:00:00Z', update_id: 'tx-1' },
      { record_time: '2024-01-15T12:00:00Z', update_id: 'tx-2' }, // highest
      { record_time: '2024-01-15T08:00:00Z', update_id: 'tx-3' },
    ];

    const max = findMaxRecordTime(txs);
    expect(max).toBe('2024-01-15T12:00:00Z');
  });

  it('should NOT just use the last element', () => {
    const txs = [
      { record_time: '2024-01-15T12:00:00Z', update_id: 'tx-1' }, // highest
      { record_time: '2024-01-15T08:00:00Z', update_id: 'tx-2' }, // last but not highest
    ];

    const max = findMaxRecordTime(txs);
    const lastElement = txs[txs.length - 1].record_time;

    // Max should be from tx-1, not the last element tx-2
    expect(max).toBe('2024-01-15T12:00:00Z');
    expect(max).not.toBe(lastElement);
  });

  it('should handle single transaction', () => {
    const txs = [{ record_time: '2024-01-15T10:00:00Z' }];
    expect(findMaxRecordTime(txs)).toBe('2024-01-15T10:00:00Z');
  });

  it('should handle empty array', () => {
    expect(findMaxRecordTime([])).toBeNull();
  });
});

// ─────────────────────────────────────────────────────────────
// Per-tx error isolation (Fix #9/#16/#17)
// ─────────────────────────────────────────────────────────────
describe('Per-tx error isolation', () => {
  /**
   * Simulates the per-tx try/catch pattern.
   */
  function processWithIsolation(items, decoder) {
    const updates = [];
    const events = [];
    const errors = [];

    for (const item of items) {
      try {
        const result = decoder(item);
        if (result.update) updates.push(result.update);
        if (result.events) events.push(...result.events);
      } catch (err) {
        errors.push({ tx_id: item.id, error: err.message });
      }
    }

    return { updates, events, errors };
  }

  it('should continue processing after one item fails', () => {
    const items = [
      { id: 'tx-1', valid: true },
      { id: 'tx-bad', valid: false },
      { id: 'tx-3', valid: true },
    ];

    const result = processWithIsolation(items, (item) => {
      if (!item.valid) throw new Error('decode failed');
      return { update: { id: item.id }, events: [{ id: `${item.id}:0` }] };
    });

    expect(result.updates).toHaveLength(2);
    expect(result.events).toHaveLength(2);
    expect(result.errors).toHaveLength(1);
    expect(result.errors[0].tx_id).toBe('tx-bad');
  });

  it('should collect all errors without aborting', () => {
    const items = [
      { id: 'tx-1', valid: false },
      { id: 'tx-2', valid: false },
      { id: 'tx-3', valid: true },
    ];

    const result = processWithIsolation(items, (item) => {
      if (!item.valid) throw new Error('bad');
      return { update: { id: item.id }, events: [] };
    });

    expect(result.updates).toHaveLength(1);
    expect(result.errors).toHaveLength(2);
  });
});
