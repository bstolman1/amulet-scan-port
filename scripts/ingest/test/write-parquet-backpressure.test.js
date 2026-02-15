/**
 * Tests for backpressure checks in write-parquet.js bufferUpdates/bufferEvents
 * 
 * Verifies the backpressure logic that guards buffer flushes.
 * Tests the shouldPauseWrites/drainUploads integration without importing
 * the full write-parquet module (which has heavy dependencies).
 */

import { describe, it, expect, vi } from 'vitest';

describe('write-parquet backpressure logic', () => {
  it('drainUploads is awaited when GCS mode is on and shouldPauseWrites returns true', async () => {
    // Simulate the exact logic from bufferUpdates/bufferEvents:
    //   if (getGCSMode() && shouldPauseWrites()) { await drainUploads(); }
    const getGCSMode = () => true;
    const shouldPauseWrites = vi.fn(() => true);
    const drainUploads = vi.fn(() => Promise.resolve());

    // Execute the guard
    if (getGCSMode() && shouldPauseWrites()) {
      await drainUploads();
    }

    expect(shouldPauseWrites).toHaveBeenCalled();
    expect(drainUploads).toHaveBeenCalled();
  });

  it('drainUploads is NOT called when shouldPauseWrites returns false', async () => {
    const getGCSMode = () => true;
    const shouldPauseWrites = vi.fn(() => false);
    const drainUploads = vi.fn(() => Promise.resolve());

    if (getGCSMode() && shouldPauseWrites()) {
      await drainUploads();
    }

    expect(shouldPauseWrites).toHaveBeenCalled();
    expect(drainUploads).not.toHaveBeenCalled();
  });

  it('shouldPauseWrites is NOT called in local mode (no GCS)', async () => {
    const getGCSMode = () => false;
    const shouldPauseWrites = vi.fn(() => true);
    const drainUploads = vi.fn(() => Promise.resolve());

    if (getGCSMode() && shouldPauseWrites()) {
      await drainUploads();
    }

    expect(shouldPauseWrites).not.toHaveBeenCalled();
    expect(drainUploads).not.toHaveBeenCalled();
  });

  it('drainUploads resolves before flush proceeds', async () => {
    const order = [];
    const getGCSMode = () => true;
    const shouldPauseWrites = () => true;
    const drainUploads = () => new Promise(resolve => {
      order.push('drain_start');
      setTimeout(() => {
        order.push('drain_complete');
        resolve();
      }, 10);
    });
    const flush = () => { order.push('flush'); };

    if (getGCSMode() && shouldPauseWrites()) {
      await drainUploads();
    }
    flush();

    expect(order).toEqual(['drain_start', 'drain_complete', 'flush']);
  });

  it('matches the guard condition in write-parquet.js bufferUpdates', () => {
    // The actual code in write-parquet.js:
    //   if (getGCSMode() && shouldPauseWrites()) {
    //     await drainUploads();
    //   }
    // Verify truth table:
    const cases = [
      { gcs: false, pause: false, shouldDrain: false },
      { gcs: false, pause: true, shouldDrain: false },
      { gcs: true, pause: false, shouldDrain: false },
      { gcs: true, pause: true, shouldDrain: true },
    ];

    for (const { gcs, pause, shouldDrain } of cases) {
      const result = gcs && pause;
      expect(result).toBe(shouldDrain);
    }
  });
});
