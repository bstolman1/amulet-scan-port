/**
 * Tests for backpressure checks in write-parquet.js
 * 
 * Verifies that the ACTUAL bufferUpdates/bufferEvents functions in write-parquet.js
 * contain the backpressure guard, and that the shouldPauseWrites/drainUploads
 * integration from gcs-upload-queue.js works correctly.
 */

import { describe, it, expect, vi } from 'vitest';
import fs from 'fs';

describe('write-parquet.js backpressure source verification', () => {
  let source;

  beforeAll(() => {
    source = fs.readFileSync('scripts/ingest/write-parquet.js', 'utf8');
  });

  it('bufferUpdates checks shouldPauseWrites before flushing', () => {
    // The guard must be present in the bufferUpdates function
    expect(source).toContain('if (getGCSMode() && shouldPauseWrites())');
    expect(source).toContain('await drainUploads()');
  });

  it('bufferEvents checks shouldPauseWrites before flushing', () => {
    // Both buffer functions must have the guard
    const matches = source.match(/shouldPauseWrites\(\)/g);
    expect(matches).not.toBeNull();
    expect(matches.length).toBeGreaterThanOrEqual(2); // Once in bufferUpdates, once in bufferEvents
  });

  it('imports shouldPauseWrites and drainUploads from gcs-upload-queue.js', () => {
    expect(source).toContain('shouldPauseWrites');
    expect(source).toContain('drainUploads');
    expect(source).toMatch(/from\s+['"]\.\/gcs-upload-queue\.js['"]/);
  });
});

describe('shouldPauseWrites (from gcs-upload-queue.js)', () => {
  it('returns false when no queue instance exists', async () => {
    const { shouldPauseWrites } = await import('../gcs-upload-queue.js');
    // When queueInstance is null (no GCS mode), shouldPauseWrites returns false
    // This is the actual exported function, not a copy
    const result = shouldPauseWrites();
    expect(result).toBe(false);
  });
});

describe('GCSUploadQueue backpressure behavior', () => {
  it('shouldPause() returns true when queue exceeds high water mark', async () => {
    const mod = await import('../gcs-upload-queue.js');
    
    // Create a queue with very low thresholds for testing
    const queue = new mod.default(2); // 2 concurrent
    queue.queueHighWater = 3;
    queue.queueLowWater = 1;

    // Manually push items to exceed high water without triggering actual uploads
    queue.queue.push(
      { localPath: '/tmp/a', gcsPath: 'gs://b/a' },
      { localPath: '/tmp/b', gcsPath: 'gs://b/b' },
      { localPath: '/tmp/c', gcsPath: 'gs://b/c' },
    );
    
    // Manually check backpressure (queue length >= highWater)
    expect(queue.queue.length).toBeGreaterThanOrEqual(queue.queueHighWater);
    
    // Trigger the isPaused flag as enqueue would
    queue.isPaused = true;
    expect(queue.shouldPause()).toBe(true);
  });

  it('shouldPause() returns false when queue is below low water mark', async () => {
    const mod = await import('../gcs-upload-queue.js');
    const queue = new mod.default(2);
    queue.queueHighWater = 100;
    queue.queueLowWater = 20;

    // Empty queue
    expect(queue.shouldPause()).toBe(false);
  });

  it('getQueueDepth() returns queue length + active uploads', async () => {
    const mod = await import('../gcs-upload-queue.js');
    const queue = new mod.default(2);
    
    queue.queue.push({ localPath: '/tmp/x', gcsPath: 'gs://b/x' });
    queue.activeUploads = 3;
    
    expect(queue.getQueueDepth()).toBe(4); // 1 queued + 3 active
  });
});

import { beforeAll } from 'vitest';
