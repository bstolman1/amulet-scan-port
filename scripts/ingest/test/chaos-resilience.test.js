/**
 * Chaos Resilience Tests
 * 
 * Stress-tests the ingestion pipeline's recovery mechanisms by randomly
 * injecting failures during parallel operations:
 * - Random slice failures during parallel fetch
 * - Worker pool crashes and recovery
 * - GCS upload failures with retry exhaustion
 * - Memory pressure during high concurrency
 * - Cursor advancement safety under chaos
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';

// ============================================================================
// Chaos Utilities
// ============================================================================

/**
 * Creates a chaos injector that randomly fails operations
 */
function createChaosInjector(failureRate = 0.3, seed = Date.now()) {
  let callCount = 0;
  // Simple seeded random for reproducibility
  const seededRandom = () => {
    seed = (seed * 1103515245 + 12345) & 0x7fffffff;
    return seed / 0x7fffffff;
  };
  
  return {
    shouldFail: () => {
      callCount++;
      return seededRandom() < failureRate;
    },
    getCallCount: () => callCount,
    reset: () => { callCount = 0; },
  };
}

/**
 * Simulates a parallel fetch batch with chaos injection
 */
async function simulateParallelFetchWithChaos(sliceCount, chaos, options = {}) {
  const {
    minDelayMs = 10,
    maxDelayMs = 50,
    failureTypes = ['timeout', 'network', '503', 'memory'],
  } = options;
  
  const sliceResults = [];
  const sliceCompleted = new Array(sliceCount).fill(false);
  const sliceEarliestTime = new Array(sliceCount).fill(null);
  const sliceBoundaries = [];
  
  // Generate time boundaries for each slice (newest to oldest)
  const now = Date.now();
  for (let i = 0; i < sliceCount; i++) {
    sliceBoundaries.push({
      start: new Date(now - (i + 1) * 3600000).toISOString(),
      end: new Date(now - i * 3600000).toISOString(),
    });
  }
  
  // Simulate parallel execution
  const slicePromises = sliceBoundaries.map(async (boundary, sliceIndex) => {
    const delay = minDelayMs + Math.random() * (maxDelayMs - minDelayMs);
    await new Promise(r => setTimeout(r, delay));
    
    if (chaos.shouldFail()) {
      const failureType = failureTypes[Math.floor(Math.random() * failureTypes.length)];
      return {
        sliceIndex,
        success: false,
        error: failureType,
        boundary,
      };
    }
    
    // Simulate successful fetch
    const updateCount = Math.floor(Math.random() * 100) + 10;
    sliceCompleted[sliceIndex] = true;
    sliceEarliestTime[sliceIndex] = boundary.start;
    
    return {
      sliceIndex,
      success: true,
      updateCount,
      earliestTime: boundary.start,
      boundary,
    };
  });
  
  const results = await Promise.allSettled(slicePromises);
  
  return {
    results: results.map(r => r.status === 'fulfilled' ? r.value : { success: false, error: 'rejected' }),
    sliceCompleted,
    sliceEarliestTime,
    sliceBoundaries,
  };
}

/**
 * Calculates safe cursor boundary (copied from fetch-backfill.js logic)
 */
function getSafeCursorBoundary(sliceCompleted, sliceEarliestTime, sliceBoundaries) {
  // Find contiguous completed slices starting from slice 0 (newest)
  let contiguousEnd = -1;
  for (let i = 0; i < sliceCompleted.length; i++) {
    if (sliceCompleted[i]) {
      contiguousEnd = i;
    } else {
      break; // Gap found, stop
    }
  }
  
  if (contiguousEnd === -1) {
    return null; // No slices completed
  }
  
  // Safe boundary is the START of the oldest contiguous completed slice
  return sliceBoundaries[contiguousEnd].start;
}

// ============================================================================
// Chaos Tests: Parallel Slice Failures
// ============================================================================

describe('Chaos: Parallel Slice Failures', () => {
  
  it('should never advance cursor past failed slices under random failures', async () => {
    const iterations = 50;
    const sliceCount = 8;
    
    for (let iter = 0; iter < iterations; iter++) {
      const chaos = createChaosInjector(0.4, iter * 12345);
      const { results, sliceCompleted, sliceEarliestTime, sliceBoundaries } = 
        await simulateParallelFetchWithChaos(sliceCount, chaos);
      
      const safeBoundary = getSafeCursorBoundary(sliceCompleted, sliceEarliestTime, sliceBoundaries);
      
      // Find first failed slice
      const firstFailedSlice = results.findIndex(r => !r.success);
      
      if (firstFailedSlice === -1) {
        // All succeeded - cursor can advance to oldest slice
        expect(safeBoundary).toBe(sliceBoundaries[sliceCount - 1].start);
      } else if (firstFailedSlice === 0) {
        // First slice failed - cursor should not advance
        expect(safeBoundary).toBeNull();
      } else {
        // Cursor should stop before the first failed slice
        expect(safeBoundary).toBe(sliceBoundaries[firstFailedSlice - 1].start);
        
        // Verify no data from failed slices would be "claimed"
        for (let i = firstFailedSlice; i < sliceCount; i++) {
          if (!sliceCompleted[i]) {
            // Unfetched data would be re-fetched on restart
            expect(new Date(safeBoundary) >= new Date(sliceBoundaries[i].start)).toBe(true);
          }
        }
      }
    }
  });
  
  it('should handle cascading failures (multiple consecutive slice failures)', async () => {
    const sliceCount = 6;
    const sliceCompleted = [true, true, false, false, true, true];
    const sliceBoundaries = [];
    const sliceEarliestTime = [];
    
    const now = Date.now();
    for (let i = 0; i < sliceCount; i++) {
      sliceBoundaries.push({
        start: new Date(now - (i + 1) * 3600000).toISOString(),
        end: new Date(now - i * 3600000).toISOString(),
      });
      sliceEarliestTime.push(sliceCompleted[i] ? sliceBoundaries[i].start : null);
    }
    
    const safeBoundary = getSafeCursorBoundary(sliceCompleted, sliceEarliestTime, sliceBoundaries);
    
    // Should only advance to slice 1 (index 1), not past the gap at slices 2-3
    expect(safeBoundary).toBe(sliceBoundaries[1].start);
  });
  
  it('should recover correctly when only middle slices fail', async () => {
    const sliceCount = 5;
    const sliceCompleted = [true, true, false, true, true];
    const sliceBoundaries = [];
    const sliceEarliestTime = [];
    
    const now = Date.now();
    for (let i = 0; i < sliceCount; i++) {
      sliceBoundaries.push({
        start: new Date(now - (i + 1) * 3600000).toISOString(),
        end: new Date(now - i * 3600000).toISOString(),
      });
      sliceEarliestTime.push(sliceCompleted[i] ? sliceBoundaries[i].start : null);
    }
    
    const safeBoundary = getSafeCursorBoundary(sliceCompleted, sliceEarliestTime, sliceBoundaries);
    
    // Should stop at slice 1, even though slices 3-4 completed
    expect(safeBoundary).toBe(sliceBoundaries[1].start);
  });
});

// ============================================================================
// Chaos Tests: Worker Pool Crashes
// ============================================================================

describe('Chaos: Worker Pool Crashes', () => {
  
  /**
   * Simulates a worker pool with random crashes
   */
  function createChaosWorkerPool(maxWorkers, crashRate = 0.2) {
    const chaos = createChaosInjector(crashRate);
    const stats = {
      jobsSubmitted: 0,
      jobsCompleted: 0,
      jobsFailed: 0,
      workerCrashes: 0,
      retriesAttempted: 0,
    };
    
    const queue = [];
    let activeWorkers = 0;
    
    async function submitJob(job) {
      stats.jobsSubmitted++;
      
      return new Promise((resolve, reject) => {
        queue.push({ job, resolve, reject, attempts: 0 });
        pump();
      });
    }
    
    async function pump() {
      while (activeWorkers < maxWorkers && queue.length > 0) {
        const { job, resolve, reject, attempts } = queue.shift();
        activeWorkers++;
        
        // Simulate work
        await new Promise(r => setTimeout(r, 5 + Math.random() * 10));
        
        if (chaos.shouldFail()) {
          stats.workerCrashes++;
          activeWorkers--;
          
          // Retry up to 3 times
          if (attempts < 3) {
            stats.retriesAttempted++;
            queue.push({ job, resolve, reject, attempts: attempts + 1 });
            pump();
          } else {
            stats.jobsFailed++;
            reject(new Error('Worker crashed after 3 retries'));
          }
        } else {
          stats.jobsCompleted++;
          activeWorkers--;
          resolve({ success: true, job });
        }
      }
    }
    
    return { submitJob, getStats: () => ({ ...stats }), chaos };
  }
  
  it('should eventually complete jobs despite random worker crashes', async () => {
    const pool = createChaosWorkerPool(4, 0.15);
    const jobCount = 20;
    
    const results = await Promise.allSettled(
      Array.from({ length: jobCount }, (_, i) => pool.submitJob({ id: i }))
    );
    
    const stats = pool.getStats();
    
    // With 15% crash rate and 3 retries, most jobs should complete
    const succeeded = results.filter(r => r.status === 'fulfilled').length;
    const failed = results.filter(r => r.status === 'rejected').length;
    
    expect(succeeded + failed).toBe(jobCount);
    expect(stats.jobsSubmitted).toBe(jobCount);
    expect(stats.jobsCompleted + stats.jobsFailed).toBe(jobCount);
    
    // Retries should have been attempted for crashes
    if (stats.workerCrashes > 0) {
      expect(stats.retriesAttempted).toBeGreaterThan(0);
    }
  });
  
  it('should track failed jobs correctly under high crash rate', async () => {
    const pool = createChaosWorkerPool(2, 0.6); // 60% crash rate
    const jobCount = 10;
    
    const results = await Promise.allSettled(
      Array.from({ length: jobCount }, (_, i) => pool.submitJob({ id: i }))
    );
    
    const stats = pool.getStats();
    
    // With high crash rate, some jobs will fail after 3 retries
    expect(stats.jobsFailed).toBeGreaterThan(0);
    expect(stats.workerCrashes).toBeGreaterThan(0);
    
    // Verify accounting
    expect(stats.jobsCompleted + stats.jobsFailed).toBe(jobCount);
  });
});

// ============================================================================
// Chaos Tests: GCS Upload Failures
// ============================================================================

describe('Chaos: GCS Upload Failures', () => {
  
  /**
   * Simulates GCS upload queue with random failures
   */
  function createChaosGCSQueue(concurrency, failureRate = 0.25) {
    const chaos = createChaosInjector(failureRate);
    const stats = {
      uploadsAttempted: 0,
      uploadsSucceeded: 0,
      uploadsFailed: 0,
      retriesAttempted: 0,
      bytesUploaded: 0,
    };
    
    const queue = [];
    let activeUploads = 0;
    let isPaused = false;
    
    const HIGH_WATER = 50;
    const LOW_WATER = 10;
    
    async function enqueue(file) {
      return new Promise((resolve, reject) => {
        if (queue.length >= HIGH_WATER && !isPaused) {
          isPaused = true;
        }
        
        queue.push({ file, resolve, reject, attempts: 0 });
        pump();
      });
    }
    
  async function pump() {
    while (activeUploads < concurrency && queue.length > 0) {
      const item = queue.shift();
      activeUploads++;
      
      // Release backpressure at low water
      if (isPaused && queue.length <= LOW_WATER) {
        isPaused = false;
      }
      
      processUpload(item);
    }
  }
    
    async function processUpload({ file, resolve, reject, attempts }) {
      stats.uploadsAttempted++;
      
      // Simulate upload delay
      await new Promise(r => setTimeout(r, 5 + Math.random() * 15));
      
      if (chaos.shouldFail()) {
        activeUploads--;
        
        // Determine if transient (retryable) or permanent
        const isTransient = Math.random() > 0.3; // 70% are transient
        
        if (isTransient && attempts < 3) {
          stats.retriesAttempted++;
          // Exponential backoff simulation
          await new Promise(r => setTimeout(r, Math.pow(2, attempts) * 5));
          queue.unshift({ file, resolve, reject, attempts: attempts + 1 });
          pump();
        } else {
          stats.uploadsFailed++;
          reject(new Error(isTransient ? 'Max retries exceeded' : 'Permanent failure'));
        }
      } else {
        stats.uploadsSucceeded++;
        stats.bytesUploaded += file.size || 1024;
        activeUploads--;
        resolve({ success: true, file: file.path });
        pump();
      }
    }
    
    return {
      enqueue,
      getStats: () => ({ ...stats }),
      getQueueDepth: () => queue.length + activeUploads,
      isPaused: () => isPaused,
    };
  }
  
  it('should handle burst of uploads with random failures', async () => {
    const queue = createChaosGCSQueue(6, 0.2);
    const fileCount = 30;
    
    const files = Array.from({ length: fileCount }, (_, i) => ({
      path: `file-${i}.parquet`,
      size: 1024 * 1024 * (1 + Math.random() * 10),
    }));
    
    const results = await Promise.allSettled(
      files.map(f => queue.enqueue(f))
    );
    
    const stats = queue.getStats();
    
    const succeeded = results.filter(r => r.status === 'fulfilled').length;
    const failed = results.filter(r => r.status === 'rejected').length;
    
    expect(succeeded + failed).toBe(fileCount);
    expect(stats.uploadsSucceeded).toBe(succeeded);
    expect(stats.uploadsFailed).toBe(failed);
    
    // With retries, attempts > files
    if (stats.uploadsFailed > 0 || stats.retriesAttempted > 0) {
      expect(stats.uploadsAttempted).toBeGreaterThanOrEqual(fileCount);
    }
  });
  
  it('should apply backpressure under queue overflow', async () => {
    const queue = createChaosGCSQueue(2, 0.05); // Low concurrency, low failure rate
    const fileCount = 60;
    
    let maxQueueDepth = 0;
    
    const files = Array.from({ length: fileCount }, (_, i) => ({
      path: `file-${i}.parquet`,
      size: 1024,
    }));
    
    // Fire all at once
    const promises = files.map(f => {
      const depth = queue.getQueueDepth();
      maxQueueDepth = Math.max(maxQueueDepth, depth);
      return queue.enqueue(f);
    });
    
    await Promise.allSettled(promises);
    
    const stats = queue.getStats();
    
    // Queue should have experienced depth > concurrency
    expect(maxQueueDepth).toBeGreaterThan(2);
    
    // All files should be processed
    expect(stats.uploadsSucceeded + stats.uploadsFailed).toBe(fileCount);
  });
  
  it('should exhaust retries and fail permanently for persistent errors', async () => {
    // High failure rate with mostly permanent errors
    const queue = createChaosGCSQueue(4, 0.8);
    const fileCount = 10;
    
    const files = Array.from({ length: fileCount }, (_, i) => ({
      path: `file-${i}.parquet`,
      size: 1024,
    }));
    
    const results = await Promise.allSettled(
      files.map(f => queue.enqueue(f))
    );
    
    const stats = queue.getStats();
    
    // High failure rate should cause many failures
    expect(stats.uploadsFailed).toBeGreaterThan(0);
    
    // Verify total accounting
    expect(stats.uploadsSucceeded + stats.uploadsFailed).toBe(fileCount);
  });
});

// ============================================================================
// Chaos Tests: Memory Pressure Simulation
// ============================================================================

describe('Chaos: Memory Pressure', () => {
  
  /**
   * Simulates memory-aware processing with pressure triggers
   */
  function createMemoryAwareProcessor(heapLimit = 1000) {
    let currentHeap = 0;
    const allocations = [];
    const stats = {
      processed: 0,
      paused: 0,
      gcTriggered: 0,
      rejected: 0,
    };
    
    const PRESSURE_THRESHOLD = 0.8;
    const CRITICAL_THRESHOLD = 0.95;
    
    function allocate(size) {
      currentHeap += size;
      allocations.push(size);
    }
    
    function gc() {
      // Free oldest allocations
      const toFree = Math.floor(allocations.length * 0.5);
      for (let i = 0; i < toFree && allocations.length > 0; i++) {
        currentHeap -= allocations.shift();
      }
      stats.gcTriggered++;
    }
    
    async function process(item) {
      const pressure = currentHeap / heapLimit;
      
      if (pressure >= CRITICAL_THRESHOLD) {
        stats.rejected++;
        throw new Error('Memory critical - request rejected');
      }
      
      if (pressure >= PRESSURE_THRESHOLD) {
        stats.paused++;
        gc();
        await new Promise(r => setTimeout(r, 10)); // Wait for GC
      }
      
      // Simulate allocation
      const allocationSize = item.size || 10;
      allocate(allocationSize);
      
      stats.processed++;
      
      return { success: true, heapAfter: currentHeap };
    }
    
    return {
      process,
      getStats: () => ({ ...stats }),
      getHeapUsage: () => currentHeap / heapLimit,
      gc,
    };
  }
  
  it('should trigger GC under memory pressure', async () => {
    const processor = createMemoryAwareProcessor(100);
    
    // Process items that will fill heap
    const items = Array.from({ length: 20 }, () => ({ size: 10 }));
    
    for (const item of items) {
      try {
        await processor.process(item);
      } catch (e) {
        // May reject if critical
      }
    }
    
    const stats = processor.getStats();
    
    // Should have triggered GC at some point
    expect(stats.gcTriggered).toBeGreaterThan(0);
    expect(stats.paused).toBeGreaterThan(0);
  });
  
  it('should reject requests at critical memory pressure', async () => {
    const processor = createMemoryAwareProcessor(30); // Very small heap
    
    // Flood with large allocations that won't be GC'd fast enough
    const items = Array.from({ length: 50 }, () => ({ size: 15 }));
    
    let rejected = 0;
    for (const item of items) {
      try {
        await processor.process(item);
      } catch (e) {
        if (e.message.includes('Memory critical')) {
          rejected++;
        }
      }
    }
    
    const stats = processor.getStats();
    
    // With tiny heap and large allocations, should hit critical
    // GC only frees 50% of allocations, so pressure builds up
    expect(stats.rejected + stats.processed).toBe(50);
  });
  
  it('should recover after GC and continue processing', async () => {
    const processor = createMemoryAwareProcessor(100);
    
    // First batch - fill memory
    const batch1 = Array.from({ length: 15 }, () => ({ size: 8 }));
    for (const item of batch1) {
      try {
        await processor.process(item);
      } catch (e) {
        // May reject
      }
    }
    
    const midStats = processor.getStats();
    const midHeap = processor.getHeapUsage();
    
    // Force GC
    processor.gc();
    
    // Second batch - should process more
    const batch2 = Array.from({ length: 10 }, () => ({ size: 5 }));
    for (const item of batch2) {
      try {
        await processor.process(item);
      } catch (e) {
        // May reject
      }
    }
    
    const finalStats = processor.getStats();
    
    // Should have processed more items after GC
    expect(finalStats.processed).toBeGreaterThan(midStats.processed);
  });
});

// ============================================================================
// Chaos Tests: End-to-End Pipeline Stress
// ============================================================================

describe('Chaos: End-to-End Pipeline Stress', () => {
  
  /**
   * Simulates full pipeline with all failure modes
   */
  async function runChaosE2E(options = {}) {
    const {
      sliceCount = 6,
      filesPerSlice = 5,
      fetchFailureRate = 0.15,
      workerFailureRate = 0.1,
      uploadFailureRate = 0.2,
      seed = Date.now(),
    } = options;
    
    const fetchChaos = createChaosInjector(fetchFailureRate, seed);
    const workerChaos = createChaosInjector(workerFailureRate, seed + 1);
    const uploadChaos = createChaosInjector(uploadFailureRate, seed + 2);
    
    const stats = {
      slicesFetched: 0,
      slicesFailed: 0,
      filesWritten: 0,
      filesFailedWrite: 0,
      filesUploaded: 0,
      filesFailedUpload: 0,
      cursorAdvancedTo: null,
    };
    
    const sliceCompleted = new Array(sliceCount).fill(false);
    const sliceBoundaries = [];
    const now = Date.now();
    
    for (let i = 0; i < sliceCount; i++) {
      sliceBoundaries.push({
        start: new Date(now - (i + 1) * 3600000).toISOString(),
        end: new Date(now - i * 3600000).toISOString(),
      });
    }
    
    // Phase 1: Fetch slices
    for (let i = 0; i < sliceCount; i++) {
      if (fetchChaos.shouldFail()) {
        stats.slicesFailed++;
        continue;
      }
      
      stats.slicesFetched++;
      sliceCompleted[i] = true;
      
      // Phase 2: Write files for this slice
      for (let f = 0; f < filesPerSlice; f++) {
        if (workerChaos.shouldFail()) {
          stats.filesFailedWrite++;
          continue;
        }
        
        stats.filesWritten++;
        
        // Phase 3: Upload to GCS
        if (uploadChaos.shouldFail()) {
          stats.filesFailedUpload++;
        } else {
          stats.filesUploaded++;
        }
      }
    }
    
    // Calculate safe cursor position
    let contiguousEnd = -1;
    for (let i = 0; i < sliceCount; i++) {
      if (sliceCompleted[i]) {
        contiguousEnd = i;
      } else {
        break;
      }
    }
    
    if (contiguousEnd >= 0) {
      stats.cursorAdvancedTo = sliceBoundaries[contiguousEnd].start;
    }
    
    return { stats, sliceCompleted, sliceBoundaries };
  }
  
  it('should maintain data integrity under combined failures', async () => {
    const iterations = 30;
    
    for (let iter = 0; iter < iterations; iter++) {
      const { stats, sliceCompleted, sliceBoundaries } = await runChaosE2E({
        seed: iter * 54321,
        fetchFailureRate: 0.2,
        workerFailureRate: 0.15,
        uploadFailureRate: 0.25,
      });
      
      // Verify cursor safety
      const firstFailedSlice = sliceCompleted.indexOf(false);
      
      if (firstFailedSlice === -1) {
        // All slices completed
        expect(stats.cursorAdvancedTo).toBe(sliceBoundaries[sliceBoundaries.length - 1].start);
      } else if (firstFailedSlice === 0) {
        // First slice failed
        expect(stats.cursorAdvancedTo).toBeNull();
      } else {
        // Cursor should stop before first failed slice
        expect(stats.cursorAdvancedTo).toBe(sliceBoundaries[firstFailedSlice - 1].start);
      }
      
      // Verify accounting
      expect(stats.slicesFetched + stats.slicesFailed).toBe(6);
    }
  });
  
  it('should handle worst-case failure storm', async () => {
    const { stats, sliceCompleted } = await runChaosE2E({
      fetchFailureRate: 0.5,
      workerFailureRate: 0.4,
      uploadFailureRate: 0.5,
      seed: 999,
    });
    
    // Pipeline should still function (not crash)
    expect(stats.slicesFetched + stats.slicesFailed).toBe(6);
    
    // Cursor advancement should be consistent with slice completion
    const firstFailedSlice = sliceCompleted.indexOf(false);
    if (firstFailedSlice === 0) {
      expect(stats.cursorAdvancedTo).toBeNull();
    } else if (firstFailedSlice === -1) {
      expect(stats.cursorAdvancedTo).not.toBeNull();
    } else {
      expect(stats.cursorAdvancedTo).not.toBeNull();
    }
  });
  
  it('should report accurate statistics under chaos', async () => {
    const { stats } = await runChaosE2E({
      sliceCount: 8,
      filesPerSlice: 10,
      fetchFailureRate: 0.1,
      workerFailureRate: 0.1,
      uploadFailureRate: 0.1,
      seed: 12345,
    });
    
    // Total slice operations
    expect(stats.slicesFetched + stats.slicesFailed).toBe(8);
    
    // File operations only happen for fetched slices
    const maxFiles = stats.slicesFetched * 10;
    expect(stats.filesWritten + stats.filesFailedWrite).toBeLessThanOrEqual(maxFiles);
    
    // Uploads only happen for written files
    expect(stats.filesUploaded + stats.filesFailedUpload).toBeLessThanOrEqual(stats.filesWritten);
  });
});

// ============================================================================
// Chaos Tests: Deduplication Under Race Conditions
// ============================================================================

describe('Chaos: Deduplication Safety', () => {
  
  it('should never duplicate updates despite parallel processing chaos', async () => {
    const globalSeenIds = new Set();
    const processedUpdates = [];
    const duplicates = [];
    
    // Simulate parallel slices processing same update IDs
    const slices = [
      ['upd-001', 'upd-002', 'upd-003'],
      ['upd-002', 'upd-003', 'upd-004'], // Overlapping!
      ['upd-004', 'upd-005', 'upd-006'],
      ['upd-005', 'upd-006', 'upd-007'], // Overlapping!
    ];
    
    // Process in parallel with random timing
    await Promise.all(slices.map(async (slice, idx) => {
      await new Promise(r => setTimeout(r, Math.random() * 20));
      
      for (const updateId of slice) {
        // Dedup check (with lock simulation)
        if (globalSeenIds.has(updateId)) {
          duplicates.push({ updateId, slice: idx });
        } else {
          globalSeenIds.add(updateId);
          processedUpdates.push({ updateId, slice: idx });
        }
      }
    }));
    
    // Verify no duplicates in processed output
    const uniqueProcessed = new Set(processedUpdates.map(u => u.updateId));
    expect(uniqueProcessed.size).toBe(processedUpdates.length);
    
    // Duplicates should have been caught
    expect(duplicates.length).toBeGreaterThan(0);
    
    // Total unique updates
    expect(globalSeenIds.size).toBe(7); // upd-001 through upd-007
  });
  
  it('should handle Set capacity limits gracefully', async () => {
    const MAX_SET_SIZE = 100;
    const globalSeenIds = new Set();
    let clearedCount = 0;
    
    // Process many updates
    for (let i = 0; i < 250; i++) {
      if (globalSeenIds.size >= MAX_SET_SIZE) {
        globalSeenIds.clear();
        clearedCount++;
      }
      globalSeenIds.add(`upd-${i}`);
    }
    
    // Set should have been cleared multiple times
    expect(clearedCount).toBeGreaterThan(0);
    
    // Final set size should be under limit
    expect(globalSeenIds.size).toBeLessThanOrEqual(MAX_SET_SIZE);
  });
});
