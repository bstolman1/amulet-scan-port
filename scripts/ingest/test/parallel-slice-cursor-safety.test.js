/**
 * Parallel Slice Cursor Safety Tests
 * 
 * Verifies that cursor advancement is CONSERVATIVE during parallel slice processing.
 * The cursor should ONLY advance to the oldest boundary of CONTIGUOUSLY completed slices.
 * 
 * This prevents data gaps when:
 * - Older slices complete before newer slices
 * - A newer slice fails after older slices have completed
 */

import { describe, it, expect, beforeEach } from 'vitest';

/**
 * Simulates the getSafeCursorBoundary logic from parallelFetchBatch
 */
function getSafeCursorBoundary(sliceCompleted, sliceBoundaries, sliceEarliestTime, startBefore) {
  const concurrency = sliceCompleted.length;
  
  // Find the first incomplete slice starting from 0
  let contiguousCompleteCount = 0;
  for (let i = 0; i < concurrency; i++) {
    if (sliceCompleted[i]) {
      contiguousCompleteCount++;
    } else {
      break; // Found first incomplete
    }
  }
  
  if (contiguousCompleteCount === 0) {
    // No slices complete yet - cursor stays at startBefore
    return startBefore;
  }
  
  // Safe boundary is the END (sliceAfter) of the last contiguously completed slice
  const lastCompleteIdx = contiguousCompleteCount - 1;
  const safeTime = sliceEarliestTime[lastCompleteIdx] || sliceBoundaries[lastCompleteIdx].sliceAfter;
  
  return safeTime;
}

/**
 * Creates test slice boundaries for a given time range
 */
function createSliceBoundaries(startMs, endMs, concurrency) {
  const rangeMs = endMs - startMs;
  const sliceMs = rangeMs / concurrency;
  const boundaries = [];
  
  for (let i = 0; i < concurrency; i++) {
    boundaries.push({
      sliceBefore: new Date(endMs - (i * sliceMs)).toISOString(),
      sliceAfter: new Date(endMs - ((i + 1) * sliceMs)).toISOString(),
    });
  }
  
  return boundaries;
}

describe('Parallel Slice Cursor Safety', () => {
  // Time range: 1000ms → 0ms (maxTime → minTime)
  const startMs = 0;
  const endMs = 1000;
  const startBefore = new Date(endMs).toISOString();
  const concurrency = 4;
  
  let sliceBoundaries;
  let sliceCompleted;
  let sliceEarliestTime;
  
  beforeEach(() => {
    sliceBoundaries = createSliceBoundaries(startMs, endMs, concurrency);
    sliceCompleted = [false, false, false, false];
    sliceEarliestTime = sliceBoundaries.map(b => b.sliceBefore);
  });
  
  describe('getSafeCursorBoundary', () => {
    it('returns startBefore when no slices are complete', () => {
      const result = getSafeCursorBoundary(
        sliceCompleted,
        sliceBoundaries,
        sliceEarliestTime,
        startBefore
      );
      
      expect(result).toBe(startBefore);
    });
    
    it('advances cursor when slice 0 (newest) completes first', () => {
      sliceCompleted[0] = true;
      sliceEarliestTime[0] = sliceBoundaries[0].sliceAfter; // Processed to slice boundary
      
      const result = getSafeCursorBoundary(
        sliceCompleted,
        sliceBoundaries,
        sliceEarliestTime,
        startBefore
      );
      
      expect(result).toBe(sliceBoundaries[0].sliceAfter);
    });
    
    it('does NOT advance cursor when only slice 3 (oldest) completes', () => {
      // This is the critical case! Slice 3 completing before slice 0
      // should NOT advance the cursor past slice 0's range.
      sliceCompleted[3] = true;
      sliceEarliestTime[3] = sliceBoundaries[3].sliceAfter; // Very old timestamp
      
      const result = getSafeCursorBoundary(
        sliceCompleted,
        sliceBoundaries,
        sliceEarliestTime,
        startBefore
      );
      
      // Cursor should NOT move - slice 0, 1, 2 are incomplete
      expect(result).toBe(startBefore);
    });
    
    it('does NOT advance cursor when slices 2, 3 complete but 0, 1 incomplete', () => {
      sliceCompleted[2] = true;
      sliceCompleted[3] = true;
      sliceEarliestTime[2] = sliceBoundaries[2].sliceAfter;
      sliceEarliestTime[3] = sliceBoundaries[3].sliceAfter;
      
      const result = getSafeCursorBoundary(
        sliceCompleted,
        sliceBoundaries,
        sliceEarliestTime,
        startBefore
      );
      
      // Cursor should NOT move - slices 0, 1 are incomplete
      expect(result).toBe(startBefore);
    });
    
    it('advances to slice 1 boundary when slices 0, 1 complete', () => {
      sliceCompleted[0] = true;
      sliceCompleted[1] = true;
      sliceEarliestTime[0] = sliceBoundaries[0].sliceAfter;
      sliceEarliestTime[1] = sliceBoundaries[1].sliceAfter;
      
      const result = getSafeCursorBoundary(
        sliceCompleted,
        sliceBoundaries,
        sliceEarliestTime,
        startBefore
      );
      
      // Cursor advances to slice 1's boundary (contiguous block 0,1)
      expect(result).toBe(sliceBoundaries[1].sliceAfter);
    });
    
    it('advances to slice 2 boundary when slices 0, 1, 2 complete', () => {
      sliceCompleted[0] = true;
      sliceCompleted[1] = true;
      sliceCompleted[2] = true;
      sliceEarliestTime[0] = sliceBoundaries[0].sliceAfter;
      sliceEarliestTime[1] = sliceBoundaries[1].sliceAfter;
      sliceEarliestTime[2] = sliceBoundaries[2].sliceAfter;
      
      const result = getSafeCursorBoundary(
        sliceCompleted,
        sliceBoundaries,
        sliceEarliestTime,
        startBefore
      );
      
      // Cursor advances to slice 2's boundary
      expect(result).toBe(sliceBoundaries[2].sliceAfter);
    });
    
    it('advances to minimum when ALL slices complete', () => {
      sliceCompleted = [true, true, true, true];
      sliceEarliestTime[0] = sliceBoundaries[0].sliceAfter;
      sliceEarliestTime[1] = sliceBoundaries[1].sliceAfter;
      sliceEarliestTime[2] = sliceBoundaries[2].sliceAfter;
      sliceEarliestTime[3] = sliceBoundaries[3].sliceAfter;
      
      const result = getSafeCursorBoundary(
        sliceCompleted,
        sliceBoundaries,
        sliceEarliestTime,
        startBefore
      );
      
      // Cursor advances to slice 3's boundary (oldest)
      expect(result).toBe(sliceBoundaries[3].sliceAfter);
    });
    
    it('uses actual earliest time within slice if available', () => {
      sliceCompleted[0] = true;
      // Slice processed data but not all the way to its boundary
      const partialTime = new Date(900).toISOString(); // Not at boundary (750)
      sliceEarliestTime[0] = partialTime;
      
      const result = getSafeCursorBoundary(
        sliceCompleted,
        sliceBoundaries,
        sliceEarliestTime,
        startBefore
      );
      
      // Should use the actual earliest time, not the boundary
      expect(result).toBe(partialTime);
    });
  });
  
  describe('Race condition scenarios', () => {
    it('prevents data gap when slice 0 fails after slice 3 completes', () => {
      // Simulate: slice 3 completes, then slice 0 fails
      sliceCompleted[1] = false; // Still running
      sliceCompleted[2] = true;  // Completed
      sliceCompleted[3] = true;  // Completed
      // Slice 0 failed - NOT marked complete
      sliceCompleted[0] = false;
      
      const result = getSafeCursorBoundary(
        sliceCompleted,
        sliceBoundaries,
        sliceEarliestTime,
        startBefore
      );
      
      // Cursor should NOT advance - slice 0 is incomplete
      expect(result).toBe(startBefore);
      // On restart, we'll re-fetch from startBefore, including slice 0's range
    });
    
    it('prevents data gap when middle slice fails', () => {
      // Simulate: slices 0, 3 complete, but slice 1 fails
      sliceCompleted[0] = true;
      sliceCompleted[1] = false; // Failed
      sliceCompleted[2] = true;
      sliceCompleted[3] = true;
      
      sliceEarliestTime[0] = sliceBoundaries[0].sliceAfter;
      sliceEarliestTime[2] = sliceBoundaries[2].sliceAfter;
      sliceEarliestTime[3] = sliceBoundaries[3].sliceAfter;
      
      const result = getSafeCursorBoundary(
        sliceCompleted,
        sliceBoundaries,
        sliceEarliestTime,
        startBefore
      );
      
      // Cursor advances only to slice 0's boundary (contiguous: just slice 0)
      expect(result).toBe(sliceBoundaries[0].sliceAfter);
      // Slice 1, 2, 3's data will be re-fetched on restart
    });
  });
  
  describe('Slice boundary calculation', () => {
    it('creates non-overlapping slices covering full range', () => {
      const boundaries = createSliceBoundaries(0, 1000, 4);
      
      expect(boundaries).toHaveLength(4);
      
      // Slice 0: 1000 → 750 (newest)
      expect(new Date(boundaries[0].sliceBefore).getTime()).toBe(1000);
      expect(new Date(boundaries[0].sliceAfter).getTime()).toBe(750);
      
      // Slice 1: 750 → 500
      expect(new Date(boundaries[1].sliceBefore).getTime()).toBe(750);
      expect(new Date(boundaries[1].sliceAfter).getTime()).toBe(500);
      
      // Slice 2: 500 → 250
      expect(new Date(boundaries[2].sliceBefore).getTime()).toBe(500);
      expect(new Date(boundaries[2].sliceAfter).getTime()).toBe(250);
      
      // Slice 3: 250 → 0 (oldest)
      expect(new Date(boundaries[3].sliceBefore).getTime()).toBe(250);
      expect(new Date(boundaries[3].sliceAfter).getTime()).toBe(0);
    });
    
    it('handles uneven division correctly', () => {
      const boundaries = createSliceBoundaries(0, 1000, 3);
      
      expect(boundaries).toHaveLength(3);
      
      // Each slice is ~333ms
      expect(new Date(boundaries[0].sliceBefore).getTime()).toBeCloseTo(1000, 0);
      expect(new Date(boundaries[2].sliceAfter).getTime()).toBeCloseTo(0, 0);
    });
  });
});
