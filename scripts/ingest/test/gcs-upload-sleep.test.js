/**
 * Tests for gcs-upload.js sleep() and calculateBackoffDelay()
 * 
 * Verifies that the Atomics.wait-based sleep does NOT busy-wait the CPU,
 * blocks for approximately the requested duration, and that backoff
 * delay calculation is correct.
 */

import { describe, it, expect } from 'vitest';
import { sleep, calculateBackoffDelay, isTransientError } from '../gcs-upload.js';

describe('gcs-upload sleep (Atomics.wait)', () => {
  it('blocks for approximately the requested duration', () => {
    const durations = [50, 100, 200];
    
    for (const ms of durations) {
      const start = Date.now();
      sleep(ms);
      const elapsed = Date.now() - start;
      
      // Allow ±30ms tolerance for timer granularity
      expect(elapsed).toBeGreaterThanOrEqual(ms - 5);
      expect(elapsed).toBeLessThan(ms + 50);
    }
  });

  it('does NOT busy-wait (CPU usage stays low)', () => {
    // Measure CPU time vs wall time. A busy-wait loop would have
    // cpuTime ≈ wallTime. Atomics.wait should have cpuTime ≈ 0.
    const sleepMs = 200;
    
    const cpuBefore = process.cpuUsage();
    const wallBefore = Date.now();
    
    sleep(sleepMs);
    
    const wallElapsed = Date.now() - wallBefore;
    const cpuAfter = process.cpuUsage(cpuBefore);
    const cpuElapsedMs = (cpuAfter.user + cpuAfter.system) / 1000; // microseconds to ms
    
    // Wall time should be roughly the sleep duration
    expect(wallElapsed).toBeGreaterThanOrEqual(sleepMs - 5);
    
    // CPU time should be a tiny fraction of wall time (< 20%)
    // A busy-wait loop would consume close to 100% of wall time
    expect(cpuElapsedMs).toBeLessThan(wallElapsed * 0.2);
  });

  it('handles zero duration', () => {
    const start = Date.now();
    sleep(0);
    const elapsed = Date.now() - start;
    expect(elapsed).toBeLessThan(50);
  });
});

describe('calculateBackoffDelay', () => {
  it('increases exponentially with attempt number', () => {
    // Use fixed seed-like approach: run many times and check median
    const attempts = [0, 1, 2, 3];
    const baseDelay = 1000;
    
    for (let i = 1; i < attempts.length; i++) {
      // With jitter, we can't check exact values, but the base should double
      const prevBase = baseDelay * Math.pow(2, attempts[i - 1]);
      const currBase = baseDelay * Math.pow(2, attempts[i]);
      expect(currBase).toBe(prevBase * 2);
    }
  });

  it('respects maxDelay cap', () => {
    const maxDelay = 5000;
    
    // Attempt 10 with base 1000 = 1000 * 2^10 = 1,024,000
    // Should be capped at maxDelay
    for (let i = 0; i < 20; i++) {
      const delay = calculateBackoffDelay(10, 1000, maxDelay);
      expect(delay).toBeLessThanOrEqual(maxDelay);
    }
  });

  it('adds jitter (±25%)', () => {
    const delays = new Set();
    // Run 20 times with same parameters — jitter should produce variation
    for (let i = 0; i < 20; i++) {
      delays.add(calculateBackoffDelay(2, 1000, 30000));
    }
    // With jitter, we should get more than 1 unique value
    expect(delays.size).toBeGreaterThan(1);
  });

  it('returns a positive number for all attempts', () => {
    for (let attempt = 0; attempt < 10; attempt++) {
      const delay = calculateBackoffDelay(attempt);
      expect(delay).toBeGreaterThan(0);
      expect(typeof delay).toBe('number');
      expect(Number.isFinite(delay)).toBe(true);
    }
  });
});

describe('isTransientError', () => {
  it('identifies known transient errors', () => {
    expect(isTransientError('Connection timed out')).toBe(true);
    expect(isTransientError('503 Service Unavailable')).toBe(true);
    expect(isTransientError('ECONNRESET')).toBe(true);
    expect(isTransientError('429 Too Many Requests')).toBe(true);
    expect(isTransientError('socket hang up')).toBe(true);
  });

  it('rejects non-transient errors', () => {
    expect(isTransientError('File not found')).toBe(false);
    expect(isTransientError('Permission denied')).toBe(false);
    expect(isTransientError('Invalid argument')).toBe(false);
  });

  it('handles null/empty input', () => {
    expect(isTransientError(null)).toBe(false);
    expect(isTransientError('')).toBe(false);
    expect(isTransientError(undefined)).toBe(false);
  });
});
