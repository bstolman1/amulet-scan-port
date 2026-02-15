/**
 * Tests for MetricsCollector
 */
import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import { MetricsCollector } from '../metrics-collector.js';

describe('MetricsCollector', () => {
  let m;

  beforeEach(() => {
    m = new MetricsCollector();
  });

  afterEach(() => {
    m.reset();
  });

  describe('counters', () => {
    it('increments by 1 by default', () => {
      m.increment('requests');
      m.increment('requests');
      expect(m.snapshot().counters.requests).toBe(2);
    });

    it('increments by custom amount', () => {
      m.increment('bytes', 1024);
      m.increment('bytes', 2048);
      expect(m.snapshot().counters.bytes).toBe(3072);
    });
  });

  describe('gauges', () => {
    it('sets and overwrites gauge values', () => {
      m.gauge('queue_depth', 10);
      expect(m.snapshot().gauges.queue_depth).toBe(10);
      m.gauge('queue_depth', 5);
      expect(m.snapshot().gauges.queue_depth).toBe(5);
    });
  });

  describe('timings', () => {
    it('tracks min, max, avg, count', () => {
      m.timing('latency', 10);
      m.timing('latency', 30);
      m.timing('latency', 20);

      const snap = m.snapshot();
      expect(snap.timings.latency.count).toBe(3);
      expect(snap.timings.latency.min).toBe(10);
      expect(snap.timings.latency.max).toBe(30);
      expect(Number(snap.timings.latency.avg)).toBeCloseTo(20, 0);
    });

    it('computes percentiles correctly', () => {
      for (let i = 1; i <= 100; i++) {
        m.timing('response', i);
      }
      expect(m.percentile('response', 50)).toBe(50);
      expect(m.percentile('response', 95)).toBe(95);
      expect(m.percentile('response', 99)).toBe(99);
    });

    it('returns 0 for missing timing', () => {
      expect(m.percentile('nonexistent', 50)).toBe(0);
    });
  });

  describe('histograms', () => {
    it('counts values into buckets', () => {
      m.histogram('size', 5);
      m.histogram('size', 50);
      m.histogram('size', 500);
      m.histogram('size', 5000);

      const h = m.snapshot().histograms.size;
      expect(h.count).toBe(4);
      expect(h.buckets[10]).toBe(1);   // 5 <= 10
      expect(h.buckets[50]).toBe(2);   // 5,50 <= 50
      expect(h.buckets[500]).toBe(3);  // 5,50,500 <= 500
      expect(h.buckets[5000]).toBe(4); // all <= 5000
    });
  });

  describe('snapshot', () => {
    it('includes memory and uptime', () => {
      const snap = m.snapshot();
      expect(snap.memory_mb).toBeGreaterThanOrEqual(0);
      expect(snap.rss_mb).toBeGreaterThanOrEqual(0);
      expect(snap.uptime_seconds).toBeGreaterThanOrEqual(0);
      expect(snap.ts).toBeDefined();
    });
  });

  describe('Prometheus export', () => {
    it('outputs valid Prometheus text format', () => {
      m.increment('uploads_total', 42);
      m.gauge('queue_depth', 7);
      m.timing('batch_ms', 100);
      m.timing('batch_ms', 200);

      const text = m.toPrometheus('test');
      
      expect(text).toContain('# TYPE test_uploads_total_total counter');
      expect(text).toContain('test_uploads_total_total 42');
      expect(text).toContain('# TYPE test_queue_depth gauge');
      expect(text).toContain('test_queue_depth 7');
      expect(text).toContain('test_batch_ms_count 2');
      expect(text).toContain('test_heap_bytes');
    });
  });

  describe('periodic flush', () => {
    it('calls registered flush callbacks', async () => {
      const callback = vi.fn();
      m.onFlush(callback);
      m.startPeriodicFlush(50); // 50ms interval

      await new Promise(r => setTimeout(r, 120));
      m.stopPeriodicFlush();

      expect(callback).toHaveBeenCalled();
      const arg = callback.mock.calls[0][0];
      expect(arg.ts).toBeDefined();
      expect(arg.counters).toBeDefined();
    });
  });

  describe('reset', () => {
    it('clears all metrics', () => {
      m.increment('a', 10);
      m.gauge('b', 20);
      m.timing('c', 30);
      m.reset();

      const snap = m.snapshot();
      expect(snap.counters).toEqual({});
      expect(snap.gauges).toEqual({});
      expect(snap.timings).toEqual({});
    });
  });
});
