/**
 * Ingestion Metrics Collector
 * 
 * Collects and exposes structured metrics for the ingestion pipeline.
 * Supports periodic snapshots, StatsD-compatible output, and Prometheus text format.
 * 
 * Usage:
 *   import { metrics } from './metrics-collector.js';
 *   metrics.increment('updates_ingested', 500);
 *   metrics.gauge('queue_depth', queue.length);
 *   metrics.timing('batch_latency_ms', elapsed);
 *   const snapshot = metrics.snapshot();
 */

const FLUSH_INTERVAL_MS = parseInt(process.env.METRICS_FLUSH_INTERVAL_MS) || 60000;

class MetricsCollector {
  constructor() {
    this.counters = {};
    this.gauges = {};
    this.timings = {};
    this.histograms = {};
    this.startTime = Date.now();
    this._flushCallbacks = [];
    this._flushTimer = null;
  }

  /**
   * Increment a counter by amount (default 1).
   */
  increment(name, amount = 1) {
    this.counters[name] = (this.counters[name] || 0) + amount;
  }

  /**
   * Set a gauge to a specific value.
   */
  gauge(name, value) {
    this.gauges[name] = value;
  }

  /**
   * Record a timing value (accumulates for percentile calculation).
   */
  timing(name, valueMs) {
    if (!this.timings[name]) {
      this.timings[name] = { count: 0, sum: 0, min: Infinity, max: -Infinity, values: [] };
    }
    const t = this.timings[name];
    t.count++;
    t.sum += valueMs;
    t.min = Math.min(t.min, valueMs);
    t.max = Math.max(t.max, valueMs);
    // Keep last 1000 values for percentile calculation
    if (t.values.length < 1000) {
      t.values.push(valueMs);
    } else {
      t.values[Math.floor(Math.random() * 1000)] = valueMs; // reservoir sampling
    }
  }

  /**
   * Record a value in a histogram bucket.
   */
  histogram(name, value, buckets = [10, 50, 100, 250, 500, 1000, 5000]) {
    if (!this.histograms[name]) {
      this.histograms[name] = { count: 0, sum: 0, buckets: {} };
      for (const b of buckets) {
        this.histograms[name].buckets[b] = 0;
      }
    }
    const h = this.histograms[name];
    h.count++;
    h.sum += value;
    for (const b of buckets) {
      if (value <= b) h.buckets[b]++;
    }
  }

  /**
   * Get percentile from timing data.
   */
  percentile(name, p) {
    const t = this.timings[name];
    if (!t || t.values.length === 0) return 0;
    const sorted = [...t.values].sort((a, b) => a - b);
    const idx = Math.ceil((p / 100) * sorted.length) - 1;
    return sorted[Math.max(0, idx)];
  }

  /**
   * Take a point-in-time snapshot of all metrics.
   */
  snapshot() {
    const uptimeSeconds = (Date.now() - this.startTime) / 1000;
    const timingSummaries = {};

    for (const [name, t] of Object.entries(this.timings)) {
      timingSummaries[name] = {
        count: t.count,
        avg: t.count > 0 ? (t.sum / t.count).toFixed(2) : 0,
        min: t.min === Infinity ? 0 : t.min,
        max: t.max === -Infinity ? 0 : t.max,
        p50: this.percentile(name, 50),
        p95: this.percentile(name, 95),
        p99: this.percentile(name, 99),
      };
    }

    return {
      ts: new Date().toISOString(),
      uptime_seconds: Math.round(uptimeSeconds),
      counters: { ...this.counters },
      gauges: { ...this.gauges },
      timings: timingSummaries,
      histograms: { ...this.histograms },
      memory_mb: Math.round(process.memoryUsage().heapUsed / 1024 / 1024),
      rss_mb: Math.round(process.memoryUsage().rss / 1024 / 1024),
    };
  }

  /**
   * Export in Prometheus text exposition format.
   */
  toPrometheus(prefix = 'canton_ingest') {
    const lines = [];

    for (const [name, value] of Object.entries(this.counters)) {
      lines.push(`# TYPE ${prefix}_${name}_total counter`);
      lines.push(`${prefix}_${name}_total ${value}`);
    }

    for (const [name, value] of Object.entries(this.gauges)) {
      lines.push(`# TYPE ${prefix}_${name} gauge`);
      lines.push(`${prefix}_${name} ${value}`);
    }

    for (const [name, t] of Object.entries(this.timings)) {
      lines.push(`# TYPE ${prefix}_${name} summary`);
      lines.push(`${prefix}_${name}{quantile="0.5"} ${this.percentile(name, 50)}`);
      lines.push(`${prefix}_${name}{quantile="0.95"} ${this.percentile(name, 95)}`);
      lines.push(`${prefix}_${name}{quantile="0.99"} ${this.percentile(name, 99)}`);
      lines.push(`${prefix}_${name}_count ${t.count}`);
      lines.push(`${prefix}_${name}_sum ${t.sum}`);
    }

    // Memory gauges
    const mem = process.memoryUsage();
    lines.push(`# TYPE ${prefix}_heap_bytes gauge`);
    lines.push(`${prefix}_heap_bytes ${mem.heapUsed}`);
    lines.push(`# TYPE ${prefix}_rss_bytes gauge`);
    lines.push(`${prefix}_rss_bytes ${mem.rss}`);

    return lines.join('\n') + '\n';
  }

  /**
   * Register a callback to run on each flush interval.
   */
  onFlush(callback) {
    this._flushCallbacks.push(callback);
  }

  /**
   * Start periodic flushing (logs snapshot at interval).
   */
  startPeriodicFlush(intervalMs = FLUSH_INTERVAL_MS) {
    if (this._flushTimer) return;
    this._flushTimer = setInterval(() => {
      const snap = this.snapshot();
      for (const cb of this._flushCallbacks) {
        try { cb(snap); } catch { }
      }
      // Default: log as structured JSON
      if (this._flushCallbacks.length === 0) {
        console.log(JSON.stringify({ type: 'metrics_flush', ...snap }));
      }
    }, intervalMs);
    this._flushTimer.unref?.(); // Don't keep process alive
  }

  /**
   * Stop periodic flushing.
   */
  stopPeriodicFlush() {
    if (this._flushTimer) {
      clearInterval(this._flushTimer);
      this._flushTimer = null;
    }
  }

  /**
   * Reset all metrics (for testing).
   */
  reset() {
    this.counters = {};
    this.gauges = {};
    this.timings = {};
    this.histograms = {};
    this.startTime = Date.now();
    this._flushCallbacks = [];
    this.stopPeriodicFlush();
  }
}

// Singleton instance
export const metrics = new MetricsCollector();

// Export class for testing
export { MetricsCollector };

export default metrics;
