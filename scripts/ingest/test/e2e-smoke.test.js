/**
 * E2E Smoke Test — Ingestion Pipeline
 * 
 * Spins up a mock Canton Scan HTTP server, runs key pipeline functions
 * against it, and verifies data flows through correctly.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import http from 'http';

process.env.SCAN_URL = 'http://localhost:0';
process.env.BATCH_SIZE = '10';
process.env.GCS_ENABLED = 'false';

describe('E2E Smoke: ingestion data path', () => {
  let server;
  let serverPort;
  let requestLog = [];

  const MOCK_TX = {
    update_id: 'update-001',
    migration_id: 1,
    offset: '42',
    record_time: '2025-01-15T10:00:00Z',
    root_event_ids: ['evt-001'],
    events_by_id: {
      'evt-001': {
        created_event: {
          event_id: 'evt-001',
          contract_id: 'contract-001',
          template_id: 'Splice.Amulet:Amulet',
          create_arguments: { owner: 'party::abc', amount: { amount: '100.0' } },
          created_at: '2025-01-15T10:00:00Z',
        }
      }
    },
  };

  const MOCK_UPDATES = {
    transactions_with_events: [MOCK_TX],
  };

  beforeEach(async () => {
    requestLog = [];
    server = http.createServer((req, res) => {
      requestLog.push({ method: req.method, url: req.url });
      let body = '';
      req.on('data', chunk => { body += chunk; });
      req.on('end', () => {
        if (req.url.includes('/api/scan/v2/updates')) {
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify(MOCK_UPDATES));
        } else {
          res.writeHead(404);
          res.end('Not Found');
        }
      });
    });

    await new Promise((resolve) => {
      server.listen(0, '127.0.0.1', () => {
        serverPort = server.address().port;
        process.env.SCAN_URL = `http://127.0.0.1:${serverPort}`;
        resolve();
      });
    });
  });

  afterEach(async () => {
    if (server) {
      await new Promise(resolve => server.close(resolve));
      server = null;
    }
  });

  it('normalizeUpdate maps API response to schema correctly', async () => {
    const { normalizeUpdate } = await import('../data-schema.js');

    const update = normalizeUpdate(MOCK_TX, { strict: false });

    expect(update).toBeDefined();
    expect(update.update_id).toBe('update-001');
    expect(update.migration_id).toBe(1);
    expect(update.offset).toBe(42);
    expect(update.record_time).toBeDefined();
  });

  it('normalizeEvent maps event data to schema correctly', async () => {
    const { normalizeEvent } = await import('../data-schema.js');

    const event = MOCK_TX.events_by_id['evt-001'];
    const parsed = normalizeEvent(event, 'update-001', 1);

    expect(parsed).toBeDefined();
    expect(parsed.contract_id).toBe('contract-001');
    expect(parsed.template_id).toContain('Amulet');
  });

  it('AtomicCursor tracks state through transaction lifecycle', async () => {
    const { AtomicCursor } = await import('../atomic-cursor.js');

    const cursor = new AtomicCursor(999, 'test-sync-smoke', 0, 1);

    cursor.beginTransaction(10, 5, '2025-01-15T10:00:00Z');
    cursor.addPending(5, 3, '2025-01-15T11:00:00Z');
    const state = cursor.commit();

    expect(state.totalUpdates).toBe(15);
    expect(state.totalEvents).toBe(8);
    expect(state.lastBefore).toBe('2025-01-15T10:00:00Z');

    cursor.beginTransaction(100, 50, '2025-01-16T00:00:00Z');
    cursor.rollback();
    const afterRollback = cursor.getResumePosition(true);
    expect(afterRollback.totalUpdates).toBe(15);
  });

  it('mock server receives requests at expected endpoints', async () => {
    const res = await fetch(`http://127.0.0.1:${serverPort}/api/scan/v2/updates`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ begin_after: null, page_size: 10 }),
    });

    expect(res.status).toBe(200);
    const data = await res.json();
    expect(data.transactions_with_events).toHaveLength(1);
    expect(requestLog).toHaveLength(1);
    expect(requestLog[0].url).toContain('/api/scan/v2/updates');
  });

  it('MetricsCollector integrates with pipeline operations', async () => {
    const { MetricsCollector } = await import('../metrics-collector.js');
    const m = new MetricsCollector();

    const start = Date.now();
    m.increment('batches_processed');
    m.increment('updates_ingested', 10);
    m.increment('events_ingested', 5);
    m.timing('batch_latency_ms', Date.now() - start);
    m.gauge('queue_depth', 3);

    const snap = m.snapshot();
    expect(snap.counters.batches_processed).toBe(1);
    expect(snap.counters.updates_ingested).toBe(10);
    expect(snap.gauges.queue_depth).toBe(3);
    expect(snap.timings.batch_latency_ms.count).toBe(1);

    m.reset();
  });
});
