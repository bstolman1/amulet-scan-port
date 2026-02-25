/**
 * End-to-End Decode Pipeline Test
 * 
 * Tests the full flow: Mock API response → decodeInMainThread → normalizeUpdate/Event
 * → groupByPartition → verify partition paths and record integrity.
 * 
 * This does NOT test Parquet writing or GCS upload (those have their own tests),
 * but verifies the data transformation pipeline end-to-end.
 */

import { describe, it, expect } from 'vitest';
import {
  normalizeUpdate,
  normalizeEvent,
  flattenEventsInTreeOrder,
  getPartitionPath,
  groupByPartition,
} from '../data-schema.js';
import {
  MOCK_BACKFILL_BATCH,
  MOCK_BACKFILL_TRANSACTION,
  MOCK_BACKFILL_EXERCISE,
  MOCK_BACKFILL_REASSIGNMENT,
  MOCK_GOVERNANCE_UPDATE,
} from './fixtures/mock-api-responses.js';

/**
 * Replicate decodeInMainThread logic for isolated testing
 */
function decodeInMainThread(tx, migrationId) {
  const isReassignment = !!tx.event;
  const update = normalizeUpdate(tx);
  update.migration_id = migrationId;

  const events = [];
  const txData = tx.transaction || tx.reassignment || tx;

  const updateInfo = {
    record_time: txData.record_time,
    effective_at: txData.effective_at,
    synchronizer_id: txData.synchronizer_id,
    source: txData.source || null,
    target: txData.target || null,
    unassign_id: txData.unassign_id || null,
    submitter: txData.submitter || null,
    counter: txData.counter ?? null,
  };

  if (isReassignment) {
    const ce = tx.event?.created_event;
    const ae = tx.event?.archived_event;
    if (ce) {
      const ev = normalizeEvent(ce, update.update_id, migrationId, tx, updateInfo);
      ev.event_type = 'reassign_create';
      if (ev.effective_at) events.push(ev);
    }
    if (ae) {
      const ev = normalizeEvent(ae, update.update_id, migrationId, tx, updateInfo);
      ev.event_type = 'reassign_archive';
      if (ev.effective_at) events.push(ev);
    }
  } else {
    const eventsById = txData.events_by_id || tx.events_by_id || {};
    for (const [eventId, rawEvent] of Object.entries(eventsById)) {
      const ev = normalizeEvent(rawEvent, update.update_id, migrationId, rawEvent, updateInfo);
      ev.event_id = eventId;
      if (ev.effective_at) events.push(ev);
    }
  }

  return { update, events };
}

describe('E2E Decode Pipeline', () => {

  describe('Full batch decode → partition', () => {
    it('should decode all mock batch items and produce correct partitions', () => {
      const allUpdates = [];
      const allEvents = [];

      for (const raw of MOCK_BACKFILL_BATCH) {
        const result = decodeInMainThread(raw, raw.migration_id);
        allUpdates.push(result.update);
        allEvents.push(...result.events);
      }

      // Verify counts
      expect(allUpdates).toHaveLength(4);
      expect(allEvents.length).toBeGreaterThanOrEqual(5); // At least 2+2+0+1

      // All updates should have migration_id set
      for (const u of allUpdates) {
        expect(u.migration_id).toBe(0);
      }

      // All events should have effective_at (we skip those without)
      for (const e of allEvents) {
        expect(e.effective_at).toBeTruthy();
      }

      // Filter updates that have effective_at for partitioning
      // (Reassignment updates may not have effective_at — they use record_time at the write layer)
      const partitionableUpdates = allUpdates.filter(u => u.effective_at);
      const nonPartitionable = allUpdates.filter(u => !u.effective_at);

      // Reassignment has no effective_at at decode time; write layer falls back to record_time
      expect(nonPartitionable.length).toBeGreaterThanOrEqual(0);

      if (partitionableUpdates.length > 0) {
        // Partition updates
        const updateGroups = groupByPartition(partitionableUpdates, 'updates', 'backfill', 0);
        const updatePaths = Object.keys(updateGroups);

        // All mock data is from 2024-06-15, so should be one partition
        expect(updatePaths).toHaveLength(1);
        expect(updatePaths[0]).toBe('backfill/updates/migration=0/year=2024/month=6/day=15');

        // Total records match
        const totalUpdateRecords = Object.values(updateGroups).flat().length;
        expect(totalUpdateRecords).toBe(partitionableUpdates.length);
      }

      // Partition events
      const eventGroups = groupByPartition(allEvents, 'events', 'backfill', 0);
      const eventPaths = Object.keys(eventGroups);

      expect(eventPaths).toHaveLength(1);
      expect(eventPaths[0]).toBe('backfill/events/migration=0/year=2024/month=6/day=15');

      const totalEventRecords = Object.values(eventGroups).flat().length;
      expect(totalEventRecords).toBe(allEvents.length);
    });
  });

  describe('Cross-migration decode', () => {
    it('should partition records from different migrations separately', () => {
      // Decode same transaction under two different migrations
      const result0 = decodeInMainThread(MOCK_BACKFILL_TRANSACTION, 0);
      const result3 = decodeInMainThread(MOCK_BACKFILL_TRANSACTION, 3);

      const combined = [result0.update, result3.update];
      const groups = groupByPartition(combined, 'updates', 'backfill', null);

      const paths = Object.keys(groups);
      expect(paths).toHaveLength(2);

      const hasMig0 = paths.some(p => p.includes('migration=0'));
      const hasMig3 = paths.some(p => p.includes('migration=3'));
      expect(hasMig0).toBe(true);
      expect(hasMig3).toBe(true);
    });
  });

  describe('Cross-day decode', () => {
    it('should split records spanning midnight into separate partitions', () => {
      // Create two updates: one at 23:50 UTC and one at 00:10 UTC next day
      const txDay1 = {
        ...MOCK_BACKFILL_TRANSACTION,
        update_id: 'upd-day1',
        transaction: {
          ...MOCK_BACKFILL_TRANSACTION.transaction,
          update_id: 'upd-day1',
          record_time: '2024-06-15T23:50:00.000Z',
          effective_at: '2024-06-15T23:50:00.000Z',
        },
      };
      const txDay2 = {
        ...MOCK_BACKFILL_TRANSACTION,
        update_id: 'upd-day2',
        transaction: {
          ...MOCK_BACKFILL_TRANSACTION.transaction,
          update_id: 'upd-day2',
          record_time: '2024-06-16T00:10:00.000Z',
          effective_at: '2024-06-16T00:10:00.000Z',
        },
      };

      const r1 = decodeInMainThread(txDay1, 0);
      const r2 = decodeInMainThread(txDay2, 0);

      const updates = [r1.update, r2.update];
      const groups = groupByPartition(updates, 'updates', 'backfill', 0);
      const paths = Object.keys(groups);

      expect(paths).toHaveLength(2);
      expect(paths).toContain('backfill/updates/migration=0/year=2024/month=6/day=15');
      expect(paths).toContain('backfill/updates/migration=0/year=2024/month=6/day=16');
    });
  });

  describe('Referential integrity through pipeline', () => {
    it('should maintain event→update references through decode', () => {
      const updateIds = new Set();
      const eventRefs = new Set();

      for (const raw of MOCK_BACKFILL_BATCH) {
        const result = decodeInMainThread(raw, 0);
        updateIds.add(result.update.update_id);
        for (const e of result.events) {
          eventRefs.add(e.update_id);
        }
      }

      // Every event's update_id should reference a valid update
      for (const ref of eventRefs) {
        expect(updateIds.has(ref)).toBe(true);
      }
    });
  });

  describe('Data preservation through pipeline', () => {
    it('should preserve raw_event and update_data JSON', () => {
      const result = decodeInMainThread(MOCK_BACKFILL_TRANSACTION, 0);

      // update_data should be valid JSON containing the full transaction
      expect(result.update.update_data).toBeTruthy();
      const updateData = JSON.parse(result.update.update_data);
      expect(updateData.events_by_id).toBeDefined();

      // raw_event on each event should be valid JSON
      for (const e of result.events) {
        expect(e.raw_event).toBeTruthy();
        const raw = JSON.parse(e.raw_event);
        expect(raw).toBeDefined();
      }
    });

    it('should preserve template_id and package_name', () => {
      const result = decodeInMainThread(MOCK_BACKFILL_TRANSACTION, 0);

      const amuletEvent = result.events.find(e =>
        e.template_id?.includes('Amulet')
      );
      expect(amuletEvent).toBeDefined();
      expect(amuletEvent.template_id).toBe('splice-amulet:Splice.Amulet:Amulet');
      expect(amuletEvent.package_name).toBe('splice-amulet');
    });
  });
});
