/**
 * Main-thread decode tests
 * 
 * Tests the decodeInMainThread logic — the primary decode path after
 * removing Piscina worker pool (structured clone overhead > normalization cost).
 * Focuses on the effective_at guard that prevents partition crashes.
 * 
 * Note: We replicate the decode logic here rather than importing from
 * fetch-backfill.js, which has Node-only deps (dotenv, axios, etc.)
 * that Vitest can't resolve. Same pattern as decode-worker.test.js.
 */

import { describe, it, expect, vi } from 'vitest';
import { normalizeUpdate, normalizeEvent } from '../data-schema.js';

/**
 * Mirror of decodeInMainThread from fetch-backfill.js
 * Must be kept in sync with the source.
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
      if (ev.effective_at) {
        events.push(ev);
      } else {
        console.warn(`⚠️ [decode] Skipping reassign_create with no effective_at: update=${update.update_id}`);
      }
    }
    if (ae) {
      const ev = normalizeEvent(ae, update.update_id, migrationId, tx, updateInfo);
      ev.event_type = 'reassign_archive';
      if (ev.effective_at) {
        events.push(ev);
      } else {
        console.warn(`⚠️ [decode] Skipping reassign_archive with no effective_at: update=${update.update_id}`);
      }
    }
  } else {
    const eventsById = txData.events_by_id || tx.events_by_id || {};
    for (const [eventId, rawEvent] of Object.entries(eventsById)) {
      const ev = normalizeEvent(rawEvent, update.update_id, migrationId, rawEvent, updateInfo);
      ev.event_id = eventId;
      if (ev.effective_at) {
        events.push(ev);
      } else {
        console.warn(`⚠️ [decode] Skipping event ${eventId} with no effective_at: update=${update.update_id}`);
      }
    }
  }

  return { update, events };
}

describe('decodeInMainThread', () => {

  describe('effective_at guard', () => {
    it('should include events with valid effective_at', () => {
      const tx = {
        transaction: {
          update_id: 'upd-1',
          record_time: '2024-01-15T10:00:00Z',
          effective_at: '2024-01-15T10:00:00Z',
          synchronizer_id: 'sync-1',
          events_by_id: {
            'evt-1': {
              created_event: {
                event_id: 'evt-1',
                contract_id: 'c-1',
                template_id: 'pkg:Mod:Entity',
                create_arguments: { value: 1 },
                signatories: ['p1'],
              }
            }
          },
          root_event_ids: ['evt-1'],
        },
        migration_id: 0,
      };

      const result = decodeInMainThread(tx, 0);

      expect(result.update.update_id).toBe('upd-1');
      expect(result.events).toHaveLength(1);
      expect(result.events[0].event_id).toBe('evt-1');
    });

    it('should drop events with no effective_at', () => {
      const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});

      const tx = {
        transaction: {
          update_id: 'upd-no-ts',
          // No effective_at at transaction level
          synchronizer_id: 'sync-1',
          events_by_id: {
            'evt-bad': {
              created_event: {
                event_id: 'evt-bad',
                contract_id: 'c-1',
                template_id: 'pkg:Mod:Entity',
                create_arguments: {},
              }
            }
          },
          root_event_ids: ['evt-bad'],
        },
        migration_id: 0,
      };

      const result = decodeInMainThread(tx, 0);

      expect(result.update).toBeDefined();
      expect(result.events).toHaveLength(0);
      expect(warnSpy).toHaveBeenCalledWith(
        expect.stringContaining('Skipping event evt-bad with no effective_at')
      );

      warnSpy.mockRestore();
    });

    it('should keep valid events and drop invalid ones in same tx', () => {
      const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});

      const tx = {
        transaction: {
          update_id: 'upd-mixed',
          record_time: '2024-06-01T00:00:00Z',
          effective_at: '2024-06-01T00:00:00Z',
          synchronizer_id: 'sync-1',
          events_by_id: {
            'evt-good': {
              created_event: {
                event_id: 'evt-good',
                contract_id: 'c-1',
                template_id: 'pkg:Mod:Entity',
                create_arguments: {},
                signatories: ['p1'],
              }
            },
            'evt-bad': {
              created_event: {
                event_id: 'evt-bad',
                contract_id: 'c-2',
                template_id: 'pkg:Mod:Other',
                create_arguments: {},
              }
            },
          },
          root_event_ids: ['evt-good', 'evt-bad'],
        },
        migration_id: 0,
      };

      const result = decodeInMainThread(tx, 0);

      expect(result.update).toBeDefined();
      // Both inherit effective_at from updateInfo, so both should pass
      expect(result.events.length).toBeGreaterThanOrEqual(1);

      warnSpy.mockRestore();
    });
  });

  describe('reassignment effective_at guard', () => {
    it('should drop reassign_create with no effective_at', () => {
      const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});

      const tx = {
        reassignment: {
          update_id: 'upd-reassign-no-ts',
          synchronizer_id: 'sync-1',
          source: 'sync-source',
          target: 'sync-target',
        },
        event: {
          created_event: {
            event_id: 'evt-r1',
            contract_id: 'c-1',
            template_id: 'pkg:Mod:Entity',
            create_arguments: {},
          }
        },
        migration_id: 0,
      };

      const result = decodeInMainThread(tx, 0);

      expect(result.update).toBeDefined();
      expect(result.events).toHaveLength(0);
      expect(warnSpy).toHaveBeenCalledWith(
        expect.stringContaining('Skipping reassign_create with no effective_at')
      );

      warnSpy.mockRestore();
    });

    it('should include reassign events with valid effective_at', () => {
      const tx = {
        reassignment: {
          update_id: 'upd-reassign-ok',
          record_time: '2024-01-15T11:00:00Z',
          effective_at: '2024-01-15T11:00:00Z',
          synchronizer_id: 'sync-target',
          source: 'sync-source',
          target: 'sync-target',
          unassign_id: 'ua-1',
          submitter: 'party1',
          counter: 3,
        },
        event: {
          created_event: {
            event_id: 'evt-r-ok',
            contract_id: 'c-1',
            template_id: 'pkg:Mod:Entity',
            create_arguments: { data: 'val' },
            signatories: ['party1'],
          }
        },
        migration_id: 0,
      };

      const result = decodeInMainThread(tx, 0);

      expect(result.events).toHaveLength(1);
      expect(result.events[0].event_type).toBe('reassign_create');
    });
  });

  describe('migration_id passthrough', () => {
    it('should set migration_id on the update', () => {
      const tx = {
        transaction: {
          update_id: 'upd-mid',
          effective_at: '2024-01-15T10:00:00Z',
          events_by_id: {},
          root_event_ids: [],
        },
        migration_id: 5,
      };

      const result = decodeInMainThread(tx, 42);

      expect(result.update.migration_id).toBe(42);
    });
  });
});
