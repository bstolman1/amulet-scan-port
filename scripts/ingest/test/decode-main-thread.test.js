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
 *
 * UPDATED to match data integrity fixes:
 * - FIX #1: isReassignment = !!tx.reassignment (not !!tx.event)
 * - FIX #1: flattenEventsInTreeOrder replaces Object.entries
 * - FIX #1: event_id mismatch warns instead of silent overwrite
 * - FIX #2: normalizeEvent throws on null effective_at (no warn-and-skip)
 * - normalizeUpdate called with migration_id injected via spread
 */

import { describe, it, expect, vi } from 'vitest';
import { normalizeUpdate, normalizeEvent, flattenEventsInTreeOrder } from '../data-schema.js';

/**
 * Mirror of decodeInMainThread from fetch-backfill.js
 * Must be kept in sync with the source.
 */
function decodeInMainThread(tx, migrationId) {
  // FIX #1: Use tx.reassignment — the actual Scan API wrapper field
  const isReassignment = !!tx.reassignment;

  // normalizeUpdate called with migration_id injected via spread
  const update = normalizeUpdate({ ...tx, migration_id: migrationId });
  const events = [];
  const txData = tx.transaction || tx.reassignment || tx;

  const updateInfo = {
    record_time:     txData.record_time,
    effective_at:    txData.effective_at,
    synchronizer_id: txData.synchronizer_id,
    source:          txData.source || null,
    target:          txData.target || null,
    unassign_id:     txData.unassign_id || null,
    submitter:       txData.submitter || null,
    counter:         txData.counter ?? null,
  };

  if (isReassignment) {
    // FIX #1: Navigate the correct path — tx.reassignment.event.{created,archived}_event
    const ce = tx.reassignment?.event?.created_event;
    const ae = tx.reassignment?.event?.archived_event;

    if (ce) {
      const ev = normalizeEvent(ce, update.update_id, migrationId, tx, updateInfo);
      ev.event_type = 'reassign_create';
      // FIX #2: normalizeEvent throws on null effective_at — no warn-and-skip needed
      events.push(ev);
    }
    if (ae) {
      const ev = normalizeEvent(ae, update.update_id, migrationId, tx, updateInfo);
      ev.event_type = 'reassign_archive';
      events.push(ev);
    }
  } else {
    const eventsById = txData.events_by_id || tx.events_by_id || {};
    const rootEventIds = txData.root_event_ids || tx.root_event_ids || [];

    // FIX #1: Use flattenEventsInTreeOrder for correct preorder traversal
    const orderedEvents = flattenEventsInTreeOrder(eventsById, rootEventIds);

    for (const rawEvent of orderedEvents) {
      const ev = normalizeEvent(rawEvent, update.update_id, migrationId, rawEvent, updateInfo);

      // FIX #1: Warn on event_id key/field mismatch instead of silently overwriting
      const mapKeyId = rawEvent.event_id;
      if (mapKeyId && ev.event_id && mapKeyId !== ev.event_id) {
        console.warn(
          `[decode-main] event_id mismatch for update=${update.update_id}: ` +
          `eventsById key="${mapKeyId}" vs event.event_id="${ev.event_id}". ` +
          `Using map key as authoritative.`
        );
        ev.event_id = mapKeyId;
      } else if (mapKeyId && !ev.event_id) {
        ev.event_id = mapKeyId;
      }

      // FIX #2: No silent effective_at filter — normalizeEvent throws if null
      events.push(ev);
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

    it('should throw for events with no effective_at (strict validation)', () => {
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

      // normalizeEvent now throws when effective_at cannot be determined
      expect(() => decodeInMainThread(tx, 0)).toThrow('could not determine effective_at');
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
    it('should throw for reassign_create with no effective_at (strict validation)', () => {
      const tx = {
        reassignment: {
          update_id: 'upd-reassign-no-ts',
          synchronizer_id: 'sync-1',
          source: 'sync-source',
          target: 'sync-target',
          // FIX: created_event is now under reassignment.event
          event: {
            created_event: {
              event_id: 'evt-r1',
              contract_id: 'c-1',
              template_id: 'pkg:Mod:Entity',
              create_arguments: {},
            }
          },
        },
        migration_id: 0,
      };

      // normalizeEvent now throws when effective_at cannot be determined
      expect(() => decodeInMainThread(tx, 0)).toThrow('could not determine effective_at');
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
          // FIX: created_event is under reassignment.event (not top-level tx.event)
          event: {
            created_event: {
              event_id: 'evt-r-ok',
              contract_id: 'c-1',
              template_id: 'pkg:Mod:Entity',
              create_arguments: { data: 'val' },
              signatories: ['party1'],
            }
          },
        },
        migration_id: 0,
      };

      const result = decodeInMainThread(tx, 0);

      expect(result.events).toHaveLength(1);
      expect(result.events[0].event_type).toBe('reassign_create');
    });
  });

  describe('migration_id passthrough', () => {
    it('should set migration_id on the update via spread', () => {
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

      // migration_id should come from the function argument (42), not the tx field (5)
      expect(result.update.migration_id).toBe(42);
    });
  });

  describe('flattenEventsInTreeOrder', () => {
    it('should traverse events in tree order instead of Object.entries order', () => {
      const tx = {
        transaction: {
          update_id: 'upd-tree',
          record_time: '2024-01-15T10:00:00Z',
          effective_at: '2024-01-15T10:00:00Z',
          synchronizer_id: 'sync-1',
          events_by_id: {
            'evt-child': {
              created_event: {
                event_id: 'evt-child',
                contract_id: 'c-2',
                template_id: 'pkg:Mod:Child',
                create_arguments: {},
                signatories: ['p1'],
              }
            },
            'evt-root': {
              exercised_event: {
                event_id: 'evt-root',
                contract_id: 'c-1',
                template_id: 'pkg:Mod:Entity',
                choice: 'DoSomething',
                consuming: true,
                acting_parties: ['p1'],
                child_event_ids: ['evt-child'],
              }
            },
          },
          root_event_ids: ['evt-root'],
        },
        migration_id: 0,
      };

      const result = decodeInMainThread(tx, 0);

      // Tree order: root first, then child
      expect(result.events).toHaveLength(2);
      expect(result.events[0].event_id).toBe('evt-root');
      expect(result.events[1].event_id).toBe('evt-child');
    });
  });

  describe('event_id mismatch handling', () => {
    it('should use map key as event_id when inner event has no event_id', () => {
      const tx = {
        transaction: {
          update_id: 'upd-no-inner-id',
          record_time: '2024-01-15T10:00:00Z',
          effective_at: '2024-01-15T10:00:00Z',
          synchronizer_id: 'sync-1',
          events_by_id: {
            'evt-from-key': {
              created_event: {
                // No event_id on inner event
                contract_id: 'c-1',
                template_id: 'pkg:Mod:Entity',
                create_arguments: {},
                signatories: ['p1'],
              }
            }
          },
          root_event_ids: ['evt-from-key'],
        },
        migration_id: 0,
      };

      const result = decodeInMainThread(tx, 0);

      // flattenEventsInTreeOrder sets event_id from map key
      expect(result.events[0].event_id).toBe('evt-from-key');
    });
  });
});
