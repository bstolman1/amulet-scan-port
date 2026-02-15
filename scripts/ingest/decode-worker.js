/**
 * Decode Worker - Piscina Worker Thread (BATCHED)
 * 
 * Processes BATCHES of transactions per message to amortize
 * structured clone serialization overhead.
 * 
 * Previous approach: 1 tx per message = 1000 postMessage calls per page
 * New approach: ~250 txs per message = 4 postMessage calls per page
 * This keeps decode off the main thread (freeing event loop for HTTP I/O)
 * while cutting serialization overhead by ~250x.
 */

import { normalizeUpdate, normalizeEvent } from './data-schema.js';

/**
 * Decode a batch of transactions into normalized updates + events
 * @param {Object} params - Task parameters
 * @param {Object[]} params.txs - Array of raw transactions
 * @param {number} params.migrationId - Migration ID
 * @returns {Object} - { updates: [], events: [] }
 */
export default async function decodeBatchTask({ txs, migrationId }) {
  const updates = [];
  const events = [];

  for (const tx of txs) {
    const isReassignment = !!tx.event;

    const update = normalizeUpdate(tx);
    update.migration_id = migrationId;
    updates.push(update);

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
        }
      }

      if (ae) {
        const ev = normalizeEvent(ae, update.update_id, migrationId, tx, updateInfo);
        ev.event_type = 'reassign_archive';
        if (ev.effective_at) {
          events.push(ev);
        }
      }
    } else {
      const eventsById = txData.events_by_id || tx.events_by_id || {};
      for (const [eventId, rawEvent] of Object.entries(eventsById)) {
        const ev = normalizeEvent(rawEvent, update.update_id, migrationId, rawEvent, updateInfo);
        ev.event_id = eventId;
        if (ev.effective_at) {
          events.push(ev);
        }
      }
    }
  }

  return { updates, events };
}
