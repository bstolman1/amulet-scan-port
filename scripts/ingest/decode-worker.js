/**
 * Decode Worker - Piscina Worker Thread
 * 
 * Handles CPU-intensive normalization of ledger transactions
 * in parallel worker threads for maximum throughput.
 */

import { normalizeUpdate, normalizeEvent } from './parquet-schema.js';

/**
 * Decode a single transaction into normalized update + events
 * @param {Object} params - Task parameters
 * @param {Object} params.tx - Raw transaction data
 * @param {number} params.migrationId - Migration ID
 * @returns {Object} - { update, events }
 */
export default async function decodeTask({ tx, migrationId }) {
  const isReassignment = !!tx.event;

  // Normalize update record
  const update = normalizeUpdate(tx);
  update.migration_id = migrationId;

  const events = [];

  // Extract base data used for event timing/context
  const txData = tx.transaction || tx.reassignment || tx;

  const updateInfo = {
    record_time: txData.record_time,
    effective_at: txData.effective_at,
    synchronizer_id: txData.synchronizer_id,
    // Reassignment-specific fields
    source: txData.source || null,
    target: txData.target || null,
    unassign_id: txData.unassign_id || null,
    submitter: txData.submitter || null,
    counter: txData.counter ?? null,
  };

  if (isReassignment) {
    // Handle reassignment events
    const ce = tx.event?.created_event;
    const ae = tx.event?.archived_event;

    if (ce) {
      const ev = normalizeEvent(ce, update.update_id, migrationId, tx, updateInfo);
      ev.event_type = 'reassign_create';
      events.push(ev);
    }

    if (ae) {
      const ev = normalizeEvent(ae, update.update_id, migrationId, tx, updateInfo);
      ev.event_type = 'reassign_archive';
      events.push(ev);
    }
  } else {
    // Handle regular transaction events
    const eventsById = txData.events_by_id || tx.events_by_id || {};
    for (const [eventId, rawEvent] of Object.entries(eventsById)) {
      const ev = normalizeEvent(rawEvent, update.update_id, migrationId, rawEvent, updateInfo);
      ev.event_id = eventId;
      events.push(ev);
    }
  }

  return { update, events };
}
