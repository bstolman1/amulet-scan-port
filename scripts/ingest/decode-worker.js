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
import { normalizeUpdate, normalizeEvent, flattenEventsInTreeOrder } from './data-schema.js';

/**
 * Decode a batch of transactions into normalized updates + events.
 *
 * @param {Object}   params             - Task parameters
 * @param {Object[]} params.txs         - Array of raw transactions
 * @param {number}   params.migrationId - Migration ID for all txs in this batch
 * @returns {{ updates: object[], events: object[], errors: object[] }}
 *   Partial results are always returned — a bad tx is collected in `errors`
 *   rather than aborting the entire batch.
 */
export default async function decodeBatchTask({ txs, migrationId }) {
  const updates = [];
  const events = [];
  const errors = [];

  // FIX #2: Capture a single stable timestamp for the whole batch so that
  // recorded_at/timestamp are consistent across every update and event,
  // rather than drifting as new Date() is called per-record.
  const batchTimestamp = new Date();
  const normalizeOptions = { batchTimestamp };

  for (const tx of txs) {
    // FIX #7: Wrap each transaction in try/catch so one malformed tx cannot
    // kill the entire batch. Errors are collected and returned to the caller
    // alongside whatever partial results were successfully decoded.
    try {
      // FIX #1: Use tx.reassignment (the actual Scan API wrapper field) rather
      // than tx.event, which is unrelated and could produce false positives.
      const isReassignment = !!tx.reassignment;

      // FIX #4: Pass migrationId via options spread so normalizeUpdate handles
      // it in one place rather than mutating the returned object post-hoc.
      // Post-hoc mutation risks divergence if the tx carries its own migration_id.
      const update = normalizeUpdate({ ...tx, migration_id: migrationId }, normalizeOptions);
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
        const ce = tx.reassignment?.event?.created_event;
        const ae = tx.reassignment?.event?.archived_event;

        if (ce) {
          // FIX #2: Pass normalizeOptions (includes batchTimestamp)
          const ev = normalizeEvent(ce, update.update_id, migrationId, tx, updateInfo, normalizeOptions);
          ev.event_type = 'reassign_create';
          // FIX #3: normalizeEvent now throws on null effective_at, so this
          // guard will never suppress a valid event. Remove the silent filter
          // and trust the upstream contract. If effective_at were somehow null,
          // the throw above would have already caught it with full context.
          events.push(ev);
        }

        if (ae) {
          // FIX #2: Pass normalizeOptions (includes batchTimestamp)
          const ev = normalizeEvent(ae, update.update_id, migrationId, tx, updateInfo, normalizeOptions);
          ev.event_type = 'reassign_archive';
          events.push(ev);
        }

      } else {
        const eventsById = txData.events_by_id || tx.events_by_id || {};
        const rootEventIds = txData.root_event_ids || tx.root_event_ids || [];

        // FIX #5: Use flattenEventsInTreeOrder to preserve preorder tree traversal
        // per Scan API docs (root_event_ids → child_event_ids). Object.entries
        // iteration order is not guaranteed to be semantically tree-ordered.
        const orderedEvents = flattenEventsInTreeOrder(eventsById, rootEventIds);

        for (const rawEvent of orderedEvents) {
          // flattenEventsInTreeOrder already attaches event_id as rawEvent.event_id
          // FIX #2: Pass normalizeOptions (includes batchTimestamp)
          const ev = normalizeEvent(rawEvent, update.update_id, migrationId, rawEvent, updateInfo, normalizeOptions);

          // FIX #6: normalizeEvent reads event_id from rawEvent (which flattenEventsInTreeOrder
          // already populated from the eventsById key). Only overwrite if they differ,
          // and warn so key/field mismatches in malformed API responses are visible.
          const mapKeyId = rawEvent.event_id;
          if (mapKeyId && ev.event_id && mapKeyId !== ev.event_id) {
            console.warn(
              `[decode-worker] event_id mismatch for update=${update.update_id}: ` +
              `eventsById key="${mapKeyId}" vs event.event_id="${ev.event_id}". ` +
              `Using map key as authoritative (structural API identifier).`
            );
            ev.event_id = mapKeyId;
          } else if (mapKeyId && !ev.event_id) {
            // normalizeEvent couldn't find an event_id — use the map key
            ev.event_id = mapKeyId;
          }

          // FIX #3: Remove silent effective_at filter. normalizeEvent throws on
          // null effective_at, so this condition can never be false for a
          // correctly-normalized event. Keeping it as a silent filter would mask
          // bugs and contradict the upstream contract.
          events.push(ev);
        }
      }

    } catch (err) {
      // FIX #7: Collect per-tx errors rather than propagating, so the rest of
      // the batch is not lost. The caller should inspect errors[] and decide
      // whether to retry, dead-letter, or abort.
      const txId = tx?.update_id || tx?.transaction?.update_id || tx?.reassignment?.update_id || 'UNKNOWN';
      console.error(`[decode-worker] Failed to decode tx ${txId}: ${err.message}`);
      errors.push({
        tx_id: txId,
        error: err.message,
        stack: err.stack,
      });
    }
  }

  return { updates, events, errors };
}
