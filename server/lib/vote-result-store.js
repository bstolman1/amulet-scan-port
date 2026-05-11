/**
 * Vote Result Store — DuckDB persistence for historical SV vote results
 *
 * Stores completed vote results in the existing `vote_requests` table.
 * Each result is keyed by tracking_cid (unique per proposal).
 *
 * Storage strategy:
 *   - Structured columns for filtering (status, action_tag, requester, etc.)
 *   - Full raw VoteResult JSON in `payload` for lossless frontend parsing
 *   - ON CONFLICT upsert so re-syncs are idempotent
 */

import { query } from '../duckdb/connection.js';
import { initEngineSchema } from '../engine/schema.js';

let schemaReady = false;

async function ensureSchema() {
  if (schemaReady) return;
  await initEngineSchema();
  schemaReady = true;
}

/**
 * Map a Scan API outcome tag to a status string.
 */
function outcomeToStatus(outcomeTag) {
  if (outcomeTag === 'VRO_Accepted') return 'accepted';
  if (outcomeTag === 'VRO_Rejected') return 'rejected';
  return 'expired';
}

/**
 * Upsert an array of raw Scan API VoteResult objects into the vote_requests table.
 * Fire-and-forget safe — logs errors but never throws.
 *
 * @param {Array} results — raw `dso_rules_vote_results` from the Scan API
 * @returns {{ inserted: number, skipped: number }}
 */
export async function upsertVoteResults(results) {
  if (!results || !Array.isArray(results) || results.length === 0) {
    return { inserted: 0, skipped: 0 };
  }

  await ensureSchema();

  let inserted = 0;
  let skipped = 0;

  for (const result of results) {
    try {
      const request = result?.request || {};
      const action = request?.action || {};
      const votes = request?.votes || [];
      const outcome = result?.outcome || {};

      const trackingCid =
        result?.request_tracking_cid ||
        result?.requestTrackingCid ||
        result?.tracking_cid ||
        '';

      if (!trackingCid) {
        skipped++;
        continue;
      }

      let acceptCount = 0;
      let rejectCount = 0;
      for (const vote of votes) {
        const voteData = Array.isArray(vote) ? vote[1] : vote;
        if (voteData?.accept) acceptCount++;
        else rejectCount++;
      }

      const status = outcomeToStatus(outcome?.tag);

      await query(
        `INSERT INTO vote_requests (
          event_id, tracking_cid, proposal_id, status, is_closed,
          action_tag, action_value, requester,
          reason, reason_url,
          votes, vote_count, accept_count, reject_count,
          vote_before, effective_at,
          payload, is_human, created_at, updated_at
        ) VALUES (?, ?, ?, ?, TRUE, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT (event_id) DO UPDATE SET
          status       = EXCLUDED.status,
          is_closed    = TRUE,
          votes        = EXCLUDED.votes,
          vote_count   = EXCLUDED.vote_count,
          accept_count = EXCLUDED.accept_count,
          reject_count = EXCLUDED.reject_count,
          payload      = EXCLUDED.payload,
          updated_at   = CURRENT_TIMESTAMP`,
        [
          trackingCid,                                       // event_id (PK)
          trackingCid,                                       // tracking_cid
          trackingCid,                                       // proposal_id
          status,                                            // status
          action?.tag || 'Unknown',                          // action_tag
          JSON.stringify(action?.value || null),              // action_value
          request?.requester || '',                           // requester
          request?.reason?.body || '',                        // reason
          request?.reason?.url || '',                         // reason_url
          JSON.stringify(votes),                              // votes
          acceptCount + rejectCount,                          // vote_count
          acceptCount,                                        // accept_count
          rejectCount,                                        // reject_count
          request?.vote_before || request?.voteBefore || '',  // vote_before
          result?.completed_at || result?.completedAt || null, // effective_at
          JSON.stringify(result),                              // payload (full raw)
          true,                                                // is_human
        ],
      );

      inserted++;
    } catch (err) {
      console.error(`[vote-result-store] Failed to upsert tracking_cid=${result?.request_tracking_cid}: ${err.message}`);
      skipped++;
    }
  }

  return { inserted, skipped };
}

/**
 * Read stored vote results from DuckDB.
 * Returns raw VoteResult objects (from the payload column) so the frontend
 * can run its existing parseVoteResults() logic identically.
 *
 * @param {{ limit?: number, status?: string, actionTag?: string }} opts
 * @returns {Array} — raw VoteResult objects
 */
export async function getStoredVoteResults({ limit = 500, status, actionTag } = {}) {
  await ensureSchema();

  const conditions = ['is_closed = TRUE'];
  const params = [];

  if (status) {
    conditions.push('status = ?');
    params.push(status);
  }

  if (actionTag) {
    conditions.push('action_tag = ?');
    params.push(actionTag);
  }

  const where = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';

  params.push(limit);

  const rows = await query(
    `SELECT payload FROM vote_requests ${where} ORDER BY effective_at DESC NULLS LAST LIMIT ?`,
    params,
  );

  return rows
    .map((row) => {
      try {
        return typeof row.payload === 'string' ? JSON.parse(row.payload) : row.payload;
      } catch {
        return null;
      }
    })
    .filter(Boolean);
}

/**
 * Return the count of stored vote results.
 */
export async function getVoteResultCount() {
  await ensureSchema();
  const rows = await query('SELECT COUNT(*) AS cnt FROM vote_requests WHERE is_closed = TRUE');
  return Number(rows[0]?.cnt ?? 0);
}
