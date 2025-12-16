#!/usr/bin/env node
/**
 * Validate Live Updates payloads from the local DuckDB API.
 *
 * Goals:
 * - Confirm payloads include critical fields (migration_id, synchronizer_id, contract_id, template_id, event_id)
 * - Detect "corrupted" payloads (non-JSON payload/raw, missing IDs)
 * - Confirm displayed/live data is >= the migration 4 backfill checkpoint (if available)
 *
 * Usage:
 *   node scripts/validate-live-updates.js --base http://localhost:3001 --limit 200
 */

const args = process.argv.slice(2);
const getArg = (name, fallback) => {
  const i = args.indexOf(name);
  return i >= 0 ? args[i + 1] : fallback;
};

const BASE = getArg('--base', 'http://localhost:3001');
const LIMIT = Number(getArg('--limit', '200'));

async function fetchJson(path) {
  const res = await fetch(`${BASE}${path}`);
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`${res.status} ${res.statusText} for ${path}\n${text}`);
  }
  return res.json();
}

function pct(n, d) {
  if (!d) return '0%';
  return `${Math.round((n / d) * 1000) / 10}%`;
}

function iso(ms) {
  return ms ? new Date(ms).toISOString() : null;
}

function isIsoDateString(s) {
  if (typeof s !== 'string') return false;
  const t = Date.parse(s);
  return Number.isFinite(t);
}

function summarizeMissing(events, key) {
  const missing = events.filter((e) => e?.[key] === null || e?.[key] === undefined || e?.[key] === '').length;
  return { missing, total: events.length, pct: pct(missing, events.length) };
}

(async () => {
  const liveStatus = await fetchJson('/api/stats/live-status').catch(() => null);
  const latest = await fetchJson(`/api/events/latest?limit=${LIMIT}&offset=0`);
  const events = Array.isArray(latest?.data) ? latest.data : [];

  const m4 = liveStatus?.backfill_cursors?.filter((c) => c?.migration_id === 4)
    ?.sort((a, b) => Date.parse(b?.max_time || 0) - Date.parse(a?.max_time || 0))?.[0];

  const m4Max = m4?.max_time && isIsoDateString(m4.max_time) ? Date.parse(m4.max_time) : null;

  const effectiveTimes = events
    .map((e) => (e?.effective_at && isIsoDateString(e.effective_at) ? Date.parse(e.effective_at) : null))
    .filter((t) => t !== null);

  const newestEffective = effectiveTimes.length ? Math.max(...effectiveTimes) : null;
  const oldestEffective = effectiveTimes.length ? Math.min(...effectiveTimes) : null;

  // Basic field presence checks
  const checks = {
    event_id: summarizeMissing(events, 'event_id'),
    update_id: summarizeMissing(events, 'update_id'),
    migration_id: summarizeMissing(events, 'migration_id'),
    synchronizer_id: summarizeMissing(events, 'synchronizer_id'),
    contract_id: summarizeMissing(events, 'contract_id'),
    template_id: summarizeMissing(events, 'template_id'),
  };

  // Payload sanity checks
  const payloadNonObject = events.filter((e) => e?.payload && typeof e.payload !== 'object').length;
  const rawNonObject = events.filter((e) => e?.raw && typeof e.raw !== 'object').length;

  // Event ID format sanity (should often contain ':' per docs, but don't hard-fail)
  const noColon = events.filter((e) => typeof e?.event_id === 'string' && !e.event_id.includes(':')).length;

  // Backfill→live bridge check
  const newestAtOrAfterM4 = m4Max && newestEffective ? newestEffective >= m4Max : null;

  const report = {
    base: BASE,
    source: latest?.source,
    count: events.length,
    effective_at_window: {
      oldest: iso(oldestEffective),
      newest: iso(newestEffective),
    },
    migration_4_backfill_checkpoint: m4?.max_time || null,
    newest_effective_at_is_at_or_after_m4: newestAtOrAfterM4,
    missing_fields: checks,
    payload_sanity: {
      payload_non_object: payloadNonObject,
      raw_non_object: rawNonObject,
      event_id_without_colon: { count: noColon, pct: pct(noColon, events.length) },
    },
    examples: {
      first: events[0] || null,
      sample_ids: events.slice(0, 5).map((e) => ({
        event_id: e?.event_id,
        migration_id: e?.migration_id,
        effective_at: e?.effective_at,
        contract_id: e?.contract_id,
        template_id: e?.template_id,
      })),
    },
  };

  console.log(JSON.stringify(report, null, 2));

  // Minimal exit code behavior for CI/manual use
  const hardMissing = checks.migration_id.missing > 0 || checks.event_id.missing > 0;
  if (hardMissing) process.exitCode = 2;
})();
