// scripts/fetch-backfill-history.js
//
// Full-history backfill indexer using Scan backfilling APIs:
//
//   POST /v0/backfilling/migration-info
//   POST /v0/backfilling/updates-before
//
// It scans all migrations & synchronizers, walks backwards in time,
// and stores:
//   - ledger_updates (transactions + reassignments)
//   - ledger_events  (created events, archives, exercises)
// with a resumable cursor per (migration_id, synchronizer_id).

import axios from "axios";
import { Agent as HttpAgent } from "http";
import { Agent as HttpsAgent } from "https";
import { createClient } from "@supabase/supabase-js";

process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

// ---------- Config ----------

const BASE_URL = process.env.BASE_URL || "https://scan.sv-1.global.canton.network.sync.global/api/scan";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY;

if (!SUPABASE_URL || !SUPABASE_ANON_KEY) {
  console.error("❌ SUPABASE_URL or SUPABASE_ANON_KEY missing in env");
  process.exit(1);
}

const PAGE_SIZE = parseInt(process.env.BACKFILL_PAGE_SIZE || "200", 10);

// HTTP client
const scanClient = axios.create({
  baseURL: BASE_URL,
  httpAgent: new HttpAgent({ keepAlive: true, keepAliveMsecs: 30000 }),
  httpsAgent: new HttpsAgent({
    keepAlive: true,
    keepAliveMsecs: 30000,
    rejectUnauthorized: false,
  }),
  timeout: 120000,
});

const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);

// ---------- Helpers ----------

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function retryWithBackoff(fn, options = {}) {
  const {
    maxRetries = 5,
    baseDelay = 1000,
    maxDelay = 30000,
    shouldRetry = (error) => {
      // Retry on timeout, connection, and temporary errors
      const retryableCodes = ["PGRST301", "08000", "08003", "08006", "57014", "40001", "40P01"];
      const retryableMessages = ["timeout", "connection", "temporary", "lock", "deadlock"];

      if (error.code && retryableCodes.includes(error.code)) return true;
      if (error.message) {
        const msg = error.message.toLowerCase();
        return retryableMessages.some((term) => msg.includes(term));
      }
      return false;
    },
  } = options;

  let lastError;

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;

      if (attempt === maxRetries || !shouldRetry(error)) {
        throw error;
      }

      // Exponential backoff with jitter
      const exponentialDelay = Math.min(baseDelay * Math.pow(2, attempt), maxDelay);
      const jitter = Math.random() * exponentialDelay * 0.3; // 30% jitter
      const delay = exponentialDelay + jitter;

      console.log(
        `   ⏳ Retry attempt ${attempt + 1}/${maxRetries} after ${Math.round(delay)}ms (error: ${error.message})`,
      );
      await sleep(delay);
    }
  }

  throw lastError;
}

function getEventTime(txOrReassign) {
  return txOrReassign.record_time || txOrReassign.event?.record_time || txOrReassign.effective_at;
}

// ---------- Migration discovery ----------

async function detectAllMigrations() {
  console.log("🔎 Detecting available migrations via /v0/state/acs/snapshot-timestamp");

  const migrations = [];
  let id = 1;

  while (true) {
    try {
      const res = await scanClient.get("/v0/state/acs/snapshot-timestamp", {
        params: { migration_id: id, before: new Date().toISOString() },
      });

      if (res.data?.record_time) {
        migrations.push(id);
        console.log(`  • migration_id=${id} record_time=${res.data.record_time}`);
        id++;
      } else {
        break;
      }
    } catch (err) {
      if (err.response && err.response.status === 404) {
        break;
      }
      console.error(`❌ Error probing migration_id=${id}:`, err.response?.status, err.message);
      break;
    }
  }

  console.log(`✅ Found migrations: ${migrations.join(", ")}`);
  return migrations;
}

// ---------- Backfilling metadata ----------

async function fetchMigrationInfo(migration_id) {
  try {
    const res = await scanClient.post("/v0/backfilling/migration-info", {
      migration_id,
    });
    return res.data;
  } catch (err) {
    if (err.response && err.response.status === 404) {
      console.log(`ℹ️  No backfilling info for migration_id=${migration_id} (404)`);
      return null;
    }
    throw err;
  }
}

// ---------- Cursor persistence ----------

async function getCursor(migration_id, synchronizer_id, min_time, max_time) {
  console.log(`   📍 Getting cursor for synchronizer=${synchronizer_id.substring(0, 20)}...`);

  return await retryWithBackoff(async () => {
    const { data, error } = await supabase
      .from("backfill_cursors")
      .select("*")
      .eq("migration_id", migration_id)
      .eq("synchronizer_id", synchronizer_id)
      .maybeSingle();

    if (error && error.code !== "PGRST116") throw error;

    if (!data) {
      console.log(`   ➕ Creating new cursor for synchronizer=${synchronizer_id.substring(0, 20)}...`);
      const { data: inserted, error: insertError } = await supabase
        .from("backfill_cursors")
        .insert({
          migration_id,
          synchronizer_id,
          min_time,
          max_time,
          last_before: null,
          complete: false,
        })
        .select("*")
        .single();

      if (insertError) throw insertError;
      console.log(`   ✅ New cursor created`);
      return inserted;
    }

    console.log(`   ✅ Found existing cursor: complete=${data.complete}, last_before=${data.last_before}`);
    return data;
  });
}

async function updateCursorLastBefore(migration_id, synchronizer_id, last_before, complete = false) {
  console.log(
    `   🔄 Updating cursor: migration=${migration_id}, synchronizer=${synchronizer_id.substring(0, 20)}..., complete=${complete}`,
  );

  await retryWithBackoff(async () => {
    const { error } = await supabase
      .from("backfill_cursors")
      .update({
        last_before,
        complete,
        updated_at: new Date().toISOString(),
      })
      .eq("migration_id", migration_id)
      .eq("synchronizer_id", synchronizer_id);

    if (error) throw error;
  });

  console.log(`   ✅ Cursor updated successfully`);
}

// ---------- DB inserts ----------

async function upsertInBatches(table, rows, batchSize = 500) {
  if (!rows.length) return;

  const onConflict = table === "ledger_updates" ? "update_id" : "event_id";

  for (let i = 0; i < rows.length; i += batchSize) {
    const batch = rows.slice(i, i + batchSize);

    await retryWithBackoff(async () => {
      const { error } = await supabase.from(table).upsert(batch, { onConflict });

      if (error) throw error;
    });

    console.log(`   📝 Upserted ${batch.length} rows to ${table} (${i + batch.length}/${rows.length})`);
  }
}

async function upsertUpdatesAndEvents(transactions) {
  if (!transactions.length) return;

  const updatesRows = [];
  const eventsRows = [];

  for (const tx of transactions) {
    const isReassignment = !!tx.event;
    const kind = isReassignment ? "reassignment" : "transaction";
    const updateId = tx.update_id;

    const recordTime = tx.record_time || tx.event?.record_time;
    const effectiveAt = tx.effective_at || tx.event?.created_event?.created_at;
    const synchronizerId = tx.synchronizer_id || tx.event?.source_synchronizer;

    updatesRows.push({
      update_id: updateId,
      migration_id: tx.migration_id || tx.event?.migration_id || null,
      synchronizer_id: synchronizerId,
      record_time: recordTime,
      effective_at: effectiveAt,
      offset: tx.offset || null,
      workflow_id: tx.workflow_id || null,
      kind,
      raw: tx,
    });

    if (isReassignment) {
      const ce = tx.event.created_event;
      if (ce) {
        eventsRows.push({
          event_id: ce.event_id,
          update_id: updateId,
          contract_id: ce.contract_id,
          template_id: ce.template_id,
          package_name: ce.package_name,
          event_type: "reassign_create",
          payload: ce.create_arguments || {},
          signatories: ce.signatories || [],
          observers: ce.observers || [],
          created_at_ts: ce.created_at,
          raw: ce,
        });
      }
    } else {
      const eventsById = tx.events_by_id || {};
      for (const [eventId, ev] of Object.entries(eventsById)) {
        let eventType = ev.event_type || ev.kind || "unknown";
        eventType = String(eventType).toLowerCase();

        eventsRows.push({
          event_id: eventId,
          update_id: updateId,
          contract_id: ev.contract_id || null,
          template_id: ev.template_id || null,
          package_name: ev.package_name || null,
          event_type: eventType,
          payload: ev.create_arguments || ev.exercise_arguments || {},
          signatories: ev.signatories || [],
          observers: ev.observers || [],
          created_at_ts: ev.created_at || recordTime,
          raw: ev,
        });
      }
    }
  }

  // Upsert in batches to avoid statement timeout
  await upsertInBatches("ledger_updates", updatesRows, 500);
  await upsertInBatches("ledger_events", eventsRows, 500);
}

// ---------- Core paging over /v0/backfilling/updates-before ----------

async function backfillForSynchronizer(migration_id, range) {
  const synchronizerId = range.synchronizer_id;
  const minTime = range.min;
  const maxTime = range.max;

  console.log(`\n📡 Backfilling migration=${migration_id}, synchronizer=${synchronizerId}`);
  console.log(`   time range: ${minTime} .. ${maxTime}`);

  let cursor = await getCursor(migration_id, synchronizerId, minTime, maxTime);

  if (cursor.complete) {
    console.log("   ✅ Already complete, skipping.");
    return;
  }

  let before = cursor.last_before || maxTime;
  const atOrAfter = minTime;

  while (true) {
    console.log(`   ➜ Requesting /backfilling/updates-before before=${before}, at_or_after=${atOrAfter}`);

    let res;
    try {
      res = await scanClient.post("/v0/backfilling/updates-before", {
        migration_id,
        synchronizer_id: synchronizerId,
        before,
        at_or_after: atOrAfter,
        count: PAGE_SIZE,
      });
    } catch (err) {
      const status = err.response?.status;
      const msg = err.response?.data?.error || err.message;
      console.error(`   ❌ Error from updates-before (status ${status || "n/a"}): ${msg}`);

      if ([429, 500, 502, 503, 504].includes(status)) {
        console.log("   ⏳ Transient error, backing off and retrying...");
        await sleep(5000);
        continue;
      }

      throw err;
    }

    const txs = res.data?.transactions || [];
    if (!txs.length) {
      console.log("   ✅ No more transactions in this range. Marking complete.");
      await updateCursorLastBefore(migration_id, synchronizerId, before, true);
      break;
    }

    await upsertUpdatesAndEvents(txs);

    console.log(`   ✅ Stored ${txs.length} transactions in database`);

    let earliest = null;
    for (const tx of txs) {
      const t = getEventTime(tx);
      if (!t) continue;
      if (!earliest || t < earliest) earliest = t;
    }

    if (!earliest || earliest <= atOrAfter) {
      console.log("   ✅ Reached lower bound of range; marking synchronizer as complete.");
      await updateCursorLastBefore(migration_id, synchronizerId, earliest, true);
      break;
    }

    before = earliest;
    await updateCursorLastBefore(migration_id, synchronizerId, before, false);

    console.log(`   📥 Stored ${txs.length} updates, new before=${before} (still > ${atOrAfter})`);
  }
}

// ---------- Orchestration ----------

async function run() {
  console.log("\n" + "=".repeat(80));
  console.log("🚀 Backfilling full ledger history");
  console.log("   BASE_URL:", BASE_URL);
  console.log("   PAGE_SIZE:", PAGE_SIZE);
  console.log("=".repeat(80));

  const migrations = await detectAllMigrations();

  for (const migration_id of migrations) {
    console.log("\n" + "-".repeat(80));
    console.log(`📘 Migration ${migration_id}: fetching backfilling metadata`);
    console.log("-".repeat(80));

    const info = await fetchMigrationInfo(migration_id);
    if (!info) {
      console.log("   ℹ️  No backfilling info; skipping this migration.");
      continue;
    }

    const ranges = info.record_time_range || [];
    if (!ranges.length) {
      console.log("   ℹ️  No synchronizer ranges; skipping.");
      continue;
    }

    console.log(`   Found ${ranges.length} synchronizer ranges for migration ${migration_id}`);

    for (const range of ranges) {
      await backfillForSynchronizer(migration_id, range);
    }

    console.log(`✅ Completed migration ${migration_id} (all synchronizers backfilled as far as Scan allows).`);
  }

  console.log("\n🎉 Full-history backfill run finished.");
}

run().catch((err) => {
  console.error("\n❌ FATAL in backfill indexer:", err.message);
  console.error(err.stack);
  process.exit(1);
});
