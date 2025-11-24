// scripts/ingest-updates.js
//
// Incremental ingester:
// - Detects latest migration
// - Reads last offset from ledger_updates
// - Calls /v2/updates for that migration
// - Appends new rows to ledger_updates + ledger_events

import axios from "axios";
import { Agent as HttpAgent } from "http";
import { Agent as HttpsAgent } from "https";
import { createClient } from "@supabase/supabase-js";

process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

const BASE_URL = process.env.BASE_URL || "https://scan.sv-1.global.canton.network.sync.global/api/scan";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY;

if (!SUPABASE_URL || !SUPABASE_ANON_KEY) {
  console.error("❌ SUPABASE_URL or SUPABASE_ANON_KEY missing in env");
  process.exit(1);
}

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

// ---------- Retry helper ----------

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
        `⏳ Retry attempt ${attempt + 1}/${maxRetries} after ${Math.round(delay)}ms (error: ${error.message})`,
      );
      await sleep(delay);
    }
  }

  throw lastError;
}

async function detectLatestMigration() {
  console.log("🔎 Detecting latest migration via /v0/state/acs/snapshot-timestamp");
  let id = 1;
  let latest = null;

  while (true) {
    try {
      const res = await scanClient.get("/v0/state/acs/snapshot-timestamp", {
        params: { before: new Date().toISOString(), migration_id: id },
      });
      if (res.data?.record_time) {
        latest = id;
        id++;
      } else {
        break;
      }
    } catch (err) {
      break;
    }
  }

  if (!latest) throw new Error("No valid migration found");
  console.log(`📘 Using latest migration_id: ${latest}`);
  return latest;
}

async function run() {
  const migrationId = await detectLatestMigration();

  // Find last offset for this migration (if any)
  const lastRow = await retryWithBackoff(async () => {
    const { data, error } = await supabase
      .from("ledger_updates")
      .select("offset")
      .eq("migration_id", migrationId)
      .not("offset", "is", null)
      .order("record_time", { ascending: false })
      .limit(1)
      .maybeSingle();

    if (error && error.code !== "PGRST116") {
      throw error;
    }

    return data;
  });

  const lastOffset = lastRow?.offset || null;
  console.log(`🔄 ingest-updates for migration=${migrationId}, lastOffset=${lastOffset ?? "none"}`);

  const body = {
    migration_id: migrationId,
    batch_size: 500,
  };
  if (lastOffset) body.begin = lastOffset;

  const res = await scanClient.post("/v2/updates", body);
  const updates = res.data?.updates || [];

  if (!updates.length) {
    console.log("✅ No new updates.");
    return;
  }

  console.log(`📥 Received ${updates.length} updates from /v2/updates`);

  const updatesRows = [];
  const eventsRows = [];

  for (const u of updates) {
    const recordTime = u.record_time || u.effective_at || null;
    const effectiveAt = u.effective_at || null;
    const isReassignment = !!u.event;
    const kind = isReassignment ? "reassignment" : "transaction";

    updatesRows.push({
      update_id: u.update_id,
      migration_id: u.migration_id ?? migrationId,
      synchronizer_id: u.synchronizer_id ?? u.event?.source_synchronizer ?? null,
      record_time: recordTime,
      effective_at: effectiveAt,
      offset: u.offset || null,
      workflow_id: u.workflow_id || null,
      kind,
      raw: u,
    });

    if (isReassignment) {
      const ce = u.event.created_event;
      if (ce) {
        eventsRows.push({
          event_id: ce.event_id,
          update_id: u.update_id,
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
      const eventsById = u.events_by_id || {};
      for (const [eventId, ev] of Object.entries(eventsById)) {
        let eventType = ev.event_type || ev.kind || "unknown";
        eventType = String(eventType).toLowerCase();

        eventsRows.push({
          event_id: eventId,
          update_id: u.update_id,
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
  const batchSize = 500;

  if (updatesRows.length) {
    for (let i = 0; i < updatesRows.length; i += batchSize) {
      const batch = updatesRows.slice(i, i + batchSize);

      await retryWithBackoff(async () => {
        const { error } = await supabase.from("ledger_updates").upsert(batch, { onConflict: "update_id" });
        if (error) throw error;
      });

      console.log(`   📝 Upserted ${batch.length} updates (${i + batch.length}/${updatesRows.length})`);
    }
  }

  if (eventsRows.length) {
    for (let i = 0; i < eventsRows.length; i += batchSize) {
      const batch = eventsRows.slice(i, i + batchSize);

      await retryWithBackoff(async () => {
        const { error } = await supabase.from("ledger_events").upsert(batch, { onConflict: "event_id" });
        if (error) throw error;
      });

      console.log(`   📝 Upserted ${batch.length} events (${i + batch.length}/${eventsRows.length})`);
    }
  }

  console.log(`✅ Stored ${updatesRows.length} updates and ${eventsRows.length} events for migration=${migrationId}`);
}

run().catch((err) => {
  console.error("❌ Fatal in ingest-updates:", err.message);
  console.error(err.stack);
  process.exit(1);
});
