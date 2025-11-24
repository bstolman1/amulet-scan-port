/*
 * fetch-acs-snapshot
 * ------------------
 * Creates a new full ACS snapshot every time it's triggered.
 * Automatically chains snapshots using previous_snapshot_id.
 * Includes archived contracts (because we use full ACS).
 * No incremental pruning (safe).
 * Compatible with your Supabase schema.
 */

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

// How many ACS pages each invocation processes
const PAGES_PER_BATCH = 40;
const PAGE_SIZE = 500;

// Retry helpers
const MAX_RETRIES = 3;
const INITIAL_RETRY_DELAY = 1000;

// ----------------------------
// Decimal helper (10 decimals)
// ----------------------------
class Decimal {
  private value: string;
  constructor(val: string | number) {
    this.value = typeof val === "number" ? val.toFixed(10) : val;
  }
  plus(other: Decimal) {
    return new Decimal((parseFloat(this.value) + parseFloat(other.value)).toFixed(10));
  }
  minus(other: Decimal) {
    return new Decimal((parseFloat(this.value) - parseFloat(other.value)).toFixed(10));
  }
  toString() {
    return parseFloat(this.value).toFixed(10);
  }
  toNumber() {
    return parseFloat(this.value);
  }
}

// ------------------------------------
// Retry with exponential backoff
// ------------------------------------
async function retryWithBackoff(fn: () => Promise<any>, name: string): Promise<any> {
  let lastErr: any;
  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      if (attempt < MAX_RETRIES) {
        const delay = INITIAL_RETRY_DELAY * Math.pow(2, attempt);
        console.warn(`⚠️ ${name} failed (attempt ${attempt + 1}). Retrying in ${delay}ms...`);
        await new Promise((r) => setTimeout(r, delay));
      }
    }
  }
  console.error(`❌ ${name} failed after ${MAX_RETRIES + 1} attempts`);
  throw lastErr;
}

// ------------------------------------
// Template detection helpers
// ------------------------------------
function isTemplate(e: any, moduleName: string, entityName: string) {
  const t = e?.template_id;
  if (!t) return false;
  const parts = t.split(":");
  const entity = parts.pop();
  const module_ = parts.pop();
  return module_ === moduleName && entity === entityName;
}

// ------------------------------------
// Migration detection
// ------------------------------------
async function detectLatestMigration(baseUrl: string): Promise<number> {
  console.log("🔎 Detecting latest migration...");
  let id = 1;
  let latest = null;

  while (true) {
    try {
      const res = await fetch(
        `${baseUrl}/v0/state/acs/snapshot-timestamp?before=${new Date().toISOString()}&migration_id=${id}`,
      );
      const data = await res.json();
      if (data?.record_time) {
        latest = id;
        id++;
      } else break;
    } catch {
      break;
    }
  }

  if (!latest) throw new Error("No valid migration found");
  console.log(`📘 Latest migration_id: ${latest}`);
  return latest;
}

// ------------------------------------
// Snapshot timestamp fetch
// ------------------------------------
async function fetchSnapshotTimestamp(baseUrl: string, migration_id: number) {
  const res = await fetch(
    `${baseUrl}/v0/state/acs/snapshot-timestamp?before=${new Date().toISOString()}&migration_id=${migration_id}`,
  );
  const data = await res.json();
  let rt = data.record_time;

  const verify = await fetch(`${baseUrl}/v0/state/acs/snapshot-timestamp?before=${rt}&migration_id=${migration_id}`);
  const v = await verify.json();
  if (v?.record_time && v.record_time !== rt) rt = v.record_time;

  console.log(`📅 Snapshot timestamp: ${rt}`);
  return rt;
}

// ------------------------------------
// Get last completed snapshot (for chaining)
// ------------------------------------
async function getPreviousSnapshot(supabaseAdmin: any, migration_id: number) {
  const { data, error } = await supabaseAdmin
    .from("acs_snapshots")
    .select("id")
    .eq("migration_id", migration_id)
    .eq("status", "completed")
    .order("completed_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) {
    console.warn("⚠️ Failed to fetch previous snapshot:", error.message);
    return null;
  }

  return data || null;
}

// ------------------------------------
// Process a batch of ACS events
// ------------------------------------
async function processBatch(
  baseUrl: string,
  migration_id: number,
  record_time: string,
  supabaseAdmin: any,
  snapshot: any,
) {
  console.log(`📦 Batch starting at cursor: ${snapshot.cursor_after}`);

  const templatesData: Record<string, any[]> = {};
  const templateStats: Record<string, { count: number }> = {};
  const perPackage: Record<string, { amulet: Decimal; locked: Decimal }> = {};

  let amuletTotal = new Decimal(snapshot.amulet_total || "0");
  let lockedTotal = new Decimal(snapshot.locked_total || "0");

  let after = snapshot.cursor_after;
  let pagesProcessed = 0;
  const seen = new Set();

  while (pagesProcessed < PAGES_PER_BATCH) {
    const res = await fetch(`${baseUrl}/v0/state/acs`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        migration_id,
        record_time,
        page_size: PAGE_SIZE,
        after,
        daml_value_encoding: "compact_json",
      }),
    });

    const data = await res.json();
    const events = data.created_events || [];
    const rangeTo = data.range?.to;

    if (events.length === 0) return { isComplete: true, nextCursor: after };

    for (const e of events) {
      const id = e.contract_id ?? e.event_id;
      if (seen.has(id)) continue;
      seen.add(id);

      const templateId = e.template_id;
      const args = e.create_arguments || {};

      templatesData[templateId] ||= [];
      templateStats[templateId] ||= { count: 0 };

      templatesData[templateId].push(args);
      templateStats[templateId].count++;

      if (isTemplate(e, "Splice.Amulet", "Amulet")) {
        const val = args?.amount?.initialAmount;
        if (typeof val === "string") amuletTotal = amuletTotal.plus(new Decimal(val));
      }

      if (isTemplate(e, "Splice.Amulet", "LockedAmulet")) {
        const val = args?.amulet?.amount?.initialAmount;
        if (typeof val === "string") lockedTotal = lockedTotal.plus(new Decimal(val));
      }
    }

    pagesProcessed++;
    after = rangeTo ?? after + events.length;

    console.log(`📄 Page ${pagesProcessed}/${PAGES_PER_BATCH}`);
  }

  // Upload template data
  for (const [templateId, entries] of Object.entries(templatesData)) {
    const filePath = `${snapshot.id}/${templateId.replace(/[:.]/g, "_")}.json`;

    const content = JSON.stringify({ data: entries }, null, 2);

    await retryWithBackoff(
      () =>
        supabaseAdmin.storage
          .from("acs-data")
          .upload(filePath, new Blob([content], { type: "application/json" }), { upsert: true }),
      `upload ${templateId}`,
    );

    await retryWithBackoff(
      () =>
        supabaseAdmin.from("acs_template_stats").upsert(
          {
            snapshot_id: snapshot.id,
            template_id: templateId,
            contract_count: templateStats[templateId].count,
            storage_path: filePath,
          },
          { onConflict: "snapshot_id,template_id" },
        ),
      `stats ${templateId}`,
    );
  }

  // Update snapshot progress
  const circulating = amuletTotal.minus(lockedTotal);

  await retryWithBackoff(
    () =>
      supabaseAdmin
        .from("acs_snapshots")
        .update({
          cursor_after: after,
          processed_pages: (snapshot.processed_pages || 0) + pagesProcessed,
          processed_events: (snapshot.processed_events || 0) + seen.size,
          amulet_total: amuletTotal.toString(),
          locked_total: lockedTotal.toString(),
          circulating_supply: circulating.toString(),
          last_progress_update: new Date().toISOString(),
        })
        .eq("id", snapshot.id),
    "snapshot progress",
  );

  return { isComplete: false, nextCursor: after };
}

// ------------------------------------
// MAIN SERVE HANDLER
// ------------------------------------
Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return new Response(null, { headers: corsHeaders });

  const supabaseAdmin = createClient(Deno.env.get("SUPABASE_URL")!, Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!);

  const BASE_URL = "https://scan.sv-1.global.canton.network.sync.global/api/scan";

  try {
    const body = await req.json().catch(() => ({}));
    const snapshotId = body.snapshot_id;

    let snapshot;

    // ---------------------------------------
    // If snapshot_id specified → resume
    // ---------------------------------------
    if (snapshotId) {
      const { data, error } = await supabaseAdmin.from("acs_snapshots").select("*").eq("id", snapshotId).single();

      if (error || !data) throw new Error("Snapshot not found");
      snapshot = data;
    }

    // ---------------------------------------
    // Create a new snapshot
    // ---------------------------------------
    if (!snapshotId) {
      const migration_id = await detectLatestMigration(BASE_URL);
      const record_time = await fetchSnapshotTimestamp(BASE_URL, migration_id);

      const previous = await getPreviousSnapshot(supabaseAdmin, migration_id);

      const { data, error } = await supabaseAdmin
        .from("acs_snapshots")
        .insert({
          round: 0,
          snapshot_data: {},
          sv_url: BASE_URL,
          migration_id,
          record_time,
          amulet_total: 0,
          locked_total: 0,
          circulating_supply: 0,
          status: "processing",
        })
        .select()
        .single();

      if (error || !data) throw new Error("Failed to create snapshot");
      snapshot = data;

      // Kick off first batch asynchronously
      supabaseAdmin.functions.invoke("fetch-acs-snapshot", { body: { snapshot_id: snapshot.id } }).catch(console.error);

      return new Response(
        JSON.stringify({
          message: "Snapshot started",
          snapshot_id: snapshot.id,
        }),
        { headers: { ...corsHeaders, "Content-Type": "application/json" } },
      );
    }

    // ---------------------------------------
    // Process next batch
    // ---------------------------------------
    const { isComplete, nextCursor } = await processBatch(
      BASE_URL,
      snapshot.migration_id,
      snapshot.record_time,
      supabaseAdmin,
      snapshot,
    );

    if (isComplete) {
      await supabaseAdmin
        .from("acs_snapshots")
        .update({
          status: "completed",
        })
        .eq("id", snapshot.id);

      return new Response(JSON.stringify({ message: "Snapshot completed", snapshot_id: snapshot.id }), {
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      });
    }

    // Continue to next batch
    supabaseAdmin.functions.invoke("fetch-acs-snapshot", {
      body: { snapshot_id: snapshot.id },
    });

    return new Response(
      JSON.stringify({
        message: "Batch processed",
        snapshot_id: snapshot.id,
        cursor: nextCursor,
      }),
      { headers: { ...corsHeaders, "Content-Type": "application/json" } },
    );
  } catch (err: any) {
    return new Response(JSON.stringify({ error: err.message }), {
      status: 500,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  }
});
