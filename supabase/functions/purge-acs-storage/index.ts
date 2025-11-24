import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.3";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

interface PurgeRequest {
  snapshot_id?: string;
  purge_all?: boolean;
  webhookSecret?: string;
}

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    const request: PurgeRequest = await req.json().catch(() => ({}));

    const supabaseUrl = Deno.env.get("SUPABASE_URL")!;
    const serviceRoleKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
    const webhookKey = Deno.env.get("ACS_UPLOAD_WEBHOOK_SECRET");

    // Only check webhook secret if provided (allows both webhook and authenticated UI calls)
    if (request.webhookSecret && request.webhookSecret !== webhookKey) {
      return new Response(JSON.stringify({ error: "Unauthorized" }), {
        status: 403,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
    }

    const supabase = createClient(supabaseUrl, serviceRoleKey);

    const bucket = "acs-data";
    let deletedFiles = 0;
    let deletedStats = 0;

    console.log("🧹 Starting ACS purge...");

    // -------------------------------------------------------
    // STEP 1 — Break stuck snapshots (processing → failed)
    // -------------------------------------------------------
    console.log("🔧 Resetting snapshots stuck in 'processing'...");

    await supabase
      .from("acs_snapshots")
      .update({
        status: "failed",
        error_message: "Purged due to stuck processing state",
      })
      .in("status", ["processing"]);

    // -------------------------------------------------------
    // STEP 2 — Delete template stats
    // -------------------------------------------------------
    console.log("🗑️ Clearing template stats...");

    const { count: statsCount } = await supabase.from("acs_template_stats").delete({ count: "exact" }).neq("id", "");

    deletedStats = statsCount || 0;

    // -------------------------------------------------------
    // STEP 3 — Recursive storage deletion
    // -------------------------------------------------------
    async function deletePrefix(prefix: string = ""): Promise<void> {
      const { data: entries, error } = await supabase.storage.from(bucket).list(prefix, { limit: 1000 });

      if (error) {
        console.error("Storage list error:", error);
        return;
      }

      for (const entry of entries ?? []) {
        const fullPath = prefix ? `${prefix}/${entry.name}` : entry.name;

        if (entry.metadata) {
          // File
          await supabase.storage.from(bucket).remove([fullPath]);
          deletedFiles++;
        } else {
          // Folder (must delete contents first)
          await deletePrefix(fullPath);
          await supabase.storage.from(bucket).remove([fullPath]);
        }
      }
    }

    if (request.purge_all) {
      console.log("🗑️ Purging ALL storage files...");
      await deletePrefix("");
    } else if (request.snapshot_id) {
      console.log(`🗑️ Purging storage for snapshot: ${request.snapshot_id}`);
      await deletePrefix(request.snapshot_id);
    } else {
      console.log("🗑️ Purging incomplete snapshot storage...");
      const { data: incomplete } = await supabase.from("acs_snapshots").select("id").in("status", ["failed"]);

      for (const snap of incomplete ?? []) {
        await deletePrefix(snap.id);
      }
    }

    // -------------------------------------------------------
    // STEP 4 — Delete snapshot rows themselves
    // -------------------------------------------------------
    console.log("🗑️ Deleting snapshot rows...");

    await supabase.from("acs_snapshots").delete().neq("id", "");

    console.log("✅ ACS purge complete.");

    return new Response(
      JSON.stringify({
        success: true,
        deleted_files: deletedFiles,
        deleted_stats: deletedStats,
        snapshot_id: request.purge_all ? "all" : (request.snapshot_id ?? "all_incomplete"),
      }),
      {
        status: 200,
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      },
    );
  } catch (err) {
    console.error("💥 Purge failed:", err);

    const message = err instanceof Error ? err.message : typeof err === "string" ? err : "Unknown error";

    return new Response(JSON.stringify({ error: message }), {
      status: 500,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  }
});
