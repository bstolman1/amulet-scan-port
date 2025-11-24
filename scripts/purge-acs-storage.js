import { serve } from "https://deno.land/std/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js";

serve(async (req) => {
  try {
    const { purge_all, snapshot_id, webhookSecret } = await req.json();

    // Auth guard
    if (webhookSecret !== Deno.env.get("ACS_UPLOAD_WEBHOOK_SECRET")) {
      return new Response("Unauthorized", { status: 403 });
    }

    const supabase = createClient(
      Deno.env.get("SUPABASE_URL")!,
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
    );

    const bucket = "acs-data";
    let deletedFiles = 0;
    let deletedStats = 0;

    // ---------------------------
    // BREAK ALL SNAPSHOT FK CHAINS
    // ---------------------------
    await supabase
      .from("acs_snapshots")
      .update({ previous_snapshot_id: null })
      .not("previous_snapshot_id", "is", null);

    // ---------------------------
    // DELETE ALL DEPENDENT TABLES
    // ---------------------------
    await supabase.from("acs_contract_state").delete().neq("contract_id", "");
    await supabase.from("acs_snapshot_chunks").delete().neq("id", "");
    
    const { count: statsCount } = await supabase
      .from("acs_template_stats")
      .delete({ count: "exact" })
      .neq("id", "");

    deletedStats = statsCount || 0;

    // ---------------------------
    // RECURSIVE STORAGE DELETE
    // ---------------------------
    async function deletePrefix(prefix: string = "") {
      const { data: entries, error } = await supabase.storage
        .from(bucket)
        .list(prefix, { limit: 1000 });

      if (error) {
        console.error("List error:", error);
        return;
      }

      for (const entry of entries ?? []) {
        const fullPath = prefix ? `${prefix}/${entry.name}` : entry.name;

        if (entry.metadata) {
          // File
          await supabase.storage.from(bucket).remove([fullPath]);
          deletedFiles++;
        } else {
          // Folder
          await deletePrefix(fullPath);
          await supabase.storage.from(bucket).remove([fullPath]);
        }
      }
    }

    if (purge_all) {
      await deletePrefix("");
    } else if (snapshot_id) {
      await deletePrefix(snapshot_id);
    }

    // ---------------------------
    // DELETE ALL SNAPSHOTS
    // ---------------------------
    await supabase.from("acs_snapshots").delete().neq("id", "");

    return new Response(
      JSON.stringify({
        success: true,
        deleted_files: deletedFiles,
        deleted_stats: deletedStats,
        snapshot_id: purge_all ? "all" : snapshot_id,
      }),
      { status: 200, headers: { "Content-Type": "application/json" } }
    );

  } catch (err) {
    console.error("Purge failed:", err);
    return new Response(
      JSON.stringify({ error: err.message }),
      { status: 500 }
    );
  }
});
