import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.3";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

// ------------------------------
// Value pickers
// ------------------------------
function pickAmount(obj: any): number {
  if (!obj) return 0;
  const candidates = [
    obj?.amount?.initialAmount,
    obj?.amulet?.amount?.initialAmount,
    obj?.state?.amount?.initialAmount,
    obj?.create_arguments?.amount?.initialAmount,
    obj?.balance?.initialAmount,
    obj?.amount,
  ];
  for (const v of candidates) {
    const n = typeof v === "string" ? parseFloat(v) : Number(v);
    if (!isNaN(n)) return n;
  }
  return 0;
}

function pickLockedAmount(obj: any): number {
  const v = obj?.amulet?.amount?.initialAmount;
  if (v !== undefined && v !== null) {
    const n = typeof v === "string" ? parseFloat(v) : Number(v);
    if (!isNaN(n)) return n;
  }
  return pickAmount(obj);
}

// ------------------------------
// Concurrency limiter
// ------------------------------
async function limitConcurrency<T>(tasks: (() => Promise<T>)[], limit: number): Promise<T[]> {
  const results: T[] = [];
  const executing: Promise<void>[] = [];
  for (const task of tasks) {
    const p = task().then((r) => {
      results.push(r);
      executing.splice(executing.indexOf(p), 1);
    });
    executing.push(p);
    if (executing.length >= limit) await Promise.race(executing);
  }
  await Promise.all(executing);
  return results;
}

// ------------------------------
// Main server function
// ------------------------------
Deno.serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    const { snapshot_id, template_suffix, mode = "circulating" } = await req.json();

    if (!snapshot_id || !template_suffix) {
      return new Response(JSON.stringify({ error: "snapshot_id and template_suffix are required" }), {
        status: 400,
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      });
    }

    const supabase = createClient(Deno.env.get("SUPABASE_URL") ?? "", Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "");

    // ---------------------------
    // Fetch template stats
    // ---------------------------
    const { data: templates, error: tsErr } = await supabase
      .from("acs_template_stats")
      .select("template_id, storage_path")
      .eq("snapshot_id", snapshot_id)
      .like("template_id", `%${template_suffix}`);

    if (tsErr) throw tsErr;

    let totalSum = 0;
    let totalCount = 0;

    const picker = mode === "locked" ? pickLockedAmount : pickAmount;

    // ---------------------------
    // Process templates
    // ---------------------------
    for (const t of templates ?? []) {
      if (!t.storage_path) continue;

      const { data: manifestFile, error: mErr } = await supabase.storage.from("acs-data").download(t.storage_path);

      if (mErr || !manifestFile) continue;

      const manifestText = await manifestFile.text();
      let parsed: any;
      try {
        parsed = JSON.parse(manifestText);
      } catch {
        continue;
      }

      let chunkPaths: string[] = [];

      // ----------------------------------------------
      // Manifest Type 1 — parsed.chunks: [{ path }]
      // ----------------------------------------------
      if (parsed?.chunks && Array.isArray(parsed.chunks)) {
        chunkPaths = parsed.chunks.map((c: any) => c.path || c.storagePath).filter((p: string) => !!p);
      }

      // ----------------------------------------------
      // Manifest Type 2 — parsed.chunk_paths: ["..."]
      // ----------------------------------------------
      if (parsed?.chunk_paths) {
        chunkPaths.push(...parsed.chunk_paths);
      }

      // ----------------------------------------------
      // Normalize relative paths
      // ----------------------------------------------
      const manifestDir = t.storage_path.substring(0, t.storage_path.lastIndexOf("/") + 1);
      chunkPaths = chunkPaths.map((p) => (p.includes("/") ? p : manifestDir + p));

      // ----------------------------------------------
      // 🔥 DEDUPLICATE HERE — MOST IMPORTANT FIX
      // ----------------------------------------------
      chunkPaths = [...new Set(chunkPaths)];

      console.log(`Template ${t.template_id}: ${chunkPaths.length} unique chunk files.`);

      // ----------------------------------------------
      // Chunk processing tasks
      // ----------------------------------------------
      const tasks = chunkPaths.map((path) => async () => {
        try {
          const { data: chunkFile } = await supabase.storage.from("acs-data").download(path);

          if (!chunkFile) return { sum: 0, count: 0 };

          const text = await chunkFile.text();
          const arr = JSON.parse(text);

          if (!Array.isArray(arr)) return { sum: 0, count: 0 };

          const sum = arr.reduce((a, it) => a + picker(it), 0);
          return { sum, count: arr.length };
        } catch (err) {
          console.error(`Error loading chunk ${path}:`, err);
          return { sum: 0, count: 0 };
        }
      });

      // ----------------------------------------------
      // Process with concurrency limit
      // ----------------------------------------------
      const results = await limitConcurrency(tasks, 6);

      for (const r of results) {
        totalSum += r.sum;
        totalCount += r.count;
      }
    }

    return new Response(
      JSON.stringify({
        sum: totalSum,
        count: totalCount,
        templateCount: templates?.length ?? 0,
      }),
      { headers: { ...corsHeaders, "Content-Type": "application/json" } },
    );
  } catch (e) {
    console.error("aggregate-template-sum error", e);
    return new Response(JSON.stringify({ error: (e as Error)?.message ?? "Internal server error" }), {
      status: 500,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  }
});
