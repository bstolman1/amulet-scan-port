import { useQuery } from "@tanstack/react-query";
import { supabase } from "@/integrations/supabase/client";
import { useDuckDBForLedger, checkDuckDBConnection } from "@/lib/backend-config";
import { getACSContracts as getLocalACSContracts } from "@/lib/duckdb-api-client";

// Cached DuckDB availability check
let duckDBAvailable: boolean | null = null;
let duckDBCheckTime = 0;
async function isDuckDBAvailable(): Promise<boolean> {
  const now = Date.now();
  if (duckDBAvailable !== null && now - duckDBCheckTime < 30_000) {
    return duckDBAvailable;
  }
  duckDBAvailable = await checkDuckDBConnection();
  duckDBCheckTime = now;
  return duckDBAvailable;
}

interface ChunkManifest {
  templateId: string;
  totalChunks: number;
  totalEntries: number;
  chunks: Array<{
    index: number;
    path: string;
    entryCount: number;
  }>;
}

/**
 * Helper function to fetch template data, handling both chunked and direct formats
 */
async function fetchTemplateData(storagePath: string): Promise<any[]> {
  const { data: fileData, error: downloadError } = await supabase.storage.from("acs-data").download(storagePath);

  if (downloadError) throw downloadError;
  if (!fileData) throw new Error("No data returned from storage");

  const text = await fileData.text();
  const parsed = JSON.parse(text);

  // Check if it's a manifest file (support both new and legacy shapes)
  if (parsed && parsed.chunks && Array.isArray(parsed.chunks)) {
    const totalChunks = parsed.totalChunks ?? parsed.total_chunks ?? parsed.chunks.length;
    const totalEntries = parsed.totalEntries ?? parsed.total_entries ?? undefined;

    // Normalize chunk objects: support {index,path,entryCount} and {chunkIndex,storagePath,contractCount}
    const normalized = (parsed.chunks as any[])
      .map((c) => ({
        index: c.index ?? c.chunkIndex ?? 0,
        path: c.path ?? c.storagePath ?? "",
        entryCount: c.entryCount ?? c.contractCount ?? 0,
      }))
      .filter((c) => !!c.path);

    // De-duplicate by path in case manifest contains repeated entries
    const byPath = new Map<string, { index: number; path: string; entryCount: number }>();
    for (const c of normalized) {
      if (!byPath.has(c.path)) byPath.set(c.path, c);
    }
    const chunks = Array.from(byPath.values());

    // Fallback: auto-discover additional chunks in the same folder when manifest looks incomplete
    let discoveredChunkPaths: string[] = [];
    try {
      const sample = chunks[0];
      if (sample?.path?.includes("/_") || sample?.path?.includes("/")) {
        const lastSlash = sample.path.lastIndexOf("/");
        const dir = sample.path.substring(0, lastSlash);
        const file = sample.path.substring(lastSlash + 1);
        const basePrefix = file.split("_chunk_")[0] + "_chunk_"; // e.g. <hash>_Splice_Amulet_Amulet_chunk_

        if (basePrefix.includes("_chunk_")) {
          const { data: listed, error: listError } = await supabase.storage
            .from("acs-data")
            .list(dir, { limit: 1000, search: basePrefix });

          if (!listError && Array.isArray(listed) && listed.length > 0) {
            const names = listed
              .filter((it) => it.name.startsWith(basePrefix) && it.name.endsWith(".json"))
              .map((it) => `${dir}/${it.name}`);
            const existing = new Set(chunks.map((c) => c.path));
            for (const p of names) if (!existing.has(p)) discoveredChunkPaths.push(p);
          }
        }
      }
    } catch (e) {
      // Fallback discovery failed, continue with manifest chunks only
    }

    const allChunkDescriptors = [
      ...chunks,
      ...discoveredChunkPaths.map((p, i) => ({ index: chunks.length + i, path: p, entryCount: 0 })),
    ];

    // Download all chunks in parallel
    const chunkPromises = allChunkDescriptors.map(async (chunk) => {
      const { data: chunkData, error: chunkError } = await supabase.storage.from("acs-data").download(chunk.path);

      if (chunkError || !chunkData) return [] as any[];

      const chunkText = await chunkData.text();
      const chunkArray = JSON.parse(chunkText);
      return Array.isArray(chunkArray) ? chunkArray : [];
    });

    const chunkArrays = await Promise.all(chunkPromises);
    return chunkArrays.flat();
  }

  // Direct file format (backward compatibility)
  return Array.isArray(parsed) ? parsed : [];
}

/**
 * Fetch and aggregate data across all templates matching a suffix
 * For example, all templates ending in "Splice:Amulet:Amulet" regardless of package hash
 */
export function useAggregatedTemplateData(
  snapshotId: string | undefined,
  templateSuffix: string,
  enabled: boolean = true,
) {
  const useDuckDB = useDuckDBForLedger();

  return useQuery({
    queryKey: ["aggregated-template-data", snapshotId, templateSuffix, useDuckDB ? "duckdb" : "supabase"],
    queryFn: async () => {
      if (!templateSuffix) {
        throw new Error("Missing templateSuffix");
      }

      // Try DuckDB first if configured and available
      if (useDuckDB && await isDuckDBAvailable()) {
        try {
          console.log(`[useAggregatedTemplateData] Using DuckDB for ${templateSuffix}`);
          // For local ACS, we use the entity name pattern (e.g., "Splice:Amulet:Amulet")
          const response = await getLocalACSContracts({ 
            entity: templateSuffix.split(':').pop(), // Get entity name like "Amulet"
            limit: 10000 
          });
          
          return {
            data: response.data,
            templateCount: 1,
            totalContracts: response.data.length,
            templateIds: [templateSuffix],
            source: "duckdb",
          };
        } catch (error) {
          console.warn("DuckDB aggregated data fetch failed, falling back to Supabase:", error);
        }
      }

      // Supabase fallback
      if (!snapshotId) {
        throw new Error("Missing snapshotId for Supabase query");
      }

      // Support both legacy and new template id separators in module path (":" vs ".")
      const firstColon = templateSuffix.indexOf(":");
      const dotVariant =
        firstColon !== -1
          ? templateSuffix.slice(0, firstColon) + "." + templateSuffix.slice(firstColon + 1)
          : templateSuffix;

      // Find all templates matching either suffix pattern
      const { data: templateStats, error: statsError } = await supabase
        .from("acs_template_stats")
        .select("template_id, storage_path, contract_count")
        .eq("snapshot_id", snapshotId)
        .or(`template_id.like.%:${templateSuffix},template_id.like.%:${dotVariant}`);

      if (statsError) throw statsError;
      if (!templateStats || templateStats.length === 0) {
        return { data: [], templateCount: 0, totalContracts: 0 };
      }

      // Fetch data from all matching templates
      const allData: any[] = [];
      let totalContracts = 0;

      for (const template of templateStats) {
        try {
          const contractsArray = await fetchTemplateData(template.storage_path);
          // Use loop instead of spread to avoid "Maximum call stack size exceeded"
          for (const contract of contractsArray) {
            allData.push(contract);
          }
          totalContracts += contractsArray.length;
        } catch (error) {
          console.error(`Error loading template ${template.template_id}:`, error);
        }
      }

      return {
        data: allData,
        templateCount: templateStats.length,
        totalContracts,
        templateIds: templateStats.map((t) => t.template_id),
      };
    },
    enabled: enabled && !!snapshotId && !!templateSuffix,
    staleTime: 5 * 60 * 1000, // 5 minutes
  });
}
