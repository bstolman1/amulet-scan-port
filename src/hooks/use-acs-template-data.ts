import { useQuery } from "@tanstack/react-query";
import { supabase } from "@/integrations/supabase/client";

interface TemplateDataMetadata {
  template_id: string;
  snapshot_timestamp: string;
  entry_count: number;
}

interface TemplateDataResponse<T = any> {
  metadata: TemplateDataMetadata;
  data: T[];
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
  console.log(`[fetchTemplateData] Loading from: ${storagePath}`);

  // Download the file from storage
  const { data: fileData, error: downloadError } = await supabase.storage.from("acs-data").download(storagePath);

  if (downloadError) {
    console.error(`[fetchTemplateData] Download error:`, downloadError);
    throw downloadError;
  }
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

    console.log(
      `[fetchTemplateData] Manifest detected: ${chunks.length} unique chunks (declared: ${totalChunks}), expected entries: ${totalEntries ?? "unknown"}`,
    );

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

          if (listError) {
            console.warn("[fetchTemplateData] Chunk list failed:", listError);
          } else if (Array.isArray(listed) && listed.length > 0) {
            const names = listed
              .filter((it) => it.name.startsWith(basePrefix) && it.name.endsWith(".json"))
              .map((it) => `${dir}/${it.name}`);
            const existing = new Set(chunks.map((c) => c.path));
            for (const p of names) if (!existing.has(p)) discoveredChunkPaths.push(p);
            if (discoveredChunkPaths.length > 0) {
              console.log(
                `[fetchTemplateData] 🔎 Discovered ${discoveredChunkPaths.length} additional chunks via prefix listing`,
              );
            }
          }
        }
      }
    } catch (e) {
      console.warn("[fetchTemplateData] Fallback discovery errored:", e);
    }

    const allChunkDescriptors = [
      ...chunks,
      ...discoveredChunkPaths.map((p, i) => ({ index: chunks.length + i, path: p, entryCount: 0 })),
    ];

    // Download all chunks in parallel
    const chunkPromises = allChunkDescriptors.map(async (chunk) => {
      console.log(`[fetchTemplateData] Downloading chunk ${chunk.index}: ${chunk.path}`);
      const { data: chunkData, error: chunkError } = await supabase.storage.from("acs-data").download(chunk.path);

      if (chunkError) {
        console.warn(`[fetchTemplateData] Failed to download chunk ${chunk.index}:`, chunkError);
        return [] as any[];
      }

      if (!chunkData) return [] as any[];

      const chunkText = await chunkData.text();
      const chunkArray = JSON.parse(chunkText);
      const count = Array.isArray(chunkArray) ? chunkArray.length : 0;
      console.log(
        `[fetchTemplateData] Chunk ${chunk.index} loaded: ${count} contracts (manifest said: ${chunk.entryCount})`,
      );
      return Array.isArray(chunkArray) ? chunkArray : [];
    });

    const chunkArrays = await Promise.all(chunkPromises);
    const allData = chunkArrays.flat();
    console.log(
      `[fetchTemplateData] ✅ Total loaded from manifest: ${allData.length} entries (expected: ${totalEntries ?? "unknown"})`,
    );
    return allData;
  }

  // Direct file format (backward compatibility)
  const directCount = Array.isArray(parsed) ? parsed.length : 0;
  console.log(`[fetchTemplateData] Direct file loaded: ${directCount} contracts`);
  return Array.isArray(parsed) ? parsed : [];
}

/**
 * Fetch template data from Supabase Storage for a given snapshot
 */
export function useACSTemplateData<T = any>(
  snapshotId: string | undefined,
  templateId: string,
  enabled: boolean = true,
) {
  return useQuery({
    queryKey: ["acs-template-data", snapshotId, templateId],
    queryFn: async (): Promise<TemplateDataResponse<T>> => {
      if (!snapshotId || !templateId) {
        throw new Error("Missing snapshotId or templateId");
      }

      // Get the storage path from template stats
      const { data: templateStats, error: statsError } = await supabase
        .from("acs_template_stats")
        .select("storage_path")
        .eq("snapshot_id", snapshotId)
        .eq("template_id", templateId)
        .maybeSingle();

      if (statsError) throw statsError;
      if (!templateStats?.storage_path) {
        throw new Error(`No storage path found for template ${templateId}`);
      }

      // Fetch template data (handles both chunked and direct formats)
      const contractsArray = await fetchTemplateData(templateStats.storage_path);

      // Get snapshot info for metadata
      const { data: snapshot } = await supabase.from("acs_snapshots").select("timestamp").eq("id", snapshotId).single();

      // Wrap in expected format with metadata
      return {
        metadata: {
          template_id: templateId,
          snapshot_timestamp: snapshot?.timestamp || new Date().toISOString(),
          entry_count: Array.isArray(contractsArray) ? contractsArray.length : 0,
        },
        data: Array.isArray(contractsArray) ? contractsArray : [],
      } as TemplateDataResponse<T>;
    },
    enabled: enabled && !!snapshotId && !!templateId,
    staleTime: 5 * 60 * 1000, // 5 minutes
  });
}

/**
 * Get all available templates for a snapshot
 */
export function useACSTemplates(snapshotId: string | undefined) {
  return useQuery({
    queryKey: ["acs-templates", snapshotId],
    queryFn: async () => {
      if (!snapshotId) throw new Error("Missing snapshotId");

      const { data, error } = await supabase
        .from("acs_template_stats")
        .select("template_id, contract_count, storage_path")
        .eq("snapshot_id", snapshotId)
        .order("contract_count", { ascending: false });

      if (error) throw error;
      return data;
    },
    enabled: !!snapshotId,
    staleTime: 5 * 60 * 1000,
  });
}
