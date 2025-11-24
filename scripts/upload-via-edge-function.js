/**
 * Upload fetched ACS data to Lovable Cloud via Edge Function (Chunked, Hardened)
 */

import fs from "fs";
import path from "path";
import axios from "axios";

let CHUNK_SIZE = parseInt(process.env.UPLOAD_CHUNK_SIZE || "5", 10);
const MIN_CHUNK_SIZE = 1;
const MAX_CHUNK_SIZE = parseInt(process.env.MAX_UPLOAD_CHUNK_SIZE || String(CHUNK_SIZE), 10);
const UPLOAD_DELAY_MS = parseInt(process.env.UPLOAD_DELAY_MS || "1000", 10);

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isRetryableUploadError(err) {
  const status = err.response?.status;
  const code = err.code;

  if (status === 429 || status === 500 || status === 502 || status === 503 || status === 504 || status === 546) {
    return true;
  }

  if (
    code === "ECONNRESET" ||
    code === "ECONNABORTED" ||
    code === "ETIMEDOUT" ||
    code === "EAI_AGAIN" ||
    code === "ENOTFOUND" ||
    code === "EHOSTUNREACH" ||
    code === "EPIPE"
  ) {
    return true;
  }

  return false;
}

/**
 * Low-level call to the Edge function with infinite retry on retryable errors.
 * phase: "start" | "append" | "complete"
 */
async function callEdgeFunction(phase, payload) {
  const edgeFunctionUrl = process.env.EDGE_FUNCTION_URL;
  const webhookSecret = process.env.ACS_UPLOAD_WEBHOOK_SECRET;

  if (!edgeFunctionUrl || !webhookSecret) {
    throw new Error("Missing EDGE_FUNCTION_URL or ACS_UPLOAD_WEBHOOK_SECRET environment variables");
  }

  payload.webhookSecret = webhookSecret;

  let attempt = 0;
  const MAX_BACKOFF_MS = 60000;

  // eslint-disable-next-line no-constant-condition
  while (true) {
    try {
      const res = await axios.post(edgeFunctionUrl, payload, {
        headers: {
          "Content-Type": "application/json",
          "x-webhook-secret": webhookSecret,
        },
        timeout: 300000,
      });
      return res.data;
    } catch (err) {
      const status = err.response?.status;
      const body = err.response?.data;
      const bodyCode = body?.code;
      const retryable = isRetryableUploadError(err);

      console.error(
        `❌ Edge function ${phase} failed (attempt ${
          attempt + 1
        }): ${err.message} (status: ${status || err.code || "unknown"})`,
      );

      // On worker limit, shrink CHUNK_SIZE if we're in append phase
      if (phase === "append" && (status === 546 || bodyCode === "WORKER_LIMIT") && CHUNK_SIZE > MIN_CHUNK_SIZE) {
        CHUNK_SIZE = Math.max(MIN_CHUNK_SIZE, Math.floor(CHUNK_SIZE / 2));
        console.log(`⚠️  Worker limit hit; reducing CHUNK_SIZE to ${CHUNK_SIZE}`);
      }

      if (!retryable) {
        console.error("💥 Non-retryable error in edge call, aborting.");
        throw err;
      }

      attempt += 1;
      const backoffMs = Math.min(2000 * Math.pow(2, attempt - 1), MAX_BACKOFF_MS);
      console.log(`⏳ Retrying ${phase} in ${backoffMs}ms (attempt ${attempt})...`);
      await sleep(backoffMs);
    }
  }
}

/**
 * High-level batch uploader.
 * - templatesData: { [templateId]: any[] }
 * - snapshotId: string | null
 * - summary: object (for start)
 * - isComplete: whether to call complete at the end
 */
export async function uploadBatch({ templatesData, snapshotId, summary, isComplete = true }) {
  const webhookSecret = process.env.ACS_UPLOAD_WEBHOOK_SECRET;
  const edgeFunctionUrl = process.env.EDGE_FUNCTION_URL;

  if (!edgeFunctionUrl || !webhookSecret) {
    throw new Error("Missing EDGE_FUNCTION_URL or ACS_UPLOAD_WEBHOOK_SECRET environment variables");
  }

  try {
    // Convert templatesData object to array format expected by edge function
    const templates = Object.entries(templatesData).map(([templateId, data]) => ({
      filename: templateId.replace(/[:.]/g, "_") + ".json",
      content: JSON.stringify(data, null, 2),
      templateId,
      chunkIndex: 0,
      totalChunks: 1,
      isChunked: false,
    }));

    console.log(`📦 Uploading ${templates.length} templates...`);

    // PHASE 1: Start - Create snapshot (only if no snapshotId provided)
    if (!snapshotId) {
      console.log(`[1/3] Creating snapshot...`);
      const startResult = await callEdgeFunction("start", {
        mode: "start",
        summary,
      });
      snapshotId = startResult.snapshot_id;
      console.log(`✅ Snapshot created: ${snapshotId}`);
    }

    // PHASE 2: Append - Upload templates in chunks
    console.log(
      `\n[2/3] Uploading templates in chunks (starting CHUNK_SIZE=${CHUNK_SIZE}, delay=${UPLOAD_DELAY_MS}ms)...`,
    );

    const initialConfiguredChunkSize = CHUNK_SIZE;
    let totalProcessedReported = 0;

    for (let i = 0; i < templates.length; ) {
      const chunk = templates.slice(i, i + CHUNK_SIZE);
      const chunkNum = Math.floor(i / CHUNK_SIZE) + 1;
      const totalChunks = Math.ceil(templates.length / CHUNK_SIZE);

      console.log(
        `   📤 Uploading chunk ${chunkNum}/${totalChunks} (${chunk.length} templates, CHUNK_SIZE=${CHUNK_SIZE})...`,
      );

      try {
        const result = await callEdgeFunction("append", {
          mode: "append",
          snapshot_id: snapshotId,
          templates: chunk,
        });

        const processed = typeof result?.processed === "number" ? result.processed : chunk.length;

        totalProcessedReported += processed;
        console.log(
          `   ✓ Chunk ${chunkNum}/${totalChunks} complete (${totalProcessedReported}/${templates.length} templates reported processed)`,
        );

        // On success, gently grow CHUNK_SIZE up to 2x the initial configured value
        const maxChunkSizeGrowth = Math.max(initialConfiguredChunkSize, MAX_CHUNK_SIZE);
        if (CHUNK_SIZE < maxChunkSizeGrowth) {
          CHUNK_SIZE = Math.min(maxChunkSizeGrowth, CHUNK_SIZE + 1);
        }

        i += chunk.length; // advance

        // Delay with jitter between chunks
        if (i < templates.length) {
          const jitter = UPLOAD_DELAY_MS * (0.8 + Math.random() * 0.4);
          await sleep(Math.floor(jitter));
        }
      } catch (err) {
        // callEdgeFunction already retried; if we get here it's non-retryable
        console.error(`❌ Fatal error uploading chunk ${chunkNum}: ${err.message}`);
        throw err;
      }
    }

    // PHASE 3: Complete - Mark snapshot as complete (only if isComplete flag is set)
    if (isComplete) {
      console.log(`\n[3/3] Finalizing snapshot ${snapshotId}...`);
      await callEdgeFunction("complete", {
        mode: "complete",
        snapshot_id: snapshotId,
      });
      console.log(`✅ Snapshot marked complete: ${snapshotId}`);
      console.log(`   Templates processed (reported): ${totalProcessedReported}`);
    }

    return snapshotId; // Return snapshot ID for reuse in next batch
  } catch (err) {
    console.error("\n❌ Upload failed:", err.message);
    throw err; // Re-throw to allow caller to handle
  }
}

// Allow running standalone for backward compatibility
if (import.meta.url === `file://${process.argv[1]}`) {
  (async () => {
    try {
      // Read summary
      const summaryData = JSON.parse(fs.readFileSync("circulating-supply-single-sv.json", "utf-8"));

      // Read all template files
      const acsDir = "./acs_full";
      const files = fs.readdirSync(acsDir);

      const templatesData = {};
      files.forEach((file) => {
        if (!file.endsWith(".json")) return;
        const filePath = path.join(acsDir, file);
        const content = JSON.parse(fs.readFileSync(filePath, "utf-8"));

        // Best-effort reconstruction of templateId; matches your previous convention
        const templateId = file.replace(".json", "").replace(/_/g, ":");
        templatesData[templateId] = content;
      });

      await uploadBatch({
        templatesData,
        snapshotId: null,
        summary: summaryData,
        isComplete: true,
      });

      console.log("\n✅ Standalone upload complete!");
      process.exit(0);
    } catch (err) {
      console.error("\n❌ Standalone upload failed:", err.message);
      process.exit(1);
    }
  })();
}
