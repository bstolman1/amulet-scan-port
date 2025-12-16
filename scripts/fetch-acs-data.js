/**
 * Hardened ACS Fetcher
 * - Throttled progress (about once per minute / every 20 pages)
 * - Progress failures NEVER crash the job
 * - Adaptive chunk sizing (handles 546 WORKER_LIMIT)
 * - Resumable snapshots via Supabase
 * - Range handling / retry for Canton ACS
 * - 🆕 Final flush: one template-chunk per append with persistent retry
 */

import axios from "axios";
import { Agent as HttpAgent } from "http";
import { Agent as HttpsAgent } from "https";
import fs from "fs";
import BigNumber from "bignumber.js";
import { createClient } from "@supabase/supabase-js";

// TLS config (secure by default)
// Set INSECURE_TLS=1 only in controlled environments with self-signed certs.
const INSECURE_TLS = ['1', 'true', 'yes'].includes(String(process.env.INSECURE_TLS || '').toLowerCase());
if (INSECURE_TLS) {
  process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";
}

// Canton client with keepalive
const cantonClient = axios.create({
  httpAgent: new HttpAgent({ keepAlive: true, keepAliveMsecs: 30000 }),
  httpsAgent: new HttpsAgent({
    keepAlive: true,
    keepAliveMsecs: 30000,
    rejectUnauthorized: !INSECURE_TLS,
  }),
  timeout: 120000,
});

// Env / config
const BASE_URL = process.env.BASE_URL || "https://scan.sv-1.global.canton.network.sync.global/api/scan";
const EDGE_FUNCTION_URL = process.env.EDGE_FUNCTION_URL;
const WEBHOOK_SECRET = process.env.ACS_UPLOAD_WEBHOOK_SECRET;

let UPLOAD_CHUNK_SIZE = parseInt(process.env.UPLOAD_CHUNK_SIZE || "5", 10);
const MIN_CHUNK_SIZE = 1;
const MAX_CHUNK_SIZE = parseInt(process.env.UPLOAD_CHUNK_SIZE || "5", 10);
const ENTRIES_PER_CHUNK = parseInt(process.env.ENTRIES_PER_CHUNK || "5000", 10);
const PAGE_SIZE = parseInt(process.env.PAGE_SIZE || "500", 10);
const MAX_INFLIGHT_UPLOADS = parseInt(process.env.MAX_INFLIGHT_UPLOADS || "2", 10);

// Supabase (for resume)
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY;
let supabase = null;
if (SUPABASE_URL && SUPABASE_ANON_KEY) {
  supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);
}

// Startup logging
console.log("\n" + "=".repeat(80));
console.log("🚀 Hardened ACS Data Fetcher - Starting");
console.log("=".repeat(80));
console.log("⚙️  Configuration:");
console.log(`   - Base URL: ${BASE_URL}`);
console.log(`   - Edge Function URL: ${EDGE_FUNCTION_URL ? "✅ Configured" : "❌ Not configured"}`);
console.log(`   - Webhook Secret: ${WEBHOOK_SECRET ? "✅ Configured" : "❌ Not configured"}`);
console.log(`   - Supabase URL: ${SUPABASE_URL ? "✅ Configured" : "❌ Not configured"}`);
console.log(`   - Supabase Anon Key: ${SUPABASE_ANON_KEY ? "✅ Configured" : "❌ Not configured"}`);
console.log(`   - Supabase Client: ${supabase ? "✅ Initialized" : "❌ Not initialized"}`);
console.log(`   - Page Size: ${PAGE_SIZE}`);
console.log(`   - Upload Chunk Size: ${UPLOAD_CHUNK_SIZE} (min: ${MIN_CHUNK_SIZE}, max: ${MAX_CHUNK_SIZE})`);
console.log(`   - Entries Per Chunk: ${ENTRIES_PER_CHUNK} (templates split if larger)`);
console.log(`   - Max In-Flight Uploads: ${MAX_INFLIGHT_UPLOADS}`);
console.log("=".repeat(80) + "\n");

// ---------- Utility helpers ----------

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function safeFileName(templateId) {
  return templateId.replace(/[:.]/g, "_");
}

function chunkTemplateEntries(templateId, entries) {
  const chunks = [];
  for (let i = 0; i < entries.length; i += ENTRIES_PER_CHUNK) {
    chunks.push({
      templateId,
      chunkIndex: chunks.length,
      totalChunks: Math.ceil(entries.length / ENTRIES_PER_CHUNK),
      entries: entries.slice(i, i + ENTRIES_PER_CHUNK),
    });
  }
  return chunks;
}

function isTemplate(e, moduleName, entityName) {
  const t = e?.template_id;
  if (!t) return false;
  const parts = t.split(":");
  const entity = parts.pop();
  const module_ = parts.pop();
  return module_ === moduleName && entity === entityName;
}

// ---------- SAFE progress sender (never throws) ----------

async function safeProgress(snapshotId, progress) {
  if (!EDGE_FUNCTION_URL || !WEBHOOK_SECRET || !snapshotId) return;

  const payload = {
    mode: "progress",
    webhookSecret: WEBHOOK_SECRET,
    snapshot_id: snapshotId,
    progress,
  };

  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      await axios.post(EDGE_FUNCTION_URL, payload, {
        headers: {
          "Content-Type": "application/json",
          "x-webhook-secret": WEBHOOK_SECRET,
        },
        timeout: 15000,
      });
      return; // success
    } catch (err) {
      const status = err.response?.status;
      console.log(`⚠️ Progress attempt ${attempt}/3 failed (status: ${status || "n/a"}): ${err.message}`);
      await sleep(2000 * attempt);
    }
  }

  console.log("❗ Progress permanently failed — continuing snapshot anyway.");
}

// ---------- Generic upload helper (for start/append/complete) ----------

async function uploadToEdgeFunction(phase, data, retryCount = 0) {
  if (!EDGE_FUNCTION_URL || !WEBHOOK_SECRET) return null;

  const MAX_RETRIES = 5;
  const is546Error = (error) => error.response?.status === 546 || error.message?.includes("546");

  try {
    const res = await axios.post(EDGE_FUNCTION_URL, data, {
      headers: {
        "Content-Type": "application/json",
        "x-webhook-secret": WEBHOOK_SECRET,
      },
      timeout: 300000, // up to 5 minutes for big batches
    });

    // On successful append, we can gently ramp chunk size back up
    if (phase === "append" && UPLOAD_CHUNK_SIZE < MAX_CHUNK_SIZE) {
      UPLOAD_CHUNK_SIZE = Math.min(UPLOAD_CHUNK_SIZE + 1, MAX_CHUNK_SIZE);
      console.log(`✅ Upload successful - increased template batch size to ${UPLOAD_CHUNK_SIZE}`);
    }

    return res.data;
  } catch (err) {
    const status = err.response?.status;
    console.error(`❌ Upload failed (${phase}): ${err.message} (status: ${status || "unknown"})`);

    // Special handling for 546 (Supabase worker limit) during normal append
    if (phase === "append" && is546Error(err) && retryCount < MAX_RETRIES) {
      if (UPLOAD_CHUNK_SIZE > MIN_CHUNK_SIZE) {
        UPLOAD_CHUNK_SIZE = Math.max(MIN_CHUNK_SIZE, Math.floor(UPLOAD_CHUNK_SIZE / 2));
        console.log(`⚠️  Reduced template batch size to ${UPLOAD_CHUNK_SIZE} due to 546 WORKER_LIMIT`);
      }

      const backoffMs = Math.min(2000 * Math.pow(2, retryCount), 32000);
      console.log(`⏳ Waiting ${backoffMs}ms before retry ${retryCount + 1}/${MAX_RETRIES}...`);
      await sleep(backoffMs);

      return uploadToEdgeFunction(phase, data, retryCount + 1);
    }

    // For start/complete or non-546 append failures: fail hard
    throw err;
  }
}

/**
 * 🆕 Final-stage helper:
 * Upload a SINGLE template-chunk with persistent retries.
 * - Wraps uploadToEdgeFunction("append", ...)
 * - If uploadToEdgeFunction exhausts its internal retries and throws,
 *   this will keep retrying on 546 / typical transient network errors.
 */
async function persistentAppendSingleTemplateChunk(snapshotId, templateChunk) {
  if (!snapshotId || !EDGE_FUNCTION_URL || !WEBHOOK_SECRET) return;

  const data = {
    mode: "append",
    webhookSecret: WEBHOOK_SECRET,
    snapshot_id: snapshotId,
    templates: [templateChunk],
  };

  let attempt = 0;

  while (true) {
    try {
      await uploadToEdgeFunction("append", data);
      return;
    } catch (err) {
      attempt++;
      const status = err.response?.status;
      const code = err.code;

      const isRetryable =
        status === 546 ||
        status === 502 ||
        status === 503 ||
        status === 504 ||
        status === 429 ||
        code === "ECONNRESET" ||
        code === "ETIMEDOUT" ||
        code === "ENOTFOUND" ||
        code === "ECONNABORTED" ||
        code === "EAI_AGAIN" ||
        code === "EHOSTUNREACH" ||
        code === "EPIPE";

      if (!isRetryable) {
        console.error(
          `❌ Final chunk upload for ${templateChunk.templateId} failed with non-retryable error (status: ${
            status || code || "unknown"
          }).`,
        );
        throw err;
      }

      const backoffMs = Math.min(60000, 2000 * Math.pow(2, Math.min(attempt, 6)));
      console.warn(
        `⏳ Final chunk upload retry ${attempt} for ${templateChunk.templateId} ` +
          `(chunk ${templateChunk.chunkIndex + 1}/${templateChunk.totalChunks}) in ${backoffMs}ms...`,
      );
      await sleep(backoffMs);
    }
  }
}

// ---------- Migration & snapshot timestamp ----------

// NEW: detect all migrations (1..N)
async function detectAllMigrations(baseUrl) {
  console.log("🔎 Probing for all valid migration IDs...");
  const migrations = [];
  let id = 1;

  while (true) {
    try {
      const res = await cantonClient.get(`${baseUrl}/v0/state/acs/snapshot-timestamp`, {
        params: { before: new Date().toISOString(), migration_id: id },
      });
      if (res.data?.record_time) {
        migrations.push(id);
        id++;
      } else {
        break;
      }
    } catch {
      break;
    }
  }

  if (migrations.length === 0) throw new Error("No valid migrations found.");
  console.log(`📘 Found migrations: [${migrations.join(", ")}]`);
  return migrations;
}

// Keep existing latest-migration helper (used in error handler)
async function detectLatestMigration(baseUrl) {
  console.log("🔎 Probing for latest valid migration ID...");
  let id = 1;
  let latest = null;

  while (true) {
    try {
      const res = await cantonClient.get(`${baseUrl}/v0/state/acs/snapshot-timestamp`, {
        params: { before: new Date().toISOString(), migration_id: id },
      });
      if (res.data?.record_time) {
        latest = id;
        id++;
      } else {
        break;
      }
    } catch {
      break;
    }
  }

  if (!latest) throw new Error("No valid migration found.");
  console.log(`📘 Using latest migration_id: ${latest}`);
  return latest;
}

async function fetchSnapshotTimestamp(baseUrl, migration_id) {
  const res = await cantonClient.get(`${baseUrl}/v0/state/acs/snapshot-timestamp`, {
    params: { before: new Date().toISOString(), migration_id },
  });

  let record_time = res.data.record_time;
  console.log(`📅 Initial snapshot timestamp: ${record_time}`);

  const verify = await cantonClient.get(`${baseUrl}/v0/state/acs/snapshot-timestamp`, {
    params: { before: record_time, migration_id },
  });

  if (verify.data?.record_time && verify.data.record_time !== record_time) {
    record_time = verify.data.record_time;
    console.log(`🔁 Updated to verified snapshot: ${record_time}`);
  }

  return record_time;
}

// ---------- Resume support via Supabase ----------

async function checkForExistingSnapshot(migration_id) {
  if (!supabase) {
    console.log("\n⚠️  Supabase not configured - cannot resume existing snapshots. Will always start new.");
    return null;
  }

  console.log("\n" + "-".repeat(80));
  console.log("🔍 Checking for existing in-progress snapshots...");
  console.log(`   - Query: acs_snapshots WHERE migration_id=${migration_id} AND status='processing'`);

  const { data, error } = await supabase
    .from("acs_snapshots")
    .select("*")
    .eq("migration_id", migration_id)
    .eq("status", "processing")
    .order("started_at", { ascending: false })
    .limit(1);

  if (error) {
    console.error("❌ Error querying snapshots:", error.message);
    return null;
  }

  if (!data || data.length === 0) {
    console.log("ℹ️  No in-progress snapshots found.");
    console.log("-".repeat(80) + "\n");
    return null;
  }

  const snapshot = data[0];
  const startedAt = new Date(snapshot.started_at);
  const now = new Date();
  const runtimeMinutes = ((now - startedAt) / 1000 / 60).toFixed(1);

  console.log("✅ FOUND EXISTING IN-PROGRESS SNAPSHOT - WILL RESUME");
  console.log(`   - Snapshot ID: ${snapshot.id}`);
  console.log(`   - Started: ${snapshot.started_at} (${runtimeMinutes} minutes ago)`);
  console.log(`   - Processed Pages: ${snapshot.processed_pages || 0}`);
  console.log(`   - Processed Events: ${snapshot.processed_events || 0}`);
  console.log(`   - Cursor Position: ${snapshot.cursor_after || 0}`);
  console.log(`   - Amulet Total: ${snapshot.amulet_total || "0"}`);
  console.log(`   - Locked Total: ${snapshot.locked_total || "0"}`);
  console.log("-".repeat(80) + "\n");

  return snapshot;
}

// NEW: does this migration already have at least one completed snapshot?
async function hasCompletedSnapshot(migration_id) {
  if (!supabase) return false;

  const { data, error } = await supabase
    .from("acs_snapshots")
    .select("id")
    .eq("migration_id", migration_id)
    .eq("status", "completed")
    .limit(1);

  if (error) {
    console.error(`❌ Error checking completed snapshot for migration ${migration_id}:`, error.message);
    return false;
  }

  return !!(data && data.length > 0);
}

// ---------- Main fetch loop ----------

async function fetchAllACS(baseUrl, migration_id, record_time, existingSnapshot) {
  console.log("📦 Fetching ACS snapshot and uploading in real-time…");
  console.log(`   - Migration ID: ${migration_id}`);
  console.log(`   - Record Time: ${record_time}`);

  const allEvents = [];
  let after = existingSnapshot?.cursor_after || 0;
  let page = existingSnapshot?.processed_pages || 1;
  const seen = new Set();

  let amuletTotal = new BigNumber(existingSnapshot?.amulet_total || 0);
  let lockedTotal = new BigNumber(existingSnapshot?.locked_total || 0);

  const perPackage = {};
  const templatesByPackage = {};
  const templatesData = {};
  const pendingUploads = {};

  const outputDir = "./acs_full";
  if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir);

  // Snapshot metadata
  let snapshotId = existingSnapshot?.id || null;
  let canonicalPkg = existingSnapshot?.canonical_package || "unknown";

  const startTime = Date.now();
  let lastProgressLog = startTime;
  let lastProgressSent = startTime;

  // Start or resume snapshot
  if (existingSnapshot) {
    console.log("\n" + "🔄".repeat(40));
    console.log("🔄 RESUMING EXISTING SNAPSHOT");
    console.log("🔄".repeat(40));
    console.log(`   - Snapshot ID: ${snapshotId}`);
    console.log(`   - Resuming from Page: ${page}`);
    console.log(`   - Resuming from Cursor: ${after}`);
    console.log(`   - Previous Amulet Total: ${amuletTotal.toString()}`);
    console.log(`   - Previous Locked Total: ${lockedTotal.toString()}`);
    console.log("🔄".repeat(40) + "\n");
  } else if (EDGE_FUNCTION_URL && WEBHOOK_SECRET) {
    console.log("\n" + "🚀".repeat(40));
    console.log("🚀 CREATING NEW SNAPSHOT");
    console.log("🚀".repeat(40));
    const startResult = await uploadToEdgeFunction("start", {
      mode: "start",
      webhookSecret: WEBHOOK_SECRET,
      summary: {
        sv_url: baseUrl,
        migration_id,
        record_time,
        canonical_package: canonicalPkg,
        totals: {
          amulet: "0",
          locked: "0",
          circulating: "0",
        },
        entry_count: 0,
      },
    });
    snapshotId = startResult?.snapshot_id;
    console.log(`   ✅ New Snapshot Created: ${snapshotId}`);
    console.log("🚀".repeat(40) + "\n");
  }

  const inflightUploads = [];

  const MAX_RETRIES = 8;
  const BASE_DELAY = 3000;
  const MAX_PAGE_COOLDOWNS = 2;
  const COOLDOWN_AFTER_FAIL_MS = parseInt(process.env.RETRY_COOLDOWN_MS || "15000", 10);
  const JITTER_MS = 500;

  let lastPage = false;

  while (true) {
    let retryCount = 0;
    let cooldowns = 0;
    let pageSuccess = false;

    while (retryCount < MAX_RETRIES && !pageSuccess) {
      try {
        const res = await cantonClient.post(
          `${baseUrl}/v0/state/acs`,
          {
            migration_id,
            record_time,
            page_size: PAGE_SIZE,
            after,
            daml_value_encoding: "compact_json",
          },
          {
            headers: { "Content-Type": "application/json" },
            timeout: 120000,
          },
        );

        const events = res.data.created_events || [];
        const rangeTo = res.data.range?.to;

        if (!events.length) {
          console.log("\n✅ No more events — finished.");
          pageSuccess = true;
          lastPage = true;
          break;
        }

        const pageTemplates = new Set();

        for (const e of events) {
          const id = e.contract_id || e.event_id;
          if (id && seen.has(id)) continue;
          seen.add(id);

          const templateId = e.template_id || "unknown";
          const pkg = templateId.split(":")[0] || "unknown";
          perPackage[pkg] ||= { amulet: new BigNumber(0), locked: new BigNumber(0) };
          templatesByPackage[pkg] ||= new Set();
          templatesData[templateId] ||= [];

          templatesByPackage[pkg].add(templateId);
          pageTemplates.add(templateId);

          const { create_arguments } = e;
          templatesData[templateId].push(create_arguments || {});

          if (isTemplate(e, "Splice.Amulet", "Amulet")) {
            const amt = new BigNumber(create_arguments?.amount?.initialAmount ?? "0");
            amuletTotal = amuletTotal.plus(amt);
            perPackage[pkg].amulet = perPackage[pkg].amulet.plus(amt);
          } else if (isTemplate(e, "Splice.Amulet", "LockedAmulet")) {
            const amt = new BigNumber(create_arguments?.amulet?.amount?.initialAmount ?? "0");
            lockedTotal = lockedTotal.plus(amt);
            perPackage[pkg].locked = perPackage[pkg].locked.plus(amt);
          }
        }

        allEvents.push(...events);

        // Add templates from this page into pending upload buffer
        for (const templateId of pageTemplates) {
          pendingUploads[templateId] = templatesData[templateId];
        }

        // Throttled pipelined uploads (UNCHANGED behavior)
        if (snapshotId && Object.keys(pendingUploads).length >= UPLOAD_CHUNK_SIZE) {
          // Respect max inflight uploads
          while (inflightUploads.length >= MAX_INFLIGHT_UPLOADS) {
            await Promise.race(inflightUploads);
            for (let i = inflightUploads.length - 1; i >= 0; i--) {
              if (inflightUploads[i].settled) inflightUploads.splice(i, 1);
            }
          }

          const allKeys = Object.keys(pendingUploads);
          const batchKeys = allKeys.slice(0, UPLOAD_CHUNK_SIZE);

          const chunkedTemplates = [];
          for (const templateId of batchKeys) {
            const entries = pendingUploads[templateId];
            const count = entries.length;

            if (count > ENTRIES_PER_CHUNK) {
              console.log(`📦 Splitting ${templateId} (${count} entries) into chunks of ${ENTRIES_PER_CHUNK}...`);
              const chunks = chunkTemplateEntries(templateId, entries);
              chunkedTemplates.push(
                ...chunks.map((chunk) => ({
                  filename: `${safeFileName(chunk.templateId)}_chunk_${chunk.chunkIndex}.json`,
                  content: JSON.stringify(chunk.entries, null, 2),
                  templateId: chunk.templateId,
                  chunkIndex: chunk.chunkIndex,
                  totalChunks: chunk.totalChunks,
                  isChunked: true,
                })),
              );
            } else {
              chunkedTemplates.push({
                filename: `${safeFileName(templateId)}.json`,
                content: JSON.stringify(entries, null, 2),
                templateId,
                chunkIndex: 0,
                totalChunks: 1,
                isChunked: false,
              });
            }
          }

          console.log(
            `📤 Starting upload of ${chunkedTemplates.length} chunks from ${batchKeys.length} templates (template batch size: ${UPLOAD_CHUNK_SIZE})...`,
          );

          // Snapshot the batch so we can re-queue on failure
          const uploadSnapshot = {};
          batchKeys.forEach((k) => {
            uploadSnapshot[k] = pendingUploads[k];
            delete pendingUploads[k];
          });

          const uploadPromise = (async () => {
            try {
              await uploadToEdgeFunction("append", {
                mode: "append",
                webhookSecret: WEBHOOK_SECRET,
                snapshot_id: snapshotId,
                templates: chunkedTemplates,
              });

              console.log(`✅ Batch uploaded successfully (${chunkedTemplates.length} chunks)`);
            } catch (err) {
              console.error(`❌ Batch upload failed (append): ${err.message}. Re-queuing templates.`);
              Object.assign(pendingUploads, uploadSnapshot);
            }
            uploadPromise.settled = true;
          })();

          inflightUploads.push(uploadPromise);
        }

        // Page-level logging
        console.log(`📄 Page ${page} fetched (${events.length} events)`);

        const now = Date.now();
        const elapsedMs = now - startTime;
        const elapsedMinutes = elapsedMs / 1000 / 60;
        const pagesPerMin = elapsedMinutes > 0 ? (page / elapsedMinutes).toFixed(2) : "0.00";
        const eventsPerPage = page > 0 ? Math.round(allEvents.length / page) : 0;

        // Console status every 10 pages
        if (page % 10 === 0) {
          console.log("\n" + "-".repeat(80));
          console.log(`📊 STATUS UPDATE - Page ${page}`);
          console.log("-".repeat(80));
          console.log(`   - Snapshot ID: ${snapshotId || "N/A"}`);
          console.log(`   - Events Processed: ${allEvents.length.toLocaleString()}`);
          console.log(`   - Elapsed Time: ${elapsedMinutes.toFixed(1)} minutes`);
          console.log(`   - Processing Speed: ${pagesPerMin} pages/min, ${eventsPerPage} events/page`);
          console.log(`   - Amulet Total: ${amuletTotal.toString()}`);
          console.log(`   - Locked Total: ${lockedTotal.toString()}`);
          console.log(`   - In-flight Uploads: ${inflightUploads.length}/${MAX_INFLIGHT_UPLOADS}`);
          console.log("-".repeat(80) + "\n");
          lastProgressLog = now;
        }

        // Throttled progress to Supabase (safe & non-fatal)
        const shouldSendProgress = now - lastProgressSent >= 60000 || page % 20 === 0;

        if (snapshotId && shouldSendProgress) {
          await safeProgress(snapshotId, {
            processed_pages: page,
            processed_events: allEvents.length,
            elapsed_time_ms: elapsedMs,
            pages_per_minute: parseFloat(pagesPerMin),
          });
          lastProgressSent = now;
        }

        // Pagination
        if (events.length < PAGE_SIZE) {
          console.log("\n✅ Last page reached.");
          pageSuccess = true;
          lastPage = true;
          break;
        }

        after = rangeTo ?? after + events.length;
        page++;
        pageSuccess = true;
      } catch (err) {
        const statusCode = err.response?.status;
        const msg = err.response?.data?.error || err.message;

        const isRetryable =
          statusCode === 502 ||
          statusCode === 503 ||
          statusCode === 504 ||
          statusCode === 429 ||
          err.code === "ECONNRESET" ||
          err.code === "ETIMEDOUT" ||
          err.code === "ENOTFOUND" ||
          err.code === "ECONNABORTED" ||
          err.code === "EAI_AGAIN" ||
          err.code === "EHOSTUNREACH" ||
          err.code === "EPIPE";

        // Range error from Canton (reset offset)
        const rangeMatch = msg.match(/range\s*\((\d+)\s*to\s*(\d+)\)/i);
        if (rangeMatch) {
          const minRange = parseInt(rangeMatch[1]);
          const maxRange = parseInt(rangeMatch[2]);
          console.log(`📘 Detected snapshot range: ${minRange}–${maxRange}`);
          after = minRange;
          console.log(`🔁 Restarting from offset ${after}…`);
          pageSuccess = true;
          break;
        }

        if (isRetryable && retryCount < MAX_RETRIES - 1) {
          retryCount++;
          const delay = BASE_DELAY * Math.pow(2, retryCount - 1);
          const jitter = Math.floor(Math.random() * JITTER_MS);
          console.warn(`⚠️ Page ${page} failed (status ${statusCode || err.code}): ${msg}`);
          console.log(`🔄 Retry ${retryCount}/${MAX_RETRIES} in ${delay + jitter}ms...`);
          await sleep(delay + jitter);
          continue;
        }

        if (isRetryable && cooldowns < MAX_PAGE_COOLDOWNS) {
          cooldowns++;
          const cooldownDelay = COOLDOWN_AFTER_FAIL_MS * cooldowns;
          console.warn(
            `⏳ Page ${page} still failing. Cooling down ${cooldownDelay}ms (${cooldowns}/${MAX_PAGE_COOLDOWNS})...`,
          );
          await sleep(cooldownDelay);
          retryCount = 0;
          continue;
        }

        console.error(`❌ Page ${page} failed after ${retryCount + 1} attempts: ${msg}`);
        throw err;
      }
    }

    if (!pageSuccess) {
      console.error("❌ Stopping due to repeated page failures.");
      break;
    }

    if (lastPage) {
      console.log("✅ Reached last page, exiting main loop.");
      break;
    }
  }

  console.log(`\n✅ Fetched ${allEvents.length.toLocaleString()} ACS entries.`);

  // Wait for in-flight uploads to finish
  if (inflightUploads.length > 0) {
    console.log(`⏳ Waiting for ${inflightUploads.length} in-flight uploads...`);
    await Promise.all(inflightUploads);
  }

  // 🆕 Final templates upload (whatever remains in pendingUploads),
  // uploaded ONE CHUNK PER APPEND with persistent retry.
  if (snapshotId && Object.keys(pendingUploads).length > 0) {
    console.log(`📤 Uploading final ${Object.keys(pendingUploads).length} templates (one chunk per append)...`);
    const chunkedTemplates = [];

    for (const [templateId, entries] of Object.entries(pendingUploads)) {
      const count = entries.length;
      if (count > ENTRIES_PER_CHUNK) {
        console.log(`📦 Splitting ${templateId} (${count} entries) into chunks of ${ENTRIES_PER_CHUNK}...`);
        const chunks = chunkTemplateEntries(templateId, entries);
        chunkedTemplates.push(
          ...chunks.map((chunk) => ({
            filename: `${safeFileName(chunk.templateId)}_chunk_${chunk.chunkIndex}.json`,
            content: JSON.stringify(chunk.entries, null, 2),
            templateId: chunk.templateId,
            chunkIndex: chunk.chunkIndex,
            totalChunks: chunk.totalChunks,
            isChunked: true,
          })),
        );
      } else {
        chunkedTemplates.push({
          filename: `${safeFileName(templateId)}.json`,
          content: JSON.stringify(entries, null, 2),
          templateId,
          chunkIndex: 0,
          totalChunks: 1,
          isChunked: false,
        });
      }
    }

    for (const tpl of chunkedTemplates) {
      console.log(`📤 Uploading final chunk for ${tpl.templateId} (chunk ${tpl.chunkIndex + 1}/${tpl.totalChunks})...`);
      await persistentAppendSingleTemplateChunk(snapshotId, tpl);
    }

    console.log("✅ Final pending templates uploaded.");
  }

  // Write local per-template backup
  for (const [templateId, data] of Object.entries(templatesData)) {
    const fileName = `${outputDir}/${safeFileName(templateId)}.json`;
    fs.writeFileSync(fileName, JSON.stringify(data, null, 2));
  }
  console.log(`📂 Exported ${Object.keys(templatesData).length} template files to ${outputDir}/`);

  // Determine canonical package & templates
  const canonicalPkgEntry = Object.entries(perPackage).sort((a, b) => b[1].amulet.minus(a[1].amulet))[0];
  canonicalPkg = canonicalPkgEntry ? canonicalPkgEntry[0] : "unknown";

  const canonicalTemplates = templatesByPackage[canonicalPkg] ? Array.from(templatesByPackage[canonicalPkg]) : [];

  const circulatingSupply = amuletTotal.plus(lockedTotal);

  const summary = {
    amulet_total: amuletTotal.toString(),
    locked_total: lockedTotal.toString(),
    circulating_supply: circulatingSupply.toString(),
    canonical_package: canonicalPkg,
    templates: canonicalTemplates,
    migration_id,
    record_time,
  };

  fs.writeFileSync("./circulating-supply-single-sv.json", JSON.stringify(summary, null, 2));
  console.log("📄 Wrote summary to circulating-supply-single-sv.json\n");

  // Mark snapshot complete
  if (snapshotId) {
    console.log("🏁 Marking snapshot as complete...");
    await uploadToEdgeFunction("complete", {
      mode: "complete",
      webhookSecret: WEBHOOK_SECRET,
      snapshot_id: snapshotId,
      summary: {
        totals: {
          amulet: amuletTotal.toString(),
          locked: lockedTotal.toString(),
          circulating: circulatingSupply.toString(),
        },
        entry_count: allEvents.length,
        canonical_package: canonicalPkg,
      },
    });
    console.log("✅ Snapshot completed!");
  } else {
    console.log("⚠️ No snapshot record created (missing EDGE_FUNCTION_URL or ACS_UPLOAD_WEBHOOK_SECRET).");
  }

  return {
    allEvents,
    amuletTotal,
    lockedTotal,
    canonicalPkg,
    canonicalTemplates,
    snapshotId,
  };
}

// ---------- Entrypoint ----------

async function run() {
  try {
    console.log("\n" + "=".repeat(80));
    console.log("🔎 STEP 1: Detecting All Migrations");
    console.log("=".repeat(80));

    const migrationIds = await detectAllMigrations(BASE_URL);
    const latestMigrationId = Math.max(...migrationIds);
    const frozenMigrationIds = migrationIds.filter((id) => id !== latestMigrationId);

    console.log(`📘 Latest migration: ${latestMigrationId}`);
    console.log(`📦 Frozen (historical) migrations: [${frozenMigrationIds.join(", ") || "none"}]`);

    // 1️⃣ Bootstrap / finalize older migrations (1..N-1) ONCE
    for (const mid of frozenMigrationIds) {
      console.log("\n" + "=".repeat(80));
      console.log(`📦 BOOTSTRAP STEP: Migration ${mid}`);
      console.log("=".repeat(80));

      const alreadyComplete = await hasCompletedSnapshot(mid);
      if (alreadyComplete) {
        console.log(`✅ Migration ${mid} already has at least one completed snapshot. Skipping.`);
        continue;
      }

      console.log(`🆕 No completed snapshot for migration ${mid} — creating or resuming one.`);

      const record_time = await fetchSnapshotTimestamp(BASE_URL, mid);
      const existingSnapshot = await checkForExistingSnapshot(mid);

      const startTime = Date.now();
      await fetchAllACS(BASE_URL, mid, record_time, existingSnapshot);
      const elapsedMinutes = ((Date.now() - startTime) / 1000 / 60).toFixed(2);

      console.log(`✅ Migration ${mid} final snapshot completed in ${elapsedMinutes} minutes.`);
    }

    // 2️⃣ Live migration (latest) — this is what your 3-hour cron should keep updating
    console.log("\n" + "=".repeat(80));
    console.log(`🚀 LIVE STEP: Latest Migration ${latestMigrationId}`);
    console.log("=".repeat(80));

    const record_time = await fetchSnapshotTimestamp(BASE_URL, latestMigrationId);
    const existingSnapshot = await checkForExistingSnapshot(latestMigrationId);

    console.log("\n" + "=".repeat(80));
    if (existingSnapshot) {
      console.log("🔄 DECISION: RESUMING EXISTING SNAPSHOT FOR LATEST MIGRATION");
      console.log("=".repeat(80));
      console.log("   This GitHub Actions run is continuing a previous snapshot (long-running cron).");
      console.log(`   - Snapshot: ${existingSnapshot.id}`);
      console.log(`   - Resume from page: ${existingSnapshot.processed_pages || 1}`);
      console.log(`   - Resume from cursor: ${existingSnapshot.cursor_after || 0}`);
    } else {
      console.log("🆕 DECISION: STARTING NEW SNAPSHOT FOR LATEST MIGRATION");
      console.log("=".repeat(80));
      console.log("   No in-progress snapshot found. Creating a new one.");
      console.log(`   - Migration ID: ${latestMigrationId}`);
      console.log(`   - Record Time: ${record_time}`);
    }
    console.log("=".repeat(80) + "\n");

    const startTime = Date.now();
    const { allEvents, amuletTotal, lockedTotal, canonicalPkg } = await fetchAllACS(
      BASE_URL,
      latestMigrationId,
      record_time,
      existingSnapshot,
    );

    const elapsedMinutes = ((Date.now() - startTime) / 1000 / 60).toFixed(2);

    console.log("\n" + "=".repeat(80));
    console.log("✅ LATEST MIGRATION SNAPSHOT COMPLETED SUCCESSFULLY");
    console.log("=".repeat(80));
    console.log(`   - Migration ID: ${latestMigrationId}`);
    console.log(`   - Total Events: ${allEvents.length.toLocaleString()}`);
    console.log(`   - Canonical Package: ${canonicalPkg}`);
    console.log(`   - Amulet Total: ${amuletTotal.toString()}`);
    console.log(`   - Locked Total: ${lockedTotal.toString()}`);
    console.log(`   - Elapsed Time: ${elapsedMinutes} minutes`);
    console.log("=".repeat(80) + "\n");
  } catch (err) {
    console.error("\n" + "=".repeat(80));
    console.error("❌ FATAL ERROR");
    console.error("=".repeat(80));
    console.error("Error Message:", err.message);
    console.error("Error Stack:", err.stack);
    if (err.response) {
      console.error("Response Status:", err.response.status);
      try {
        console.error(
          "Response Data:",
          typeof err.response.data === "string" ? err.response.data : JSON.stringify(err.response.data, null, 2),
        );
      } catch {
        console.error("Response Data: [unserializable]");
      }
    }
    console.error("=".repeat(80) + "\n");

    // Try to mark snapshot as failed if we have a snapshot ID
    if (supabase) {
      try {
        // Try to get the snapshot ID from the existing snapshot check
        const migration_id = await detectLatestMigration(BASE_URL);
        const { data: failedSnapshot } = await supabase
          .from("acs_snapshots")
          .select("id")
          .eq("migration_id", migration_id)
          .eq("status", "processing")
          .order("started_at", { ascending: false })
          .limit(1)
          .single();

        if (failedSnapshot?.id) {
          console.error(`🔄 Marking snapshot ${failedSnapshot.id} as failed...`);
          await supabase
            .from("acs_snapshots")
            .update({
              status: "failed",
              error_message: `Workflow failed: ${err.message}`,
              completed_at: new Date().toISOString(),
            })
            .eq("id", failedSnapshot.id);
          console.error(`✅ Snapshot ${failedSnapshot.id} marked as failed`);
        }
      } catch (updateErr) {
        console.error("Failed to mark snapshot as failed:", updateErr.message);
      }
    }

    process.exit(1);
  }
}

run();
