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
import pg from "pg";
import { from as copyFrom } from "pg-copy-streams";
import { Readable } from "stream";

const { Client } = pg;

process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

// ---------- Config ----------

const BASE_URL = process.env.BASE_URL || "https://scan.sv-1.global.canton.network.sync.global/api/scan";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY;
const SUPABASE_DB_URL = process.env.SUPABASE_DB_URL;

if (!SUPABASE_URL || !SUPABASE_ANON_KEY) {
  console.error("❌ SUPABASE_URL or SUPABASE_ANON_KEY missing in env");
  process.exit(1);
}

if (!SUPABASE_DB_URL) {
  console.error("❌ SUPABASE_DB_URL missing in env - required for fast COPY operations");
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

// PostgreSQL client for fast COPY operations
let pgClient = null;

async function getPgClient() {
  if (!pgClient) {
    console.log("\n🔌 Attempting PostgreSQL connection...");
    
    // Parse and log connection details (masking password)
    const dbUrl = SUPABASE_DB_URL || '';
    const urlParts = dbUrl.match(/postgresql:\/\/([^:]+):([^@]+)@([^\/]+)\/(.+)/);
    if (urlParts) {
      console.log('🔍 DB Connection Debug:');
      console.log('  - User:', urlParts[1]);
      console.log('  - Password length:', urlParts[2] ? urlParts[2].length : 0, 'chars (ends with:', urlParts[2] ? '****' + urlParts[2].slice(-4) : 'MISSING', ')');
      console.log('  - Host:', urlParts[3]);
      console.log('  - Database:', urlParts[4]);
    } else {
      console.log('⚠️ Could not parse SUPABASE_DB_URL format');
      console.log('  - URL starts with:', dbUrl.substring(0, 30) + '...');
    }
    
    pgClient = new Client({
      connectionString: SUPABASE_DB_URL,
      ssl: { rejectUnauthorized: false },
    });
    
    try {
      await pgClient.connect();
      console.log("✅ PostgreSQL client connected for fast COPY operations");
    } catch (connError) {
      console.error("❌ PostgreSQL connection FAILED:");
      console.error("  - Error name:", connError.name);
      console.error("  - Error code:", connError.code);
      console.error("  - Error message:", connError.message);
      throw connError;
    }
  }
  return pgClient;
}

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
      const cursor_name = `backfill_m${migration_id}_${synchronizer_id.substring(0, 30)}`;
      const { data: inserted, error: insertError } = await supabase
        .from("backfill_cursors")
        .insert({
          cursor_name,
          migration_id,
          synchronizer_id,
          min_time,
          max_time,
          last_before: null,
          last_processed_round: 0,
          complete: false,
          updated_at: new Date().toISOString(),
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

// Fast bulk insert using PostgreSQL COPY with ON CONFLICT handling
async function bulkCopyWithUpsert(table, rows, columns) {
  if (!rows.length) return;

  const client = await getPgClient();
  const tempTable = `temp_${table}_${Date.now()}_${Math.floor(Math.random() * 1000)}`;

  try {
    // Create a regular (non-TEMP) table for staging
    // Temp tables don't work reliably with connection poolers
    const createTempTableQuery = `
      CREATE TABLE IF NOT EXISTS ${tempTable} (LIKE ${table} INCLUDING DEFAULTS);
    `;
    await client.query(createTempTableQuery);
    console.log(`   📋 Created staging table: ${tempTable}`);

    // Quote column names to handle reserved keywords like "offset"
    const quotedColumns = columns.map(c => `"${c}"`);

    // Prepare TSV data for COPY
    const tsvData = rows.map((row, rowIdx) => {
      return columns.map(col => {
        let val = row[col];
        if (val === null || val === undefined) return '\\N';
        
        // ============ CRITICAL: ARRAY HANDLING FIRST ============
        // Arrays MUST be converted to PostgreSQL syntax {elem1,elem2}
        // NEVER JSON syntax ["elem1","elem2"]
        
        // Step 1: If it's a stringified JSON array, parse it
        if (typeof val === 'string' && val.trim().startsWith('[')) {
          if (rowIdx === 0) console.log(`   🔄 Parsing stringified array in ${col}:`, val.substring(0, 100));
          try {
            val = JSON.parse(val);
            if (rowIdx === 0) console.log(`   ✅ Parsed to array:`, val);
          } catch (e) {
            console.error(`   ❌ Failed to parse array string in ${col}:`, e.message);
            return '{}';
          }
        }
        
        // Step 2: Convert JavaScript array → PostgreSQL array literal
        if (Array.isArray(val)) {
          if (val.length === 0) return '{}';
          
          // Format each element with proper escaping
          const elements = val.map(elem => {
            if (elem === null || elem === undefined) return 'NULL';
            const str = String(elem);
            // PostgreSQL needs quotes around elements with special chars
            if (str.match(/[{},"\\\s]/) || str === '' || str.toLowerCase() === 'null') {
              // Escape backslashes and quotes
              return '"' + str.replace(/\\/g, '\\\\').replace(/"/g, '\\"') + '"';
            }
            return str;
          });
          
          const pgArray = '{' + elements.join(',') + '}';
          
          if (rowIdx === 0 && (col === 'signatories' || col === 'observers')) {
            console.log(`   ✅ PostgreSQL array for ${col}:`, pgArray.substring(0, 200));
          }
          
          return pgArray;
        }
        
        // Step 3: Handle JSONB objects (not arrays!)
        if (typeof val === 'object') {
          const jsonStr = JSON.stringify(val);
          // Escape special chars for TSV format
          return jsonStr.replace(/\\/g, '\\\\').replace(/\t/g, '\\t').replace(/\n/g, '\\n').replace(/\r/g, '\\r');
        }
        
        // Handle strings - escape special characters for TSV
        if (typeof val === 'string') {
          return val.replace(/\\/g, '\\\\').replace(/\t/g, '\\t').replace(/\n/g, '\\n').replace(/\r/g, '\\r');
        }
        
        // Numbers and booleans - convert to string
        return String(val);
      }).join('\t');
    }).join('\n');

    // Use COPY to load data into staging table (with quoted column names)
    const copyQuery = `COPY ${tempTable} (${quotedColumns.join(', ')}) FROM STDIN WITH (FORMAT text, NULL '\\N')`;
    const stream = client.query(copyFrom(copyQuery));
    
    const readable = Readable.from([tsvData]);
    
    await new Promise((resolve, reject) => {
      stream.on('finish', resolve);
      stream.on('error', reject);
      readable.pipe(stream);
    });

    console.log(`   ⚡ COPY inserted ${rows.length} rows into staging table ${tempTable}`);

    // Upsert from staging table to actual table
    const conflictColumn = table === "ledger_updates" ? "update_id" : "event_id";
    const updateColumns = columns.filter(c => c !== conflictColumn);
    
    const upsertQuery = `
      INSERT INTO ${table} (${quotedColumns.join(', ')})
      SELECT ${quotedColumns.join(', ')} FROM ${tempTable}
      ON CONFLICT ("${conflictColumn}") DO UPDATE SET
        ${updateColumns.map(c => `"${c}" = EXCLUDED."${c}"`).join(', ')}
    `;
    
    console.log(`   🔍 DEBUG: Executing upsert query for ${table}:`);
    console.log(`   🔍 First 500 chars: ${upsertQuery.substring(0, 500)}`);
    
    const result = await client.query(upsertQuery);
    console.log(`   ✅ Upserted ${result.rowCount} rows to ${table}`);

    // Clean up staging table
    await client.query(`DROP TABLE IF EXISTS ${tempTable}`);
    console.log(`   🗑️  Dropped staging table: ${tempTable}`);

  } catch (error) {
    console.error(`   ❌ Error in bulkCopyWithUpsert for ${table}:`, error.message);
    // Try to clean up staging table even on error
    try {
      await client.query(`DROP TABLE IF EXISTS ${tempTable}`);
    } catch (cleanupError) {
      console.error(`   ⚠️  Could not clean up staging table ${tempTable}:`, cleanupError.message);
    }
    throw error;
  }
}

async function upsertInBatches(table, rows, batchSize = 2000) {
  if (!rows.length) return;

  // Determine columns from first row
  const columns = Object.keys(rows[0]);

  for (let i = 0; i < rows.length; i += batchSize) {
    const batch = rows.slice(i, i + batchSize);

    await retryWithBackoff(async () => {
      await bulkCopyWithUpsert(table, batch, columns);
    });

    console.log(`   📝 Processed batch (${i + batch.length}/${rows.length})`);
  }
}

// Helper to ensure value is an array, parsing JSON strings if needed
function ensureArray(value) {
  if (!value) return [];
  if (Array.isArray(value)) return value;
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (trimmed.startsWith('[')) {
      try {
        const parsed = JSON.parse(trimmed);
        return Array.isArray(parsed) ? parsed : [];
      } catch (e) {
        console.error(`   ⚠️  Failed to parse array string: ${trimmed.substring(0, 100)}...`);
        return [];
      }
    }
  }
  return [];
}

async function upsertUpdatesAndEvents(transactions) {
  if (!transactions.length) return;

  const updatesRows = [];
  const eventsRows = [];

  console.log(`   🔍 TRACE: Processing ${transactions.length} transactions`);

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
      update_type: kind,
      update_data: tx,
      round: 0,
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
          event_data: ce,
          round: 0,
          payload: ce.create_arguments || {},
          signatories: ensureArray(ce.signatories),
          observers: ensureArray(ce.observers),
          created_at_ts: ce.created_at,
          raw: ce,
          migration_id: tx.migration_id || tx.event?.migration_id || null,
        });
      }
    } else {
      const eventsById = tx.events_by_id || {};
      for (const [eventId, ev] of Object.entries(eventsById)) {
        let eventType = ev.event_type || ev.kind || "unknown";
        eventType = String(eventType).toLowerCase();

        const signatories = ensureArray(ev.signatories);
        const observers = ensureArray(ev.observers);
        
        // Debug first event to trace array handling
        if (eventsRows.length === 0) {
          console.log(`   🔍 TRACE: First event signatories:`, {
            rawType: typeof ev.signatories,
            rawValue: ev.signatories,
            isArray: Array.isArray(ev.signatories),
            afterEnsureArray: signatories,
            isArrayAfter: Array.isArray(signatories)
          });
        }

        eventsRows.push({
          event_id: eventId,
          update_id: updateId,
          contract_id: ev.contract_id || null,
          template_id: ev.template_id || null,
          package_name: ev.package_name || null,
          event_type: eventType,
          event_data: ev,
          round: 0,
          payload: ev.create_arguments || ev.exercise_arguments || {},
          signatories,
          observers,
          created_at_ts: ev.created_at || recordTime,
          raw: ev,
          migration_id: tx.migration_id || null,
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
  console.log("\n🔧 Environment Check:");
  console.log("   SUPABASE_URL present:", !!process.env.SUPABASE_URL);
  console.log("   SUPABASE_ANON_KEY present:", !!process.env.SUPABASE_ANON_KEY);
  console.log("   SUPABASE_DB_URL present:", !!process.env.SUPABASE_DB_URL);
  console.log("   SUPABASE_DB_URL length:", process.env.SUPABASE_DB_URL?.length || 0, "chars");
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
  
  // Close PostgreSQL connection
  if (pgClient) {
    await pgClient.end();
    console.log("✅ PostgreSQL client disconnected");
  }
}

run().catch(async (err) => {
  console.error("\n" + "=".repeat(80));
  console.error("❌ FATAL ERROR in backfill indexer");
  console.error("=".repeat(80));
  console.error("📋 Error Details:");
  console.error("  - Message:", err.message);
  console.error("  - Name:", err.name);
  console.error("  - Code:", err.code);
  console.error("\n📚 Stack Trace:");
  console.error(err.stack);
  
  console.error("\n🔧 Environment Variables at Error Time:");
  console.error("  - NODE_ENV:", process.env.NODE_ENV);
  console.error("  - SUPABASE_URL:", process.env.SUPABASE_URL ? "SET" : "MISSING");
  console.error("  - SUPABASE_ANON_KEY:", process.env.SUPABASE_ANON_KEY ? "SET (length: " + process.env.SUPABASE_ANON_KEY.length + ")" : "MISSING");
  console.error("  - SUPABASE_DB_URL:", process.env.SUPABASE_DB_URL ? "SET (length: " + process.env.SUPABASE_DB_URL.length + ")" : "MISSING");
  console.error("  - BASE_URL:", process.env.BASE_URL || "(using default)");
  console.error("=".repeat(80));
  
  // Close PostgreSQL connection on error
  if (pgClient) {
    try {
      await pgClient.end();
      console.error("✅ PostgreSQL client disconnected");
    } catch (closeErr) {
      console.error("⚠️ Error closing PostgreSQL connection:", closeErr.message);
    }
  }
  
  process.exit(1);
});
