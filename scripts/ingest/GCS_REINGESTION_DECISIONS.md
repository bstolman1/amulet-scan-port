# GCS Data Re-Ingestion: Decisions & Context Log

**Date range:** March 30 - April 5, 2026
**Participants:** Josefin (operator), Claude (engineering)
**Branch:** `claude/fix-gcs-data-ingestion-z1Dnr`
**VM:** `governance-dashboard` (8 CPU, 31GB RAM, 49GB disk, GCP)

---

## 1. Problem Statement

The data ingestion pipeline from Canton Scan API to Google Cloud Storage (`canton-bucket`) broke around March 11, 2026. Three distinct data issues were identified:

- **`raw/updates/events/`** — Missing data entirely for March 20 and 21
- **`raw/updates/updates/`** — Corrupted/incomplete data from March 11-17 (file counts dropped from ~5000/day to ~25/day)
- **`raw/backfill/`** — Ends around March 3, 2026; `raw/updates/` picks up from there

The root cause of the original pipeline failure was not definitively identified, but the data corruption pattern suggests the live `fetch-updates.js` process crashed or stalled around March 11 and was either not restarted or restarted incorrectly.

---

## 2. Architecture Context

### Data Flow
```
Canton Scan API (/v2/updates) → fetch-updates.js → Parquet files → /tmp/ledger_raw/ → GCS (canton-bucket)
```

### GCS Bucket Structure
```
canton-bucket/raw/
  backfill/{updates,events}/migration=N/year=Y/month=M/day=D/
  updates/{updates,events}/migration=N/year=Y/month=M/day=D/
```

- Partition paths use **unpadded** numeric values: `month=3`, `day=5` (NOT `month=03`, `day=05`)
- `getUtcPartition()` in `data-schema.js` returns raw integers

### Key Components
| File | Role |
|------|------|
| `fetch-updates.js` | Live continuous ingestion (forward pagination via `/v2/updates` with `after` semantics) |
| `fetch-backfill.js` | Historical backfill (backward pagination via `/v0/backfilling/updates-before`) |
| `reingest-updates.js` | **NEW** — Targeted re-ingestion tool for fixing gaps (created during this work) |
| `write-parquet.js` | Parquet file writer with GCS upload pipeline |
| `parquet-worker.js` | DuckDB-based worker that converts JSONL → Parquet |
| `data-schema.js` | Schema definitions, `normalizeUpdate()`, `normalizeEvent()`, partition path generation |
| `gcs-upload.js` / `gcs-upload-queue.js` | Upload to GCS with retry and backpressure |
| `path-utils.js` | Path utilities, `isGCSMode()`, `getTmpRawDir()` |
| `atomic-cursor.js` | Crash-safe cursor persistence |

### How DuckDB Reads Data
- DuckDB reads from **local disk** (`DATA_DIR/raw`), NOT directly from GCS
- Uses recursive glob: `read_parquet('${basePath}/**/updates-*.parquet')` — reads both backfill and updates folders
- Uses `UNION ALL` — **does not deduplicate**. Duplicate `update_id` across parquet files will appear as duplicate rows in queries.
- GCS is archival/primary storage; local disk is the query source

### Process Management
- PM2 manages only the API server (`duckdb-api` / `server.js`)
- Ingestion scripts (`fetch-updates.js`, `fetch-backfill.js`) run as standalone processes — no supervisor
- No automatic restart on crash

### Cursors
- **Backfill cursor:** `/home/ben/ledger_data/cursors/cursor-4-global-domain__*.json` — contains `max_time` field
- **Live cursor:** `/home/ben/ledger_data/cursors/live-cursor.json` — contains `record_time`, `migration_id`
- Live cursor saved every 5 batches and on empty poll; worst case 5 batches of re-processing on crash

---

## 3. Key Decisions

### Decision 1: Full Re-Ingestion (Option C)

**Choice:** Clean ALL `raw/updates/` data for migration 4 from March 3-30 and re-ingest everything from scratch.

**Alternatives considered:**
- Option A: Patch only missing days (March 20-21 events) — risk of hidden corruption in other days
- Option B: Patch March 11-21 only — still risk of gaps at boundaries

**Reason:** User explicitly prioritized data integrity over speed: "no missing data and no duplicated data, extremely important." Full re-ingestion is the only approach that guarantees complete, clean data across the entire affected range.

### Decision 2: Backfill Boundary as Start Point

**Choice:** Start re-ingestion from the backfill cursor's `max_time` (`2026-03-03T00:20:04.274468Z`), not from `2026-03-03T00:00:00Z`.

**Reason:** The backfill data covers March 3 from 00:00 to 00:20. Starting re-ingestion from the exact backfill boundary prevents any overlap between `raw/backfill/` and `raw/updates/`. The Canton API's `after` semantics guarantee records returned are strictly after the given timestamp.

### Decision 3: Keep March 3 Backfill Data

**Choice:** Leave `raw/backfill/{updates,events}/migration=4/year=2026/month=3/day=3/` (11 update files, 30 event files) intact.

**Reason:** The backfill covers 00:00-00:20 on March 3, and re-ingested updates cover 00:20 onward. Together they provide complete coverage with no gap and no overlap. Deleting the backfill would create a 20-minute gap. DuckDB's recursive glob reads both folders, so the data is seamlessly queryable.

### Decision 4: Migration 4 Only

**Choice:** Re-ingest only migration 4 data.

**Reason:** Only migration 4 has data in the March 3-30 range. The backfill cursor for migration 4 confirms this. Other migrations' data is either before this range or nonexistent.

### Decision 5: Clean Before Re-Ingest (--clean flag)

**Choice:** Delete all existing `raw/updates/` files for the target date range before re-ingesting.

**Reason:** Prevents any possibility of duplicate data from old corrupted files mixing with new clean data. The 228,552 files deleted were the corrupted/incomplete data from the broken pipeline.

### Decision 6: Conservative Resource Settings for Re-Ingestion

**Choice:** Override `.env` settings in `reingest-updates.js`:
| Setting | Default (.env) | Re-Ingestion Override | Reason |
|---------|---------------|----------------------|--------|
| `PARQUET_WORKERS` | 12 | 6 | Prevent OOM on 31GB VM |
| `GCS_UPLOAD_CONCURRENCY` | 48 | 16 | Fewer open connections/buffers |
| `MAX_ROWS_PER_FILE` | 100,000 | 50,000 | Smaller memory footprint per file |
| `MIN_ROWS_PER_FILE` | 25,000 | 10,000 | Match lower max |
| `FLUSH_EVERY` | 50 batches | 20 batches | Less data buffered before writing |

**Reason:** The VM previously hit 23GB RAM usage and became unresponsive. Re-ingestion is a long-running bulk process where stability matters more than throughput. These settings reduced RAM from 23GB to ~3GB.

### Decision 7: Resume Strategy on Interruption

**Choice:** When the script is interrupted mid-day:
1. Identify the last **complete day** (script moved past it to the next day)
2. Delete all data for the **partial day** from GCS
3. Resume with `--after=<end-of-last-complete-day>` (e.g., `--after=2026-03-10T23:59:59.999999Z`)

**Reason:** This guarantees zero duplicates and zero gaps. We sacrifice at most one day of progress but avoid the complexity of identifying exactly which parquet files were uploaded after the last cursor report. The "delete partial day, re-do from day boundary" approach is simple, deterministic, and safe.

**Alternative considered:** Resume from the exact last cursor position. This risks duplicates because files may have been uploaded to GCS after the last reported cursor (the parquet writer is asynchronous — files are written and uploaded between cursor reports). We encountered this exact issue when the first ENOSPC crash happened: 3 events files were uploaded after the batch 740 cursor.

### Decision 8: Live Cursor Update After Re-Ingestion

**Choice:** After re-ingestion completed, query the last parquet file in GCS to find the exact `MAX(record_time)` (`2026-03-30T23:59:59.793000Z`) and write it to `live-cursor.json`.

**Reason:** This ensures `fetch-updates.js` starts from the precise point where re-ingestion ended — no gap (would miss data) and no overlap (would create duplicates). The alternative of using the old live cursor (`2026-03-30T20:13:02Z`) would have re-fetched ~4 hours of March 30 data, creating duplicates.

### Decision 9: Multi-Node Failover

**Choice:** Add 13 SV scan endpoints to `reingest-updates.js` with automatic failover after 3 consecutive errors.

**Reason:** The original reingest script used a single endpoint. When that node went down (503s), the script had no alternatives and would abort after 10 retries. `fetch-updates.js` already had this capability; we replicated it.

**Endpoints used:**
- Global-Synchronizer-Foundation (sv-1.sync.global) — primary
- Digital-Asset-1, Digital-Asset-2
- Cumberland-1, Cumberland-2
- Five-North-1, Tradeweb-Markets-1, Proof-Group-1
- Liberty-City-Ventures-1, MPC-Holding-Inc, Orb-1-LP-1
- SV-Nodeops-Limited, C7-Technology-Services-Limited

### Decision 10: Cooldown on Full Network Outage

**Choice:** When all endpoints are failing, wait 5 minutes then retry. Up to 6 cooldowns (30 minutes total) before aborting.

**Reason:** During a network-wide outage, the original failover logic created an infinite bounce between two nodes (error counter reset on each switch). The cooldown pattern is: try all endpoints → all fail → wait 5 min → try again → all fail → wait 5 min → ... → abort after 30 min. This handles temporary full-network outages while still aborting on sustained failures.

### Decision 11: Fresh HTTP Client on Failover

**Choice:** Recreate the axios client (new TCP connection pool) on every endpoint failover, instead of just changing the `baseURL`.

**Reason:** Root cause of recurring stalls in `fetch-updates.js`. Axios reuses TCP connections via HTTP keep-alive. When a Scan API node hangs (doesn't close connection, just stops responding), all new requests queue behind the dead socket. This is why `curl` works (fresh connection) but the script keeps timing out. Creating a new client drops the stuck connections.

### Decision 12: Increase DuckDB maximum_object_size

**Choice:** Increase `maximum_object_size` from 16MB (default) to 64MB in both `parquet-worker.js` and `write-parquet.js`.

**Reason:** Some Canton update records have `update_data` fields exceeding 16MB (~23MB observed). This caused the parquet writer to fail, and the script got stuck retrying the same batch indefinitely. The 64MB limit provides headroom for even larger records.

### Decision 13: VM Hardening

**Choice:** Added 4GB swap, increased file descriptor limit to 65536, fixed file ownership.

**Reason:**
- **Swap:** Prevents OOM killer from crashing the VM when memory spikes temporarily
- **File descriptors:** Default 1024 is too low for parquet workers + concurrent GCS uploads
- **Ownership:** Scripts run as `josefin` but data directories owned by `ben`; permission mismatches caused silent failures (cursor not saved, temp files not writable)

---

## 4. Current State (as of April 5, 2026)

### GCS Data Status
- **`raw/backfill/`** — Complete through migration 4, ending at `2026-03-03T00:20:04.274468Z`
- **`raw/updates/` March 3-30** — Fully re-ingested. 21,364,168 updates, 469,932,897 events. Every day has both updates and events files.
- **`raw/updates/` March 31+** — Needs to be ingested by `fetch-updates.js` from cursor `2026-03-30T23:59:59.793000Z`

### Files Changed
| File | Changes |
|------|---------|
| `scripts/ingest/reingest-updates.js` | **NEW** — Full re-ingestion tool with audit, clean, resume, failover, cooldown |
| `scripts/ingest/parquet-worker.js` | Increased `maximum_object_size` to 64MB |
| `scripts/ingest/write-parquet.js` | Increased `maximum_object_size` to 64MB |
| `scripts/ingest/fetch-updates.js` | Fresh HTTP client on failover (stuck TCP fix) |

### Known Remaining Issues
1. **Recurring stall at cursor `2026-03-31T23:40:24.200750Z`** — `fetch-updates.js` consistently times out at this specific position across multiple restarts and multiple API nodes. Needs investigation: may be a problematic record that causes the API to take >30s, or a subtle cursor/pagination issue at that exact timestamp.
2. **No process supervisor** — `fetch-updates.js` runs as a standalone process with no automatic restart. If it crashes, nobody knows until data gaps appear. Needs PM2 or systemd integration with monitoring/alerting.
3. **No alerting** — No notification system for pipeline failures. Operator must manually check.
4. **DuckDB reads local disk, not GCS** — Data uploaded to GCS still needs to be synced to local disk for DuckDB queries. This sync mechanism was not modified during this work.

### Commits on Branch
```
a09a21a Fix fetch-updates.js stalling on stuck TCP connections after API timeout
56b8ea9 Fix failover loop: add 5-min cooldown when all Scan API nodes are down
a5579fc Increase DuckDB maximum_object_size to 64MB for large update records
ec38bd6 Tune reingest-updates.js for stability on 31GB VM
7adc994 Add multi-node failover to reingest-updates.js
ff0876f Add --after flag to reingest-updates.js for resuming after interruption
a9faf2c Production-ready re-ingestion: correct event processing, backfill boundary detection, robust retries
97049d4 Add GCS bucket probe and verbose gsutil debugging to diagnose path mismatch
a2bb603 Fix partition path formatting: use unpadded values to match GCS paths
ce0729e Fix gsutil error handling and optimize audit with bulk GCS listing
151412f Add targeted re-ingestion script for fixing GCS data gaps
```

---

## 5. Operational Runbook

### How to Resume Re-Ingestion After Interruption

```bash
# 1. Start tmux (ALWAYS use tmux for long-running operations)
tmux new -s reingest

# 2. Check which days have data in GCS
for d in 3 4 5 6 7 8 9 10 ...; do
  echo -n "day=$d: "
  gsutil ls "gs://canton-bucket/raw/updates/events/migration=4/year=2026/month=3/day=$d/" 2>/dev/null | wc -l
done

# 3. Identify the partial day (significantly fewer files than complete days)
#    Complete days have 340-470 events files and 20-60 updates files

# 4. Delete the partial day
gsutil -m rm "gs://canton-bucket/raw/updates/events/migration=4/year=2026/month=M/day=D/**"
gsutil -m rm "gs://canton-bucket/raw/updates/updates/migration=4/year=2026/month=M/day=D/**"

# 5. Resume from end of last complete day
node scripts/ingest/reingest-updates.js \
  --start=2026-03-03 --end=2026-03-30 --migration=4 \
  --after=2026-MM-DDT23:59:59.999999Z --force
```

### How to Start Live Ingestion After Re-Ingestion

```bash
# 1. Find the exact last record_time in GCS
gsutil ls "gs://canton-bucket/raw/updates/updates/migration=4/year=2026/month=M/day=D/" | tail -1
gsutil cp "<that-file>" /tmp/last-updates.parquet
duckdb -c "SELECT MAX(record_time) FROM read_parquet('/tmp/last-updates.parquet');"

# 2. Write the cursor
cat > /home/ben/ledger_data/cursors/live-cursor.json << EOF
{
  "migration_id": 4,
  "record_time": "<MAX_RECORD_TIME>",
  "updated_at": "$(date -u +%Y-%m-%dT%H:%M:%S.000Z)",
  "mode": "live",
  "semantics": "forward"
}
EOF

# 3. Ensure permissions
sudo chown josefin:josefin /home/ben/ledger_data/cursors/live-cursor.json

# 4. Start in tmux
tmux new -s live-ingest
node scripts/ingest/fetch-updates.js
# Detach: Ctrl+B, then D
# Reattach: tmux attach -t live-ingest
```

### Environment Prerequisites
```bash
# File descriptor limit
ulimit -n 65536

# Swap (if not already configured)
sudo fallocate -l 4G /swapfile && sudo chmod 600 /swapfile
sudo mkswap /swapfile && sudo swapon /swapfile

# Permissions
sudo chown -R josefin:josefin /home/ben/ledger_data/cursors/
sudo chown -R josefin:josefin /tmp/ledger_raw/

# Verify disk space (need at least 10GB free in /tmp)
df -h /tmp
```
