

## Performance Upgrade Plan — No Additional Sharding

### Bottleneck Analysis

After reading the full pipeline (`fetch-backfill.js`, `write-parquet.js`, `parquet-worker.js`, `gcs-upload-queue.js`, `gcs-upload.js`), there are **four high-impact bottlenecks** that can be eliminated:

```text
Current Pipeline (per batch of 1000 txs):

  API fetch → decode → JSON.stringify each record
    → write temp .jsonl file to disk
      → DuckDB: new Database(':memory:') + connect()
        → DuckDB: read_json_auto(tempfile) → COPY TO .parquet
          → DuckDB: re-read .parquet for validation (COUNT, DESCRIBE, sample)
            → close DB + close conn
              → spawn gsutil child process → upload → delete local
```

Each of these steps has unnecessary overhead that compounds across millions of files.

---

### Change 1: Replace `gsutil` CLI with `@google-cloud/storage` SDK

**File:** `scripts/ingest/gcs-upload-queue.js`

**Problem:** Every upload spawns a `gsutil` child process. At 48 concurrent uploads, that's 48 OS processes with their own Python runtimes, each doing TLS handshake independently. Process spawn overhead is ~50-100ms per file.

**Fix:** Replace `gsutilUpload()` with the `@google-cloud/storage` Node.js SDK. This gives:
- Connection pooling and HTTP/2 multiplexing (one TLS session, many uploads)
- No process spawn overhead
- Streaming upload support (pipe file directly)
- Native resumable uploads for large files

**Expected speedup:** 3-5x on GCS upload throughput.

**Implementation:**
- Install `@google-cloud/storage`
- Replace `gsutilUpload()` function with SDK-based `streamUpload()`
- Keep the same queue/backpressure/retry architecture
- Remove `computeLocalMD5` / `getGCSObjectMD5` (SDK handles integrity via CRC32C automatically)

---

### Change 2: Reuse DuckDB connections in persistent workers

**File:** `scripts/ingest/parquet-worker.js`

**Problem:** Every Parquet file write does `new duckdb.Database(':memory:')` + `db.connect()` + `conn.close()` + `db.close()`. DuckDB initialization is expensive (~20-50ms per instance). At 50K files per migration, that's 15-40 minutes of pure DuckDB init overhead.

**Fix:** Create the DuckDB instance once when the persistent worker starts, reuse it across all jobs. Just run the COPY query on each message.

**Expected speedup:** 2x on Parquet write throughput.

**Implementation:**
- Move `duckdb.Database(':memory:')` and `db.connect()` to module-level initialization (runs once per worker thread)
- Remove `conn.close()` / `db.close()` from `processJob()`
- Add cleanup only in the worker exit handler

---

### Change 3: Make post-write validation sampling-based

**File:** `scripts/ingest/parquet-worker.js`

**Problem:** Every single Parquet file is re-read after writing for validation (COUNT, DESCRIBE, sample queries). This doubles the I/O per file and adds 3 DuckDB queries per write.

**Fix:** Validate only every Nth file (e.g., every 20th). The pipeline has been stable — full validation on every file is unnecessary overhead.

**Expected speedup:** ~30% on Parquet write latency.

**Implementation:**
- Add a `PARQUET_VALIDATION_SAMPLE_RATE` env var (default: 20 = validate 1 in 20 files)
- Track a counter in the persistent worker, only run validation queries when `counter % sampleRate === 0`
- Still validate the first 5 files on startup for early error detection

---

### Change 4: Increase `MAX_ROWS_PER_FILE` floor and reduce file count

**File:** `scripts/ingest/write-parquet.js`, `.env`

**Problem:** With `MIN_ROWS_PER_FILE=5000`, the pipeline produces many small files. Each file incurs: DuckDB init, JSONL write, Parquet write, validation, GCS upload, GCS delete. Fewer, larger files amortize all this overhead.

**Fix:** Raise `MIN_ROWS_PER_FILE` to 25,000 and `MAX_ROWS_PER_FILE` to 100,000. This reduces total file count by ~5x, cutting per-file overhead proportionally.

**Expected speedup:** 2-3x overall (fewer files = fewer DuckDB inits, fewer GCS uploads, fewer disk operations).

**Implementation:**
- Update `.env`: `MIN_ROWS_PER_FILE=25000`, `MAX_ROWS_PER_FILE=100000`
- Verify `PARQUET_ROW_GROUP=100000` is set for efficient read performance

---

### Combined Expected Impact

```text
Current:  ~500 updates/sec sustained
After:    ~3000-5000 updates/sec sustained (6-10x improvement)
```

The changes are independent and can be implemented incrementally:
1. Change 4 (env vars only) — immediate, zero risk
2. Change 2 (reuse DuckDB) — small code change, high impact
3. Change 3 (sampling validation) — small code change, moderate impact
4. Change 1 (GCS SDK) — largest change, highest impact on upload-bound runs

### Technical Notes

- Change 1 requires `npm install @google-cloud/storage` in `scripts/ingest/`
- Change 2 requires testing that DuckDB in-memory state doesn't leak across jobs (it shouldn't since each COPY reads from a fresh temp file)
- All changes are backward-compatible — no cursor format changes, no GCS path changes

