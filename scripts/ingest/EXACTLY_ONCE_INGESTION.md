# Exactly-Once Ingestion from Scan API → GCS

## Goal

Make the raw data ingestion pipeline from the Scan API into GCS provably correct: **zero gaps and zero duplicates**, by design — not by downstream dedup. The same end-state should hold whether the pipeline runs clean, crashes mid-batch, is killed (`SIGINT`/`SIGTERM`), hits an unhandled rejection, or is restarted repeatedly on the same cursor.

## Methodology

The design rests on three invariants that together make every batch **idempotent** under re-execution:

1. **Deterministic filenames.** A Parquet file's GCS path is a pure function of `(cursor position, partition)`. Since the same cursor always produces the same API response, re-writing the same batch produces the *same object name*, so GCS overwrites instead of creating a duplicate.
2. **Synchronous per-batch writes.** Each API batch is fully materialized to GCS before the cursor is advanced. Nothing sits in an async buffer between "cursor moved" and "data durable."
3. **Write-then-advance cursor.** The cursor is only persisted *after* all uploads for that batch are confirmed. A crash before the save just re-plays the same batch — and because of invariant #1, that replay is a no-op in GCS.

The crash-safety matrix falls out of those invariants:

| Crash point | Outcome |
|---|---|
| During Parquet build / GCS upload | Cursor unchanged → restart re-fetches → same filename → overwrite → no dup, no gap |
| After upload, before cursor save | Same as above |
| After cursor save | Next batch fetched → clean resume |
| `SIGINT`/`SIGTERM` | Cursor saved immediately (nothing buffered) |
| `uncaughtException` / `unhandledRejection` | Last-known cursor persisted via safety-net handlers |

## Implementation

### `scripts/ingest/write-parquet.js`

Exported the shared record-normalization helpers `mapUpdateRecord` and `mapEventRecord` so both ingestion scripts use identical schema mapping.

### `scripts/ingest/reingest-updates.js` (backfill / targeted re-ingest)

Entire buffer-based pipeline replaced with a deterministic per-batch write path:

- `deterministicFileName(type, afterRecordTime, partition, chunkIdx, chunkCount)` — `{type}-ri-{sha256(afterRecordTime|partition).hex16}.parquet`, with a reproducible `-c{i}of{N}` suffix when chunked (file uses the `ri` prefix to distinguish from live files).
- `chunkLinesByBytes(lines)` — greedy byte-based splitter that activates at `MAX_JSONL_BYTES_PER_CHUNK`; chunk boundaries are a pure function of the serialized input, so retries land on the same filenames.
- `jsonlToParquetViaDuckDB` — DuckDB CLI invocation with `memory_limit='2GB'`, `threads=1`, `preserve_insertion_order=false`, disk spill via `temp_directory`, `ROW_GROUP_SIZE 5000`, ZSTD.
- `writePartitionToGCS` — writes JSONL slice → Parquet via DuckDB → uploads to `raw/{partition}/{fileName}` via the `@google-cloud/storage` SDK (uses VM service account / ADC; replaced `gsutil`, which silently failed on expired user credentials), cleans up temp files in `finally`.
- `writeBatchToGCS` — groups updates and events by partition day and writes each partition serially before returning.
- `processUpdates` (per batch): fetch → normalize → filter stragglers whose `effective_at` precedes `START_DATE` → `writeBatchToGCS(...)` → return counts. The main loop calls `saveReingestCursor(...)` only after this returns.
- Resilience backported from `fetch-updates.js`:
  - `probeAllScanEndpoints()` at startup with failover to the fastest healthy endpoint.
  - `fetchUpdatesAPI()` using `AbortController` (more reliable than axios' timeout).
  - Adaptive `page_size` / `timeout` for stuck cursors — after repeated `FETCH_TIMEOUT` hits at the same cursor, halve `page_size` (min 1) and grow timeout up to 3× base; reset on a successful fetch.
  - `uncaughtException` / `unhandledRejection` handlers that persist `_shutdownState`'s cursor before exit.
  - `SIGINT`/`SIGTERM` handlers that save cursor then exit.
- Safety guard: `--clean` / `--clean-backfill` refuses to run when a saved resume cursor exists unless `--force` is passed — prevents destroying days of cumulative progress on a resume.

### `scripts/ingest/fetch-updates.js` (live ingestion)

Same deterministic write path, identical to reingest for correctness parity:

- `deterministicFileName` uses prefix `live` (vs. `ri` for reingest) so live and reingest files never collide in the same partition.
- `writePartitionToGCS` / `writeBatchToGCS` are the mirror of the reingest versions, uploading via the GCS SDK with the same overwrite-on-same-key semantics.
- `processUpdates` now **returns** normalized records instead of buffering; the main loop calls `writeBatchToGCS(updates, events, migrationId, afterRecordTime)` *before* advancing `afterRecordTime` and before `saveLiveCursor(...)`. Cursor-tracking fields `_liveAfterMigrationId` / `_liveAfterRecordTime` are updated on every save so the shutdown path always has the latest cursor.
- Optional `--keep-raw` binary-writer path is preserved: `binaryWriter.bufferUpdates` / `bufferEvents` still run when `USE_BINARY` is true. `flushAll()` / `getBufferStats()` were simplified to only manage the binary writer — the Parquet path has nothing to flush.
- Removed the unused `import * as parquetWriter`; kept the named imports `mapUpdateRecord` / `mapEventRecord`.
- `uncaughtException` / `unhandledRejection` handlers alert and include `_liveAfterRecordTime` so restart resumes cleanly.

### Shared current semantics

Both scripts pass `node --check` and now share:

- the same exactly-once write path (`sha256(cursor|partition)` → overwrite),
- the same adaptive backoff for stuck cursors,
- the same endpoint-failover strategy at startup,
- the same crash-safe cursor handling across graceful and ungraceful exits,
- GCS access via `@google-cloud/storage` SDK with VM/ADC credentials (no `gsutil` in the write path).

## Running a re-ingestion

```
node scripts/ingest/reingest-updates.js --start=2026-03-03 --end=2026-04-10 --migration=4 --clean-backfill
```

On restart — whether from a crash, a signal, or manual `Ctrl-C` — re-running the same command auto-resumes from the saved cursor. Replays overwrite their own files; the pipeline converges to the same GCS state regardless of how many times a batch is re-executed.
