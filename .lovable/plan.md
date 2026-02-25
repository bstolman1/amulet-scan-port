

# Comprehensive Audit: fetch-backfill Pipeline

## Executive Summary

The fetch-backfill pipeline is a well-architected system for ingesting historical Canton ledger data into GCS as Parquet files. After reviewing ~5,000 lines across 12 core modules and 10 test files, I've identified **5 critical issues**, **4 moderate issues**, and **6 test gaps** that need addressing.

---

## Architecture Overview (for reference)

```text
Canton Scan API
    │
    ▼
fetch-backfill.js  ──── detectMigrations() → getMigrationInfo() → backfillSynchronizer()
    │                                                                    │
    ├── parallelFetchBatch() ──── fetchTimeSliceStreaming() ×N slices    │
    │       │                          │                                 │
    │       │                     fetchBackfillBefore() ← retryWithBackoff()
    │       │                          │
    │       ▼                          ▼
    │   processBackfillItems() ── decode-worker.js (Piscina) or main-thread
    │       │
    │       ▼
    │   data-schema.js ── normalizeUpdate() + normalizeEvent()
    │       │
    │       ▼
    │   write-parquet.js ── groupByPartition() → DuckDB worker pool → .parquet
    │       │
    │       ▼
    │   gcs-upload-queue.js ── gsutil cp → GCS (async background)
    │       │
    │       ▼
    │   atomic-cursor.js ── crash-safe cursor with GCS checkpoints
    │
    ▼
 cursor-{migration}-{sync}.json  ──  Resume point
```

---

## Critical Issues (Must Fix)

### 1. `saveAtomic` called on `AtomicCursor` but method doesn't exist

**File:** `fetch-backfill.js` lines 1486, 1526, 1740, 1779, 1829
**File:** `atomic-cursor.js`

The `backfillSynchronizer` function calls `atomicCursor.saveAtomic({...})` repeatedly, but `AtomicCursor` has no `saveAtomic` method. It has `beginTransaction`/`commit`/`_writeConfirmedState`. This means every cursor save outside of the transaction flow is silently failing or throwing.

The `atomicWriteFile` function exists as a standalone export, and `saveAtomic` may have been intended as a convenience wrapper. This is the single most dangerous bug — cursor state may not be persisting correctly on error paths, graceful shutdown, and initial creation.

**Fix:** Add a `saveAtomic(data)` method to `AtomicCursor` that merges the provided data into `confirmedState` and calls `_writeConfirmedState()`, OR refactor all call sites to use the transaction API.

### 2. Cursor resumes from `last_before` but `backfillSynchronizer` loads via legacy `loadCursor` not `AtomicCursor.load()`

**File:** `fetch-backfill.js` lines 1392-1393 vs 2085

The `backfillSynchronizer` creates an `AtomicCursor` and calls `.load()` which reads `last_confirmed_before` and GCS-confirmed position. But the outer `runBackfill` loop at line 2085 calls the legacy `loadCursor()` function (line 586) which just does `JSON.parse(readFileSync(...))` — no backup recovery, no GCS-awareness. If the main cursor file is corrupted, `runBackfill` will miss it while `AtomicCursor` would have recovered from `.bak`.

**Fix:** Replace `loadCursor` calls in `runBackfill` with `AtomicCursor` instances, or at minimum use `readCursorSafe`.

### 3. Global dedup Set can silently drop duplicates after clear

**File:** `fetch-backfill.js` lines 1047-1053, 1179-1184

When `globalSeenUpdateIds` exceeds `GLOBAL_DEDUP_MAX` (250K), it's cleared entirely. The comment says "downstream processing can handle occasional dups" — but there's no downstream dedup. If the same update_id appears in two different slices and the Set was cleared between them, both copies get written to Parquet. For 717M events this could create meaningful data inflation.

**Fix:** Add a dedup step in BigQuery/DuckDB views (cheap `QUALIFY ROW_NUMBER()` window), and document this as expected behavior. OR use an LRU-style eviction instead of full clear.

### 4. `seenUpdateIds.clear()` at 50K in `fetchTimeSliceStreaming` breaks intra-slice dedup

**File:** `fetch-backfill.js` lines 1000-1002

Within a single time slice, after seeing 50K update IDs, the dedup set is cleared. Since the backfill API paginates backward by timestamp (subtracting 1ms), and records can share the same millisecond, this can cause the same record to be fetched twice within the same slice if the set was cleared between pages. The 50K threshold is too low for high-density periods.

**Fix:** Raise to 500K or use a Bloom filter. Memory impact is negligible (~40MB for 500K strings).

### 5. `decodeInMainThread` is exported but `processBackfillItems` uses it — error propagation gap

**File:** `fetch-backfill.js` lines 776-793 vs 839-894

In the main-thread fallback path (lines 782-792), if `decodeInMainThread` throws (e.g., from `normalizeUpdate`'s `SchemaValidationError`), the exception propagates up and crashes the batch. But in the worker pool path (line 800), the `.catch` fallback catches the error and returns partial results silently. If one chunk fails in the worker pool, those records are lost without the cursor knowing.

**Fix:** The worker pool catch block should propagate errors, not silently return partial results. Or at minimum, log the count of lost records and subtract from the update/event totals before cursor advancement.

---

## Moderate Issues

### 6. `write-parquet.js` doesn't set `currentMigrationId` during backfill

The `currentMigrationId` variable in `write-parquet.js` (line 152) defaults to `null` and is only set via `setMigrationId()`. But `fetch-backfill.js` never calls `setMigrationId()`. The migration ID is embedded in each record's `migration_id` field and used by `groupByPartition` — so it works, but only because `groupByPartition` falls back to `record.migration_id`. This is fragile if any code path passes `currentMigrationId` to partition logic.

### 7. Empty response handler step tiers may skip data in gaps

**File:** `bulletproof-backfill.js` lines 424-431

After 5000 consecutive empties, step size jumps to 1 hour. If a gap is exactly 59 minutes, the 1-hour step could skip past data that exists just before the gap's end. This is unlikely but possible during migration boundaries.

### 8. GCS upload queue: failed files kept on disk but never retried automatically

**File:** `gcs-upload-queue.js` lines 364-379

When uploads fail permanently, files are kept on disk and logged to `failed-uploads.jsonl`. But there's no automatic retry mechanism — the `retry-failed-uploads.js` script exists but isn't invoked automatically. On long runs, /tmp could accumulate failed files.

### 9. `offset` parsed with `parseInt` may lose precision for very large values

**File:** `data-schema.js` line 218

Canton offsets can be very large hex strings (e.g., `000000000000000001`). `parseInt` works here but if offsets exceed `Number.MAX_SAFE_INTEGER`, precision is lost. Should use `BigInt` or keep as string.

---

## Test Gaps

### Gap 1: No test for `saveAtomic` method (because it doesn't exist)
The most-called cursor method in `fetch-backfill.js` has zero test coverage because it's missing from the class.

### Gap 2: No integration test for the full decode → partition → GCS upload flow
Tests exist for individual components (data-schema, gcs-upload, atomic-cursor) but nothing tests the pipeline end-to-end with mock API responses flowing through to Parquet files landing in the correct GCS partition paths.

### Gap 3: No test for multi-migration sequencing
`runBackfill` iterates migrations 0→1→2→3. No test verifies that completing migration 0 correctly transitions to migration 1 with proper cursor isolation.

### Gap 4: No test for `parallelFetchBatch` cursor safety with actual data
The chaos tests simulate slice completion patterns but don't test with real `processBackfillItems` callbacks. A test should verify that cursor advancement matches actual written data.

### Gap 5: No test for `groupByPartition` with cross-midnight records
A single batch can span midnight UTC. No test verifies that records are correctly split into separate day partitions within one flush.

### Gap 6: No test for `decodeInMainThread` error handling
The exported function is the primary decode path but has no dedicated test for how errors in `normalizeUpdate` or `normalizeEvent` propagate.

---

## Implementation Plan

### Phase 1: Fix Critical Bugs (4 tasks)
1. **Add `saveAtomic` method to `AtomicCursor`** — Accept a data object, merge into `confirmedState`, write atomically. Add comprehensive tests.
2. **Replace legacy `loadCursor` in `runBackfill`** — Use `AtomicCursor` for the completion check at line 2085.
3. **Fix worker pool error swallowing** — Remove silent catch in `processBackfillItems` worker path; propagate errors.
4. **Raise intra-slice dedup limit** — Change 50K to 500K in `fetchTimeSliceStreaming`.

### Phase 2: Add Missing Tests (5 tasks)
5. **End-to-end pipeline test** — Mock API → decode → partition → write Parquet → verify file contents + partition paths.
6. **Multi-migration sequencing test** — Verify cursor isolation between migrations 0, 1, 2.
7. **Cross-midnight partition test** — Verify `groupByPartition` splits correctly when records span UTC midnight.
8. **`decodeInMainThread` error propagation test** — Verify errors from `normalizeUpdate` propagate correctly.
9. **`saveAtomic` + GCS checkpoint round-trip test** — Write cursor, simulate crash, reload, verify GCS-safe resume position.

### Phase 3: Harden Edge Cases (3 tasks)
10. **Add downstream dedup documentation** — Document that BigQuery views should use `QUALIFY ROW_NUMBER() OVER (PARTITION BY update_id ORDER BY recorded_at) = 1`.
11. **Add automatic retry for failed GCS uploads** — Run `retry-failed-uploads.js` on a timer within the main process.
12. **Reduce empty response step tier aggressiveness** — Cap maximum step at 5 minutes instead of 1 hour to prevent data skipping.

