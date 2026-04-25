# Deprecated Scripts

This document tracks scripts that have been retired and what to use instead.

---

## `fetch-backfill.js` — DEPRECATED 2026-04-25

### Use `reingest-updates.js` instead

Both tools fetch historical ledger data from Canton Scan API into GCS. Going
forward, **use `reingest-updates.js` for all backfill operations**, including
fresh backfills of new migrations.

### Why

`fetch-backfill.js` has a systematic data-loss bug at every batch boundary.

The cursor advancement uses:

```js
const d = new Date(newEarliestTime);
d.setMilliseconds(d.getMilliseconds() - 1);
before = d.toISOString();
```

(`fetch-backfill.js:1437-1445` and `fetch-backfill.js:1003-1011`)

Canton's `record_time` has **microsecond precision** (e.g.
`2026-04-08T03:14:21.477185Z`). JavaScript's `Date` object stores only
milliseconds, so `new Date('...477185Z').toISOString()` silently truncates to
`...477Z`. `setMilliseconds(-1)` then jumps the cursor to `...476Z`,
**skipping the entire `(0.476Z, 0.477185Z]` window**. Any records in that
~1 ms range that weren't already in the previous batch are permanently lost.

### Empirical impact

Discovered by `verify-scan-completeness.js` on 2026-04-25 across a 5-day
sample of M4 backfill data. Every sampled backfill day drifted by 0.1–0.3%
relative to the Scan API:

| Date         | GCS     | Scan    | Diff   | Drift  |
|--------------|---------|---------|--------|--------|
| 2025-12-19   | 571,437 | 572,177 | -740   | -0.13% |
| 2026-01-02   | 486,518 | 487,072 | -554   | -0.11% |
| 2026-01-22   | 735,109 | 736,797 | -1,688 | -0.23% |
| 2026-02-05   | 914,430 | 917,033 | -2,603 | -0.28% |
| 2026-02-19   | 915,479 | 917,593 | -2,114 | -0.23% |

Three control days (re-ingested via `reingest-updates.js` or written by live
ingest) matched the Scan API **exactly** with 0 drift, confirming the bug
is specific to `fetch-backfill.js`'s code path.

Total estimated loss across the original M4 backfill (79 days,
2025-12-16 → 2026-03-02): **~150–200K missing updates**, plus proportional
events at the ~15:1 event-to-update ratio.

### Why the replacement works

`reingest-updates.js` uses Canton's `/v2/updates` endpoint with forward
pagination. Cursor advancement uses `MAX(record_time)` directly as a string,
preserving full microsecond precision — no `Date` arithmetic, no precision
loss, no skipped windows. It has been verified to produce exact counts
matching the Scan API.

`/v2/updates` returns historical data going back to migration 0 (verified
empirically by `verify-scan-completeness.js`, which paginated thousands of
historical pages successfully).

### Remediation status (M4 backfill, in progress)

Each affected M4 backfill day is being re-fetched via:

```bash
node reingest-updates.js --start=YYYY-MM-DD --end=YYYY-MM-DD --migration=4 --clean
```

After each day's re-fetch is verified by `verify-scan-completeness.js` against
Scan API (must show MATCH, not DRIFT), the original `raw/backfill/...` files
for that day are deleted via `gsutil -m rm`. Net result: each day's data
exists exactly once, in the proven-good `raw/updates/` partition. Analytics
queries already `UNION ALL` across both partitions, so downstream impact is
zero.

M0–M3 backfill remediation will be planned after M4 completes successfully.

### Why not just fix `fetch-backfill.js`?

A 1-line fix replacing `setMilliseconds(-1)` with proper microsecond
arithmetic would handle the precision loss, but:

- Tied records at the same `record_time` would still need explicit
  handling, requiring more changes
- Maintaining two parallel pagination implementations is operational debt
- `reingest-updates.js` has already been validated end-to-end against Scan
  API on three independent days

The simpler path is to retire the buggy implementation and standardize on the
proven one.

### When (if ever) might `fetch-backfill.js` be revived?

Only if `/v2/updates` introduces a hard retention limit that excludes
older migration data. As of 2026-04-25 it serves all historical data
back to migration 0 without issue.

If revived, the precision bug must be fixed before any production use.
