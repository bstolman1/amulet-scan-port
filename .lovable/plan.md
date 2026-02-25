

# Build `check-cursor-progress.js` — Cursor-Based Backfill Progress CLI

## Problem

The existing `check-backfill-progress.js` checks GCS folder presence (partition-based), which is meaningless for a cursor-driven, non-sequential pipeline. `shard-progress.js` works but is shard-focused. You need a simple tool that reads cursor files, compares `last_before` against migration time boundaries, and shows real progress.

## What Gets Built

A new script: `scripts/ingest/check-cursor-progress.js`

### Behavior

```text
$ node check-cursor-progress.js

═══════════════════════════════════════════════════════════════
📊 BACKFILL PROGRESS (Cursor-Based)
═══════════════════════════════════════════════════════════════

Migration 0
  Synchronizer: global-domain__...
  Cursor at:      2025-03-12T14:22:00Z
  Range:          2025-01-15 → 2025-04-01
  Progress:       [████████████████░░░░] 78.2%
  Updates:        12,340,000
  Events:         45,120,000
  ETA:            ~3h 20m
  Status:         🔄 Active (updated 12s ago)

Migration 1
  Status:         ✅ Complete

Migration 2
  Status:         ⏳ Not started

Migration 3
  Cursor at:      2025-09-15T08:00:00Z
  Range:          2025-06-25 → 2025-12-10
  Progress:       [█████████████░░░░░░░] 67.3%
  ...

───────────────────────────────────────────────────────────────
Overall: 2/4 complete | 61.2% total
───────────────────────────────────────────────────────────────
```

### Flags

- `--watch` / `-w` — Refresh every 3 seconds (reuses pattern from `shard-progress.js`)
- `--migration N` — Show only migration N
- `--json` — Output as JSON (for piping to other tools)

### Technical Approach

1. **Read cursor directory** via `getCursorDir()` from `path-utils.js` — same resolution logic as the pipeline itself
2. **Parse all `cursor-*.json` files** — extract `migration_id`, `synchronizer_id`, `last_before`, `min_time`, `max_time`, `complete`, `total_updates`, `total_events`, `started_at`, `updated_at`
3. **Calculate progress** using `(max_time - last_before) / (max_time - min_time)` — the backfill moves backward from `max_time` toward `min_time`, so `last_before` approaching `min_time` means done
4. **Calculate ETA** from `(elapsed_time / progress_pct) * remaining_pct`
5. **Detect stale cursors** — if `updated_at` is >60s old, mark as stalled
6. **Group by migration** — aggregate shards if present (sum updates/events, use weighted average progress)
7. **Completion check** — if `complete === true` OR `last_before <= min_time`, mark migration done

### Changes

| File | Action |
|---|---|
| `scripts/ingest/check-cursor-progress.js` | **Replace** entirely — cursor-based instead of partition-based |
| `scripts/ingest/check-backfill-progress.js` | Keep as-is (still useful for GCS audit, different purpose) |

The new script reuses `getCursorDir` from `path-utils.js` so it respects `CURSOR_DIR` and `DATA_DIR` env vars, and works on both Windows and Linux without hardcoded paths.

