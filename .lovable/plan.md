

# Fix Remaining Crash Risks in Backfill Pipeline

## Problem

Beyond the disk backpressure fix, there are several remaining issues that can cause the backfill to stop unexpectedly. The most likely previous culprit is that migration detection treats transient API errors (503/timeout) as "no more migrations," causing migration 4 to be skipped entirely.

## Changes

### 1. Retry migration detection on transient errors (HIGH PRIORITY)

**File:** `scripts/ingest/fetch-backfill.js` -- `detectMigrations()` function (lines 640-674)

Currently, any non-404 error breaks the detection loop. Add retry logic so that 503/429/timeout errors retry 3 times before giving up, and only a confirmed 404 stops the scan.

### 2. Cap unhandledRejection handler to prevent surprise exits (HIGH PRIORITY)

**File:** `scripts/ingest/fetch-backfill.js` -- lines 2187-2193

Change the `unhandledRejection` handler from re-throwing (which kills the process) to logging the error and continuing. Only truly fatal rejections should exit.

### 3. Add max transient error limit (MEDIUM)

**File:** `scripts/ingest/fetch-backfill.js` -- the `while(true)` loop at line 1478

Add a `MAX_CONSECUTIVE_TRANSIENT_ERRORS` constant (e.g., 50). If exceeded, save cursor and exit cleanly instead of retrying forever.

### 4. Add worker respawn cooldown (LOW)

**File:** `scripts/ingest/parquet-writer-pool.js` -- `_spawnPersistentWorker` called from `exit` handler (line 151)

Add a counter and cooldown: if the same worker slot crashes more than 5 times in 60 seconds, stop respawning and log a fatal error instead of looping.

## Technical Details

### Migration detection retry (the key fix)

```text
detectMigrations():
  for id = 0, 1, 2, ...
    POST /v0/backfilling/migration-info { migration_id: id }
    
    Current behavior:
      404 -> break (correct)
      503 -> break (WRONG - skips remaining migrations)
    
    Fixed behavior:
      404 -> break (correct)
      503/429/timeout -> retry up to 3 times with backoff
      3 retries exhausted -> break with warning (conservative)
```

### unhandledRejection fix

```text
Current:  throw reason;  // kills process
Fixed:    console.error('Unhandled rejection:', reason);
          // Log but continue - let the specific subsystem handle its own errors
```

### Transient error cap

```text
MAX_CONSECUTIVE_TRANSIENT_ERRORS = 50

if (consecutiveTransientErrors >= MAX_CONSECUTIVE_TRANSIENT_ERRORS) {
  save cursor
  log "Too many consecutive errors, exiting to allow restart"
  process.exit(1)
}
```

### Worker respawn guard

```text
Track: recentCrashes[] with timestamps
On worker exit:
  if (crashes in last 60s > 5):
    log FATAL "Worker respawn loop detected"
    stop respawning that slot
  else:
    respawn normally
```

