# Backfill Optimization Guide: 84M Rows in 24 Hours

This guide explains how to run the optimized backfill process to load 84M rows in 1 day.

## Overview

The optimizations provide **10x-50x speedup** through:
1. ✅ **UNLOGGED tables** (3x-10x faster writes)
2. ✅ **Dropped indexes** (5x-15x faster inserts)
3. ✅ **Large batch sizes** (10,000 rows per batch)
4. ✅ **Parallel workers** (6-12 concurrent processes)

---

## Step-by-Step Process

### PHASE 1: Prepare Database (5 minutes)

**Run the optimization script:**

```bash
psql $SUPABASE_DB_URL -f scripts/optimize-for-backfill.sql
```

This will:
- Make tables UNLOGGED (no WAL overhead)
- Drop all indexes
- Disable autovacuum

**⚠️ WARNING:** Tables are now NOT crash-safe. Don't run this in production with live traffic.

---

### PHASE 2: Run Parallel Backfill (8-24 hours)

#### Option A: GitHub Actions (Recommended)

1. Go to **Actions** → **Parallel Backfill**
2. Click **Run workflow**
3. Set parameters:
   - `migration_id`: Target migration (e.g., `1`)
   - `worker_count`: `8` (or `12` for faster load)
4. Monitor progress in Actions logs

#### Option B: Manual Parallel Execution

Run multiple instances of the script with different `WORKER_ID`:

**Terminal 1:**
```bash
WORKER_ID=0 WORKER_COUNT=8 node scripts/fetch-backfill-history.js
```

**Terminal 2:**
```bash
WORKER_ID=1 WORKER_COUNT=8 node scripts/fetch-backfill-history.js
```

**Terminal 3-8:** (repeat with `WORKER_ID=2` through `7`)

Each worker processes a different subset of synchronizers in parallel.

---

### PHASE 3: Restore Database (2-6 hours)

**After backfill completes, run restoration:**

```bash
psql $SUPABASE_DB_URL -f scripts/restore-after-backfill.sql
```

This will:
- Make tables LOGGED again (restore durability)
- Rebuild all indexes (takes longest - 2-6 hours for 84M rows)
- Re-enable autovacuum
- Run ANALYZE to update statistics

**Index creation progress:**
```sql
-- Monitor index creation
SELECT 
  schemaname, 
  tablename, 
  indexname, 
  pg_size_pretty(pg_relation_size(indexrelid)) as size
FROM pg_stat_user_indexes 
WHERE schemaname = 'public' 
  AND (tablename = 'ledger_updates' OR tablename = 'ledger_events')
ORDER BY tablename, indexname;
```

---

## Performance Expectations

### Single-threaded (before optimization):
- **Speed:** ~200-500 rows/sec
- **Time for 84M rows:** 46-97 hours ❌

### Optimized + 8 workers:
- **Speed:** ~20,000-30,000 rows/sec aggregate
- **Time for 84M rows:** 8-12 hours ✅

### Optimized + 12 workers:
- **Speed:** ~30,000-50,000 rows/sec aggregate
- **Time for 84M rows:** 5-8 hours ✅✅

---

## Monitoring Progress

### Check row counts:
```sql
SELECT 
  'ledger_updates' as table, COUNT(*) as rows 
FROM ledger_updates
UNION ALL
SELECT 
  'ledger_events' as table, COUNT(*) as rows 
FROM ledger_events;
```

### Check backfill cursors:
```sql
SELECT 
  cursor_name,
  migration_id,
  synchronizer_id,
  last_before,
  complete,
  updated_at
FROM backfill_cursors
ORDER BY updated_at DESC
LIMIT 20;
```

### Estimate completion time:
```sql
-- If you started at 10:00 AM and have 20M rows after 2 hours:
-- Rate: 20M / 2 hours = 10M rows/hour
-- Remaining: 84M - 20M = 64M rows
-- Time left: 64M / 10M = 6.4 hours
-- ETA: 10:00 AM + 2h + 6.4h = 6:24 PM
```

---

## Troubleshooting

### Problem: Workers are failing

**Check database connections:**
```bash
psql $SUPABASE_DB_URL -c "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database();"
```

If you see errors like "too many connections", reduce `worker_count`.

### Problem: Backfill is slower than expected

1. **Check if indexes still exist:**
```sql
\di ledger_updates*
\di ledger_events*
```

2. **Verify tables are UNLOGGED:**
```sql
SELECT 
  schemaname, 
  tablename, 
  relpersistence 
FROM pg_class c 
JOIN pg_namespace n ON c.relnamespace = n.oid
WHERE n.nspname = 'public' 
  AND c.relname IN ('ledger_updates', 'ledger_events');
-- relpersistence should be 'u' for UNLOGGED
```

3. **Check batch size in script:**
Verify `upsertInBatches` is using 10000+ rows.

---

## Rollback / Emergency Stop

If you need to abort:

1. **Stop all workers** (Ctrl+C or cancel GitHub Actions)

2. **Restore normal state:**
```bash
psql $SUPABASE_DB_URL -f scripts/restore-after-backfill.sql
```

3. **Optionally truncate and restart:**
```sql
TRUNCATE ledger_updates, ledger_events, backfill_cursors;
```

---

## Post-Backfill Verification

After restoration completes, verify data integrity:

```sql
-- Check for orphaned events (events without updates)
SELECT COUNT(*) 
FROM ledger_events e
LEFT JOIN ledger_updates u ON e.update_id = u.update_id
WHERE u.update_id IS NULL;
-- Should be 0 or very small

-- Check round distribution
SELECT 
  migration_id,
  MIN(round) as min_round,
  MAX(round) as max_round,
  COUNT(*) as total_updates
FROM ledger_updates
GROUP BY migration_id;

-- Check for NULL migration_id (should be 0 or very few)
SELECT COUNT(*) FROM ledger_updates WHERE migration_id IS NULL;
SELECT COUNT(*) FROM ledger_events WHERE migration_id IS NULL;
```

---

## Additional Tuning (Advanced)

### Increase PostgreSQL memory settings (if you have control):

```sql
ALTER SYSTEM SET shared_buffers = '8GB';
ALTER SYSTEM SET work_mem = '256MB';
ALTER SYSTEM SET maintenance_work_mem = '2GB';
ALTER SYSTEM SET effective_cache_size = '24GB';
SELECT pg_reload_conf();
```

### Monitor write amplification:

```sql
SELECT 
  xact_commit + xact_rollback as transactions,
  tup_inserted,
  tup_updated,
  pg_size_pretty(pg_database_size(current_database())) as db_size
FROM pg_stat_database 
WHERE datname = current_database();
```

---

## Summary Checklist

- [ ] Run `scripts/optimize-for-backfill.sql`
- [ ] Verify tables are UNLOGGED and indexes dropped
- [ ] Start 8-12 parallel workers
- [ ] Monitor progress every 2 hours
- [ ] After completion, run `scripts/restore-after-backfill.sql`
- [ ] Wait for index creation (2-6 hours)
- [ ] Verify data integrity
- [ ] 🎉 Done!
