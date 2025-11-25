-- ============================================================
-- RESTORATION SCRIPT: AFTER BACKFILL
-- ============================================================
-- Run this AFTER backfill completes to restore durability and performance
-- ============================================================

-- 1. Make tables LOGGED again (restore durability)
ALTER TABLE ledger_updates SET LOGGED;
ALTER TABLE ledger_events SET LOGGED;
ALTER TABLE backfill_cursors SET LOGGED;

-- 2. Recreate all indexes for query performance
-- This may take several hours for 84M rows, but it's necessary

-- Primary lookup indexes
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ledger_updates_update_id ON ledger_updates(update_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ledger_events_event_id ON ledger_events(event_id);

-- Migration filtering indexes
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ledger_updates_migration_id ON ledger_updates(migration_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ledger_events_migration_id ON ledger_events(migration_id);

-- Round-based queries
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ledger_updates_round ON ledger_updates(round);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ledger_events_round ON ledger_events(round);

-- Time-based queries
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ledger_updates_record_time ON ledger_updates(record_time);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ledger_events_created_at_ts ON ledger_events(created_at_ts);

-- Relationship indexes
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ledger_events_update_id ON ledger_events(update_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ledger_events_contract_id ON ledger_events(contract_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ledger_events_template_id ON ledger_events(template_id);

-- Synchronizer filtering
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ledger_updates_synchronizer_id ON ledger_updates(synchronizer_id);

-- 3. Re-enable autovacuum
ALTER TABLE ledger_updates SET (autovacuum_enabled = true);
ALTER TABLE ledger_events SET (autovacuum_enabled = true);
ALTER TABLE backfill_cursors SET (autovacuum_enabled = true);

-- 4. Run ANALYZE to update statistics for query planner
ANALYZE ledger_updates;
ANALYZE ledger_events;
ANALYZE backfill_cursors;

-- 5. Optional: VACUUM FULL to reclaim space and defragment
-- (Only run if you have maintenance window - can take hours)
-- VACUUM FULL ledger_updates;
-- VACUUM FULL ledger_events;

SELECT 'Restoration complete - tables are now LOGGED with all indexes rebuilt' as status;
