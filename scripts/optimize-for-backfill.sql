-- ============================================================
-- OPTIMIZATION SCRIPT: BEFORE BACKFILL
-- ============================================================
-- Run this BEFORE starting the backfill process to maximize speed
-- This will make writes 10x-50x faster
-- ============================================================

-- 1. Make tables UNLOGGED (3x-10x faster writes, no WAL overhead)
ALTER TABLE ledger_updates SET UNLOGGED;
ALTER TABLE ledger_events SET UNLOGGED;
ALTER TABLE backfill_cursors SET UNLOGGED;

-- 2. Drop all indexes on target tables (5x-15x faster inserts)
DROP INDEX IF EXISTS idx_ledger_events_migration_id;
DROP INDEX IF EXISTS idx_ledger_updates_migration_id;
DROP INDEX IF EXISTS idx_ledger_updates_round;
DROP INDEX IF EXISTS idx_ledger_events_round;
DROP INDEX IF EXISTS idx_ledger_updates_record_time;
DROP INDEX IF EXISTS idx_ledger_events_created_at_ts;
DROP INDEX IF EXISTS idx_ledger_updates_update_id;
DROP INDEX IF EXISTS idx_ledger_events_event_id;
DROP INDEX IF EXISTS idx_ledger_events_update_id;
DROP INDEX IF EXISTS idx_ledger_events_contract_id;
DROP INDEX IF EXISTS idx_ledger_events_template_id;
DROP INDEX IF EXISTS idx_ledger_updates_synchronizer_id;

-- 3. Disable autovacuum during bulk load (prevents interference)
ALTER TABLE ledger_updates SET (autovacuum_enabled = false);
ALTER TABLE ledger_events SET (autovacuum_enabled = false);
ALTER TABLE backfill_cursors SET (autovacuum_enabled = false);

-- 4. Increase maintenance_work_mem for faster index creation later
-- (This is session-level, so you'd set this in your connection)
-- SET maintenance_work_mem = '2GB';

SELECT 'Backfill optimization complete - tables are now UNLOGGED with no indexes' as status;
