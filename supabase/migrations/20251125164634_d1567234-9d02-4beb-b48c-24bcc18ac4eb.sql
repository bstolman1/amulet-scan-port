-- Drop the index on round column first
DROP INDEX IF EXISTS idx_ledger_events_round;

-- Remove the round column from ledger_events
ALTER TABLE ledger_events DROP COLUMN IF EXISTS round;