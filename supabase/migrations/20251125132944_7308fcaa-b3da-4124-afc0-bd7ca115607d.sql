-- Add migration_id column to ledger_events table
ALTER TABLE ledger_events ADD COLUMN migration_id bigint;

-- Add an index for faster lookups by migration_id
CREATE INDEX idx_ledger_events_migration_id ON ledger_events(migration_id);