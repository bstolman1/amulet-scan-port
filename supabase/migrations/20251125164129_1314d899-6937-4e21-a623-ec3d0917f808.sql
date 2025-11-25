-- Add indexes on ledger_events for better query performance
CREATE INDEX IF NOT EXISTS idx_ledger_events_template_id ON ledger_events(template_id);
CREATE INDEX IF NOT EXISTS idx_ledger_events_event_type ON ledger_events(event_type);
CREATE INDEX IF NOT EXISTS idx_ledger_events_created_at ON ledger_events(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_ledger_events_round ON ledger_events(round);

-- Add composite index for common query patterns (template + time)
CREATE INDEX IF NOT EXISTS idx_ledger_events_template_created ON ledger_events(template_id, created_at DESC);

-- Add indexes on ledger_updates for better query performance
CREATE INDEX IF NOT EXISTS idx_ledger_updates_update_type ON ledger_updates(update_type);
CREATE INDEX IF NOT EXISTS idx_ledger_updates_created_at ON ledger_updates(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_ledger_updates_round ON ledger_updates(round);

-- Add composite index for common query patterns
CREATE INDEX IF NOT EXISTS idx_ledger_updates_type_created ON ledger_updates(update_type, created_at DESC);

-- Add indexes on backfill_cursors for monitoring queries
CREATE INDEX IF NOT EXISTS idx_backfill_cursors_cursor_name ON backfill_cursors(cursor_name);
CREATE INDEX IF NOT EXISTS idx_backfill_cursors_updated_at ON backfill_cursors(updated_at DESC);

-- Optimize for LIKE queries on template_id (text pattern matching)
CREATE INDEX IF NOT EXISTS idx_ledger_events_template_pattern ON ledger_events(template_id text_pattern_ops);
CREATE INDEX IF NOT EXISTS idx_ledger_events_package_name ON ledger_events(package_name) WHERE package_name IS NOT NULL;