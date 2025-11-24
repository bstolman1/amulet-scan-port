-- Add missing columns to ledger_updates table for backfill script
ALTER TABLE public.ledger_updates
ADD COLUMN IF NOT EXISTS update_id text UNIQUE,
ADD COLUMN IF NOT EXISTS migration_id bigint,
ADD COLUMN IF NOT EXISTS synchronizer_id text,
ADD COLUMN IF NOT EXISTS record_time timestamp with time zone,
ADD COLUMN IF NOT EXISTS effective_at timestamp with time zone,
ADD COLUMN IF NOT EXISTS "offset" bigint,
ADD COLUMN IF NOT EXISTS workflow_id text,
ADD COLUMN IF NOT EXISTS kind text,
ADD COLUMN IF NOT EXISTS raw jsonb;

-- Add missing columns to ledger_events table for backfill script
ALTER TABLE public.ledger_events
ADD COLUMN IF NOT EXISTS event_id text UNIQUE,
ADD COLUMN IF NOT EXISTS update_id text,
ADD COLUMN IF NOT EXISTS contract_id text,
ADD COLUMN IF NOT EXISTS template_id text,
ADD COLUMN IF NOT EXISTS package_name text,
ADD COLUMN IF NOT EXISTS payload jsonb,
ADD COLUMN IF NOT EXISTS signatories text[],
ADD COLUMN IF NOT EXISTS observers text[],
ADD COLUMN IF NOT EXISTS created_at_ts timestamp with time zone,
ADD COLUMN IF NOT EXISTS raw jsonb;