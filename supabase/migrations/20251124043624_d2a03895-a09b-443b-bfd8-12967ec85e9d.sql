-- Add migration_id column to backfill_cursors table
ALTER TABLE public.backfill_cursors
ADD COLUMN migration_id bigint;