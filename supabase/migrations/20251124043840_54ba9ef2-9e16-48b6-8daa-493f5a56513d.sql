-- Add missing columns to backfill_cursors table for the backfill script
ALTER TABLE public.backfill_cursors
ADD COLUMN IF NOT EXISTS synchronizer_id text,
ADD COLUMN IF NOT EXISTS min_time timestamp with time zone,
ADD COLUMN IF NOT EXISTS max_time timestamp with time zone,
ADD COLUMN IF NOT EXISTS last_before timestamp with time zone,
ADD COLUMN IF NOT EXISTS complete boolean DEFAULT false;