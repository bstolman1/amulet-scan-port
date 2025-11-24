-- Add missing columns to acs_snapshots table
ALTER TABLE public.acs_snapshots 
ADD COLUMN IF NOT EXISTS migration_id bigint,
ADD COLUMN IF NOT EXISTS record_time timestamp with time zone,
ADD COLUMN IF NOT EXISTS sv_url text,
ADD COLUMN IF NOT EXISTS canonical_package text,
ADD COLUMN IF NOT EXISTS amulet_total numeric DEFAULT 0,
ADD COLUMN IF NOT EXISTS locked_total numeric DEFAULT 0,
ADD COLUMN IF NOT EXISTS circulating_supply numeric DEFAULT 0,
ADD COLUMN IF NOT EXISTS entry_count integer DEFAULT 0,
ADD COLUMN IF NOT EXISTS status text DEFAULT 'completed',
ADD COLUMN IF NOT EXISTS error_message text,
ADD COLUMN IF NOT EXISTS updated_at timestamp with time zone DEFAULT now();

-- Create index on status for faster queries
CREATE INDEX IF NOT EXISTS idx_acs_snapshots_status ON public.acs_snapshots(status);
CREATE INDEX IF NOT EXISTS idx_acs_snapshots_timestamp ON public.acs_snapshots(timestamp DESC);

-- Update existing rows to have default values
UPDATE public.acs_snapshots 
SET 
  migration_id = 0,
  record_time = timestamp,
  sv_url = 'https://sv.amulet.network',
  status = 'completed',
  amulet_total = 0,
  locked_total = 0,
  circulating_supply = 0,
  entry_count = 0,
  updated_at = created_at
WHERE migration_id IS NULL;

-- Create trigger to update updated_at
CREATE OR REPLACE TRIGGER update_acs_snapshots_updated_at
  BEFORE UPDATE ON public.acs_snapshots
  FOR EACH ROW
  EXECUTE FUNCTION public.update_updated_at_column();

-- Update acs_template_stats table structure
ALTER TABLE public.acs_template_stats
ADD COLUMN IF NOT EXISTS snapshot_id uuid,
ADD COLUMN IF NOT EXISTS template_id text,
ADD COLUMN IF NOT EXISTS contract_count integer DEFAULT 0,
ADD COLUMN IF NOT EXISTS field_sums jsonb,
ADD COLUMN IF NOT EXISTS status_tallies jsonb,
ADD COLUMN IF NOT EXISTS storage_path text;

-- Migrate data: use template_name as template_id and instance_count as contract_count
UPDATE public.acs_template_stats
SET 
  template_id = template_name,
  contract_count = instance_count
WHERE template_id IS NULL;

-- Create index for better query performance
CREATE INDEX IF NOT EXISTS idx_acs_template_stats_snapshot_id ON public.acs_template_stats(snapshot_id);
CREATE INDEX IF NOT EXISTS idx_acs_template_stats_template_id ON public.acs_template_stats(template_id);

-- Create trigger to update updated_at for acs_template_stats
CREATE OR REPLACE TRIGGER update_acs_template_stats_updated_at
  BEFORE UPDATE ON public.acs_template_stats
  FOR EACH ROW
  EXECUTE FUNCTION public.update_updated_at_column();