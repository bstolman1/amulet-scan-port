-- Enable INSERT and UPDATE for backfill operations
-- These tables are populated by automated backfill scripts

-- backfill_cursors policies
CREATE POLICY "Allow backfill operations on cursors"
ON public.backfill_cursors
FOR ALL
USING (true)
WITH CHECK (true);

-- ledger_updates policies
CREATE POLICY "Allow backfill operations on ledger_updates"
ON public.ledger_updates
FOR ALL
USING (true)
WITH CHECK (true);

-- ledger_events policies
CREATE POLICY "Allow backfill operations on ledger_events"
ON public.ledger_events
FOR ALL
USING (true)
WITH CHECK (true);