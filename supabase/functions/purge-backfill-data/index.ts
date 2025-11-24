import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.75.0";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

interface PurgeRequest {
  purge_all?: boolean;
  migration_id?: number;
  webhookSecret?: string;
}

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    const body: PurgeRequest = await req.json();
    const { purge_all, migration_id, webhookSecret } = body;

    const SUPABASE_URL = Deno.env.get('SUPABASE_URL')!;
    const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!;
    const WEBHOOK_SECRET = Deno.env.get('ACS_UPLOAD_WEBHOOK_SECRET');

    // Validate webhook secret if provided
    if (webhookSecret && WEBHOOK_SECRET && webhookSecret !== WEBHOOK_SECRET) {
      console.error("Invalid webhook secret");
      return new Response(
        JSON.stringify({ error: "Invalid webhook secret" }),
        { status: 403, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

    let deletedCursors = 0;
    let deletedUpdates = 0;
    let deletedEvents = 0;
    let resetLiveCursor = false;

    console.log("Starting backfill data purge...", { purge_all, migration_id });

    // Helper function to delete in small batches by fetching IDs first
    const deleteBatchByIds = async (table: string, batchSize: number, filter?: any) => {
      let totalDeleted = 0;
      let hasMore = true;

      while (hasMore) {
        // First, fetch a batch of IDs
        let selectQuery = supabase
          .from(table)
          .select('id')
          .limit(batchSize);

        if (filter) {
          Object.entries(filter).forEach(([key, value]) => {
            selectQuery = selectQuery.eq(key, value);
          });
        }

        const { data: records, error: selectError } = await selectQuery;

        if (selectError) {
          console.error(`Error selecting from ${table}:`, selectError);
          throw selectError;
        }

        if (!records || records.length === 0) {
          hasMore = false;
          break;
        }

        // Split into smaller chunks to avoid URL length limits (max 100 IDs per delete)
        const ids = records.map(r => r.id);
        const deleteChunkSize = 100;

        for (let i = 0; i < ids.length; i += deleteChunkSize) {
          const chunk = ids.slice(i, i + deleteChunkSize);
          const { error: deleteError } = await supabase
            .from(table)
            .delete()
            .in('id', chunk);

          if (deleteError) {
            console.error(`Error deleting from ${table}:`, deleteError);
            throw deleteError;
          }
        }

        totalDeleted += records.length;
        hasMore = records.length === batchSize;

        console.log(`Deleted ${records.length} records from ${table} (total: ${totalDeleted})`);
      }

      return totalDeleted;
    };

    // Delete ledger events (using id as primary key)
    if (purge_all) {
      console.log("Deleting all ledger events in batches...");
      deletedEvents = await deleteBatchByIds('ledger_events', 500);
      console.log(`✅ Deleted ${deletedEvents} total ledger events`);
    }

    // Delete ledger updates (using id as primary key)
    if (purge_all) {
      console.log("Deleting all ledger updates in batches...");
      deletedUpdates = await deleteBatchByIds('ledger_updates', 500);
      console.log(`✅ Deleted ${deletedUpdates} total ledger updates`);
    }

    // Delete backfill cursors
    if (purge_all) {
      console.log("Deleting all backfill cursors in batches...");
      deletedCursors = await deleteBatchByIds('backfill_cursors', 200);
      console.log(`✅ Deleted ${deletedCursors} total backfill cursors`);
    }

    // Reset live update cursor if purging all
    if (purge_all) {
      console.log("Resetting live update cursor...");
      try {
        await deleteBatchByIds('live_update_cursor', 100);
        resetLiveCursor = true;
        console.log("✅ Live update cursor reset");
      } catch (cursorError) {
        console.error("Error resetting live update cursor:", cursorError);
        // Don't throw, this is optional
      }
    }

    const result = {
      success: true,
      deleted_cursors: deletedCursors,
      deleted_updates: deletedUpdates,
      deleted_events: deletedEvents,
      reset_live_cursor: resetLiveCursor,
      migration_id: migration_id || null,
      purge_all: purge_all || false,
    };

    console.log("Backfill purge complete:", result);

    return new Response(
      JSON.stringify(result),
      {
        status: 200,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      }
    );

  } catch (error) {
    console.error("Error in purge-backfill-data:", error);

    // Handle Supabase errors which are objects with code, message, etc.
    let errorMessage = 'Unknown error occurred';
    let errorDetails = '';

    if (error && typeof error === 'object') {
      if ('message' in error) {
        errorMessage = String(error.message);
      }
      if ('code' in error) {
        errorDetails = `Code: ${error.code}`;
      }
      if ('details' in error && error.details) {
        errorDetails += ` Details: ${error.details}`;
      }
      if ('hint' in error && error.hint) {
        errorDetails += ` Hint: ${error.hint}`;
      }
      // Fallback to JSON stringify
      if (!errorDetails) {
        errorDetails = JSON.stringify(error);
      }
    } else if (error instanceof Error) {
      errorMessage = error.message;
      errorDetails = error.stack || error.toString();
    }

    return new Response(
      JSON.stringify({
        error: errorMessage,
        details: errorDetails
      }),
      {
        status: 500,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      }
    );
  }
});
