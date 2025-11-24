#!/usr/bin/env node

/**
 * ingest-updates.js
 * Ingests live ledger updates from Canton Network
 */

const axios = require('axios');
const { createClient } = require('@supabase/supabase-js');

const BASE_URL = process.env.BASE_URL || 'https://scan.sv-1.global.canton.network.sync.global/api/scan';
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY;

if (!SUPABASE_URL || !SUPABASE_ANON_KEY) {
  console.error('❌ Missing SUPABASE_URL or SUPABASE_ANON_KEY');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);
const CURSOR_NAME = 'live_updates';

async function getLastProcessedUpdate() {
  const { data, error } = await supabase
    .from('live_update_cursor')
    .select('last_processed_round')
    .eq('cursor_name', CURSOR_NAME)
    .single();

  if (error && error.code !== 'PGRST116') {
    throw error;
  }

  return data?.last_processed_round || 0;
}

async function updateCursor(round) {
  const { error } = await supabase
    .from('live_update_cursor')
    .upsert({
      cursor_name: CURSOR_NAME,
      last_processed_round: round,
      updated_at: new Date().toISOString(),
    }, {
      onConflict: 'cursor_name',
    });

  if (error) {
    throw error;
  }
}

async function fetchUpdates(afterMigrationId, afterRecordTime) {
  const payload = {
    page_size: 100,
  };

  if (afterMigrationId && afterRecordTime) {
    payload.after = {
      after_migration_id: afterMigrationId,
      after_record_time: afterRecordTime,
    };
  }

  const res = await axios.post(`${BASE_URL}/v2/updates`, payload);
  return res.data;
}

async function processUpdate(transaction) {
  const roundMatch = transaction.record_time?.match(/r(\d+)/);
  const round = roundMatch ? parseInt(roundMatch[1], 10) : 0;

  const updateData = {
    round,
    update_type: transaction.workflow_id || 'unknown',
    timestamp: transaction.record_time,
    update_data: transaction,
  };

  const { error } = await supabase
    .from('ledger_updates')
    .insert(updateData);

  if (error) {
    // Ignore duplicate key errors
    if (error.code !== '23505') {
      throw error;
    }
  }

  return round;
}

async function main() {
  console.log('🚀 Starting live updates ingestion...\n');

  try {
    const lastRound = await getLastProcessedUpdate();
    console.log(`📍 Last processed round: ${lastRound}\n`);

    let afterMigrationId = null;
    let afterRecordTime = null;
    let processedCount = 0;
    let maxRound = lastRound;

    // Fetch updates in pages
    while (true) {
      console.log(`📄 Fetching updates page...`);
      const { transactions } = await fetchUpdates(afterMigrationId, afterRecordTime);

      if (!transactions || transactions.length === 0) {
        console.log('   ℹ️  No more updates\n');
        break;
      }

      for (const tx of transactions) {
        const round = await processUpdate(tx);
        maxRound = Math.max(maxRound, round);
        processedCount++;
      }

      console.log(`   ✓ Processed ${transactions.length} updates (total: ${processedCount})`);

      // Update cursor for next page
      const lastTx = transactions[transactions.length - 1];
      afterMigrationId = lastTx.migration_id;
      afterRecordTime = lastTx.record_time;

      // If we got less than page size, we're done
      if (transactions.length < 100) {
        break;
      }
    }

    if (maxRound > lastRound) {
      await updateCursor(maxRound);
      console.log(`\n✅ Ingestion complete! Processed ${processedCount} updates (up to round ${maxRound})`);
    } else {
      console.log(`\n✅ No new updates found`);
    }

  } catch (error) {
    console.error('❌ Error:', error.message);
    if (error.response) {
      console.error('Response data:', error.response.data);
    }
    process.exit(1);
  }
}

main();
