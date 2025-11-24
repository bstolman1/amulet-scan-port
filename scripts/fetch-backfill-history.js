#!/usr/bin/env node

/**
 * fetch-backfill-history.js
 * Backfills historical ACS template statistics from Canton Network
 */

const axios = require('axios');
const { createClient } = require('@supabase/supabase-js');

const BASE_URL = process.env.BASE_URL || 'https://scan.sv-1.global.canton.network.sync.global/api/scan';
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY;
const BACKFILL_PAGE_SIZE = parseInt(process.env.BACKFILL_PAGE_SIZE || '200', 10);

if (!SUPABASE_URL || !SUPABASE_ANON_KEY) {
  console.error('❌ Missing SUPABASE_URL or SUPABASE_ANON_KEY');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);
const CURSOR_NAME = 'backfill_ledger';

async function getLastProcessedRound() {
  const { data, error } = await supabase
    .from('backfill_cursors')
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
    .from('backfill_cursors')
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

async function fetchAcsSnapshotForRound(round) {
  try {
    const res = await axios.get(`${BASE_URL}/v0/acs-snapshot`, {
      params: { round },
    });
    return res.data;
  } catch (error) {
    if (error.response?.status === 404) {
      return null;
    }
    throw error;
  }
}

async function processSnapshot(round, snapshotData) {
  const events = snapshotData?.acs_snapshot?.events || [];
  
  // Count instances per template
  const templateCounts = {};
  for (const event of events) {
    const templateId = event.template_id;
    if (templateId) {
      templateCounts[templateId] = (templateCounts[templateId] || 0) + 1;
    }
  }

  // Insert into acs_template_stats
  const statsToInsert = Object.entries(templateCounts).map(([template_name, instance_count]) => ({
    round,
    template_name,
    instance_count,
  }));

  if (statsToInsert.length > 0) {
    const { error } = await supabase
      .from('acs_template_stats')
      .upsert(statsToInsert, {
        onConflict: 'round,template_name',
      });

    if (error) {
      throw error;
    }
  }

  return statsToInsert.length;
}

async function main() {
  console.log('🚀 Starting backfill process...\n');

  try {
    const startRound = await getLastProcessedRound();
    console.log(`📍 Starting from round: ${startRound}\n`);

    // Fetch current round
    const { data: currentSnapshotMeta } = await axios.get(`${BASE_URL}/v0/acs-snapshot-timestamp`);
    const currentRoundMatch = currentSnapshotMeta.record_time.match(/r(\d+)/);
    const currentRound = currentRoundMatch ? parseInt(currentRoundMatch[1], 10) : 0;

    console.log(`📊 Current round: ${currentRound}\n`);

    let processedCount = 0;
    for (let round = startRound + 1; round <= currentRound; round++) {
      console.log(`📄 Processing round ${round}...`);

      const snapshot = await fetchAcsSnapshotForRound(round);
      
      if (!snapshot) {
        console.log(`   ⚠️  No snapshot found for round ${round}, skipping`);
        continue;
      }

      const templateCount = await processSnapshot(round, snapshot);
      console.log(`   ✓ Processed ${templateCount} templates`);

      await updateCursor(round);
      processedCount++;

      // Rate limiting
      if (processedCount % 10 === 0) {
        console.log(`   💤 Taking a short break...\n`);
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
    }

    console.log(`\n✅ Backfill complete! Processed ${processedCount} rounds`);

  } catch (error) {
    console.error('❌ Error:', error.message);
    if (error.response) {
      console.error('Response data:', error.response.data);
    }
    process.exit(1);
  }
}

main();
