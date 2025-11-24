#!/usr/bin/env node

/**
 * fetch-acs-data.js
 * Fetches the full ACS snapshot from Canton Network and uploads to Supabase
 * with real-time streaming to avoid timeouts.
 */

const axios = require('axios');
const { createClient } = require('@supabase/supabase-js');

const BASE_URL = process.env.BASE_URL || 'https://scan.sv-1.global.canton.network.sync.global/api/scan';
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY;
const PAGE_SIZE = parseInt(process.env.PAGE_SIZE || '500', 10);
const UPLOAD_CHUNK_SIZE = parseInt(process.env.UPLOAD_CHUNK_SIZE || '1', 10);
const UPLOAD_DELAY_MS = parseInt(process.env.UPLOAD_DELAY_MS || '200', 10);
const RETRY_COOLDOWN_MS = parseInt(process.env.RETRY_COOLDOWN_MS || '15000', 10);
const MAX_INFLIGHT_UPLOADS = parseInt(process.env.MAX_INFLIGHT_UPLOADS || '1', 10);

if (!SUPABASE_URL || !SUPABASE_ANON_KEY) {
  console.error('❌ Missing SUPABASE_URL or SUPABASE_ANON_KEY');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);

async function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function fetchAcsSnapshotTimestamp() {
  console.log('📡 Fetching ACS snapshot timestamp...');
  const res = await axios.get(`${BASE_URL}/v0/acs-snapshot-timestamp`);
  return res.data;
}

async function fetchAcsPage(migrationId, recordTime, afterToken = null) {
  const payload = {
    migration_id: migrationId,
    record_time: recordTime,
    page_size: PAGE_SIZE,
  };
  if (afterToken !== null) {
    payload.after = afterToken;
  }

  const res = await axios.post(`${BASE_URL}/v0/state/acs`, payload);
  return res.data;
}

async function uploadAcsSnapshot(round, timestamp, snapshotData) {
  const { error } = await supabase
    .from('acs_snapshots')
    .upsert({
      round,
      timestamp,
      snapshot_data: snapshotData,
    }, {
      onConflict: 'round',
      ignoreDuplicates: false,
    });

  if (error) {
    throw new Error(`Failed to upload snapshot: ${error.message}`);
  }
}

async function main() {
  console.log('🚀 Starting ACS snapshot fetch and upload...\n');

  try {
    // 1. Get snapshot metadata
    const { record_time, migration_id } = await fetchAcsSnapshotTimestamp();
    console.log(`✅ Snapshot timestamp: ${record_time} (migration_id: ${migration_id})\n`);

    // 2. Determine round number from record_time
    const roundMatch = record_time.match(/r(\d+)/);
    const round = roundMatch ? parseInt(roundMatch[1], 10) : 0;
    console.log(`📊 Round: ${round}\n`);

    // 3. Fetch all pages
    let afterToken = null;
    let pageNum = 1;
    let allEvents = [];
    let totalEvents = 0;

    while (true) {
      console.log(`📄 Fetching page ${pageNum} (after: ${afterToken})...`);
      const page = await fetchAcsPage(migration_id, record_time, afterToken);
      
      const events = page.created_events || [];
      allEvents = allEvents.concat(events);
      totalEvents += events.length;
      
      console.log(`   ✓ Got ${events.length} events (total: ${totalEvents})`);

      if (!page.next_page_token) {
        console.log('   ℹ️  No more pages\n');
        break;
      }

      afterToken = page.next_page_token;
      pageNum++;
      
      // Small delay between pages
      await sleep(100);
    }

    console.log(`\n📦 Total events fetched: ${totalEvents}`);

    // 4. Upload to Supabase
    console.log(`\n💾 Uploading snapshot to Supabase...`);
    await uploadAcsSnapshot(round, record_time, { events: allEvents });
    console.log(`✅ Successfully uploaded snapshot for round ${round}\n`);

  } catch (error) {
    console.error('❌ Error:', error.message);
    if (error.response) {
      console.error('Response data:', error.response.data);
    }
    process.exit(1);
  }
}

main();
