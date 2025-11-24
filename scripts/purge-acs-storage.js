#!/usr/bin/env node

/**
 * purge-acs-storage.js
 * Cleans up old ACS snapshots from Supabase to manage storage
 */

const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY;
const RETENTION_DAYS = parseInt(process.env.RETENTION_DAYS || '30', 10);

if (!SUPABASE_URL || !SUPABASE_ANON_KEY) {
  console.error('❌ Missing SUPABASE_URL or SUPABASE_ANON_KEY');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);

async function purgeOldSnapshots() {
  const cutoffDate = new Date();
  cutoffDate.setDate(cutoffDate.getDate() - RETENTION_DAYS);
  const cutoffISO = cutoffDate.toISOString();

  console.log(`🗑️  Purging snapshots older than ${cutoffISO}...\n`);

  const { data, error } = await supabase
    .from('acs_snapshots')
    .delete()
    .lt('created_at', cutoffISO)
    .select('id');

  if (error) {
    throw error;
  }

  console.log(`✅ Deleted ${data?.length || 0} old snapshots`);
}

async function main() {
  console.log('🚀 Starting ACS storage purge...\n');

  try {
    await purgeOldSnapshots();
    console.log('\n✅ Purge complete!');
  } catch (error) {
    console.error('❌ Error:', error.message);
    process.exit(1);
  }
}

main();
