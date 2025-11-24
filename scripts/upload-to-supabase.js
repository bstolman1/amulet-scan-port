#!/usr/bin/env node

/**
 * upload-to-supabase.js
 * Generic utility for uploading data to Supabase tables
 */

const { createClient } = require('@supabase/supabase-js');
const fs = require('fs');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY;
const TABLE_NAME = process.env.TABLE_NAME;
const DATA_FILE = process.env.DATA_FILE;

if (!SUPABASE_URL || !SUPABASE_ANON_KEY || !TABLE_NAME || !DATA_FILE) {
  console.error('❌ Missing required environment variables');
  console.error('Required: SUPABASE_URL, SUPABASE_ANON_KEY, TABLE_NAME, DATA_FILE');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);

async function uploadData() {
  console.log(`📂 Reading data from ${DATA_FILE}...`);
  const fileContent = fs.readFileSync(DATA_FILE, 'utf8');
  const data = JSON.parse(fileContent);

  console.log(`📊 Found ${data.length} records`);

  // Upload in batches
  const BATCH_SIZE = 100;
  let uploaded = 0;

  for (let i = 0; i < data.length; i += BATCH_SIZE) {
    const batch = data.slice(i, i + BATCH_SIZE);
    
    console.log(`📤 Uploading batch ${Math.floor(i / BATCH_SIZE) + 1}...`);
    
    const { error } = await supabase
      .from(TABLE_NAME)
      .upsert(batch);

    if (error) {
      throw error;
    }

    uploaded += batch.length;
    console.log(`   ✓ Uploaded ${uploaded}/${data.length} records`);
  }

  console.log(`\n✅ Successfully uploaded ${uploaded} records to ${TABLE_NAME}`);
}

async function main() {
  console.log('🚀 Starting data upload to Supabase...\n');

  try {
    await uploadData();
  } catch (error) {
    console.error('❌ Error:', error.message);
    process.exit(1);
  }
}

main();
