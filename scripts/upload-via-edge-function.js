#!/usr/bin/env node

/**
 * upload-via-edge-function.js
 * Uploads data via Supabase Edge Function for enhanced security
 */

const axios = require('axios');

const EDGE_FUNCTION_URL = process.env.EDGE_FUNCTION_URL;
const ACS_UPLOAD_WEBHOOK_SECRET = process.env.ACS_UPLOAD_WEBHOOK_SECRET;

if (!EDGE_FUNCTION_URL || !ACS_UPLOAD_WEBHOOK_SECRET) {
  console.error('❌ Missing EDGE_FUNCTION_URL or ACS_UPLOAD_WEBHOOK_SECRET');
  process.exit(1);
}

async function uploadViaEdgeFunction(data) {
  console.log(`📤 Uploading ${data.events?.length || 0} events via edge function...`);

  const res = await axios.post(EDGE_FUNCTION_URL, data, {
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${ACS_UPLOAD_WEBHOOK_SECRET}`,
    },
    timeout: 60000,
  });

  return res.data;
}

async function main(dataPayload) {
  console.log('🚀 Starting upload via edge function...\n');

  try {
    const result = await uploadViaEdgeFunction(dataPayload);
    console.log('✅ Upload successful:', result);
  } catch (error) {
    console.error('❌ Error:', error.message);
    if (error.response) {
      console.error('Response data:', error.response.data);
    }
    process.exit(1);
  }
}

// If called directly with data file argument
if (require.main === module) {
  const dataFile = process.argv[2];
  if (!dataFile) {
    console.error('❌ Usage: node upload-via-edge-function.js <data-file.json>');
    process.exit(1);
  }

  const fs = require('fs');
  const data = JSON.parse(fs.readFileSync(dataFile, 'utf8'));
  main(data);
}

module.exports = { uploadViaEdgeFunction };
