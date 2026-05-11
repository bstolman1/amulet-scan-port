#!/usr/bin/env node

/**
 * Backfill Vote Results
 *
 * One-shot script that fetches all historical vote results from the Canton
 * Scan API and persists them into the local DuckDB vote_requests table.
 *
 * Safe to run multiple times — uses ON CONFLICT upsert.
 *
 * Usage:
 *   node scripts/backfill-vote-results.js
 *   node scripts/backfill-vote-results.js --limit 1000
 */

import '../server/env.js';
import { initEngineSchema } from '../server/engine/schema.js';
import { upsertVoteResults, getVoteResultCount } from '../server/lib/vote-result-store.js';
import { getCurrentEndpoint, checkAllEndpoints } from '../server/lib/endpoint-rotation.js';
import { extractHostname, createDispatcher } from '../server/lib/undici-dispatcher.js';

const limit = parseInt(process.argv.find(a => a.startsWith('--limit='))?.split('=')[1] || '1000', 10);

async function main() {
  console.log('── Backfill Vote Results ──');

  // 1. Ensure DuckDB schema exists
  console.log('Initializing DuckDB schema...');
  await initEngineSchema();

  const beforeCount = await getVoteResultCount();
  console.log(`Currently stored: ${beforeCount} vote results`);

  // 2. Find a healthy Scan API endpoint
  console.log('Checking Scan API endpoints...');
  await checkAllEndpoints();
  const endpoint = getCurrentEndpoint();
  console.log(`Using endpoint: ${endpoint.name}`);

  // 3. Fetch all vote results
  const hostname = extractHostname(endpoint.url);
  const dispatcher = hostname ? createDispatcher(hostname) : undefined;
  const scanUrl = `${endpoint.url}/v0/admin/sv/voteresults`;

  console.log(`Fetching up to ${limit} vote results from Scan API...`);

  const res = await fetch(scanUrl, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Accept: 'application/json',
      ...(hostname ? { Host: hostname } : {}),
    },
    body: JSON.stringify({ limit }),
    ...(dispatcher ? { dispatcher } : {}),
    signal: AbortSignal.timeout(60_000),
  });

  if (!res.ok) {
    const text = await res.text();
    console.error(`Scan API returned ${res.status}: ${text.slice(0, 500)}`);
    process.exit(1);
  }

  const data = await res.json();
  const results = data.dso_rules_vote_results || [];
  console.log(`Fetched ${results.length} vote results from Scan API`);

  if (results.length === 0) {
    console.log('Nothing to backfill.');
    process.exit(0);
  }

  // 4. Upsert into DuckDB
  console.log('Upserting into DuckDB...');
  const { inserted, skipped } = await upsertVoteResults(results);

  const afterCount = await getVoteResultCount();
  console.log(`\nDone.`);
  console.log(`  Fetched:  ${results.length}`);
  console.log(`  Upserted: ${inserted}`);
  console.log(`  Skipped:  ${skipped}`);
  console.log(`  Stored:   ${beforeCount} → ${afterCount}`);

  process.exit(0);
}

main().catch((err) => {
  console.error('Backfill failed:', err);
  process.exit(1);
});
