#!/usr/bin/env node
/**
 * Vote Requests Diagnostic Script
 * 
 * Compares vote_requests indexed in DuckDB with Explorer's completed votes
 * to identify missing proposals.
 * 
 * Usage: node scripts/diagnose-vote-requests.js [--base http://localhost:3001]
 */

const BASE_URL = process.argv.includes('--base') 
  ? process.argv[process.argv.indexOf('--base') + 1] 
  : 'http://localhost:3001';

// Explorer data from user paste - Request IDs (first 6 chars) with key metadata
// Format: { id_prefix, requester, votes_accept, votes_reject, status, description_snippet }
const EXPLORER_PROPOSALS = [
  // Page 1 - Most Recent
  { id: '002dbc', requester: 'Global-Synchronizer-Foundation', votes: '13', status: 'accepted', desc: 'Nightly Wallet' },
  { id: '002088', requester: 'Global-Synchronizer-Foundation', votes: '13', status: 'accepted', desc: 'Ubyx Clearing' },
  { id: '00dcff', requester: 'Global-Synchronizer-Foundation', votes: '13', status: 'accepted', desc: 'Cantory token factory' },
  { id: '00056c', requester: 'Global-Synchronizer-Foundation', votes: '13', status: 'accepted', desc: 'CC Thank You' },
  { id: '000b84', requester: 'MPC-Holding-Inc', votes: '10 3', status: 'accepted', desc: 'Rhein Finance enzoBTC' },
  { id: '0089d5', requester: 'Global-Synchronizer-Foundation', votes: '13', status: 'accepted', desc: 'Ubyx milestone CIP-0071' },
  { id: '00d372', requester: 'Global-Synchronizer-Foundation', votes: '13', status: 'accepted', desc: 'Talos milestone CIP-0085' },
  { id: '00fc5e', requester: 'Global-Synchronizer-Foundation', votes: '9 4', status: 'accepted', desc: 'ALL DEFI pause' },
  { id: '000734', requester: 'Global-Synchronizer-Foundation', votes: '9 4', status: 'accepted', desc: 'Fulcrum pause' },
  { id: '00b60e', requester: 'Global-Synchronizer-Foundation', votes: '9 4', status: 'accepted', desc: 'Registerlabs pause' },
  { id: '009cf0', requester: 'Global-Synchronizer-Foundation', votes: '9 4', status: 'accepted', desc: 'HandlPay pause' },
  { id: '00207f', requester: 'Global-Synchronizer-Foundation', votes: '9 4', status: 'accepted', desc: 'Orphil LLC pause' },
  { id: '00958f', requester: 'MPC-Holding-Inc', votes: '13', status: 'accepted', desc: 'Otoclick Tabiri Market' },
  { id: '0048f5', requester: 'Global-Synchronizer-Foundation', votes: '9 4', status: 'accepted', desc: 'Alum Labs removal' },
  { id: '00423c', requester: 'Global-Synchronizer-Foundation', votes: '9 4', status: 'accepted', desc: 'Macao Mining removal' },
  { id: '003b0b', requester: 'Five-North-1', votes: '13', status: 'accepted', desc: 'Mandalo revoke old' },
  { id: '00cfb8', requester: 'Five-North-1', votes: '13', status: 'accepted', desc: 'Mandalo new party' },
  { id: '006a88', requester: 'Digital-Asset-2', votes: '13', status: 'accepted', desc: 'Node Fortress wallet' },
  { id: '00cbd9', requester: 'Five-North-1', votes: '13', status: 'accepted', desc: 'Axymos xNS' },
  { id: '0074ac', requester: 'Global-Synchronizer-Foundation', votes: '13', status: 'accepted', desc: 'DTCC SV weight increase' },
  
  // Page 2
  { id: '0096f0', requester: 'Global-Synchronizer-Foundation', votes: '12 1', status: 'accepted', desc: 'Republic milestone CIP-0080' },
  { id: '00a49d', requester: 'Global-Synchronizer-Foundation', votes: '12 1', status: 'accepted', desc: 'Ledger milestone CIP-0069' },
  { id: '007569', requester: 'Global-Synchronizer-Foundation', votes: '12 1', status: 'accepted', desc: 'Kairo DEX' },
  { id: '005bb3', requester: 'Global-Synchronizer-Foundation', votes: '12 1', status: 'accepted', desc: 'Zero Hash milestone CIP-0060' },
  { id: '006d72', requester: 'Digital-Asset-2', votes: '12 1', status: 'accepted', desc: 'DeSyn Protocol' },
  { id: '0046fe', requester: 'Global-Synchronizer-Foundation', votes: '12 1', status: 'accepted', desc: 'CC Browser' },
  { id: '004654', requester: 'Digital-Asset-2', votes: '13', status: 'accepted', desc: 'OpusAccess' },
  { id: '0097e0', requester: 'Global-Synchronizer-Foundation', votes: '13', status: 'accepted', desc: 'Zero Hash resubmit' },
  { id: '005bbc', requester: 'Five-North-1', votes: '13', status: 'accepted', desc: 'Send Safe' },
  { id: '0035d1', requester: 'Five-North-1', votes: '13', status: 'accepted', desc: 'Unhedged' },
  { id: '003711', requester: 'Global-Synchronizer-Foundation', votes: '13', status: 'accepted', desc: 'SciFeCap' },
  { id: '0000ab', requester: 'Global-Synchronizer-Foundation', votes: '13', status: 'accepted', desc: 'USDT0 weight increase' },
  { id: '00865f', requester: 'Digital-Asset-2', votes: '12 1', status: 'accepted', desc: 'Opus28' },
  { id: '001fd2', requester: 'Five-North-1', votes: '13', status: 'accepted', desc: 'Modulo' },
  { id: '00c755', requester: 'Five-North-1', votes: '12 1', status: 'accepted', desc: 'Canton Finance' },
  { id: '007f12', requester: 'Global-Synchronizer-Foundation', votes: '13', status: 'accepted', desc: 'Zero Hash' },
  { id: '00b168', requester: 'C7-Technology-Services-Limited', votes: '12 1', status: 'accepted', desc: 'Flowryd' },
  { id: '00e6fa', requester: 'Digital-Asset-2', votes: '13', status: 'accepted', desc: 'Major upgrade 0.5.1' },
  { id: '0004c6', requester: 'Digital-Asset-2', votes: '13', status: 'accepted', desc: 'RIZE Score' },
  { id: '0031c5', requester: 'MPC-Holding-Inc', votes: '11 2', status: 'accepted', desc: 'MPCH weight increase' },
  
  // Page 3
  { id: '004a1c', requester: 'Global-Synchronizer-Foundation', votes: '13', status: 'accepted', desc: 'Zodia Custody' },
  { id: '00ce00', requester: 'Digital-Asset-2', votes: '13', status: 'accepted', desc: 'Lithium Digital' },
  { id: '00c1b8', requester: 'Global-Synchronizer-Foundation', votes: '12 1', status: 'accepted', desc: 'ACME Lend' },
  { id: '001f17', requester: 'Digital-Asset-2', votes: '12 1', status: 'accepted', desc: 'Thetamarkets' },
  { id: '00dafd', requester: 'Digital-Asset-2', votes: '13', status: 'accepted', desc: 'UnitedApp' },
  { id: '005b3a', requester: 'Digital-Asset-2', votes: '13', status: 'accepted', desc: 'Cantor8 Vault partyID fix' },
  { id: '00256f', requester: 'Proof-Group-1', votes: '12 1', status: 'accepted', desc: 'Flipside' },
  { id: '006d09', requester: 'Five-North-1', votes: '9 1 3', status: 'accepted', desc: 'Conton Bot' },
  { id: '008f41', requester: 'Digital-Asset-1', votes: '12 1', status: 'accepted', desc: 'Verity' },
  { id: '005fe2', requester: 'Digital-Asset-2', votes: '12 1', status: 'accepted', desc: 'Akascan' },
  { id: '0051c9', requester: 'Five-North-1', votes: '12 1', status: 'accepted', desc: 'Zoro Wallet' },
  { id: '0099a1', requester: 'Global-Synchronizer-Foundation', votes: '12 1', status: 'accepted', desc: 'HandlPay' },
  { id: '00a0fd', requester: 'Global-Synchronizer-Foundation', votes: '12 1', status: 'accepted', desc: 'PropNotary' },
  { id: '00dc3c', requester: 'Five-North-1', votes: '12 1', status: 'accepted', desc: 'Trade.Fast' },
  { id: '008b19', requester: 'Digital-Asset-2', votes: '12 1', status: 'accepted', desc: 'Modo by StakeTab' },
  { id: '00ebe8', requester: 'Proof-Group-1', votes: '12 1', status: 'accepted', desc: 'Mandalo Inc' },
  { id: '0055c9', requester: 'Digital-Asset-2', votes: '8 5', status: 'rejected', desc: 'Cantor8 Vault (rejected)' },
  { id: '006ce8', requester: 'Digital-Asset-2', votes: '13', status: 'accepted', desc: 'Cantor8 Swaps' },
  { id: '009af5', requester: 'Global-Synchronizer-Foundation', votes: '12 1', status: 'accepted', desc: 'AngelHack resubmit' },
  { id: '005acd', requester: 'Global-Synchronizer-Foundation', votes: '13', status: 'accepted', desc: 'Supplier compliance app' },
  
  // Page 4
  { id: '0070e2', requester: 'Global-Synchronizer-Foundation', votes: '13', status: 'accepted', desc: 'Zenith SV' },
  { id: '009a84', requester: 'Global-Synchronizer-Foundation', votes: '13', status: 'accepted', desc: 'Excellar (new party)' },
  { id: '0080a9', requester: 'MPC-Holding-Inc', votes: '13', status: 'accepted', desc: 'Crypto Treasury Connector' },
  { id: '00672e', requester: 'Digital-Asset-2', votes: '13', status: 'accepted', desc: 'Cypherock Wallet' },
  { id: '006f55', requester: 'Digital-Asset-2', votes: '13', status: 'accepted', desc: 'Console Wallet' },
  { id: '00acec', requester: 'Global-Synchronizer-Foundation', votes: '1 3 9', status: 'rejected', desc: 'Excellar (rejected)' },
  { id: '0047fc', requester: 'Digital-Asset-2', votes: '9 4', status: 'accepted', desc: 'ByBit corrected' },
  { id: '009d75', requester: 'Digital-Asset-2', votes: '8 5', status: 'rejected', desc: 'ByBit (rejected)' },
  { id: '000e30', requester: 'Digital-Asset-2', votes: '11 1 1', status: 'accepted', desc: 'MEXC' },
  { id: '003c9d', requester: 'Digital-Asset-2', votes: '5 8', status: 'rejected', desc: 'Kraken (rejected)' },
  { id: '007ec7', requester: 'Digital-Asset-2', votes: '11 1 1', status: 'accepted', desc: 'Kraken corrected' },
  { id: '00263c', requester: 'Digital-Asset-2', votes: '12 1', status: 'accepted', desc: 'Binance' },
  { id: '0004ab', requester: 'Digital-Asset-2', votes: '13', status: 'accepted', desc: 'KuCoin' },
  { id: '00bc62', requester: 'Digital-Asset-2', votes: '1 3 9', status: 'rejected', desc: 'KuCoin (rejected)' },
  { id: '00a9d7', requester: 'Digital-Asset-2', votes: '13', status: 'accepted', desc: 'Can Track' },
  { id: '007d90', requester: 'Global-Synchronizer-Foundation', votes: '13', status: 'accepted', desc: 'CIP-0067 fixed' },
  { id: '000b3f', requester: 'Global-Synchronizer-Foundation', votes: '3 1 9', status: 'rejected', desc: 'CIP-0067 (rejected)' },
  { id: '00c4a6', requester: 'C7-Technology-Services-Limited', votes: '13', status: 'accepted', desc: 'Texture Capital' },
  { id: '006ee1', requester: 'Global-Synchronizer-Foundation', votes: '13', status: 'accepted', desc: 'IntellectEU CIP-0058' },
  { id: '0039af', requester: 'Digital-Asset-2', votes: '13', status: 'accepted', desc: 'SV timeout bump' },
  
  // Page 5
  { id: '00523e', requester: 'Digital-Asset-2', votes: '13', status: 'accepted', desc: 'Profitr' },
  { id: '00916d', requester: 'Digital-Asset-2', votes: '13', status: 'accepted', desc: 'CB Wallet' },
  { id: '007bf8', requester: 'Global-Synchronizer-Foundation', votes: '13', status: 'accepted', desc: 'Pilot by Macao Mining' },
  { id: '002726', requester: 'Global-Synchronizer-Foundation', votes: '13', status: 'accepted', desc: 'CIP-0067 allocation' },
  { id: '007d16', requester: 'Digital-Asset-2', votes: '13', status: 'accepted', desc: 'DA-1 weight restore' },
  { id: '00c60a', requester: 'Digital-Asset-2', votes: '13', status: 'accepted', desc: 'DA-2 weight restore' },
  { id: '008a0d', requester: 'Global-Synchronizer-Foundation', votes: '1 3 9', status: 'rejected', desc: 'CIP-0067 (rejected)' },
  { id: '0017eb', requester: 'Global-Synchronizer-Foundation', votes: '13', status: 'accepted', desc: 'Talos SV' },
  { id: '002450', requester: 'Five-North-1', votes: '13', status: 'accepted', desc: '5N ID' },
  { id: '008ed4', requester: 'MPC-Holding-Inc', votes: '13', status: 'accepted', desc: 'Canton Tax Planner' },
  { id: '00095f', requester: 'MPC-Holding-Inc', votes: '13', status: 'accepted', desc: 'Orphil LLC' },
  { id: '00370e', requester: 'Global-Synchronizer-Foundation', votes: '13', status: 'accepted', desc: 'ALL DEFI' },
  { id: '00610c', requester: 'Digital-Asset-2', votes: '13', status: 'accepted', desc: 'ByBit preapproved' },
  { id: '007395', requester: 'Five-North-1', votes: '13', status: 'accepted', desc: 'Brikly CNCB' },
  { id: '00e58f', requester: 'Digital-Asset-2', votes: '11 2', status: 'accepted', desc: 'Daml 0.4.18 CIP-79' },
  { id: '0053c1', requester: 'Global-Synchronizer-Foundation', votes: '13', status: 'accepted', desc: 'GSF weight +11' },
  { id: '005bfa', requester: 'Digital-Asset-2', votes: '12 1', status: 'accepted', desc: 'Canton Monitor' },
  { id: '001e88', requester: 'Digital-Asset-2', votes: '13', status: 'accepted', desc: 'Transfer fees zero CIP-78' },
  { id: '00c3f6', requester: 'Digital-Asset-2', votes: '12 1', status: 'accepted', desc: 'BitSafe CBTC' },
  { id: '00c941', requester: 'Digital-Asset-2', votes: '13', status: 'accepted', desc: 'USDC Bridge' },
  
  // Page 6
  { id: '00c088', requester: 'MPC-Holding-Inc', votes: '13', status: 'accepted', desc: 'MPCH new PartyID' },
  { id: '009f77', requester: 'Global-Synchronizer-Foundation', votes: '13', status: 'accepted', desc: 'AngelHack CIP-0053' },
  { id: '00a54f', requester: 'Global-Synchronizer-Foundation', votes: '13', status: 'accepted', desc: 'Kaiko CIP-0063' },
  { id: '00d56d', requester: 'Five-North-1', votes: '13', status: 'accepted', desc: 'CantonOps' },
  { id: '00ed49', requester: 'Digital-Asset-2', votes: '13', status: 'accepted', desc: 'Daml 0.4.17 CIP-78' },
  { id: '002a54', requester: 'Digital-Asset-2', votes: '13', status: 'accepted', desc: 'DA-1 offboard' },
  { id: '0002ef', requester: 'Global-Synchronizer-Foundation', votes: '11 2', status: 'accepted', desc: 'GSF weight +10' },
  { id: '00a1d1', requester: 'Global-Synchronizer-Foundation', votes: '13', status: 'accepted', desc: 'Obligate' },
  { id: '001dad', requester: 'Global-Synchronizer-Foundation', votes: '13', status: 'accepted', desc: 'USDM1' },
  { id: '008d1a', requester: 'Digital-Asset-2', votes: '13', status: 'accepted', desc: 'Canton Explorer' },
  { id: '0012c0', requester: 'Digital-Asset-2', votes: '13', status: 'accepted', desc: 'DA-1 to DA-2 shift' },
  { id: '004dff', requester: 'Digital-Asset-2', votes: '13', status: 'accepted', desc: 'DA-1 to DA-2 shift 2' },
  { id: '003a08', requester: 'Global-Synchronizer-Foundation', votes: '13', status: 'accepted', desc: 'Noves Data App' },
  { id: '001e46', requester: 'Digital-Asset-2', votes: '13', status: 'accepted', desc: 'Cygnet by IPBlock' },
  { id: '001122', requester: 'Global-Synchronizer-Foundation', votes: '9 4', status: 'accepted', desc: 'GSF weight +26.5' },
  { id: '006600', requester: 'Global-Synchronizer-Foundation', votes: '4 9', status: 'rejected', desc: 'GSF weight +31.5 (rejected)' },
  { id: '00d393', requester: 'Digital-Asset-2', votes: '13', status: 'accepted', desc: 'T-RIZE Group' },
  { id: '00ac3b', requester: 'Five-North-1', votes: '12 1', status: 'accepted', desc: 'Send' },
  { id: '00cc52', requester: 'Proof-Group-1', votes: '12 1', status: 'accepted', desc: 'Canton Nodes' },
  { id: '002a2b', requester: 'Digital-Asset-2', votes: '12 1', status: 'accepted', desc: 'Trakx' },
];

async function fetchJson(url) {
  try {
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }
    return await response.json();
  } catch (error) {
    console.error(`❌ Failed to fetch ${url}:`, error.message);
    return null;
  }
}

function parseVotes(voteStr) {
  const parts = voteStr.split(' ').map(Number);
  return {
    accept: parts[0] || 0,
    reject: parts[1] || 0,
    abstain: parts[2] || 0,
  };
}

async function main() {
  console.log('═══════════════════════════════════════════════════════════════');
  console.log('     VOTE REQUESTS DIAGNOSTIC TOOL');
  console.log('═══════════════════════════════════════════════════════════════');
  console.log(`API Base: ${BASE_URL}`);
  console.log(`Explorer proposals to check: ${EXPLORER_PROPOSALS.length}`);
  console.log('');

  // Step 1: Fetch vote_requests from local API
  console.log('📊 Step 1: Fetching vote_requests from DuckDB API...');
  const voteRequestsResponse = await fetchJson(`${BASE_URL}/api/events/vote-requests?status=all&limit=5000`);
  
  if (!voteRequestsResponse) {
    console.error('❌ Could not fetch vote requests. Is the server running?');
    process.exit(1);
  }

  const voteRequests = voteRequestsResponse.data || [];
  console.log(`   Found ${voteRequests.length} vote requests in index`);
  console.log(`   Source: ${voteRequestsResponse.source}`);
  if (voteRequestsResponse._summary) {
    console.log(`   Summary: active=${voteRequestsResponse._summary.activeCount}, historical=${voteRequestsResponse._summary.historicalCount}`);
  }
  console.log('');

  // Step 2: Build lookup maps
  console.log('📊 Step 2: Building lookup maps...');
  
  // Map by contract_id prefix (first 6 chars)
  const byIdPrefix = new Map();
  // Map by stable_id prefix
  const byStablePrefix = new Map();
  // Map by proposal_id prefix
  const byProposalPrefix = new Map();
  
  for (const vr of voteRequests) {
    if (vr.contract_id) {
      const prefix = vr.contract_id.slice(0, 6).toLowerCase();
      if (!byIdPrefix.has(prefix)) byIdPrefix.set(prefix, []);
      byIdPrefix.get(prefix).push(vr);
    }
    if (vr.stable_id) {
      const prefix = vr.stable_id.slice(0, 6).toLowerCase();
      if (!byStablePrefix.has(prefix)) byStablePrefix.set(prefix, []);
      byStablePrefix.get(prefix).push(vr);
    }
    if (vr.proposal_id) {
      const prefix = vr.proposal_id.slice(0, 6).toLowerCase();
      if (!byProposalPrefix.has(prefix)) byProposalPrefix.set(prefix, []);
      byProposalPrefix.get(prefix).push(vr);
    }
  }
  
  console.log(`   Contract ID prefixes: ${byIdPrefix.size}`);
  console.log(`   Stable ID prefixes: ${byStablePrefix.size}`);
  console.log(`   Proposal ID prefixes: ${byProposalPrefix.size}`);
  console.log('');

  // Step 3: Compare with Explorer data
  console.log('📊 Step 3: Comparing with Explorer data...');
  console.log('');
  
  const found = [];
  const missing = [];
  const statusMismatch = [];
  
  for (const ep of EXPLORER_PROPOSALS) {
    const prefix = ep.id.toLowerCase();
    
    // Try to find in any of our maps
    let matches = byIdPrefix.get(prefix) || byStablePrefix.get(prefix) || byProposalPrefix.get(prefix) || [];
    
    if (matches.length === 0) {
      missing.push(ep);
    } else {
      const match = matches[0]; // Take first match
      found.push({ explorer: ep, indexed: match });
      
      // Check status match
      const explorerStatus = ep.status;
      const indexedStatus = match.status?.toLowerCase();
      if (explorerStatus !== indexedStatus && 
          !(explorerStatus === 'accepted' && indexedStatus === 'accepted') &&
          !(explorerStatus === 'rejected' && indexedStatus === 'rejected')) {
        statusMismatch.push({ explorer: ep, indexed: match });
      }
    }
  }
  
  // Step 4: Report Results
  console.log('═══════════════════════════════════════════════════════════════');
  console.log('     RESULTS');
  console.log('═══════════════════════════════════════════════════════════════');
  console.log('');
  console.log(`✅ Found in index:     ${found.length}/${EXPLORER_PROPOSALS.length}`);
  console.log(`❌ Missing from index: ${missing.length}/${EXPLORER_PROPOSALS.length}`);
  console.log(`⚠️  Status mismatches: ${statusMismatch.length}`);
  console.log('');
  
  if (missing.length > 0) {
    console.log('───────────────────────────────────────────────────────────────');
    console.log('MISSING PROPOSALS (not found in vote_requests index):');
    console.log('───────────────────────────────────────────────────────────────');
    for (const m of missing) {
      const votes = parseVotes(m.votes);
      console.log(`  ${m.id}... | ${m.status.padEnd(8)} | ${m.votes.padEnd(6)} | ${m.requester.slice(0, 20).padEnd(20)} | ${m.desc.slice(0, 40)}`);
    }
    console.log('');
  }
  
  if (statusMismatch.length > 0) {
    console.log('───────────────────────────────────────────────────────────────');
    console.log('STATUS MISMATCHES:');
    console.log('───────────────────────────────────────────────────────────────');
    for (const m of statusMismatch) {
      console.log(`  ${m.explorer.id}... | Explorer: ${m.explorer.status} | Index: ${m.indexed.status}`);
    }
    console.log('');
  }
  
  // Step 5: Index stats
  console.log('───────────────────────────────────────────────────────────────');
  console.log('INDEX STATISTICS:');
  console.log('───────────────────────────────────────────────────────────────');
  
  const statusCounts = {};
  const isHumanCounts = { true: 0, false: 0 };
  const withReason = { yes: 0, no: 0 };
  const withVotes = { yes: 0, no: 0 };
  
  for (const vr of voteRequests) {
    const status = vr.status || 'unknown';
    statusCounts[status] = (statusCounts[status] || 0) + 1;
    
    if (vr.is_human === true) isHumanCounts.true++;
    else isHumanCounts.false++;
    
    if (vr.reason && vr.reason.trim()) withReason.yes++;
    else withReason.no++;
    
    if (vr.vote_count > 0) withVotes.yes++;
    else withVotes.no++;
  }
  
  console.log(`  Status breakdown:`);
  for (const [status, count] of Object.entries(statusCounts).sort((a, b) => b[1] - a[1])) {
    console.log(`    ${status}: ${count}`);
  }
  console.log('');
  console.log(`  is_human: true=${isHumanCounts.true}, false=${isHumanCounts.false}`);
  console.log(`  has_reason: yes=${withReason.yes}, no=${withReason.no}`);
  console.log(`  has_votes: yes=${withVotes.yes}, no=${withVotes.no}`);
  console.log('');
  
  // Step 6: Recommendations
  console.log('═══════════════════════════════════════════════════════════════');
  console.log('     RECOMMENDATIONS');
  console.log('═══════════════════════════════════════════════════════════════');
  
  if (missing.length > 0) {
    console.log('');
    console.log('1. MISSING PROPOSALS:');
    console.log('   The vote-request indexer is not finding these proposals.');
    console.log('   Possible causes:');
    console.log('   - Binary files containing these events were not scanned');
    console.log('   - Template index is incomplete or stale');
    console.log('   - is_human filter is excluding valid proposals');
    console.log('');
    console.log('   Fix: Re-run the indexer with full scan:');
    console.log('   curl -X POST "http://localhost:3001/api/events/vote-requests/build-index?force=true"');
  }
  
  const matchRate = ((found.length / EXPLORER_PROPOSALS.length) * 100).toFixed(1);
  console.log('');
  console.log(`📈 Match rate: ${matchRate}%`);
  console.log('');
  
  // Exit with status based on completeness
  if (missing.length > 10) {
    console.log('⚠️  Significant gaps detected. Index may need rebuilding.');
    process.exit(2);
  } else if (missing.length > 0) {
    console.log('⚠️  Some proposals missing. Review the list above.');
    process.exit(1);
  } else {
    console.log('✅ All Explorer proposals found in index!');
    process.exit(0);
  }
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
