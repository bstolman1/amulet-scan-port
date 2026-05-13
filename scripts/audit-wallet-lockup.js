#!/usr/bin/env node

/**
 * Wallet Lockup Audit — Quarterly Immobility Check
 *
 * Verifies that designated wallets holding a locked CC purchase have
 * remained inactive (no outbound transfers) during the lock-up period.
 *
 * Queries the Canton Scan API for:
 *   1. Current holdings summary (unlocked / locked / total balance)
 *   2. Transaction history (detects any transfers since deposit)
 *   3. Holdings state detail (lock expiry, contract-level data)
 *
 * Usage:
 *   node scripts/audit-wallet-lockup.js
 *   node scripts/audit-wallet-lockup.js --json          # machine-readable output
 *   node scripts/audit-wallet-lockup.js --verbose       # include raw API responses
 */

import '../server/env.js';
import { getCurrentEndpoint, checkAllEndpoints } from '../server/lib/endpoint-rotation.js';
import { extractHostname, createDispatcher } from '../server/lib/undici-dispatcher.js';

// ─── Configuration ───────────────────────────────────────────────────────────

const WALLETS = [
  {
    label: 'Brevan Howard Wallet 1',
    partyId: '23d169c2-0909-4c70-81d1-1922de6febaa::1220b770cd6350fe69e14bb55a42588237a15747a22392faa3fa8fe60cd83843585f',
  },
  {
    label: 'Brevan Howard Wallet 2',
    partyId: '23d169c2-0909-4c70-81d1-1922de6febaa::1220e226ff1393b8a1e4954f41ccb55b9cc71b85aee2d6f6c30e4b553803e635ed22',
  },
  {
    label: 'Brevan Howard Wallet 3',
    partyId: '23d169c2-0909-4c70-81d1-1922de6febaa::12206c4c9c59523446ba3057497faeef75e589f6120e5f38121a23ad3632386a49c7',
  },
];

const PURCHASE_AMOUNT_CC = 10_000_000; // 10M CC total across 3 wallets
const LOCKUP_MONTHS = 12;

// ─── CLI flags ───────────────────────────────────────────────────────────────

const JSON_OUTPUT = process.argv.includes('--json');
const VERBOSE = process.argv.includes('--verbose');

// ─── Scan API helpers ────────────────────────────────────────────────────────

async function scanPost(path, body = {}) {
  const endpoint = getCurrentEndpoint();
  const url = `${endpoint.url}${path}`;
  const hostname = extractHostname(endpoint.url);
  const dispatcher = hostname ? createDispatcher(hostname) : undefined;

  const res = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Accept: 'application/json',
      ...(hostname ? { Host: hostname } : {}),
    },
    body: JSON.stringify(body),
    ...(dispatcher ? { dispatcher } : {}),
    signal: AbortSignal.timeout(30_000),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`POST ${path} failed (${res.status}): ${text.slice(0, 300)}`);
  }
  return res.json();
}

async function scanGet(path) {
  const endpoint = getCurrentEndpoint();
  const url = `${endpoint.url}${path}`;
  const hostname = extractHostname(endpoint.url);
  const dispatcher = hostname ? createDispatcher(hostname) : undefined;

  const res = await fetch(url, {
    method: 'GET',
    headers: {
      Accept: 'application/json',
      ...(hostname ? { Host: hostname } : {}),
    },
    ...(dispatcher ? { dispatcher } : {}),
    signal: AbortSignal.timeout(30_000),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`GET ${path} failed (${res.status}): ${text.slice(0, 300)}`);
  }
  return res.json();
}

// ─── Audit logic ─────────────────────────────────────────────────────────────

async function fetchLatestRoundInfo() {
  return scanGet('/v0/round-of-latest-data');
}

async function fetchHoldingsSummary(partyId, recordTime) {
  return scanPost('/v0/holdings/summary', {
    migration_id: 0,
    record_time: recordTime,
    record_time_match: 'before',
    owner_party_ids: [partyId],
  });
}

async function fetchTransactionsByParty(partyId, limit = 100) {
  return scanPost('/v0/transactions/by-party', { party: partyId, limit });
}

async function fetchHoldingsState(partyId, recordTime) {
  const allEvents = [];
  let nextPage;

  for (let page = 0; page < 5; page++) {
    const resp = await scanPost('/v0/holdings/state', {
      migration_id: 0,
      record_time: recordTime,
      record_time_match: 'before',
      page_size: 500,
      owner_party_ids: [partyId],
      ...(nextPage !== undefined ? { after: nextPage } : {}),
    });
    allEvents.push(...(resp.created_events || []));
    if (!resp.next_page_token) break;
    nextPage = resp.next_page_token;
  }

  return allEvents;
}

function classifyTransactions(transactions) {
  const outbound = [];
  const inbound = [];
  const other = [];

  for (const tx of transactions) {
    const type = tx.transaction_type || '';
    if (type === 'transfer' && tx.transfer) {
      const senderParty = tx.transfer.sender?.party;
      const receivers = tx.transfer.receivers || [];
      outbound.push({
        date: tx.date,
        type,
        sender: senderParty,
        receivers: receivers.map(r => ({ party: r.party, amount: r.amount })),
        balanceChanges: tx.transfer.balance_changes,
      });
    } else if (type === 'mint' || type === 'tap') {
      inbound.push({ date: tx.date, type, amount: tx.mint?.amulet_amount || tx.tap?.amulet_amount });
    } else {
      other.push({ date: tx.date, type });
    }
  }

  return { outbound, inbound, other };
}

function analyzeLocks(holdingsEvents) {
  const locked = [];
  const unlocked = [];

  for (const ev of holdingsEvents) {
    const payload = ev.create_arguments || {};
    const templateId = ev.template_id || '';
    const isLocked = templateId.includes('LockedAmulet') || !!payload.lock;
    const amount = payload.amount?.initialAmount || '0';

    const entry = {
      contractId: ev.contract_id,
      templateId,
      amount,
      createdAt: ev.created_at,
    };

    if (isLocked) {
      entry.lockHolders = payload.lock?.holders || [];
      entry.lockExpiry = payload.lock?.expiresAt || null;
      locked.push(entry);
    } else {
      unlocked.push(entry);
    }
  }

  return { locked, unlocked };
}

// ─── Main ────────────────────────────────────────────────────────────────────

async function main() {
  const auditTime = new Date().toISOString();

  if (!JSON_OUTPUT) {
    console.log('══════════════════════════════════════════════════════════════');
    console.log('  WALLET LOCKUP AUDIT — Quarterly Immobility Check');
    console.log(`  Run at: ${auditTime}`);
    console.log(`  Purchase: ${PURCHASE_AMOUNT_CC.toLocaleString()} CC across ${WALLETS.length} wallets`);
    console.log(`  Lock-up: ${LOCKUP_MONTHS} months`);
    console.log('══════════════════════════════════════════════════════════════\n');
  }

  // 1. Find a healthy endpoint
  if (!JSON_OUTPUT) console.log('Checking Scan API endpoints...');
  await checkAllEndpoints();
  const endpoint = getCurrentEndpoint();
  if (!JSON_OUTPUT) console.log(`Using endpoint: ${endpoint.name}\n`);

  // 2. Get current round / timestamp
  const roundInfo = await fetchLatestRoundInfo();
  const recordTime = roundInfo.effectiveAt;
  if (!JSON_OUTPUT) console.log(`Latest round: ${roundInfo.round}  |  Record time: ${recordTime}\n`);

  const results = [];

  for (const wallet of WALLETS) {
    if (!JSON_OUTPUT) {
      console.log('──────────────────────────────────────────────────────────────');
      console.log(`  ${wallet.label}`);
      console.log(`  Party: ${wallet.partyId.slice(0, 20)}...${wallet.partyId.slice(-12)}`);
      console.log('──────────────────────────────────────────────────────────────');
    }

    const walletResult = {
      label: wallet.label,
      partyId: wallet.partyId,
      auditTime,
      holdings: null,
      transactions: null,
      lockStatus: null,
      immobile: null,
      alerts: [],
    };

    // ── Holdings summary ──
    try {
      const holdingsResp = await fetchHoldingsSummary(wallet.partyId, recordTime);
      const summary = (holdingsResp.summaries || [])[0] || null;

      if (summary) {
        walletResult.holdings = {
          totalUnlocked: summary.total_unlocked_coin,
          totalLocked: summary.total_locked_coin,
          totalHoldings: summary.total_coin_holdings,
          totalAvailable: summary.total_available_coin,
          holdingFeesUnlocked: summary.accumulated_holding_fees_unlocked,
          holdingFeesLocked: summary.accumulated_holding_fees_locked,
          computedAsOfRound: holdingsResp.computed_as_of_round,
        };

        if (!JSON_OUTPUT) {
          console.log(`\n  Holdings:`);
          console.log(`    Total holdings:   ${summary.total_coin_holdings} CC`);
          console.log(`    Unlocked:         ${summary.total_unlocked_coin} CC`);
          console.log(`    Locked:           ${summary.total_locked_coin} CC`);
          console.log(`    Available:        ${summary.total_available_coin} CC`);
          console.log(`    Holding fees:     ${summary.accumulated_holding_fees_total} CC`);
        }

        if (parseFloat(summary.total_unlocked_coin) > 0) {
          walletResult.alerts.push('UNLOCKED_BALANCE_DETECTED: Wallet has unlocked CC that could be transferred.');
        }
      } else {
        walletResult.holdings = null;
        walletResult.alerts.push('NO_HOLDINGS_FOUND: No holdings data returned for this party.');
        if (!JSON_OUTPUT) console.log('\n  Holdings: NONE FOUND');
      }

      if (VERBOSE && !JSON_OUTPUT) {
        console.log('\n  [verbose] Raw holdings response:', JSON.stringify(holdingsResp, null, 2));
      }
    } catch (err) {
      walletResult.alerts.push(`HOLDINGS_ERROR: ${err.message}`);
      if (!JSON_OUTPUT) console.log(`\n  Holdings: ERROR — ${err.message}`);
    }

    // ── Transaction history ──
    try {
      const txResp = await fetchTransactionsByParty(wallet.partyId, 200);
      const txList = txResp.transactions || [];
      const classified = classifyTransactions(txList);

      walletResult.transactions = {
        total: txList.length,
        outboundTransfers: classified.outbound.length,
        inboundMints: classified.inbound.length,
        other: classified.other.length,
        outboundDetails: classified.outbound,
      };

      if (!JSON_OUTPUT) {
        console.log(`\n  Transactions (last 200):`);
        console.log(`    Total:             ${txList.length}`);
        console.log(`    Outbound transfers: ${classified.outbound.length}`);
        console.log(`    Inbound (mint/tap): ${classified.inbound.length}`);
        console.log(`    Other:             ${classified.other.length}`);
      }

      if (classified.outbound.length > 0) {
        walletResult.alerts.push(`OUTBOUND_TRANSFERS_DETECTED: ${classified.outbound.length} outbound transfer(s) found.`);
        if (!JSON_OUTPUT) {
          console.log('\n    *** ALERT: Outbound transfers detected! ***');
          for (const tx of classified.outbound) {
            console.log(`      Date: ${tx.date}`);
            for (const r of tx.receivers) {
              console.log(`        -> ${r.party?.slice(0, 20)}... : ${r.amount} CC`);
            }
          }
        }
      }

      if (VERBOSE && !JSON_OUTPUT) {
        console.log('\n  [verbose] Raw transactions:', JSON.stringify(txList.slice(0, 5), null, 2));
      }
    } catch (err) {
      walletResult.alerts.push(`TRANSACTIONS_ERROR: ${err.message}`);
      if (!JSON_OUTPUT) console.log(`\n  Transactions: ERROR — ${err.message}`);
    }

    // ── Holdings state (lock details) ──
    try {
      const holdingsEvents = await fetchHoldingsState(wallet.partyId, recordTime);
      const { locked, unlocked } = analyzeLocks(holdingsEvents);

      walletResult.lockStatus = {
        lockedContracts: locked.length,
        unlockedContracts: unlocked.length,
        locks: locked,
      };

      if (!JSON_OUTPUT) {
        console.log(`\n  Lock Status:`);
        console.log(`    Locked contracts:   ${locked.length}`);
        console.log(`    Unlocked contracts: ${unlocked.length}`);
        for (const l of locked) {
          console.log(`    Contract: ${l.contractId?.slice(0, 16)}...`);
          console.log(`      Amount:  ${l.amount}`);
          console.log(`      Expiry:  ${l.lockExpiry || 'none set'}`);
          console.log(`      Holders: ${(l.lockHolders || []).length}`);
        }
      }

      if (VERBOSE && !JSON_OUTPUT && holdingsEvents.length > 0) {
        console.log('\n  [verbose] Raw holdings events:', JSON.stringify(holdingsEvents.slice(0, 3), null, 2));
      }
    } catch (err) {
      walletResult.alerts.push(`LOCK_STATUS_ERROR: ${err.message}`);
      if (!JSON_OUTPUT) console.log(`\n  Lock Status: ERROR — ${err.message}`);
    }

    // ── Immobility verdict ──
    const outboundCount = walletResult.transactions?.outboundTransfers || 0;
    const hasUnlocked = parseFloat(walletResult.holdings?.totalUnlocked || '0') > 0;
    walletResult.immobile = outboundCount === 0;

    if (!JSON_OUTPUT) {
      console.log(`\n  VERDICT: ${walletResult.immobile ? 'IMMOBILE (pass)' : 'ACTIVITY DETECTED (fail)'}`);
      if (walletResult.alerts.length > 0) {
        console.log(`  Alerts:`);
        for (const a of walletResult.alerts) {
          console.log(`    - ${a}`);
        }
      }
      console.log('');
    }

    results.push(walletResult);
  }

  // ── Summary ──
  const allImmobile = results.every(r => r.immobile);
  const totalAlerts = results.reduce((sum, r) => sum + r.alerts.length, 0);

  const report = {
    auditType: 'quarterly_lockup_immobility',
    auditTime,
    endpoint: endpoint.name,
    latestRound: roundInfo.round,
    recordTime,
    purchaseAmountCC: PURCHASE_AMOUNT_CC,
    lockupMonths: LOCKUP_MONTHS,
    walletCount: WALLETS.length,
    allImmobile,
    totalAlerts,
    wallets: results,
  };

  if (JSON_OUTPUT) {
    console.log(JSON.stringify(report, null, 2));
  } else {
    console.log('══════════════════════════════════════════════════════════════');
    console.log('  AUDIT SUMMARY');
    console.log('══════════════════════════════════════════════════════════════');
    console.log(`  All wallets immobile: ${allImmobile ? 'YES' : 'NO'}`);
    console.log(`  Total alerts:         ${totalAlerts}`);
    for (const r of results) {
      const status = r.immobile ? 'PASS' : 'FAIL';
      const balance = r.holdings?.totalHoldings || 'unknown';
      console.log(`  ${r.label}: ${status}  (balance: ${balance} CC)`);
    }
    console.log('══════════════════════════════════════════════════════════════\n');
  }

  process.exit(allImmobile && totalAlerts === 0 ? 0 : 1);
}

main().catch(err => {
  console.error('Audit failed:', err);
  process.exit(2);
});
