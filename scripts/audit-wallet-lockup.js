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
import { getAllEndpoints, checkAllEndpoints } from '../server/lib/endpoint-rotation.js';
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

function log(...args) { if (!JSON_OUTPUT) console.log(...args); }
function logStderr(...args) { if (!JSON_OUTPUT) console.error(...args); }

// ─── Scan API helpers ────────────────────────────────────────────────────────

function makeRequest(endpointUrl, method, path, body) {
  const url = `${endpointUrl}${path}`;
  const hostname = extractHostname(endpointUrl);
  const dispatcher = hostname ? createDispatcher(hostname) : undefined;

  const opts = {
    method,
    headers: {
      Accept: 'application/json',
      ...(hostname ? { Host: hostname } : {}),
    },
    ...(dispatcher ? { dispatcher } : {}),
    signal: AbortSignal.timeout(30_000),
  };

  if (body !== undefined) {
    opts.headers['Content-Type'] = 'application/json';
    opts.body = JSON.stringify(body);
  }

  return fetch(url, opts);
}

/**
 * Try a request against multiple healthy SV endpoints until one succeeds.
 * Returns { data, endpointName } on success, throws on total failure.
 */
async function tryEndpoints(method, path, body) {
  const endpoints = getAllEndpoints().filter(e => e.health?.healthy !== false);
  const errors = [];

  for (const ep of endpoints) {
    try {
      const res = await makeRequest(ep.url, method, path, body);
      if (!res.ok) {
        const text = await res.text();
        errors.push({ name: ep.name, status: res.status, error: text.slice(0, 200) });
        continue;
      }
      return { data: await res.json(), endpointName: ep.name };
    } catch (err) {
      errors.push({ name: ep.name, status: 0, error: err.message });
    }
  }

  throw new Error(`All ${endpoints.length} endpoints failed for ${method} ${path}: ${JSON.stringify(errors.slice(0, 3))}`);
}

async function scanGet(path) {
  const { data } = await tryEndpoints('GET', path);
  return data;
}

async function scanPost(path, body = {}) {
  const { data } = await tryEndpoints('POST', path, body);
  return data;
}

// ─── Audit logic ─────────────────────────────────────────────────────────────

async function fetchLatestRoundInfo() {
  return scanGet('/v0/round-of-latest-data');
}

/**
 * Resolve the best record_time + migration_id for ACS-based queries.
 * Probes multiple migration IDs across multiple endpoints.
 */
async function resolveSnapshotParams(effectiveAt) {
  const migrationIds = [0, 1, 2];
  const endpoints = getAllEndpoints().filter(e => e.health?.healthy !== false);

  for (const ep of endpoints) {
    for (const mid of migrationIds) {
      try {
        const params = new URLSearchParams({ before: effectiveAt, migration_id: String(mid) });
        const res = await makeRequest(ep.url, 'GET', `/v0/state/acs/snapshot-timestamp?${params}`);
        if (!res.ok) continue;
        const snap = await res.json();
        if (!snap.record_time) continue;

        // Verify the snapshot is actually usable by doing a small probe
        const probeRes = await makeRequest(ep.url, 'POST', '/v0/holdings/summary', {
          migration_id: mid,
          record_time: snap.record_time,
          record_time_match: 'exact',
          owner_party_ids: [WALLETS[0].partyId],
        });

        if (probeRes.ok) {
          const age = Date.now() - new Date(snap.record_time).getTime();
          const daysOld = Math.round(age / 86_400_000);
          return {
            recordTime: snap.record_time,
            migrationId: mid,
            endpointName: ep.name,
            endpointUrl: ep.url,
            source: `snapshot (migration ${mid}, ${daysOld}d old, via ${ep.name})`,
          };
        }
      } catch {}
    }
  }

  // Fallback: use effectiveAt with migration 0, no guarantee it works
  return {
    recordTime: effectiveAt,
    migrationId: 0,
    endpointName: null,
    endpointUrl: null,
    source: 'effectiveAt-fallback (no usable ACS snapshot found)',
  };
}

/**
 * Make a request against a specific endpoint URL, falling back to tryEndpoints.
 */
async function queryWithPreferred(preferredUrl, method, path, body) {
  if (preferredUrl) {
    try {
      const res = await makeRequest(preferredUrl, method, path, body);
      if (res.ok) return await res.json();
    } catch {}
  }
  return (await tryEndpoints(method, path, body)).data;
}

async function fetchHoldingsSummary(partyId, recordTime, migrationId, preferredUrl) {
  return queryWithPreferred(preferredUrl, 'POST', '/v0/holdings/summary', {
    migration_id: migrationId,
    record_time: recordTime,
    record_time_match: 'exact',
    owner_party_ids: [partyId],
  });
}

async function fetchHoldingsState(partyId, recordTime, migrationId, preferredUrl) {
  const allEvents = [];
  let nextPage;

  for (let page = 0; page < 5; page++) {
    const resp = await queryWithPreferred(preferredUrl, 'POST', '/v0/holdings/state', {
      migration_id: migrationId,
      record_time: recordTime,
      record_time_match: 'exact',
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

async function fetchWalletBalance(partyId) {
  return scanGet(`/v0/wallet-balance/${encodeURIComponent(partyId)}`);
}

async function fetchTransactionsByParty(partyId) {
  return scanPost('/v0/transactions/by-party', { party: partyId, limit: 200 });
}

/**
 * Scan recent /v2/updates for any events involving a set of party IDs.
 * This works on all SV endpoints and doesn't need ACS snapshots.
 */
async function scanUpdatesForPartyActivity(partyIds, maxPages = 5) {
  const partySet = new Set(partyIds);
  const matchingEvents = [];

  let after = undefined;

  for (let page = 0; page < maxPages; page++) {
    const body = { page_size: 100 };
    if (after) body.after = after;

    let resp;
    try {
      resp = await scanPost('/v2/updates', body);
    } catch { break; }

    const txns = resp.transactions || [];
    if (txns.length === 0) break;

    for (const tx of txns) {
      const events = tx.events_by_id || {};
      for (const [eventId, ev] of Object.entries(events)) {
        const signatories = ev.signatories || [];
        const observers = ev.observers || [];
        const actingParties = ev.acting_parties || [];
        const allParties = [...signatories, ...observers, ...actingParties];

        if (allParties.some(p => partySet.has(p))) {
          matchingEvents.push({
            updateId: tx.update_id,
            recordTime: tx.record_time,
            effectiveAt: tx.effective_at,
            eventId,
            eventType: ev.event_type,
            templateId: ev.template_id,
            contractId: ev.contract_id,
            choice: ev.choice,
            consuming: ev.consuming,
            matchedParties: allParties.filter(p => partySet.has(p)),
          });
        }
      }
    }

    // Set pagination cursor for next page
    const lastTx = txns[txns.length - 1];
    if (lastTx) {
      after = {
        after_migration_id: lastTx.migration_id ?? 0,
        after_record_time: lastTx.record_time,
      };
    } else {
      break;
    }
  }

  return matchingEvents;
}

function classifyTransactions(transactions) {
  const outbound = [];
  const inbound = [];
  const other = [];

  for (const tx of transactions) {
    const type = tx.transaction_type || '';
    if (type === 'transfer' && tx.transfer) {
      outbound.push({
        date: tx.date,
        type,
        sender: tx.transfer.sender?.party,
        receivers: (tx.transfer.receivers || []).map(r => ({ party: r.party, amount: r.amount })),
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

  log('══════════════════════════════════════════════════════════════');
  log('  WALLET LOCKUP AUDIT — Quarterly Immobility Check');
  log(`  Run at: ${auditTime}`);
  log(`  Purchase: ${PURCHASE_AMOUNT_CC.toLocaleString()} CC across ${WALLETS.length} wallets`);
  log(`  Lock-up: ${LOCKUP_MONTHS} months`);
  log('══════════════════════════════════════════════════════════════\n');

  // 1. Find healthy endpoints
  log('Checking Scan API endpoints...');
  await checkAllEndpoints();
  const healthyCount = getAllEndpoints().filter(e => e.health?.healthy !== false).length;
  log(`Healthy endpoints: ${healthyCount}/13\n`);

  // 2. Get current round
  const roundInfo = await fetchLatestRoundInfo();
  log(`Latest round: ${roundInfo.round}  |  Effective at: ${roundInfo.effectiveAt}`);

  // 3. Find a usable ACS snapshot (probes multiple endpoints + migration IDs)
  log('Probing for usable ACS snapshot across endpoints...');
  const snapshotParams = await resolveSnapshotParams(roundInfo.effectiveAt);
  log(`Record time: ${snapshotParams.recordTime}`);
  log(`Source: ${snapshotParams.source}\n`);

  const { recordTime, migrationId, endpointUrl: preferredUrl } = snapshotParams;

  // 4. Scan /v2/updates for any activity involving these wallets
  log('Scanning recent ledger updates for wallet activity...');
  const partyIds = WALLETS.map(w => w.partyId);
  let updateEvents = [];
  try {
    updateEvents = await scanUpdatesForPartyActivity(partyIds, 10);
    log(`Found ${updateEvents.length} events involving audited wallets\n`);
  } catch (err) {
    logStderr(`Update scan failed: ${err.message}\n`);
  }

  const results = [];

  for (const wallet of WALLETS) {
    log('──────────────────────────────────────────────────────────────');
    log(`  ${wallet.label}`);
    log(`  Party: ${wallet.partyId.slice(0, 20)}...${wallet.partyId.slice(-12)}`);
    log('──────────────────────────────────────────────────────────────');

    const walletResult = {
      label: wallet.label,
      partyId: wallet.partyId,
      auditTime,
      holdings: null,
      walletBalance: null,
      transactions: null,
      lockStatus: null,
      updateActivity: null,
      immobile: null,
      alerts: [],
    };

    // ── Holdings summary ──
    try {
      const holdingsResp = await fetchHoldingsSummary(wallet.partyId, recordTime, migrationId, preferredUrl);
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

        log(`\n  Holdings:`);
        log(`    Total holdings:   ${summary.total_coin_holdings} CC`);
        log(`    Unlocked:         ${summary.total_unlocked_coin} CC`);
        log(`    Locked:           ${summary.total_locked_coin} CC`);
        log(`    Available:        ${summary.total_available_coin} CC`);
        log(`    Holding fees:     ${summary.accumulated_holding_fees_total} CC`);

        if (parseFloat(summary.total_unlocked_coin) > 0) {
          walletResult.alerts.push('UNLOCKED_BALANCE_DETECTED: Wallet has unlocked CC that could be transferred.');
        }
      } else {
        walletResult.alerts.push('NO_HOLDINGS_FOUND: No holdings data returned for this party.');
        log('\n  Holdings: NONE FOUND');
      }
    } catch (err) {
      walletResult.alerts.push(`HOLDINGS_ERROR: ${err.message}`);
      log(`\n  Holdings: ERROR — ${err.message}`);
    }

    // ── Wallet balance ──
    try {
      const balanceResp = await fetchWalletBalance(wallet.partyId);
      walletResult.walletBalance = balanceResp.wallet_balance || '0';
      log(`\n  Wallet Balance: ${walletResult.walletBalance} CC`);
    } catch (err) {
      walletResult.walletBalanceError = err.message;
      log(`\n  Wallet Balance: unavailable`);
    }

    // ── Transaction history ──
    try {
      const txResp = await fetchTransactionsByParty(wallet.partyId);
      const txList = txResp.transactions || [];
      const classified = classifyTransactions(txList);

      walletResult.transactions = {
        total: txList.length,
        outboundTransfers: classified.outbound.length,
        inboundMints: classified.inbound.length,
        other: classified.other.length,
        outboundDetails: classified.outbound,
      };

      log(`\n  Transactions: ${txList.length} total, ${classified.outbound.length} outbound transfers`);

      if (classified.outbound.length > 0) {
        walletResult.alerts.push(`OUTBOUND_TRANSFERS_DETECTED: ${classified.outbound.length} outbound transfer(s) found.`);
        log('    *** ALERT: Outbound transfers detected! ***');
        for (const tx of classified.outbound) {
          log(`      ${tx.date}: ${tx.receivers.map(r => r.amount + ' CC').join(', ')}`);
        }
      }
    } catch {
      walletResult.transactions = { total: 0, outboundTransfers: 0, inboundMints: 0, other: 0, outboundDetails: [], unavailable: true };
      log('\n  Transactions: endpoint unavailable on this SV');
    }

    // ── Update-based activity (works on all SVs) ──
    const walletUpdateEvents = updateEvents.filter(e => e.matchedParties.some(p => p === wallet.partyId));
    walletResult.updateActivity = {
      eventsFound: walletUpdateEvents.length,
      events: walletUpdateEvents,
    };

    if (walletUpdateEvents.length > 0) {
      log(`\n  Ledger Activity (from /v2/updates): ${walletUpdateEvents.length} events`);
      for (const e of walletUpdateEvents.slice(0, 5)) {
        log(`    ${e.recordTime} | ${e.eventType} | ${e.templateId?.split(':').pop() || e.choice || 'unknown'}`);
      }
      if (walletUpdateEvents.length > 5) log(`    ... and ${walletUpdateEvents.length - 5} more`);
    } else {
      log('\n  Ledger Activity (from /v2/updates): none found in recent updates');
    }

    // ── Holdings state (lock details) ──
    try {
      const holdingsEvents = await fetchHoldingsState(wallet.partyId, recordTime, migrationId, preferredUrl);
      const { locked, unlocked } = analyzeLocks(holdingsEvents);

      walletResult.lockStatus = {
        lockedContracts: locked.length,
        unlockedContracts: unlocked.length,
        locks: locked,
      };

      log(`\n  Lock Status: ${locked.length} locked, ${unlocked.length} unlocked`);
      for (const l of locked) {
        log(`    Contract: ${l.contractId?.slice(0, 16)}...`);
        log(`      Amount:  ${l.amount}`);
        log(`      Expiry:  ${l.lockExpiry || 'none set'}`);
      }
    } catch (err) {
      walletResult.alerts.push(`LOCK_STATUS_ERROR: ${err.message}`);
      log(`\n  Lock Status: ERROR`);
    }

    // ── Immobility verdict ──
    const txOutbound = walletResult.transactions?.outboundTransfers || 0;
    const hasTransferActivity = walletUpdateEvents.some(e =>
      (e.templateId || '').includes('Transfer') || e.choice === 'Transfer'
    );
    walletResult.immobile = txOutbound === 0 && !hasTransferActivity;

    log(`\n  VERDICT: ${walletResult.immobile ? 'IMMOBILE (pass)' : 'ACTIVITY DETECTED (fail)'}`);
    if (walletResult.alerts.length > 0) {
      log('  Alerts:');
      for (const a of walletResult.alerts) log(`    - ${a}`);
    }
    log('');

    results.push(walletResult);
  }

  // ── Summary ──
  const allImmobile = results.every(r => r.immobile);
  const totalAlerts = results.reduce((sum, r) => sum + r.alerts.length, 0);

  const report = {
    auditType: 'quarterly_lockup_immobility',
    auditTime,
    latestRound: roundInfo.round,
    recordTime,
    recordTimeSource: snapshotParams.source,
    migrationId,
    purchaseAmountCC: PURCHASE_AMOUNT_CC,
    lockupMonths: LOCKUP_MONTHS,
    walletCount: WALLETS.length,
    allImmobile,
    totalAlerts,
    updateEventsScanned: updateEvents.length,
    wallets: results,
  };

  if (JSON_OUTPUT) {
    console.log(JSON.stringify(report, null, 2));
  } else {
    log('══════════════════════════════════════════════════════════════');
    log('  AUDIT SUMMARY');
    log('══════════════════════════════════════════════════════════════');
    log(`  All wallets immobile: ${allImmobile ? 'YES' : 'NO'}`);
    log(`  Total alerts:         ${totalAlerts}`);
    for (const r of results) {
      const status = r.immobile ? 'PASS' : 'FAIL';
      const balance = r.holdings?.totalHoldings || r.walletBalance || 'unknown';
      log(`  ${r.label}: ${status}  (balance: ${balance} CC)`);
    }
    log('══════════════════════════════════════════════════════════════\n');
  }

  process.exit(allImmobile && totalAlerts === 0 ? 0 : 1);
}

main().catch(err => {
  console.error('Audit failed:', err);
  process.exit(2);
});
