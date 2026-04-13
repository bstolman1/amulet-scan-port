#!/usr/bin/env node

/**
 * Featured Apps Report Generator (Local / Internal Use)
 *
 * Queries Canton Scan API directly to generate a text report of:
 * - All Featured Apps with cumulative CC mined
 * - FA approval dates (from on-chain vote results)
 * - Milestone tracking ($10M / $25M thresholds)
 * - CIP Locking readiness assessment
 *
 * Usage:
 *   node scripts/featured-apps-report.mjs
 *   node scripts/featured-apps-report.mjs --json          # output raw JSON
 *   node scripts/featured-apps-report.mjs --csv           # output CSV
 *   node scripts/featured-apps-report.mjs --debug         # dump sample data structures
 *   SCAN_URL=https://scan.sv-1.global.canton.network.sync.global/api/scan node scripts/featured-apps-report.mjs
 */

const SCAN_BASE =
  process.env.SCAN_URL ||
  'https://scan.sv-1.global.canton.network.sync.global/api/scan';

const THRESHOLDS = {
  LOCK_AMOUNT: 25_000_000,
  MILESTONE_10M: 10_000_000,
  MILESTONE_25M: 25_000_000,
};

// ─── helpers ────────────────────────────────────────────────────────────────

async function scanGet(path) {
  const url = `${SCAN_BASE}/${path}`;
  const res = await fetch(url, {
    method: 'GET',
    headers: { Accept: 'application/json' },
    signal: AbortSignal.timeout(30_000),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`GET ${path} → ${res.status}: ${text.slice(0, 200)}`);
  }
  return res.json();
}

async function scanPost(path, body) {
  const url = `${SCAN_BASE}/${path}`;
  const res = await fetch(url, {
    method: 'POST',
    headers: { Accept: 'application/json', 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(30_000),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`POST ${path} → ${res.status}: ${text.slice(0, 200)}`);
  }
  return res.json();
}

/**
 * Parse a Canton/DAML timestamp value into an ISO string.
 * Handles: plain strings, { microsecondsSinceEpoch }, { seconds, nanos }, numbers.
 */
function parseTimestamp(value) {
  if (!value) return null;
  if (typeof value === 'string') {
    const d = new Date(value);
    return isNaN(d) ? null : d.toISOString();
  }
  if (typeof value === 'number') {
    return new Date(value).toISOString();
  }
  if (typeof value === 'object') {
    if (value.microsecondsSinceEpoch != null) {
      const micros = Number(value.microsecondsSinceEpoch);
      if (!isNaN(micros)) return new Date(micros / 1000).toISOString();
    }
    if (value.seconds != null) {
      const s = Number(value.seconds);
      const n = value.nanos ? Number(value.nanos) : 0;
      if (!isNaN(s)) return new Date(s * 1000 + Math.floor(n / 1e6)).toISOString();
    }
    if (value.unixtime != null) {
      const s = Number(value.unixtime);
      if (!isNaN(s)) return new Date(s * 1000).toISOString();
    }
    if (typeof value.value === 'string') return parseTimestamp(value.value);
    if (typeof value.timestamp === 'string') return parseTimestamp(value.timestamp);
  }
  return null;
}

/**
 * Deep-search a JSON value for a key, returning the first match found.
 */
function deepFind(obj, key) {
  if (!obj || typeof obj !== 'object') return undefined;
  if (key in obj) return obj[key];
  for (const v of Object.values(obj)) {
    const found = deepFind(v, key);
    if (found !== undefined) return found;
  }
  return undefined;
}

function fmtCC(n) {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(2)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return n.toFixed(2);
}

function fmtDate(d) {
  if (!d) return 'Unknown';
  return new Date(d).toISOString().slice(0, 10);
}

function daysBetween(a, b) {
  if (!a || !b) return null;
  return Math.floor((new Date(b) - new Date(a)) / 86_400_000);
}

function pad(s, len, align = 'left') {
  const str = String(s);
  if (align === 'right') return str.padStart(len);
  return str.padEnd(len);
}

// ─── main ───────────────────────────────────────────────────────────────────

async function main() {
  const flags = new Set(process.argv.slice(2));
  const wantJson = flags.has('--json');
  const wantCsv = flags.has('--csv');

  console.error(`Scan API: ${SCAN_BASE}`);
  console.error('Fetching data...\n');

  // 1. Parallel fetch all data sources
  const [featuredAppsData, latestRoundData, voteResultsData, topProvidersRaw] =
    await Promise.all([
      scanGet('v0/featured-apps').catch((e) => {
        console.error(`  ⚠ featured-apps: ${e.message}`);
        return { featured_apps: [] };
      }),
      scanGet('v0/round-of-latest-data').catch((e) => {
        console.error(`  ⚠ latest-round: ${e.message}`);
        return { round: 0 };
      }),
      scanPost('v0/admin/sv/voteresults', {
        actionName: 'SRARC_GrantFeaturedAppRight',
        accepted: true,
        limit: 500,
      }).catch((e) => {
        console.error(`  ⚠ vote-results: ${e.message}`);
        return { dso_rules_vote_results: [] };
      }),
      (async () => {
        try {
          const latest = await scanGet('v0/round-of-latest-data');
          return scanGet(
            `v0/top-providers-by-app-rewards?round=${latest.round}&limit=1000`
          );
        } catch (e) {
          console.error(`  ⚠ top-providers: ${e.message}`);
          return { providersAndRewards: [] };
        }
      })(),
    ]);

  const featuredApps = featuredAppsData.featured_apps || [];
  const voteResults = voteResultsData.dso_rules_vote_results || [];
  const topProviders = topProvidersRaw.providersAndRewards || [];
  const latestRound = latestRoundData.round || 0;

  console.error(`  Featured Apps: ${featuredApps.length}`);
  console.error(`  Vote results (GrantFeaturedAppRight): ${voteResults.length}`);
  console.error(`  Top providers: ${topProviders.length}`);
  console.error(`  Latest round: ${latestRound}\n`);

  // 2. Build lookup: provider → cumulative rewards
  const rewardsByProvider = new Map();
  for (const p of topProviders) {
    const provider = p.provider || p.party;
    if (provider) rewardsByProvider.set(provider, parseFloat(p.rewards || '0'));
  }

  // 3. Build lookup: provider → approval date from vote results
  //
  // Vote result structure (from Canton Scan API):
  //   request.action.tag = "ARC_DsoRules"
  //   request.action.value.dsoAction.tag = "SRARC_GrantFeaturedAppRight"
  //   request.action.value.dsoAction.value.provider = "mainnet:app::122..."
  //   completed_at = string | { microsecondsSinceEpoch: "..." } | { seconds, nanos }
  //   outcome.tag = "VRO_Accepted" | "VRO_Rejected" | "VRO_Expired"

  if (flags.has('--debug') && voteResults.length > 0) {
    console.error('\n  [DEBUG] Sample vote result (first accepted):');
    const sample = voteResults.find(vr => vr.outcome?.tag === 'VRO_Accepted') || voteResults[0];
    console.error(JSON.stringify(sample, null, 2).split('\n').map(l => '    ' + l).join('\n'));
    console.error();
  }

  const approvalByProvider = new Map();
  let vrAccepted = 0;
  let vrProviderFound = 0;
  for (const vr of voteResults) {
    try {
      if (vr.outcome?.tag !== 'VRO_Accepted') continue;
      vrAccepted++;

      const action = vr.request?.action;
      const actionValue = action?.value;

      // Drill into the nested action structure to find the provider
      let provider =
        actionValue?.dsoAction?.value?.provider ||  // Most common path
        actionValue?.provider ||                     // Direct path
        actionValue?.amuletRulesAction?.value?.provider ||
        deepFind(actionValue, 'provider');           // Fallback: deep search

      if (!provider) continue;
      vrProviderFound++;

      // Parse completed_at which may be a DAML timestamp object
      const completedAt =
        parseTimestamp(vr.completed_at) ||
        parseTimestamp(vr.completedAt) ||
        parseTimestamp(vr.request?.completed_at);

      if (!completedAt) continue;

      // Keep earliest approval per provider
      const existing = approvalByProvider.get(provider);
      if (!existing || new Date(completedAt) < new Date(existing)) {
        approvalByProvider.set(provider, completedAt);
      }
    } catch { /* skip */ }
  }

  console.error(`  Vote results accepted: ${vrAccepted}, provider extracted: ${vrProviderFound}`);
  console.error(`  Approval dates matched to providers: ${approvalByProvider.size}`);

  // 4. Try round-party-totals for milestone tracking (batched in 50-round chunks)
  //    We sample evenly across the full range to get milestone estimates.
  const milestonesByProvider = new Map();
  const RPT_BATCH = 50; // API max per request
  const RPT_SAMPLE_BATCHES = 40; // Sample up to 40 batches (~2000 rounds)
  if (latestRound > 0) {
    try {
      // Spread sample batches evenly across the full round range
      const step = Math.max(1, Math.floor(latestRound / RPT_SAMPLE_BATCHES));
      const batches = [];
      for (let start = 0; start < latestRound; start += step) {
        const end = Math.min(start + RPT_BATCH - 1, latestRound);
        batches.push({ start_round: start, end_round: end });
      }
      // Always include the most recent 50 rounds
      batches.push({ start_round: Math.max(0, latestRound - RPT_BATCH + 1), end_round: latestRound });

      console.error(`  Fetching round-party-totals: ${batches.length} batches (sampled)...`);
      let totalEntries = 0;

      // Fetch in parallel with concurrency limit
      const CONCURRENCY = 5;
      for (let i = 0; i < batches.length; i += CONCURRENCY) {
        const chunk = batches.slice(i, i + CONCURRENCY);
        const results = await Promise.all(
          chunk.map(b =>
            scanPost('v0/round-party-totals', b).catch(() => ({ entries: [] }))
          )
        );
        for (const rpt of results) {
          const entries = rpt.entries || [];
          totalEntries += entries.length;
          for (const e of entries) {
            const cum = parseFloat(e.cumulative_app_rewards || '0');
            const party = e.party;
            if (!party || cum === 0) continue;

            if (!milestonesByProvider.has(party)) {
              milestonesByProvider.set(party, { m10: null, m25: null });
            }
            const ms = milestonesByProvider.get(party);
            const roundDate = e.closed_round_effective_at || null;
            if (cum >= THRESHOLDS.MILESTONE_10M && (!ms.m10 || e.closed_round < ms.m10.round)) {
              ms.m10 = { round: e.closed_round, date: roundDate, amount: cum };
            }
            if (cum >= THRESHOLDS.MILESTONE_25M && (!ms.m25 || e.closed_round < ms.m25.round)) {
              ms.m25 = { round: e.closed_round, date: roundDate, amount: cum };
            }
          }
        }
      }
      console.error(`  Round-party-totals: ${totalEntries} entries from ${batches.length} batches`);
    } catch (e) {
      console.error(`  ⚠ round-party-totals unavailable: ${e.message}`);
    }
  }

  // 5. Build per-app report rows
  const now = new Date();
  const rows = featuredApps
    .map((app) => {
      const payload = app.payload || app;
      const provider = payload.provider || app.provider || '';
      const appName =
        payload.appName || payload.app_name || payload.name || provider.split('::')[0] || 'Unknown';
      const cum = rewardsByProvider.get(provider) || 0;
      const approval = approvalByProvider.get(provider) || null;
      const daysSinceApproval = approval
        ? Math.floor((now - new Date(approval)) / 86_400_000)
        : null;
      const ms = milestonesByProvider.get(provider) || { m10: null, m25: null };
      const daysTo10m = approval && ms.m10?.date ? daysBetween(approval, ms.m10.date) : null;
      const daysTo25m = approval && ms.m25?.date ? daysBetween(approval, ms.m25.date) : null;

      return {
        appName,
        provider,
        providerShort: provider.split('::')[0],
        approvalDate: approval,
        daysSinceApproval,
        cumulativeCC: cum,
        hasReached10m: cum >= THRESHOLDS.MILESTONE_10M,
        hasReached25m: cum >= THRESHOLDS.MILESTONE_25M,
        canLockDay1: cum >= THRESHOLDS.LOCK_AMOUNT,
        daysTo10m,
        daysTo25m,
      };
    })
    .sort((a, b) => b.cumulativeCC - a.cumulativeCC);

  // 6. Summary stats
  const totalLifetimeRewards = rows.reduce((s, r) => s + r.cumulativeCC, 0);
  const above10m = rows.filter((r) => r.hasReached10m).length;
  const above25m = rows.filter((r) => r.hasReached25m).length;
  const lockReady = rows.filter((r) => r.canLockDay1).length;

  // ─── output ─────────────────────────────────────────────────────────────

  if (wantJson) {
    const output = {
      generatedAt: now.toISOString(),
      latestRound,
      totalFeaturedApps: rows.length,
      totalLifetimeRewards,
      appsAbove10m: above10m,
      appsAbove25m: above25m,
      canLockDay1: lockReady,
      apps: rows,
    };
    console.log(JSON.stringify(output, null, 2));
    return;
  }

  if (wantCsv) {
    console.log(
      'Rank,App Name,Provider (short),Approval Date,Days Since Approval,Cumulative CC,>=10M,>=25M,Can Lock Day1,Days to 10M,Days to 25M'
    );
    rows.forEach((r, i) => {
      console.log(
        [
          i + 1,
          `"${r.appName}"`,
          `"${r.providerShort}"`,
          r.approvalDate ? fmtDate(r.approvalDate) : '',
          r.daysSinceApproval ?? '',
          r.cumulativeCC.toFixed(2),
          r.hasReached10m ? 'Y' : 'N',
          r.hasReached25m ? 'Y' : 'N',
          r.canLockDay1 ? 'Y' : 'N',
          r.daysTo10m ?? '',
          r.daysTo25m ?? '',
        ].join(',')
      );
    });
    return;
  }

  // ─── text report ────────────────────────────────────────────────────────

  const line = '═'.repeat(110);
  const thinLine = '─'.repeat(110);

  console.log();
  console.log(line);
  console.log('  FEATURED APPS (FA) REPORT — Canton Network');
  console.log(`  Generated: ${now.toISOString()}  |  Latest Round: ${latestRound.toLocaleString()}`);
  console.log(line);
  console.log();

  // Summary
  console.log('  SUMMARY');
  console.log(thinLine);
  console.log(`  Total Featured Apps on-chain:      ${rows.length}`);
  console.log(`  Lifetime FA Rewards (all FAs):     ${totalLifetimeRewards.toLocaleString(undefined, { maximumFractionDigits: 2 })} CC  (${fmtCC(totalLifetimeRewards)})`);
  console.log(`  FAs with >= 10M CC mined:          ${above10m}`);
  console.log(`  FAs with >= 25M CC mined:          ${above25m}`);
  console.log(`  FAs that can lock 25M CC on Day 1:  ${lockReady}`);
  console.log();

  // CIP Readiness
  console.log('  CIP LOCKING READINESS');
  console.log(thinLine);
  console.log(`  If the FA Locking CIP passes, partners must lock 25M CC within 6 months.`);
  console.log();
  console.log(`    READY (>= 25M CC):          ${lockReady} FA(s)`);
  console.log(`    APPROACHING (10M–25M CC):   ${above10m - above25m} FA(s)`);
  console.log(`    BELOW THRESHOLD (< 10M CC): ${rows.length - above10m} FA(s)`);
  console.log();

  // Detail table
  console.log('  DETAILED BREAKDOWN (ranked by cumulative CC)');
  console.log(thinLine);

  const hdr =
    pad('#', 4) +
    pad('App Name', 28) +
    pad('Provider', 20) +
    pad('FA Approved', 14) +
    pad('Days as FA', 11, 'right') +
    pad('Cumulative CC', 18, 'right') +
    pad('Days→10M', 10, 'right') +
    pad('Days→25M', 10, 'right') +
    pad('Lock?', 7, 'right');

  console.log(`  ${hdr}`);
  console.log(`  ${'─'.repeat(hdr.length)}`);

  rows.forEach((r, i) => {
    const lockIcon = r.canLockDay1 ? '  YES' : (r.hasReached10m ? '  ~' : '  NO');
    const row =
      pad(String(i + 1), 4) +
      pad(r.appName.slice(0, 26), 28) +
      pad(r.providerShort.slice(0, 18), 20) +
      pad(fmtDate(r.approvalDate), 14) +
      pad(r.daysSinceApproval !== null ? `${r.daysSinceApproval}d` : '--', 11, 'right') +
      pad(fmtCC(r.cumulativeCC), 18, 'right') +
      pad(r.daysTo10m !== null ? `${r.daysTo10m}d` : (r.hasReached10m ? 'Y' : '--'), 10, 'right') +
      pad(r.daysTo25m !== null ? `${r.daysTo25m}d` : (r.hasReached25m ? 'Y' : '--'), 10, 'right') +
      pad(lockIcon, 7, 'right');
    console.log(`  ${row}`);
  });

  console.log();
  console.log(line);
  console.log('  NOTES');
  console.log(thinLine);
  if (approvalByProvider.size === 0) {
    console.log('  * FA approval dates: Could not determine from on-chain vote results.');
    console.log('    The voteresults API may not return historical GrantFeaturedAppRight actions.');
  } else {
    console.log(`  * FA approval dates: Found ${approvalByProvider.size} from on-chain GrantFeaturedAppRight vote results.`);
  }
  if (milestonesByProvider.size === 0) {
    console.log('  * Milestone timing: round-party-totals endpoint unavailable. Days to 10M/25M not calculated.');
  } else {
    console.log(`  * Milestone timing: Calculated from ${milestonesByProvider.size} provider histories.`);
  }
  console.log('  * "Cumulative CC" = total app rewards mined by the provider party since launch.');
  console.log('  * "Lock Ready" = provider has already accrued >= 25M CC (could lock on Day 1).');
  console.log('  * "Days→10M / Days→25M" = days from FA approval to reaching that milestone.');
  console.log(line);
  console.log();
}

main().catch((err) => {
  console.error('Fatal:', err);
  process.exit(1);
});
