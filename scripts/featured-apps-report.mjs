#!/usr/bin/env node

/**
 * Featured Apps Report Generator (Local / Internal Use)
 *
 * Queries Canton Scan API directly to produce a detailed report of:
 * - All Featured Apps with cumulative CC mined
 * - FA approval dates (from on-chain vote results)
 * - Precise milestone tracking: days from FA approval to $10M / $25M CC
 * - CIP Locking readiness assessment
 *
 * Usage:
 *   node scripts/featured-apps-report.mjs
 *   node scripts/featured-apps-report.mjs --json
 *   node scripts/featured-apps-report.mjs --csv
 *   node scripts/featured-apps-report.mjs --debug
 *   SCAN_URL=https://... node scripts/featured-apps-report.mjs
 *
 * Data sources (all from Canton Scan API):
 *   GET  /v0/featured-apps                      → current on-chain FAs
 *   GET  /v0/round-of-latest-data               → latest round number
 *   GET  /v0/top-providers-by-app-rewards       → cumulative CC per provider
 *   POST /v0/admin/sv/voteresults               → FA approval dates
 *   POST /v0/round-totals                       → round timestamps (date mapping)
 *   POST /v0/round-party-totals                 → per-party cumulative CC at a round
 */

const SCAN_BASE =
  process.env.SCAN_URL ||
  'https://scan.sv-1.global.canton.network.sync.global/api/scan';

const THRESHOLDS = {
  LOCK_AMOUNT: 25_000_000,
  MILESTONE_10M: 10_000_000,
  MILESTONE_25M: 25_000_000,
};

// ─── HTTP helpers ───────────────────────────────────────────────────────────

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

// ─── Parsing helpers ────────────────────────────────────────────────────────

/** Parse Canton/DAML timestamps (string, microsecondsSinceEpoch, {seconds,nanos}). */
function parseTimestamp(value) {
  if (!value) return null;
  if (typeof value === 'string') { const d = new Date(value); return isNaN(d) ? null : d.toISOString(); }
  if (typeof value === 'number') return new Date(value).toISOString();
  if (typeof value === 'object') {
    if (value.microsecondsSinceEpoch != null) {
      const m = Number(value.microsecondsSinceEpoch);
      if (!isNaN(m)) return new Date(m / 1000).toISOString();
    }
    if (value.seconds != null) {
      const s = Number(value.seconds), n = value.nanos ? Number(value.nanos) : 0;
      if (!isNaN(s)) return new Date(s * 1000 + Math.floor(n / 1e6)).toISOString();
    }
    if (value.unixtime != null) { const s = Number(value.unixtime); if (!isNaN(s)) return new Date(s * 1000).toISOString(); }
    if (typeof value.value === 'string') return parseTimestamp(value.value);
  }
  return null;
}

/** Deep-search a JSON value for a key. */
function deepFind(obj, key) {
  if (!obj || typeof obj !== 'object') return undefined;
  if (key in obj) return obj[key];
  for (const v of Object.values(obj)) { const f = deepFind(v, key); if (f !== undefined) return f; }
  return undefined;
}

// ─── Formatting helpers ─────────────────────────────────────────────────────

function fmtCC(n) {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(2)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return n.toFixed(2);
}
function fmtDate(d) { return d ? new Date(d).toISOString().slice(0, 10) : 'Unknown'; }
function daysBetween(a, b) { if (!a || !b) return null; return Math.floor((new Date(b) - new Date(a)) / 86_400_000); }
function pad(s, len, align = 'left') { const str = String(s); return align === 'right' ? str.padStart(len) : str.padEnd(len); }

// ─── Round-party-totals: get a provider's cumulative CC at a specific round ─

/**
 * Fetch cumulative_app_rewards for `provider` at or near `round`.
 * Requests a small window and returns the best match.
 * Returns { round, cumCC } or null.
 */
async function getCumulativeAtRound(provider, targetRound, windowSize = 10) {
  const start = Math.max(0, targetRound - windowSize + 1);
  const end = targetRound;
  try {
    const rpt = await scanPost('v0/round-party-totals', { start_round: start, end_round: end });
    const entries = (rpt.entries || [])
      .filter(e => e.party === provider)
      .sort((a, b) => b.closed_round - a.closed_round); // latest first
    if (entries.length === 0) return null;
    return { round: entries[0].closed_round, cumCC: parseFloat(entries[0].cumulative_app_rewards || '0') };
  } catch { return null; }
}

/**
 * Binary search for the first round where provider's cumulative CC >= threshold.
 * Returns the round number, or null if not found.
 */
async function binarySearchMilestone(provider, threshold, lowRound, highRound) {
  let lo = lowRound, hi = highRound, bestRound = null;

  // Limit iterations to prevent runaway
  for (let iter = 0; iter < 20 && lo <= hi; iter++) {
    const mid = Math.floor((lo + hi) / 2);
    const result = await getCumulativeAtRound(provider, mid, 10);

    if (!result) {
      // No data at this round — try a wider window, or move right
      const wider = await getCumulativeAtRound(provider, mid, 50);
      if (!wider) { lo = mid + 1; continue; }
      if (wider.cumCC >= threshold) { bestRound = wider.round; hi = wider.round - 1; }
      else { lo = wider.round + 1; }
      continue;
    }

    if (result.cumCC >= threshold) {
      bestRound = result.round;
      hi = result.round - 1;
    } else {
      lo = result.round + 1;
    }
  }
  return bestRound;
}

// ─── Round-totals: map round numbers to dates ───────────────────────────────

/**
 * Fetch dates for a set of round numbers using v0/round-totals.
 * Returns Map<roundNumber, isoDateString>.
 * round-totals also has a 50-round-per-request limit.
 */
async function fetchRoundDates(roundNumbers) {
  if (roundNumbers.length === 0) return new Map();
  const unique = [...new Set(roundNumbers)].sort((a, b) => a - b);
  const dateMap = new Map();

  // Batch into groups of up to 50 contiguous rounds
  const batches = [];
  let i = 0;
  while (i < unique.length) {
    const batchStart = unique[i];
    let batchEnd = batchStart;
    let j = i + 1;
    while (j < unique.length && unique[j] - batchStart < 50) {
      batchEnd = unique[j];
      j++;
    }
    batches.push({ start_round: batchStart, end_round: batchEnd });
    i = j;
  }

  const CONCURRENCY = 5;
  for (let b = 0; b < batches.length; b += CONCURRENCY) {
    const chunk = batches.slice(b, b + CONCURRENCY);
    const results = await Promise.all(
      chunk.map(batch => scanPost('v0/round-totals', batch).catch(() => ({ entries: [] })))
    );
    for (const rt of results) {
      for (const e of (rt.entries || [])) {
        if (e.closed_round != null && e.closed_round_effective_at) {
          dateMap.set(e.closed_round, e.closed_round_effective_at);
        }
      }
    }
  }
  return dateMap;
}

// ─── Main ───────────────────────────────────────────────────────────────────

async function main() {
  const flags = new Set(process.argv.slice(2));
  const wantJson = flags.has('--json');
  const wantCsv = flags.has('--csv');
  const debug = flags.has('--debug');

  console.error(`Scan API: ${SCAN_BASE}`);
  console.error('Fetching data...\n');

  // ── Phase 1: Fetch core data sources in parallel ──────────────────────────

  const [featuredAppsData, latestRoundData, voteResultsData, topProvidersRaw] =
    await Promise.all([
      scanGet('v0/featured-apps').catch(e => { console.error(`  ⚠ featured-apps: ${e.message}`); return { featured_apps: [] }; }),
      scanGet('v0/round-of-latest-data').catch(e => { console.error(`  ⚠ latest-round: ${e.message}`); return { round: 0 }; }),
      scanPost('v0/admin/sv/voteresults', { actionName: 'SRARC_GrantFeaturedAppRight', accepted: true, limit: 500 })
        .catch(e => { console.error(`  ⚠ vote-results: ${e.message}`); return { dso_rules_vote_results: [] }; }),
      (async () => {
        try {
          const latest = await scanGet('v0/round-of-latest-data');
          return scanGet(`v0/top-providers-by-app-rewards?round=${latest.round}&limit=1000`);
        } catch (e) { console.error(`  ⚠ top-providers: ${e.message}`); return { providersAndRewards: [] }; }
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

  // ── Build provider → cumulative rewards map ───────────────────────────────

  const rewardsByProvider = new Map();
  for (const p of topProviders) {
    const provider = p.provider || p.party;
    if (provider) rewardsByProvider.set(provider, parseFloat(p.rewards || '0'));
  }

  // ── Build provider → approval date from vote results ──────────────────────

  if (debug && voteResults.length > 0) {
    const sample = voteResults.find(vr => vr.outcome?.tag === 'VRO_Accepted') || voteResults[0];
    console.error('  [DEBUG] Sample vote result structure:');
    console.error('    action.tag:', sample.request?.action?.tag);
    console.error('    dsoAction.tag:', sample.request?.action?.value?.dsoAction?.tag);
    console.error('    provider:', sample.request?.action?.value?.dsoAction?.value?.provider);
    console.error('    completedAt:', sample.completedAt || sample.completed_at);
    console.error();
  }

  const approvalByProvider = new Map();
  let vrAccepted = 0, vrProviderFound = 0;
  for (const vr of voteResults) {
    try {
      if (vr.outcome?.tag !== 'VRO_Accepted') continue;
      vrAccepted++;

      const actionValue = vr.request?.action?.value;
      let provider =
        actionValue?.dsoAction?.value?.provider ||
        actionValue?.provider ||
        actionValue?.amuletRulesAction?.value?.provider ||
        deepFind(actionValue, 'provider');
      if (!provider) continue;
      vrProviderFound++;

      const completedAt =
        parseTimestamp(vr.completedAt) ||
        parseTimestamp(vr.completed_at) ||
        parseTimestamp(vr.request?.completed_at);
      if (!completedAt) continue;

      const existing = approvalByProvider.get(provider);
      if (!existing || new Date(completedAt) < new Date(existing)) {
        approvalByProvider.set(provider, completedAt);
      }
    } catch { /* skip */ }
  }

  console.error(`  Vote results accepted: ${vrAccepted}, provider extracted: ${vrProviderFound}`);
  console.error(`  Approval dates matched: ${approvalByProvider.size}\n`);

  // ── Phase 2: Coarse sampling to identify milestone ranges ─────────────────
  //
  // round-party-totals has cumulative_app_rewards per party per round, but
  // NO date field. round-totals has dates but is aggregate (not per-party).
  // Strategy:
  //   1. Sample round-party-totals broadly → find approximate crossing ranges
  //   2. Binary search within those ranges → find exact crossing rounds
  //   3. Fetch round-totals for those rounds → get dates

  // Build list of FA providers we care about (those that have reached a milestone)
  const faProviders = new Set(
    featuredApps.map(a => (a.payload || a).provider || a.provider).filter(Boolean)
  );

  // Track: provider → { belowRound10m, aboveRound10m, belowRound25m, aboveRound25m }
  // "below" = last sampled round where cumCC < threshold
  // "above" = first sampled round where cumCC >= threshold
  const searchRanges = new Map();
  for (const provider of faProviders) {
    const cum = rewardsByProvider.get(provider) || 0;
    searchRanges.set(provider, {
      above10m: cum >= THRESHOLDS.MILESTONE_10M,
      above25m: cum >= THRESHOLDS.MILESTONE_25M,
      below10m: 0, found10m: null,
      below25m: 0, found25m: null,
    });
  }

  const RPT_BATCH = 50;
  const RPT_SAMPLE_BATCHES = 60;
  if (latestRound > 0) {
    try {
      const step = Math.max(RPT_BATCH, Math.floor(latestRound / RPT_SAMPLE_BATCHES));
      const batches = [];
      for (let s = 0; s < latestRound; s += step) {
        batches.push({ start_round: s, end_round: Math.min(s + RPT_BATCH - 1, latestRound) });
      }
      batches.push({ start_round: Math.max(0, latestRound - RPT_BATCH + 1), end_round: latestRound });

      console.error(`  Phase 2a: Coarse sampling — ${batches.length} batches...`);

      const CONCURRENCY = 6;
      for (let i = 0; i < batches.length; i += CONCURRENCY) {
        const chunk = batches.slice(i, i + CONCURRENCY);
        const results = await Promise.all(
          chunk.map(b => scanPost('v0/round-party-totals', b).catch(() => ({ entries: [] })))
        );
        for (const rpt of results) {
          for (const e of (rpt.entries || [])) {
            const sr = searchRanges.get(e.party);
            if (!sr) continue;
            const cum = parseFloat(e.cumulative_app_rewards || '0');
            const rnd = e.closed_round;

            // Track 10M crossing range
            if (sr.above10m) {
              if (cum < THRESHOLDS.MILESTONE_10M && rnd > sr.below10m) sr.below10m = rnd;
              if (cum >= THRESHOLDS.MILESTONE_10M && (sr.found10m === null || rnd < sr.found10m)) sr.found10m = rnd;
            }
            // Track 25M crossing range
            if (sr.above25m) {
              if (cum < THRESHOLDS.MILESTONE_25M && rnd > sr.below25m) sr.below25m = rnd;
              if (cum >= THRESHOLDS.MILESTONE_25M && (sr.found25m === null || rnd < sr.found25m)) sr.found25m = rnd;
            }
          }
        }
      }
    } catch (e) {
      console.error(`  ⚠ Coarse sampling failed: ${e.message}`);
    }
  }

  // ── Phase 3: Binary search for exact milestone crossing rounds ────────────

  // Collect all (provider, threshold, lowRound, highRound) searches
  const searches = [];
  for (const [provider, sr] of searchRanges) {
    if (sr.above10m && sr.found10m !== null) {
      searches.push({ provider, threshold: THRESHOLDS.MILESTONE_10M, label: '10m',
        lo: sr.below10m, hi: sr.found10m });
    }
    if (sr.above25m && sr.found25m !== null) {
      searches.push({ provider, threshold: THRESHOLDS.MILESTONE_25M, label: '25m',
        lo: sr.below25m, hi: sr.found25m });
    }
  }

  console.error(`  Phase 2b: Binary search for ${searches.length} milestones...`);

  // milestoneRounds: provider → { round10m, round25m }
  const milestoneRounds = new Map();

  // Run searches with limited concurrency
  const SEARCH_CONCURRENCY = 3;
  for (let i = 0; i < searches.length; i += SEARCH_CONCURRENCY) {
    const batch = searches.slice(i, i + SEARCH_CONCURRENCY);
    const results = await Promise.all(
      batch.map(async s => {
        const round = await binarySearchMilestone(s.provider, s.threshold, s.lo, s.hi);
        return { ...s, round };
      })
    );
    for (const r of results) {
      if (!milestoneRounds.has(r.provider)) milestoneRounds.set(r.provider, {});
      const mr = milestoneRounds.get(r.provider);
      if (r.label === '10m') mr.round10m = r.round;
      if (r.label === '25m') mr.round25m = r.round;
    }
    // Progress
    if ((i + SEARCH_CONCURRENCY) % 15 === 0 || i + SEARCH_CONCURRENCY >= searches.length) {
      console.error(`    ...${Math.min(i + SEARCH_CONCURRENCY, searches.length)}/${searches.length} done`);
    }
  }

  // ── Phase 4: Fetch dates for all milestone rounds ─────────────────────────

  const roundsNeedingDates = [];
  for (const [, mr] of milestoneRounds) {
    if (mr.round10m != null) roundsNeedingDates.push(mr.round10m);
    if (mr.round25m != null) roundsNeedingDates.push(mr.round25m);
  }

  console.error(`  Phase 3: Fetching dates for ${new Set(roundsNeedingDates).size} milestone rounds...`);
  const roundDateMap = await fetchRoundDates(roundsNeedingDates);
  console.error(`  Dates resolved: ${roundDateMap.size}\n`);

  // ── Phase 5: Assemble report rows ─────────────────────────────────────────

  const now = new Date();
  const rows = featuredApps
    .map(app => {
      const payload = app.payload || app;
      const provider = payload.provider || app.provider || '';
      const appName = payload.appName || payload.app_name || payload.name || provider.split('::')[0] || 'Unknown';
      const cum = rewardsByProvider.get(provider) || 0;
      const approval = approvalByProvider.get(provider) || null;
      const daysSinceApproval = approval ? Math.floor((now - new Date(approval)) / 86_400_000) : null;

      const mr = milestoneRounds.get(provider) || {};

      // Get milestone dates from round-totals
      const date10m = mr.round10m != null ? (roundDateMap.get(mr.round10m) || null) : null;
      const date25m = mr.round25m != null ? (roundDateMap.get(mr.round25m) || null) : null;

      const daysTo10m = approval && date10m ? daysBetween(approval, date10m) : null;
      const daysTo25m = approval && date25m ? daysBetween(approval, date25m) : null;

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
        milestone10m: { round: mr.round10m || null, date: date10m, daysFromApproval: daysTo10m },
        milestone25m: { round: mr.round25m || null, date: date25m, daysFromApproval: daysTo25m },
      };
    })
    .sort((a, b) => b.cumulativeCC - a.cumulativeCC);

  // ── Summary stats ─────────────────────────────────────────────────────────

  const totalLifetimeRewards = rows.reduce((s, r) => s + r.cumulativeCC, 0);
  const above10m = rows.filter(r => r.hasReached10m).length;
  const above25m = rows.filter(r => r.hasReached25m).length;
  const lockReady = rows.filter(r => r.canLockDay1).length;

  // ── Output: JSON ──────────────────────────────────────────────────────────

  if (wantJson) {
    console.log(JSON.stringify({
      generatedAt: now.toISOString(), latestRound,
      totalFeaturedApps: rows.length, totalLifetimeRewards,
      appsAbove10m: above10m, appsAbove25m: above25m, canLockDay1: lockReady,
      apps: rows,
    }, null, 2));
    return;
  }

  // ── Output: CSV ───────────────────────────────────────────────────────────

  if (wantCsv) {
    console.log('Rank,App Name,Provider (short),Approval Date,Days as FA,Cumulative CC,>=10M,>=25M,Can Lock Day1,10M Date,Days to 10M,25M Date,Days to 25M');
    rows.forEach((r, i) => {
      console.log([
        i + 1,
        `"${r.appName}"`,
        `"${r.providerShort}"`,
        r.approvalDate ? fmtDate(r.approvalDate) : '',
        r.daysSinceApproval ?? '',
        r.cumulativeCC.toFixed(2),
        r.hasReached10m ? 'Y' : 'N',
        r.hasReached25m ? 'Y' : 'N',
        r.canLockDay1 ? 'Y' : 'N',
        r.milestone10m.date ? fmtDate(r.milestone10m.date) : '',
        r.milestone10m.daysFromApproval ?? '',
        r.milestone25m.date ? fmtDate(r.milestone25m.date) : '',
        r.milestone25m.daysFromApproval ?? '',
      ].join(','));
    });
    return;
  }

  // ── Output: Text report ───────────────────────────────────────────────────

  const W = 130;
  const line = '═'.repeat(W);
  const thinLine = '─'.repeat(W);

  console.log();
  console.log(line);
  console.log('  FEATURED APPS (FA) REPORT — Canton Network');
  console.log(`  Generated: ${now.toISOString()}  |  Latest Round: ${latestRound.toLocaleString()}`);
  console.log(line);
  console.log();

  console.log('  SUMMARY');
  console.log(thinLine);
  console.log(`  Total Featured Apps on-chain:      ${rows.length}`);
  console.log(`  Lifetime FA Rewards (all FAs):     ${totalLifetimeRewards.toLocaleString(undefined, { maximumFractionDigits: 2 })} CC  (${fmtCC(totalLifetimeRewards)})`);
  console.log(`  FAs with >= 10M CC mined:          ${above10m}`);
  console.log(`  FAs with >= 25M CC mined:          ${above25m}`);
  console.log(`  FAs that can lock 25M CC on Day 1:  ${lockReady}`);
  console.log();

  console.log('  CIP LOCKING READINESS');
  console.log(thinLine);
  console.log('  If the FA Locking CIP passes, partners must lock 25M CC within 6 months.');
  console.log();
  console.log(`    READY (>= 25M CC):          ${lockReady} FA(s)`);
  console.log(`    APPROACHING (10M–25M CC):   ${above10m - above25m} FA(s)`);
  console.log(`    BELOW THRESHOLD (< 10M CC): ${rows.length - above10m} FA(s)`);
  console.log();

  console.log('  DETAILED BREAKDOWN (ranked by cumulative CC)');
  console.log(thinLine);

  const hdr =
    pad('#', 4) +
    pad('App Name', 28) +
    pad('Provider', 20) +
    pad('FA Approved', 13) +
    pad('Days FA', 8, 'right') +
    pad('Cumulative CC', 16, 'right') +
    pad('10M Date', 12, 'right') +
    pad('Days→10M', 10, 'right') +
    pad('25M Date', 12, 'right') +
    pad('Days→25M', 10, 'right') +
    pad('Lock?', 7, 'right');

  console.log(`  ${hdr}`);
  console.log(`  ${'─'.repeat(hdr.length)}`);

  rows.forEach((r, i) => {
    const lockIcon = r.canLockDay1 ? '  YES' : (r.hasReached10m ? '  ~' : '  NO');

    const d10m = r.milestone10m.daysFromApproval;
    const d25m = r.milestone25m.daysFromApproval;
    const dt10m = r.milestone10m.date;
    const dt25m = r.milestone25m.date;

    const row =
      pad(String(i + 1), 4) +
      pad(r.appName.slice(0, 26), 28) +
      pad(r.providerShort.slice(0, 18), 20) +
      pad(fmtDate(r.approvalDate), 13) +
      pad(r.daysSinceApproval !== null ? `${r.daysSinceApproval}d` : '--', 8, 'right') +
      pad(fmtCC(r.cumulativeCC), 16, 'right') +
      pad(dt10m ? fmtDate(dt10m) : (r.hasReached10m ? '~' : '--'), 12, 'right') +
      pad(d10m !== null ? `${d10m}d` : (r.hasReached10m ? '~' : '--'), 10, 'right') +
      pad(dt25m ? fmtDate(dt25m) : (r.hasReached25m ? '~' : '--'), 12, 'right') +
      pad(d25m !== null ? `${d25m}d` : (r.hasReached25m ? '~' : '--'), 10, 'right') +
      pad(lockIcon, 7, 'right');

    console.log(`  ${row}`);
  });

  console.log();
  console.log(line);
  console.log('  NOTES');
  console.log(thinLine);
  console.log(`  * FA approval dates: ${approvalByProvider.size > 0 ? `${approvalByProvider.size} found` : 'NOT available'} from on-chain GrantFeaturedAppRight vote results.`);
  console.log(`  * Milestone timing: Binary-searched ${searches.length} milestones, resolved ${roundDateMap.size} round dates.`);
  console.log('  * "Cumulative CC" = total app rewards mined by the provider party since launch.');
  console.log('  * "Lock Ready" = provider has accrued >= 25M CC and could lock on Day 1.');
  console.log('  * "Days→10M/25M" = days from FA approval to reaching that CC milestone.');
  console.log('  * "~" = milestone reached but exact date could not be pinpointed.');
  console.log(line);
  console.log();
  console.log(`  DATA SOURCES (all from Canton Scan API: ${SCAN_BASE})`);
  console.log(thinLine);
  console.log('  1. GET  /v0/featured-apps                  — List of on-chain Featured Apps (provider party IDs)');
  console.log('  2. GET  /v0/top-providers-by-app-rewards   — Cumulative CC mined per app provider');
  console.log('  3. POST /v0/admin/sv/voteresults           — Historical vote results for GrantFeaturedAppRight');
  console.log('     (actionName=SRARC_GrantFeaturedAppRight) → gives FA approval date (completedAt field)');
  console.log('  4. POST /v0/round-party-totals             — Per-party cumulative rewards at a given round range');
  console.log('     (max 50 rounds/request)                  → used for binary search of milestone crossing rounds');
  console.log('  5. POST /v0/round-totals                   — Aggregate round data with closed_round_effective_at');
  console.log('     (max 50 rounds/request)                  → converts milestone round numbers to calendar dates');
  console.log(line);
  console.log();
}

main().catch(err => { console.error('Fatal:', err); process.exit(1); });
