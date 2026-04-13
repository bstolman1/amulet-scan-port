/**
 * Featured Apps Report API
 *
 * Aggregates data from multiple Canton Scan endpoints to produce a comprehensive
 * report on Featured App (FA) rewards, milestones, and CIP locking readiness.
 *
 * Data sources (all via Scan Proxy):
 * - /v0/featured-apps                    → current on-chain FAs
 * - /v0/top-providers-by-app-rewards     → cumulative CC mined per provider
 * - /v0/admin/sv/voteresults             → FA approval dates (GrantFeaturedAppRight)
 * - /v0/round-party-totals              → historical per-party cumulative rewards
 * - /v0/round-of-latest-data            → latest round number
 */

import { Router } from 'express';
import { getCurrentEndpoint } from '../lib/endpoint-rotation.js';
import { extractHostname, createDispatcher } from '../lib/undici-dispatcher.js';

const router = Router();

const THRESHOLDS = {
  LOCK_AMOUNT: 25_000_000,     // $25M CC locking requirement
  MILESTONE_10M: 10_000_000,   // $10M milestone
  MILESTONE_25M: 25_000_000,   // $25M milestone
};

/**
 * Helper to call the Scan API directly from the backend.
 * Mirrors the proxy logic but returns parsed JSON.
 */
async function scanFetch(path, method = 'GET', body = null) {
  const endpoint = getCurrentEndpoint();
  const url = `${endpoint.url}/${path}`;
  const hostname = extractHostname(endpoint.url);
  const dispatcher = hostname ? createDispatcher(hostname) : undefined;

  const fetchOptions = {
    method,
    headers: {
      Accept: 'application/json',
      ...(hostname ? { Host: hostname } : {}),
    },
    ...(dispatcher ? { dispatcher } : {}),
    signal: AbortSignal.timeout(30000),
  };

  if (body && (method === 'POST' || method === 'PUT')) {
    fetchOptions.headers['Content-Type'] = 'application/json';
    fetchOptions.body = JSON.stringify(body);
  }

  const res = await fetch(url, fetchOptions);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Scan API ${method} ${path} failed (${res.status}): ${text.slice(0, 200)}`);
  }
  return res.json();
}

/**
 * GET /api/featured-apps-report
 *
 * Returns a comprehensive report with:
 * - List of all current FAs with rewards, approval dates, milestones
 * - Summary statistics (total lifetime rewards, counts by threshold)
 * - CIP locking readiness assessment
 */
router.get('/', async (req, res) => {
  const startTime = Date.now();
  console.log('\n📊 FEATURED APPS REPORT: Generating...');

  try {
    // Step 1: Fetch all data sources in parallel
    const [
      featuredAppsData,
      latestRoundData,
      voteResultsData,
      topProvidersData,
    ] = await Promise.all([
      scanFetch('v0/featured-apps').catch(err => {
        console.warn('  ⚠ Failed to fetch featured apps:', err.message);
        return { featured_apps: [] };
      }),
      scanFetch('v0/round-of-latest-data').catch(err => {
        console.warn('  ⚠ Failed to fetch latest round:', err.message);
        return { round: 0, effectiveAt: null };
      }),
      scanFetch('v0/admin/sv/voteresults', 'POST', {
        actionName: 'SRARC_GrantFeaturedAppRight',
        accepted: true,
        limit: 500,
      }).catch(err => {
        console.warn('  ⚠ Failed to fetch vote results:', err.message);
        return { dso_rules_vote_results: [] };
      }),
      scanFetch('v0/top-providers-by-app-rewards', 'GET').catch(async () => {
        // Retry with round param if simple GET fails
        try {
          const latest = await scanFetch('v0/round-of-latest-data');
          return scanFetch(`v0/top-providers-by-app-rewards?round=${latest.round}&limit=1000`);
        } catch (err) {
          console.warn('  ⚠ Failed to fetch top providers:', err.message);
          return { providersAndRewards: [] };
        }
      }),
    ]);

    const featuredApps = featuredAppsData.featured_apps || [];
    const voteResults = voteResultsData.dso_rules_vote_results || [];
    const topProviders = topProvidersData.providersAndRewards || [];
    const latestRound = latestRoundData.round || 0;

    console.log(`  📋 Featured Apps: ${featuredApps.length}`);
    console.log(`  🗳️  Vote Results (GrantFeaturedAppRight): ${voteResults.length}`);
    console.log(`  🏆 Top Providers: ${topProviders.length}`);
    console.log(`  🔄 Latest Round: ${latestRound}`);

    // Step 2: Build lookup maps

    // Map provider party ID → cumulative rewards
    const rewardsByProvider = new Map();
    for (const p of topProviders) {
      const provider = p.provider || p.party;
      const rewards = parseFloat(p.rewards || '0');
      if (provider) {
        rewardsByProvider.set(provider, rewards);
      }
    }

    // Map provider party ID → approval date from vote results
    // Vote results for GrantFeaturedAppRight contain the provider in the action value
    const approvalByProvider = new Map();
    for (const vr of voteResults) {
      try {
        const action = vr.request?.action;
        const completedAt = vr.completed_at;
        const outcome = vr.outcome?.tag;

        if (outcome !== 'VRO_Accepted') continue;

        // Extract provider from the action value
        // Structure: { tag: "SRARC_GrantFeaturedAppRight", value: { provider: "..." } }
        // or nested as { dsoAction: { tag: ..., value: { provider: ... } } }
        let provider = null;
        if (action?.value?.provider) {
          provider = action.value.provider;
        } else if (action?.value?.dsoAction?.value?.provider) {
          provider = action.value.dsoAction.value.provider;
        } else if (typeof action?.value === 'object') {
          // Try to find provider in nested structure
          const valueStr = JSON.stringify(action.value);
          const providerMatch = valueStr.match(/"provider"\s*:\s*"([^"]+)"/);
          if (providerMatch) {
            provider = providerMatch[1];
          }
        }

        if (provider && completedAt) {
          // Keep the earliest approval date if there are multiple
          const existing = approvalByProvider.get(provider);
          if (!existing || new Date(completedAt) < new Date(existing)) {
            approvalByProvider.set(provider, completedAt);
          }
        }
      } catch (err) {
        console.warn('  ⚠ Error parsing vote result:', err.message);
      }
    }

    console.log(`  🗓️  Approval dates found: ${approvalByProvider.size}`);

    // Step 3: Try to fetch round-party-totals for milestone tracking
    // This gives us historical cumulative_app_rewards per party per round
    let roundPartyTotals = [];
    if (latestRound > 0 && featuredApps.length > 0) {
      try {
        // Fetch a sample of rounds to track milestones
        // We'll request the full range but the API may limit
        const rptData = await scanFetch('v0/round-party-totals', 'POST', {
          start_round: 0,
          end_round: latestRound,
        });
        roundPartyTotals = rptData.entries || [];
        console.log(`  📈 Round-party-totals entries: ${roundPartyTotals.length}`);
      } catch (err) {
        console.warn('  ⚠ Round-party-totals not available:', err.message);
      }
    }

    // Build milestone data: for each provider, find the first round where
    // cumulative_app_rewards crossed $10M and $25M
    const milestonesByProvider = new Map();
    if (roundPartyTotals.length > 0) {
      // Sort by round ascending
      const sorted = [...roundPartyTotals].sort((a, b) => a.closed_round - b.closed_round);
      for (const entry of sorted) {
        const party = entry.party;
        const cumRewards = parseFloat(entry.cumulative_app_rewards || '0');
        const round = entry.closed_round;
        const roundDate = entry.closed_round_effective_at || null;

        if (!milestonesByProvider.has(party)) {
          milestonesByProvider.set(party, { milestone10m: null, milestone25m: null });
        }
        const ms = milestonesByProvider.get(party);

        if (cumRewards >= THRESHOLDS.MILESTONE_10M && !ms.milestone10m) {
          ms.milestone10m = { round, date: roundDate, amount: cumRewards };
        }
        if (cumRewards >= THRESHOLDS.MILESTONE_25M && !ms.milestone25m) {
          ms.milestone25m = { round, date: roundDate, amount: cumRewards };
        }
      }
    }

    // Step 4: Build the report for each Featured App
    const now = new Date();
    const appReports = featuredApps.map(app => {
      const payload = app.payload || app;
      const provider = payload.provider || app.provider || '';
      const appName = payload.appName || payload.app_name || payload.name || '';
      const dso = payload.dso || '';
      const createdAt = app.created_at || null;

      const cumulativeRewards = rewardsByProvider.get(provider) || 0;
      const approvalDate = approvalByProvider.get(provider) || null;
      const milestones = milestonesByProvider.get(provider) || { milestone10m: null, milestone25m: null };

      // Calculate days since FA approval
      let daysSinceApproval = null;
      if (approvalDate) {
        daysSinceApproval = Math.floor((now - new Date(approvalDate)) / (1000 * 60 * 60 * 24));
      }

      // Calculate days from approval to milestones
      let daysToMilestone10m = null;
      let daysToMilestone25m = null;
      if (approvalDate && milestones.milestone10m?.date) {
        daysToMilestone10m = Math.floor(
          (new Date(milestones.milestone10m.date) - new Date(approvalDate)) / (1000 * 60 * 60 * 24)
        );
      }
      if (approvalDate && milestones.milestone25m?.date) {
        daysToMilestone25m = Math.floor(
          (new Date(milestones.milestone25m.date) - new Date(approvalDate)) / (1000 * 60 * 60 * 24)
        );
      }

      return {
        appName,
        provider: provider.split('::')[0], // Short form for display
        providerFull: provider,
        dso,
        createdAt,
        approvalDate,
        daysSinceApproval,
        cumulativeRewards,
        hasReached10m: cumulativeRewards >= THRESHOLDS.MILESTONE_10M,
        hasReached25m: cumulativeRewards >= THRESHOLDS.MILESTONE_25M,
        canLockDay1: cumulativeRewards >= THRESHOLDS.LOCK_AMOUNT,
        milestone10m: milestones.milestone10m ? {
          ...milestones.milestone10m,
          daysFromApproval: daysToMilestone10m,
        } : null,
        milestone25m: milestones.milestone25m ? {
          ...milestones.milestone25m,
          daysFromApproval: daysToMilestone25m,
        } : null,
      };
    });

    // Sort by cumulative rewards descending
    appReports.sort((a, b) => b.cumulativeRewards - a.cumulativeRewards);

    // Step 5: Compute summary statistics
    const totalLifetimeRewards = appReports.reduce((sum, a) => sum + a.cumulativeRewards, 0);
    const appsAbove10m = appReports.filter(a => a.hasReached10m).length;
    const appsAbove25m = appReports.filter(a => a.hasReached25m).length;
    const canLockDay1Count = appReports.filter(a => a.canLockDay1).length;

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(2);
    console.log(`  ✅ Report generated in ${elapsed}s`);
    console.log(`     Total FAs: ${appReports.length}`);
    console.log(`     Total Lifetime FA Rewards: ${totalLifetimeRewards.toLocaleString()} CC`);
    console.log(`     FAs >= $10M CC: ${appsAbove10m}`);
    console.log(`     FAs >= $25M CC: ${appsAbove25m}`);
    console.log(`     Can lock $25M on Day 1: ${canLockDay1Count}`);

    res.json({
      report: {
        generatedAt: now.toISOString(),
        latestRound,
        totalFeaturedApps: appReports.length,
        totalLifetimeRewards,
        appsAbove10m,
        appsAbove25m,
        canLockDay1Count,
        thresholds: THRESHOLDS,
        apps: appReports,
      },
      meta: {
        queryTimeSeconds: parseFloat(elapsed),
        dataSources: {
          featuredApps: featuredApps.length,
          voteResults: voteResults.length,
          topProviders: topProviders.length,
          roundPartyTotals: roundPartyTotals.length,
        },
        hasApprovalDates: approvalByProvider.size > 0,
        hasMilestoneData: milestonesByProvider.size > 0,
      },
    });
  } catch (err) {
    console.error('❌ Featured Apps Report error:', err);
    res.status(500).json({ error: err.message });
  }
});

export default router;
