/**
 * messageEnricher.js
 *
 * Background job: fetch per-message URLs for announce-group topics and merge
 * them into the cached lifecycle data so GovernanceFlow.tsx can match vote
 * reasonUrls (/g/GROUP/message/NNN) to their lifecycle cards.
 *
 * Why a background job?
 *   572 announce-group topics × ~200ms inter-call delay = ~2 min minimum.
 *   This cannot run inside an HTTP request cycle without timing out.
 *   Instead, dataFetcher.js calls runEnrichmentInBackground() after writing
 *   the initial cache, and this module runs independently, updating the cache
 *   in batches and writing enrichment-status.json so the frontend can poll.
 *
 * Concurrency:
 *   Only one enrichment job runs at a time. If a second refresh is triggered
 *   while enrichment is running the old job is aborted and a new one starts.
 */

import { readCache, writeCache, readEnrichmentStatus, writeEnrichmentStatus } from './fileRepository.js';
import { fetchTopicMessages, delay } from './groupsApiClient.js';
import { FETCH_PAGE_DELAY_MS } from './constants.js';

/**
 * Groups whose topics should be enriched with per-message URLs.
 * Only announce groups are included — these are the groups whose URLs appear
 * in vote reasonUrls.
 */
const MESSAGE_ENRICHMENT_GROUPS = new Set([
  'supervalidator-announce',
  'tokenomics-announce',
  'sv-announce',
]);

// ── Singleton job state ────────────────────────────────────────────────────

let _activeController = null;

/**
 * Abort any in-progress enrichment job and start a new one.
 * Safe to call multiple times — previous job is cancelled cleanly.
 *
 * @param {string} cacheTimestamp - cachedAt from the data we're enriching,
 *   used to detect stale jobs (if another refresh runs mid-enrichment we stop)
 */
export function runEnrichmentInBackground(cacheTimestamp) {
  // Cancel any previous job
  if (_activeController) {
    console.log('📨 Aborting previous enrichment job');
    _activeController.abort();
  }

  _activeController = new AbortController();
  const signal = _activeController.signal;

  // Fire-and-forget — intentionally not awaited
  _runEnrichment(signal, cacheTimestamp).catch(err => {
    if (err.name !== 'AbortError') {
      console.error('📨 Enrichment job failed:', err.message);
    }
  });
}

async function _runEnrichment(signal, cacheTimestamp) {
  console.log('📨 Starting background message URL enrichment…');

  await writeEnrichmentStatus({
    state: 'running',
    startedAt: new Date().toISOString(),
    completedAt: null,
    total: 0,
    processed: 0,
    enriched: 0,
    totalMessages: 0,
    error: null,
  });

  try {
    const cache = await readCache();
    if (!cache) {
      throw new Error('No cache found — enrichment cannot run without initial data');
    }

    // Bail if the cache has been replaced by a newer refresh
    if (cache.cachedAt !== cacheTimestamp) {
      console.log('📨 Cache has been refreshed since enrichment started — aborting stale job');
      await writeEnrichmentStatus({
        state: 'aborted',
        startedAt: new Date().toISOString(),
        completedAt: new Date().toISOString(),
        total: 0,
        processed: 0,
        enriched: 0,
        totalMessages: 0,
        error: 'Cache replaced by newer refresh',
      });
      return;
    }

    const allTopics = cache.allTopics ?? [];
    const toEnrich = allTopics.filter(t => MESSAGE_ENRICHMENT_GROUPS.has(t.groupName));

    console.log(`📨 ${toEnrich.length} announce-group topics to enrich`);

    await writeEnrichmentStatus({
      state: 'running',
      startedAt: new Date().toISOString(),
      completedAt: null,
      total: toEnrich.length,
      processed: 0,
      enriched: 0,
      totalMessages: 0,
      error: null,
    });

    // Build a lookup map: topic id → topic object in the live allTopics array
    // so we can mutate in place and write back in batches
    const topicById = new Map(allTopics.map(t => [t.id, t]));

    let processed = 0;
    let enriched = 0;
    let totalMessages = 0;
    const BATCH_SIZE = 25; // write cache every N topics

    for (const topic of toEnrich) {
      if (signal.aborted) {
        console.log('📨 Enrichment aborted');
        break;
      }

      const messageUrls = await fetchTopicMessages(
        topic.rawId,
        topic.groupId,
        topic.groupName,
        signal,
      );

      if (messageUrls.length > 0) {
        const live = topicById.get(topic.id);
        if (live) {
          const existing = new Set(live.linkedUrls ?? []);
          for (const u of messageUrls) existing.add(u);
          live.linkedUrls = [...existing];
        }
        enriched++;
        totalMessages += messageUrls.length;
      }

      processed++;

      // Batch checkpoint: write cache every BATCH_SIZE topics so progress
      // is durable even if the server restarts mid-enrichment
      if (processed % BATCH_SIZE === 0) {
        // Re-read cache to make sure we're not clobbering a concurrent write,
        // then splice in the updated allTopics
        const latest = await readCache();
        if (latest && latest.cachedAt === cacheTimestamp) {
          await writeCache({ ...latest, allTopics });
        }

        await writeEnrichmentStatus({
          state: 'running',
          startedAt: new Date().toISOString(),
          completedAt: null,
          total: toEnrich.length,
          processed,
          enriched,
          totalMessages,
          error: null,
        });

        console.log(`📨 Enrichment progress: ${processed}/${toEnrich.length} (${enriched} enriched, ${totalMessages} message URLs)`);
      }

      await delay(FETCH_PAGE_DELAY_MS);
    }

    // Final cache write
    if (!signal.aborted) {
      const latest = await readCache();
      if (latest && latest.cachedAt === cacheTimestamp) {
        await writeCache({ ...latest, allTopics });
      }
    }

    const finalState = signal.aborted ? 'aborted' : 'complete';
    await writeEnrichmentStatus({
      state: finalState,
      startedAt: new Date().toISOString(),
      completedAt: new Date().toISOString(),
      total: toEnrich.length,
      processed,
      enriched,
      totalMessages,
      error: null,
    });

    console.log(`📨 Enrichment ${finalState}: ${enriched}/${processed} topics enriched, ${totalMessages} message URLs indexed`);

  } catch (err) {
    if (err.name === 'AbortError') return;
    console.error('📨 Enrichment error:', err.message);
    await writeEnrichmentStatus({
      state: 'error',
      startedAt: new Date().toISOString(),
      completedAt: new Date().toISOString(),
      total: 0,
      processed: 0,
      enriched: 0,
      totalMessages: 0,
      error: err.message,
    });
    throw err;
  }
}
