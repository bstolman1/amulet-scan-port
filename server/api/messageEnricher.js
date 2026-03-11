/**
 * messageEnricher.js
 *
 * Background job: fetch per-message URLs for announce-group topics and merge
 * them into the cached lifecycle data so GovernanceFlow.tsx can match vote
 * reasonUrls (/g/GROUP/message/NNN) to their lifecycle cards.
 */

import { readCache, writeCache, writeEnrichmentStatus } from './fileRepository.js';
import { fetchTopicMessages, delay } from './groupsApiClient.js';
import { FETCH_PAGE_DELAY_MS } from './constants.js';

const MESSAGE_ENRICHMENT_GROUPS = new Set([
  'supervalidator-announce',
  'tokenomics-announce',
  'sv-announce',
]);

let _activeController = null;

export function runEnrichmentInBackground(cacheTimestamp) {
  if (_activeController) {
    console.log('📨 Aborting previous enrichment job');
    _activeController.abort();
  }

  const controller = new AbortController();
  _activeController = controller;

  _runEnrichment(controller, cacheTimestamp).catch((err) => {
    if (err.name !== 'AbortError') {
      console.error('📨 Enrichment job failed:', err.message);
    }
  });
}

async function _runEnrichment(controller, cacheTimestamp) {
  const signal = controller.signal;
  const startedAt = new Date().toISOString();

  let total = 0;
  let processed = 0;
  let enriched = 0;
  let totalMessages = 0;

  try {
    const cache = await readCache();
    if (!cache) {
      throw new Error('No cache found — enrichment cannot run without initial data');
    }

    if (cache.cachedAt !== cacheTimestamp) {
      await writeEnrichmentStatus({
        state: 'aborted',
        startedAt,
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
    const toEnrich = allTopics.filter((t) => MESSAGE_ENRICHMENT_GROUPS.has(t.groupName));
    total = toEnrich.length;

    await writeEnrichmentStatus({
      state: 'running',
      startedAt,
      completedAt: null,
      total,
      processed: 0,
      enriched: 0,
      totalMessages: 0,
      error: null,
    });

    const topicById = new Map(allTopics.map((t) => [t.id, t]));

    const lifecycleItems = cache.lifecycleItems ?? [];
    const lifecycleTopicById = new Map();

    for (const item of lifecycleItems) {
      for (const t of (item.topics ?? [])) {
        lifecycleTopicById.set(t.id, t);
      }
      for (const stageTopics of Object.values(item.stages ?? {})) {
        for (const t of stageTopics) {
          lifecycleTopicById.set(t.id, t);
        }
      }
    }

    const BATCH_SIZE = 25;

    for (const topic of toEnrich) {
      if (signal.aborted) {
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

        const lcTopic = lifecycleTopicById.get(topic.id);
        if (lcTopic) {
          const existing = new Set(lcTopic.linkedUrls ?? []);
          for (const u of messageUrls) existing.add(u);
          lcTopic.linkedUrls = [...existing];
        }

        enriched++;
        totalMessages += messageUrls.length;
      }

      processed++;

      if (processed % BATCH_SIZE === 0) {
        const latest = await readCache();
        if (latest && latest.cachedAt === cacheTimestamp) {
          await writeCache({ ...latest, allTopics, lifecycleItems });
        }

        await writeEnrichmentStatus({
          state: 'running',
          startedAt,
          completedAt: null,
          total,
          processed,
          enriched,
          totalMessages,
          error: null,
        });
      }

      await delay(FETCH_PAGE_DELAY_MS);
    }

    if (!signal.aborted) {
      const latest = await readCache();
      if (latest && latest.cachedAt === cacheTimestamp) {
        await writeCache({ ...latest, allTopics, lifecycleItems });
      }
    }

    await writeEnrichmentStatus({
      state: signal.aborted ? 'aborted' : 'complete',
      startedAt,
      completedAt: new Date().toISOString(),
      total,
      processed,
      enriched,
      totalMessages,
      error: null,
    });
  } catch (err) {
    if (err.name === 'AbortError') {
      await writeEnrichmentStatus({
        state: 'aborted',
        startedAt,
        completedAt: new Date().toISOString(),
        total,
        processed,
        enriched,
        totalMessages,
        error: null,
      });
      return;
    }

    console.error('📨 Enrichment error:', err.message);

    await writeEnrichmentStatus({
      state: 'error',
      startedAt,
      completedAt: new Date().toISOString(),
      total,
      processed,
      enriched,
      totalMessages,
      error: err.message,
    });

    throw err;
  } finally {
    if (_activeController === controller) {
      _activeController = null;
    }
  }
}
