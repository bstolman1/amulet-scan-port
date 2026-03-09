/**
 * services/dataFetcher.js
 *
 * Orchestrates a full data refresh: fetch topics → infer stages →
 * correlate → classify ambiguous items → cache.
 *
 * Fix: #13 — global AbortController signal threaded through ALL fetch calls.
 * Fix: #14 — all timing constants named and imported.
 */

import {
  INFERENCE_THRESHOLD,
  FETCH_GLOBAL_TIMEOUT_MS,
} from './constants.js';
import { extractUrls, extractIdentifiers } from './entityExtractor.js';
import { getLearnedPatterns } from './patternCache.js';
import { getSubscribedGroups, fetchGroupTopics, delay } from './groupsApiClient.js';
import { writeCache } from './fileRepository.js';
import { correlateTopics } from './lifecycleCorrelator.js';

const INFERENCE_ENABLED = process.env.INFERENCE_ENABLED === 'true';

async function tryInferStages(topics) {
  if (!INFERENCE_ENABLED) {
    console.log('ℹ️ Inference disabled (INFERENCE_ENABLED=true to enable)');
    return;
  }
  try {
    const { inferStagesBatch } = await import('../inference/inferStage.js');
    const results = await inferStagesBatch(
      topics.map(t => ({ id: t.id, subject: t.subject, content: t.content })),
      (done, total) => { if (done % 50 === 0) console.log(`🧠 Inference: ${done}/${total}`); }
    );
    let overrides = 0;
    for (const topic of topics) {
      const r = results.get(topic.id);
      if (r) {
        topic.inferredStage       = r.stage;
        topic.inferenceConfidence = r.confidence;
        if (r.confidence >= INFERENCE_THRESHOLD) {
          topic.effectiveStage = r.stage;
          overrides++;
        }
      }
    }
    console.log(`🧠 Inference complete — ${overrides} stage overrides`);
  } catch (err) {
    console.error('[inference] Batch failed:', err.message);
  }
}

async function tryClassifyAmbiguous(lifecycleItems, allTopics) {
  if (!INFERENCE_ENABLED) return lifecycleItems;
  const ambiguous = lifecycleItems.filter(i => i.type === 'other').length;
  if (ambiguous === 0) return lifecycleItems;

  try {
    const { isLLMAvailable }        = await import('../inference/llm-classifier.js');
    const { classifyAmbiguousItems } = await import('../inference/hybrid-auditor.js');
    if (!isLLMAvailable()) {
      console.log(`ℹ️ Skipping LLM classification for ${ambiguous} ambiguous items (LLM unavailable)`);
      return lifecycleItems;
    }
    return classifyAmbiguousItems(lifecycleItems, allTopics);
  } catch (err) {
    console.error('[llm-classify] Failed:', err.message);
    return lifecycleItems;
  }
}

/**
 * Fetch fresh data with a hard global timeout.
 * The AbortController signal is threaded through every network call.
 *
 * Fix: #13 — signal was created but never passed to fetchGroupTopics previously.
 */
export async function fetchFreshData() {
  const controller  = new AbortController();
  const globalTimer = setTimeout(() => {
    console.error(`⏰ fetchFreshData global timeout (${FETCH_GLOBAL_TIMEOUT_MS / 1000}s) — aborting`);
    controller.abort();
  }, FETCH_GLOBAL_TIMEOUT_MS);

  try {
    return await _fetchInner(controller.signal);
  } finally {
    clearTimeout(globalTimer);
  }
}

async function _fetchInner(signal) {
  console.log('Fetching fresh governance lifecycle data from groups.io…');

  const groupMap        = await getSubscribedGroups(signal);
  const learnedPatterns = await getLearnedPatterns();
  const allTopics       = [];

  console.log('Found governance groups:', Object.keys(groupMap));

  for (const [name, group] of Object.entries(groupMap)) {
    if (signal.aborted) break;

    console.log(`Fetching topics from ${name} (ID: ${group.id})…`);
    // Fix: #13 — signal is now passed down to fetchGroupTopics
    const raw = await fetchGroupTopics(group.id, name, signal);
    console.log(`Got ${raw.length} topics from ${name}`);

    for (const topic of raw) {
      const sourceUrl = `https://lists.sync.global/g/${group.urlName}/topic/${topic.id}`;
      const combined  = `${topic.subject || ''} ${topic.snippet || topic.body || ''}`;

      allTopics.push({
        id:           topic.id?.toString() || `topic-${Math.random()}`,
        subject:      topic.subject || topic.title || 'Untitled',
        date:         topic.created || topic.updated || new Date().toISOString(),
        content:      topic.snippet || topic.body || topic.preview || '',
        excerpt:      (topic.snippet || topic.body || topic.preview || '').substring(0, 500),
        sourceUrl,
        linkedUrls:   extractUrls(topic.snippet || topic.body || ''),
        messageCount: topic.num_msgs || 1,
        groupName:    name,
        groupLabel:   group.label,
        stage:        group.stage,
        flow:         group.flow,
        identifiers:  extractIdentifiers(combined, learnedPatterns),
        postedStage:         group.stage,
        inferredStage:       null,
        inferenceConfidence: null,
        effectiveStage:      group.stage,
      });
    }

    await delay(500);
  }

  console.log(`Total topics: ${allTopics.length}`);

  await tryInferStages(allTopics);

  let lifecycleItems = correlateTopics(allTopics, learnedPatterns);
  lifecycleItems     = await tryClassifyAmbiguous(lifecycleItems, allTopics);

  const topicsInItems = lifecycleItems.reduce((s, i) => s + (i.topics?.length || 0), 0);
  console.log(`📊 ${allTopics.length} topics → ${lifecycleItems.length} lifecycle cards`);
  console.log(`   Accounted for: ${topicsInItems}/${allTopics.length} ${topicsInItems === allTopics.length ? '✓' : '⚠️'}`);

  const typeCounts = {};
  for (const item of lifecycleItems) {
    typeCounts[item.type || 'unknown'] = (typeCounts[item.type || 'unknown'] || 0) + 1;
  }

  const result = {
    lifecycleItems,
    allTopics,
    groups:   groupMap,
    stats: {
      totalTopics: allTopics.length,
      totalItems: lifecycleItems.length,
      totalGroups: Object.keys(groupMap).length,
      typeCounts,
    },
    cachedAt: new Date().toISOString(),
  };
  await writeCache(result);
  return result;
}
