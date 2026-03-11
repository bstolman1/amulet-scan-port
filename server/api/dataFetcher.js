/**
 * services/dataFetcher.js
 *
 * Orchestrates a full data refresh: fetch topics → enrich message URLs →
 * infer stages → correlate → classify ambiguous items → cache.
 *
 * Fix: #13 — global AbortController signal threaded through ALL fetch calls.
 * Fix: #14 — all timing constants named and imported.
 *
 * Added: message URL enrichment for announce-group topics.
 *   Vote reasonUrls point to individual messages (/g/GROUP/message/NNN) rather
 *   than topic URLs (/g/GROUP/topic/ID). Without indexing message URLs the
 *   frontend vote-matching code in GovernanceFlow.tsx cannot resolve these votes
 *   to their lifecycle cards. We fetch message IDs for topics in the announce
 *   groups and append their permalink URLs to each topic's linkedUrls array.
 *   This adds one API call per topic in those groups; we do it sequentially with
 *   the existing FETCH_PAGE_DELAY_MS inter-call delay to stay within rate limits.
 */

import {
  INFERENCE_THRESHOLD,
  FETCH_GLOBAL_TIMEOUT_MS,
  FETCH_PAGE_DELAY_MS,
} from './constants.js';
import { extractUrls, extractIdentifiers } from './entityExtractor.js';
import { getLearnedPatterns } from './patternCache.js';
import { getSubscribedGroups, fetchGroupTopics, fetchTopicMessages, delay } from './groupsApiClient.js';
import { writeCache } from './fileRepository.js';
import { correlateTopics } from './lifecycleCorrelator.js';

const INFERENCE_ENABLED = process.env.INFERENCE_ENABLED === 'true';

/**
 * Groups whose topics should be enriched with per-message URLs.
 *
 * These are the groups whose URLs appear in vote reasonUrls. Only announce
 * groups are included — fetching messages for discussion/tokenomics groups
 * would add hundreds of API calls for topics that votes never reference.
 *
 * Extend this set if new groups start appearing in vote reasonUrls.
 */
const MESSAGE_ENRICHMENT_GROUPS = new Set([
  'supervalidator-announce',
  'tokenomics-announce',
  'sv-announce',
]);

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
 * Enrich topics from announce groups with per-message permalink URLs.
 *
 * After this runs, each announce-group topic's linkedUrls will contain both:
 *   - URLs extracted from the topic body/snippet (existing behaviour)
 *   - The permalink URL for every message in that topic (new)
 *
 * This allows GovernanceFlow.tsx's URL-based vote matching to resolve votes
 * whose reasonUrl points to /g/GROUP/message/NNN rather than the topic URL.
 *
 * We process topics sequentially (not in parallel) to avoid bursting the
 * Groups.io rate limit. The inter-call delay matches FETCH_PAGE_DELAY_MS.
 *
 * @param {object[]}    topics  - all topics built by _fetchInner
 * @param {AbortSignal} signal
 */
async function enrichWithMessageUrls(topics, signal) {
  const toEnrich = topics.filter(t => MESSAGE_ENRICHMENT_GROUPS.has(t.groupName));

  if (toEnrich.length === 0) {
    console.log('📨 No announce-group topics to enrich with message URLs');
    return;
  }

  console.log(`📨 Enriching ${toEnrich.length} announce-group topics with message URLs…`);
  let enriched = 0;
  let totalMessages = 0;

  for (const topic of toEnrich) {
    if (signal?.aborted) {
      console.warn('📨 Message URL enrichment aborted by global timeout');
      break;
    }

    // topic.groupId is set from group.id in _fetchInner below
    const messageUrls = await fetchTopicMessages(
      topic.rawId,       // the original numeric Groups.io topic ID
      topic.groupId,     // Groups.io group ID
      topic.groupName,   // URL slug, e.g. 'supervalidator-announce'
      signal,
    );

    if (messageUrls.length > 0) {
      // Merge with any URLs already extracted from the snippet/body,
      // deduplicating so the array stays clean.
      const existing = new Set(topic.linkedUrls);
      for (const u of messageUrls) existing.add(u);
      topic.linkedUrls = [...existing];
      enriched++;
      totalMessages += messageUrls.length;
    }

    await delay(FETCH_PAGE_DELAY_MS);
  }

  console.log(`📨 Message URL enrichment complete: ${enriched}/${toEnrich.length} topics enriched, ${totalMessages} message URLs indexed`);
}

/**
 * Fetch fresh data with a hard global timeout.
 * The AbortController signal is threaded through every network call.
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
    const raw = await fetchGroupTopics(group.id, name, signal);
    console.log(`Got ${raw.length} topics from ${name}`);

    for (const topic of raw) {
      const sourceUrl = `https://lists.sync.global/g/${group.urlName}/topic/${topic.id}`;
      const combined  = `${topic.subject || ''} ${topic.snippet || topic.body || ''}`;

      allTopics.push({
        id:           topic.id?.toString() || `topic-${Math.random()}`,
        // rawId and groupId are needed by enrichWithMessageUrls to call
        // fetchTopicMessages. rawId preserves the original numeric ID before
        // it gets stringified; groupId is the Groups.io group numeric ID.
        rawId:        topic.id,
        groupId:      group.id,
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

  // Enrich announce-group topics with per-message URLs BEFORE correlating,
  // so that the enriched linkedUrls are present in the lifecycle items that
  // the frontend receives. This is the fix for vote reasonUrl matching.
  await enrichWithMessageUrls(allTopics, signal);

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
