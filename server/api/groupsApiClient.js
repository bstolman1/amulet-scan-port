/**
 * Groups.io API client.
 *
 * Fixes over the original:
 *  - AbortSignal is threaded through every fetch call so the global timeout
 *    actually cancels in-flight requests (the original ignored it).
 *  - All magic numbers are constants.
 *  - Rate-limit retry and pagination logic are isolated functions.
 *  - getApiKey() lazy read is still here so env is read after dotenv loads.
 *
 * Added:
 *  - fetchTopicMessages — fetches all message permalinks for a single topic.
 *    Used by dataFetcher to populate linkedUrls with message-level URLs so
 *    vote reasonUrls (/message/NNN) can be matched to their parent lifecycle card.
 */

import {
  BASE_URL,
  GOVERNANCE_GROUPS,
  MAX_TOPICS_PER_GROUP,
  TOPICS_PAGE_LIMIT,
  FETCH_RETRY_MAX,
  FETCH_RETRY_DELAY_MS,
  FETCH_PAGE_DELAY_MS,
  FETCH_GROUP_DELAY_MS,
  FETCH_TIMEOUT_MS,
} from './constants.js';

const getApiKey = () => process.env.GROUPS_IO_API_KEY;

export const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

// ── Auth header ───────────────────────────────────────────────────────────

function authHeader() {
  return { Authorization: `Bearer ${getApiKey()}` };
}

// ── Subscriptions / group map ─────────────────────────────────────────────

/**
 * Fetch all governance group subscriptions and return a name→metadata map.
 * @param {AbortSignal} [signal]
 * @returns {Promise<Record<string, object>>}
 */
export async function getSubscribedGroups(signal) {
  const url = `${BASE_URL}/api/v1/getsubs?limit=100`;
  const response = await fetch(url, {
    headers: authHeader(),
    signal: AbortSignal.any([AbortSignal.timeout(FETCH_TIMEOUT_MS), signal].filter(Boolean)),
  });

  if (!response.ok) {
    throw new Error(`Failed to fetch subscriptions: ${response.status}`);
  }

  const data = await response.json();
  const subs = data.data ?? [];
  const groupMap = {};

  for (const sub of subs) {
    const groupName = sub.group_name ?? '';
    const match = groupName.match(/\+([^+]+)$/);
    if (!match) continue;

    const subgroupName = match[1];
    if (!GOVERNANCE_GROUPS[subgroupName]) continue;

    console.log(`Group ${subgroupName} URL: ${BASE_URL}/g/${subgroupName}`);
    groupMap[subgroupName] = {
      id: sub.group_id,
      fullName: groupName,
      urlName: subgroupName,
      ...GOVERNANCE_GROUPS[subgroupName],
    };
  }

  return groupMap;
}

// ── Topic pagination ──────────────────────────────────────────────────────

/**
 * Fetch up to maxTopics topics from a group, handling pagination and retries.
 *
 * @param {number} groupId
 * @param {string} groupName  - for logging
 * @param {AbortSignal} [signal]
 * @returns {Promise<object[]>}
 */
export async function fetchGroupTopics(groupId, groupName, signal) {
  const allTopics = [];
  let pageToken = null;
  let retries = 0;

  while (allTopics.length < MAX_TOPICS_PER_GROUP) {
    if (signal?.aborted) {
      console.warn(`[${groupName}] Fetch aborted by global timeout`);
      break;
    }

    let url = `${BASE_URL}/api/v1/gettopics?group_id=${groupId}&limit=${TOPICS_PAGE_LIMIT}`;
    if (pageToken) url += `&page_token=${pageToken}`;

    let response;
    try {
      response = await fetch(url, {
        headers: authHeader(),
        signal: AbortSignal.any(
          [AbortSignal.timeout(FETCH_TIMEOUT_MS), signal].filter(Boolean),
        ),
      });
    } catch (err) {
      if (err.name === 'AbortError' || signal?.aborted) {
        console.warn(`[${groupName}] Fetch aborted`);
        break;
      }
      if (retries < FETCH_RETRY_MAX) {
        retries++;
        console.log(`[${groupName}] Connection error, retrying ${retries}/${FETCH_RETRY_MAX}...`);
        await delay(FETCH_RETRY_DELAY_MS);
        continue;
      }
      console.error(`[${groupName}] Fetch failed after retries:`, err.message);
      break;
    }

    if (!response.ok) {
      console.error(`[${groupName}] HTTP ${response.status}`);
      if ((response.status === 429 || response.status === 400) && retries < FETCH_RETRY_MAX) {
        retries++;
        console.log(`[${groupName}] Rate limited, retrying ${retries}/${FETCH_RETRY_MAX}...`);
        await delay(FETCH_RETRY_DELAY_MS);
        continue;
      }
      break;
    }

    const data = await response.json();
    const topics = data.data ?? [];
    allTopics.push(...topics);
    retries = 0;

    if (!data.has_more || !data.next_page_token) break;
    pageToken = data.next_page_token;
    await delay(FETCH_PAGE_DELAY_MS);
  }

  return allTopics;
}

// ── Message fetching ──────────────────────────────────────────────────────

/**
 * Fetch all message permalinks for a single topic.
 *
 * Vote reasonUrls point to individual messages (/g/GROUP/message/NNN) rather
 * than topic URLs (/g/GROUP/topic/ID). Without indexing message URLs we cannot
 * match votes back to their lifecycle cards. This function fetches the message
 * list for a topic and returns the full permalink for each message.
 *
 * The Groups.io getmessages endpoint returns a page of messages. Each message
 * object has an `id` field; the permalink is constructed as:
 *   ${BASE_URL}/g/${groupUrlName}/message/${message.id}
 *
 * We only call this for announce-group topics (the groups whose URLs appear in
 * vote reasonUrls) to keep the extra API call count manageable. Caller is
 * responsible for filtering which topics to enrich.
 *
 * @param {number|string} topicId       - Groups.io topic ID
 * @param {number|string} groupId       - Groups.io group ID
 * @param {string}        groupUrlName  - URL slug, e.g. 'supervalidator-announce'
 * @param {AbortSignal}   [signal]
 * @returns {Promise<string[]>}         - list of absolute message permalink URLs
 */
export async function fetchTopicMessages(topicId, groupId, groupUrlName, signal) {
  const messageUrls = [];
  let pageToken = null;
  let retries = 0;
  // Messages per topic are typically < 20; cap at 10 pages (1000 msgs) as a safety limit
  const PAGE_CAP = 10;
  let page = 0;

  while (page < PAGE_CAP) {
    if (signal?.aborted) break;

    let url = `${BASE_URL}/api/v1/getmessages?group_id=${groupId}&topic_id=${topicId}&limit=100`;
    if (pageToken) url += `&page_token=${pageToken}`;

    let response;
    try {
      response = await fetch(url, {
        headers: authHeader(),
        signal: AbortSignal.any(
          [AbortSignal.timeout(FETCH_TIMEOUT_MS), signal].filter(Boolean),
        ),
      });
    } catch (err) {
      if (err.name === 'AbortError' || signal?.aborted) break;
      if (retries < FETCH_RETRY_MAX) {
        retries++;
        await delay(FETCH_RETRY_DELAY_MS);
        continue;
      }
      console.error(`[fetchTopicMessages] topic=${topicId} failed:`, err.message);
      break;
    }

    if (!response.ok) {
      if ((response.status === 429 || response.status === 400) && retries < FETCH_RETRY_MAX) {
        retries++;
        await delay(FETCH_RETRY_DELAY_MS);
        continue;
      }
      // 404 = topic has no messages yet; not an error
      if (response.status !== 404) {
        console.warn(`[fetchTopicMessages] topic=${topicId} HTTP ${response.status}`);
      }
      break;
    }

    const data = await response.json();
    const messages = data.data ?? [];

    for (const msg of messages) {
      if (msg.id) {
        messageUrls.push(`${BASE_URL}/g/${groupUrlName}/message/${msg.id}`);
      }
    }

    retries = 0;
    page++;

    if (!data.has_more || !data.next_page_token) break;
    pageToken = data.next_page_token;
    await delay(FETCH_PAGE_DELAY_MS);
  }

  return messageUrls;
}

// ── Orchestrator ──────────────────────────────────────────────────────────

/**
 * Fetch all topics from all governance groups sequentially.
 * Sequential (not concurrent) to respect rate limits.
 *
 * @param {AbortSignal} [signal] - global timeout signal
 * @returns {Promise<{ groupMap: object, rawTopics: object[] }>}
 */
export async function fetchAllGovernanceTopics(signal) {
  const groupMap = await getSubscribedGroups(signal);
  console.log('Found governance groups:', Object.keys(groupMap));

  const rawTopics = [];

  for (const [name, group] of Object.entries(groupMap)) {
    if (signal?.aborted) break;
    console.log(`Fetching topics from ${name} (ID: ${group.id})...`);
    const topics = await fetchGroupTopics(group.id, name, signal);
    console.log(`Got ${topics.length} topics from ${name}`);
    rawTopics.push({ group, topics });
    await delay(FETCH_GROUP_DELAY_MS);
  }

  return { groupMap, rawTopics };
}
