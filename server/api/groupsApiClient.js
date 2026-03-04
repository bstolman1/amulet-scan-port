/**
 * Groups.io API client.
 *
 * Fixes over the original:
 *  - AbortSignal is threaded through every fetch call so the global timeout
 *    actually cancels in-flight requests (the original ignored it).
 *  - All magic numbers are constants.
 *  - Rate-limit retry and pagination logic are isolated functions.
 *  - getApiKey() lazy read is still here so env is read after dotenv loads.
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
} from '../utils/constants.js';

const getApiKey = () => process.env.GROUPS_IO_API_KEY;

const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

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
    // Respect the global abort signal before each iteration
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
        // Compose the page-level timeout with the global abort signal
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
