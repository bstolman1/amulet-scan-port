/**
 * Groups.io Content Fetcher
 * 
 * Fetches full post content from groups.io API and caches it.
 * Content is fetched ONCE per post and never re-fetched unless explicitly requested.
 * 
 * Design principle: "LLMs may read raw human text exactly once per artifact;
 * results are cached and treated as authoritative governance metadata."
 */

import { 
  getCachedContent, 
  cachePostContent, 
  cachePostsBulk,
  getContentCacheStats 
} from './post-content-cache.js';

// Read API_KEY lazily to ensure env.js has loaded
const getApiKey = () => process.env.GROUPS_IO_API_KEY;
const BASE_URL = 'https://lists.sync.global';

// Rate limiting
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

/**
 * Fetch a single topic's full content from groups.io
 * Returns cached content if available
 */
export async function fetchTopicContent(topicId, groupId, options = {}) {
  const { forceRefresh = false } = options;
  
  // Check cache first (unless force refresh)
  if (!forceRefresh) {
    const cached = getCachedContent(topicId);
    if (cached && cached.body) {
      return cached;
    }
  }
  
  if (!getApiKey()) {
    console.warn('⚠️ GROUPS_IO_API_KEY not set, cannot fetch content');
    return null;
  }
  
  try {
    // Fetch the topic details (includes full body)
    const url = `${BASE_URL}/api/v1/gettopic?topic_id=${topicId}`;
    
    const response = await fetch(url, {
      headers: { 'Authorization': `Bearer ${getApiKey()}` },
    });
    
    if (!response.ok) {
      console.error(`Failed to fetch topic ${topicId}: ${response.status}`);
      return null;
    }
    
    const data = await response.json();
    const topic = data.topic || data;
    
    // Also fetch the first message for full body content
    let fullBody = topic.body || topic.snippet || '';
    
    // Try to get the first message which usually has the full content
    try {
      const msgsUrl = `${BASE_URL}/api/v1/getmessages?topic_id=${topicId}&limit=1`;
      const msgsResponse = await fetch(msgsUrl, {
        headers: { 'Authorization': `Bearer ${getApiKey()}` },
      });
      
      if (msgsResponse.ok) {
        const msgsData = await msgsResponse.json();
        const messages = msgsData.data || [];
        if (messages.length > 0 && messages[0].body) {
          fullBody = messages[0].body;
        }
      }
    } catch (err) {
      // Ignore message fetch errors, use topic body
    }
    
    // Clean the body text
    const cleanBody = cleanBodyText(fullBody);
    
    // Cache the content
    const cached = cachePostContent({
      topicId: topicId,
      subject: topic.subject || topic.title || '',
      body: cleanBody,
      author: topic.poster_email || topic.poster_name || null,
      timestamp: topic.created || null,
      sourceUrl: `${BASE_URL}/g/topic/${topicId}`,
      groupName: groupId || null,
    });
    
    return cached;
  } catch (err) {
    console.error(`Error fetching topic ${topicId}:`, err.message);
    return null;
  }
}

/**
 * Clean body text for LLM consumption
 * - Strip quoted replies (lines starting with >)
 * - Strip email signatures (after -- or ___ lines)
 * - Normalize whitespace
 * - Truncate to reasonable length
 */
function cleanBodyText(body, maxLength = 6000) {
  if (!body) return '';
  
  let cleaned = body;
  
  // Remove HTML tags if present
  cleaned = cleaned.replace(/<[^>]+>/g, ' ');
  
  // Remove quoted replies (lines starting with >)
  cleaned = cleaned.split('\n')
    .filter(line => !line.trim().startsWith('>'))
    .join('\n');
  
  // Remove email signatures (after -- or ___ lines)
  const sigPatterns = [/^--\s*$/m, /^_{3,}$/m, /^-{3,}$/m];
  for (const pattern of sigPatterns) {
    const match = cleaned.match(pattern);
    if (match) {
      cleaned = cleaned.substring(0, match.index);
    }
  }
  
  // Remove common email footer patterns
  cleaned = cleaned.replace(/Sent from my \w+/gi, '');
  cleaned = cleaned.replace(/On .+ wrote:/g, '');
  
  // Normalize whitespace
  cleaned = cleaned.replace(/\r\n/g, '\n');
  cleaned = cleaned.replace(/\n{3,}/g, '\n\n');
  cleaned = cleaned.replace(/[ \t]+/g, ' ');
  cleaned = cleaned.trim();
  
  // Truncate to max length (preserving word boundaries)
  if (cleaned.length > maxLength) {
    cleaned = cleaned.substring(0, maxLength);
    const lastSpace = cleaned.lastIndexOf(' ');
    if (lastSpace > maxLength - 100) {
      cleaned = cleaned.substring(0, lastSpace);
    }
    cleaned += '\n[truncated]';
  }
  
  return cleaned;
}

/**
 * Batch fetch content for multiple topics
 * Only fetches topics not already in cache
 */
export async function fetchTopicsContentBatch(topics, options = {}) {
  const { forceRefresh = false, onProgress = null } = options;
  
  if (!getApiKey()) {
    console.warn('⚠️ GROUPS_IO_API_KEY not set, cannot fetch content');
    return { fetched: 0, cached: topics.length, failed: 0 };
  }
  
  const toFetch = [];
  const alreadyCached = [];
  
  for (const topic of topics) {
    if (!forceRefresh) {
      const cached = getCachedContent(topic.id);
      if (cached && cached.body) {
        alreadyCached.push(topic.id);
        continue;
      }
    }
    toFetch.push(topic);
  }
  
  if (toFetch.length === 0) {
    console.log(`📄 All ${alreadyCached.length} topics already in content cache`);
    return { fetched: 0, cached: alreadyCached.length, failed: 0 };
  }
  
  console.log(`📄 Fetching content for ${toFetch.length} topics (${alreadyCached.length} already cached)...`);
  
  const results = [];
  let failed = 0;
  
  // Fetch in small batches with rate limiting
  const batchSize = 3;
  for (let i = 0; i < toFetch.length; i += batchSize) {
    const batch = toFetch.slice(i, i + batchSize);
    
    const batchPromises = batch.map(async (topic) => {
      try {
        const content = await fetchTopicContent(topic.id, topic.groupName);
        if (content) {
          return {
            topicId: topic.id,
            subject: content.subject,
            body: content.body,
            author: content.author,
            timestamp: content.timestamp,
            sourceUrl: content.source_url,
            groupName: topic.groupName,
          };
        }
        failed++;
        return null;
      } catch (err) {
        failed++;
        return null;
      }
    });
    
    const batchResults = await Promise.all(batchPromises);
    results.push(...batchResults.filter(r => r !== null));
    
    if (onProgress) {
      onProgress(Math.min(i + batchSize, toFetch.length), toFetch.length);
    }
    
    // Rate limit between batches
    if (i + batchSize < toFetch.length) {
      await delay(500);
    }
  }
  
  // Bulk cache all results
  if (results.length > 0) {
    cachePostsBulk(results);
  }
  
  console.log(`📄 Content fetch complete: ${results.length} fetched, ${failed} failed, ${alreadyCached.length} were cached`);
  
  return { 
    fetched: results.length, 
    cached: alreadyCached.length, 
    failed,
    total: topics.length,
  };
}

/**
 * Get content for a topic (from cache or fetch)
 */
export async function getTopicContent(topicId, groupName) {
  // Try cache first
  const cached = getCachedContent(topicId);
  if (cached && cached.body) {
    return cached;
  }
  
  // Fetch if not cached
  return await fetchTopicContent(topicId, groupName);
}

/**
 * Check if we have content cached for a topic
 */
export function hasContentCached(topicId) {
  const cached = getCachedContent(topicId);
  return cached && cached.body && cached.body.length > 0;
}

export { getContentCacheStats };
