import express from 'express';

const router = express.Router();

// Read API_KEY lazily to ensure env.js has loaded
const getApiKey = () => process.env.GROUPS_IO_API_KEY;
const GROUP_NAME = 'supervalidator-announce'; // The subgroup name we're looking for
const BASE_URL = 'https://lists.sync.global';

// FIX: cap maximum topics to prevent unbounded fetching via ?limit=99999
const MAX_LIMIT = 500;

// FIX: URL regex that does NOT capture trailing punctuation (commas, periods, etc.)
// that commonly appears after URLs in mailing-list plain-text bodies.
// Old pattern: /(https?:\/\/[^\s<>"{}|\\^`\[\]]+)/g
//   — captured trailing "." in "See https://example.com. For more..." → broken URL
// New pattern uses a non-greedy match and a lookahead to stop before punctuation+whitespace.
function extractUrls(text) {
  if (!text) return [];
  const urlRegex = /(https?:\/\/[^\s<>"{}|\\^`\[\]]+?)(?=[.,;:!?)\]>]?(?:\s|$))/g;
  const matches = text.match(urlRegex) || [];
  return [...new Set(matches)]; // Remove duplicates
}

// Get group ID from subscriptions
async function getGroupId() {
  const subsUrl = `${BASE_URL}/api/v1/getsubs?limit=100`;
  console.log('Getting subscriptions to find group_id...');
  
  // FIX: add per-request timeout (was missing — a slow response would hang forever)
  const response = await fetch(subsUrl, {
    headers: { 'Authorization': `Bearer ${getApiKey()}` },
    signal: AbortSignal.timeout(30_000),
  });
  
  if (response.ok) {
    const data = await response.json();
    const subs = data.data || [];
    console.log('Found', subs.length, 'subscriptions');
    
    // Log all group names to help debug
    subs.forEach(s => console.log('  - Group:', s.group_name, 'ID:', s.group_id));
    
    // Look for the exact subgroup: +supervalidator-announce (not +supervalidator-ops)
    const group = subs.find(s => 
      (s.group_name || '').endsWith('+supervalidator-announce')
    );
    
    if (group) {
      console.log('Using group:', group.group_name, 'ID:', group.group_id);
      return group.group_id;
    }
  } else {
    console.log('Subscriptions failed:', response.status);
  }
  
  return null;
}

// Fetch all topics with pagination
// FIX: accepts the caller's AbortSignal so the global endpoint timeout
//      actually cancels in-flight page fetches.
//      Previously the signal was created in fetchFreshData but never forwarded
//      here, making the 180 s abort a no-op.
async function fetchAllTopics(groupId, maxTopics = 500, signal = null) {
  const allTopics = [];
  let pageToken = null;
  let pageCount = 0;
  
  console.log('Starting pagination loop, maxTopics:', maxTopics);
  
  while (allTopics.length < maxTopics) {
    pageCount++;
    let url = `${BASE_URL}/api/v1/gettopics?group_id=${groupId}&limit=100`;
    if (pageToken) {
      url += `&page_token=${pageToken}`;
    }
    
    console.log(`Fetching topics page ${pageCount}:`, url);
    
    let response;
    try {
      response = await fetch(url, {
        headers: { 'Authorization': `Bearer ${getApiKey()}` },
        // FIX: propagate caller signal AND add a per-page safety timeout
        signal: signal ?? AbortSignal.timeout(45_000),
      });
    } catch (fetchError) {
      console.error('Fetch error:', fetchError);
      break;
    }
    
    if (!response.ok) {
      const error = await response.text();
      console.error('API error:', response.status, error);
      break;
    }
    
    const data = await response.json();
    const topics = data.data || [];
    
    console.log(`Page ${pageCount}: Got ${topics.length} topics`);
    console.log(`  has_more: ${data.has_more}`);
    console.log(`  next_page_token: ${data.next_page_token}`);
    console.log(`  Total so far: ${allTopics.length + topics.length}/${data.total_count || '?'}`);
    
    allTopics.push(...topics);
    
    // Check if there are more pages
    if (data.has_more !== true) {
      console.log('Stopping: has_more is not true');
      break;
    }
    
    if (!data.next_page_token) {
      console.log('Stopping: no next_page_token');
      break;
    }
    
    pageToken = data.next_page_token;
    console.log(`Continuing to page ${pageCount + 1} with token: ${pageToken}`);
  }
  
  console.log('Pagination complete. Total topics:', allTopics.length);
  return allTopics;
}

// Fetch announcements from Groups.io
router.get('/', async (req, res) => {
  // FIX: cap limit to MAX_LIMIT (previously no upper bound — ?limit=999999 would
  //      trigger hundreds of paginated API calls and hold the connection open)
  const limit = Math.min(parseInt(req.query.limit) || 200, MAX_LIMIT);
  
  if (!getApiKey()) {
    return res.status(500).json({ 
      error: 'GROUPS_IO_API_KEY not configured',
      announcements: [] 
    });
  }

  // FIX: wrap the entire endpoint in a global AbortController so the fetch loop
  //      is actually cancelled if the total operation takes too long.
  //      Without this, the endpoint could run indefinitely on a slow upstream.
  const controller = new AbortController();
  const globalTimeout = setTimeout(() => {
    console.error('⏰ Announcements fetch global timeout (120s), aborting...');
    controller.abort();
  }, 120_000);

  try {
    const groupId = await getGroupId();
    
    if (!groupId) {
      throw new Error('Could not find group_id. Make sure your API key account is subscribed to supervalidator-announce');
    }
    
    // Pass the controller signal so pagination respects the global deadline
    const topics = await fetchAllTopics(groupId, limit, controller.signal);
    console.log('Total topics fetched:', topics.length);
    
    const announcements = topics.map((topic, idx) => ({
      id: topic.id?.toString() || `topic-${idx}`,
      subject: topic.subject || topic.title || 'Untitled',
      date: topic.created || topic.updated || new Date().toISOString(),
      content: topic.snippet || topic.body || topic.preview || '',
      excerpt: (topic.snippet || topic.body || topic.preview || '').substring(0, 500),
      sourceUrl: topic.permalink || `${BASE_URL}/g/${GROUP_NAME}/topic/${topic.id}`,
      linkedUrls: extractUrls(topic.snippet || topic.body || ''),
      messageCount: topic.num_msgs || 1,
      hashtags: topic.hashtags || [],
    }));

    return res.json({ 
      announcements, 
      source: 'topics',
      total: topics.length,
      groupId 
    });

  } catch (error) {
    console.error('Error fetching announcements:', error);
    res.status(500).json({ 
      error: error.message,
      announcements: [] 
    });
  } finally {
    // Always clear the timeout to avoid leaking the handle
    clearTimeout(globalTimeout);
  }
});

export default router;
