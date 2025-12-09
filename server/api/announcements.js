import express from 'express';

const router = express.Router();

const API_KEY = process.env.GROUPS_IO_API_KEY;
const GROUP_NAME = 'supervalidator-announce'; // The subgroup name we're looking for
const BASE_URL = 'https://lists.sync.global';

// Helper to extract ALL URLs from text
function extractUrls(text) {
  if (!text) return [];
  const urlRegex = /(https?:\/\/[^\s<>"{}|\\^`\[\]]+)/g;
  const matches = text.match(urlRegex) || [];
  return [...new Set(matches)]; // Remove duplicates
}

// Get group ID from subscriptions
async function getGroupId() {
  const subsUrl = `${BASE_URL}/api/v1/getsubs?limit=100`;
  console.log('Getting subscriptions to find group_id...');
  
  const response = await fetch(subsUrl, {
    headers: { 'Authorization': `Bearer ${API_KEY}` },
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
async function fetchAllTopics(groupId, maxTopics = 500) {
  const allTopics = [];
  let pageToken = null;
  let pageCount = 0;
  
  while (allTopics.length < maxTopics) {
    pageCount++;
    // Groups.io API allows up to 100 per page, but let's try higher
    let url = `${BASE_URL}/api/v1/gettopics?group_id=${groupId}&limit=200`;
    if (pageToken) {
      url += `&page_token=${pageToken}`;
    }
    
    console.log(`Fetching topics page ${pageCount}:`, url);
    
    const response = await fetch(url, {
      headers: { 'Authorization': `Bearer ${API_KEY}` },
    });
    
    if (!response.ok) {
      const error = await response.text();
      throw new Error(`Topics API failed: ${response.status} - ${error}`);
    }
    
    const data = await response.json();
    const topics = data.data || [];
    
    console.log(`Page ${pageCount}: Got ${topics.length} topics, has_more=${data.has_more}, next_page_token=${data.next_page_token}`);
    console.log(`Total so far: ${allTopics.length + topics.length}/${data.total_count || '?'}`);
    
    allTopics.push(...topics);
    
    // Check if there are more pages
    if (!data.has_more || !data.next_page_token) {
      console.log('No more pages to fetch');
      break;
    }
    
    pageToken = data.next_page_token;
  }
  
  return allTopics;
}

// Fetch announcements from Groups.io
router.get('/', async (req, res) => {
  const limit = parseInt(req.query.limit) || 200;
  
  if (!API_KEY) {
    return res.status(500).json({ 
      error: 'GROUPS_IO_API_KEY not configured',
      announcements: [] 
    });
  }

  try {
    const groupId = await getGroupId();
    
    if (!groupId) {
      throw new Error('Could not find group_id. Make sure your API key account is subscribed to supervalidator-announce');
    }
    
    const topics = await fetchAllTopics(groupId, limit);
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
  }
});

export default router;
