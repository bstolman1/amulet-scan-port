import express from 'express';

const router = express.Router();

const API_KEY = process.env.GROUPS_IO_API_KEY;
const GROUP_NAME = 'supervalidator-announce';
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
  // First try to get subscriptions to find the group_id
  const subsUrl = `${BASE_URL}/api/v1/getsubs`;
  console.log('Getting subscriptions to find group_id...');
  
  const response = await fetch(subsUrl, {
    headers: { 'Authorization': `Bearer ${API_KEY}` },
  });
  
  if (response.ok) {
    const data = await response.json();
    console.log('Subscriptions response:', JSON.stringify(data, null, 2).substring(0, 500));
    
    const subs = data.data || [];
    const group = subs.find(s => 
      s.group_name === GROUP_NAME || 
      s.name === GROUP_NAME ||
      (s.group_name || '').includes('supervalidator')
    );
    
    if (group) {
      console.log('Found group:', group.group_id || group.id);
      return group.group_id || group.id;
    }
  } else {
    console.log('Subscriptions failed:', response.status, await response.text());
  }
  
  return null;
}

// Fetch announcements from Groups.io
router.get('/', async (req, res) => {
  const limit = parseInt(req.query.limit) || 100;
  
  if (!API_KEY) {
    return res.status(500).json({ 
      error: 'GROUPS_IO_API_KEY not configured',
      announcements: [] 
    });
  }

  try {
    // First, try to get the group_id
    const groupId = await getGroupId();
    
    let topicsUrl;
    if (groupId) {
      topicsUrl = `${BASE_URL}/api/v1/gettopics?group_id=${groupId}&limit=${limit}`;
    } else {
      // Fallback to group_name
      topicsUrl = `${BASE_URL}/api/v1/gettopics?group_name=${GROUP_NAME}&limit=${limit}`;
    }
    
    console.log('Fetching topics from:', topicsUrl);
    
    const topicsResponse = await fetch(topicsUrl, {
      headers: { 'Authorization': `Bearer ${API_KEY}` },
    });

    console.log('Topics response status:', topicsResponse.status);

    if (topicsResponse.ok) {
      const topicsData = await topicsResponse.json();
      console.log('Topics data:', JSON.stringify(topicsData, null, 2).substring(0, 1000));
      
      const topics = topicsData.data || topicsData.topics || [];
      console.log('Found', topics.length, 'topics');
      
      const announcements = topics.map((topic, idx) => ({
        id: topic.id?.toString() || `topic-${idx}`,
        subject: topic.subject || topic.title || 'Untitled',
        date: topic.created || topic.updated || topic.date || new Date().toISOString(),
        content: topic.snippet || topic.body || topic.preview || '',
        excerpt: (topic.snippet || topic.body || topic.preview || '').substring(0, 500),
        sourceUrl: topic.permalink || `${BASE_URL}/g/${GROUP_NAME}/topic/${topic.id}`,
        linkedUrls: extractUrls(topic.snippet || topic.body || ''),
        messageCount: topic.num_msgs || topic.msg_count || 1,
        hashtags: topic.hashtags || [],
      }));

      return res.json({ 
        announcements, 
        source: 'topics',
        total: topicsData.total_count || topics.length,
        groupId 
      });
    }

    // Topics failed - log the error
    const topicsError = await topicsResponse.text();
    console.log('Topics error:', topicsError);
    
    throw new Error(`Groups.io API failed: ${topicsResponse.status} - ${topicsError}`);

  } catch (error) {
    console.error('Error fetching announcements:', error);
    res.status(500).json({ 
      error: error.message,
      announcements: [] 
    });
  }
});

export default router;
