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
    // Try gettopics endpoint first (uses group_name parameter)
    const topicsUrl = `${BASE_URL}/api/v1/gettopics?group_name=${GROUP_NAME}&limit=${limit}`;
    console.log('Fetching topics from:', topicsUrl);
    
    const topicsResponse = await fetch(topicsUrl, {
      headers: {
        'Authorization': `Bearer ${API_KEY}`,
      },
    });

    console.log('Topics response status:', topicsResponse.status);

    if (topicsResponse.ok) {
      const topicsData = await topicsResponse.json();
      console.log('Topics data keys:', Object.keys(topicsData));
      
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
        total: topicsData.total_count || topics.length 
      });
    }

    // If topics failed, try getmessages
    const messagesUrl = `${BASE_URL}/api/v1/getmessages?group_name=${GROUP_NAME}&limit=${limit}`;
    console.log('Topics failed, trying messages:', messagesUrl);
    
    const messagesResponse = await fetch(messagesUrl, {
      headers: {
        'Authorization': `Bearer ${API_KEY}`,
      },
    });

    console.log('Messages response status:', messagesResponse.status);

    if (messagesResponse.ok) {
      const messagesData = await messagesResponse.json();
      console.log('Messages data keys:', Object.keys(messagesData));
      
      const messages = messagesData.data || messagesData.messages || [];
      console.log('Found', messages.length, 'messages');
      
      const announcements = messages.map((msg, idx) => ({
        id: msg.id?.toString() || `msg-${idx}`,
        subject: msg.subject || 'Untitled',
        date: msg.created || msg.date || new Date().toISOString(),
        content: msg.body || msg.snippet || msg.plain_body || '',
        excerpt: (msg.body || msg.snippet || msg.plain_body || '').substring(0, 500),
        sourceUrl: msg.permalink || `${BASE_URL}/g/${GROUP_NAME}/message/${msg.id}`,
        linkedUrls: extractUrls(msg.body || msg.plain_body || ''),
        sender: msg.sender_name || msg.from_name || msg.poster_name || 'Unknown',
      }));

      return res.json({ 
        announcements, 
        source: 'messages',
        total: messagesData.total_count || messages.length 
      });
    }

    // Both failed - return error with details
    const topicsError = await topicsResponse.text();
    const messagesError = await messagesResponse.text();
    
    throw new Error(`Topics API: ${topicsResponse.status} - ${topicsError}. Messages API: ${messagesResponse.status} - ${messagesError}`);

  } catch (error) {
    console.error('Error fetching announcements:', error);
    res.status(500).json({ 
      error: error.message,
      announcements: [] 
    });
  }
});

export default router;
