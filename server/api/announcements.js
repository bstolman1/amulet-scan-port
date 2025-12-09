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
  const limit = parseInt(req.query.limit) || 50;
  
  if (!API_KEY) {
    return res.status(500).json({ 
      error: 'GROUPS_IO_API_KEY not configured',
      announcements: [] 
    });
  }

  try {
    // Try fetching messages
    const messagesUrl = `${BASE_URL}/api/v1/getmessages?group_name=${GROUP_NAME}&limit=${limit}`;
    
    const response = await fetch(messagesUrl, {
      headers: {
        'Authorization': `Bearer ${API_KEY}`,
        'Content-Type': 'application/json',
      },
    });

    if (!response.ok) {
      // Try topics endpoint as fallback
      const topicsUrl = `${BASE_URL}/api/v1/gettopics?group_name=${GROUP_NAME}&limit=${limit}`;
      const topicsResponse = await fetch(topicsUrl, {
        headers: {
          'Authorization': `Bearer ${API_KEY}`,
          'Content-Type': 'application/json',
        },
      });

      if (!topicsResponse.ok) {
        const text = await topicsResponse.text();
        throw new Error(`Groups.io API failed: ${topicsResponse.status} - ${text}`);
      }

      const topicsData = await topicsResponse.json();
      const announcements = (topicsData.topics || topicsData.data || []).map((topic, idx) => ({
        id: topic.id?.toString() || `topic-${idx}`,
        subject: topic.subject || topic.title || 'Untitled',
        date: topic.created || topic.date || new Date().toISOString(),
        content: topic.snippet || topic.body || topic.content || '',
        excerpt: (topic.snippet || topic.body || topic.content || '').substring(0, 500),
        sourceUrl: topic.permalink || `${BASE_URL}/g/${GROUP_NAME}/topic/${topic.id}`,
        linkedUrls: extractUrls(topic.body || topic.content || ''),
        messageCount: topic.num_msgs || 1,
      }));

      return res.json({ announcements, source: 'topics' });
    }

    const data = await response.json();
    const messages = data.messages || data.data || [];
    
    const announcements = messages.map((msg, idx) => ({
      id: msg.id?.toString() || `msg-${idx}`,
      subject: msg.subject || 'Untitled',
      date: msg.created || msg.date || new Date().toISOString(),
      content: msg.body || msg.content || msg.snippet || '',
      excerpt: (msg.body || msg.content || msg.snippet || '').substring(0, 500),
      sourceUrl: msg.permalink || `${BASE_URL}/g/${GROUP_NAME}/message/${msg.id}`,
      linkedUrls: extractUrls(msg.body || msg.content || ''),
      sender: msg.sender || msg.from_name || 'Unknown',
    }));

    res.json({ announcements, source: 'messages' });

  } catch (error) {
    console.error('Error fetching announcements:', error);
    res.status(500).json({ 
      error: error.message,
      announcements: [] 
    });
  }
});

export default router;
