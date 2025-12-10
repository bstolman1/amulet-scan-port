import express from 'express';

const router = express.Router();

const API_KEY = process.env.GROUPS_IO_API_KEY;
const BASE_URL = 'https://lists.sync.global';

// Define the governance groups and their lifecycle stages
const GOVERNANCE_GROUPS = {
  // CIP Flow: cip-discuss → cip-announce → supervalidator-announce
  'cip-discuss': { stage: 'proposal', flow: 'cip', label: 'CIP Discussion' },
  'cip-announce': { stage: 'review', flow: 'cip', label: 'CIP Announcement' },
  
  // Tokenomics Flow: tokenomics → tokenomics-announce → supervalidator-announce
  'tokenomics': { stage: 'proposal', flow: 'tokenomics', label: 'TC Discussion' },
  'tokenomics-announce': { stage: 'review', flow: 'tokenomics', label: 'TC Announcement' },
  
  // Final stage for both flows
  'supervalidator-announce': { stage: 'vote', flow: 'all', label: 'SV Announcement' },
};

const LIFECYCLE_STAGES = ['proposal', 'review', 'vote', 'result'];

// Helper to extract URLs from text
function extractUrls(text) {
  if (!text) return [];
  const urlRegex = /(https?:\/\/[^\s<>"{}|\\^`\[\]]+)/g;
  const matches = text.match(urlRegex) || [];
  return [...new Set(matches)];
}

// Extract identifiers from subject/content for correlation
function extractIdentifiers(text) {
  if (!text) return { cipNumber: null, appName: null, validatorName: null, keywords: [] };
  
  const identifiers = {
    cipNumber: null,
    appName: null,
    validatorName: null,
    keywords: [],
  };
  
  // Extract CIP numbers (e.g., CIP-123, CIP 123, CIP#123)
  const cipMatch = text.match(/CIP[#\-\s]?(\d+)/i);
  if (cipMatch) {
    identifiers.cipNumber = `CIP-${cipMatch[1]}`;
  }
  
  // Extract featured app mentions
  const appPatterns = [
    /featured\s+app[:\s]+([A-Za-z0-9\s]+?)(?:\s+[-–]|\s+for|\s*$)/i,
    /app[:\s]+([A-Za-z0-9\s]+?)(?:\s+[-–]|\s+application|\s*$)/i,
  ];
  for (const pattern of appPatterns) {
    const match = text.match(pattern);
    if (match) {
      identifiers.appName = match[1].trim();
      break;
    }
  }
  
  // Extract validator mentions
  const validatorPatterns = [
    /validator[:\s]+([A-Za-z0-9\s]+?)(?:\s+[-–]|\s+application|\s*$)/i,
    /super\s*validator[:\s]+([A-Za-z0-9\s]+?)(?:\s+[-–]|\s*$)/i,
  ];
  for (const pattern of validatorPatterns) {
    const match = text.match(pattern);
    if (match) {
      identifiers.validatorName = match[1].trim();
      break;
    }
  }
  
  // Extract key words for fuzzy matching
  const words = text.toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .split(/\s+/)
    .filter(w => w.length > 3 && !['the', 'and', 'for', 'this', 'that', 'with', 'from'].includes(w));
  identifiers.keywords = [...new Set(words)].slice(0, 10);
  
  return identifiers;
}

// Get all subscribed groups
async function getSubscribedGroups() {
  const subsUrl = `${BASE_URL}/api/v1/getsubs?limit=100`;
  
  const response = await fetch(subsUrl, {
    headers: { 'Authorization': `Bearer ${API_KEY}` },
  });
  
  if (!response.ok) {
    throw new Error(`Failed to fetch subscriptions: ${response.status}`);
  }
  
  const data = await response.json();
  const subs = data.data || [];
  
  // Map group names to IDs
  const groupMap = {};
  for (const sub of subs) {
    const groupName = sub.group_name || '';
    // Extract the subgroup name (after the +)
    const match = groupName.match(/\+([^+]+)$/);
    if (match) {
      const subgroupName = match[1];
      if (GOVERNANCE_GROUPS[subgroupName]) {
        groupMap[subgroupName] = {
          id: sub.group_id,
          fullName: groupName,
          ...GOVERNANCE_GROUPS[subgroupName],
        };
      }
    }
  }
  
  return groupMap;
}

// Helper to delay between requests
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// Fetch topics from a specific group with pagination and retries
async function fetchGroupTopics(groupId, groupName, maxTopics = 300) {
  const allTopics = [];
  let pageToken = null;
  let retries = 0;
  const maxRetries = 3;
  
  while (allTopics.length < maxTopics) {
    let url = `${BASE_URL}/api/v1/gettopics?group_id=${groupId}&limit=100`;
    if (pageToken) {
      url += `&page_token=${pageToken}`;
    }
    
    try {
      const response = await fetch(url, {
        headers: { 'Authorization': `Bearer ${API_KEY}` },
      });
      
      if (!response.ok) {
        console.error(`Failed to fetch topics for ${groupName}: ${response.status}`);
        if (response.status === 429 || response.status === 400) {
          // Rate limited - wait and retry
          if (retries < maxRetries) {
            retries++;
            console.log(`Rate limited, waiting 2s and retrying (${retries}/${maxRetries})...`);
            await delay(2000);
            continue;
          }
        }
        break;
      }
      
      const data = await response.json();
      const topics = data.data || [];
      allTopics.push(...topics);
      retries = 0; // Reset retries on success
      
      if (data.has_more !== true || !data.next_page_token) {
        break;
      }
      
      pageToken = data.next_page_token;
      // Small delay between pagination requests
      await delay(200);
    } catch (err) {
      console.error(`Fetch error for ${groupName}:`, err.message);
      if (retries < maxRetries) {
        retries++;
        console.log(`Connection error, waiting 2s and retrying (${retries}/${maxRetries})...`);
        await delay(2000);
        continue;
      }
      break;
    }
  }
  
  return allTopics;
}

// Calculate similarity between two topics for correlation
function calculateSimilarity(topic1, topic2) {
  const ids1 = topic1.identifiers;
  const ids2 = topic2.identifiers;
  
  let score = 0;
  
  // Exact CIP match = high confidence
  if (ids1.cipNumber && ids1.cipNumber === ids2.cipNumber) {
    score += 100;
  }
  
  // App name match
  if (ids1.appName && ids2.appName && 
      ids1.appName.toLowerCase() === ids2.appName.toLowerCase()) {
    score += 80;
  }
  
  // Validator name match
  if (ids1.validatorName && ids2.validatorName &&
      ids1.validatorName.toLowerCase() === ids2.validatorName.toLowerCase()) {
    score += 80;
  }
  
  // Subject similarity (fuzzy match)
  const subjectWords1 = topic1.subject.toLowerCase().split(/\s+/);
  const subjectWords2 = topic2.subject.toLowerCase().split(/\s+/);
  const commonWords = subjectWords1.filter(w => subjectWords2.includes(w) && w.length > 3);
  score += commonWords.length * 5;
  
  // Keyword overlap
  const commonKeywords = ids1.keywords.filter(k => ids2.keywords.includes(k));
  score += commonKeywords.length * 3;
  
  // Date proximity (within 30 days = bonus)
  const date1 = new Date(topic1.date);
  const date2 = new Date(topic2.date);
  const daysDiff = Math.abs((date1 - date2) / (1000 * 60 * 60 * 24));
  if (daysDiff <= 7) score += 20;
  else if (daysDiff <= 30) score += 10;
  else if (daysDiff <= 90) score += 5;
  
  return score;
}

// Group related topics into lifecycle items
function correlateTopics(allTopics) {
  const lifecycleItems = [];
  const used = new Set();
  
  // Sort topics by date (newest first)
  const sortedTopics = [...allTopics].sort((a, b) => 
    new Date(b.date) - new Date(a.date)
  );
  
  for (const topic of sortedTopics) {
    if (used.has(topic.id)) continue;
    
    // Create a new lifecycle item
    const item = {
      id: `lifecycle-${topic.id}`,
      primaryId: topic.identifiers.cipNumber || topic.identifiers.appName || topic.identifiers.validatorName || topic.subject.slice(0, 30),
      type: topic.identifiers.cipNumber ? 'cip' : 
            topic.identifiers.appName ? 'featured-app' :
            topic.identifiers.validatorName ? 'validator' : 'other',
      stages: {},
      topics: [],
      firstDate: topic.date,
      lastDate: topic.date,
      currentStage: topic.stage,
    };
    
    // Find related topics
    for (const candidate of sortedTopics) {
      if (used.has(candidate.id)) continue;
      if (candidate.id === topic.id) {
        item.stages[topic.stage] = item.stages[topic.stage] || [];
        item.stages[topic.stage].push(topic);
        item.topics.push(topic);
        used.add(topic.id);
        continue;
      }
      
      const similarity = calculateSimilarity(topic, candidate);
      if (similarity >= 30) { // Threshold for correlation
        item.stages[candidate.stage] = item.stages[candidate.stage] || [];
        item.stages[candidate.stage].push(candidate);
        item.topics.push(candidate);
        used.add(candidate.id);
        
        // Update dates
        if (new Date(candidate.date) < new Date(item.firstDate)) {
          item.firstDate = candidate.date;
        }
        if (new Date(candidate.date) > new Date(item.lastDate)) {
          item.lastDate = candidate.date;
        }
      }
    }
    
    // Determine current stage (latest stage with activity)
    for (const stage of LIFECYCLE_STAGES.slice().reverse()) {
      if (item.stages[stage] && item.stages[stage].length > 0) {
        item.currentStage = stage;
        break;
      }
    }
    
    lifecycleItems.push(item);
  }
  
  return lifecycleItems;
}

// Main endpoint
router.get('/', async (req, res) => {
  if (!API_KEY) {
    return res.status(500).json({ 
      error: 'GROUPS_IO_API_KEY not configured',
      lifecycleItems: [],
      groups: {},
    });
  }

  try {
    console.log('Fetching governance lifecycle data...');
    
    // Get all subscribed governance groups
    const groupMap = await getSubscribedGroups();
    console.log('Found governance groups:', Object.keys(groupMap));
    
    // Fetch topics from groups SEQUENTIALLY to avoid rate limiting
    const allTopics = [];
    const groupEntries = Object.entries(groupMap);
    
    for (const [name, group] of groupEntries) {
      console.log(`Fetching topics from ${name} (ID: ${group.id})...`);
      const topics = await fetchGroupTopics(group.id, name, 300);
      console.log(`Got ${topics.length} topics from ${name}`);
      
      // Log a sample topic to debug URL structure
      if (topics.length > 0) {
        console.log(`Sample topic keys: ${Object.keys(topics[0]).join(', ')}`);
        console.log(`Sample permalink: ${topics[0].permalink || 'none'}`);
        console.log(`Sample group_name: ${topics[0].group_name || 'none'}`);
      }
      
      const mappedTopics = topics.map(topic => {
        // Prefer the API's permalink, or construct from topic's group_name if available
        let sourceUrl = topic.permalink;
        
        if (!sourceUrl) {
          // Use the topic's own group_name if available, otherwise fall back to our group info
          const topicGroupName = topic.group_name || group.fullName;
          // Groups.io subgroup URLs: /g/parentgroup/subgroup/topic/ID
          // The group_name format is typically "parentgroup+subgroup"
          const urlPath = topicGroupName.replace(/\+/g, '/');
          sourceUrl = `${BASE_URL}/g/${urlPath}/topic/${topic.id}`;
        }
        
        return {
          id: topic.id?.toString() || `topic-${Math.random()}`,
          subject: topic.subject || topic.title || 'Untitled',
          date: topic.created || topic.updated || new Date().toISOString(),
          content: topic.snippet || topic.body || topic.preview || '',
          excerpt: (topic.snippet || topic.body || topic.preview || '').substring(0, 500),
          sourceUrl,
          linkedUrls: extractUrls(topic.snippet || topic.body || ''),
          messageCount: topic.num_msgs || 1,
          groupName: name,
          groupLabel: group.label,
          stage: group.stage,
          flow: group.flow,
          identifiers: extractIdentifiers((topic.subject || '') + ' ' + (topic.snippet || '')),
        };
      });
      
      allTopics.push(...mappedTopics);
      
      // Delay between groups to avoid rate limiting
      await delay(500);
    }
    
    console.log(`Total topics across all groups: ${allTopics.length}`);
    
    // Correlate topics into lifecycle items
    const lifecycleItems = correlateTopics(allTopics);
    console.log(`Correlated into ${lifecycleItems.length} lifecycle items`);
    
    // Summary stats
    const stats = {
      totalTopics: allTopics.length,
      lifecycleItems: lifecycleItems.length,
      byType: {
        cip: lifecycleItems.filter(i => i.type === 'cip').length,
        'featured-app': lifecycleItems.filter(i => i.type === 'featured-app').length,
        validator: lifecycleItems.filter(i => i.type === 'validator').length,
        other: lifecycleItems.filter(i => i.type === 'other').length,
      },
      byStage: {
        proposal: lifecycleItems.filter(i => i.currentStage === 'proposal').length,
        review: lifecycleItems.filter(i => i.currentStage === 'review').length,
        vote: lifecycleItems.filter(i => i.currentStage === 'vote').length,
        result: lifecycleItems.filter(i => i.currentStage === 'result').length,
      },
      groupCounts: Object.fromEntries(
        Object.entries(groupMap).map(([name, group]) => [
          name,
          allTopics.filter(t => t.groupName === name).length
        ])
      ),
    };
    
    return res.json({
      lifecycleItems,
      allTopics,
      groups: groupMap,
      stats,
    });

  } catch (error) {
    console.error('Error fetching governance lifecycle:', error);
    res.status(500).json({ 
      error: error.message,
      lifecycleItems: [],
      groups: {},
    });
  }
});

export default router;
