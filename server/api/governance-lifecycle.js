import express from 'express';

const router = express.Router();

const API_KEY = process.env.GROUPS_IO_API_KEY;
const BASE_URL = 'https://lists.sync.global';

// Define the governance groups and their lifecycle stages
// Each group maps to a specific flow and stage within that flow
// CIP Flow: cip-discuss → cip-vote → cip-announce → sv-announce
// Featured App Flow: tokenomics → tokenomics-announce → sv-announce
// Validator Flow: tokenomics → sv-announce
const GOVERNANCE_GROUPS = {
  // CIP Flow groups
  'cip-discuss': { stage: 'cip-discuss', flow: 'cip', label: 'CIP Discussion' },
  'cip-vote': { stage: 'cip-vote', flow: 'cip', label: 'CIP Vote' },
  'cip-announce': { stage: 'cip-announce', flow: 'cip', label: 'CIP Announcement' },
  // Shared groups (tokenomics for featured-app and validator)
  'tokenomics': { stage: 'tokenomics', flow: 'shared', label: 'Tokenomics Discussion' },
  'tokenomics-announce': { stage: 'tokenomics-announce', flow: 'featured-app', label: 'Tokenomics Announcement' },
  // Final stage for all flows
  'supervalidator-announce': { stage: 'sv-announce', flow: 'shared', label: 'SV Announcement' },
};

// Define type-specific workflow stages
const WORKFLOW_STAGES = {
  cip: ['cip-discuss', 'cip-vote', 'cip-announce', 'sv-announce'],
  'featured-app': ['tokenomics', 'tokenomics-announce', 'sv-announce'],
  validator: ['tokenomics', 'sv-announce'],
  other: ['tokenomics', 'sv-announce'], // Fallback
};

// All possible stages (for correlation)
const ALL_STAGES = [...new Set(Object.values(WORKFLOW_STAGES).flat())];

// Helper to extract URLs from text
function extractUrls(text) {
  if (!text) return [];
  const urlRegex = /(https?:\/\/[^\s<>"{}|\\^`\[\]]+)/g;
  const matches = text.match(urlRegex) || [];
  return [...new Set(matches)];
}

// Extract the primary entity name from a subject line
// This extracts the key identifier that makes each proposal unique
function extractPrimaryEntityName(text) {
  if (!text) return null;
  
  // Common patterns for app/validator names in subjects:
  // "New Featured App Request: Node Fortress"
  // "Node Fortress Featured App Tokenomics"
  // "[Node Fortress] Featured App Request"
  // "Featured App Vote Proposal - Node Fortress"
  // "Supervalidator Onboarding: CompanyName"
  
  // Remove common prefixes/suffixes and extract the unique name
  const cleanText = text
    .replace(/^(re:|fwd:|fw:)\s*/i, '')  // Remove email prefixes
    .replace(/\s*-\s*vote\s*(proposal)?\s*$/i, '')  // Remove "- Vote Proposal" suffix
    .replace(/\s*vote\s*(proposal)?\s*$/i, '')  // Remove "Vote Proposal" suffix
    .trim();
  
  // Pattern 1: "Something: EntityName" or "Something - EntityName"
  const colonMatch = cleanText.match(/(?:request|proposal|onboarding|tokenomics|announce|announcement)[:\s-]+(.+?)(?:\s*[-–]\s*(?:vote|proposal|announcement))?$/i);
  if (colonMatch) {
    const name = colonMatch[1].trim();
    if (name.length > 2 && !/^(the|this|new|our)$/i.test(name)) {
      return name;
    }
  }
  
  // Pattern 2: "EntityName Featured App/Validator Something"
  const prefixMatch = cleanText.match(/^([A-Za-z][A-Za-z0-9\s]{2,30}?)\s+(?:featured\s*app|super\s*validator|validator|tokenomics|onboarding)/i);
  if (prefixMatch) {
    const name = prefixMatch[1].trim();
    // Filter out generic words
    if (name.length > 2 && !/^(new|the|this|our|featured|app|vote|proposal)$/i.test(name)) {
      return name;
    }
  }
  
  // Pattern 3: "[EntityName]" anywhere in text
  const bracketMatch = cleanText.match(/\[([^\]]+)\]/);
  if (bracketMatch) {
    const name = bracketMatch[1].trim();
    if (name.length > 2) {
      return name;
    }
  }
  
  // Pattern 4: Look for a capitalized multi-word name (like "Node Fortress")
  const capitalizedMatch = cleanText.match(/([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)/);
  if (capitalizedMatch) {
    const name = capitalizedMatch[1].trim();
    // Filter out common phrases
    if (!/^(Featured App|Super Validator|Vote Proposal|New Request|Token Economics)$/i.test(name)) {
      return name;
    }
  }
  
  return null;
}

// Extract identifiers from subject/content for correlation
function extractIdentifiers(text) {
  if (!text) return { cipNumber: null, appName: null, validatorName: null, entityName: null, network: null, keywords: [] };
  
  const identifiers = {
    cipNumber: null,
    appName: null,
    validatorName: null,
    entityName: null,  // Primary entity name for correlation
    network: null,     // 'testnet' or 'mainnet'
    keywords: [],
  };
  
  // Extract CIP numbers (e.g., CIP-123, CIP 123, CIP#123) and format as 4-digit
  // Check for TBD/unassigned CIPs first
  const tbdMatch = text.match(/CIP[#\-\s]*(TBD|00XX|XXXX|\?\?|unassigned)/i);
  if (tbdMatch) {
    identifiers.cipNumber = 'CIP-00XX';
  } else {
    const cipMatch = text.match(/CIP[#\-\s]?(\d+)/i);
    if (cipMatch) {
      identifiers.cipNumber = `CIP-${cipMatch[1].padStart(4, '0')}`;
    }
  }
  
  // Detect network (testnet or mainnet)
  if (/testnet|test\s*net|tn\b/i.test(text)) {
    identifiers.network = 'testnet';
  } else if (/mainnet|main\s*net|mn\b/i.test(text)) {
    identifiers.network = 'mainnet';
  }
  
  // Check if text contains featured app indicators
  const isFeaturedApp = /featured\s*app|app\s+(?:application|listing|request|tokenomics|vote)/i.test(text);
  
  // Check if text contains validator indicators
  const isValidator = /super\s*validator|validator\s+(?:application|onboarding|license|candidate)|sv\s+(?:application|onboarding)/i.test(text);
  
  // Extract the primary entity name
  const entityName = extractPrimaryEntityName(text);
  identifiers.entityName = entityName;
  
  if (isFeaturedApp && entityName) {
    identifiers.appName = entityName;
  }
  
  if (isValidator && entityName) {
    identifiers.validatorName = entityName;
  }
  
  // Extract key words for fuzzy matching (fallback)
  const words = text.toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .split(/\s+/)
    .filter(w => w.length > 3 && !['the', 'and', 'for', 'this', 'that', 'with', 'from', 'featured', 'validator', 'tokenomics', 'announcement', 'announce', 'proposal', 'vote', 'request', 'application', 'listing', 'onboarding', 'supervalidator'].includes(w));
  identifiers.keywords = [...new Set(words)].slice(0, 10);
  
  return identifiers;
}

// Helper to delay between requests (moved up to be available)
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// Fetch group details to get the proper URL path
async function getGroupDetails(groupId) {
  const url = `${BASE_URL}/api/v1/getgroup?group_id=${groupId}`;
  console.log(`Fetching group details for ID ${groupId}...`);
  try {
    const response = await fetch(url, {
      headers: { 'Authorization': `Bearer ${API_KEY}` },
    });
    if (response.ok) {
      const data = await response.json();
      const group = data.group || data;
      // Log all relevant fields to understand URL structure
      console.log(`Group ${groupId}:`, {
        name: group.name,
        nice_group_name: group.nice_group_name,
        alias: group.alias,
        parent_group_id: group.parent_group_id,
      });
      return group;
    } else {
      const text = await response.text();
      console.error(`Failed to get group ${groupId}: ${response.status} - ${text}`);
    }
  } catch (err) {
    console.error(`Failed to get group details for ${groupId}:`, err.message);
  }
  return null;
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
        // URL format is simply /g/{subgroup-name}/topic/{id}
        // The subgroup name (after the +) is the URL path
        const urlName = subgroupName;
        
        console.log(`Group ${subgroupName} URL: ${BASE_URL}/g/${urlName}`);
        
        groupMap[subgroupName] = {
          id: sub.group_id,
          fullName: groupName,
          urlName: urlName,
          ...GOVERNANCE_GROUPS[subgroupName],
        };
      }
    }
  }
  
  return groupMap;
}


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
// Normalize entity name for comparison (case-insensitive, whitespace-normalized)
function normalizeEntityName(name) {
  if (!name) return null;
  return name.toLowerCase().replace(/\s+/g, ' ').trim();
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
  
  // Entity name match - this is the PRIMARY correlation for featured-app and validator
  const entity1 = normalizeEntityName(ids1.entityName);
  const entity2 = normalizeEntityName(ids2.entityName);
  
  if (entity1 && entity2) {
    if (entity1 === entity2) {
      // Exact entity name match = definite correlation
      score += 100;
    } else if (entity1.includes(entity2) || entity2.includes(entity1)) {
      // Partial entity name match (e.g., "Node Fortress" and "Node Fortress App")
      score += 80;
    }
  }
  
  // App name match (legacy, may be redundant with entityName)
  if (ids1.appName && ids2.appName) {
    const app1 = normalizeEntityName(ids1.appName);
    const app2 = normalizeEntityName(ids2.appName);
    if (app1 === app2) {
      score += 50; // Add some points but don't double-count if entityName already matched
    }
  }
  
  // Validator name match (legacy)
  if (ids1.validatorName && ids2.validatorName) {
    const val1 = normalizeEntityName(ids1.validatorName);
    const val2 = normalizeEntityName(ids2.validatorName);
    if (val1 === val2) {
      score += 50;
    }
  }
  
  // Date proximity (within 90 days = small bonus, but not enough to correlate on its own)
  const date1 = new Date(topic1.date);
  const date2 = new Date(topic2.date);
  const daysDiff = Math.abs((date1 - date2) / (1000 * 60 * 60 * 24));
  if (daysDiff <= 7) score += 10;
  else if (daysDiff <= 30) score += 5;
  else if (daysDiff <= 90) score += 2;
  
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
    
    // Determine the type and primary ID based on identifiers
    const topicEntityName = topic.identifiers.entityName;
    const type = topic.identifiers.cipNumber ? 'cip' : 
          topic.identifiers.appName ? 'featured-app' :
          topic.identifiers.validatorName ? 'validator' : 'other';
    
    // Create a new lifecycle item
    const item = {
      id: `lifecycle-${topic.id}`,
      primaryId: topic.identifiers.cipNumber || topicEntityName || topic.subject.slice(0, 40),
      type,
      network: topic.identifiers.network,  // 'testnet', 'mainnet', or null
      stages: {},
      topics: [],
      firstDate: topic.date,
      lastDate: topic.date,
      currentStage: topic.stage,
    };
    
    // Log for debugging
    console.log(`Processing topic: "${topic.subject.slice(0, 60)}..." -> type=${type}, entity="${topicEntityName || 'none'}"`);
    
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
      
      // For CIPs: ONLY correlate if they have an EXACT CIP number match
      if (item.type === 'cip') {
        const candidateCip = candidate.identifiers.cipNumber;
        const topicCip = topic.identifiers.cipNumber;
        // Only correlate if BOTH have the exact same CIP number
        if (!candidateCip || !topicCip || candidateCip !== topicCip) {
          continue;
        }
        // Exact CIP match - add to the lifecycle item
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
        continue;
      }
      
      // For Featured Apps and Validators: REQUIRE entity name match
      // This is the key change - each app/validator gets its own lifecycle
      if (item.type === 'featured-app' || item.type === 'validator') {
        const topicEntity = normalizeEntityName(topicEntityName);
        const candidateEntity = normalizeEntityName(candidate.identifiers.entityName);
        
        // Only correlate if BOTH have an entity name and they match
        if (!topicEntity || !candidateEntity) {
          continue;
        }
        
        // Check for exact match or substring match (e.g., "Node Fortress" matches "Node Fortress App")
        const matches = topicEntity === candidateEntity || 
                       topicEntity.includes(candidateEntity) || 
                       candidateEntity.includes(topicEntity);
        
        if (!matches) {
          continue;
        }
        
        // Entity name matches - add to the lifecycle item
        console.log(`  -> Correlating with: "${candidate.subject.slice(0, 60)}..." (entity="${candidateEntity}")`);
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
        continue;
      }
      
      // For "other" type: use similarity matching (fallback behavior)
      const similarity = calculateSimilarity(topic, candidate);
      if (similarity >= 70) { // Higher threshold for others
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
    
    // Determine current stage (latest stage with activity) based on item type
    const typeStages = WORKFLOW_STAGES[item.type] || WORKFLOW_STAGES.other;
    for (const stage of typeStages.slice().reverse()) {
      if (item.stages[stage] && item.stages[stage].length > 0) {
        item.currentStage = stage;
        break;
      }
    }
    
    lifecycleItems.push(item);
  }
  
  // Sort lifecycle items by CIP number descending (highest first)
  // CIP-00XX (TBD/unassigned) should come first
  lifecycleItems.sort((a, b) => {
    const aIsTBD = a.primaryId?.includes('00XX');
    const bIsTBD = b.primaryId?.includes('00XX');
    
    // TBD CIPs come first
    if (aIsTBD && !bIsTBD) return -1;
    if (!aIsTBD && bIsTBD) return 1;
    
    const aNum = a.primaryId?.match(/CIP-?(\d+)/i)?.[1];
    const bNum = b.primaryId?.match(/CIP-?(\d+)/i)?.[1];
    if (aNum && bNum) {
      return parseInt(bNum) - parseInt(aNum);
    }
    // CIPs come before non-CIPs
    if (aNum) return -1;
    if (bNum) return 1;
    // For non-CIPs, sort by date descending
    return new Date(b.lastDate) - new Date(a.lastDate);
  });
  
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
        // Use the group's urlName for the proper URL path
        // Format: /g/{urlName}/topic/{id}
        const sourceUrl = `${BASE_URL}/g/${group.urlName}/topic/${topic.id}`;
        
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
    // For featured-app and validator counts, deduplicate by primaryId (same app on testnet+mainnet = 1)
    const uniqueFeaturedApps = new Set(
      lifecycleItems.filter(i => i.type === 'featured-app').map(i => i.primaryId.toLowerCase())
    );
    const uniqueValidators = new Set(
      lifecycleItems.filter(i => i.type === 'validator').map(i => i.primaryId.toLowerCase())
    );
    
    const stats = {
      totalTopics: allTopics.length,
      lifecycleItems: lifecycleItems.length,
      byType: {
        cip: lifecycleItems.filter(i => i.type === 'cip').length,
        'featured-app': uniqueFeaturedApps.size,  // Deduplicated count
        validator: uniqueValidators.size,          // Deduplicated count
        other: lifecycleItems.filter(i => i.type === 'other').length,
      },
      byStage: Object.fromEntries(
        ALL_STAGES.map(stage => [stage, lifecycleItems.filter(i => i.currentStage === stage).length])
      ),
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
