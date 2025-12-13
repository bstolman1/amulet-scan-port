import express from 'express';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

// Optional inference - only import if ENABLE_INFERENCE=true
let inferStage = null;
const INFERENCE_ENABLED = process.env.ENABLE_INFERENCE === 'true';
if (INFERENCE_ENABLED) {
  try {
    const inferModule = await import('../inference/inferStage.js');
    inferStage = inferModule.inferStage;
    console.log('Stage inference enabled');
  } catch (err) {
    console.warn('Stage inference disabled - failed to load:', err.message);
  }
}

const __dirname = path.dirname(fileURLToPath(import.meta.url));
// Cache directory - uses DATA_DIR/cache if DATA_DIR is set, otherwise project data/cache
const BASE_DATA_DIR = process.env.DATA_DIR || path.resolve(__dirname, '../../data');
const CACHE_DIR = path.join(BASE_DATA_DIR, 'cache');
const CACHE_FILE = path.join(CACHE_DIR, 'governance-lifecycle.json');

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
  outcome: ['sv-announce'],
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
  
  // Remove common prefixes/suffixes first
  let cleanText = text
    .replace(/^(re:|fwd:|fw:)\s*/i, '')
    .replace(/\s*-\s*vote\s*(proposal)?\s*$/i, '')
    .replace(/\s*vote\s*(proposal)?\s*$/i, '')
    .trim();
  
  // Pattern 0a: "Validator Approved: EntityName" - extract entity name from validator approval announcements
  const validatorApprovedMatch = cleanText.match(/validator\s*(?:approved|operator\s+approved)[:\s-]+(.+?)$/i);
  if (validatorApprovedMatch) {
    let name = validatorApprovedMatch[1].trim();
    console.log(`EXTRACT: "Validator Approved" pattern matched, extracted: "${name}" from "${cleanText.slice(0, 60)}"`);
    if (name.length > 1) {
      return name;
    }
  }
  
  // Pattern 0b: "Featured App Approved: AppName" - MUST come first for these specific announcements
  const featuredApprovedMatch = cleanText.match(/featured\s*app\s*approved[:\s-]+(.+?)$/i);
  if (featuredApprovedMatch) {
    let name = featuredApprovedMatch[1].trim();
    console.log(`EXTRACT: "Featured App Approved" pattern matched, extracted: "${name}" from "${cleanText.slice(0, 60)}"`);
    if (name.length > 1) {
      return name;
    }
  }
  
  // Pattern 0: "to implement/apply Featured Application status for AppName"
  // "for Featured Application status for AppName"
  // "for featured app rights for AppName"
  const forAppMatch = cleanText.match(/(?:to\s+)?(?:implement|apply|for)\s+featured\s*(?:app(?:lication)?|application)\s+(?:status|rights)\s+for\s+([A-Za-z0-9][\w\s-]*?)$/i);
  if (forAppMatch) {
    let name = forAppMatch[1].trim();
    if (name.length > 1) {
      return name;
    }
  }
  
  // Pattern 1: "MainNet: AppName by Company" or "TestNet: AppName by Company"
  // Extract just the app name (before "by") - this handles "MainNet: Akascan by Akasec"
  const networkAppMatch = cleanText.match(/(?:mainnet|testnet|main\s*net|test\s*net)[:\s]+([^:]+?)(?:\s+by\s+.+)?$/i);
  if (networkAppMatch) {
    let name = networkAppMatch[1].trim();
    // Remove trailing "by Company" if still present
    name = name.replace(/\s+by\s+.*$/i, '').trim();
    if (name.length > 2) {
      return name;
    }
  }
  
  // Pattern 2: "Featured App Vote Proposal: AppName" or "Request: AppName" or "Approved: AppName"
  const requestMatch = cleanText.match(/(?:request|proposal|onboarding|tokenomics|announce|announcement|approved|vote\s*proposal)[:\s-]+(.+?)$/i);
  if (requestMatch) {
    let name = requestMatch[1].trim();
    // Remove network prefix if present
    name = name.replace(/^(?:mainnet|testnet|main\s*net|test\s*net)[:\s]+/i, '').trim();
    // Remove "by Company" suffix
    name = name.replace(/\s+by\s+.*$/i, '').trim();
    if (name.length > 2 && !/^(the|this|new|our)$/i.test(name)) {
      return name;
    }
  }
  
  // Pattern 3: "New Featured App Request: AppName"
  const newRequestMatch = cleanText.match(/new\s+featured\s*app\s+request[:\s-]+(.+?)$/i);
  if (newRequestMatch) {
    let name = newRequestMatch[1].trim();
    name = name.replace(/^(?:mainnet|testnet)[:\s]+/i, '').trim();
    name = name.replace(/\s+by\s+.*$/i, '').trim();
    if (name.length > 2) {
      return name;
    }
  }
  
  // Pattern 4: "AppName Featured App Tokenomics" (name at start)
  const prefixMatch = cleanText.match(/^([A-Za-z][A-Za-z0-9\s-]{1,30}?)\s+(?:featured\s*app|super\s*validator|validator|tokenomics|onboarding)/i);
  if (prefixMatch) {
    const name = prefixMatch[1].trim();
    if (name.length > 1 && !/^(new|the|this|our|featured|app|vote|proposal|to)$/i.test(name)) {
      return name;
    }
  }
  
  // Pattern 5: "[AppName]" in brackets
  const bracketMatch = cleanText.match(/\[([^\]]+)\]/);
  if (bracketMatch) {
    const name = bracketMatch[1].trim();
    if (name.length > 2) {
      return name;
    }
  }
  
  // Pattern 6: Look for capitalized multi-word name
  const capitalizedMatch = cleanText.match(/([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)/);
  if (capitalizedMatch) {
    const name = capitalizedMatch[1].trim();
    if (!/^(Featured App|Super Validator|Vote Proposal|New Request|Token Economics|Main Net|Test Net)$/i.test(name)) {
      return name;
    }
  }
  
  // Pattern 7: Single capitalized word at end after "for" (e.g., "for MEXC", "for Kraken")
  const forSingleMatch = cleanText.match(/for\s+([A-Z][A-Za-z0-9-]+)$/);
  if (forSingleMatch) {
    const name = forSingleMatch[1].trim();
    if (name.length > 1 && !/^(the|this|that|it)$/i.test(name)) {
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
  
  // Check if this is a CIP discussion (even without a number) - e.g., "CIP Discuss-"
  const isCipDiscussion = /\bCIP\s*(?:Discuss|Discussion|Vote|Announce)/i.test(text);
  
  // Detect network (testnet or mainnet)
  if (/testnet|test\s*net|tn\b/i.test(text)) {
    identifiers.network = 'testnet';
  } else if (/mainnet|main\s*net|mn\b/i.test(text)) {
    identifiers.network = 'mainnet';
  }
  
  // Check if text contains featured app indicators - BUT NOT if it's a CIP discussion about featured apps
  // Also detect "Vote Proposal on MainNet/TestNet:" patterns as featured app related
  const isFeaturedApp = !isCipDiscussion && (
    /featured\s*app|featured\s*application|app\s+(?:application|listing|request|tokenomics|vote|approved)|application\s+status\s+for/i.test(text) ||
    /vote\s+proposal\s+(?:on|for)\s+(?:mainnet|testnet|main\s*net|test\s*net)/i.test(text) ||
    /featured\s+app\s+rights/i.test(text)
  );
  
  // Check if text contains validator indicators - including "validator approved" and "validator operator approved"
  const isValidator = /super\s*validator|validator\s+(?:approved|application|onboarding|license|candidate|operator\s+approved)|sv\s+(?:application|onboarding)|validator\s+operator/i.test(text);
  
  // Extract the primary entity name
  const entityName = extractPrimaryEntityName(text);
  identifiers.entityName = entityName;
  
  // Add CIP discussion flag to identifiers
  identifiers.isCipDiscussion = isCipDiscussion;
  
  // Debug log for featured app detection
  if (text.toLowerCase().includes('featured app approved')) {
    console.log(`IDENTIFIERS: isFeaturedApp=${isFeaturedApp}, entityName="${entityName}", text="${text.slice(0, 60)}"`);
  }
  
  // Debug log for validator detection
  if (text.toLowerCase().includes('validator approved')) {
    console.log(`IDENTIFIERS: isValidator=${isValidator}, entityName="${entityName}", text="${text.slice(0, 60)}"`);
  }
  
  // Don't set appName if this is a CIP discussion (even if it mentions featured apps)
  if (isFeaturedApp && entityName && !isCipDiscussion) {
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
    // IMPORTANT: Only classify as featured-app/validator if we have a valid entity name
    // This prevents generic "Featured App Approved" type topics from grouping everything
    const topicEntityName = topic.identifiers.entityName;
    const hasCip = !!topic.identifiers.cipNumber;
    const isCipDiscussion = !!topic.identifiers.isCipDiscussion;  // CIP Discuss without a number
    const hasAppIndicator = !!topic.identifiers.appName;
    const hasValidatorIndicator = !!topic.identifiers.validatorName;
    
    // Type determination: require entity name for featured-app/validator classification
    // Check if this is an outcome - matches "Tokenomics Outcomes" with or without a date suffix
    const subjectTrimmed = topic.subject.trim();
    const isOutcome = /\bTokenomics\s+Outcomes\b/i.test(subjectTrimmed);
    const isValidatorOperations = /\bValidator\s+Operations\b/i.test(subjectTrimmed);
    
    // Debug log for outcome detection
    if (subjectTrimmed.toLowerCase().includes('outcome')) {
      console.log(`[Outcome check] Subject: "${topic.subject}" -> isOutcome: ${isOutcome}`);
    }
    
    let type;
    if (isOutcome) {
      type = 'outcome';
    } else if (isValidatorOperations || hasValidatorIndicator) {
      type = 'validator';
    } else if (hasFeaturedAppIndicator) {
      type = 'featured-app';
    } else {
      type = 'other';
    }
    
    // For outcomes, each topic gets its own card based on the topic date
    // This ensures "Tokenomics Outcomes" from different dates are separate cards
    let primaryId;
    if (type === 'outcome') {
      // Always use the topic's actual date for the primaryId
      // This ensures each outcome topic is a separate card
      const topicDate = new Date(topic.date);
      const formattedDate = topicDate.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
      primaryId = `Outcomes - ${formattedDate}`;
    } else {
      primaryId = topic.identifiers.cipNumber || topicEntityName || topic.subject.slice(0, 40);
    }
    
    // Create a new lifecycle item
    const item = {
      id: `lifecycle-${topic.id}`,
      primaryId,
      type,
      network: topic.identifiers.network,  // 'testnet', 'mainnet', or null
      stages: {},
      topics: [],
      firstDate: topic.date,
      lastDate: topic.date,
      currentStage: topic.stage,
    };
    
    // Log for debugging - show the primaryId being assigned
    console.log(`NEW LIFECYCLE: "${topic.subject.slice(0, 60)}..." -> type=${type}, primaryId="${primaryId}", entity="${topicEntityName || 'none'}"`);
    
    // Add the starting topic first
    item.stages[topic.stage] = item.stages[topic.stage] || [];
    item.stages[topic.stage].push(topic);
    item.topics.push(topic);
    used.add(topic.id);
    
    // For types that require entity names, skip correlation if no entity found
    // Each topic without an entity will be its own separate item
    if ((type === 'featured-app' || type === 'validator') && !topicEntityName) {
      console.log(`  -> No entity name found, creating standalone item`);
      lifecycleItems.push(item);
      continue;
    }
    
    // For outcomes, each one is standalone - don't correlate
    if (type === 'outcome') {
      console.log(`  -> Outcome type - creating standalone item`);
      lifecycleItems.push(item);
      continue;
    }
    
    // Find related topics
    for (const candidate of sortedTopics) {
      if (used.has(candidate.id)) continue;
      if (candidate.id === topic.id) continue; // Already added above
      
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

// Helper to read cached data
function readCache() {
  try {
    if (fs.existsSync(CACHE_FILE)) {
      const data = JSON.parse(fs.readFileSync(CACHE_FILE, 'utf-8'));
      return data;
    }
  } catch (err) {
    console.error('Error reading cache:', err.message);
  }
  return null;
}

// Helper to write cache
function writeCache(data) {
  try {
    if (!fs.existsSync(CACHE_DIR)) {
      fs.mkdirSync(CACHE_DIR, { recursive: true });
    }
    fs.writeFileSync(CACHE_FILE, JSON.stringify(data, null, 2));
    console.log(`✅ Cached governance data to ${CACHE_FILE}`);
  } catch (err) {
    console.error('Error writing cache:', err.message);
  }
}

// Fetch fresh data from groups.io
async function fetchFreshData() {
  console.log('Fetching fresh governance lifecycle data from groups.io...');
  
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
    
    for (const topic of topics) {
      const sourceUrl = `${BASE_URL}/g/${group.urlName}/topic/${topic.id}`;

      const mapped = {
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

      // ───── Pattern A: inference annotation ─────
      mapped.postedStage = mapped.stage;
      mapped.inferredStage = null;
      mapped.inferenceConfidence = null;
      mapped.effectiveStage = mapped.stage;

      if (inferStage) {
        try {
          const inference = await inferStage(mapped.subject, mapped.content || '');
          if (inference && inference.stage) {
            mapped.inferredStage = inference.stage;
            mapped.inferenceConfidence = inference.confidence;

            if (inference.confidence >= 0.85 && inference.stage !== 'other') {
              mapped.effectiveStage = inference.stage;
            }
          }
        } catch (err) {
          console.error('Inference failed for topic', mapped.id, err.message);
        }
      }
      // ───────────────────────────────────────────

      allTopics.push(mapped);
    }
    await delay(500);
  }
  
  console.log(`Total topics across all groups: ${allTopics.length}`);
  
  // Correlate topics into lifecycle items
  const lifecycleItems = correlateTopics(allTopics);
  console.log(`Correlated into ${lifecycleItems.length} lifecycle items`);
  
  // Summary stats
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
      'featured-app': uniqueFeaturedApps.size,
      validator: uniqueValidators.size,
      outcome: lifecycleItems.filter(i => i.type === 'outcome').length,
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
  
  return {
    lifecycleItems,
    allTopics,
    groups: groupMap,
    stats,
    cachedAt: new Date().toISOString(),
  };
}

// Main endpoint - reads from cache first
router.get('/', async (req, res) => {
  const forceRefresh = req.query.refresh === 'true';
  
  // Try to read from cache first (unless force refresh)
  if (!forceRefresh) {
    const cached = readCache();
    if (cached) {
      console.log(`Serving cached governance data from ${cached.cachedAt}`);
      return res.json(cached);
    }
  }
  
  // No cache or force refresh - fetch fresh data
  if (!API_KEY) {
    return res.status(500).json({ 
      error: 'GROUPS_IO_API_KEY not configured',
      lifecycleItems: [],
      groups: {},
    });
  }

  try {
    const data = await fetchFreshData();
    writeCache(data);
    return res.json(data);

  } catch (error) {
    console.error('Error fetching governance lifecycle:', error);
    
    // On error, try to serve stale cache
    const cached = readCache();
    if (cached) {
      console.log('Serving stale cache due to fetch error');
      return res.json({ ...cached, stale: true, error: error.message });
    }
    
    res.status(500).json({ 
      error: error.message,
      lifecycleItems: [],
      groups: {},
    });
  }
});

// Refresh endpoint - explicitly fetches fresh data
router.post('/refresh', async (req, res) => {
  if (!API_KEY) {
    console.error('GROUPS_IO_API_KEY is not set in environment');
    return res.status(500).json({ error: 'GROUPS_IO_API_KEY not configured. Please set this environment variable.' });
  }

  try {
    console.log('Starting governance lifecycle refresh...');
    const data = await fetchFreshData();
    writeCache(data);
    console.log('Refresh complete:', data.stats);
    return res.json({ success: true, stats: data.stats, cachedAt: data.cachedAt });
  } catch (error) {
    console.error('Error refreshing governance lifecycle:', error.stack || error);
    res.status(500).json({ error: error.message, stack: error.stack });
  }
});

// Cache info endpoint
router.get('/cache-info', (req, res) => {
  const cached = readCache();
  if (cached) {
    res.json({
      hasCachedData: true,
      cachedAt: cached.cachedAt,
      stats: cached.stats,
    });
  } else {
    res.json({ hasCachedData: false });
  }
});

export { fetchFreshData, writeCache, readCache };
export default router;
