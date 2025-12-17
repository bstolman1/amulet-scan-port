import express from 'express';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { inferStagesBatch } from '../inference/inferStage.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
// Cache directory - uses DATA_DIR/cache if DATA_DIR is set, otherwise project data/cache
const BASE_DATA_DIR = process.env.DATA_DIR || path.resolve(__dirname, '../../data');
const CACHE_DIR = path.join(BASE_DATA_DIR, 'cache');
const CACHE_FILE = path.join(CACHE_DIR, 'governance-lifecycle.json');

// Inference confidence threshold - only override postedStage if confidence >= threshold
const INFERENCE_THRESHOLD = 0.85;
// Feature flag to enable/disable inference (set via env var)
const INFERENCE_ENABLED = process.env.INFERENCE_ENABLED === 'true';
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
  'protocol-upgrade': ['tokenomics', 'sv-announce'],
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
// Returns { name: string|null, isMultiEntity: boolean }
function extractPrimaryEntityName(text) {
  if (!text) return { name: null, isMultiEntity: false };
  
  // Remove common prefixes/suffixes first
  let cleanText = text
    .replace(/^(re:|fwd:|fw:)\s*/i, '')
    .replace(/\s*-\s*vote\s*(proposal)?\s*$/i, '')
    .replace(/\s*vote\s*(proposal)?\s*$/i, '')
    .trim();
  
  // MULTI-ENTITY DETECTION: Check for batch approval patterns first
  // "Validator Operators Approved: A, B, C, D..." or "Featured Apps Approved: X, Y, Z"
  const batchApprovalMatch = cleanText.match(/(?:validator\s*operators?|validators|featured\s*apps?)\s*approved[:\s-]+(.+?)$/i);
  if (batchApprovalMatch) {
    const content = batchApprovalMatch[1].trim();
    // If contains comma or "and" with multiple entities, it's multi-entity
    if (content.includes(',') || /\s+and\s+/i.test(content)) {
      console.log(`EXTRACT: Multi-entity batch approval detected: "${content.slice(0, 60)}"`);
      return { name: null, isMultiEntity: true };
    }
  }
  
  // Pattern 0a: "Validator Approved: EntityName" - extract entity name from validator approval announcements
  const validatorApprovedMatch = cleanText.match(/validator\s*(?:approved|operator\s+approved)[:\s-]+(.+?)$/i);
  if (validatorApprovedMatch) {
    let name = validatorApprovedMatch[1].trim();
    // Check for multi-entity (comma-separated list)
    if (name.includes(',') || /\s+and\s+/i.test(name)) {
      console.log(`EXTRACT: Multi-entity validator list detected: "${name.slice(0, 60)}"`);
      return { name: null, isMultiEntity: true };
    }
    console.log(`EXTRACT: "Validator Approved" pattern matched, extracted: "${name}" from "${cleanText.slice(0, 60)}"`);
    if (name.length > 1) {
      return { name, isMultiEntity: false };
    }
  }
  
  // Pattern 0b: "Featured App Approved: AppName" - MUST come first for these specific announcements
  const featuredApprovedMatch = cleanText.match(/featured\s*app\s*approved[:\s-]+(.+?)$/i);
  if (featuredApprovedMatch) {
    let name = featuredApprovedMatch[1].trim();
    // Check for multi-entity
    if (name.includes(',') || /\s+and\s+/i.test(name)) {
      console.log(`EXTRACT: Multi-entity app list detected: "${name.slice(0, 60)}"`);
      return { name: null, isMultiEntity: true };
    }
    console.log(`EXTRACT: "Featured App Approved" pattern matched, extracted: "${name}" from "${cleanText.slice(0, 60)}"`);
    if (name.length > 1) {
      return { name, isMultiEntity: false };
    }
  }
  
  // Pattern 0: "to implement/apply Featured Application status for AppName"
  // "for Featured Application status for AppName"
  // "for featured app rights for AppName"
  const forAppMatch = cleanText.match(/(?:to\s+)?(?:implement|apply|for)\s+featured\s*(?:app(?:lication)?|application)\s+(?:status|rights)\s+for\s+([A-Za-z0-9][\w\s-]*?)$/i);
  if (forAppMatch) {
    let name = forAppMatch[1].trim();
    if (name.length > 1) {
      return { name, isMultiEntity: false };
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
      return { name, isMultiEntity: false };
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
      return { name, isMultiEntity: false };
    }
  }
  
  // Pattern 3: "New Featured App Request: AppName"
  const newRequestMatch = cleanText.match(/new\s+featured\s*app\s+request[:\s-]+(.+?)$/i);
  if (newRequestMatch) {
    let name = newRequestMatch[1].trim();
    name = name.replace(/^(?:mainnet|testnet)[:\s]+/i, '').trim();
    name = name.replace(/\s+by\s+.*$/i, '').trim();
    if (name.length > 2) {
      return { name, isMultiEntity: false };
    }
  }
  
  // Pattern 4: "AppName Featured App Tokenomics" (name at start)
  const prefixMatch = cleanText.match(/^([A-Za-z][A-Za-z0-9\s-]{1,30}?)\s+(?:featured\s*app|super\s*validator|validator|tokenomics|onboarding)/i);
  if (prefixMatch) {
    const name = prefixMatch[1].trim();
    if (name.length > 1 && !/^(new|the|this|our|featured|app|vote|proposal|to)$/i.test(name)) {
      return { name, isMultiEntity: false };
    }
  }
  
  // Pattern 5: "[AppName]" in brackets
  const bracketMatch = cleanText.match(/\[([^\]]+)\]/);
  if (bracketMatch) {
    const name = bracketMatch[1].trim();
    if (name.length > 2) {
      return { name, isMultiEntity: false };
    }
  }
  
  // Pattern 6: Look for capitalized multi-word name
  const capitalizedMatch = cleanText.match(/([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)/);
  if (capitalizedMatch) {
    const name = capitalizedMatch[1].trim();
    if (!/^(Featured App|Super Validator|Vote Proposal|New Request|Token Economics|Main Net|Test Net)$/i.test(name)) {
      return { name, isMultiEntity: false };
    }
  }
  
  // Pattern 7: Single capitalized word at end after "for" (e.g., "for MEXC", "for Kraken")
  const forSingleMatch = cleanText.match(/for\s+([A-Z][A-Za-z0-9-]+)$/);
  if (forSingleMatch) {
    const name = forSingleMatch[1].trim();
    if (name.length > 1 && !/^(the|this|that|it)$/i.test(name)) {
      return { name, isMultiEntity: false };
    }
  }
  
  return { name: null, isMultiEntity: false };
}

// Extract identifiers from subject/content for correlation
function extractIdentifiers(text) {
  if (!text) return { cipNumber: null, appName: null, validatorName: null, entityName: null, isMultiEntity: false, network: null, keywords: [] };
  
  const identifiers = {
    cipNumber: null,
    appName: null,
    validatorName: null,
    entityName: null,  // Primary entity name for correlation
    isMultiEntity: false, // True if this is a batch approval with multiple entities
    network: null,     // 'testnet' or 'mainnet'
    keywords: [],
  };
  
  // Extract CIP numbers (e.g., CIP-123, CIP 123, CIP#123, CIP - 0066) and format as 4-digit
  // Check for TBD/unassigned CIPs first
  const tbdMatch = text.match(/CIP\s*[-#]?\s*(TBD|00XX|XXXX|\?\?|unassigned)/i);
  if (tbdMatch) {
    identifiers.cipNumber = 'CIP-00XX';
  } else {
    // Handle various CIP formats: "CIP-0066", "CIP 0066", "CIP#0066", "CIP - 0066"
    // Use explicit pattern: CIP, optional spaces, optional separator, optional spaces, digits
    const cipMatch = text.match(/CIP\s*[-#]?\s*(\d{2,})/i);
    if (cipMatch) {
      identifiers.cipNumber = `CIP-${cipMatch[1].padStart(4, '0')}`;
      console.log(`EXTRACTED CIP: "${identifiers.cipNumber}" from "${text.slice(0, 60)}"`);
    }
  }
  
  // Check if this is a CIP discussion (even without a number)
  // Only set this if it's clearly a CIP-specific topic, not just any mention of CIP
  // e.g., "CIP Discussion:", "CIP-XXXX:", "New CIP Proposal" - but NOT "discussed CIP requirements"
  const isCipDiscussion = /^\s*(?:re:\s*)?CIP[#\-\s]|^(?:re:\s*)?(?:new\s+)?CIP\s+(?:discuss|proposal|draft)/i.test(text);
  
  // Detect network (testnet or mainnet)
  if (/testnet|test\s*net|tn\b/i.test(text)) {
    identifiers.network = 'testnet';
  } else if (/mainnet|main\s*net|mn\b/i.test(text)) {
    identifiers.network = 'mainnet';
  }
  
  // Check for Vote Proposal patterns - distinguish between CIP votes and other votes
  // CIP Vote Proposals contain a CIP number or are specifically about CIP governance
  // Featured App Vote Proposals mention "featured app" or network (MainNet/TestNet)
  const isVoteProposal = /\bVote\s+Proposal\b/i.test(text);
  const isCipVoteProposal = isVoteProposal && (
    /CIP[#\-\s]?\d+/i.test(text) || 
    /\bCIP\s+(?:vote|voting|approval)\b/i.test(text)
  );
  const isFeaturedAppVoteProposal = isVoteProposal && (
    /featured\s*app|featured\s*application|app\s+rights/i.test(text) ||
    /(?:mainnet|testnet|main\s*net|test\s*net):/i.test(text)
  );
  const isValidatorVoteProposal = isVoteProposal && (
    /validator\s+(?:operator|onboarding|license)/i.test(text)
  );
  
  // Check if text contains featured app indicators - BUT NOT if it's a CIP discussion about featured apps
  // Also detect "Vote Proposal on MainNet/TestNet:" patterns as featured app related
  const isFeaturedApp = !isCipDiscussion && (
    /featured\s*app|featured\s*application|app\s+(?:application|listing|request|tokenomics|vote|approved)|application\s+status\s+for/i.test(text) ||
    isFeaturedAppVoteProposal ||
    /featured\s+app\s+rights/i.test(text)
  );
  
  // Check if text contains validator indicators - including "validator approved" and "validator operator approved"
  const isValidator = (
    /super\s*validator|validator\s+(?:approved|application|onboarding|license|candidate|operator\s+approved)|sv\s+(?:application|onboarding)|validator\s+operator/i.test(text) ||
    isValidatorVoteProposal
  );
  
  // Extract the primary entity name (now returns { name, isMultiEntity })
  const entityResult = extractPrimaryEntityName(text);
  identifiers.entityName = entityResult.name;
  identifiers.isMultiEntity = entityResult.isMultiEntity;
  
  // Add CIP discussion flag to identifiers
  identifiers.isCipDiscussion = isCipDiscussion;
  
  // Add vote proposal type flags for better type determination
  identifiers.isCipVoteProposal = isCipVoteProposal;
  identifiers.isFeaturedAppVoteProposal = isFeaturedAppVoteProposal;
  identifiers.isValidatorVoteProposal = isValidatorVoteProposal;
  
  // Debug log for featured app detection
  if (text.toLowerCase().includes('featured app approved')) {
    console.log(`IDENTIFIERS: isFeaturedApp=${isFeaturedApp}, entityName="${entityResult.name}", isMultiEntity=${entityResult.isMultiEntity}, text="${text.slice(0, 60)}"`);
  }
  
  // Debug log for validator detection
  if (text.toLowerCase().includes('validator approved')) {
    console.log(`IDENTIFIERS: isValidator=${isValidator}, entityName="${entityResult.name}", isMultiEntity=${entityResult.isMultiEntity}, text="${text.slice(0, 60)}"`);
  }
  
  // Don't set appName if this is a CIP discussion (even if it mentions featured apps)
  if (isFeaturedApp && entityResult.name && !isCipDiscussion) {
    identifiers.appName = entityResult.name;
  }
  
  if (isValidator && entityResult.name) {
    identifiers.validatorName = entityResult.name;
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

// Normalize entity name for comparison
// Removes common suffixes, normalizes whitespace/case, removes punctuation
function normalizeEntityName(name) {
  if (!name) return null;
  return name
    .toLowerCase()
    // Remove common company suffixes
    .replace(/\b(llc|inc|corp|ltd|gmbh|ag|sa|bv|pty|co|company|limited|incorporated|corporation)\b\.?/gi, '')
    // Remove punctuation
    .replace(/[.,\-_'"()]/g, ' ')
    // Normalize whitespace
    .replace(/\s+/g, ' ')
    .trim();
}

// Even more aggressive normalization - removes all spaces for fuzzy comparison
function normalizeEntityNameStrict(name) {
  if (!name) return null;
  const normalized = normalizeEntityName(name);
  if (!normalized) return null;
  // Remove all spaces for strict comparison
  return normalized.replace(/\s+/g, '');
}

// Calculate Levenshtein distance between two strings
function levenshteinDistance(str1, str2) {
  if (!str1 || !str2) return Infinity;
  const m = str1.length;
  const n = str2.length;
  
  // Quick exit for very different lengths
  if (Math.abs(m - n) > Math.max(m, n) * 0.5) return Infinity;
  
  const dp = Array(m + 1).fill(null).map(() => Array(n + 1).fill(0));
  
  for (let i = 0; i <= m; i++) dp[i][0] = i;
  for (let j = 0; j <= n; j++) dp[0][j] = j;
  
  for (let i = 1; i <= m; i++) {
    for (let j = 1; j <= n; j++) {
      const cost = str1[i - 1] === str2[j - 1] ? 0 : 1;
      dp[i][j] = Math.min(
        dp[i - 1][j] + 1,      // deletion
        dp[i][j - 1] + 1,      // insertion
        dp[i - 1][j - 1] + cost // substitution
      );
    }
  }
  
  return dp[m][n];
}

// Check if two entity names match (with fuzzy matching)
function entitiesMatch(entity1, entity2) {
  if (!entity1 || !entity2) return false;
  
  const norm1 = normalizeEntityName(entity1);
  const norm2 = normalizeEntityName(entity2);
  
  if (!norm1 || !norm2) return false;
  
  // Exact match after normalization
  if (norm1 === norm2) return true;
  
  // Substring match (e.g., "Node Fortress" matches "Node Fortress App")
  if (norm1.includes(norm2) || norm2.includes(norm1)) return true;
  
  // Strict match (no spaces) - e.g., "nodefortress" matches "node fortress"
  const strict1 = normalizeEntityNameStrict(entity1);
  const strict2 = normalizeEntityNameStrict(entity2);
  if (strict1 === strict2) return true;
  
  // Fuzzy match using Levenshtein distance
  // Allow up to 20% character differences for longer names
  const maxLen = Math.max(norm1.length, norm2.length);
  const minLen = Math.min(norm1.length, norm2.length);
  
  // Only use fuzzy matching for names of similar length (within 30%)
  if (minLen < maxLen * 0.7) return false;
  
  const distance = levenshteinDistance(norm1, norm2);
  const threshold = Math.max(2, Math.floor(maxLen * 0.2)); // At least 2, or 20% of length
  
  if (distance <= threshold) {
    console.log(`  -> Fuzzy match: "${entity1}" ~ "${entity2}" (distance=${distance}, threshold=${threshold})`);
    return true;
  }
  
  return false;
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
    
    // Determine the type based on GROUP FLOW (primary) and subject-line heuristics (secondary)
    // The group a topic is posted to IS the ground truth for its stage
    const topicEntityName = topic.identifiers.entityName;
    const hasCip = !!topic.identifiers.cipNumber;
    const isCipDiscussion = !!topic.identifiers.isCipDiscussion;
    const hasAppIndicator = !!topic.identifiers.appName;
    const hasValidatorIndicator = !!topic.identifiers.validatorName;
    
    // Check for special subject-line patterns
    const subjectTrimmed = topic.subject.trim();
    const isOutcome = /\bTokenomics\s+Outcomes\b/i.test(subjectTrimmed);
    const isValidatorOperations = /\bValidator\s+Operations\b/i.test(subjectTrimmed);
    // "Vote Proposal" patterns are typically CIP-related votes, not validator applications
    const isVoteProposal = /\bVote\s+Proposal\b/i.test(subjectTrimmed);
    // Protocol upgrade patterns: "synchronizer migration", "Splice X.X", "protocol upgrade", "network upgrade"
    const isProtocolUpgrade = /\b(?:synchronizer\s+migration|splice\s+\d+\.\d+|protocol\s+upgrade|network\s+upgrade|hard\s*fork|migration\s+to\s+splice)\b/i.test(subjectTrimmed);
    
    // TYPE DETERMINATION: Use group flow as primary signal
    // CIP groups (cip-discuss, cip-vote, cip-announce) -> cip type
    // Shared groups need subject-line disambiguation for featured-app vs validator
    let type;
    
    if (isOutcome) {
      // Outcomes are a special case regardless of group
      type = 'outcome';
    } else if (isProtocolUpgrade) {
      // Protocol upgrades are a special type (migration, splice version upgrades)
      type = 'protocol-upgrade';
    } else if (topic.flow === 'cip') {
      // CIP groups are usually CIP type, but occasionally non-CIP announcements land here.
      // Be strict: only treat as CIP if there's an explicit CIP number / CIP vote proposal.
      // Otherwise, fall back to strong subject heuristics (validator/featured-app).
      if (topic.identifiers.isCipVoteProposal || hasCip) {
        type = 'cip';
      } else if (topic.identifiers.isValidatorVoteProposal || hasValidatorIndicator || isValidatorOperations) {
        type = 'validator';
      } else if (topic.identifiers.isFeaturedAppVoteProposal || hasAppIndicator) {
        type = 'featured-app';
      } else {
        // Default for ambiguous items posted in CIP groups
        type = 'other';
      }
    } else if (topic.flow === 'featured-app') {
      // tokenomics-announce is specifically for featured-app flow
      type = 'featured-app';
    } else if (topic.flow === 'shared') {
      // Shared groups (tokenomics, sv-announce) need subject-line disambiguation
      // Use specific vote proposal type flags for better accuracy
      // STRICT CIP detection: only classify as CIP if there's an actual CIP number
      // isCipDiscussion alone is NOT enough - many topics mention "CIP" without being CIPs
      if (topic.identifiers.isCipVoteProposal || hasCip) {
        // Only CIP-specific vote proposals or topics with explicit CIP numbers
        type = 'cip';
      } else if (topic.identifiers.isValidatorVoteProposal || isValidatorOperations || hasValidatorIndicator) {
        type = 'validator';
      } else if (topic.identifiers.isFeaturedAppVoteProposal || hasAppIndicator) {
        type = 'featured-app';
      } else if (isVoteProposal) {
        // Generic vote proposal without clear indicators - check for network prefix
        // "Vote Proposal on MainNet:" is typically featured app
        if (/(?:mainnet|testnet|main\s*net|test\s*net):/i.test(subjectTrimmed)) {
          type = 'featured-app';
        } else {
          // Default generic vote proposals to featured-app (most common in practice)
          type = 'featured-app';
        }
      } else {
        // Default shared group topics to featured-app (most common)
        type = 'featured-app';
      }
    } else {
      // Fallback - should rarely happen since all groups have a flow
      type = 'featured-app';
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
      primaryId = topic.identifiers.cipNumber || topicEntityName || topic.subject;
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
    
    // For types that require entity names, skip correlation if no entity found OR if multi-entity
    // Multi-entity batch approvals (e.g., "Validators Approved: A, B, C") are always standalone
    if ((type === 'featured-app' || type === 'validator') && (!topicEntityName || topic.identifiers.isMultiEntity)) {
      if (topic.identifiers.isMultiEntity) {
        console.log(`  -> Multi-entity batch approval, creating standalone item`);
      } else {
        console.log(`  -> No entity name found, creating standalone item`);
      }
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
      
      // Skip multi-entity candidates - they should be standalone
      if (candidate.identifiers.isMultiEntity) continue;
      
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
      
      // For Featured Apps and Validators: Use fuzzy entity matching
      // This handles variations like "Node Fortress" vs "Node Fortress LLC" vs "NodeFortress"
      if (item.type === 'featured-app' || item.type === 'validator') {
        const candidateEntity = candidate.identifiers.entityName;
        
        // Only correlate if candidate has an entity name
        if (!candidateEntity) {
          continue;
        }
        
        // CRITICAL: Determine candidate's type and only correlate if types match
        // This prevents CIP topics from being grouped with featured-app items just because
        // they share an entity name
        const candidateSubject = candidate.subject.trim();
        const candidateIsOutcome = /\bTokenomics\s+Outcomes\b/i.test(candidateSubject);
        const candidateIsProtocolUpgrade = /\b(?:synchronizer\s+migration|splice\s+\d+\.\d+|protocol\s+upgrade|network\s+upgrade|hard\s*fork|migration\s+to\s+splice)\b/i.test(candidateSubject);
        const candidateIsVoteProposal = /\bVote\s+Proposal\b/i.test(candidateSubject);
        const candidateIsValidatorOperations = /\bValidator\s+Operations\b/i.test(candidateSubject);
        const candidateHasCip = !!candidate.identifiers.cipNumber;
        const candidateIsCipDiscussion = !!candidate.identifiers.isCipDiscussion;
        
        let candidateType;
        if (candidateIsOutcome) {
          candidateType = 'outcome';
        } else if (candidateIsProtocolUpgrade) {
          candidateType = 'protocol-upgrade';
        } else if (candidate.flow === 'cip') {
          candidateType = 'cip';
        } else if (candidate.flow === 'featured-app') {
          candidateType = 'featured-app';
        } else if (candidate.flow === 'shared') {
          // Use specific vote proposal type flags for better accuracy
          if (candidate.identifiers.isCipVoteProposal || candidateHasCip || candidateIsCipDiscussion) {
            candidateType = 'cip';
          } else if (candidate.identifiers.isValidatorVoteProposal || candidateIsValidatorOperations || candidate.identifiers.validatorName) {
            candidateType = 'validator';
          } else if (candidate.identifiers.isFeaturedAppVoteProposal || candidate.identifiers.appName) {
            candidateType = 'featured-app';
          } else if (candidateIsVoteProposal) {
            // Generic vote proposal - check for network prefix
            if (/(?:mainnet|testnet|main\s*net|test\s*net):/i.test(candidateSubject)) {
              candidateType = 'featured-app';
            } else {
              candidateType = 'featured-app';
            }
          } else {
            candidateType = 'featured-app';
          }
        } else {
          candidateType = 'featured-app';
        }
        
        // Only correlate if types match
        if (candidateType !== item.type) {
          continue;
        }
        
        // Use fuzzy matching function (handles normalization, substrings, Levenshtein distance)
        if (!entitiesMatch(topicEntityName, candidateEntity)) {
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
  
  // Diagnostic: find topics that weren't correlated
  const unaccounted = sortedTopics.filter(t => !used.has(t.id));
  if (unaccounted.length > 0) {
    console.log(`⚠️ ${unaccounted.length} topics not in any lifecycle item:`);
    for (const t of unaccounted.slice(0, 10)) {
      console.log(`   - ID: ${t.id} | Group: ${t.groupName} | Subject: "${t.subject.slice(0, 50)}..."`);
    }
    if (unaccounted.length > 10) {
      console.log(`   ... and ${unaccounted.length - 10} more`);
    }
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

// Fix types based on primaryId patterns (for cached data that may have incorrect types)
function fixLifecycleItemTypes(data) {
  if (!data || !data.lifecycleItems) return data;
  
  for (const item of data.lifecycleItems) {
    // Fix type based on primaryId patterns
    if (item.primaryId) {
      const pid = item.primaryId.toUpperCase();
      if (pid.includes('CIP-') || pid.includes('CIP ') || pid.startsWith('CIP')) {
        item.type = 'cip';
      } else if (item.type === 'other') {
        // Check topics for better type inference
        const hasAppName = item.topics?.some(t => t.identifiers?.appName);
        const hasValidatorName = item.topics?.some(t => t.identifiers?.validatorName);
        if (hasAppName) item.type = 'featured-app';
        else if (hasValidatorName) item.type = 'validator';
      }
    }
  }
  
  return data;
}

// Helper to read cached data
function readCache() {
  try {
    if (fs.existsSync(CACHE_FILE)) {
      const data = JSON.parse(fs.readFileSync(CACHE_FILE, 'utf-8'));
      // Fix types in cached data
      return fixLifecycleItemTypes(data);
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
    
    const mappedTopics = topics.map(topic => {
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
        // Inference metadata - will be populated below if INFERENCE_ENABLED
        postedStage: group.stage,  // Original forum-derived stage (preserved)
        inferredStage: null,       // Model output label
        inferenceConfidence: null, // Model confidence score
        effectiveStage: group.stage, // inferredStage if confidence >= THRESHOLD else postedStage
      };
    });
    
    allTopics.push(...mappedTopics);
    await delay(500);
  }
  
  console.log(`Total topics across all groups: ${allTopics.length}`);
  
  // ========== INFERENCE STEP (additive metadata only) ==========
  // Run zero-shot NLI classification on all topics in BATCH if inference is enabled
  // This adds postedStage, inferredStage, inferenceConfidence, effectiveStage
  // Does NOT change grouping, correlation, or UI behavior
  if (INFERENCE_ENABLED) {
    console.log('🧠 Running batch inference classification on topics...');
    
    try {
      // Run batch inference - loads model once, processes all topics
      const inferenceResults = await inferStagesBatch(
        allTopics.map(t => ({ id: t.id, subject: t.subject, content: t.content })),
        (processed, total) => console.log(`🧠 Progress: ${processed}/${total} topics...`)
      );
      
      let overrideCount = 0;
      
      // Apply results to topics
      for (const topic of allTopics) {
        const result = inferenceResults.get(topic.id);
        if (result) {
          topic.inferredStage = result.stage;
          topic.inferenceConfidence = result.confidence;
          
          // Override effectiveStage only if confidence meets threshold
          if (result.confidence >= INFERENCE_THRESHOLD) {
            topic.effectiveStage = result.stage;
            overrideCount++;
          }
        }
      }
      
      console.log(`🧠 Inference complete: ${inferenceResults.size}/${allTopics.length} classified, ${overrideCount} overrides (threshold: ${INFERENCE_THRESHOLD})`);
    } catch (err) {
      console.error('[inference] Batch inference failed:', err.message);
    }
  } else {
    console.log('ℹ️ Inference disabled (set INFERENCE_ENABLED=true to enable)');
  }
  // ========== END INFERENCE STEP ==========
  
  // Correlate topics into lifecycle items
  const lifecycleItems = correlateTopics(allTopics);
  
  // Count topics in lifecycle items to verify none are dropped
  const topicsInItems = lifecycleItems.reduce((sum, item) => sum + (item.topics?.length || 0), 0);
  const standaloneItems = lifecycleItems.filter(item => (item.topics?.length || 0) === 1).length;
  const groupedItems = lifecycleItems.filter(item => (item.topics?.length || 0) > 1).length;
  
  console.log(`📊 Correlated ${allTopics.length} topics → ${lifecycleItems.length} lifecycle items`);
  console.log(`   ├─ ${topicsInItems}/${allTopics.length} topics accounted for ${topicsInItems === allTopics.length ? '✓' : '⚠️ MISMATCH'}`);
  console.log(`   ├─ ${groupedItems} grouped items (multiple topics)`);
  console.log(`   └─ ${standaloneItems} standalone items (single topic)`)
  
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
      'protocol-upgrade': lifecycleItems.filter(i => i.type === 'protocol-upgrade').length,
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
    // Check if we have stale cache to serve (even on force refresh when no API key)
    const staleCache = readCache();
    if (staleCache) {
      console.log('No API key configured, serving existing cache');
      return res.json({ ...staleCache, stale: true, warning: 'GROUPS_IO_API_KEY not configured - showing cached data' });
    }
    
    // No cache AND no API key - return empty data with 200 (not 500) so UI can handle gracefully
    console.warn('No GROUPS_IO_API_KEY and no cache available');
    return res.json({ 
      lifecycleItems: [],
      allTopics: [],
      groups: {},
      stats: { totalTopics: 0, lifecycleItems: 0, byType: {}, byStage: {}, groupCounts: {} },
      warning: 'GROUPS_IO_API_KEY not configured. Please set the API key or load cached data.',
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
    return res.status(500).json({ error: 'GROUPS_IO_API_KEY not configured' });
  }

  try {
    const data = await fetchFreshData();
    writeCache(data);
    return res.json({ success: true, stats: data.stats, cachedAt: data.cachedAt });
  } catch (error) {
    console.error('Error refreshing governance lifecycle:', error);
    res.status(500).json({ error: error.message });
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

// ========== DIAGNOSTIC ENDPOINT (Phase 2) ==========
// Shows high-confidence disagreements between postedStage and inferredStage
// This is for observation only - does NOT change any behavior
router.get('/inference-disagreements', (req, res) => {
  const cached = readCache();
  
  if (!cached || !cached.allTopics) {
    return res.json({
      error: 'No cached data available. Run with INFERENCE_ENABLED=true first.',
      disagreements: [],
      stats: null,
    });
  }
  
  const threshold = parseFloat(req.query.threshold) || INFERENCE_THRESHOLD;
  const allTopics = cached.allTopics;
  
  // Find disagreements: postedStage != inferredStage AND confidence >= threshold
  const disagreements = allTopics
    .filter(topic => {
      return (
        topic.inferredStage !== null &&
        topic.inferenceConfidence !== null &&
        topic.inferredStage !== topic.postedStage &&
        topic.inferenceConfidence >= threshold
      );
    })
    .map(topic => ({
      id: topic.id,
      subject: topic.subject,
      postedStage: topic.postedStage,
      inferredStage: topic.inferredStage,
      confidence: topic.inferenceConfidence,
      groupName: topic.groupName,
      date: topic.date,
      sourceUrl: topic.sourceUrl,
    }))
    .sort((a, b) => b.confidence - a.confidence);
  
  // Calculate stats by stage pair
  const stagePairs = {};
  for (const d of disagreements) {
    const key = `${d.postedStage} → ${d.inferredStage}`;
    stagePairs[key] = (stagePairs[key] || 0) + 1;
  }
  
  // Topics with inference data
  const topicsWithInference = allTopics.filter(t => t.inferredStage !== null);
  const agreements = topicsWithInference.filter(t => t.inferredStage === t.postedStage);
  
  const stats = {
    totalTopics: allTopics.length,
    topicsWithInference: topicsWithInference.length,
    agreementCount: agreements.length,
    disagreementCount: disagreements.length,
    agreementRate: topicsWithInference.length > 0 
      ? ((agreements.length / topicsWithInference.length) * 100).toFixed(1) + '%'
      : 'N/A',
    threshold,
    stagePairBreakdown: stagePairs,
  };
  
  // Console-friendly report
  console.log('\n========== INFERENCE DISAGREEMENT REPORT ==========');
  console.log(`Threshold: ${threshold}`);
  console.log(`Topics with inference: ${stats.topicsWithInference}`);
  console.log(`Agreements: ${stats.agreementCount} (${stats.agreementRate})`);
  console.log(`Disagreements: ${stats.disagreementCount}`);
  console.log('\nStage pair breakdown:');
  for (const [pair, count] of Object.entries(stagePairs).sort((a, b) => b[1] - a[1])) {
    console.log(`  ${pair}: ${count}`);
  }
  console.log('\nTop disagreements:');
  for (const d of disagreements.slice(0, 20)) {
    console.log(`  [${d.confidence.toFixed(2)}] "${d.subject.slice(0, 50)}..."`);
    console.log(`    posted: ${d.postedStage} → inferred: ${d.inferredStage}`);
  }
  console.log('====================================================\n');
  
  res.json({
    stats,
    disagreements,
    cachedAt: cached.cachedAt,
  });
});

export { fetchFreshData, writeCache, readCache };
export default router;
