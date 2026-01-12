import express from 'express';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { inferStagesBatch } from '../inference/inferStage.js';
import { 
  classifyTopicsBatch, 
  classifyTopic,
  isLLMAvailable, 
  getClassificationStats,
  getAllClassifications,
  PROMPT_VERSION,
} from '../inference/llm-classifier.js';
import {
  fetchTopicsContentBatch,
  getTopicContent,
  hasContentCached,
  getContentCacheStats,
} from '../inference/content-fetcher.js';
import {
  getCachedClassification,
  invalidateClassification,
  clearLLMCache,
  getLLMCacheStats,
  getSampleClassifications,
} from '../inference/llm-classification-cache.js';
import {
  getCachedContent,
  clearContentCache,
} from '../inference/post-content-cache.js';
// Hybrid audit system - LLM verifies all rule-based classifications
import {
  isAuditorAvailable,
  verifyAllItems,
  verifyItem,
  getAuditStatus,
  getAuditDisagreements,
  getAuditReviewItems,
  clearAuditCache,
  getSampleAuditEntries,
  getAllAuditEntries,
  AUDIT_VERSION,
} from '../inference/hybrid-auditor.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
// Cache directory - uses DATA_DIR/cache if DATA_DIR is set, otherwise project data/cache
const BASE_DATA_DIR = process.env.DATA_DIR || path.resolve(__dirname, '../../data');
const CACHE_DIR = path.join(BASE_DATA_DIR, 'cache');
const CACHE_FILE = path.join(CACHE_DIR, 'governance-lifecycle.json');
const OVERRIDES_FILE = path.join(CACHE_DIR, 'governance-overrides.json');
const LEARNED_PATTERNS_FILE = path.join(CACHE_DIR, 'learned-patterns.json');

// Inference confidence threshold - only override postedStage if confidence >= threshold
const INFERENCE_THRESHOLD = 0.85;
// Feature flag to enable/disable inference (set via env var)
const INFERENCE_ENABLED = process.env.INFERENCE_ENABLED === 'true';
const router = express.Router();

// ========== LEARNED PATTERNS SYSTEM ==========
// Load patterns learned from manual corrections
let _learnedPatternsCache = null;
let _learnedPatternsMtime = 0;

function getLearnedPatterns() {
  try {
    if (!fs.existsSync(LEARNED_PATTERNS_FILE)) {
      return null;
    }
    const stats = fs.statSync(LEARNED_PATTERNS_FILE);
    // Reload if file changed
    if (stats.mtimeMs > _learnedPatternsMtime) {
      const data = JSON.parse(fs.readFileSync(LEARNED_PATTERNS_FILE, 'utf8'));
      _learnedPatternsCache = data.patterns || null;
      _learnedPatternsMtime = stats.mtimeMs;
      console.log('📚 Loaded learned patterns:', {
        validators: _learnedPatternsCache?.validatorKeywords?.length || 0,
        apps: _learnedPatternsCache?.featuredAppKeywords?.length || 0,
        cips: _learnedPatternsCache?.cipKeywords?.length || 0,
        entities: Object.keys(_learnedPatternsCache?.entityNameMappings || {}).length,
      });
    }
    return _learnedPatternsCache;
  } catch (e) {
    console.error('Failed to load learned patterns:', e.message);
    return null;
  }
}

// Check if text matches learned keywords for a type
function matchesLearnedKeywords(text, keywords) {
  if (!keywords || keywords.length === 0) return false;
  const textLower = text.toLowerCase();
  return keywords.some(kw => textLower.includes(kw.toLowerCase()));
}

// Get learned type override for an entity name
function getLearnedEntityType(entityName) {
  const patterns = getLearnedPatterns();
  if (!patterns?.entityNameMappings || !entityName) return null;
  const key = entityName.toLowerCase();
  return patterns.entityNameMappings[key] || null;
}

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
  
  // Pattern 0a-pre: "Node as a Service" patterns - extract this as entity name for validator approvals
  const naasMatch = cleanText.match(/^(node\s+as\s+a\s+service)\s+(?:reviewed\s+and\s+)?approved/i);
  if (naasMatch) {
    const name = 'Node as a Service';
    console.log(`EXTRACT: "Node as a Service" pattern matched, extracted: "${name}" from "${cleanText.slice(0, 60)}"`);
    return { name, isMultiEntity: false };
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
  
  // Pattern 0: "to Feature AppName" or "to implement/apply Featured Application status for AppName"
  // "for Featured Application status for AppName"
  // "for featured app rights for AppName"
  // Also handles "to Feature the Console Wallet" -> extracts "Console Wallet"
  const toFeatureMatch = cleanText.match(/to\s+feature\s+(?:the\s+)?([A-Za-z0-9][\w\s-]*?)$/i);
  if (toFeatureMatch) {
    let name = toFeatureMatch[1].trim();
    // Remove trailing "by Company" if present
    name = name.replace(/\s+by\s+.*$/i, '').trim();
    if (name.length > 1) {
      console.log(`EXTRACT: "to Feature" pattern matched, extracted: "${name}" from "${cleanText.slice(0, 60)}"`);
      return { name, isMultiEntity: false };
    }
  }
  
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
  
  // Extract CIP numbers (e.g., CIP-123, CIP 123, CIP#123, CIP - 0066, or standalone "0054 Add Figment...")
  // Check for TBD/unassigned CIPs first
  const tbdMatch = text.match(/CIP\s*[-#]?\s*(TBD|00XX|XXXX|\?\?|unassigned)/i);
  if (tbdMatch) {
    identifiers.cipNumber = 'CIP-00XX';
  } else {
    // Handle various CIP formats: "CIP-0066", "CIP 0066", "CIP#0066", "CIP - 0066"
    const cipMatch = text.match(/CIP\s*[-#]?\s*(\d{2,})/i);
    if (cipMatch) {
      identifiers.cipNumber = `CIP-${cipMatch[1].padStart(4, '0')}`;
      console.log(`EXTRACTED CIP: "${identifiers.cipNumber}" from "${text.slice(0, 60)}"`);
    } else {
      // Also check for standalone 4-digit number at the start (e.g., "0054 Add Figment...")
      // This catches CIP announcements that don't include "CIP-" prefix
      const standaloneMatch = text.match(/^\s*0*(\d{4})\s+/);
      if (standaloneMatch) {
        identifiers.cipNumber = `CIP-${standaloneMatch[1]}`;
        console.log(`EXTRACTED standalone CIP: "${identifiers.cipNumber}" from "${text.slice(0, 60)}"`);
      }
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
  const learnedPatterns = getLearnedPatterns();
  
  const isFeaturedApp = !isCipDiscussion && (
    /featured\s*app|featured\s*application|app\s+(?:application|listing|request|tokenomics|vote|approved)|application\s+status\s+for/i.test(text) ||
    isFeaturedAppVoteProposal ||
    /featured\s+app\s+rights/i.test(text) ||
    // Check learned patterns
    matchesLearnedKeywords(text, learnedPatterns?.featuredAppKeywords)
  );
  
  // Check if text contains validator indicators - including "validator approved", "validator operator approved", 
  // "node as a service", "validator operators approved", etc.
  const isValidator = (
    /super\s*validator|validator\s+(?:approved|application|onboarding|license|candidate|operator\s+approved)|sv\s+(?:application|onboarding)|validator\s+operator|node\s+as\s+a\s+service|validator\s+operators\s+approved/i.test(text) ||
    isValidatorVoteProposal ||
    // Check learned patterns
    matchesLearnedKeywords(text, learnedPatterns?.validatorKeywords)
  );
  
  // Check learned CIP keywords
  const hasCipKeywords = matchesLearnedKeywords(text, learnedPatterns?.cipKeywords);
  
  // Extract the primary entity name (now returns { name, isMultiEntity })
  const entityResult = extractPrimaryEntityName(text);
  identifiers.entityName = entityResult.name;
  identifiers.isMultiEntity = entityResult.isMultiEntity;
  
  // Check if entity has a learned type override
  const learnedEntityType = getLearnedEntityType(entityResult.name);
  if (learnedEntityType) {
    identifiers.learnedType = learnedEntityType;
  }
  
  // Add CIP discussion flag to identifiers (include learned keywords)
  identifiers.isCipDiscussion = isCipDiscussion || hasCipKeywords;
  
  // Add vote proposal type flags for better type determination
  identifiers.isCipVoteProposal = isCipVoteProposal;
  identifiers.isFeaturedAppVoteProposal = isFeaturedAppVoteProposal;
  identifiers.isValidatorVoteProposal = isValidatorVoteProposal;
  identifiers.isValidator = isValidator;  // Direct validator indicator flag
  identifiers.isFeaturedApp = isFeaturedApp;  // Direct featured app indicator flag
  
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
    // LEARNED PATTERNS: If entity has a learned type, use it as override
    let type;
    
    // Check for learned type override first (from manual corrections)
    if (topic.identifiers.learnedType) {
      type = topic.identifiers.learnedType;
      console.log(`📚 Using learned type for "${topicEntityName}": ${type}`);
    } else if (isOutcome) {
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
      } else if (topic.identifiers.isValidatorVoteProposal || topic.identifiers.isValidator || hasValidatorIndicator || isValidatorOperations) {
        type = 'validator';
      } else if (topic.identifiers.isFeaturedAppVoteProposal || hasAppIndicator) {
        type = 'featured-app';
      } else {
        // Default for ambiguous items posted in CIP groups
        type = 'other';
      }
    } else if (topic.flow === 'featured-app') {
      // tokenomics-announce is specifically for featured-app flow
      // But check if it's actually a validator approval
      if (/validator.*approved|approved.*validator|validator\s*operator.*approved/i.test(subjectTrimmed)) {
        type = 'validator';
      } else {
        type = 'featured-app';
      }
    } else if (topic.flow === 'shared') {
      // Shared groups (tokenomics, sv-announce) need subject-line disambiguation
      // Use specific vote proposal type flags for better accuracy
      // STRICT CIP detection: only classify as CIP if there's an actual CIP number
      // isCipDiscussion alone is NOT enough - many topics mention "CIP" without being CIPs
      if (topic.identifiers.isCipVoteProposal || hasCip) {
        // Only CIP-specific vote proposals or topics with explicit CIP numbers
        type = 'cip';
      } else if (topic.identifiers.isValidatorVoteProposal || topic.identifiers.isValidator || isValidatorOperations || hasValidatorIndicator) {
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
          // tokenomics-announce is primarily featured-app flow, but can contain validator approvals
          if (/validator.*approved|approved.*validator|validator\s*operator.*approved/i.test(candidateSubject)) {
            candidateType = 'validator';
          } else {
            candidateType = 'featured-app';
          }
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
    
    // Create standalone items for unaccounted topics so they're not lost
    for (const topic of unaccounted) {
      const item = {
        id: `lifecycle-${topic.id}`,
        primaryId: topic.identifiers.entityName || topic.subject,
        type: 'other',
        network: topic.identifiers.network,
        stages: { [topic.stage]: [topic] },
        topics: [topic],
        firstDate: topic.date,
        lastDate: topic.date,
        currentStage: topic.stage,
        wasUnaccounted: true, // Flag for debugging
      };
      lifecycleItems.push(item);
      used.add(topic.id);
    }
    console.log(`   → Created ${unaccounted.length} standalone items for unaccounted topics`);
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

// ========== HYBRID AUDIT MODEL ==========
// Design principle: "assumed semantics vs verified semantics"
// - Rule-based = assumed semantics (deterministic, fast, may be wrong)
// - LLM-verified = verified semantics (reads content, cached permanently)
//
// This implements Option 3 - Hybrid Audit Model:
// 1. Rules classify everything (deterministic)
// 2. LLM reads content and confirms OR flags disagreement
// 3. Disagreements are flagged for human review
// 4. Results are cached and treated as authoritative

async function runHybridAudit(lifecycleItems, allTopics) {
  if (!isAuditorAvailable()) {
    console.log('⚠️ Hybrid audit unavailable (OPENAI_API_KEY not set)');
    console.log('   All items have assumed semantics only (rule-based)');
    return lifecycleItems;
  }
  
  console.log('\n========== HYBRID AUDIT MODEL ==========');
  console.log('Key distinction: assumed semantics vs verified semantics');
  console.log('- Rule-based: assumed (may be wrong)');
  console.log('- LLM-verified: verified (reads actual content)');
  console.log('=========================================\n');
  
  // Step 1: Fetch content for ALL items (not just ambiguous)
  // This is a one-time pass - content is cached permanently
  const topicsNeedingContent = [];
  for (const item of lifecycleItems) {
    for (const topic of (item.topics || [])) {
      if (!hasContentCached(topic.id)) {
        topicsNeedingContent.push({
          id: topic.id,
          groupName: topic.groupName,
        });
      }
    }
  }
  
  if (topicsNeedingContent.length > 0) {
    console.log(`📄 Fetching content for ${topicsNeedingContent.length} topics (one-time pass)...`);
    await fetchTopicsContentBatch(topicsNeedingContent, {
      onProgress: (done, total) => {
        if (done % 20 === 0 || done === total) {
          console.log(`📄 Content fetch progress: ${done}/${total}`);
        }
      }
    });
  }
  
  // Step 2: Run hybrid audit on ALL items
  const auditResult = await verifyAllItems(lifecycleItems, {
    onProgress: (done, total) => {
      if (done % 20 === 0 || done === total) {
        console.log(`📋 Audit progress: ${done}/${total}`);
      }
    }
  });
  
  // Step 3: Annotate lifecycle items with audit data
  const auditEntries = getAllAuditEntries();
  let annotated = 0;
  
  for (const item of lifecycleItems) {
    const itemId = item.id || item.primaryId;
    const auditEntry = auditEntries.get(itemId);
    
    if (auditEntry) {
      // Store both rule-based and LLM types for comparison
      item.ruleType = auditEntry.rule_type;
      item.llmType = auditEntry.llm_type;
      item.agreement = auditEntry.agreement;
      item.needsReview = auditEntry.needs_review;
      item.llmConfidence = auditEntry.llm_confidence;
      item.llmReasoning = auditEntry.llm_reasoning;
      item.verifiedSemantics = true;
      
      // If there's disagreement AND LLM has high confidence, use LLM type
      // Otherwise keep rule-based type as canonical
      if (!auditEntry.agreement && auditEntry.llm_confidence >= 0.85) {
        item.type = auditEntry.llm_type;
        item.typeSource = 'llm_override';
      } else {
        item.typeSource = 'rule';
      }
      
      annotated++;
    } else {
      item.verifiedSemantics = false;
      item.typeSource = 'rule';
    }
  }
  
  console.log(`\n📋 Annotated ${annotated} items with audit data`);
  console.log(`   Agreements: ${auditResult.agreements} | Disagreements: ${auditResult.disagreements}`);
  
  return lifecycleItems;
}

// Legacy function for backward compatibility
async function classifyAmbiguousItems(lifecycleItems, allTopics) {
  // Now delegates to hybrid audit
  return runHybridAudit(lifecycleItems, allTopics);
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

// ========== MANUAL OVERRIDES SYSTEM ==========
// Allows manual correction of type classifications that persist across refreshes

// Read overrides from file
function readOverrides() {
  try {
    if (fs.existsSync(OVERRIDES_FILE)) {
      const data = JSON.parse(fs.readFileSync(OVERRIDES_FILE, 'utf-8'));
      // Ensure all override types exist
      return {
        itemOverrides: data.itemOverrides || {},
        topicOverrides: data.topicOverrides || {},
        mergeOverrides: data.mergeOverrides || {},
        extractOverrides: data.extractOverrides || {},
        moveOverrides: data.moveOverrides || {},
      };
    }
  } catch (err) {
    console.error('Error reading overrides:', err.message);
  }
  return { itemOverrides: {}, topicOverrides: {}, mergeOverrides: {}, extractOverrides: {}, moveOverrides: {} };
}

// Write overrides to file
function writeOverrides(overrides) {
  try {
    if (!fs.existsSync(CACHE_DIR)) {
      fs.mkdirSync(CACHE_DIR, { recursive: true });
    }
    fs.writeFileSync(OVERRIDES_FILE, JSON.stringify(overrides, null, 2));
    console.log(`✅ Saved overrides to ${OVERRIDES_FILE}`);
    return true;
  } catch (err) {
    console.error('Error writing overrides:', err.message);
    return false;
  }
}

// ========== AUDIT LOG SYSTEM ==========
const AUDIT_LOG_FILE = path.join(CACHE_DIR, 'override-audit-log.json');

// Read audit log
function readAuditLog() {
  try {
    if (fs.existsSync(AUDIT_LOG_FILE)) {
      const data = JSON.parse(fs.readFileSync(AUDIT_LOG_FILE, 'utf8'));
      return Array.isArray(data) ? data : [];
    }
  } catch (err) {
    console.error('Error reading audit log:', err.message);
  }
  return [];
}

// Write audit log
function writeAuditLog(entries) {
  try {
    if (!fs.existsSync(CACHE_DIR)) {
      fs.mkdirSync(CACHE_DIR, { recursive: true });
    }
    fs.writeFileSync(AUDIT_LOG_FILE, JSON.stringify(entries, null, 2));
    return true;
  } catch (err) {
    console.error('Error writing audit log:', err.message);
    return false;
  }
}

// Log an override action
function logOverrideAction(action) {
  const entries = readAuditLog();
  const entry = {
    id: `audit-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    timestamp: new Date().toISOString(),
    ...action,
  };
  entries.push(entry);
  writeAuditLog(entries);
  console.log(`📝 Audit log: ${action.actionType} - ${action.targetId} (${action.originalValue || 'n/a'} → ${action.newValue || 'n/a'})`);
  return entry;
}

// Helper to find original classification for an item/topic
function findOriginalClassification(targetId, targetType = 'item') {
  const cached = readCache();
  if (!cached || !cached.lifecycleItems) return null;
  
  if (targetType === 'item') {
    const item = cached.lifecycleItems.find(i => i.id === targetId || i.primaryId === targetId);
    return item ? { type: item.type, primaryId: item.primaryId, id: item.id } : null;
  } else if (targetType === 'topic') {
    for (const item of cached.lifecycleItems) {
      const topic = (item.topics || []).find(t => String(t.id) === String(targetId));
      if (topic) {
        return { 
          parentType: item.type, 
          parentId: item.primaryId, 
          stage: topic.effectiveStage || topic.stage,
          subject: topic.subject?.slice(0, 100),
        };
      }
    }
  }
  return null;
}

// Apply overrides to lifecycle data (type changes and merges)
function applyOverrides(data) {
  if (!data || !data.lifecycleItems) return data;
  
  const overrides = readOverrides();
  const hasItemOverrides = overrides.itemOverrides && Object.keys(overrides.itemOverrides).length > 0;
  const hasMergeOverrides = overrides.mergeOverrides && Object.keys(overrides.mergeOverrides).length > 0;
  const hasTopicOverrides = overrides.topicOverrides && Object.keys(overrides.topicOverrides).length > 0;
  const hasExtractOverrides = overrides.extractOverrides && Object.keys(overrides.extractOverrides).length > 0;
  
  if (!hasItemOverrides && !hasMergeOverrides && !hasTopicOverrides && !hasExtractOverrides) {
    return data;
  }
  
  let appliedCount = 0;
  let mergeCount = 0;
  let topicOverrideCount = 0;
  let extractCount = 0;
  
  // First pass: Apply type overrides
  if (hasItemOverrides) {
    data.lifecycleItems = data.lifecycleItems.map(item => {
      const override = overrides.itemOverrides[item.id] || overrides.itemOverrides[item.primaryId];
      if (override) {
        console.log(`Applying type override: "${item.primaryId}" -> type=${override.type}`);
        appliedCount++;
        return {
          ...item,
          type: override.type,
          overrideApplied: true,
          overrideReason: override.reason || 'Manual correction',
        };
      }
      return item;
    });
  }
  
  // Second pass: Apply merge overrides
  // Supports:
  //  - Item-level merges: key matches lifecycle item id or primaryId
  //  - Topic-level merges: key matches a topic id (moves just that topic)
  if (hasMergeOverrides) {
    const targetItems = new Map();

    // Build target lookup by primaryId
    data.lifecycleItems.forEach(item => {
      if (item.primaryId) {
        targetItems.set(item.primaryId.toUpperCase(), item);
      }
    });

    const mergedItemIds = new Set();
    const mergedItemPrimaryIds = new Set();

    // Helper to merge a single topic into one or multiple targets
    const mergeTopicIntoTargets = (topic, targets, sourcePrimaryId) => {
      targets.forEach(targetCip => {
        const normalizedTarget = String(targetCip).toUpperCase();
        const target = targetItems.get(normalizedTarget);
        if (!target) return;

        // If we're merging into a CIP, map non-CIP stages into the CIP workflow
        // so the merged topic is actually visible on the CIP card.
        let stage = topic.stage;
        if (target.type === 'cip' && WORKFLOW_STAGES.cip && !WORKFLOW_STAGES.cip.includes(stage)) {
          stage = WORKFLOW_STAGES.cip[0];
        }

        const topicForTarget = stage === topic.stage ? topic : { ...topic, stage };

        if (!target.topics.find(t => t.id === topicForTarget.id)) {
          target.topics.push(topicForTarget);
        }

        if (!target.stages[stage]) target.stages[stage] = [];
        if (!target.stages[stage].find(t => t.id === topicForTarget.id)) {
          target.stages[stage].push(topicForTarget);
        }

        target.mergedFrom = target.mergedFrom || [];
        if (sourcePrimaryId && !target.mergedFrom.includes(sourcePrimaryId)) {
          target.mergedFrom.push(sourcePrimaryId);
        }
      });
    };

    // Walk items and apply both item-level and topic-level merges
    data.lifecycleItems.forEach(item => {
      const itemMergeOverride = overrides.mergeOverrides[item.id] || overrides.mergeOverrides[item.primaryId];

      // Item-level merge: move ALL topics to targets, remove the item afterwards
      if (itemMergeOverride) {
        const targets = Array.isArray(itemMergeOverride.mergeInto)
          ? itemMergeOverride.mergeInto
          : [itemMergeOverride.mergeInto];

        console.log(`Merging item "${item.primaryId}" into ${targets.join(', ')}`);

        item.topics.forEach(topic => {
          mergeTopicIntoTargets(topic, targets, item.primaryId);
        });

        // Update date ranges on all targets
        targets.forEach(targetCip => {
          const target = targetItems.get(String(targetCip).toUpperCase());
          if (!target) return;
          if (new Date(item.firstDate) < new Date(target.firstDate)) target.firstDate = item.firstDate;
          if (new Date(item.lastDate) > new Date(target.lastDate)) target.lastDate = item.lastDate;
        });

        mergedItemIds.add(item.id);
        mergedItemPrimaryIds.add(item.primaryId);
        mergeCount++;
        return;
      }

      // Topic-level merge: move only the topics that have an override keyed by topic.id
      if (!item.topics || item.topics.length === 0) return;

      const topicsToMove = [];
      const remainingTopics = [];

      item.topics.forEach(topic => {
        // Check by stringified topic.id and also by topic subject as fallback
        const topicIdStr = String(topic.id);
        let topicMergeOverride = overrides.mergeOverrides[topicIdStr];
        
        // Fallback: check if override was saved by subject (for older overrides or debugging)
        if (!topicMergeOverride && topic.subject) {
          topicMergeOverride = overrides.mergeOverrides[topic.subject];
        }
        
        if (topicMergeOverride) {
          const targets = Array.isArray(topicMergeOverride.mergeInto)
            ? topicMergeOverride.mergeInto
            : [topicMergeOverride.mergeInto];

          console.log(`Merging topic id="${topicIdStr}" subject="${topic.subject?.slice(0, 50)}" (from "${item.primaryId}") into ${targets.join(', ')}`);
          topicsToMove.push({ topic, targets });
          mergeCount++;
        } else {
          remainingTopics.push(topic);
        }
      });

      if (topicsToMove.length === 0) return;

      // Apply topic moves
      topicsToMove.forEach(({ topic, targets }) => {
        mergeTopicIntoTargets(topic, targets, item.primaryId);
      });

      // Remove moved topics from item.topics
      item.topics = remainingTopics;

      // Also remove moved topics from item.stages
      if (item.stages) {
        Object.keys(item.stages).forEach(stageKey => {
          item.stages[stageKey] = (item.stages[stageKey] || []).filter(t => {
            const tIdStr = String(t.id);
            return !overrides.mergeOverrides[tIdStr] && !overrides.mergeOverrides[t.subject];
          });
          if (item.stages[stageKey].length === 0) {
            delete item.stages[stageKey];
          }
        });
      }

      // Recompute date range based on remaining topics
      if (item.topics.length > 0) {
        const dates = item.topics
          .map(t => t.date)
          .filter(Boolean)
          .map(d => new Date(d))
          .filter(d => !isNaN(d.getTime()));

        if (dates.length > 0) {
          item.firstDate = new Date(Math.min(...dates.map(d => d.getTime()))).toISOString();
          item.lastDate = new Date(Math.max(...dates.map(d => d.getTime()))).toISOString();
        }
      } else {
        // If nothing left, remove the whole item
        mergedItemIds.add(item.id);
        mergedItemPrimaryIds.add(item.primaryId);
      }
    });

    // Remove merged/emptied items from the list
    if (mergedItemIds.size > 0 || mergedItemPrimaryIds.size > 0) {
      data.lifecycleItems = data.lifecycleItems.filter(item =>
        !mergedItemIds.has(item.id) && !mergedItemPrimaryIds.has(item.primaryId)
      );
    }
  }
  
  // Apply move overrides: move a topic from one card to a specific target card by ID
  const hasMoveOverrides = overrides.moveOverrides && Object.keys(overrides.moveOverrides).length > 0;
  if (hasMoveOverrides) {
    // Build a map from card id/primaryId to item
    const cardMap = new Map();
    data.lifecycleItems.forEach(item => {
      cardMap.set(String(item.id), item);
      cardMap.set(String(item.primaryId), item);
    });
    
    const movedTopicIds = new Set();
    
    data.lifecycleItems.forEach(item => {
      if (!item.topics) return;
      
      const topicsToMove = [];
      const remainingTopics = [];
      
      item.topics.forEach(topic => {
        const topicIdStr = String(topic.id);
        const moveOverride = overrides.moveOverrides[topicIdStr];
        
        if (moveOverride && moveOverride.targetCardId) {
          const targetCard = cardMap.get(moveOverride.targetCardId);
          if (targetCard && targetCard.id !== item.id) {
            topicsToMove.push({ topic, targetCard });
            movedTopicIds.add(topicIdStr);
          } else {
            // Target not found or same card, keep in place
            remainingTopics.push(topic);
          }
        } else {
          remainingTopics.push(topic);
        }
      });
      
      // Move topics to target cards
      topicsToMove.forEach(({ topic, targetCard }) => {
        console.log(`Moving topic "${topic.subject?.slice(0, 50)}" from "${item.primaryId}" to "${targetCard.primaryId}"`);
        
        // Add to target card
        targetCard.topics = targetCard.topics || [];
        targetCard.topics.push(topic);
        
        // Add to target's stages
        const stage = topic.effectiveStage || topic.stage;
        if (stage) {
          targetCard.stages = targetCard.stages || {};
          targetCard.stages[stage] = targetCard.stages[stage] || [];
          targetCard.stages[stage].push(topic);
        }
        
        // Update target date range
        if (topic.date) {
          const topicDate = new Date(topic.date);
          if (!isNaN(topicDate.getTime())) {
            if (!targetCard.firstDate || topicDate < new Date(targetCard.firstDate)) {
              targetCard.firstDate = topicDate.toISOString();
            }
            if (!targetCard.lastDate || topicDate > new Date(targetCard.lastDate)) {
              targetCard.lastDate = topicDate.toISOString();
            }
          }
        }
      });
      
      // Update source item
      item.topics = remainingTopics;
      
      // Remove from source stages
      if (item.stages && movedTopicIds.size > 0) {
        Object.keys(item.stages).forEach(stageKey => {
          item.stages[stageKey] = (item.stages[stageKey] || []).filter(t => !movedTopicIds.has(String(t.id)));
          if (item.stages[stageKey].length === 0) {
            delete item.stages[stageKey];
          }
        });
      }
    });
    
    // Remove empty items
    data.lifecycleItems = data.lifecycleItems.filter(item => item.topics && item.topics.length > 0);
    
    console.log(`Applied ${movedTopicIds.size} move overrides`);
  }
  
  // Third pass: Apply topic-level type overrides (move individual topics to different categories)
  if (hasTopicOverrides) {
    const topicsToMove = []; // { topic, sourceItem, newType }
    
    // Identify topics that need to be moved
    data.lifecycleItems.forEach(item => {
      if (!item.topics) return;
      
      item.topics.forEach(topic => {
        const topicIdStr = String(topic.id);
        const override = overrides.topicOverrides[topicIdStr];
        if (override && override.type !== item.type) {
          topicsToMove.push({ topic, sourceItem: item, newType: override.type, reason: override.reason });
        }
      });
    });
    
    // Move topics to their new type categories
    topicsToMove.forEach(({ topic, sourceItem, newType, reason }) => {
      // Remove from source item
      sourceItem.topics = sourceItem.topics.filter(t => String(t.id) !== String(topic.id));
      
      // Remove from source stages
      if (sourceItem.stages) {
        Object.keys(sourceItem.stages).forEach(stageKey => {
          sourceItem.stages[stageKey] = (sourceItem.stages[stageKey] || []).filter(t => String(t.id) !== String(topic.id));
          if (sourceItem.stages[stageKey].length === 0) {
            delete sourceItem.stages[stageKey];
          }
        });
      }
      
      // Find or create target lifecycle item of the new type
      // For topic reclassification, we create a new standalone item for the topic
      const newItemId = `topic-reclassified-${topic.id}`;
      let targetItem = data.lifecycleItems.find(i => i.id === newItemId);
      
      if (!targetItem) {
        // Determine appropriate stages for the new type
        const targetStages = WORKFLOW_STAGES[newType] || WORKFLOW_STAGES.other;
        const effectiveStage = targetStages.includes(topic.stage) ? topic.stage : targetStages[0];
        
        targetItem = {
          id: newItemId,
          primaryId: topic.subject?.slice(0, 80) || `Reclassified Topic ${topic.id}`,
          type: newType,
          network: sourceItem.network || null,
          stages: { [effectiveStage]: [{ ...topic, stage: effectiveStage }] },
          topics: [{ ...topic, stage: effectiveStage }],
          firstDate: topic.date,
          lastDate: topic.date,
          currentStage: effectiveStage,
          overrideApplied: true,
          overrideReason: reason || 'Topic reclassified',
        };
        data.lifecycleItems.push(targetItem);
      }
      
      topicOverrideCount++;
      console.log(`Moved topic "${topic.subject?.slice(0, 50)}" from ${sourceItem.type} to ${newType}`);
    });
    
    // Remove any source items that are now empty
    data.lifecycleItems = data.lifecycleItems.filter(item => 
      (item.topics && item.topics.length > 0) || item.id.startsWith('topic-reclassified-') || item.id.startsWith('topic-extracted-')
    );
  }
  
  // Fourth pass: Apply extract overrides (extract a topic to its own card, keeping same type)
  if (hasExtractOverrides) {
    const topicsToExtract = []; // { topic, sourceItem, customName }
    
    // Identify topics that need to be extracted
    data.lifecycleItems.forEach(item => {
      if (!item.topics) return;
      
      item.topics.forEach(topic => {
        const topicIdStr = String(topic.id);
        const override = overrides.extractOverrides[topicIdStr];
        if (override) {
          topicsToExtract.push({ topic, sourceItem: item, customName: override.customName, reason: override.reason });
        }
      });
    });
    
    // Extract topics to their own cards
    topicsToExtract.forEach(({ topic, sourceItem, customName, reason }) => {
      // Remove from source item
      sourceItem.topics = sourceItem.topics.filter(t => String(t.id) !== String(topic.id));
      
      // Remove from source stages
      if (sourceItem.stages) {
        Object.keys(sourceItem.stages).forEach(stageKey => {
          sourceItem.stages[stageKey] = (sourceItem.stages[stageKey] || []).filter(t => String(t.id) !== String(topic.id));
          if (sourceItem.stages[stageKey].length === 0) {
            delete sourceItem.stages[stageKey];
          }
        });
      }
      
      // Create a new standalone item for the extracted topic (keeping same type)
      const newItemId = `topic-extracted-${topic.id}`;
      const effectiveStage = topic.stage || Object.keys(sourceItem.stages || {})[0] || 'announced';
      
      const extractedItem = {
        id: newItemId,
        primaryId: customName || topic.subject?.slice(0, 80) || `Extracted Topic ${topic.id}`,
        type: sourceItem.type, // Keep the same type
        network: sourceItem.network || null,
        stages: { [effectiveStage]: [{ ...topic, stage: effectiveStage }] },
        topics: [{ ...topic, stage: effectiveStage }],
        firstDate: topic.date,
        lastDate: topic.date,
        currentStage: effectiveStage,
        overrideApplied: true,
        overrideReason: reason || 'Topic extracted to own card',
        extractedFrom: sourceItem.primaryId,
      };
      data.lifecycleItems.push(extractedItem);
      
      extractCount++;
      console.log(`Extracted topic "${topic.subject?.slice(0, 50)}" from "${sourceItem.primaryId}" to its own card${customName ? ` with name "${customName}"` : ''}`);
    });
    
    // Remove any source items that are now empty
    data.lifecycleItems = data.lifecycleItems.filter(item => 
      (item.topics && item.topics.length > 0) || item.id.startsWith('topic-reclassified-') || item.id.startsWith('topic-extracted-')
    );
  }
  
  if (appliedCount > 0) console.log(`Applied ${appliedCount} type overrides`);
  if (mergeCount > 0) console.log(`Applied ${mergeCount} merge overrides`);
  if (topicOverrideCount > 0) console.log(`Applied ${topicOverrideCount} topic type overrides`);
  if (extractCount > 0) console.log(`Applied ${extractCount} extract overrides`);
  
  return data;
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
  let lifecycleItems = correlateTopics(allTopics);
  
  // ========== LLM CLASSIFICATION STEP ==========
  // Classify ambiguous items (type='other') using LLM
  const ambiguousBefore = lifecycleItems.filter(i => i.type === 'other').length;
  if (ambiguousBefore > 0) {
    lifecycleItems = await classifyAmbiguousItems(lifecycleItems, allTopics);
  }
  // ========== END LLM CLASSIFICATION ==========
  
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
      // Apply manual overrides before returning
      const withOverrides = applyOverrides(cached);
      return res.json(withOverrides);
    }
  }
  
  // No cache or force refresh - fetch fresh data
  if (!API_KEY) {
    // Check if we have stale cache to serve (even on force refresh when no API key)
    const staleCache = readCache();
    if (staleCache) {
      console.log('No API key configured, serving existing cache');
      const withOverrides = applyOverrides(staleCache);
      return res.json({ ...withOverrides, stale: true, warning: 'GROUPS_IO_API_KEY not configured - showing cached data' });
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
    // Apply manual overrides before returning
    const withOverrides = applyOverrides(data);
    return res.json(withOverrides);

  } catch (error) {
    console.error('Error fetching governance lifecycle:', error);
    
    // On error, try to serve stale cache
    const cached = readCache();
    if (cached) {
      console.log('Serving stale cache due to fetch error');
      const withOverrides = applyOverrides(cached);
      return res.json({ ...withOverrides, stale: true, error: error.message });
    }
    
    res.status(500).json({ 
      error: error.message,
      lifecycleItems: [],
      groups: {},
    });
  }
});

// Refresh endpoint - explicitly fetches fresh data
async function handleRefresh(req, res) {
  if (!API_KEY) {
    return res.status(500).json({ error: 'GROUPS_IO_API_KEY not configured' });
  }

  try {
    const data = await fetchFreshData();
    writeCache(data);
    return res.json({ success: true, stats: data.stats, cachedAt: data.cachedAt });
  } catch (error) {
    console.error('Error refreshing governance lifecycle:', error);
    return res.status(500).json({ error: error.message });
  }
}

// POST is the intended method (safe for browsers/tools)
router.post('/refresh', handleRefresh);

// Convenience: allow GET so visiting in a browser doesn't show "Cannot GET"
router.get('/refresh', handleRefresh);

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

// ========== MANUAL OVERRIDES ENDPOINTS ==========

// Get all current overrides
router.get('/overrides', (req, res) => {
  const overrides = readOverrides();
  res.json(overrides);
});

// Set/update an override for a lifecycle item
router.post('/overrides', (req, res) => {
  const { itemId, primaryId, type, reason } = req.body;
  
  if (!type || (!itemId && !primaryId)) {
    return res.status(400).json({ error: 'Missing required fields: type and either itemId or primaryId' });
  }
  
  const validTypes = ['cip', 'featured-app', 'validator', 'protocol-upgrade', 'outcome', 'other'];
  if (!validTypes.includes(type)) {
    return res.status(400).json({ error: `Invalid type. Must be one of: ${validTypes.join(', ')}` });
  }
  
  const key = primaryId || itemId;
  
  // Find original classification for audit log
  const original = findOriginalClassification(key, 'item');
  
  const overrides = readOverrides();
  overrides.itemOverrides[key] = {
    type,
    originalType: original?.type || null,
    reason: reason || 'Manual correction',
    createdAt: new Date().toISOString(),
  };
  
  if (writeOverrides(overrides)) {
    // Log to audit trail
    logOverrideAction({
      actionType: 'reclassify_item',
      targetId: key,
      targetLabel: original?.primaryId || key,
      originalValue: original?.type || 'unknown',
      newValue: type,
      reason: reason || 'Manual correction',
    });
    
    console.log(`✅ Override set: "${key}" -> ${type} (${reason || 'Manual correction'})`);
    res.json({ success: true, override: overrides.itemOverrides[key] });
  } else {
    res.status(500).json({ error: 'Failed to save override' });
  }
});

// Delete an override
router.delete('/overrides/:key', (req, res) => {
  const { key } = req.params;
  const overrides = readOverrides();
  
  if (overrides.itemOverrides[key]) {
    delete overrides.itemOverrides[key];
    if (writeOverrides(overrides)) {
      res.json({ success: true, message: `Override removed for "${key}"` });
    } else {
      res.status(500).json({ error: 'Failed to save changes' });
    }
  } else {
    res.status(404).json({ error: `No override found for "${key}"` });
  }
});

// ========== AUDIT LOG ENDPOINTS ==========

// Get the full audit log
router.get('/audit-log', (req, res) => {
  const entries = readAuditLog();
  const limit = parseInt(req.query.limit) || 100;
  const actionType = req.query.actionType;
  
  let filtered = entries;
  if (actionType) {
    filtered = entries.filter(e => e.actionType === actionType);
  }
  
  // Return most recent first
  const sorted = filtered.sort((a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime());
  
  res.json({
    total: entries.length,
    filtered: sorted.length,
    entries: sorted.slice(0, limit),
    actionTypes: [...new Set(entries.map(e => e.actionType))],
  });
});

// Get audit log stats
router.get('/audit-log/stats', (req, res) => {
  const entries = readAuditLog();
  
  const byActionType = {};
  const byMonth = {};
  
  entries.forEach(e => {
    // Count by action type
    byActionType[e.actionType] = (byActionType[e.actionType] || 0) + 1;
    
    // Count by month
    const month = e.timestamp?.slice(0, 7) || 'unknown';
    byMonth[month] = (byMonth[month] || 0) + 1;
  });
  
  res.json({
    total: entries.length,
    byActionType,
    byMonth,
    oldestEntry: entries[0]?.timestamp || null,
    newestEntry: entries[entries.length - 1]?.timestamp || null,
  });
});

// Backfill audit log from existing overrides
// This creates audit entries for overrides that were created before audit logging was added
router.post('/audit-log/backfill', (req, res) => {
  const overrides = readOverrides();
  const existingAuditLog = readAuditLog();
  const existingIds = new Set(existingAuditLog.map(e => e.targetId));
  
  const backfilledEntries = [];
  
  // Process item overrides
  Object.entries(overrides.itemOverrides || {}).forEach(([key, override]) => {
    if (existingIds.has(key)) return; // Already logged
    
    const original = findOriginalClassification(key, 'item');
    backfilledEntries.push({
      id: `backfill-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
      timestamp: override.createdAt || new Date().toISOString(),
      backfilled: true,
      actionType: 'reclassify_item',
      targetId: key,
      targetLabel: original?.primaryId || key,
      originalValue: override.originalType || original?.type || 'unknown',
      newValue: override.type,
      reason: override.reason || 'Manual correction',
    });
  });
  
  // Process topic overrides
  Object.entries(overrides.topicOverrides || {}).forEach(([key, override]) => {
    if (existingIds.has(key)) return;
    
    const original = findOriginalClassification(key, 'topic');
    backfilledEntries.push({
      id: `backfill-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
      timestamp: override.createdAt || new Date().toISOString(),
      backfilled: true,
      actionType: 'reclassify_topic',
      targetId: key,
      targetLabel: original?.subject || key,
      originalValue: override.originalParentType || original?.parentType || 'unknown',
      originalParentId: override.originalParentId || original?.parentId || null,
      newValue: override.type,
      reason: override.reason || 'Manual topic reclassification',
    });
  });
  
  // Process extract overrides
  Object.entries(overrides.extractOverrides || {}).forEach(([key, override]) => {
    if (existingIds.has(key)) return;
    
    const original = findOriginalClassification(key, 'topic');
    backfilledEntries.push({
      id: `backfill-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
      timestamp: override.createdAt || new Date().toISOString(),
      backfilled: true,
      actionType: 'extract_topic',
      targetId: key,
      targetLabel: original?.subject || key,
      originalValue: override.originalParentId || original?.parentId || 'unknown',
      newValue: override.customName || 'new card',
      reason: override.reason || 'Extracted to own card',
    });
  });
  
  // Process merge overrides
  Object.entries(overrides.mergeOverrides || {}).forEach(([key, override]) => {
    if (existingIds.has(key)) return;
    
    const originalItem = findOriginalClassification(key, 'item');
    const originalTopic = findOriginalClassification(key, 'topic');
    const original = originalItem || originalTopic;
    
    backfilledEntries.push({
      id: `backfill-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
      timestamp: override.createdAt || new Date().toISOString(),
      backfilled: true,
      actionType: 'merge',
      targetId: key,
      targetLabel: original?.subject || original?.primaryId || key,
      originalValue: override.originalParentId || original?.parentId || original?.primaryId || 'unknown',
      newValue: Array.isArray(override.mergeInto) ? override.mergeInto.join(', ') : override.mergeInto,
      reason: override.reason || 'Manual merge',
    });
  });
  
  // Process move overrides
  Object.entries(overrides.moveOverrides || {}).forEach(([key, override]) => {
    if (existingIds.has(key)) return;
    
    const original = findOriginalClassification(key, 'topic');
    backfilledEntries.push({
      id: `backfill-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
      timestamp: override.createdAt || new Date().toISOString(),
      backfilled: true,
      actionType: 'move_topic',
      targetId: key,
      targetLabel: original?.subject || key,
      originalValue: override.originalParentId || original?.parentId || 'unknown',
      newValue: override.targetCardId,
      reason: override.reason || 'Manual topic move',
    });
  });
  
  if (backfilledEntries.length > 0) {
    // Prepend backfilled entries (they're historical)
    const updatedLog = [...backfilledEntries, ...existingAuditLog];
    // Sort by timestamp
    updatedLog.sort((a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime());
    writeAuditLog(updatedLog);
    
    console.log(`📝 Backfilled ${backfilledEntries.length} audit entries`);
  }
  
  res.json({
    success: true,
    backfilledCount: backfilledEntries.length,
    backfilledEntries,
    totalAuditEntries: existingAuditLog.length + backfilledEntries.length,
  });
});

// Check what overrides exist that haven't been audited yet
router.get('/audit-log/pending-backfill', (req, res) => {
  const overrides = readOverrides();
  const existingAuditLog = readAuditLog();
  const existingIds = new Set(existingAuditLog.map(e => e.targetId));
  
  const pending = {
    itemOverrides: Object.keys(overrides.itemOverrides || {}).filter(k => !existingIds.has(k)),
    topicOverrides: Object.keys(overrides.topicOverrides || {}).filter(k => !existingIds.has(k)),
    extractOverrides: Object.keys(overrides.extractOverrides || {}).filter(k => !existingIds.has(k)),
    mergeOverrides: Object.keys(overrides.mergeOverrides || {}).filter(k => !existingIds.has(k)),
    moveOverrides: Object.keys(overrides.moveOverrides || {}).filter(k => !existingIds.has(k)),
  };
  
  const totalPending = pending.itemOverrides.length + pending.topicOverrides.length + 
    pending.extractOverrides.length + pending.mergeOverrides.length + pending.moveOverrides.length;
  
  res.json({
    totalPending,
    pending,
    existingAuditEntries: existingAuditLog.length,
  });
});

router.post('/overrides/topic', (req, res) => {
  const { topicId, newType, reason } = req.body;
  
  if (!topicId || !newType) {
    return res.status(400).json({ error: 'Missing required fields: topicId and newType' });
  }
  
  const validTypes = ['cip', 'featured-app', 'validator', 'protocol-upgrade', 'outcome', 'other'];
  if (!validTypes.includes(newType)) {
    return res.status(400).json({ error: `Invalid type. Must be one of: ${validTypes.join(', ')}` });
  }
  
  const key = String(topicId);
  
  // Find original classification for audit log
  const original = findOriginalClassification(key, 'topic');
  
  const overrides = readOverrides();
  if (!overrides.topicOverrides) {
    overrides.topicOverrides = {};
  }
  
  overrides.topicOverrides[key] = {
    type: newType,
    originalParentType: original?.parentType || null,
    originalParentId: original?.parentId || null,
    reason: reason || 'Manual topic reclassification',
    createdAt: new Date().toISOString(),
  };
  
  if (writeOverrides(overrides)) {
    // Log to audit trail
    logOverrideAction({
      actionType: 'reclassify_topic',
      targetId: key,
      targetLabel: original?.subject || key,
      originalValue: original?.parentType || 'unknown',
      originalParentId: original?.parentId || null,
      newValue: newType,
      reason: reason || 'Manual topic reclassification',
    });
    
    console.log(`✅ Topic override set: "${key}" -> ${newType} (${reason || 'Manual topic reclassification'})`);
    res.json({ success: true, override: overrides.topicOverrides[key] });
  } else {
    res.status(500).json({ error: 'Failed to save topic override' });
  }
});

// Extract a topic to its own card (keeps same type, just separates it)
router.post('/overrides/extract', (req, res) => {
  const { topicId, customName, reason } = req.body;
  
  if (!topicId) {
    return res.status(400).json({ error: 'Missing required field: topicId' });
  }
  
  const key = String(topicId);
  
  // Find original classification for audit log
  const original = findOriginalClassification(key, 'topic');
  
  const overrides = readOverrides();
  if (!overrides.extractOverrides) {
    overrides.extractOverrides = {};
  }
  
  overrides.extractOverrides[key] = {
    customName: customName || null,
    originalParentId: original?.parentId || null,
    originalParentType: original?.parentType || null,
    reason: reason || 'Extracted to own card',
    createdAt: new Date().toISOString(),
  };
  
  if (writeOverrides(overrides)) {
    // Log to audit trail
    logOverrideAction({
      actionType: 'extract_topic',
      targetId: key,
      targetLabel: original?.subject || key,
      originalValue: original?.parentId || 'unknown',
      newValue: customName || 'new card',
      reason: reason || 'Extracted to own card',
    });
    
    console.log(`✅ Extract override set: "${key}"${customName ? ` -> "${customName}"` : ''}`);
    res.json({ success: true, override: overrides.extractOverrides[key] });
  } else {
    res.status(500).json({ error: 'Failed to save extract override' });
  }
});

// Set a merge override (merge one item into one or multiple CIPs)
// Supports both single CIP (string) and multiple CIPs (array)
router.post('/overrides/merge', (req, res) => {
  const { sourceId, sourcePrimaryId, mergeInto, reason } = req.body;
  
  if (!mergeInto || (!sourceId && !sourcePrimaryId)) {
    return res.status(400).json({ error: 'Missing required fields: mergeInto and either sourceId or sourcePrimaryId' });
  }
  
  // Normalize the target CIP number(s) - support both string and array
  const normalizeCip = (cip) => {
    return cip.toUpperCase().replace(/^CIP\s*[-#]?\s*0*/, 'CIP-').replace(/CIP-(\d+)/, (_, num) => `CIP-${num.padStart(4, '0')}`);
  };
  
  let normalizedTargets;
  if (Array.isArray(mergeInto)) {
    normalizedTargets = mergeInto.map(normalizeCip);
  } else {
    normalizedTargets = [normalizeCip(mergeInto)];
  }
  
  // Always stringify the key for consistent lookup
  const key = String(sourcePrimaryId || sourceId);
  
  // Find original classification for audit log
  // Could be an item (card) or a topic
  const originalItem = findOriginalClassification(key, 'item');
  const originalTopic = findOriginalClassification(key, 'topic');
  const original = originalItem || originalTopic;
  
  const overrides = readOverrides();
  if (!overrides.mergeOverrides) {
    overrides.mergeOverrides = {};
  }
  
  overrides.mergeOverrides[key] = {
    mergeInto: normalizedTargets, // Now always an array
    originalParentId: original?.parentId || original?.primaryId || null,
    originalType: original?.parentType || original?.type || null,
    reason: reason || 'Manual merge',
    createdAt: new Date().toISOString(),
  };
  
  console.log(`Saving merge override: key="${key}", targets=${normalizedTargets.join(', ')}`);
  
  if (writeOverrides(overrides)) {
    // Log to audit trail
    logOverrideAction({
      actionType: 'merge',
      targetId: key,
      targetLabel: original?.subject || original?.primaryId || key,
      originalValue: original?.parentId || original?.primaryId || 'unknown',
      newValue: normalizedTargets.join(', '),
      reason: reason || 'Manual merge',
    });
    
    console.log(`✅ Merge override set: "${key}" -> ${normalizedTargets.join(', ')}`);
    res.json({ success: true, override: overrides.mergeOverrides[key] });
  } else {
    res.status(500).json({ error: 'Failed to save merge override' });
  }
});

// Debug endpoint: show merge override status and why things aren't matching
router.get('/overrides/merge-debug', (req, res) => {
  const overrides = readOverrides();
  const cached = readCache();
  
  if (!cached || !cached.lifecycleItems) {
    return res.json({ error: 'No cached data', mergeOverrides: overrides.mergeOverrides || {} });
  }
  
  const mergeKeys = Object.keys(overrides.mergeOverrides || {});
  const allTopicIds = [];
  const allItemIds = [];
  
  cached.lifecycleItems.forEach(item => {
    allItemIds.push({ id: item.id, primaryId: item.primaryId, type: item.type });
    (item.topics || []).forEach(topic => {
      allTopicIds.push({
        id: String(topic.id),
        subject: topic.subject?.slice(0, 80),
        stage: topic.stage,
        parentPrimaryId: item.primaryId,
        parentType: item.type,
      });
    });
  });
  
  // Check which merge keys match
  const matchResults = mergeKeys.map(key => {
    const override = overrides.mergeOverrides[key];
    const matchedTopic = allTopicIds.find(t => t.id === key || t.subject === key);
    const matchedItem = allItemIds.find(i => i.id === key || i.primaryId === key);
    return {
      key,
      override,
      matchedTopic: matchedTopic || null,
      matchedItem: matchedItem || null,
      hasMatch: !!(matchedTopic || matchedItem),
    };
  });
  
  res.json({
    mergeOverrideCount: mergeKeys.length,
    mergeOverrideKeys: mergeKeys,
    matchResults,
    sampleTopicIds: allTopicIds.slice(0, 20),
  });
});

router.get('/cip-list', (req, res) => {
  const cached = readCache();
  if (!cached || !cached.lifecycleItems) {
    return res.json({ cips: [] });
  }
  
  const cips = cached.lifecycleItems
    .filter(item => item.type === 'cip' && item.primaryId.match(/^CIP-\d+$/i))
    .map(item => ({
      primaryId: item.primaryId,
      firstDate: item.firstDate,
      lastDate: item.lastDate,
      topicCount: item.topics.length,
    }))
    .sort((a, b) => {
      const numA = parseInt(a.primaryId.match(/\d+/)?.[0] || '0');
      const numB = parseInt(b.primaryId.match(/\d+/)?.[0] || '0');
      return numB - numA;
    });
  
  res.json({ cips });
});

// Get all lifecycle cards for the "Move to card" dropdown
router.get('/card-list', (req, res) => {
  const cached = readCache();
  if (!cached || !cached.lifecycleItems) {
    return res.json({ cards: [] });
  }
  
  const cards = cached.lifecycleItems
    .map(item => ({
      id: item.id,
      primaryId: item.primaryId,
      type: item.type,
      firstDate: item.firstDate,
      lastDate: item.lastDate,
      topicCount: item.topics.length,
      // Include first topic subject as preview
      preview: item.topics[0]?.subject?.slice(0, 60) || '',
    }))
    .sort((a, b) => {
      // Sort by type first, then by date
      if (a.type !== b.type) return a.type.localeCompare(b.type);
      return new Date(b.lastDate).getTime() - new Date(a.lastDate).getTime();
    });
  
  res.json({ cards });
});

// Move a topic from one card to another
router.post('/overrides/move-topic', (req, res) => {
  const { topicId, targetCardId, reason } = req.body;
  
  if (!topicId || !targetCardId) {
    return res.status(400).json({ error: 'Missing required fields: topicId and targetCardId' });
  }
  
  const key = String(topicId);
  
  // Find original classification for audit log
  const original = findOriginalClassification(key, 'topic');
  
  // Find target card info
  const cached = readCache();
  let targetInfo = null;
  if (cached?.lifecycleItems) {
    const targetCard = cached.lifecycleItems.find(i => i.id === targetCardId || i.primaryId === targetCardId);
    if (targetCard) {
      targetInfo = { id: targetCard.id, primaryId: targetCard.primaryId, type: targetCard.type };
    }
  }
  
  const overrides = readOverrides();
  if (!overrides.moveOverrides) {
    overrides.moveOverrides = {};
  }
  
  overrides.moveOverrides[key] = {
    targetCardId: String(targetCardId),
    originalParentId: original?.parentId || null,
    originalParentType: original?.parentType || null,
    targetType: targetInfo?.type || null,
    reason: reason || 'Manual topic move',
    createdAt: new Date().toISOString(),
  };
  
  console.log(`Saving move override: topic "${key}" -> card "${targetCardId}"`);
  
  if (writeOverrides(overrides)) {
    // Log to audit trail
    logOverrideAction({
      actionType: 'move_topic',
      targetId: key,
      targetLabel: original?.subject || key,
      originalValue: original?.parentId || 'unknown',
      newValue: targetInfo?.primaryId || targetCardId,
      reason: reason || 'Manual topic move',
    });
    
    console.log(`✅ Move override set: topic "${key}" -> card "${targetCardId}"`);
    res.json({ success: true, override: overrides.moveOverrides[key] });
  } else {
    res.status(500).json({ error: 'Failed to save move override' });
  }
});

// Also allow GET for convenience (browser-friendly)
router.get('/audit-log/backfill', (req, res) => {
  const overrides = readOverrides();
  const existingAuditLog = readAuditLog();
  const existingIds = new Set(existingAuditLog.map(e => e.targetId));
  
  const backfilledEntries = [];
  
  // Process item overrides
  Object.entries(overrides.itemOverrides || {}).forEach(([key, override]) => {
    if (existingIds.has(key)) return;
    const original = findOriginalClassification(key, 'item');
    backfilledEntries.push({
      id: `backfill-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
      timestamp: override.createdAt || new Date().toISOString(),
      backfilled: true,
      actionType: 'reclassify_item',
      targetId: key,
      targetLabel: original?.primaryId || key,
      originalValue: override.originalType || original?.type || 'unknown',
      newValue: override.type,
      reason: override.reason || 'Manual correction',
    });
  });
  
  // Process topic overrides
  Object.entries(overrides.topicOverrides || {}).forEach(([key, override]) => {
    if (existingIds.has(key)) return;
    const original = findOriginalClassification(key, 'topic');
    backfilledEntries.push({
      id: `backfill-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
      timestamp: override.createdAt || new Date().toISOString(),
      backfilled: true,
      actionType: 'reclassify_topic',
      targetId: key,
      targetLabel: original?.subject || key,
      originalValue: override.originalParentType || original?.parentType || 'unknown',
      originalParentId: override.originalParentId || original?.parentId || null,
      newValue: override.type,
      reason: override.reason || 'Manual topic reclassification',
    });
  });
  
  // Process extract overrides
  Object.entries(overrides.extractOverrides || {}).forEach(([key, override]) => {
    if (existingIds.has(key)) return;
    const original = findOriginalClassification(key, 'topic');
    backfilledEntries.push({
      id: `backfill-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
      timestamp: override.createdAt || new Date().toISOString(),
      backfilled: true,
      actionType: 'extract_topic',
      targetId: key,
      targetLabel: original?.subject || key,
      originalValue: override.originalParentId || original?.parentId || 'unknown',
      newValue: override.customName || 'new card',
      reason: override.reason || 'Extracted to own card',
    });
  });
  
  // Process merge overrides
  Object.entries(overrides.mergeOverrides || {}).forEach(([key, override]) => {
    if (existingIds.has(key)) return;
    const originalItem = findOriginalClassification(key, 'item');
    const originalTopic = findOriginalClassification(key, 'topic');
    const original = originalItem || originalTopic;
    backfilledEntries.push({
      id: `backfill-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
      timestamp: override.createdAt || new Date().toISOString(),
      backfilled: true,
      actionType: 'merge',
      targetId: key,
      targetLabel: original?.subject || original?.primaryId || key,
      originalValue: override.originalParentId || original?.parentId || original?.primaryId || 'unknown',
      newValue: Array.isArray(override.mergeInto) ? override.mergeInto.join(', ') : override.mergeInto,
      reason: override.reason || 'Manual merge',
    });
  });
  
  // Process move overrides
  Object.entries(overrides.moveOverrides || {}).forEach(([key, override]) => {
    if (existingIds.has(key)) return;
    const original = findOriginalClassification(key, 'topic');
    backfilledEntries.push({
      id: `backfill-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
      timestamp: override.createdAt || new Date().toISOString(),
      backfilled: true,
      actionType: 'move_topic',
      targetId: key,
      targetLabel: original?.subject || key,
      originalValue: override.originalParentId || original?.parentId || 'unknown',
      newValue: override.targetCardId,
      reason: override.reason || 'Manual topic move',
    });
  });
  
  if (backfilledEntries.length > 0) {
    const updatedLog = [...backfilledEntries, ...existingAuditLog];
    updatedLog.sort((a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime());
    writeAuditLog(updatedLog);
    console.log(`📝 Backfilled ${backfilledEntries.length} audit entries`);
  }
  
  res.json({
    success: true,
    backfilledCount: backfilledEntries.length,
    backfilledEntries,
    totalAuditEntries: existingAuditLog.length + backfilledEntries.length,
  });
});


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

// Analyze overrides to help improve regex patterns
// Returns all overrides with their original detected types from cached data
router.get('/overrides/analysis', (req, res) => {
  const overrides = readOverrides();
  const cached = readCache();
  
  if (!cached || !cached.lifecycleItems) {
    return res.json({
      overrides: overrides.itemOverrides || {},
      mergeOverrides: overrides.mergeOverrides || {},
      analysis: [],
      suggestions: [],
    });
  }
  
  // Build a map of primaryId -> original data from topics before overrides were applied
  // We need to look at topics to understand what the regex originally detected
  const analysis = [];
  
  // For each override, find the original item and its topics
  for (const [key, override] of Object.entries(overrides.itemOverrides || {})) {
    // Find matching lifecycle item (check both current and with override removed)
    const item = cached.lifecycleItems.find(li => 
      li.primaryId === key || li.id === key
    );
    
    // Also search through all topics to find what was detected
    const matchingTopics = cached.allTopics?.filter(topic => {
      const subjectLower = topic.subject.toLowerCase();
      const keyLower = key.toLowerCase();
      return (
        subjectLower.includes(keyLower) ||
        topic.identifiers?.appName?.toLowerCase() === keyLower ||
        topic.identifiers?.validatorName?.toLowerCase() === keyLower ||
        topic.identifiers?.entityName?.toLowerCase() === keyLower
      );
    }) || [];
    
    analysis.push({
      key,
      overrideType: override.type,
      overrideReason: override.reason,
      createdAt: override.createdAt,
      originalType: item?.type,
      wasLLMClassified: item?.llmClassified || false,
      topicCount: item?.topics?.length || matchingTopics.length,
      sampleSubjects: (item?.topics || matchingTopics).slice(0, 5).map(t => t.subject),
      // Indicators from topics that might help improve regex
      hasValidatorKeywords: (item?.topics || matchingTopics).some(t => 
        /validator|node\s+as\s+a\s+service|sv\s+onboarding/i.test(t.subject)
      ),
      hasFeaturedAppKeywords: (item?.topics || matchingTopics).some(t =>
        /featured\s+app|to\s+feature|app\s+rights/i.test(t.subject)
      ),
      hasApprovedKeyword: (item?.topics || matchingTopics).some(t =>
        /\bapproved\b/i.test(t.subject)
      ),
    });
  }
  
  // Generate suggestions based on patterns in overrides
  const suggestions = [];
  
  // Count type corrections
  const corrections = {};
  for (const item of analysis) {
    if (item.originalType && item.originalType !== item.overrideType) {
      const correctionKey = `${item.originalType} -> ${item.overrideType}`;
      corrections[correctionKey] = corrections[correctionKey] || [];
      corrections[correctionKey].push(item);
    }
  }
  
  // Suggest regex improvements based on common corrections
  for (const [correction, items] of Object.entries(corrections)) {
    if (items.length >= 2) {
      const subjects = items.flatMap(i => i.sampleSubjects);
      suggestions.push({
        correction,
        count: items.length,
        suggestion: `Consider adding regex pattern for: "${correction}" (${items.length} manual corrections)`,
        affectedItems: items.map(i => i.key),
        sampleSubjects: subjects.slice(0, 10),
      });
    }
  }
  
  // Log summary
  console.log('\n========== OVERRIDES ANALYSIS ==========');
  console.log(`Total item overrides: ${Object.keys(overrides.itemOverrides || {}).length}`);
  console.log(`Total merge overrides: ${Object.keys(overrides.mergeOverrides || {}).length}`);
  console.log('\nCorrections by type:');
  for (const [correction, items] of Object.entries(corrections)) {
    console.log(`  ${correction}: ${items.length} items`);
    items.slice(0, 3).forEach(item => {
      console.log(`    - ${item.key}`);
    });
  }
  console.log('=========================================\n');
  
  res.json({
    overrides: overrides.itemOverrides || {},
    mergeOverrides: overrides.mergeOverrides || {},
    analysis,
    suggestions,
    summary: {
      totalItemOverrides: Object.keys(overrides.itemOverrides || {}).length,
      totalMergeOverrides: Object.keys(overrides.mergeOverrides || {}).length,
      correctionsByType: Object.fromEntries(
        Object.entries(corrections).map(([k, v]) => [k, v.length])
      ),
    },
  });
});

// Debug endpoint: LLM classification status (enhanced with hybrid audit)
router.get('/llm-status', async (req, res) => {
  const stats = getClassificationStats();
  const auditStatus = getAuditStatus();
  const cached = readCache();
  
  let verifiedCount = 0;
  let unverifiedCount = 0;
  let agreementsCount = 0;
  let disagreementsCount = 0;
  let otherTypeCount = 0;
  let totalItems = 0;
  let totalTopics = 0;
  const verifiedSamples = [];
  const disagreementSamples = [];
  
  if (cached?.lifecycleItems) {
    totalItems = cached.lifecycleItems.length;
    totalTopics = cached.allTopics?.length || 0;
    
    for (const item of cached.lifecycleItems) {
      if (item.verifiedSemantics) {
        verifiedCount++;
        if (item.agreement) {
          agreementsCount++;
        } else {
          disagreementsCount++;
          if (disagreementSamples.length < 5) {
            disagreementSamples.push({
              primaryId: item.primaryId,
              ruleType: item.ruleType,
              llmType: item.llmType,
              confidence: item.llmConfidence,
              reasoning: item.llmReasoning,
              needsReview: item.needsReview,
            });
          }
        }
        if (verifiedSamples.length < 5) {
          verifiedSamples.push({
            primaryId: item.primaryId,
            type: item.type,
            typeSource: item.typeSource,
            ruleType: item.ruleType,
            llmType: item.llmType,
            agreement: item.agreement,
          });
        }
      } else {
        unverifiedCount++;
      }
      if (item.type === 'other') {
        otherTypeCount++;
      }
    }
  }
  
  res.json({
    designPrinciple: "assumed semantics vs verified semantics",
    hybridAuditModel: {
      description: "Rules classify everything; LLM reads content and confirms OR flags disagreement",
      ruleType: "Assumed semantics (deterministic, fast)",
      llmType: "Verified semantics (reads content, cached permanently)",
    },
    auditorAvailable: auditStatus.available,
    llmAvailable: stats.llmAvailable,
    apiKeySet: !!process.env.OPENAI_API_KEY,
    model: stats.model,
    auditVersion: AUDIT_VERSION,
    lifecycle: {
      totalTopics,
      totalItems,
      verifiedSemantics: verifiedCount,
      assumedSemantics: unverifiedCount,
      agreements: agreementsCount,
      disagreements: disagreementsCount,
      agreementRate: verifiedCount > 0 ? ((agreementsCount / verifiedCount) * 100).toFixed(1) + '%' : 'N/A',
      otherTypeCount,
    },
    cache: {
      audit: auditStatus.stats,
      llm: stats.llm,
      content: stats.content,
    },
    samples: {
      verified: verifiedSamples,
      disagreements: disagreementSamples,
    },
    needsReview: {
      count: auditStatus.needsReviewCount,
      message: auditStatus.needsReviewCount > 0 
        ? `${auditStatus.needsReviewCount} items have high-confidence disagreements`
        : 'No items need review',
    },
    endpoints: {
      refresh: 'POST /api/governance-lifecycle/refresh - Fetch data and run hybrid audit',
      auditStatus: 'GET /api/governance-lifecycle/audit-status - Detailed audit statistics',
      disagreements: 'GET /api/governance-lifecycle/audit-disagreements - List all disagreements',
      reviewItems: 'GET /api/governance-lifecycle/audit-review - Items needing human review',
      clearAudit: 'POST /api/governance-lifecycle/clear-audit-cache - Clear audit cache (triggers full re-audit)',
    },
  });
});

// Force reclassify a specific topic
router.post('/reclassify/:topicId', async (req, res) => {
  const { topicId } = req.params;
  
  if (!isLLMAvailable()) {
    return res.status(500).json({ error: 'OPENAI_API_KEY not configured' });
  }
  
  // Get content
  const content = getCachedContent(topicId);
  if (!content) {
    return res.status(404).json({ error: 'Topic content not found in cache' });
  }
  
  // Force reclassify
  const result = await classifyTopic(topicId, content.subject, content.body, { forceReclassify: true });
  
  res.json({
    topicId,
    subject: content.subject,
    classification: result,
    note: 'Run POST /refresh to apply to lifecycle items',
  });
});

// Clear LLM cache (for full reindex)
router.post('/clear-llm-cache', (req, res) => {
  clearLLMCache();
  res.json({ success: true, message: 'LLM classification cache cleared. Run POST /refresh to reclassify.' });
});

// ========== HYBRID AUDIT ENDPOINTS ==========

// Get hybrid audit status and statistics
router.get('/audit-status', (req, res) => {
  const auditStatus = getAuditStatus();
  const cached = readCache();
  
  // Count verified vs unverified items
  let verifiedCount = 0;
  let unverifiedCount = 0;
  let agreementsInData = 0;
  let disagreementsInData = 0;
  
  if (cached?.lifecycleItems) {
    for (const item of cached.lifecycleItems) {
      if (item.verifiedSemantics) {
        verifiedCount++;
        if (item.agreement) agreementsInData++;
        else disagreementsInData++;
      } else {
        unverifiedCount++;
      }
    }
  }
  
  res.json({
    designPrinciple: "assumed semantics vs verified semantics",
    explanation: {
      ruleBasedType: "Assumed semantics - deterministic, fast, may be wrong",
      llmVerifiedType: "Verified semantics - read actual content, cached permanently",
      agreement: "Rule and LLM agree on classification",
      needsReview: "High-confidence disagreement requiring human review",
    },
    auditorAvailable: auditStatus.available,
    auditVersion: AUDIT_VERSION,
    lifecycle: {
      totalItems: cached?.lifecycleItems?.length || 0,
      verifiedSemantics: verifiedCount,
      assumedSemantics: unverifiedCount,
      agreements: agreementsInData,
      disagreements: disagreementsInData,
    },
    auditCache: auditStatus.stats,
    needsReview: {
      count: auditStatus.needsReviewCount,
      samples: auditStatus.needsReviewItems,
    },
    endpoints: {
      status: 'GET /api/governance-lifecycle/audit-status - This endpoint',
      disagreements: 'GET /api/governance-lifecycle/audit-disagreements - List all disagreements',
      reviewItems: 'GET /api/governance-lifecycle/audit-review - Items needing human review',
      clearAudit: 'POST /api/governance-lifecycle/clear-audit-cache - Clear all audit data',
      reverify: 'POST /api/governance-lifecycle/reverify/:itemId - Re-verify a specific item',
    },
  });
});

// Get all disagreements (rule != LLM)
router.get('/audit-disagreements', (req, res) => {
  const disagreements = getAuditDisagreements();
  const limit = parseInt(req.query.limit) || 100;
  
  // Group by type mismatch
  const byMismatch = {};
  for (const item of disagreements) {
    const key = `${item.rule_type} → ${item.llm_type}`;
    if (!byMismatch[key]) {
      byMismatch[key] = { count: 0, items: [] };
    }
    byMismatch[key].count++;
    if (byMismatch[key].items.length < 5) {
      byMismatch[key].items.push({
        itemId: item.item_id,
        primaryId: item.primary_id,
        confidence: item.llm_confidence,
        reasoning: item.llm_reasoning,
        needsReview: item.needs_review,
      });
    }
  }
  
  res.json({
    total: disagreements.length,
    byMismatch,
    items: disagreements.slice(0, limit).map(d => ({
      itemId: d.item_id,
      primaryId: d.primary_id,
      ruleType: d.rule_type,
      llmType: d.llm_type,
      confidence: d.llm_confidence,
      reasoning: d.llm_reasoning,
      needsReview: d.needs_review,
      classifiedAt: d.classified_at,
    })),
  });
});

// Get items needing human review (high-confidence disagreements)
router.get('/audit-review', (req, res) => {
  const reviewItems = getAuditReviewItems();
  
  res.json({
    total: reviewItems.length,
    message: reviewItems.length > 0 
      ? `${reviewItems.length} items have high-confidence disagreements and need human review`
      : 'No items need review - all classifications are in agreement or low-confidence',
    items: reviewItems.map(r => ({
      itemId: r.item_id,
      primaryId: r.primary_id,
      ruleType: r.rule_type,
      llmType: r.llm_type,
      confidence: r.llm_confidence,
      reasoning: r.llm_reasoning,
      classifiedAt: r.classified_at,
    })),
  });
});

// Clear audit cache (for full re-audit)
router.post('/clear-audit-cache', (req, res) => {
  clearAuditCache();
  res.json({ 
    success: true, 
    message: 'Audit cache cleared. Run POST /refresh to re-verify all items.',
    note: 'This will trigger a one-time LLM pass over ALL lifecycle items',
  });
});

// Re-verify a specific item
router.post('/reverify/:itemId', async (req, res) => {
  const { itemId } = req.params;
  
  if (!isAuditorAvailable()) {
    return res.status(500).json({ error: 'OPENAI_API_KEY not configured' });
  }
  
  const cached = readCache();
  const item = cached?.lifecycleItems?.find(i => 
    i.id === itemId || i.primaryId === itemId
  );
  
  if (!item) {
    return res.status(404).json({ error: 'Item not found in cache' });
  }
  
  const result = await verifyItem(item, { forceReverify: true });
  
  res.json({
    itemId,
    primaryId: item.primaryId,
    ruleType: item.type,
    verification: result,
    note: 'Run POST /refresh to apply updated verification to cache',
  });
});

// ========== CLASSIFICATION IMPROVEMENT ANALYSIS ==========
// Analyzes audit log to generate suggestions for improving classification rules

router.get('/classification-improvements', (req, res) => {
  const auditLog = readAuditLog();
  const overrides = readOverrides();
  const cached = readCache();
  
  if (auditLog.length === 0 && Object.keys(overrides.itemOverrides || {}).length === 0) {
    return res.json({
      message: 'No manual corrections found. Classification system appears accurate.',
      suggestions: [],
      stats: { totalCorrections: 0 },
    });
  }
  
  // Collect all corrections with context
  const corrections = [];
  
  // From audit log (includes reclassifications with subject/label)
  for (const entry of auditLog) {
    if (entry.actionType === 'reclassify_item' || entry.actionType === 'reclassify_topic') {
      corrections.push({
        source: 'audit',
        targetId: entry.targetId,
        label: entry.targetLabel || entry.targetId,
        originalType: entry.originalValue,
        correctedType: entry.newValue,
        reason: entry.reason,
        timestamp: entry.timestamp,
      });
    }
  }
  
  // From overrides (in case some weren't logged)
  for (const [key, override] of Object.entries(overrides.itemOverrides || {})) {
    if (!corrections.find(c => c.targetId === key)) {
      corrections.push({
        source: 'override',
        targetId: key,
        label: key,
        originalType: override.originalType,
        correctedType: override.type,
        reason: override.reason,
        timestamp: override.createdAt,
      });
    }
  }
  
  for (const [key, override] of Object.entries(overrides.topicOverrides || {})) {
    if (!corrections.find(c => c.targetId === key)) {
      // Find topic subject from cache
      let subject = key;
      if (cached?.lifecycleItems) {
        for (const item of cached.lifecycleItems) {
          const topic = (item.topics || []).find(t => String(t.id) === String(key));
          if (topic) {
            subject = topic.subject || key;
            break;
          }
        }
      }
      corrections.push({
        source: 'override',
        targetId: key,
        label: subject,
        originalType: override.originalParentType || 'unknown',
        correctedType: override.type,
        reason: override.reason,
        timestamp: override.createdAt,
      });
    }
  }
  
  // Analyze patterns
  const patternAnalysis = analyzeCorrections(corrections, cached);
  
  // Generate improvement suggestions
  const suggestions = generateImprovementSuggestions(patternAnalysis);
  
  res.json({
    stats: {
      totalCorrections: corrections.length,
      byOriginalType: patternAnalysis.byOriginalType,
      byCorrectedType: patternAnalysis.byCorrectedType,
      typeTransitions: patternAnalysis.typeTransitions,
    },
    patterns: patternAnalysis.patterns,
    suggestions,
    corrections: corrections.slice(0, 50), // Sample of corrections
  });
});

// Analyze correction patterns to identify systematic issues
function analyzeCorrections(corrections, cached) {
  const byOriginalType = {};
  const byCorrectedType = {};
  const typeTransitions = {};
  const patterns = [];
  
  // Collect word frequencies by transition type
  const wordsByTransition = {};
  
  for (const correction of corrections) {
    const orig = correction.originalType || 'unknown';
    const corr = correction.correctedType;
    const label = correction.label || '';
    
    byOriginalType[orig] = (byOriginalType[orig] || 0) + 1;
    byCorrectedType[corr] = (byCorrectedType[corr] || 0) + 1;
    
    const transition = `${orig} → ${corr}`;
    if (!typeTransitions[transition]) {
      typeTransitions[transition] = { count: 0, examples: [] };
    }
    typeTransitions[transition].count++;
    if (typeTransitions[transition].examples.length < 5) {
      typeTransitions[transition].examples.push(label.slice(0, 100));
    }
    
    // Extract keywords from label for pattern detection
    const words = label.toLowerCase()
      .replace(/[^a-z0-9\s]/g, ' ')
      .split(/\s+/)
      .filter(w => w.length > 2);
    
    if (!wordsByTransition[transition]) {
      wordsByTransition[transition] = {};
    }
    for (const word of words) {
      wordsByTransition[transition][word] = (wordsByTransition[transition][word] || 0) + 1;
    }
  }
  
  // Identify significant word patterns per transition
  for (const [transition, wordCounts] of Object.entries(wordsByTransition)) {
    const totalTransitions = typeTransitions[transition]?.count || 1;
    const significantWords = Object.entries(wordCounts)
      .filter(([word, count]) => {
        // Word appears in >50% of this transition type
        return count >= Math.max(2, totalTransitions * 0.5);
      })
      .map(([word, count]) => ({ word, frequency: count / totalTransitions }))
      .sort((a, b) => b.frequency - a.frequency)
      .slice(0, 10);
    
    if (significantWords.length > 0) {
      patterns.push({
        transition,
        transitionCount: totalTransitions,
        keywords: significantWords,
        examples: typeTransitions[transition]?.examples || [],
      });
    }
  }
  
  // Sort patterns by frequency
  patterns.sort((a, b) => b.transitionCount - a.transitionCount);
  
  return {
    byOriginalType,
    byCorrectedType,
    typeTransitions,
    patterns,
  };
}

// Generate specific improvement suggestions based on patterns
// FOCUSED ON ACTIVE CLASSIFIERS: governance-lifecycle.js + llm-classifier.js
// NOW INCLUDES: provenance, scope, confidence scoring, and versioning
function generateImprovementSuggestions(analysis) {
  const suggestions = [];
  
  // Load current patterns to check for duplicates
  const currentPatterns = getLearnedPatterns();
  
  for (const pattern of analysis.patterns) {
    const [origType, corrType] = pattern.transition.split(' → ');
    const keywords = pattern.keywords.map(k => k.word);
    
    // Calculate confidence/generality score
    const confidence = calculateConfidence(pattern, analysis);
    
    // Determine scope
    const scope = {
      applies: 'future_only', // future_only | reclassify_on_demand
      retroactive: false,
      description: 'Applies to future classifications only. Existing items unchanged.',
    };
    
    // Build provenance
    const provenance = {
      sourceCorrections: pattern.transitionCount,
      affectedEntities: pattern.examples.slice(0, 5),
      transition: pattern.transition,
      avgKeywordFrequency: pattern.keywords.reduce((sum, k) => sum + k.frequency, 0) / pattern.keywords.length,
    };
    
    if (pattern.transitionCount >= 1) {
      // === RULE-BASED PATTERN SUGGESTIONS (governance-lifecycle.js) ===
      
      if (corrType === 'validator' && origType !== 'validator') {
        const newKeywords = filterNewKeywords(keywords, currentPatterns?.validatorKeywords, ['validator', 'operator', 'node', 'the', 'and', 'for']);
        if (newKeywords.length > 0) {
          suggestions.push({
            file: 'governance-lifecycle.js',
            location: 'extractIdentifiers/isValidator regex',
            type: 'add_keyword',
            priority: pattern.transitionCount >= 3 ? 'high' : 'medium',
            description: `Add keywords to validator detection: ${newKeywords.slice(0, 3).join(', ')}`,
            keywords: newKeywords,
            codeChange: {
              target: 'VALIDATOR_KEYWORDS',
              action: 'add',
              values: newKeywords,
            },
            examples: pattern.examples,
            reason: `${pattern.transitionCount} items misclassified as ${origType} were actually validators`,
            confidence,
            scope,
            provenance,
            learningLayer: 'pattern', // pattern | instructional
          });
        }
      }
      
      if (corrType === 'featured-app' && origType !== 'featured-app') {
        const newKeywords = filterNewKeywords(keywords, currentPatterns?.featuredAppKeywords, ['featured', 'app', 'application', 'the', 'and', 'for']);
        if (newKeywords.length > 0) {
          suggestions.push({
            file: 'governance-lifecycle.js',
            location: 'extractIdentifiers/isFeaturedApp regex',
            type: 'add_keyword',
            priority: pattern.transitionCount >= 3 ? 'high' : 'medium',
            description: `Add keywords to featured-app detection: ${newKeywords.slice(0, 3).join(', ')}`,
            keywords: newKeywords,
            codeChange: {
              target: 'FEATURED_APP_KEYWORDS',
              action: 'add',
              values: newKeywords,
            },
            examples: pattern.examples,
            reason: `${pattern.transitionCount} items misclassified as ${origType} were actually featured apps`,
            confidence,
            scope,
            provenance,
            learningLayer: 'pattern',
          });
        }
      }
      
      if (corrType === 'cip' && origType !== 'cip') {
        const newKeywords = filterNewKeywords(keywords, currentPatterns?.cipKeywords, ['cip', 'proposal', 'the', 'and', 'for']);
        if (newKeywords.length > 0) {
          suggestions.push({
            file: 'governance-lifecycle.js',
            location: 'extractIdentifiers/cipNumber detection',
            type: 'add_keyword',
            priority: pattern.transitionCount >= 3 ? 'high' : 'medium',
            description: `Add CIP-related keywords: ${newKeywords.slice(0, 3).join(', ')}`,
            keywords: newKeywords,
            codeChange: {
              target: 'CIP_KEYWORDS',
              action: 'add',
              values: newKeywords,
            },
            examples: pattern.examples,
            reason: `${pattern.transitionCount} items misclassified as ${origType} were actually CIPs`,
            confidence,
            scope,
            provenance,
            learningLayer: 'pattern',
          });
        }
      }
      
      if (corrType === 'protocol-upgrade') {
        const newKeywords = filterNewKeywords(keywords, currentPatterns?.protocolUpgradeKeywords, ['upgrade', 'splice', 'migration', 'the', 'and', 'for']);
        if (newKeywords.length > 0) {
          suggestions.push({
            file: 'governance-lifecycle.js',
            location: 'correlateTopics/type detection',
            type: 'add_keyword',
            priority: pattern.transitionCount >= 2 ? 'high' : 'medium',
            description: `Add protocol-upgrade keywords: ${newKeywords.slice(0, 3).join(', ')}`,
            keywords: newKeywords,
            codeChange: {
              target: 'PROTOCOL_UPGRADE_KEYWORDS',
              action: 'add',
              values: newKeywords,
            },
            examples: pattern.examples,
            reason: `${pattern.transitionCount} items were manually reclassified to protocol-upgrade`,
            confidence,
            scope,
            provenance,
            learningLayer: 'pattern',
          });
        }
      }
      
      if (corrType === 'outcome') {
        const newKeywords = filterNewKeywords(keywords, currentPatterns?.outcomeKeywords, ['outcome', 'tokenomics', 'report', 'the', 'and', 'for']);
        if (newKeywords.length > 0) {
          suggestions.push({
            file: 'governance-lifecycle.js',
            location: 'correlateTopics/type detection',
            type: 'add_keyword',
            priority: pattern.transitionCount >= 2 ? 'high' : 'medium',
            description: `Add outcome keywords: ${newKeywords.slice(0, 3).join(', ')}`,
            keywords: newKeywords,
            codeChange: {
              target: 'OUTCOME_KEYWORDS',
              action: 'add',
              values: newKeywords,
            },
            examples: pattern.examples,
            reason: `${pattern.transitionCount} items were manually reclassified to outcome`,
            confidence,
            scope,
            provenance,
            learningLayer: 'pattern',
          });
        }
      }
      
      // === LLM PROMPT SUGGESTIONS (llm-classifier.js) ===
      // Split into two layers: instructional (definitions) vs pattern (examples)
      
      if (pattern.transitionCount >= 2) {
        // Pattern layer: add specific examples and keywords
        suggestions.push({
          file: 'llm-classifier.js',
          location: 'CLASSIFICATION_PROMPT',
          type: 'prompt_enhancement',
          priority: pattern.transitionCount >= 4 ? 'high' : 'medium',
          description: `Add ${corrType} disambiguation examples to LLM prompt`,
          keywords,
          promptAddition: generatePatternPromptAddition(origType, corrType, keywords, pattern.examples),
          examples: pattern.examples,
          reason: `${pattern.transitionCount} items misclassified as ${origType} were actually ${corrType}`,
          confidence,
          scope,
          provenance,
          learningLayer: 'pattern',
          promptType: 'example_injection', // example_injection | definition_clarification
        });
        
        // If high volume corrections, also suggest instructional improvements
        if (pattern.transitionCount >= 4) {
          suggestions.push({
            file: 'llm-classifier.js',
            location: 'CLASSIFICATION_PROMPT definitions',
            type: 'prompt_enhancement',
            priority: 'medium',
            description: `Clarify ${corrType} vs ${origType} definition boundary`,
            promptAddition: generateInstructionalPromptAddition(origType, corrType),
            examples: pattern.examples,
            reason: `High volume of ${origType} → ${corrType} corrections suggests unclear definition`,
            confidence: { ...confidence, level: 'contextual' }, // Definition changes are always contextual
            scope,
            provenance,
            learningLayer: 'instructional',
            promptType: 'definition_clarification',
          });
        }
      }
    }
  }
  
  // Sort by priority then by transition count
  const priorityOrder = { high: 0, medium: 1, low: 2 };
  suggestions.sort((a, b) => {
    const pDiff = priorityOrder[a.priority] - priorityOrder[b.priority];
    if (pDiff !== 0) return pDiff;
    return (b.provenance?.sourceCorrections || 0) - (a.provenance?.sourceCorrections || 0);
  });
  
  return suggestions;
}

// Calculate confidence/generality score for a pattern
function calculateConfidence(pattern, analysis) {
  const count = pattern.transitionCount;
  const avgFreq = pattern.keywords.reduce((sum, k) => sum + k.frequency, 0) / pattern.keywords.length;
  
  // Check if pattern appears across multiple entity types
  const uniqueEntities = new Set(pattern.examples.map(e => 
    e.toLowerCase().replace(/[^a-z]/g, '').slice(0, 20)
  )).size;
  
  let level = 'edge-case';
  let description = 'Rare pattern, may be brittle';
  
  if (count >= 5 && avgFreq > 0.7 && uniqueEntities >= 3) {
    level = 'general';
    description = 'Strong pattern seen across multiple entities/flows';
  } else if (count >= 2 && avgFreq > 0.5) {
    level = 'contextual';
    description = 'Pattern specific to certain lifecycle types';
  }
  
  return {
    level,
    description,
    sourceCount: count,
    avgKeywordMatch: avgFreq,
    uniqueEntities,
  };
}

// Filter out keywords that already exist in current patterns
function filterNewKeywords(keywords, existingKeywords, excludeList) {
  const existing = new Set([...(existingKeywords || []), ...excludeList].map(k => k.toLowerCase()));
  return keywords.filter(k => !existing.has(k.toLowerCase()));
}

// Generate pattern-layer prompt addition (examples and keywords)
function generatePatternPromptAddition(origType, corrType, keywords, examples) {
  const exampleText = examples.slice(0, 2).map(e => `"${e}"`).join(', ');
  
  return `
**Disambiguation: ${origType} vs ${corrType}**
- When subject contains: ${keywords.slice(0, 5).join(', ')}
- And the context suggests ${corrType}-specific governance
- Classify as **${corrType}**, not ${origType}
- Examples: ${exampleText}`;
}

// Generate instructional-layer prompt addition (definition clarification)
function generateInstructionalPromptAddition(origType, corrType) {
  const definitions = {
    'validator': 'entities operating network infrastructure (nodes, validators, super validators)',
    'featured-app': 'applications seeking or maintaining featured status on the network',
    'cip': 'Canton Improvement Proposals (CIP-XXXX format) for protocol changes',
    'protocol-upgrade': 'network-wide upgrades, migrations, or infrastructure changes',
    'outcome': 'monthly reports, tokenomics outcomes, or periodic summaries',
    'other': 'items that do not fit any specific governance category',
  };
  
  return `
**Definition clarification for ${corrType}:**
${corrType}: ${definitions[corrType] || 'See category definition'}

Key distinction from ${origType}: Focus on the primary governance action, not incidental mentions.`;
}

// ========== APPLY IMPROVEMENTS ==========
// Endpoint to apply learned patterns to the classification system

router.post('/apply-improvements', async (req, res) => {
  const { acceptedProposals, dryRun = true } = req.body;
  
  // Get current improvements
  const auditLog = readAuditLog();
  const overrides = readOverrides();
  const cached = readCache();
  
  // Load existing patterns for versioning
  let existingData = null;
  try {
    if (fs.existsSync(LEARNED_PATTERNS_FILE)) {
      existingData = JSON.parse(fs.readFileSync(LEARNED_PATTERNS_FILE, 'utf8'));
    }
  } catch (e) {}
  
  // Check learning mode (if disabled, reject)
  const learningMode = existingData?.learningMode ?? true;
  if (!learningMode && !dryRun) {
    return res.json({
      success: false,
      message: 'Learning mode is disabled. Enable it to apply improvements.',
      learningMode: false,
    });
  }
  
  // Collect corrections
  const corrections = [];
  for (const entry of auditLog) {
    if (entry.actionType === 'reclassify_item' || entry.actionType === 'reclassify_topic') {
      corrections.push({
        targetId: entry.targetId,
        label: entry.targetLabel || entry.targetId,
        originalType: entry.originalValue,
        correctedType: entry.newValue,
        timestamp: entry.timestamp,
      });
    }
  }
  
  for (const [key, override] of Object.entries(overrides.itemOverrides || {})) {
    if (!corrections.find(c => c.targetId === key)) {
      corrections.push({
        targetId: key,
        label: key,
        originalType: override.originalType,
        correctedType: override.type,
        timestamp: override.createdAt,
      });
    }
  }
  
  if (corrections.length === 0) {
    return res.json({
      success: false,
      message: 'No corrections found to learn from',
    });
  }
  
  // Generate learned patterns
  const learnedPatterns = generateLearnedPatterns(corrections, cached);
  
  // Calculate version (semantic versioning)
  const currentVersion = existingData?.version || '1.0.0';
  const [major, minor, patch] = currentVersion.split('.').map(Number);
  
  // Increment patch for additive changes, minor for structural changes
  const newPatternCount = Object.values(learnedPatterns).reduce((sum, arr) => 
    sum + (Array.isArray(arr) ? arr.length : Object.keys(arr).length), 0
  );
  const oldPatternCount = existingData?.patterns 
    ? Object.values(existingData.patterns).reduce((sum, arr) => 
        sum + (Array.isArray(arr) ? arr.length : Object.keys(arr).length), 0)
    : 0;
  
  let newVersion;
  if (newPatternCount > oldPatternCount * 1.5) {
    // Significant change - minor version bump
    newVersion = `${major}.${minor + 1}.0`;
  } else {
    // Additive change - patch version bump  
    newVersion = `${major}.${minor}.${patch + 1}`;
  }
  
  // Build data with versioning and provenance
  const newData = {
    version: newVersion,
    previousVersion: existingData?.version || null,
    generatedAt: new Date().toISOString(),
    basedOnCorrections: corrections.length,
    learningMode: true,
    patterns: learnedPatterns,
    history: [
      ...(existingData?.history || []).slice(-9), // Keep last 10 entries
      {
        version: newVersion,
        timestamp: new Date().toISOString(),
        correctionsApplied: corrections.length,
        acceptedProposals: acceptedProposals?.length || 'all',
        patternsAdded: {
          validator: learnedPatterns.validatorKeywords?.length || 0,
          featuredApp: learnedPatterns.featuredAppKeywords?.length || 0,
          cip: learnedPatterns.cipKeywords?.length || 0,
          protocolUpgrade: learnedPatterns.protocolUpgradeKeywords?.length || 0,
          outcome: learnedPatterns.outcomeKeywords?.length || 0,
          entities: Object.keys(learnedPatterns.entityNameMappings || {}).length,
        },
      },
    ],
  };
  
  if (!dryRun) {
    fs.writeFileSync(LEARNED_PATTERNS_FILE, JSON.stringify(newData, null, 2));
    // Clear cache so new patterns are loaded
    _learnedPatternsCache = null;
    _learnedPatternsMtime = 0;
  }
  
  res.json({
    success: true,
    dryRun,
    message: dryRun 
      ? 'Dry run - patterns generated but not saved. Set dryRun=false to apply.'
      : `Learned patterns v${newVersion} saved. Future classifications will use these patterns.`,
    correctionsAnalyzed: corrections.length,
    version: newVersion,
    previousVersion: existingData?.version || null,
    patternsGenerated: learnedPatterns,
    savedTo: dryRun ? null : LEARNED_PATTERNS_FILE,
  });
});

// Toggle learning mode
router.post('/learning-mode', (req, res) => {
  const { enabled } = req.body;
  
  if (typeof enabled !== 'boolean') {
    return res.status(400).json({ error: 'enabled must be a boolean' });
  }
  
  try {
    let data = { learningMode: enabled, patterns: {}, version: '1.0.0' };
    if (fs.existsSync(LEARNED_PATTERNS_FILE)) {
      data = JSON.parse(fs.readFileSync(LEARNED_PATTERNS_FILE, 'utf8'));
    }
    
    data.learningMode = enabled;
    data.learningModeChangedAt = new Date().toISOString();
    
    fs.writeFileSync(LEARNED_PATTERNS_FILE, JSON.stringify(data, null, 2));
    
    res.json({
      success: true,
      learningMode: enabled,
      message: enabled 
        ? 'Learning mode enabled. Corrections will be analyzed for proposals.'
        : 'Learning mode disabled. Corrections apply locally only.',
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ========== TEST AGAINST HISTORY ==========
// Validate proposals against historical classifications to prevent drift
router.post('/test-proposals', async (req, res) => {
  const { proposedPatterns, sampleSize = 50 } = req.body;
  
  const cached = readCache();
  const overrides = readOverrides();
  
  if (!cached?.lifecycleItems || cached.lifecycleItems.length === 0) {
    return res.json({
      success: false,
      message: 'No historical data available for testing',
    });
  }
  
  // Get a sample of historical items
  const allItems = cached.lifecycleItems || [];
  const sampleItems = allItems
    .filter(item => item.type && item.primaryId)
    .slice(0, Math.min(sampleSize, allItems.length));
  
  // Load current patterns for comparison
  const currentPatterns = getLearnedPatterns() || {};
  
  // Merge proposed patterns with current patterns (simulating what would happen)
  const testPatterns = {
    validatorKeywords: [...new Set([
      ...(currentPatterns.validatorKeywords || []),
      ...(proposedPatterns?.validatorKeywords || []),
    ])],
    featuredAppKeywords: [...new Set([
      ...(currentPatterns.featuredAppKeywords || []),
      ...(proposedPatterns?.featuredAppKeywords || []),
    ])],
    cipKeywords: [...new Set([
      ...(currentPatterns.cipKeywords || []),
      ...(proposedPatterns?.cipKeywords || []),
    ])],
    protocolUpgradeKeywords: [...new Set([
      ...(currentPatterns.protocolUpgradeKeywords || []),
      ...(proposedPatterns?.protocolUpgradeKeywords || []),
    ])],
    outcomeKeywords: [...new Set([
      ...(currentPatterns.outcomeKeywords || []),
      ...(proposedPatterns?.outcomeKeywords || []),
    ])],
    entityNameMappings: {
      ...(currentPatterns.entityNameMappings || {}),
      ...(proposedPatterns?.entityNameMappings || {}),
    },
  };
  
  // Test each item with current vs proposed patterns
  const results = {
    unchanged: [],
    improved: [],
    changed: [],
    degraded: [],
  };
  
  for (const item of sampleItems) {
    const subject = item.primaryId || '';
    const currentType = item.type;
    
    // Get the "true" type from overrides if available (human-verified)
    const overrideKey = item.primaryId;
    const trueType = overrides.itemOverrides?.[overrideKey]?.type || currentType;
    
    // Simulate classification with current patterns
    const currentResult = simulateClassification(subject, currentPatterns);
    
    // Simulate classification with proposed patterns
    const proposedResult = simulateClassification(subject, testPatterns);
    
    if (currentResult === proposedResult) {
      results.unchanged.push({
        id: item.primaryId,
        subject: subject.slice(0, 80),
        type: currentResult,
      });
    } else if (proposedResult === trueType && currentResult !== trueType) {
      // Proposed result matches the verified type - improvement
      results.improved.push({
        id: item.primaryId,
        subject: subject.slice(0, 80),
        currentType: currentResult,
        proposedType: proposedResult,
        trueType,
        reason: 'Proposed matches verified type',
      });
    } else if (currentResult === trueType && proposedResult !== trueType) {
      // Current was correct, proposed would break it - degradation
      results.degraded.push({
        id: item.primaryId,
        subject: subject.slice(0, 80),
        currentType: currentResult,
        proposedType: proposedResult,
        trueType,
        reason: 'Proposed would change correct classification',
      });
    } else {
      // Changed but unclear if better or worse
      results.changed.push({
        id: item.primaryId,
        subject: subject.slice(0, 80),
        currentType: currentResult,
        proposedType: proposedResult,
      });
    }
  }
  
  // Calculate summary metrics
  const total = sampleItems.length;
  const summary = {
    total,
    unchanged: results.unchanged.length,
    improved: results.improved.length,
    changed: results.changed.length,
    degraded: results.degraded.length,
    unchangedPercent: ((results.unchanged.length / total) * 100).toFixed(1),
    improvedPercent: ((results.improved.length / total) * 100).toFixed(1),
    degradedPercent: ((results.degraded.length / total) * 100).toFixed(1),
    safeToApply: results.degraded.length === 0,
    recommendation: results.degraded.length === 0 
      ? (results.improved.length > 0 ? 'Safe to apply - improvements detected' : 'Safe to apply - no regressions')
      : `Caution: ${results.degraded.length} items would regress`,
  };
  
  res.json({
    success: true,
    summary,
    results: {
      improved: results.improved.slice(0, 10),
      degraded: results.degraded,
      changed: results.changed.slice(0, 10),
    },
    testedPatterns: {
      current: {
        validator: currentPatterns.validatorKeywords?.length || 0,
        featuredApp: currentPatterns.featuredAppKeywords?.length || 0,
        cip: currentPatterns.cipKeywords?.length || 0,
      },
      proposed: {
        validator: testPatterns.validatorKeywords?.length || 0,
        featuredApp: testPatterns.featuredAppKeywords?.length || 0,
        cip: testPatterns.cipKeywords?.length || 0,
      },
    },
  });
});

// Simulate classification based on patterns (simplified version of correlateTopics logic)
function simulateClassification(subject, patterns) {
  if (!subject) return 'other';
  const textLower = subject.toLowerCase();
  
  // Check for CIP pattern
  if (/CIP\s*[-#]?\s*\d+/i.test(subject) || /^\s*0*\d{4}\s+/.test(subject)) {
    return 'cip';
  }
  
  // Check learned keywords
  if (patterns?.validatorKeywords?.some(kw => textLower.includes(kw.toLowerCase()))) {
    return 'validator';
  }
  if (patterns?.featuredAppKeywords?.some(kw => textLower.includes(kw.toLowerCase()))) {
    return 'featured-app';
  }
  if (patterns?.cipKeywords?.some(kw => textLower.includes(kw.toLowerCase()))) {
    return 'cip';
  }
  if (patterns?.protocolUpgradeKeywords?.some(kw => textLower.includes(kw.toLowerCase()))) {
    return 'protocol-upgrade';
  }
  if (patterns?.outcomeKeywords?.some(kw => textLower.includes(kw.toLowerCase()))) {
    return 'outcome';
  }
  
  // Check entity mappings
  for (const [entity, type] of Object.entries(patterns?.entityNameMappings || {})) {
    if (textLower.includes(entity.toLowerCase())) {
      return type;
    }
  }
  
  // Built-in patterns
  if (/validator\s*(?:approved|operator|onboarding|license)|super\s*validator|node\s+as\s+a\s+service/i.test(subject)) {
    return 'validator';
  }
  if (/featured\s*app|featured\s*application|app\s+(?:listing|request|tokenomics)/i.test(subject)) {
    return 'featured-app';
  }
  if (/splice\s+\d|upgrade|migration/i.test(subject)) {
    return 'protocol-upgrade';
  }
  if (/tokenomics\s+outcome|monthly\s+report/i.test(subject)) {
    return 'outcome';
  }
  
  return 'other';
}

// Generate learned patterns from corrections
function generateLearnedPatterns(corrections, cached) {
  const patterns = {
    validatorKeywords: new Set(),
    featuredAppKeywords: new Set(),
    cipKeywords: new Set(),
    protocolUpgradeKeywords: new Set(),
    outcomeKeywords: new Set(),
    entityNameMappings: {},  // entityName -> correctType
  };
  
  for (const correction of corrections) {
    const { label, originalType, correctedType } = correction;
    
    // Extract significant words from label
    const words = (label || '')
      .toLowerCase()
      .replace(/[^a-z0-9\s]/g, ' ')
      .split(/\s+/)
      .filter(w => w.length > 2 && !['the', 'and', 'for', 'new', 'vote', 'proposal'].includes(w));
    
    // Add words to appropriate keyword set based on corrected type
    switch (correctedType) {
      case 'validator':
        words.forEach(w => patterns.validatorKeywords.add(w));
        break;
      case 'featured-app':
        words.forEach(w => patterns.featuredAppKeywords.add(w));
        break;
      case 'cip':
        words.forEach(w => patterns.cipKeywords.add(w));
        break;
      case 'protocol-upgrade':
        words.forEach(w => patterns.protocolUpgradeKeywords.add(w));
        break;
      case 'outcome':
        words.forEach(w => patterns.outcomeKeywords.add(w));
        break;
    }
    
    // Track entity name -> type mappings
    const entityMatch = label?.match(/^([A-Z][A-Za-z0-9\s-]+?)(?:\s*[-:]|$)/);
    if (entityMatch) {
      const entityName = entityMatch[1].trim();
      if (entityName.length > 2) {
        patterns.entityNameMappings[entityName.toLowerCase()] = correctedType;
      }
    }
  }
  
  // Convert sets to arrays
  return {
    validatorKeywords: [...patterns.validatorKeywords],
    featuredAppKeywords: [...patterns.featuredAppKeywords],
    cipKeywords: [...patterns.cipKeywords],
    protocolUpgradeKeywords: [...patterns.protocolUpgradeKeywords],
    outcomeKeywords: [...patterns.outcomeKeywords],
    entityNameMappings: patterns.entityNameMappings,
  };
}

// Load learned patterns if they exist
function loadLearnedPatterns() {
  const learnedPatternsFile = path.join(CACHE_DIR, 'learned-patterns.json');
  try {
    if (fs.existsSync(learnedPatternsFile)) {
      const data = JSON.parse(fs.readFileSync(learnedPatternsFile, 'utf8'));
      return data.patterns || null;
    }
  } catch (e) {
    console.error('Failed to load learned patterns:', e.message);
  }
  return null;
}

// Get learned patterns status (enhanced with versioning and history)
router.get('/learned-patterns', (req, res) => {
  const learnedPatternsFile = path.join(CACHE_DIR, 'learned-patterns.json');
  
  try {
    if (!fs.existsSync(learnedPatternsFile)) {
      return res.json({
        exists: false,
        learningMode: true,
        message: 'No learned patterns. Use POST /apply-improvements to generate.',
      });
    }
    
    const data = JSON.parse(fs.readFileSync(learnedPatternsFile, 'utf8'));
    const patterns = data.patterns || {};
    
    res.json({
      exists: true,
      version: data.version || '1.0.0',
      previousVersion: data.previousVersion || null,
      generatedAt: data.generatedAt,
      basedOnCorrections: data.basedOnCorrections,
      learningMode: data.learningMode ?? true,
      learningModeChangedAt: data.learningModeChangedAt,
      patterns,
      stats: {
        validatorKeywords: patterns.validatorKeywords?.length || 0,
        featuredAppKeywords: patterns.featuredAppKeywords?.length || 0,
        cipKeywords: patterns.cipKeywords?.length || 0,
        protocolUpgradeKeywords: patterns.protocolUpgradeKeywords?.length || 0,
        outcomeKeywords: patterns.outcomeKeywords?.length || 0,
        entityMappings: Object.keys(patterns.entityNameMappings || {}).length,
      },
      history: data.history || [],
      // Calculate pending changes indicator
      pendingChanges: data.history?.length > 0 
        ? `v${data.version} has ${data.basedOnCorrections} learned corrections`
        : null,
    });
  } catch (e) {
    console.error('Failed to load learned patterns:', e.message);
    res.json({
      exists: false,
      error: e.message,
    });
  }
});

// Export training data for potential model fine-tuning
router.get('/classification-training-data', (req, res) => {
  const auditLog = readAuditLog();
  const overrides = readOverrides();
  const cached = readCache();
  
  const trainingData = [];
  
  // Collect all corrections with full topic content
  const processedIds = new Set();
  
  // From audit log
  for (const entry of auditLog) {
    if (entry.actionType === 'reclassify_item' || entry.actionType === 'reclassify_topic') {
      const targetId = entry.targetId;
      if (processedIds.has(targetId)) continue;
      processedIds.add(targetId);
      
      // Find full topic content from cache
      let content = null;
      if (cached?.lifecycleItems) {
        for (const item of cached.lifecycleItems) {
          if (item.primaryId === targetId || item.id === targetId) {
            // Item-level correction
            const topics = item.topics || [];
            content = {
              subject: item.primaryId,
              body: topics.map(t => t.subject).join(' | '),
              stage: item.currentStage,
            };
            break;
          }
          const topic = (item.topics || []).find(t => String(t.id) === String(targetId));
          if (topic) {
            content = {
              subject: topic.subject,
              body: topic.content || topic.excerpt || '',
              stage: topic.stage,
            };
            break;
          }
        }
      }
      
      trainingData.push({
        id: targetId,
        originalType: entry.originalValue,
        correctedType: entry.newValue,
        label: entry.targetLabel,
        content,
        source: 'audit',
      });
    }
  }
  
  // From overrides
  for (const [key, override] of Object.entries(overrides.itemOverrides || {})) {
    if (processedIds.has(key)) continue;
    processedIds.add(key);
    
    let content = null;
    if (cached?.lifecycleItems) {
      const item = cached.lifecycleItems.find(i => i.primaryId === key || i.id === key);
      if (item) {
        const topics = item.topics || [];
        content = {
          subject: item.primaryId,
          body: topics.map(t => t.subject).join(' | '),
          stage: item.currentStage,
        };
      }
    }
    
    trainingData.push({
      id: key,
      originalType: override.originalType,
      correctedType: override.type,
      label: key,
      content,
      source: 'override',
    });
  }
  
  res.json({
    count: trainingData.length,
    format: 'jsonl',
    description: 'Training data for classification model improvement',
    data: trainingData,
    // JSONL format for direct use
    jsonl: trainingData.map(d => JSON.stringify({
      text: `${d.content?.subject || d.label}\n${d.content?.body || ''}`.trim(),
      label: d.correctedType,
      original_label: d.originalType,
    })).join('\n'),
  });
});

export { fetchFreshData, writeCache, readCache };
export default router;
