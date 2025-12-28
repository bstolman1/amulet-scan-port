/**
 * LLM-based classification for ambiguous governance topics
 * 
 * Design principle: "LLMs may read raw human text exactly once per artifact;
 * results are cached and treated as authoritative governance metadata."
 * 
 * Classification is run ONCE per post and cached. Never re-run unless:
 * - Post body changed (detected via content hash)
 * - Manual reindex requested
 * - Prompt/schema version changed
 */

import {
  getCachedClassification,
  needsClassification,
  cacheClassification,
  cacheClassificationsBulk,
  getLLMCacheStats,
  getAllClassifications,
  VALID_TYPES,
  PROMPT_VERSION,
} from './llm-classification-cache.js';

import {
  getCachedContent,
  getContentCacheStats,
} from './post-content-cache.js';

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const MODEL = 'gpt-4o-mini';

// Enhanced classification prompt that uses full post content
const CLASSIFICATION_PROMPT = `You are an expert governance topic classifier for the Canton Network / Splice ecosystem. Your job is to analyze the full forum post content to classify each topic accurately.

## Classification Types:

**cip** - Canton Improvement Proposals
- Technical protocol changes, standards, new features
- Usually contains "CIP-XXXX", "CIP #XX", or 4-digit proposal numbers
- Discusses protocol specifications, governance rules, voting parameters
- Formal proposals for network changes

**validator** - Validator/Super-Validator applications and operations
- About companies becoming validators or super-validators
- Node operators, infrastructure providers joining the network
- Validator approvals, applications, onboarding discussions
- Look for company names with terms like "SV", "validator", "operator", "node"

**featured-app** - Featured applications on the network
- Apps being reviewed, featured, or added to Canton/Splice
- Usually mentions app/product names with terms like "featured", "app", "application"
- May reference testnet or mainnet deployments

**protocol-upgrade** - Network upgrades and migrations
- Splice version upgrades (e.g., Splice 0.2.x)
- Synchronizer migrations, hard forks, breaking changes
- Global domain participant upgrades

**outcome** - Tokenomics outcome reports
- Periodic reports about network tokenomics
- Round outcomes, CC statistics, network metrics

**governance_discussion** - General governance discussions
- Discussions about governance processes
- Policy discussions, procedural matters
- Not tied to a specific CIP or proposal

**meta** - Informational/administrative content
- Administrative announcements
- Meeting notes, status updates
- Technical documentation

**other** - Only use this if topic genuinely doesn't fit ANY other category

## Analysis Guidelines:
1. Read the FULL post content carefully, not just the subject
2. Look for key entity names (companies, apps, protocols)
3. CIPs almost always have a number - if no number but discusses protocol changes, still use "cip"
4. When in doubt between validator/featured-app, check if it's infrastructure (validator) vs application (featured-app)
5. Consider the overall intent and what action is being proposed or discussed

## Response Format:
Respond with a JSON object containing:
- type: one of the classification types (lowercase)
- confidence: number between 0 and 1
- reasoning: brief explanation (1-2 sentences max)

Example:
{"type": "cip", "confidence": 0.95, "reasoning": "Contains CIP-0054 proposal for adding a new super validator."}`;

/**
 * Check if LLM classification is available
 */
export function isLLMAvailable() {
  return !!OPENAI_API_KEY;
}

/**
 * Classify a single topic using LLM with full post content
 * Uses cached classification if available and valid
 * 
 * @param {string} topicId - The topic ID
 * @param {string} subject - The topic subject line
 * @param {string} body - The full post body
 * @param {object} options - Options (forceReclassify, etc.)
 * @returns {Promise<{type: string, confidence: number, reasoning: string, llmClassified: boolean, cached: boolean}>}
 */
export async function classifyTopic(topicId, subject, body, options = {}) {
  const { forceReclassify = false } = options;
  
  if (!OPENAI_API_KEY) {
    console.warn('⚠️ OPENAI_API_KEY not set, skipping LLM classification');
    return { type: null, confidence: 0, llmClassified: false, cached: false };
  }
  
  // Check if we need to classify (unless forced)
  if (!forceReclassify) {
    const check = needsClassification(topicId, subject, body);
    if (!check.needs) {
      // Return cached result
      const cached = getCachedClassification(topicId);
      return {
        type: cached.llm_type,
        confidence: cached.llm_confidence,
        reasoning: cached.llm_reasoning,
        llmClassified: true,
        cached: true,
      };
    }
  }
  
  try {
    // Build the prompt with full content
    const userPrompt = buildClassificationPrompt(subject, body);
    
    const response = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${OPENAI_API_KEY}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        model: MODEL,
        messages: [
          { role: 'system', content: CLASSIFICATION_PROMPT },
          { role: 'user', content: userPrompt }
        ],
        max_tokens: 150,
        temperature: 0,
        response_format: { type: 'json_object' },
      }),
    });

    if (!response.ok) {
      const error = await response.text();
      console.error('❌ OpenAI API error:', response.status, error);
      return { type: null, confidence: 0, llmClassified: false, cached: false };
    }

    const data = await response.json();
    const content = data.choices[0]?.message?.content;
    const tokensUsed = data.usage?.total_tokens || 0;
    
    // Parse the JSON response
    let result;
    try {
      result = JSON.parse(content);
    } catch (err) {
      console.error('❌ Failed to parse LLM response:', content);
      return { type: null, confidence: 0, llmClassified: false, cached: false };
    }
    
    // Validate and normalize the type
    let type = result.type?.toLowerCase();
    if (!VALID_TYPES.includes(type)) {
      console.warn(`⚠️ Invalid type "${type}" from LLM, defaulting to "other"`);
      type = 'other';
    }
    
    const confidence = typeof result.confidence === 'number' 
      ? Math.max(0, Math.min(1, result.confidence)) 
      : 0.8;
    const reasoning = result.reasoning || null;
    
    // Cache the result
    cacheClassification({
      topicId,
      subject,
      body,
      type,
      confidence,
      reasoning,
      model: MODEL,
      tokensUsed,
    });
    
    console.log(`🤖 LLM classified "${subject.slice(0, 50)}..." -> ${type} (${(confidence * 100).toFixed(0)}%)`);
    
    return { 
      type, 
      confidence,
      reasoning,
      llmClassified: true,
      cached: false,
    };
  } catch (error) {
    console.error('❌ LLM classification error:', error.message);
    return { type: null, confidence: 0, llmClassified: false, cached: false };
  }
}

/**
 * Build the user prompt with subject and body
 */
function buildClassificationPrompt(subject, body) {
  const bodyTruncated = body?.length > 4000 ? body.substring(0, 4000) + '\n[truncated]' : body;
  
  return `Title:
${subject || 'No subject'}

Forum post content:
"""
${bodyTruncated || 'No content available'}
"""

Task:
Classify this content into one of the following lifecycle types:
- cip
- validator
- featured-app
- protocol-upgrade
- outcome
- governance_discussion
- meta
- other

Return a JSON object with: type, confidence (0-1), and short reasoning.`;
}

/**
 * Batch classify multiple topics
 * Only classifies topics that need classification (not cached or content changed)
 * 
 * @param {Array<{id: string, subject: string, groupName: string}>} topics - Topics to classify
 * @param {object} options - Options
 * @returns {Promise<{classified: number, cached: number, failed: number, results: Map}>}
 */
export async function classifyTopicsBatch(topics, options = {}) {
  const { forceReclassify = false, onProgress = null } = options;
  
  if (!OPENAI_API_KEY || topics.length === 0) {
    return { classified: 0, cached: 0, failed: 0, results: new Map() };
  }
  
  const results = new Map();
  const toClassify = [];
  let cachedCount = 0;
  
  // Check which topics need classification
  for (const topic of topics) {
    // Get cached content (must have been fetched first)
    const content = getCachedContent(topic.id);
    const body = content?.body || topic.excerpt || '';
    const subject = content?.subject || topic.subject || '';
    
    if (!forceReclassify) {
      const check = needsClassification(topic.id, subject, body);
      if (!check.needs) {
        // Use cached result
        const cached = getCachedClassification(topic.id);
        results.set(topic.id, {
          type: cached.llm_type,
          confidence: cached.llm_confidence,
          reasoning: cached.llm_reasoning,
          llmClassified: true,
          cached: true,
        });
        cachedCount++;
        continue;
      }
    }
    
    toClassify.push({
      id: topic.id,
      subject,
      body,
      groupName: topic.groupName,
    });
  }
  
  if (toClassify.length === 0) {
    console.log(`🤖 All ${cachedCount} topics already have valid LLM classifications`);
    return { classified: 0, cached: cachedCount, failed: 0, results };
  }
  
  console.log(`🤖 Classifying ${toClassify.length} topics with LLM (${cachedCount} already cached)...`);
  
  // Process in small batches to avoid rate limits
  const batchSize = 3;
  let classified = 0;
  let failed = 0;
  const toCache = [];
  
  for (let i = 0; i < toClassify.length; i += batchSize) {
    const batch = toClassify.slice(i, i + batchSize);
    
    const promises = batch.map(async (topic) => {
      const result = await classifyTopic(topic.id, topic.subject, topic.body, { forceReclassify: true });
      return { id: topic.id, subject: topic.subject, body: topic.body, result };
    });
    
    const batchResults = await Promise.all(promises);
    
    for (const { id, subject, body, result } of batchResults) {
      results.set(id, result);
      if (result.type && result.llmClassified) {
        classified++;
      } else {
        failed++;
      }
    }
    
    if (onProgress) {
      onProgress(Math.min(i + batchSize, toClassify.length), toClassify.length);
    }
    
    // Rate limit between batches
    if (i + batchSize < toClassify.length) {
      await new Promise(resolve => setTimeout(resolve, 300));
    }
  }
  
  console.log(`🤖 LLM classification complete: ${classified} classified, ${failed} failed, ${cachedCount} were cached`);
  
  return { 
    classified, 
    cached: cachedCount, 
    failed, 
    results,
    total: topics.length,
  };
}

/**
 * Get classification for a topic (from cache, doesn't trigger new classification)
 */
export function getClassification(topicId) {
  return getCachedClassification(topicId);
}

/**
 * Get statistics about the LLM classification system
 */
export function getClassificationStats() {
  const llmStats = getLLMCacheStats();
  const contentStats = getContentCacheStats();
  
  return {
    llmAvailable: isLLMAvailable(),
    model: MODEL,
    promptVersion: PROMPT_VERSION,
    llm: llmStats,
    content: contentStats,
  };
}

// Re-export for convenience
export { 
  getAllClassifications, 
  VALID_TYPES, 
  PROMPT_VERSION,
  getLLMCacheStats,
};
