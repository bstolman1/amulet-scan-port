/**
 * Hybrid Audit System - LLM verifies all rule-based classifications
 * 
 * Design principle: "LLMs may read raw human text exactly once per artifact;
 * results are cached and treated as authoritative governance metadata."
 * 
 * This implements the hybrid audit model:
 * 1. Rules classify everything (deterministic, fast)
 * 2. LLM reads content and confirms OR flags disagreement
 * 3. Disagreements are flagged for human review
 * 
 * Key distinction: assumed semantics vs verified semantics
 * - Rule-based: assumed semantics (may be wrong)
 * - LLM-verified: verified semantics (read actual content)
 */

import {
  getCachedAudit,
  needsVerification,
  cacheAuditResult,
  cacheAuditResultsBulk,
  getAuditStats,
  getDisagreements,
  getItemsNeedingReview,
  getAllAuditEntries,
  clearAuditCache,
  getSampleAuditEntries,
  VALID_TYPES,
  AUDIT_VERSION,
} from './audit-cache.js';

import {
  getCachedContent,
  getContentCacheStats,
} from './post-content-cache.js';

import { generateContentHash } from './post-content-cache.js';

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const MODEL = 'gpt-4o-mini';

// Verification prompt - simpler than classification, just verify the rule's output
const VERIFICATION_PROMPT = `You are auditing governance topic classifications for the Canton Network / Splice ecosystem. Your job is to read the full forum post and verify if the rule-based classification is correct.

## Classification Types:
- **cip**: Canton Improvement Proposals (protocol changes, CIP-XXXX numbers)
- **validator**: Validator/Super-Validator applications and operations
- **featured-app**: Featured applications on the network
- **protocol-upgrade**: Network upgrades and migrations (Splice versions, etc.)
- **outcome**: Tokenomics outcome reports
- **governance_discussion**: General governance discussions
- **meta**: Informational/administrative content
- **other**: Doesn't fit any category

## Your Task:
1. Read the post content carefully
2. Determine what type this content actually is
3. Compare with the rule-based classification
4. If they match, confirm. If not, provide the correct type.

## Response Format (JSON):
{
  "llm_type": "the correct type based on content",
  "confidence": 0.0 to 1.0,
  "reasoning": "brief explanation (1-2 sentences)"
}`;

/**
 * Check if auditor is available
 */
export function isAuditorAvailable() {
  return !!OPENAI_API_KEY;
}

/**
 * Verify a single item's classification
 * @param {object} item - Lifecycle item with type, topics, etc.
 * @param {object} options - Options
 * @returns {Promise<{llmType, confidence, reasoning, agreement, needsReview}>}
 */
export async function verifyItem(item, options = {}) {
  const { forceReverify = false } = options;
  
  if (!OPENAI_API_KEY) {
    return { error: 'OPENAI_API_KEY not set' };
  }
  
  const itemId = item.id || item.primaryId;
  const ruleType = item.type;
  const firstTopic = item.topics?.[0];
  
  // Get content for the first topic
  const content = getCachedContent(firstTopic?.id);
  const subject = content?.subject || firstTopic?.subject || item.primaryId;
  const body = content?.body || firstTopic?.excerpt || '';
  const contentHash = generateContentHash(subject, body);
  
  // Check if we need to verify
  if (!forceReverify) {
    const check = needsVerification(itemId, contentHash);
    if (!check.needs) {
      const cached = getCachedAudit(itemId);
      return {
        llmType: cached.llm_type,
        confidence: cached.llm_confidence,
        reasoning: cached.llm_reasoning,
        ruleType: cached.rule_type,
        agreement: cached.agreement,
        needsReview: cached.needs_review,
        cached: true,
      };
    }
  }
  
  try {
    const userPrompt = buildVerificationPrompt(subject, body, ruleType);
    
    const response = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${OPENAI_API_KEY}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        model: MODEL,
        messages: [
          { role: 'system', content: VERIFICATION_PROMPT },
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
      return { error: `API error: ${response.status}` };
    }

    const data = await response.json();
    const resultContent = data.choices[0]?.message?.content;
    const tokensUsed = data.usage?.total_tokens || 0;
    
    let result;
    try {
      result = JSON.parse(resultContent);
    } catch (err) {
      console.error('❌ Failed to parse LLM response:', resultContent);
      return { error: 'Failed to parse response' };
    }
    
    // Normalize and validate the type
    let llmType = result.llm_type?.toLowerCase();
    if (!VALID_TYPES.includes(llmType)) {
      llmType = 'other';
    }
    
    const confidence = typeof result.confidence === 'number' 
      ? Math.max(0, Math.min(1, result.confidence)) 
      : 0.8;
    const reasoning = result.reasoning || null;
    
    // Cache the result
    const auditEntry = cacheAuditResult({
      itemId,
      primaryId: item.primaryId,
      ruleType,
      llmType,
      llmConfidence: confidence,
      llmReasoning: reasoning,
      llmModel: MODEL,
      contentHash,
      tokensUsed,
    });
    
    const symbol = auditEntry.agreement ? '✓' : '⚠️';
    console.log(`${symbol} Verified "${item.primaryId?.slice(0, 40)}": rule=${ruleType} llm=${llmType} (${(confidence * 100).toFixed(0)}%)`);
    
    return {
      llmType,
      confidence,
      reasoning,
      ruleType,
      agreement: auditEntry.agreement,
      needsReview: auditEntry.needs_review,
      cached: false,
    };
  } catch (error) {
    console.error('❌ Verification error:', error.message);
    return { error: error.message };
  }
}

/**
 * Build verification prompt with content
 */
function buildVerificationPrompt(subject, body, ruleType) {
  const bodyTruncated = body?.length > 4000 ? body.substring(0, 4000) + '\n[truncated]' : body;
  
  return `## Rule-based classification: "${ruleType}"

## Title:
${subject || 'No subject'}

## Forum post content:
"""
${bodyTruncated || 'No content available'}
"""

Verify if the rule-based classification "${ruleType}" is correct. If incorrect, provide the correct type.`;
}

/**
 * Verify all lifecycle items (one-time pass)
 * @param {Array} lifecycleItems - All items to verify
 * @param {object} options - Options (forceReverify, onProgress)
 * @returns {Promise<{verified, cached, failed, agreements, disagreements}>}
 */
export async function verifyAllItems(lifecycleItems, options = {}) {
  const { forceReverify = false, onProgress = null } = options;
  
  if (!OPENAI_API_KEY) {
    console.log('⚠️ Audit unavailable (OPENAI_API_KEY not set)');
    return { verified: 0, cached: 0, failed: 0, agreements: 0, disagreements: 0 };
  }
  
  if (!lifecycleItems?.length) {
    return { verified: 0, cached: 0, failed: 0, agreements: 0, disagreements: 0 };
  }
  
  console.log(`\n📋 Starting hybrid audit of ${lifecycleItems.length} lifecycle items...`);
  console.log('   Design principle: "assumed semantics vs verified semantics"');
  console.log('   Rule-based = assumed | LLM-verified = verified\n');
  
  const toVerify = [];
  let cachedCount = 0;
  let cachedAgreements = 0;
  let cachedDisagreements = 0;
  
  // Check which items need verification
  for (const item of lifecycleItems) {
    const itemId = item.id || item.primaryId;
    const firstTopic = item.topics?.[0];
    const content = getCachedContent(firstTopic?.id);
    const subject = content?.subject || firstTopic?.subject || item.primaryId;
    const body = content?.body || firstTopic?.excerpt || '';
    const contentHash = generateContentHash(subject, body);
    
    if (!forceReverify) {
      const check = needsVerification(itemId, contentHash);
      if (!check.needs) {
        const cached = getCachedAudit(itemId);
        cachedCount++;
        if (cached.agreement) {
          cachedAgreements++;
        } else {
          cachedDisagreements++;
        }
        continue;
      }
    }
    
    toVerify.push(item);
  }
  
  if (toVerify.length === 0) {
    console.log(`📋 All ${cachedCount} items already verified (cached)`);
    console.log(`   ✓ Agreements: ${cachedAgreements} | ⚠️ Disagreements: ${cachedDisagreements}`);
    return { 
      verified: 0, 
      cached: cachedCount, 
      failed: 0, 
      agreements: cachedAgreements, 
      disagreements: cachedDisagreements 
    };
  }
  
  console.log(`📋 Verifying ${toVerify.length} items with LLM (${cachedCount} already cached)...`);
  
  // Process in small batches
  const batchSize = 3;
  let verified = 0;
  let failed = 0;
  let newAgreements = 0;
  let newDisagreements = 0;
  
  for (let i = 0; i < toVerify.length; i += batchSize) {
    const batch = toVerify.slice(i, i + batchSize);
    
    const promises = batch.map(item => verifyItem(item, { forceReverify: true }));
    const results = await Promise.all(promises);
    
    for (const result of results) {
      if (result.error) {
        failed++;
      } else {
        verified++;
        if (result.agreement) {
          newAgreements++;
        } else {
          newDisagreements++;
        }
      }
    }
    
    if (onProgress) {
      onProgress(Math.min(i + batchSize, toVerify.length), toVerify.length);
    }
    
    // Rate limit
    if (i + batchSize < toVerify.length) {
      await new Promise(resolve => setTimeout(resolve, 300));
    }
  }
  
  const totalAgreements = cachedAgreements + newAgreements;
  const totalDisagreements = cachedDisagreements + newDisagreements;
  const total = cachedCount + verified;
  const agreementRate = total > 0 ? ((totalAgreements / total) * 100).toFixed(1) : 0;
  
  console.log(`\n📋 Hybrid audit complete:`);
  console.log(`   Total verified: ${total} (${verified} new, ${cachedCount} cached)`);
  console.log(`   ✓ Agreements: ${totalAgreements} (${agreementRate}%)`);
  console.log(`   ⚠️ Disagreements: ${totalDisagreements}`);
  if (failed > 0) {
    console.log(`   ❌ Failed: ${failed}`);
  }
  
  return {
    verified,
    cached: cachedCount,
    failed,
    agreements: totalAgreements,
    disagreements: totalDisagreements,
    agreementRate: parseFloat(agreementRate),
    total,
  };
}

/**
 * Get audit status and statistics
 */
export function getAuditStatus() {
  const stats = getAuditStats();
  const needsReview = getItemsNeedingReview();
  const contentStats = getContentCacheStats();
  
  return {
    available: isAuditorAvailable(),
    model: MODEL,
    auditVersion: AUDIT_VERSION,
    stats,
    needsReviewCount: needsReview.length,
    needsReviewItems: needsReview.slice(0, 10), // First 10 for preview
    contentCacheStats: contentStats,
  };
}

/**
 * Get items where rule and LLM disagree
 */
export function getAuditDisagreements() {
  return getDisagreements();
}

/**
 * Get items flagged for review
 */
export function getAuditReviewItems() {
  return getItemsNeedingReview();
}

// Re-export utilities
export {
  getAllAuditEntries,
  clearAuditCache,
  getSampleAuditEntries,
  getAuditStats,
  VALID_TYPES,
  AUDIT_VERSION,
};
