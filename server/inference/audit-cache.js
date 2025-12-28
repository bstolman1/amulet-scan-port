/**
 * Hybrid Audit Cache - Stores both rule-based and LLM classifications for comparison
 * 
 * Design principle: "LLMs may read raw human text exactly once per artifact;
 * results are cached and treated as authoritative governance metadata."
 * 
 * This implements the hybrid audit model where:
 * - Rules classify everything (deterministic)
 * - LLM reads content and confirms OR flags disagreement
 * - Disagreements are flagged for human review
 * 
 * Each lifecycle item stores:
 * - rule_type: Classification from deterministic rules
 * - llm_type: Classification from LLM (verified semantics)
 * - agreement: Boolean - do they match?
 * - needs_review: Boolean - flagged for human review
 * - llm_confidence: LLM confidence score
 * - llm_reasoning: LLM reasoning
 * - classified_at: When LLM classification occurred
 * - content_hash: Hash of input for invalidation
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { generateContentHash } from './post-content-cache.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const BASE_DATA_DIR = process.env.DATA_DIR || path.resolve(__dirname, '../../data');
const AUDIT_CACHE_DIR = path.join(BASE_DATA_DIR, 'cache', 'audit');
const AUDIT_INDEX_FILE = path.join(AUDIT_CACHE_DIR, 'audit-index.json');

// Current audit schema version
export const AUDIT_VERSION = '1.0.0';

// Valid types for classification
export const VALID_TYPES = ['cip', 'validator', 'featured-app', 'protocol-upgrade', 'outcome', 'governance_discussion', 'meta', 'other'];

// Ensure cache directory exists
function ensureCacheDir() {
  if (!fs.existsSync(AUDIT_CACHE_DIR)) {
    fs.mkdirSync(AUDIT_CACHE_DIR, { recursive: true });
  }
}

/**
 * Read the audit index
 */
export function readAuditIndex() {
  try {
    if (fs.existsSync(AUDIT_INDEX_FILE)) {
      return JSON.parse(fs.readFileSync(AUDIT_INDEX_FILE, 'utf-8'));
    }
  } catch (err) {
    console.error('Error reading audit index:', err.message);
  }
  return {
    version: AUDIT_VERSION,
    items: {},
    stats: {
      total: 0,
      verified: 0,
      agreements: 0,
      disagreements: 0,
      needsReview: 0,
      lastUpdated: null,
      byRuleType: {},
      byLLMType: {},
    }
  };
}

/**
 * Write the audit index
 */
function writeAuditIndex(index) {
  ensureCacheDir();
  index.stats.lastUpdated = new Date().toISOString();
  index.version = AUDIT_VERSION;
  fs.writeFileSync(AUDIT_INDEX_FILE, JSON.stringify(index, null, 2));
}

/**
 * Get cached audit entry for an item
 */
export function getCachedAudit(itemId) {
  const index = readAuditIndex();
  return index.items[String(itemId)] || null;
}

/**
 * Check if an item needs LLM verification
 * Returns true if:
 * - No cached audit exists
 * - Content has changed (hash mismatch)
 * - Audit schema version changed
 */
export function needsVerification(itemId, contentHash) {
  const cached = getCachedAudit(itemId);
  
  if (!cached) {
    return { needs: true, reason: 'no_cache' };
  }
  
  if (cached.content_hash !== contentHash) {
    return { needs: true, reason: 'content_changed' };
  }
  
  if (cached.audit_version !== AUDIT_VERSION) {
    return { needs: true, reason: 'version_changed' };
  }
  
  return { needs: false, reason: 'cached' };
}

/**
 * Store an audit result (rule type + LLM verification)
 */
export function cacheAuditResult({
  itemId,
  primaryId,
  ruleType,
  llmType,
  llmConfidence,
  llmReasoning,
  llmModel,
  contentHash,
  tokensUsed = 0,
}) {
  ensureCacheDir();
  const index = readAuditIndex();
  const idStr = String(itemId);
  
  // Determine agreement
  const agreement = normalizeType(ruleType) === normalizeType(llmType);
  const needsReview = !agreement && llmConfidence > 0.7; // High confidence disagreement
  
  // Update old entry stats
  const oldEntry = index.items[idStr];
  if (oldEntry) {
    if (oldEntry.rule_type) {
      index.stats.byRuleType[oldEntry.rule_type] = Math.max(0, (index.stats.byRuleType[oldEntry.rule_type] || 0) - 1);
    }
    if (oldEntry.llm_type) {
      index.stats.byLLMType[oldEntry.llm_type] = Math.max(0, (index.stats.byLLMType[oldEntry.llm_type] || 0) - 1);
    }
    if (oldEntry.agreement) {
      index.stats.agreements = Math.max(0, index.stats.agreements - 1);
    } else if (oldEntry.llm_type) {
      index.stats.disagreements = Math.max(0, index.stats.disagreements - 1);
    }
    if (oldEntry.needs_review) {
      index.stats.needsReview = Math.max(0, index.stats.needsReview - 1);
    }
  }
  
  const entry = {
    item_id: idStr,
    primary_id: primaryId,
    rule_type: ruleType,
    llm_type: llmType,
    agreement,
    needs_review: needsReview,
    llm_confidence: llmConfidence,
    llm_reasoning: llmReasoning || null,
    llm_model: llmModel || 'gpt-4o-mini',
    classified_at: new Date().toISOString(),
    content_hash: contentHash,
    audit_version: AUDIT_VERSION,
    tokens_used: tokensUsed,
  };
  
  // Update new stats
  index.stats.byRuleType[ruleType] = (index.stats.byRuleType[ruleType] || 0) + 1;
  index.stats.byLLMType[llmType] = (index.stats.byLLMType[llmType] || 0) + 1;
  
  if (agreement) {
    index.stats.agreements++;
  } else {
    index.stats.disagreements++;
  }
  
  if (needsReview) {
    index.stats.needsReview++;
  }
  
  index.items[idStr] = entry;
  index.stats.total = Object.keys(index.items).length;
  index.stats.verified = Object.values(index.items).filter(e => e.llm_type).length;
  
  writeAuditIndex(index);
  
  return entry;
}

/**
 * Bulk cache multiple audit results
 */
export function cacheAuditResultsBulk(results) {
  ensureCacheDir();
  const index = readAuditIndex();
  let count = 0;
  
  for (const result of results) {
    const idStr = String(result.itemId);
    const agreement = normalizeType(result.ruleType) === normalizeType(result.llmType);
    const needsReview = !agreement && result.llmConfidence > 0.7;
    
    // Update old entry stats
    const oldEntry = index.items[idStr];
    if (oldEntry) {
      if (oldEntry.rule_type) {
        index.stats.byRuleType[oldEntry.rule_type] = Math.max(0, (index.stats.byRuleType[oldEntry.rule_type] || 0) - 1);
      }
      if (oldEntry.llm_type) {
        index.stats.byLLMType[oldEntry.llm_type] = Math.max(0, (index.stats.byLLMType[oldEntry.llm_type] || 0) - 1);
      }
      if (oldEntry.agreement) {
        index.stats.agreements = Math.max(0, index.stats.agreements - 1);
      } else if (oldEntry.llm_type) {
        index.stats.disagreements = Math.max(0, index.stats.disagreements - 1);
      }
      if (oldEntry.needs_review) {
        index.stats.needsReview = Math.max(0, index.stats.needsReview - 1);
      }
    }
    
    index.items[idStr] = {
      item_id: idStr,
      primary_id: result.primaryId,
      rule_type: result.ruleType,
      llm_type: result.llmType,
      agreement,
      needs_review: needsReview,
      llm_confidence: result.llmConfidence,
      llm_reasoning: result.llmReasoning || null,
      llm_model: result.llmModel || 'gpt-4o-mini',
      classified_at: new Date().toISOString(),
      content_hash: result.contentHash,
      audit_version: AUDIT_VERSION,
      tokens_used: result.tokensUsed || 0,
    };
    
    // Update new stats
    index.stats.byRuleType[result.ruleType] = (index.stats.byRuleType[result.ruleType] || 0) + 1;
    index.stats.byLLMType[result.llmType] = (index.stats.byLLMType[result.llmType] || 0) + 1;
    
    if (agreement) {
      index.stats.agreements++;
    } else {
      index.stats.disagreements++;
    }
    
    if (needsReview) {
      index.stats.needsReview++;
    }
    
    count++;
  }
  
  if (count > 0) {
    index.stats.total = Object.keys(index.items).length;
    index.stats.verified = Object.values(index.items).filter(e => e.llm_type).length;
    writeAuditIndex(index);
    console.log(`📋 Audit cache: stored ${count} entries`);
  }
  
  return count;
}

/**
 * Get all items needing review (disagreements with high confidence)
 */
export function getItemsNeedingReview() {
  const index = readAuditIndex();
  return Object.values(index.items).filter(item => item.needs_review);
}

/**
 * Get all disagreements
 */
export function getDisagreements() {
  const index = readAuditIndex();
  return Object.values(index.items).filter(item => !item.agreement);
}

/**
 * Get audit statistics
 */
export function getAuditStats() {
  const index = readAuditIndex();
  return {
    version: index.version,
    total: index.stats.total || 0,
    verified: index.stats.verified || 0,
    agreements: index.stats.agreements || 0,
    disagreements: index.stats.disagreements || 0,
    needsReview: index.stats.needsReview || 0,
    agreementRate: index.stats.verified > 0 
      ? ((index.stats.agreements / index.stats.verified) * 100).toFixed(1) + '%'
      : 'N/A',
    lastUpdated: index.stats.lastUpdated || null,
    byRuleType: index.stats.byRuleType || {},
    byLLMType: index.stats.byLLMType || {},
    cacheLocation: AUDIT_CACHE_DIR,
  };
}

/**
 * Get all audit entries as a Map
 */
export function getAllAuditEntries() {
  const index = readAuditIndex();
  return new Map(
    Object.entries(index.items).map(([id, entry]) => [id, entry])
  );
}

/**
 * Clear all audit data
 */
export function clearAuditCache() {
  if (fs.existsSync(AUDIT_INDEX_FILE)) {
    fs.unlinkSync(AUDIT_INDEX_FILE);
  }
  console.log('🗑️ Audit cache cleared');
}

/**
 * Normalize type for comparison (handle equivalent types)
 */
function normalizeType(type) {
  if (!type) return 'other';
  const normalized = type.toLowerCase().trim();
  
  // Handle some equivalences
  if (normalized === 'governance_discussion' || normalized === 'discussion') {
    return 'governance_discussion';
  }
  if (normalized === 'protocol-upgrade' || normalized === 'upgrade') {
    return 'protocol-upgrade';
  }
  if (normalized === 'featured-app' || normalized === 'app') {
    return 'featured-app';
  }
  
  return normalized;
}

/**
 * Resolve a disagreement manually (set the correct type)
 */
export function resolveDisagreement(itemId, resolvedType, resolvedBy = 'manual') {
  const index = readAuditIndex();
  const idStr = String(itemId);
  
  if (!index.items[idStr]) {
    return null;
  }
  
  const entry = index.items[idStr];
  
  // Update stats for old state
  if (entry.needs_review) {
    index.stats.needsReview = Math.max(0, index.stats.needsReview - 1);
  }
  if (!entry.agreement) {
    index.stats.disagreements = Math.max(0, index.stats.disagreements - 1);
    index.stats.agreements++;
  }
  
  // Update entry
  entry.resolved_type = resolvedType;
  entry.resolved_by = resolvedBy;
  entry.resolved_at = new Date().toISOString();
  entry.needs_review = false;
  entry.agreement = true; // Treat as resolved
  
  writeAuditIndex(index);
  console.log(`✓ Resolved disagreement for ${itemId} -> ${resolvedType}`);
  
  return entry;
}

/**
 * Get sample audit entries for debugging
 */
export function getSampleAuditEntries(limit = 10, filter = null) {
  const index = readAuditIndex();
  let entries = Object.values(index.items);
  
  if (filter === 'disagreements') {
    entries = entries.filter(e => !e.agreement);
  } else if (filter === 'needs_review') {
    entries = entries.filter(e => e.needs_review);
  } else if (filter === 'agreements') {
    entries = entries.filter(e => e.agreement);
  }
  
  return entries.slice(0, limit);
}
