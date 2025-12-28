/**
 * LLM Classification Cache - Persistent storage for LLM classification results
 * 
 * Design principle: "LLMs may read raw human text exactly once per artifact;
 * results are cached and treated as authoritative governance metadata."
 * 
 * Classification is run ONCE per post and cached. Never re-run unless:
 * - Post body changed (detected via content hash)
 * - Manual reindex requested
 * - Prompt/schema version changed
 * 
 * Stores:
 * - llm_type: Classification result
 * - llm_confidence: Model confidence (0-1)
 * - llm_reasoning: Short reasoning from model
 * - llm_model: Model used
 * - llm_classified_at: When classification occurred
 * - llm_input_hash: Hash of input (subject+body) for invalidation
 * - prompt_version: Version of prompt used (for schema changes)
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { generateContentHash } from './post-content-cache.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const BASE_DATA_DIR = process.env.DATA_DIR || path.resolve(__dirname, '../../data');
const LLM_CACHE_DIR = path.join(BASE_DATA_DIR, 'cache', 'llm-classifications');
const LLM_INDEX_FILE = path.join(LLM_CACHE_DIR, 'index.json');

// Current prompt/schema version - increment when classification schema changes
export const PROMPT_VERSION = '2.0.0';

// Valid classification types
export const VALID_TYPES = ['cip', 'validator', 'featured-app', 'protocol-upgrade', 'outcome', 'governance_discussion', 'meta', 'other'];

// Ensure cache directory exists
function ensureCacheDir() {
  if (!fs.existsSync(LLM_CACHE_DIR)) {
    fs.mkdirSync(LLM_CACHE_DIR, { recursive: true });
  }
}

/**
 * Read the LLM classification index
 */
export function readLLMIndex() {
  try {
    if (fs.existsSync(LLM_INDEX_FILE)) {
      return JSON.parse(fs.readFileSync(LLM_INDEX_FILE, 'utf-8'));
    }
  } catch (err) {
    console.error('Error reading LLM index:', err.message);
  }
  return { 
    classifications: {}, 
    stats: { 
      totalClassified: 0, 
      lastUpdated: null,
      promptVersion: PROMPT_VERSION,
      byType: {},
      tokenUsage: { total: 0 },
    } 
  };
}

/**
 * Write the LLM classification index
 */
function writeLLMIndex(index) {
  ensureCacheDir();
  index.stats.lastUpdated = new Date().toISOString();
  index.stats.promptVersion = PROMPT_VERSION;
  fs.writeFileSync(LLM_INDEX_FILE, JSON.stringify(index, null, 2));
}

/**
 * Get cached LLM classification for a topic
 * @returns {object|null} Cached classification or null
 */
export function getCachedClassification(topicId) {
  const index = readLLMIndex();
  return index.classifications[String(topicId)] || null;
}

/**
 * Check if a topic needs (re)classification
 * Returns true if:
 * - No cached classification exists
 * - Input content has changed (hash mismatch)
 * - Prompt version changed
 */
export function needsClassification(topicId, currentSubject, currentBody) {
  const cached = getCachedClassification(topicId);
  
  if (!cached) {
    return { needs: true, reason: 'no_cache' };
  }
  
  // Check prompt version
  if (cached.prompt_version !== PROMPT_VERSION) {
    return { needs: true, reason: 'prompt_version_changed' };
  }
  
  // Check content hash
  const currentHash = generateContentHash(currentSubject, currentBody);
  if (cached.llm_input_hash !== currentHash) {
    return { needs: true, reason: 'content_changed' };
  }
  
  return { needs: false, reason: 'cached' };
}

/**
 * Store an LLM classification result
 */
export function cacheClassification({
  topicId,
  subject,
  body,
  type,
  confidence,
  reasoning,
  model,
  tokensUsed = 0,
}) {
  ensureCacheDir();
  const index = readLLMIndex();
  const idStr = String(topicId);
  
  const inputHash = generateContentHash(subject, body);
  
  const entry = {
    topic_id: idStr,
    llm_type: type,
    llm_confidence: confidence,
    llm_reasoning: reasoning || null,
    llm_model: model || 'gpt-4o-mini',
    llm_classified_at: new Date().toISOString(),
    llm_input_hash: inputHash,
    prompt_version: PROMPT_VERSION,
    tokens_used: tokensUsed,
  };
  
  // Update type stats
  const oldEntry = index.classifications[idStr];
  if (oldEntry?.llm_type) {
    index.stats.byType[oldEntry.llm_type] = Math.max(0, (index.stats.byType[oldEntry.llm_type] || 0) - 1);
  }
  index.stats.byType[type] = (index.stats.byType[type] || 0) + 1;
  
  index.classifications[idStr] = entry;
  index.stats.totalClassified = Object.keys(index.classifications).length;
  index.stats.tokenUsage.total += tokensUsed;
  
  writeLLMIndex(index);
  
  return entry;
}

/**
 * Bulk cache multiple classification results
 */
export function cacheClassificationsBulk(results) {
  ensureCacheDir();
  const index = readLLMIndex();
  let count = 0;
  
  for (const result of results) {
    const idStr = String(result.topicId);
    const inputHash = generateContentHash(result.subject, result.body);
    
    // Update type stats
    const oldEntry = index.classifications[idStr];
    if (oldEntry?.llm_type) {
      index.stats.byType[oldEntry.llm_type] = Math.max(0, (index.stats.byType[oldEntry.llm_type] || 0) - 1);
    }
    index.stats.byType[result.type] = (index.stats.byType[result.type] || 0) + 1;
    
    index.classifications[idStr] = {
      topic_id: idStr,
      llm_type: result.type,
      llm_confidence: result.confidence,
      llm_reasoning: result.reasoning || null,
      llm_model: result.model || 'gpt-4o-mini',
      llm_classified_at: new Date().toISOString(),
      llm_input_hash: inputHash,
      prompt_version: PROMPT_VERSION,
      tokens_used: result.tokensUsed || 0,
    };
    
    index.stats.tokenUsage.total += (result.tokensUsed || 0);
    count++;
  }
  
  if (count > 0) {
    index.stats.totalClassified = Object.keys(index.classifications).length;
    writeLLMIndex(index);
    console.log(`🤖 LLM cache: stored ${count} classifications`);
  }
  
  return count;
}

/**
 * Get all cached classifications as a Map for quick lookup
 */
export function getAllClassifications() {
  const index = readLLMIndex();
  return new Map(
    Object.entries(index.classifications).map(([id, entry]) => [id, entry])
  );
}

/**
 * Get stats about the LLM classification cache
 */
export function getLLMCacheStats() {
  const index = readLLMIndex();
  return {
    totalClassified: index.stats.totalClassified || 0,
    lastUpdated: index.stats.lastUpdated || null,
    promptVersion: index.stats.promptVersion || PROMPT_VERSION,
    byType: index.stats.byType || {},
    tokenUsage: index.stats.tokenUsage || { total: 0 },
    cacheLocation: LLM_CACHE_DIR,
  };
}

/**
 * Invalidate a specific classification (forces re-classification on next run)
 */
export function invalidateClassification(topicId) {
  const index = readLLMIndex();
  const idStr = String(topicId);
  
  if (index.classifications[idStr]) {
    const oldEntry = index.classifications[idStr];
    if (oldEntry.llm_type) {
      index.stats.byType[oldEntry.llm_type] = Math.max(0, (index.stats.byType[oldEntry.llm_type] || 0) - 1);
    }
    delete index.classifications[idStr];
    index.stats.totalClassified = Object.keys(index.classifications).length;
    writeLLMIndex(index);
    console.log(`🗑️ Invalidated classification for topic ${topicId}`);
    return true;
  }
  return false;
}

/**
 * Clear all LLM classifications (for testing/full reindex)
 */
export function clearLLMCache() {
  if (fs.existsSync(LLM_INDEX_FILE)) {
    fs.unlinkSync(LLM_INDEX_FILE);
  }
  console.log('🗑️ LLM classification cache cleared');
}

/**
 * Get sample of classified items for debugging
 */
export function getSampleClassifications(limit = 10) {
  const index = readLLMIndex();
  const entries = Object.values(index.classifications);
  return entries.slice(0, limit);
}
