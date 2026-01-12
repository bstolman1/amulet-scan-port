/**
 * Per-Decision Explainability - Audit-grade classification traces
 * 
 * For every governance item, persists:
 * - Signals extracted (keywords, regex hits)
 * - Patterns applied (learned patterns with confidence)
 * - LLM reasoning (if LLM was consulted)
 * - Final decision logic
 * 
 * This enables:
 * - Perfect debugging
 * - Historical replay
 * - Audit-grade traceability
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const BASE_DATA_DIR = process.env.DATA_DIR || path.resolve(__dirname, '../../data');
const DECISIONS_DIR = path.join(BASE_DATA_DIR, 'cache', 'decision-traces');
const DECISIONS_INDEX_FILE = path.join(DECISIONS_DIR, 'index.json');

// Ensure directories exist
function ensureDir() {
  if (!fs.existsSync(DECISIONS_DIR)) {
    fs.mkdirSync(DECISIONS_DIR, { recursive: true });
  }
}

/**
 * Read the decisions index
 */
function readDecisionsIndex() {
  try {
    if (fs.existsSync(DECISIONS_INDEX_FILE)) {
      return JSON.parse(fs.readFileSync(DECISIONS_INDEX_FILE, 'utf-8'));
    }
  } catch (err) {
    console.error('Error reading decisions index:', err.message);
  }
  return {
    version: '1.0.0',
    createdAt: new Date().toISOString(),
    decisionCount: 0,
    lastUpdated: null,
  };
}

/**
 * Write the decisions index
 */
function writeDecisionsIndex(index) {
  ensureDir();
  index.lastUpdated = new Date().toISOString();
  fs.writeFileSync(DECISIONS_INDEX_FILE, JSON.stringify(index, null, 2));
}

/**
 * Store a classification decision trace
 * 
 * @param {object} params - Decision parameters
 * @returns {object} Stored trace with ID
 */
export function storeDecisionTrace({
  itemId,
  subject,
  body = null,
  finalType,
  classifierVersion,
  signals = {},
  patternsMatched = [],
  llmReasoning = null,
  decisionLogic = [],
  confidence = null,
  metadata = {},
}) {
  ensureDir();
  
  const trace = {
    id: `trace-${itemId}-${Date.now()}`,
    itemId,
    subject: subject?.slice(0, 200),
    bodyHash: body ? hashContent(body) : null,
    finalType,
    classifierVersion,
    decidedAt: new Date().toISOString(),
    
    // What signals were extracted
    signals: {
      keywords: signals.keywords || [],
      regexHits: signals.regexHits || [],
      entityMatches: signals.entityMatches || [],
      ...signals,
    },
    
    // Which learned patterns contributed
    patternsMatched: patternsMatched.map(p => ({
      keyword: p.keyword,
      type: p.type,
      confidence: p.confidence,
      layer: p.layer || 'pattern',
    })),
    
    // LLM reasoning if consulted
    llm: llmReasoning ? {
      consulted: true,
      type: llmReasoning.type,
      confidence: llmReasoning.confidence,
      reasoning: llmReasoning.reasoning,
      model: llmReasoning.model,
    } : { consulted: false },
    
    // Step-by-step decision logic
    decisionLogic: decisionLogic.map((step, idx) => ({
      step: idx + 1,
      action: step.action,
      input: step.input,
      result: step.result,
      confidence: step.confidence,
    })),
    
    // Overall confidence
    confidence: confidence || calculateOverallConfidence(signals, patternsMatched, llmReasoning),
    
    // Additional metadata
    metadata: {
      sourceStage: metadata.sourceStage,
      sourceGroup: metadata.sourceGroup,
      ...metadata,
    },
  };
  
  // Store individual trace file
  const traceFile = path.join(DECISIONS_DIR, `${itemId}.json`);
  
  // If file exists, append to history
  let existingData = { traces: [] };
  if (fs.existsSync(traceFile)) {
    try {
      existingData = JSON.parse(fs.readFileSync(traceFile, 'utf-8'));
    } catch {}
  }
  
  existingData.traces.push(trace);
  existingData.latestTrace = trace;
  existingData.itemId = itemId;
  existingData.subject = subject;
  existingData.lastUpdated = new Date().toISOString();
  
  fs.writeFileSync(traceFile, JSON.stringify(existingData, null, 2));
  
  // Update index
  const index = readDecisionsIndex();
  index.decisionCount++;
  writeDecisionsIndex(index);
  
  return trace;
}

/**
 * Get decision trace for an item
 */
export function getDecisionTrace(itemId) {
  const traceFile = path.join(DECISIONS_DIR, `${itemId}.json`);
  
  if (!fs.existsSync(traceFile)) {
    return null;
  }
  
  try {
    return JSON.parse(fs.readFileSync(traceFile, 'utf-8'));
  } catch (err) {
    console.error(`Error reading trace for ${itemId}:`, err.message);
    return null;
  }
}

/**
 * Get latest decision for an item
 */
export function getLatestDecision(itemId) {
  const data = getDecisionTrace(itemId);
  return data?.latestTrace || null;
}

/**
 * Query decisions by various criteria
 */
export function queryDecisions({
  type = null,
  fromDate = null,
  toDate = null,
  hasLLM = null,
  minConfidence = null,
  limit = 100,
}) {
  ensureDir();
  
  const results = [];
  const files = fs.readdirSync(DECISIONS_DIR).filter(f => f.endsWith('.json') && f !== 'index.json');
  
  for (const file of files) {
    if (results.length >= limit) break;
    
    try {
      const data = JSON.parse(fs.readFileSync(path.join(DECISIONS_DIR, file), 'utf-8'));
      const trace = data.latestTrace;
      
      if (!trace) continue;
      
      // Apply filters
      if (type && trace.finalType !== type) continue;
      if (hasLLM !== null && trace.llm?.consulted !== hasLLM) continue;
      if (minConfidence && trace.confidence < minConfidence) continue;
      if (fromDate && new Date(trace.decidedAt) < new Date(fromDate)) continue;
      if (toDate && new Date(trace.decidedAt) > new Date(toDate)) continue;
      
      results.push({
        itemId: data.itemId,
        subject: data.subject,
        ...trace,
      });
    } catch {}
  }
  
  return results.sort((a, b) => new Date(b.decidedAt) - new Date(a.decidedAt));
}

/**
 * Get decision stats
 */
export function getDecisionStats() {
  ensureDir();
  
  const index = readDecisionsIndex();
  const files = fs.readdirSync(DECISIONS_DIR).filter(f => f.endsWith('.json') && f !== 'index.json');
  
  const stats = {
    totalItems: files.length,
    totalDecisions: index.decisionCount,
    byType: {},
    withLLM: 0,
    avgConfidence: 0,
    lastUpdated: index.lastUpdated,
  };
  
  let totalConfidence = 0;
  let confidenceCount = 0;
  
  for (const file of files) {
    try {
      const data = JSON.parse(fs.readFileSync(path.join(DECISIONS_DIR, file), 'utf-8'));
      const trace = data.latestTrace;
      
      if (!trace) continue;
      
      stats.byType[trace.finalType] = (stats.byType[trace.finalType] || 0) + 1;
      
      if (trace.llm?.consulted) {
        stats.withLLM++;
      }
      
      if (trace.confidence) {
        totalConfidence += trace.confidence;
        confidenceCount++;
      }
    } catch {}
  }
  
  stats.avgConfidence = confidenceCount > 0 
    ? (totalConfidence / confidenceCount).toFixed(2) 
    : 0;
  
  return stats;
}

/**
 * Generate human-readable explanation for a decision
 */
export function explainDecision(itemId) {
  const data = getDecisionTrace(itemId);
  
  if (!data?.latestTrace) {
    return `No decision trace found for item: ${itemId}`;
  }
  
  const trace = data.latestTrace;
  const lines = [
    `## Classification Decision for: ${trace.subject}`,
    ``,
    `**Final Type:** ${trace.finalType}`,
    `**Confidence:** ${(trace.confidence * 100).toFixed(0)}%`,
    `**Decided At:** ${trace.decidedAt}`,
    `**Classifier Version:** ${trace.classifierVersion}`,
    ``,
  ];
  
  // Signals
  if (trace.signals?.keywords?.length > 0 || trace.signals?.regexHits?.length > 0) {
    lines.push(`### Signals Detected`);
    if (trace.signals.keywords?.length > 0) {
      lines.push(`- Keywords: ${trace.signals.keywords.join(', ')}`);
    }
    if (trace.signals.regexHits?.length > 0) {
      lines.push(`- Regex matches: ${trace.signals.regexHits.join(', ')}`);
    }
    if (trace.signals.entityMatches?.length > 0) {
      lines.push(`- Entity matches: ${trace.signals.entityMatches.join(', ')}`);
    }
    lines.push(``);
  }
  
  // Patterns
  if (trace.patternsMatched?.length > 0) {
    lines.push(`### Learned Patterns Applied`);
    for (const p of trace.patternsMatched) {
      lines.push(`- "${p.keyword}" → ${p.type} (${(p.confidence * 100).toFixed(0)}% confidence)`);
    }
    lines.push(``);
  }
  
  // LLM
  if (trace.llm?.consulted) {
    lines.push(`### LLM Verification`);
    lines.push(`- Model: ${trace.llm.model}`);
    lines.push(`- LLM Type: ${trace.llm.type} (${(trace.llm.confidence * 100).toFixed(0)}% confidence)`);
    lines.push(`- Reasoning: ${trace.llm.reasoning}`);
    lines.push(``);
  }
  
  // Decision logic
  if (trace.decisionLogic?.length > 0) {
    lines.push(`### Decision Steps`);
    for (const step of trace.decisionLogic) {
      lines.push(`${step.step}. ${step.action}: ${step.result}`);
    }
    lines.push(``);
  }
  
  return lines.join('\n');
}

/**
 * Calculate overall confidence from components
 */
function calculateOverallConfidence(signals, patterns, llm) {
  let confidence = 0.5; // Base confidence
  
  // Boost from signals
  if (signals?.regexHits?.length > 0) {
    confidence += 0.2;
  }
  if (signals?.keywords?.length > 0) {
    confidence += 0.1;
  }
  
  // Boost from patterns
  if (patterns?.length > 0) {
    const avgPatternConf = patterns.reduce((sum, p) => sum + (p.confidence || 0.5), 0) / patterns.length;
    confidence += avgPatternConf * 0.2;
  }
  
  // LLM provides strong signal
  if (llm?.confidence) {
    confidence = (confidence + llm.confidence) / 2;
  }
  
  return Math.min(1.0, Math.max(0, confidence));
}

/**
 * Simple content hash
 */
function hashContent(content) {
  if (!content) return null;
  let hash = 0;
  for (let i = 0; i < content.length; i++) {
    const char = content.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash = hash & hash;
  }
  return hash.toString(16);
}

/**
 * Clear all decision traces (for testing)
 */
export function clearDecisionTraces() {
  ensureDir();
  const files = fs.readdirSync(DECISIONS_DIR);
  for (const file of files) {
    fs.unlinkSync(path.join(DECISIONS_DIR, file));
  }
  console.log('🗑️ Decision traces cleared');
}

export default {
  storeDecisionTrace,
  getDecisionTrace,
  getLatestDecision,
  queryDecisions,
  getDecisionStats,
  explainDecision,
  clearDecisionTraces,
};
