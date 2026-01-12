/**
 * Golden Evaluation Set - Fixed benchmark for classification accuracy
 * 
 * Design: A curated set of ~50-150 governance items with verified ground-truth labels.
 * This set NEVER changes (except to fix obvious errors), providing:
 * - Objective progress measurement across classifier versions
 * - Regression guarantees
 * - Credibility with auditors/stakeholders
 * 
 * Each golden item includes:
 * - id: Unique identifier
 * - subject: The topic subject line
 * - body: Full content (optional but recommended)
 * - trueType: Human-verified classification
 * - addedAt: When added to golden set
 * - addedBy: Who verified this classification
 * - notes: Why this is a good test case (edge case, etc.)
 * - category: 'standard' | 'edge_case' | 'boundary'
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const BASE_DATA_DIR = process.env.DATA_DIR || path.resolve(__dirname, '../../data');
const GOLDEN_SET_DIR = path.join(BASE_DATA_DIR, 'cache', 'golden-set');
const GOLDEN_SET_FILE = path.join(GOLDEN_SET_DIR, 'golden-items.json');
const EVALUATION_HISTORY_FILE = path.join(GOLDEN_SET_DIR, 'evaluation-history.json');

// Ensure directories exist
function ensureDir() {
  if (!fs.existsSync(GOLDEN_SET_DIR)) {
    fs.mkdirSync(GOLDEN_SET_DIR, { recursive: true });
  }
}

/**
 * Read the golden evaluation set
 */
export function readGoldenSet() {
  try {
    if (fs.existsSync(GOLDEN_SET_FILE)) {
      return JSON.parse(fs.readFileSync(GOLDEN_SET_FILE, 'utf-8'));
    }
  } catch (err) {
    console.error('Error reading golden set:', err.message);
  }
  return {
    version: '1.0.0',
    createdAt: new Date().toISOString(),
    lastModified: null,
    items: [],
    stats: {
      total: 0,
      byType: {},
      byCategory: {},
    },
  };
}

/**
 * Write the golden evaluation set
 */
function writeGoldenSet(data) {
  ensureDir();
  data.lastModified = new Date().toISOString();
  data.stats = calculateStats(data.items);
  fs.writeFileSync(GOLDEN_SET_FILE, JSON.stringify(data, null, 2));
}

/**
 * Calculate stats for the golden set
 */
function calculateStats(items) {
  const byType = {};
  const byCategory = {};
  
  for (const item of items) {
    byType[item.trueType] = (byType[item.trueType] || 0) + 1;
    byCategory[item.category || 'standard'] = (byCategory[item.category || 'standard'] || 0) + 1;
  }
  
  return {
    total: items.length,
    byType,
    byCategory,
  };
}

/**
 * Add an item to the golden set
 */
export function addGoldenItem({
  id,
  subject,
  body = null,
  trueType,
  addedBy = 'system',
  notes = null,
  category = 'standard',
  sourceUrl = null,
}) {
  const goldenSet = readGoldenSet();
  
  // Check for duplicates
  if (goldenSet.items.find(i => i.id === id)) {
    return { success: false, error: 'Item already exists in golden set' };
  }
  
  const newItem = {
    id,
    subject,
    body,
    trueType,
    addedAt: new Date().toISOString(),
    addedBy,
    notes,
    category,
    sourceUrl,
    frozen: true, // Golden items are frozen by default
  };
  
  goldenSet.items.push(newItem);
  writeGoldenSet(goldenSet);
  
  console.log(`🏆 Added to golden set: "${subject.slice(0, 50)}..." (${trueType})`);
  
  return { success: true, item: newItem };
}

/**
 * Remove an item from the golden set (use sparingly - only for errors)
 */
export function removeGoldenItem(id, reason) {
  const goldenSet = readGoldenSet();
  const idx = goldenSet.items.findIndex(i => i.id === id);
  
  if (idx === -1) {
    return { success: false, error: 'Item not found in golden set' };
  }
  
  const removed = goldenSet.items.splice(idx, 1)[0];
  
  // Log removal for audit
  goldenSet.removalLog = goldenSet.removalLog || [];
  goldenSet.removalLog.push({
    id,
    subject: removed.subject,
    trueType: removed.trueType,
    removedAt: new Date().toISOString(),
    reason,
  });
  
  writeGoldenSet(goldenSet);
  
  console.log(`🗑️ Removed from golden set: "${removed.subject.slice(0, 50)}..." (${reason})`);
  
  return { success: true, removed };
}

/**
 * Update a golden item's true type (use sparingly - only for labeling errors)
 */
export function updateGoldenItemType(id, newType, reason) {
  const goldenSet = readGoldenSet();
  const item = goldenSet.items.find(i => i.id === id);
  
  if (!item) {
    return { success: false, error: 'Item not found in golden set' };
  }
  
  const oldType = item.trueType;
  item.trueType = newType;
  item.typeHistory = item.typeHistory || [];
  item.typeHistory.push({
    from: oldType,
    to: newType,
    changedAt: new Date().toISOString(),
    reason,
  });
  
  writeGoldenSet(goldenSet);
  
  console.log(`✏️ Updated golden item type: "${item.subject.slice(0, 50)}..." ${oldType} → ${newType}`);
  
  return { success: true, item };
}

/**
 * Read evaluation history
 */
export function readEvaluationHistory() {
  try {
    if (fs.existsSync(EVALUATION_HISTORY_FILE)) {
      return JSON.parse(fs.readFileSync(EVALUATION_HISTORY_FILE, 'utf-8'));
    }
  } catch (err) {
    console.error('Error reading evaluation history:', err.message);
  }
  return { evaluations: [] };
}

/**
 * Write evaluation history
 */
function writeEvaluationHistory(data) {
  ensureDir();
  fs.writeFileSync(EVALUATION_HISTORY_FILE, JSON.stringify(data, null, 2));
}

/**
 * Evaluate a classifier against the golden set
 * Returns accuracy metrics and identifies regressions vs the previous evaluation
 * 
 * @param {Function} classifyFn - Function that takes (subject, body) and returns predicted type
 * @param {string} classifierVersion - Version of the classifier being tested
 * @param {object} metadata - Additional metadata about the classifier
 */
export async function evaluateAgainstGoldenSet(classifyFn, classifierVersion, metadata = {}) {
  const goldenSet = readGoldenSet();
  const history = readEvaluationHistory();
  
  if (goldenSet.items.length === 0) {
    return {
      success: false,
      error: 'Golden set is empty. Add items first.',
    };
  }
  
  const results = {
    classifierVersion,
    evaluatedAt: new Date().toISOString(),
    metadata,
    goldenSetVersion: goldenSet.version,
    goldenSetSize: goldenSet.items.length,
    correct: 0,
    incorrect: 0,
    accuracy: 0,
    byType: {},
    byCategory: {},
    predictions: [],
    regressions: [],
    improvements: [],
  };
  
  // Get previous evaluation for comparison
  const prevEval = history.evaluations[history.evaluations.length - 1];
  const prevPredictions = new Map(
    (prevEval?.predictions || []).map(p => [p.id, p])
  );
  
  // Evaluate each item
  for (const item of goldenSet.items) {
    let predictedType;
    
    try {
      predictedType = await classifyFn(item.subject, item.body);
    } catch (err) {
      console.error(`Error classifying golden item ${item.id}:`, err.message);
      predictedType = 'error';
    }
    
    const isCorrect = predictedType === item.trueType;
    const prediction = {
      id: item.id,
      subject: item.subject.slice(0, 80),
      trueType: item.trueType,
      predictedType,
      correct: isCorrect,
      category: item.category,
    };
    
    results.predictions.push(prediction);
    
    if (isCorrect) {
      results.correct++;
    } else {
      results.incorrect++;
    }
    
    // Track by type
    if (!results.byType[item.trueType]) {
      results.byType[item.trueType] = { total: 0, correct: 0 };
    }
    results.byType[item.trueType].total++;
    if (isCorrect) results.byType[item.trueType].correct++;
    
    // Track by category
    const cat = item.category || 'standard';
    if (!results.byCategory[cat]) {
      results.byCategory[cat] = { total: 0, correct: 0 };
    }
    results.byCategory[cat].total++;
    if (isCorrect) results.byCategory[cat].correct++;
    
    // Check for regression or improvement vs previous
    const prevPred = prevPredictions.get(item.id);
    if (prevPred) {
      if (prevPred.correct && !isCorrect) {
        // Regression: was correct, now incorrect
        results.regressions.push({
          id: item.id,
          subject: item.subject.slice(0, 60),
          trueType: item.trueType,
          previousPrediction: prevPred.predictedType,
          currentPrediction: predictedType,
        });
      } else if (!prevPred.correct && isCorrect) {
        // Improvement: was incorrect, now correct
        results.improvements.push({
          id: item.id,
          subject: item.subject.slice(0, 60),
          trueType: item.trueType,
          previousPrediction: prevPred.predictedType,
          currentPrediction: predictedType,
        });
      }
    }
  }
  
  // Calculate accuracy
  results.accuracy = results.goldenSetSize > 0 
    ? ((results.correct / results.goldenSetSize) * 100).toFixed(1)
    : '0.0';
  
  // Calculate per-type accuracy
  for (const type in results.byType) {
    const t = results.byType[type];
    t.accuracy = t.total > 0 ? ((t.correct / t.total) * 100).toFixed(1) : '0.0';
  }
  
  // Calculate per-category accuracy
  for (const cat in results.byCategory) {
    const c = results.byCategory[cat];
    c.accuracy = c.total > 0 ? ((c.correct / c.total) * 100).toFixed(1) : '0.0';
  }
  
  // Calculate change from previous
  if (prevEval) {
    results.previousAccuracy = prevEval.accuracy;
    results.accuracyDelta = (parseFloat(results.accuracy) - parseFloat(prevEval.accuracy)).toFixed(1);
    results.previousVersion = prevEval.classifierVersion;
  }
  
  // Save to history
  history.evaluations.push(results);
  writeEvaluationHistory(history);
  
  // Log summary
  console.log(`\n🏆 Golden Set Evaluation - ${classifierVersion}`);
  console.log(`   Accuracy: ${results.accuracy}%${results.accuracyDelta ? ` (${results.accuracyDelta > 0 ? '+' : ''}${results.accuracyDelta}%)` : ''}`);
  console.log(`   Correct: ${results.correct}/${results.goldenSetSize}`);
  if (results.regressions.length > 0) {
    console.log(`   ⚠️ Regressions: ${results.regressions.length}`);
  }
  if (results.improvements.length > 0) {
    console.log(`   ✅ Improvements: ${results.improvements.length}`);
  }
  console.log('');
  
  return results;
}

/**
 * Check if a proposal would cause regressions on the golden set
 * This is the core NO-REGRESSION POLICY enforcement
 * 
 * @param {Function} currentClassifyFn - Current classifier
 * @param {Function} proposedClassifyFn - Proposed classifier with new patterns
 * @returns {object} Policy result with allow/block decision
 */
export async function checkNoRegressionPolicy(currentClassifyFn, proposedClassifyFn) {
  const goldenSet = readGoldenSet();
  
  if (goldenSet.items.length === 0) {
    return {
      allowed: true,
      reason: 'No golden set to validate against',
      regressions: [],
      improvements: [],
    };
  }
  
  const regressions = [];
  const improvements = [];
  const unchanged = [];
  
  for (const item of goldenSet.items) {
    let currentPrediction, proposedPrediction;
    
    try {
      currentPrediction = await currentClassifyFn(item.subject, item.body);
      proposedPrediction = await proposedClassifyFn(item.subject, item.body);
    } catch (err) {
      console.error(`Error in policy check for ${item.id}:`, err.message);
      continue;
    }
    
    const currentCorrect = currentPrediction === item.trueType;
    const proposedCorrect = proposedPrediction === item.trueType;
    
    if (currentCorrect && !proposedCorrect) {
      regressions.push({
        id: item.id,
        subject: item.subject.slice(0, 60),
        trueType: item.trueType,
        currentPrediction,
        proposedPrediction,
        category: item.category,
      });
    } else if (!currentCorrect && proposedCorrect) {
      improvements.push({
        id: item.id,
        subject: item.subject.slice(0, 60),
        trueType: item.trueType,
        currentPrediction,
        proposedPrediction,
      });
    } else {
      unchanged.push({ id: item.id, correct: currentCorrect });
    }
  }
  
  // Apply no-regression policy
  const allowed = regressions.length === 0;
  
  return {
    allowed,
    reason: allowed 
      ? (improvements.length > 0 
          ? `Safe to apply: ${improvements.length} improvements, 0 regressions`
          : 'Safe to apply: no changes to golden set accuracy')
      : `BLOCKED: ${regressions.length} regression(s) on golden set`,
    regressions,
    improvements,
    unchanged: unchanged.length,
    stats: {
      total: goldenSet.items.length,
      regressions: regressions.length,
      improvements: improvements.length,
      unchanged: unchanged.length,
    },
  };
}

/**
 * Get golden set with full items
 */
export function getGoldenSetFull() {
  return readGoldenSet();
}

/**
 * Get golden set summary (for UI)
 */
export function getGoldenSetSummary() {
  const gs = readGoldenSet();
  const history = readEvaluationHistory();
  const lastEval = history.evaluations[history.evaluations.length - 1];
  
  return {
    version: gs.version,
    itemCount: gs.items.length,
    stats: gs.stats,
    lastModified: gs.lastModified,
    lastEvaluation: lastEval ? {
      accuracy: lastEval.accuracy,
      classifierVersion: lastEval.classifierVersion,
      evaluatedAt: lastEval.evaluatedAt,
      regressions: lastEval.regressions?.length || 0,
      improvements: lastEval.improvements?.length || 0,
    } : null,
    evaluationCount: history.evaluations.length,
  };
}

/**
 * Get evaluation history
 */
export function getEvaluationHistory() {
  return readEvaluationHistory();
}

/**
 * Import golden items from manual corrections (helper for bootstrapping)
 * Only imports items that have been corrected AND verified over time
 */
export function importFromCorrections(corrections, minCorrectionAge = 7) {
  const ageThreshold = minCorrectionAge * 24 * 60 * 60 * 1000; // days to ms
  const now = Date.now();
  let imported = 0;
  
  for (const correction of corrections) {
    const correctionAge = now - new Date(correction.timestamp || 0).getTime();
    
    // Only import if correction is old enough (has stood the test of time)
    if (correctionAge >= ageThreshold) {
      const result = addGoldenItem({
        id: `golden-${correction.targetId}`,
        subject: correction.label || correction.targetId,
        trueType: correction.correctedType,
        addedBy: 'import-from-corrections',
        notes: `Imported from correction: ${correction.originalType} → ${correction.correctedType}`,
        category: 'standard',
      });
      
      if (result.success) imported++;
    }
  }
  
  return { imported, total: corrections.length };
}

export default {
  readGoldenSet,
  addGoldenItem,
  removeGoldenItem,
  updateGoldenItemType,
  evaluateAgainstGoldenSet,
  checkNoRegressionPolicy,
  getGoldenSetFull,
  getGoldenSetSummary,
  getEvaluationHistory,
  importFromCorrections,
};
