/**
 * Classification improvement analyser.
 *
 * Analyses manual corrections (from the audit log and overrides) to generate
 * actionable suggestions for improving the classification rules.
 *
 * Pure business logic: no I/O, no Express. All functions are testable.
 */

import {
  SUGGESTION_MIN_CORRECTIONS,
  KEYWORD_SIGNIFICANCE_RATIO,
  KEYWORD_MIN_OCCURRENCES,
  HIGH_PRIORITY_THRESHOLD,
  INSTRUCTIONAL_SUGGESTION_THRESHOLD,
} from '../utils/constants.js';

// ── Correction analysis ───────────────────────────────────────────────────

/**
 * Analyse a list of corrections to identify systematic mis-classification
 * patterns and the keywords that distinguish them.
 *
 * @param {{ label: string, originalType: string, correctedType: string }[]} corrections
 * @returns {object} patternAnalysis
 */
export function analyzeCorrections(corrections) {
  const byOriginalType = {};
  const byCorrectedType = {};
  const typeTransitions = {};
  const wordsByTransition = {};

  for (const correction of corrections) {
    const orig = correction.originalType ?? 'unknown';
    const corr = correction.correctedType;
    const label = correction.label ?? '';

    byOriginalType[orig] = (byOriginalType[orig] ?? 0) + 1;
    byCorrectedType[corr] = (byCorrectedType[corr] ?? 0) + 1;

    const transition = `${orig} → ${corr}`;
    typeTransitions[transition] ??= { count: 0, examples: [] };
    typeTransitions[transition].count++;
    if (typeTransitions[transition].examples.length < 5) {
      typeTransitions[transition].examples.push(label.slice(0, 100));
    }

    // Extract keywords
    const words = label
      .toLowerCase()
      .replace(/[^a-z0-9\s]/g, ' ')
      .split(/\s+/)
      .filter(w => w.length > 2);

    wordsByTransition[transition] ??= {};
    for (const word of words) {
      wordsByTransition[transition][word] =
        (wordsByTransition[transition][word] ?? 0) + 1;
    }
  }

  // Find keywords that appear frequently within each transition
  const patterns = [];
  for (const [transition, wordCounts] of Object.entries(wordsByTransition)) {
    const total = typeTransitions[transition]?.count ?? 1;
    const significantWords = Object.entries(wordCounts)
      .filter(
        ([, count]) =>
          count >= KEYWORD_MIN_OCCURRENCES &&
          count >= total * KEYWORD_SIGNIFICANCE_RATIO,
      )
      .map(([word, count]) => ({ word, frequency: count / total }))
      .sort((a, b) => b.frequency - a.frequency)
      .slice(0, 10);

    if (significantWords.length > 0) {
      patterns.push({
        transition,
        transitionCount: total,
        keywords: significantWords,
        examples: typeTransitions[transition]?.examples ?? [],
      });
    }
  }

  patterns.sort((a, b) => b.transitionCount - a.transitionCount);

  return { byOriginalType, byCorrectedType, typeTransitions, patterns };
}

// ── Confidence scoring ────────────────────────────────────────────────────

function scoreConfidence(pattern) {
  const count = pattern.transitionCount;
  const avgFreq =
    pattern.keywords.reduce((s, k) => s + k.frequency, 0) / pattern.keywords.length;
  const uniqueEntities = new Set(
    pattern.examples.map(e => e.toLowerCase().replace(/[^a-z]/g, '').slice(0, 20)),
  ).size;

  let level = 'edge-case';
  let description = 'Rare pattern, may be brittle';
  if (count >= 5 && avgFreq > 0.7 && uniqueEntities >= 3) {
    level = 'general';
    description = 'Strong pattern seen across multiple entities/flows';
  } else if (count >= 2 && avgFreq > 0.5) {
    level = 'contextual';
    description = 'Pattern specific to certain lifecycle types';
  }

  return { level, description, sourceCount: count, avgKeywordMatch: avgFreq, uniqueEntities };
}

// ── Keyword filtering ─────────────────────────────────────────────────────

const GENERIC_STOP_WORDS = new Set([
  'the', 'and', 'for', 'new', 'this', 'that', 'with', 'from',
  'have', 'has', 'are', 'was', 'were', 'been',
]);

/**
 * Filter out keywords that already exist in a pattern set or are generic.
 */
function filterNewKeywords(keywords, existingKeywords, builtinExclude) {
  const existing = new Set([
    ...(existingKeywords ?? []).map(k => (typeof k === 'string' ? k : k.keyword).toLowerCase()),
    ...builtinExclude.map(k => k.toLowerCase()),
    ...GENERIC_STOP_WORDS,
  ]);

  const filtered = keywords.filter(k => !existing.has(k.toLowerCase()));

  if (keywords.length > 0 && filtered.length === 0) {
    console.warn('⚠️  All keywords filtered:', { original: keywords.slice(0, 5) });
  }

  return filtered.length > 0 ? filtered : keywords.slice(0, 5);
}

// ── Prompt text builders ──────────────────────────────────────────────────

function patternPromptAddition(origType, corrType, keywords, examples) {
  return `
**Disambiguation: ${origType} vs ${corrType}**
- When subject contains: ${keywords.slice(0, 5).join(', ')}
- Classify as **${corrType}**, not ${origType}
- Examples: ${examples.slice(0, 2).map(e => `"${e}"`).join(', ')}`;
}

const TYPE_DEFINITIONS = {
  validator: 'entities operating network infrastructure (nodes, validators, super validators)',
  'featured-app': 'applications seeking or maintaining featured status on the network',
  cip: 'Canton Improvement Proposals (CIP-XXXX format) for protocol changes',
  'protocol-upgrade': 'network-wide upgrades, migrations, or infrastructure changes',
  outcome: 'monthly reports, tokenomics outcomes, or periodic summaries',
  other: 'items that do not fit any specific governance category',
};

function instructionalPromptAddition(origType, corrType) {
  return `
**Definition clarification for ${corrType}:**
${corrType}: ${TYPE_DEFINITIONS[corrType] ?? 'See category definition'}

Key distinction from ${origType}: Focus on the primary governance action, not incidental mentions.`;
}

// ── Main entry point ──────────────────────────────────────────────────────

const SCOPE = {
  applies: 'future_only',
  retroactive: false,
  description: 'Applies to future classifications only. Existing items unchanged.',
};

/**
 * Generate improvement suggestions from analysed patterns.
 *
 * @param {object} analysis - from analyzeCorrections()
 * @param {object|null} currentPatterns - existing learned patterns for deduplication
 * @returns {object[]} suggestions sorted by priority
 */
export function generateImprovementSuggestions(analysis, currentPatterns = null) {
  const suggestions = [];

  for (const pattern of analysis.patterns) {
    if (pattern.transitionCount < SUGGESTION_MIN_CORRECTIONS) continue;

    const [origType, corrType] = pattern.transition.split(' → ');
    const keywords = pattern.keywords.map(k => k.word);
    const confidence = scoreConfidence(pattern);
    const priority = pattern.transitionCount >= HIGH_PRIORITY_THRESHOLD ? 'high' : 'medium';
    const provenance = {
      sourceCorrections: pattern.transitionCount,
      affectedEntities: pattern.examples.slice(0, 5),
      transition: pattern.transition,
    };

    // ── Rule-based suggestions (governance-lifecycle.js) ──────────────
    const keywordTargets = {
      validator: { existingKey: 'validatorKeywords', builtin: ['validator', 'operator'], target: 'VALIDATOR_KEYWORDS' },
      'featured-app': { existingKey: 'featuredAppKeywords', builtin: ['featured', 'app', 'application'], target: 'FEATURED_APP_KEYWORDS' },
      cip: { existingKey: 'cipKeywords', builtin: ['cip', 'proposal'], target: 'CIP_KEYWORDS' },
      'protocol-upgrade': { existingKey: 'protocolUpgradeKeywords', builtin: ['upgrade', 'splice', 'migration'], target: 'PROTOCOL_UPGRADE_KEYWORDS' },
      outcome: { existingKey: 'outcomeKeywords', builtin: ['outcome', 'tokenomics', 'report'], target: 'OUTCOME_KEYWORDS' },
    };

    const targetSpec = keywordTargets[corrType];
    if (targetSpec) {
      const newKws = filterNewKeywords(keywords, currentPatterns?.[targetSpec.existingKey], targetSpec.builtin);
      suggestions.push({
        file: 'governance-lifecycle.js',
        location: `extractIdentifiers/${corrType} detection`,
        type: 'add_keyword',
        priority,
        description: `Add keywords to ${corrType} detection: ${newKws.slice(0, 3).join(', ')}`,
        keywords: newKws,
        codeChange: { target: targetSpec.target, action: 'add', values: newKws },
        examples: pattern.examples,
        reason: `${pattern.transitionCount} items misclassified as ${origType} were actually ${corrType}`,
        confidence,
        scope: SCOPE,
        provenance,
        learningLayer: 'pattern',
      });
    }

    // ── LLM prompt suggestions ────────────────────────────────────────
    if (pattern.transitionCount >= 2) {
      suggestions.push({
        file: 'llm-classifier.js',
        location: 'CLASSIFICATION_PROMPT',
        type: 'prompt_enhancement',
        priority,
        description: `Add ${corrType} disambiguation examples to LLM prompt`,
        keywords,
        promptAddition: patternPromptAddition(origType, corrType, keywords, pattern.examples),
        examples: pattern.examples,
        reason: `${pattern.transitionCount} items misclassified as ${origType} were actually ${corrType}`,
        confidence,
        scope: SCOPE,
        provenance,
        learningLayer: 'pattern',
        promptType: 'example_injection',
      });

      if (pattern.transitionCount >= INSTRUCTIONAL_SUGGESTION_THRESHOLD) {
        suggestions.push({
          file: 'llm-classifier.js',
          location: 'CLASSIFICATION_PROMPT definitions',
          type: 'prompt_enhancement',
          priority: 'medium',
          description: `Clarify ${corrType} vs ${origType} definition boundary`,
          promptAddition: instructionalPromptAddition(origType, corrType),
          examples: pattern.examples,
          reason: `High volume of ${origType} → ${corrType} corrections suggests unclear definition`,
          confidence: { ...confidence, level: 'contextual' },
          scope: SCOPE,
          provenance,
          learningLayer: 'instructional',
          promptType: 'definition_clarification',
        });
      }
    }
  }

  const priorityOrder = { high: 0, medium: 1, low: 2 };
  suggestions.sort((a, b) => {
    const pDiff = priorityOrder[a.priority] - priorityOrder[b.priority];
    if (pDiff !== 0) return pDiff;
    return (b.provenance?.sourceCorrections ?? 0) - (a.provenance?.sourceCorrections ?? 0);
  });

  return suggestions;
}

// ── Learned pattern generation ────────────────────────────────────────────

/**
 * Build a set of learned patterns from manual corrections.
 *
 * @param {{ label: string, correctedType: string }[]} corrections
 * @param {object|null} existingPatterns - patterns to merge into
 * @returns {object} new patterns object
 */
export function generateLearnedPatterns(corrections, existingPatterns = null) {
  const now = new Date().toISOString();
  const wordCounts = {
    validator: {}, 'featured-app': {}, cip: {},
    'protocol-upgrade': {}, outcome: {},
  };
  const entityMappings = {};

  for (const { label = '', correctedType } of corrections) {
    const words = label
      .toLowerCase()
      .replace(/[^a-z0-9\s]/g, ' ')
      .split(/\s+/)
      .filter(w => w.length > 2 && !['the', 'and', 'for', 'new', 'vote', 'proposal'].includes(w));

    if (wordCounts[correctedType]) {
      for (const w of words) {
        wordCounts[correctedType][w] = (wordCounts[correctedType][w] ?? 0) + 1;
      }
    }

    const entityMatch = label.match(/^([A-Z][A-Za-z0-9\s-]+?)(?:\s*[-:]|$)/);
    if (entityMatch) {
      const name = entityMatch[1].trim().toLowerCase();
      if (name.length > 2) {
        entityMappings[name] = {
          type: correctedType,
          confidence: 1.0,
          createdAt: now,
          lastReinforced: now,
          sourceCount: 1,
        };
      }
    }
  }

  const createArray = type => {
    const total = corrections.filter(c => c.correctedType === type).length;
    return Object.entries(wordCounts[type] ?? {}).map(([keyword, count]) => ({
      keyword,
      confidence: Math.min(1.0, count / Math.max(1, total) + 0.5),
      createdAt: now,
      lastReinforced: now,
      matchCount: 0,
      sourceCount: count,
    }));
  };

  const mergeArrays = (newArr, existing) => {
    if (!existing) return newArr;
    const map = new Map(
      existing.map(p => [typeof p === 'string' ? p : p.keyword, p]),
    );
    for (const entry of newArr) {
      const old = map.get(entry.keyword);
      if (old && typeof old === 'object') {
        entry.confidence = Math.min(1.0, (old.confidence ?? 0.8) + 0.1);
        entry.createdAt = old.createdAt ?? now;
        entry.matchCount = old.matchCount ?? 0;
        entry.lastReinforced = now;
      }
      map.set(entry.keyword, entry);
    }
    return Array.from(map.values());
  };

  return {
    validatorKeywords: mergeArrays(createArray('validator'), existingPatterns?.validatorKeywords),
    featuredAppKeywords: mergeArrays(createArray('featured-app'), existingPatterns?.featuredAppKeywords),
    cipKeywords: mergeArrays(createArray('cip'), existingPatterns?.cipKeywords),
    protocolUpgradeKeywords: mergeArrays(createArray('protocol-upgrade'), existingPatterns?.protocolUpgradeKeywords),
    outcomeKeywords: mergeArrays(createArray('outcome'), existingPatterns?.outcomeKeywords),
    entityNameMappings: { ...(existingPatterns?.entityNameMappings ?? {}), ...entityMappings },
  };
}

// ── Classifier simulation (for impact preview / testing) ─────────────────

/**
 * Simulate the rule-based classifier outcome for a subject string,
 * given a set of learned patterns.
 *
 * @param {string} subject
 * @param {object} patterns
 * @returns {string} - one of VALID_TYPES
 */
export function simulateClassification(subject, patterns) {
  if (!subject) return 'other';
  const lower = subject.toLowerCase();

  if (/CIP\s*[-#]?\s*\d+/i.test(subject) || /^\s*0*\d{4}\s+/.test(subject)) return 'cip';

  const checkKeywords = (keywords) =>
    (keywords ?? []).some(kw => {
      const word = typeof kw === 'string' ? kw : kw.keyword;
      return lower.includes(word.toLowerCase());
    });

  if (checkKeywords(patterns?.validatorKeywords)) return 'validator';
  if (checkKeywords(patterns?.featuredAppKeywords)) return 'featured-app';
  if (checkKeywords(patterns?.cipKeywords)) return 'cip';
  if (checkKeywords(patterns?.protocolUpgradeKeywords)) return 'protocol-upgrade';
  if (checkKeywords(patterns?.outcomeKeywords)) return 'outcome';

  for (const [entity, meta] of Object.entries(patterns?.entityNameMappings ?? {})) {
    if (lower.includes(entity.toLowerCase())) return meta.type ?? meta;
  }

  // Fallback built-in regexes
  if (/validator\s*(?:approved|operator|onboarding|license)|super\s*validator|node\s+as\s+a\s+service/i.test(subject)) return 'validator';
  if (/featured\s*app|featured\s*application|app\s+(?:listing|request|tokenomics)/i.test(subject)) return 'featured-app';
  if (/splice\s+\d|upgrade|migration/i.test(subject)) return 'protocol-upgrade';
  if (/tokenomics\s+outcome|monthly\s+report/i.test(subject)) return 'outcome';

  return 'other';
}
