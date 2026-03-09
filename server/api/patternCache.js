/**
 * In-memory cache for learned patterns with async file backing.
 *
 * Fixes:
 *  - No synchronous fs calls.
 *  - No manual mtime tracking / race conditions.
 *  - Atomic write via temp-file rename in the repository layer.
 *  - Batch writes: reinforcePattern accumulates mutations and flushes once.
 */

import {
  readLearnedPatternsFile,
  writeLearnedPatternsFile,
} from '../repositories/fileRepository.js';
import {
  DECAY_HALF_LIFE_DAYS,
  MIN_CONFIDENCE,
  REINFORCEMENT_BOOST,
  SURVIVAL_REINFORCEMENT_MULTIPLIER,
  REINFORCEMENT_LOG_MAX,
} from '../utils/constants.js';

// ── Module-level singleton ─────────────────────────────────────────────────

/** @type {{ data: LearnedPatternsFile | null, dirty: boolean }} */
let _state = { data: null, dirty: false };

/** @returns {Promise<import('../repositories/fileRepository.js').LearnedPatternsFile | null>} */
async function getFileData() {
  if (_state.data === null) {
    _state.data = await readLearnedPatternsFile();
  }
  return _state.data;
}

/** Force the cache to reload from disk on next access. */
export function invalidatePatternCache() {
  _state = { data: null, dirty: false };
}

// ── Public API ─────────────────────────────────────────────────────────────

/**
 * Return the active patterns object, or null if no patterns exist.
 * @returns {Promise<object | null>}
 */
export async function getLearnedPatterns() {
  const file = await getFileData();
  return file?.patterns ?? null;
}

/**
 * Return the full patterns file (includes version, history, etc.)
 * @returns {Promise<LearnedPatternsFile | null>}
 */
export async function getLearnedPatternsFile() {
  return getFileData();
}

/**
 * Replace the patterns file entirely and flush to disk.
 * @param {LearnedPatternsFile} newData
 */
export async function saveLearnedPatternsFile(newData) {
  await writeLearnedPatternsFile(newData);
  _state = { data: newData, dirty: false };
  console.log(`📚 Patterns v${newData.version} saved to disk`);
}

// ── Confidence decay helpers ───────────────────────────────────────────────

/**
 * Calculate the current confidence of a pattern, applying time decay.
 * @param {{ confidence?: number, createdAt?: string, lastReinforced?: string }} pattern
 * @returns {number}
 */
export function calculatePatternConfidence(pattern) {
  const createdAt = pattern.createdAt
    ? new Date(pattern.createdAt).getTime()
    : Date.now();
  const lastReinforced = pattern.lastReinforced
    ? new Date(pattern.lastReinforced).getTime()
    : createdAt;

  const daysSinceReinforcement =
    (Date.now() - lastReinforced) / (1000 * 60 * 60 * 24);
  const decayFactor = Math.pow(0.5, daysSinceReinforcement / DECAY_HALF_LIFE_DAYS);
  const baseConfidence = pattern.confidence ?? 1.0;

  return Math.max(MIN_CONFIDENCE, baseConfidence * decayFactor);
}

/** @param {{ confidence?: number, createdAt?: string, lastReinforced?: string }} pattern */
export function shouldArchivePattern(pattern) {
  return calculatePatternConfidence(pattern) < MIN_CONFIDENCE * 2;
}

// ── Keyword matching ───────────────────────────────────────────────────────

/**
 * Returns true if text contains any of the learned keywords.
 * Supports both plain string arrays and object arrays { keyword: string }.
 */
export function matchesLearnedKeywords(text, keywords) {
  if (!keywords?.length) return false;
  const lower = text.toLowerCase();
  return keywords.some(kw => {
    const word = typeof kw === 'string' ? kw : kw.keyword;
    return lower.includes(word.toLowerCase());
  });
}

// ── Reinforcement ─────────────────────────────────────────────────────────

/** Map from pattern array key to keyword string -> accumulated boost */
const _pendingReinforcements = new Map(); // key: `${arrayKey}::${keyword}`

const ARRAY_KEY_MAP = {
  validator: 'validatorKeywords',
  'featured-app': 'featuredAppKeywords',
  cip: 'cipKeywords',
  'protocol-upgrade': 'protocolUpgradeKeywords',
  outcome: 'outcomeKeywords',
};

/**
 * Queue a reinforcement boost for a pattern keyword.
 * Call flushReinforcements() to persist in a single write.
 *
 * @param {'validator'|'featured-app'|'cip'|'protocol-upgrade'|'outcome'} patternType
 * @param {string} keyword
 * @param {'match'|'survival'} source
 */
export function queueReinforcement(patternType, keyword, source = 'match') {
  const arrayKey = ARRAY_KEY_MAP[patternType];
  if (!arrayKey) return;

  const boost =
    source === 'survival'
      ? REINFORCEMENT_BOOST * SURVIVAL_REINFORCEMENT_MULTIPLIER
      : REINFORCEMENT_BOOST;

  const mapKey = `${arrayKey}::${keyword}`;
  _pendingReinforcements.set(mapKey, (_pendingReinforcements.get(mapKey) ?? 0) + boost);
}

/**
 * Apply all queued reinforcements in a single async file write.
 * @returns {Promise<number>} Number of patterns reinforced.
 */
export async function flushReinforcements() {
  if (_pendingReinforcements.size === 0) return 0;

  const file = await getFileData();
  if (!file?.patterns) {
    _pendingReinforcements.clear();
    return 0;
  }

  const now = new Date().toISOString();
  let count = 0;

  for (const [mapKey, boost] of _pendingReinforcements) {
    const [arrayKey, keyword] = mapKey.split('::');
    const arr = file.patterns[arrayKey];
    if (!Array.isArray(arr)) continue;

    const pattern = arr.find(
      p => (typeof p === 'string' ? p : p.keyword) === keyword,
    );
    if (!pattern || typeof pattern !== 'object') continue;

    pattern.lastReinforced = now;
    pattern.matchCount = (pattern.matchCount ?? 0) + 1;
    pattern.confidence = Math.min(1.0, (pattern.confidence ?? 0.8) + boost);

    pattern.reinforcementLog = pattern.reinforcementLog ?? [];
    pattern.reinforcementLog.push({
      timestamp: now,
      source: mapKey,
      newConfidence: pattern.confidence,
    });
    if (pattern.reinforcementLog.length > REINFORCEMENT_LOG_MAX) {
      pattern.reinforcementLog = pattern.reinforcementLog.slice(-REINFORCEMENT_LOG_MAX);
    }
    count++;
  }

  _pendingReinforcements.clear();

  if (count > 0) {
    await writeLearnedPatternsFile(file);
    console.log(`📈 Flushed ${count} pattern reinforcements`);
  }

  return count;
}

/**
 * Queue reinforcements for all patterns that survived a classification cycle
 * (i.e. were not manually corrected).
 *
 * @param {object[]} classifiedItems - lifecycle items
 * @param {Set<string>} correctedIds - IDs of items that were corrected
 * @returns {Promise<{ queued: number, flushed: number }>}
 */
export async function reinforceSurvivingPatterns(classifiedItems, correctedIds) {
  const patterns = await getLearnedPatterns();
  if (!patterns) return { queued: 0, flushed: 0 };

  let queued = 0;

  for (const item of classifiedItems) {
    if (correctedIds.has(item.id) || correctedIds.has(item.primaryId)) continue;

    const subject = (item.primaryId ?? item.subject ?? '').toLowerCase();

    const checkAndQueue = (keywords, type) => {
      if (!keywords) return;
      for (const kw of keywords) {
        const keyword = typeof kw === 'string' ? kw : kw.keyword;
        if (subject.includes(keyword.toLowerCase())) {
          queueReinforcement(type, keyword, 'survival');
          queued++;
        }
      }
    };

    if (item.type === 'validator') checkAndQueue(patterns.validatorKeywords, 'validator');
    else if (item.type === 'featured-app') checkAndQueue(patterns.featuredAppKeywords, 'featured-app');
    else if (item.type === 'cip') checkAndQueue(patterns.cipKeywords, 'cip');
    else if (item.type === 'protocol-upgrade') checkAndQueue(patterns.protocolUpgradeKeywords, 'protocol-upgrade');
    else if (item.type === 'outcome') checkAndQueue(patterns.outcomeKeywords, 'outcome');
  }

  const flushed = await flushReinforcements();
  return { queued, flushed };
}
