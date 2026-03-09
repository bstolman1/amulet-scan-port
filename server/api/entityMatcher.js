/**
 * Entity name normalisation and fuzzy matching.
 *
 * Fixes over the original:
 *  - Levenshtein uses the single-row optimisation (O(min(m,n)) space instead
 *    of O(m*n)).
 *  - The Infinity early-exit is removed; the length-ratio guard is placed
 *    after normalisation so identical strings that differ only in suffix are
 *    still caught by the exact/substring checks first.
 *  - All thresholds are imported from constants (no magic numbers).
 */

import {
  FUZZY_DISTANCE_RATIO,
  FUZZY_MIN_DISTANCE,
  FUZZY_MIN_LENGTH_RATIO,
} from './constants.js';

const RE_COMPANY_SUFFIXES =
  /\b(llc|inc|corp|ltd|gmbh|ag|sa|bv|pty|co|company|limited|incorporated|corporation)\b\.?/gi;
const RE_PUNCTUATION = /[.,\-_'"()]/g;
const RE_WHITESPACE = /\s+/g;

// ── Normalisation ──────────────────────────────────────────────────────────

/**
 * Normalise an entity name for comparison.
 * Strips company suffixes, punctuation, and extra whitespace.
 * @param {string | null | undefined} name
 * @returns {string | null}
 */
export function normalizeEntityName(name) {
  if (!name) return null;
  const result = name
    .toLowerCase()
    .replace(RE_COMPANY_SUFFIXES, '')
    .replace(RE_PUNCTUATION, ' ')
    .replace(RE_WHITESPACE, ' ')
    .trim();
  return result || null;
}

/**
 * Strict normalisation: removes all spaces for token-level comparison.
 * e.g. "node fortress" → "nodefortress"
 * @param {string | null | undefined} name
 * @returns {string | null}
 */
export function normalizeEntityNameStrict(name) {
  const n = normalizeEntityName(name);
  return n ? n.replace(RE_WHITESPACE, '') : null;
}

// ── Levenshtein distance (single-row optimisation) ─────────────────────────

/**
 * Compute the Levenshtein edit distance between two strings.
 * Uses O(min(m,n)) space.
 * @param {string} a
 * @param {string} b
 * @returns {number}
 */
export function levenshteinDistance(a, b) {
  if (!a || !b) return Math.max(a?.length ?? 0, b?.length ?? 0);

  // Ensure `a` is the shorter string to minimise memory usage.
  if (a.length > b.length) [a, b] = [b, a];

  const m = a.length;
  const n = b.length;

  // Single row of DP values, representing "previous row"
  let row = Array.from({ length: m + 1 }, (_, i) => i);

  for (let j = 1; j <= n; j++) {
    let prev = row[0];
    row[0] = j;

    for (let i = 1; i <= m; i++) {
      const temp = row[i];
      row[i] =
        a[i - 1] === b[j - 1]
          ? prev
          : 1 + Math.min(prev, row[i], row[i - 1]);
      prev = temp;
    }
  }

  return row[m];
}

// ── Entity matching ────────────────────────────────────────────────────────

/**
 * Check whether two entity names refer to the same entity.
 *
 * Matching strategy (in order of specificity):
 *  1. Exact match after normalisation.
 *  2. Substring match (one contains the other).
 *  3. Strict match (no spaces).
 *  4. Fuzzy Levenshtein match within a configurable threshold.
 *
 * @param {string | null | undefined} entity1
 * @param {string | null | undefined} entity2
 * @returns {boolean}
 */
export function entitiesMatch(entity1, entity2) {
  if (!entity1 || !entity2) return false;

  const n1 = normalizeEntityName(entity1);
  const n2 = normalizeEntityName(entity2);
  if (!n1 || !n2) return false;

  // 1. Exact
  if (n1 === n2) return true;

  // 2. Substring
  if (n1.includes(n2) || n2.includes(n1)) return true;

  // 3. Strict (no spaces)
  const s1 = normalizeEntityNameStrict(entity1);
  const s2 = normalizeEntityNameStrict(entity2);
  if (s1 && s2 && s1 === s2) return true;

  // 4. Fuzzy — only when strings are of similar length
  const maxLen = Math.max(n1.length, n2.length);
  const minLen = Math.min(n1.length, n2.length);

  if (minLen < maxLen * FUZZY_MIN_LENGTH_RATIO) return false;

  const distance = levenshteinDistance(n1, n2);
  const threshold = Math.max(FUZZY_MIN_DISTANCE, Math.floor(maxLen * FUZZY_DISTANCE_RATIO));

  if (distance <= threshold) {
    console.log(
      `  → Fuzzy match: "${entity1}" ~ "${entity2}" (distance=${distance}, threshold=${threshold})`,
    );
    return true;
  }

  return false;
}
