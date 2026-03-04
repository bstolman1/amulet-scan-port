/**
 * Correlates raw topics (one per forum post) into lifecycle items
 * (one per governance proposal/entity).
 *
 * Pure business logic — no I/O, no Express, fully testable.
 */

import { entitiesMatch, normalizeEntityName } from './entityMatcher.js';
import {
  WORKFLOW_STAGES,
  ALL_STAGES,
  SIMILARITY_THRESHOLD_OTHER,
  DATE_PROXIMITY_CLOSE_DAYS,
  DATE_PROXIMITY_NEAR_DAYS,
  DATE_PROXIMITY_FAR_DAYS,
  DATE_PROXIMITY_CLOSE_BONUS,
  DATE_PROXIMITY_NEAR_BONUS,
  DATE_PROXIMITY_FAR_BONUS,
  ENTITY_EXACT_SCORE,
  ENTITY_PARTIAL_SCORE,
  APP_NAME_SCORE,
  CIP_EXACT_SCORE,
} from './constants.js';

// ── Type-detection patterns ────────────────────────────────────────────────

const RE_OUTCOME = /\bTokenomics\s+Outcomes\b/i;
const RE_VALIDATOR_OPS = /\bValidator\s+Operations\b/i;
const RE_VOTE_PROPOSAL = /\bVote\s+Proposal\b/i;
const RE_PROTOCOL_UPGRADE =
  /\b(?:synchronizer\s+migration|splice\s+\d+\.\d+|protocol\s+upgrade|network\s+upgrade|hard\s*fork|migration\s+to\s+splice)\b/i;
const RE_VALIDATOR_APPROVED =
  /validator.*approved|approved.*validator|validator\s*operator.*approved/i;
const RE_NETWORK_COLON = /(?:mainnet|testnet|main\s*net|test\s*net):/i;

// ── Similarity scoring ────────────────────────────────────────────────────

/**
 * Calculate a similarity score between two topics for "other" type correlation.
 * @returns {number}
 */
function calculateSimilarity(topic1, topic2) {
  const ids1 = topic1.identifiers;
  const ids2 = topic2.identifiers;
  let score = 0;

  if (ids1.cipNumber && ids1.cipNumber === ids2.cipNumber) score += CIP_EXACT_SCORE;

  const e1 = normalizeEntityName(ids1.entityName);
  const e2 = normalizeEntityName(ids2.entityName);
  if (e1 && e2) {
    if (e1 === e2) score += ENTITY_EXACT_SCORE;
    else if (e1.includes(e2) || e2.includes(e1)) score += ENTITY_PARTIAL_SCORE;
  }

  if (ids1.appName && ids2.appName) {
    const a1 = normalizeEntityName(ids1.appName);
    const a2 = normalizeEntityName(ids2.appName);
    if (a1 === a2) score += APP_NAME_SCORE;
  }

  if (ids1.validatorName && ids2.validatorName) {
    const v1 = normalizeEntityName(ids1.validatorName);
    const v2 = normalizeEntityName(ids2.validatorName);
    if (v1 === v2) score += APP_NAME_SCORE;
  }

  const daysDiff =
    Math.abs(new Date(topic1.date) - new Date(topic2.date)) / 86_400_000;
  if (daysDiff <= DATE_PROXIMITY_CLOSE_DAYS) score += DATE_PROXIMITY_CLOSE_BONUS;
  else if (daysDiff <= DATE_PROXIMITY_NEAR_DAYS) score += DATE_PROXIMITY_NEAR_BONUS;
  else if (daysDiff <= DATE_PROXIMITY_FAR_DAYS) score += DATE_PROXIMITY_FAR_BONUS;

  return score;
}

// ── Type determination ────────────────────────────────────────────────────

/**
 * Determine the governance type for a topic based on its group flow
 * and subject-line heuristics.
 *
 * @param {object} topic - raw topic with .flow, .identifiers, .subject
 * @returns {string} - one of the VALID_TYPES
 */
export function determineTopicType(topic) {
  const ids = topic.identifiers;
  const subject = topic.subject.trim();

  // Learned type override takes highest priority
  if (ids.learnedType) return ids.learnedType;

  if (RE_OUTCOME.test(subject)) return 'outcome';
  if (RE_PROTOCOL_UPGRADE.test(subject)) return 'protocol-upgrade';

  const hasCip = !!ids.cipNumber;
  const hasApp = !!ids.appName;
  const hasValidator = !!ids.validatorName;
  const isValidatorOps = RE_VALIDATOR_OPS.test(subject);
  const isVoteProposal = RE_VOTE_PROPOSAL.test(subject);

  switch (topic.flow) {
    case 'cip':
      if (ids.isCipVoteProposal || hasCip) return 'cip';
      if (ids.isValidatorVoteProposal || ids.isValidator || hasValidator || isValidatorOps) return 'validator';
      if (ids.isFeaturedAppVoteProposal || hasApp) return 'featured-app';
      return 'other';

    case 'featured-app':
      // tokenomics-announce is primarily featured-app, but validator approvals land here too
      return RE_VALIDATOR_APPROVED.test(subject) ? 'validator' : 'featured-app';

    case 'shared': {
      if (ids.isCipVoteProposal || hasCip) return 'cip';
      if (ids.isValidatorVoteProposal || ids.isValidator || isValidatorOps || hasValidator) return 'validator';
      if (ids.isFeaturedAppVoteProposal || hasApp) return 'featured-app';
      if (isVoteProposal) return 'featured-app'; // generic vote = featured-app (most common)
      return 'featured-app';
    }

    default:
      return 'featured-app';
  }
}

/**
 * Determine the governance type for a candidate topic during correlation.
 * Mirrors determineTopicType but used for candidate evaluation.
 */
function determineCandidateType(candidate) {
  return determineTopicType(candidate);
}

// ── Primary ID ───────────────────────────────────────────────────────────

function buildPrimaryId(topic, type) {
  if (type === 'outcome') {
    const d = new Date(topic.date);
    const label = d.toLocaleDateString('en-US', {
      month: 'short', day: 'numeric', year: 'numeric',
    });
    return `Outcomes - ${label}`;
  }
  return topic.identifiers.cipNumber ?? topic.identifiers.entityName ?? topic.subject;
}

// ── Core correlator ───────────────────────────────────────────────────────

/**
 * Group flat topics into lifecycle items.
 *
 * @param {object[]} allTopics
 * @returns {object[]} lifecycle items
 */
export function correlateTopics(allTopics) {
  const items = [];
  const used = new Set();

  // Newest first so we build items from the most recent anchor
  const sorted = [...allTopics].sort(
    (a, b) => new Date(b.date) - new Date(a.date),
  );

  for (const topic of sorted) {
    if (used.has(topic.id)) continue;

    const type = determineTopicType(topic);
    const primaryId = buildPrimaryId(topic, type);
    const entityName = topic.identifiers.entityName;

    const item = {
      id: `lifecycle-${topic.id}`,
      primaryId,
      type,
      network: topic.identifiers.network ?? null,
      stages: { [topic.stage]: [topic] },
      topics: [topic],
      firstDate: topic.date,
      lastDate: topic.date,
      currentStage: topic.stage,
    };

    used.add(topic.id);
    console.log(
      `NEW LIFECYCLE: "${topic.subject.slice(0, 60)}" → type=${type}, primaryId="${primaryId}"`,
    );

    // Outcomes are always standalone
    if (type === 'outcome') {
      items.push(item);
      continue;
    }

    // No entity name (or multi-entity batch) → standalone
    if (
      (type === 'featured-app' || type === 'validator') &&
      (!entityName || topic.identifiers.isMultiEntity)
    ) {
      items.push(item);
      continue;
    }

    // ── Gather related topics ───────────────────────────────────────────
    for (const candidate of sorted) {
      if (used.has(candidate.id) || candidate.id === topic.id) continue;
      if (candidate.identifiers.isMultiEntity) continue;

      let matched = false;

      if (type === 'cip') {
        const tCip = topic.identifiers.cipNumber;
        const cCip = candidate.identifiers.cipNumber;
        matched = !!(tCip && cCip && tCip === cCip);
      } else if (type === 'featured-app' || type === 'validator') {
        const cEntity = candidate.identifiers.entityName;
        if (!cEntity) continue;
        if (determineCandidateType(candidate) !== type) continue;
        matched = entitiesMatch(entityName, cEntity);
        if (matched) {
          console.log(
            `  → Correlating: "${candidate.subject.slice(0, 60)}" (entity="${cEntity}")`,
          );
        }
      } else {
        // 'other' — similarity threshold
        matched = calculateSimilarity(topic, candidate) >= SIMILARITY_THRESHOLD_OTHER;
      }

      if (!matched) continue;

      item.stages[candidate.stage] = item.stages[candidate.stage] ?? [];
      item.stages[candidate.stage].push(candidate);
      item.topics.push(candidate);
      used.add(candidate.id);

      if (new Date(candidate.date) < new Date(item.firstDate)) item.firstDate = candidate.date;
      if (new Date(candidate.date) > new Date(item.lastDate)) item.lastDate = candidate.date;
    }

    // ── Set current stage ───────────────────────────────────────────────
    const stageOrder = WORKFLOW_STAGES[item.type] ?? WORKFLOW_STAGES.other;
    for (const stage of [...stageOrder].reverse()) {
      if (item.stages[stage]?.length > 0) {
        item.currentStage = stage;
        break;
      }
    }

    items.push(item);
  }

  // ── Recover uncorrelated topics ─────────────────────────────────────
  const orphans = sorted.filter(t => !used.has(t.id));
  if (orphans.length > 0) {
    console.log(`⚠️  ${orphans.length} uncorrelated topics → creating standalone items`);
    for (const topic of orphans) {
      items.push({
        id: `lifecycle-${topic.id}`,
        primaryId: topic.identifiers.entityName ?? topic.subject,
        type: 'other',
        network: topic.identifiers.network ?? null,
        stages: { [topic.stage]: [topic] },
        topics: [topic],
        firstDate: topic.date,
        lastDate: topic.date,
        currentStage: topic.stage,
        wasUnaccounted: true,
      });
      used.add(topic.id);
    }
  }

  // ── Sort ─────────────────────────────────────────────────────────────
  items.sort((a, b) => {
    const aIsTBD = a.primaryId?.includes('00XX');
    const bIsTBD = b.primaryId?.includes('00XX');
    if (aIsTBD && !bIsTBD) return -1;
    if (!aIsTBD && bIsTBD) return 1;

    const aNum = a.primaryId?.match(/CIP-?(\d+)/i)?.[1];
    const bNum = b.primaryId?.match(/CIP-?(\d+)/i)?.[1];
    if (aNum && bNum) return parseInt(bNum) - parseInt(aNum);
    if (aNum) return -1;
    if (bNum) return 1;

    return new Date(b.lastDate) - new Date(a.lastDate);
  });

  return items;
}

// ── Post-hoc type fixup (for stale cache) ────────────────────────────────

/**
 * Repair type fields in cached data where the type was mis-serialised.
 * @param {object} data
 * @returns {object}
 */
export function fixLifecycleItemTypes(data) {
  if (!data?.lifecycleItems) return data;

  for (const item of data.lifecycleItems) {
    if (!item.primaryId) continue;
    const pid = item.primaryId.toUpperCase();
    if (pid.includes('CIP-') || pid.startsWith('CIP')) {
      item.type = 'cip';
    } else if (item.type === 'other') {
      const hasApp = item.topics?.some(t => t.identifiers?.appName);
      const hasValidator = item.topics?.some(t => t.identifiers?.validatorName);
      if (hasApp) item.type = 'featured-app';
      else if (hasValidator) item.type = 'validator';
    }
  }

  return data;
}
