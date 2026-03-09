/**
 * Entity extraction and identifier parsing.
 *
 * All functions are pure (no I/O, no side effects) so they are trivially
 * unit-testable.  Regex patterns are defined as named constants at the top
 * so they are compiled once and easy to audit.
 */

// ── Regex constants ────────────────────────────────────────────────────────

const RE_STRIP_PREFIX = /^(re:|fwd:|fw:)\s*/i;
const RE_STRIP_VOTE_SUFFIX = /\s*-?\s*vote\s*(proposal)?\s*$/i;
const RE_BATCH_APPROVAL = /(?:validator\s*operators?|validators|featured\s*apps?)\s*approved[:\s-]+(.+?)$/i;
const RE_NAAS = /^(node\s+as\s+a\s+service)\s+(?:reviewed\s+and\s+)?approved/i;
const RE_VALIDATOR_APPROVED = /validator\s*(?:approved|operator\s+approved)[:\s-]+(.+?)$/i;
const RE_FEATURED_APPROVED = /featured\s*app\s*approved[:\s-]+(.+?)$/i;
const RE_TO_FEATURE = /to\s+feature\s+(?:the\s+)?([A-Za-z0-9][\w\s-]*?)$/i;
const RE_FOR_APP = /(?:to\s+)?(?:implement|apply|for)\s+featured\s*(?:app(?:lication)?|application)\s+(?:status|rights)\s+for\s+([A-Za-z0-9][\w\s-]*?)$/i;
const RE_NETWORK_APP = /(?:mainnet|testnet|main\s*net|test\s*net)[:\s]+([^:]+?)(?:\s+by\s+.+)?$/i;
const RE_REQUEST = /(?:request|proposal|onboarding|tokenomics|announce|announcement|approved|vote\s*proposal)[:\s-]+(.+?)$/i;
const RE_NEW_REQUEST = /new\s+featured\s*app\s+request[:\s-]+(.+?)$/i;
const RE_PREFIX_MATCH = /^([A-Za-z][A-Za-z0-9\s-]{1,30}?)\s+(?:featured\s*app|super\s*validator|validator|tokenomics|onboarding)/i;
const RE_BRACKET = /\[([^\]]+)\]/;
const RE_CAPITALIZED = /([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)/;
const RE_FOR_SINGLE = /for\s+([A-Z][A-Za-z0-9-]+)$/;
const RE_NETWORK_PREFIX = /^(?:mainnet|testnet|main\s*net|test\s*net)[:\s]+/i;
const RE_BY_SUFFIX = /\s+by\s+.*$/i;
const RE_MULTI_ENTITY = /,|\s+and\s+/i;

// CIP extraction
const RE_CIP_TBD = /CIP\s*[-#]?\s*(TBD|00XX|XXXX|\?\?|unassigned)/i;
const RE_CIP_NUMBER = /CIP\s*[-#]?\s*(\d{2,})/i;
const RE_CIP_STANDALONE = /^\s*0*(\d{4})\s+/;

// Network
const RE_TESTNET = /testnet|test\s*net|tn\b/i;
const RE_MAINNET = /mainnet|main\s*net|mn\b/i;

// Type indicators
const RE_CIP_DISCUSSION = /^\s*(?:re:\s*)?CIP[#\-\s]|^(?:re:\s*)?(?:new\s+)?CIP\s+(?:discuss|proposal|draft)/i;
const RE_VOTE_PROPOSAL = /\bVote\s+Proposal\b/i;
const RE_CIP_VOTE = /CIP[#\-\s]?\d+|CIP\s+(?:vote|voting|approval)/i;
const RE_FEATURED_APP = /featured\s*app|featured\s*application|app\s+(?:application|listing|request|tokenomics|vote|approved)|application\s+status\s+for/i;
const RE_FEATURED_APP_VOTE = /featured\s*app|featured\s*application|app\s+rights/i;
const RE_NETWORK_COLON = /(?:mainnet|testnet|main\s*net|test\s*net):/i;
const RE_VALIDATOR = /super\s*validator|validator\s+(?:approved|application|onboarding|license|candidate|operator\s+approved)|sv\s+(?:application|onboarding)|validator\s+operator|node\s+as\s+a\s+service|validator\s+operators\s+approved/i;
const RE_VALIDATOR_VOTE = /validator\s+(?:operator|onboarding|license)/i;

const RESERVED_ENTITY_NAMES = new Set([
  'featured app', 'super validator', 'vote proposal', 'new request',
  'token economics', 'main net', 'test net',
]);

const GENERIC_ENTITY_WORDS = new Set([
  'the', 'and', 'for', 'this', 'that', 'with', 'from',
  'featured', 'app', 'vote', 'proposal', 'to',
]);

// ── Entity name extraction ─────────────────────────────────────────────────

/**
 * Extract the primary entity name (app or validator name) from a subject line.
 * @param {string} text
 * @returns {{ name: string | null, isMultiEntity: boolean }}
 */
export function extractPrimaryEntityName(text) {
  if (!text) return { name: null, isMultiEntity: false };

  let clean = text
    .replace(RE_STRIP_PREFIX, '')
    .replace(RE_STRIP_VOTE_SUFFIX, '')
    .trim();

  // Batch approvals: "Validators Approved: A, B, C"
  const batchMatch = clean.match(RE_BATCH_APPROVAL);
  if (batchMatch) {
    const content = batchMatch[1].trim();
    if (RE_MULTI_ENTITY.test(content)) {
      return { name: null, isMultiEntity: true };
    }
  }

  // "Node as a Service" pattern
  if (RE_NAAS.test(clean)) {
    return { name: 'Node as a Service', isMultiEntity: false };
  }

  // "Validator Approved: EntityName"
  const validatorApprovedMatch = clean.match(RE_VALIDATOR_APPROVED);
  if (validatorApprovedMatch) {
    const name = validatorApprovedMatch[1].trim();
    if (RE_MULTI_ENTITY.test(name)) return { name: null, isMultiEntity: true };
    if (name.length > 1) return { name, isMultiEntity: false };
  }

  // "Featured App Approved: AppName"
  const featuredApprovedMatch = clean.match(RE_FEATURED_APPROVED);
  if (featuredApprovedMatch) {
    const name = featuredApprovedMatch[1].trim();
    if (RE_MULTI_ENTITY.test(name)) return { name: null, isMultiEntity: true };
    if (name.length > 1) return { name, isMultiEntity: false };
  }

  // "to Feature AppName"
  const toFeatureMatch = clean.match(RE_TO_FEATURE);
  if (toFeatureMatch) {
    const name = toFeatureMatch[1].replace(RE_BY_SUFFIX, '').trim();
    if (name.length > 1) return { name, isMultiEntity: false };
  }

  // "for Featured Application status for AppName"
  const forAppMatch = clean.match(RE_FOR_APP);
  if (forAppMatch) {
    const name = forAppMatch[1].trim();
    if (name.length > 1) return { name, isMultiEntity: false };
  }

  // "MainNet: AppName by Company"
  const networkAppMatch = clean.match(RE_NETWORK_APP);
  if (networkAppMatch) {
    const name = networkAppMatch[1].replace(RE_BY_SUFFIX, '').trim();
    if (name.length > 2) return { name, isMultiEntity: false };
  }

  // "Request: AppName" / "Approved: AppName"
  const requestMatch = clean.match(RE_REQUEST);
  if (requestMatch) {
    const name = requestMatch[1]
      .replace(RE_NETWORK_PREFIX, '')
      .replace(RE_BY_SUFFIX, '')
      .trim();
    if (name.length > 2 && !/^(the|this|new|our)$/i.test(name)) {
      return { name, isMultiEntity: false };
    }
  }

  // "New Featured App Request: AppName"
  const newRequestMatch = clean.match(RE_NEW_REQUEST);
  if (newRequestMatch) {
    const name = newRequestMatch[1]
      .replace(RE_NETWORK_PREFIX, '')
      .replace(RE_BY_SUFFIX, '')
      .trim();
    if (name.length > 2) return { name, isMultiEntity: false };
  }

  // "AppName Featured App Tokenomics"
  const prefixMatch = clean.match(RE_PREFIX_MATCH);
  if (prefixMatch) {
    const name = prefixMatch[1].trim();
    if (name.length > 1 && !GENERIC_ENTITY_WORDS.has(name.toLowerCase())) {
      return { name, isMultiEntity: false };
    }
  }

  // "[AppName]"
  const bracketMatch = clean.match(RE_BRACKET);
  if (bracketMatch) {
    const name = bracketMatch[1].trim();
    if (name.length > 2) return { name, isMultiEntity: false };
  }

  // Capitalized multi-word name
  const capitalizedMatch = clean.match(RE_CAPITALIZED);
  if (capitalizedMatch) {
    const name = capitalizedMatch[1].trim();
    if (!RESERVED_ENTITY_NAMES.has(name.toLowerCase())) {
      return { name, isMultiEntity: false };
    }
  }

  // Single capitalized word after "for"
  const forSingleMatch = clean.match(RE_FOR_SINGLE);
  if (forSingleMatch) {
    const name = forSingleMatch[1].trim();
    if (name.length > 1 && !/^(the|this|that|it)$/i.test(name)) {
      return { name, isMultiEntity: false };
    }
  }

  return { name: null, isMultiEntity: false };
}

// ── Identifier extraction ──────────────────────────────────────────────────

const STOP_WORDS = new Set([
  'the', 'and', 'for', 'this', 'that', 'with', 'from', 'featured',
  'validator', 'tokenomics', 'announcement', 'announce', 'proposal',
  'vote', 'request', 'application', 'listing', 'onboarding', 'supervalidator',
]);

/**
 * Extract structured identifiers from a subject + snippet string.
 *
 * @param {string} text
 * @param {{ validatorKeywords?: any[], featuredAppKeywords?: any[], cipKeywords?: any[] }} [learnedPatterns]
 * @returns {Identifiers}
 */
export function extractIdentifiers(text, learnedPatterns = null) {
  if (!text) {
    return {
      cipNumber: null, appName: null, validatorName: null,
      entityName: null, isMultiEntity: false, network: null,
      keywords: [], isCipDiscussion: false,
      isCipVoteProposal: false, isFeaturedAppVoteProposal: false,
      isValidatorVoteProposal: false, isValidator: false, isFeaturedApp: false,
    };
  }

  // ── CIP number ──
  let cipNumber = null;
  if (RE_CIP_TBD.test(text)) {
    cipNumber = 'CIP-00XX';
  } else {
    const m = text.match(RE_CIP_NUMBER);
    if (m) {
      cipNumber = `CIP-${m[1].padStart(4, '0')}`;
    } else {
      const standalone = text.match(RE_CIP_STANDALONE);
      if (standalone) cipNumber = `CIP-${standalone[1]}`;
    }
  }

  // ── Network ──
  let network = null;
  if (RE_TESTNET.test(text)) network = 'testnet';
  else if (RE_MAINNET.test(text)) network = 'mainnet';

  // ── Vote proposal flags ──
  const isVoteProposal = RE_VOTE_PROPOSAL.test(text);
  const isCipVoteProposal = isVoteProposal && RE_CIP_VOTE.test(text);
  const isFeaturedAppVoteProposal =
    isVoteProposal && (RE_FEATURED_APP_VOTE.test(text) || RE_NETWORK_COLON.test(text));
  const isValidatorVoteProposal = isVoteProposal && RE_VALIDATOR_VOTE.test(text);

  // ── Type indicators ──
  const isCipDiscussion = RE_CIP_DISCUSSION.test(text);

  const isFeaturedApp =
    !isCipDiscussion &&
    (RE_FEATURED_APP.test(text) ||
      isFeaturedAppVoteProposal ||
      /featured\s+app\s+rights/i.test(text) ||
      (learnedPatterns
        ? matchesKeywords(text, learnedPatterns.featuredAppKeywords)
        : false));

  const isValidator =
    RE_VALIDATOR.test(text) ||
    isValidatorVoteProposal ||
    (learnedPatterns ? matchesKeywords(text, learnedPatterns.validatorKeywords) : false);

  const hasCipKeywords = learnedPatterns
    ? matchesKeywords(text, learnedPatterns.cipKeywords)
    : false;

  // ── Entity name ──
  const { name: entityName, isMultiEntity } = extractPrimaryEntityName(text);

  const appName = isFeaturedApp && entityName && !isCipDiscussion ? entityName : null;
  const validatorName = isValidator && entityName ? entityName : null;

  // ── Fallback keywords ──
  const keywords = [
    ...new Set(
      text
        .toLowerCase()
        .replace(/[^a-z0-9\s]/g, ' ')
        .split(/\s+/)
        .filter(w => w.length > 3 && !STOP_WORDS.has(w)),
    ),
  ].slice(0, 10);

  return {
    cipNumber,
    appName,
    validatorName,
    entityName,
    isMultiEntity,
    network,
    keywords,
    isCipDiscussion: isCipDiscussion || hasCipKeywords,
    isCipVoteProposal,
    isFeaturedAppVoteProposal,
    isValidatorVoteProposal,
    isValidator,
    isFeaturedApp,
  };
}

// ── Internal helper (not exported) ────────────────────────────────────────
function matchesKeywords(text, keywords) {
  if (!keywords?.length) return false;
  const lower = text.toLowerCase();
  return keywords.some(kw => {
    const word = typeof kw === 'string' ? kw : kw.keyword;
    return lower.includes(word.toLowerCase());
  });
}

// ── URL extraction ─────────────────────────────────────────────────────────

const URL_RE = /(https?:\/\/[^\s<>"{}|\\^`[\]]+)/g;

/** @param {string} text @returns {string[]} */
export function extractUrls(text) {
  if (!text) return [];
  return [...new Set(text.match(URL_RE) ?? [])];
}
