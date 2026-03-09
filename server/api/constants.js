// ============================================================
// CONSTANTS — all magic numbers and configuration in one place
// ============================================================
import path from 'path';

// ---------- Paths ----------
export const BASE_DATA_DIR = process.env.DATA_DIR || path.resolve(process.cwd(), 'data');
export const CACHE_DIR = path.join(BASE_DATA_DIR, 'cache');
export const CACHE_FILE = path.join(CACHE_DIR, 'governance-lifecycle.json');
export const OVERRIDES_FILE = path.join(CACHE_DIR, 'governance-overrides.json');
export const LEARNED_PATTERNS_FILE = path.join(CACHE_DIR, 'learned-patterns.json');
export const AUDIT_LOG_FILE = path.join(CACHE_DIR, 'override-audit-log.json');
export const PATTERN_BACKUPS_DIR = path.join(CACHE_DIR, 'pattern-backups');

// ---------- Feature flags ----------
export const INFERENCE_ENABLED = process.env.INFERENCE_ENABLED === 'true';

// ---------- Inference / classification ----------
export const INFERENCE_THRESHOLD = 0.85;
export const LLM_OVERRIDE_CONFIDENCE = 0.85;

// ---------- Fuzzy matching ----------
export const FUZZY_DISTANCE_RATIO = 0.20;
export const FUZZY_MIN_DISTANCE = 2;
export const FUZZY_MIN_LENGTH_RATIO = 0.7;

// ---------- Correlation / similarity ----------
export const SIMILARITY_THRESHOLD_OTHER = 70;
export const DATE_PROXIMITY_CLOSE_DAYS = 7;
export const DATE_PROXIMITY_NEAR_DAYS = 30;
export const DATE_PROXIMITY_FAR_DAYS = 90;
export const DATE_PROXIMITY_CLOSE_BONUS = 10;
export const DATE_PROXIMITY_NEAR_BONUS = 5;
export const DATE_PROXIMITY_FAR_BONUS = 2;
export const ENTITY_EXACT_SCORE = 100;
export const ENTITY_PARTIAL_SCORE = 80;
export const APP_NAME_SCORE = 50;
export const CIP_EXACT_SCORE = 100;

// ---------- Pattern learning ----------
export const DECAY_HALF_LIFE_DAYS = 30;
export const MIN_CONFIDENCE = 0.1;
export const REINFORCEMENT_BOOST = 0.2;
export const SURVIVAL_REINFORCEMENT_MULTIPLIER = 1.5;
export const REINFORCEMENT_LOG_MAX = 20;

// ---------- Pattern improvement analysis ----------
export const SUGGESTION_MIN_CORRECTIONS = 1;
export const KEYWORD_SIGNIFICANCE_RATIO = 0.5;
export const KEYWORD_MIN_OCCURRENCES = 2;
export const HIGH_PRIORITY_THRESHOLD = 3;
export const INSTRUCTIONAL_SUGGESTION_THRESHOLD = 4;

// ---------- Versioning ----------
export const MINOR_VERSION_GROWTH_RATIO = 1.5;
export const CHANGELOG_MAX_ENTRIES = 20;
export const HISTORY_MAX_ENTRIES = 10;

// ---------- API / fetching ----------
export const BASE_URL = 'https://lists.sync.global';
export const MAX_TOPICS_PER_GROUP = 300;
export const TOPICS_PAGE_LIMIT = 100;
export const FETCH_RETRY_MAX = 3;
export const FETCH_RETRY_DELAY_MS = 2000;
export const FETCH_PAGE_DELAY_MS = 200;
export const FETCH_GROUP_DELAY_MS = 500;
export const FETCH_TIMEOUT_MS = 45_000;
export const FETCH_FRESH_TIMEOUT_MS = 180_000;

// ---------- Lifecycle item ID prefixes ----------
export const RECLASSIFIED_ITEM_PREFIX = 'topic-reclassified-';
export const EXTRACTED_ITEM_PREFIX = 'topic-extracted-';

// ---------- Governance groups ----------
export const GOVERNANCE_GROUPS = {
  'cip-discuss':             { stage: 'cip-discuss',         flow: 'cip',          label: 'CIP Discussion' },
  'cip-vote':                { stage: 'cip-vote',            flow: 'cip',          label: 'CIP Vote' },
  'cip-announce':            { stage: 'cip-announce',        flow: 'cip',          label: 'CIP Announcement' },
  'tokenomics':              { stage: 'tokenomics',          flow: 'shared',       label: 'Tokenomics Discussion' },
  'tokenomics-announce':     { stage: 'tokenomics-announce', flow: 'featured-app', label: 'Tokenomics Announcement' },
  'supervalidator-announce': { stage: 'sv-announce',         flow: 'shared',       label: 'SV Announcement' },
};

// ---------- Workflow stages ----------
export const WORKFLOW_STAGES = {
  cip:                ['cip-discuss', 'cip-vote', 'cip-announce', 'sv-announce'],
  'featured-app':     ['tokenomics', 'tokenomics-announce', 'sv-announce'],
  validator:          ['tokenomics', 'sv-announce'],
  'protocol-upgrade': ['tokenomics', 'sv-announce'],
  outcome:            ['sv-announce'],
  other:              ['tokenomics', 'sv-announce'],
};

export const ALL_STAGES = [...new Set(Object.values(WORKFLOW_STAGES).flat())];

export const VALID_TYPES = ['cip', 'featured-app', 'validator', 'protocol-upgrade', 'outcome', 'other'];

export const FETCH_GLOBAL_TIMEOUT_MS = 120000;
