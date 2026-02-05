/**
 * Shared governance utilities for vote counting, status determination,
 * and type classification. Used by both Governance and GovernanceFlow pages.
 */

// ============================================================================
// Vote Counting
// ============================================================================

export interface VoteCount {
  votesFor: number;
  votesAgainst: number;
  totalVotes: number;
}

export interface ParsedVote {
  party: string;
  sv: string;
  vote: 'accept' | 'reject' | 'abstain';
  reason: string;
  reasonUrl: string;
  castAt: string | null;
}

export interface VoteParseResult extends VoteCount {
  votedSvs: ParsedVote[];
}

/**
 * Parse votes array from VoteRequest payload.
 * Handles both tuple format [[svName, voteObj], ...] and object format.
 * This is the single source of truth for vote counting.
 */
export function parseVotes(votes: any): VoteParseResult {
  if (!votes) {
    return { votesFor: 0, votesAgainst: 0, totalVotes: 0, votedSvs: [] };
  }

  const votesArray = Array.isArray(votes) ? votes : Object.entries(votes);

  let votesFor = 0;
  let votesAgainst = 0;
  const votedSvs: ParsedVote[] = [];

  for (const vote of votesArray) {
    const [svName, voteData] = Array.isArray(vote)
      ? vote
      : [vote.sv || 'Unknown', vote];

    // Handle case variations in the accept/reject fields
    const isAccept = voteData?.accept === true || voteData?.Accept === true;
    const isReject =
      voteData?.accept === false ||
      voteData?.reject === true ||
      voteData?.Reject === true;

    if (isAccept) votesFor++;
    else if (isReject) votesAgainst++;

    votedSvs.push({
      party: svName,
      sv: voteData?.sv || svName,
      vote: isAccept ? 'accept' : isReject ? 'reject' : 'abstain',
      reason: voteData?.reason?.body || voteData?.reason || '',
      reasonUrl: voteData?.reason?.url || '',
      castAt: voteData?.optCastAt || null,
    });
  }

  return {
    votesFor,
    votesAgainst,
    totalVotes: votesArray.length,
    votedSvs,
  };
}

/**
 * Count votes without parsing full details.
 * Lighter weight version for when you only need counts.
 */
export function countVotes(votes: any): VoteCount {
  if (!votes) {
    return { votesFor: 0, votesAgainst: 0, totalVotes: 0 };
  }

  const votesArray = Array.isArray(votes) ? votes : Object.entries(votes);

  let votesFor = 0;
  let votesAgainst = 0;

  for (const vote of votesArray) {
    const [, voteData] = Array.isArray(vote)
      ? vote
      : [vote.sv || 'Unknown', vote];

    const isAccept = voteData?.accept === true || voteData?.Accept === true;
    const isReject =
      voteData?.accept === false ||
      voteData?.reject === true ||
      voteData?.Reject === true;

    if (isAccept) votesFor++;
    else if (isReject) votesAgainst++;
  }

  return {
    votesFor,
    votesAgainst,
    totalVotes: votesArray.length,
  };
}

// ============================================================================
// Vote Status Determination
// ============================================================================

export type VoteStatus = 'approved' | 'rejected' | 'pending' | 'expired';

export interface StatusDetermination {
  status: VoteStatus;
  isExpired: boolean;
  meetsThreshold: boolean;
}

/**
 * Determine the status of a vote based on vote counts, threshold, and deadline.
 *
 * Status logic:
 * - approved: votesFor >= threshold (regardless of deadline)
 * - pending: deadline not passed and votesFor < threshold
 * - expired: deadline passed and votesFor < threshold
 * - rejected: explicitly rejected (for historical votes with VRO_Rejected outcome)
 *
 * Note: For active (in-progress) votes, we don't have explicit rejection -
 * votes are either approved, pending, or expired.
 */
export function determineVoteStatus(
  votesFor: number,
  threshold: number,
  voteBefore: Date | string | null,
  now: Date = new Date()
): StatusDetermination {
  const voteDeadline = voteBefore
    ? voteBefore instanceof Date
      ? voteBefore
      : new Date(voteBefore)
    : null;

  const isExpired = voteDeadline !== null && voteDeadline < now;
  const meetsThreshold = votesFor >= threshold;

  let status: VoteStatus;
  if (meetsThreshold) {
    status = 'approved';
  } else if (isExpired) {
    status = 'expired';
  } else {
    status = 'pending';
  }

  return { status, isExpired, meetsThreshold };
}

/**
 * Map historical vote outcome tag to status.
 * Used for Scan API vote results which have explicit outcome.
 */
export function mapOutcomeToStatus(
  outcomeTag: string | undefined
): VoteStatus {
  if (outcomeTag === 'VRO_Accepted') return 'approved';
  if (outcomeTag === 'VRO_Rejected') return 'rejected';
  return 'expired';
}

// ============================================================================
// Type Classification for Governance Items
// ============================================================================

export type GovernanceItemType =
  | 'cip'
  | 'featured-app'
  | 'validator'
  | 'protocol-upgrade'
  | 'outcome'
  | 'other';

/**
 * Detect if a vote represents a milestone/reward vote.
 * NOTE: some milestone votes don't encode "milestone" in the action tag,
 * but do in the proposal text.
 */
export function isMilestoneVote(actionTag: string, text: string): boolean {
  const milestoneText = /\bmilestone(?:s|\(s\))?\b/i;

  return (
    /MintUnclaimedRewards/i.test(actionTag) ||
    /SRARC_MintUnclaimed/i.test(actionTag) ||
    /MintRewards/i.test(actionTag) ||
    /DistributeRewards/i.test(actionTag) ||
    /Reward/i.test(actionTag) ||
    /Coupon/i.test(actionTag) ||
    milestoneText.test(text)
  );
}

export interface VoteTypeMapping {
  type: GovernanceItemType;
  key: string;
  stages: Array<'sv-onchain-vote' | 'sv-milestone'>;
}

/**
 * Extract governance item type and key from vote request data.
 *
 * IMPORTANT: The order of checks matters!
 * 1. CIP references (most specific - has CIP number)
 * 2. Validator actions (check before featured-app to avoid milestone over-matching)
 * 3. Protocol Upgrade actions
 * 4. Featured App actions (including milestone, but only if not matched above)
 *
 * This ordering prevents milestone votes for validators/protocol upgrades
 * from being incorrectly classified as featured-app.
 */
export function extractVoteTypeMapping(
  actionTag: string,
  actionValue: Record<string, any>,
  reasonBody: string,
  reasonUrl: string
): VoteTypeMapping | null {
  const text = `${reasonBody || ''} ${reasonUrl || ''}`;
  const isMilestone = isMilestoneVote(actionTag, text);

  // Determine stages: milestone votes appear in BOTH on-chain vote AND milestone
  const stages: Array<'sv-onchain-vote' | 'sv-milestone'> = ['sv-onchain-vote'];
  if (isMilestone) {
    stages.push('sv-milestone');
  }

  // 1. Check for CIP references (highest priority - most specific)
  const cipMatch = text.match(/CIP[#\-\s]?0*(\d+)/i);
  if (cipMatch) {
    return {
      type: 'cip',
      key: `CIP-${cipMatch[1].padStart(4, '0')}`,
      stages,
    };
  }

  // 2. Check for Validator actions (before featured-app to handle validator milestones)
  if (
    actionTag.includes('OnboardValidator') ||
    actionTag.includes('OffboardValidator') ||
    actionTag.includes('ValidatorOnboarding') ||
    actionTag.includes('ValidatorLicense') ||
    // Check text for validator context (but not just any mention of "validator")
    /validator\s+(?:operator|onboarding|license|offboard)/i.test(text)
  ) {
    const validatorName =
      (actionValue as any)?.validator ||
      (actionValue as any)?.name ||
      text.match(/validator[:\s]+([^\s,]+)/i)?.[1];
    if (validatorName) {
      return { type: 'validator', key: validatorName.toLowerCase(), stages };
    }
  }

  // 3. Check for Protocol Upgrade actions
  if (
    actionTag.includes('ScheduleDomainMigration') ||
    actionTag.includes('ProtocolUpgrade') ||
    actionTag.includes('Synchronizer') ||
    /\b(?:migration|splice\s+\d+\.\d+|protocol\s+upgrade)\b/i.test(text)
  ) {
    const version =
      text.match(/splice[:\s]*(\d+\.\d+)/i)?.[1] ||
      text.match(/version[:\s]*(\d+\.\d+)/i)?.[1];
    return { type: 'protocol-upgrade', key: version || 'upgrade', stages };
  }

  // 4. Check for Featured App actions (including milestone rewards for apps)
  // This comes AFTER validator/protocol checks to avoid over-matching
  if (
    actionTag.includes('GrantFeaturedAppRight') ||
    actionTag.includes('RevokeFeaturedAppRight') ||
    actionTag.includes('SetFeaturedAppRight') ||
    actionTag.includes('FeaturedApp') ||
    text.toLowerCase().includes('featured app') ||
    // Milestone votes that didn't match above are likely featured-app milestones
    isMilestone
  ) {
    const appName =
      (actionValue as any)?.provider ||
      (actionValue as any)?.featuredAppProvider ||
      (actionValue as any)?.featuredApp ||
      (actionValue as any)?.beneficiary ||
      (actionValue as any)?.name ||
      text.match(/(?:mainnet|testnet):\s*([^\s,]+)/i)?.[1] ||
      text.match(/app[:\s]+([^\s,]+)/i)?.[1];
    if (appName) {
      const normalized = String(appName).replace(/::/g, '::').toLowerCase();
      return { type: 'featured-app', key: normalized, stages };
    }
  }

  return null;
}

// ============================================================================
// Action Parsing
// ============================================================================

/**
 * Parse action tag into human-readable title.
 */
export function parseActionTitle(tag: string): string {
  return tag
    .replace(/^(SRARC_|ARC_|CRARC_|ARAC_)/, '')
    .replace(/([A-Z])/g, ' $1')
    .trim();
}

/**
 * Extract simple displayable values from nested action objects.
 * Limits depth to prevent huge JSON dumps.
 */
export function extractSimpleFields(
  obj: any,
  prefix = '',
  depth = 0
): Record<string, string> {
  if (!obj || depth > 2) return {};

  const result: Record<string, string> = {};

  for (const [key, value] of Object.entries(obj)) {
    // Skip internal/technical fields
    if (
      ['tag', 'value', 'packageId', 'moduleName', 'entityName', 'dso'].includes(
        key
      )
    )
      continue;

    const fieldName = prefix ? `${prefix}.${key}` : key;

    if (value === null || value === undefined) continue;

    if (typeof value === 'string') {
      // Only include if it's not a huge hash/ID
      if (value.length < 100 && !value.match(/^[a-f0-9]{64}$/i)) {
        result[fieldName] = value;
      }
    } else if (typeof value === 'number' || typeof value === 'boolean') {
      result[fieldName] = String(value);
    } else if (Array.isArray(value)) {
      if (value.length > 0 && typeof value[0] !== 'object') {
        result[fieldName] =
          value.slice(0, 3).join(', ') +
          (value.length > 3 ? ` (+${value.length - 3} more)` : '');
      } else {
        result[fieldName] = `[${value.length} items]`;
      }
    } else if (typeof value === 'object') {
      const nested = extractSimpleFields(value, fieldName, depth + 1);
      Object.assign(result, nested);
    }
  }

  return result;
}

/**
 * Parse action structure and extract meaningful title and details.
 */
export function parseAction(action: any): {
  title: string;
  actionType: string;
  actionDetails: Record<string, string>;
} {
  if (!action)
    return { title: 'Unknown Action', actionType: 'Unknown', actionDetails: {} };

  // Handle nested tag/value structure
  const outerTag = action.tag || Object.keys(action)[0] || 'Unknown';
  const outerValue = action.value || action[outerTag] || action;

  // Extract inner action (e.g., dsoAction)
  const innerAction =
    outerValue?.dsoAction || outerValue?.amuletRulesAction || outerValue;
  const innerTag = innerAction?.tag || '';
  const innerValue = innerAction?.value || innerAction;

  const actionType = innerTag || outerTag;
  const title = parseActionTitle(actionType);
  const actionDetails = extractSimpleFields(innerValue);

  return { title, actionType, actionDetails };
}

// ============================================================================
// Date Utilities
// ============================================================================

/**
 * Safe date formatter that won't crash on invalid dates.
 */
export function safeFormatDate(
  dateStr: string | Date | null | undefined,
  formatFn: (date: Date) => string = (d) =>
    d.toLocaleDateString('en-US', {
      month: 'short',
      day: 'numeric',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    })
): string {
  if (!dateStr) return 'N/A';
  try {
    const date = dateStr instanceof Date ? dateStr : new Date(dateStr);
    if (isNaN(date.getTime())) return 'N/A';
    return formatFn(date);
  } catch {
    return 'N/A';
  }
}
