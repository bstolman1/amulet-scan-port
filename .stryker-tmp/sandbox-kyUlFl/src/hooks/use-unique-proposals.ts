// @ts-nocheck
function stryNS_9fa48() {
  var g = typeof globalThis === 'object' && globalThis && globalThis.Math === Math && globalThis || new Function("return this")();
  var ns = g.__stryker__ || (g.__stryker__ = {});
  if (ns.activeMutant === undefined && g.process && g.process.env && g.process.env.__STRYKER_ACTIVE_MUTANT__) {
    ns.activeMutant = g.process.env.__STRYKER_ACTIVE_MUTANT__;
  }
  function retrieveNS() {
    return ns;
  }
  stryNS_9fa48 = retrieveNS;
  return retrieveNS();
}
stryNS_9fa48();
function stryCov_9fa48() {
  var ns = stryNS_9fa48();
  var cov = ns.mutantCoverage || (ns.mutantCoverage = {
    static: {},
    perTest: {}
  });
  function cover() {
    var c = cov.static;
    if (ns.currentTestId) {
      c = cov.perTest[ns.currentTestId] = cov.perTest[ns.currentTestId] || {};
    }
    var a = arguments;
    for (var i = 0; i < a.length; i++) {
      c[a[i]] = (c[a[i]] || 0) + 1;
    }
  }
  stryCov_9fa48 = cover;
  cover.apply(null, arguments);
}
function stryMutAct_9fa48(id) {
  var ns = stryNS_9fa48();
  function isActive(id) {
    if (ns.activeMutant === id) {
      if (ns.hitCount !== void 0 && ++ns.hitCount > ns.hitLimit) {
        throw new Error('Stryker: Hit count limit reached (' + ns.hitCount + ')');
      }
      return true;
    }
    return false;
  }
  stryMutAct_9fa48 = isActive;
  return isActive(id);
}
import { useMemo } from "react";
import { useGovernanceEvents } from "./use-governance-events";

// Uses fast indexed VoteRequest data from DuckDB
export interface GovernanceAction {
  id: string;
  type: 'vote_completed' | 'rule_change' | 'confirmation';
  actionTag: string;
  actionTitle: string;
  templateType: 'VoteRequest' | 'DsoRules' | 'AmuletRules' | 'Confirmation';
  status: 'passed' | 'failed' | 'expired' | 'executed';
  effectiveAt: string;
  requester: string | null;
  reason: string | null;
  reasonUrl: string | null;
  votesFor: number;
  votesAgainst: number;
  totalVotes: number;
  contractId: string;
  /** Stable VoteRequest identifier from payload.id (preferred dedupe key). */
  payloadId: string | null;
  cipReference: string | null;
  voteBefore: string | null;
  targetEffectiveAt: string | null;
  actionDetails: Record<string, unknown> | null;
}
export interface UniqueProposal {
  /** Stable dedupe key (payloadId when available). */
  proposalId: string;
  proposalHash: string;
  actionType: string;
  title: string;
  status: 'approved' | 'rejected' | 'pending' | 'expired';
  latestEventTime: string;
  createdAt: string | null;
  voteBefore: string | null;
  requester: string | null;
  reason: string | null;
  reasonUrl: string | null;
  votesFor: number;
  votesAgainst: number;
  totalVotes: number;
  contractId: string;
  cipReference: string | null;
  eventCount: number;
  lastEventType: 'created' | 'archived';
  actionDetails: Record<string, unknown> | null;
  rawData: GovernanceAction;
}

// Helper to parse action structure and extract meaningful title (same as Governance History)
const parseAction = (action: any): {
  title: string;
  actionType: string;
  actionDetails: any;
} => {
  if (stryMutAct_9fa48("3005")) {
    {}
  } else {
    stryCov_9fa48("3005");
    if (stryMutAct_9fa48("3008") ? false : stryMutAct_9fa48("3007") ? true : stryMutAct_9fa48("3006") ? action : (stryCov_9fa48("3006", "3007", "3008"), !action)) return {
      title: "Unknown Action",
      actionType: "Unknown",
      actionDetails: null
    };

    // Handle nested tag/value structure: { tag: "ARC_DsoRules", value: { dsoAction: { tag: "SRARC_...", value: {...} } } }
    const outerTag = stryMutAct_9fa48("3014") ? (action.tag || Object.keys(action)[0]) && "Unknown" : stryMutAct_9fa48("3013") ? false : stryMutAct_9fa48("3012") ? true : (stryCov_9fa48("3012", "3013", "3014"), (stryMutAct_9fa48("3016") ? action.tag && Object.keys(action)[0] : stryMutAct_9fa48("3015") ? false : (stryCov_9fa48("3015", "3016"), action.tag || Object.keys(action)[0])) || "Unknown");
    const outerValue = stryMutAct_9fa48("3020") ? (action.value || action[outerTag]) && action : stryMutAct_9fa48("3019") ? false : stryMutAct_9fa48("3018") ? true : (stryCov_9fa48("3018", "3019", "3020"), (stryMutAct_9fa48("3022") ? action.value && action[outerTag] : stryMutAct_9fa48("3021") ? false : (stryCov_9fa48("3021", "3022"), action.value || action[outerTag])) || action);

    // Extract inner action (e.g., dsoAction)
    const innerAction = stryMutAct_9fa48("3025") ? (outerValue?.dsoAction || outerValue?.amuletRulesAction) && outerValue : stryMutAct_9fa48("3024") ? false : stryMutAct_9fa48("3023") ? true : (stryCov_9fa48("3023", "3024", "3025"), (stryMutAct_9fa48("3027") ? outerValue?.dsoAction && outerValue?.amuletRulesAction : stryMutAct_9fa48("3026") ? false : (stryCov_9fa48("3026", "3027"), (stryMutAct_9fa48("3028") ? outerValue.dsoAction : (stryCov_9fa48("3028"), outerValue?.dsoAction)) || (stryMutAct_9fa48("3029") ? outerValue.amuletRulesAction : (stryCov_9fa48("3029"), outerValue?.amuletRulesAction)))) || outerValue);
    const innerTag = stryMutAct_9fa48("3032") ? innerAction?.tag && "" : stryMutAct_9fa48("3031") ? false : stryMutAct_9fa48("3030") ? true : (stryCov_9fa48("3030", "3031", "3032"), (stryMutAct_9fa48("3033") ? innerAction.tag : (stryCov_9fa48("3033"), innerAction?.tag)) || "");
    const innerValue = stryMutAct_9fa48("3037") ? innerAction?.value && innerAction : stryMutAct_9fa48("3036") ? false : stryMutAct_9fa48("3035") ? true : (stryCov_9fa48("3035", "3036", "3037"), (stryMutAct_9fa48("3038") ? innerAction.value : (stryCov_9fa48("3038"), innerAction?.value)) || innerAction);

    // Build human-readable title
    const actionType = stryMutAct_9fa48("3041") ? innerTag && outerTag : stryMutAct_9fa48("3040") ? false : stryMutAct_9fa48("3039") ? true : (stryCov_9fa48("3039", "3040", "3041"), innerTag || outerTag);
    const title = stryMutAct_9fa48("3042") ? actionType.replace(/^(SRARC_|ARC_|CRARC_|ARAC_)/, "").replace(/([A-Z])/g, " $1") : (stryCov_9fa48("3042"), actionType.replace(stryMutAct_9fa48("3043") ? /(SRARC_|ARC_|CRARC_|ARAC_)/ : (stryCov_9fa48("3043"), /^(SRARC_|ARC_|CRARC_|ARAC_)/), "").replace(stryMutAct_9fa48("3045") ? /([^A-Z])/g : (stryCov_9fa48("3045"), /([A-Z])/g), " $1").trim());
    return {
      title,
      actionType,
      actionDetails: innerValue
    };
  }
};

// Parse votes array to count for/against (same as Governance History)
const parseVotes = (votes: unknown): {
  votesFor: number;
  votesAgainst: number;
} => {
  if (stryMutAct_9fa48("3048")) {
    {}
  } else {
    stryCov_9fa48("3048");
    if (stryMutAct_9fa48("3051") ? false : stryMutAct_9fa48("3050") ? true : stryMutAct_9fa48("3049") ? votes : (stryCov_9fa48("3049", "3050", "3051"), !votes)) return {
      votesFor: 0,
      votesAgainst: 0
    };

    // Handle array of tuples format: [["SV Name", { sv, accept, reason, optCastAt }], ...]
    const votesArray = Array.isArray(votes) ? votes : Object.entries(votes);
    let votesFor = 0;
    let votesAgainst = 0;
    for (const vote of votesArray) {
      if (stryMutAct_9fa48("3053")) {
        {}
      } else {
        stryCov_9fa48("3053");
        const [, voteData] = Array.isArray(vote) ? vote : stryMutAct_9fa48("3054") ? [] : (stryCov_9fa48("3054"), ['', vote]);
        const isAccept = stryMutAct_9fa48("3058") ? voteData?.accept === true && (voteData as any)?.Accept === true : stryMutAct_9fa48("3057") ? false : stryMutAct_9fa48("3056") ? true : (stryCov_9fa48("3056", "3057", "3058"), (stryMutAct_9fa48("3060") ? voteData?.accept !== true : stryMutAct_9fa48("3059") ? false : (stryCov_9fa48("3059", "3060"), (stryMutAct_9fa48("3061") ? voteData.accept : (stryCov_9fa48("3061"), voteData?.accept)) === (stryMutAct_9fa48("3062") ? false : (stryCov_9fa48("3062"), true)))) || (stryMutAct_9fa48("3064") ? (voteData as any)?.Accept !== true : stryMutAct_9fa48("3063") ? false : (stryCov_9fa48("3063", "3064"), (stryMutAct_9fa48("3065") ? (voteData as any).Accept : (stryCov_9fa48("3065"), (voteData as any)?.Accept)) === (stryMutAct_9fa48("3066") ? false : (stryCov_9fa48("3066"), true)))));
        const isReject = stryMutAct_9fa48("3069") ? (voteData?.accept === false || voteData?.reject === true) && (voteData as any)?.Reject === true : stryMutAct_9fa48("3068") ? false : stryMutAct_9fa48("3067") ? true : (stryCov_9fa48("3067", "3068", "3069"), (stryMutAct_9fa48("3071") ? voteData?.accept === false && voteData?.reject === true : stryMutAct_9fa48("3070") ? false : (stryCov_9fa48("3070", "3071"), (stryMutAct_9fa48("3073") ? voteData?.accept !== false : stryMutAct_9fa48("3072") ? false : (stryCov_9fa48("3072", "3073"), (stryMutAct_9fa48("3074") ? voteData.accept : (stryCov_9fa48("3074"), voteData?.accept)) === (stryMutAct_9fa48("3075") ? true : (stryCov_9fa48("3075"), false)))) || (stryMutAct_9fa48("3077") ? voteData?.reject !== true : stryMutAct_9fa48("3076") ? false : (stryCov_9fa48("3076", "3077"), (stryMutAct_9fa48("3078") ? voteData.reject : (stryCov_9fa48("3078"), voteData?.reject)) === (stryMutAct_9fa48("3079") ? false : (stryCov_9fa48("3079"), true)))))) || (stryMutAct_9fa48("3081") ? (voteData as any)?.Reject !== true : stryMutAct_9fa48("3080") ? false : (stryCov_9fa48("3080", "3081"), (stryMutAct_9fa48("3082") ? (voteData as any).Reject : (stryCov_9fa48("3082"), (voteData as any)?.Reject)) === (stryMutAct_9fa48("3083") ? false : (stryCov_9fa48("3083"), true)))));
        if (stryMutAct_9fa48("3085") ? false : stryMutAct_9fa48("3084") ? true : (stryCov_9fa48("3084", "3085"), isAccept)) stryMutAct_9fa48("3086") ? votesFor-- : (stryCov_9fa48("3086"), votesFor++);else if (stryMutAct_9fa48("3088") ? false : stryMutAct_9fa48("3087") ? true : (stryCov_9fa48("3087", "3088"), isReject)) stryMutAct_9fa48("3089") ? votesAgainst-- : (stryCov_9fa48("3089"), votesAgainst++);
      }
    }
    return {
      votesFor,
      votesAgainst
    };
  }
};

// Extract proposal hash from contract_id (first 12 chars)
const extractProposalHash = (contractId: string): string => {
  if (stryMutAct_9fa48("3091")) {
    {}
  } else {
    stryCov_9fa48("3091");
    return stryMutAct_9fa48("3094") ? contractId?.slice(0, 12) && 'unknown' : stryMutAct_9fa48("3093") ? false : stryMutAct_9fa48("3092") ? true : (stryCov_9fa48("3092", "3093", "3094"), (stryMutAct_9fa48("3096") ? contractId.slice(0, 12) : stryMutAct_9fa48("3095") ? contractId : (stryCov_9fa48("3095", "3096"), contractId?.slice(0, 12))) || 'unknown');
  }
};

// Extract CIP reference from reason text
const extractCipReference = (reason: string | null, reasonUrl: string | null): string | null => {
  if (stryMutAct_9fa48("3098")) {
    {}
  } else {
    stryCov_9fa48("3098");
    if (stryMutAct_9fa48("3101") ? !reason || !reasonUrl : stryMutAct_9fa48("3100") ? false : stryMutAct_9fa48("3099") ? true : (stryCov_9fa48("3099", "3100", "3101"), (stryMutAct_9fa48("3102") ? reason : (stryCov_9fa48("3102"), !reason)) && (stryMutAct_9fa48("3103") ? reasonUrl : (stryCov_9fa48("3103"), !reasonUrl)))) return null;
    const text = `${stryMutAct_9fa48("3107") ? reason && '' : stryMutAct_9fa48("3106") ? false : stryMutAct_9fa48("3105") ? true : (stryCov_9fa48("3105", "3106", "3107"), reason || '')} ${stryMutAct_9fa48("3111") ? reasonUrl && '' : stryMutAct_9fa48("3110") ? false : stryMutAct_9fa48("3109") ? true : (stryCov_9fa48("3109", "3110", "3111"), reasonUrl || '')}`;
    const match = text.match(stryMutAct_9fa48("3118") ? /CIP[#\-\s]?0*(\D+)/i : stryMutAct_9fa48("3117") ? /CIP[#\-\s]?0*(\d)/i : stryMutAct_9fa48("3116") ? /CIP[#\-\s]?0(\d+)/i : stryMutAct_9fa48("3115") ? /CIP[#\-\S]?0*(\d+)/i : stryMutAct_9fa48("3114") ? /CIP[^#\-\s]?0*(\d+)/i : stryMutAct_9fa48("3113") ? /CIP[#\-\s]0*(\d+)/i : (stryCov_9fa48("3113", "3114", "3115", "3116", "3117", "3118"), /CIP[#\-\s]?0*(\d+)/i));
    return match ? match[1].padStart(4, '0') : null;
  }
};
export function useUniqueProposals(votingThreshold = 10) {
  if (stryMutAct_9fa48("3120")) {
    {}
  } else {
    stryCov_9fa48("3120");
    const {
      data: rawEventsResult,
      isLoading,
      error
    } = useGovernanceEvents();

    // Extract events array from the result object
    const rawEvents = stryMutAct_9fa48("3121") ? rawEventsResult.events : (stryCov_9fa48("3121"), rawEventsResult?.events);

    // Transform raw events from indexed data into GovernanceAction format
    // Use the SAME parsing logic as the Governance History tab
    const governanceActions = useMemo(() => {
      if (stryMutAct_9fa48("3122")) {
        {}
      } else {
        stryCov_9fa48("3122");
        if (stryMutAct_9fa48("3125") ? false : stryMutAct_9fa48("3124") ? true : stryMutAct_9fa48("3123") ? rawEvents?.length : (stryCov_9fa48("3123", "3124", "3125"), !(stryMutAct_9fa48("3126") ? rawEvents.length : (stryCov_9fa48("3126"), rawEvents?.length)))) return stryMutAct_9fa48("3127") ? ["Stryker was here"] : (stryCov_9fa48("3127"), []);
        const actions: GovernanceAction[] = stryMutAct_9fa48("3128") ? ["Stryker was here"] : (stryCov_9fa48("3128"), []);
        for (const event of rawEvents) {
          if (stryMutAct_9fa48("3129")) {
            {}
          } else {
            stryCov_9fa48("3129");
            const templateId = stryMutAct_9fa48("3132") ? event.template_id && '' : stryMutAct_9fa48("3131") ? false : stryMutAct_9fa48("3130") ? true : (stryCov_9fa48("3130", "3131", "3132"), event.template_id || '');
            if (stryMutAct_9fa48("3136") ? false : stryMutAct_9fa48("3135") ? true : stryMutAct_9fa48("3134") ? templateId.includes('VoteRequest') : (stryCov_9fa48("3134", "3135", "3136"), !templateId.includes('VoteRequest'))) continue;

            // Process ALL events (not just archived) to get complete data
            const payload = event.payload as Record<string, unknown> | undefined;
            if (stryMutAct_9fa48("3140") ? false : stryMutAct_9fa48("3139") ? true : stryMutAct_9fa48("3138") ? payload : (stryCov_9fa48("3138", "3139", "3140"), !payload)) continue;
            const payloadId = (stryMutAct_9fa48("3143") ? typeof payload.id !== 'string' : stryMutAct_9fa48("3142") ? false : stryMutAct_9fa48("3141") ? true : (stryCov_9fa48("3141", "3142", "3143"), typeof payload.id === 'string')) ? payload.id as string : null;

            // Parse action using same logic as Governance History
            const action = stryMutAct_9fa48("3147") ? payload.action && {} : stryMutAct_9fa48("3146") ? false : stryMutAct_9fa48("3145") ? true : (stryCov_9fa48("3145", "3146", "3147"), payload.action || {});
            const {
              title,
              actionType,
              actionDetails
            } = parseAction(action);

            // Parse votes using same logic as Governance History
            const votesRaw = payload.votes;
            const {
              votesFor,
              votesAgainst
            } = parseVotes(votesRaw);
            const totalVotes = stryMutAct_9fa48("3148") ? votesFor - votesAgainst : (stryCov_9fa48("3148"), votesFor + votesAgainst);

            // Extract requester
            const requester = stryMutAct_9fa48("3151") ? payload.requester as string && null : stryMutAct_9fa48("3150") ? false : stryMutAct_9fa48("3149") ? true : (stryCov_9fa48("3149", "3150", "3151"), payload.requester as string || null);

            // Extract reason (has url and body)
            const reasonObj = payload.reason as {
              url?: string;
              body?: string;
            } | string | null;
            const reasonBody = (stryMutAct_9fa48("3154") ? typeof reasonObj !== 'string' : stryMutAct_9fa48("3153") ? false : stryMutAct_9fa48("3152") ? true : (stryCov_9fa48("3152", "3153", "3154"), typeof reasonObj === 'string')) ? reasonObj : stryMutAct_9fa48("3158") ? reasonObj?.body && null : stryMutAct_9fa48("3157") ? false : stryMutAct_9fa48("3156") ? true : (stryCov_9fa48("3156", "3157", "3158"), (stryMutAct_9fa48("3159") ? reasonObj.body : (stryCov_9fa48("3159"), reasonObj?.body)) || null);
            const reasonUrl = (stryMutAct_9fa48("3162") ? typeof reasonObj !== 'object' : stryMutAct_9fa48("3161") ? false : stryMutAct_9fa48("3160") ? true : (stryCov_9fa48("3160", "3161", "3162"), typeof reasonObj === 'object')) ? stryMutAct_9fa48("3166") ? reasonObj?.url && null : stryMutAct_9fa48("3165") ? false : stryMutAct_9fa48("3164") ? true : (stryCov_9fa48("3164", "3165", "3166"), (stryMutAct_9fa48("3167") ? reasonObj.url : (stryCov_9fa48("3167"), reasonObj?.url)) || null) : null;

            // Extract timing fields
            const voteBefore = stryMutAct_9fa48("3170") ? payload.voteBefore as string && null : stryMutAct_9fa48("3169") ? false : stryMutAct_9fa48("3168") ? true : (stryCov_9fa48("3168", "3169", "3170"), payload.voteBefore as string || null);
            const targetEffectiveAt = stryMutAct_9fa48("3173") ? payload.targetEffectiveAt as string && null : stryMutAct_9fa48("3172") ? false : stryMutAct_9fa48("3171") ? true : (stryCov_9fa48("3171", "3172", "3173"), payload.targetEffectiveAt as string || null);

            // Determine status based on votes and deadline
            const now = new Date();
            const voteDeadline = voteBefore ? new Date(voteBefore) : null;
            const isExpired = stryMutAct_9fa48("3176") ? voteDeadline || voteDeadline < now : stryMutAct_9fa48("3175") ? false : stryMutAct_9fa48("3174") ? true : (stryCov_9fa48("3174", "3175", "3176"), voteDeadline && (stryMutAct_9fa48("3179") ? voteDeadline >= now : stryMutAct_9fa48("3178") ? voteDeadline <= now : stryMutAct_9fa48("3177") ? true : (stryCov_9fa48("3177", "3178", "3179"), voteDeadline < now)));
            const isClosed = stryMutAct_9fa48("3182") ? event.event_type !== 'archived' : stryMutAct_9fa48("3181") ? false : stryMutAct_9fa48("3180") ? true : (stryCov_9fa48("3180", "3181", "3182"), event.event_type === 'archived');
            let status: GovernanceAction['status'] = 'failed';
            if (stryMutAct_9fa48("3188") ? votesFor < votingThreshold : stryMutAct_9fa48("3187") ? votesFor > votingThreshold : stryMutAct_9fa48("3186") ? false : stryMutAct_9fa48("3185") ? true : (stryCov_9fa48("3185", "3186", "3187", "3188"), votesFor >= votingThreshold)) {
              if (stryMutAct_9fa48("3189")) {
                {}
              } else {
                stryCov_9fa48("3189");
                status = 'passed';
              }
            } else if (stryMutAct_9fa48("3193") ? isClosed && isExpired && votesFor < votingThreshold : stryMutAct_9fa48("3192") ? false : stryMutAct_9fa48("3191") ? true : (stryCov_9fa48("3191", "3192", "3193"), isClosed || (stryMutAct_9fa48("3195") ? isExpired || votesFor < votingThreshold : stryMutAct_9fa48("3194") ? false : (stryCov_9fa48("3194", "3195"), isExpired && (stryMutAct_9fa48("3198") ? votesFor >= votingThreshold : stryMutAct_9fa48("3197") ? votesFor <= votingThreshold : stryMutAct_9fa48("3196") ? true : (stryCov_9fa48("3196", "3197", "3198"), votesFor < votingThreshold)))))) {
              if (stryMutAct_9fa48("3199")) {
                {}
              } else {
                stryCov_9fa48("3199");
                status = (stryMutAct_9fa48("3202") ? totalVotes !== 0 : stryMutAct_9fa48("3201") ? false : stryMutAct_9fa48("3200") ? true : (stryCov_9fa48("3200", "3201", "3202"), totalVotes === 0)) ? 'expired' : 'failed';
              }
            }
            actions.push({
              id: stryMutAct_9fa48("3208") ? (event.event_id || event.contract_id || payloadId) && '' : stryMutAct_9fa48("3207") ? false : stryMutAct_9fa48("3206") ? true : (stryCov_9fa48("3206", "3207", "3208"), (stryMutAct_9fa48("3210") ? (event.event_id || event.contract_id) && payloadId : stryMutAct_9fa48("3209") ? false : (stryCov_9fa48("3209", "3210"), (stryMutAct_9fa48("3212") ? event.event_id && event.contract_id : stryMutAct_9fa48("3211") ? false : (stryCov_9fa48("3211", "3212"), event.event_id || event.contract_id)) || payloadId)) || ''),
              type: 'vote_completed',
              actionTag: actionType,
              actionTitle: stryMutAct_9fa48("3217") ? title && 'Unknown' : stryMutAct_9fa48("3216") ? false : stryMutAct_9fa48("3215") ? true : (stryCov_9fa48("3215", "3216", "3217"), title || 'Unknown'),
              templateType: 'VoteRequest',
              status,
              effectiveAt: stryMutAct_9fa48("3222") ? (event.effective_at || event.timestamp) && '' : stryMutAct_9fa48("3221") ? false : stryMutAct_9fa48("3220") ? true : (stryCov_9fa48("3220", "3221", "3222"), (stryMutAct_9fa48("3224") ? event.effective_at && event.timestamp : stryMutAct_9fa48("3223") ? false : (stryCov_9fa48("3223", "3224"), event.effective_at || event.timestamp)) || ''),
              requester,
              reason: reasonBody,
              reasonUrl,
              votesFor,
              votesAgainst,
              totalVotes,
              contractId: stryMutAct_9fa48("3228") ? event.contract_id && '' : stryMutAct_9fa48("3227") ? false : stryMutAct_9fa48("3226") ? true : (stryCov_9fa48("3226", "3227", "3228"), event.contract_id || ''),
              payloadId,
              cipReference: extractCipReference(reasonBody, reasonUrl),
              voteBefore,
              targetEffectiveAt,
              actionDetails
            });
          }
        }
        return stryMutAct_9fa48("3230") ? actions : (stryCov_9fa48("3230"), actions.sort(stryMutAct_9fa48("3231") ? () => undefined : (stryCov_9fa48("3231"), (a, b) => stryMutAct_9fa48("3232") ? new Date(b.effectiveAt).getTime() + new Date(a.effectiveAt).getTime() : (stryCov_9fa48("3232"), new Date(b.effectiveAt).getTime() - new Date(a.effectiveAt).getTime()))));
      }
    }, stryMutAct_9fa48("3233") ? [] : (stryCov_9fa48("3233"), [rawEvents, votingThreshold]));
    const uniqueProposals = useMemo(() => {
      if (stryMutAct_9fa48("3234")) {
        {}
      } else {
        stryCov_9fa48("3234");
        if (stryMutAct_9fa48("3237") ? false : stryMutAct_9fa48("3236") ? true : stryMutAct_9fa48("3235") ? governanceActions?.length : (stryCov_9fa48("3235", "3236", "3237"), !(stryMutAct_9fa48("3238") ? governanceActions.length : (stryCov_9fa48("3238"), governanceActions?.length)))) return stryMutAct_9fa48("3239") ? ["Stryker was here"] : (stryCov_9fa48("3239"), []);

        // Deduplicate by payload.id (preferred), falling back to contract_id.
        // Keep latest event by effectiveAt and count how many events were merged.
        const keyMap = new Map<string, {
          latest: (typeof governanceActions)[0];
          count: number;
        }>();
        for (const ev of governanceActions) {
          if (stryMutAct_9fa48("3240")) {
            {}
          } else {
            stryCov_9fa48("3240");
            const key = stryMutAct_9fa48("3243") ? ev.payloadId && ev.contractId : stryMutAct_9fa48("3242") ? false : stryMutAct_9fa48("3241") ? true : (stryCov_9fa48("3241", "3242", "3243"), ev.payloadId || ev.contractId);
            if (stryMutAct_9fa48("3246") ? false : stryMutAct_9fa48("3245") ? true : stryMutAct_9fa48("3244") ? key : (stryCov_9fa48("3244", "3245", "3246"), !key)) continue;
            const existing = keyMap.get(key);
            if (stryMutAct_9fa48("3249") ? false : stryMutAct_9fa48("3248") ? true : stryMutAct_9fa48("3247") ? existing : (stryCov_9fa48("3247", "3248", "3249"), !existing)) {
              if (stryMutAct_9fa48("3250")) {
                {}
              } else {
                stryCov_9fa48("3250");
                keyMap.set(key, {
                  latest: ev,
                  count: 1
                });
                continue;
              }
            }
            stryMutAct_9fa48("3252") ? existing.count -= 1 : (stryCov_9fa48("3252"), existing.count += 1);
            const currentTime = new Date(ev.effectiveAt).getTime();
            const existingTime = new Date(existing.latest.effectiveAt).getTime();
            if (stryMutAct_9fa48("3256") ? currentTime <= existingTime : stryMutAct_9fa48("3255") ? currentTime >= existingTime : stryMutAct_9fa48("3254") ? false : stryMutAct_9fa48("3253") ? true : (stryCov_9fa48("3253", "3254", "3255", "3256"), currentTime > existingTime)) {
              if (stryMutAct_9fa48("3257")) {
                {}
              } else {
                stryCov_9fa48("3257");
                existing.latest = ev;
              }
            }
          }
        }
        const proposals: UniqueProposal[] = stryMutAct_9fa48("3258") ? ["Stryker was here"] : (stryCov_9fa48("3258"), []);
        for (const [proposalId, entry] of keyMap) {
          if (stryMutAct_9fa48("3259")) {
            {}
          } else {
            stryCov_9fa48("3259");
            const event = entry.latest;
            const proposalHash = extractProposalHash(proposalId);
            const actionType = stryMutAct_9fa48("3262") ? event.actionTag && 'Unknown' : stryMutAct_9fa48("3261") ? false : stryMutAct_9fa48("3260") ? true : (stryCov_9fa48("3260", "3261", "3262"), event.actionTag || 'Unknown');

            // Map the status from GovernanceAction to UniqueProposal status
            let status: UniqueProposal['status'];
            switch (event.status) {
              case 'passed':
                if (stryMutAct_9fa48("3264")) {} else {
                  stryCov_9fa48("3264");
                  status = 'approved';
                  break;
                }
              case 'failed':
                if (stryMutAct_9fa48("3267")) {} else {
                  stryCov_9fa48("3267");
                  status = 'rejected';
                  break;
                }
              case 'expired':
                if (stryMutAct_9fa48("3270")) {} else {
                  stryCov_9fa48("3270");
                  status = 'expired';
                  break;
                }
              case 'executed':
                if (stryMutAct_9fa48("3273")) {} else {
                  stryCov_9fa48("3273");
                  status = 'approved';
                  break;
                }
              default:
                if (stryMutAct_9fa48("3276")) {} else {
                  stryCov_9fa48("3276");
                  if (stryMutAct_9fa48("3280") ? event.votesFor < votingThreshold : stryMutAct_9fa48("3279") ? event.votesFor > votingThreshold : stryMutAct_9fa48("3278") ? false : stryMutAct_9fa48("3277") ? true : (stryCov_9fa48("3277", "3278", "3279", "3280"), event.votesFor >= votingThreshold)) {
                    if (stryMutAct_9fa48("3281")) {
                      {}
                    } else {
                      stryCov_9fa48("3281");
                      status = 'approved';
                    }
                  } else if (stryMutAct_9fa48("3285") ? event.type !== 'vote_completed' : stryMutAct_9fa48("3284") ? false : stryMutAct_9fa48("3283") ? true : (stryCov_9fa48("3283", "3284", "3285"), event.type === 'vote_completed')) {
                    if (stryMutAct_9fa48("3287")) {
                      {}
                    } else {
                      stryCov_9fa48("3287");
                      status = 'rejected';
                    }
                  } else {
                    if (stryMutAct_9fa48("3289")) {
                      {}
                    } else {
                      stryCov_9fa48("3289");
                      status = 'expired';
                    }
                  }
                }
            }
            proposals.push({
              proposalId,
              proposalHash,
              actionType,
              title: stryMutAct_9fa48("3294") ? event.actionTitle && 'Unknown' : stryMutAct_9fa48("3293") ? false : stryMutAct_9fa48("3292") ? true : (stryCov_9fa48("3292", "3293", "3294"), event.actionTitle || 'Unknown'),
              status,
              latestEventTime: event.effectiveAt,
              createdAt: event.effectiveAt,
              voteBefore: event.voteBefore,
              requester: event.requester,
              reason: event.reason,
              reasonUrl: event.reasonUrl,
              votesFor: event.votesFor,
              votesAgainst: event.votesAgainst,
              totalVotes: event.totalVotes,
              contractId: event.contractId,
              cipReference: event.cipReference,
              eventCount: entry.count,
              lastEventType: (stryMutAct_9fa48("3298") ? event.type !== 'vote_completed' : stryMutAct_9fa48("3297") ? false : stryMutAct_9fa48("3296") ? true : (stryCov_9fa48("3296", "3297", "3298"), event.type === 'vote_completed')) ? 'archived' : 'created',
              actionDetails: event.actionDetails,
              rawData: event
            });
          }
        }

        // Sort by latest event time descending
        return stryMutAct_9fa48("3302") ? proposals : (stryCov_9fa48("3302"), proposals.sort(stryMutAct_9fa48("3303") ? () => undefined : (stryCov_9fa48("3303"), (a, b) => stryMutAct_9fa48("3304") ? new Date(b.latestEventTime).getTime() + new Date(a.latestEventTime).getTime() : (stryCov_9fa48("3304"), new Date(b.latestEventTime).getTime() - new Date(a.latestEventTime).getTime()))));
      }
    }, stryMutAct_9fa48("3305") ? [] : (stryCov_9fa48("3305"), [governanceActions, votingThreshold]));
    const stats = useMemo(() => {
      if (stryMutAct_9fa48("3306")) {
        {}
      } else {
        stryCov_9fa48("3306");
        const total = uniqueProposals.length;
        const approved = stryMutAct_9fa48("3307") ? uniqueProposals.length : (stryCov_9fa48("3307"), uniqueProposals.filter(stryMutAct_9fa48("3308") ? () => undefined : (stryCov_9fa48("3308"), p => stryMutAct_9fa48("3311") ? p.status !== 'approved' : stryMutAct_9fa48("3310") ? false : stryMutAct_9fa48("3309") ? true : (stryCov_9fa48("3309", "3310", "3311"), p.status === 'approved'))).length);
        const rejected = stryMutAct_9fa48("3313") ? uniqueProposals.length : (stryCov_9fa48("3313"), uniqueProposals.filter(stryMutAct_9fa48("3314") ? () => undefined : (stryCov_9fa48("3314"), p => stryMutAct_9fa48("3317") ? p.status !== 'rejected' : stryMutAct_9fa48("3316") ? false : stryMutAct_9fa48("3315") ? true : (stryCov_9fa48("3315", "3316", "3317"), p.status === 'rejected'))).length);
        const pending = stryMutAct_9fa48("3319") ? uniqueProposals.length : (stryCov_9fa48("3319"), uniqueProposals.filter(stryMutAct_9fa48("3320") ? () => undefined : (stryCov_9fa48("3320"), p => stryMutAct_9fa48("3323") ? p.status !== 'pending' : stryMutAct_9fa48("3322") ? false : stryMutAct_9fa48("3321") ? true : (stryCov_9fa48("3321", "3322", "3323"), p.status === 'pending'))).length);
        const expired = stryMutAct_9fa48("3325") ? uniqueProposals.length : (stryCov_9fa48("3325"), uniqueProposals.filter(stryMutAct_9fa48("3326") ? () => undefined : (stryCov_9fa48("3326"), p => stryMutAct_9fa48("3329") ? p.status !== 'expired' : stryMutAct_9fa48("3328") ? false : stryMutAct_9fa48("3327") ? true : (stryCov_9fa48("3327", "3328", "3329"), p.status === 'expired'))).length);
        const duplicatesRemoved = stryMutAct_9fa48("3331") ? (governanceActions?.length || 0) + total : (stryCov_9fa48("3331"), (stryMutAct_9fa48("3334") ? governanceActions?.length && 0 : stryMutAct_9fa48("3333") ? false : stryMutAct_9fa48("3332") ? true : (stryCov_9fa48("3332", "3333", "3334"), (stryMutAct_9fa48("3335") ? governanceActions.length : (stryCov_9fa48("3335"), governanceActions?.length)) || 0)) - total);
        return {
          total,
          approved,
          rejected,
          pending,
          expired,
          duplicatesRemoved
        };
      }
    }, stryMutAct_9fa48("3337") ? [] : (stryCov_9fa48("3337"), [uniqueProposals, governanceActions]));
    return {
      proposals: uniqueProposals,
      stats,
      isLoading,
      error,
      rawEventCount: stryMutAct_9fa48("3341") ? governanceActions?.length && 0 : stryMutAct_9fa48("3340") ? false : stryMutAct_9fa48("3339") ? true : (stryCov_9fa48("3339", "3340", "3341"), (stryMutAct_9fa48("3342") ? governanceActions.length : (stryCov_9fa48("3342"), governanceActions?.length)) || 0),
      // Expose data source info for the UI
      dataSource: stryMutAct_9fa48("3345") ? rawEventsResult?.source && null : stryMutAct_9fa48("3344") ? false : stryMutAct_9fa48("3343") ? true : (stryCov_9fa48("3343", "3344", "3345"), (stryMutAct_9fa48("3346") ? rawEventsResult.source : (stryCov_9fa48("3346"), rawEventsResult?.source)) || null),
      fromIndex: stryMutAct_9fa48("3349") ? rawEventsResult?.fromIndex && false : stryMutAct_9fa48("3348") ? false : stryMutAct_9fa48("3347") ? true : (stryCov_9fa48("3347", "3348", "3349"), (stryMutAct_9fa48("3350") ? rawEventsResult.fromIndex : (stryCov_9fa48("3350"), rawEventsResult?.fromIndex)) || (stryMutAct_9fa48("3351") ? true : (stryCov_9fa48("3351"), false))),
      indexedAt: stryMutAct_9fa48("3354") ? rawEventsResult?.indexedAt && null : stryMutAct_9fa48("3353") ? false : stryMutAct_9fa48("3352") ? true : (stryCov_9fa48("3352", "3353", "3354"), (stryMutAct_9fa48("3355") ? rawEventsResult.indexedAt : (stryCov_9fa48("3355"), rawEventsResult?.indexedAt)) || null)
    };
  }
}