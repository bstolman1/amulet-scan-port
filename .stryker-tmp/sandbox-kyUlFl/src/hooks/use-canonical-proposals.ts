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
import { useQuery } from "@tanstack/react-query";
import { apiFetch } from "@/lib/duckdb-api-client";
export interface CanonicalProposal {
  proposal_id: string | null;
  event_id: string;
  stable_id: string | null;
  contract_id: string;
  template_id: string | null;
  effective_at: string | null;
  status: "in_progress" | "accepted" | "rejected" | "expired" | string;
  is_closed: boolean;
  action_tag: string | null;
  action_value: Record<string, unknown> | null;
  requester: string | null;
  reason: string | Record<string, unknown> | null;
  reason_url: string | null;
  votes: Array<[string, {
    sv: string;
    accept: boolean;
    reason?: {
      body?: string;
      url?: string;
    };
  }]>;
  vote_count: number;
  accept_count: number;
  reject_count: number;
  vote_before: string | null;
  target_effective_at: string | null;
  tracking_cid: string | null;
  dso: string | null;
  semantic_key: string | null;
  action_subject: string | null;
  is_human: boolean;
  related_count: number;
  first_seen: string | null;
  last_seen: string | null;
}
export interface CanonicalProposalStats {
  rawEvents: number;
  lifecycleProposals: number;
  humanProposals: number;
  byStatus: {
    in_progress: number;
    accepted: number;
    rejected: number;
    expired: number;
  };
}
interface CanonicalProposalsResponse {
  proposals: CanonicalProposal[];
  total: number;
  stats: CanonicalProposalStats;
  source: string;
  indexedAt: string | null;
}
interface DedupeStatsResponse {
  totalRows: number;
  uniqueProposals: number;
  duplicateRows: number;
  duplicatePct: number;
  humanProposals: number;
  statusBreakdown: Array<{
    status: string;
    count: number;
  }>;
  explanation: {
    model: string;
    deduplication: string;
    duplicates: string;
    humanFilter: string;
  };
}

/**
 * Hook to fetch canonical governance proposals (deduplicated by proposal_id)
 * This is the PRIMARY hook for governance UIs matching explorer semantics.
 * 
 * Key concept: 1 governance proposal = 1 unique VoteRequest ID (proposal_id)
 * Multiple rows in vote_requests represent state updates, NOT separate proposals.
 */
export function useCanonicalProposals(options?: {
  limit?: number;
  offset?: number;
  status?: "all" | "active" | "accepted" | "rejected" | "expired" | "historical";
  humanOnly?: boolean;
}) {
  if (stryMutAct_9fa48("1371")) {
    {}
  } else {
    stryCov_9fa48("1371");
    const {
      limit = 500,
      offset = 0,
      status = "all",
      humanOnly = stryMutAct_9fa48("1373") ? false : (stryCov_9fa48("1373"), true)
    } = stryMutAct_9fa48("1376") ? options && {} : stryMutAct_9fa48("1375") ? false : stryMutAct_9fa48("1374") ? true : (stryCov_9fa48("1374", "1375", "1376"), options || {});
    return useQuery({
      queryKey: stryMutAct_9fa48("1378") ? [] : (stryCov_9fa48("1378"), ["canonicalProposals", limit, offset, status, humanOnly]),
      queryFn: async (): Promise<CanonicalProposalsResponse> => {
        if (stryMutAct_9fa48("1380")) {
          {}
        } else {
          stryCov_9fa48("1380");
          const params = new URLSearchParams({
            limit: String(limit),
            offset: String(offset),
            status,
            human: String(humanOnly)
          });
          const response = await apiFetch<CanonicalProposalsResponse>(`/api/events/canonical-proposals?${params}`);
          return response;
        }
      },
      staleTime: 30_000
    });
  }
}

/**
 * Hook to fetch canonical proposal statistics
 */
export function useCanonicalProposalStats() {
  if (stryMutAct_9fa48("1383")) {
    {}
  } else {
    stryCov_9fa48("1383");
    return useQuery({
      queryKey: stryMutAct_9fa48("1385") ? [] : (stryCov_9fa48("1385"), ["canonicalProposalStats"]),
      queryFn: async (): Promise<CanonicalProposalStats & {
        source: string;
        indexedAt: string | null;
      }> => {
        if (stryMutAct_9fa48("1387")) {
          {}
        } else {
          stryCov_9fa48("1387");
          return apiFetch("/api/events/canonical-proposals/stats");
        }
      },
      staleTime: 30_000
    });
  }
}

/**
 * Hook to fetch deduplication diagnostic stats
 * Answers: "How many unique proposals vs duplicate state updates?"
 */
export function useDedupeStats() {
  if (stryMutAct_9fa48("1389")) {
    {}
  } else {
    stryCov_9fa48("1389");
    return useQuery({
      queryKey: stryMutAct_9fa48("1391") ? [] : (stryCov_9fa48("1391"), ["dedupeStats"]),
      queryFn: async (): Promise<DedupeStatsResponse> => {
        if (stryMutAct_9fa48("1393")) {
          {}
        } else {
          stryCov_9fa48("1393");
          return apiFetch("/api/events/vote-requests/dedupe-stats");
        }
      },
      staleTime: 60_000
    });
  }
}

/**
 * Helper to parse action from canonical proposal
 */
export function parseCanonicalAction(proposal: CanonicalProposal): {
  title: string;
  actionType: string;
  actionDetails: Record<string, unknown> | null;
} {
  if (stryMutAct_9fa48("1395")) {
    {}
  } else {
    stryCov_9fa48("1395");
    const actionTag = stryMutAct_9fa48("1398") ? proposal.action_tag && "Unknown" : stryMutAct_9fa48("1397") ? false : stryMutAct_9fa48("1396") ? true : (stryCov_9fa48("1396", "1397", "1398"), proposal.action_tag || "Unknown");
    const actionValue = proposal.action_value;

    // Build human-readable title from action tag
    const title = stryMutAct_9fa48("1400") ? actionTag.replace(/^(SRARC_|ARC_|CRARC_|ARAC_)/, "").replace(/([A-Z])/g, " $1") : (stryCov_9fa48("1400"), actionTag.replace(stryMutAct_9fa48("1401") ? /(SRARC_|ARC_|CRARC_|ARAC_)/ : (stryCov_9fa48("1401"), /^(SRARC_|ARC_|CRARC_|ARAC_)/), "").replace(stryMutAct_9fa48("1403") ? /([^A-Z])/g : (stryCov_9fa48("1403"), /([A-Z])/g), " $1").trim());
    return {
      title,
      actionType: actionTag,
      actionDetails: actionValue
    };
  }
}

/**
 * Helper to parse votes from canonical proposal
 */
export function parseCanonicalVotes(proposal: CanonicalProposal): {
  votesFor: number;
  votesAgainst: number;
  votedSvs: Array<{
    party: string;
    sv: string;
    vote: "accept" | "reject" | "abstain";
    reason: string;
    reasonUrl: string;
    castAt: string | null;
  }>;
} {
  if (stryMutAct_9fa48("1406")) {
    {}
  } else {
    stryCov_9fa48("1406");
    const votes = stryMutAct_9fa48("1409") ? proposal.votes && [] : stryMutAct_9fa48("1408") ? false : stryMutAct_9fa48("1407") ? true : (stryCov_9fa48("1407", "1408", "1409"), proposal.votes || (stryMutAct_9fa48("1410") ? ["Stryker was here"] : (stryCov_9fa48("1410"), [])));
    const votedSvs: Array<{
      party: string;
      sv: string;
      vote: "accept" | "reject" | "abstain";
      reason: string;
      reasonUrl: string;
      castAt: string | null;
    }> = stryMutAct_9fa48("1411") ? ["Stryker was here"] : (stryCov_9fa48("1411"), []);
    let votesFor = 0;
    let votesAgainst = 0;
    const toPartyString = (key: unknown): string => {
      if (stryMutAct_9fa48("1412")) {
        {}
      } else {
        stryCov_9fa48("1412");
        if (stryMutAct_9fa48("1415") ? typeof key !== "string" : stryMutAct_9fa48("1414") ? false : stryMutAct_9fa48("1413") ? true : (stryCov_9fa48("1413", "1414", "1415"), typeof key === "string")) return key;
        if (stryMutAct_9fa48("1419") ? key || typeof key === "object" : stryMutAct_9fa48("1418") ? false : stryMutAct_9fa48("1417") ? true : (stryCov_9fa48("1417", "1418", "1419"), key && (stryMutAct_9fa48("1421") ? typeof key !== "object" : stryMutAct_9fa48("1420") ? true : (stryCov_9fa48("1420", "1421"), typeof key === "object")))) {
          if (stryMutAct_9fa48("1423")) {
            {}
          } else {
            stryCov_9fa48("1423");
            const anyKey = key as any;
            return String(stryMutAct_9fa48("1426") ? (anyKey.party || anyKey.text || anyKey.sv || anyKey.voter) && "Unknown" : stryMutAct_9fa48("1425") ? false : stryMutAct_9fa48("1424") ? true : (stryCov_9fa48("1424", "1425", "1426"), (stryMutAct_9fa48("1428") ? (anyKey.party || anyKey.text || anyKey.sv) && anyKey.voter : stryMutAct_9fa48("1427") ? false : (stryCov_9fa48("1427", "1428"), (stryMutAct_9fa48("1430") ? (anyKey.party || anyKey.text) && anyKey.sv : stryMutAct_9fa48("1429") ? false : (stryCov_9fa48("1429", "1430"), (stryMutAct_9fa48("1432") ? anyKey.party && anyKey.text : stryMutAct_9fa48("1431") ? false : (stryCov_9fa48("1431", "1432"), anyKey.party || anyKey.text)) || anyKey.sv)) || anyKey.voter)) || "Unknown"));
          }
        }
        return "Unknown";
      }
    };
    for (const vote of votes) {
      if (stryMutAct_9fa48("1435")) {
        {}
      } else {
        stryCov_9fa48("1435");
        const tuple = Array.isArray(vote) ? vote : stryMutAct_9fa48("1436") ? [] : (stryCov_9fa48("1436"), [vote as any, null]);
        const svKey = tuple[0];
        const voteData = tuple[1] as any;
        const party = toPartyString(svKey);
        const isAccept = stryMutAct_9fa48("1439") ? voteData?.accept !== true : stryMutAct_9fa48("1438") ? false : stryMutAct_9fa48("1437") ? true : (stryCov_9fa48("1437", "1438", "1439"), (stryMutAct_9fa48("1440") ? voteData.accept : (stryCov_9fa48("1440"), voteData?.accept)) === (stryMutAct_9fa48("1441") ? false : (stryCov_9fa48("1441"), true)));
        const isReject = stryMutAct_9fa48("1444") ? voteData?.accept !== false : stryMutAct_9fa48("1443") ? false : stryMutAct_9fa48("1442") ? true : (stryCov_9fa48("1442", "1443", "1444"), (stryMutAct_9fa48("1445") ? voteData.accept : (stryCov_9fa48("1445"), voteData?.accept)) === (stryMutAct_9fa48("1446") ? true : (stryCov_9fa48("1446"), false)));
        if (stryMutAct_9fa48("1448") ? false : stryMutAct_9fa48("1447") ? true : (stryCov_9fa48("1447", "1448"), isAccept)) stryMutAct_9fa48("1449") ? votesFor-- : (stryCov_9fa48("1449"), votesFor++);else if (stryMutAct_9fa48("1451") ? false : stryMutAct_9fa48("1450") ? true : (stryCov_9fa48("1450", "1451"), isReject)) stryMutAct_9fa48("1452") ? votesAgainst-- : (stryCov_9fa48("1452"), votesAgainst++);
        votedSvs.push({
          party,
          sv: String(stryMutAct_9fa48("1456") ? voteData?.sv && party : stryMutAct_9fa48("1455") ? false : stryMutAct_9fa48("1454") ? true : (stryCov_9fa48("1454", "1455", "1456"), (stryMutAct_9fa48("1457") ? voteData.sv : (stryCov_9fa48("1457"), voteData?.sv)) || party)),
          vote: isAccept ? "accept" : isReject ? "reject" : "abstain",
          reason: String(stryMutAct_9fa48("1463") ? voteData?.reason?.body && "" : stryMutAct_9fa48("1462") ? false : stryMutAct_9fa48("1461") ? true : (stryCov_9fa48("1461", "1462", "1463"), (stryMutAct_9fa48("1465") ? voteData.reason?.body : stryMutAct_9fa48("1464") ? voteData?.reason.body : (stryCov_9fa48("1464", "1465"), voteData?.reason?.body)) || "")),
          reasonUrl: String(stryMutAct_9fa48("1469") ? voteData?.reason?.url && "" : stryMutAct_9fa48("1468") ? false : stryMutAct_9fa48("1467") ? true : (stryCov_9fa48("1467", "1468", "1469"), (stryMutAct_9fa48("1471") ? voteData.reason?.url : stryMutAct_9fa48("1470") ? voteData?.reason.url : (stryCov_9fa48("1470", "1471"), voteData?.reason?.url)) || "")),
          castAt: null // Not available in canonical model
        });
      }
    }
    return {
      votesFor,
      votesAgainst,
      votedSvs
    };
  }
}

/**
 * Helper to get status display properties
 */
export function getProposalStatusDisplay(status: string): {
  label: string;
  color: string;
  bgColor: string;
} {
  if (stryMutAct_9fa48("1474")) {
    {}
  } else {
    stryCov_9fa48("1474");
    switch (status) {
      case "accepted":
        if (stryMutAct_9fa48("1475")) {} else {
          stryCov_9fa48("1475");
          return {
            label: "Accepted",
            color: "text-success",
            bgColor: "bg-success/10"
          };
        }
      case "rejected":
        if (stryMutAct_9fa48("1481")) {} else {
          stryCov_9fa48("1481");
          return {
            label: "Rejected",
            color: "text-destructive",
            bgColor: "bg-destructive/10"
          };
        }
      case "expired":
        if (stryMutAct_9fa48("1487")) {} else {
          stryCov_9fa48("1487");
          return {
            label: "Expired",
            color: "text-muted-foreground",
            bgColor: "bg-muted"
          };
        }
      case "in_progress":
        if (stryMutAct_9fa48("1493")) {} else {
          stryCov_9fa48("1493");
          return {
            label: "In Progress",
            color: "text-warning",
            bgColor: "bg-warning/10"
          };
        }
      default:
        if (stryMutAct_9fa48("1499")) {} else {
          stryCov_9fa48("1499");
          return {
            label: stryMutAct_9fa48("1503") ? status && "Unknown" : stryMutAct_9fa48("1502") ? false : stryMutAct_9fa48("1501") ? true : (stryCov_9fa48("1501", "1502", "1503"), status || "Unknown"),
            color: "text-muted-foreground",
            bgColor: "bg-muted"
          };
        }
    }
  }
}