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
const SCAN_API_BASE = "https://scan.sv-1.global.canton.network.sync.global/api/scan";
export interface VoteResultRequest {
  actionName?: string;
  accepted?: boolean;
  requester?: string;
  effectiveFrom?: string;
  effectiveTo?: string;
  limit?: number;
}
export interface VoteResult {
  request_tracking_cid: string;
  request: {
    requester: string;
    action: {
      tag: string;
      value: any;
    };
    reason: {
      url: string;
      body: string;
    };
    vote_before: string;
    votes: Array<[string, {
      sv: string;
      accept: boolean;
      reason: {
        url: string;
        body: string;
      };
    }]>;
    expires_at: string;
  };
  completed_at: string;
  offboarded_voters: string[];
  abstaining_voters: string[];
  outcome: {
    tag: "VRO_Accepted" | "VRO_Rejected" | "VRO_Expired";
    value?: any;
  };
}
export interface VoteResultsResponse {
  dso_rules_vote_results: VoteResult[];
}

// Parsed vote result for display
export interface ParsedVoteResult {
  id: string;
  trackingCid: string;
  actionType: string;
  actionTitle: string;
  actionDetails: any;
  requester: string;
  reasonBody: string;
  reasonUrl: string;
  voteBefore: string;
  completedAt: string;
  expiresAt: string;
  outcome: "accepted" | "rejected" | "expired";
  votesFor: number;
  votesAgainst: number;
  totalVotes: number;
  votes: Array<{
    svName: string;
    svParty: string;
    accept: boolean;
    reasonUrl: string;
    reasonBody: string;
  }>;
  abstainers: string[];
  offboarded: string[];
}

// Parse action tag into readable title
function parseActionTitle(tag: string): string {
  if (stryMutAct_9fa48("2641")) {
    {}
  } else {
    stryCov_9fa48("2641");
    return stryMutAct_9fa48("2642") ? tag.replace(/^(SRARC_|ARC_|CRARC_|ARAC_)/, "").replace(/([A-Z])/g, " $1") : (stryCov_9fa48("2642"), tag.replace(stryMutAct_9fa48("2643") ? /(SRARC_|ARC_|CRARC_|ARAC_)/ : (stryCov_9fa48("2643"), /^(SRARC_|ARC_|CRARC_|ARAC_)/), "").replace(stryMutAct_9fa48("2645") ? /([^A-Z])/g : (stryCov_9fa48("2645"), /([A-Z])/g), " $1").trim());
  }
}

// Parse date from various formats (ISO string, DAML objects, protobuf timestamps, etc.)
function parseDate(value: any): string {
  if (stryMutAct_9fa48("2647")) {
    {}
  } else {
    stryCov_9fa48("2647");
    if (stryMutAct_9fa48("2650") ? false : stryMutAct_9fa48("2649") ? true : stryMutAct_9fa48("2648") ? value : (stryCov_9fa48("2648", "2649", "2650"), !value)) return "";

    // Already a string
    if (stryMutAct_9fa48("2654") ? typeof value !== "string" : stryMutAct_9fa48("2653") ? false : stryMutAct_9fa48("2652") ? true : (stryCov_9fa48("2652", "2653", "2654"), typeof value === "string")) return value;

    // Number (assume milliseconds)
    if (stryMutAct_9fa48("2658") ? typeof value !== "number" : stryMutAct_9fa48("2657") ? false : stryMutAct_9fa48("2656") ? true : (stryCov_9fa48("2656", "2657", "2658"), typeof value === "number")) return new Date(value).toISOString();
    if (stryMutAct_9fa48("2662") ? typeof value !== "object" : stryMutAct_9fa48("2661") ? false : stryMutAct_9fa48("2660") ? true : (stryCov_9fa48("2660", "2661", "2662"), typeof value === "object")) {
      if (stryMutAct_9fa48("2664")) {
        {}
      } else {
        stryCov_9fa48("2664");
        // DAML style: { microsecondsSinceEpoch: "..." | number }
        if (stryMutAct_9fa48("2667") ? value.microsecondsSinceEpoch == null : stryMutAct_9fa48("2666") ? false : stryMutAct_9fa48("2665") ? true : (stryCov_9fa48("2665", "2666", "2667"), value.microsecondsSinceEpoch != null)) {
          if (stryMutAct_9fa48("2668")) {
            {}
          } else {
            stryCov_9fa48("2668");
            const micros = Number(value.microsecondsSinceEpoch);
            if (stryMutAct_9fa48("2671") ? false : stryMutAct_9fa48("2670") ? true : stryMutAct_9fa48("2669") ? Number.isNaN(micros) : (stryCov_9fa48("2669", "2670", "2671"), !Number.isNaN(micros))) return new Date(stryMutAct_9fa48("2672") ? micros * 1000 : (stryCov_9fa48("2672"), micros / 1000)).toISOString();
          }
        }

        // Alternative epoch seconds: { unixtime: "..." | number }
        if (stryMutAct_9fa48("2675") ? value.unixtime == null : stryMutAct_9fa48("2674") ? false : stryMutAct_9fa48("2673") ? true : (stryCov_9fa48("2673", "2674", "2675"), value.unixtime != null)) {
          if (stryMutAct_9fa48("2676")) {
            {}
          } else {
            stryCov_9fa48("2676");
            const seconds = Number(value.unixtime);
            if (stryMutAct_9fa48("2679") ? false : stryMutAct_9fa48("2678") ? true : stryMutAct_9fa48("2677") ? Number.isNaN(seconds) : (stryCov_9fa48("2677", "2678", "2679"), !Number.isNaN(seconds))) return new Date(stryMutAct_9fa48("2680") ? seconds / 1000 : (stryCov_9fa48("2680"), seconds * 1000)).toISOString();
          }
        }

        // Protobuf Timestamp: { seconds: "..."|number, nanos?: number }
        if (stryMutAct_9fa48("2683") ? value.seconds == null : stryMutAct_9fa48("2682") ? false : stryMutAct_9fa48("2681") ? true : (stryCov_9fa48("2681", "2682", "2683"), value.seconds != null)) {
          if (stryMutAct_9fa48("2684")) {
            {}
          } else {
            stryCov_9fa48("2684");
            const seconds = Number(value.seconds);
            const nanos = (stryMutAct_9fa48("2687") ? value.nanos == null : stryMutAct_9fa48("2686") ? false : stryMutAct_9fa48("2685") ? true : (stryCov_9fa48("2685", "2686", "2687"), value.nanos != null)) ? Number(value.nanos) : 0;
            if (stryMutAct_9fa48("2690") ? false : stryMutAct_9fa48("2689") ? true : stryMutAct_9fa48("2688") ? Number.isNaN(seconds) : (stryCov_9fa48("2688", "2689", "2690"), !Number.isNaN(seconds))) return new Date(stryMutAct_9fa48("2691") ? seconds * 1000 - Math.floor(nanos / 1e6) : (stryCov_9fa48("2691"), (stryMutAct_9fa48("2692") ? seconds / 1000 : (stryCov_9fa48("2692"), seconds * 1000)) + Math.floor(stryMutAct_9fa48("2693") ? nanos * 1e6 : (stryCov_9fa48("2693"), nanos / 1e6)))).toISOString();
          }
        }

        // Common wrappers: { value: "2025-..." } or { timestamp: "..." }
        if (stryMutAct_9fa48("2696") ? typeof value.value !== "string" : stryMutAct_9fa48("2695") ? false : stryMutAct_9fa48("2694") ? true : (stryCov_9fa48("2694", "2695", "2696"), typeof value.value === "string")) return value.value;
        if (stryMutAct_9fa48("2700") ? typeof value.timestamp !== "string" : stryMutAct_9fa48("2699") ? false : stryMutAct_9fa48("2698") ? true : (stryCov_9fa48("2698", "2699", "2700"), typeof value.timestamp === "string")) return value.timestamp;
        if (stryMutAct_9fa48("2704") ? typeof value.iso !== "string" : stryMutAct_9fa48("2703") ? false : stryMutAct_9fa48("2702") ? true : (stryCov_9fa48("2702", "2703", "2704"), typeof value.iso === "string")) return value.iso;
      }
    }
    return "";
  }
}

// Parse vote results into display format
function parseVoteResults(results: VoteResult[]): ParsedVoteResult[] {
  if (stryMutAct_9fa48("2707")) {
    {}
  } else {
    stryCov_9fa48("2707");
    if (stryMutAct_9fa48("2710") ? !results && !Array.isArray(results) : stryMutAct_9fa48("2709") ? false : stryMutAct_9fa48("2708") ? true : (stryCov_9fa48("2708", "2709", "2710"), (stryMutAct_9fa48("2711") ? results : (stryCov_9fa48("2711"), !results)) || (stryMutAct_9fa48("2712") ? Array.isArray(results) : (stryCov_9fa48("2712"), !Array.isArray(results))))) return stryMutAct_9fa48("2713") ? ["Stryker was here"] : (stryCov_9fa48("2713"), []);
    return results.map(result => {
      if (stryMutAct_9fa48("2714")) {
        {}
      } else {
        stryCov_9fa48("2714");
        // Safely access nested properties with type coercion for safety
        const request = (result?.request || {}) as any;
        const action = stryMutAct_9fa48("2717") ? request?.action && {
          tag: "Unknown",
          value: null
        } : stryMutAct_9fa48("2716") ? false : stryMutAct_9fa48("2715") ? true : (stryCov_9fa48("2715", "2716", "2717"), (stryMutAct_9fa48("2718") ? request.action : (stryCov_9fa48("2718"), request?.action)) || {
          tag: "Unknown",
          value: null
        });
        const votes = stryMutAct_9fa48("2723") ? (request?.votes || request?.Votes) && [] : stryMutAct_9fa48("2722") ? false : stryMutAct_9fa48("2721") ? true : (stryCov_9fa48("2721", "2722", "2723"), (stryMutAct_9fa48("2725") ? request?.votes && request?.Votes : stryMutAct_9fa48("2724") ? false : (stryCov_9fa48("2724", "2725"), (stryMutAct_9fa48("2726") ? request.votes : (stryCov_9fa48("2726"), request?.votes)) || (stryMutAct_9fa48("2727") ? request.Votes : (stryCov_9fa48("2727"), request?.Votes)))) || (stryMutAct_9fa48("2728") ? ["Stryker was here"] : (stryCov_9fa48("2728"), [])));
        const trackingCid = stryMutAct_9fa48("2731") ? (result?.request_tracking_cid || (result as any)?.requestTrackingCid || (result as any)?.tracking_cid || (result as any)?.trackingCid || request?.trackingCid) && "" : stryMutAct_9fa48("2730") ? false : stryMutAct_9fa48("2729") ? true : (stryCov_9fa48("2729", "2730", "2731"), (stryMutAct_9fa48("2733") ? (result?.request_tracking_cid || (result as any)?.requestTrackingCid || (result as any)?.tracking_cid || (result as any)?.trackingCid) && request?.trackingCid : stryMutAct_9fa48("2732") ? false : (stryCov_9fa48("2732", "2733"), (stryMutAct_9fa48("2735") ? (result?.request_tracking_cid || (result as any)?.requestTrackingCid || (result as any)?.tracking_cid) && (result as any)?.trackingCid : stryMutAct_9fa48("2734") ? false : (stryCov_9fa48("2734", "2735"), (stryMutAct_9fa48("2737") ? (result?.request_tracking_cid || (result as any)?.requestTrackingCid) && (result as any)?.tracking_cid : stryMutAct_9fa48("2736") ? false : (stryCov_9fa48("2736", "2737"), (stryMutAct_9fa48("2739") ? result?.request_tracking_cid && (result as any)?.requestTrackingCid : stryMutAct_9fa48("2738") ? false : (stryCov_9fa48("2738", "2739"), (stryMutAct_9fa48("2740") ? result.request_tracking_cid : (stryCov_9fa48("2740"), result?.request_tracking_cid)) || (stryMutAct_9fa48("2741") ? (result as any).requestTrackingCid : (stryCov_9fa48("2741"), (result as any)?.requestTrackingCid)))) || (stryMutAct_9fa48("2742") ? (result as any).tracking_cid : (stryCov_9fa48("2742"), (result as any)?.tracking_cid)))) || (stryMutAct_9fa48("2743") ? (result as any).trackingCid : (stryCov_9fa48("2743"), (result as any)?.trackingCid)))) || (stryMutAct_9fa48("2744") ? request.trackingCid : (stryCov_9fa48("2744"), request?.trackingCid)))) || "");
        let votesFor = 0;
        let votesAgainst = 0;
        const parsedVotes: ParsedVoteResult["votes"] = stryMutAct_9fa48("2746") ? ["Stryker was here"] : (stryCov_9fa48("2746"), []);
        for (const vote of votes) {
          if (stryMutAct_9fa48("2747")) {
            {}
          } else {
            stryCov_9fa48("2747");
            // Handle both array format [svName, voteData] and object format
            const [svName, voteData] = Array.isArray(vote) ? vote : stryMutAct_9fa48("2748") ? [] : (stryCov_9fa48("2748"), [stryMutAct_9fa48("2751") ? vote?.sv && "Unknown" : stryMutAct_9fa48("2750") ? false : stryMutAct_9fa48("2749") ? true : (stryCov_9fa48("2749", "2750", "2751"), (stryMutAct_9fa48("2752") ? vote.sv : (stryCov_9fa48("2752"), vote?.sv)) || "Unknown"), vote]);
            if (stryMutAct_9fa48("2756") ? voteData.accept : stryMutAct_9fa48("2755") ? false : stryMutAct_9fa48("2754") ? true : (stryCov_9fa48("2754", "2755", "2756"), voteData?.accept)) {
              if (stryMutAct_9fa48("2757")) {
                {}
              } else {
                stryCov_9fa48("2757");
                stryMutAct_9fa48("2758") ? votesFor-- : (stryCov_9fa48("2758"), votesFor++);
              }
            } else {
              if (stryMutAct_9fa48("2759")) {
                {}
              } else {
                stryCov_9fa48("2759");
                stryMutAct_9fa48("2760") ? votesAgainst-- : (stryCov_9fa48("2760"), votesAgainst++);
              }
            }
            parsedVotes.push({
              svName: stryMutAct_9fa48("2764") ? svName && "Unknown" : stryMutAct_9fa48("2763") ? false : stryMutAct_9fa48("2762") ? true : (stryCov_9fa48("2762", "2763", "2764"), svName || "Unknown"),
              svParty: stryMutAct_9fa48("2768") ? voteData?.sv && "" : stryMutAct_9fa48("2767") ? false : stryMutAct_9fa48("2766") ? true : (stryCov_9fa48("2766", "2767", "2768"), (stryMutAct_9fa48("2769") ? voteData.sv : (stryCov_9fa48("2769"), voteData?.sv)) || ""),
              accept: stryMutAct_9fa48("2771") ? voteData?.accept && false : (stryCov_9fa48("2771"), (stryMutAct_9fa48("2772") ? voteData.accept : (stryCov_9fa48("2772"), voteData?.accept)) ?? (stryMutAct_9fa48("2773") ? true : (stryCov_9fa48("2773"), false))),
              reasonUrl: stryMutAct_9fa48("2776") ? voteData?.reason?.url && "" : stryMutAct_9fa48("2775") ? false : stryMutAct_9fa48("2774") ? true : (stryCov_9fa48("2774", "2775", "2776"), (stryMutAct_9fa48("2778") ? voteData.reason?.url : stryMutAct_9fa48("2777") ? voteData?.reason.url : (stryCov_9fa48("2777", "2778"), voteData?.reason?.url)) || ""),
              reasonBody: stryMutAct_9fa48("2782") ? voteData?.reason?.body && "" : stryMutAct_9fa48("2781") ? false : stryMutAct_9fa48("2780") ? true : (stryCov_9fa48("2780", "2781", "2782"), (stryMutAct_9fa48("2784") ? voteData.reason?.body : stryMutAct_9fa48("2783") ? voteData?.reason.body : (stryCov_9fa48("2783", "2784"), voteData?.reason?.body)) || "")
            });
          }
        }
        let outcome: ParsedVoteResult["outcome"] = "expired";
        const outcomeTag = stryMutAct_9fa48("2789") ? result?.outcome?.tag && "" : stryMutAct_9fa48("2788") ? false : stryMutAct_9fa48("2787") ? true : (stryCov_9fa48("2787", "2788", "2789"), (stryMutAct_9fa48("2791") ? result.outcome?.tag : stryMutAct_9fa48("2790") ? result?.outcome.tag : (stryCov_9fa48("2790", "2791"), result?.outcome?.tag)) || "");
        if (stryMutAct_9fa48("2795") ? outcomeTag !== "VRO_Accepted" : stryMutAct_9fa48("2794") ? false : stryMutAct_9fa48("2793") ? true : (stryCov_9fa48("2793", "2794", "2795"), outcomeTag === "VRO_Accepted")) outcome = "accepted";else if (stryMutAct_9fa48("2800") ? outcomeTag !== "VRO_Rejected" : stryMutAct_9fa48("2799") ? false : stryMutAct_9fa48("2798") ? true : (stryCov_9fa48("2798", "2799", "2800"), outcomeTag === "VRO_Rejected")) outcome = "rejected";
        return {
          id: trackingCid ? stryMutAct_9fa48("2804") ? trackingCid : (stryCov_9fa48("2804"), trackingCid.slice(0, 12)) : "unknown",
          trackingCid: trackingCid,
          actionType: stryMutAct_9fa48("2808") ? action?.tag && "Unknown" : stryMutAct_9fa48("2807") ? false : stryMutAct_9fa48("2806") ? true : (stryCov_9fa48("2806", "2807", "2808"), (stryMutAct_9fa48("2809") ? action.tag : (stryCov_9fa48("2809"), action?.tag)) || "Unknown"),
          actionTitle: parseActionTitle(stryMutAct_9fa48("2813") ? action?.tag && "Unknown" : stryMutAct_9fa48("2812") ? false : stryMutAct_9fa48("2811") ? true : (stryCov_9fa48("2811", "2812", "2813"), (stryMutAct_9fa48("2814") ? action.tag : (stryCov_9fa48("2814"), action?.tag)) || "Unknown")),
          actionDetails: stryMutAct_9fa48("2816") ? action.value : (stryCov_9fa48("2816"), action?.value),
          requester: stryMutAct_9fa48("2819") ? request?.requester && "" : stryMutAct_9fa48("2818") ? false : stryMutAct_9fa48("2817") ? true : (stryCov_9fa48("2817", "2818", "2819"), (stryMutAct_9fa48("2820") ? request.requester : (stryCov_9fa48("2820"), request?.requester)) || ""),
          reasonBody: stryMutAct_9fa48("2824") ? request?.reason?.body && "" : stryMutAct_9fa48("2823") ? false : stryMutAct_9fa48("2822") ? true : (stryCov_9fa48("2822", "2823", "2824"), (stryMutAct_9fa48("2826") ? request.reason?.body : stryMutAct_9fa48("2825") ? request?.reason.body : (stryCov_9fa48("2825", "2826"), request?.reason?.body)) || ""),
          reasonUrl: stryMutAct_9fa48("2830") ? request?.reason?.url && "" : stryMutAct_9fa48("2829") ? false : stryMutAct_9fa48("2828") ? true : (stryCov_9fa48("2828", "2829", "2830"), (stryMutAct_9fa48("2832") ? request.reason?.url : stryMutAct_9fa48("2831") ? request?.reason.url : (stryCov_9fa48("2831", "2832"), request?.reason?.url)) || ""),
          voteBefore: parseDate(stryMutAct_9fa48("2834") ? (request?.vote_before ?? request?.voteBefore) && (request?.voteBefore as any)?.value : (stryCov_9fa48("2834"), (stryMutAct_9fa48("2835") ? request?.vote_before && request?.voteBefore : (stryCov_9fa48("2835"), (stryMutAct_9fa48("2836") ? request.vote_before : (stryCov_9fa48("2836"), request?.vote_before)) ?? (stryMutAct_9fa48("2837") ? request.voteBefore : (stryCov_9fa48("2837"), request?.voteBefore)))) ?? (stryMutAct_9fa48("2838") ? (request?.voteBefore as any).value : (stryCov_9fa48("2838"), (request?.voteBefore as any)?.value)))),
          completedAt: parseDate(stryMutAct_9fa48("2839") ? (result as any)?.completed_at && (result as any)?.completedAt : (stryCov_9fa48("2839"), (stryMutAct_9fa48("2840") ? (result as any).completed_at : (stryCov_9fa48("2840"), (result as any)?.completed_at)) ?? (stryMutAct_9fa48("2841") ? (result as any).completedAt : (stryCov_9fa48("2841"), (result as any)?.completedAt)))),
          expiresAt: parseDate(stryMutAct_9fa48("2842") ? request?.expires_at && request?.expiresAt : (stryCov_9fa48("2842"), (stryMutAct_9fa48("2843") ? request.expires_at : (stryCov_9fa48("2843"), request?.expires_at)) ?? (stryMutAct_9fa48("2844") ? request.expiresAt : (stryCov_9fa48("2844"), request?.expiresAt)))),
          outcome,
          votesFor,
          votesAgainst,
          totalVotes: stryMutAct_9fa48("2845") ? votesFor - votesAgainst : (stryCov_9fa48("2845"), votesFor + votesAgainst),
          votes: parsedVotes,
          abstainers: stryMutAct_9fa48("2848") ? result?.abstaining_voters && [] : stryMutAct_9fa48("2847") ? false : stryMutAct_9fa48("2846") ? true : (stryCov_9fa48("2846", "2847", "2848"), (stryMutAct_9fa48("2849") ? result.abstaining_voters : (stryCov_9fa48("2849"), result?.abstaining_voters)) || (stryMutAct_9fa48("2850") ? ["Stryker was here"] : (stryCov_9fa48("2850"), []))),
          offboarded: stryMutAct_9fa48("2853") ? result?.offboarded_voters && [] : stryMutAct_9fa48("2852") ? false : stryMutAct_9fa48("2851") ? true : (stryCov_9fa48("2851", "2852", "2853"), (stryMutAct_9fa48("2854") ? result.offboarded_voters : (stryCov_9fa48("2854"), result?.offboarded_voters)) || (stryMutAct_9fa48("2855") ? ["Stryker was here"] : (stryCov_9fa48("2855"), [])))
        };
      }
    });
  }
}
export function useScanVoteResults(request: VoteResultRequest = {}) {
  if (stryMutAct_9fa48("2856")) {
    {}
  } else {
    stryCov_9fa48("2856");
    return useQuery({
      queryKey: stryMutAct_9fa48("2858") ? [] : (stryCov_9fa48("2858"), ["scanVoteResults", request]),
      queryFn: async (): Promise<ParsedVoteResult[]> => {
        if (stryMutAct_9fa48("2860")) {
          {}
        } else {
          stryCov_9fa48("2860");
          const res = await fetch(`${SCAN_API_BASE}/v0/admin/sv/voteresults`, {
            method: "POST",
            headers: {
              "Content-Type": "application/json"
            },
            body: JSON.stringify({
              ...request,
              limit: stryMutAct_9fa48("2867") ? request.limit && 500 : (stryCov_9fa48("2867"), request.limit ?? 500)
            })
          });
          if (stryMutAct_9fa48("2870") ? false : stryMutAct_9fa48("2869") ? true : stryMutAct_9fa48("2868") ? res.ok : (stryCov_9fa48("2868", "2869", "2870"), !res.ok)) {
            if (stryMutAct_9fa48("2871")) {
              {}
            } else {
              stryCov_9fa48("2871");
              throw new Error(`Failed to fetch vote results: ${res.status}`);
            }
          }
          const data: VoteResultsResponse = await res.json();
          const parsed = parseVoteResults(stryMutAct_9fa48("2875") ? data.dso_rules_vote_results && [] : stryMutAct_9fa48("2874") ? false : stryMutAct_9fa48("2873") ? true : (stryCov_9fa48("2873", "2874", "2875"), data.dso_rules_vote_results || (stryMutAct_9fa48("2876") ? ["Stryker was here"] : (stryCov_9fa48("2876"), []))));
          // Sort by completedAt DESC (most recent first)
          return stryMutAct_9fa48("2877") ? parsed : (stryCov_9fa48("2877"), parsed.sort((a, b) => {
            if (stryMutAct_9fa48("2878")) {
              {}
            } else {
              stryCov_9fa48("2878");
              const dateA = a.completedAt ? new Date(a.completedAt).getTime() : 0;
              const dateB = b.completedAt ? new Date(b.completedAt).getTime() : 0;
              return stryMutAct_9fa48("2879") ? dateB + dateA : (stryCov_9fa48("2879"), dateB - dateA);
            }
          }));
        }
      },
      staleTime: 60_000,
      // 1 minute
      retry: 2
    });
  }
}

// Hook to fetch all governance history (no filters)
export function useGovernanceVoteHistory(limit = 500) {
  if (stryMutAct_9fa48("2880")) {
    {}
  } else {
    stryCov_9fa48("2880");
    return useScanVoteResults({
      limit
    });
  }
}