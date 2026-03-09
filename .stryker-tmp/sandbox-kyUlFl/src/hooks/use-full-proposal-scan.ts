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
import { useState, useCallback, useRef, useEffect } from "react";
import { getDuckDBApiUrl } from "@/lib/backend-config";
interface Vote {
  svName: string;
  sv: string;
  accept: boolean;
  reasonUrl: string;
  reasonBody: string;
  castAt?: string;
}
interface Proposal {
  proposalKey: string;
  latestTimestamp: number;
  latestContractId: string;
  requester: string;
  actionType: string;
  actionDetails: any;
  reasonUrl: string;
  reasonBody: string;
  voteBefore: string;
  voteBeforeTimestamp: number;
  votes: Vote[];
  votesFor: number;
  votesAgainst: number;
  trackingCid: string | null;
  rawTimestamp: string;
  status: 'executed' | 'rejected' | 'expired' | 'in_progress'; // Ledger-derived status
}
interface Stats {
  total: number;
  byActionType: Record<string, number>;
  byStatus: {
    executed: number;
    rejected: number;
    expired: number;
    in_progress: number;
  };
}
interface FullProposalScanResponse {
  summary: {
    filesScanned: number;
    totalFilesInDataset: number;
    totalVoteRequests: number;
    uniqueProposals: number;
    rawMode?: boolean;
  };
  stats: Stats;
  proposals: Proposal[];
  rawVoteRequests?: any[];
  debug?: {
    dedupLog: any[];
    byKeySource: Record<string, number>;
    highMergeProposals: any[];
    sampleKeys: any[];
  };
}
interface ScanProgress {
  filesScanned: number;
  totalFiles: number;
  percent: number;
  uniqueProposals: number;
  totalVoteRequests: number;
  rawCount?: number;
}
interface ScanOptions {
  debug?: boolean;
  raw?: boolean;
  concurrency?: number;
  limit?: number;
}
export function useFullProposalScan(enabled: boolean = stryMutAct_9fa48("1945") ? true : (stryCov_9fa48("1945"), false), options: ScanOptions = {}) {
  if (stryMutAct_9fa48("1946")) {
    {}
  } else {
    stryCov_9fa48("1946");
    const [data, setData] = useState<FullProposalScanResponse | null>(null);
    const [progress, setProgress] = useState<ScanProgress | null>(null);
    const [isLoading, setIsLoading] = useState(stryMutAct_9fa48("1947") ? true : (stryCov_9fa48("1947"), false));
    const [error, setError] = useState<Error | null>(null);
    const eventSourceRef = useRef<EventSource | null>(null);
    const hasStartedRef = useRef(stryMutAct_9fa48("1948") ? true : (stryCov_9fa48("1948"), false));
    const stopScan = useCallback(() => {
      if (stryMutAct_9fa48("1949")) {
        {}
      } else {
        stryCov_9fa48("1949");
        if (stryMutAct_9fa48("1951") ? false : stryMutAct_9fa48("1950") ? true : (stryCov_9fa48("1950", "1951"), eventSourceRef.current)) {
          if (stryMutAct_9fa48("1952")) {
            {}
          } else {
            stryCov_9fa48("1952");
            eventSourceRef.current.close();
            eventSourceRef.current = null;
          }
        }
        setIsLoading(stryMutAct_9fa48("1953") ? true : (stryCov_9fa48("1953"), false));
      }
    }, stryMutAct_9fa48("1954") ? ["Stryker was here"] : (stryCov_9fa48("1954"), []));
    const startScan = useCallback(async () => {
      if (stryMutAct_9fa48("1955")) {
        {}
      } else {
        stryCov_9fa48("1955");
        if (stryMutAct_9fa48("1957") ? false : stryMutAct_9fa48("1956") ? true : (stryCov_9fa48("1956", "1957"), isLoading)) return;
        setIsLoading(stryMutAct_9fa48("1958") ? false : (stryCov_9fa48("1958"), true));
        setError(null);
        setProgress(null);
        setData(null);
        try {
          if (stryMutAct_9fa48("1959")) {
            {}
          } else {
            stryCov_9fa48("1959");
            const backendUrl = getDuckDBApiUrl();
            const params = new URLSearchParams();
            if (stryMutAct_9fa48("1961") ? false : stryMutAct_9fa48("1960") ? true : (stryCov_9fa48("1960", "1961"), options.debug)) params.append('debug', 'true');
            if (stryMutAct_9fa48("1965") ? false : stryMutAct_9fa48("1964") ? true : (stryCov_9fa48("1964", "1965"), options.raw)) params.append('raw', 'true');
            if (stryMutAct_9fa48("1969") ? false : stryMutAct_9fa48("1968") ? true : (stryCov_9fa48("1968", "1969"), options.concurrency)) params.append('concurrency', options.concurrency.toString());
            if (stryMutAct_9fa48("1972") ? false : stryMutAct_9fa48("1971") ? true : (stryCov_9fa48("1971", "1972"), options.limit)) params.append('limit', options.limit.toString());
            const queryString = params.toString();
            const url = `${backendUrl}/api/events/governance/proposals/stream${queryString ? '?' + queryString : ''}`;

            // Close any existing connection
            if (stryMutAct_9fa48("1978") ? false : stryMutAct_9fa48("1977") ? true : (stryCov_9fa48("1977", "1978"), eventSourceRef.current)) {
              if (stryMutAct_9fa48("1979")) {
                {}
              } else {
                stryCov_9fa48("1979");
                eventSourceRef.current.close();
              }
            }
            const eventSource = new EventSource(url);
            eventSourceRef.current = eventSource;
            eventSource.addEventListener('start', e => {
              if (stryMutAct_9fa48("1981")) {
                {}
              } else {
                stryCov_9fa48("1981");
                const data = JSON.parse(e.data);
                console.log('[SSE] Scan started:', data);
                setProgress({
                  filesScanned: 0,
                  totalFiles: data.totalFiles,
                  percent: 0,
                  uniqueProposals: 0,
                  totalVoteRequests: 0
                });
              }
            });
            eventSource.addEventListener('progress', e => {
              if (stryMutAct_9fa48("1985")) {
                {}
              } else {
                stryCov_9fa48("1985");
                const progressData = JSON.parse(e.data);
                setProgress(progressData);
              }
            });
            eventSource.addEventListener('complete', e => {
              if (stryMutAct_9fa48("1987")) {
                {}
              } else {
                stryCov_9fa48("1987");
                const result = JSON.parse(e.data);
                console.log('[SSE] Scan complete:', result.summary);
                setData(result);
                setProgress({
                  filesScanned: result.summary.filesScanned,
                  totalFiles: result.summary.totalFilesInDataset,
                  percent: 100,
                  uniqueProposals: result.summary.uniqueProposals,
                  totalVoteRequests: result.summary.totalVoteRequests
                });
                setIsLoading(stryMutAct_9fa48("1990") ? true : (stryCov_9fa48("1990"), false));
                eventSource.close();
                eventSourceRef.current = null;
              }
            });
            eventSource.addEventListener('error', e => {
              if (stryMutAct_9fa48("1992")) {
                {}
              } else {
                stryCov_9fa48("1992");
                console.error('[SSE] Error:', e);
                setError(new Error('SSE connection failed'));
                setIsLoading(stryMutAct_9fa48("1995") ? true : (stryCov_9fa48("1995"), false));
                eventSource.close();
                eventSourceRef.current = null;
              }
            });
            eventSource.onerror = () => {
              if (stryMutAct_9fa48("1996")) {
                {}
              } else {
                stryCov_9fa48("1996");
                // Only set error if we haven't completed
                if (stryMutAct_9fa48("1999") ? isLoading || !data : stryMutAct_9fa48("1998") ? false : stryMutAct_9fa48("1997") ? true : (stryCov_9fa48("1997", "1998", "1999"), isLoading && (stryMutAct_9fa48("2000") ? data : (stryCov_9fa48("2000"), !data)))) {
                  if (stryMutAct_9fa48("2001")) {
                    {}
                  } else {
                    stryCov_9fa48("2001");
                    setError(new Error('Connection to scan endpoint failed'));
                    setIsLoading(stryMutAct_9fa48("2003") ? true : (stryCov_9fa48("2003"), false));
                  }
                }
                eventSource.close();
                eventSourceRef.current = null;
              }
            };
          }
        } catch (err) {
          if (stryMutAct_9fa48("2004")) {
            {}
          } else {
            stryCov_9fa48("2004");
            console.error('Error starting scan:', err);
            setError(err instanceof Error ? err : new Error('Unknown error'));
            setIsLoading(stryMutAct_9fa48("2007") ? true : (stryCov_9fa48("2007"), false));
          }
        }
      }
    }, stryMutAct_9fa48("2008") ? [] : (stryCov_9fa48("2008"), [isLoading, data]));

    // Auto-start when enabled becomes true
    useEffect(() => {
      if (stryMutAct_9fa48("2009")) {
        {}
      } else {
        stryCov_9fa48("2009");
        if (stryMutAct_9fa48("2012") ? enabled && !hasStartedRef.current || !data : stryMutAct_9fa48("2011") ? false : stryMutAct_9fa48("2010") ? true : (stryCov_9fa48("2010", "2011", "2012"), (stryMutAct_9fa48("2014") ? enabled || !hasStartedRef.current : stryMutAct_9fa48("2013") ? true : (stryCov_9fa48("2013", "2014"), enabled && (stryMutAct_9fa48("2015") ? hasStartedRef.current : (stryCov_9fa48("2015"), !hasStartedRef.current)))) && (stryMutAct_9fa48("2016") ? data : (stryCov_9fa48("2016"), !data)))) {
          if (stryMutAct_9fa48("2017")) {
            {}
          } else {
            stryCov_9fa48("2017");
            hasStartedRef.current = stryMutAct_9fa48("2018") ? false : (stryCov_9fa48("2018"), true);
            startScan();
          }
        }
      }
    }, stryMutAct_9fa48("2019") ? [] : (stryCov_9fa48("2019"), [enabled, startScan, data]));

    // Cleanup on unmount
    useEffect(() => {
      if (stryMutAct_9fa48("2020")) {
        {}
      } else {
        stryCov_9fa48("2020");
        return () => {
          if (stryMutAct_9fa48("2021")) {
            {}
          } else {
            stryCov_9fa48("2021");
            if (stryMutAct_9fa48("2023") ? false : stryMutAct_9fa48("2022") ? true : (stryCov_9fa48("2022", "2023"), eventSourceRef.current)) {
              if (stryMutAct_9fa48("2024")) {
                {}
              } else {
                stryCov_9fa48("2024");
                eventSourceRef.current.close();
              }
            }
          }
        };
      }
    }, stryMutAct_9fa48("2025") ? ["Stryker was here"] : (stryCov_9fa48("2025"), []));
    return {
      data,
      progress,
      isLoading,
      error,
      refetch: startScan,
      stop: stopScan
    };
  }
}