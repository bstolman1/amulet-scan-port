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
}

interface Stats {
  total: number;
  byActionType: Record<string, number>;
  byStatus: {
    approved: number;
    rejected: number;
    pending: number;
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
  filesPerSec?: number;
  rawCount?: number;
}

interface ScanOptions {
  debug?: boolean;
  raw?: boolean;
  concurrency?: number;
  limit?: number;
}

export function useFullProposalScan(enabled: boolean = false, options: ScanOptions = {}) {
  const [data, setData] = useState<FullProposalScanResponse | null>(null);
  const [progress, setProgress] = useState<ScanProgress | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const eventSourceRef = useRef<EventSource | null>(null);
  const hasStartedRef = useRef(false);

  const stopScan = useCallback(() => {
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
    }
    setIsLoading(false);
  }, []);

  const startScan = useCallback(async () => {
    if (isLoading) return;
    
    setIsLoading(true);
    setError(null);
    setProgress(null);
    setData(null);

    try {
      const backendUrl = getDuckDBApiUrl();
      const params = new URLSearchParams();
      if (options.debug) params.append('debug', 'true');
      if (options.raw) params.append('raw', 'true');
      if (options.concurrency) params.append('concurrency', options.concurrency.toString());
      if (options.limit) params.append('limit', options.limit.toString());
      const queryString = params.toString();
      const url = `${backendUrl}/api/events/governance/proposals/stream${queryString ? '?' + queryString : ''}`;
      
      // Close any existing connection
      if (eventSourceRef.current) {
        eventSourceRef.current.close();
      }

      const eventSource = new EventSource(url);
      eventSourceRef.current = eventSource;

      eventSource.addEventListener('start', (e) => {
        const data = JSON.parse(e.data);
        console.log('[SSE] Scan started:', data);
        setProgress({
          filesScanned: 0,
          totalFiles: data.totalFiles,
          percent: 0,
          uniqueProposals: 0,
          totalVoteRequests: 0,
        });
      });

      eventSource.addEventListener('progress', (e) => {
        const progressData = JSON.parse(e.data);
        setProgress(progressData);
      });

      eventSource.addEventListener('complete', (e) => {
        const result = JSON.parse(e.data);
        console.log('[SSE] Scan complete:', result.summary);
        setData(result);
        setProgress({
          filesScanned: result.summary.filesScanned,
          totalFiles: result.summary.totalFilesInDataset,
          percent: 100,
          uniqueProposals: result.summary.uniqueProposals,
          totalVoteRequests: result.summary.totalVoteRequests,
        });
        setIsLoading(false);
        eventSource.close();
        eventSourceRef.current = null;
      });

      eventSource.addEventListener('error', (e) => {
        console.error('[SSE] Error:', e);
        setError(new Error('SSE connection failed'));
        setIsLoading(false);
        eventSource.close();
        eventSourceRef.current = null;
      });

      eventSource.onerror = () => {
        // Only set error if we haven't completed
        if (isLoading && !data) {
          setError(new Error('Connection to scan endpoint failed'));
          setIsLoading(false);
        }
        eventSource.close();
        eventSourceRef.current = null;
      };

    } catch (err) {
      console.error('Error starting scan:', err);
      setError(err instanceof Error ? err : new Error('Unknown error'));
      setIsLoading(false);
    }
  }, [isLoading, data]);

  // Auto-start when enabled becomes true
  useEffect(() => {
    if (enabled && !hasStartedRef.current && !data) {
      hasStartedRef.current = true;
      startScan();
    }
  }, [enabled, startScan, data]);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      if (eventSourceRef.current) {
        eventSourceRef.current.close();
      }
    };
  }, []);

  return {
    data,
    progress,
    isLoading,
    error,
    refetch: startScan,
    stop: stopScan,
  };
}
