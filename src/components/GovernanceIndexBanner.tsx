import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Vote, RefreshCw, CheckCircle, AlertTriangle, Loader2 } from "lucide-react";
import { apiFetch } from "@/lib/duckdb-api-client";
import { toast } from "@/hooks/use-toast";

interface IndexStatus {
  isIndexing: boolean;
  progress: {
    phase: string;
    current: number;
    total: number;
    records: number;
    proposals: number;
  } | null;
  stats: {
    total: number;
    approved: number;
    rejected: number;
    pending: number;
    expired: number;
  } | null;
  cachePopulated: boolean;
  lastIndexedAt: string | null;
}

export function GovernanceIndexBanner() {
  const queryClient = useQueryClient();
  const [isBuilding, setIsBuilding] = useState(false);

  const { data: status, isLoading, error } = useQuery({
    queryKey: ["governance-index-status"],
    queryFn: () => apiFetch<IndexStatus>("/api/governance/index/status"),
    staleTime: 5_000,
    refetchInterval: isBuilding ? 2000 : 30_000,
    refetchOnWindowFocus: true,
  });

  const buildMutation = useMutation({
    mutationFn: async () => {
      setIsBuilding(true);
      const res = await fetch("http://localhost:3001/api/governance/index/build", {
        method: "POST",
      });
      if (!res.ok) throw new Error("Failed to start governance index build");
      return res.json();
    },
    onSuccess: () => {
      toast({
        title: "Governance Index Build Started",
        description: "Building proposals from the persistent VoteRequest index. This should be fast.",
      });
      const pollInterval = setInterval(async () => {
        try {
          const fresh = await apiFetch<IndexStatus>("/api/governance/index/status");
          queryClient.setQueryData(["governance-index-status"], fresh);
          if (!fresh.isIndexing) {
            clearInterval(pollInterval);
            setIsBuilding(false);
            // Invalidate proposals queries to refresh data
            queryClient.invalidateQueries({ queryKey: ["governance-proposals"] });
            queryClient.invalidateQueries({ queryKey: ["governance-proposal-stats"] });
            queryClient.invalidateQueries({ queryKey: ["governance-action-types"] });
            toast({
              title: "Governance Index Complete",
              description: `Indexed ${fresh.stats?.total ?? 0} unique proposals (${fresh.stats?.approved ?? 0} approved, ${fresh.stats?.rejected ?? 0} rejected).`,
            });
          }
        } catch (err) {
          console.error("Error polling governance index status:", err);
        }
      }, 2000);
    },
    onError: (err: Error) => {
      setIsBuilding(false);
      toast({
        title: "Index Build Failed",
        description: err.message,
        variant: "destructive",
      });
    },
  });

  if (isLoading) return null;
  if (error) return null;

  const isPopulated = status?.cachePopulated || (status?.stats?.total ?? 0) > 0;
  const totalProposals = status?.stats?.total ?? 0;
  const inProgress = isBuilding || status?.isIndexing;
  const progress = status?.progress;

  return (
    <Alert className={isPopulated ? "bg-primary/10 border-primary/30" : "bg-warning/10 border-warning/30"}>
      <Vote className="h-4 w-4" />
      <AlertDescription className="flex flex-wrap items-center justify-between gap-2">
        <div className="flex items-center gap-3">
          {inProgress ? (
            <>
              <Loader2 className="h-4 w-4 animate-spin text-primary" />
                <span>
                  <strong>Governance Index:</strong>{" "}
                  {progress ? (
                    <>
                      {progress.phase} ({progress.current}/{progress.total} records, {progress.proposals} proposals)
                    </>
                  ) : (
                    "Building..."
                  )}
                </span>
              <Badge variant="outline" className="text-xs border-primary text-primary">
                In Progress
              </Badge>
            </>
          ) : isPopulated ? (
            <>
              <CheckCircle className="h-4 w-4 text-primary" />
              <span>
                <strong>Governance Index:</strong> {totalProposals} unique proposals indexed
              </span>
              <div className="flex gap-1">
                <Badge variant="outline" className="text-xs border-green-500 text-green-500">
                  {status?.stats?.approved ?? 0} approved
                </Badge>
                <Badge variant="outline" className="text-xs border-red-500 text-red-500">
                  {status?.stats?.rejected ?? 0} rejected
                </Badge>
                <Badge variant="outline" className="text-xs border-yellow-500 text-yellow-500">
                  {status?.stats?.pending ?? 0} pending
                </Badge>
              </div>
            </>
          ) : (
            <>
              <AlertTriangle className="h-4 w-4 text-warning" />
              <span>
                <strong>Governance Index:</strong> Not built — Run indexer to see historical proposals
              </span>
              <Badge variant="outline" className="text-xs border-warning text-warning">
                Build Required
              </Badge>
            </>
          )}
        </div>
        <Button
          size="sm"
          variant={isPopulated ? "outline" : "default"}
          disabled={inProgress}
          onClick={() => buildMutation.mutate()}
          className="gap-1"
        >
          <RefreshCw className={`h-3.5 w-3.5 ${inProgress ? "animate-spin" : ""}`} />
          {inProgress ? "Building..." : isPopulated ? "Rebuild" : "Build Index"}
        </Button>
      </AlertDescription>
    </Alert>
  );
}
