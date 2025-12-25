import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Database, RefreshCw, CheckCircle, AlertTriangle } from "lucide-react";
import { apiFetch } from "@/lib/duckdb-api-client";
import { toast } from "@/hooks/use-toast";

interface IndexStatus {
  populated: boolean;
  isIndexing: boolean;
  stats: {
    total: number;
    active: number;
    historical: number;
    closed: number;
  };
  lastIndexedAt: string | null;
  totalIndexed: number;
}

export function VoteRequestIndexBanner() {
  const queryClient = useQueryClient();
  const [isBuilding, setIsBuilding] = useState(false);

  const { data: status, isLoading, error } = useQuery({
    queryKey: ["vote-request-index-status"],
    queryFn: () => apiFetch<IndexStatus>("/api/events/vote-request-index/status"),
    staleTime: 5_000, // Short stale time to pick up index changes quickly
    refetchInterval: isBuilding ? 3000 : 30_000, // Poll every 30s normally, faster when building
    refetchOnWindowFocus: true,
  });

  const buildMutation = useMutation({
    mutationFn: async () => {
      setIsBuilding(true);
      const res = await fetch("http://localhost:3001/api/events/vote-request-index/build?force=true", {
        method: "POST",
      });
      if (!res.ok) throw new Error("Failed to start index build");
      return res.json();
    },
    onSuccess: () => {
      toast({
        title: "Index Build Started",
        description: "VoteRequest indexing is running in the background. This may take a few minutes.",
      });
      // Poll for completion
      const pollInterval = setInterval(async () => {
        const fresh = await apiFetch<IndexStatus>("/api/events/vote-request-index/status");
        queryClient.setQueryData(["vote-request-index-status"], fresh);
        if (!fresh.isIndexing) {
          clearInterval(pollInterval);
          setIsBuilding(false);
          toast({
            title: "Index Build Complete",
            description: `Indexed ${fresh.stats?.total ?? 0} VoteRequest events (${fresh.stats?.historical ?? 0} historical).`,
          });
        }
      }, 3000);
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

  const isPopulated = status?.populated || (status?.stats?.total ?? 0) > 0;
  const historicalCount = status?.stats?.historical ?? 0;
  const inProgress = isBuilding || status?.isIndexing;

  return (
    <Alert className={isPopulated ? "bg-success/10 border-success/30" : "bg-warning/10 border-warning/30"}>
      <Database className="h-4 w-4" />
      <AlertDescription className="flex flex-wrap items-center justify-between gap-2">
        <div className="flex flex-wrap items-center gap-3">
          {isPopulated ? (
            <>
              <CheckCircle className="h-4 w-4 text-success" />
              <span>
                <strong>VoteRequest Index:</strong> {historicalCount} completed votes indexed
                {status?.lastIndexedAt && (
                  <span className="text-muted-foreground ml-2 text-xs">
                    (Last built: {new Date(status.lastIndexedAt).toLocaleString()})
                  </span>
                )}
              </span>
              <Badge variant="outline" className="text-xs">
                Ready
              </Badge>
            </>
          ) : (
            <>
              <AlertTriangle className="h-4 w-4 text-warning" />
              <span>
                <strong>VoteRequest Index:</strong> Not built — Governance History will be slow
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
