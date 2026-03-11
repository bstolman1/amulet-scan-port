import { useEffect, useRef, useState, useMemo } from "react";
import { useSearchParams } from "react-router-dom";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { CheckCircle, XCircle, Clock, AlertTriangle } from "lucide-react";
import { format } from "date-fns";
import { useGovernanceVoteHistory, ParsedVoteResult } from "@/hooks/use-scan-vote-results";
import { cn } from "@/lib/utils";

interface GovernanceHistoryTableProps {
  limit?: number;
}

const safeFormatDate = (dateStr: string | null | undefined, formatStr: string = "MMM d, yyyy HH:mm"): string => {
  if (!dateStr || typeof dateStr !== "string") return "N/A";
  try {
    const date = new Date(dateStr);
    if (isNaN(date.getTime())) return "N/A";
    return format(date, formatStr);
  } catch {
    return "N/A";
  }
};

export function GovernanceHistoryTable({ limit = 500 }: GovernanceHistoryTableProps) {
  const [searchParams] = useSearchParams();
  const highlightedProposalId = searchParams.get("proposal");
  const proposalRefs = useRef<Map<string, HTMLDivElement>>(new Map());
  const { data: voteResults, isLoading, error } = useGovernanceVoteHistory(limit);
  const [typeFilter, setTypeFilter] = useState<string>("all");

  useEffect(() => {
    if (highlightedProposalId && !isLoading && voteResults) {
      const timer = setTimeout(() => {
        const element = proposalRefs.current.get(highlightedProposalId);
        if (element) {
          element.scrollIntoView({ behavior: "smooth", block: "center" });
          element.classList.add("ring-2", "ring-pink-500", "ring-offset-2", "ring-offset-background");
          setTimeout(() => {
            element.classList.remove("ring-2", "ring-pink-500", "ring-offset-2", "ring-offset-background");
          }, 3000);
        }
      }, 300);
      return () => clearTimeout(timer);
    }
  }, [highlightedProposalId, isLoading, voteResults]);

  // Derive unique action titles from loaded results for the filter
  const actionTypes = useMemo(() => {
    if (!voteResults) return [];
    const seen = new Set<string>();
    return voteResults
      .map((r) => r.actionTitle)
      .filter((t) => {
        if (!t || seen.has(t)) return false;
        seen.add(t);
        return true;
      })
      .sort();
  }, [voteResults]);

  const filtered = useMemo(() => {
    if (!voteResults) return [];
    if (typeFilter === "all") return voteResults;
    return voteResults.filter((r) => r.actionTitle === typeFilter);
  }, [voteResults, typeFilter]);

  const getOutcomeVariant = (outcome: ParsedVoteResult["outcome"]) => {
    switch (outcome) {
      case "accepted": return "default";
      case "rejected": return "destructive";
      case "expired":  return "secondary";
      default:         return "outline";
    }
  };

  const getOutcomeIcon = (outcome: ParsedVoteResult["outcome"]) => {
    switch (outcome) {
      case "accepted": return <CheckCircle className="h-4 w-4 text-green-500" />;
      case "rejected": return <XCircle className="h-4 w-4 text-red-500" />;
      case "expired":  return <Clock className="h-4 w-4 text-yellow-500" />;
      default:         return <Clock className="h-4 w-4" />;
    }
  };

  const stats = {
    total:    voteResults?.length || 0,
    accepted: voteResults?.filter((r) => r.outcome === "accepted").length || 0,
    rejected: voteResults?.filter((r) => r.outcome === "rejected").length || 0,
    expired:  voteResults?.filter((r) => r.outcome === "expired").length || 0,
  };

  if (error) {
    return (
      <Alert variant="destructive">
        <AlertTriangle className="h-4 w-4" />
        <AlertDescription>
          Failed to load governance history from Scan API: {error.message}
        </AlertDescription>
      </Alert>
    );
  }

  return (
    <div className="space-y-6">
      {/* Stats row */}
      <div className="grid gap-4 md:grid-cols-4">
        <Card className="p-6">
          <h3 className="text-sm font-medium text-muted-foreground mb-2">Total Votes</h3>
          {isLoading ? <Skeleton className="h-8 w-24" /> : (
            <div>
              <p className="text-2xl font-bold">{stats.total}</p>
              <p className="text-xs text-muted-foreground mt-1">governance decisions</p>
            </div>
          )}
        </Card>
        <Card className="p-6">
          <h3 className="text-sm font-medium text-muted-foreground mb-2">Accepted</h3>
          {isLoading ? <Skeleton className="h-8 w-24" /> : (
            <p className="text-2xl font-bold">{stats.accepted}</p>
          )}
        </Card>
        <Card className="p-6">
          <h3 className="text-sm font-medium text-muted-foreground mb-2">Rejected</h3>
          {isLoading ? <Skeleton className="h-8 w-24" /> : (
            <p className="text-2xl font-bold">{stats.rejected}</p>
          )}
        </Card>
        <Card className="p-6">
          <h3 className="text-sm font-medium text-muted-foreground mb-2">Expired</h3>
          {isLoading ? <Skeleton className="h-8 w-24" /> : (
            <p className="text-2xl font-bold">{stats.expired}</p>
          )}
        </Card>
      </div>

      {/* Type filter */}
      {!isLoading && actionTypes.length > 0 && (
        <div className="flex flex-wrap gap-2">
          <button
            onClick={() => setTypeFilter("all")}
            className={cn(
              "px-3 py-1.5 rounded-full text-xs font-medium border transition-colors",
              typeFilter === "all"
                ? "bg-[#F3FF97] text-[#030206] border-[#F3FF97]"
                : "bg-transparent text-muted-foreground border-border hover:border-foreground hover:text-foreground"
            )}
          >
            All ({stats.total})
          </button>
          {actionTypes.map((type) => {
            const count = voteResults?.filter((r) => r.actionTitle === type).length || 0;
            return (
              <button
                key={type}
                onClick={() => setTypeFilter(type)}
                className={cn(
                  "px-3 py-1.5 rounded-full text-xs font-medium border transition-colors",
                  typeFilter === type
                    ? "bg-[#F3FF97] text-[#030206] border-[#F3FF97]"
                    : "bg-transparent text-muted-foreground border-border hover:border-foreground hover:text-foreground"
                )}
              >
                {type} ({count})
              </button>
            );
          })}
        </div>
      )}

      {/* Vote results list */}
      <div className="space-y-3">
        {isLoading ? (
          <div className="space-y-4">
            {[1, 2, 3].map((i) => <Skeleton key={i} className="h-32 w-full" />)}
          </div>
        ) : filtered.length === 0 ? (
          <p className="text-center text-muted-foreground py-8">No governance history found</p>
        ) : (
          filtered.map((result, idx) => {
            const proposalKey = result.trackingCid || `idx-${idx}`;
            const shortId = result.trackingCid?.slice(0, 12);
            const isHighlighted = highlightedProposalId && (
              highlightedProposalId === proposalKey ||
              highlightedProposalId === shortId ||
              result.trackingCid?.startsWith(highlightedProposalId)
            );

            return (
              <Card
                key={proposalKey}
                ref={(el) => {
                  if (el && result.trackingCid) {
                    proposalRefs.current.set(result.trackingCid, el);
                    if (shortId) proposalRefs.current.set(shortId, el);
                  }
                }}
                className={cn(
                  "p-4 space-y-3 transition-all",
                  isHighlighted && "ring-2 ring-pink-500 ring-offset-2 ring-offset-background"
                )}
              >
                <div className="flex justify-between items-start">
                  <div className="flex-1 space-y-2">
                    <div className="flex items-center gap-2">
                      {getOutcomeIcon(result.outcome)}
                      <p className="text-sm font-semibold">{result.actionTitle || "Unknown Action"}</p>
                    </div>

                    <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
                      <div>
                        <p className="text-xs text-muted-foreground">Completed At</p>
                        <p className="text-sm">{safeFormatDate(result.completedAt)}</p>
                      </div>
                      <div>
                        <p className="text-xs text-muted-foreground">Vote Before</p>
                        <p className="text-sm">{safeFormatDate(result.voteBefore)}</p>
                      </div>
                      <div>
                        <p className="text-xs text-muted-foreground">Votes</p>
                        <p className="text-sm">
                          <span className="text-green-500">{result.votesFor} for</span>
                          {" / "}
                          <span className="text-red-500">{result.votesAgainst} against</span>
                        </p>
                      </div>
                    </div>

                    <div className="p-3 rounded-lg bg-background/30 border border-border/30">
                      <p className="text-xs text-muted-foreground mb-1 font-semibold">Reason</p>
                      {result.reasonBody && (
                        <p className="text-sm mb-1">{result.reasonBody}</p>
                      )}
                      {result.reasonUrl && (
                        <a
                          href={result.reasonUrl}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="text-sm text-primary hover:underline break-all"
                        >
                          {result.reasonUrl}
                        </a>
                      )}
                      {!result.reasonBody && !result.reasonUrl && (
                        <p className="text-sm text-muted-foreground italic">No reason provided</p>
                      )}
                    </div>
                  </div>

                  <Badge variant={getOutcomeVariant(result.outcome)} className="capitalize ml-4 shrink-0">
                    {result.outcome}
                  </Badge>
                </div>
              </Card>
            );
          })
        )}
      </div>
    </div>
  );
}
