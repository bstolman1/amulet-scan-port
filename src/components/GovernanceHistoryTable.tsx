import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Skeleton } from "@/components/ui/skeleton";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { CheckCircle, XCircle, Clock, Code, AlertTriangle } from "lucide-react";
import { format } from "date-fns";
import { useGovernanceVoteHistory, ParsedVoteResult } from "@/hooks/use-scan-vote-results";

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

const truncateParty = (party: string | undefined | null, maxLen = 30) => {
  if (!party) return "Unknown";
  if (party.length <= maxLen) return party;
  return `${party.substring(0, 15)}...${party.substring(party.length - 12)}`;
};

export function GovernanceHistoryTable({ limit = 500 }: GovernanceHistoryTableProps) {
  const { data: voteResults, isLoading, error } = useGovernanceVoteHistory(limit);

  const getOutcomeVariant = (outcome: ParsedVoteResult["outcome"]) => {
    switch (outcome) {
      case "accepted": return "default";
      case "rejected": return "destructive";
      case "expired": return "secondary";
      default: return "outline";
    }
  };

  const getOutcomeIcon = (outcome: ParsedVoteResult["outcome"]) => {
    switch (outcome) {
      case "accepted": return <CheckCircle className="h-4 w-4 text-green-500" />;
      case "rejected": return <XCircle className="h-4 w-4 text-red-500" />;
      case "expired": return <Clock className="h-4 w-4 text-yellow-500" />;
      default: return <Clock className="h-4 w-4" />;
    }
  };

  // Stats
  const stats = {
    total: voteResults?.length || 0,
    accepted: voteResults?.filter((r) => r.outcome === "accepted").length || 0,
    rejected: voteResults?.filter((r) => r.outcome === "rejected").length || 0,
    expired: voteResults?.filter((r) => r.outcome === "expired").length || 0,
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
          {isLoading ? (
            <Skeleton className="h-8 w-24" />
          ) : (
            <div>
              <p className="text-2xl font-bold">{stats.total}</p>
              <p className="text-xs text-muted-foreground mt-1">governance decisions</p>
            </div>
          )}
        </Card>

        <Card className="p-6">
          <h3 className="text-sm font-medium text-muted-foreground mb-2">Accepted</h3>
          {isLoading ? (
            <Skeleton className="h-8 w-24" />
          ) : (
            <div>
              <p className="text-2xl font-bold">{stats.accepted}</p>
            </div>
          )}
        </Card>

        <Card className="p-6">
          <h3 className="text-sm font-medium text-muted-foreground mb-2">Rejected</h3>
          {isLoading ? (
            <Skeleton className="h-8 w-24" />
          ) : (
            <div>
              <p className="text-2xl font-bold">{stats.rejected}</p>
            </div>
          )}
        </Card>

        <Card className="p-6">
          <h3 className="text-sm font-medium text-muted-foreground mb-2">Expired</h3>
          {isLoading ? (
            <Skeleton className="h-8 w-24" />
          ) : (
            <div>
              <p className="text-2xl font-bold">{stats.expired}</p>
            </div>
          )}
        </Card>
      </div>

      {/* Vote results list - matches ACS page card format */}
      <div className="space-y-3">
        {isLoading ? (
          <div className="space-y-4">
            {[1, 2, 3].map((i) => (
              <Skeleton key={i} className="h-32 w-full" />
            ))}
          </div>
        ) : voteResults?.length === 0 ? (
          <p className="text-center text-muted-foreground py-8">No governance history found</p>
        ) : (
          voteResults?.map((result, idx) => (
            <Card key={result.trackingCid || idx} className="p-4 space-y-3">
              <div className="flex justify-between items-start">
                <div className="flex-1 space-y-2">
                  <div className="flex items-center gap-2">
                    {getOutcomeIcon(result.outcome)}
                    <p className="text-sm font-semibold">{result.actionTitle || "Unknown Action"}</p>
                  </div>

                  <div>
                    <p className="text-xs text-muted-foreground">Action Type</p>
                    <p className="font-mono text-xs">{result.actionType}</p>
                  </div>

                  <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                    <div>
                      <p className="text-xs text-muted-foreground">Completed At</p>
                      <p className="text-sm">{safeFormatDate(result.completedAt)}</p>
                    </div>
                    <div>
                      <p className="text-xs text-muted-foreground">Vote Before</p>
                      <p className="text-sm">{safeFormatDate(result.voteBefore)}</p>
                    </div>
                    <div>
                      <p className="text-xs text-muted-foreground">Expires At</p>
                      <p className="text-sm">{safeFormatDate(result.expiresAt)}</p>
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

                  <div>
                    <p className="text-xs text-muted-foreground">Tracking CID</p>
                    <p className="font-mono text-xs break-all">{result.trackingCid || "N/A"}</p>
                  </div>

                  <Collapsible className="pt-2 border-t">
                    <CollapsibleTrigger asChild>
                      <Button variant="ghost" size="sm" className="w-full justify-start">
                        <Code className="h-4 w-4 mr-2" />
                        Show Raw JSON
                      </Button>
                    </CollapsibleTrigger>
                    <CollapsibleContent className="mt-2">
                      <pre className="text-xs bg-muted p-3 rounded overflow-auto max-h-96">
                        {JSON.stringify(result, null, 2)}
                      </pre>
                    </CollapsibleContent>
                  </Collapsible>
                </div>
                <Badge variant={getOutcomeVariant(result.outcome)} className="capitalize">
                  {result.outcome}
                </Badge>
              </div>
            </Card>
          ))
        )}
      </div>
    </div>
  );
}
