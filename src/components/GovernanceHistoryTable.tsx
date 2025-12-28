import { useState } from "react";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Skeleton } from "@/components/ui/skeleton";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { CheckCircle, XCircle, Clock, History, Code, ExternalLink, AlertTriangle, Users, ChevronDown } from "lucide-react";
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

const truncateParty = (party: string | undefined | null, maxLen = 24) => {
  if (!party) return "Unknown";
  if (party.length <= maxLen) return party;
  return `${party.slice(0, 18)}…${party.slice(-6)}`;
};

export function GovernanceHistoryTable({ limit = 500 }: GovernanceHistoryTableProps) {
  const { data: voteResults, isLoading, error } = useGovernanceVoteHistory(limit);
  const [expandedCards, setExpandedCards] = useState<Set<string>>(new Set());
  const [showRawJson, setShowRawJson] = useState<Set<string>>(new Set());

  const toggleCard = (id: string) => {
    setExpandedCards((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const toggleRawJson = (id: string, e: React.MouseEvent) => {
    e.stopPropagation();
    setShowRawJson((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

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
      case "accepted": return <CheckCircle className="h-4 w-4 text-success" />;
      case "rejected": return <XCircle className="h-4 w-4 text-destructive" />;
      case "expired": return <Clock className="h-4 w-4 text-warning" />;
      default: return <History className="h-4 w-4" />;
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
            <div className="flex items-center gap-2">
              <CheckCircle className="h-5 w-5 text-success" />
              <p className="text-2xl font-bold text-success">{stats.accepted}</p>
            </div>
          )}
        </Card>

        <Card className="p-6">
          <h3 className="text-sm font-medium text-muted-foreground mb-2">Rejected</h3>
          {isLoading ? (
            <Skeleton className="h-8 w-24" />
          ) : (
            <div className="flex items-center gap-2">
              <XCircle className="h-5 w-5 text-destructive" />
              <p className="text-2xl font-bold text-destructive">{stats.rejected}</p>
            </div>
          )}
        </Card>

        <Card className="p-6">
          <h3 className="text-sm font-medium text-muted-foreground mb-2">Expired</h3>
          {isLoading ? (
            <Skeleton className="h-8 w-24" />
          ) : (
            <div className="flex items-center gap-2">
              <Clock className="h-5 w-5 text-warning" />
              <p className="text-2xl font-bold text-warning">{stats.expired}</p>
            </div>
          )}
        </Card>
      </div>

      {/* Vote results list */}
      <div className="space-y-3">
        {isLoading ? (
          Array.from({ length: 5 }).map((_, i) => (
            <Skeleton key={i} className="h-32 w-full" />
          ))
        ) : voteResults?.length === 0 ? (
          <Card className="p-8">
            <p className="text-center text-muted-foreground">No governance history found</p>
          </Card>
        ) : (
          voteResults?.map((result) => (
            <Card key={result.trackingCid} className="p-4 space-y-3">
              <div 
                className="flex justify-between items-start cursor-pointer"
                onClick={() => toggleCard(result.trackingCid)}
              >
                <div className="flex-1 space-y-2">
                  <div className="flex items-center gap-2 flex-wrap">
                    {getOutcomeIcon(result.outcome)}
                    <p className="text-sm font-semibold">{result.actionTitle}</p>
                    <span className="text-xs font-mono text-muted-foreground">({result.actionType})</span>
                    <ChevronDown 
                      className={cn(
                        "h-4 w-4 text-muted-foreground transition-transform ml-2",
                        expandedCards.has(result.trackingCid) && "rotate-180"
                      )} 
                    />
                  </div>

                  {/* Reason URL */}
                  {result.reasonUrl && (
                    <a 
                      href={result.reasonUrl} 
                      target="_blank" 
                      rel="noopener noreferrer"
                      className="text-xs text-primary hover:underline inline-flex items-center gap-1"
                      onClick={(e) => e.stopPropagation()}
                    >
                      <ExternalLink className="h-3 w-3" />
                      {result.reasonUrl}
                    </a>
                  )}
                </div>
                <Badge variant={getOutcomeVariant(result.outcome)} className="capitalize">
                  {result.outcome}
                </Badge>
              </div>

              {/* Expanded content */}
              {expandedCards.has(result.trackingCid) && (
                <div className="space-y-4 pt-3 border-t">
                  {/* Action Details */}
                  {result.actionDetails && (
                    <div>
                      <p className="text-xs text-muted-foreground mb-1">Action Details</p>
                      <pre className="text-xs bg-muted p-3 rounded overflow-auto max-h-48 font-mono">
                        {typeof result.actionDetails === "object" 
                          ? JSON.stringify(result.actionDetails, null, 2)
                          : String(result.actionDetails)
                        }
                      </pre>
                    </div>
                  )}

                  {/* Reason */}
                  {result.reasonBody && (
                    <div>
                      <p className="text-xs text-muted-foreground mb-1">Reason</p>
                      <p className="text-sm bg-muted/50 p-3 rounded">{result.reasonBody}</p>
                    </div>
                  )}

                  {/* Voting Stats */}
                  <div className="grid grid-cols-3 gap-4 text-center py-2">
                    <div className="p-3 rounded-lg bg-success/10">
                      <p className="text-xs text-muted-foreground uppercase">Votes For</p>
                      <p className="text-xl font-semibold text-success">{result.votesFor}</p>
                    </div>
                    <div className="p-3 rounded-lg bg-destructive/10">
                      <p className="text-xs text-muted-foreground uppercase">Votes Against</p>
                      <p className="text-xl font-semibold text-destructive">{result.votesAgainst}</p>
                    </div>
                    <div className="p-3 rounded-lg bg-muted/50">
                      <p className="text-xs text-muted-foreground uppercase">Abstained</p>
                      <p className="text-xl font-semibold">{result.abstainers.length}</p>
                    </div>
                  </div>

                  {/* Individual Votes */}
                  {result.votes.length > 0 && (
                    <Collapsible className="border-t pt-3">
                      <CollapsibleTrigger asChild>
                        <Button variant="ghost" size="sm" className="w-full justify-start">
                          <Users className="h-4 w-4 mr-2" />
                          Individual Votes ({result.votes.length})
                        </Button>
                      </CollapsibleTrigger>
                      <CollapsibleContent className="mt-2 space-y-2">
                        {result.votes.map((vote, idx) => (
                          <Card 
                            key={idx} 
                            className={cn(
                              "p-3 flex items-center justify-between",
                              vote.accept ? "border-success/30 bg-success/5" : "border-destructive/30 bg-destructive/5"
                            )}
                          >
                            <div className="space-y-1">
                              <p className="text-sm font-medium">{vote.svName}</p>
                              {vote.svParty && (
                                <p className="text-xs font-mono text-muted-foreground">
                                  {truncateParty(vote.svParty)}
                                </p>
                              )}
                              {vote.reasonBody && (
                                <p className="text-xs text-muted-foreground mt-1">{vote.reasonBody}</p>
                              )}
                            </div>
                            <Badge 
                              variant="outline" 
                              className={vote.accept ? "border-success text-success" : "border-destructive text-destructive"}
                            >
                              {vote.accept ? "For" : "Against"}
                            </Badge>
                          </Card>
                        ))}
                      </CollapsibleContent>
                    </Collapsible>
                  )}

                  {/* Abstainers */}
                  {result.abstainers.length > 0 && (
                    <Collapsible className="border-t pt-3">
                      <CollapsibleTrigger asChild>
                        <Button variant="ghost" size="sm" className="w-full justify-start">
                          <Clock className="h-4 w-4 mr-2" />
                          Abstainers ({result.abstainers.length})
                        </Button>
                      </CollapsibleTrigger>
                      <CollapsibleContent className="mt-2 flex flex-wrap gap-2">
                        {result.abstainers.map((a, idx) => (
                          <Badge key={idx} variant="outline" className="text-xs font-mono">
                            {truncateParty(a)}
                          </Badge>
                        ))}
                      </CollapsibleContent>
                    </Collapsible>
                  )}

                  {/* Metadata */}
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-sm border-t pt-3">
                    <div>
                      <p className="text-xs text-muted-foreground">Completed At</p>
                      <p className="font-medium">{safeFormatDate(result.completedAt)}</p>
                    </div>
                    <div>
                      <p className="text-xs text-muted-foreground">Vote Before</p>
                      <p className="font-medium">{safeFormatDate(result.voteBefore)}</p>
                    </div>
                    <div>
                      <p className="text-xs text-muted-foreground">Expires At</p>
                      <p className="font-medium">{safeFormatDate(result.expiresAt)}</p>
                    </div>
                    <div>
                      <p className="text-xs text-muted-foreground">Requester</p>
                      <p className="font-mono text-xs break-all">{truncateParty(result.requester)}</p>
                    </div>
                  </div>

                  {/* Tracking CID */}
                  <div className="border-t pt-3">
                    <p className="text-xs text-muted-foreground">Tracking CID</p>
                    <p className="font-mono text-xs break-all">{result.trackingCid}</p>
                  </div>

                  {/* Show Raw JSON */}
                  <Collapsible className="border-t pt-3">
                    <CollapsibleTrigger asChild>
                      <Button 
                        variant="ghost" 
                        size="sm" 
                        className="w-full justify-start"
                        onClick={(e) => toggleRawJson(result.trackingCid, e)}
                      >
                        <Code className="h-4 w-4 mr-2" />
                        Show Full JSON Data
                      </Button>
                    </CollapsibleTrigger>
                    <CollapsibleContent className="mt-2">
                      <pre className="text-xs bg-muted p-3 rounded overflow-auto max-h-96 font-mono">
                        {JSON.stringify(result, null, 2)}
                      </pre>
                    </CollapsibleContent>
                  </Collapsible>
                </div>
              )}
            </Card>
          ))
        )}
      </div>
    </div>
  );
}
