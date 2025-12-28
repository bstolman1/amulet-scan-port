import { useState } from "react";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Skeleton } from "@/components/ui/skeleton";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { CheckCircle, XCircle, Clock, ChevronDown, History, Users, ExternalLink, AlertTriangle } from "lucide-react";
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
  const { data: voteResults, isLoading, error } = useGovernanceVoteHistory(limit);
  const [expandedRows, setExpandedRows] = useState<Set<string>>(new Set());

  const toggleRow = (id: string) => {
    setExpandedRows((prev) => {
      const next = new Set(prev);
      if (next.has(id)) {
        next.delete(id);
      } else {
        next.add(id);
      }
      return next;
    });
  };

  const getOutcomeColor = (outcome: ParsedVoteResult["outcome"]) => {
    switch (outcome) {
      case "accepted":
        return "bg-success/10 text-success border-success/20";
      case "rejected":
        return "bg-destructive/10 text-destructive border-destructive/20";
      case "expired":
        return "bg-warning/10 text-warning border-warning/20";
      default:
        return "bg-muted text-muted-foreground";
    }
  };

  const getOutcomeIcon = (outcome: ParsedVoteResult["outcome"]) => {
    switch (outcome) {
      case "accepted":
        return <CheckCircle className="h-4 w-4" />;
      case "rejected":
        return <XCircle className="h-4 w-4" />;
      case "expired":
        return <Clock className="h-4 w-4" />;
      default:
        return <History className="h-4 w-4" />;
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
    <div className="space-y-4">
      {/* Stats row */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <Card className="glass-card p-4">
          <div className="flex items-center justify-between">
            <span className="text-sm text-muted-foreground">Total</span>
            <History className="h-4 w-4 text-primary" />
          </div>
          {isLoading ? (
            <Skeleton className="h-8 w-16 mt-1" />
          ) : (
            <p className="text-2xl font-bold text-primary">{stats.total}</p>
          )}
        </Card>
        <Card className="glass-card p-4">
          <div className="flex items-center justify-between">
            <span className="text-sm text-muted-foreground">Accepted</span>
            <CheckCircle className="h-4 w-4 text-success" />
          </div>
          {isLoading ? (
            <Skeleton className="h-8 w-16 mt-1" />
          ) : (
            <p className="text-2xl font-bold text-success">{stats.accepted}</p>
          )}
        </Card>
        <Card className="glass-card p-4">
          <div className="flex items-center justify-between">
            <span className="text-sm text-muted-foreground">Rejected</span>
            <XCircle className="h-4 w-4 text-destructive" />
          </div>
          {isLoading ? (
            <Skeleton className="h-8 w-16 mt-1" />
          ) : (
            <p className="text-2xl font-bold text-destructive">{stats.rejected}</p>
          )}
        </Card>
        <Card className="glass-card p-4">
          <div className="flex items-center justify-between">
            <span className="text-sm text-muted-foreground">Expired</span>
            <Clock className="h-4 w-4 text-warning" />
          </div>
          {isLoading ? (
            <Skeleton className="h-8 w-16 mt-1" />
          ) : (
            <p className="text-2xl font-bold text-warning">{stats.expired}</p>
          )}
        </Card>
      </div>

      {/* Table */}
      <Card className="glass-card overflow-hidden">
        <div className="overflow-x-auto">
          <Table>
            <TableHeader>
              <TableRow className="border-border/50 hover:bg-transparent">
                <TableHead className="w-[40px]"></TableHead>
                <TableHead>Action</TableHead>
                <TableHead>Outcome</TableHead>
                <TableHead>Votes</TableHead>
                <TableHead>Completed</TableHead>
                <TableHead className="text-right">Requester</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {isLoading ? (
                Array.from({ length: 10 }).map((_, i) => (
                  <TableRow key={i} className="border-border/50">
                    <TableCell><Skeleton className="h-4 w-4" /></TableCell>
                    <TableCell><Skeleton className="h-4 w-40" /></TableCell>
                    <TableCell><Skeleton className="h-5 w-20" /></TableCell>
                    <TableCell><Skeleton className="h-4 w-16" /></TableCell>
                    <TableCell><Skeleton className="h-4 w-32" /></TableCell>
                    <TableCell><Skeleton className="h-4 w-24 ml-auto" /></TableCell>
                  </TableRow>
                ))
              ) : voteResults?.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={6} className="text-center py-8 text-muted-foreground">
                    No governance history found
                  </TableCell>
                </TableRow>
              ) : (
                voteResults?.map((result) => (
                  <Collapsible key={result.trackingCid} asChild>
                    <>
                      <TableRow 
                        className={cn(
                          "border-border/50 cursor-pointer transition-colors",
                          expandedRows.has(result.trackingCid) && "bg-muted/30"
                        )}
                        onClick={() => toggleRow(result.trackingCid)}
                      >
                        <TableCell>
                          <CollapsibleTrigger asChild>
                            <Button variant="ghost" size="icon" className="h-6 w-6">
                              <ChevronDown 
                                className={cn(
                                  "h-4 w-4 transition-transform",
                                  expandedRows.has(result.trackingCid) && "rotate-180"
                                )} 
                              />
                            </Button>
                          </CollapsibleTrigger>
                        </TableCell>
                        <TableCell>
                          <div className="flex flex-col">
                            <span className="font-medium">{result.actionTitle}</span>
                            <span className="text-xs text-muted-foreground font-mono">
                              {result.actionType}
                            </span>
                          </div>
                        </TableCell>
                        <TableCell>
                          <Badge className={cn("gap-1", getOutcomeColor(result.outcome))}>
                            {getOutcomeIcon(result.outcome)}
                            {result.outcome}
                          </Badge>
                        </TableCell>
                        <TableCell>
                          <div className="flex items-center gap-2">
                            <span className="text-success">{result.votesFor}</span>
                            <span className="text-muted-foreground">/</span>
                            <span className="text-destructive">{result.votesAgainst}</span>
                            {result.abstainers.length > 0 && (
                              <span className="text-xs text-muted-foreground">
                                (+{result.abstainers.length} abstain)
                              </span>
                            )}
                          </div>
                        </TableCell>
                        <TableCell>
                          <span className="text-sm">
                            {safeFormatDate(result.completedAt)}
                          </span>
                        </TableCell>
                        <TableCell className="text-right">
                          <span className="text-xs font-mono text-muted-foreground">
                            {result.requester?.split("::")[0]?.slice(0, 16)}...
                          </span>
                        </TableCell>
                      </TableRow>
                      <CollapsibleContent asChild>
                        <TableRow className="bg-muted/20 border-border/50">
                          <TableCell colSpan={6} className="p-0">
                            <div className="p-4 space-y-4">
                              {/* Reason */}
                              {(result.reasonBody || result.reasonUrl) && (
                                <div className="space-y-1">
                                  <span className="text-sm font-medium">Reason</span>
                                  {result.reasonBody && (
                                    <p className="text-sm text-muted-foreground">{result.reasonBody}</p>
                                  )}
                                  {result.reasonUrl && (
                                    <a 
                                      href={result.reasonUrl} 
                                      target="_blank" 
                                      rel="noopener noreferrer"
                                      className="text-sm text-primary hover:underline inline-flex items-center gap-1"
                                      onClick={(e) => e.stopPropagation()}
                                    >
                                      <ExternalLink className="h-3 w-3" />
                                      {result.reasonUrl}
                                    </a>
                                  )}
                                </div>
                              )}

                              {/* Votes breakdown */}
                              <div className="space-y-2">
                                <div className="flex items-center gap-2">
                                  <Users className="h-4 w-4" />
                                  <span className="text-sm font-medium">Votes ({result.totalVotes})</span>
                                </div>
                                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-2">
                                  {result.votes.map((vote, idx) => (
                                    <div 
                                      key={idx}
                                      className={cn(
                                        "flex items-center justify-between p-2 rounded-md text-sm",
                                        vote.accept ? "bg-success/10" : "bg-destructive/10"
                                      )}
                                    >
                                      <span className="font-mono text-xs truncate max-w-[200px]">
                                        {vote.svName}
                                      </span>
                                      <Badge 
                                        variant="outline" 
                                        className={cn(
                                          "text-xs",
                                          vote.accept ? "border-success text-success" : "border-destructive text-destructive"
                                        )}
                                      >
                                        {vote.accept ? "For" : "Against"}
                                      </Badge>
                                    </div>
                                  ))}
                                </div>
                              </div>

                              {/* Abstainers */}
                              {result.abstainers.length > 0 && (
                                <div className="space-y-1">
                                  <span className="text-sm font-medium text-muted-foreground">
                                    Abstained ({result.abstainers.length})
                                  </span>
                                  <div className="flex flex-wrap gap-1">
                                    {result.abstainers.map((a, idx) => (
                                      <Badge key={idx} variant="outline" className="text-xs font-mono">
                                        {a.split("::")[0]?.slice(0, 16)}...
                                      </Badge>
                                    ))}
                                  </div>
                                </div>
                              )}

                              {/* Metadata */}
                              <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-xs text-muted-foreground">
                                <div>
                                  <span className="font-medium">Vote Before</span>
                                  <p>{safeFormatDate(result.voteBefore)}</p>
                                </div>
                                <div>
                                  <span className="font-medium">Expires At</span>
                                  <p>{safeFormatDate(result.expiresAt)}</p>
                                </div>
                                <div className="col-span-2">
                                  <span className="font-medium">Tracking CID</span>
                                  <p className="font-mono truncate">{result.trackingCid}</p>
                                </div>
                              </div>
                            </div>
                          </TableCell>
                        </TableRow>
                      </CollapsibleContent>
                    </>
                  </Collapsible>
                ))
              )}
            </TableBody>
          </Table>
        </div>
      </Card>
    </div>
  );
}
