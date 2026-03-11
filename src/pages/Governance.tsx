import { useEffect, useRef } from "react";
import { useSearchParams } from "react-router-dom";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Vote, CheckCircle, XCircle, Clock, Users, Globe, ChevronDown, Server } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { scanApi } from "@/lib/api-client";
import { Skeleton } from "@/components/ui/skeleton";
import { useAggregatedTemplateData } from "@/hooks/use-aggregated-template-data";
import { useActiveVoteRequests } from "@/hooks/use-active-vote-requests";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { format } from "date-fns";
import { cn } from "@/lib/utils";
import { GovernanceHistoryTable } from "@/components/GovernanceHistoryTable";

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

const Governance = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const highlightedProposalId = searchParams.get("proposal");
  const activeTab = searchParams.get("tab") || "scanapi";
  const proposalRefs = useRef<Map<string, HTMLDivElement>>(new Map());

  const handleTabChange = (value: string) => {
    setSearchParams((prev) => {
      const newParams = new URLSearchParams(prev);
      newParams.set("tab", value);
      if (value !== activeTab) newParams.delete("proposal");
      return newParams;
    });
  };

  const { data: dsoInfo } = useQuery({
    queryKey: ["dsoInfo"],
    queryFn: () => scanApi.fetchDsoInfo(),
    retry: 1,
  });

  const {
    data: activeVoteRequestsData,
    isLoading: activeVoteRequestsLoading,
    isError: activeVoteRequestsError,
  } = useActiveVoteRequests();

  const {
    data: voteRequestsData,
    isLoading: localLoading,
    isError: localError,
  } = useAggregatedTemplateData(undefined, "Splice:DsoRules:VoteRequest");

  const isLoading = activeVoteRequestsLoading && localLoading;
  const isError = activeVoteRequestsError && localError;

  useEffect(() => {
    if (highlightedProposalId && !isLoading) {
      const timer = setTimeout(() => {
        const element = proposalRefs.current.get(highlightedProposalId);
        if (element) {
          element.scrollIntoView({ behavior: "smooth", block: "center" });
          element.classList.add("ring-2", "ring-pink-500", "ring-offset-2", "ring-offset-background");
          setTimeout(() => {
            element.classList.remove("ring-2", "ring-pink-500", "ring-offset-2", "ring-offset-background");
          }, 3000);
        }
      }, 1000);
      return () => clearTimeout(timer);
    }
  }, [highlightedProposalId, isLoading]);

  const svCount = dsoInfo?.sv_node_states?.length || 0;
  const votingThreshold = dsoInfo?.voting_threshold || Math.ceil(svCount * 0.67) || 1;

  const extractSimpleFields = (obj: any, prefix = "", depth = 0): Record<string, string> => {
    if (!obj || depth > 2) return {};
    const result: Record<string, string> = {};
    for (const [key, value] of Object.entries(obj)) {
      if (["tag", "value", "packageId", "moduleName", "entityName", "dso"].includes(key)) continue;
      const fieldName = prefix ? `${prefix}.${key}` : key;
      if (value === null || value === undefined) continue;
      if (typeof value === "string") {
        if (value.length < 100 && !value.match(/^[a-f0-9]{64}$/i)) result[fieldName] = value;
      } else if (typeof value === "number" || typeof value === "boolean") {
        result[fieldName] = String(value);
      } else if (Array.isArray(value)) {
        result[fieldName] = value.length > 0 && typeof value[0] !== "object"
          ? value.slice(0, 3).join(", ") + (value.length > 3 ? ` (+${value.length - 3} more)` : "")
          : `[${value.length} items]`;
      } else if (typeof value === "object") {
        Object.assign(result, extractSimpleFields(value, fieldName, depth + 1));
      }
    }
    return result;
  };

  const parseAction = (action: any): { title: string; actionType: string } => {
    if (!action) return { title: "Unknown Action", actionType: "Unknown" };
    const outerTag = action.tag || Object.keys(action)[0] || "Unknown";
    const outerValue = action.value || action[outerTag] || action;
    const innerAction = outerValue?.dsoAction || outerValue?.amuletRulesAction || outerValue;
    const innerTag = innerAction?.tag || "";
    const actionType = innerTag || outerTag;
    const title = actionType
      .replace(/^(SRARC_|ARC_|CRARC_|ARAC_)/, "")
      .replace(/([A-Z])/g, " $1")
      .trim();
    return { title, actionType };
  };

  const parseVotes = (votes: any): { votesFor: number; votesAgainst: number; votedSvs: any[] } => {
    if (!votes) return { votesFor: 0, votesAgainst: 0, votedSvs: [] };
    const votesArray = Array.isArray(votes) ? votes : Object.entries(votes);
    let votesFor = 0;
    let votesAgainst = 0;
    const votedSvs: any[] = [];
    for (const vote of votesArray) {
      const [svName, voteData] = Array.isArray(vote) ? vote : [vote.sv || "Unknown", vote];
      const isAccept = voteData?.accept === true || voteData?.Accept === true;
      const isReject = voteData?.accept === false || voteData?.reject === true || voteData?.Reject === true;
      if (isAccept) votesFor++;
      else if (isReject) votesAgainst++;
      votedSvs.push({
        party: svName,
        vote: isAccept ? "accept" : isReject ? "reject" : "abstain",
        castAt: voteData?.optCastAt || null,
      });
    }
    return { votesFor, votesAgainst, votedSvs };
  };

  const rawVoteRequests = activeVoteRequestsData?.data || voteRequestsData?.data || [];

  const proposals = rawVoteRequests.map((voteRequest: any) => {
    const contract = voteRequest.contract || voteRequest;
    const payload = contract.payload || contract.create_arguments || contract;
    const action = payload.action || voteRequest.action || {};
    const { title, actionType } = parseAction(action);
    const votesRaw = payload.votes || voteRequest.votes || [];
    const { votesFor, votesAgainst, votedSvs } = parseVotes(votesRaw);
    const requester = payload.requester || voteRequest.requester || "Unknown";
    const reasonObj = payload.reason || voteRequest.reason || {};
    const reasonBody = reasonObj?.body || (typeof reasonObj === "string" ? reasonObj : "");
    const reasonUrl = reasonObj?.url || "";
    const voteBefore = payload.voteBefore || voteRequest.voteBefore;
    const targetEffectiveAt = payload.targetEffectiveAt || voteRequest.targetEffectiveAt;
    const trackingCid = payload.trackingCid || voteRequest.trackingCid || contract.contract_id || voteRequest.contract_id;

    const threshold = votingThreshold || svCount || 1;
    const now = new Date();
    const voteDeadline = voteBefore ? new Date(voteBefore) : null;
    const isExpired = voteDeadline && voteDeadline < now;
    let status: "approved" | "rejected" | "pending" = "pending";
    if (votesFor >= threshold) status = "approved";
    else if (isExpired && votesFor < threshold) status = "rejected";

    return {
      id: trackingCid?.slice(0, 12) || "unknown",
      title,
      actionType,
      reasonBody,
      reasonUrl,
      requester,
      status,
      votesFor,
      votesAgainst,
      votedSvs,
      voteBefore,
      targetEffectiveAt,
    };
  });

  const totalProposals = proposals?.length || 0;
  const activeProposals = proposals?.filter((p: any) => p.status === "pending").length || 0;

  const getStatusColor = (status: string) => {
    switch (status) {
      case "approved": return "bg-success/10 text-success border-success/20";
      case "rejected": return "bg-destructive/10 text-destructive border-destructive/20";
      case "pending":  return "bg-warning/10 text-warning border-warning/20";
      default:         return "bg-muted text-muted-foreground";
    }
  };

  const getStatusIcon = (status: string) => {
    switch (status) {
      case "approved": return <CheckCircle className="h-4 w-4" />;
      case "rejected": return <XCircle className="h-4 w-4" />;
      case "pending":  return <Clock className="h-4 w-4" />;
      default:         return <Vote className="h-4 w-4" />;
    }
  };

  return (
    <DashboardLayout>
      <div className="space-y-6">

        {/* Page Header */}
        <div>
          <h2 className="text-3xl font-bold mb-2">Governance</h2>
          <p className="text-muted-foreground">DSO proposals and voting activity</p>
        </div>

        {/* Summary Cards */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <Card className="glass-card p-6">
            <div className="flex items-center justify-between mb-2">
              <h3 className="text-sm font-medium text-muted-foreground">Super Validators</h3>
              <Users className="h-5 w-5 text-primary" />
            </div>
            {!svCount ? <Skeleton className="h-10 w-full" /> : (
              <>
                <p className="text-3xl font-bold text-primary mb-1">{svCount}</p>
                <p className="text-xs text-muted-foreground">Active SVs</p>
              </>
            )}
          </Card>

          <Card className="glass-card p-6">
            <div className="flex items-center justify-between mb-2">
              <h3 className="text-sm font-medium text-muted-foreground">Voting Threshold</h3>
              <Vote className="h-5 w-5 text-chart-3" />
            </div>
            {!votingThreshold ? <Skeleton className="h-10 w-full" /> : (
              <>
                <p className="text-3xl font-bold text-chart-3 mb-1">{votingThreshold}</p>
                <p className="text-xs text-muted-foreground">Votes required</p>
              </>
            )}
          </Card>

          <Card className="glass-card p-6">
            <div className="flex items-center justify-between mb-2">
              <h3 className="text-sm font-medium text-muted-foreground">Total Proposals</h3>
              <Server className="h-5 w-5 text-chart-2" />
            </div>
            {isLoading ? <Skeleton className="h-10 w-full" /> : (
              <>
                <p className="text-3xl font-bold text-chart-2 mb-1">{totalProposals}</p>
                <p className="text-xs text-muted-foreground">All proposals</p>
              </>
            )}
          </Card>

          <Card className="glass-card p-6">
            <div className="flex items-center justify-between mb-2">
              <h3 className="text-sm font-medium text-muted-foreground">Active Proposals</h3>
              <Clock className="h-5 w-5 text-warning" />
            </div>
            {isLoading ? <Skeleton className="h-10 w-full" /> : (
              <>
                <p className="text-3xl font-bold text-warning mb-1">{activeProposals}</p>
                <p className="text-xs text-muted-foreground">In voting</p>
              </>
            )}
          </Card>
        </div>

        {/* Tabs */}
        <Tabs value={activeTab} onValueChange={handleTabChange} className="space-y-6">
          <TabsList>
            <TabsTrigger value="scanapi" className="gap-1 data-[state=active]:bg-[#F3FF97] data-[state=active]:text-[#030206]">
              <Globe className="h-4 w-4" />
              Historical Governance
            </TabsTrigger>
            <TabsTrigger value="active" className="data-[state=active]:bg-[#F3FF97] data-[state=active]:text-[#030206]">
              <Clock className="h-4 w-4" />
              Active Governance
            </TabsTrigger>
          </TabsList>

          {/* ── Historical Governance ── */}
          <TabsContent value="scanapi">
            <Card className="glass-card">
              <div className="p-6">
                <div className="mb-6">
                  <h3 className="text-xl font-bold flex items-center gap-2">
                    <Globe className="h-5 w-5 text-primary" />
                    Historical Governance
                  </h3>
                  <p className="text-sm text-muted-foreground mt-1">
                    Complete governance history from the Canton Scan API
                  </p>
                </div>
                <GovernanceHistoryTable limit={500} />
              </div>
            </Card>
          </TabsContent>

          {/* ── Active Governance ── */}
          <TabsContent value="active">
            <Card className="glass-card">
              <div className="p-6">
                <div className="mb-6">
                  <h3 className="text-xl font-bold flex items-center gap-2">
                    <Clock className="h-5 w-5 text-warning" />
                    Active Governance
                  </h3>
                  <p className="text-sm text-muted-foreground mt-1">
                    In-progress vote requests open for Super Validator voting
                  </p>
                </div>

                {isError ? (
                  <div className="text-center py-12">
                    <Vote className="h-12 w-12 text-muted-foreground mx-auto mb-4" />
                    <p className="text-muted-foreground mb-2">Unable to load proposals from Scan API.</p>
                    <p className="text-xs text-muted-foreground">Check network connectivity to the Canton Scan API.</p>
                  </div>
                ) : isLoading ? (
                  <div className="space-y-4">
                    {[1, 2, 3].map((i) => <Skeleton key={i} className="h-32 w-full" />)}
                  </div>
                ) : !proposals?.length ? (
                  <div className="text-center py-12">
                    <CheckCircle className="h-12 w-12 text-success mx-auto mb-4" />
                    <p className="text-muted-foreground mb-2">No active proposals at the moment</p>
                    <p className="text-sm text-muted-foreground">
                      All governance proposals have been resolved. New proposals will appear here when submitted by DSO members.
                    </p>
                  </div>
                ) : (
                  <div className="space-y-4">
                    {proposals.map((proposal: any, index: number) => {
                      const isHighlighted = highlightedProposalId === proposal.id;
                      return (
                        <Collapsible key={index} defaultOpen={isHighlighted}>
                          <div
                            ref={(el) => {
                              if (el && proposal.id) proposalRefs.current.set(proposal.id, el);
                            }}
                            className={cn(
                              "p-6 rounded-lg bg-muted/30 hover:bg-muted/50 transition-all border",
                              isHighlighted ? "border-pink-500/50 bg-pink-500/10" : "border-border/50"
                            )}
                          >
                            {/* Header row */}
                            <div className="flex items-start justify-between mb-4">
                              <div className="flex items-center gap-3">
                                <div className="gradient-accent p-2 rounded-lg">
                                  {getStatusIcon(proposal.status)}
                                </div>
                                <div>
                                  <h4 className="font-semibold text-lg">{proposal.title}</h4>
                                  <p className="text-sm text-muted-foreground">
                                    Requested by{" "}
                                    <span className="font-medium text-foreground">{proposal.requester}</span>
                                  </p>
                                </div>
                              </div>
                              <Badge className={getStatusColor(proposal.status)}>
                                {proposal.status}
                              </Badge>
                            </div>

                            {/* Reason */}
                            <div className="mb-4 p-3 rounded-lg bg-background/30 border border-border/30">
                              <p className="text-xs font-semibold text-muted-foreground mb-1">Reason</p>
                              {proposal.reasonBody && (
                                <p className="text-sm mb-1">{proposal.reasonBody}</p>
                              )}
                              {proposal.reasonUrl && (
                                <a
                                  href={proposal.reasonUrl}
                                  target="_blank"
                                  rel="noopener noreferrer"
                                  className="text-sm text-primary hover:underline break-all"
                                >
                                  {proposal.reasonUrl}
                                </a>
                              )}
                              {!proposal.reasonBody && !proposal.reasonUrl && (
                                <p className="text-sm text-muted-foreground italic">No reason provided</p>
                              )}
                            </div>

                            {/* Stats grid */}
                            <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-4">
                              <div className="p-3 rounded-lg bg-background/50">
                                <p className="text-xs text-muted-foreground mb-1">Votes For</p>
                                <p className="text-lg font-bold text-success">{proposal.votesFor}</p>
                              </div>
                              <div className="p-3 rounded-lg bg-background/50">
                                <p className="text-xs text-muted-foreground mb-1">Votes Against</p>
                                <p className="text-lg font-bold text-destructive">{proposal.votesAgainst}</p>
                              </div>
                              <div className="p-3 rounded-lg bg-background/50">
                                <p className="text-xs text-muted-foreground mb-1">Vote Deadline</p>
                                <p className="text-xs font-mono">{safeFormatDate(proposal.voteBefore)}</p>
                              </div>
                              <div className="p-3 rounded-lg bg-background/50">
                                <p className="text-xs text-muted-foreground mb-1">Target Effective</p>
                                <p className="text-xs font-mono">{safeFormatDate(proposal.targetEffectiveAt)}</p>
                              </div>
                            </div>

                            {/* Votes Cast collapsible */}
                            {proposal.votedSvs?.length > 0 && (
                              <Collapsible>
                                <CollapsibleTrigger asChild>
                                  <Button
                                    variant="ghost"
                                    size="sm"
                                    className="w-full justify-between p-3 h-auto rounded-lg bg-muted/30 border border-border/30 hover:bg-muted/50"
                                  >
                                    <div className="flex items-center gap-2">
                                      <Users className="h-4 w-4" />
                                      <span className="text-sm font-semibold">
                                        Votes Cast ({proposal.votedSvs.length})
                                      </span>
                                    </div>
                                    <div className="flex items-center gap-2">
                                      <Badge variant="outline" className="border-success/50 text-success text-xs">
                                        {proposal.votesFor} ✓
                                      </Badge>
                                      <Badge variant="outline" className="border-destructive/50 text-destructive text-xs">
                                        {proposal.votesAgainst} ✗
                                      </Badge>
                                      <ChevronDown className="h-4 w-4" />
                                    </div>
                                  </Button>
                                </CollapsibleTrigger>
                                <CollapsibleContent className="mt-2 space-y-1 max-h-48 overflow-y-auto">
                                  {proposal.votedSvs.map((sv: any, idx: number) => (
                                    <div
                                      key={idx}
                                      className={`px-3 py-2 rounded border text-sm flex items-center justify-between ${
                                        sv.vote === "accept"
                                          ? "bg-success/5 border-success/30"
                                          : "bg-destructive/5 border-destructive/30"
                                      }`}
                                    >
                                      <span className="font-medium text-xs truncate max-w-[200px]" title={sv.party}>
                                        {sv.party}
                                      </span>
                                      <div className="flex items-center gap-2">
                                        {sv.castAt && (
                                          <span className="text-xs text-muted-foreground hidden sm:inline">
                                            {safeFormatDate(sv.castAt, "MMM d, HH:mm")}
                                          </span>
                                        )}
                                        <Badge
                                          variant="outline"
                                          className={`text-xs ${sv.vote === "accept" ? "border-success text-success" : "border-destructive text-destructive"}`}
                                        >
                                          {sv.vote === "accept" ? "✓" : "✗"}
                                        </Badge>
                                      </div>
                                    </div>
                                  ))}
                                </CollapsibleContent>
                              </Collapsible>
                            )}
                          </div>
                        </Collapsible>
                      );
                    })}
                  </div>
                )}
              </div>
            </Card>
          </TabsContent>
        </Tabs>
      </div>
    </DashboardLayout>
  );
};

export default Governance;
