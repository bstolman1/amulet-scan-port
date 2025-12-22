import { useEffect, useRef } from "react";
import { useSearchParams } from "react-router-dom";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Vote, CheckCircle, XCircle, Clock, Users, Code, DollarSign, History, Database, AlertTriangle } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { scanApi } from "@/lib/api-client";
import { Skeleton } from "@/components/ui/skeleton";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { useLatestACSSnapshot } from "@/hooks/use-acs-snapshots";
import { useAggregatedTemplateData } from "@/hooks/use-aggregated-template-data";
import { useGovernanceEvents } from "@/hooks/use-governance-events";
import { DataSourcesFooter } from "@/components/DataSourcesFooter";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { format } from "date-fns";
import { apiFetch } from "@/lib/duckdb-api-client";
import { cn } from "@/lib/utils";

// Safe date formatter that won't crash on invalid dates
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
  const [searchParams] = useSearchParams();
  const highlightedProposalId = searchParams.get("proposal");
  const proposalRefs = useRef<Map<string, HTMLDivElement>>(new Map());
  const { data: dsoInfo } = useQuery({
    queryKey: ["dsoInfo"],
    queryFn: () => scanApi.fetchDsoInfo(),
    retry: 1,
  });

  const { data: latestSnapshot } = useLatestACSSnapshot();
  const { data: governanceEvents, isLoading: eventsLoading, error: eventsError } = useGovernanceEvents();

  // Fetch snapshot info for the banner
  const { data: snapshotInfo } = useQuery({
    queryKey: ["acs-snapshot-info"],
    queryFn: () => apiFetch<{ data: { migration_id: number; snapshot_time: string; path: string; type: string; file_count: number } | null }>("/api/acs/snapshot-info"),
    retry: 1,
    staleTime: 60 * 1000,
  });

  // Fetch vote requests from LOCAL ACS first
  const {
    data: localVoteRequestsData,
    isLoading: localLoading,
    isError: localError,
  } = useAggregatedTemplateData(undefined, "Splice:DsoRules:VoteRequest");

  // Fetch DsoRules from LOCAL ACS
  const { data: localDsoRulesData } = useAggregatedTemplateData(
    undefined,
    "Splice:DsoRules:DsoRules",
  );

  // Fetch Confirmations from LOCAL ACS
  const { data: localConfirmationsData } = useAggregatedTemplateData(
    undefined,
    "Splice:DsoRules:Confirmation",
  );

  // Check if local ACS has governance data
  const localHasGovernanceData = (localVoteRequestsData?.data?.length || 0) > 0;

  // FALLBACK: Fetch from live Canton Scan API if local ACS has no governance data
  const { data: liveVoteRequestsData, isLoading: liveLoading } = useQuery({
    queryKey: ["live-vote-requests"],
    queryFn: async () => {
      const proposals = await scanApi.fetchGovernanceProposals();
      return { data: proposals, source: "live" };
    },
    enabled: !localLoading && !localHasGovernanceData,
    staleTime: 60 * 1000,
    retry: 1,
  });

  // Use local data if available, otherwise use live fallback
  const voteRequestsData = localHasGovernanceData ? localVoteRequestsData : liveVoteRequestsData;
  const dsoRulesData = localDsoRulesData;
  const confirmationsData = localConfirmationsData;
  const isLoading = localLoading || (liveLoading && !localHasGovernanceData);
  const isError = localError && !liveVoteRequestsData;
  const isUsingLiveFallback = !localHasGovernanceData && !!liveVoteRequestsData;

  // Debug: Log data loading status
  console.log("🔍 Governance Data Status:", {
    localVoteRequests: localVoteRequestsData?.data?.length ?? "loading",
    liveVoteRequests: liveVoteRequestsData?.data?.length ?? "not loaded",
    usingLiveFallback: isUsingLiveFallback,
    dsoRules: dsoRulesData?.data?.length ?? "loading",
    confirmations: confirmationsData?.data?.length ?? "loading",
    events: governanceEvents?.length ?? "loading",
  });

  // Scroll to highlighted proposal when data loads
  useEffect(() => {
    if (highlightedProposalId && !isLoading) {
      // Small delay to ensure DOM is rendered
      const timer = setTimeout(() => {
        const element = proposalRefs.current.get(highlightedProposalId);
        if (element) {
          element.scrollIntoView({ behavior: "smooth", block: "center" });
          // Add a brief flash effect
          element.classList.add("ring-2", "ring-pink-500", "ring-offset-2", "ring-offset-background");
          setTimeout(() => {
            element.classList.remove("ring-2", "ring-pink-500", "ring-offset-2", "ring-offset-background");
          }, 3000);
        }
      }, 300);
      return () => clearTimeout(timer);
    }
  }, [highlightedProposalId, isLoading]);

  const confirmations = confirmationsData?.data || [];

  // Get SV count and voting threshold from DsoRules
  // Handle both flat and nested payload structure from DuckDB
  const dsoRulesRaw = dsoRulesData?.data?.[0];
  const dsoRulesPayload = dsoRulesRaw?.payload || dsoRulesRaw || {};
  const svs = dsoRulesPayload.svs || {};
  const svCount = Object.keys(svs).length || 0;
  const votingThreshold = dsoInfo?.voting_threshold || Math.ceil(svCount * 0.67) || 1; // 2/3 majority

  // Helper to safely extract field values from nested structure
  const getField = (record: any, ...fieldNames: string[]) => {
    if (!record) return undefined;
    for (const field of fieldNames) {
      if (record[field] !== undefined && record[field] !== null) return record[field];
      if (record.payload?.[field] !== undefined && record.payload?.[field] !== null) return record.payload[field];
    }
    return undefined;
  };

  // Helper to parse action structure and extract meaningful title
  const parseAction = (action: any): { title: string; actionType: string; actionDetails: any } => {
    if (!action) return { title: "Unknown Action", actionType: "Unknown", actionDetails: null };
    
    // Handle nested tag/value structure: { tag: "ARC_DsoRules", value: { dsoAction: { tag: "SRARC_...", value: {...} } } }
    const outerTag = action.tag || Object.keys(action)[0] || "Unknown";
    const outerValue = action.value || action[outerTag] || action;
    
    // Extract inner action (e.g., dsoAction)
    const innerAction = outerValue?.dsoAction || outerValue?.amuletRulesAction || outerValue;
    const innerTag = innerAction?.tag || "";
    const innerValue = innerAction?.value || innerAction;
    
    // Build human-readable title
    const actionType = innerTag || outerTag;
    const title = actionType
      .replace(/^(SRARC_|ARC_|CRARC_|ARAC_)/, "")
      .replace(/([A-Z])/g, " $1")
      .trim();
    
    return { title, actionType, actionDetails: innerValue };
  };

  // Helper to parse votes array (format: [[svName, voteObj], ...])
  const parseVotes = (votes: any): { votesFor: number; votesAgainst: number; votedSvs: any[] } => {
    if (!votes) return { votesFor: 0, votesAgainst: 0, votedSvs: [] };
    
    // Handle array of tuples format: [["SV Name", { sv, accept, reason, optCastAt }], ...]
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
        sv: voteData?.sv || svName,
        vote: isAccept ? "accept" : isReject ? "reject" : "abstain",
        reason: voteData?.reason?.body || voteData?.reason || "",
        reasonUrl: voteData?.reason?.url || "",
        castAt: voteData?.optCastAt || null,
      });
    }
    
    return { votesFor, votesAgainst, votedSvs };
  };

  // Process proposals from ACS data with full JSON parsing
  const proposals =
    voteRequestsData?.data?.map((voteRequest: any) => {
      // Handle both flat structure and nested payload structure from DuckDB
      const payload = voteRequest.payload || voteRequest;
      
      // Parse action
      const action = payload.action || voteRequest.action || {};
      const { title, actionType, actionDetails } = parseAction(action);
      
      // Parse votes
      const votesRaw = payload.votes || voteRequest.votes || [];
      const { votesFor, votesAgainst, votedSvs } = parseVotes(votesRaw);

      // Extract requester information
      const requester = payload.requester || voteRequest.requester || "Unknown";

      // Extract reason (has url and body)
      const reasonObj = payload.reason || voteRequest.reason || {};
      const reasonBody = reasonObj?.body || (typeof reasonObj === "string" ? reasonObj : "");
      const reasonUrl = reasonObj?.url || "";

      // Extract timing fields
      const voteBefore = payload.voteBefore || voteRequest.voteBefore;
      const targetEffectiveAt = payload.targetEffectiveAt || voteRequest.targetEffectiveAt;
      const trackingCid = payload.trackingCid || voteRequest.trackingCid || voteRequest.contract_id;

      // Determine status based on votes and threshold
      const threshold = votingThreshold || svCount || 1;
      let status: "approved" | "rejected" | "pending" = "pending";
      
      // Check if voting deadline has passed
      const now = new Date();
      const voteDeadline = voteBefore ? new Date(voteBefore) : null;
      const isExpired = voteDeadline && voteDeadline < now;
      
      // Only mark as approved if enough votes FOR
      if (votesFor >= threshold) {
        status = "approved";
      } 
      // Only mark as rejected if deadline passed AND not enough votes
      else if (isExpired && votesFor < threshold) {
        status = "rejected";
      }
      // Otherwise it's still pending (deadline not reached or voting ongoing)

      return {
        id: trackingCid?.slice(0, 12) || "unknown",
        contractId: voteRequest.contract_id || trackingCid,
        trackingCid,
        title,
        actionType,
        actionDetails,
        action, // Keep full action for detailed display
        reasonBody,
        reasonUrl,
        requester,
        status,
        votesFor,
        votesAgainst,
        votedSvs,
        voteBefore,
        targetEffectiveAt,
        rawData: voteRequest,
      };
    }) || [];

  const totalProposals = proposals?.length || 0;
  const activeProposals = proposals?.filter((p: any) => p.status === "pending").length || 0;

  const getStatusColor = (status: string) => {
    switch (status) {
      case "approved":
        return "bg-success/10 text-success border-success/20";
      case "rejected":
        return "bg-destructive/10 text-destructive border-destructive/20";
      case "pending":
        return "bg-warning/10 text-warning border-warning/20";
      default:
        return "bg-muted text-muted-foreground";
    }
  };

  const getStatusIcon = (status: string) => {
    switch (status) {
      case "approved":
        return <CheckCircle className="h-4 w-4" />;
      case "rejected":
        return <XCircle className="h-4 w-4" />;
      case "pending":
        return <Clock className="h-4 w-4" />;
      default:
        return <Vote className="h-4 w-4" />;
    }
  };

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-3xl font-bold mb-2">Governance</h2>
            <p className="text-muted-foreground">DSO proposals and voting activity</p>
          </div>
        </div>

        {/* Snapshot Info Banner */}
        {snapshotInfo?.data && (
          <Alert className="bg-muted/50 border-primary/20">
            <Database className="h-4 w-4" />
            <AlertDescription className="flex flex-wrap items-center gap-x-4 gap-y-1 text-sm">
              <span>
                <strong>ACS Snapshot:</strong> Migration {snapshotInfo.data.migration_id}
              </span>
              <span>
                <strong>Time:</strong> {format(new Date(snapshotInfo.data.snapshot_time), "PPpp")}
              </span>
              <span>
                <strong>Files:</strong> {snapshotInfo.data.file_count}
              </span>
              <span className="max-w-[28rem] truncate" title={snapshotInfo.data.path}>
                <strong>Path:</strong> {snapshotInfo.data.path}
              </span>
              <Badge variant="outline" className="text-xs">
                {snapshotInfo.data.type}
              </Badge>
            </AlertDescription>
          </Alert>
        )}

        {/* Live Fallback Warning */}
        {isUsingLiveFallback && (
          <Alert className="bg-yellow-500/10 border-yellow-500/30">
            <AlertTriangle className="h-4 w-4 text-yellow-500" />
            <AlertDescription className="text-sm">
              <strong>Using live Canton Scan API</strong> — Local ACS snapshot doesn't contain VoteRequest contracts. 
              Governance data is being fetched from the live network.
            </AlertDescription>
          </Alert>
        )}

        {/* Summary Cards */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <Card className="glass-card p-6">
            <div className="flex items-center justify-between mb-2">
              <h3 className="text-sm font-medium text-muted-foreground">Super Validators</h3>
              <Users className="h-5 w-5 text-primary" />
            </div>
            {!svCount ? (
              <Skeleton className="h-10 w-full" />
            ) : (
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
            {!votingThreshold ? (
              <Skeleton className="h-10 w-full" />
            ) : (
              <>
                <p className="text-3xl font-bold text-chart-3 mb-1">{votingThreshold}</p>
                <p className="text-xs text-muted-foreground">Votes required</p>
              </>
            )}
          </Card>

          <Card className="glass-card p-6">
            <div className="flex items-center justify-between mb-2">
              <h3 className="text-sm font-medium text-muted-foreground">Total Proposals</h3>
              <Vote className="h-5 w-5 text-chart-2" />
            </div>
            {isLoading ? (
              <Skeleton className="h-10 w-full" />
            ) : (
              <>
                <p className="text-3xl font-bold text-chart-2 mb-1">{totalProposals}</p>
                <p className="text-xs text-muted-foreground">All time</p>
              </>
            )}
          </Card>

          <Card className="glass-card p-6">
            <div className="flex items-center justify-between mb-2">
              <h3 className="text-sm font-medium text-muted-foreground">Active Proposals</h3>
              <Clock className="h-5 w-5 text-warning" />
            </div>
            {isLoading ? (
              <Skeleton className="h-10 w-full" />
            ) : (
              <>
                <p className="text-3xl font-bold text-warning mb-1">{activeProposals}</p>
                <p className="text-xs text-muted-foreground">In voting</p>
              </>
            )}
          </Card>

          <Card className="glass-card p-6">
            <div className="flex items-center justify-between mb-2">
              <h3 className="text-sm font-medium text-muted-foreground">DSO Party</h3>
              <Vote className="h-5 w-5 text-chart-3" />
            </div>
            {!dsoInfo ? (
              <Skeleton className="h-10 w-full" />
            ) : (
              <>
                <p className="text-xs font-mono text-chart-3 mb-1 truncate">{dsoInfo.dso_party_id.split("::")[0]}</p>
                <p className="text-xs text-muted-foreground">Governance entity</p>
              </>
            )}
          </Card>
        </div>

        {/* Info Alert */}
        <Alert>
          <Vote className="h-4 w-4" />
          <AlertDescription>
            Governance proposals are voted on by Super Validators. A proposal requires{" "}
            <strong>{dsoInfo?.voting_threshold || "N"}</strong> votes to pass. Proposals can include network parameter
            changes, featured app approvals, and other critical network decisions.
          </AlertDescription>
        </Alert>

        {/* Proposals List */}
        <Tabs defaultValue="active" className="space-y-6">
          <TabsList>
            <TabsTrigger value="active">Active Proposals</TabsTrigger>
            <TabsTrigger value="history">Governance History</TabsTrigger>
          </TabsList>

          <TabsContent value="active">
            <Card className="glass-card">
              <div className="p-6">
                <h3 className="text-xl font-bold mb-6 flex items-center gap-2">
                  Active Proposals
                  <Badge variant="secondary" className="ml-2 text-xs">
                    ACS Snapshot
                  </Badge>
                  {proposals?.length > 0 && (
                    <Badge variant="outline" className="ml-1">
                      {proposals.length} active
                    </Badge>
                  )}
                </h3>

            {isError ? (
              <div className="text-center py-12">
                <Vote className="h-12 w-12 text-muted-foreground mx-auto mb-4" />
                <p className="text-muted-foreground mb-2">
                  Unable to load proposals from local ACS data.
                </p>
                <p className="text-xs text-muted-foreground">
                  Ensure the local server is running (cd server && npm start) or check network connectivity.
                </p>
              </div>
            ) : isLoading ? (
              <div className="space-y-4">
                {[1, 2, 3].map((i) => (
                  <Skeleton key={i} className="h-32 w-full" />
                ))}
              </div>
            ) : !proposals?.length ? (
              <div className="text-center py-12">
                <Vote className="h-12 w-12 text-muted-foreground mx-auto mb-4" />
                <p className="text-muted-foreground mb-2">No proposals available at the moment</p>
                <p className="text-sm text-muted-foreground">
                  Governance proposals will appear here when submitted by DSO members
                </p>
              </div>
            ) : (
              <div className="space-y-4">
                {proposals?.map((proposal: any, index: number) => {
                  const isHighlighted = highlightedProposalId === proposal.id;
                  return (
                  <Collapsible key={index} defaultOpen={isHighlighted}>
                    <div 
                      ref={(el) => {
                        if (el && proposal.id) {
                          proposalRefs.current.set(proposal.id, el);
                        }
                      }}
                      className={cn(
                        "p-6 rounded-lg bg-muted/30 hover:bg-muted/50 transition-all border",
                        isHighlighted 
                          ? "border-pink-500/50 bg-pink-500/10" 
                          : "border-border/50"
                      )}
                    >
                      <div className="flex items-start justify-between mb-4">
                        <div className="flex items-center space-x-3">
                          <div className="gradient-accent p-2 rounded-lg">{getStatusIcon(proposal.status)}</div>
                          <div className="flex-1">
                            <h4 className="font-semibold text-lg">{proposal.title}</h4>
                            <p className="text-sm text-muted-foreground">
                              Proposal #{proposal.id}
                              <span className="mx-2">•</span>
                              <span className="font-mono text-xs">{proposal.actionType}</span>
                            </p>
                            <p className="text-xs text-muted-foreground mt-1">
                              Requested by: <span className="font-medium text-foreground">{proposal.requester}</span>
                            </p>
                          </div>
                        </div>
                        <Badge className={getStatusColor(proposal.status)}>{proposal.status}</Badge>
                      </div>

                      {/* Action Details */}
                      {proposal.actionDetails && typeof proposal.actionDetails === "object" && (
                        <div className="mb-4 p-3 rounded-lg bg-primary/5 border border-primary/20">
                          <p className="text-sm text-muted-foreground mb-2 font-semibold">Action Details:</p>
                          <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 text-sm">
                            {Object.entries(proposal.actionDetails)
                              .filter(([_, value]) => value !== null && value !== undefined)
                              .map(([key, value]: [string, any]) => (
                              <div key={key} className="flex flex-col">
                                <span className="text-xs text-muted-foreground capitalize">{key.replace(/([A-Z])/g, " $1").trim()}</span>
                                <span className="font-mono text-xs break-all">
                                  {typeof value === "string" || typeof value === "number" 
                                    ? String(value) 
                                    : JSON.stringify(value)}
                                </span>
                              </div>
                            ))}
                          </div>
                        </div>
                      )}

                      {/* Reason Section */}
                      <div className="mb-4 p-3 rounded-lg bg-background/30 border border-border/30">
                        <p className="text-sm text-muted-foreground mb-1 font-semibold">Reason:</p>
                        {proposal.reasonBody && typeof proposal.reasonBody === "string" && (
                          <p className="text-sm mb-2">{proposal.reasonBody}</p>
                        )}
                        {proposal.reasonUrl && typeof proposal.reasonUrl === "string" && (
                          <a 
                            href={proposal.reasonUrl} 
                            target="_blank" 
                            rel="noopener noreferrer"
                            className="text-sm text-primary hover:underline break-all"
                          >
                            {proposal.reasonUrl}
                          </a>
                        )}
                        {(!proposal.reasonBody || typeof proposal.reasonBody !== "string") && 
                         (!proposal.reasonUrl || typeof proposal.reasonUrl !== "string") && (
                          <p className="text-sm text-muted-foreground italic">No reason provided</p>
                        )}
                      </div>

                      {/* Stats Grid */}
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
                          <p className="text-xs text-muted-foreground mb-1">Target Effective</p>
                          <p className="text-xs font-mono">
                            {safeFormatDate(proposal.targetEffectiveAt)}
                          </p>
                        </div>
                        <div className="p-3 rounded-lg bg-background/50">
                          <p className="text-xs text-muted-foreground mb-1">Vote Deadline</p>
                          <p className="text-xs font-mono">
                            {safeFormatDate(proposal.voteBefore)}
                          </p>
                        </div>
                      </div>

                      {/* Votes Cast */}
                      {proposal.votedSvs?.length > 0 && (
                        <div className="mb-4">
                          <p className="text-xs text-muted-foreground mb-2 font-semibold">Votes Cast ({proposal.votedSvs.length}):</p>
                          <div className="space-y-2">
                            {proposal.votedSvs.map((sv: any, idx: number) => (
                              <div 
                                key={idx}
                                className={`p-2 rounded border text-sm ${
                                  sv.vote === "accept" 
                                    ? "bg-success/5 border-success/30" 
                                    : "bg-destructive/5 border-destructive/30"
                                }`}
                              >
                                <div className="flex items-center justify-between mb-1">
                                  <span className="font-medium">{sv.party}</span>
                                  <Badge 
                                    variant="outline" 
                                    className={sv.vote === "accept" ? "border-success text-success" : "border-destructive text-destructive"}
                                  >
                                    {sv.vote === "accept" ? "✓ Accept" : "✗ Reject"}
                                  </Badge>
                                </div>
                                {sv.reason && typeof sv.reason === "string" && (
                                  <p className="text-xs text-muted-foreground italic">"{sv.reason}"</p>
                                )}
                                {sv.castAt && (
                                  <p className="text-xs text-muted-foreground mt-1">
                                    Cast: {safeFormatDate(sv.castAt)}
                                  </p>
                                )}
                              </div>
                            ))}
                          </div>
                        </div>
                      )}

                      <CollapsibleTrigger asChild>
                        <Button variant="ghost" size="sm" className="w-full mt-2">
                          <Code className="h-4 w-4 mr-2" />
                          View Full JSON Data
                        </Button>
                      </CollapsibleTrigger>

                      <CollapsibleContent className="mt-4">
                        <div className="p-4 rounded-lg bg-background/70 border border-border/50">
                          <p className="text-xs text-muted-foreground mb-2 font-semibold">
                            Contract ID: <span className="font-mono">{proposal.contractId}</span>
                          </p>
                          <pre className="text-xs overflow-x-auto p-3 bg-muted/30 rounded border border-border/30 max-h-96">
                            {JSON.stringify(proposal.rawData, null, 2)}
                          </pre>
                        </div>
                      </CollapsibleContent>
                    </div>
                  </Collapsible>
                  );
                })}
              </div>
            )}
          </div>
        </Card>
      </TabsContent>

      <TabsContent value="history">
        <Card className="glass-card">
          <div className="p-6">
            <h3 className="text-xl font-bold mb-6 flex items-center gap-2">
              <History className="h-5 w-5" />
              Governance History
              <Badge variant="secondary" className="ml-2 text-xs">
                Ledger Events
              </Badge>
              {governanceEvents?.length ? (
                <Badge variant="outline" className="ml-1">
                  {governanceEvents.filter((e: any) => e.template_id?.includes('VoteRequest')).length} archived
                </Badge>
              ) : null}
            </h3>
            
            <Alert className="mb-4 bg-muted/30 border-border/50">
              <Database className="h-4 w-4" />
              <AlertDescription className="text-sm">
                History shows <strong>archived</strong> VoteRequest contracts from ledger backfill/updates. 
                These are proposals that have been completed (approved/rejected/expired).
              </AlertDescription>
            </Alert>
            
            {eventsLoading ? (
              <div className="space-y-3">
                {[1, 2, 3].map((i) => (
                  <Skeleton key={i} className="h-32 w-full" />
                ))}
              </div>
            ) : !governanceEvents?.length ? (
              <div className="text-center py-12">
                <Vote className="h-12 w-12 text-muted-foreground mx-auto mb-4" />
                <p className="text-muted-foreground mb-2">No historical governance events found</p>
                <p className="text-xs text-muted-foreground mb-4">
                  Governance history requires ledger backfill data with archived VoteRequest events.
                </p>
                <p className="text-xs text-muted-foreground">
                  Run backfill ingestion to populate historical governance data.
                </p>
              </div>
            ) : (
              <div className="space-y-4">
                {governanceEvents
                  .filter((event: any) => event.template_id?.includes('VoteRequest'))
                  .slice(0, 100)
                  .map((event: any, index: number) => {
                    // Parse the event the same way as active proposals
                    const payload = event.payload || event.event_data || event;
                    const action = payload.action || {};
                    const { title, actionType, actionDetails } = parseAction(action);
                    
                    const votesRaw = payload.votes || [];
                    const { votesFor, votesAgainst, votedSvs } = parseVotes(votesRaw);
                    
                    const requester = payload.requester || "Unknown";
                    const reasonObj = payload.reason || {};
                    const reasonBody = reasonObj?.body || (typeof reasonObj === "string" ? reasonObj : "");
                    const reasonUrl = reasonObj?.url || "";
                    const voteBefore = payload.voteBefore;
                    const targetEffectiveAt = payload.targetEffectiveAt;
                    const trackingCid = payload.trackingCid || event.contract_id;
                    
                    // Determine status
                    const threshold = votingThreshold || svCount || 1;
                    let status: "approved" | "rejected" | "pending" | "archived" = "archived";
                    const now = new Date();
                    const voteDeadline = voteBefore ? new Date(voteBefore) : null;
                    const isExpired = voteDeadline && voteDeadline < now;
                    
                    if (votesFor >= threshold) {
                      status = "approved";
                    } else if (isExpired && votesFor < threshold) {
                      status = "rejected";
                    } else if (event.event_type === "archived") {
                      status = "archived";
                    }
                    
                    const eventTs = event.effective_at || event.timestamp || event.created_at;
                    
                    return (
                      <Collapsible key={event.event_id || `${event.contract_id}-${index}`}>
                        <div className="p-6 rounded-lg bg-muted/30 hover:bg-muted/50 transition-all border border-border/50">
                          <div className="flex items-start justify-between mb-4">
                            <div className="flex items-center space-x-3">
                              <div className="gradient-accent p-2 rounded-lg">{getStatusIcon(status)}</div>
                              <div className="flex-1">
                                <h4 className="font-semibold text-lg">{title}</h4>
                                <p className="text-sm text-muted-foreground">
                                  {trackingCid?.slice(0, 12) || "unknown"}
                                  <span className="mx-2">•</span>
                                  <span className="font-mono text-xs">{actionType}</span>
                                  <span className="mx-2">•</span>
                                  <span className="text-xs">{event.event_type}</span>
                                </p>
                                <p className="text-xs text-muted-foreground mt-1">
                                  Requested by: <span className="font-medium text-foreground">{requester}</span>
                                </p>
                              </div>
                            </div>
                            <Badge className={getStatusColor(status)}>{status}</Badge>
                          </div>

                          {/* Action Details */}
                          {actionDetails && typeof actionDetails === "object" && Object.keys(actionDetails).length > 0 && (
                            <div className="mb-4 p-3 rounded-lg bg-primary/5 border border-primary/20">
                              <p className="text-sm text-muted-foreground mb-2 font-semibold">Action Details:</p>
                              <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 text-sm">
                                {Object.entries(actionDetails)
                                  .filter(([_, value]) => value !== null && value !== undefined)
                                  .slice(0, 8)
                                  .map(([key, value]: [string, any]) => (
                                  <div key={key} className="flex flex-col">
                                    <span className="text-xs text-muted-foreground capitalize">{key.replace(/([A-Z])/g, " $1").trim()}</span>
                                    <span className="font-mono text-xs break-all">
                                      {typeof value === "string" || typeof value === "number" 
                                        ? String(value).slice(0, 100) 
                                        : JSON.stringify(value).slice(0, 100)}
                                    </span>
                                  </div>
                                ))}
                              </div>
                            </div>
                          )}

                          {/* Reason Section */}
                          <div className="mb-4 p-3 rounded-lg bg-background/30 border border-border/30">
                            <p className="text-sm text-muted-foreground mb-1 font-semibold">Reason:</p>
                            {reasonBody && typeof reasonBody === "string" && (
                              <p className="text-sm mb-2">{reasonBody}</p>
                            )}
                            {reasonUrl && typeof reasonUrl === "string" && (
                              <a 
                                href={reasonUrl} 
                                target="_blank" 
                                rel="noopener noreferrer"
                                className="text-sm text-primary hover:underline break-all"
                              >
                                {reasonUrl}
                              </a>
                            )}
                            {(!reasonBody || typeof reasonBody !== "string") && 
                             (!reasonUrl || typeof reasonUrl !== "string") && (
                              <p className="text-sm text-muted-foreground italic">No reason provided</p>
                            )}
                          </div>

                          {/* Stats Grid */}
                          <div className="grid grid-cols-2 md:grid-cols-5 gap-3 mb-4">
                            <div className="p-3 rounded-lg bg-background/50">
                              <p className="text-xs text-muted-foreground mb-1">Votes For</p>
                              <p className="text-lg font-bold text-success">{votesFor}</p>
                            </div>
                            <div className="p-3 rounded-lg bg-background/50">
                              <p className="text-xs text-muted-foreground mb-1">Votes Against</p>
                              <p className="text-lg font-bold text-destructive">{votesAgainst}</p>
                            </div>
                            <div className="p-3 rounded-lg bg-background/50">
                              <p className="text-xs text-muted-foreground mb-1">Event Time</p>
                              <p className="text-xs font-mono">
                                {safeFormatDate(eventTs)}
                              </p>
                            </div>
                            <div className="p-3 rounded-lg bg-background/50">
                              <p className="text-xs text-muted-foreground mb-1">Target Effective</p>
                              <p className="text-xs font-mono">
                                {safeFormatDate(targetEffectiveAt)}
                              </p>
                            </div>
                            <div className="p-3 rounded-lg bg-background/50">
                              <p className="text-xs text-muted-foreground mb-1">Vote Deadline</p>
                              <p className="text-xs font-mono">
                                {safeFormatDate(voteBefore)}
                              </p>
                            </div>
                          </div>

                          {/* Votes Cast */}
                          {votedSvs?.length > 0 && (
                            <div className="mb-4">
                              <p className="text-xs text-muted-foreground mb-2 font-semibold">Votes Cast ({votedSvs.length}):</p>
                              <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
                                {votedSvs.slice(0, 10).map((sv: any, idx: number) => (
                                  <div 
                                    key={idx}
                                    className={`p-2 rounded border text-sm ${
                                      sv.vote === "accept" 
                                        ? "bg-success/5 border-success/30" 
                                        : "bg-destructive/5 border-destructive/30"
                                    }`}
                                  >
                                    <div className="flex items-center justify-between">
                                      <span className="font-medium text-xs truncate max-w-[200px]">{sv.party}</span>
                                      <Badge 
                                        variant="outline" 
                                        className={`text-xs ${sv.vote === "accept" ? "border-success text-success" : "border-destructive text-destructive"}`}
                                      >
                                        {sv.vote === "accept" ? "✓" : "✗"}
                                      </Badge>
                                    </div>
                                  </div>
                                ))}
                              </div>
                              {votedSvs.length > 10 && (
                                <p className="text-xs text-muted-foreground mt-2">... and {votedSvs.length - 10} more votes</p>
                              )}
                            </div>
                          )}

                          <CollapsibleTrigger asChild>
                            <Button variant="ghost" size="sm" className="w-full mt-2">
                              <Code className="h-4 w-4 mr-2" />
                              View Full JSON Data
                            </Button>
                          </CollapsibleTrigger>

                          <CollapsibleContent className="mt-4">
                            <div className="p-4 rounded-lg bg-background/70 border border-border/50">
                              <p className="text-xs text-muted-foreground mb-2 font-semibold">
                                Event ID: <span className="font-mono">{event.event_id}</span>
                              </p>
                              <p className="text-xs text-muted-foreground mb-2 font-semibold">
                                Contract ID: <span className="font-mono">{event.contract_id}</span>
                              </p>
                              <pre className="text-xs overflow-x-auto p-3 bg-muted/30 rounded border border-border/30 max-h-96">
                                {JSON.stringify(event, null, 2)}
                              </pre>
                            </div>
                          </CollapsibleContent>
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

        {/* Governance Info */}
        <Card className="glass-card">
          <div className="p-6">
            <h3 className="text-xl font-bold mb-4">About Canton Network Governance</h3>
            <div className="space-y-4 text-muted-foreground">
              <p>
                The Canton Network is governed by the Decentralized System Operator (DSO), which consists of Super
                Validators who participate in governance decisions through proposals and voting.
              </p>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mt-6">
                <div className="p-4 rounded-lg bg-muted/30">
                  <h4 className="font-semibold text-foreground mb-2">Voting Process</h4>
                  <p className="text-sm">
                    Proposals require a minimum threshold of votes from Super Validators to be approved. The current
                    threshold is {dsoInfo?.voting_threshold || "N"} votes.
                  </p>
                </div>
                <div className="p-4 rounded-lg bg-muted/30">
                  <h4 className="font-semibold text-foreground mb-2">Proposal Types</h4>
                  <p className="text-sm">
                    Governance includes network parameters, featured app approvals, validator onboarding, and other
                    critical network decisions.
                  </p>
                </div>
              </div>
            </div>
          </div>
        </Card>

        <DataSourcesFooter
          snapshotId={latestSnapshot?.id}
          templateSuffixes={[
            "Splice:DsoRules:VoteRequest",
            "Splice:DsoRules:DsoRules",
            "Splice:DsoRules:Confirmation",
          ]}
          isProcessing={false}
        />
      </div>
    </DashboardLayout>
  );
};

export default Governance;
