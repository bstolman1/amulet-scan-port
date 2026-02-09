import { useEffect, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Vote, CheckCircle, XCircle, Clock, Users, Code, DollarSign, History, Database, AlertTriangle, ChevronDown, Loader2, Link2, Server, Hash, Globe } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { scanApi } from "@/lib/api-client";
import { Skeleton } from "@/components/ui/skeleton";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { useAggregatedTemplateData } from "@/hooks/use-aggregated-template-data";
import { useGovernanceEvents } from "@/hooks/use-governance-events";
import { useUniqueProposals } from "@/hooks/use-unique-proposals";

import { useGovernanceProposals } from "@/hooks/use-governance-proposals";
import { useActiveVoteRequests } from "@/hooks/use-active-vote-requests";

import { useCanonicalProposals, useDedupeStats, parseCanonicalAction, parseCanonicalVotes } from "@/hooks/use-canonical-proposals";
import { DataSourcesFooter } from "@/components/DataSourcesFooter";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { format } from "date-fns";
import { apiFetch } from "@/lib/duckdb-api-client";
import { cn } from "@/lib/utils";
import { GovernanceHistoryTable } from "@/components/GovernanceHistoryTable";
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
  const [searchParams, setSearchParams] = useSearchParams();
  const highlightedProposalId = searchParams.get("proposal");
  const activeTab = searchParams.get("tab") || "scanapi";
  const proposalRefs = useRef<Map<string, HTMLDivElement>>(new Map());
  
  // Handle tab changes via URL
  const handleTabChange = (value: string) => {
    setSearchParams((prev) => {
      const newParams = new URLSearchParams(prev);
      newParams.set("tab", value);
      // Clear proposal when switching tabs
      if (value !== activeTab) {
        newParams.delete("proposal");
      }
      return newParams;
    });
  };
  
  const { data: dsoInfo } = useQuery({
    queryKey: ["dsoInfo"],
    queryFn: () => scanApi.fetchDsoInfo(),
    retry: 1,
  });
  

  const { data: governanceEventsResult, isLoading: eventsLoading, error: eventsError } = useGovernanceEvents();
  const governanceEvents = governanceEventsResult?.events;
  const dataSource = governanceEventsResult?.source;
  const fromIndex = governanceEventsResult?.fromIndex;

  // Fetch snapshot info for the banner
  const { data: snapshotInfo } = useQuery({
    queryKey: ["acs-snapshot-info"],
    queryFn: () => apiFetch<{ data: { migration_id: number; snapshot_time: string; path: string; type: string; file_count: number } | null }>("/api/acs/snapshot-info"),
    retry: 1,
    staleTime: 60 * 1000,
  });

  // Fetch active vote requests from Scan API (PRIMARY SOURCE for active governance)
  const {
    data: activeVoteRequestsData,
    isLoading: activeVoteRequestsLoading,
    isError: activeVoteRequestsError,
  } = useActiveVoteRequests();

  // Fallback: Fetch vote requests from LOCAL ACS (if Scan API fails)
  const {
    data: voteRequestsData,
    isLoading: localLoading,
    isError: localError,
  } = useAggregatedTemplateData(undefined, "Splice:DsoRules:VoteRequest");

  // Combined loading/error states - prefer Scan API data
  const isLoading = activeVoteRequestsLoading && localLoading;
  const isError = activeVoteRequestsError && localError;


  // Fetch Confirmations from LOCAL ACS
  const { data: confirmationsData } = useAggregatedTemplateData(
    undefined,
    "Splice:DsoRules:Confirmation",
  );

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

  // Get SV count from Scan API dsoInfo (sv_node_states array)
  const svCount = dsoInfo?.sv_node_states?.length || 0;
  const votingThreshold = dsoInfo?.voting_threshold || Math.ceil(svCount * 0.67) || 1; // 2/3 majority

  // Unique proposals (deduplicated by proposal hash + action type)
  const { 
    proposals: uniqueProposals, 
    stats: uniqueStats, 
    isLoading: uniqueLoading, 
    rawEventCount,
    fromIndex: uniqueFromIndex,
    dataSource: uniqueDataSource,
  } = useUniqueProposals(votingThreshold);

  // Semantic grouped proposals from the new endpoint
  const {
    data: semanticProposals,
    isLoading: semanticLoading,
    error: semanticError,
  } = useGovernanceProposals();


  // Canonical proposals from DuckDB index (deduplicated by proposal_id)
  // This is the PRIMARY source for historical governance data matching explorer counts
  const {
    data: canonicalData,
    isLoading: canonicalLoading,
    error: canonicalError,
  } = useCanonicalProposals({ limit: 500, humanOnly: true });

  // Dedupe stats for debugging
  const { data: dedupeStats } = useDedupeStats();

  // Helper to safely extract field values from nested structure
  const getField = (record: any, ...fieldNames: string[]) => {
    if (!record) return undefined;
    for (const field of fieldNames) {
      if (record[field] !== undefined && record[field] !== null) return record[field];
      if (record.payload?.[field] !== undefined && record.payload?.[field] !== null) return record.payload[field];
    }
    return undefined;
  };

  // Helper to extract simple displayable values from nested objects
  const extractSimpleFields = (obj: any, prefix = "", depth = 0): Record<string, string> => {
    if (!obj || depth > 2) return {}; // Limit depth to prevent huge JSON dumps
    
    const result: Record<string, string> = {};
    
    for (const [key, value] of Object.entries(obj)) {
      // Skip internal/technical fields
      if (["tag", "value", "packageId", "moduleName", "entityName", "dso"].includes(key)) continue;
      
      const fieldName = prefix ? `${prefix}.${key}` : key;
      
      if (value === null || value === undefined) continue;
      
      if (typeof value === "string") {
        // Only include if it's not a huge hash/ID
        if (value.length < 100 && !value.match(/^[a-f0-9]{64}$/i)) {
          result[fieldName] = value;
        }
      } else if (typeof value === "number" || typeof value === "boolean") {
        result[fieldName] = String(value);
      } else if (Array.isArray(value)) {
        // Show array length instead of contents
        if (value.length > 0 && typeof value[0] !== "object") {
          result[fieldName] = value.slice(0, 3).join(", ") + (value.length > 3 ? ` (+${value.length - 3} more)` : "");
        } else {
          result[fieldName] = `[${value.length} items]`;
        }
      } else if (typeof value === "object") {
        // Recurse into nested objects but limit depth
        const nested = extractSimpleFields(value, fieldName, depth + 1);
        Object.assign(result, nested);
      }
    }
    
    return result;
  };

  // Helper to parse action structure and extract meaningful title
  const parseAction = (action: any): { title: string; actionType: string; actionDetails: Record<string, string> } => {
    if (!action) return { title: "Unknown Action", actionType: "Unknown", actionDetails: {} };
    
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
    
    // Extract only simple displayable fields
    const actionDetails = extractSimpleFields(innerValue);
    
    return { title, actionType, actionDetails };
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

  // Use Scan API data as primary source, fallback to local ACS
  const rawVoteRequests = activeVoteRequestsData?.data || voteRequestsData?.data || [];
  const dataSourceLabel = activeVoteRequestsData?.data?.length ? "Scan API" : (voteRequestsData?.data?.length ? "Local ACS" : "No data");

  // Process proposals from Scan API or ACS data with full JSON parsing
  const proposals =
    rawVoteRequests.map((voteRequest: any) => {
      // Handle Scan API format: { contract: {...}, state: {...} }
      // vs ACS format: { payload: {...} } or flat structure
      const contract = voteRequest.contract || voteRequest;
      const payload = contract.payload || contract.create_arguments || contract;
      
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
      const trackingCid = payload.trackingCid || voteRequest.trackingCid || contract.contract_id || voteRequest.contract_id;

      // STATUS DERIVATION FOR ACTIVE PROPOSALS
      // ⚠️ Client-side derivation is ACCEPTABLE here because:
      // - Active proposals are CURRENT state (not historical)
      // - There are no exercised events - only created contracts exist
      // - This threshold-based logic approximates what an active proposal looks like
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
        contractId: contract.contract_id || trackingCid,
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
              <Server className="h-5 w-5 text-chart-2" />
            </div>
            {isLoading ? (
              <Skeleton className="h-10 w-full" />
            ) : (
              <>
                <p className="text-3xl font-bold text-chart-2 mb-1">{totalProposals}</p>
                <p className="text-xs text-muted-foreground">Local ACS</p>
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
        <Tabs value={activeTab} onValueChange={handleTabChange} className="space-y-6">
          <TabsList className="flex-wrap">
            <TabsTrigger value="scanapi" className="gap-1">
              <Globe className="h-4 w-4" />
              Historical Governance
            </TabsTrigger>
            <TabsTrigger value="active">Active Governance</TabsTrigger>
          </TabsList>

          {/* Scan API History Tab - Complete vote results from external API */}
          <TabsContent value="scanapi">
            <Card className="glass-card">
              <div className="p-6">
                <div className="flex items-center justify-between mb-6">
                  <div>
                    <h3 className="text-xl font-bold flex items-center gap-2">
                      <Globe className="h-5 w-5 text-primary" />
                      Scan API Vote Results
                    </h3>
                    <p className="text-sm text-muted-foreground mt-1">
                      Complete governance history from scan.sv-1.global.canton.network.sync.global
                    </p>
                  </div>
                </div>
                <GovernanceHistoryTable limit={500} />
              </div>
            </Card>
          </TabsContent>

          <TabsContent value="active">
            <Card className="glass-card">
              <div className="p-6">
                <div className="flex items-center justify-between mb-6">
                  <div>
                    <h3 className="text-xl font-bold flex items-center gap-2">
                      <Clock className="h-5 w-5 text-warning" />
                      Active Proposals
                    </h3>
                    <p className="text-sm text-muted-foreground mt-1">
                      In-progress vote requests from {dataSourceLabel}
                    </p>
                  </div>
                  <Badge variant="outline" className="text-xs">
                    {dataSourceLabel}
                  </Badge>
                </div>

            {isError ? (
              <div className="text-center py-12">
                <Vote className="h-12 w-12 text-muted-foreground mx-auto mb-4" />
                <p className="text-muted-foreground mb-2">
                  Unable to load proposals from Scan API.
                </p>
                <p className="text-xs text-muted-foreground">
                  Check network connectivity to the Canton Scan API.
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
                <CheckCircle className="h-12 w-12 text-success mx-auto mb-4" />
                <p className="text-muted-foreground mb-2">No active proposals at the moment</p>
                <p className="text-sm text-muted-foreground">
                  All governance proposals have been resolved. New proposals will appear here when submitted by DSO members.
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

                      {/* Action Details - Collapsible (only show if has displayable fields) */}
                      {proposal.actionDetails && Object.keys(proposal.actionDetails).length > 0 && (
                        <Collapsible className="mb-4">
                          <CollapsibleTrigger asChild>
                            <Button variant="ghost" size="sm" className="w-full justify-between p-3 h-auto rounded-lg bg-primary/5 border border-primary/20 hover:bg-primary/10">
                              <span className="text-sm font-semibold">Action Details ({Object.keys(proposal.actionDetails).length} fields)</span>
                              <ChevronDown className="h-4 w-4" />
                            </Button>
                          </CollapsibleTrigger>
                          <CollapsibleContent className="mt-2 p-3 rounded-lg bg-primary/5 border border-primary/20">
                            <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 text-sm">
                              {Object.entries(proposal.actionDetails)
                                .slice(0, 12) // Limit to first 12 fields
                                .map(([key, value]: [string, string]) => (
                                <div key={key} className="flex flex-col">
                                  <span className="text-xs text-muted-foreground capitalize">
                                    {key.replace(/\./g, " › ").replace(/([A-Z])/g, " $1").trim()}
                                  </span>
                                  <span className="font-mono text-xs break-all line-clamp-2">
                                    {value}
                                  </span>
                                </div>
                              ))}
                            </div>
                            {Object.keys(proposal.actionDetails).length > 12 && (
                              <p className="text-xs text-muted-foreground mt-2">
                                +{Object.keys(proposal.actionDetails).length - 12} more fields (see full JSON)
                              </p>
                            )}
                          </CollapsibleContent>
                        </Collapsible>
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

                      {/* Votes Cast - Compact Collapsible */}
                      {proposal.votedSvs?.length > 0 && (
                        <Collapsible className="mb-4">
                          <CollapsibleTrigger asChild>
                            <Button variant="ghost" size="sm" className="w-full justify-between p-3 h-auto rounded-lg bg-muted/30 border border-border/30 hover:bg-muted/50">
                              <div className="flex items-center gap-2">
                                <Users className="h-4 w-4" />
                                <span className="text-sm font-semibold">Votes Cast ({proposal.votedSvs.length})</span>
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
                                <span className="font-medium text-xs truncate max-w-[200px]" title={sv.party}>{sv.party}</span>
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

      {/* Semantic Groups Tab - Grouped by action_type::subject */}
      <TabsContent value="semantic">
        <Card className="glass-card">
          <div className="p-6">
            <div className="flex items-center justify-between mb-6">
              <div>
                <h3 className="text-xl font-bold flex items-center gap-2">
                  <Link2 className="h-5 w-5" />
                  Semantic Proposal Groups
                </h3>
                <p className="text-sm text-muted-foreground mt-1">
                  Proposals grouped by <code className="bg-muted px-1 rounded">action_type::subject</code> to link re-proposals together
                </p>
              </div>
              <div className="flex items-center gap-2 text-sm text-muted-foreground flex-wrap">
                {semanticProposals?.total != null && (
                  <Badge variant="outline">
                    {semanticProposals.total} unique groups
                  </Badge>
                )}
                {semanticProposals?.fromIndex && (
                  <Badge variant="secondary" className="bg-green-500/20 text-green-400 border-green-500/30">
                    <Database className="w-3 h-3 mr-1" />
                    From Index
                  </Badge>
                )}
              </div>
            </div>

            {/* Stats Row */}
            {semanticProposals?.byStatus && (
              <div className="grid grid-cols-2 sm:grid-cols-5 gap-3 mb-6">
                <div className="p-3 rounded-lg bg-muted/30 text-center">
                  <div className="text-2xl font-bold">{semanticProposals.total}</div>
                  <div className="text-xs text-muted-foreground">Total Groups</div>
                </div>
                <div className="p-3 rounded-lg bg-success/10 text-center">
                  <div className="text-2xl font-bold text-success">{semanticProposals.byStatus.executed}</div>
                  <div className="text-xs text-muted-foreground">Executed</div>
                </div>
                <div className="p-3 rounded-lg bg-destructive/10 text-center">
                  <div className="text-2xl font-bold text-destructive">{semanticProposals.byStatus.rejected}</div>
                  <div className="text-xs text-muted-foreground">Rejected</div>
                </div>
                <div className="p-3 rounded-lg bg-warning/10 text-center">
                  <div className="text-2xl font-bold text-warning">{semanticProposals.byStatus.in_progress}</div>
                  <div className="text-xs text-muted-foreground">In Progress</div>
                </div>
                <div className="p-3 rounded-lg bg-muted/50 text-center">
                  <div className="text-2xl font-bold text-muted-foreground">{semanticProposals.byStatus.expired}</div>
                  <div className="text-xs text-muted-foreground">Expired</div>
                </div>
              </div>
            )}

            {semanticLoading ? (
              <div className="space-y-3">
                {[1, 2, 3].map((i) => (
                  <Skeleton key={i} className="h-24 w-full" />
                ))}
              </div>
            ) : semanticError ? (
              <div className="text-center py-12">
                <AlertTriangle className="h-12 w-12 text-destructive mx-auto mb-4" />
                <p className="text-muted-foreground">Failed to load semantic groups</p>
                <p className="text-xs text-muted-foreground mt-2">
                  Ensure the VoteRequest index is built: POST /api/events/vote-requests/index/build
                </p>
              </div>
            ) : !semanticProposals?.proposals?.length ? (
              <div className="text-center py-12">
                <Vote className="h-12 w-12 text-muted-foreground mx-auto mb-4" />
                <p className="text-muted-foreground">No semantic groups found</p>
                <p className="text-xs text-muted-foreground mt-2">
                  Build the VoteRequest index first to see grouped proposals
                </p>
              </div>
            ) : (
              <div className="space-y-4">
                {semanticProposals.proposals.map((proposal) => {
                  const statusMap: Record<string, 'approved' | 'rejected' | 'pending' | 'expired'> = {
                    'executed': 'approved',
                    'rejected': 'rejected',
                    'in_progress': 'pending',
                    'expired': 'expired',
                  };
                  const displayStatus = statusMap[proposal.latest_status] || 'pending';
                  const title = proposal.action_type
                    .replace(/^(SRARC_|ARC_|CRARC_|ARAC_)/, "")
                    .replace(/([A-Z])/g, " $1")
                    .trim();

                  return (
                    <Collapsible key={proposal.semantic_key}>
                      <div className="p-6 rounded-lg bg-muted/30 hover:bg-muted/50 transition-all border border-border/50">
                        <div className="flex items-start justify-between mb-4">
                          <div className="flex items-center space-x-3">
                            <div className="gradient-accent p-2 rounded-lg">{getStatusIcon(displayStatus)}</div>
                            <div className="flex-1">
                              <h4 className="font-semibold text-lg">{title}</h4>
                              <p className="text-sm text-muted-foreground">
                                <span className="font-mono text-xs">{proposal.action_type}</span>
                                {proposal.action_subject && (
                                  <>
                                    <span className="mx-2">•</span>
                                    <span className="text-xs text-primary/80 truncate max-w-[200px] inline-block align-bottom" title={proposal.action_subject}>
                                      {proposal.action_subject.slice(0, 30)}{proposal.action_subject.length > 30 ? '...' : ''}
                                    </span>
                                  </>
                                )}
                              </p>
                              <p className="text-xs text-muted-foreground mt-1">
                                Requested by: <span className="font-medium text-foreground">{proposal.latest_requester || 'Unknown'}</span>
                              </p>
                            </div>
                          </div>
                          <div className="flex items-center gap-2">
                            <div className="flex items-center gap-1 text-sm">
                              <span className="text-success font-medium">{proposal.accept_count}</span>
                              <span className="text-muted-foreground">/</span>
                              <span className="text-destructive font-medium">{proposal.reject_count}</span>
                            </div>
                            {proposal.related_count > 1 && (
                              <Badge variant="outline" className="text-xs bg-primary/10 border-primary/30">
                                <Link2 className="w-3 h-3 mr-1" />
                                {proposal.related_count} related
                              </Badge>
                            )}
                            <Badge className={cn(
                              "text-xs",
                              displayStatus === 'approved' && "bg-success/10 text-success border-success/20",
                              displayStatus === 'rejected' && "bg-destructive/10 text-destructive border-destructive/20",
                              displayStatus === 'pending' && "bg-warning/10 text-warning border-warning/20",
                              displayStatus === 'expired' && "bg-muted text-muted-foreground",
                            )}>
                              {displayStatus === 'approved' && <CheckCircle className="h-3 w-3 mr-1" />}
                              {displayStatus === 'rejected' && <XCircle className="h-3 w-3 mr-1" />}
                              {displayStatus === 'pending' && <Clock className="h-3 w-3 mr-1" />}
                              {proposal.latest_status}
                            </Badge>
                            <span className="text-xs text-muted-foreground whitespace-nowrap">
                              {safeFormatDate(proposal.last_seen, "MMM d, yyyy")}
                            </span>
                            <CollapsibleTrigger asChild>
                              <Button variant="ghost" size="sm">
                                <ChevronDown className="h-4 w-4" />
                              </Button>
                            </CollapsibleTrigger>
                          </div>
                        </div>

                        {/* Reason Section */}
                        <div className="mb-4 p-3 rounded-lg bg-background/30 border border-border/30">
                          <p className="text-sm text-muted-foreground mb-1 font-semibold">Reason:</p>
                          {proposal.latest_reason_body && typeof proposal.latest_reason_body === "string" && (
                            <p className="text-sm mb-2">{proposal.latest_reason_body}</p>
                          )}
                          {proposal.latest_reason_url && typeof proposal.latest_reason_url === "string" && (
                            <a 
                              href={proposal.latest_reason_url} 
                              target="_blank" 
                              rel="noopener noreferrer"
                              className="text-sm text-primary hover:underline break-all"
                            >
                              {proposal.latest_reason_url}
                            </a>
                          )}
                          {(!proposal.latest_reason_body || typeof proposal.latest_reason_body !== "string") && 
                           (!proposal.latest_reason_url || typeof proposal.latest_reason_url !== "string") && (
                            <p className="text-sm text-muted-foreground italic">No reason provided</p>
                          )}
                        </div>

                        {/* Stats Grid */}
                        <div className="grid grid-cols-2 md:grid-cols-5 gap-3 mb-4">
                          <div className="p-3 rounded-lg bg-background/50">
                            <p className="text-xs text-muted-foreground mb-1">Votes For</p>
                            <p className="text-lg font-bold text-success">{proposal.accept_count}</p>
                          </div>
                          <div className="p-3 rounded-lg bg-background/50">
                            <p className="text-xs text-muted-foreground mb-1">Votes Against</p>
                            <p className="text-lg font-bold text-destructive">{proposal.reject_count}</p>
                          </div>
                          <div className="p-3 rounded-lg bg-background/50">
                            <p className="text-xs text-muted-foreground mb-1">First Seen</p>
                            <p className="text-xs font-mono">
                              {safeFormatDate(proposal.first_seen, "MMM d, yyyy HH:mm")}
                            </p>
                          </div>
                          <div className="p-3 rounded-lg bg-background/50">
                            <p className="text-xs text-muted-foreground mb-1">Vote Deadline</p>
                            <p className="text-xs font-mono">
                              {safeFormatDate(proposal.latest_vote_before)}
                            </p>
                          </div>
                          <div className="p-3 rounded-lg bg-background/50">
                            <p className="text-xs text-muted-foreground mb-1">Related Proposals</p>
                            <p className="text-lg font-bold">{proposal.related_count}</p>
                          </div>
                        </div>

                        <CollapsibleContent className="mt-4">
                          <div className="space-y-3">
                            {/* Semantic Key */}
                            <div className="p-3 rounded-lg bg-background/70 border border-border/50">
                              <p className="text-xs text-muted-foreground mb-2 font-semibold">
                                Semantic Key: <span className="font-mono text-primary">{proposal.semantic_key}</span>
                              </p>
                              <p className="text-xs text-muted-foreground">
                                Latest Contract ID: <span className="font-mono">{proposal.latest_contract_id}</span>
                              </p>
                            </div>
                          </div>
                        </CollapsibleContent>
                      </div>
                    </Collapsible>
                  );
                })}
              </div>
            )}

            {/* Info about semantic grouping */}
            <Alert className="mt-6">
              <Link2 className="h-4 w-4" />
              <AlertDescription className="text-sm">
                This view groups proposals by <code className="bg-muted px-1 rounded">action_type::subject</code> to link re-submitted proposals together.
                The "Related" count shows how many VoteRequests share the same semantic key.
              </AlertDescription>
            </Alert>
          </div>
        </Card>
      </TabsContent>

      {/* Unique Proposals Tab - Deduplicated View */}
      <TabsContent value="unique">
        <Card className="glass-card">
          <div className="p-6">
            <div className="flex items-center justify-between mb-6">
              <div>
                <h3 className="text-xl font-bold flex items-center gap-2">
                  <Database className="h-5 w-5" />
                  All Historical VoteRequests
                </h3>
                <p className="text-sm text-muted-foreground mt-1">
                  One row per unique <code className="bg-muted px-1 rounded">payload.id</code> (falls back to contract_id)
                </p>
              </div>
              <div className="flex items-center gap-2 text-sm text-muted-foreground flex-wrap">
                <Badge variant="outline">
                  {rawEventCount} events → {uniqueStats.total} unique
                </Badge>
                {uniqueStats.duplicatesRemoved > 0 && (
                  <Badge variant="secondary" className="text-xs">
                    {uniqueStats.duplicatesRemoved} duplicates removed
                  </Badge>
                )}
                {uniqueFromIndex && (
                  <Badge variant="secondary" className="bg-green-500/20 text-green-400 border-green-500/30">
                    <Database className="w-3 h-3 mr-1" />
                    From Index
                  </Badge>
                )}
                {uniqueDataSource && !uniqueFromIndex && (
                  <Badge variant="outline" className="text-yellow-400 border-yellow-500/30">
                    Source: {uniqueDataSource}
                  </Badge>
                )}
              </div>
            </div>

            {/* Stats Row */}
            <div className="grid grid-cols-2 sm:grid-cols-5 gap-3 mb-6">
              <div className="p-3 rounded-lg bg-muted/30 text-center">
                <div className="text-2xl font-bold">{uniqueStats.total}</div>
                <div className="text-xs text-muted-foreground">Total</div>
              </div>
              <div className="p-3 rounded-lg bg-success/10 text-center">
                <div className="text-2xl font-bold text-success">{uniqueStats.approved}</div>
                <div className="text-xs text-muted-foreground">Approved</div>
              </div>
              <div className="p-3 rounded-lg bg-destructive/10 text-center">
                <div className="text-2xl font-bold text-destructive">{uniqueStats.rejected}</div>
                <div className="text-xs text-muted-foreground">Rejected</div>
              </div>
              <div className="p-3 rounded-lg bg-warning/10 text-center">
                <div className="text-2xl font-bold text-warning">{uniqueStats.pending}</div>
                <div className="text-xs text-muted-foreground">Pending</div>
              </div>
              <div className="p-3 rounded-lg bg-muted/50 text-center">
                <div className="text-2xl font-bold text-muted-foreground">{uniqueStats.expired}</div>
                <div className="text-xs text-muted-foreground">Expired</div>
              </div>
            </div>

            {uniqueLoading ? (
              <div className="space-y-3">
                {[1, 2, 3].map((i) => (
                  <Skeleton key={i} className="h-24 w-full" />
                ))}
              </div>
            ) : !uniqueProposals?.length ? (
              <div className="text-center py-12">
                <Vote className="h-12 w-12 text-muted-foreground mx-auto mb-4" />
                <p className="text-muted-foreground">No unique proposals found</p>
              </div>
            ) : (
              <div className="space-y-4">
                {uniqueProposals.map((proposal) => (
                  <Collapsible key={proposal.proposalId}>
                    <div className="p-6 rounded-lg bg-muted/30 hover:bg-muted/50 transition-all border border-border/50">
                      <div className="flex items-start justify-between mb-4">
                        <div className="flex items-center space-x-3">
                          <div className="gradient-accent p-2 rounded-lg">{getStatusIcon(proposal.status)}</div>
                          <div className="flex-1">
                            <h4 className="font-semibold text-lg">{proposal.title}</h4>
                            <p className="text-sm text-muted-foreground">
                              {proposal.proposalHash}
                              <span className="mx-2">•</span>
                              <span className="font-mono text-xs">{proposal.actionType}</span>
                              {proposal.cipReference && (
                                <>
                                  <span className="mx-2">•</span>
                                  <span className="text-xs">CIP-{proposal.cipReference}</span>
                                </>
                              )}
                            </p>
                            <p className="text-xs text-muted-foreground mt-1">
                              Requested by: <span className="font-medium text-foreground">{proposal.requester || 'Unknown'}</span>
                            </p>
                          </div>
                        </div>
                        <div className="flex items-center gap-2">
                          <div className="flex items-center gap-1 text-sm">
                            <span className="text-success font-medium">{proposal.votesFor}</span>
                            <span className="text-muted-foreground">/</span>
                            <span className="text-destructive font-medium">{proposal.votesAgainst}</span>
                          </div>
                          <Badge variant="outline" className="text-xs">
                            {proposal.eventCount}
                          </Badge>
                          <Badge className={cn(
                            "text-xs",
                            proposal.status === 'approved' && "bg-success/10 text-success border-success/20",
                            proposal.status === 'rejected' && "bg-destructive/10 text-destructive border-destructive/20",
                            proposal.status === 'pending' && "bg-warning/10 text-warning border-warning/20",
                            proposal.status === 'expired' && "bg-muted text-muted-foreground",
                          )}>
                            {proposal.status === 'approved' && <CheckCircle className="h-3 w-3 mr-1" />}
                            {proposal.status === 'rejected' && <XCircle className="h-3 w-3 mr-1" />}
                            {proposal.status === 'pending' && <Clock className="h-3 w-3 mr-1" />}
                            {proposal.status}
                          </Badge>
                          <span className="text-xs text-muted-foreground whitespace-nowrap">
                            {safeFormatDate(proposal.latestEventTime, "MMM d, yyyy")}
                          </span>
                          <CollapsibleTrigger asChild>
                            <Button variant="ghost" size="sm">
                              <ChevronDown className="h-4 w-4" />
                            </Button>
                          </CollapsibleTrigger>
                        </div>
                      </div>

                      {/* Action Details */}
                      {proposal.actionDetails && typeof proposal.actionDetails === "object" && Object.keys(proposal.actionDetails).length > 0 && (
                        <div className="mb-4 p-3 rounded-lg bg-primary/5 border border-primary/20">
                          <p className="text-sm text-muted-foreground mb-2 font-semibold">Action Details:</p>
                          <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 text-sm">
                            {Object.entries(proposal.actionDetails)
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
                        {proposal.reason && typeof proposal.reason === "string" && (
                          <p className="text-sm mb-2">{proposal.reason}</p>
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
                        {(!proposal.reason || typeof proposal.reason !== "string") && 
                         (!proposal.reasonUrl || typeof proposal.reasonUrl !== "string") && (
                          <p className="text-sm text-muted-foreground italic">No reason provided</p>
                        )}
                      </div>

                      {/* Stats Grid */}
                      <div className="grid grid-cols-2 md:grid-cols-5 gap-3 mb-4">
                        <div className="p-3 rounded-lg bg-background/50">
                          <p className="text-xs text-muted-foreground mb-1">Votes For</p>
                          <p className="text-lg font-bold text-success">{proposal.votesFor}</p>
                        </div>
                        <div className="p-3 rounded-lg bg-background/50">
                          <p className="text-xs text-muted-foreground mb-1">Votes Against</p>
                          <p className="text-lg font-bold text-destructive">{proposal.votesAgainst}</p>
                        </div>
                        <div className="p-3 rounded-lg bg-background/50">
                          <p className="text-xs text-muted-foreground mb-1">Created</p>
                          <p className="text-xs font-mono">
                            {safeFormatDate(proposal.createdAt, "MMM d, yyyy HH:mm")}
                          </p>
                        </div>
                        <div className="p-3 rounded-lg bg-background/50">
                          <p className="text-xs text-muted-foreground mb-1">Vote Deadline</p>
                          <p className="text-xs font-mono">
                            {safeFormatDate(proposal.voteBefore)}
                          </p>
                        </div>
                        <div className="p-3 rounded-lg bg-background/50">
                          <p className="text-xs text-muted-foreground mb-1">Events</p>
                          <p className="text-lg font-bold">{proposal.eventCount}</p>
                        </div>
                      </div>

                      <CollapsibleContent className="mt-4">
                        <div className="space-y-3">
                          {/* Raw JSON */}
                          <div className="p-3 rounded-lg bg-background/70 border border-border/50">
                            <p className="text-xs text-muted-foreground mb-2 font-semibold">
                              Contract ID: <span className="font-mono">{proposal.contractId}</span>
                            </p>
                            <pre className="text-xs overflow-x-auto p-3 bg-muted/30 rounded border border-border/30 max-h-64">
                              {JSON.stringify(proposal.rawData, null, 2)}
                            </pre>
                          </div>
                        </div>
                      </CollapsibleContent>
                    </div>
                  </Collapsible>
                ))}
              </div>
            )}

            {/* Info about deduplication */}
            <Alert className="mt-6">
              <AlertTriangle className="h-4 w-4" />
              <AlertDescription className="text-sm">
                This view deduplicates proposals by combining <code className="bg-muted px-1 rounded">proposal_hash + action_type</code>. 
                Each row shows the <strong>latest state</strong> of a unique proposal. The "Events" column shows how many state updates exist for that proposal.
              </AlertDescription>
            </Alert>
          </div>
        </Card>
      </TabsContent>

      <TabsContent value="history">
        <Card className="glass-card">
          <div className="p-6">
            <h3 className="text-xl font-bold mb-6 flex items-center gap-2">
              <History className="h-5 w-5" />
              Canonical Governance Proposals
              {canonicalData?.proposals?.length ? (
                <>
                  <Badge variant="outline" className="ml-2">
                    {canonicalData.total} Proposals
                  </Badge>
                  <Badge variant="secondary" className="ml-1 bg-green-500/20 text-green-400 border-green-500/30">
                    <Database className="w-3 h-3 mr-1" />
                    From Index
                  </Badge>
                </>
              ) : null}
            </h3>

            {/* Model explanation alert */}
            <Alert className="mb-6 bg-muted/30 border-primary/20">
              <Hash className="h-4 w-4" />
              <AlertDescription className="text-sm">
                <strong>Canonical Model:</strong> Each row = 1 unique governance proposal, grouped by <code className="bg-muted px-1 rounded">proposal_key</code> (normalized action + mailing list URL).
                Status computed once per proposal after grouping all related VoteRequest events.
              </AlertDescription>
            </Alert>

            {/* Status breakdown */}
            {canonicalData?.stats?.byStatus && (
              <div className="grid grid-cols-2 sm:grid-cols-5 gap-3 mb-6">
                <div className="p-3 rounded-lg bg-muted/30 text-center">
                  <div className="text-2xl font-bold">{canonicalData.total}</div>
                  <div className="text-xs text-muted-foreground">Total Proposals</div>
                </div>
                <div className="p-3 rounded-lg bg-success/10 text-center">
                  <div className="text-2xl font-bold text-success">{canonicalData.stats.byStatus.accepted}</div>
                  <div className="text-xs text-muted-foreground">Accepted</div>
                </div>
                <div className="p-3 rounded-lg bg-destructive/10 text-center">
                  <div className="text-2xl font-bold text-destructive">{canonicalData.stats.byStatus.rejected}</div>
                  <div className="text-xs text-muted-foreground">Rejected</div>
                </div>
                <div className="p-3 rounded-lg bg-warning/10 text-center">
                  <div className="text-2xl font-bold text-warning">{canonicalData.stats.byStatus.in_progress}</div>
                  <div className="text-xs text-muted-foreground">In Progress</div>
                </div>
                <div className="p-3 rounded-lg bg-muted/50 text-center">
                  <div className="text-2xl font-bold text-muted-foreground">{canonicalData.stats.byStatus.expired}</div>
                  <div className="text-xs text-muted-foreground">Expired</div>
                </div>
              </div>
            )}

            {canonicalLoading ? (
              <div className="space-y-3">
                {[1, 2, 3].map((i) => (
                  <Skeleton key={i} className="h-32 w-full" />
                ))}
              </div>
            ) : canonicalError ? (
              <div className="text-center py-12">
                <AlertTriangle className="h-12 w-12 text-destructive mx-auto mb-4" />
                <p className="text-muted-foreground mb-2">Failed to load canonical proposals</p>
                <p className="text-xs text-muted-foreground">
                  Ensure the VoteRequest index is built: POST /api/events/vote-request-index/build
                </p>
              </div>
            ) : !canonicalData?.proposals?.length ? (
              <div className="text-center py-12">
                <Vote className="h-12 w-12 text-muted-foreground mx-auto mb-4" />
                <p className="text-muted-foreground mb-2">No canonical proposals found</p>
                <p className="text-xs text-muted-foreground">
                  Build the VoteRequest index to see deduplicated governance proposals
                </p>
              </div>
            ) : (
              <div className="space-y-4">
                {canonicalData.proposals.map((proposal) => {
                  const { title, actionType, actionDetails } = parseCanonicalAction(proposal);
                  const { votesFor, votesAgainst, votedSvs } = parseCanonicalVotes(proposal);
                  
                  // Map status to display
                  const statusMap: Record<string, 'approved' | 'rejected' | 'pending' | 'expired'> = {
                    'accepted': 'approved',
                    'rejected': 'rejected',
                    'in_progress': 'pending',
                    'expired': 'expired',
                  };
                  const displayStatus = statusMap[proposal.status] || 'pending';
                  
                  // Parse reason
                  const reasonObj = typeof proposal.reason === 'object' ? proposal.reason : null;
                  const reasonBody = reasonObj?.body || (typeof proposal.reason === 'string' ? proposal.reason : '');
                  const reasonUrl = proposal.reason_url || reasonObj?.url || '';
                  
                  return (
                    <Collapsible key={proposal.proposal_id || proposal.event_id}>
                      <div className="p-6 rounded-lg bg-muted/30 hover:bg-muted/50 transition-all border border-border/50">
                        <div className="flex items-start justify-between mb-4">
                          <div className="flex items-center space-x-3">
                            <div className="gradient-accent p-2 rounded-lg">{getStatusIcon(displayStatus)}</div>
                            <div className="flex-1">
                              <h4 className="font-semibold text-lg">{title}</h4>
                              <p className="text-sm text-muted-foreground">
                                <span className="font-mono text-xs">{actionType}</span>
                                {proposal.action_subject && (
                                  <>
                                    <span className="mx-2">•</span>
                                    <span className="text-xs text-primary/80 truncate max-w-[200px] inline-block align-bottom" title={proposal.action_subject}>
                                      {proposal.action_subject.slice(0, 30)}{proposal.action_subject.length > 30 ? '...' : ''}
                                    </span>
                                  </>
                                )}
                              </p>
                              <p className="text-xs text-muted-foreground mt-1">
                                Requested by: <span className="font-medium text-foreground">{proposal.requester || 'Unknown'}</span>
                              </p>
                            </div>
                          </div>
                          <div className="flex items-center gap-2">
                            <div className="flex items-center gap-1 text-sm">
                              <span className="text-success font-medium">{proposal.accept_count}</span>
                              <span className="text-muted-foreground">/</span>
                              <span className="text-destructive font-medium">{proposal.reject_count}</span>
                            </div>
                            {proposal.related_count > 1 && (
                              <Badge variant="outline" className="text-xs bg-primary/10 border-primary/30">
                                <Link2 className="w-3 h-3 mr-1" />
                                {proposal.related_count} related
                              </Badge>
                            )}
                            <Badge className={cn(
                              "text-xs",
                              displayStatus === 'approved' && "bg-success/10 text-success border-success/20",
                              displayStatus === 'rejected' && "bg-destructive/10 text-destructive border-destructive/20",
                              displayStatus === 'pending' && "bg-warning/10 text-warning border-warning/20",
                              displayStatus === 'expired' && "bg-muted text-muted-foreground",
                            )}>
                              {displayStatus === 'approved' && <CheckCircle className="h-3 w-3 mr-1" />}
                              {displayStatus === 'rejected' && <XCircle className="h-3 w-3 mr-1" />}
                              {displayStatus === 'pending' && <Clock className="h-3 w-3 mr-1" />}
                              {proposal.status}
                            </Badge>
                            <span className="text-xs text-muted-foreground whitespace-nowrap">
                              {safeFormatDate(proposal.effective_at, "MMM d, yyyy")}
                            </span>
                            <CollapsibleTrigger asChild>
                              <Button variant="ghost" size="sm">
                                <ChevronDown className="h-4 w-4" />
                              </Button>
                            </CollapsibleTrigger>
                          </div>
                        </div>

                        {/* Action Details */}
                        {actionDetails && typeof actionDetails === "object" && Object.keys(actionDetails).length > 0 && (
                          <div className="mb-4 p-3 rounded-lg bg-primary/5 border border-primary/20">
                            <p className="text-sm text-muted-foreground mb-2 font-semibold">Action Details:</p>
                            <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 text-sm">
                              {Object.entries(actionDetails)
                                .filter(([_, value]) => value !== null && value !== undefined)
                                .slice(0, 8)
                                .map(([key, value]: [string, unknown]) => (
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
                            <p className="text-lg font-bold text-success">{proposal.accept_count}</p>
                          </div>
                          <div className="p-3 rounded-lg bg-background/50">
                            <p className="text-xs text-muted-foreground mb-1">Votes Against</p>
                            <p className="text-lg font-bold text-destructive">{proposal.reject_count}</p>
                          </div>
                          <div className="p-3 rounded-lg bg-background/50">
                            <p className="text-xs text-muted-foreground mb-1">First Seen</p>
                            <p className="text-xs font-mono">
                              {safeFormatDate(proposal.first_seen, "MMM d, yyyy HH:mm")}
                            </p>
                          </div>
                          <div className="p-3 rounded-lg bg-background/50">
                            <p className="text-xs text-muted-foreground mb-1">Vote Deadline</p>
                            <p className="text-xs font-mono">
                              {safeFormatDate(proposal.vote_before)}
                            </p>
                          </div>
                          <div className="p-3 rounded-lg bg-background/50">
                            <p className="text-xs text-muted-foreground mb-1">Related</p>
                            <p className="text-lg font-bold">{proposal.related_count}</p>
                          </div>
                        </div>

                        {/* Votes Cast */}
                        {votedSvs?.length > 0 && (
                          <div className="mb-4">
                            <p className="text-xs text-muted-foreground mb-2 font-semibold">Votes Cast ({votedSvs.length}):</p>
                            <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
                              {votedSvs.slice(0, 10).map((sv, idx) => (
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

                        <CollapsibleContent className="mt-4">
                          <div className="space-y-3">
                            {/* IDs Section */}
                            <div className="p-3 rounded-lg bg-background/70 border border-border/50">
                              <p className="text-xs text-muted-foreground mb-2 font-semibold">
                                Proposal ID: <span className="font-mono text-primary">{proposal.proposal_id || proposal.contract_id}</span>
                              </p>
                              <p className="text-xs text-muted-foreground mb-2">
                                Contract ID: <span className="font-mono">{proposal.contract_id}</span>
                              </p>
                              {proposal.semantic_key && (
                                <p className="text-xs text-muted-foreground">
                                  Semantic Key: <span className="font-mono">{proposal.semantic_key}</span>
                                </p>
                              )}
                            </div>
                          </div>
                        </CollapsibleContent>
                      </div>
                    </Collapsible>
                  );
                })}
              </div>
            )}

            {/* Model explanation */}
            <Alert className="mt-6">
              <Database className="h-4 w-4" />
              <AlertDescription className="text-sm">
                This view shows <strong>canonical proposals</strong> deduplicated by <code className="bg-muted px-1 rounded">proposal_id = COALESCE(tracking_cid, contract_id)</code>.
                Each row represents one governance proposal's final state, not individual ledger events.
                The "Related" count shows state updates for each proposal.
              </AlertDescription>
            </Alert>
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
