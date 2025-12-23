import { useEffect, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Vote, CheckCircle, XCircle, Clock, Users, Code, DollarSign, History, Database, AlertTriangle, ChevronDown, Search, Filter, RefreshCw, ExternalLink } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { scanApi } from "@/lib/api-client";
import { Skeleton } from "@/components/ui/skeleton";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { useLatestACSSnapshot } from "@/hooks/use-acs-snapshots";
import { useAggregatedTemplateData } from "@/hooks/use-aggregated-template-data";
import { useGovernanceEvents } from "@/hooks/use-governance-events";
import { useUniqueProposals } from "@/hooks/use-unique-proposals";
import { useGovernanceProposals, useProposalStats, useActionTypes, formatActionType, getStatusColor as getIndexedStatusColor, type Proposal } from "@/hooks/use-governance-proposals";
import { DataSourcesFooter } from "@/components/DataSourcesFooter";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { format } from "date-fns";
import { apiFetch } from "@/lib/duckdb-api-client";
import { cn } from "@/lib/utils";
import { VoteRequestIndexBanner } from "@/components/VoteRequestIndexBanner";
import { GovernanceIndexBanner } from "@/components/GovernanceIndexBanner";

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

// Indexed Proposal Card Component
const IndexedProposalCard = ({ proposal }: { proposal: Proposal }) => {
  const getStatusIcon = (status: Proposal['status']) => {
    switch (status) {
      case 'approved': return <CheckCircle className="h-4 w-4" />;
      case 'rejected': return <XCircle className="h-4 w-4" />;
      case 'pending': return <Clock className="h-4 w-4" />;
      default: return <Vote className="h-4 w-4" />;
    }
  };

  return (
    <Collapsible>
      <div className="p-6 rounded-lg bg-muted/30 hover:bg-muted/50 transition-all border border-border/50">
        <div className="flex items-start justify-between gap-4 mb-4">
          <div className="flex items-center gap-3 min-w-0 flex-1">
            <div className={cn(
              "p-2 rounded-lg shrink-0",
              proposal.status === 'approved' && "bg-green-500/20",
              proposal.status === 'rejected' && "bg-red-500/20",
              proposal.status === 'pending' && "bg-yellow-500/20",
              proposal.status === 'expired' && "bg-muted",
            )}>
              {getStatusIcon(proposal.status)}
            </div>
            <div className="min-w-0 flex-1">
              <h4 className="font-semibold text-lg truncate">{formatActionType(proposal.actionType)}</h4>
              <p className="text-sm text-muted-foreground truncate">
                Requested by: <span className="font-medium text-foreground">{proposal.requester}</span>
              </p>
            </div>
          </div>
          <div className="flex items-center gap-2 shrink-0">
            <div className="flex items-center gap-1 text-sm">
              <span className="text-green-400 font-medium">{proposal.votesFor}</span>
              <span className="text-muted-foreground">/</span>
              <span className="text-red-400 font-medium">{proposal.votesAgainst}</span>
            </div>
            <Badge className={cn("text-xs border", getIndexedStatusColor(proposal.status))}>
              {getStatusIcon(proposal.status)}
              <span className="ml-1 capitalize">{proposal.status}</span>
            </Badge>
            <span className="text-xs text-muted-foreground whitespace-nowrap hidden sm:inline">
              {safeFormatDate(proposal.rawTimestamp, "MMM d, yyyy")}
            </span>
            <CollapsibleTrigger asChild>
              <Button variant="ghost" size="sm">
                <ChevronDown className="h-4 w-4" />
              </Button>
            </CollapsibleTrigger>
          </div>
        </div>

        {/* Reason Preview */}
        {proposal.reasonBody && (
          <p className="text-sm text-muted-foreground mb-3 line-clamp-2">
            {proposal.reasonBody}
          </p>
        )}

        {/* Reason URL */}
        {proposal.reasonUrl && (
          <a 
            href={proposal.reasonUrl}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-1 text-sm text-primary hover:underline mb-3"
          >
            <ExternalLink className="h-3 w-3" />
            {proposal.reasonUrl.length > 60 ? proposal.reasonUrl.slice(0, 60) + '...' : proposal.reasonUrl}
          </a>
        )}

        <CollapsibleContent className="mt-4 space-y-4">
          {/* Action Details */}
          {proposal.actionDetails && typeof proposal.actionDetails === "object" && (
            <div className="p-3 rounded-lg bg-primary/5 border border-primary/20">
              <p className="text-sm text-muted-foreground mb-2 font-semibold">Action Details:</p>
              <pre className="text-xs font-mono overflow-x-auto max-h-48">
                {JSON.stringify(proposal.actionDetails, null, 2)}
              </pre>
            </div>
          )}

          {/* Full Reason */}
          {proposal.reasonBody && (
            <div className="p-3 rounded-lg bg-background/30 border border-border/30">
              <p className="text-sm text-muted-foreground mb-1 font-semibold">Full Reason:</p>
              <p className="text-sm whitespace-pre-wrap">{proposal.reasonBody}</p>
            </div>
          )}

          {/* Stats Grid */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <div className="p-3 rounded-lg bg-background/50">
              <p className="text-xs text-muted-foreground mb-1">Votes For</p>
              <p className="text-lg font-bold text-green-400">{proposal.votesFor}</p>
            </div>
            <div className="p-3 rounded-lg bg-background/50">
              <p className="text-xs text-muted-foreground mb-1">Votes Against</p>
              <p className="text-lg font-bold text-red-400">{proposal.votesAgainst}</p>
            </div>
            <div className="p-3 rounded-lg bg-background/50">
              <p className="text-xs text-muted-foreground mb-1">Vote Deadline</p>
              <p className="text-xs font-mono">{safeFormatDate(proposal.voteBefore)}</p>
            </div>
            <div className="p-3 rounded-lg bg-background/50">
              <p className="text-xs text-muted-foreground mb-1">Last Updated</p>
              <p className="text-xs font-mono">{safeFormatDate(proposal.rawTimestamp)}</p>
            </div>
          </div>

          {/* Votes Cast */}
          {proposal.votes?.length > 0 && (
            <div>
              <p className="text-xs text-muted-foreground mb-2 font-semibold">
                Votes Cast ({proposal.votes.length}):
              </p>
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 max-h-64 overflow-y-auto">
                {proposal.votes.map((vote, idx) => (
                  <div 
                    key={idx}
                    className={cn(
                      "p-2 rounded border text-sm",
                      vote.accept 
                        ? "bg-green-500/5 border-green-500/30" 
                        : "bg-red-500/5 border-red-500/30"
                    )}
                  >
                    <div className="flex items-center justify-between mb-1">
                      <span className="font-medium truncate">{vote.svName}</span>
                      <Badge 
                        variant="outline" 
                        className={cn(
                          "text-xs",
                          vote.accept ? "border-green-500 text-green-400" : "border-red-500 text-red-400"
                        )}
                      >
                        {vote.accept ? "✓ Accept" : "✗ Reject"}
                      </Badge>
                    </div>
                    {vote.reasonBody && (
                      <p className="text-xs text-muted-foreground italic truncate">"{vote.reasonBody}"</p>
                    )}
                    {vote.castAt && (
                      <p className="text-xs text-muted-foreground mt-1">
                        Cast: {safeFormatDate(vote.castAt)}
                      </p>
                    )}
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Contract ID */}
          <div className="p-3 rounded-lg bg-muted/30 border border-border/30">
            <p className="text-xs text-muted-foreground mb-1">Contract ID:</p>
            <p className="text-xs font-mono break-all">{proposal.latestContractId}</p>
          </div>
        </CollapsibleContent>
      </div>
    </Collapsible>
  );
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

  // Fetch vote requests from LOCAL ACS only
  const {
    data: voteRequestsData,
    isLoading,
    isError,
  } = useAggregatedTemplateData(undefined, "Splice:DsoRules:VoteRequest");

  // Fetch DsoRules from LOCAL ACS
  const { data: dsoRulesData } = useAggregatedTemplateData(
    undefined,
    "Splice:DsoRules:DsoRules",
  );

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

  // Get SV count and voting threshold from DsoRules
  // Handle both flat and nested payload structure from DuckDB
  const dsoRulesRaw = dsoRulesData?.data?.[0];
  const dsoRulesPayload = dsoRulesRaw?.payload || dsoRulesRaw || {};
  const svs = dsoRulesPayload.svs || {};
  const svCount = Object.keys(svs).length || 0;
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

  // Indexed Proposals state
  const [indexedSearch, setIndexedSearch] = useState("");
  const [indexedStatusFilter, setIndexedStatusFilter] = useState<string>("all");
  const [indexedActionFilter, setIndexedActionFilter] = useState<string>("all");
  const [indexedPage, setIndexedPage] = useState(0);
  const indexedLimit = 20;

  // Indexed Proposals from governance indexer
  const { 
    data: indexedProposalsData, 
    isLoading: indexedLoading, 
    error: indexedError,
    refetch: refetchIndexed,
  } = useGovernanceProposals({
    limit: indexedLimit,
    offset: indexedPage * indexedLimit,
    status: indexedStatusFilter === "all" ? null : indexedStatusFilter as any,
    actionType: indexedActionFilter === "all" ? null : indexedActionFilter,
    search: indexedSearch || null,
  });

  const { data: indexedStats } = useProposalStats();
  const { data: actionTypes } = useActionTypes();

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

        {/* Index Banners */}
        <div className="space-y-3">
          <GovernanceIndexBanner />
          <VoteRequestIndexBanner />
        </div>

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
        <Tabs defaultValue="indexed" className="space-y-6">
          <TabsList className="flex-wrap h-auto gap-1">
            <TabsTrigger value="indexed">
              Indexed Proposals
              {indexedStats?.total ? (
                <Badge variant="secondary" className="ml-2 text-xs">
                  {indexedStats.total}
                </Badge>
              ) : null}
            </TabsTrigger>
            <TabsTrigger value="active">Active (ACS)</TabsTrigger>
            <TabsTrigger value="unique">
              Unique Proposals
              {uniqueStats.total > 0 && (
                <Badge variant="secondary" className="ml-2 text-xs">
                  {uniqueStats.total}
                </Badge>
              )}
            </TabsTrigger>
            <TabsTrigger value="history">Governance History</TabsTrigger>
          </TabsList>

          {/* Indexed Proposals Tab - From Binary File Index */}
          <TabsContent value="indexed">
            <Card className="glass-card">
              <div className="p-6">
                <div className="flex flex-col sm:flex-row items-start sm:items-center justify-between gap-4 mb-6">
                  <div>
                    <h3 className="text-xl font-bold flex items-center gap-2">
                      <Database className="h-5 w-5" />
                      Indexed Governance Proposals
                    </h3>
                    <p className="text-sm text-muted-foreground mt-1">
                      Historical proposals extracted from binary ledger files
                    </p>
                  </div>
                  <Button 
                    variant="outline" 
                    size="sm" 
                    onClick={() => refetchIndexed()}
                    disabled={indexedLoading}
                  >
                    <RefreshCw className={cn("h-4 w-4 mr-2", indexedLoading && "animate-spin")} />
                    Refresh
                  </Button>
                </div>

                {/* Stats Row */}
                {indexedStats && (
                  <div className="grid grid-cols-2 sm:grid-cols-5 gap-3 mb-6">
                    <div className="p-3 rounded-lg bg-muted/30 text-center">
                      <div className="text-2xl font-bold">{indexedStats.total}</div>
                      <div className="text-xs text-muted-foreground">Total</div>
                    </div>
                    <div className="p-3 rounded-lg bg-green-500/10 text-center">
                      <div className="text-2xl font-bold text-green-400">{indexedStats.byStatus?.approved || 0}</div>
                      <div className="text-xs text-muted-foreground">Approved</div>
                    </div>
                    <div className="p-3 rounded-lg bg-red-500/10 text-center">
                      <div className="text-2xl font-bold text-red-400">{indexedStats.byStatus?.rejected || 0}</div>
                      <div className="text-xs text-muted-foreground">Rejected</div>
                    </div>
                    <div className="p-3 rounded-lg bg-yellow-500/10 text-center">
                      <div className="text-2xl font-bold text-yellow-400">{indexedStats.byStatus?.pending || 0}</div>
                      <div className="text-xs text-muted-foreground">Pending</div>
                    </div>
                    <div className="p-3 rounded-lg bg-muted/50 text-center">
                      <div className="text-2xl font-bold text-muted-foreground">{indexedStats.byStatus?.expired || 0}</div>
                      <div className="text-xs text-muted-foreground">Expired</div>
                    </div>
                  </div>
                )}

                {/* Filters */}
                <div className="flex flex-col sm:flex-row gap-3 mb-6">
                  <div className="relative flex-1">
                    <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
                    <Input
                      placeholder="Search proposals..."
                      value={indexedSearch}
                      onChange={(e) => {
                        setIndexedSearch(e.target.value);
                        setIndexedPage(0);
                      }}
                      className="pl-10"
                    />
                  </div>
                  <Select 
                    value={indexedStatusFilter} 
                    onValueChange={(v) => {
                      setIndexedStatusFilter(v);
                      setIndexedPage(0);
                    }}
                  >
                    <SelectTrigger className="w-[140px]">
                      <Filter className="h-4 w-4 mr-2" />
                      <SelectValue placeholder="Status" />
                    </SelectTrigger>
                    <SelectContent className="bg-popover">
                      <SelectItem value="all">All Status</SelectItem>
                      <SelectItem value="approved">Approved</SelectItem>
                      <SelectItem value="rejected">Rejected</SelectItem>
                      <SelectItem value="pending">Pending</SelectItem>
                      <SelectItem value="expired">Expired</SelectItem>
                    </SelectContent>
                  </Select>
                  <Select 
                    value={indexedActionFilter} 
                    onValueChange={(v) => {
                      setIndexedActionFilter(v);
                      setIndexedPage(0);
                    }}
                  >
                    <SelectTrigger className="w-[200px]">
                      <SelectValue placeholder="Action Type" />
                    </SelectTrigger>
                    <SelectContent className="bg-popover max-h-[300px]">
                      <SelectItem value="all">All Actions</SelectItem>
                      {actionTypes?.map((at) => (
                        <SelectItem key={at.type} value={at.type}>
                          {formatActionType(at.type)} ({at.count})
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>

                {/* Proposals List */}
                {indexedError ? (
                  <div className="text-center py-12">
                    <AlertTriangle className="h-12 w-12 text-destructive mx-auto mb-4" />
                    <p className="text-muted-foreground mb-2">Failed to load indexed proposals</p>
                    <p className="text-xs text-muted-foreground">
                      Ensure the server is running and the governance indexer is enabled
                    </p>
                  </div>
                ) : indexedLoading ? (
                  <div className="space-y-4">
                    {[1, 2, 3].map((i) => (
                      <Skeleton key={i} className="h-32 w-full" />
                    ))}
                  </div>
                ) : !indexedProposalsData?.proposals?.length ? (
                  <div className="text-center py-12">
                    <Vote className="h-12 w-12 text-muted-foreground mx-auto mb-4" />
                    <p className="text-muted-foreground mb-2">No proposals found</p>
                    <p className="text-sm text-muted-foreground">
                      {indexedSearch || indexedStatusFilter !== "all" || indexedActionFilter !== "all" 
                        ? "Try adjusting your filters" 
                        : "Run the governance indexer to populate proposals"}
                    </p>
                  </div>
                ) : (
                  <>
                    <div className="space-y-4">
                      {indexedProposalsData.proposals.map((proposal: Proposal) => (
                        <IndexedProposalCard key={proposal.proposalKey} proposal={proposal} />
                      ))}
                    </div>

                    {/* Pagination */}
                    {indexedProposalsData.pagination && (
                      <div className="flex items-center justify-between mt-6 pt-4 border-t border-border/50">
                        <p className="text-sm text-muted-foreground">
                          Showing {indexedPage * indexedLimit + 1}-{Math.min((indexedPage + 1) * indexedLimit, indexedProposalsData.pagination.total)} of {indexedProposalsData.pagination.total}
                        </p>
                        <div className="flex gap-2">
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={() => setIndexedPage(p => Math.max(0, p - 1))}
                            disabled={indexedPage === 0}
                          >
                            Previous
                          </Button>
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={() => setIndexedPage(p => p + 1)}
                            disabled={!indexedProposalsData.pagination.hasMore}
                          >
                            Next
                          </Button>
                        </div>
                      </div>
                    )}
                  </>
                )}
              </div>
            </Card>
          </TabsContent>

          <TabsContent value="active">
            <Card className="glass-card">
              <div className="p-6">
                <h3 className="text-xl font-bold mb-6">Recent Proposals</h3>

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
              Historical Governance Events (DuckDB)
              {governanceEvents?.length ? (
                <>
                  <Badge variant="outline" className="ml-2">
                    {governanceEvents.filter((e: any) => e.template_id?.includes('VoteRequest')).length} VoteRequests
                  </Badge>
                  {fromIndex && (
                    <Badge variant="secondary" className="ml-1 bg-green-500/20 text-green-400 border-green-500/30">
                      <Database className="w-3 h-3 mr-1" />
                      From Index
                    </Badge>
                  )}
                  {dataSource && !fromIndex && (
                    <Badge variant="outline" className="ml-1 text-yellow-400 border-yellow-500/30">
                      Source: {dataSource}
                    </Badge>
                  )}
                </>
              ) : null}
            </h3>
            
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
                <p className="text-xs text-muted-foreground">
                  Governance history is extracted from local DuckDB backfill data
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
