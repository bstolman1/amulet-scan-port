import { useState, useEffect } from "react";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Vote, CheckCircle, XCircle, Clock, Users, Code, DollarSign, History, Database, AlertTriangle, Timer, UserPlus, AppWindow, Settings } from "lucide-react";
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
import { format, formatDistanceToNow, differenceInSeconds, differenceInMinutes, differenceInHours, differenceInDays } from "date-fns";
import { apiFetch } from "@/lib/duckdb-api-client";

// Countdown Timer Component
const CountdownTimer = ({ deadline }: { deadline: string }) => {
  const [timeLeft, setTimeLeft] = useState<string>("");
  const [isExpired, setIsExpired] = useState(false);
  const [urgency, setUrgency] = useState<"normal" | "warning" | "critical">("normal");

  useEffect(() => {
    const updateCountdown = () => {
      const now = new Date();
      const target = new Date(deadline);
      const diffSeconds = differenceInSeconds(target, now);

      if (diffSeconds <= 0) {
        setIsExpired(true);
        setTimeLeft("Expired");
        return;
      }

      const days = differenceInDays(target, now);
      const hours = differenceInHours(target, now) % 24;
      const minutes = differenceInMinutes(target, now) % 60;
      const seconds = diffSeconds % 60;

      // Set urgency level
      if (days < 1 && hours < 6) {
        setUrgency("critical");
      } else if (days < 2) {
        setUrgency("warning");
      } else {
        setUrgency("normal");
      }

      if (days > 0) {
        setTimeLeft(`${days}d ${hours}h ${minutes}m`);
      } else if (hours > 0) {
        setTimeLeft(`${hours}h ${minutes}m ${seconds}s`);
      } else {
        setTimeLeft(`${minutes}m ${seconds}s`);
      }
    };

    updateCountdown();
    const interval = setInterval(updateCountdown, 1000);
    return () => clearInterval(interval);
  }, [deadline]);

  const colorClass = isExpired
    ? "text-destructive"
    : urgency === "critical"
    ? "text-destructive animate-pulse"
    : urgency === "warning"
    ? "text-warning"
    : "text-primary";

  return (
    <div className={`flex items-center gap-1.5 ${colorClass}`}>
      <Timer className="h-3.5 w-3.5" />
      <span className="font-mono font-semibold text-sm">{timeLeft}</span>
    </div>
  );
};

// Action Details Display Component
const ActionDetailsDisplay = ({ actionType, details }: { actionType: string; details: any }) => {
  if (!details || Object.keys(details).length === 0) return null;

  const renderDetails = () => {
    switch (actionType) {
      case "SRARC_UpdateSvRewardWeight":
        return (
          <>
            {details.svParty && (
              <div className="flex items-center gap-2">
                <Users className="h-4 w-4 text-muted-foreground" />
                <span className="text-muted-foreground">Target SV:</span>
                <span className="font-mono text-sm">{details.svParty.split("::")[0]}</span>
              </div>
            )}
            {details.newRewardWeight && (
              <div className="flex items-center gap-2">
                <DollarSign className="h-4 w-4 text-muted-foreground" />
                <span className="text-muted-foreground">New Weight:</span>
                <span className="font-bold text-primary">
                  {(parseInt(details.newRewardWeight) / 10000).toFixed(2)}%
                </span>
                <span className="text-xs text-muted-foreground">
                  ({parseInt(details.newRewardWeight).toLocaleString()} bp)
                </span>
              </div>
            )}
          </>
        );

      case "SRARC_AddSv":
        return (
          <>
            {details.newSv && (
              <div className="flex items-center gap-2">
                <UserPlus className="h-4 w-4 text-success" />
                <span className="text-muted-foreground">New SV:</span>
                <span className="font-mono text-sm">{details.newSv.split("::")[0]}</span>
              </div>
            )}
            {details.svName && (
              <div className="flex items-center gap-2">
                <span className="text-muted-foreground">Name:</span>
                <span className="font-semibold">{details.svName}</span>
              </div>
            )}
            {details.svRewardWeight && (
              <div className="flex items-center gap-2">
                <DollarSign className="h-4 w-4 text-muted-foreground" />
                <span className="text-muted-foreground">Initial Weight:</span>
                <span className="font-bold text-primary">
                  {(parseInt(details.svRewardWeight) / 10000).toFixed(2)}%
                </span>
              </div>
            )}
            {details.participantId && (
              <div className="flex items-center gap-2">
                <span className="text-muted-foreground">Participant ID:</span>
                <span className="font-mono text-xs">{details.participantId}</span>
              </div>
            )}
          </>
        );

      case "SRARC_RemoveSv":
        return (
          <>
            {details.svParty && (
              <div className="flex items-center gap-2">
                <Users className="h-4 w-4 text-destructive" />
                <span className="text-muted-foreground">Remove SV:</span>
                <span className="font-mono text-sm">{details.svParty.split("::")[0]}</span>
              </div>
            )}
          </>
        );

      case "SRARC_GrantFeaturedAppRight":
        return (
          <>
            {details.provider && (
              <div className="flex items-center gap-2">
                <AppWindow className="h-4 w-4 text-chart-2" />
                <span className="text-muted-foreground">Provider:</span>
                <span className="font-mono text-sm">{details.provider.split("::")[0]}</span>
              </div>
            )}
            {details.featuredAppRight?.provider && (
              <div className="flex items-center gap-2">
                <AppWindow className="h-4 w-4 text-chart-2" />
                <span className="text-muted-foreground">App Provider:</span>
                <span className="font-mono text-sm">{details.featuredAppRight.provider.split("::")[0]}</span>
              </div>
            )}
          </>
        );

      case "SRARC_RevokeFeaturedAppRight":
        return (
          <>
            {details.provider && (
              <div className="flex items-center gap-2">
                <AppWindow className="h-4 w-4 text-destructive" />
                <span className="text-muted-foreground">Revoke From:</span>
                <span className="font-mono text-sm">{details.provider.split("::")[0]}</span>
              </div>
            )}
          </>
        );

      case "SRARC_SetConfig":
      case "CRARC_AddFutureAmuletConfigSchedule":
        return (
          <>
            <div className="flex items-center gap-2 mb-2">
              <Settings className="h-4 w-4 text-chart-3" />
              <span className="text-muted-foreground">Configuration Update</span>
            </div>
            {details.newConfig && (
              <div className="text-xs bg-background/50 p-2 rounded font-mono max-h-32 overflow-auto">
                {JSON.stringify(details.newConfig, null, 2).slice(0, 500)}
                {JSON.stringify(details.newConfig).length > 500 && "..."}
              </div>
            )}
            {details.newScheduleItem && (
              <div className="text-xs bg-background/50 p-2 rounded font-mono max-h-32 overflow-auto">
                {JSON.stringify(details.newScheduleItem, null, 2).slice(0, 500)}
              </div>
            )}
          </>
        );

      default:
        // Generic display for unknown action types
        return (
          <div className="text-xs bg-background/50 p-2 rounded font-mono max-h-32 overflow-auto">
            {JSON.stringify(details, null, 2).slice(0, 300)}
            {JSON.stringify(details).length > 300 && "..."}
          </div>
        );
    }
  };

  return (
    <div className="mb-4 p-3 rounded-lg bg-primary/5 border border-primary/20">
      <p className="text-sm text-muted-foreground mb-2 font-semibold">Action Details:</p>
      <div className="space-y-2 text-sm">
        {renderDetails()}
      </div>
    </div>
  );
};

const Governance = () => {
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

  // Helper to parse action and get human-readable title
  const parseAction = (action: any) => {
    if (!action) return { title: "Unknown Action", type: "Unknown", details: null };
    
    const tag = action.tag || "";
    const value = action.value || {};
    const dsoAction = value.dsoAction || {};
    const dsoActionTag = dsoAction.tag || "";
    const dsoActionValue = dsoAction.value || {};
    
    // Build human-readable title from the action type
    const actionTypeMap: Record<string, string> = {
      "SRARC_UpdateSvRewardWeight": "Update SV Reward Weight",
      "SRARC_AddSv": "Add Super Validator",
      "SRARC_RemoveSv": "Remove Super Validator",
      "SRARC_GrantFeaturedAppRight": "Grant Featured App Right",
      "SRARC_RevokeFeaturedAppRight": "Revoke Featured App Right",
      "CRARC_AddFutureAmuletConfigSchedule": "Update Amulet Config",
      "SRARC_SetConfig": "Set DSO Config",
    };
    
    const title = actionTypeMap[dsoActionTag] || dsoActionTag.replace(/^SRARC_|^CRARC_|^ARC_/g, "").replace(/([A-Z])/g, " $1").trim();
    
    return {
      title: title || tag.replace(/^ARC_/g, "").replace(/([A-Z])/g, " $1").trim(),
      type: dsoActionTag || tag,
      details: dsoActionValue,
    };
  };

  // Process proposals from ACS data with full JSON parsing
  // Note: ACS data has fields nested in payload, so we need to extract them
  const proposals =
    voteRequestsData?.data?.map((voteRequest: any) => {
      // Handle both flat structure and nested payload structure from DuckDB
      const payload = voteRequest.payload || voteRequest;
      
      // Parse votes - can be array of tuples [[svName, voteObj], ...] or object {svName: voteObj}
      const rawVotes = payload.votes || voteRequest.votes || [];
      let votesArray: Array<{ svName: string; vote: any }> = [];
      
      if (Array.isArray(rawVotes)) {
        // Array of tuples: [["SV-Name", {sv: "...", accept: true, reason: {...}}], ...]
        votesArray = rawVotes.map((v: any) => ({
          svName: Array.isArray(v) ? v[0] : v.svName || "Unknown",
          vote: Array.isArray(v) ? v[1] : v,
        }));
      } else if (typeof rawVotes === "object") {
        // Object format: {svName: voteObj}
        votesArray = Object.entries(rawVotes).map(([svName, vote]) => ({ svName, vote }));
      }
      
      const votesFor = votesArray.filter((v) => v.vote?.accept === true).length;
      const votesAgainst = votesArray.filter((v) => v.vote?.accept === false).length;
      
      // Parse action
      const action = payload.action || voteRequest.action || {};
      const parsedAction = parseAction(action);

      // Extract requester information
      const requester = payload.requester || voteRequest.requester || "Unknown";

      // Extract reason - handle object with url and body
      const reasonObj = payload.reason || voteRequest.reason || {};
      const reasonBody = typeof reasonObj === "object" ? reasonObj.body : reasonObj;
      const reasonUrl = typeof reasonObj === "object" ? reasonObj.url : null;

      // Extract voting information for display
      const votedSvs = votesArray.map((v) => ({
        party: v.svName,
        vote: v.vote?.accept === true ? "accept" : v.vote?.accept === false ? "reject" : "pending",
        reason: v.vote?.reason?.body || "",
        castAt: v.vote?.optCastAt,
      }));

      // Determine status based on votes and threshold
      const threshold = votingThreshold || svCount;
      let status: "approved" | "rejected" | "pending" = "pending";
      if (votesFor >= threshold) status = "approved";
      else if (votesAgainst > svCount - threshold) status = "rejected";

      const trackingCid = payload.trackingCid || voteRequest.trackingCid || voteRequest.contract_id;
      const voteBefore = payload.voteBefore || voteRequest.voteBefore;
      const targetEffectiveAt = payload.targetEffectiveAt || voteRequest.targetEffectiveAt;

      return {
        id: trackingCid?.slice(0, 12) || "unknown",
        trackingCid,
        title: parsedAction.title,
        actionType: parsedAction.type,
        actionDetails: parsedAction.details,
        reasonBody,
        reasonUrl,
        requester,
        status,
        votesFor,
        votesAgainst,
        totalVotes: votesArray.length,
        votedSvs,
        voteBefore,
        targetEffectiveAt,
        rawData: voteRequest, // Keep full JSON for debugging
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
                {proposals?.map((proposal: any, index: number) => (
                  <Collapsible key={index}>
                    <div className="p-6 rounded-lg bg-muted/30 hover:bg-muted/50 transition-smooth border border-border/50">
                      <div className="flex items-start justify-between mb-4">
                        <div className="flex items-center space-x-3">
                          <div className="gradient-accent p-2 rounded-lg">{getStatusIcon(proposal.status)}</div>
                          <div className="flex-1">
                            <h4 className="font-semibold text-lg">{proposal.title}</h4>
                            <p className="text-sm text-muted-foreground">Proposal #{proposal.id}</p>
                            <p className="text-xs text-muted-foreground mt-1">
                              Requested by: <span className="font-mono">{proposal.requester}</span>
                            </p>
                          </div>
                        </div>
                        <Badge className={getStatusColor(proposal.status)}>{proposal.status}</Badge>
                      </div>

                      {/* Action Details */}
                      <ActionDetailsDisplay actionType={proposal.actionType} details={proposal.actionDetails} />

                      {/* Reason */}
                      <div className="mb-4 p-3 rounded-lg bg-background/30 border border-border/30">
                        <p className="text-sm text-muted-foreground mb-1 font-semibold">Reason:</p>
                        <p className="text-sm">{proposal.reasonBody || "No reason provided"}</p>
                        {proposal.reasonUrl && (
                          <a 
                            href={proposal.reasonUrl} 
                            target="_blank" 
                            rel="noopener noreferrer"
                            className="text-xs text-primary hover:underline mt-2 inline-block"
                          >
                            {proposal.reasonUrl}
                          </a>
                        )}
                      </div>

                      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-4">
                        <div className="p-3 rounded-lg bg-background/50">
                          <p className="text-xs text-muted-foreground mb-1">Votes For</p>
                          <p className="text-lg font-bold text-success">{proposal.votesFor || 0}</p>
                        </div>
                        <div className="p-3 rounded-lg bg-background/50">
                          <p className="text-xs text-muted-foreground mb-1">Votes Against</p>
                          <p className="text-lg font-bold text-destructive">{proposal.votesAgainst || 0}</p>
                        </div>
                        <div className="p-3 rounded-lg bg-background/50">
                          <p className="text-xs text-muted-foreground mb-1">Target Effective</p>
                          <p className="text-xs font-mono">
                            {proposal.targetEffectiveAt ? format(new Date(proposal.targetEffectiveAt), "MMM d, yyyy HH:mm") : "N/A"}
                          </p>
                        </div>
                        <div className="p-3 rounded-lg bg-background/50">
                          <p className="text-xs text-muted-foreground mb-1">Time Remaining</p>
                          {proposal.voteBefore ? (
                            <CountdownTimer deadline={proposal.voteBefore} />
                          ) : (
                            <p className="text-xs font-mono text-muted-foreground">N/A</p>
                          )}
                          {proposal.voteBefore && (
                            <p className="text-xs text-muted-foreground mt-1">
                              {format(new Date(proposal.voteBefore), "MMM d, HH:mm")}
                            </p>
                          )}
                        </div>
                      </div>

                      {proposal.votedSvs?.length > 0 && (
                        <div className="mb-4">
                          <p className="text-xs text-muted-foreground mb-2 font-semibold">
                            Votes Cast ({proposal.totalVotes}):
                          </p>
                          <div className="space-y-2">
                            {proposal.votedSvs.map((sv: any, idx: number) => (
                              <div 
                                key={idx}
                                className={`p-2 rounded-lg border ${
                                  sv.vote === "accept"
                                    ? "border-success/30 bg-success/5"
                                    : sv.vote === "reject"
                                    ? "border-destructive/30 bg-destructive/5"
                                    : "border-border/30 bg-muted/30"
                                }`}
                              >
                                <div className="flex items-center justify-between">
                                  <span className="font-mono text-sm">{sv.party}</span>
                                  <Badge
                                    variant="outline"
                                    className={
                                      sv.vote === "accept"
                                        ? "border-success/50 text-success"
                                        : sv.vote === "reject"
                                        ? "border-destructive/50 text-destructive"
                                        : "border-muted-foreground/50"
                                    }
                                  >
                                    {sv.vote}
                                  </Badge>
                                </div>
                                {sv.reason && (
                                  <p className="text-xs text-muted-foreground mt-1">{sv.reason}</p>
                                )}
                                {sv.castAt && (
                                  <p className="text-xs text-muted-foreground mt-1">
                                    Cast: {format(new Date(sv.castAt), "MMM d, yyyy HH:mm")}
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
                            Action Type: {proposal.actionType}
                          </p>
                          <pre className="text-xs overflow-x-auto p-3 bg-muted/30 rounded border border-border/30">
                            {JSON.stringify(proposal.rawData, null, 2)}
                          </pre>
                        </div>
                      </CollapsibleContent>
                    </div>
                  </Collapsible>
                ))}
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
              Historical Governance Events
            </h3>
            
            {eventsLoading ? (
              <div className="space-y-3">
                {[1, 2, 3].map((i) => (
                  <Skeleton key={i} className="h-20 w-full" />
                ))}
              </div>
            ) : !governanceEvents?.length ? (
              <div className="text-center py-12">
                <Vote className="h-12 w-12 text-muted-foreground mx-auto mb-4" />
                <p className="text-muted-foreground">No historical governance events found</p>
              </div>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Event Type</TableHead>
                    <TableHead>Round</TableHead>
                    <TableHead>Template</TableHead>
                    <TableHead>Timestamp</TableHead>
                    <TableHead>Details</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {governanceEvents.slice(0, 100).map((event: any, idx: number) => {
                    const ts = event.timestamp || event.effective_at || event.created_at;
                    const date = ts ? new Date(ts) : null;
                    const timestampLabel = date && !Number.isNaN(date.getTime())
                      ? format(date, "MMM d, yyyy HH:mm")
                      : "-";

                    return (
                      <TableRow key={event.event_id || event.contract_id || `${event.event_type}-${idx}`}>
                        <TableCell className="font-mono text-xs">{event.event_type}</TableCell>
                        <TableCell>{typeof event.round === "number" ? event.round.toLocaleString() : "-"}</TableCell>
                        <TableCell className="text-xs truncate max-w-[200px]">
                          {event.template_id?.split(":").pop() || "-"}
                        </TableCell>
                        <TableCell className="text-xs">{timestampLabel}</TableCell>
                        <TableCell>
                          <Collapsible>
                            <CollapsibleTrigger asChild>
                              <Button variant="ghost" size="sm">
                                <Code className="h-3 w-3" />
                              </Button>
                            </CollapsibleTrigger>
                            <CollapsibleContent>
                              <pre className="text-xs bg-muted p-2 rounded mt-2 overflow-auto max-h-48">
                                {JSON.stringify(event.payload || event.event_data, null, 2)}
                              </pre>
                            </CollapsibleContent>
                          </Collapsible>
                        </TableCell>
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
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
