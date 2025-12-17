import { DashboardLayout } from "@/components/DashboardLayout";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Vote, CheckCircle, XCircle, Clock, Users, Code, DollarSign, History } from "lucide-react";
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

const Governance = () => {
  const { data: dsoInfo } = useQuery({
    queryKey: ["dsoInfo"],
    queryFn: () => scanApi.fetchDsoInfo(),
    retry: 1,
  });

  const { data: latestSnapshot } = useLatestACSSnapshot();
  const { data: governanceEvents, isLoading: eventsLoading, error: eventsError } = useGovernanceEvents();

  // Fetch vote requests FIRST - this is the PRIMARY governance data source
  const {
    data: voteRequestsData,
    isLoading,
    isError,
    error: voteRequestError,
  } = useAggregatedTemplateData(undefined, "Splice:DsoRules:VoteRequest");

  // Fetch DsoRules to get SV count and voting threshold
  const { data: dsoRulesData } = useAggregatedTemplateData(
    undefined,
    "Splice:DsoRules:DsoRules",
  );

  // Fetch Confirmations (secondary governance data)
  const { data: confirmationsData } = useAggregatedTemplateData(
    undefined,
    "Splice:DsoRules:Confirmation",
  );

  // Debug: Log data loading status
  console.log("🔍 Governance Data Status:", {
    voteRequests: voteRequestsData?.data?.length ?? "loading",
    dsoRules: dsoRulesData?.data?.length ?? "loading", 
    confirmations: confirmationsData?.data?.length ?? "loading",
    events: governanceEvents?.length ?? "loading",
    voteRequestError: voteRequestError?.message,
    eventsError: eventsError?.message,
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

  // Process proposals from ACS data with full JSON parsing
  // Note: ACS data has fields nested in payload, so we need to extract them
  const proposals =
    voteRequestsData?.data?.map((voteRequest: any) => {
      // Handle both flat structure and nested payload structure from DuckDB
      const payload = voteRequest.payload || voteRequest;
      const votes = payload.votes || voteRequest.votes || {};
      const votesList = Object.values(votes);
      const votesFor = votesList.filter((v: any) => v?.accept || v?.Accept).length;
      const votesAgainst = votesList.filter((v: any) => v?.reject || v?.Reject).length;
      const action = payload.action || voteRequest.action || {};
      const actionKey = Object.keys(action)[0] || "Unknown";
      const actionData = action[actionKey];
      const title = actionKey.replace(/ARC_|_/g, " ");

      // Extract requester information
      const requester = payload.requester || voteRequest.requester || "Unknown";
      const requesterParty = payload.requesterName || voteRequest.requesterName || requester;

      // Extract reason
      const reason = payload.reason?.url || payload.reason || voteRequest.reason?.url || voteRequest.reason || "No reason provided";

      // Extract voting information
      const votedSvs = Object.keys(votes).map((svParty) => ({
        party: svParty,
        vote: votes[svParty]?.accept || votes[svParty]?.Accept ? "accept" : "reject",
        weight: votes[svParty]?.expiresAt || "N/A",
      }));

      // Determine status based on votes and threshold
      const threshold = votingThreshold || svCount;
      let status: "approved" | "rejected" | "pending" = "pending";
      if (votesFor >= threshold) status = "approved";
      else if (votesAgainst > svCount - threshold) status = "rejected";

      const trackingCid = payload.trackingCid || voteRequest.trackingCid || voteRequest.contract_id;
      const effectiveAt = payload.effectiveAt || voteRequest.effectiveAt;
      const expiresAt = payload.expiresAt || voteRequest.expiresAt;

      return {
        id: trackingCid?.slice(0, 12) || "unknown",
        trackingCid,
        title,
        actionType: actionKey,
        actionData,
        description: reason,
        requester,
        requesterParty,
        status,
        votesFor,
        votesAgainst,
        votedSvs,
        effectiveAt,
        expiresAt,
        createdAt: effectiveAt,
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
                  {voteRequestError?.message || "Ensure the local server is running (cd server && npm start)"}
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
                              Requested by: <span className="font-mono">{proposal.requesterParty.slice(0, 40)}...</span>
                            </p>
                          </div>
                        </div>
                        <Badge className={getStatusColor(proposal.status)}>{proposal.status}</Badge>
                      </div>

                      <div className="mb-4 p-3 rounded-lg bg-background/30 border border-border/30">
                        <p className="text-sm text-muted-foreground mb-1 font-semibold">Reason:</p>
                        <p className="text-sm">{proposal.description}</p>
                      </div>

                      <div className="grid grid-cols-1 md:grid-cols-4 gap-3 mb-4">
                        <div className="p-3 rounded-lg bg-background/50">
                          <p className="text-xs text-muted-foreground mb-1">Votes For</p>
                          <p className="text-lg font-bold text-success">{proposal.votesFor || 0}</p>
                        </div>
                        <div className="p-3 rounded-lg bg-background/50">
                          <p className="text-xs text-muted-foreground mb-1">Votes Against</p>
                          <p className="text-lg font-bold text-destructive">{proposal.votesAgainst || 0}</p>
                        </div>
                        <div className="p-3 rounded-lg bg-background/50">
                          <p className="text-xs text-muted-foreground mb-1">Effective At</p>
                          <p className="text-xs font-mono">
                            {proposal.effectiveAt ? new Date(proposal.effectiveAt).toLocaleDateString() : "N/A"}
                          </p>
                        </div>
                        <div className="p-3 rounded-lg bg-background/50">
                          <p className="text-xs text-muted-foreground mb-1">Expires At</p>
                          <p className="text-xs font-mono">
                            {proposal.expiresAt ? new Date(proposal.expiresAt).toLocaleDateString() : "N/A"}
                          </p>
                        </div>
                      </div>

                      {proposal.votedSvs?.length > 0 && (
                        <div className="mb-4">
                          <p className="text-xs text-muted-foreground mb-2 font-semibold">Votes Cast:</p>
                          <div className="flex flex-wrap gap-2">
                            {proposal.votedSvs.map((sv: any, idx: number) => (
                              <Badge
                                key={idx}
                                variant="outline"
                                className={
                                  sv.vote === "accept"
                                    ? "border-success/50 text-success"
                                    : "border-destructive/50 text-destructive"
                                }
                              >
                                {sv.party.slice(0, 20)}... - {sv.vote}
                              </Badge>
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
