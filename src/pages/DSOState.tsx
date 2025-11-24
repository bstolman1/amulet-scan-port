import { useState } from "react";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Badge } from "@/components/ui/badge";
import { Shield, CheckCircle, AlertCircle, Code } from "lucide-react";
import { useLatestACSSnapshot } from "@/hooks/use-acs-snapshots";
import { useAggregatedTemplateData } from "@/hooks/use-aggregated-template-data";
import { DataSourcesFooter } from "@/components/DataSourcesFooter";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Button } from "@/components/ui/button";

const DSOState = () => {
  const { data: latestSnapshot } = useLatestACSSnapshot();

  const nodeStatesQuery = useAggregatedTemplateData(latestSnapshot?.id, "DSO:SvState:SvNodeState", !!latestSnapshot);

  const statusReportsQuery = useAggregatedTemplateData(
    latestSnapshot?.id,
    "DSO:SvState:SvStatusReport",
    !!latestSnapshot,
  );

  const rewardStatesQuery = useAggregatedTemplateData(
    latestSnapshot?.id,
    "DSO:SvState:SvRewardState",
    !!latestSnapshot,
  );

  const nodeStatesData = nodeStatesQuery.data?.data || [];
  const statusReportsData = statusReportsQuery.data?.data || [];
  const rewardStatesData = rewardStatesQuery.data?.data || [];

  // Helper to safely extract field values from nested structure
  const getField = (record: any, ...fieldNames: string[]) => {
    for (const field of fieldNames) {
      if (record[field] !== undefined && record[field] !== null) return record[field];
      if (record.payload?.[field] !== undefined && record.payload?.[field] !== null) return record.payload[field];
    }
    return undefined;
  };
  const isLoading = nodeStatesQuery.isLoading || statusReportsQuery.isLoading || rewardStatesQuery.isLoading;

  // Debug logging
  console.log("🔍 DEBUG DSOState: Node states count:", nodeStatesData.length);
  console.log("🔍 DEBUG DSOState: Status reports count:", statusReportsData.length);
  console.log("🔍 DEBUG DSOState: Reward states count:", rewardStatesData.length);
  if (nodeStatesData.length > 0) {
    console.log("🔍 DEBUG DSOState: First node state:", JSON.stringify(nodeStatesData[0], null, 2));
  }

  const formatParty = (party: string) => {
    if (!party) return "Unknown";
    if (party.length > 30) {
      return `${party.substring(0, 15)}...${party.substring(party.length - 12)}`;
    }
    return party;
  };

  const activeNodes = nodeStatesData.filter(
    (node: any) => node.payload?.state === "active" || node.state === "active",
  ).length;

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <h1 className="text-3xl font-bold mb-2 flex items-center gap-2">
            <Shield className="h-8 w-8 text-primary" />
            DSO State & SV Nodes
          </h1>
          <p className="text-muted-foreground">
            Monitor Decentralized Synchronizer Operator state, SV node status, and reward information.
          </p>
        </div>

        <div className="grid gap-4 md:grid-cols-3">
          <Card className="p-6">
            <h3 className="text-sm font-medium text-muted-foreground mb-2">SV Node States</h3>
            {isLoading ? (
              <Skeleton className="h-8 w-24" />
            ) : (
              <div>
                <p className="text-2xl font-bold">{nodeStatesData.length}</p>
                <p className="text-xs text-muted-foreground mt-1">{activeNodes} active</p>
              </div>
            )}
          </Card>

          <Card className="p-6">
            <h3 className="text-sm font-medium text-muted-foreground mb-2">Status Reports</h3>
            {isLoading ? (
              <Skeleton className="h-8 w-24" />
            ) : (
              <p className="text-2xl font-bold">{statusReportsData.length}</p>
            )}
          </Card>

          <Card className="p-6">
            <h3 className="text-sm font-medium text-muted-foreground mb-2">Reward States</h3>
            {isLoading ? (
              <Skeleton className="h-8 w-24" />
            ) : (
              <p className="text-2xl font-bold">{rewardStatesData.length}</p>
            )}
          </Card>
        </div>

        <Card className="p-6">
          <Tabs defaultValue="nodes" className="w-full">
            <TabsList className="grid w-full grid-cols-3">
              <TabsTrigger value="nodes">Node States ({nodeStatesData.length})</TabsTrigger>
              <TabsTrigger value="reports">Status Reports ({statusReportsData.length})</TabsTrigger>
              <TabsTrigger value="rewards">Rewards ({rewardStatesData.length})</TabsTrigger>
            </TabsList>

            <TabsContent value="nodes" className="space-y-3 mt-4">
              {isLoading ? (
                <div className="space-y-4">
                  {[1, 2, 3].map((i) => (
                    <Skeleton key={i} className="h-32 w-full" />
                  ))}
                </div>
              ) : nodeStatesData.length === 0 ? (
                <p className="text-center text-muted-foreground py-8">No node states found</p>
              ) : (
                nodeStatesData.map((node: any, idx: number) => {
                  const state = node.payload?.state || node.state;
                  const svName = node.payload?.svName || node.svName;
                  const svParty = node.payload?.svParty || node.svParty;
                  const isActive = state === "active";

                  const stateValue = typeof state === "object" ? JSON.stringify(state) : state;
                  const nameValue = typeof svName === "object" ? JSON.stringify(svName) : svName;
                  const partyValue = typeof svParty === "object" ? JSON.stringify(svParty) : svParty;

                  return (
                    <Card key={idx} className="p-4 space-y-3">
                      <div className="flex justify-between items-start">
                        <div className="flex-1 space-y-2">
                          <div className="flex items-center gap-2">
                            {isActive ? (
                              <CheckCircle className="h-4 w-4 text-green-500" />
                            ) : (
                              <AlertCircle className="h-4 w-4 text-yellow-500" />
                            )}
                            <p className="text-sm font-semibold">{nameValue || "Unknown SV"}</p>
                          </div>

                          <div>
                            <p className="text-xs text-muted-foreground">Party ID</p>
                            <p className="font-mono text-xs break-all">{partyValue || "Unknown"}</p>
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
                                {JSON.stringify(node, null, 2)}
                              </pre>
                            </CollapsibleContent>
                          </Collapsible>
                        </div>
                        <Badge variant={isActive ? "default" : "secondary"}>{stateValue || "Unknown"}</Badge>
                      </div>
                    </Card>
                  );
                })
              )}
            </TabsContent>

            <TabsContent value="reports" className="space-y-3 mt-4">
              {isLoading ? (
                <div className="space-y-4">
                  {[1, 2].map((i) => (
                    <Skeleton key={i} className="h-32 w-full" />
                  ))}
                </div>
              ) : statusReportsData.length === 0 ? (
                <p className="text-center text-muted-foreground py-8">No status reports found</p>
              ) : (
                statusReportsData.map((report: any, idx: number) => (
                  <Card key={idx} className="p-4 space-y-3">
                    <div className="flex justify-between items-start">
                      <div className="flex-1 space-y-2">
                        <div>
                          <p className="text-xs text-muted-foreground">SV Name</p>
                          <p className="text-sm font-semibold">
                            {formatParty(report.payload?.svName || report.svName || "Unknown")}
                          </p>
                        </div>

                        {(report.payload?.timestamp || report.timestamp) && (
                          <div>
                            <p className="text-xs text-muted-foreground">Reported At</p>
                            <p className="text-sm">
                              {new Date(report.payload?.timestamp || report.timestamp).toLocaleString()}
                            </p>
                          </div>
                        )}

                        <Collapsible className="pt-2 border-t">
                          <CollapsibleTrigger asChild>
                            <Button variant="ghost" size="sm" className="w-full justify-start">
                              <Code className="h-4 w-4 mr-2" />
                              Show Raw JSON
                            </Button>
                          </CollapsibleTrigger>
                          <CollapsibleContent className="mt-2">
                            <pre className="text-xs bg-muted p-3 rounded overflow-auto max-h-96">
                              {JSON.stringify(report, null, 2)}
                            </pre>
                          </CollapsibleContent>
                        </Collapsible>
                      </div>
                      <Badge variant="outline">Report</Badge>
                    </div>
                  </Card>
                ))
              )}
            </TabsContent>

            <TabsContent value="rewards" className="space-y-3 mt-4">
              {isLoading ? (
                <div className="space-y-4">
                  {[1, 2].map((i) => (
                    <Skeleton key={i} className="h-32 w-full" />
                  ))}
                </div>
              ) : rewardStatesData.length === 0 ? (
                <p className="text-center text-muted-foreground py-8">No reward states found</p>
              ) : (
                rewardStatesData.map((reward: any, idx: number) => {
                  const round = reward.payload?.round || reward.round;
                  const roundValue = typeof round === "object" ? round?.number : round;
                  const svParty = reward.payload?.svParty || reward.svParty;
                  const svRewardWeight = reward.payload?.svRewardWeight || reward.svRewardWeight;

                  return (
                    <Card key={idx} className="p-4 space-y-3">
                      <div className="flex justify-between items-start">
                        <div className="flex-1 space-y-2">
                          <div className="grid grid-cols-2 gap-3">
                            <div>
                              <p className="text-xs text-muted-foreground">Round</p>
                              <p className="text-sm font-semibold">{roundValue || "Unknown"}</p>
                            </div>
                            {svRewardWeight && (
                              <div>
                                <p className="text-xs text-muted-foreground">Reward Weight</p>
                                <p className="text-sm">{svRewardWeight}</p>
                              </div>
                            )}
                          </div>

                          <div>
                            <p className="text-xs text-muted-foreground">SV Party</p>
                            <p className="font-mono text-xs break-all">{svParty || "Unknown"}</p>
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
                                {JSON.stringify(reward, null, 2)}
                              </pre>
                            </CollapsibleContent>
                          </Collapsible>
                        </div>
                        <Badge variant="default">Reward</Badge>
                      </div>
                    </Card>
                  );
                })
              )}
            </TabsContent>
          </Tabs>
        </Card>

        <DataSourcesFooter
          snapshotId={latestSnapshot?.id}
          templateSuffixes={["DSO:SvState:SvNodeState", "DSO:SvState:SvStatusReport", "DSO:SvState:SvRewardState"]}
          isProcessing={false}
        />
      </div>
    </DashboardLayout>
  );
};

export default DSOState;
