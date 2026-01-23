import { DashboardLayout } from "@/components/DashboardLayout";
import { Card } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Badge } from "@/components/ui/badge";
import { Shield, CheckCircle, AlertCircle, Code } from "lucide-react";
import { useSvNodeStates, useDsoInfo, useDsoRules } from "@/hooks/use-canton-scan-api";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Button } from "@/components/ui/button";

const DSOState = () => {
  const { data: dsoInfo, isLoading: dsoLoading } = useDsoInfo();
  const { data: svNodeStates, isLoading: nodesLoading } = useSvNodeStates();
  const { data: dsoRules, isLoading: rulesLoading } = useDsoRules();

  const isLoading = dsoLoading || nodesLoading || rulesLoading;
  const nodeStatesData = svNodeStates || [];

  // Helper to safely extract field values from nested structure
  const getField = (record: any, ...fieldNames: string[]) => {
    for (const field of fieldNames) {
      if (record[field] !== undefined && record[field] !== null) return record[field];
      if (record?.contract?.payload?.[field] !== undefined) return record.contract.payload[field];
      if (record?.payload?.[field] !== undefined) return record.payload[field];
    }
    return undefined;
  };

  const formatParty = (party: string) => {
    if (!party) return "Unknown";
    if (party.length > 30) {
      return `${party.substring(0, 15)}...${party.substring(party.length - 12)}`;
    }
    return party;
  };

  // Extract SV info from DSO rules if available
  const svInfoMap = dsoRules?.contract?.payload?.svs || {};
  const svInfoList = Object.entries(svInfoMap).map(([party, info]: [string, any]) => ({
    party,
    name: info?.name,
    joinedAt: info?.joinedAsOfRound?.number,
  }));

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <h1 className="text-3xl font-bold mb-2 flex items-center gap-2">
            <Shield className="h-8 w-8 text-primary" />
            DSO State & SV Nodes
          </h1>
          <p className="text-muted-foreground">
            Monitor Decentralized Synchronizer Operator state and SV node information from live network data.
          </p>
        </div>

        <div className="grid gap-4 md:grid-cols-4">
          <Card className="p-6">
            <h3 className="text-sm font-medium text-muted-foreground mb-2">SV Node States</h3>
            {isLoading ? (
              <Skeleton className="h-8 w-24" />
            ) : (
              <p className="text-2xl font-bold">{nodeStatesData.length}</p>
            )}
          </Card>

          <Card className="p-6">
            <h3 className="text-sm font-medium text-muted-foreground mb-2">Voting Threshold</h3>
            {isLoading ? (
              <Skeleton className="h-8 w-24" />
            ) : (
              <p className="text-2xl font-bold">{dsoInfo?.voting_threshold || "—"}</p>
            )}
          </Card>

          <Card className="p-6">
            <h3 className="text-sm font-medium text-muted-foreground mb-2">Latest Round</h3>
            {isLoading ? (
              <Skeleton className="h-8 w-24" />
            ) : (
              <p className="text-2xl font-bold">
                {dsoInfo?.latest_mining_round?.contract?.payload?.round?.number || "—"}
              </p>
            )}
          </Card>

          <Card className="p-6">
            <h3 className="text-sm font-medium text-muted-foreground mb-2">Initial Round</h3>
            {isLoading ? (
              <Skeleton className="h-8 w-24" />
            ) : (
              <p className="text-2xl font-bold">{dsoInfo?.initial_round || "—"}</p>
            )}
          </Card>
        </div>

        <Card className="p-6">
          <Tabs defaultValue="nodes" className="w-full">
            <TabsList className="grid w-full grid-cols-2">
              <TabsTrigger value="nodes">SV Node States ({nodeStatesData.length})</TabsTrigger>
              <TabsTrigger value="svinfo">SV Registry ({svInfoList.length})</TabsTrigger>
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
                  const payload = node?.contract?.payload || node?.payload || node;
                  const state = payload?.state;
                  const svName = payload?.svName;
                  const svParty = payload?.sv;

                  const stateValue = typeof state === "object" ? JSON.stringify(state) : state;
                  const nameValue = typeof svName === "object" ? JSON.stringify(svName) : svName;
                  const partyValue = typeof svParty === "object" ? JSON.stringify(svParty) : svParty;
                  const isActive = stateValue === "active" || stateValue?.includes("Synchronized");

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

            <TabsContent value="svinfo" className="space-y-3 mt-4">
              {isLoading ? (
                <div className="space-y-4">
                  {[1, 2, 3].map((i) => (
                    <Skeleton key={i} className="h-20 w-full" />
                  ))}
                </div>
              ) : svInfoList.length === 0 ? (
                <p className="text-center text-muted-foreground py-8">No SV registry data found</p>
              ) : (
                svInfoList.map((sv, idx) => (
                  <Card key={idx} className="p-4">
                    <div className="flex justify-between items-start">
                      <div className="flex-1 space-y-2">
                        <p className="text-sm font-semibold">{sv.name || "Unknown"}</p>
                        <div>
                          <p className="text-xs text-muted-foreground">Party</p>
                          <p className="font-mono text-xs break-all">{formatParty(sv.party)}</p>
                        </div>
                      </div>
                      {sv.joinedAt !== undefined && (
                        <Badge variant="outline">Joined Round {sv.joinedAt}</Badge>
                      )}
                    </div>
                  </Card>
                ))
              )}
            </TabsContent>
          </Tabs>
        </Card>

        <Card className="p-4 text-xs text-muted-foreground">
          <p>
            Data sourced directly from Canton Scan API <code>/v0/dso</code> endpoint.
          </p>
        </Card>
      </div>
    </DashboardLayout>
  );
};

export default DSOState;