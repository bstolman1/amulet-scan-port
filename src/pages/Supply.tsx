import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import {
  Coins,
  Lock,
  TrendingUp,
  Package,
  RefreshCw,
  AlertCircle,
  CheckCircle,
  Clock,
  ChevronDown,
  ChevronRight,
  Database,
} from "lucide-react";
import { useLatestACSSnapshot } from "@/hooks/use-acs-snapshots";
import { useAggregatedTemplateData } from "@/hooks/use-aggregated-template-data";
import { useQueryClient, useQuery } from "@tanstack/react-query";
import { toast } from "sonner";
import { useState } from "react";
import { DataSourcesFooter } from "@/components/DataSourcesFooter";
import { PaginationControls } from "@/components/PaginationControls";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { scanApi } from "@/lib/api-client";
import { useLocalACSAvailable } from "@/hooks/use-local-acs";
import { toCC } from "@/lib/amount-utils";

const Supply = () => {
  const queryClient = useQueryClient();
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [searchTerm, setSearchTerm] = useState("");
  const [currentPage, setCurrentPage] = useState(1);
  const [openItems, setOpenItems] = useState<Record<number, boolean>>({});
  const itemsPerPage = 20;

  // Check if local ACS data is available
  const { data: localAcsAvailable } = useLocalACSAvailable();

  const handleForceRefresh = async () => {
    try {
      setIsRefreshing(true);
      toast.info("Refreshing data...");
      await queryClient.cancelQueries({ predicate: () => true });
      await queryClient.invalidateQueries({ predicate: () => true });
      await queryClient.refetchQueries({ predicate: () => true, type: "active" });
      toast.success("All data refreshed!");
    } catch (err) {
      console.error("[ForceRefresh] error", err);
      toast.error("Refresh failed. Check console logs.");
    } finally {
      setIsRefreshing(false);
    }
  };

  const { data: latestSnapshot } = useLatestACSSnapshot();

  // Check if local ACS server is available
  const { data: localServerAvailable } = useQuery({
    queryKey: ["localServerCheck"],
    queryFn: async () => {
      try {
        const response = await fetch("http://localhost:3001/api/health");
        return response.ok;
      } catch {
        return false;
      }
    },
    staleTime: 30_000,
  });

  // Use server-side aggregation for supply totals (bypasses 10k limit)
  const { data: supplyData, isLoading: supplyLoading, error: supplyError } = useQuery({
    queryKey: ["serverSupplyTotals"],
    queryFn: async () => {
      const response = await fetch("http://localhost:3001/api/acs/rich-list?limit=1");
      if (!response.ok) throw new Error("Failed to fetch supply data");
      const data = await response.json();
      return {
        totalSupply: data.totalSupply || 0,
        unlockedSupply: data.unlockedSupply || 0,
        lockedSupply: data.lockedSupply || 0,
        holderCount: data.holderCount || 0,
      };
    },
    enabled: !!localServerAvailable,
    staleTime: 30_000,
  });

  // Fallback to client-side calculation if server unavailable
  const { data: amuletData, isLoading: amuletLoading } = useAggregatedTemplateData(
    latestSnapshot?.id,
    "Splice:Amulet:Amulet",
    !!latestSnapshot && !localServerAvailable,
  );

  const { data: lockedData, isLoading: lockedLoading } = useAggregatedTemplateData(
    latestSnapshot?.id,
    "Splice:Amulet:LockedAmulet",
    !!latestSnapshot && !localServerAvailable,
  );

  // Fetch allocations
  const allocationsQuery = useAggregatedTemplateData(
    latestSnapshot?.id,
    "Splice:AmuletAllocation:AmuletAllocation",
    !!latestSnapshot,
  );

  // Fetch mining rounds
  const { data: latestRound, isLoading: latestRoundLoading } = useQuery({
    queryKey: ["latestRound"],
    queryFn: () => scanApi.fetchLatestRound(),
  });

  const { data: openRoundsData, isLoading: openLoading } = useAggregatedTemplateData(
    latestSnapshot?.id,
    "Splice:Round:OpenMiningRound",
    !!latestSnapshot,
  );

  const { data: issuingRoundsData, isLoading: issuingLoading } = useAggregatedTemplateData(
    latestSnapshot?.id,
    "Splice:Round:IssuingMiningRound",
    !!latestSnapshot,
  );

  const { data: closedRoundsData, isLoading: closedLoading } = useAggregatedTemplateData(
    latestSnapshot?.id,
    "Splice:Round:ClosedMiningRound",
    !!latestSnapshot,
  );

  const isLoading =
    supplyLoading ||
    amuletLoading ||
    lockedLoading ||
    allocationsQuery.isLoading ||
    openLoading ||
    issuingLoading ||
    closedLoading ||
    latestRoundLoading;

  // Use server-side totals if available, otherwise fallback to client calculation
  const totalUnlocked = supplyData?.unlockedSupply ?? (amuletData?.data || []).reduce((sum: number, amulet: any) => {
    const amount = toCC(amulet.amount?.initialAmount || "0");
    return sum + amount;
  }, 0);

  const totalLocked = supplyData?.lockedSupply ?? (lockedData?.data || []).reduce((sum: number, locked: any) => {
    const amount = toCC(locked.amulet?.amount?.initialAmount || locked.amount?.initialAmount || "0");
    return sum + amount;
  }, 0);

  const totalSupply = supplyData?.totalSupply ?? (totalUnlocked + totalLocked);
  const circulatingSupply = totalUnlocked;

  // Process allocations
  const getField = (obj: any, fieldNames: string[]) => {
    for (const name of fieldNames) {
      if (obj?.[name] !== undefined && obj?.[name] !== null) return obj[name];
      if (name.includes(".")) {
        const parts = name.split(".");
        let current = obj;
        for (const part of parts) {
          if (current?.[part] !== undefined && current?.[part] !== null) {
            current = current[part];
          } else {
            current = null;
            break;
          }
        }
        if (current !== null) return current;
      }
      if (obj?.payload?.[name] !== undefined && obj?.payload?.[name] !== null) return obj.payload[name];
    }
    return null;
  };

  const allocations = allocationsQuery.data?.data || [];
  const filteredAllocations = allocations.filter((allocation: any) => {
    if (!searchTerm) return true;
    const search = searchTerm.toLowerCase();
    const executor = getField(allocation, ["executor", "allocation.settlement.executor"]) || "";
    const sender = getField(allocation, ["sender", "allocation.transferLeg.sender"]) || "";
    const receiver = getField(allocation, ["receiver", "allocation.transferLeg.receiver"]) || "";
    const amount = getField(allocation, ["amount", "allocation.transferLeg.amount"]) || "";

    return (
      executor.toLowerCase().includes(search) ||
      sender.toLowerCase().includes(search) ||
      receiver.toLowerCase().includes(search) ||
      amount.toString().includes(search)
    );
  });

  const paginatedAllocations = filteredAllocations.slice((currentPage - 1) * itemsPerPage, currentPage * itemsPerPage);

  const totalAllocationsPages = Math.ceil(filteredAllocations.length / itemsPerPage);

  const totalAllocationAmount = filteredAllocations.reduce((sum: number, allocation: any) => {
    const amount = toCC(getField(allocation, ["amount", "allocation.transferLeg.amount"]) || "0");
    return sum + amount;
  }, 0);

  // Process mining rounds
  const openRounds = openRoundsData?.data || [];
  const issuingRounds = issuingRoundsData?.data || [];
  const closedRounds = (closedRoundsData?.data || []).slice(0, 20);

  const formatAmount = (amount: number) => {
    return amount.toLocaleString(undefined, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
  };

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div className="flex items-center justify-between">
          <div>
            <div className="flex items-center gap-2 mb-2">
              <h2 className="text-3xl font-bold">Supply & Tokenomics</h2>
              {localAcsAvailable && (
                <Badge variant="outline" className="bg-green-500/10 text-green-500 border-green-500/30">
                  <Database className="h-3 w-3 mr-1" />
                  Local ACS
                </Badge>
              )}
            </div>
            <p className="text-muted-foreground">Track supply, allocations, and mining rounds from ACS snapshots</p>
          </div>
          <Button onClick={handleForceRefresh} variant="outline" size="sm" className="gap-2" disabled={isRefreshing}>
            <RefreshCw className={`h-4 w-4 ${isRefreshing ? "animate-spin" : ""}`} />
            {isRefreshing ? "Refreshing..." : "Force Refresh"}
          </Button>
        </div>

        {/* Supply Stats */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <Card className="glass-card p-6">
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-sm font-medium text-muted-foreground">Total Supply</h3>
              <Coins className="h-5 w-5 text-primary" />
            </div>
            {isLoading ? (
              <Skeleton className="h-10 w-full" />
            ) : (
              <>
                <p className="text-3xl font-bold text-primary mb-1">{formatAmount(totalSupply)}</p>
                <p className="text-xs text-muted-foreground">CC</p>
              </>
            )}
          </Card>

          <Card className="glass-card p-6">
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-sm font-medium text-muted-foreground">Unlocked Canton Coins</h3>
              <Package className="h-5 w-5 text-success" />
            </div>
            {isLoading ? (
              <Skeleton className="h-10 w-full" />
            ) : (
              <>
                <p className="text-3xl font-bold text-success mb-1">{formatAmount(totalUnlocked)}</p>
                <p className="text-xs text-muted-foreground">
                  {((totalUnlocked / totalSupply) * 100).toFixed(1)}% of supply
                </p>
              </>
            )}
          </Card>

          <Card className="glass-card p-6">
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-sm font-medium text-muted-foreground">Locked Canton Coins</h3>
              <Lock className="h-5 w-5 text-warning" />
            </div>
            {isLoading ? (
              <Skeleton className="h-10 w-full" />
            ) : (
              <>
                <p className="text-3xl font-bold text-warning mb-1">{formatAmount(totalLocked)}</p>
                <p className="text-xs text-muted-foreground">
                  {((totalLocked / totalSupply) * 100).toFixed(1)}% of supply
                </p>
              </>
            )}
          </Card>

          <Card className="glass-card p-6">
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-sm font-medium text-muted-foreground">Circulating Supply</h3>
              <TrendingUp className="h-5 w-5 text-chart-2" />
            </div>
            {isLoading ? (
              <Skeleton className="h-10 w-full" />
            ) : (
              <>
                <p className="text-3xl font-bold text-chart-2 mb-1">{formatAmount(circulatingSupply)}</p>
                <p className="text-xs text-muted-foreground">
                  {((circulatingSupply / totalSupply) * 100).toFixed(1)}% of supply
                </p>
              </>
            )}
          </Card>
        </div>

        {/* Allocations Section */}
        <div className="space-y-4">
          <div>
            <h3 className="text-2xl font-bold mb-2">Amulet Allocations</h3>
            <p className="text-muted-foreground">Locked amulet allocations and transfer settlements</p>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <Card className="glass-card p-6">
              <div className="flex items-center justify-between mb-3">
                <h3 className="text-sm font-medium text-muted-foreground">Total Allocations</h3>
                <Lock className="h-5 w-5 text-primary" />
              </div>
              {isLoading ? (
                <Skeleton className="h-10 w-full" />
              ) : (
                <p className="text-3xl font-bold text-primary">{allocations.length.toLocaleString()}</p>
              )}
            </Card>

            <Card className="glass-card p-6">
              <div className="flex items-center justify-between mb-3">
                <h3 className="text-sm font-medium text-muted-foreground">Total Amount</h3>
                <Lock className="h-5 w-5 text-primary" />
              </div>
              {isLoading ? (
                <Skeleton className="h-10 w-full" />
              ) : (
                <p className="text-3xl font-bold text-primary">
                  {totalAllocationAmount.toLocaleString(undefined, { maximumFractionDigits: 4 })} CC
                </p>
              )}
            </Card>

            <Card className="glass-card p-6">
              <div className="flex items-center justify-between mb-3">
                <h3 className="text-sm font-medium text-muted-foreground">Unique Executors</h3>
                <Lock className="h-5 w-5 text-primary" />
              </div>
              {isLoading ? (
                <Skeleton className="h-10 w-full" />
              ) : (
                <p className="text-3xl font-bold text-primary">
                  {
                    new Set(allocations.map((a: any) => getField(a, ["executor", "allocation.settlement.executor"])))
                      .size
                  }
                </p>
              )}
            </Card>
          </div>

          <Input
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            placeholder="Search by executor, sender, receiver, or amount..."
            className="max-w-md"
          />

          {isLoading ? (
            <div className="grid gap-4">
              <Skeleton className="h-32 w-full" />
              <Skeleton className="h-32 w-full" />
            </div>
          ) : (
            <div className="space-y-4">
              {paginatedAllocations.map((allocation: any, index: number) => {
                const itemKey = (currentPage - 1) * itemsPerPage + index;
                const executor = getField(allocation, ["executor", "allocation.settlement.executor"]);
                const sender = getField(allocation, ["sender", "allocation.transferLeg.sender"]);
                const receiver = getField(allocation, ["receiver", "allocation.transferLeg.receiver"]);
                const amount = getField(allocation, ["amount", "allocation.transferLeg.amount"]);
                const requestedAt = getField(allocation, ["requestedAt", "allocation.settlement.requestedAt"]);
                const transferLegId = getField(allocation, ["transferLegId", "allocation.transferLegId"]);

                return (
                  <Card key={index}>
                    <Collapsible
                      open={openItems[itemKey] || false}
                      onOpenChange={(isOpen) => setOpenItems((prev) => ({ ...prev, [itemKey]: isOpen }))}
                    >
                      <CollapsibleTrigger className="w-full">
                        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                          <div className="flex items-center gap-2">
                            {openItems[itemKey] ? (
                              <ChevronDown className="h-4 w-4" />
                            ) : (
                              <ChevronRight className="h-4 w-4" />
                            )}
                            <CardTitle className="text-base font-medium">
                              Allocation {(currentPage - 1) * itemsPerPage + index + 1}
                            </CardTitle>
                          </div>
                          <Badge variant="secondary">{amount ? `${parseFloat(amount).toFixed(4)} CC` : "N/A"}</Badge>
                        </CardHeader>
                      </CollapsibleTrigger>
                      <CardContent>
                        <div className="grid gap-2 text-sm">
                          <div className="flex justify-between">
                            <span className="text-muted-foreground">Executor:</span>
                            <span className="font-mono text-xs">{executor || "N/A"}</span>
                          </div>
                          <div className="flex justify-between">
                            <span className="text-muted-foreground">Sender:</span>
                            <span className="font-mono text-xs">{sender || "N/A"}</span>
                          </div>
                          <div className="flex justify-between">
                            <span className="text-muted-foreground">Receiver:</span>
                            <span className="font-mono text-xs">{receiver || "N/A"}</span>
                          </div>
                          <div className="flex justify-between">
                            <span className="text-muted-foreground">Transfer Leg ID:</span>
                            <span className="font-mono text-xs">{transferLegId || "N/A"}</span>
                          </div>
                          {requestedAt && (
                            <div className="flex justify-between">
                              <span className="text-muted-foreground">Requested At:</span>
                              <span className="text-xs">{new Date(requestedAt).toLocaleString()}</span>
                            </div>
                          )}
                        </div>
                        <CollapsibleContent>
                          <div className="mt-4 p-4 rounded-lg bg-muted/50">
                            <p className="text-xs font-semibold mb-2">Raw JSON:</p>
                            <pre className="text-xs overflow-auto max-h-64">{JSON.stringify(allocation, null, 2)}</pre>
                          </div>
                        </CollapsibleContent>
                      </CardContent>
                    </Collapsible>
                  </Card>
                );
              })}
            </div>
          )}

          <PaginationControls
            currentPage={currentPage}
            totalItems={filteredAllocations.length}
            pageSize={itemsPerPage}
            onPageChange={setCurrentPage}
          />
        </div>

        {/* Mining Rounds Section */}
        <div className="space-y-6">
          <div>
            <h3 className="text-2xl font-bold mb-2">Mining Rounds</h3>
            <p className="text-muted-foreground">Track open, issuing, and closed mining rounds</p>
          </div>

          {/* Current Round Info */}
          <Card className="glass-card">
            <div className="p-6">
              <h3 className="text-xl font-bold mb-4 flex items-center">
                <Clock className="h-5 w-5 mr-2 text-primary" />
                Current Round
              </h3>
              {isLoading ? (
                <Skeleton className="h-24 w-full" />
              ) : latestRound ? (
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <div className="p-4 rounded-lg bg-primary/10">
                    <p className="text-sm text-muted-foreground mb-1">Round Number</p>
                    <p className="text-3xl font-bold text-primary">{latestRound.round.toLocaleString()}</p>
                  </div>
                  <div className="p-4 rounded-lg bg-muted/30">
                    <p className="text-sm text-muted-foreground mb-1">Effective At</p>
                    <p className="text-lg font-semibold">{new Date(latestRound.effectiveAt).toLocaleString()}</p>
                  </div>
                </div>
              ) : (
                <p className="text-muted-foreground text-center">Unable to load current round data</p>
              )}
            </div>
          </Card>

          {/* Open Rounds */}
          <div>
            <h4 className="text-xl font-bold mb-4 flex items-center">
              <AlertCircle className="h-5 w-5 mr-2 text-warning" />
              Open Rounds
            </h4>
            {isLoading ? (
              <Skeleton className="h-48 w-full" />
            ) : openRounds.length === 0 ? (
              <Card className="glass-card p-6">
                <p className="text-muted-foreground text-center">No open rounds at the moment</p>
              </Card>
            ) : (
              <div className="space-y-4">
                {openRounds.map((round: any) => (
                  <Card key={round.id} className="glass-card">
                    <div className="p-6">
                      <div className="flex items-start justify-between mb-4">
                        <div>
                          <h4 className="text-xl font-bold mb-1">Round {round.roundNumber}</h4>
                          <p className="text-sm text-muted-foreground">
                            Opens: {new Date(round.opensAt).toLocaleString()}
                          </p>
                          <p className="text-sm text-muted-foreground">
                            Target Close: {new Date(round.targetClosesAt).toLocaleString()}
                          </p>
                        </div>
                        <Badge className="bg-warning/10 text-warning border-warning/20">
                          <Clock className="h-3 w-3 mr-1" />
                          open
                        </Badge>
                      </div>

                      <div className="p-4 rounded-lg bg-muted/30">
                        <p className="text-sm text-muted-foreground mb-1">Contract ID</p>
                        <p className="font-mono text-xs truncate">{round.contractId}</p>
                      </div>
                    </div>
                  </Card>
                ))}
              </div>
            )}
          </div>

          {/* Issuing Rounds */}
          <div>
            <h4 className="text-xl font-bold mb-4 flex items-center">
              <Clock className="h-5 w-5 mr-2 text-primary" />
              Issuing Rounds
            </h4>
            {isLoading ? (
              <Skeleton className="h-48 w-full" />
            ) : issuingRounds.length === 0 ? (
              <Card className="glass-card p-6">
                <p className="text-muted-foreground text-center">No issuing rounds at the moment</p>
              </Card>
            ) : (
              <div className="space-y-4">
                {issuingRounds.map((round: any) => (
                  <Card key={round.id} className="glass-card">
                    <div className="p-6">
                      <div className="flex items-start justify-between mb-4">
                        <div>
                          <h4 className="text-xl font-bold mb-1">Round {round.roundNumber}</h4>
                          <p className="text-sm text-muted-foreground">
                            Opens: {new Date(round.opensAt).toLocaleString()}
                          </p>
                        </div>
                        <Badge className="bg-primary/10 text-primary border-primary/20">
                          <Clock className="h-3 w-3 mr-1" />
                          issuing
                        </Badge>
                      </div>

                      <div className="p-4 rounded-lg bg-muted/30">
                        <p className="text-sm text-muted-foreground mb-1">Contract ID</p>
                        <p className="font-mono text-xs truncate">{round.contractId}</p>
                      </div>
                    </div>
                  </Card>
                ))}
              </div>
            )}
          </div>

          {/* Closed Rounds */}
          <div>
            <h4 className="text-xl font-bold mb-4 flex items-center">
              <CheckCircle className="h-5 w-5 mr-2 text-success" />
              Recently Closed Rounds
            </h4>
            {isLoading ? (
              <Skeleton className="h-48 w-full" />
            ) : closedRounds.length === 0 ? (
              <Card className="glass-card p-6">
                <p className="text-muted-foreground text-center">No closed rounds available</p>
              </Card>
            ) : (
              <div className="space-y-4">
                {closedRounds.map((round: any) => (
                  <Card key={round.contractId} className="glass-card">
                    <div className="p-6">
                      <div className="flex items-start justify-between mb-4">
                        <div>
                          <h4 className="text-xl font-bold mb-1">Round {round.roundNumber}</h4>
                          <p className="text-sm text-muted-foreground">
                            Closed: {new Date(round.createdAt).toLocaleString()}
                          </p>
                        </div>
                        <Badge className="bg-success/10 text-success border-success/20">
                          <CheckCircle className="h-3 w-3 mr-1" />
                          closed
                        </Badge>
                      </div>

                      <div className="p-4 rounded-lg bg-muted/30">
                        <p className="text-sm text-muted-foreground mb-1">Contract ID</p>
                        <p className="font-mono text-xs truncate">{round.contractId}</p>
                      </div>
                    </div>
                  </Card>
                ))}
              </div>
            )}
          </div>
        </div>

        <DataSourcesFooter
          snapshotId={latestSnapshot?.id}
          templateSuffixes={[
            "Splice:Amulet:Amulet",
            "Splice:Amulet:LockedAmulet",
            "Splice:AmuletAllocation:AmuletAllocation",
            "Splice:Round:OpenMiningRound",
            "Splice:Round:IssuingMiningRound",
            "Splice:Round:ClosedMiningRound",
          ]}
          isProcessing={false}
        />
      </div>
    </DashboardLayout>
  );
};

export default Supply;
