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
import { useLocalACSAvailable } from "@/hooks/use-local-acs";
import { useAggregatedTemplateData } from "@/hooks/use-aggregated-template-data";
import { useQueryClient, useQuery, keepPreviousData } from "@tanstack/react-query";
import { toast } from "sonner";
import { useState } from "react";
import { DataSourcesFooter } from "@/components/DataSourcesFooter";
import { PaginationControls } from "@/components/PaginationControls";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { scanApi } from "@/lib/api-client";
import { toCC } from "@/lib/amount-utils";
import { getACSRichList, getACSAllocations, getACSMiningRounds } from "@/lib/duckdb-api-client";

const Supply = () => {
  const queryClient = useQueryClient();
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [searchTerm, setSearchTerm] = useState("");
  const [currentPage, setCurrentPage] = useState(1);
  const [openItems, setOpenItems] = useState<Record<string | number, boolean>>({});
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

  // No longer need latestSnapshot - data comes from updates

  // Fetch supply stats from server-side aggregation (uses same endpoint as rich-list)
  const { data: supplyStats, isLoading: supplyLoading } = useQuery({
    queryKey: ["supply-stats"],
    queryFn: () => getACSRichList({ limit: 1 }), // We only need the stats, not the holder list
    staleTime: 30000,
  });

  // Fetch allocations from server-side
  const { data: allocationsData, isLoading: allocationsLoading } = useQuery({
    queryKey: ["allocations", searchTerm, currentPage],
    queryFn: () => getACSAllocations({ 
      limit: itemsPerPage, 
      offset: (currentPage - 1) * itemsPerPage,
      search: searchTerm || undefined 
    }),
    staleTime: 30000,
    placeholderData: keepPreviousData, // Keep old data while fetching new page
  });

  // Fetch mining rounds from server-side
  const { data: miningRoundsData, isLoading: miningRoundsLoading } = useQuery({
    queryKey: ["mining-rounds"],
    queryFn: () => getACSMiningRounds({ closedLimit: 20 }),
    staleTime: 30000,
  });

  // Fetch latest round from scan API
  const { data: latestRound, isLoading: latestRoundLoading } = useQuery({
    queryKey: ["latestRound"],
    queryFn: () => scanApi.fetchLatestRound(),
  });

  const isLoading = supplyLoading || allocationsLoading || miningRoundsLoading || latestRoundLoading;

  // Use server-side aggregated supply metrics
  const totalUnlocked = supplyStats?.unlockedSupply || 0;
  const totalLocked = supplyStats?.lockedSupply || 0;
  const totalSupply = supplyStats?.totalSupply || 0;
  const circulatingSupply = totalUnlocked;

  // Server-side allocations data
  const allocations = allocationsData?.data || [];
  const totalAllocationsCount = allocationsData?.totalCount || 0;
  const totalAllocationAmount = allocationsData?.totalAmount || 0;
  const uniqueExecutors = allocationsData?.uniqueExecutors || 0;

  // Server-side mining rounds data
  const openRounds = miningRoundsData?.openRounds || [];
  const issuingRounds = miningRoundsData?.issuingRounds || [];
  const closedRounds = miningRoundsData?.closedRounds || [];

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
                  Updates
                </Badge>
              )}
            </div>
            <p className="text-muted-foreground">Track supply, allocations, and mining rounds from updates data</p>
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
                <p className="text-3xl font-bold text-primary">{totalAllocationsCount.toLocaleString()}</p>
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
                <p className="text-3xl font-bold text-primary">{uniqueExecutors}</p>
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
              {allocations.map((allocation: any, index: number) => {
                const itemKey = (currentPage - 1) * itemsPerPage + index;

                return (
                  <Card key={allocation.contract_id || index}>
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
                          <Badge variant="secondary">{allocation.amount ? `${allocation.amount.toFixed(4)} CC` : "N/A"}</Badge>
                        </CardHeader>
                      </CollapsibleTrigger>
                      <CardContent>
                        <div className="grid gap-2 text-sm">
                          <div className="flex justify-between">
                            <span className="text-muted-foreground">Executor:</span>
                            <span className="font-mono text-xs">{allocation.executor || "N/A"}</span>
                          </div>
                          <div className="flex justify-between">
                            <span className="text-muted-foreground">Sender:</span>
                            <span className="font-mono text-xs">{allocation.sender || "N/A"}</span>
                          </div>
                          <div className="flex justify-between">
                            <span className="text-muted-foreground">Receiver:</span>
                            <span className="font-mono text-xs">{allocation.receiver || "N/A"}</span>
                          </div>
                          <div className="flex justify-between">
                            <span className="text-muted-foreground">Transfer Leg ID:</span>
                            <span className="font-mono text-xs">{allocation.transfer_leg_id || "N/A"}</span>
                          </div>
                          {allocation.requested_at && (
                            <div className="flex justify-between">
                              <span className="text-muted-foreground">Requested At:</span>
                              <span className="text-xs">{new Date(allocation.requested_at).toLocaleString()}</span>
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
            totalItems={totalAllocationsCount}
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

          {/* Current Round Info - from LIVE scan API */}
          <Card className="glass-card">
            <div className="p-6">
              <h3 className="text-xl font-bold mb-4 flex items-center">
                <Clock className="h-5 w-5 mr-2 text-primary" />
                Current Round
                <Badge variant="outline" className="ml-2 text-xs">Live</Badge>
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

          {/* Open Rounds - from ACS snapshot */}
          <div>
            <h4 className="text-xl font-bold mb-4 flex items-center">
              <AlertCircle className="h-5 w-5 mr-2 text-warning" />
              Open Rounds ({miningRoundsData?.counts?.open || 0})
              <Badge variant="outline" className="ml-2 text-xs">ACS Snapshot</Badge>
            </h4>
            {miningRoundsLoading ? (
              <Skeleton className="h-48 w-full" />
            ) : openRounds.length === 0 ? (
              <Card className="glass-card p-6">
                <p className="text-muted-foreground text-center">No open rounds at the moment</p>
              </Card>
            ) : (
              <div className="space-y-4">
                {openRounds.map((round: any, idx: number) => {
                  const roundKey = `open-${idx}`;
                  return (
                    <Card key={round.contract_id || roundKey} className="glass-card">
                      <Collapsible
                        open={openItems[roundKey] || false}
                        onOpenChange={(isOpen) => setOpenItems((prev) => ({ ...prev, [roundKey]: isOpen }))}
                      >
                        <CollapsibleTrigger className="w-full">
                          <div className="p-6">
                            <div className="flex items-start justify-between mb-4">
                              <div className="flex items-center gap-2">
                                {openItems[roundKey] ? (
                                  <ChevronDown className="h-4 w-4" />
                                ) : (
                                  <ChevronRight className="h-4 w-4" />
                                )}
                                <div className="text-left">
                                  <h4 className="text-xl font-bold mb-1">Round {round.round_number}</h4>
                                  <p className="text-sm text-muted-foreground">
                                    Opens: {round.opens_at ? new Date(round.opens_at).toLocaleString() : 'N/A'}
                                  </p>
                                  <p className="text-sm text-muted-foreground">
                                    Target Close: {round.target_closes_at ? new Date(round.target_closes_at).toLocaleString() : 'N/A'}
                                  </p>
                                </div>
                              </div>
                              <Badge className="bg-warning/10 text-warning border-warning/20">
                                <Clock className="h-3 w-3 mr-1" />
                                open
                              </Badge>
                            </div>
                          </div>
                        </CollapsibleTrigger>
                        <CollapsibleContent>
                          <div className="px-6 pb-6 space-y-4">
                            <div className="p-4 rounded-lg bg-muted/30">
                              <p className="text-sm text-muted-foreground mb-1">Contract ID</p>
                              <p className="font-mono text-xs break-all">{round.contract_id}</p>
                            </div>
                            <div className="p-4 rounded-lg bg-muted/50">
                              <p className="text-xs font-semibold mb-2">Full Payload:</p>
                              <pre className="text-xs overflow-auto max-h-96 whitespace-pre-wrap">
                                {JSON.stringify(typeof round.payload === 'string' ? JSON.parse(round.payload) : round.payload, null, 2)}
                              </pre>
                            </div>
                          </div>
                        </CollapsibleContent>
                      </Collapsible>
                    </Card>
                  );
                })}
              </div>
            )}
          </div>

          {/* Issuing Rounds */}
          <div>
            <h4 className="text-xl font-bold mb-4 flex items-center">
              <Clock className="h-5 w-5 mr-2 text-primary" />
              Issuing Rounds ({miningRoundsData?.counts?.issuing || 0})
            </h4>
            {miningRoundsLoading ? (
              <Skeleton className="h-48 w-full" />
            ) : issuingRounds.length === 0 ? (
              <Card className="glass-card p-6">
                <p className="text-muted-foreground text-center">No issuing rounds at the moment</p>
              </Card>
            ) : (
              <div className="space-y-4">
                {issuingRounds.map((round: any, idx: number) => {
                  const roundKey = `issuing-${idx}`;
                  return (
                    <Card key={round.contract_id || roundKey} className="glass-card">
                      <Collapsible
                        open={openItems[roundKey] || false}
                        onOpenChange={(isOpen) => setOpenItems((prev) => ({ ...prev, [roundKey]: isOpen }))}
                      >
                        <CollapsibleTrigger className="w-full">
                          <div className="p-6">
                            <div className="flex items-start justify-between mb-4">
                              <div className="flex items-center gap-2">
                                {openItems[roundKey] ? (
                                  <ChevronDown className="h-4 w-4" />
                                ) : (
                                  <ChevronRight className="h-4 w-4" />
                                )}
                                <div className="text-left">
                                  <h4 className="text-xl font-bold mb-1">Round {round.round_number}</h4>
                                  <p className="text-sm text-muted-foreground">
                                    Opens: {round.opens_at ? new Date(round.opens_at).toLocaleString() : 'N/A'}
                                  </p>
                                </div>
                              </div>
                              <Badge className="bg-primary/10 text-primary border-primary/20">
                                <Clock className="h-3 w-3 mr-1" />
                                issuing
                              </Badge>
                            </div>
                          </div>
                        </CollapsibleTrigger>
                        <CollapsibleContent>
                          <div className="px-6 pb-6 space-y-4">
                            <div className="p-4 rounded-lg bg-muted/30">
                              <p className="text-sm text-muted-foreground mb-1">Contract ID</p>
                              <p className="font-mono text-xs break-all">{round.contract_id}</p>
                            </div>
                            <div className="p-4 rounded-lg bg-muted/50">
                              <p className="text-xs font-semibold mb-2">Full Payload:</p>
                              <pre className="text-xs overflow-auto max-h-96 whitespace-pre-wrap">
                                {JSON.stringify(typeof round.payload === 'string' ? JSON.parse(round.payload) : round.payload, null, 2)}
                              </pre>
                            </div>
                          </div>
                        </CollapsibleContent>
                      </Collapsible>
                    </Card>
                  );
                })}
              </div>
            )}
          </div>

          {/* Closed Rounds (Pending Archive) */}
          <div>
            <h4 className="text-xl font-bold mb-4 flex items-center">
              <CheckCircle className="h-5 w-5 mr-2 text-success" />
              Closed Rounds - Pending Archive ({miningRoundsData?.counts?.closed || 0})
            </h4>
            <p className="text-sm text-muted-foreground mb-4">
              These rounds are closed but still exist as active contracts awaiting archival. For historical round data, see the Round Statistics page.
            </p>
            {miningRoundsLoading ? (
              <Skeleton className="h-48 w-full" />
            ) : closedRounds.length === 0 ? (
              <Card className="glass-card p-6">
                <p className="text-muted-foreground text-center">No closed rounds pending archive</p>
              </Card>
            ) : (
              <div className="space-y-4">
                {closedRounds.map((round: any, idx: number) => {
                  const roundKey = `closed-${idx}`;
                  return (
                    <Card key={round.contract_id || roundKey} className="glass-card">
                      <Collapsible
                        open={openItems[roundKey] || false}
                        onOpenChange={(isOpen) => setOpenItems((prev) => ({ ...prev, [roundKey]: isOpen }))}
                      >
                        <CollapsibleTrigger className="w-full">
                          <div className="p-6">
                            <div className="flex items-start justify-between mb-4">
                              <div className="flex items-center gap-2">
                                {openItems[roundKey] ? (
                                  <ChevronDown className="h-4 w-4" />
                                ) : (
                                  <ChevronRight className="h-4 w-4" />
                                )}
                                <div className="text-left">
                                  <h4 className="text-xl font-bold mb-1">Round {round.round_number}</h4>
                                </div>
                              </div>
                              <Badge className="bg-success/10 text-success border-success/20">
                                <CheckCircle className="h-3 w-3 mr-1" />
                                closed
                              </Badge>
                            </div>
                          </div>
                        </CollapsibleTrigger>
                        <CollapsibleContent>
                          <div className="px-6 pb-6 space-y-4">
                            <div className="p-4 rounded-lg bg-muted/30">
                              <p className="text-sm text-muted-foreground mb-1">Contract ID</p>
                              <p className="font-mono text-xs break-all">{round.contract_id}</p>
                            </div>
                            <div className="p-4 rounded-lg bg-muted/50">
                              <p className="text-xs font-semibold mb-2">Full Payload:</p>
                              <pre className="text-xs overflow-auto max-h-96 whitespace-pre-wrap">
                                {JSON.stringify(typeof round.payload === 'string' ? JSON.parse(round.payload) : round.payload, null, 2)}
                              </pre>
                            </div>
                          </div>
                        </CollapsibleContent>
                      </Collapsible>
                    </Card>
                  );
                })}
              </div>
            )}
          </div>
        </div>

        <DataSourcesFooter
          snapshotId={undefined}
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
