import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  Coins,
  TrendingUp,
  RefreshCw,
  AlertCircle,
  CheckCircle,
  Clock,
  ChevronDown,
  ChevronRight,
  Layers,
} from "lucide-react";
import { useQueryClient, useQuery } from "@tanstack/react-query";
import { toast } from "sonner";
import { useState } from "react";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { scanApi } from "@/lib/api-client";

const Supply = () => {
  const queryClient = useQueryClient();
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [openItems, setOpenItems] = useState<Record<string | number, boolean>>({});

  const handleForceRefresh = async () => {
    try {
      setIsRefreshing(true);
      toast.info("Refreshing data...");
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

  // Fetch latest round from SCAN API
  const { data: latestRound, isLoading: latestRoundLoading } = useQuery({
    queryKey: ["latestRound"],
    queryFn: () => scanApi.fetchLatestRound(),
    staleTime: 30_000,
  });

  // Fetch total balance from SCAN API (derived from round totals)
  const { data: totalBalance, isLoading: balanceLoading } = useQuery({
    queryKey: ["totalBalance"],
    queryFn: () => scanApi.fetchTotalBalance(),
    staleTime: 30_000,
  });

  // Fetch round totals for recent rounds
  const { data: roundTotals, isLoading: roundTotalsLoading } = useQuery({
    queryKey: ["roundTotals", latestRound?.round],
    queryFn: async () => {
      if (!latestRound?.round) return null;
      const startRound = Math.max(1, latestRound.round - 10);
      return scanApi.fetchRoundTotals({ start_round: startRound, end_round: latestRound.round });
    },
    enabled: !!latestRound?.round,
    staleTime: 30_000,
  });

  // Fetch mining rounds from SCAN API
  const { data: miningRounds, isLoading: miningRoundsLoading } = useQuery({
    queryKey: ["allMiningRounds"],
    queryFn: () => scanApi.fetchAllMiningRoundsCurrent(),
    staleTime: 30_000,
  });

  // Fetch closed rounds from SCAN API
  const { data: closedRoundsData, isLoading: closedRoundsLoading } = useQuery({
    queryKey: ["closedRounds"],
    queryFn: () => scanApi.fetchClosedRounds(),
    staleTime: 30_000,
  });

  const isLoading = latestRoundLoading || balanceLoading || miningRoundsLoading;

  const totalSupply = parseFloat(totalBalance?.total_balance || "0");
  const openRounds = miningRounds?.open_rounds || [];
  const issuingRounds = miningRounds?.issuing_rounds || [];
  const closedRounds = closedRoundsData?.rounds?.slice(0, 10) || [];

  // Get latest round totals for additional stats
  const latestTotals = roundTotals?.entries?.[roundTotals.entries.length - 1];
  const cumulativeAppRewards = parseFloat(latestTotals?.cumulative_app_rewards || "0");
  const cumulativeValidatorRewards = parseFloat(latestTotals?.cumulative_validator_rewards || "0");

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
              <Badge variant="outline" className="bg-primary/10 text-primary border-primary/30">
                <Coins className="h-3 w-3 mr-1" />
                Live from Scan API
              </Badge>
            </div>
            <p className="text-muted-foreground">
              Track total supply, mining rounds, and network statistics
            </p>
          </div>
          <Button onClick={handleForceRefresh} variant="outline" size="sm" className="gap-2" disabled={isRefreshing}>
            <RefreshCw className={`h-4 w-4 ${isRefreshing ? "animate-spin" : ""}`} />
            {isRefreshing ? "Refreshing..." : "Force Refresh"}
          </Button>
        </div>

        {/* Supply Stats from SCAN API */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <Card className="glass-card p-6">
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-sm font-medium text-muted-foreground">Total Amulet Balance</h3>
              <Coins className="h-5 w-5 text-primary" />
            </div>
            {isLoading ? (
              <Skeleton className="h-10 w-full" />
            ) : (
              <>
                <p className="text-3xl font-bold text-primary mb-1">{formatAmount(totalSupply)}</p>
                <p className="text-xs text-muted-foreground">CC (from round {latestRound?.round})</p>
              </>
            )}
          </Card>

          <Card className="glass-card p-6">
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-sm font-medium text-muted-foreground">Current Round</h3>
              <Layers className="h-5 w-5 text-primary" />
            </div>
            {latestRoundLoading ? (
              <Skeleton className="h-10 w-full" />
            ) : (
              <>
                <p className="text-3xl font-bold text-primary mb-1">{latestRound?.round?.toLocaleString() || "—"}</p>
                <p className="text-xs text-muted-foreground">
                  {latestRound?.effectiveAt ? new Date(latestRound.effectiveAt).toLocaleString() : "—"}
                </p>
              </>
            )}
          </Card>

          <Card className="glass-card p-6">
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-sm font-medium text-muted-foreground">Cumulative App Rewards</h3>
              <TrendingUp className="h-5 w-5 text-chart-2" />
            </div>
            {roundTotalsLoading ? (
              <Skeleton className="h-10 w-full" />
            ) : (
              <>
                <p className="text-3xl font-bold text-chart-2 mb-1">{formatAmount(cumulativeAppRewards)}</p>
                <p className="text-xs text-muted-foreground">CC total distributed</p>
              </>
            )}
          </Card>

          <Card className="glass-card p-6">
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-sm font-medium text-muted-foreground">Cumulative Validator Rewards</h3>
              <TrendingUp className="h-5 w-5 text-chart-3" />
            </div>
            {roundTotalsLoading ? (
              <Skeleton className="h-10 w-full" />
            ) : (
              <>
                <p className="text-3xl font-bold text-chart-3 mb-1">{formatAmount(cumulativeValidatorRewards)}</p>
                <p className="text-xs text-muted-foreground">CC total distributed</p>
              </>
            )}
          </Card>
        </div>

        {/* Mining Rounds Section */}
        <div className="space-y-6">
          <div>
            <h3 className="text-2xl font-bold mb-2">Mining Rounds</h3>
            <p className="text-muted-foreground">Track open, issuing, and closed mining rounds from the Scan API</p>
          </div>

          {/* Open Rounds */}
          <div>
            <h4 className="text-xl font-bold mb-4 flex items-center">
              <AlertCircle className="h-5 w-5 mr-2 text-warning" />
              Open Rounds ({openRounds.length})
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
                                    Opened: {round.opened_at ? new Date(round.opened_at).toLocaleString() : 'N/A'}
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
                            {round.payload && (
                              <div className="p-4 rounded-lg bg-muted/50">
                                <p className="text-xs font-semibold mb-2">Payload:</p>
                                <pre className="text-xs overflow-auto max-h-96 whitespace-pre-wrap">
                                  {JSON.stringify(round.payload, null, 2)}
                                </pre>
                              </div>
                            )}
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
              Issuing Rounds ({issuingRounds.length})
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
                                    Issued: {round.issued_at ? new Date(round.issued_at).toLocaleString() : 'N/A'}
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
                            {round.payload && (
                              <div className="p-4 rounded-lg bg-muted/50">
                                <p className="text-xs font-semibold mb-2">Payload:</p>
                                <pre className="text-xs overflow-auto max-h-96 whitespace-pre-wrap">
                                  {JSON.stringify(round.payload, null, 2)}
                                </pre>
                              </div>
                            )}
                          </div>
                        </CollapsibleContent>
                      </Collapsible>
                    </Card>
                  );
                })}
              </div>
            )}
          </div>

          {/* Closed Rounds */}
          <div>
            <h4 className="text-xl font-bold mb-4 flex items-center">
              <CheckCircle className="h-5 w-5 mr-2 text-chart-2" />
              Recently Closed Rounds ({closedRounds.length})
            </h4>
            {closedRoundsLoading ? (
              <Skeleton className="h-48 w-full" />
            ) : closedRounds.length === 0 ? (
              <Card className="glass-card p-6">
                <p className="text-muted-foreground text-center">No closed rounds available</p>
              </Card>
            ) : (
              <div className="space-y-4">
                {closedRounds.map((item: any, idx: number) => {
                  const round = item.contract || item;
                  const roundKey = `closed-${idx}`;
                  const roundNumber = round.payload?.round?.number;
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
                                  <h4 className="text-xl font-bold mb-1">Round {roundNumber || idx + 1}</h4>
                                  <p className="text-sm text-muted-foreground">
                                    Closed: {round.created_at ? new Date(round.created_at).toLocaleString() : 'N/A'}
                                  </p>
                                </div>
                              </div>
                              <Badge className="bg-chart-2/10 text-chart-2 border-chart-2/20">
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
                            {round.payload && (
                              <div className="p-4 rounded-lg bg-muted/50">
                                <p className="text-xs font-semibold mb-2">Payload:</p>
                                <pre className="text-xs overflow-auto max-h-96 whitespace-pre-wrap">
                                  {JSON.stringify(round.payload, null, 2)}
                                </pre>
                              </div>
                            )}
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

        {/* Round Totals History */}
        {roundTotals?.entries && roundTotals.entries.length > 0 && (
          <div className="space-y-4">
            <h3 className="text-2xl font-bold">Recent Round Statistics</h3>
            <p className="text-muted-foreground">Per-round rewards and balance changes</p>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border">
                    <th className="text-left p-3 font-medium">Round</th>
                    <th className="text-left p-3 font-medium">Closed At</th>
                    <th className="text-right p-3 font-medium">App Rewards</th>
                    <th className="text-right p-3 font-medium">Validator Rewards</th>
                    <th className="text-right p-3 font-medium">Total Balance</th>
                  </tr>
                </thead>
                <tbody>
                  {roundTotals.entries.slice().reverse().map((entry: any) => (
                    <tr key={entry.closed_round} className="border-b border-border/50 hover:bg-muted/30">
                      <td className="p-3 font-mono">{entry.closed_round}</td>
                      <td className="p-3 text-muted-foreground">
                        {new Date(entry.closed_round_effective_at).toLocaleString()}
                      </td>
                      <td className="p-3 text-right font-mono">
                        {parseFloat(entry.app_rewards).toFixed(4)}
                      </td>
                      <td className="p-3 text-right font-mono">
                        {parseFloat(entry.validator_rewards).toFixed(4)}
                      </td>
                      <td className="p-3 text-right font-mono">
                        {parseFloat(entry.total_amulet_balance).toLocaleString(undefined, { maximumFractionDigits: 2 })}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </div>
    </DashboardLayout>
  );
};

export default Supply;
