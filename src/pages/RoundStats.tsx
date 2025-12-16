import { DashboardLayout } from "@/components/DashboardLayout";
import { Card } from "@/components/ui/card";
import { TrendingUp, TrendingDown, Database, Activity, Clock, CheckCircle } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { Skeleton } from "@/components/ui/skeleton";
import { Badge } from "@/components/ui/badge";
import { getACSMiningRounds, isApiAvailable } from "@/lib/duckdb-api-client";
import { DataSourcesFooter } from "@/components/DataSourcesFooter";
import { useLatestACSSnapshot } from "@/hooks/use-acs-snapshots";

const StatCard = ({
  label,
  value,
  color,
  isChange,
}: {
  label: string;
  value: string;
  color?: string;
  isChange?: boolean;
}) => {
  const parsed = parseFloat(value);
  const isPositive = parsed >= 0;

  const dynamicColor = isChange && !color ? (isPositive ? "text-success" : "text-destructive") : (color ?? "");

  return (
    <div className="p-4 rounded-lg bg-muted/30">
      <p className="text-sm text-muted-foreground mb-1">{label}</p>
      <p className={`text-xl font-bold ${dynamicColor}`}>
        {isNaN(parsed)
          ? value
          : `${parsed.toLocaleString(undefined, {
              maximumFractionDigits: 6,
            })} ${!label.toLowerCase().includes("rate") ? "CC" : ""}`}
      </p>
    </div>
  );
};

const RoundStats = () => {
  const { data: snapshot } = useLatestACSSnapshot();

  // Fetch mining rounds from local ACS - longer cache for instant page loads
  const { data: miningRoundsData, isLoading, isFetching } = useQuery({
    queryKey: ["localMiningRounds"],
    queryFn: async () => {
      const available = await isApiAvailable();
      if (!available) return null;
      return getACSMiningRounds({ closedLimit: 20 });
    },
    staleTime: 5 * 60 * 1000, // 5 minutes
    gcTime: 30 * 60 * 1000, // Keep in cache
    refetchOnWindowFocus: false,
  });

  const openRounds = miningRoundsData?.openRounds || [];
  const issuingRounds = miningRoundsData?.issuingRounds || [];
  const closedRounds = miningRoundsData?.closedRounds || [];

  const formatAmount = (val: any) => {
    const num = parseFloat(val || "0");
    return num.toLocaleString(undefined, { maximumFractionDigits: 6 });
  };

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <div className="flex items-center gap-2 mb-2">
            <Activity className="h-8 w-8 text-primary" />
            <h2 className="text-3xl font-bold">Round Statistics</h2>
            <Badge variant="outline" className="bg-green-500/10 text-green-500 border-green-500/30">
              <Database className="h-3 w-3 mr-1" />
              Local ACS
            </Badge>
          </div>
          <p className="text-muted-foreground">Mining rounds from the Active Contract Set</p>
        </div>

        {/* Summary Cards */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <Card className="p-6">
            <div className="flex items-center justify-between mb-2">
              <h3 className="text-sm font-medium text-muted-foreground">Open Rounds</h3>
              <Clock className="h-5 w-5 text-warning" />
            </div>
            {isLoading ? (
              <Skeleton className="h-10 w-full" />
            ) : (
              <p className="text-3xl font-bold text-warning">{openRounds.length}</p>
            )}
          </Card>

          <Card className="p-6">
            <div className="flex items-center justify-between mb-2">
              <h3 className="text-sm font-medium text-muted-foreground">Issuing Rounds</h3>
              <TrendingUp className="h-5 w-5 text-primary" />
            </div>
            {isLoading ? (
              <Skeleton className="h-10 w-full" />
            ) : (
              <p className="text-3xl font-bold text-primary">{issuingRounds.length}</p>
            )}
          </Card>

          <Card className="p-6">
            <div className="flex items-center justify-between mb-2">
              <h3 className="text-sm font-medium text-muted-foreground">Closed Rounds</h3>
              <CheckCircle className="h-5 w-5 text-success" />
            </div>
            {isLoading ? (
              <Skeleton className="h-10 w-full" />
            ) : (
              <p className="text-3xl font-bold text-success">{closedRounds.length}</p>
            )}
          </Card>
        </div>

        {isLoading ? (
          <div className="space-y-4">
            {[1, 2, 3].map((i) => (
              <Skeleton key={i} className="h-64 w-full" />
            ))}
          </div>
        ) : !miningRoundsData ? (
          <Card className="glass-card p-6">
            <p className="text-muted-foreground text-center">
              No mining round data available. Ensure DuckDB server is running.
            </p>
          </Card>
        ) : (
          <div className="space-y-6">
            {/* Open Mining Rounds */}
            {openRounds.length > 0 && (
              <div>
                <h3 className="text-xl font-semibold mb-4 flex items-center gap-2">
                  <Clock className="h-5 w-5 text-warning" />
                  Open Mining Rounds
                </h3>
                <div className="grid gap-4">
                  {openRounds.map((round: any) => (
                    <Card key={round.round_number} className="p-6">
                      <div className="flex items-center justify-between mb-4">
                        <h4 className="text-2xl font-bold">Round {round.round_number}</h4>
                        <Badge variant="outline" className="bg-warning/10 text-warning border-warning/30">
                          Open
                        </Badge>
                      </div>
                      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                        <StatCard label="Amulet Per Year" value={formatAmount(round.amulet_to_issue_per_year)} color="text-primary" />
                        <StatCard label="Opens At" value={round.opens_at ? new Date(round.opens_at).toLocaleString() : "N/A"} />
                        <StatCard label="Target Closes At" value={round.target_closes_at ? new Date(round.target_closes_at).toLocaleString() : "N/A"} />
                        <StatCard label="Migration ID" value={String(round.migration_id || "N/A")} />
                      </div>
                    </Card>
                  ))}
                </div>
              </div>
            )}

            {/* Issuing Rounds */}
            {issuingRounds.length > 0 && (
              <div>
                <h3 className="text-xl font-semibold mb-4 flex items-center gap-2">
                  <TrendingUp className="h-5 w-5 text-primary" />
                  Issuing Rounds
                </h3>
                <div className="grid gap-4">
                  {issuingRounds.map((round: any) => (
                    <Card key={round.round_number} className="p-6">
                      <div className="flex items-center justify-between mb-4">
                        <h4 className="text-2xl font-bold">Round {round.round_number}</h4>
                        <Badge variant="outline" className="bg-primary/10 text-primary border-primary/30">
                          Issuing
                        </Badge>
                      </div>
                      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                        <StatCard label="Amulet Per Year" value={formatAmount(round.amulet_to_issue_per_year)} color="text-primary" />
                        <StatCard label="Opens At" value={round.opens_at ? new Date(round.opens_at).toLocaleString() : "N/A"} />
                        <StatCard label="Target Closes At" value={round.target_closes_at ? new Date(round.target_closes_at).toLocaleString() : "N/A"} />
                        <StatCard label="Migration ID" value={String(round.migration_id || "N/A")} />
                      </div>
                    </Card>
                  ))}
                </div>
              </div>
            )}

            {/* Closed Rounds */}
            {closedRounds.length > 0 && (
              <div>
                <h3 className="text-xl font-semibold mb-4 flex items-center gap-2">
                  <CheckCircle className="h-5 w-5 text-success" />
                  Closed Rounds (Recent {closedRounds.length})
                </h3>
                <div className="space-y-4">
                  {closedRounds.map((round: any) => (
                    <Card key={round.round_number} className="glass-card">
                      <div className="p-6">
                        <div className="flex items-center justify-between mb-6">
                          <div>
                            <h3 className="text-2xl font-bold">Round {round.round_number}</h3>
                            <p className="text-sm text-muted-foreground">
                              Closed: {round.target_closes_at ? new Date(round.target_closes_at).toLocaleString() : "N/A"}
                            </p>
                          </div>
                          <Badge variant="outline" className="bg-success/10 text-success border-success/30">
                            Closed
                          </Badge>
                        </div>

                        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                          <StatCard label="Amulet Per Year" value={formatAmount(round.amulet_to_issue_per_year)} color="text-primary" />
                          <StatCard label="Opens At" value={round.opens_at ? new Date(round.opens_at).toLocaleString() : "N/A"} />
                          <StatCard label="Target Closes At" value={round.target_closes_at ? new Date(round.target_closes_at).toLocaleString() : "N/A"} />
                          <StatCard label="Migration ID" value={String(round.migration_id || "N/A")} />
                        </div>
                      </div>
                    </Card>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}

        <DataSourcesFooter
          snapshotId={snapshot?.id}
          templateSuffixes={[
            "Splice:Round:OpenMiningRound",
            "Splice:Round:IssuingMiningRound", 
            "Splice:Round:ClosedMiningRound"
          ]}
          isProcessing={false}
        />
      </div>
    </DashboardLayout>
  );
};

export default RoundStats;