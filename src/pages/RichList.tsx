import { DashboardLayout } from "@/components/DashboardLayout";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Wallet, Coins, Database } from "lucide-react";
import { Skeleton } from "@/components/ui/skeleton";
import { DataSourcesFooter } from "@/components/DataSourcesFooter";
import { useState } from "react";
import { Input } from "@/components/ui/input";
import { useLocalACSAvailable } from "@/hooks/use-local-acs";
import { useQuery } from "@tanstack/react-query";
import { getACSRichList, isApiAvailable } from "@/lib/duckdb-api-client";

const RichList = () => {
  const [searchTerm, setSearchTerm] = useState("");
  const { data: localAcsAvailable } = useLocalACSAvailable();

  // Use server-side aggregated rich list endpoint
  const { data: richListData, isLoading, error } = useQuery({
    queryKey: ["acs-rich-list", searchTerm],
    queryFn: async () => {
      const available = await isApiAvailable();
      if (!available) {
        throw new Error("DuckDB API not available");
      }
      return getACSRichList({ limit: 100, search: searchTerm || undefined });
    },
    staleTime: 60_000,
    enabled: true,
  });

  const topHolders = richListData?.data || [];
  const totalSupply = richListData?.totalSupply || 0;
  const holderCount = richListData?.holderCount || 0;

  const formatAmount = (amount: number) => {
    return amount.toLocaleString(undefined, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
  };

  const formatParty = (party: string) => {
    if (!party) return "Unknown";
    const parts = party.split("::");
    return parts[0]?.substring(0, 30) || party.substring(0, 30);
  };

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <div className="flex items-center gap-2 mb-2">
            <h2 className="text-3xl font-bold">Rich List</h2>
            {localAcsAvailable && (
              <Badge variant="outline" className="bg-green-500/10 text-green-500 border-green-500/30">
                <Database className="h-3 w-3 mr-1" />
                Updates
              </Badge>
            )}
          </div>
          <p className="text-muted-foreground">Top CC holders and balance distribution</p>
        </div>

        {/* Summary Cards */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <Card className="glass-card p-6">
            <div className="flex items-center justify-between mb-2">
              <h3 className="text-sm font-medium text-muted-foreground">Total Holders</h3>
              <Wallet className="h-5 w-5 text-primary" />
            </div>
            {isLoading ? (
              <Skeleton className="h-10 w-full" />
            ) : (
              <>
                <p className="text-3xl font-bold text-primary mb-1">{holderCount.toLocaleString()}</p>
                <p className="text-xs text-muted-foreground">Unique holders</p>
              </>
            )}
          </Card>

          <Card className="glass-card p-6">
            <div className="flex items-center justify-between mb-2">
              <h3 className="text-sm font-medium text-muted-foreground">Total Balance</h3>
              <Coins className="h-5 w-5 text-success" />
            </div>
            {isLoading ? (
              <Skeleton className="h-10 w-full" />
            ) : (
              <>
                <p className="text-3xl font-bold text-success mb-1">{formatAmount(totalSupply)}</p>
                <p className="text-xs text-muted-foreground">CC</p>
              </>
            )}
          </Card>
        </div>

        {/* Search */}
        <div className="flex gap-4">
          <Input
            placeholder="Search by party ID..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            className="max-w-md"
          />
        </div>

        {/* Error State */}
        {error && (
          <Card className="glass-card p-6 border-destructive/50">
            <p className="text-destructive">
              {error instanceof Error ? error.message : "Failed to load rich list data"}
            </p>
            <p className="text-xs text-muted-foreground mt-2">
              Make sure the local DuckDB server is running at localhost:3001
            </p>
          </Card>
        )}

        {/* Top Holders Table */}
        <Card className="glass-card">
          <div className="p-6">
            <h3 className="text-xl font-bold mb-4">Top 100 Holders</h3>
            {isLoading ? (
              <Skeleton className="h-96 w-full" />
            ) : topHolders.length === 0 ? (
              <p className="text-muted-foreground text-center py-8">No holders found</p>
            ) : (
              <div className="space-y-3">
                {topHolders.map((holder, index) => (
                  <Card key={holder.owner} className="p-4 hover:shadow-md transition-smooth">
                    <div className="flex items-start justify-between mb-3">
                      <div className="flex items-center space-x-3">
                        <div className="text-center min-w-[40px]">
                          <Badge variant="outline" className="text-lg font-bold">
                            #{index + 1}
                          </Badge>
                        </div>
                        <div className="flex-1">
                          <p className="text-sm text-muted-foreground mb-1">Party ID</p>
                          <p className="font-mono text-xs break-all">{holder.owner}</p>
                          <p className="text-xs text-muted-foreground mt-1">
                            {formatParty(holder.owner)}
                          </p>
                        </div>
                      </div>
                    </div>

                    <div className="grid grid-cols-1 md:grid-cols-4 gap-3">
                      <div className="p-3 rounded-lg bg-success/10 border border-success/20">
                        <p className="text-xs text-muted-foreground mb-1">Unlocked Balance</p>
                        <p className="text-lg font-bold text-success">{formatAmount(holder.amount)} CC</p>
                      </div>
                      
                      <div className="p-3 rounded-lg bg-warning/10 border border-warning/20">
                        <p className="text-xs text-muted-foreground mb-1">Locked Balance</p>
                        <p className="text-lg font-bold text-warning">{formatAmount(holder.locked)} CC</p>
                      </div>
                      
                      <div className="p-3 rounded-lg bg-primary/10 border border-primary/20">
                        <p className="text-xs text-muted-foreground mb-1">Total Balance</p>
                        <p className="text-xl font-bold text-primary">{formatAmount(holder.total)} CC</p>
                      </div>
                      
                      <div className="p-3 rounded-lg bg-muted/30">
                        <p className="text-xs text-muted-foreground mb-1">% of Total Supply</p>
                        <p className="text-lg font-bold">
                          {totalSupply > 0 ? ((holder.total / totalSupply) * 100).toFixed(4) : 0}%
                        </p>
                      </div>
                    </div>

                    <div className="mt-3 pt-3 border-t border-border/30">
                      <div className="grid grid-cols-2 gap-2 text-xs">
                        <div>
                          <span className="text-muted-foreground">Unlocked %:</span>
                          <span className="ml-2 font-semibold">
                            {holder.total > 0 ? ((holder.amount / holder.total) * 100).toFixed(1) : 0}%
                          </span>
                        </div>
                        <div>
                          <span className="text-muted-foreground">Locked %:</span>
                          <span className="ml-2 font-semibold">
                            {holder.total > 0 ? ((holder.locked / holder.total) * 100).toFixed(1) : 0}%
                          </span>
                        </div>
                      </div>
                    </div>
                  </Card>
                ))}
              </div>
            )}
          </div>
        </Card>

        <DataSourcesFooter
          snapshotId={undefined}
          templateSuffixes={["Splice:Amulet:Amulet", "Splice:Amulet:LockedAmulet"]}
          isProcessing={false}
        />
      </div>
    </DashboardLayout>
  );
};

export default RichList;