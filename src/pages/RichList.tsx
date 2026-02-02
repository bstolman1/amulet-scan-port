import { DashboardLayout } from "@/components/DashboardLayout";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Wallet, Coins, Clock, RefreshCw } from "lucide-react";
import { Skeleton } from "@/components/ui/skeleton";
import { DataSourcesFooter } from "@/components/DataSourcesFooter";
import { useState, useMemo } from "react";
import { Input } from "@/components/ui/input";
import { ACSStatusBanner } from "@/components/ACSStatusBanner";
import { useRichList } from "@/hooks/use-rich-list";
import { Button } from "@/components/ui/button";
import { useQueryClient } from "@tanstack/react-query";

const RichList = () => {
  const [searchTerm, setSearchTerm] = useState("");
  const queryClient = useQueryClient();

  // Use Scan API rich list
  const { data: richListData, isLoading, error, isFetching } = useRichList(100);

  // Filter by search term client-side
  const filteredHolders = useMemo(() => {
    if (!richListData?.data) return [];
    if (!searchTerm) return richListData.data;
    const lower = searchTerm.toLowerCase();
    return richListData.data.filter((h) => h.owner.toLowerCase().includes(lower));
  }, [richListData?.data, searchTerm]);

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

  const handleRefresh = () => {
    queryClient.invalidateQueries({ queryKey: ["scan-api", "rich-list"] });
  };

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <ACSStatusBanner />
        <div>
          <div className="flex items-center gap-2 mb-2">
            <h2 className="text-3xl font-bold">Rich List</h2>
            <Badge variant="outline" className="bg-success/10 text-success border-success/30">
              <Clock className="h-3 w-3 mr-1" />
              Live API
            </Badge>
            <Button
              variant="ghost"
              size="sm"
              onClick={handleRefresh}
              disabled={isFetching}
              className="ml-2"
            >
              <RefreshCw className={`h-4 w-4 ${isFetching ? "animate-spin" : ""}`} />
            </Button>
          </div>
          <p className="text-muted-foreground">
            Top CC holders and balance distribution
            {richListData?.recordTime && (
              <span className="text-xs ml-2">
                (As of round {richListData.round.toLocaleString()} - {new Date(richListData.recordTime).toLocaleString()})
              </span>
            )}
          </p>
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
                <p className="text-xs text-muted-foreground">Unique holders (from holdings state)</p>
              </>
            )}
          </Card>

          <Card className="glass-card p-6">
            <div className="flex items-center justify-between mb-2">
              <h3 className="text-sm font-medium text-muted-foreground">Total Balance (Top 100)</h3>
              <Coins className="h-5 w-5 text-success" />
            </div>
            {isLoading ? (
              <Skeleton className="h-10 w-full" />
            ) : (
              <>
                <p className="text-3xl font-bold text-success mb-1">{formatAmount(totalSupply)}</p>
                <p className="text-xs text-muted-foreground">CC (after holding fees)</p>
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
              The /v0/holdings/state endpoint may not be available on this SV
            </p>
          </Card>
        )}

        {/* Top Holders Table */}
        <Card className="glass-card">
          <div className="p-6">
            <h3 className="text-xl font-bold mb-4">Top 100 Holders</h3>
            {isLoading ? (
              <div className="space-y-3">
                {[1, 2, 3, 4, 5].map((i) => (
                  <Skeleton key={i} className="h-32 w-full" />
                ))}
              </div>
            ) : filteredHolders.length === 0 ? (
              <p className="text-muted-foreground text-center py-8">
                {searchTerm ? "No holders match your search" : "No holders found"}
              </p>
            ) : (
              <div className="space-y-3">
                {filteredHolders.map((holder, index) => (
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
                        <p className="text-xs text-muted-foreground mb-1">% of Top 100</p>
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
