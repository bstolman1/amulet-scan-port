import { DashboardLayout } from "@/components/DashboardLayout";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Wallet, Coins, Database } from "lucide-react";
import { Skeleton } from "@/components/ui/skeleton";
import { useLatestACSSnapshot } from "@/hooks/use-acs-snapshots";
import { useAggregatedTemplateData } from "@/hooks/use-aggregated-template-data";
import { DataSourcesFooter } from "@/components/DataSourcesFooter";
import { useState } from "react";
import { Input } from "@/components/ui/input";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { useLocalACSAvailable } from "@/hooks/use-local-acs";
import { toCC } from "@/lib/amount-utils";

interface HolderBalance {
  owner: string;
  amount: number;
  locked: number;
  total: number;
}

const RichList = () => {
  const [searchTerm, setSearchTerm] = useState("");
  const { data: localAcsAvailable } = useLocalACSAvailable();

  const { data: snapshot } = useLatestACSSnapshot();

  // Fetch Amulet contracts - aggregated across ALL packages
  const { data: amuletData, isLoading: amuletLoading } = useAggregatedTemplateData(
    snapshot?.id,
    "Splice:Amulet:Amulet",
    !!snapshot,
  );

  // Fetch LockedAmulet contracts - aggregated across ALL packages
  const { data: lockedData, isLoading: lockedLoading } = useAggregatedTemplateData(
    snapshot?.id,
    "Splice:Amulet:LockedAmulet",
    !!snapshot,
  );

  const isLoading = amuletLoading || lockedLoading;

  // Aggregate balances by owner
  const holderBalances: HolderBalance[] = (() => {
    const balanceMap = new Map<string, HolderBalance>();

    // Process regular amulets from all packages
    (amuletData?.data || []).forEach((amulet: any) => {
      const owner = amulet.owner;
      const amount = toCC(amulet.amount?.initialAmount || "0");

      if (!balanceMap.has(owner)) {
        balanceMap.set(owner, { owner, amount: 0, locked: 0, total: 0 });
      }
      const holder = balanceMap.get(owner)!;
      holder.amount += amount;
      holder.total += amount;
    });

    // Process locked amulets from all packages
    (lockedData?.data || []).forEach((locked: any) => {
      const owner = locked.amulet?.owner || locked.owner;
      const amount = toCC(locked.amulet?.amount?.initialAmount || locked.amount?.initialAmount || "0");

      if (!balanceMap.has(owner)) {
        balanceMap.set(owner, { owner, amount: 0, locked: 0, total: 0 });
      }
      const holder = balanceMap.get(owner)!;
      holder.locked += amount;
      holder.total += amount;
    });

    return Array.from(balanceMap.values())
      .sort((a, b) => b.total - a.total)
      .filter((h) => {
        if (!searchTerm) return true;
        return h.owner.toLowerCase().includes(searchTerm.toLowerCase());
      });
  })();

  const topHolders = holderBalances.slice(0, 100);
  const totalSupply = holderBalances.reduce((sum, h) => sum + h.total, 0);

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
                Local ACS
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
                <p className="text-3xl font-bold text-primary mb-1">{holderBalances.length.toLocaleString()}</p>
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
                        <p className="text-lg font-bold">{((holder.total / totalSupply) * 100).toFixed(4)}%</p>
                      </div>
                    </div>

                    <div className="mt-3 pt-3 border-t border-border/30">
                      <div className="grid grid-cols-2 gap-2 text-xs">
                        <div>
                          <span className="text-muted-foreground">Unlocked %:</span>
                          <span className="ml-2 font-semibold">{holder.total > 0 ? ((holder.amount / holder.total) * 100).toFixed(1) : 0}%</span>
                        </div>
                        <div>
                          <span className="text-muted-foreground">Locked %:</span>
                          <span className="ml-2 font-semibold">{holder.total > 0 ? ((holder.locked / holder.total) * 100).toFixed(1) : 0}%</span>
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
          snapshotId={snapshot?.id}
          templateSuffixes={["Splice:Amulet:Amulet", "Splice:Amulet:LockedAmulet"]}
          isProcessing={false}
        />
      </div>
    </DashboardLayout>
  );
};

export default RichList;
