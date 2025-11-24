import { DashboardLayout } from "@/components/DashboardLayout";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Wallet, Coins } from "lucide-react";
import { Skeleton } from "@/components/ui/skeleton";
import { useLatestACSSnapshot } from "@/hooks/use-acs-snapshots";
import { useAggregatedTemplateData } from "@/hooks/use-aggregated-template-data";
import { DataSourcesFooter } from "@/components/DataSourcesFooter";
import { useState } from "react";
import { Input } from "@/components/ui/input";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";

interface HolderBalance {
  owner: string;
  amount: number;
  locked: number;
  total: number;
}

const RichList = () => {
  const [searchTerm, setSearchTerm] = useState("");

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
      const amount = parseFloat(amulet.amount?.initialAmount || "0");

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
      const amount = parseFloat(locked.amulet?.amount?.initialAmount || locked.amount?.initialAmount || "0");

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
          <h2 className="text-3xl font-bold mb-2">Rich List</h2>
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
              <div className="overflow-x-auto">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead className="w-16">#</TableHead>
                      <TableHead>Holder</TableHead>
                      <TableHead className="text-right">Unlocked</TableHead>
                      <TableHead className="text-right">Locked</TableHead>
                      <TableHead className="text-right">Total Balance</TableHead>
                      <TableHead className="text-right">% of Supply</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {topHolders.map((holder, index) => (
                      <TableRow key={holder.owner}>
                        <TableCell className="font-medium">{index + 1}</TableCell>
                        <TableCell>
                          <Badge variant="outline" className="font-mono text-xs">
                            {formatParty(holder.owner)}
                          </Badge>
                        </TableCell>
                        <TableCell className="text-right">{formatAmount(holder.amount)}</TableCell>
                        <TableCell className="text-right text-warning">{formatAmount(holder.locked)}</TableCell>
                        <TableCell className="text-right font-semibold">{formatAmount(holder.total)}</TableCell>
                        <TableCell className="text-right text-muted-foreground">
                          {((holder.total / totalSupply) * 100).toFixed(2)}%
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
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
