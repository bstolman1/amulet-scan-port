import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { useQuery } from "@tanstack/react-query";
import { scanApi } from "@/lib/api-client";
import { TrendingUp, DollarSign, Clock, User } from "lucide-react";

export default function PriceVotes() {
  const { data: priceVotes, isLoading } = useQuery({
    queryKey: ["amuletPriceVotes"],
    queryFn: () => scanApi.fetchAmuletPriceVotes(),
    staleTime: 30_000,
  });

  const { data: dsoInfo } = useQuery({
    queryKey: ["dsoInfo"],
    queryFn: () => scanApi.fetchDsoInfo(),
  });

  const votes = priceVotes?.amulet_price_votes || [];

  // Calculate average price from votes
  const avgPrice = votes.length > 0
    ? votes.reduce((sum, v) => {
        const price = parseFloat(v.payload?.amuletPrice || "0");
        return sum + price;
      }, 0) / votes.length
    : 0;

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <h1 className="text-3xl font-bold">Amulet Price Votes</h1>
          <p className="text-muted-foreground">
            Current price votes from Super Validators for the Amulet token
          </p>
        </div>

        {/* Summary Cards */}
        <div className="grid gap-6 md:grid-cols-3">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <TrendingUp className="h-5 w-5" />
                Total Votes
              </CardTitle>
            </CardHeader>
            <CardContent>
              {isLoading ? (
                <Skeleton className="h-8 w-16" />
              ) : (
                <p className="text-3xl font-bold">{votes.length}</p>
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <DollarSign className="h-5 w-5" />
                Average Price
              </CardTitle>
            </CardHeader>
            <CardContent>
              {isLoading ? (
                <Skeleton className="h-8 w-24" />
              ) : (
                <p className="text-3xl font-bold">
                  ${avgPrice.toFixed(6)}
                </p>
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <User className="h-5 w-5" />
                Voting SVs
              </CardTitle>
            </CardHeader>
            <CardContent>
              {isLoading ? (
                <Skeleton className="h-8 w-16" />
              ) : (
                <p className="text-3xl font-bold">
                  {new Set(votes.map(v => v.payload?.sv)).size}
                </p>
              )}
            </CardContent>
          </Card>
        </div>

        {/* Votes Table */}
        <Card>
          <CardHeader>
            <CardTitle>Price Votes</CardTitle>
            <CardDescription>Individual price votes from each Super Validator</CardDescription>
          </CardHeader>
          <CardContent>
            {isLoading ? (
              <div className="space-y-2">
                <Skeleton className="h-10 w-full" />
                <Skeleton className="h-10 w-full" />
                <Skeleton className="h-10 w-full" />
              </div>
            ) : votes.length > 0 ? (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Super Validator</TableHead>
                    <TableHead>Price (USD)</TableHead>
                    <TableHead>Contract ID</TableHead>
                    <TableHead>Created At</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {votes.map((vote, i) => {
                    const sv = vote.payload?.sv || "Unknown";
                    const price = parseFloat(vote.payload?.amuletPrice || "0");
                    const createdAt = vote.created_at 
                      ? new Date(vote.created_at).toLocaleString()
                      : "—";
                    
                    // Try to find SV name from DSO info
                    const svInfo = dsoInfo?.sv_node_states?.find(
                      s => s.contract?.payload?.sv === sv
                    );
                    const svName = svInfo?.contract?.payload?.name || sv.slice(0, 16) + "...";

                    return (
                      <TableRow key={i}>
                        <TableCell>
                          <div className="flex items-center gap-2">
                            <span className="font-medium">{svName}</span>
                            <Badge variant="outline" className="font-mono text-xs">
                              {sv.slice(0, 12)}...
                            </Badge>
                          </div>
                        </TableCell>
                        <TableCell>
                          <span className="font-mono font-medium">${price.toFixed(6)}</span>
                        </TableCell>
                        <TableCell>
                          <code className="text-xs text-muted-foreground">
                            {vote.contract_id?.slice(0, 16)}...
                          </code>
                        </TableCell>
                        <TableCell>
                          <div className="flex items-center gap-1 text-muted-foreground">
                            <Clock className="h-3 w-3" />
                            <span className="text-sm">{createdAt}</span>
                          </div>
                        </TableCell>
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
            ) : (
              <p className="text-muted-foreground text-center py-8">
                No price votes found
              </p>
            )}
          </CardContent>
        </Card>
      </div>
    </DashboardLayout>
  );
}
