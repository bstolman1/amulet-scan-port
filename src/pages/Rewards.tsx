import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { useQuery } from "@tanstack/react-query";
import { scanApi } from "@/lib/api-client";
import { Award, TrendingUp, Users, Coins, Trophy } from "lucide-react";

export default function Rewards() {
  const { data: latestRound } = useQuery({
    queryKey: ["latestRound"],
    queryFn: () => scanApi.fetchLatestRound(),
  });

  const { data: rewardsCollected, isLoading: loadingRewards } = useQuery({
    queryKey: ["rewardsCollected", latestRound?.round],
    queryFn: () => scanApi.fetchRewardsCollected(latestRound?.round),
    enabled: !!latestRound?.round,
  });

  const { data: topProviders, isLoading: loadingProviders } = useQuery({
    queryKey: ["topProviders"],
    queryFn: () => scanApi.fetchTopProviders(25),
  });

  const { data: topValidators, isLoading: loadingValidators } = useQuery({
    queryKey: ["topValidatorsByRewards", latestRound?.round],
    queryFn: () => scanApi.fetchTopValidatorsByRewards(latestRound!.round, 25),
    enabled: !!latestRound?.round,
  });

  const { data: topTraffic, isLoading: loadingTraffic } = useQuery({
    queryKey: ["topValidatorsByTraffic", latestRound?.round],
    queryFn: () => scanApi.fetchTopValidatorsByPurchasedTraffic(latestRound!.round, 25),
    enabled: !!latestRound?.round,
  });

  const totalRewards = parseFloat(rewardsCollected?.amount || "0");

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <h1 className="text-3xl font-bold">Rewards & Leaderboards</h1>
          <p className="text-muted-foreground">
            Track rewards collected and top performers across the network
          </p>
        </div>

        {/* Summary Cards */}
        <div className="grid gap-6 md:grid-cols-3">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Award className="h-5 w-5" />
                Total Rewards Collected
              </CardTitle>
            </CardHeader>
            <CardContent>
              {loadingRewards ? (
                <Skeleton className="h-8 w-32" />
              ) : (
                <div>
                  <p className="text-3xl font-bold">{totalRewards.toLocaleString(undefined, { maximumFractionDigits: 2 })}</p>
                  <p className="text-muted-foreground text-sm">CC through round {latestRound?.round}</p>
                </div>
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Users className="h-5 w-5" />
                Top App Providers
              </CardTitle>
            </CardHeader>
            <CardContent>
              {loadingProviders ? (
                <Skeleton className="h-8 w-16" />
              ) : (
                <p className="text-3xl font-bold">
                  {topProviders?.providersAndRewards?.length || 0}
                </p>
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <TrendingUp className="h-5 w-5" />
                Current Round
              </CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-3xl font-bold">{latestRound?.round || "—"}</p>
            </CardContent>
          </Card>
        </div>

        {/* Top App Providers */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Trophy className="h-5 w-5 text-yellow-500" />
              Top App Providers by Rewards
            </CardTitle>
            <CardDescription>App providers ranked by cumulative rewards earned</CardDescription>
          </CardHeader>
          <CardContent>
            {loadingProviders ? (
              <div className="space-y-2">
                <Skeleton className="h-10 w-full" />
                <Skeleton className="h-10 w-full" />
                <Skeleton className="h-10 w-full" />
              </div>
            ) : topProviders?.providersAndRewards?.length ? (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead className="w-12">Rank</TableHead>
                    <TableHead>Provider</TableHead>
                    <TableHead className="text-right">Rewards (CC)</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {topProviders.providersAndRewards.slice(0, 10).map((p, i) => (
                    <TableRow key={i}>
                      <TableCell>
                        <Badge variant={i < 3 ? "default" : "secondary"}>
                          #{i + 1}
                        </Badge>
                      </TableCell>
                      <TableCell>
                        <code className="text-xs">{p.provider.slice(0, 24)}...</code>
                      </TableCell>
                      <TableCell className="text-right font-mono">
                        {parseFloat(p.rewards).toLocaleString(undefined, { maximumFractionDigits: 2 })}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            ) : (
              <p className="text-muted-foreground text-center py-4">No app provider data</p>
            )}
          </CardContent>
        </Card>

        {/* Top Validators by Rewards */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Coins className="h-5 w-5 text-green-500" />
              Top Validators by Validator Rewards
            </CardTitle>
            <CardDescription>Validators ranked by cumulative validator rewards earned</CardDescription>
          </CardHeader>
          <CardContent>
            {loadingValidators ? (
              <div className="space-y-2">
                <Skeleton className="h-10 w-full" />
                <Skeleton className="h-10 w-full" />
                <Skeleton className="h-10 w-full" />
              </div>
            ) : topValidators?.validatorsAndRewards?.length ? (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead className="w-12">Rank</TableHead>
                    <TableHead>Validator</TableHead>
                    <TableHead className="text-right">Rewards (CC)</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {topValidators.validatorsAndRewards.slice(0, 10).map((v, i) => (
                    <TableRow key={i}>
                      <TableCell>
                        <Badge variant={i < 3 ? "default" : "secondary"}>
                          #{i + 1}
                        </Badge>
                      </TableCell>
                      <TableCell>
                        <code className="text-xs">{v.provider.slice(0, 24)}...</code>
                      </TableCell>
                      <TableCell className="text-right font-mono">
                        {parseFloat(v.rewards).toLocaleString(undefined, { maximumFractionDigits: 2 })}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            ) : (
              <p className="text-muted-foreground text-center py-4">No validator reward data</p>
            )}
          </CardContent>
        </Card>

        {/* Top Validators by Traffic */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <TrendingUp className="h-5 w-5 text-blue-500" />
              Top Validators by Purchased Traffic
            </CardTitle>
            <CardDescription>Validators ranked by total traffic purchased</CardDescription>
          </CardHeader>
          <CardContent>
            {loadingTraffic ? (
              <div className="space-y-2">
                <Skeleton className="h-10 w-full" />
                <Skeleton className="h-10 w-full" />
                <Skeleton className="h-10 w-full" />
              </div>
            ) : topTraffic?.validatorsByPurchasedTraffic?.length ? (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead className="w-12">Rank</TableHead>
                    <TableHead>Validator</TableHead>
                    <TableHead className="text-right">Traffic Purchased</TableHead>
                    <TableHead className="text-right">CC Spent</TableHead>
                    <TableHead className="text-right"># Purchases</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {topTraffic.validatorsByPurchasedTraffic.slice(0, 10).map((v: any, i: number) => (
                    <TableRow key={i}>
                      <TableCell>
                        <Badge variant={i < 3 ? "default" : "secondary"}>
                          #{i + 1}
                        </Badge>
                      </TableCell>
                      <TableCell>
                        <code className="text-xs">{v.validator.slice(0, 24)}...</code>
                      </TableCell>
                      <TableCell className="text-right font-mono">
                        {v.totalTrafficPurchased?.toLocaleString() || "—"}
                      </TableCell>
                      <TableCell className="text-right font-mono">
                        {parseFloat(v.totalCcSpent || "0").toLocaleString(undefined, { maximumFractionDigits: 2 })}
                      </TableCell>
                      <TableCell className="text-right">
                        {v.numPurchases || 0}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            ) : (
              <p className="text-muted-foreground text-center py-4">No traffic purchase data</p>
            )}
          </CardContent>
        </Card>
      </div>
    </DashboardLayout>
  );
}
