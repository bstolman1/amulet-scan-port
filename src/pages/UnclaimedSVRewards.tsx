import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Award, Users, TrendingUp, Search, Code, History } from "lucide-react";
import { useStateAcs } from "@/hooks/use-canton-scan-api";
import { useRewardClaimEvents } from "@/hooks/use-governance-events";
import { useState } from "react";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { format } from "date-fns";

const UnclaimedSVRewards = () => {
  const [searchTerm, setSearchTerm] = useState("");

  const { data: rewardEvents, isLoading: eventsLoading } = useRewardClaimEvents();

  // Fetch reward coupons from live ACS
  const rewardTemplates = [
    "Splice.Amulet:ValidatorRewardCoupon",
    "Splice.Amulet:SvRewardCoupon",
    "Splice.Amulet:AppRewardCoupon",
  ];
  const { data: rewardCoupons, isLoading: couponsLoading } = useStateAcs(rewardTemplates);

  const isLoading = couponsLoading;
  const allCoupons = rewardCoupons || [];

  // Helper to safely extract field values from nested structure
  const getField = (record: any, ...fieldNames: string[]) => {
    for (const field of fieldNames) {
      if (record[field] !== undefined && record[field] !== null) return record[field];
      if (record.create_arguments?.[field] !== undefined) return record.create_arguments[field];
      if (record.payload?.[field] !== undefined) return record.payload[field];
    }
    return undefined;
  };

  // Aggregate reward coupons by user
  const aggregatedRewards = (() => {
    const userMap = new Map<string, { user: string; totalAmount: number; coupons: any[] }>();

    allCoupons.forEach((coupon: any) => {
      const user = getField(coupon, "user", "validator", "validatorUser");
      if (!user) return;

      if (!userMap.has(user)) {
        userMap.set(user, { user, totalAmount: 0, coupons: [] });
      }
      const info = userMap.get(user)!;
      const amount = parseFloat(getField(coupon, "amount", "rewardAmount") || "0");
      info.totalAmount += amount;
      info.coupons.push(coupon);
    });

    return Array.from(userMap.values())
      .sort((a, b) => b.totalAmount - a.totalAmount)
      .filter((v) => {
        if (!searchTerm) return true;
        return v.user.toLowerCase().includes(searchTerm.toLowerCase());
      });
  })();

  // Calculate totals
  const totalCoupons = allCoupons.length;
  const uniqueUsers = aggregatedRewards.length;
  const totalRewardAmount = aggregatedRewards.reduce((sum, r) => sum + r.totalAmount, 0);

  // Process historical reward events
  const claimedRewards = (rewardEvents || []).filter((event: any) =>
    (event.event_type || "").toLowerCase().includes("claim") ||
    (event.template_id || "").includes("ClaimReward")
  );

  const expiredRewards = (rewardEvents || []).filter((event: any) =>
    (event.event_type || "").toLowerCase().includes("expire") ||
    (event.payload?.status || "").toLowerCase() === "expired"
  );

  const formatParty = (party: string) => {
    if (!party) return "Unknown";
    const parts = party.split("::");
    if (parts.length > 1) {
      return parts[0].substring(0, 30);
    }
    return party.substring(0, 30);
  };

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <h2 className="text-3xl font-bold mb-2">SV Rewards</h2>
          <p className="text-muted-foreground">
            Overview of validator reward coupons from live network data
          </p>
        </div>

        <Tabs defaultValue="unclaimed" className="space-y-6">
          <TabsList>
            <TabsTrigger value="unclaimed">Unclaimed Rewards</TabsTrigger>
            <TabsTrigger value="claimed">Claimed Rewards</TabsTrigger>
            <TabsTrigger value="expired">Expired Rewards</TabsTrigger>
          </TabsList>

          <TabsContent value="unclaimed" className="space-y-6">
            {/* Summary Cards */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <Card className="glass-card">
                <CardHeader className="pb-3">
                  <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                    <Award className="h-4 w-4" />
                    Total Reward Coupons
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  {isLoading ? (
                    <Skeleton className="h-10 w-full" />
                  ) : (
                    <>
                      <p className="text-3xl font-bold text-primary">{totalCoupons.toLocaleString()}</p>
                      <p className="text-xs text-muted-foreground mt-1">Live unclaimed coupons</p>
                    </>
                  )}
                </CardContent>
              </Card>

              <Card className="glass-card">
                <CardHeader className="pb-3">
                  <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                    <Users className="h-4 w-4" />
                    Unique Validators
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  {isLoading ? (
                    <Skeleton className="h-10 w-full" />
                  ) : (
                    <>
                      <p className="text-3xl font-bold text-chart-2">{uniqueUsers}</p>
                      <p className="text-xs text-muted-foreground mt-1">Validators with rewards</p>
                    </>
                  )}
                </CardContent>
              </Card>

              <Card className="glass-card">
                <CardHeader className="pb-3">
                  <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                    <TrendingUp className="h-4 w-4" />
                    Total Reward Amount
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  {isLoading ? (
                    <Skeleton className="h-10 w-full" />
                  ) : (
                    <>
                      <p className="text-3xl font-bold text-green-500">{totalRewardAmount.toFixed(4)}</p>
                      <p className="text-xs text-muted-foreground mt-1">Total unclaimed CC</p>
                    </>
                  )}
                </CardContent>
              </Card>
            </div>

            {/* Reward Coupons */}
            <Card className="glass-card">
              <CardHeader>
                <div className="flex items-center justify-between">
                  <div>
                    <CardTitle>Unclaimed Reward Coupons</CardTitle>
                    <CardDescription className="mt-1">Validator reward coupons from live ACS state</CardDescription>
                  </div>
                  <div className="flex items-center gap-2">
                    <Search className="h-4 w-4 text-muted-foreground" />
                    <Input
                      placeholder="Search by validator..."
                      value={searchTerm}
                      onChange={(e) => setSearchTerm(e.target.value)}
                      className="w-64"
                    />
                  </div>
                </div>
              </CardHeader>
              <CardContent>
                {isLoading ? (
                  <div className="space-y-3">
                    {[1, 2, 3, 4, 5].map((i) => (
                      <Skeleton key={i} className="h-24 w-full" />
                    ))}
                  </div>
                ) : aggregatedRewards.length === 0 ? (
                  <div className="text-center py-12">
                    <Award className="h-12 w-12 mx-auto mb-4 opacity-50" />
                    <p className="text-muted-foreground">
                      {searchTerm ? "No rewards found matching your search" : "No unclaimed rewards found"}
                    </p>
                  </div>
                ) : (
                  <div className="space-y-4">
                    {aggregatedRewards.slice(0, 50).map((reward, i) => (
                      <Card key={i} className="p-4">
                        <div className="flex justify-between items-start mb-3">
                          <div className="flex-1">
                            <p className="text-xs text-muted-foreground">Validator User</p>
                            <p className="font-mono text-sm break-all">{formatParty(reward.user)}</p>
                          </div>
                          <div className="text-right">
                            <p className="text-xs text-muted-foreground">Total Reward</p>
                            <p className="text-xl font-bold text-primary">{reward.totalAmount.toFixed(4)} CC</p>
                          </div>
                        </div>

                        <div className="mb-2">
                          <Badge variant="secondary">{reward.coupons.length} Coupons</Badge>
                        </div>

                        <Collapsible>
                          <CollapsibleTrigger asChild>
                            <Button variant="ghost" size="sm" className="w-full justify-start">
                              <Code className="h-4 w-4 mr-2" />
                              Show Details
                            </Button>
                          </CollapsibleTrigger>
                          <CollapsibleContent className="mt-2 space-y-2">
                            {reward.coupons.slice(0, 5).map((coupon: any, idx: number) => (
                              <div key={idx} className="bg-muted/30 p-3 rounded text-sm">
                                <div className="grid grid-cols-2 gap-2">
                                  <div>
                                    <p className="text-xs text-muted-foreground">Amount</p>
                                    <p>{parseFloat(getField(coupon, "amount", "rewardAmount") || "0").toFixed(4)} CC</p>
                                  </div>
                                  <div>
                                    <p className="text-xs text-muted-foreground">Round</p>
                                    <p>{getField(coupon, "round")?.number || "—"}</p>
                                  </div>
                                </div>
                              </div>
                            ))}
                            {reward.coupons.length > 5 && (
                              <p className="text-xs text-muted-foreground">
                                ... and {reward.coupons.length - 5} more coupons
                              </p>
                            )}
                          </CollapsibleContent>
                        </Collapsible>
                      </Card>
                    ))}
                  </div>
                )}
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="claimed" className="space-y-6">
            <Card className="glass-card">
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <History className="h-5 w-5" />
                  Claimed Rewards History
                </CardTitle>
                <CardDescription>Rewards that have been successfully claimed (from historical data)</CardDescription>
              </CardHeader>
              <CardContent>
                {eventsLoading ? (
                  <div className="space-y-3">
                    {[1, 2, 3].map((i) => (
                      <Skeleton key={i} className="h-20 w-full" />
                    ))}
                  </div>
                ) : claimedRewards.length === 0 ? (
                  <div className="flex items-center justify-center py-12 text-muted-foreground">
                    <div className="text-center space-y-2">
                      <Award className="h-12 w-12 mx-auto opacity-50" />
                      <p className="font-medium">No claimed rewards found</p>
                      <p className="text-sm">Historical claim events will appear here</p>
                    </div>
                  </div>
                ) : (
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Event Type</TableHead>
                        <TableHead>Round</TableHead>
                        <TableHead>Amount</TableHead>
                        <TableHead>Timestamp</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {claimedRewards.slice(0, 50).map((event: any) => (
                        <TableRow key={event.id}>
                          <TableCell className="font-mono text-xs">{event.event_type}</TableCell>
                          <TableCell>{event.round?.toLocaleString()}</TableCell>
                          <TableCell className="font-semibold">
                            {parseFloat(event.payload?.amount || "0").toFixed(4)} CC
                          </TableCell>
                          <TableCell className="text-xs">
                            {format(new Date(event.timestamp), "MMM d, yyyy HH:mm")}
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                )}
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="expired" className="space-y-6">
            <Card className="glass-card">
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <History className="h-5 w-5" />
                  Expired Rewards History
                </CardTitle>
                <CardDescription>Rewards that expired before being claimed (from historical data)</CardDescription>
              </CardHeader>
              <CardContent>
                {eventsLoading ? (
                  <div className="space-y-3">
                    {[1, 2, 3].map((i) => (
                      <Skeleton key={i} className="h-20 w-full" />
                    ))}
                  </div>
                ) : expiredRewards.length === 0 ? (
                  <div className="flex items-center justify-center py-12 text-muted-foreground">
                    <div className="text-center space-y-2">
                      <Award className="h-12 w-12 mx-auto opacity-50" />
                      <p className="font-medium">No expired rewards found</p>
                      <p className="text-sm">Historical expiry events will appear here</p>
                    </div>
                  </div>
                ) : (
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Event Type</TableHead>
                        <TableHead>Round</TableHead>
                        <TableHead>Amount</TableHead>
                        <TableHead>Timestamp</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {expiredRewards.slice(0, 50).map((event: any) => (
                        <TableRow key={event.id}>
                          <TableCell className="font-mono text-xs">{event.event_type}</TableCell>
                          <TableCell>{event.round?.toLocaleString()}</TableCell>
                          <TableCell className="font-semibold">
                            {parseFloat(event.payload?.amount || "0").toFixed(4)} CC
                          </TableCell>
                          <TableCell className="text-xs">
                            {format(new Date(event.timestamp), "MMM d, yyyy HH:mm")}
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                )}
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>

        <Card className="p-4 text-xs text-muted-foreground">
          <p>
            Unclaimed rewards sourced from Canton Scan API <code>/v0/state/acs</code> endpoint. Historical data from local DuckDB.
          </p>
        </Card>
      </div>
    </DashboardLayout>
  );
};

export default UnclaimedSVRewards;