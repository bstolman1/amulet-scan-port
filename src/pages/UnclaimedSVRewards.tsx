import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Award, Users, TrendingUp, Search, Code, History } from "lucide-react";
import { useLatestACSSnapshot } from "@/hooks/use-acs-snapshots";
import { useAggregatedTemplateData } from "@/hooks/use-aggregated-template-data";
import { useRewardClaimEvents } from "@/hooks/use-governance-events";
import { DataSourcesFooter } from "@/components/DataSourcesFooter";
import { useState } from "react";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { format } from "date-fns";

interface ValidatorInfo {
  user: string;
  validator: string;
  count: number;
}

const UnclaimedSVRewards = () => {
  const [searchTerm, setSearchTerm] = useState("");

  const { data: snapshot } = useLatestACSSnapshot();
  const { data: rewardEvents, isLoading: eventsLoading } = useRewardClaimEvents();

  // Fetch ValidatorRewardCoupon contracts - the actual unclaimed rewards
  const { data: rewardCouponsData, isLoading: couponsLoading } = useAggregatedTemplateData(
    snapshot?.id,
    "Splice:Amulet:ValidatorRewardCoupon",
  );

  // Fetch SvRewardCoupon contracts
  const { data: svRewardCouponsData, isLoading: svCouponsLoading } = useAggregatedTemplateData(
    snapshot?.id,
    "Splice:Amulet:SvRewardCoupon",
  );

  // Fetch AppRewardCoupon contracts
  const { data: appRewardCouponsData, isLoading: appCouponsLoading } = useAggregatedTemplateData(
    snapshot?.id,
    "Splice:Amulet:AppRewardCoupon",
  );

  // Fetch UnclaimedReward contracts
  const { data: unclaimedRewardsData, isLoading: unclaimedLoading } = useAggregatedTemplateData(
    snapshot?.id,
    "Splice:Amulet:UnclaimedReward",
  );

  const isLoading = couponsLoading || svCouponsLoading || appCouponsLoading || unclaimedLoading;
  const rewardCoupons = [
    ...(rewardCouponsData?.data || []),
    ...(svRewardCouponsData?.data || []),
    ...(appRewardCouponsData?.data || []),
    ...(unclaimedRewardsData?.data || []),
  ];

  // Helper to safely extract field values from nested structure
  const getField = (record: any, ...fieldNames: string[]) => {
    for (const field of fieldNames) {
      if (record[field] !== undefined && record[field] !== null) return record[field];
      if (record.payload?.[field] !== undefined && record.payload?.[field] !== null) return record.payload[field];
    }
    return undefined;
  };

  // Debug logging
  console.log("🔍 DEBUG UnclaimedSVRewards: Total reward coupons:", rewardCoupons.length);
  console.log("🔍 DEBUG UnclaimedSVRewards: First 3 coupons:", rewardCoupons.slice(0, 3));
  if (rewardCoupons.length > 0) {
    console.log("🔍 DEBUG UnclaimedSVRewards: First coupon structure:", JSON.stringify(rewardCoupons[0], null, 2));
  }

  // Aggregate reward coupons by user
  const aggregatedRewards = (() => {
    const userMap = new Map<string, { user: string; totalAmount: number; coupons: any[] }>();

    rewardCoupons.forEach((coupon: any) => {
      const user = getField(coupon, "user", "validator", "validatorUser");

      if (!user) return; // Skip if no user identifier found

      if (!userMap.has(user)) {
        userMap.set(user, {
          user,
          totalAmount: 0,
          coupons: [],
        });
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
  const totalCoupons = rewardCoupons.length;
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
            Overview of validator reward coupons including unclaimed, claimed, and expired rewards
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
                      <p className="text-xs text-muted-foreground mt-1">Unclaimed reward coupons</p>
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
                      <p className="text-3xl font-bold text-success">{totalRewardAmount.toFixed(4)}</p>
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
                    <CardDescription className="mt-1">Validator reward coupons awaiting collection</CardDescription>
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
                    {aggregatedRewards.map((reward, i) => (
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

                        {/* Individual Coupons */}
                        <div className="space-y-2 mt-3 pt-3 border-t">
                          {reward.coupons.map((coupon: any, idx: number) => {
                            const amount = getField(coupon, "amount", "rewardAmount");
                            const roundNum = getField(coupon, "round")?.number;
                            const dso = getField(coupon, "dso");

                            return (
                              <div key={idx} className="bg-muted/30 p-3 rounded space-y-2">
                                <div className="grid grid-cols-2 gap-3 text-sm">
                                  <div>
                                    <p className="text-xs text-muted-foreground">Amount</p>
                                    <p className="font-semibold">{parseFloat(amount || "0").toFixed(4)} CC</p>
                                  </div>
                                  <div>
                                    <p className="text-xs text-muted-foreground">Round</p>
                                    <p className="font-mono">{roundNum || "N/A"}</p>
                                  </div>
                                </div>

                                {dso && (
                                  <div>
                                    <p className="text-xs text-muted-foreground">DSO</p>
                                    <p className="font-mono text-xs break-all">{dso}</p>
                                  </div>
                                )}

                                <Collapsible>
                                  <CollapsibleTrigger asChild>
                                    <Button variant="ghost" size="sm" className="w-full justify-start">
                                      <Code className="h-4 w-4 mr-2" />
                                      Show Raw JSON
                                    </Button>
                                  </CollapsibleTrigger>
                                  <CollapsibleContent className="mt-2">
                                    <pre className="text-xs bg-muted p-3 rounded overflow-auto max-h-96">
                                      {JSON.stringify(coupon, null, 2)}
                                    </pre>
                                  </CollapsibleContent>
                                </Collapsible>
                              </div>
                            );
                          })}
                        </div>
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
                <CardDescription>Rewards that have been successfully claimed by validators from historical data</CardDescription>
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
                          <TableCell>{event.round.toLocaleString()}</TableCell>
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
                <CardDescription>Rewards that expired before being claimed from historical data</CardDescription>
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
                          <TableCell>{event.round.toLocaleString()}</TableCell>
                          <TableCell className="font-semibold text-muted-foreground">
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

        <DataSourcesFooter
          snapshotId={snapshot?.id}
          templateSuffixes={[
            "Splice:Amulet:ValidatorRewardCoupon",
            "Splice:Amulet:SvRewardCoupon",
            "Splice:Amulet:AppRewardCoupon",
            "Splice:Amulet:UnclaimedReward",
          ]}
          isProcessing={false}
        />
      </div>
    </DashboardLayout>
  );
};

export default UnclaimedSVRewards;
