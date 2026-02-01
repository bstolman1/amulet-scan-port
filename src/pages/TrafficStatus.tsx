import { useState } from "react";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Progress } from "@/components/ui/progress";
import { useQuery } from "@tanstack/react-query";
import { scanApi } from "@/lib/api-client";
import { Search, Activity, TrendingUp, Server } from "lucide-react";

export default function TrafficStatus() {
  const [domainId, setDomainId] = useState("");
  const [memberId, setMemberId] = useState("");
  const [searchParams, setSearchParams] = useState<{ domainId: string; memberId: string } | null>(null);

  const { data: trafficStatus, isLoading, error } = useQuery({
    queryKey: ["trafficStatus", searchParams?.domainId, searchParams?.memberId],
    queryFn: () => scanApi.fetchTrafficStatus(searchParams!.domainId, searchParams!.memberId),
    enabled: !!searchParams?.domainId && !!searchParams?.memberId,
  });

  const { data: dsoInfo } = useQuery({
    queryKey: ["dsoInfo"],
    queryFn: () => scanApi.fetchDsoInfo(),
  });

  const handleSearch = () => {
    if (domainId && memberId) {
      setSearchParams({ domainId, memberId });
    }
  };

  const utilizationPercent = trafficStatus
    ? Math.min(100, (trafficStatus.traffic_status.actual.total_consumed / trafficStatus.traffic_status.actual.total_limit) * 100)
    : 0;

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <h1 className="text-3xl font-bold">Traffic Status</h1>
          <p className="text-muted-foreground">
            Check traffic consumption and limits for domain members
          </p>
        </div>

        {/* Search Form */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Search className="h-5 w-5" />
              Lookup Traffic Status
            </CardTitle>
            <CardDescription>
              Enter a domain ID and member ID to check traffic status
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              <div className="grid gap-4 md:grid-cols-2">
                <div className="space-y-2">
                  <label className="text-sm font-medium">Domain ID</label>
                  <Input
                    placeholder="global-synchronizer::..."
                    value={domainId}
                    onChange={(e) => setDomainId(e.target.value)}
                  />
                  {dsoInfo?.dso_rules?.domain_id && (
                    <Button 
                      variant="ghost" 
                      size="sm"
                      onClick={() => setDomainId(dsoInfo.dso_rules!.domain_id!)}
                    >
                      Use Global Synchronizer
                    </Button>
                  )}
                </div>
                <div className="space-y-2">
                  <label className="text-sm font-medium">Member ID</label>
                  <Input
                    placeholder="PAR::validator::..."
                    value={memberId}
                    onChange={(e) => setMemberId(e.target.value)}
                  />
                </div>
              </div>
              <Button onClick={handleSearch} disabled={!domainId || !memberId}>
                <Search className="h-4 w-4 mr-2" />
                Check Traffic Status
              </Button>
            </div>
          </CardContent>
        </Card>

        {/* Results */}
        {searchParams && (
          <div className="grid gap-6 md:grid-cols-3">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Activity className="h-5 w-5" />
                  Traffic Consumed
                </CardTitle>
              </CardHeader>
              <CardContent>
                {isLoading ? (
                  <Skeleton className="h-8 w-24" />
                ) : error ? (
                  <p className="text-destructive text-sm">Failed to load traffic status</p>
                ) : trafficStatus ? (
                  <div className="space-y-2">
                    <p className="text-3xl font-bold">
                      {trafficStatus.traffic_status.actual.total_consumed.toLocaleString()}
                    </p>
                    <p className="text-muted-foreground text-sm">bytes consumed</p>
                  </div>
                ) : null}
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Server className="h-5 w-5" />
                  Traffic Limit
                </CardTitle>
              </CardHeader>
              <CardContent>
                {isLoading ? (
                  <Skeleton className="h-8 w-24" />
                ) : error ? (
                  <p className="text-destructive text-sm">—</p>
                ) : trafficStatus ? (
                  <div className="space-y-2">
                    <p className="text-3xl font-bold">
                      {trafficStatus.traffic_status.actual.total_limit.toLocaleString()}
                    </p>
                    <p className="text-muted-foreground text-sm">bytes limit</p>
                  </div>
                ) : null}
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <TrendingUp className="h-5 w-5" />
                  Traffic Purchased
                </CardTitle>
              </CardHeader>
              <CardContent>
                {isLoading ? (
                  <Skeleton className="h-8 w-24" />
                ) : error ? (
                  <p className="text-destructive text-sm">—</p>
                ) : trafficStatus ? (
                  <div className="space-y-2">
                    <p className="text-3xl font-bold">
                      {trafficStatus.traffic_status.target.total_purchased.toLocaleString()}
                    </p>
                    <p className="text-muted-foreground text-sm">bytes purchased</p>
                  </div>
                ) : null}
              </CardContent>
            </Card>
          </div>
        )}

        {/* Utilization Chart */}
        {trafficStatus && (
          <Card>
            <CardHeader>
              <CardTitle>Traffic Utilization</CardTitle>
              <CardDescription>Current consumption vs limit</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                <div className="flex justify-between text-sm">
                  <span>Consumed: {trafficStatus.traffic_status.actual.total_consumed.toLocaleString()}</span>
                  <span>Limit: {trafficStatus.traffic_status.actual.total_limit.toLocaleString()}</span>
                </div>
                <Progress value={utilizationPercent} className="h-4" />
                <div className="flex justify-between items-center">
                  <Badge variant={utilizationPercent > 80 ? "destructive" : utilizationPercent > 50 ? "secondary" : "default"}>
                    {utilizationPercent.toFixed(1)}% utilized
                  </Badge>
                </div>
              </div>
            </CardContent>
          </Card>
        )}
      </div>
    </DashboardLayout>
  );
}
