import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { useQuery } from "@tanstack/react-query";
import { getDuckDBApiUrl } from "@/lib/backend-config";
import {
  Package,
  Trophy,
  Lock,
  CheckCircle2,
  XCircle,
  AlertTriangle,
  Coins,
  Calendar,
  TrendingUp,
  Shield,
} from "lucide-react";

interface AppReport {
  appName: string;
  provider: string;
  providerFull: string;
  dso: string;
  createdAt: string | null;
  approvalDate: string | null;
  daysSinceApproval: number | null;
  cumulativeRewards: number;
  hasReached10m: boolean;
  hasReached25m: boolean;
  canLockDay1: boolean;
  milestone10m: {
    round: number;
    date: string;
    amount: number;
    daysFromApproval: number | null;
  } | null;
  milestone25m: {
    round: number;
    date: string;
    amount: number;
    daysFromApproval: number | null;
  } | null;
}

interface ReportData {
  report: {
    generatedAt: string;
    latestRound: number;
    totalFeaturedApps: number;
    totalLifetimeRewards: number;
    appsAbove10m: number;
    appsAbove25m: number;
    canLockDay1Count: number;
    thresholds: {
      LOCK_AMOUNT: number;
      MILESTONE_10M: number;
      MILESTONE_25M: number;
    };
    apps: AppReport[];
  };
  meta: {
    queryTimeSeconds: number;
    dataSources: {
      featuredApps: number;
      voteResults: number;
      topProviders: number;
      roundPartyTotals: number;
    };
    hasApprovalDates: boolean;
    hasMilestoneData: boolean;
  };
}

function formatCC(amount: number): string {
  if (amount >= 1_000_000) {
    return `${(amount / 1_000_000).toFixed(2)}M`;
  }
  if (amount >= 1_000) {
    return `${(amount / 1_000).toFixed(1)}K`;
  }
  return amount.toFixed(2);
}

function formatCCFull(amount: number): string {
  return amount.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

function formatDate(dateStr: string | null): string {
  if (!dateStr) return "Unknown";
  return new Date(dateStr).toLocaleDateString("en-US", {
    year: "numeric",
    month: "short",
    day: "numeric",
  });
}

export default function FeaturedAppsReport() {
  const baseUrl = getDuckDBApiUrl();

  const { data, isLoading, error } = useQuery<ReportData>({
    queryKey: ["featured-apps-report"],
    queryFn: async () => {
      const res = await fetch(`${baseUrl}/api/featured-apps-report`);
      if (!res.ok) {
        const text = await res.text();
        throw new Error(`Failed to fetch report: ${res.status} ${text}`);
      }
      return res.json();
    },
    staleTime: 5 * 60 * 1000,
  });

  const report = data?.report;
  const meta = data?.meta;

  return (
    <DashboardLayout>
      <TooltipProvider>
        <div className="space-y-6">
          {/* Header */}
          <div>
            <div className="flex items-center gap-2 mb-2">
              <Shield className="h-8 w-8 text-primary" />
              <h1 className="text-3xl font-bold">Featured Apps Report</h1>
            </div>
            <p className="text-muted-foreground">
              Historical FA rewards analysis &amp; CIP locking readiness assessment
            </p>
          </div>

          {/* Loading State */}
          {isLoading && (
            <div className="space-y-4">
              <div className="grid gap-4 md:grid-cols-4">
                {[1, 2, 3, 4].map((i) => (
                  <Skeleton key={i} className="h-32 w-full" />
                ))}
              </div>
              <Skeleton className="h-96 w-full" />
            </div>
          )}

          {/* Error State */}
          {error && (
            <Card className="p-8 text-center border-destructive">
              <AlertTriangle className="h-12 w-12 mx-auto mb-4 text-destructive" />
              <h3 className="text-lg font-semibold mb-2">Failed to Load Report</h3>
              <p className="text-muted-foreground">{(error as Error).message}</p>
            </Card>
          )}

          {report && (
            <>
              {/* Summary Cards */}
              <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
                <Card>
                  <CardHeader className="pb-2">
                    <CardTitle className="text-sm font-medium flex items-center gap-2">
                      <Package className="h-4 w-4" />
                      Total Featured Apps
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <p className="text-3xl font-bold">{report.totalFeaturedApps}</p>
                    <p className="text-xs text-muted-foreground mt-1">
                      Currently on-chain
                    </p>
                  </CardContent>
                </Card>

                <Card>
                  <CardHeader className="pb-2">
                    <CardTitle className="text-sm font-medium flex items-center gap-2">
                      <Coins className="h-4 w-4" />
                      Lifetime FA Rewards
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <p className="text-3xl font-bold">{formatCC(report.totalLifetimeRewards)}</p>
                    <p className="text-xs text-muted-foreground mt-1">
                      {formatCCFull(report.totalLifetimeRewards)} CC total
                    </p>
                  </CardContent>
                </Card>

                <Card>
                  <CardHeader className="pb-2">
                    <CardTitle className="text-sm font-medium flex items-center gap-2">
                      <TrendingUp className="h-4 w-4" />
                      FAs Above Thresholds
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-1">
                      <div className="flex justify-between">
                        <span className="text-sm text-muted-foreground">&ge; 10M CC:</span>
                        <span className="font-bold">{report.appsAbove10m}</span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-sm text-muted-foreground">&ge; 25M CC:</span>
                        <span className="font-bold">{report.appsAbove25m}</span>
                      </div>
                    </div>
                  </CardContent>
                </Card>

                <Card className="border-primary/50">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-sm font-medium flex items-center gap-2">
                      <Lock className="h-4 w-4 text-primary" />
                      CIP Lock Ready (Day 1)
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <p className="text-3xl font-bold text-primary">{report.canLockDay1Count}</p>
                    <p className="text-xs text-muted-foreground mt-1">
                      Can lock {formatCC(report.thresholds.LOCK_AMOUNT)} CC today
                    </p>
                  </CardContent>
                </Card>
              </div>

              {/* CIP Locking Readiness Summary */}
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Shield className="h-5 w-5 text-primary" />
                    CIP Locking Readiness
                  </CardTitle>
                  <CardDescription>
                    If the FA Locking CIP passes, partners must lock 25M CC within 6 months.
                    This shows which FAs have sufficient cumulative rewards today.
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="grid gap-3 md:grid-cols-3">
                    <div className="rounded-lg bg-green-500/10 border border-green-500/20 p-4">
                      <div className="flex items-center gap-2 mb-2">
                        <CheckCircle2 className="h-5 w-5 text-green-500" />
                        <span className="font-semibold text-green-400">Ready to Lock</span>
                      </div>
                      <p className="text-2xl font-bold">{report.canLockDay1Count}</p>
                      <p className="text-xs text-muted-foreground">
                        Have &ge; 25M CC in cumulative rewards
                      </p>
                    </div>
                    <div className="rounded-lg bg-yellow-500/10 border border-yellow-500/20 p-4">
                      <div className="flex items-center gap-2 mb-2">
                        <AlertTriangle className="h-5 w-5 text-yellow-500" />
                        <span className="font-semibold text-yellow-400">Approaching</span>
                      </div>
                      <p className="text-2xl font-bold">
                        {report.appsAbove10m - report.appsAbove25m}
                      </p>
                      <p className="text-xs text-muted-foreground">
                        Between 10M and 25M CC
                      </p>
                    </div>
                    <div className="rounded-lg bg-red-500/10 border border-red-500/20 p-4">
                      <div className="flex items-center gap-2 mb-2">
                        <XCircle className="h-5 w-5 text-red-500" />
                        <span className="font-semibold text-red-400">Below Threshold</span>
                      </div>
                      <p className="text-2xl font-bold">
                        {report.totalFeaturedApps - report.appsAbove10m}
                      </p>
                      <p className="text-xs text-muted-foreground">
                        Less than 10M CC
                      </p>
                    </div>
                  </div>
                </CardContent>
              </Card>

              {/* Detailed Table */}
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Trophy className="h-5 w-5 text-yellow-500" />
                    Featured Apps Detail
                  </CardTitle>
                  <CardDescription>
                    All Featured Apps ranked by cumulative CC mined, with approval dates and milestone tracking
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="overflow-x-auto">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead className="w-10">#</TableHead>
                          <TableHead>App Name</TableHead>
                          <TableHead>Provider</TableHead>
                          <TableHead className="text-center">
                            <Tooltip>
                              <TooltipTrigger>FA Approved</TooltipTrigger>
                              <TooltipContent>
                                Date the GrantFeaturedAppRight vote was accepted
                              </TooltipContent>
                            </Tooltip>
                          </TableHead>
                          <TableHead className="text-right">
                            <Tooltip>
                              <TooltipTrigger>Cumulative CC</TooltipTrigger>
                              <TooltipContent>
                                Total Canton Coin mined since launch
                              </TooltipContent>
                            </Tooltip>
                          </TableHead>
                          <TableHead className="text-center">
                            <Tooltip>
                              <TooltipTrigger>Days to 10M</TooltipTrigger>
                              <TooltipContent>
                                Days from FA approval to reaching 10M CC milestone
                              </TooltipContent>
                            </Tooltip>
                          </TableHead>
                          <TableHead className="text-center">
                            <Tooltip>
                              <TooltipTrigger>Days to 25M</TooltipTrigger>
                              <TooltipContent>
                                Days from FA approval to reaching 25M CC milestone
                              </TooltipContent>
                            </Tooltip>
                          </TableHead>
                          <TableHead className="text-center">
                            <Tooltip>
                              <TooltipTrigger>Lock Ready</TooltipTrigger>
                              <TooltipContent>
                                Can this FA lock 25M CC on Day 1 if the CIP passes?
                              </TooltipContent>
                            </Tooltip>
                          </TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {report.apps.map((app, i) => (
                          <TableRow key={app.providerFull || i}>
                            <TableCell>
                              <Badge variant={i < 3 ? "default" : "secondary"}>
                                {i + 1}
                              </Badge>
                            </TableCell>
                            <TableCell className="font-medium">
                              {app.appName || "Unknown"}
                            </TableCell>
                            <TableCell>
                              <Tooltip>
                                <TooltipTrigger>
                                  <code className="text-xs">
                                    {app.provider.slice(0, 16)}...
                                  </code>
                                </TooltipTrigger>
                                <TooltipContent className="max-w-md break-all">
                                  {app.providerFull}
                                </TooltipContent>
                              </Tooltip>
                            </TableCell>
                            <TableCell className="text-center">
                              <div className="flex flex-col items-center">
                                <span className="text-sm">{formatDate(app.approvalDate)}</span>
                                {app.daysSinceApproval !== null && (
                                  <span className="text-xs text-muted-foreground">
                                    ({app.daysSinceApproval}d ago)
                                  </span>
                                )}
                              </div>
                            </TableCell>
                            <TableCell className="text-right font-mono">
                              <Tooltip>
                                <TooltipTrigger>
                                  <span className={
                                    app.hasReached25m
                                      ? "text-green-400 font-bold"
                                      : app.hasReached10m
                                        ? "text-yellow-400"
                                        : ""
                                  }>
                                    {formatCC(app.cumulativeRewards)}
                                  </span>
                                </TooltipTrigger>
                                <TooltipContent>
                                  {formatCCFull(app.cumulativeRewards)} CC
                                </TooltipContent>
                              </Tooltip>
                            </TableCell>
                            <TableCell className="text-center">
                              {app.milestone10m ? (
                                <Tooltip>
                                  <TooltipTrigger>
                                    <Badge variant="outline" className="bg-yellow-500/10 text-yellow-400 border-yellow-500/30">
                                      {app.milestone10m.daysFromApproval !== null
                                        ? `${app.milestone10m.daysFromApproval}d`
                                        : formatDate(app.milestone10m.date)}
                                    </Badge>
                                  </TooltipTrigger>
                                  <TooltipContent>
                                    Reached 10M CC on {formatDate(app.milestone10m.date)}
                                    {app.milestone10m.daysFromApproval !== null &&
                                      ` (${app.milestone10m.daysFromApproval} days after FA approval)`}
                                  </TooltipContent>
                                </Tooltip>
                              ) : app.hasReached10m ? (
                                <Badge variant="outline" className="bg-yellow-500/10 text-yellow-400 border-yellow-500/30">
                                  Reached
                                </Badge>
                              ) : (
                                <span className="text-xs text-muted-foreground">--</span>
                              )}
                            </TableCell>
                            <TableCell className="text-center">
                              {app.milestone25m ? (
                                <Tooltip>
                                  <TooltipTrigger>
                                    <Badge variant="outline" className="bg-green-500/10 text-green-400 border-green-500/30">
                                      {app.milestone25m.daysFromApproval !== null
                                        ? `${app.milestone25m.daysFromApproval}d`
                                        : formatDate(app.milestone25m.date)}
                                    </Badge>
                                  </TooltipTrigger>
                                  <TooltipContent>
                                    Reached 25M CC on {formatDate(app.milestone25m.date)}
                                    {app.milestone25m.daysFromApproval !== null &&
                                      ` (${app.milestone25m.daysFromApproval} days after FA approval)`}
                                  </TooltipContent>
                                </Tooltip>
                              ) : app.hasReached25m ? (
                                <Badge variant="outline" className="bg-green-500/10 text-green-400 border-green-500/30">
                                  Reached
                                </Badge>
                              ) : (
                                <span className="text-xs text-muted-foreground">--</span>
                              )}
                            </TableCell>
                            <TableCell className="text-center">
                              {app.canLockDay1 ? (
                                <CheckCircle2 className="h-5 w-5 text-green-500 mx-auto" />
                              ) : (
                                <XCircle className="h-5 w-5 text-red-500/50 mx-auto" />
                              )}
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                </CardContent>
              </Card>

              {/* Data Source Info */}
              {meta && (
                <Card className="bg-muted/30">
                  <CardContent className="pt-4">
                    <div className="flex flex-wrap gap-4 text-xs text-muted-foreground">
                      <span>
                        <Calendar className="h-3 w-3 inline mr-1" />
                        Generated: {new Date(report.generatedAt).toLocaleString()}
                      </span>
                      <span>Round: {report.latestRound.toLocaleString()}</span>
                      <span>Query: {meta.queryTimeSeconds.toFixed(2)}s</span>
                      <span>
                        Vote results: {meta.dataSources.voteResults}
                        {!meta.hasApprovalDates && " (no approval dates matched)"}
                      </span>
                      <span>
                        Round-party data: {meta.dataSources.roundPartyTotals > 0 ? "available" : "unavailable"}
                      </span>
                    </div>
                    {!meta.hasApprovalDates && (
                      <p className="text-xs text-yellow-400 mt-2">
                        <AlertTriangle className="h-3 w-3 inline mr-1" />
                        FA approval dates could not be determined from on-chain vote results.
                        The vote results API may not include historical GrantFeaturedAppRight actions.
                      </p>
                    )}
                    {!meta.hasMilestoneData && (
                      <p className="text-xs text-yellow-400 mt-2">
                        <AlertTriangle className="h-3 w-3 inline mr-1" />
                        Round-party-totals endpoint not available. Milestone timing (days to 10M/25M)
                        cannot be calculated without historical per-party round data.
                      </p>
                    )}
                  </CardContent>
                </Card>
              )}
            </>
          )}
        </div>
      </TooltipProvider>
    </DashboardLayout>
  );
}
