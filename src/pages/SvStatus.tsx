import { DashboardLayout } from "@/components/DashboardLayout";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { useQuery } from "@tanstack/react-query";
import { scanApi } from "@/lib/api-client";
import type { SvNodeStatus } from "@/lib/api-client";
import {
  Activity,
  CheckCircle2,
  XCircle,
  RefreshCw,
  ExternalLink,
  Clock,
  Server,
} from "lucide-react";

function StatusBadge({ online }: { online: boolean }) {
  return online ? (
    <Badge className="bg-green-500/15 text-green-600 border-green-500/30 gap-1">
      <CheckCircle2 className="h-3 w-3" />
      Online
    </Badge>
  ) : (
    <Badge variant="destructive" className="gap-1">
      <XCircle className="h-3 w-3" />
      Offline
    </Badge>
  );
}

function LatencyBadge({ latency }: { latency: number | null }) {
  if (latency === null) return <span className="text-muted-foreground text-sm">—</span>;
  const color =
    latency < 500
      ? "text-green-600"
      : latency < 1500
      ? "text-yellow-600"
      : "text-red-600";
  return (
    <span className={`text-sm font-mono ${color}`}>{latency} ms</span>
  );
}

function ScanLink({ url }: { url: string }) {
  // Derive a user-facing scan UI URL from the API base URL
  // e.g. https://scan.sv-1.../api/scan -> https://scan.sv-1...
  const base = url.replace(/\/api\/scan$/, "");
  return (
    <a
      href={base}
      target="_blank"
      rel="noopener noreferrer"
      className="flex items-center gap-1 text-xs font-mono text-primary hover:underline max-w-xs truncate"
      title={base}
    >
      {base.replace("https://", "")}
      <ExternalLink className="h-3 w-3 flex-shrink-0" />
    </a>
  );
}

export default function SvStatus() {
  const {
    data,
    isLoading,
    error,
    refetch,
    isFetching,
    dataUpdatedAt,
  } = useQuery({
    queryKey: ["svNodeStatus"],
    queryFn: () => scanApi.fetchSvNodeStatus(),
    staleTime: 60_000,
    refetchInterval: 120_000,
  });

  const statuses: SvNodeStatus[] = data?.sv_statuses ?? [];
  const onlineCount = statuses.filter((s) => s.online).length;
  const offlineCount = statuses.length - onlineCount;

  const versions = [...new Set(statuses.filter((s) => s.version).map((s) => s.version!))];
  const versionConsistent = versions.length <= 1;

  const checkedAt = data?.checked_at
    ? new Date(data.checked_at).toLocaleTimeString()
    : null;

  return (
    <DashboardLayout>
      <div className="space-y-6">
        {/* Header */}
        <div className="flex items-start justify-between">
          <div>
            <h1 className="text-3xl font-bold flex items-center gap-2">
              <Activity className="h-8 w-8 text-primary" />
              SV Status
            </h1>
            <p className="text-muted-foreground mt-1">
              Live connectivity and version status for all known Super Validator scan endpoints
            </p>
          </div>
          <Button
            variant="outline"
            size="sm"
            onClick={() => refetch()}
            disabled={isFetching}
            className="gap-2"
          >
            <RefreshCw className={`h-4 w-4 ${isFetching ? "animate-spin" : ""}`} />
            Refresh
          </Button>
        </div>

        {/* Summary cards */}
        <div className="grid gap-4 md:grid-cols-4">
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm text-muted-foreground">Total SVs</CardTitle>
            </CardHeader>
            <CardContent>
              {isLoading ? (
                <Skeleton className="h-8 w-12" />
              ) : (
                <div className="flex items-center gap-2">
                  <Server className="h-5 w-5 text-primary" />
                  <p className="text-3xl font-bold">{statuses.length}</p>
                </div>
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm text-muted-foreground">Online</CardTitle>
            </CardHeader>
            <CardContent>
              {isLoading ? (
                <Skeleton className="h-8 w-12" />
              ) : (
                <div className="flex items-center gap-2">
                  <CheckCircle2 className="h-5 w-5 text-green-500" />
                  <p className="text-3xl font-bold text-green-600">{onlineCount}</p>
                </div>
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm text-muted-foreground">Offline</CardTitle>
            </CardHeader>
            <CardContent>
              {isLoading ? (
                <Skeleton className="h-8 w-12" />
              ) : (
                <div className="flex items-center gap-2">
                  <XCircle className="h-5 w-5 text-destructive" />
                  <p className={`text-3xl font-bold ${offlineCount > 0 ? "text-destructive" : ""}`}>
                    {offlineCount}
                  </p>
                </div>
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm text-muted-foreground">Versions</CardTitle>
            </CardHeader>
            <CardContent>
              {isLoading ? (
                <Skeleton className="h-8 w-24" />
              ) : (
                <div className="space-y-1">
                  {versions.length === 0 ? (
                    <p className="text-muted-foreground text-sm">—</p>
                  ) : (
                    versions.map((v) => (
                      <Badge
                        key={v}
                        variant={versionConsistent ? "default" : "secondary"}
                        className="text-xs font-mono"
                      >
                        {v}
                      </Badge>
                    ))
                  )}
                </div>
              )}
            </CardContent>
          </Card>
        </div>

        {/* Status table */}
        <Card>
          <CardHeader>
            <div className="flex items-center justify-between">
              <div>
                <CardTitle>SV Scan Endpoints</CardTitle>
                <CardDescription>
                  Connectivity check via <code className="text-xs">/api/scan/version</code> on each node
                </CardDescription>
              </div>
              {checkedAt && (
                <span className="flex items-center gap-1 text-xs text-muted-foreground">
                  <Clock className="h-3 w-3" />
                  Last checked {checkedAt}
                </span>
              )}
            </div>
          </CardHeader>
          <CardContent>
            {isLoading ? (
              <div className="space-y-3">
                {Array.from({ length: 8 }).map((_, i) => (
                  <Skeleton key={i} className="h-12 w-full" />
                ))}
              </div>
            ) : error ? (
              <div className="text-center py-8 text-destructive">
                <XCircle className="h-10 w-10 mx-auto mb-2 opacity-60" />
                <p className="font-medium">Failed to load SV status</p>
                <p className="text-sm text-muted-foreground mt-1">{String(error)}</p>
              </div>
            ) : statuses.length === 0 ? (
              <div className="text-center py-8 text-muted-foreground">
                <Server className="h-10 w-10 mx-auto mb-2 opacity-40" />
                <p>No SV endpoints configured</p>
              </div>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Status</TableHead>
                    <TableHead>SV Name</TableHead>
                    <TableHead>Scan URL</TableHead>
                    <TableHead>Version</TableHead>
                    <TableHead>Latency</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {statuses
                    .slice()
                    .sort((a, b) => {
                      // Online first, then alphabetical
                      if (a.online !== b.online) return a.online ? -1 : 1;
                      return a.name.localeCompare(b.name);
                    })
                    .map((sv) => (
                      <TableRow key={sv.url}>
                        <TableCell>
                          <StatusBadge online={sv.online} />
                        </TableCell>
                        <TableCell className="font-medium">
                          {sv.name.replace(/-/g, " ")}
                        </TableCell>
                        <TableCell>
                          <ScanLink url={sv.url} />
                        </TableCell>
                        <TableCell>
                          {sv.version ? (
                            <Badge variant="outline" className="font-mono text-xs">
                              {sv.version}
                            </Badge>
                          ) : (
                            <span className="text-muted-foreground text-sm">—</span>
                          )}
                        </TableCell>
                        <TableCell>
                          <LatencyBadge latency={sv.latency} />
                        </TableCell>
                      </TableRow>
                    ))}
                </TableBody>
              </Table>
            )}
          </CardContent>
        </Card>

        <p className="text-xs text-muted-foreground">
          Auto-refreshes every 2 minutes. Each SV's scan endpoint is probed directly from the server.
        </p>
      </div>
    </DashboardLayout>
  );
}
