import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
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
import type { SvEnvStatus, SvServiceCheck } from "@/lib/api-client";
import { Activity, RefreshCw, Clock, CheckCircle2, XCircle } from "lucide-react";

const ENV_LABELS: Record<string, string> = { dev: "dev", test: "test", main: "main" };
const SERVICES = ["mediator", "scan", "sv"] as const;
type Service = (typeof SERVICES)[number];

function StatusCell({ value }: { value: number }) {
  return value === 0 ? (
    <span className="text-green-600 font-semibold text-sm">OK</span>
  ) : (
    <span className="text-destructive font-semibold text-sm">BAD</span>
  );
}

function SummaryBadge({ ok, total }: { ok: number; total: number }) {
  const allOk = ok === total;
  return (
    <Badge
      variant={allOk ? "default" : "destructive"}
      className={allOk ? "bg-green-500/15 text-green-700 border-green-500/30" : ""}
    >
      {ok}/{total}
    </Badge>
  );
}

function EnvSection({ env }: { env: SvEnvStatus }) {
  if (!env.status) {
    return (
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">{ENV_LABELS[env.env] ?? env.env}</CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-sm text-destructive">
            Failed to load: {env.error || "Unknown error"}
          </p>
        </CardContent>
      </Card>
    );
  }

  // Get the union of all node names across services
  const nodeNames = Array.from(
    new Set([
      ...Object.keys(env.status.mediator.nodes),
      ...Object.keys(env.status.scan.nodes),
      ...Object.keys(env.status.sv.nodes),
    ])
  ).sort();

  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="text-base">{ENV_LABELS[env.env] ?? env.env}</CardTitle>
      </CardHeader>
      <CardContent className="p-0">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Name</TableHead>
              <TableHead>MEDIATOR</TableHead>
              <TableHead>SCAN</TableHead>
              <TableHead>SV</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {nodeNames.map((name) => (
              <TableRow key={name}>
                <TableCell className="font-medium text-sm">{name}</TableCell>
                <TableCell>
                  {name in env.status!.mediator.nodes ? (
                    <StatusCell value={env.status!.mediator.nodes[name]} />
                  ) : (
                    <span className="text-muted-foreground text-xs">—</span>
                  )}
                </TableCell>
                <TableCell>
                  {name in env.status!.scan.nodes ? (
                    <StatusCell value={env.status!.scan.nodes[name]} />
                  ) : (
                    <span className="text-muted-foreground text-xs">—</span>
                  )}
                </TableCell>
                <TableCell>
                  {name in env.status!.sv.nodes ? (
                    <StatusCell value={env.status!.sv.nodes[name]} />
                  ) : (
                    <span className="text-muted-foreground text-xs">—</span>
                  )}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  );
}

function getServiceCounts(envs: SvEnvStatus[], env: string, service: Service) {
  const e = envs.find((x) => x.env === env);
  if (!e?.status) return null;
  const check: SvServiceCheck = e.status[service];
  const nodes = Object.values(check.nodes);
  const total = nodes.length;
  const ok = nodes.filter((v) => v === 0).length;
  return { ok, total };
}

export default function SvStatus() {
  const { data, isLoading, error, refetch, isFetching, dataUpdatedAt } = useQuery({
    queryKey: ["svNodeStatus"],
    queryFn: () => scanApi.fetchSvNodeStatus(),
    staleTime: 30_000,
    refetchInterval: 60_000,
  });

  const environments: SvEnvStatus[] = data?.environments ?? [];
  const checkedAt = dataUpdatedAt ? new Date(dataUpdatedAt).toLocaleTimeString() : null;

  return (
    <DashboardLayout>
      <div className="space-y-6">
        {/* Header */}
        <div className="flex items-start justify-between">
          <div>
            <h1 className="text-3xl font-bold flex items-center gap-2">
              <Activity className="h-8 w-8 text-primary" />
              SV Status Monitor
            </h1>
            <p className="text-muted-foreground mt-1">
              Live health of MEDIATOR, SCAN, and SV services across all environments
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

        {/* Summary table */}
        <Card>
          <CardHeader className="pb-3">
            <div className="flex items-center justify-between">
              <CardTitle className="text-base">Summary</CardTitle>
              {checkedAt && (
                <span className="flex items-center gap-1 text-xs text-muted-foreground">
                  <Clock className="h-3 w-3" />
                  Last checked {checkedAt}
                </span>
              )}
            </div>
          </CardHeader>
          <CardContent className="p-0">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Env</TableHead>
                  <TableHead>MEDIATOR</TableHead>
                  <TableHead>SCAN</TableHead>
                  <TableHead>SV</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {isLoading ? (
                  ["dev", "test", "main"].map((env) => (
                    <TableRow key={env}>
                      <TableCell><Skeleton className="h-5 w-12" /></TableCell>
                      <TableCell><Skeleton className="h-5 w-12" /></TableCell>
                      <TableCell><Skeleton className="h-5 w-12" /></TableCell>
                      <TableCell><Skeleton className="h-5 w-12" /></TableCell>
                    </TableRow>
                  ))
                ) : error ? (
                  <TableRow>
                    <TableCell colSpan={4} className="text-center text-destructive py-4">
                      Failed to load SV status
                    </TableCell>
                  </TableRow>
                ) : (
                  ["dev", "test", "main"].map((env) => (
                    <TableRow key={env}>
                      <TableCell className="font-semibold">{env}</TableCell>
                      {SERVICES.map((svc) => {
                        const counts = getServiceCounts(environments, env, svc);
                        return (
                          <TableCell key={svc}>
                            {counts ? (
                              <SummaryBadge ok={counts.ok} total={counts.total} />
                            ) : (
                              <span className="text-muted-foreground text-xs">—</span>
                            )}
                          </TableCell>
                        );
                      })}
                    </TableRow>
                  ))
                )}
              </TableBody>
            </Table>
          </CardContent>
        </Card>

        {/* Per-environment detail tables */}
        {isLoading ? (
          <div className="space-y-4">
            {[1, 2, 3].map((i) => <Skeleton key={i} className="h-64 w-full" />)}
          </div>
        ) : (
          environments.map((env) => <EnvSection key={env.env} env={env} />)
        )}

        <p className="text-xs text-muted-foreground">
          Status updated every 60 seconds.
        </p>
      </div>
    </DashboardLayout>
  );
}
