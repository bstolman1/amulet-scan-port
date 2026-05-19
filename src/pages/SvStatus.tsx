import { useMemo, useState } from "react";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
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
import type { SvEnvStatus } from "@/lib/api-client";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { Activity, RefreshCw, Clock, Eye } from "lucide-react";

const DEFAULT_SV = "SV-Nodeops-Limited";
const ALL_ENVS = "__all__";
const ENVS = ["dev", "test", "main"] as const;
const ENV_LABELS: Record<string, string> = { dev: "dev", test: "test", main: "main" };

function getAllServices(environments: SvEnvStatus[]): string[] {
  const services = new Set<string>();
  for (const env of environments) {
    if (!env.status) continue;
    for (const svc of Object.keys(env.status)) {
      services.add(svc);
    }
  }
  return Array.from(services).sort();
}

function getServiceDescriptions(environments: SvEnvStatus[]): Record<string, string> {
  const descriptions: Record<string, string> = {};
  for (const env of environments) {
    if (!env.status) continue;
    for (const [svc, check] of Object.entries(env.status)) {
      if (check.description && !descriptions[svc]) {
        descriptions[svc] = check.description;
      }
    }
  }
  return descriptions;
}

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

function ServiceHeader({ name, description }: { name: string; description?: string }) {
  const label = name.toUpperCase();
  if (!description) return <>{label}</>;
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span className="border-b border-dotted border-muted-foreground/50">
          {label}
        </span>
      </TooltipTrigger>
      <TooltipContent>{description}</TooltipContent>
    </Tooltip>
  );
}

function EnvSection({ env, services, descriptions }: { env: SvEnvStatus; services: string[]; descriptions: Record<string, string> }) {
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

  const nodeNames = Array.from(
    new Set(services.flatMap((svc) => Object.keys(env.status![svc]?.nodes ?? {})))
  ).sort();

  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="text-base">{ENV_LABELS[env.env] ?? env.env}</CardTitle>
      </CardHeader>
      <CardContent className="p-0">
        <Table className="table-fixed">
          <TableHeader>
            <TableRow>
              <TableHead className="w-2/5">Name</TableHead>
              {services.map((svc) => (
                <TableHead key={svc}>
                  <ServiceHeader name={svc} description={descriptions[svc]} />
                </TableHead>
              ))}
            </TableRow>
          </TableHeader>
          <TableBody>
            {nodeNames.map((name) => (
              <TableRow key={name}>
                <TableCell className="font-medium text-sm"><span className="whitespace-nowrap">{name}</span></TableCell>
                {services.map((svc) => {
                  const nodes = env.status![svc]?.nodes;
                  return (
                    <TableCell key={svc}>
                      {nodes && name in nodes ? (
                        <StatusCell value={nodes[name]} />
                      ) : (
                        <span className="text-muted-foreground text-xs">—</span>
                      )}
                    </TableCell>
                  );
                })}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  );
}

function getServiceCounts(envs: SvEnvStatus[], envName: string, service: string) {
  const e = envs.find((x) => x.env === envName);
  if (!e?.status?.[service]) return null;
  const nodes = Object.values(e.status[service].nodes);
  const total = nodes.length;
  const ok = nodes.filter((v) => v === 0).length;
  return { ok, total };
}

export default function SvStatus() {
  const [selectedSv, setSelectedSv] = useState(DEFAULT_SV);
  const [selectedEnv, setSelectedEnv] = useState(ALL_ENVS);

  const { data: svList } = useQuery({
    queryKey: ["svList"],
    queryFn: () => scanApi.fetchSvList(),
    staleTime: Infinity,
  });

  const { data, isLoading, error, refetch, isFetching, dataUpdatedAt } = useQuery({
    queryKey: ["svDsoStatus", selectedSv],
    queryFn: () => scanApi.fetchSvDsoStatus(selectedSv),
    staleTime: 30_000,
    refetchInterval: 60_000,
  });

  const ENV_ORDER: Record<string, number> = { dev: 0, test: 1, main: 2 };
  const rawEnvironments: SvEnvStatus[] = useMemo(
    () => [...(data?.environments ?? [])].sort((a, b) => (ENV_ORDER[a.env] ?? 99) - (ENV_ORDER[b.env] ?? 99)),
    [data],
  );
  const allServices = useMemo(() => getAllServices(rawEnvironments), [rawEnvironments]);
  const serviceDescriptions = useMemo(() => getServiceDescriptions(rawEnvironments), [rawEnvironments]);

  const environments = useMemo(() => {
    if (selectedEnv === ALL_ENVS) return rawEnvironments;
    return rawEnvironments.filter((env) => env.env === selectedEnv);
  }, [rawEnvironments, selectedEnv]);

  const checkedAt = dataUpdatedAt ? new Date(dataUpdatedAt).toLocaleTimeString() : null;
  const displayEnvs = selectedEnv === ALL_ENVS ? [...ENVS] : [selectedEnv];

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
              Live health of SV services across all environments
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
            <Table className="table-fixed">
              <TableHeader>
                <TableRow>
                  <TableHead>Env</TableHead>
                  {isLoading ? (
                    <>
                      <TableHead><Skeleton className="h-4 w-16" /></TableHead>
                      <TableHead><Skeleton className="h-4 w-16" /></TableHead>
                      <TableHead><Skeleton className="h-4 w-16" /></TableHead>
                    </>
                  ) : (
                    allServices.map((svc) => (
                      <TableHead key={svc}>
                        <ServiceHeader name={svc} description={serviceDescriptions[svc]} />
                      </TableHead>
                    ))
                  )}
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
                    <TableCell colSpan={allServices.length + 1} className="text-center text-destructive py-4">
                      Failed to load SV status
                    </TableCell>
                  </TableRow>
                ) : (
                  displayEnvs.map((env) => (
                    <TableRow key={env}>
                      <TableCell className="font-semibold">{env}</TableCell>
                      {allServices.map((svc) => {
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

        {/* Filters */}
        <div className="flex items-center gap-3 flex-wrap">
          <Eye className="h-4 w-4 text-muted-foreground" />
          <span className="text-sm font-medium">Filter by Env:</span>
          <Select value={selectedEnv} onValueChange={setSelectedEnv}>
            <SelectTrigger className="w-[140px] bg-muted/60 border-border focus:ring-0 focus:ring-offset-0">
              <SelectValue />
            </SelectTrigger>
            <SelectContent className="bg-muted border-border">
              {[ALL_ENVS, ...ENVS].map((env) => (
                <SelectItem
                  key={env}
                  value={env}
                  className="focus:bg-primary/10 focus:text-primary data-[state=checked]:text-primary"
                >
                  {env === ALL_ENVS ? "All Envs" : env}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <span className="text-sm font-medium">As viewed from SV:</span>
          <Select value={selectedSv} onValueChange={setSelectedSv}>
            <SelectTrigger className="w-[280px] bg-muted/60 border-border focus:ring-0 focus:ring-offset-0">
              <SelectValue />
            </SelectTrigger>
            <SelectContent className="bg-muted border-border">
              {(svList ?? []).map((sv) => (
                <SelectItem
                  key={sv.id}
                  value={sv.id}
                  className="focus:bg-primary/10 focus:text-primary data-[state=checked]:text-primary"
                >
                  {sv.name}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          {(selectedSv !== DEFAULT_SV || selectedEnv !== ALL_ENVS) && (
            <Button
              variant="ghost"
              size="sm"
              onClick={() => { setSelectedSv(DEFAULT_SV); setSelectedEnv(ALL_ENVS); }}
              className="text-xs text-muted-foreground"
            >
              Reset
            </Button>
          )}
        </div>

        {/* Per-environment detail tables */}
        {isLoading ? (
          <div className="space-y-4">
            {[1, 2, 3].map((i) => <Skeleton key={i} className="h-64 w-full" />)}
          </div>
        ) : (
          environments.map((env) => (
            <EnvSection key={env.env} env={env} services={allServices} descriptions={serviceDescriptions} />
          ))
        )}

        <p className="text-xs text-muted-foreground">
          Status updated every 60 seconds.
        </p>
      </div>
    </DashboardLayout>
  );
}
