import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { useQuery } from "@tanstack/react-query";
import { scanApi } from "@/lib/api-client";
import { Server, ExternalLink, Globe } from "lucide-react";

export default function Sequencers() {
  const { data: dsoSequencers, isLoading } = useQuery({
    queryKey: ["dsoSequencers"],
    queryFn: () => scanApi.fetchDsoSequencers(),
  });

  const totalSequencers = dsoSequencers?.domainSequencers?.reduce(
    (acc, domain) => acc + (domain.sequencers?.length || 0),
    0
  ) || 0;

  const totalDomains = dsoSequencers?.domainSequencers?.length || 0;

  if (isLoading) {
    return (
      <DashboardLayout>
        <div className="space-y-6">
          <Skeleton className="h-10 w-64" />
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <Skeleton className="h-24" />
            <Skeleton className="h-24" />
            <Skeleton className="h-24" />
          </div>
          <Skeleton className="h-80 w-full" />
        </div>
      </DashboardLayout>
    );
  }

  if (!dsoSequencers?.domainSequencers?.length) {
    return (
      <DashboardLayout>
        <div className="space-y-6">
          <div>
            <div className="flex items-center gap-3 mb-2">
              <Server className="h-8 w-8 text-primary" />
              <h1 className="text-3xl font-bold">DSO Sequencers</h1>
            </div>
            <p className="text-muted-foreground">
              Domain sequencer configuration across the network
            </p>
          </div>
          <Alert>
            <AlertTitle>No Sequencer Data</AlertTitle>
            <AlertDescription>
              No sequencer information is currently available from the Canton Scan API.
            </AlertDescription>
          </Alert>
        </div>
      </DashboardLayout>
    );
  }

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <div className="flex items-center gap-3 mb-2">
            <Server className="h-8 w-8 text-primary" />
            <h1 className="text-3xl font-bold">DSO Sequencers</h1>
          </div>
          <p className="text-muted-foreground">
            Domain sequencer configuration across the network
          </p>
        </div>

        {/* Overview Stats */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm text-muted-foreground">Total Domains</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="flex items-center gap-2">
                <Globe className="h-5 w-5 text-primary" />
                <p className="text-3xl font-bold">{totalDomains}</p>
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm text-muted-foreground">Total Sequencers</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="flex items-center gap-2">
                <Server className="h-5 w-5 text-primary" />
                <p className="text-3xl font-bold">{totalSequencers}</p>
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm text-muted-foreground">Avg per Domain</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-3xl font-bold">
                {totalDomains > 0 ? (totalSequencers / totalDomains).toFixed(1) : "—"}
              </p>
            </CardContent>
          </Card>
        </div>

        {/* Sequencers by Domain */}
        <div className="space-y-4">
          {dsoSequencers.domainSequencers.map((domain, i) => (
            <Card key={i}>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Globe className="h-5 w-5" />
                  Domain
                </CardTitle>
                <CardDescription>
                  <Badge variant="outline" className="font-mono text-xs">
                    {domain.domainId}
                  </Badge>
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="grid gap-3">
                  {domain.sequencers.map((seq, j) => (
                    <div 
                      key={j} 
                      className="flex items-center justify-between p-4 bg-muted/50 rounded-lg border border-border/50"
                    >
                      <div className="flex items-center gap-4">
                        <div className="p-2 rounded-md bg-primary/10">
                          <Server className="h-4 w-4 text-primary" />
                        </div>
                        <div>
                          <p className="font-semibold">{seq.svName}</p>
                          <Badge variant="secondary" className="text-xs mt-1">
                            Migration {seq.migrationId}
                          </Badge>
                        </div>
                      </div>
                      <a 
                        href={seq.url} 
                        target="_blank" 
                        rel="noopener noreferrer" 
                        className="flex items-center gap-2 text-primary hover:underline font-mono text-sm"
                      >
                        <span className="hidden md:inline truncate max-w-xs">{seq.url}</span>
                        <span className="md:hidden">View URL</span>
                        <ExternalLink className="h-4 w-4 flex-shrink-0" />
                      </a>
                    </div>
                  ))}
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      </div>
    </DashboardLayout>
  );
}
