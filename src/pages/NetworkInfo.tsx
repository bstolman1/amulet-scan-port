import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { useQuery } from "@tanstack/react-query";
import { scanApi } from "@/lib/api-client";
import { Network, Clock, Server, FileText } from "lucide-react";
import { useAmuletRules } from "@/hooks/use-canton-scan-api";
import { Link } from "react-router-dom";

export default function NetworkInfo() {
  const { data: instanceNames, isLoading: loadingNames } = useQuery({
    queryKey: ["spliceInstanceNames"],
    queryFn: () => scanApi.fetchSpliceInstanceNames(),
  });

  const { data: migrationSchedule, isLoading: loadingMigration } = useQuery({
    queryKey: ["migrationSchedule"],
    queryFn: () => scanApi.fetchMigrationSchedule(),
  });

  const { data: amuletRulesData, isLoading: loadingAmuletRules } = useAmuletRules();

  const { data: dsoSequencers, isLoading: loadingSequencers } = useQuery({
    queryKey: ["dsoSequencers"],
    queryFn: () => scanApi.fetchDsoSequencers(),
  });

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <h1 className="text-3xl font-bold">Network Information</h1>
          <p className="text-muted-foreground">
            Network configuration, migration schedule, feature support, and sequencer info
          </p>
        </div>

        <div className="grid gap-6 md:grid-cols-2">
          {/* Instance Names */}
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Network className="h-5 w-5" />
                Splice Instance Names
              </CardTitle>
              <CardDescription>Network and service naming configuration</CardDescription>
            </CardHeader>
            <CardContent>
              {loadingNames ? (
                <div className="space-y-2">
                  <Skeleton className="h-4 w-full" />
                  <Skeleton className="h-4 w-3/4" />
                  <Skeleton className="h-4 w-1/2" />
                </div>
              ) : instanceNames ? (
                <div className="space-y-3">
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Network Name</span>
                    <span className="font-medium">{instanceNames.network_name}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Amulet Name</span>
                    <span className="font-medium">{instanceNames.amulet_name} ({instanceNames.amulet_name_acronym})</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Name Service</span>
                    <span className="font-medium">{instanceNames.name_service_name} ({instanceNames.name_service_name_acronym})</span>
                  </div>
                  {instanceNames.network_favicon_url && (
                    <div className="flex justify-between items-center">
                      <span className="text-muted-foreground">Favicon</span>
                      <img src={instanceNames.network_favicon_url} alt="Network favicon" className="h-6 w-6" />
                    </div>
                  )}
                </div>
              ) : (
                <p className="text-muted-foreground">No instance names available</p>
              )}
            </CardContent>
          </Card>

          {/* Migration Schedule */}
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Clock className="h-5 w-5" />
                Migration Schedule
              </CardTitle>
              <CardDescription>Upcoming network migrations</CardDescription>
            </CardHeader>
            <CardContent>
              {loadingMigration ? (
                <div className="space-y-2">
                  <Skeleton className="h-4 w-full" />
                  <Skeleton className="h-4 w-3/4" />
                </div>
              ) : migrationSchedule ? (
                <div className="space-y-3">
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Migration ID</span>
                    <Badge variant="outline">{migrationSchedule.migration_id}</Badge>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Scheduled Time</span>
                    <span className="font-medium">
                      {new Date(migrationSchedule.time).toLocaleString()}
                    </span>
                  </div>
                </div>
              ) : (
                <p className="text-muted-foreground">No migration scheduled</p>
              )}
            </CardContent>
          </Card>

          {/* Amulet Rules Summary */}
          <Card className="md:col-span-2">
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <FileText className="h-5 w-5" />
                Amulet Rules
              </CardTitle>
              <CardDescription>Core network configuration parameters</CardDescription>
            </CardHeader>
            <CardContent>
              {loadingAmuletRules ? (
                <div className="space-y-2">
                  <Skeleton className="h-4 w-full" />
                  <Skeleton className="h-4 w-3/4" />
                  <Skeleton className="h-4 w-1/2" />
                </div>
              ) : amuletRulesData ? (
                <div className="space-y-4">
                  {(() => {
                    const raw = amuletRulesData as any;
                    const payload = raw?.contract?.payload ?? raw?.payload ?? raw;
                    const configSchedule = payload?.configSchedule ?? payload?.config_schedule;
                    const config = configSchedule?.initialValue ?? configSchedule?.initial_value ?? payload;
                    const transferConfig = config?.transferConfig ?? config?.transfer_config;
                    const issuanceCurve = config?.issuanceCurve ?? config?.issuance_curve;
                    const initialIssuance = issuanceCurve?.initialValue ?? issuanceCurve?.initial_value;
                    
                    return (
                      <>
                        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                          <div className="p-3 rounded-lg bg-muted/50">
                            <p className="text-xs text-muted-foreground">Holding Fee Rate</p>
                            <p className="font-semibold">{transferConfig?.holdingFee?.rate ?? transferConfig?.holding_fee?.rate ?? "—"}</p>
                          </div>
                          <div className="p-3 rounded-lg bg-muted/50">
                            <p className="text-xs text-muted-foreground">Create Fee</p>
                            <p className="font-semibold">{transferConfig?.createFee?.fee ?? transferConfig?.create_fee?.fee ?? "—"}</p>
                          </div>
                          <div className="p-3 rounded-lg bg-muted/50">
                            <p className="text-xs text-muted-foreground">Max Inputs</p>
                            <p className="font-semibold">{transferConfig?.maxNumInputs ?? transferConfig?.max_num_inputs ?? "—"}</p>
                          </div>
                          <div className="p-3 rounded-lg bg-muted/50">
                            <p className="text-xs text-muted-foreground">Max Outputs</p>
                            <p className="font-semibold">{transferConfig?.maxNumOutputs ?? transferConfig?.max_num_outputs ?? "—"}</p>
                          </div>
                        </div>
                        <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
                          <div className="p-3 rounded-lg bg-muted/50">
                            <p className="text-xs text-muted-foreground">Amulet/Year</p>
                            <p className="font-semibold text-sm">{initialIssuance?.amuletToIssuePerYear ?? initialIssuance?.amulet_to_issue_per_year ?? "—"}</p>
                          </div>
                          <div className="p-3 rounded-lg bg-muted/50">
                            <p className="text-xs text-muted-foreground">Validator Reward %</p>
                            <p className="font-semibold">{initialIssuance?.validatorRewardPercentage ?? initialIssuance?.validator_reward_percentage ?? "—"}</p>
                          </div>
                          <div className="p-3 rounded-lg bg-muted/50">
                            <p className="text-xs text-muted-foreground">App Reward %</p>
                            <p className="font-semibold">{initialIssuance?.appRewardPercentage ?? initialIssuance?.app_reward_percentage ?? "—"}</p>
                          </div>
                        </div>
                        <Link to="/amulet-rules" className="text-primary hover:underline text-sm">
                          View full Amulet Rules →
                        </Link>
                      </>
                    );
                  })()}
                </div>
              ) : (
                <p className="text-muted-foreground">No Amulet Rules data available</p>
              )}
            </CardContent>
          </Card>
        </div>

        {/* DSO Sequencers */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Server className="h-5 w-5" />
              DSO Sequencers
            </CardTitle>
            <CardDescription>Domain sequencer configuration across the network</CardDescription>
          </CardHeader>
          <CardContent>
            {loadingSequencers ? (
              <div className="space-y-2">
                <Skeleton className="h-16 w-full" />
                <Skeleton className="h-16 w-full" />
              </div>
            ) : dsoSequencers?.domainSequencers?.length ? (
              <div className="space-y-4">
                {dsoSequencers.domainSequencers.map((domain, i) => (
                  <div key={i} className="border rounded-lg p-4">
                    <div className="mb-2">
                      <Badge variant="outline" className="font-mono text-xs">
                        {domain.domainId}
                      </Badge>
                    </div>
                    <div className="grid gap-2">
                      {domain.sequencers.map((seq, j) => (
                        <div key={j} className="flex items-center justify-between text-sm bg-muted/50 rounded p-2">
                          <div className="flex items-center gap-2">
                            <span className="font-medium">{seq.svName}</span>
                            <Badge variant="secondary" className="text-xs">
                              Migration {seq.migrationId}
                            </Badge>
                          </div>
                          <a href={seq.url} target="_blank" rel="noopener noreferrer" className="text-primary hover:underline font-mono text-xs">
                            {seq.url}
                          </a>
                        </div>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <p className="text-muted-foreground">No sequencer information available</p>
            )}
          </CardContent>
        </Card>
      </div>
    </DashboardLayout>
  );
}
