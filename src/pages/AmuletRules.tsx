import { useMemo } from "react";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Separator } from "@/components/ui/separator";
import { Skeleton } from "@/components/ui/skeleton";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Button } from "@/components/ui/button";
import { ChevronDown, ChevronRight } from "lucide-react";
import { useLatestACSSnapshot } from "@/hooks/use-acs-snapshots";
import { useAggregatedTemplateData } from "@/hooks/use-aggregated-template-data";
import { DataSourcesFooter } from "@/components/DataSourcesFooter";

interface NormalizedTransferStep {
  amount?: string;
  rate?: string;
}

interface NormalizedIssuanceValue {
  amuletToIssuePerYear?: string;
  validatorRewardPercentage?: string;
  appRewardPercentage?: string;
  validatorRewardCap?: string;
  featuredAppRewardCap?: string;
  unfeaturedAppRewardCap?: string;
  optValidatorFaucetCap?: string;
}

interface NormalizedIssuanceFutureValue {
  effectiveAfterMicroseconds?: string;
  values?: NormalizedIssuanceValue;
}

interface NormalizedAmuletRule {
  dso?: string;
  templateIdSuffix?: string;
  isDevNet?: boolean;
  featuredAppActivityMarkerAmount?: string;
  transferConfig?: {
    createFee?: { fee?: string };
    holdingFee?: { rate?: string };
    transferFee?: { initialRate?: string; steps?: NormalizedTransferStep[] };
    lockHolderFee?: { fee?: string };
    transferPreapprovalFee?: string | null;
    extraFeaturedAppRewardAmount?: string;
    maxNumInputs?: string;
    maxNumOutputs?: string;
    maxNumLockHolders?: string;
  };
  issuanceCurve?: {
    initialValue?: NormalizedIssuanceValue;
    futureValues?: NormalizedIssuanceFutureValue[];
  };
  decentralizedSynchronizer?: {
    requiredSynchronizers?: Array<{ required?: string; activeSynchronizer?: string }>;
    fees?: {
      baseRateTrafficLimits?: { burstAmount?: string; burstWindow?: { microseconds?: string } };
      extraTrafficPrice?: string;
      readVsWriteScalingFactor?: string;
      minTopupAmount?: string;
      tickDuration?: { microseconds?: string };
    };
  };
  packageConfig?: Record<string, string>;
  raw?: any;
}

const pickFirstDefined = <T,>(...values: Array<T | undefined | null>): T | undefined => {
  return values.find((v) => v !== undefined && v !== null);
};

const normalizeTransferSteps = (steps: any): NormalizedTransferStep[] => {
  if (!Array.isArray(steps)) return [];
  return steps
    .map((step) => {
      const amount = pickFirstDefined(
        step?.amount,
        step?.volume,
        step?.threshold,
        step?._1,
        step?.Amount,
        step?.Volume,
      );
      const rate = pickFirstDefined(step?.rate, step?.fee, step?._2, step?.Rate);
      if (amount === undefined && rate === undefined) return null;
      return { amount, rate };
    })
    .filter(Boolean) as NormalizedTransferStep[];
};

const normalizeIssuanceValue = (value: any): NormalizedIssuanceValue | undefined => {
  if (!value || typeof value !== "object") return undefined;
  return {
    amuletToIssuePerYear: pickFirstDefined(
      value.amuletToIssuePerYear,
      value.amulet_to_issue_per_year,
      value._1,
      value.amuletIssuance,
    ),
    validatorRewardPercentage: pickFirstDefined(value.validatorRewardPercentage, value.validator_reward_percentage),
    appRewardPercentage: pickFirstDefined(value.appRewardPercentage, value.app_reward_percentage),
    validatorRewardCap: pickFirstDefined(value.validatorRewardCap, value.validator_reward_cap),
    featuredAppRewardCap: pickFirstDefined(value.featuredAppRewardCap, value.featured_app_reward_cap),
    unfeaturedAppRewardCap: pickFirstDefined(value.unfeaturedAppRewardCap, value.unfeatured_app_reward_cap),
    optValidatorFaucetCap: pickFirstDefined(value.optValidatorFaucetCap, value.opt_validator_faucet_cap),
  };
};

const normalizeFutureValues = (futureValues: any): NormalizedIssuanceFutureValue[] => {
  if (!Array.isArray(futureValues)) return [];
  return futureValues
    .map((item) => {
      const effectiveAfterMicroseconds = pickFirstDefined(
        item?.effectiveAfterMicroseconds,
        item?.microseconds,
        item?.time,
        item?._1?.microseconds,
        item?._1,
        item?.effective_after_microseconds,
      );
      const values = pickFirstDefined(item?.values, item?._2, item?.futureValue, item?.value);
      const normalizedValues = normalizeIssuanceValue(values);
      if (!effectiveAfterMicroseconds && !normalizedValues) return null;
      return {
        effectiveAfterMicroseconds,
        values: normalizedValues,
      } as NormalizedIssuanceFutureValue;
    })
    .filter(Boolean) as NormalizedIssuanceFutureValue[];
};

const normalizeAmuletRule = (raw: any): NormalizedAmuletRule | null => {
  if (!raw) return null;
  const source = raw.payload ?? raw;
  const transferConfig = pickFirstDefined(
    source.transferConfig,
    source.transfer_config,
    source.TransferConfig,
    source.transfer_configSchedule,
  );
  const issuanceCurve = pickFirstDefined(source.issuanceCurve, source.issuance_curve);
  const decentralizedSynchronizer = pickFirstDefined(
    source.decentralizedSynchronizer,
    source.decentralized_synchronizer,
  );

  return {
    dso: pickFirstDefined(source.dso, source.DSO, source.owner),
    templateIdSuffix: pickFirstDefined(source.templateIdSuffix, source.template_id_suffix, "AmuletRules"),
    isDevNet: pickFirstDefined(source.isDevNet, source.is_devnet, source.is_dev_net, false),
    featuredAppActivityMarkerAmount: pickFirstDefined(
      source.featuredAppActivityMarkerAmount,
      source.featured_app_activity_marker_amount,
    ),
    transferConfig: transferConfig
      ? {
          createFee: { fee: pickFirstDefined(transferConfig.createFee?.fee, transferConfig.create_fee?.fee) },
          holdingFee: { rate: pickFirstDefined(transferConfig.holdingFee?.rate, transferConfig.holding_fee?.rate) },
          transferFee: {
            initialRate: pickFirstDefined(
              transferConfig.transferFee?.initialRate,
              transferConfig.transfer_fee?.initialRate,
              transferConfig.transfer_fee?.initial_rate,
              transferConfig.transferFee?.initial_rate,
            ),
            steps: normalizeTransferSteps(
              pickFirstDefined(
                transferConfig.transferFee?.steps,
                transferConfig.transfer_fee?.steps,
                transferConfig.transfer_fee_steps,
              ),
            ),
          },
          lockHolderFee: {
            fee: pickFirstDefined(transferConfig.lockHolderFee?.fee, transferConfig.lock_holder_fee?.fee),
          },
          transferPreapprovalFee: pickFirstDefined(
            transferConfig.transferPreapprovalFee,
            transferConfig.transfer_preapproval_fee,
          ),
          extraFeaturedAppRewardAmount: pickFirstDefined(
            transferConfig.extraFeaturedAppRewardAmount,
            transferConfig.extra_featured_app_reward_amount,
          ),
          maxNumInputs: pickFirstDefined(transferConfig.maxNumInputs, transferConfig.max_num_inputs),
          maxNumOutputs: pickFirstDefined(transferConfig.maxNumOutputs, transferConfig.max_num_outputs),
          maxNumLockHolders: pickFirstDefined(transferConfig.maxNumLockHolders, transferConfig.max_num_lock_holders),
        }
      : undefined,
    issuanceCurve: issuanceCurve
      ? {
          initialValue: normalizeIssuanceValue(
            pickFirstDefined(issuanceCurve.initialValue, issuanceCurve.initial_value),
          ),
          futureValues: normalizeFutureValues(
            pickFirstDefined(issuanceCurve.futureValues, issuanceCurve.future_values),
          ),
        }
      : undefined,
    decentralizedSynchronizer: decentralizedSynchronizer
      ? {
          requiredSynchronizers: pickFirstDefined(
            decentralizedSynchronizer.requiredSynchronizers,
            decentralizedSynchronizer.required_synchronizers,
          ),
          fees: decentralizedSynchronizer.fees,
        }
      : undefined,
    packageConfig: pickFirstDefined(source.packageConfig, source.package_config),
    raw,
  };
};

const formatMicroseconds = (value?: string) => {
  if (!value) return "—";
  const micros = Number(value);
  const days = micros / 1_000_000 / 86_400;
  if (Number.isNaN(days)) return `${value} µs`;
  return `${value} µs (${days.toFixed(2)} days)`;
};

const truncateIdentifier = (value?: string) =>
  value && value.length > 24 ? `${value.slice(0, 18)}…${value.slice(-6)}` : value || "—";

const AmuletRules = () => {
  const { data: latestSnapshot, isLoading: snapshotLoading } = useLatestACSSnapshot();
  const amuletRulesQuery = useAggregatedTemplateData(
    latestSnapshot?.id,
    "Splice:AmuletRules:AmuletRules",
    !!latestSnapshot,
  );

  const normalizedRule = useMemo(() => normalizeAmuletRule(amuletRulesQuery.data?.data?.[0]), [amuletRulesQuery.data]);

  const transferConfig = normalizedRule?.transferConfig;
  const issuanceCurve = normalizedRule?.issuanceCurve;
  const synchronizer = normalizedRule?.decentralizedSynchronizer;

  const isLoading = snapshotLoading || amuletRulesQuery.isLoading;
  const hasData = !!normalizedRule;

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div className="space-y-2">
          <div className="flex items-center gap-3">
            <Badge variant="outline" className="text-xs font-semibold">
              {normalizedRule?.templateIdSuffix || "AmuletRules"}
            </Badge>
            <Badge variant="secondary" className="text-xs">
              {normalizedRule?.isDevNet ? "Development" : "Production"}
            </Badge>
            {amuletRulesQuery.data?.templateCount !== undefined && (
              <Badge variant="outline" className="text-xs">
                {amuletRulesQuery.data.templateCount} template package(s)
              </Badge>
            )}
          </div>
          <h2 className="text-3xl font-bold">Amulet Rules Template</h2>
          <p className="text-muted-foreground max-w-3xl">
            Live configuration values pulled from the latest ACS snapshot for templates ending in AmuletRules. This view
            updates automatically when new snapshots are available, so values stay current.
          </p>
        </div>

        {isLoading && (
          <div className="grid gap-4">
            <Skeleton className="h-24 w-full" />
            <Skeleton className="h-80 w-full" />
          </div>
        )}

        {!isLoading && !hasData && (
          <Alert>
            <AlertTitle>No AmuletRules data found</AlertTitle>
            <AlertDescription>
              We couldn't find any AmuletRules contracts in the latest snapshot. Trigger a new snapshot or verify that
              the template exists in the environment.
            </AlertDescription>
          </Alert>
        )}

        {hasData && (
          <>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <Card>
                <CardHeader>
                  <CardTitle className="text-sm text-muted-foreground">DSO</CardTitle>
                </CardHeader>
                <CardContent>
                  <p className="font-mono text-sm break-all">{truncateIdentifier(normalizedRule?.dso)}</p>
                </CardContent>
              </Card>
              <Card>
                <CardHeader>
                  <CardTitle className="text-sm text-muted-foreground">Featured App Marker</CardTitle>
                </CardHeader>
                <CardContent>
                  <p className="text-2xl font-semibold">{normalizedRule?.featuredAppActivityMarkerAmount || "—"}</p>
                  <p className="text-xs text-muted-foreground">Minimum amount to mark featured app activity</p>
                </CardContent>
              </Card>
              <Card>
                <CardHeader>
                  <CardTitle className="text-sm text-muted-foreground">Transfer Limits</CardTitle>
                </CardHeader>
                <CardContent className="space-y-1 text-sm">
                  <div className="flex justify-between">
                    <span>Inputs</span>
                    <span className="font-semibold">{transferConfig?.maxNumInputs || "—"}</span>
                  </div>
                  <div className="flex justify-between">
                    <span>Outputs</span>
                    <span className="font-semibold">{transferConfig?.maxNumOutputs || "—"}</span>
                  </div>
                  <div className="flex justify-between">
                    <span>Lock Holders</span>
                    <span className="font-semibold">{transferConfig?.maxNumLockHolders || "—"}</span>
                  </div>
                </CardContent>
              </Card>
            </div>

            <Card>
              <CardHeader>
                <CardTitle>Transfer Configuration</CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
                  <div className="p-4 rounded-lg bg-muted/50 space-y-2">
                    <p className="text-muted-foreground text-xs uppercase">Create Fee</p>
                    <p className="text-2xl font-semibold">{transferConfig?.createFee?.fee || "—"}</p>
                    <p className="text-muted-foreground text-xs">Charged when a transfer is created</p>
                  </div>
                  <div className="p-4 rounded-lg bg-muted/50 space-y-2">
                    <p className="text-muted-foreground text-xs uppercase">Holding Fee</p>
                    <p className="text-2xl font-semibold">{transferConfig?.holdingFee?.rate || "—"}</p>
                    <p className="text-muted-foreground text-xs">Rate applied to held balances</p>
                  </div>
                  <div className="p-4 rounded-lg bg-muted/50 space-y-2">
                    <p className="text-muted-foreground text-xs uppercase">Lock Holder Fee</p>
                    <p className="text-2xl font-semibold">{transferConfig?.lockHolderFee?.fee || "—"}</p>
                    <p className="text-muted-foreground text-xs">Fee for maintaining lock holders</p>
                  </div>
                </div>

                <Separator />

                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <div className="space-y-3">
                    <div className="flex items-center justify-between">
                      <h3 className="font-semibold">Transfer Fee Steps</h3>
                      <Badge variant="outline">Initial rate {transferConfig?.transferFee?.initialRate || "—"}</Badge>
                    </div>
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead>Volume Threshold</TableHead>
                          <TableHead className="text-right">Rate</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {(transferConfig?.transferFee?.steps || []).length === 0 ? (
                          <TableRow>
                            <TableCell colSpan={2} className="text-center text-muted-foreground">
                              No transfer fee steps configured
                            </TableCell>
                          </TableRow>
                        ) : (
                          transferConfig?.transferFee?.steps?.map((step, index) => (
                            <TableRow key={`${step.amount}-${index}`}>
                              <TableCell className="font-medium">{step.amount || "—"}</TableCell>
                              <TableCell className="text-right">{step.rate || "—"}</TableCell>
                            </TableRow>
                          ))
                        )}
                      </TableBody>
                    </Table>
                  </div>

                  <div className="p-4 rounded-lg bg-muted/50 space-y-2 h-full">
                    <div className="flex items-center justify-between">
                      <h3 className="font-semibold">Preapproval Fee</h3>
                      <Badge variant="secondary">
                        {transferConfig?.transferPreapprovalFee ? "Configured" : "Not set"}
                      </Badge>
                    </div>
                    <p className="text-sm text-muted-foreground">
                      This template currently {transferConfig?.transferPreapprovalFee ? "charges" : "does not charge"} a
                      fee for preapproving transfers.
                    </p>
                    {transferConfig?.extraFeaturedAppRewardAmount && (
                      <div className="text-sm">
                        <p className="text-muted-foreground text-xs uppercase mb-1">Extra Featured App Reward</p>
                        <p className="font-semibold">{transferConfig.extraFeaturedAppRewardAmount}</p>
                      </div>
                    )}
                  </div>
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>Issuance Curve</CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
                  <div className="p-4 rounded-lg bg-muted/50 space-y-1">
                    <p className="text-xs text-muted-foreground uppercase">Yearly issuance</p>
                    <p className="text-2xl font-semibold">{issuanceCurve?.initialValue?.amuletToIssuePerYear || "—"}</p>
                  </div>
                  <div className="p-4 rounded-lg bg-muted/50 space-y-1">
                    <p className="text-xs text-muted-foreground uppercase">Validator reward %</p>
                    <p className="text-2xl font-semibold">
                      {issuanceCurve?.initialValue?.validatorRewardPercentage || "—"}
                    </p>
                  </div>
                  <div className="p-4 rounded-lg bg-muted/50 space-y-1">
                    <p className="text-xs text-muted-foreground uppercase">App reward %</p>
                    <p className="text-2xl font-semibold">{issuanceCurve?.initialValue?.appRewardPercentage || "—"}</p>
                  </div>
                </div>

                <Separator />

                <div className="space-y-3">
                  <div className="flex items-center justify-between">
                    <h3 className="font-semibold">Future Values</h3>
                    <Badge variant="outline">{issuanceCurve?.futureValues?.length || 0} scheduled</Badge>
                  </div>
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Effective After</TableHead>
                        <TableHead className="text-right">Yearly Issuance</TableHead>
                        <TableHead className="text-right">Validator %</TableHead>
                        <TableHead className="text-right">App %</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {(issuanceCurve?.futureValues || []).length === 0 ? (
                        <TableRow>
                          <TableCell colSpan={4} className="text-center text-muted-foreground">
                            No future issuance values configured
                          </TableCell>
                        </TableRow>
                      ) : (
                        issuanceCurve?.futureValues?.map((future, index) => (
                          <TableRow key={`${future.effectiveAfterMicroseconds}-${index}`}>
                            <TableCell>{formatMicroseconds(future.effectiveAfterMicroseconds)}</TableCell>
                            <TableCell className="text-right">{future.values?.amuletToIssuePerYear || "—"}</TableCell>
                            <TableCell className="text-right">
                              {future.values?.validatorRewardPercentage || "—"}
                            </TableCell>
                            <TableCell className="text-right">{future.values?.appRewardPercentage || "—"}</TableCell>
                          </TableRow>
                        ))
                      )}
                    </TableBody>
                  </Table>
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>Synchronizer Traffic</CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
                  <div className="p-4 rounded-lg bg-muted/50 space-y-1">
                    <p className="text-xs text-muted-foreground uppercase">Burst amount</p>
                    <p className="text-xl font-semibold">
                      {synchronizer?.fees?.baseRateTrafficLimits?.burstAmount || "—"}
                    </p>
                    <p className="text-xs text-muted-foreground">
                      Window {formatMicroseconds(synchronizer?.fees?.baseRateTrafficLimits?.burstWindow?.microseconds)}
                    </p>
                  </div>
                  <div className="p-4 rounded-lg bg-muted/50 space-y-1">
                    <p className="text-xs text-muted-foreground uppercase">Tick duration</p>
                    <p className="text-xl font-semibold">
                      {formatMicroseconds(synchronizer?.fees?.tickDuration?.microseconds)}
                    </p>
                    <p className="text-xs text-muted-foreground">
                      Read vs write scaling {synchronizer?.fees?.readVsWriteScalingFactor || "—"}
                    </p>
                  </div>
                </div>

                <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
                  <div className="p-4 rounded-lg bg-muted/50 space-y-1">
                    <p className="text-xs text-muted-foreground uppercase">Extra traffic price</p>
                    <p className="text-xl font-semibold">{synchronizer?.fees?.extraTrafficPrice || "—"}</p>
                  </div>
                  <div className="p-4 rounded-lg bg-muted/50 space-y-1">
                    <p className="text-xs text-muted-foreground uppercase">Min top-up</p>
                    <p className="text-xl font-semibold">{synchronizer?.fees?.minTopupAmount || "—"}</p>
                  </div>
                  <div className="p-4 rounded-lg bg-muted/50 space-y-1">
                    <p className="text-xs text-muted-foreground uppercase">Synchronizers</p>
                    <p className="text-xl font-semibold">{synchronizer?.requiredSynchronizers?.length || 0}</p>
                  </div>
                </div>

                {(synchronizer?.requiredSynchronizers || []).length > 0 && (
                  <div className="space-y-2">
                    <h4 className="text-sm font-semibold">Synchronizer endpoints</h4>
                    <div className="space-y-2">
                      {synchronizer?.requiredSynchronizers?.map((sync, index) => (
                        <div key={`${sync.required}-${index}`} className="p-3 rounded-md border text-sm">
                          <div className="flex justify-between">
                            <span className="text-muted-foreground">Required</span>
                            <span className="font-mono">{truncateIdentifier(sync.required)}</span>
                          </div>
                          {sync.activeSynchronizer && (
                            <div className="flex justify-between">
                              <span className="text-muted-foreground">Active</span>
                              <span className="font-mono">{truncateIdentifier(sync.activeSynchronizer)}</span>
                            </div>
                          )}
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>Package Versions</CardTitle>
              </CardHeader>
              <CardContent>
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Package</TableHead>
                      <TableHead className="text-right">Version</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {normalizedRule?.packageConfig ? (
                      Object.entries(normalizedRule.packageConfig).map(([pkg, version]) => (
                        <TableRow key={pkg}>
                          <TableCell className="font-medium">{pkg}</TableCell>
                          <TableCell className="text-right">{version}</TableCell>
                        </TableRow>
                      ))
                    ) : (
                      <TableRow>
                        <TableCell colSpan={2} className="text-center text-muted-foreground">
                          No package config found
                        </TableCell>
                      </TableRow>
                    )}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>

            <Card>
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-4">
                <CardTitle className="text-base">Raw AmuletRules Contract</CardTitle>
                <Badge variant="outline">{amuletRulesQuery.data?.totalContracts || 0} total</Badge>
              </CardHeader>
              <CardContent>
                <Collapsible>
                  <CollapsibleTrigger asChild>
                    <Button variant="ghost" size="sm" className="group flex items-center gap-2">
                      <ChevronRight className="h-4 w-4 group-data-[state=open]:hidden" />
                      <ChevronDown className="h-4 w-4 hidden group-data-[state=open]:block" />
                      Toggle raw JSON
                    </Button>
                  </CollapsibleTrigger>
                  <CollapsibleContent className="mt-3">
                    <div className="p-4 rounded-lg bg-muted/50">
                      <pre className="text-xs overflow-auto max-h-96">
                        {JSON.stringify(normalizedRule?.raw, null, 2)}
                      </pre>
                    </div>
                  </CollapsibleContent>
                </Collapsible>
              </CardContent>
            </Card>
          </>
        )}

        <DataSourcesFooter
          snapshotId={latestSnapshot?.id}
          templateSuffixes={["Splice:AmuletRules:AmuletRules"]}
          isProcessing={latestSnapshot?.status === "processing"}
        />
      </div>
    </DashboardLayout>
  );
};

export default AmuletRules;
