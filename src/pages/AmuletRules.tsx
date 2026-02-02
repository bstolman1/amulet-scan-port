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
import { useAmuletRules } from "@/hooks/use-canton-scan-api";

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
      const amount = pickFirstDefined(step?.amount, step?.volume, step?.threshold, step?._1);
      const rate = pickFirstDefined(step?.rate, step?.fee, step?._2);
      if (amount === undefined && rate === undefined) return null;
      return { amount, rate };
    })
    .filter(Boolean) as NormalizedTransferStep[];
};

const normalizeIssuanceValue = (value: any): NormalizedIssuanceValue | undefined => {
  if (!value || typeof value !== "object") return undefined;
  return {
    amuletToIssuePerYear: pickFirstDefined(value.amuletToIssuePerYear, value.amulet_to_issue_per_year),
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
      const effectiveAfterMicroseconds = pickFirstDefined(item?.effectiveAfterMicroseconds, item?._1?.microseconds, item?._1);
      const values = pickFirstDefined(item?.values, item?._2);
      const normalizedValues = normalizeIssuanceValue(values);
      if (!effectiveAfterMicroseconds && !normalizedValues) return null;
      return { effectiveAfterMicroseconds, values: normalizedValues } as NormalizedIssuanceFutureValue;
    })
    .filter(Boolean) as NormalizedIssuanceFutureValue[];
};

const normalizeAmuletRule = (raw: any): NormalizedAmuletRule | null => {
  if (!raw) return null;
  
  // Navigate to the payload - handle multiple nesting levels
  const payload = raw.contract?.payload ?? raw.payload ?? raw;
  
  // The actual config can be at the root of payload OR inside configSchedule.initialValue
  const configSchedule = pickFirstDefined(payload.configSchedule, payload.config_schedule);
  const configInitialValue = pickFirstDefined(configSchedule?.initialValue, configSchedule?.initial_value);
  
  // Use configSchedule.initialValue if available, otherwise fall back to payload root
  const source = configInitialValue ?? payload;
  
  const transferConfig = pickFirstDefined(source.transferConfig, source.transfer_config);
  const issuanceCurve = pickFirstDefined(source.issuanceCurve, source.issuance_curve);
  const decentralizedSynchronizer = pickFirstDefined(source.decentralizedSynchronizer, source.decentralized_synchronizer);
  const packageConfig = pickFirstDefined(source.packageConfig, source.package_config);

  return {
    dso: pickFirstDefined(payload.dso, payload.DSO, payload.owner),
    templateIdSuffix: "AmuletRules",
    isDevNet: pickFirstDefined(payload.isDevNet, payload.is_devnet, false),
    featuredAppActivityMarkerAmount: pickFirstDefined(source.featuredAppActivityMarkerAmount, source.featured_app_activity_marker_amount),
    transferConfig: transferConfig
      ? {
          createFee: { fee: pickFirstDefined(transferConfig.createFee?.fee, transferConfig.create_fee?.fee) },
          holdingFee: { rate: pickFirstDefined(transferConfig.holdingFee?.rate, transferConfig.holding_fee?.rate) },
          transferFee: {
            initialRate: pickFirstDefined(transferConfig.transferFee?.initialRate, transferConfig.transfer_fee?.initial_rate),
            steps: normalizeTransferSteps(pickFirstDefined(transferConfig.transferFee?.steps, transferConfig.transfer_fee?.steps)),
          },
          lockHolderFee: { fee: pickFirstDefined(transferConfig.lockHolderFee?.fee, transferConfig.lock_holder_fee?.fee) },
          transferPreapprovalFee: pickFirstDefined(transferConfig.transferPreapprovalFee, transferConfig.transfer_preapproval_fee),
          extraFeaturedAppRewardAmount: pickFirstDefined(transferConfig.extraFeaturedAppRewardAmount, transferConfig.extra_featured_app_reward_amount),
          maxNumInputs: pickFirstDefined(transferConfig.maxNumInputs, transferConfig.max_num_inputs),
          maxNumOutputs: pickFirstDefined(transferConfig.maxNumOutputs, transferConfig.max_num_outputs),
          maxNumLockHolders: pickFirstDefined(transferConfig.maxNumLockHolders, transferConfig.max_num_lock_holders),
        }
      : undefined,
    issuanceCurve: issuanceCurve
      ? {
          initialValue: normalizeIssuanceValue(pickFirstDefined(issuanceCurve.initialValue, issuanceCurve.initial_value)),
          futureValues: normalizeFutureValues(pickFirstDefined(issuanceCurve.futureValues, issuanceCurve.future_values)),
        }
      : undefined,
    decentralizedSynchronizer: decentralizedSynchronizer
      ? {
          requiredSynchronizers: pickFirstDefined(decentralizedSynchronizer.requiredSynchronizers, decentralizedSynchronizer.required_synchronizers),
          fees: decentralizedSynchronizer.fees,
        }
      : undefined,
    packageConfig: packageConfig,
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
  const { data: amuletRulesData, isLoading } = useAmuletRules();

  const normalizedRule = useMemo(() => normalizeAmuletRule(amuletRulesData), [amuletRulesData]);
  const transferConfig = normalizedRule?.transferConfig;
  const issuanceCurve = normalizedRule?.issuanceCurve;
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
          </div>
          <h2 className="text-3xl font-bold">Amulet Rules</h2>
          <p className="text-muted-foreground max-w-3xl">
            Live configuration values from the Canton Scan API <code>/v0/dso</code> endpoint. This view updates automatically when new data is available.
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
              Unable to fetch AmuletRules configuration from the Canton Scan API.
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
                  </div>
                </div>
              </CardContent>
            </Card>

            {issuanceCurve && (
              <Card>
                <CardHeader>
                  <CardTitle>Issuance Curve</CardTitle>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="p-4 rounded-lg bg-muted/50">
                    <h3 className="font-semibold mb-3">Initial Values</h3>
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
                      <div>
                        <p className="text-xs text-muted-foreground">Amulet/Year</p>
                        <p className="font-semibold">{issuanceCurve.initialValue?.amuletToIssuePerYear || "—"}</p>
                      </div>
                      <div>
                        <p className="text-xs text-muted-foreground">Validator %</p>
                        <p className="font-semibold">{issuanceCurve.initialValue?.validatorRewardPercentage || "—"}</p>
                      </div>
                      <div>
                        <p className="text-xs text-muted-foreground">App %</p>
                        <p className="font-semibold">{issuanceCurve.initialValue?.appRewardPercentage || "—"}</p>
                      </div>
                      <div>
                        <p className="text-xs text-muted-foreground">Validator Cap</p>
                        <p className="font-semibold">{issuanceCurve.initialValue?.validatorRewardCap || "—"}</p>
                      </div>
                    </div>
                  </div>

                  {issuanceCurve.futureValues && issuanceCurve.futureValues.length > 0 && (
                    <div>
                      <h3 className="font-semibold mb-3">Future Value Schedule ({issuanceCurve.futureValues.length} entries)</h3>
                      <div className="space-y-2">
                        {issuanceCurve.futureValues.slice(0, 5).map((fv, idx) => (
                          <div key={idx} className="p-3 rounded bg-muted/30 text-sm">
                            <p className="text-xs text-muted-foreground">
                              Effective after: {formatMicroseconds(fv.effectiveAfterMicroseconds)}
                            </p>
                            <p className="font-medium">
                              Amulet/Year: {fv.values?.amuletToIssuePerYear || "—"}
                            </p>
                          </div>
                        ))}
                        {issuanceCurve.futureValues.length > 5 && (
                          <p className="text-xs text-muted-foreground">
                            ... and {issuanceCurve.futureValues.length - 5} more entries
                          </p>
                        )}
                      </div>
                    </div>
                  )}
                </CardContent>
              </Card>
            )}

            <Collapsible>
              <CollapsibleTrigger asChild>
                <Button variant="outline" className="w-full justify-start">
                  <ChevronRight className="h-4 w-4 mr-2" />
                  View Raw JSON
                </Button>
              </CollapsibleTrigger>
              <CollapsibleContent>
                <Card className="mt-2">
                  <CardContent className="pt-4">
                    <pre className="text-xs overflow-auto max-h-96 bg-muted p-4 rounded">
                      {JSON.stringify(normalizedRule?.raw, null, 2)}
                    </pre>
                  </CardContent>
                </Card>
              </CollapsibleContent>
            </Collapsible>
          </>
        )}

        <Card className="p-4 text-xs text-muted-foreground">
          <p>Data sourced directly from Canton Scan API <code>/v0/dso</code> endpoint.</p>
        </Card>
      </div>
    </DashboardLayout>
  );
};

export default AmuletRules;