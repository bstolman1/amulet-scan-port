import { useMemo, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Separator } from "@/components/ui/separator";
import { Skeleton } from "@/components/ui/skeleton";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Button } from "@/components/ui/button";
import { ChevronRight } from "lucide-react";
import { useAmuletRules } from "@/hooks/use-canton-scan-api";
import { PieChart, Pie, Cell, ResponsiveContainer, Legend, Tooltip } from "recharts";

const REWARD_COLORS = {
  validator: "hsl(var(--chart-1))",
  app: "hsl(var(--chart-2))",
  sv: "hsl(var(--chart-3))",
};

interface RewardDistributionChartProps {
  validatorPct: number;
  appPct: number;
  svPct: number;
}

const RewardDistributionChart = ({ validatorPct, appPct, svPct }: RewardDistributionChartProps) => {
  const data = [
    { name: "Validator", value: validatorPct * 100, color: REWARD_COLORS.validator },
    { name: "App", value: appPct * 100, color: REWARD_COLORS.app },
    { name: "SV", value: svPct * 100, color: REWARD_COLORS.sv },
  ];

  return (
    <div className="h-[180px] w-full flex items-center justify-center">
      <ResponsiveContainer width="100%" height="100%">
        <PieChart>
          <Pie
            data={data}
            cx="50%"
            cy="50%"
            innerRadius={45}
            outerRadius={70}
            paddingAngle={3}
            dataKey="value"
          >
            {data.map((entry, index) => (
              <Cell key={`cell-${index}`} fill={entry.color} />
            ))}
          </Pie>
          <Tooltip 
            formatter={(value: number) => [`${value.toFixed(1)}%`, "Share"]}
            contentStyle={{ 
              backgroundColor: "hsl(222 47% 11%)", 
              border: "1px solid hsl(217 33% 17%)",
              borderRadius: "8px",
              color: "hsl(210 40% 98%)"
            }}
            itemStyle={{ color: "hsl(210 40% 98%)" }}
            labelStyle={{ color: "hsl(215 20% 65%)" }}
          />
          <Legend 
            verticalAlign="middle" 
            align="right" 
            layout="vertical"
            wrapperStyle={{ paddingLeft: "20px" }}
            formatter={(value, entry: any) => (
              <span className="text-sm text-foreground">{value}: {entry.payload.value.toFixed(0)}%</span>
            )}
          />
        </PieChart>
      </ResponsiveContainer>
    </div>
  );
};

const formatLargeNumber = (value?: string | number): string => {
  if (value === undefined || value === null) return "—";
  const num = typeof value === "string" ? parseFloat(value) : value;
  if (isNaN(num)) return "—";
  return num.toLocaleString("en-US", { maximumFractionDigits: 2 });
};

interface IssuanceTimelineProps {
  initialValue: NormalizedIssuanceValue | undefined;
  futureValues: NormalizedIssuanceFutureValue[] | undefined;
}

const IssuanceTimeline = ({ initialValue, futureValues }: IssuanceTimelineProps) => {
  const elapsedMicros = getMicrosecondsSinceLaunch();
  const launchDate = new Date("2024-07-01T00:00:00Z");
  
  const formatDuration = (micros: number): string => {
    const days = micros / 1_000_000 / 86_400;
    if (days < 365) return `${Math.floor(days)} days`;
    const years = days / 365;
    return `${years.toFixed(1)} years`;
  };

  const formatDate = (micros: number): string => {
    const date = new Date(launchDate.getTime() + micros / 1000);
    return date.toLocaleDateString("en-US", { month: "short", year: "numeric" });
  };

  const stages = [
    { 
      stage: 0, 
      label: "Stage 0", 
      effectiveMicros: 0, 
      values: initialValue,
      isActive: !futureValues?.length || elapsedMicros < Number(futureValues[0].effectiveAfterMicroseconds || 0)
    },
    ...(futureValues || []).map((fv, idx) => {
      const threshold = Number(fv.effectiveAfterMicroseconds || 0);
      const nextThreshold = futureValues?.[idx + 1] 
        ? Number(futureValues[idx + 1].effectiveAfterMicroseconds || 0) 
        : Infinity;
      return {
        stage: idx + 1,
        label: `Stage ${idx + 1}`,
        effectiveMicros: threshold,
        values: fv.values,
        isActive: elapsedMicros >= threshold && elapsedMicros < nextThreshold
      };
    })
  ];

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between text-sm">
        <span className="text-muted-foreground">Network Launch: Jul 2024</span>
        <span className="font-medium text-primary">Elapsed: {formatDuration(elapsedMicros)}</span>
      </div>
      <div className="relative">
        {/* Timeline bar */}
        <div className="absolute left-3 top-0 bottom-0 w-0.5 bg-border" />
        
        <div className="space-y-1">
          {stages.map((s, idx) => {
            const isPast = elapsedMicros > s.effectiveMicros && !s.isActive;
            const isFuture = elapsedMicros < s.effectiveMicros;
            
            return (
              <div key={idx} className="relative pl-8">
                {/* Timeline dot */}
                <div className={`absolute left-1.5 top-3 w-3 h-3 rounded-full border-2 ${
                  s.isActive 
                    ? "bg-primary border-primary ring-4 ring-primary/20" 
                    : isPast 
                      ? "bg-muted-foreground/50 border-muted-foreground/50" 
                      : "bg-background border-muted-foreground/30"
                }`} />
                
                <div className={`p-3 rounded-lg text-sm transition-all ${
                  s.isActive 
                    ? "bg-primary/10 border border-primary/30" 
                    : isPast
                      ? "bg-muted/30 opacity-60"
                      : "bg-muted/20"
                }`}>
                  <div className="flex items-center justify-between mb-1">
                    <div className="flex items-center gap-2">
                      <span className="font-semibold">{s.label}</span>
                      {s.isActive && <Badge variant="default" className="text-xs">Current</Badge>}
                      {isFuture && idx === stages.findIndex(st => elapsedMicros < st.effectiveMicros) && (
                        <Badge variant="outline" className="text-xs">Next</Badge>
                      )}
                    </div>
                    <span className="text-xs text-muted-foreground">
                      {s.effectiveMicros === 0 ? "Launch" : formatDate(s.effectiveMicros)}
                    </span>
                  </div>
                  <div className="flex items-center gap-4 text-xs">
                    <span>
                      <span className="text-muted-foreground">Issuance:</span>{" "}
                      <span className="font-medium">{formatLargeNumber(s.values?.amuletToIssuePerYear)}/yr</span>
                    </span>
                    <span className="text-muted-foreground">
                      V:{(parseFloat(s.values?.validatorRewardPercentage || "0") * 100).toFixed(0)}% 
                      {" "}A:{(parseFloat(s.values?.appRewardPercentage || "0") * 100).toFixed(0)}%
                    </span>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
};

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
  
  const payload = raw.contract?.payload ?? raw.payload ?? raw;
  const configSchedule = pickFirstDefined(payload.configSchedule, payload.config_schedule);
  const configInitialValue = pickFirstDefined(configSchedule?.initialValue, configSchedule?.initial_value);
  const source = configInitialValue ?? payload;
  
  const transferConfig = pickFirstDefined(source.transferConfig, source.transfer_config);
  const issuanceCurve = pickFirstDefined(source.issuanceCurve, source.issuance_curve);

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
    raw,
  };
};

const NETWORK_LAUNCH_DATE = new Date("2024-07-01T00:00:00Z");

const getMicrosecondsSinceLaunch = (): number => {
  const now = new Date();
  return (now.getTime() - NETWORK_LAUNCH_DATE.getTime()) * 1000;
};

const formatMicroseconds = (value?: string) => {
  if (!value) return "—";
  const micros = Number(value);
  const days = micros / 1_000_000 / 86_400;
  if (Number.isNaN(days)) return `${value} µs`;
  return `${value} µs (${days.toFixed(2)} days)`;
};

const getCurrentIssuanceStage = (
  initialValue: NormalizedIssuanceValue | undefined,
  futureValues: NormalizedIssuanceFutureValue[] | undefined
): { stage: number; label: string; values: NormalizedIssuanceValue | undefined } => {
  const elapsedMicros = getMicrosecondsSinceLaunch();
  
  if (!futureValues || futureValues.length === 0) {
    return { stage: 0, label: "Initial", values: initialValue };
  }

  // Find the current active stage
  let currentStage = 0;
  let currentValues = initialValue;
  
  for (let i = 0; i < futureValues.length; i++) {
    const threshold = Number(futureValues[i].effectiveAfterMicroseconds || 0);
    if (elapsedMicros >= threshold) {
      currentStage = i + 1;
      currentValues = futureValues[i].values;
    } else {
      break;
    }
  }

  const label = currentStage === 0 ? "Initial" : `Stage ${currentStage}`;
  return { stage: currentStage, label, values: currentValues };
};

const truncateIdentifier = (value?: string) =>
  value && value.length > 24 ? `${value.slice(0, 18)}…${value.slice(-6)}` : value || "—";

export function AmuletRulesContent() {
  const { data: amuletRulesData, isLoading } = useAmuletRules();
  const [jsonOpen, setJsonOpen] = useState(false);

  const normalizedRule = useMemo(() => normalizeAmuletRule(amuletRulesData), [amuletRulesData]);
  const transferConfig = normalizedRule?.transferConfig;
  const issuanceCurve = normalizedRule?.issuanceCurve;
  const hasData = !!normalizedRule;

  if (isLoading) {
    return (
      <div className="space-y-4">
        <Skeleton className="h-24 w-full" />
        <Skeleton className="h-80 w-full" />
      </div>
    );
  }

  if (!hasData) {
    return (
      <Alert>
        <AlertTitle>No AmuletRules data found</AlertTitle>
        <AlertDescription>
          Unable to fetch AmuletRules configuration from the Canton Scan API.
        </AlertDescription>
      </Alert>
    );
  }

  return (
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
        <h2 className="text-2xl font-bold">Amulet Rules</h2>
        <p className="text-muted-foreground text-sm">
          Live configuration values from the Canton Scan API <code className="text-xs">/v0/dso</code> endpoint.
        </p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm text-muted-foreground">DSO</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="font-mono text-sm break-all">{truncateIdentifier(normalizedRule?.dso)}</p>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm text-muted-foreground">Featured App Marker</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-2xl font-semibold">{normalizedRule?.featuredAppActivityMarkerAmount || "—"}</p>
            <p className="text-xs text-muted-foreground">Minimum amount to mark featured app activity</p>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="pb-2">
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
            {(() => {
              const { label, values } = getCurrentIssuanceStage(
                issuanceCurve.initialValue,
                issuanceCurve.futureValues
              );
              const validatorPct = parseFloat(values?.validatorRewardPercentage || "0");
              const appPct = parseFloat(values?.appRewardPercentage || "0");
              const svPct = 1 - validatorPct - appPct;
              
              return (
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                  {/* Current Stage Stats */}
                  <div className="p-4 rounded-lg bg-primary/10 border border-primary/20">
                    <div className="flex items-center gap-2 mb-4">
                      <h3 className="font-semibold">Current Active Stage</h3>
                      <Badge variant="default">{label}</Badge>
                    </div>
                    <div className="grid grid-cols-2 gap-x-6 gap-y-3 text-sm mb-4">
                      <div>
                        <p className="text-xs text-muted-foreground">Amulet/Year</p>
                        <p className="font-semibold text-lg">{formatLargeNumber(values?.amuletToIssuePerYear)}</p>
                      </div>
                      <div>
                        <p className="text-xs text-muted-foreground">Validator Cap</p>
                        <p className="font-semibold text-lg">{formatLargeNumber(values?.validatorRewardCap)}</p>
                      </div>
                    </div>
                    <RewardDistributionChart 
                      validatorPct={validatorPct} 
                      appPct={appPct} 
                      svPct={svPct} 
                    />
                  </div>

                  {/* Timeline */}
                  <div className="p-4 rounded-lg bg-muted/30 border border-border/50">
                    <h3 className="font-semibold mb-4">Issuance Schedule</h3>
                    <IssuanceTimeline 
                      initialValue={issuanceCurve.initialValue}
                      futureValues={issuanceCurve.futureValues}
                    />
                  </div>
                </div>
              );
            })()}
          </CardContent>
        </Card>
      )}

      <Collapsible open={jsonOpen} onOpenChange={setJsonOpen}>
        <CollapsibleTrigger asChild>
          <Button variant="outline" className="w-full justify-start">
            <ChevronRight className={`h-4 w-4 mr-2 transition-transform ${jsonOpen ? "rotate-90" : ""}`} />
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
    </div>
  );
}
