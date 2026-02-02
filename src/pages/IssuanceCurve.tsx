import { useMemo, useState } from "react";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Button } from "@/components/ui/button";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { ChevronRight, TrendingUp, Info } from "lucide-react";
import { useAmuletRules, useLatestRound } from "@/hooks/use-canton-scan-api";
import { PieChart, Pie, Cell, ResponsiveContainer, Legend, Tooltip as RechartsTooltip } from "recharts";

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
    <div className="h-[280px] w-full flex items-center justify-center">
      <ResponsiveContainer width="100%" height="100%">
        <PieChart>
          <Pie
            data={data}
            cx="40%"
            cy="50%"
            innerRadius={70}
            outerRadius={110}
            paddingAngle={3}
            dataKey="value"
          >
            {data.map((entry, index) => (
              <Cell key={`cell-${index}`} fill={entry.color} />
            ))}
          </Pie>
          <RechartsTooltip 
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
            wrapperStyle={{ paddingLeft: "30px" }}
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

interface StageActivationInfo {
  stage: number;
  label: string;
  activationTimestampMicros: number;
  estimatedDate: string;
  actualActivationRound: number | null;
  effectiveAt: string | null;
  status: "Active" | "Upcoming";
  values: NormalizedIssuanceValue | undefined;
}

interface IssuanceTimelineProps {
  initialValue: NormalizedIssuanceValue | undefined;
  futureValues: NormalizedIssuanceFutureValue[] | undefined;
  latestRound: { round: number; effectiveAt: string } | undefined;
}

const NETWORK_LAUNCH_DATE = new Date("2024-07-01T00:00:00Z");

/**
 * Convert microseconds since launch to an absolute timestamp
 */
const microsToTimestamp = (micros: number): number => {
  return NETWORK_LAUNCH_DATE.getTime() + micros / 1000;
};

/**
 * Format a timestamp as estimated date (e.g., "~Jan 2026")
 */
const formatEstimatedDate = (timestampMs: number): string => {
  const date = new Date(timestampMs);
  return `~${date.toLocaleDateString("en-US", { month: "short", year: "numeric" })}`;
};

/**
 * Format a timestamp as full UTC date/time
 */
const formatEffectiveAt = (isoString: string): string => {
  const date = new Date(isoString);
  return date.toISOString().replace("T", " ").replace(/\.\d+Z$/, " UTC");
};

/**
 * Determine if a stage is active based on the latest round's effectiveAt
 * A stage is active when: round.effectiveAt >= stage.activationTimestamp
 */
const computeStageActivation = (
  initialValue: NormalizedIssuanceValue | undefined,
  futureValues: NormalizedIssuanceFutureValue[] | undefined,
  latestRound: { round: number; effectiveAt: string } | undefined
): StageActivationInfo[] => {
  const latestEffectiveAtMs = latestRound ? new Date(latestRound.effectiveAt).getTime() : Date.now();
  
  const stages: StageActivationInfo[] = [];
  
  // Stage 0 - always active from launch
  stages.push({
    stage: 0,
    label: "Stage 0",
    activationTimestampMicros: 0,
    estimatedDate: "Jul 2024",
    actualActivationRound: 0, // Round 0 at launch
    effectiveAt: NETWORK_LAUNCH_DATE.toISOString(),
    status: "Active",
    values: initialValue,
  });
  
  if (!futureValues) return stages;
  
  for (let i = 0; i < futureValues.length; i++) {
    const fv = futureValues[i];
    const activationMicros = Number(fv.effectiveAfterMicroseconds || 0);
    const activationTimestampMs = microsToTimestamp(activationMicros);
    
    // Determine if this stage is active
    // A stage is active when the latest round's effectiveAt >= stage's activation timestamp
    const isActive = latestEffectiveAtMs >= activationTimestampMs;
    
    stages.push({
      stage: i + 1,
      label: `Stage ${i + 1}`,
      activationTimestampMicros: activationMicros,
      estimatedDate: formatEstimatedDate(activationTimestampMs),
      actualActivationRound: isActive && latestRound ? latestRound.round : null,
      effectiveAt: isActive && latestRound ? latestRound.effectiveAt : null,
      status: isActive ? "Active" : "Upcoming",
      values: fv.values,
    });
  }
  
  // Mark only the current active stage (last one that's active)
  let foundActive = false;
  for (let i = stages.length - 1; i >= 0; i--) {
    if (stages[i].status === "Active" && !foundActive) {
      foundActive = true;
    } else if (stages[i].status === "Active") {
      // Previous stages are past, not "Active"
      stages[i].status = "Active"; // Keep as Active for display, but we'll style differently
    }
  }
  
  return stages;
};

const IssuanceTimeline = ({ initialValue, futureValues, latestRound }: IssuanceTimelineProps) => {
  const stages = useMemo(
    () => computeStageActivation(initialValue, futureValues, latestRound),
    [initialValue, futureValues, latestRound]
  );
  
  // Find the current active stage (the highest numbered active stage)
  const currentStageIndex = stages.reduce((acc, s, idx) => 
    s.status === "Active" ? idx : acc, 0
  );

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between text-sm">
        <span className="text-muted-foreground">Network Launch: Jul 2024</span>
        <TooltipProvider>
          <Tooltip>
            <TooltipTrigger asChild>
              <div className="flex items-center gap-1 text-muted-foreground cursor-help">
                <Info className="h-4 w-4" />
                <span className="text-xs">How activation works</span>
              </div>
            </TooltipTrigger>
            <TooltipContent className="max-w-xs">
              <p className="text-sm">
                Minting stages activate on ledger round boundaries. Dates are approximate; round numbers are authoritative.
              </p>
            </TooltipContent>
          </Tooltip>
        </TooltipProvider>
      </div>
      <div className="relative">
        {/* Timeline bar */}
        <div className="absolute left-3 top-0 bottom-0 w-0.5 bg-border" />
        
        <div className="space-y-2">
          {stages.map((s, idx) => {
            const isCurrent = idx === currentStageIndex;
            const isPast = idx < currentStageIndex;
            const isFuture = s.status === "Upcoming";
            
            const valPct = parseFloat(s.values?.validatorRewardPercentage || "0");
            const appPctVal = parseFloat(s.values?.appRewardPercentage || "0");
            const svPctVal = 1 - valPct - appPctVal;
            
            return (
              <div key={idx} className="relative pl-10">
                {/* Timeline dot */}
                <div className={`absolute left-1.5 top-4 w-3.5 h-3.5 rounded-full border-2 ${
                  isCurrent 
                    ? "bg-primary border-primary ring-4 ring-primary/20" 
                    : isPast 
                      ? "bg-muted-foreground/50 border-muted-foreground/50" 
                      : "bg-background border-muted-foreground/30"
                }`} />
                
                <div className={`p-4 rounded-lg text-sm transition-all ${
                  isCurrent 
                    ? "bg-primary/10 border border-primary/30" 
                    : isPast
                      ? "bg-muted/30 opacity-60"
                      : "bg-muted/20"
                }`}>
                  <div className="flex items-center justify-between mb-3">
                    <div className="flex items-center gap-2">
                      <span className="font-semibold text-base">{s.label}</span>
                      {isCurrent && <Badge variant="default" className="text-xs">Current</Badge>}
                      {isFuture && idx === currentStageIndex + 1 && (
                        <Badge variant="outline" className="text-xs">Next</Badge>
                      )}
                    </div>
                    <Badge variant={s.status === "Active" ? "default" : "secondary"} className="text-xs">
                      {s.status}
                    </Badge>
                  </div>
                  
                  {/* Activation details */}
                  <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-xs mb-3 p-2 rounded bg-background/50">
                    <div>
                      <span className="text-muted-foreground">Estimated Activation:</span>
                      <p className="font-medium">{s.estimatedDate}</p>
                    </div>
                    <div>
                      <span className="text-muted-foreground">Actual Activation:</span>
                      <p className="font-medium">
                        {s.actualActivationRound !== null 
                          ? `Round ${s.actualActivationRound.toLocaleString()}` 
                          : "Not yet reached"}
                      </p>
                    </div>
                    <div className="col-span-2">
                      <span className="text-muted-foreground">Effective At:</span>
                      <p className="font-medium font-mono text-xs">
                        {s.effectiveAt ? formatEffectiveAt(s.effectiveAt) : "—"}
                      </p>
                    </div>
                  </div>
                  
                  {/* Issuance metrics */}
                  <div className="grid grid-cols-4 gap-3 text-sm">
                    <div>
                      <span className="text-muted-foreground text-xs">Issuance</span>
                      <p className="font-semibold">{formatLargeNumber(s.values?.amuletToIssuePerYear)}/yr</p>
                    </div>
                    <div>
                      <span className="text-muted-foreground text-xs">Validator</span>
                      <p className="font-semibold">{(valPct * 100).toFixed(0)}%</p>
                    </div>
                    <div>
                      <span className="text-muted-foreground text-xs">App</span>
                      <p className="font-semibold">{(appPctVal * 100).toFixed(0)}%</p>
                    </div>
                    <div>
                      <span className="text-muted-foreground text-xs">SV</span>
                      <p className="font-semibold">{(svPctVal * 100).toFixed(0)}%</p>
                    </div>
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

const pickFirstDefined = <T,>(...values: Array<T | undefined | null>): T | undefined => {
  return values.find((v) => v !== undefined && v !== null);
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

const getCurrentIssuanceStage = (
  initialValue: NormalizedIssuanceValue | undefined,
  futureValues: NormalizedIssuanceFutureValue[] | undefined,
  latestRound: { round: number; effectiveAt: string } | undefined
): { stage: number; label: string; values: NormalizedIssuanceValue | undefined } => {
  const latestEffectiveAtMs = latestRound ? new Date(latestRound.effectiveAt).getTime() : Date.now();
  
  if (!futureValues || futureValues.length === 0) {
    return { stage: 0, label: "Initial", values: initialValue };
  }

  let currentStage = 0;
  let currentValues = initialValue;
  
  for (let i = 0; i < futureValues.length; i++) {
    const activationMicros = Number(futureValues[i].effectiveAfterMicroseconds || 0);
    const activationTimestampMs = microsToTimestamp(activationMicros);
    
    if (latestEffectiveAtMs >= activationTimestampMs) {
      currentStage = i + 1;
      currentValues = futureValues[i].values;
    } else {
      break;
    }
  }

  const label = currentStage === 0 ? "Initial" : `Stage ${currentStage}`;
  return { stage: currentStage, label, values: currentValues };
};

export default function IssuanceCurve() {
  const { data: amuletRulesData, isLoading: isLoadingRules } = useAmuletRules();
  const { data: latestRoundData, isLoading: isLoadingRound } = useLatestRound();
  const [jsonOpen, setJsonOpen] = useState(false);

  const isLoading = isLoadingRules || isLoadingRound;

  const issuanceCurve = useMemo(() => {
    if (!amuletRulesData) return null;
    const raw = amuletRulesData as any;
    const payload = raw.contract?.payload ?? raw.payload ?? raw;
    const configSchedule = pickFirstDefined(payload.configSchedule, payload.config_schedule);
    const configInitialValue = pickFirstDefined(configSchedule?.initialValue, configSchedule?.initial_value);
    const source = configInitialValue ?? payload;
    const curve = pickFirstDefined(source.issuanceCurve, source.issuance_curve);
    if (!curve) return null;
    return {
      initialValue: normalizeIssuanceValue(pickFirstDefined(curve.initialValue, curve.initial_value)),
      futureValues: normalizeFutureValues(pickFirstDefined(curve.futureValues, curve.future_values)),
    };
  }, [amuletRulesData]);

  if (isLoading) {
    return (
      <DashboardLayout>
        <div className="space-y-6">
          <Skeleton className="h-10 w-64" />
          <Skeleton className="h-80 w-full" />
        </div>
      </DashboardLayout>
    );
  }

  if (!issuanceCurve) {
    return (
      <DashboardLayout>
        <Alert>
          <AlertTitle>No Issuance Curve data</AlertTitle>
          <AlertDescription>
            Unable to fetch issuance curve from the Canton Scan API.
          </AlertDescription>
        </Alert>
      </DashboardLayout>
    );
  }

  const { label, values } = getCurrentIssuanceStage(
    issuanceCurve.initialValue,
    issuanceCurve.futureValues,
    latestRoundData
  );
  const validatorPct = parseFloat(values?.validatorRewardPercentage || "0");
  const appPct = parseFloat(values?.appRewardPercentage || "0");
  const svPct = 1 - validatorPct - appPct;

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <div className="flex items-center gap-3 mb-2">
            <TrendingUp className="h-8 w-8 text-primary" />
            <h1 className="text-3xl font-bold">Issuance Curve</h1>
          </div>
          <p className="text-muted-foreground">
            Token issuance schedule and reward distribution across network participants
          </p>
        </div>

        {/* Current Stage Overview */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm text-muted-foreground">Current Stage</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="flex items-center gap-2">
                <p className="text-2xl font-bold">{label}</p>
                <Badge variant="default">Active</Badge>
              </div>
              {latestRoundData && (
                <p className="text-xs text-muted-foreground mt-1">
                  Round {latestRoundData.round.toLocaleString()}
                </p>
              )}
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm text-muted-foreground">Annual Issuance</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-2xl font-bold">{formatLargeNumber(values?.amuletToIssuePerYear)}</p>
              <p className="text-xs text-muted-foreground">CC per year</p>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm text-muted-foreground">Validator Reward Cap</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-2xl font-bold">{formatLargeNumber(values?.validatorRewardCap)}</p>
              <p className="text-xs text-muted-foreground">Per round</p>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm text-muted-foreground">Featured App Cap</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-2xl font-bold">{formatLargeNumber(values?.featuredAppRewardCap)}</p>
              <p className="text-xs text-muted-foreground">Per round</p>
            </CardContent>
          </Card>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Reward Distribution */}
          <Card>
            <CardHeader>
              <CardTitle>Reward Distribution</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-sm text-muted-foreground mb-4">
                How newly issued tokens are distributed among network participants
              </p>
              <RewardDistributionChart 
                validatorPct={validatorPct} 
                appPct={appPct} 
                svPct={svPct} 
              />
              <div className="grid grid-cols-3 gap-4 mt-4 text-center">
                <div className="p-3 rounded-lg bg-muted/50">
                  <p className="text-xs text-muted-foreground">Validators</p>
                  <p className="text-lg font-semibold">{(validatorPct * 100).toFixed(1)}%</p>
                </div>
                <div className="p-3 rounded-lg bg-muted/50">
                  <p className="text-xs text-muted-foreground">Apps</p>
                  <p className="text-lg font-semibold">{(appPct * 100).toFixed(1)}%</p>
                </div>
                <div className="p-3 rounded-lg bg-muted/50">
                  <p className="text-xs text-muted-foreground">Super Validators</p>
                  <p className="text-lg font-semibold">{(svPct * 100).toFixed(1)}%</p>
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Issuance Timeline */}
          <Card>
            <CardHeader>
              <CardTitle>Issuance Schedule</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-sm text-muted-foreground mb-4">
                Progression through bootstrapping and steady-state phases
              </p>
              <IssuanceTimeline 
                initialValue={issuanceCurve.initialValue}
                futureValues={issuanceCurve.futureValues}
                latestRound={latestRoundData}
              />
            </CardContent>
          </Card>
        </div>

        <Collapsible open={jsonOpen} onOpenChange={setJsonOpen}>
          <CollapsibleTrigger asChild>
            <Button variant="ghost" className="flex items-center gap-2">
              <ChevronRight className={`h-4 w-4 transition-transform ${jsonOpen ? "rotate-90" : ""}`} />
              Raw JSON Data
            </Button>
          </CollapsibleTrigger>
          <CollapsibleContent>
            <Card className="mt-2">
              <CardContent className="p-4">
                <pre className="text-xs overflow-auto max-h-96 bg-muted p-4 rounded-lg">
                  {JSON.stringify(amuletRulesData, null, 2)}
                </pre>
              </CardContent>
            </Card>
          </CollapsibleContent>
        </Collapsible>
      </div>
    </DashboardLayout>
  );
}
