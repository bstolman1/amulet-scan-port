import { useMemo, useState } from "react";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Button } from "@/components/ui/button";
import { ChevronRight, TrendingUp } from "lucide-react";
import { useAmuletRules } from "@/hooks/use-canton-scan-api";
import { PieChart, Pie, Cell, ResponsiveContainer, Legend, Tooltip } from "recharts";

const REWARD_COLORS = {
  validator: "hsl(var(--chart-1))",
  app: "hsl(var(--chart-2))",
  sv: "hsl(var(--chart-3))",
  devFund: "hsl(var(--chart-4))",
};

// CIP-82 defaults optDevelopmentFundPercentage to 0.05 at mint time inside
// AmuletRules_MiningRound_StartIssuing. The field is never written to the
// issuance curve payload, so we hardcode it here per business rules.
const DEV_FUND_PCT = 0.05;
const DEV_FUND_APPLIES_FROM_STAGE = 2;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const pickFirstDefined = <T,>(...values: Array<T | undefined | null>): T | undefined =>
  values.find((v) => v !== undefined && v !== null);

const formatLargeNumber = (value?: string | number): string => {
  if (value === undefined || value === null) return "—";
  const num = typeof value === "string" ? parseFloat(value) : value;
  if (isNaN(num)) return "—";
  return num.toLocaleString("en-US", { maximumFractionDigits: 2 });
};

const formatCompact = (value?: string | number): string => {
  if (value === undefined || value === null) return "—";
  const num = typeof value === "string" ? parseFloat(value) : value;
  if (isNaN(num)) return "—";
  if (num >= 1_000_000_000) return `${(num / 1_000_000_000).toFixed(1).replace(/\.0$/, "")}B`;
  if (num >= 1_000_000) return `${(num / 1_000_000).toFixed(1).replace(/\.0$/, "")}M`;
  if (num >= 1_000) return `${(num / 1_000).toFixed(1).replace(/\.0$/, "")}K`;
  return num.toLocaleString("en-US", { maximumFractionDigits: 2 });
};

const NETWORK_LAUNCH_DATE = new Date("2024-07-01T00:00:00Z");

const getMicrosecondsSinceLaunch = (): number =>
  (Date.now() - NETWORK_LAUNCH_DATE.getTime()) * 1000;

/**
 * Apply the dev fund scaling for stage 2+.
 * CIP-82: 5% of each round's issuance goes to the development fund from stage 2.
 * The remaining 95% is split among validators, apps, and SVs proportionally.
 */
const applyDevFund = (
  rawValPct: number,
  rawAppPct: number,
  stageIndex: number
): {
  validatorPct: number;
  appPct: number;
  svPct: number;
  devFundPct: number | undefined;
} => {
  if (stageIndex < DEV_FUND_APPLIES_FROM_STAGE) {
    return {
      validatorPct: rawValPct,
      appPct: rawAppPct,
      svPct: 1 - rawValPct - rawAppPct,
      devFundPct: undefined,
    };
  }
  const scale = 1 - DEV_FUND_PCT;
  return {
    validatorPct: rawValPct * scale,
    appPct: rawAppPct * scale,
    svPct: (1 - rawValPct - rawAppPct) * scale,
    devFundPct: DEV_FUND_PCT,
  };
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
      const effectiveAfterMicroseconds = pickFirstDefined(
        item?.effectiveAfterMicroseconds,
        item?._1?.microseconds,
        item?._1
      );
      const rawValues = pickFirstDefined(item?.values, item?._2);
      const values = normalizeIssuanceValue(rawValues);
      if (!effectiveAfterMicroseconds && !values) return null;
      return { effectiveAfterMicroseconds, values } as NormalizedIssuanceFutureValue;
    })
    .filter(Boolean) as NormalizedIssuanceFutureValue[];
};

const getCurrentIssuanceStage = (
  initialValue: NormalizedIssuanceValue | undefined,
  futureValues: NormalizedIssuanceFutureValue[] | undefined
): { stage: number; label: string; values: NormalizedIssuanceValue | undefined } => {
  const elapsedMicros = getMicrosecondsSinceLaunch();
  if (!futureValues || futureValues.length === 0) {
    return { stage: 0, label: "Stage 0", values: initialValue };
  }
  let currentStage = 0;
  let currentValues = initialValue;
  for (let i = 0; i < futureValues.length; i++) {
    if (elapsedMicros >= Number(futureValues[i].effectiveAfterMicroseconds || 0)) {
      currentStage = i + 1;
      currentValues = futureValues[i].values;
    } else {
      break;
    }
  }
  return { stage: currentStage, label: `Stage ${currentStage}`, values: currentValues };
};

// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------

interface RewardDistributionChartProps {
  validatorPct: number;
  appPct: number;
  svPct: number;
  devFundPct?: number;
}

const RewardDistributionChart = ({ validatorPct, appPct, svPct, devFundPct }: RewardDistributionChartProps) => {
  const data = [
    { name: "Validator", value: validatorPct * 100, color: REWARD_COLORS.validator },
    { name: "App", value: appPct * 100, color: REWARD_COLORS.app },
    { name: "SV", value: svPct * 100, color: REWARD_COLORS.sv },
    ...(devFundPct != null ? [{ name: "Dev Fund", value: devFundPct * 100, color: REWARD_COLORS.devFund }] : []),
  ];

  return (
    <div className="h-[280px] w-full">
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
          <Tooltip
            formatter={(value: number) => [`${value.toFixed(1)}%`, "Share"]}
            contentStyle={{
              backgroundColor: "hsl(222 47% 11%)",
              border: "1px solid hsl(217 33% 17%)",
              borderRadius: "8px",
              color: "hsl(210 40% 98%)",
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
              <span className="text-sm text-foreground">
                {value}: {entry.payload.value.toFixed(0)}%
              </span>
            )}
          />
        </PieChart>
      </ResponsiveContainer>
    </div>
  );
};

interface IssuanceTimelineProps {
  initialValue: NormalizedIssuanceValue | undefined;
  futureValues: NormalizedIssuanceFutureValue[] | undefined;
}

const IssuanceTimeline = ({ initialValue, futureValues }: IssuanceTimelineProps) => {
  const elapsedMicros = getMicrosecondsSinceLaunch();

  const formatDuration = (micros: number): string => {
    const days = micros / 1_000_000 / 86_400;
    if (days < 365) return `${Math.floor(days)} days`;
    return `${(days / 365).toFixed(1)} years`;
  };

  const formatDate = (micros: number): string => {
    const date = new Date(NETWORK_LAUNCH_DATE.getTime() + micros / 1000);
    return date.toLocaleDateString("en-US", { month: "short", year: "numeric" });
  };

  const stages = [
    {
      stage: 0,
      label: "Stage 0",
      effectiveMicros: 0,
      values: initialValue,
      isActive:
        !futureValues?.length ||
        elapsedMicros < Number(futureValues[0].effectiveAfterMicroseconds || 0),
    },
    ...(futureValues || []).map((fv, idx) => {
      const threshold = Number(fv.effectiveAfterMicroseconds || 0);
      const nextThreshold =
        futureValues?.[idx + 1]
          ? Number(futureValues[idx + 1].effectiveAfterMicroseconds || 0)
          : Infinity;
      return {
        stage: idx + 1,
        label: `Stage ${idx + 1}`,
        effectiveMicros: threshold,
        values: fv.values,
        isActive: elapsedMicros >= threshold && elapsedMicros < nextThreshold,
      };
    }),
  ];

  const firstFutureIdx = stages.findIndex((s) => elapsedMicros < s.effectiveMicros);

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between text-sm">
        <span className="text-muted-foreground">Network Launch: Jul 2024</span>
        <span className="font-medium text-primary">Elapsed: {formatDuration(elapsedMicros)}</span>
      </div>
      <div className="relative">
        <div className="absolute left-3 top-0 bottom-0 w-0.5 bg-border" />
        <div className="space-y-2">
          {stages.map((s, idx) => {
            const isPast = !s.isActive && elapsedMicros >= s.effectiveMicros;
            const isFuture = elapsedMicros < s.effectiveMicros;
            const isNext = idx === firstFutureIdx;
            const rawValPct = parseFloat(s.values?.validatorRewardPercentage || "0");
            const rawAppPct = parseFloat(s.values?.appRewardPercentage || "0");
            const { validatorPct, appPct, svPct, devFundPct } = applyDevFund(rawValPct, rawAppPct, s.stage);

            return (
              <div key={idx} className="relative pl-10">
                <div
                  className={`absolute left-1.5 top-4 w-3.5 h-3.5 rounded-full border-2 ${
                    s.isActive
                      ? "bg-primary border-primary ring-4 ring-primary/20"
                      : isPast
                      ? "bg-muted-foreground/50 border-muted-foreground/50"
                      : "bg-background border-muted-foreground/30"
                  }`}
                />
                <div
                  className={`p-4 rounded-lg text-sm transition-all ${
                    s.isActive
                      ? "bg-primary/10 border border-primary/30"
                      : isPast
                      ? "bg-muted/30 opacity-60"
                      : "bg-muted/20"
                  }`}
                >
                  <div className="flex items-center justify-between mb-2">
                    <div className="flex items-center gap-2">
                      <span className="font-semibold text-base">{s.label}</span>
                      {s.isActive && <Badge variant="default" className="text-xs">Current</Badge>}
                      {isFuture && isNext && <Badge variant="outline" className="text-xs">Next</Badge>}
                    </div>
                    <span className="text-sm text-muted-foreground">
                      {s.effectiveMicros === 0 ? "Launch" : formatDate(s.effectiveMicros)}
                    </span>
                  </div>
                  <div className="grid gap-x-4 gap-y-1 text-sm grid-cols-[2fr_1fr_1fr_1fr_1fr]">
                    <div>
                      <span className="text-muted-foreground text-xs">Issuance/yr</span>
                      <p className="font-semibold tabular-nums">
                        {formatCompact(s.values?.amuletToIssuePerYear)}
                      </p>
                    </div>
                    <div>
                      <span className="text-muted-foreground text-xs">Validator</span>
                      <p className="font-semibold">{(validatorPct * 100).toFixed(0)}%</p>
                    </div>
                    <div>
                      <span className="text-muted-foreground text-xs">App</span>
                      <p className="font-semibold">{(appPct * 100).toFixed(0)}%</p>
                    </div>
                    <div>
                      <span className="text-muted-foreground text-xs">SV</span>
                      <p className="font-semibold">{(svPct * 100).toFixed(0)}%</p>
                    </div>
                    <div>
                      {devFundPct != null ? (
                        <>
                          <span className="text-muted-foreground text-xs">Dev Fund</span>
                          <p className="font-semibold">{(devFundPct * 100).toFixed(0)}%</p>
                        </>
                      ) : (
                        <span className="text-muted-foreground text-xs opacity-0">—</span>
                      )}
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

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

export default function IssuanceCurve() {
  const { data: amuletRulesData, isLoading } = useAmuletRules();
  const [jsonOpen, setJsonOpen] = useState(false);

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

  const { stage, label, values } = getCurrentIssuanceStage(
    issuanceCurve.initialValue,
    issuanceCurve.futureValues
  );
  const rawValPct = parseFloat(values?.validatorRewardPercentage || "0");
  const rawAppPct = parseFloat(values?.appRewardPercentage || "0");
  const { validatorPct, appPct, svPct, devFundPct } = applyDevFund(rawValPct, rawAppPct, stage);

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
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm text-muted-foreground">Annual Issuance</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-2xl font-bold">{formatCompact(values?.amuletToIssuePerYear)}</p>
              <p className="text-xs text-muted-foreground">CC per year</p>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm text-muted-foreground">Validator Reward Cap</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-2xl font-bold">{formatCompact(values?.validatorRewardCap)}</p>
              <p className="text-xs text-muted-foreground">Per round</p>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm text-muted-foreground">Featured App Cap</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-2xl font-bold">{formatCompact(values?.featuredAppRewardCap)}</p>
              <p className="text-xs text-muted-foreground">Per round</p>
            </CardContent>
          </Card>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Reward Distribution */}
          <Card>
            <CardHeader>
              <CardTitle>Reward Distribution — {label}</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-sm text-muted-foreground mb-4">
                How newly issued tokens are split among network participants
              </p>
              <RewardDistributionChart
                validatorPct={validatorPct}
                appPct={appPct}
                svPct={svPct}
                devFundPct={devFundPct}
              />
              <div
                className={`grid gap-4 mt-4 text-center ${
                  devFundPct != null ? "grid-cols-4" : "grid-cols-3"
                }`}
              >
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
                {devFundPct != null && (
                  <div className="p-3 rounded-lg bg-muted/50">
                    <p className="text-xs text-muted-foreground">Dev Fund</p>
                    <p className="text-lg font-semibold">{(devFundPct * 100).toFixed(1)}%</p>
                  </div>
                )}
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
              />
            </CardContent>
          </Card>
        </div>

        <Collapsible open={jsonOpen} onOpenChange={setJsonOpen}>
          <CollapsibleTrigger asChild>
            <Button variant="outline" className="w-full justify-start">
              <ChevronRight
                className={`h-4 w-4 mr-2 transition-transform ${jsonOpen ? "rotate-90" : ""}`}
              />
              View Raw JSON
            </Button>
          </CollapsibleTrigger>
          <CollapsibleContent>
            <Card className="mt-2">
              <CardContent className="pt-4">
                <pre className="text-xs overflow-auto max-h-96 bg-muted p-4 rounded">
                  {JSON.stringify(issuanceCurve, null, 2)}
                </pre>
              </CardContent>
            </Card>
          </CollapsibleContent>
        </Collapsible>
      </div>
    </DashboardLayout>
  );
}
