import { useMemo } from "react";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Separator } from "@/components/ui/separator";
import { Skeleton } from "@/components/ui/skeleton";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Settings, DollarSign, Lock, ArrowRightLeft } from "lucide-react";
import { useAmuletRules } from "@/hooks/use-canton-scan-api";

interface NormalizedTransferStep {
  amount?: string;
  rate?: string;
}

interface NormalizedTransferConfig {
  createFee?: { fee?: string };
  holdingFee?: { rate?: string };
  transferFee?: { initialRate?: string; steps?: NormalizedTransferStep[] };
  lockHolderFee?: { fee?: string };
  transferPreapprovalFee?: string | null;
  extraFeaturedAppRewardAmount?: string;
  maxNumInputs?: string;
  maxNumOutputs?: string;
  maxNumLockHolders?: string;
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

const truncateIdentifier = (value?: string) =>
  value && value.length > 24 ? `${value.slice(0, 18)}…${value.slice(-6)}` : value || "—";

export default function ProtocolFees() {
  const { data: amuletRulesData, isLoading } = useAmuletRules();

  const { transferConfig, dso, isDevNet, featuredAppActivityMarkerAmount } = useMemo(() => {
    if (!amuletRulesData) return { transferConfig: null, dso: null, isDevNet: false, featuredAppActivityMarkerAmount: null };
    
    const raw = amuletRulesData as any;
    const payload = raw.contract?.payload ?? raw.payload ?? raw;
    const configSchedule = pickFirstDefined(payload.configSchedule, payload.config_schedule);
    const configInitialValue = pickFirstDefined(configSchedule?.initialValue, configSchedule?.initial_value);
    const source = configInitialValue ?? payload;
    
    const tc = pickFirstDefined(source.transferConfig, source.transfer_config);
    
    const normalized: NormalizedTransferConfig | null = tc ? {
      createFee: { fee: pickFirstDefined(tc.createFee?.fee, tc.create_fee?.fee) },
      holdingFee: { rate: pickFirstDefined(tc.holdingFee?.rate, tc.holding_fee?.rate) },
      transferFee: {
        initialRate: pickFirstDefined(tc.transferFee?.initialRate, tc.transfer_fee?.initial_rate),
        steps: normalizeTransferSteps(pickFirstDefined(tc.transferFee?.steps, tc.transfer_fee?.steps)),
      },
      lockHolderFee: { fee: pickFirstDefined(tc.lockHolderFee?.fee, tc.lock_holder_fee?.fee) },
      transferPreapprovalFee: pickFirstDefined(tc.transferPreapprovalFee, tc.transfer_preapproval_fee),
      extraFeaturedAppRewardAmount: pickFirstDefined(tc.extraFeaturedAppRewardAmount, tc.extra_featured_app_reward_amount),
      maxNumInputs: pickFirstDefined(tc.maxNumInputs, tc.max_num_inputs),
      maxNumOutputs: pickFirstDefined(tc.maxNumOutputs, tc.max_num_outputs),
      maxNumLockHolders: pickFirstDefined(tc.maxNumLockHolders, tc.max_num_lock_holders),
    } : null;

    return {
      transferConfig: normalized,
      dso: pickFirstDefined(payload.dso, payload.DSO, payload.owner),
      isDevNet: pickFirstDefined(payload.isDevNet, payload.is_devnet, false),
      featuredAppActivityMarkerAmount: pickFirstDefined(source.featuredAppActivityMarkerAmount, source.featured_app_activity_marker_amount),
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

  if (!transferConfig) {
    return (
      <DashboardLayout>
        <Alert>
          <AlertTitle>No Protocol Configuration</AlertTitle>
          <AlertDescription>
            Unable to fetch protocol fees from the Canton Scan API.
          </AlertDescription>
        </Alert>
      </DashboardLayout>
    );
  }

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <div className="flex items-center gap-3 mb-2">
            <Settings className="h-8 w-8 text-primary" />
            <h1 className="text-3xl font-bold">Protocol Fees</h1>
          </div>
          <p className="text-muted-foreground">
            Network fee structure and transfer configuration parameters
          </p>
        </div>

        {/* Overview Cards */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm text-muted-foreground">DSO</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="font-mono text-sm break-all">{truncateIdentifier(dso)}</p>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm text-muted-foreground">Network Type</CardTitle>
            </CardHeader>
            <CardContent>
              <Badge variant={isDevNet ? "secondary" : "default"}>
                {isDevNet ? "Development" : "Production"}
              </Badge>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm text-muted-foreground">Featured App Marker</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-2xl font-semibold">{featuredAppActivityMarkerAmount || "—"}</p>
              <p className="text-xs text-muted-foreground">Minimum amount to mark featured app activity</p>
            </CardContent>
          </Card>
        </div>

        {/* Fee Structure */}
        <Card>
          <CardHeader>
            <div className="flex items-center gap-2">
              <DollarSign className="h-5 w-5" />
              <CardTitle>Fee Structure</CardTitle>
            </div>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
              <div className="p-5 rounded-lg bg-muted/50 space-y-3">
                <p className="text-muted-foreground text-xs uppercase font-medium">Create Fee</p>
                <p className="text-3xl font-bold">{transferConfig.createFee?.fee || "—"}</p>
                <p className="text-sm text-muted-foreground">Charged when creating a new transfer</p>
              </div>
              <div className="p-5 rounded-lg bg-muted/50 space-y-3">
                <p className="text-muted-foreground text-xs uppercase font-medium">Holding Fee Rate</p>
                <p className="text-3xl font-bold">{transferConfig.holdingFee?.rate || "—"}</p>
                <p className="text-sm text-muted-foreground">Rate applied to held balances over time</p>
              </div>
              <div className="p-5 rounded-lg bg-muted/50 space-y-3">
                <p className="text-muted-foreground text-xs uppercase font-medium">Lock Holder Fee</p>
                <p className="text-3xl font-bold">{transferConfig.lockHolderFee?.fee || "—"}</p>
                <p className="text-sm text-muted-foreground">Fee for maintaining lock holders on transfers</p>
              </div>
            </div>
          </CardContent>
        </Card>

        {/* Transfer Fee Schedule */}
        <Card>
          <CardHeader>
            <div className="flex items-center gap-2">
              <ArrowRightLeft className="h-5 w-5" />
              <CardTitle>Transfer Fee Schedule</CardTitle>
            </div>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="flex items-center gap-4">
              <span className="text-muted-foreground">Initial Rate:</span>
              <Badge variant="outline" className="text-lg px-3 py-1">
                {transferConfig.transferFee?.initialRate || "—"}
              </Badge>
            </div>
            
            <Separator />
            
            <div>
              <h4 className="font-semibold mb-3">Volume-Based Fee Steps</h4>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Volume Threshold</TableHead>
                    <TableHead className="text-right">Fee Rate</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {(transferConfig.transferFee?.steps || []).length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={2} className="text-center text-muted-foreground">
                        No transfer fee steps configured
                      </TableCell>
                    </TableRow>
                  ) : (
                    transferConfig.transferFee?.steps?.map((step, index) => (
                      <TableRow key={`${step.amount}-${index}`}>
                        <TableCell className="font-medium">{step.amount || "—"}</TableCell>
                        <TableCell className="text-right font-mono">{step.rate || "—"}</TableCell>
                      </TableRow>
                    ))
                  )}
                </TableBody>
              </Table>
            </div>
          </CardContent>
        </Card>

        {/* Transfer Limits */}
        <Card>
          <CardHeader>
            <div className="flex items-center gap-2">
              <Lock className="h-5 w-5" />
              <CardTitle>Transfer Limits</CardTitle>
            </div>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
              <div className="p-5 rounded-lg bg-muted/50">
                <p className="text-muted-foreground text-xs uppercase font-medium mb-2">Max Inputs</p>
                <p className="text-3xl font-bold">{transferConfig.maxNumInputs || "—"}</p>
                <p className="text-sm text-muted-foreground mt-1">Maximum input contracts per transfer</p>
              </div>
              <div className="p-5 rounded-lg bg-muted/50">
                <p className="text-muted-foreground text-xs uppercase font-medium mb-2">Max Outputs</p>
                <p className="text-3xl font-bold">{transferConfig.maxNumOutputs || "—"}</p>
                <p className="text-sm text-muted-foreground mt-1">Maximum output contracts per transfer</p>
              </div>
              <div className="p-5 rounded-lg bg-muted/50">
                <p className="text-muted-foreground text-xs uppercase font-medium mb-2">Max Lock Holders</p>
                <p className="text-3xl font-bold">{transferConfig.maxNumLockHolders || "—"}</p>
                <p className="text-sm text-muted-foreground mt-1">Maximum lock holders per transfer</p>
              </div>
            </div>

            <Separator className="my-6" />

            <div className="p-5 rounded-lg bg-muted/50">
              <div className="flex items-center justify-between mb-2">
                <h4 className="font-semibold">Transfer Preapproval Fee</h4>
                <Badge variant={transferConfig.transferPreapprovalFee ? "default" : "secondary"}>
                  {transferConfig.transferPreapprovalFee ? "Configured" : "Not Set"}
                </Badge>
              </div>
              <p className="text-sm text-muted-foreground">
                {transferConfig.transferPreapprovalFee 
                  ? `Preapproval fee: ${transferConfig.transferPreapprovalFee}`
                  : "No preapproval fee is currently configured for transfers."}
              </p>
            </div>
          </CardContent>
        </Card>
      </div>
    </DashboardLayout>
  );
}
