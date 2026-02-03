"use client";

import { useState, useEffect, useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import {
  ChevronDown,
  ChevronUp,
  FileDown,
  RefreshCw,
  Trophy,
  Zap,
  Info,
  AlertTriangle,
  CheckCircle2,
  Eye,
  EyeOff,
} from "lucide-react";
import { SVWeightHistoryChart } from "@/components/SVWeightHistoryChart";
import { SVWeightStackedChart } from "@/components/SVWeightStackedChart";
import { fetchConfigData, scheduleDailySync } from "@/lib/config-sync";
import { scanApi } from "@/lib/api-client";
import { Skeleton } from "@/components/ui/skeleton";
import { useToast } from "@/hooks/use-toast";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
  TableFooter,
} from "@/components/ui/table";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";

// ─────────────────────────────
// Design Principle (encoded in repo):
// Economic entitlements are displayed once and summed;
// custody paths are displayed only as nested, non-additive breakdowns.
// ─────────────────────────────

// ─────────────────────────────
// Helpers
// ─────────────────────────────
const normalizeBps = (val: any) => {
  if (val == null) return 0;
  if (typeof val === "number") return val;
  if (typeof val === "string") return parseFloat(val.replace(/_/g, "")) || 0;
  return 0;
};

const bpsToPercent = (bps: number) => (bps / 10000).toFixed(2) + "%";

const formatLastSynced = (timestamp: number) => {
  const date = new Date(timestamp);
  return date.toISOString().replace("T", " ").substring(0, 19) + " UTC";
};

// ─────────────────────────────
// Types
// ─────────────────────────────
interface EconomicBeneficiary {
  name: string;
  earnedWeightBps: number;
  earnedWeightPct: string;
  status: "Active" | "Escrowed" | "Mixed";
  hasGhostHoldings: boolean;
  custodyBreakdown: {
    label: string;
    weightBps: number;
    weightPct: string;
    isGhost: boolean;
    partyId?: string;
  }[];
  operatorName: string;
}

interface GhostLedgerEntry {
  ghostHolder: string;
  sourceBeneficiary: string;
  weightBps: number;
  weightPct: string;
  cip?: string;
}

// ─────────────────────────────
// Component
// ─────────────────────────────
const Validators = () => {
  const [expandedBeneficiary, setExpandedBeneficiary] = useState<string | null>(null);
  const [showGhostLedger, setShowGhostLedger] = useState(false);
  const { toast } = useToast();

  // Schedule hourly config sync
  useEffect(() => {
    scheduleDailySync();
  }, []);

  const {
    data: configData,
    isLoading,
    isError,
    refetch,
  } = useQuery({
    queryKey: ["sv-config", "v5"],
    queryFn: () => fetchConfigData(true),
    staleTime: 60 * 60 * 1000, // 1 hour
  });

  // ─────────────────────────────
  // Transform Config → New Display Model
  // ─────────────────────────────
  const displayModel = useMemo(() => {
    if (!configData) return null;

    const allSVs = configData.superValidators || [];
    const operators = configData.operators || [];

    // Calculate total operator weight (the invariant)
    const totalOperatorWeightBps = operators.reduce(
      (sum: number, op: any) => sum + normalizeBps(op.rewardWeightBps),
      0
    );

    // Group beneficiaries by their economic identity (name without ghost suffix)
    // Key insight: A beneficiary's "earned weight" is the sum of all their custody paths
    const beneficiaryMap = new Map<string, {
      name: string;
      totalWeightBps: number;
      directWeightBps: number;
      ghostWeightBps: number;
      custodyBreakdown: EconomicBeneficiary["custodyBreakdown"];
      operatorName: string;
    }>();

    // Ghost ledger for advanced view
    const ghostLedger: GhostLedgerEntry[] = [];

    for (const sv of allSVs) {
      const isGhost = sv.isGhost ?? false;
      const weightBps = normalizeBps(sv.weight);
      
      if (isGhost) {
        // This is a ghost entry - find the source beneficiary from the name
        // Ghost names typically contain the beneficiary name (e.g., "Fireblocks-ghost")
        const sourceName = sv.name.replace(/-ghost.*$/i, "").replace(/ghost$/i, "").trim();
        
        // Add to ghost ledger
        ghostLedger.push({
          ghostHolder: "GhostSV",
          sourceBeneficiary: sourceName || sv.operatorName,
          weightBps,
          weightPct: bpsToPercent(weightBps),
          cip: sv.comment,
        });

        // Find or create the source beneficiary entry
        const key = sourceName || sv.operatorName;
        const existing = beneficiaryMap.get(key);
        if (existing) {
          existing.totalWeightBps += weightBps;
          existing.ghostWeightBps += weightBps;
          existing.custodyBreakdown.push({
            label: `Escrowed under GhostSV`,
            weightBps,
            weightPct: bpsToPercent(weightBps),
            isGhost: true,
            partyId: sv.fullPartyId,
          });
        } else {
          beneficiaryMap.set(key, {
            name: key,
            totalWeightBps: weightBps,
            directWeightBps: 0,
            ghostWeightBps: weightBps,
            custodyBreakdown: [{
              label: `Escrowed under GhostSV`,
              weightBps,
              weightPct: bpsToPercent(weightBps),
              isGhost: true,
              partyId: sv.fullPartyId,
            }],
            operatorName: sv.operatorName,
          });
        }
      } else {
        // This is a direct holding
        const key = sv.name;
        const existing = beneficiaryMap.get(key);
        if (existing) {
          existing.totalWeightBps += weightBps;
          existing.directWeightBps += weightBps;
          existing.custodyBreakdown.unshift({
            label: `Held directly by ${sv.name}`,
            weightBps,
            weightPct: bpsToPercent(weightBps),
            isGhost: false,
            partyId: sv.fullPartyId,
          });
        } else {
          beneficiaryMap.set(key, {
            name: sv.name,
            totalWeightBps: weightBps,
            directWeightBps: weightBps,
            ghostWeightBps: 0,
            custodyBreakdown: [{
              label: `Held directly by ${sv.name}`,
              weightBps,
              weightPct: bpsToPercent(weightBps),
              isGhost: false,
              partyId: sv.fullPartyId,
            }],
            operatorName: sv.operatorName,
          });
        }
      }
    }

    // Convert to array and determine status
    const economicBeneficiaries: EconomicBeneficiary[] = Array.from(beneficiaryMap.values())
      .map((b) => {
        let status: EconomicBeneficiary["status"] = "Active";
        if (b.ghostWeightBps > 0 && b.directWeightBps > 0) {
          status = "Mixed";
        } else if (b.ghostWeightBps > 0) {
          status = "Escrowed";
        }

        return {
          name: b.name,
          earnedWeightBps: b.totalWeightBps,
          earnedWeightPct: bpsToPercent(b.totalWeightBps),
          status,
          hasGhostHoldings: b.ghostWeightBps > 0,
          custodyBreakdown: b.custodyBreakdown,
          operatorName: b.operatorName,
        };
      })
      .sort((a, b) => b.earnedWeightBps - a.earnedWeightBps);

    // Calculate total beneficiary weight
    const totalBeneficiaryWeightBps = economicBeneficiaries.reduce(
      (sum, b) => sum + b.earnedWeightBps,
      0
    );

    // Check invariant
    const drift = totalBeneficiaryWeightBps - totalOperatorWeightBps;
    const driftPct = ((drift / totalOperatorWeightBps) * 100).toFixed(2);
    const invariantSatisfied = Math.abs(drift) <= 1;

    // Network share calculation
    const networkSharePct = ((totalOperatorWeightBps / (165.50 * 100)) * 100).toFixed(2);

    return {
      totalOperatorWeightBps,
      totalOperatorWeightPct: bpsToPercent(totalOperatorWeightBps),
      totalBeneficiaryWeightBps,
      totalBeneficiaryWeightPct: bpsToPercent(totalBeneficiaryWeightBps),
      networkSharePct,
      beneficiaryCount: economicBeneficiaries.length,
      invariantSatisfied,
      drift,
      driftPct,
      economicBeneficiaries,
      ghostLedger,
      lastUpdated: configData.lastUpdated,
    };
  }, [configData]);

  if (isLoading) {
    return (
      <DashboardLayout>
        <div className="p-8 text-muted-foreground">Loading validator data...</div>
      </DashboardLayout>
    );
  }

  if (isError || !configData || !displayModel) {
    return (
      <DashboardLayout>
        <div className="p-8 text-destructive">Error loading config data.</div>
      </DashboardLayout>
    );
  }

  // ─────────────────────────────
  // Export CSV
  // ─────────────────────────────
  const exportCSV = () => {
    const rows = [
      ["Beneficiary", "Earned Weight (%)", "Earned Weight (bps)", "Status", "Operator"],
      ...displayModel.economicBeneficiaries.map((b) => [
        b.name,
        b.earnedWeightPct,
        b.earnedWeightBps,
        b.status,
        b.operatorName,
      ]),
    ];

    const csv = rows.map((r) => r.join(",")).join("\n");
    const blob = new Blob([csv], { type: "text/csv" });
    const link = document.createElement("a");
    link.href = URL.createObjectURL(blob);
    link.download = "economic-beneficiaries.csv";
    link.click();
  };

  // ─────────────────────────────
  // Render
  // ─────────────────────────────
  return (
    <DashboardLayout>
      <div className="space-y-8">
        {/* ═══════════════════════════════════════════════════════════════
            1️⃣ HEADER: Operator Summary (Single Source of Truth)
            ═══════════════════════════════════════════════════════════════ */}
        <Card className="glass-card p-6">
          <div className="flex flex-col md:flex-row justify-between md:items-start gap-4">
            <div className="space-y-1">
              <h2 className="text-2xl font-bold">Super Validator Rewards</h2>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-x-8 gap-y-2 text-sm mt-4">
                <div>
                  <span className="text-muted-foreground">Operator Reward Weight:</span>
                  <span className="ml-2 font-semibold text-foreground">
                    {displayModel.totalOperatorWeightPct}
                  </span>
                </div>
                <div>
                  <span className="text-muted-foreground">Network Share:</span>
                  <span className="ml-2 font-semibold text-foreground">
                    {displayModel.networkSharePct}%
                  </span>
                </div>
                <div>
                  <span className="text-muted-foreground">Beneficiaries:</span>
                  <span className="ml-2 font-semibold text-foreground">
                    {displayModel.beneficiaryCount}
                  </span>
                </div>
                <div>
                  <span className="text-muted-foreground">Last Synced:</span>
                  <span className="ml-2 font-mono text-xs text-foreground">
                    {formatLastSynced(displayModel.lastUpdated)}
                  </span>
                </div>
              </div>

              {/* Invariant Status */}
              <div className="mt-4 pt-4 border-t border-border">
                {displayModel.invariantSatisfied ? (
                  <div className="flex items-center gap-2 text-success">
                    <CheckCircle2 className="w-5 h-5" />
                    <span className="text-sm font-medium">
                      Beneficiary weights sum to operator total
                    </span>
                  </div>
                ) : (
                  <div className="flex items-center gap-2 text-warning">
                    <AlertTriangle className="w-5 h-5" />
                    <span className="text-sm font-medium">
                      Beneficiary weights {displayModel.drift > 0 ? "exceed" : "fall short of"} operator total by{" "}
                      {Math.abs(parseFloat(displayModel.driftPct))}%
                    </span>
                  </div>
                )}
              </div>
            </div>

            <div className="flex gap-2 flex-shrink-0">
              <Button variant="outline" size="sm" onClick={() => refetch()} className="flex items-center gap-2">
                <RefreshCw className="w-4 h-4" />
                Refresh
              </Button>
              <Button variant="outline" size="sm" onClick={exportCSV} className="flex items-center gap-2">
                <FileDown className="w-4 h-4" />
                Export CSV
              </Button>
            </div>
          </div>
        </Card>

        {/* ═══════════════════════════════════════════════════════════════
            2️⃣ PRIMARY TABLE: Economic Beneficiaries (ONLY Additive Table)
            ═══════════════════════════════════════════════════════════════ */}
        <Card className="glass-card overflow-hidden">
          <div className="p-6 border-b border-border">
            <h3 className="text-xl font-bold">Economic Beneficiaries (Reward Entitlements)</h3>
            <p className="text-sm text-muted-foreground mt-1">
              These weights partition the operator's reward pool. Each row represents a unique economic entity.
            </p>
          </div>

          <Table>
            <TableHeader>
              <TableRow className="hover:bg-transparent">
                <TableHead className="w-[40%]">Beneficiary</TableHead>
                <TableHead className="text-right">Earned Weight</TableHead>
                <TableHead className="text-center">Status</TableHead>
                <TableHead className="w-[50px]"></TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {displayModel.economicBeneficiaries.map((beneficiary) => {
                const isExpanded = expandedBeneficiary === beneficiary.name;
                const hasBreakdown = beneficiary.custodyBreakdown.length > 1 || beneficiary.hasGhostHoldings;

                return (
                  <Collapsible
                    key={beneficiary.name}
                    open={isExpanded}
                    onOpenChange={() =>
                      setExpandedBeneficiary(isExpanded ? null : beneficiary.name)
                    }
                    asChild
                  >
                    <>
                      <TableRow
                        className={`cursor-pointer ${hasBreakdown ? "hover:bg-muted/50" : ""}`}
                        onClick={() => hasBreakdown && setExpandedBeneficiary(isExpanded ? null : beneficiary.name)}
                      >
                        <TableCell className="font-medium">{beneficiary.name}</TableCell>
                        <TableCell className="text-right font-bold text-primary">
                          {beneficiary.earnedWeightPct}
                        </TableCell>
                        <TableCell className="text-center">
                          <Badge
                            variant="outline"
                            className={`
                              ${beneficiary.status === "Active" ? "border-success/50 bg-success/10 text-success" : ""}
                              ${beneficiary.status === "Escrowed" ? "border-muted-foreground/50 bg-muted/30 text-muted-foreground" : ""}
                              ${beneficiary.status === "Mixed" ? "border-chart-4/50 bg-chart-4/10 text-chart-4" : ""}
                            `}
                          >
                            {beneficiary.status}
                          </Badge>
                        </TableCell>
                        <TableCell>
                          {hasBreakdown && (
                            <CollapsibleTrigger asChild>
                              <Button variant="ghost" size="sm" className="p-1 h-auto">
                                {isExpanded ? (
                                  <ChevronUp className="h-4 w-4" />
                                ) : (
                                  <ChevronDown className="h-4 w-4" />
                                )}
                              </Button>
                            </CollapsibleTrigger>
                          )}
                        </TableCell>
                      </TableRow>

                      {/* 3️⃣ ROW EXPANSION: Custody / Ghost Breakdown (Non-Additive) */}
                      <CollapsibleContent asChild>
                        <TableRow className="bg-muted/20 hover:bg-muted/30">
                          <TableCell colSpan={4} className="p-0">
                            <div className="p-4 pl-8 border-l-2 border-muted-foreground/30 ml-4 my-2">
                              <div className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-3">
                                Custody Breakdown
                              </div>
                              <div className="space-y-2">
                                {beneficiary.custodyBreakdown.map((custody, idx) => (
                                  <div
                                    key={idx}
                                    className="flex justify-between items-center text-sm"
                                  >
                                    <span className={custody.isGhost ? "text-muted-foreground" : "text-foreground"}>
                                      {custody.label}
                                    </span>
                                    <span className={custody.isGhost ? "text-muted-foreground" : "text-foreground"}>
                                      {custody.weightPct}
                                    </span>
                                  </div>
                                ))}
                              </div>
                              {beneficiary.hasGhostHoldings && (
                                <div className="mt-3 pt-3 border-t border-border flex items-start gap-2 text-xs text-muted-foreground">
                                  <Info className="w-3 h-3 mt-0.5 flex-shrink-0" />
                                  <span>
                                    Escrowed weight is already included in {beneficiary.name}'s total above
                                  </span>
                                </div>
                              )}
                            </div>
                          </TableCell>
                        </TableRow>
                      </CollapsibleContent>
                    </>
                  </Collapsible>
                );
              })}
            </TableBody>
            <TableFooter>
              <TableRow className="bg-muted/50 font-bold">
                <TableCell>Total Economic Weight</TableCell>
                <TableCell className="text-right text-primary">
                  {displayModel.totalBeneficiaryWeightPct}
                </TableCell>
                <TableCell></TableCell>
                <TableCell></TableCell>
              </TableRow>
            </TableFooter>
          </Table>

          <div className="p-4 border-t border-border bg-muted/20 text-xs text-muted-foreground">
            🔒 This table must always sum to {displayModel.totalOperatorWeightPct}. If it doesn't → config error, not UI error.
          </div>
        </Card>

        {/* ═══════════════════════════════════════════════════════════════
            4️⃣ OPTIONAL: GhostSV Ledger (Advanced Users)
            ═══════════════════════════════════════════════════════════════ */}
        {displayModel.ghostLedger.length > 0 && (
          <Card className="glass-card overflow-hidden">
            <div className="p-4 flex items-center justify-between">
              <Button
                variant="ghost"
                size="sm"
                onClick={() => setShowGhostLedger(!showGhostLedger)}
                className="text-muted-foreground hover:text-foreground"
              >
                {showGhostLedger ? (
                  <>
                    <EyeOff className="w-4 h-4 mr-2" />
                    Hide Escrow Ledger (Advanced)
                  </>
                ) : (
                  <>
                    <Eye className="w-4 h-4 mr-2" />
                    View Escrow Ledger (Advanced)
                  </>
                )}
              </Button>
            </div>

            {showGhostLedger && (
              <>
                <div className="px-6 pb-4">
                  <div className="flex items-start gap-2 p-3 rounded-md bg-warning/10 border border-warning/30 text-warning text-sm">
                    <AlertTriangle className="w-4 h-4 mt-0.5 flex-shrink-0" />
                    <span>
                      Weights in this table are informational and <strong>MUST NOT</strong> be summed.
                    </span>
                  </div>
                </div>

                <div className="px-6 pb-2">
                  <h4 className="text-lg font-semibold">Escrow & Ghost Holdings (Non-Additive View)</h4>
                  <p className="text-sm text-muted-foreground">
                    For debugging, governance audits, and CIP verification.
                  </p>
                </div>

                <Table>
                  <TableHeader>
                    <TableRow className="hover:bg-transparent">
                      <TableHead>Ghost Holder</TableHead>
                      <TableHead>Source Beneficiary</TableHead>
                      <TableHead className="text-right">Weight</TableHead>
                      <TableHead>CIP</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {displayModel.ghostLedger.map((entry, idx) => (
                      <TableRow key={idx} className="text-muted-foreground">
                        <TableCell>
                          <Badge variant="outline" className="text-xs">
                            {entry.ghostHolder}
                          </Badge>
                        </TableCell>
                        <TableCell>{entry.sourceBeneficiary}</TableCell>
                        <TableCell className="text-right">{entry.weightPct}</TableCell>
                        <TableCell className="font-mono text-xs">
                          {entry.cip || "—"}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </>
            )}
          </Card>
        )}

        {/* SV Weight History Chart */}
        <SVWeightHistoryChart />

        {/* SV Distribution Stacked Chart */}
        <SVWeightStackedChart />

        {/* ───────────────────────────── */}
        {/* ACTIVE VALIDATORS SECTION */}
        {/* ───────────────────────────── */}
        <ActiveValidatorsSection />

        {/* Data Sources Note */}
        <Card className="glass-card p-4 text-sm text-muted-foreground">
          <p>
            <strong>Design Principle:</strong> Economic entitlements are displayed once and summed;
            custody paths are displayed only as nested, non-additive breakdowns.
          </p>
          <p className="mt-2">
            <strong>Data Sources:</strong> SuperValidator configuration data is fetched from the
            network configuration API. Active validators are queried from the Canton Network API endpoints.
          </p>
        </Card>
      </div>
    </DashboardLayout>
  );
};

// ─────────────────────────────
// Subcomponent for Active Validators Section
// ─────────────────────────────
const ActiveValidatorsSection = () => {
  const { toast } = useToast();

  const {
    data: topValidators,
    isLoading,
    isError,
  } = useQuery({
    queryKey: ["topValidators"],
    queryFn: async () => {
      const data = await scanApi.fetchTopValidators();
      const validatorIds = data.validatorsAndRewards.map((v) => v.provider);
      const livenessData = await scanApi.fetchValidatorLiveness(validatorIds);
      const latestRound = await scanApi.fetchLatestRound();
      const startRound = Math.max(0, latestRound.round - 200);
      const roundTotals = await scanApi.fetchRoundTotals({
        start_round: startRound,
        end_round: latestRound.round,
      });

      const roundDates = new Map<number, string>();
      roundTotals.entries.forEach((entry) => {
        roundDates.set(entry.closed_round, entry.closed_round_effective_at);
      });

      return {
        ...data,
        validatorsAndRewards: data.validatorsAndRewards.map((validator) => {
          const livenessInfo = livenessData.validatorsReceivedFaucets.find(
            (v) => v.validator === validator.provider
          );
          const lastActiveDate = livenessInfo?.lastCollectedInRound
            ? roundDates.get(livenessInfo.lastCollectedInRound)
            : undefined;
          return { ...validator, lastActiveDate };
        }),
      };
    },
    retry: 1,
  });

  const getRankColor = (rank: number) => {
    switch (rank) {
      case 1:
        return "gradient-primary text-primary-foreground";
      case 2:
        return "bg-chart-2/20 text-chart-2";
      case 3:
        return "bg-chart-3/20 text-chart-3";
      default:
        return "bg-muted text-muted-foreground";
    }
  };

  const formatPartyId = (partyId: string) => {
    const parts = partyId.split("::");
    return parts[0] || partyId;
  };

  return (
    <>
      <div className="flex items-center justify-between mt-8">
        <div>
          <h2 className="text-3xl font-bold mb-2">Active Validators</h2>
          <p className="text-muted-foreground">
            All {topValidators?.validatorsAndRewards?.length || 0} active validators on the Canton
            Network
          </p>
        </div>
      </div>

      <Card className="glass-card">
        <div className="p-6">
          {isLoading ? (
            <div className="space-y-4">
              {[1, 2, 3, 4].map((i) => (
                <Skeleton key={i} className="h-40 w-full" />
              ))}
            </div>
          ) : isError ? (
            <div className="text-center p-8">
              <p className="text-muted-foreground">
                Unable to load validator data. The API endpoint may be unavailable.
              </p>
            </div>
          ) : !topValidators?.validatorsAndRewards?.length ? (
            <div className="text-center p-8">
              <p className="text-muted-foreground">No validator data available</p>
            </div>
          ) : (
            <div className="space-y-4">
              {topValidators.validatorsAndRewards.map((validator, index) => {
                const rank = index + 1;
                return (
                  <div
                    key={validator.provider}
                    className="p-6 rounded-lg bg-muted/30 hover:bg-muted/50 transition-smooth border border-border/50"
                  >
                    <div className="flex items-start justify-between mb-4">
                      <div className="flex items-center space-x-4">
                        <div
                          className={`w-12 h-12 rounded-lg flex items-center justify-center font-bold ${getRankColor(
                            rank
                          )}`}
                        >
                          {rank <= 3 ? <Trophy className="h-6 w-6" /> : rank}
                        </div>
                        <div>
                          <h3 className="text-xl font-bold mb-1">
                            {formatPartyId(validator.provider)}
                          </h3>
                          <p className="font-mono text-sm text-muted-foreground truncate max-w-md">
                            {validator.provider}
                          </p>
                        </div>
                      </div>
                      <div className="flex flex-col items-end gap-1">
                        <Badge className="bg-success/10 text-success border-success/20">
                          <Zap className="h-3 w-3 mr-1" />
                          active
                        </Badge>
                        {validator.lastActiveDate && (
                          <span className="text-xs text-muted-foreground">
                            Last: {new Date(validator.lastActiveDate).toLocaleDateString()}
                          </span>
                        )}
                      </div>
                    </div>

                    <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                      <div className="p-4 rounded-lg bg-background/50">
                        <p className="text-sm text-muted-foreground mb-1">Rounds Collected</p>
                        <p className="text-2xl font-bold text-primary">
                          {parseFloat(validator.rewards).toLocaleString(undefined, {
                            maximumFractionDigits: 0,
                          })}
                        </p>
                      </div>
                      <div className="p-4 rounded-lg bg-background/50">
                        <p className="text-sm text-muted-foreground mb-1">Rank</p>
                        <p className="text-2xl font-bold text-foreground">#{rank}</p>
                      </div>
                      <div className="p-4 rounded-lg bg-background/50">
                        <p className="text-sm text-muted-foreground mb-1">Status</p>
                        <p className="text-2xl font-bold text-success">Active</p>
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </Card>
    </>
  );
};

export default Validators;
