"use client";

import { useState, useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { ChevronDown, ChevronUp, FileDown, RefreshCw, Trophy, Zap, Award, Download, TrendingUp } from "lucide-react";
import { fetchConfigData, scheduleDailySync } from "@/lib/config-sync";
import { scanApi } from "@/lib/api-client";
import { Skeleton } from "@/components/ui/skeleton";
import { useToast } from "@/hooks/use-toast";
import { Badge } from "@/components/ui/badge";

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

// ─────────────────────────────
// Component
// ─────────────────────────────
const Validators = () => {
  const [expandedOperator, setExpandedOperator] = useState<string | null>(null);
  const { toast } = useToast();

  // Schedule daily config sync
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
    staleTime: 24 * 60 * 60 * 1000,
  });

  if (isLoading) {
    return (
      <DashboardLayout>
        <div className="p-8 text-muted-foreground">Loading validator data...</div>
      </DashboardLayout>
    );
  }

  if (isError || !configData) {
    return (
      <DashboardLayout>
        <div className="p-8 text-red-400">Error loading config data.</div>
      </DashboardLayout>
    );
  }

  // ─────────────────────────────
  // Transform Config → Display Model
  // ─────────────────────────────
  const allSVs = configData.superValidators || []; // beneficiaries
  const operators = configData.operators || []; // parent-level validators

  // ✅ Count metrics
  const totalSVs = allSVs.length; // 38 total (flattened)
  const liveSVs = operators.length; // 13 live SVs (top level)
  const offboardedSVs = 0; // none offboarded
  const ghostSVs = allSVs.filter((sv: any) => sv.isGhost).length;

  // ✅ Total weight (sum of parent reward weights)
  const totalOperatorWeightBps = operators.reduce((sum: number, op: any) => sum + normalizeBps(op.rewardWeightBps), 0);
  const totalWeightPct = (totalOperatorWeightBps / 10000).toFixed(2);

  // ✅ Build operator view
  const totalNetworkWeight = totalOperatorWeightBps;
  const operatorsView = operators.map((op: any) => {
    const operatorWeight = normalizeBps(op.rewardWeightBps);
    const operatorComment = op.comment;
    const beneficiaries = allSVs
      .filter((sv: any) => sv.operatorName === op.name)
      .map((sv: any) => ({
        name: sv.name,
        address: sv.address,
        fullPartyId: sv.fullPartyId,
        weightBps: normalizeBps(sv.weight),
        weightPct: bpsToPercent(sv.weight),
        isGhost: sv.isGhost ?? false,
        joinedRound: sv.joinRound ?? "Unknown",
        comment: sv.comment,
      }));

    const totalBeneficiaryWeight = beneficiaries.reduce((sum: number, b: any) => sum + b.weightBps, 0);

    const mismatch = beneficiaries.length ? Math.abs(totalBeneficiaryWeight - operatorWeight) > 1 : false;

    const networkShare = totalNetworkWeight > 0 ? ((operatorWeight / totalNetworkWeight) * 100).toFixed(2) + "%" : "0%";

    const hasBeneficiaries = beneficiaries.length > 0;
    const statusLabel = hasBeneficiaries
      ? mismatch
        ? `⚠️ Mismatch (${bpsToPercent(totalBeneficiaryWeight)} / ${bpsToPercent(operatorWeight)})`
        : `✅ Balanced (${bpsToPercent(totalBeneficiaryWeight)})`
      : `✅ Direct (${bpsToPercent(operatorWeight)})`;

    return {
      operator: op.name,
      operatorWeight,
      operatorWeightPct: bpsToPercent(operatorWeight),
      networkShare,
      totalBeneficiaryWeight,
      totalBeneficiaryWeightPct: bpsToPercent(totalBeneficiaryWeight),
      mismatch,
      beneficiaries,
      statusLabel,
      hasBeneficiaries,
      comment: operatorComment,
    };
  });

  const balancedCount = operatorsView.filter((op) => !op.mismatch).length;
  const totalOperators = operatorsView.length;

  // ─────────────────────────────
  // Export CSV
  // ─────────────────────────────
  const exportCSV = () => {
    const rows = [
      ["Operator", "SuperValidator", "Full Party ID", "Weight (bps)", "Weight (%)", "Ghost", "Joined Round", "Network Share", "Comment"],
      ...operatorsView.flatMap((op) =>
        op.beneficiaries.map((b) => [
          op.operator,
          b.name,
          b.fullPartyId,
          b.weightBps,
          b.weightPct,
          b.isGhost ? "Yes" : "No",
          b.joinedRound,
          op.networkShare,
          b.comment || "",
        ]),
      ),
    ];

    const csv = rows.map((r) => r.join(",")).join("\n");
    const blob = new Blob([csv], { type: "text/csv" });
    const link = document.createElement("a");
    link.href = URL.createObjectURL(blob);
    link.download = "supervalidators.csv";
    link.click();
  };

  // ─────────────────────────────
  // Render
  // ─────────────────────────────
  return (
    <DashboardLayout>
      <div className="space-y-8">
        {/* Header */}
        <div className="flex justify-between items-center">
          <div>
            <h2 className="text-3xl font-bold">SuperValidators / Validators</h2>
            <p className="text-muted-foreground">Network statistics for Supervalidators and active validators</p>
          </div>
          <div className="flex gap-2">
            <Button variant="outline" onClick={() => refetch()} className="flex items-center gap-2">
              <RefreshCw className="w-4 h-4" />
              Refresh
            </Button>
            <Button variant="outline" onClick={exportCSV} className="flex items-center gap-2">
              <FileDown className="w-4 h-4" />
              Export CSV
            </Button>
          </div>
        </div>

        {/* Overview Cards */}
        <Card className="glass-card p-6">
          <div className="grid grid-cols-2 md:grid-cols-5 gap-4 text-center">
            <div>
              <p className="text-sm text-muted-foreground">Total SVs</p>
              <p className="text-xl font-semibold">{totalSVs}</p>
            </div>
            <div>
              <p className="text-sm text-muted-foreground">Live SVs</p>
              <p className="text-xl font-semibold">{liveSVs}</p>
            </div>
            <div>
              <p className="text-sm text-muted-foreground">Total Weight</p>
              <p className="text-xl font-semibold">{totalWeightPct}%</p>
            </div>
            <div>
              <p className="text-sm text-muted-foreground">Offboarded</p>
              <p className="text-xl font-semibold">{offboardedSVs}</p>
            </div>
            <div>
              <p className="text-sm text-muted-foreground">Balanced Operators</p>
              <p className="text-xl font-semibold">
                {balancedCount}/{totalOperators}
              </p>
            </div>
          </div>
        </Card>

        {/* Operators List */}
        <Card className="glass-card p-6">
          <h3 className="text-xl font-bold mb-4">Supervalidators</h3>

          {operatorsView.map((op) => {
            const expanded = expandedOperator === op.operator;
            return (
              <div key={op.operator} className="border-b border-gray-800 py-3">
                <div
                  className="flex justify-between items-center cursor-pointer"
                  onClick={() => setExpandedOperator(expanded ? null : op.operator)}
                >
                  <div className="flex flex-col">
                    <span className="font-semibold">{op.operator}</span>
                    <span className="text-sm text-muted-foreground">
                      Reward Weight: {op.operatorWeightPct} • Network Share: {op.networkShare} • Beneficiaries:{" "}
                      {op.beneficiaries.length}
                    </span>
                  </div>
                  <div className="flex items-center gap-4">
                    <span
                      className={`text-sm ${
                        op.mismatch ? "text-yellow-400" : op.hasBeneficiaries ? "text-green-400" : "text-blue-400"
                      }`}
                    >
                      {op.statusLabel}
                    </span>
                    {expanded ? <ChevronUp className="w-5 h-5" /> : <ChevronDown className="w-5 h-5" />}
                  </div>
                </div>

                {expanded && op.hasBeneficiaries && (
                  <div className="mt-3 pl-4 border-l border-gray-700 space-y-2">
                    {op.beneficiaries.map((b, idx) => (
                      <div
                        key={b.fullPartyId + idx}
                        className="flex flex-col bg-gray-900/40 p-3 rounded-lg space-y-2"
                      >
                        <div className="flex flex-col sm:flex-row sm:justify-between sm:items-start">
                          <div className="flex-1 min-w-0">
                            <span className="font-medium">{b.name}</span>
                            <p className="text-xs text-muted-foreground font-mono break-all mt-1">{b.fullPartyId}</p>
                            <p className="text-xs text-muted-foreground mt-1">Joined Round: {b.joinedRound}</p>
                            {b.comment && (
                              <p className="text-xs text-primary/80 mt-1 italic">💬 {b.comment}</p>
                            )}
                          </div>
                          <div className="text-right mt-2 sm:mt-0 sm:ml-4 flex-shrink-0">
                            <span className="text-sm text-gray-200">
                              {b.weightPct} ({b.weightBps.toLocaleString()} bps)
                            </span>
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            );
          })}
        </Card>

        {/* ───────────────────────────── */}
        {/* ACTIVE VALIDATORS SECTION (Appended) */}
        {/* ───────────────────────────── */}
        <ActiveValidatorsSection />

        {/* Note: Active validators are pulled from API, not ACS snapshots */}
        <Card className="glass-card p-4 text-sm text-muted-foreground">
          <p>
            <strong>Data Sources:</strong> SuperValidator configuration data is fetched from the network configuration
            API. Active validators are queried from the Canton Network API endpoints.
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
          const livenessInfo = livenessData.validatorsReceivedFaucets.find((v) => v.validator === validator.provider);
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
            All {topValidators?.validatorsAndRewards?.length || 0} active validators on the Canton Network
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
                            rank,
                          )}`}
                        >
                          {rank <= 3 ? <Trophy className="h-6 w-6" /> : rank}
                        </div>
                        <div>
                          <h3 className="text-xl font-bold mb-1">{formatPartyId(validator.provider)}</h3>
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
