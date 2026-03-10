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
  Info,
  Eye,
  EyeOff,
  Building2,
  Users,
} from "lucide-react";
import { fetchConfigData, scheduleDailySync } from "@/lib/config-sync";
import { Skeleton } from "@/components/ui/skeleton";
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
interface StandaloneOperator {
  name: string;
  rewardWeightBps: number;
  rewardWeightPct: string;
  hostedCount: number;
  joinRound?: number | null;
}

interface EconomicBeneficiary {
  name: string;
  earnedWeightBps: number;
  earnedWeightPct: string;
  status: "Active" | "Escrowed" | "Mixed";
  isSelf: boolean;
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
  const [svTab, setSvTab] = useState<"standalone" | "hosted">("standalone");

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
    staleTime: 60 * 60 * 1000,
  });

  // ─────────────────────────────
  // Transform Config → Display Model
  //
  // KEY CHANGE: Previously only GSF's extraBeneficiaries were processed.
  // Now we:
  //   • Show ALL 13 operators as "Standalone" rows
  //   • Show extraBeneficiaries from ALL operators in the "Hosted" tab
  // ─────────────────────────────
  const displayModel = useMemo(() => {
    if (!configData) return null;

    const operators = configData.operators || [];
    if (operators.length === 0) return null;

    // ── Standalone: one row per operator ───────────────────────────────────
    const standaloneOperators: StandaloneOperator[] = operators
      .map((op) => ({
        name: op.name,
        rewardWeightBps: normalizeBps(op.rewardWeightBps),
        rewardWeightPct: bpsToPercent(normalizeBps(op.rewardWeightBps)),
        hostedCount: (op.extraBeneficiaries || []).length,
        joinRound: op.joinRound ?? null,
      }))
      .sort((a, b) => b.rewardWeightBps - a.rewardWeightBps);

    const totalOperatorWeightBps = standaloneOperators.reduce(
      (s, op) => s + op.rewardWeightBps,
      0
    );

    // ── Hosted: beneficiaries from ALL operators ───────────────────────────
    const ghostLedger: GhostLedgerEntry[] = [];

    // Each unique economic entity gets one row.
    // Key = display name (NOT party ID — party IDs can be reused across entities).
    // We disambiguate by building the key as `operatorName|displayName` so that
    // the same name hosted by different operators gets separate rows.
    const beneficiaryMap = new Map<string, {
      name: string;
      totalWeightBps: number;
      directWeightBps: number;
      ghostWeightBps: number;
      isSelf: boolean;
      custodyBreakdown: EconomicBeneficiary["custodyBreakdown"];
      operatorName: string;
    }>();

    for (const op of operators) {
      const opName = op.name;
      const extraBeneficiaries = op.extraBeneficiaries || [];

      for (const entry of extraBeneficiaries) {
        const fullPartyId = entry.beneficiary;
        const [rawName] = fullPartyId.split("::");
        const weightBps = normalizeBps(entry.weight);
        const comment = entry.comment || "";

        const isGhostEntry = rawName.toLowerCase().includes("ghost");

        if (isGhostEntry) {
          // Derive source beneficiary name from comment when it follows "Name CIP-XXXX" pattern.
          // e.g. "Fireblocks CIP-0072 # escrow ..." → "Fireblocks"
          // Falls back to stripping "-ghost-N" from the party name.
          let sourceName = "";
          if (comment) {
            const cipMatch = comment.match(/^(.+?)\s+CIP-\d+/);
            if (cipMatch) sourceName = cipMatch[1].trim();
          }
          if (!sourceName) {
            sourceName = rawName
              .replace(/-ghost.*$/i, "")
              .replace(/ghost.*$/i, "")
              .replace(/-\d+$/, "")
              .trim();
          }

          // Determine which ghost holder this is (GhostSV or MPCH ghost, etc.)
          // Look at the party_id's address to find the host operator.
          const ghostHolder = opName;

          ghostLedger.push({
            ghostHolder,
            sourceBeneficiary: sourceName || rawName,
            weightBps,
            weightPct: bpsToPercent(weightBps),
            cip: comment,
          });

          // Attribute ghost weight to the source beneficiary's economic row.
          // Use `opName|sourceName` as the map key so hosted-by-different-operators
          // entities are kept separate.
          const key = `${opName}|${sourceName || rawName}`;
          const existing = beneficiaryMap.get(key);
          if (existing) {
            existing.totalWeightBps += weightBps;
            existing.ghostWeightBps += weightBps;
            existing.custodyBreakdown.push({
              label: `Escrowed under ${ghostHolder}`,
              weightBps,
              weightPct: bpsToPercent(weightBps),
              isGhost: true,
              partyId: fullPartyId,
            });
          } else {
            beneficiaryMap.set(key, {
              name: sourceName || rawName,
              totalWeightBps: weightBps,
              directWeightBps: 0,
              ghostWeightBps: weightBps,
              isSelf: false,
              custodyBreakdown: [{
                label: `Escrowed under ${ghostHolder}`,
                weightBps,
                weightPct: bpsToPercent(weightBps),
                isGhost: true,
                partyId: fullPartyId,
              }],
              operatorName: opName,
            });
          }
        } else {
          // Direct holding. Derive display name only when comment follows "Name CIP-XXXX" pattern.
          let displayName = rawName;
          if (comment) {
            const cipMatch = comment.match(/^(.+?)\s+CIP-\d+/);
            if (cipMatch) displayName = cipMatch[1].trim();
          }

          // Detect self-referential entries: operator holding weight for itself.
          // These are explicitly known from the YAML structure.
          const isSelf =
            (opName === "Global-Synchronizer-Foundation" &&
              (rawName === "GhostSV-validator-1" || rawName === "GSF-SVRewards-1")) ||
            (opName === "MPC-Holding-Inc" &&
              comment.toLowerCase().startsWith("mpch"));

          const key = `${opName}|${displayName}`;
          const existing = beneficiaryMap.get(key);
          if (existing) {
            existing.totalWeightBps += weightBps;
            existing.directWeightBps += weightBps;
            existing.custodyBreakdown.unshift({
              label: `Held directly by ${displayName}`,
              weightBps,
              weightPct: bpsToPercent(weightBps),
              isGhost: false,
              partyId: fullPartyId,
            });
          } else {
            beneficiaryMap.set(key, {
              name: displayName,
              totalWeightBps: weightBps,
              directWeightBps: weightBps,
              ghostWeightBps: 0,
              isSelf,
              custodyBreakdown: [{
                label: `Held directly by ${displayName}`,
                weightBps,
                weightPct: bpsToPercent(weightBps),
                isGhost: false,
                partyId: fullPartyId,
              }],
              operatorName: opName,
            });
          }
        }
      }
    }

    // Convert map → array
    const economicBeneficiaries: EconomicBeneficiary[] = Array.from(beneficiaryMap.values())
      .map((b) => {
        const status: EconomicBeneficiary["status"] =
          b.ghostWeightBps > 0 && b.directWeightBps > 0
            ? "Mixed"
            : b.ghostWeightBps > 0
            ? "Escrowed"
            : "Active";

        return {
          name: b.name,
          earnedWeightBps: b.totalWeightBps,
          earnedWeightPct: bpsToPercent(b.totalWeightBps),
          status,
          isSelf: b.isSelf,
          hasGhostHoldings: b.ghostWeightBps > 0,
          custodyBreakdown: b.custodyBreakdown,
          operatorName: b.operatorName,
        };
      })
      .sort((a, b) => b.earnedWeightBps - a.earnedWeightBps);

    const totalBeneficiaryWeightBps = economicBeneficiaries.reduce(
      (sum, b) => sum + b.earnedWeightBps,
      0
    );

    const drift = totalBeneficiaryWeightBps - totalOperatorWeightBps;
    const driftPct = ((drift / totalOperatorWeightBps) * 100).toFixed(2);
    const invariantSatisfied = Math.abs(drift) <= 1;
    const networkSharePct = ((totalOperatorWeightBps / 1000000) * 100).toFixed(2);

    return {
      standaloneOperators,
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

  const exportCSV = () => {
    const rows = [
      ["Beneficiary", "Hosted By", "Earned Weight (%)", "Earned Weight (bps)", "Status"],
      ...displayModel.economicBeneficiaries.map((b) => [
        b.name,
        b.operatorName,
        b.earnedWeightPct,
        b.earnedWeightBps,
        b.status,
      ]),
    ];
    const csv = rows.map((r) => r.join(",")).join("\n");
    const blob = new Blob([csv], { type: "text/csv" });
    const link = document.createElement("a");
    link.href = URL.createObjectURL(blob);
    link.download = "super-validators.csv";
    link.click();
  };

  // ─────────────────────────────
  // Render
  // ─────────────────────────────
  return (
    <DashboardLayout>
      <div className="space-y-8">
        {/* ═══════════════════════════════════════════════════════════════
            1️⃣ HEADER SUMMARY CARD
            ═══════════════════════════════════════════════════════════════ */}
        <Card className="glass-card p-6">
          <div className="flex flex-col md:flex-row justify-between md:items-start gap-4">
            <div className="space-y-1">
              <h2 className="text-2xl font-bold">Super Validators</h2>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-x-8 gap-y-2 text-sm mt-4">
                <div>
                  <span className="text-muted-foreground">Total Reward Weight:</span>
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
                  <span className="text-muted-foreground">Operators:</span>
                  <span className="ml-2 font-semibold text-foreground">
                    {displayModel.standaloneOperators.length}
                  </span>
                </div>
                <div>
                  <span className="text-muted-foreground">Beneficiaries:</span>
                  <span className="ml-2 font-semibold text-foreground">
                    {displayModel.beneficiaryCount}
                  </span>
                </div>
                <div className="col-span-2 md:col-span-4">
                  <span className="text-muted-foreground">Last Synced:</span>
                  <span className="ml-2 font-mono text-xs text-foreground">
                    {formatLastSynced(displayModel.lastUpdated)}
                  </span>
                </div>
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
            2️⃣ STANDALONE / HOSTED TAB TABLE
            ═══════════════════════════════════════════════════════════════ */}
        <Card className="glass-card overflow-hidden">
          {/* Tab Toggle */}
          <div className="p-4 border-b border-border flex items-center gap-2">
            <button
              onClick={() => setSvTab("standalone")}
              className={`flex items-center gap-2 px-4 py-2 rounded-md text-sm font-medium transition-colors ${
                svTab === "standalone"
                  ? "bg-primary text-primary-foreground"
                  : "text-muted-foreground hover:text-foreground hover:bg-muted"
              }`}
            >
              <Building2 className="w-4 h-4" />
              Operators
              <span className={`ml-1 px-1.5 py-0.5 rounded text-xs font-bold ${
                svTab === "standalone" ? "bg-white/20" : "bg-muted"
              }`}>
                {displayModel.standaloneOperators.length}
              </span>
            </button>
            <button
              onClick={() => setSvTab("hosted")}
              className={`flex items-center gap-2 px-4 py-2 rounded-md text-sm font-medium transition-colors ${
                svTab === "hosted"
                  ? "bg-primary text-primary-foreground"
                  : "text-muted-foreground hover:text-foreground hover:bg-muted"
              }`}
            >
              <Users className="w-4 h-4" />
              Beneficiaries
              <span className={`ml-1 px-1.5 py-0.5 rounded text-xs font-bold ${
                svTab === "hosted" ? "bg-white/20" : "bg-muted"
              }`}>
                {displayModel.beneficiaryCount}
              </span>
            </button>
          </div>

          {/* ── STANDALONE TABLE ─────────────────────────────────────────── */}
          {svTab === "standalone" && (
            <>
              <div className="px-6 pt-4 pb-2">
                <h3 className="text-lg font-bold">Operators</h3>
                <p className="text-sm text-muted-foreground mt-1">
                  The {displayModel.standaloneOperators.length} operators running their own validator infrastructure.
                </p>
              </div>
              <Table>
                <TableHeader>
                  <TableRow className="hover:bg-transparent">
                    <TableHead>Operator</TableHead>
                    <TableHead className="text-right">Reward Weight</TableHead>
                    <TableHead className="text-center">Hosted Parties</TableHead>
                    <TableHead className="text-center">Join Round</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {displayModel.standaloneOperators.map((op) => (
                    <TableRow key={op.name}>
                      <TableCell className="font-medium">{op.name}</TableCell>
                      <TableCell className="text-right font-bold text-primary">
                        {op.rewardWeightPct}
                      </TableCell>
                      <TableCell className="text-center">
                        {op.hostedCount > 0 ? (
                          <Badge
                            variant="secondary"
                            className="cursor-pointer hover:bg-primary/20"
                            onClick={() => setSvTab("hosted")}
                          >
                            {op.hostedCount}
                          </Badge>
                        ) : (
                          <span className="text-muted-foreground text-xs">—</span>
                        )}
                      </TableCell>
                      <TableCell className="text-center text-sm text-muted-foreground">
                        {op.joinRound ?? "—"}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
                <TableFooter>
                  <TableRow className="bg-muted/50 font-bold">
                    <TableCell>Total</TableCell>
                    <TableCell className="text-right text-primary">
                      {displayModel.totalOperatorWeightPct}
                    </TableCell>
                    <TableCell></TableCell>
                    <TableCell></TableCell>
                  </TableRow>
                </TableFooter>
              </Table>
            </>
          )}

          {/* ── HOSTED TABLE ──────────────────────────────────────────────── */}
          {svTab === "hosted" && (
            <>
              <div className="px-6 pt-4 pb-2">
                <h3 className="text-lg font-bold">Beneficiaries</h3>
                <p className="text-sm text-muted-foreground mt-1">
                  Parties hosted under an operator. Weights partition that operator's reward pool.
                </p>
              </div>
              <Table>
                <TableHeader>
                  <TableRow className="hover:bg-transparent">
                    <TableHead className="w-[35%]">Beneficiary</TableHead>
                    <TableHead>Hosted By</TableHead>
                    <TableHead className="text-right">Earned Weight</TableHead>
                    <TableHead className="text-center">Status</TableHead>
                    <TableHead className="w-[50px]"></TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {displayModel.economicBeneficiaries.map((beneficiary) => {
                    const isExpanded = expandedBeneficiary === `${beneficiary.operatorName}|${beneficiary.name}`;
                    const key = `${beneficiary.operatorName}|${beneficiary.name}`;
                    const hasBreakdown =
                      beneficiary.custodyBreakdown.length > 1 || beneficiary.hasGhostHoldings;

                    return (
                      <Collapsible
                        key={key}
                        open={isExpanded}
                        onOpenChange={() =>
                          setExpandedBeneficiary(isExpanded ? null : key)
                        }
                        asChild
                      >
                        <>
                          <TableRow
                            className={`cursor-pointer ${hasBreakdown ? "hover:bg-muted/50" : ""}`}
                            onClick={() =>
                              hasBreakdown && setExpandedBeneficiary(isExpanded ? null : key)
                            }
                          >
                            <TableCell className="font-medium">
                              <div className="flex items-center gap-2">
                                {beneficiary.name}
                                {beneficiary.isSelf && (
                                  <Badge variant="outline" className="text-xs border-primary/40 bg-primary/10 text-primary">
                                    Self
                                  </Badge>
                                )}
                              </div>
                            </TableCell>
                            <TableCell className="text-sm text-muted-foreground">
                              {beneficiary.operatorName}
                            </TableCell>
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

                          <CollapsibleContent asChild>
                            <TableRow className="bg-muted/20 hover:bg-muted/30">
                              <TableCell colSpan={5} className="p-0">
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
                                        <span
                                          className={
                                            custody.isGhost
                                              ? "text-muted-foreground"
                                              : "text-foreground"
                                          }
                                        >
                                          {custody.label}
                                        </span>
                                        <span
                                          className={
                                            custody.isGhost
                                              ? "text-muted-foreground"
                                              : "text-foreground"
                                          }
                                        >
                                          {custody.weightPct}
                                        </span>
                                      </div>
                                    ))}
                                  </div>
                                  {beneficiary.hasGhostHoldings && (
                                    <div className="mt-3 pt-3 border-t border-border flex items-start gap-2 text-xs text-muted-foreground">
                                      <Info className="w-3 h-3 mt-0.5 flex-shrink-0" />
                                      <span>
                                        Escrowed weight is already included in{" "}
                                        {beneficiary.name}'s total above
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
                    <TableCell></TableCell>
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
            </>
          )}
        </Card>



      </div>
    </DashboardLayout>
  );
};


export default Validators;
