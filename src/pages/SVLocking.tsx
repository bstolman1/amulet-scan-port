import { DashboardLayout } from "@/components/DashboardLayout";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { DataSourcesFooter } from "@/components/DataSourcesFooter";
import {
  Lock,
  Unlock,
  AlertTriangle,
  CheckCircle2,
  Clock,
  ChevronDown,
  Info,
  ShieldCheck,
  TrendingDown,
  Wallet,
  CalendarClock,
  ExternalLink,
  CircleAlert,
} from "lucide-react";

// ─── Types ────────────────────────────────────────────────────────────────────

type Tier = "tier1" | "tier2" | "tier3" | "none";

interface TierConfig {
  label: string;
  threshold: number; // percent
  weightMultiplier: number; // e.g. 1.0, 0.6, 0.4
  color: string;
  badgeClass: string;
}

interface LockingWallet {
  partyId: string;
  lockedAmount: number;
  type: "locking" | "unlocking";
}

interface UnlockTranche {
  initiatedDate: string;
  originalAmount: number;
  vestedAmount: number;
  remainingUnvested: number;
}

interface SVLockingRecord {
  svName: string;
  svWeight: number;
  lifetimeEarned: number; // CC
  lockedAmount: number; // CC across all reported wallets
  currentTier: Tier;
  impliedTier: Tier;
  wallets: LockingWallet[];
  unlockTranches: UnlockTranche[];
  daysUnderThreshold: number | null; // null = compliant
  weightChangeProposalUrl?: string;
  roundsUnderThreshold: number[];
}

// ─── Constants ────────────────────────────────────────────────────────────────

const TIER_CONFIG: Record<Tier, TierConfig> = {
  tier1: {
    label: "Tier 1 — 100% Weight",
    threshold: 70,
    weightMultiplier: 1.0,
    color: "text-emerald-400",
    badgeClass: "bg-emerald-500/15 text-emerald-400 border-emerald-500/30",
  },
  tier2: {
    label: "Tier 2 — 60% Weight",
    threshold: 45,
    weightMultiplier: 0.6,
    color: "text-amber-400",
    badgeClass: "bg-amber-500/15 text-amber-400 border-amber-500/30",
  },
  tier3: {
    label: "Tier 3 — 40% Weight",
    threshold: 35,
    weightMultiplier: 0.4,
    color: "text-orange-400",
    badgeClass: "bg-orange-500/15 text-orange-400 border-orange-500/30",
  },
  none: {
    label: "No Tier — 0% Weight",
    threshold: 0,
    weightMultiplier: 0,
    color: "text-red-400",
    badgeClass: "bg-red-500/15 text-red-400 border-red-500/30",
  },
};

const PHASE1_START = "2026-04-01";

// ─── Helpers ──────────────────────────────────────────────────────────────────

const formatCC = (amount: number) =>
  new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(amount);

const formatPartyId = (id: string) => id?.split("::")[0] || id;

const pct = (locked: number, earned: number) =>
  earned > 0 ? ((locked / earned) * 100).toFixed(1) : "0.0";

const getImpliedTier = (lockedPct: number): Tier => {
  if (lockedPct >= 70) return "tier1";
  if (lockedPct >= 45) return "tier2";
  if (lockedPct >= 35) return "tier3";
  return "none";
};

// ─── Sub-components ───────────────────────────────────────────────────────────

const TierBadge = ({ tier }: { tier: Tier }) => {
  const cfg = TIER_CONFIG[tier];
  return (
    <Badge variant="outline" className={`${cfg.badgeClass} border font-medium`}>
      {tier === "tier1" && <ShieldCheck className="h-3 w-3 mr-1" />}
      {(tier === "tier2" || tier === "tier3") && <Lock className="h-3 w-3 mr-1" />}
      {tier === "none" && <AlertTriangle className="h-3 w-3 mr-1" />}
      {cfg.label}
    </Badge>
  );
};

const ComplianceStatusBadge = ({
  daysUnder,
  proposalUrl,
}: {
  daysUnder: number | null;
  proposalUrl?: string;
}) => {
  if (daysUnder === null) {
    return (
      <Badge variant="outline" className="bg-emerald-500/15 text-emerald-400 border-emerald-500/30">
        <CheckCircle2 className="h-3 w-3 mr-1" />
        Compliant
      </Badge>
    );
  }
  const isUrgent = daysUnder >= 20;
  const cls = isUrgent
    ? "bg-red-500/15 text-red-400 border-red-500/30"
    : "bg-amber-500/15 text-amber-400 border-amber-500/30";
  const badge = (
    <Badge variant="outline" className={`${cls} border`}>
      <Clock className="h-3 w-3 mr-1" />
      {daysUnder}d under threshold
    </Badge>
  );
  return proposalUrl ? (
    <a href={proposalUrl} target="_blank" rel="noopener noreferrer" className="hover:opacity-80">
      {badge}
    </a>
  ) : (
    badge
  );
};

const LockPercentBar = ({
  lockedPct,
  currentTierThreshold,
}: {
  lockedPct: number;
  currentTierThreshold: number;
}) => {
  const fill = Math.min(lockedPct, 100);
  const isUnder = lockedPct < currentTierThreshold;
  const barColor = isUnder ? "bg-red-500" : lockedPct >= 70 ? "bg-emerald-500" : "bg-amber-500";
  const thresholdLeft = `${Math.min(currentTierThreshold, 100)}%`;

  return (
    <div className="space-y-1">
      <div className="flex justify-between text-xs text-muted-foreground">
        <span>Locked</span>
        <span className={isUnder ? "text-red-400 font-semibold" : "text-foreground font-semibold"}>
          {lockedPct.toFixed(1)}%
        </span>
      </div>
      <div className="relative h-2 w-full rounded-full bg-muted overflow-visible">
        <div
          className={`h-2 rounded-full transition-all ${barColor}`}
          style={{ width: `${fill}%` }}
        />
        {/* threshold marker */}
        <div
          className="absolute top-[-3px] h-4 w-0.5 bg-foreground/50 rounded"
          style={{ left: thresholdLeft }}
        />
      </div>
      <div className="flex justify-end text-xs text-muted-foreground">
        <span>Threshold: {currentTierThreshold}%</span>
      </div>
    </div>
  );
};

const UnlockTrancheRow = ({ tranche }: { tranche: UnlockTranche }) => {
  const vestedPct =
    tranche.originalAmount > 0
      ? (tranche.vestedAmount / tranche.originalAmount) * 100
      : 0;

  return (
    <div className="flex items-center justify-between py-2 border-b border-border/50 last:border-0 text-sm">
      <div>
        <p className="text-xs text-muted-foreground">Initiated</p>
        <p className="font-mono text-xs">{tranche.initiatedDate}</p>
      </div>
      <div className="text-right">
        <p className="text-xs text-muted-foreground">Original</p>
        <p className="font-mono text-xs">{formatCC(tranche.originalAmount)} CC</p>
      </div>
      <div className="text-right">
        <p className="text-xs text-muted-foreground">Vested</p>
        <p className="font-mono text-xs text-emerald-400">{formatCC(tranche.vestedAmount)} CC</p>
      </div>
      <div className="text-right">
        <p className="text-xs text-muted-foreground">Unvested</p>
        <p className="font-mono text-xs text-amber-400">{formatCC(tranche.remainingUnvested)} CC</p>
      </div>
      <Badge variant="outline" className="text-xs">
        {vestedPct.toFixed(0)}% vested
      </Badge>
    </div>
  );
};

const SVLockingCard = ({ sv }: { sv: SVLockingRecord }) => {
  const lockedPct = parseFloat(pct(sv.lockedAmount, sv.lifetimeEarned));
  const tierCfg = TIER_CONFIG[sv.currentTier];
  const impliedChanged = sv.impliedTier !== sv.currentTier;
  const effectiveWeight = Math.round(sv.svWeight * tierCfg.weightMultiplier);

  return (
    <Card className="p-6 space-y-4">
      {/* Header */}
      <div className="flex items-start justify-between gap-2 flex-wrap">
        <div>
          <h3 className="font-semibold text-lg">{sv.svName}</h3>
          <p className="text-xs text-muted-foreground">
            Base Weight: {sv.svWeight} → Effective: {effectiveWeight}
          </p>
        </div>
        <div className="flex flex-col items-end gap-1">
          <TierBadge tier={sv.currentTier} />
          <ComplianceStatusBadge
            daysUnder={sv.daysUnderThreshold}
            proposalUrl={sv.weightChangeProposalUrl}
          />
        </div>
      </div>

      {/* Lock bar */}
      <LockPercentBar lockedPct={lockedPct} currentTierThreshold={tierCfg.threshold} />

      {/* CC summary */}
      <div className="grid grid-cols-2 gap-3 text-sm">
        <div className="bg-muted/40 rounded-lg p-3">
          <p className="text-xs text-muted-foreground mb-0.5">Lifetime Earned</p>
          <p className="font-mono font-semibold">{formatCC(sv.lifetimeEarned)} CC</p>
        </div>
        <div className="bg-muted/40 rounded-lg p-3">
          <p className="text-xs text-muted-foreground mb-0.5">Total Locked</p>
          <p className={`font-mono font-semibold ${lockedPct < tierCfg.threshold ? "text-red-400" : "text-emerald-400"}`}>
            {formatCC(sv.lockedAmount)} CC
          </p>
        </div>
      </div>

      {/* Implied tier change warning */}
      {impliedChanged && (
        <div className="flex items-center gap-2 rounded-lg border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-sm text-amber-400">
          <TrendingDown className="h-4 w-4 shrink-0" />
          <span>
            Implied tier change:{" "}
            <span className="font-semibold">{TIER_CONFIG[sv.impliedTier].label}</span>
          </span>
        </div>
      )}

      {/* Rounds under threshold */}
      {sv.roundsUnderThreshold.length > 0 && (
        <div className="text-xs text-muted-foreground">
          <span className="text-amber-400 font-semibold">{sv.roundsUnderThreshold.length}</span>{" "}
          round{sv.roundsUnderThreshold.length > 1 ? "s" : ""} under threshold in last 35 days
          {sv.roundsUnderThreshold.length <= 5 && (
            <span className="ml-1">(#{sv.roundsUnderThreshold.join(", #")})</span>
          )}
        </div>
      )}

      {/* Wallets */}
      <Collapsible>
        <CollapsibleTrigger asChild>
          <Button variant="ghost" size="sm" className="w-full justify-start gap-2 text-xs">
            <Wallet className="h-3.5 w-3.5" />
            {sv.wallets.length} Reported Wallet{sv.wallets.length !== 1 ? "s" : ""}
            <ChevronDown className="h-3.5 w-3.5 ml-auto" />
          </Button>
        </CollapsibleTrigger>
        <CollapsibleContent>
          <div className="mt-2 space-y-2">
            {sv.wallets.map((w, i) => (
              <div
                key={i}
                className="flex items-center justify-between rounded border border-border/50 bg-muted/30 px-3 py-2 text-xs"
              >
                <div>
                  <Badge
                    variant="outline"
                    className={
                      w.type === "locking"
                        ? "bg-emerald-500/10 text-emerald-400 border-emerald-500/20 mr-2"
                        : "bg-amber-500/10 text-amber-400 border-amber-500/20 mr-2"
                    }
                  >
                    {w.type === "locking" ? (
                      <Lock className="h-2.5 w-2.5 mr-1" />
                    ) : (
                      <Unlock className="h-2.5 w-2.5 mr-1" />
                    )}
                    {w.type}
                  </Badge>
                  <span className="font-mono">{formatPartyId(w.partyId)}</span>
                </div>
                {w.type === "locking" && (
                  <span className="font-mono text-emerald-400">{formatCC(w.lockedAmount)} CC</span>
                )}
              </div>
            ))}
          </div>
        </CollapsibleContent>
      </Collapsible>

      {/* Unlock Tranches */}
      {sv.unlockTranches.length > 0 && (
        <Collapsible>
          <CollapsibleTrigger asChild>
            <Button variant="ghost" size="sm" className="w-full justify-start gap-2 text-xs">
              <CalendarClock className="h-3.5 w-3.5" />
              {sv.unlockTranches.length} Active Unlock Tranche{sv.unlockTranches.length !== 1 ? "s" : ""}
              <ChevronDown className="h-3.5 w-3.5 ml-auto" />
            </Button>
          </CollapsibleTrigger>
          <CollapsibleContent>
            <div className="mt-2">
              {sv.unlockTranches.map((t, i) => (
                <UnlockTrancheRow key={i} tranche={t} />
              ))}
            </div>
          </CollapsibleContent>
        </Collapsible>
      )}

      {/* Proposal link */}
      {sv.weightChangeProposalUrl && (
        <a
          href={sv.weightChangeProposalUrl}
          target="_blank"
          rel="noopener noreferrer"
          className="flex items-center gap-1 text-xs text-primary hover:underline"
        >
          <ExternalLink className="h-3 w-3" />
          View active weight-change proposal
        </a>
      )}
    </Card>
  );
};

// ─── Tier Schedule table ───────────────────────────────────────────────────────

const TierScheduleTable = () => {
  const rows = [
    { year: "Start (2026)", t1: "70%", t2: "45%", t3: "35%" },
    { year: "+1 Year", t1: "65%", t2: "50%", t3: "N/A" },
    { year: "+2 Years", t1: "60%", t2: "N/A", t3: "N/A" },
    { year: "+3 Years", t1: "55%", t2: "N/A", t3: "N/A" },
  ];
  return (
    <div className="overflow-x-auto rounded-lg border border-border">
      <table className="w-full text-sm">
        <thead className="bg-muted/40">
          <tr>
            <th className="px-4 py-2 text-left font-semibold text-muted-foreground">Year</th>
            <th className="px-4 py-2 text-center font-semibold text-emerald-400">Tier 1 (100%)</th>
            <th className="px-4 py-2 text-center font-semibold text-amber-400">Tier 2 (60%)</th>
            <th className="px-4 py-2 text-center font-semibold text-orange-400">Tier 3 (40%)</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={r.year} className="border-t border-border/50">
              <td className="px-4 py-2 font-medium">{r.year}</td>
              <td className="px-4 py-2 text-center font-mono">{r.t1}</td>
              <td className="px-4 py-2 text-center font-mono">{r.t2}</td>
              <td className="px-4 py-2 text-center font-mono text-muted-foreground">{r.t3}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

// ─── Phase Banner ─────────────────────────────────────────────────────────────

const Phase1Banner = () => (
  <div className="flex items-start gap-3 rounded-lg border border-blue-500/30 bg-blue-500/10 px-4 py-3 text-sm">
    <Info className="h-4 w-4 text-blue-400 shrink-0 mt-0.5" />
    <div className="space-y-0.5">
      <p className="font-semibold text-blue-300">Phase 1 — Transitional Enforcement (Manual)</p>
      <p className="text-blue-200/70">
        Active from <span className="font-mono">{PHASE1_START}</span>. Locking is tracked via
        disclosed wallets/custodians. SVs must report PartyIDs to{" "}
        <a
          href="mailto:sv@canton.foundation"
          className="underline hover:text-blue-200"
        >
          sv@canton.foundation
        </a>
        . On-chain automation (Phase 2) expected 3–6 months later.
      </p>
    </div>
  </div>
);

// ─── FAQ ──────────────────────────────────────────────────────────────────────

const FAQ_ITEMS = [
  {
    q: "Is locking on-chain during Phase 1?",
    a: "No. During Phase 1, locking is represented by balances held in disclosed wallets or custodial accounts. On-chain automation is expected in Phase 2.",
  },
  {
    q: "Can locked coins be held at a custodian?",
    a: "Yes. Any wallets used for locked coins must be disclosed but can be held with a self-custody wallet, institutional custodian, or qualified third-party custody provider.",
  },
  {
    q: "How do unlock tranches work?",
    a: "When an SV moves a balance into a disclosed unlocking structure, that amount vests out at 1/365.25 per day. Only the fully locked portion counts toward SV Weight.",
  },
  {
    q: "Can locking requirements be met across multiple wallets?",
    a: "Yes. Compliance is calculated in aggregate across all disclosed locking PartyIDs attributed to that SV.",
  },
  {
    q: "How long does an SV have to restore its weight after falling below a threshold?",
    a: "30 days from the start date recorded in the on-chain vote proposal. If not restored within 30 days, that higher tier becomes permanently unavailable.",
  },
  {
    q: "How should newly earned rewards be handled?",
    a: "New rewards increase the lifetime-earned denominator, so SVs should plan to maintain their target percentage on a rolling basis. Holding a buffer above the threshold is strongly recommended.",
  },
];

const FAQSection = () => (
  <section className="space-y-3">
    <h2 className="text-xl font-semibold">Frequently Asked Questions</h2>
    <div className="grid gap-3 md:grid-cols-2">
      {FAQ_ITEMS.map((item, i) => (
        <Card key={i} className="p-4 space-y-1">
          <p className="font-medium text-sm flex items-start gap-2">
            <CircleAlert className="h-4 w-4 text-primary shrink-0 mt-0.5" />
            {item.q}
          </p>
          <p className="text-sm text-muted-foreground pl-6">{item.a}</p>
        </Card>
      ))}
    </div>
  </section>
);

// ─── Mock data (replace with real API hook) ───────────────────────────────────

const MOCK_SV_DATA: SVLockingRecord[] = [
  {
    svName: "Alpha Validator",
    svWeight: 10,
    lifetimeEarned: 1_000_000,
    lockedAmount: 720_000,
    currentTier: "tier1",
    impliedTier: "tier1",
    wallets: [
      { partyId: "AlphaVault-1::abc123", lockedAmount: 500_000, type: "locking" },
      { partyId: "AlphaVault-2::def456", lockedAmount: 220_000, type: "locking" },
    ],
    unlockTranches: [],
    daysUnderThreshold: null,
    roundsUnderThreshold: [],
  },
  {
    svName: "Beta Validator",
    svWeight: 8,
    lifetimeEarned: 800_000,
    lockedAmount: 380_000,
    currentTier: "tier1",
    impliedTier: "tier2",
    wallets: [
      { partyId: "BetaMain::gh789", lockedAmount: 380_000, type: "locking" },
      { partyId: "BetaUnlock::ij012", lockedAmount: 0, type: "unlocking" },
    ],
    unlockTranches: [
      {
        initiatedDate: "2026-04-10",
        originalAmount: 120_000,
        vestedAmount: 10_000,
        remainingUnvested: 110_000,
      },
    ],
    daysUnderThreshold: 12,
    roundsUnderThreshold: [42, 43, 44],
    weightChangeProposalUrl: "#",
  },
  {
    svName: "Gamma Validator",
    svWeight: 6,
    lifetimeEarned: 500_000,
    lockedAmount: 180_000,
    currentTier: "tier3",
    impliedTier: "none",
    wallets: [
      { partyId: "GammaCustody::kl345", lockedAmount: 180_000, type: "locking" },
    ],
    unlockTranches: [
      {
        initiatedDate: "2026-03-20",
        originalAmount: 200_000,
        vestedAmount: 60_000,
        remainingUnvested: 140_000,
      },
    ],
    daysUnderThreshold: 25,
    roundsUnderThreshold: [38, 39, 40, 41, 42, 43],
    weightChangeProposalUrl: "#",
  },
];

// ─── Page ─────────────────────────────────────────────────────────────────────

const SVLocking = () => {
  const svData = MOCK_SV_DATA; // TODO: replace with useSVLockingData() hook

  const compliant = svData.filter((s) => s.daysUnderThreshold === null).length;
  const atRisk = svData.filter(
    (s) => s.daysUnderThreshold !== null && s.daysUnderThreshold < 30
  ).length;
  const critical = svData.filter(
    (s) => s.daysUnderThreshold !== null && s.daysUnderThreshold >= 30
  ).length;

  return (
    <DashboardLayout>
      <TooltipProvider>
        <div className="space-y-8">
          {/* Page header */}
          <div>
            <div className="flex items-center gap-2 mb-2">
              <Lock className="h-8 w-8 text-primary" />
              <h1 className="text-3xl font-bold">SV Locking</h1>
              <Badge variant="outline" className="ml-2">CIP-0105</Badge>
            </div>
            <p className="text-muted-foreground">
              Super Validator locking & long-term commitment tracking
            </p>
          </div>

          {/* Phase 1 banner */}
          <Phase1Banner />

          {/* Summary stats */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <Card className="p-4">
              <p className="text-xs text-muted-foreground mb-1">Total SVs</p>
              <p className="text-2xl font-bold">{svData.length}</p>
            </Card>
            <Card className="p-4">
              <p className="text-xs text-muted-foreground mb-1">Compliant</p>
              <p className="text-2xl font-bold text-emerald-400">{compliant}</p>
            </Card>
            <Card className="p-4">
              <p className="text-xs text-muted-foreground mb-1">At Risk</p>
              <p className="text-2xl font-bold text-amber-400">{atRisk}</p>
            </Card>
            <Card className="p-4">
              <p className="text-xs text-muted-foreground mb-1">Critical (&gt;30d)</p>
              <p className="text-2xl font-bold text-red-400">{critical}</p>
            </Card>
          </div>

          {/* SV cards */}
          <section>
            <h2 className="text-xl font-semibold mb-4">Super Validator Status</h2>
            <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
              {svData.map((sv) => (
                <SVLockingCard key={sv.svName} sv={sv} />
              ))}
            </div>
          </section>

          {/* Tier schedule */}
          <section className="space-y-3">
            <div className="flex items-center gap-2">
              <h2 className="text-xl font-semibold">Locking Tier Schedule</h2>
              <Tooltip>
                <TooltipTrigger asChild>
                  <Info className="h-4 w-4 text-muted-foreground cursor-help" />
                </TooltipTrigger>
                <TooltipContent className="max-w-xs">
                  <p className="text-sm">
                    Maximum lock requirements decline over time. The lock-up requirement
                    terminates automatically 30 days after the next halving (forecast late
                    summer 2029).
                  </p>
                </TooltipContent>
              </Tooltip>
            </div>
            <TierScheduleTable />
          </section>

          {/* FAQ */}
          <FAQSection />

          {/* Contact */}
          <Card className="p-5 border-dashed">
            <div className="flex items-start gap-3">
              <Info className="h-5 w-5 text-primary shrink-0 mt-0.5" />
              <div className="space-y-1">
                <p className="font-semibold">Report PartyIDs / Contact Foundation</p>
                <p className="text-sm text-muted-foreground">
                  SVs choosing to retain SV rewards weight must report all locking and
                  unlocking PartyIDs to{" "}
                  <a
                    href="mailto:sv@canton.foundation"
                    className="text-primary underline hover:opacity-80"
                  >
                    sv@canton.foundation
                  </a>
                  . Operational notices and updated disclosures should also be sent there.
                </p>
              </div>
            </div>
          </Card>

          <DataSourcesFooter
            snapshotId={undefined}
            templateSuffixes={[]}
            isProcessing={false}
          />
        </div>
      </TooltipProvider>
    </DashboardLayout>
  );
};

export default SVLocking;
