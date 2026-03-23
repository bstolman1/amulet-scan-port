import { useState, useMemo } from "react";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import {
  Search,
  Ticket,
  Code,
  Clock,
  CheckCircle2,
  XCircle,
  ChevronDown,
  ChevronUp,
  AlertTriangle,
  ArrowUpDown,
} from "lucide-react";
import { PaginationControls } from "@/components/PaginationControls";
import { DataSourcesFooter } from "@/components/DataSourcesFooter";
import { useValidatorLicenses, useTopValidatorsByFaucets } from "@/hooks/use-canton-scan-api";

// ─── Helpers ──────────────────────────────────────────────────────────────────

const truncateParty = (party: string, head = 12, tail = 8) => {
  if (!party || party.length <= head + tail + 3) return party || "—";
  return `${party.substring(0, head)}…${party.substring(party.length - tail)}`;
};

const faucetHealth = (missed: number, collected: number) => {
  if (collected === 0) return "unknown";
  const missRate = missed / (missed + collected);
  if (missRate === 0) return "perfect";
  if (missRate < 0.05) return "good";
  if (missRate < 0.15) return "warn";
  return "poor";
};

const HEALTH_CONFIG = {
  perfect: { label: "Perfect", class: "bg-primary/15 text-primary border-primary/30" },
  good:    { label: "Good",    class: "bg-primary/10 text-primary border-primary/20" },
  warn:    { label: "Warning", class: "bg-warning/15 text-warning border-warning/30" },
  poor:    { label: "Poor",    class: "bg-destructive/15 text-destructive border-destructive/30" },
  unknown: { label: "No Data", class: "bg-muted/40 text-muted-foreground border-border" },
};

type SortKey = "validator" | "collected" | "missed" | "missRate" | "version";
type SortDir = "asc" | "desc";

// ─── Merged record type ───────────────────────────────────────────────────────

interface ValidatorRecord {
  validator: string;
  sponsor: string;
  dso: string;
  version: string;
  contact: string;
  lastUpdated: string;
  firstRound: number | null;
  lastRound: number | null;
  missedCoupons: number;
  collected: number;
  missed: number;
  raw: any;
}

// ─── Validator Card ───────────────────────────────────────────────────────────

const ValidatorCard = ({ record }: { record: ValidatorRecord }) => {
  const [open, setOpen] = useState(false);
  const health = faucetHealth(record.missed, record.collected);
  const cfg = HEALTH_CONFIG[health];
  const missRate = record.collected + record.missed > 0
    ? ((record.missed / (record.collected + record.missed)) * 100).toFixed(1)
    : null;

  return (
    <Card className="glass-card px-4 py-3 space-y-3">
      {/* Row 1: validator + health badge */}
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0 flex-1">
          <p className="text-xs text-muted-foreground mb-0.5">Validator</p>
          <p className="font-mono text-xs text-foreground break-all leading-relaxed">
            {record.validator || "—"}
          </p>
        </div>
        <Badge variant="outline" className={`${cfg.class} border shrink-0 text-xs`}>
          {health === "perfect" && <CheckCircle2 className="h-3 w-3 mr-1" />}
          {health === "poor"    && <XCircle      className="h-3 w-3 mr-1" />}
          {health === "warn"    && <AlertTriangle className="h-3 w-3 mr-1" />}
          {cfg.label}
        </Badge>
      </div>

      {/* Row 2: key stats grid */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 text-xs">
        <div className="bg-muted/30 rounded-md px-2 py-1.5">
          <p className="text-muted-foreground mb-0.5">Sponsor</p>
          <p className="font-mono truncate" title={record.sponsor}>
            {truncateParty(record.sponsor)}
          </p>
        </div>
        <div className="bg-muted/30 rounded-md px-2 py-1.5">
          <p className="text-muted-foreground mb-0.5">Rounds Collected</p>
          <p className="font-semibold text-primary">{record.collected || "—"}</p>
        </div>
        <div className="bg-muted/30 rounded-md px-2 py-1.5">
          <p className="text-muted-foreground mb-0.5">Rounds Missed</p>
          <p className={`font-semibold ${record.missed > 0 ? "text-destructive" : "text-foreground"}`}>
            {record.missed ?? "—"}
          </p>
        </div>
        <div className="bg-muted/30 rounded-md px-2 py-1.5">
          <p className="text-muted-foreground mb-0.5">Miss Rate</p>
          <p className={`font-semibold ${
            missRate === null ? "text-muted-foreground"
            : parseFloat(missRate) > 15 ? "text-destructive"
            : parseFloat(missRate) > 5  ? "text-warning"
            : "text-primary"
          }`}>
            {missRate !== null ? `${missRate}%` : "—"}
          </p>
        </div>
      </div>

      {/* Row 3: version + last updated inline */}
      <div className="flex items-center gap-4 text-xs text-muted-foreground flex-wrap">
        {record.version && (
          <span>
            Version:{" "}
            <span className="text-foreground font-mono">{record.version}</span>
          </span>
        )}
        {record.contact && (
          <span>
            Contact:{" "}
            <span className="text-foreground">{record.contact}</span>
          </span>
        )}
        {record.lastUpdated && (
          <span className="flex items-center gap-1">
            <Clock className="h-3 w-3" />
            {new Date(record.lastUpdated).toLocaleDateString()}
          </span>
        )}
        {record.firstRound && (
          <span>
            Rounds:{" "}
            <span className="text-foreground font-mono">
              #{record.firstRound} → #{record.lastRound ?? "now"}
            </span>
          </span>
        )}
      </div>

      {/* Raw JSON collapsible */}
      <Collapsible open={open} onOpenChange={setOpen}>
        <CollapsibleTrigger asChild>
          <Button
            variant="ghost"
            size="sm"
            className="w-full justify-start gap-2 text-xs text-muted-foreground hover:text-foreground h-7 px-2"
          >
            <Code className="h-3.5 w-3.5" />
            Raw JSON
            {open ? <ChevronUp className="h-3 w-3 ml-auto" /> : <ChevronDown className="h-3 w-3 ml-auto" />}
          </Button>
        </CollapsibleTrigger>
        <CollapsibleContent>
          <pre className="text-xs bg-muted/40 p-3 rounded-lg overflow-auto max-h-64 mt-1">
            {JSON.stringify(record.raw, null, 2)}
          </pre>
        </CollapsibleContent>
      </Collapsible>
    </Card>
  );
};

// ─── Sort button ──────────────────────────────────────────────────────────────

const SortBtn = ({
  label,
  sortKey,
  current,
  dir,
  onClick,
}: {
  label: string;
  sortKey: SortKey;
  current: SortKey;
  dir: SortDir;
  onClick: (k: SortKey) => void;
}) => (
  <button
    onClick={() => onClick(sortKey)}
    className={`flex items-center gap-1 text-xs px-2 py-1 rounded-md transition-smooth border ${
      current === sortKey
        ? "border-primary/40 bg-primary/10 text-primary"
        : "border-border text-muted-foreground hover:text-foreground hover:border-border/80"
    }`}
  >
    {label}
    <ArrowUpDown className="h-3 w-3" />
    {current === sortKey && (
      <span className="text-[10px]">{dir === "asc" ? "↑" : "↓"}</span>
    )}
  </button>
);

// ─── Page ─────────────────────────────────────────────────────────────────────

const ValidatorLicenses = () => {
  const [search, setSearch]       = useState("");
  const [page, setPage]           = useState(1);
  const [sortKey, setSortKey]     = useState<SortKey>("collected");
  const [sortDir, setSortDir]     = useState<SortDir>("desc");
  const pageSize = 50;

  const { data: licensesData, isLoading: licensesLoading } = useValidatorLicenses();
  const { data: faucetData,   isLoading: faucetsLoading  } = useTopValidatorsByFaucets(1000);

  const isLoading = licensesLoading || faucetsLoading;
  const licenses  = licensesData || [];
  const faucets   = faucetData   || [];

  // Build faucet lookup map
  const faucetMap = useMemo(() => {
    const map = new Map<string, typeof faucets[0]>();
    faucets.forEach((f) => { if (f.validator) map.set(f.validator, f); });
    return map;
  }, [faucets]);

  // Merge licenses + faucet data into unified records
  const records: ValidatorRecord[] = useMemo(() => {
    return licenses.map((lic: any) => {
      const validator   = lic.payload?.validator   || lic.validator   || "";
      const sponsor     = lic.payload?.sponsor     || lic.sponsor     || "";
      const dso         = lic.payload?.dso         || lic.dso         || "";
      const faucetState = lic.payload?.faucetState || lic.faucetState;
      const metadata    = lic.payload?.metadata    || lic.metadata;
      const faucet      = faucetMap.get(validator);

      return {
        validator,
        sponsor,
        dso,
        version:     metadata?.version      || "",
        contact:     metadata?.contactPoint || "",
        lastUpdated: metadata?.lastUpdatedAt || "",
        firstRound:  faucetState?.firstReceivedFor?.number ?? faucet?.firstCollectedInRound ?? null,
        lastRound:   faucetState?.lastReceivedFor?.number  ?? faucet?.lastCollectedInRound  ?? null,
        missedCoupons: parseInt(faucetState?.numCouponsMissed ?? "0"),
        collected:   faucet?.numRoundsCollected ?? 0,
        missed:      faucet?.numRoundsMissed    ?? parseInt(faucetState?.numCouponsMissed ?? "0"),
        raw:         lic,
      };
    });
  }, [licenses, faucetMap]);

  // Filter
  const filtered = useMemo(() => {
    if (!search) return records;
    const q = search.toLowerCase();
    return records.filter(
      (r) =>
        r.validator.toLowerCase().includes(q) ||
        r.sponsor.toLowerCase().includes(q) ||
        r.version.toLowerCase().includes(q)
    );
  }, [records, search]);

  // Sort
  const sorted = useMemo(() => {
    return [...filtered].sort((a, b) => {
      let av: any, bv: any;
      if (sortKey === "validator") { av = a.validator; bv = b.validator; }
      else if (sortKey === "collected") { av = a.collected; bv = b.collected; }
      else if (sortKey === "missed")    { av = a.missed;    bv = b.missed;    }
      else if (sortKey === "missRate") {
        av = a.collected + a.missed > 0 ? a.missed / (a.collected + a.missed) : -1;
        bv = b.collected + b.missed > 0 ? b.missed / (b.collected + b.missed) : -1;
      }
      else if (sortKey === "version") { av = a.version; bv = b.version; }
      if (av < bv) return sortDir === "asc" ? -1 : 1;
      if (av > bv) return sortDir === "asc" ?  1 : -1;
      return 0;
    });
  }, [filtered, sortKey, sortDir]);

  const paginated = sorted.slice((page - 1) * pageSize, page * pageSize);

  const handleSort = (key: SortKey) => {
    if (key === sortKey) setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    else { setSortKey(key); setSortDir("desc"); }
    setPage(1);
  };

  // Summary stats
  const totalCollected  = records.reduce((s, r) => s + r.collected, 0);
  const totalMissed     = records.reduce((s, r) => s + r.missed, 0);
  const overallMissRate = totalCollected + totalMissed > 0
    ? ((totalMissed / (totalCollected + totalMissed)) * 100).toFixed(1)
    : "—";

  return (
    <DashboardLayout>
      <div className="space-y-6">

        {/* Header */}
        <div>
          <div className="flex items-center gap-2 mb-2">
            <Ticket className="h-8 w-8 text-primary" />
            <h1 className="text-3xl font-bold">Validator Licenses</h1>
          </div>
          <p className="text-muted-foreground">
            Active validator licenses and faucet performance across the network.
          </p>
        </div>

        {/* Summary stats */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          {[
            { label: "Total Validators", value: isLoading ? null : records.length, color: "text-foreground" },
            { label: "Rounds Collected", value: isLoading ? null : totalCollected.toLocaleString(), color: "text-primary" },
            { label: "Rounds Missed",    value: isLoading ? null : totalMissed.toLocaleString(),    color: "text-destructive" },
            { label: "Overall Miss Rate",value: isLoading ? null : `${overallMissRate}%`,           color: totalMissed > 0 ? "text-warning" : "text-primary" },
          ].map((s) => (
            <Card key={s.label} className="p-4 glass-card">
              <p className="text-xs text-muted-foreground mb-1">{s.label}</p>
              {s.value === null
                ? <Skeleton className="h-7 w-16" />
                : <p className={`text-2xl font-bold ${s.color}`}>{s.value}</p>
              }
            </Card>
          ))}
        </div>

        {/* Search + sort controls */}
        <div className="flex flex-col sm:flex-row gap-3">
          <div className="relative flex-1">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-muted-foreground h-4 w-4" />
            <Input
              placeholder="Search validator, sponsor, or version…"
              value={search}
              onChange={(e) => { setSearch(e.target.value); setPage(1); }}
              className="pl-10"
            />
          </div>
          <div className="flex items-center gap-2 flex-wrap">
            <span className="text-xs text-muted-foreground">Sort:</span>
            <SortBtn label="Collected"  sortKey="collected" current={sortKey} dir={sortDir} onClick={handleSort} />
            <SortBtn label="Missed"     sortKey="missed"    current={sortKey} dir={sortDir} onClick={handleSort} />
            <SortBtn label="Miss Rate"  sortKey="missRate"  current={sortKey} dir={sortDir} onClick={handleSort} />
            <SortBtn label="Version"    sortKey="version"   current={sortKey} dir={sortDir} onClick={handleSort} />
          </div>
        </div>

        {/* Results count */}
        {!isLoading && (
          <p className="text-xs text-muted-foreground">
            Showing{" "}
            <span className="text-foreground font-medium">{paginated.length}</span>{" "}
            of{" "}
            <span className="text-foreground font-medium">{sorted.length}</span>{" "}
            validators
            {search && ` matching "${search}"`}
          </p>
        )}

        {/* Cards */}
        {isLoading ? (
          <div className="space-y-3">
            {[1, 2, 3, 4, 5].map((i) => <Skeleton key={i} className="h-28 w-full" />)}
          </div>
        ) : sorted.length === 0 ? (
          <Card className="p-12 text-center glass-card border-dashed">
            <Ticket className="h-10 w-10 mx-auto mb-3 text-muted-foreground" />
            <p className="text-muted-foreground">No validators found{search ? ` matching "${search}"` : ""}.</p>
          </Card>
        ) : (
          <div className="space-y-2">
            {paginated.map((record, i) => (
              <ValidatorCard key={record.validator || i} record={record} />
            ))}
          </div>
        )}

        {/* Pagination */}
        {!isLoading && sorted.length > pageSize && (
          <PaginationControls
            currentPage={page}
            totalItems={sorted.length}
            pageSize={pageSize}
            onPageChange={setPage}
          />
        )}

        <DataSourcesFooter
          snapshotId={undefined}
          templateSuffixes={[]}
          isProcessing={false}
        />
      </div>
    </DashboardLayout>
  );
};

export default ValidatorLicenses;
