import { DashboardLayout } from "@/components/DashboardLayout";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Skeleton } from "@/components/ui/skeleton";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Coins, AlertTriangle, Package, Clock, Hash, Copy, Check, Search, X } from "lucide-react";
import { useDevFundCoupons } from "@/hooks/use-dev-fund-coupons";
import { Input } from "@/components/ui/input";
import { format } from "date-fns";
import { useState, useMemo } from "react";

const safeFormatDate = (dateStr: string | null | undefined): string => {
  if (!dateStr) return "N/A";
  try {
    const date = new Date(dateStr);
    if (isNaN(date.getTime())) return "N/A";
    return format(date, "MMM d, yyyy HH:mm:ss");
  } catch {
    return "N/A";
  }
};

const truncateId = (id: string, chars = 16) => {
  if (!id || id.length <= chars * 2) return id;
  return `${id.slice(0, chars)}…${id.slice(-chars)}`;
};

const CopyButton = ({ text }: { text: string }) => {
  const [copied, setCopied] = useState(false);
  const handleCopy = () => {
    navigator.clipboard.writeText(text);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };
  return (
    <button onClick={handleCopy} className="inline-flex items-center text-muted-foreground hover:text-foreground transition-colors ml-1">
      {copied ? <Check className="h-3 w-3 text-primary" /> : <Copy className="h-3 w-3" />}
    </button>
  );
};

const DevFund = () => {
  const { data: coupons, isLoading, error } = useDevFundCoupons();
  const [search, setSearch] = useState("");

  const filtered = useMemo(() => {
    if (!coupons) return [];
    if (!search.trim()) return coupons;
    const q = search.toLowerCase();
    return coupons.filter(c =>
      c.contract.contract_id.toLowerCase().includes(q) ||
      c.contract.template_id.toLowerCase().includes(q) ||
      c.domain_id.toLowerCase().includes(q)
    );
  }, [coupons, search]);

  return (
    <DashboardLayout>
      <div className="space-y-6">
        {/* Header */}
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="relative">
              <div className="absolute inset-0 bg-primary/20 rounded-lg blur-xl" />
              <div className="relative bg-primary/10 p-2.5 rounded-lg border border-primary/20">
                <Coins className="h-6 w-6 text-primary" />
              </div>
            </div>
            <div>
              <h1 className="text-2xl font-bold text-foreground">Development Fund</h1>
              <p className="text-sm text-muted-foreground">Unclaimed development fund coupons on the Canton Network</p>
            </div>
          </div>
          <Badge variant="outline" className="text-sm">
            {isLoading ? "…" : `${coupons?.length ?? 0} coupons`}
          </Badge>
        </div>

        {/* Error state */}
        {error && (
          <Alert variant="destructive">
            <AlertTriangle className="h-4 w-4" />
            <AlertDescription>
              Failed to load development fund coupons: {error instanceof Error ? error.message : "Unknown error"}
            </AlertDescription>
          </Alert>
        )}

        {/* Summary cards */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <Card className="p-5 space-y-2">
            <div className="flex items-center gap-2 text-muted-foreground text-sm">
              <Package className="h-4 w-4" />
              <span>Total Unclaimed</span>
            </div>
            {isLoading ? (
              <Skeleton className="h-8 w-20" />
            ) : (
              <p className="text-3xl font-bold text-foreground">{coupons?.length ?? 0}</p>
            )}
          </Card>
          <Card className="p-5 space-y-2">
            <div className="flex items-center gap-2 text-muted-foreground text-sm">
              <Hash className="h-4 w-4" />
              <span>Unique Domains</span>
            </div>
            {isLoading ? (
              <Skeleton className="h-8 w-20" />
            ) : (
              <p className="text-3xl font-bold text-foreground">
                {new Set(coupons?.map(c => c.domain_id) ?? []).size}
              </p>
            )}
          </Card>
          <Card className="p-5 space-y-2">
            <div className="flex items-center gap-2 text-muted-foreground text-sm">
              <Clock className="h-4 w-4" />
              <span>Latest Coupon</span>
            </div>
            {isLoading ? (
              <Skeleton className="h-8 w-32" />
            ) : (
              <p className="text-lg font-semibold text-foreground">
                {coupons && coupons.length > 0
                  ? safeFormatDate(
                      [...coupons].sort((a, b) =>
                        new Date(b.contract.created_at).getTime() - new Date(a.contract.created_at).getTime()
                      )[0].contract.created_at
                    )
                  : "N/A"}
              </p>
            )}
          </Card>
        </div>

        {/* Coupons table */}
        <Card>
          <div className="p-4 border-b border-border flex items-center justify-between gap-4">
            <h2 className="text-lg font-semibold text-foreground shrink-0">Unclaimed Coupons</h2>
            <div className="relative max-w-sm w-full">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
              <Input
                placeholder="Filter by contract ID, template, or domain…"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                className="pl-9 pr-8"
              />
              {search && (
                <button onClick={() => setSearch("")} className="absolute right-2.5 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground">
                  <X className="h-4 w-4" />
                </button>
              )}
            </div>
          </div>
          {isLoading ? (
            <div className="p-4 space-y-3">
              {Array.from({ length: 5 }).map((_, i) => (
                <Skeleton key={i} className="h-12 w-full" />
              ))}
            </div>
          ) : filtered.length === 0 ? (
            <div className="p-12 text-center text-muted-foreground">
              <Coins className="h-12 w-12 mx-auto mb-4 opacity-30" />
              <p className="text-lg font-medium">{search ? "No matching coupons" : "No unclaimed coupons"}</p>
              <p className="text-sm mt-1">
                {search
                  ? `No coupons match "${search}".`
                  : "All development fund coupons have been claimed."}
              </p>
            </div>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Contract ID</TableHead>
                  <TableHead>Template</TableHead>
                  <TableHead>Domain</TableHead>
                  <TableHead>Created At</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {filtered.map((coupon) => (
                  <TableRow key={coupon.contract.contract_id}>
                    <TableCell className="font-mono text-xs">
                      <span title={coupon.contract.contract_id}>
                        {truncateId(coupon.contract.contract_id)}
                      </span>
                      <CopyButton text={coupon.contract.contract_id} />
                    </TableCell>
                    <TableCell>
                      <Badge variant="secondary" className="font-mono text-xs">
                        {coupon.contract.template_id.split(":").pop() || coupon.contract.template_id}
                      </Badge>
                    </TableCell>
                    <TableCell className="font-mono text-xs">
                      <span title={coupon.domain_id}>
                        {truncateId(coupon.domain_id, 12)}
                      </span>
                      <CopyButton text={coupon.domain_id} />
                    </TableCell>
                    <TableCell className="text-sm text-muted-foreground">
                      {safeFormatDate(coupon.contract.created_at)}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
          {!isLoading && search && filtered.length > 0 && (
            <div className="px-4 py-2 border-t border-border text-xs text-muted-foreground">
              Showing {filtered.length} of {coupons?.length ?? 0} coupons
            </div>
          )}
        </Card>
      </div>
    </DashboardLayout>
  );
};

export default DevFund;
