import { DashboardLayout } from "@/components/DashboardLayout";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { ArrowRight, ExternalLink } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { scanApi } from "@/lib/api-client";
import { Skeleton } from "@/components/ui/skeleton";

const Transactions = () => {
  const {
    data: transactions,
    isLoading,
    isError,
    refetch,
  } = useQuery({
    queryKey: ["transactions"],
    queryFn: () => scanApi.fetchTransactions({ page_size: 20, sort_order: "desc" }),
  });

  const getStatusColor = (status: string) => {
    switch (status) {
      case "confirmed":
        return "bg-success/10 text-success border-success/20";
      case "pending":
        return "bg-warning/10 text-warning border-warning/20";
      default:
        return "bg-muted text-muted-foreground";
    }
  };

  const getTypeColor = (type: string) => {
    switch (type) {
      case "transfer":
        return "bg-primary/10 text-primary border-primary/20";
      case "mint":
        return "bg-accent/10 text-accent border-accent/20";
      case "tap":
        return "bg-chart-3/10 text-chart-3 border-chart-3/20";
      default:
        return "bg-muted text-muted-foreground";
    }
  };

  const formatPartyId = (partyId: string) => {
    if (!partyId) return "N/A";
    const parts = partyId.split("::");
    const name = parts[0] || partyId;
    const hash = parts[1] || "";
    return `${name}::${hash.substring(0, 8)}...`;
  };

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-3xl font-bold mb-2">Transaction History</h2>
            <p className="text-muted-foreground">Browse recent transactions on the Canton Network</p>
          </div>
        </div>

        <Card className="glass-card">
          <div className="p-6">
            {isLoading ? (
              <div className="space-y-4">
                {[1, 2, 3].map((i) => (
                  <Skeleton key={i} className="h-48 w-full" />
                ))}
              </div>
            ) : isError ? (
              <div className="h-48 flex flex-col items-center justify-center text-center space-y-3 text-muted-foreground">
                <p className="font-medium">Unable to load transactions</p>
                <p className="text-xs">The API may be temporarily unavailable. Please try again.</p>
                <button
                  onClick={() => refetch()}
                  className="inline-flex items-center justify-center rounded-md px-3 py-1.5 text-sm font-medium bg-primary text-primary-foreground hover:bg-primary/90 transition-smooth"
                >
                  Retry
                </button>
              </div>
            ) : !transactions?.transactions?.length ? (
              <div className="h-48 flex items-center justify-center text-muted-foreground">No recent transactions</div>
            ) : (
              <div className="space-y-4">
                {transactions.transactions.map((tx) => (
                  <div
                    key={tx.event_id}
                    className="p-6 rounded-lg bg-muted/30 hover:bg-muted/50 transition-smooth border border-border/50"
                  >
                    <div className="flex items-start justify-between mb-4">
                      <div className="flex items-center space-x-3">
                        <Badge className={getTypeColor(tx.transaction_type)}>{tx.transaction_type}</Badge>
                        <Badge className={getStatusColor("confirmed")}>confirmed</Badge>
                      </div>
                      <div className="text-right">
                        <p className="text-sm text-muted-foreground">Round</p>
                        <p className="font-mono font-semibold">{tx.round || "N/A"}</p>
                      </div>
                    </div>

                    <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
                      <div>
                        <p className="text-sm text-muted-foreground mb-1">Event ID</p>
                        <div className="flex items-center space-x-2">
                          <p className="font-mono text-sm truncate">{tx.event_id.substring(0, 20)}...</p>
                          <ExternalLink className="h-3 w-3 text-muted-foreground cursor-pointer hover:text-primary transition-smooth" />
                        </div>
                      </div>
                      {tx.transfer && (
                        <>
                          <div>
                            <p className="text-sm text-muted-foreground mb-1">Amount</p>
                            <p className="font-mono font-bold text-primary text-lg">
                              {parseFloat(tx.transfer.sender.sender_change_amount).toFixed(2)} CC
                            </p>
                          </div>
                          <div>
                            <p className="text-sm text-muted-foreground mb-1">Fee</p>
                            <p className="font-mono text-sm">
                              {parseFloat(tx.transfer.sender.sender_fee).toFixed(4)} CC
                            </p>
                          </div>
                        </>
                      )}
                      {tx.mint && (
                        <div>
                          <p className="text-sm text-muted-foreground mb-1">Minted Amount</p>
                          <p className="font-mono font-bold text-primary text-lg">
                            {parseFloat(tx.mint.amulet_amount).toFixed(2)} CC
                          </p>
                        </div>
                      )}
                    </div>

                    {tx.transfer && (
                      <div className="flex items-center space-x-3 p-4 rounded-lg bg-background/50">
                        <div className="flex-1">
                          <p className="text-xs text-muted-foreground mb-1">From</p>
                          <p className="font-mono text-sm truncate">{formatPartyId(tx.transfer.sender.party)}</p>
                        </div>
                        <ArrowRight className="h-4 w-4 text-primary flex-shrink-0" />
                        <div className="flex-1">
                          <p className="text-xs text-muted-foreground mb-1">To</p>
                          <p className="font-mono text-sm truncate">
                            {tx.transfer.receivers.length > 0 ? formatPartyId(tx.transfer.receivers[0].party) : "N/A"}
                          </p>
                        </div>
                      </div>
                    )}

                    <div className="mt-4 pt-4 border-t border-border/50">
                      <p className="text-xs text-muted-foreground">{new Date(tx.date).toLocaleString()}</p>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </Card>
      </div>
    </DashboardLayout>
  );
};

export default Transactions;
