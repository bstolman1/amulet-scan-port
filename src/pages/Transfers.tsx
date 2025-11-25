import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { ArrowRightLeft, Search } from "lucide-react";
import { useState } from "react";
import { Input } from "@/components/ui/input";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { useQuery } from "@tanstack/react-query";
import { supabase } from "@/integrations/supabase/client";

const Transfers = () => {
  const [searchTerm, setSearchTerm] = useState("");

  // Fetch transfer events
  const { data: events, isLoading } = useQuery({
    queryKey: ["transferEvents"],
    queryFn: async () => {
      const { data, error } = await supabase
        .from("ledger_events")
        .select("*")
        .or("template_id.ilike.%Transfer%,event_type.eq.exercised_event")
        .order("created_at", { ascending: false })
        .limit(500);

      if (error) throw error;
      return data;
    },
    staleTime: 30_000,
  });

  const formatTimestamp = (timestamp: string) => {
    return new Date(timestamp).toLocaleString();
  };

  const formatParty = (party: string) => {
    if (!party) return "Unknown";
    const parts = party.split("::");
    if (parts.length > 1) {
      return parts[0].substring(0, 20) + "...";
    }
    return party.substring(0, 20) + (party.length > 20 ? "..." : "");
  };

  const getTransferData = (event: any) => {
    const choice = event.event_data?.choice || "";
    const choiceArg = event.event_data?.choice_argument || {};
    const payload = event.payload || {};
    
    return {
      sender: payload.sender || choiceArg.sender || "Unknown",
      receiver: choiceArg.receiver || payload.receiver || "Unknown",
      amount: choiceArg.amount || payload.amount || "0",
      choice,
    };
  };

  const filteredEvents = events?.filter((event: any) => {
    if (!searchTerm) return true;
    const { sender, receiver } = getTransferData(event);
    return (
      sender.toLowerCase().includes(searchTerm.toLowerCase()) ||
      receiver.toLowerCase().includes(searchTerm.toLowerCase())
    );
  }) || [];

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <h2 className="text-3xl font-bold mb-2">Transfers</h2>
          <p className="text-muted-foreground">
            Recent Amulet transfer transactions
          </p>
        </div>

        {/* Summary Card */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <Card className="glass-card">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                <ArrowRightLeft className="h-4 w-4" />
                Total Transfers
              </CardTitle>
            </CardHeader>
            <CardContent>
              {isLoading ? (
                <Skeleton className="h-10 w-full" />
              ) : (
                <>
                  <p className="text-3xl font-bold text-primary">{filteredEvents.length.toLocaleString()}</p>
                  <p className="text-xs text-muted-foreground mt-1">Recent transfer events</p>
                </>
              )}
            </CardContent>
          </Card>
        </div>

        {/* Transfers Table */}
        <Card className="glass-card">
          <CardHeader>
            <div className="flex items-center justify-between">
              <div>
                <CardTitle>Transfer Events</CardTitle>
                <CardDescription className="mt-1">Recent Amulet transfer transactions</CardDescription>
              </div>
              <div className="flex items-center gap-2">
                <Search className="h-4 w-4 text-muted-foreground" />
                <Input
                  placeholder="Search by sender or receiver..."
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  className="w-64"
                />
              </div>
            </div>
          </CardHeader>
          <CardContent>
            {isLoading ? (
              <div className="space-y-3">
                {[1, 2, 3, 4, 5].map((i) => (
                  <Skeleton key={i} className="h-16 w-full" />
                ))}
              </div>
            ) : filteredEvents.length === 0 ? (
              <div className="text-center py-12">
                <ArrowRightLeft className="h-12 w-12 mx-auto mb-4 opacity-50" />
                <p className="text-muted-foreground">
                  {searchTerm ? "No transfers found matching your search" : "No transfer events found"}
                </p>
              </div>
            ) : (
              <div className="overflow-x-auto">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Timestamp</TableHead>
                      <TableHead>Sender</TableHead>
                      <TableHead>Receiver</TableHead>
                      <TableHead>Amount</TableHead>
                      <TableHead>Choice</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {filteredEvents.map((event: any) => {
                      const { sender, receiver, amount, choice } = getTransferData(event);
                      return (
                        <TableRow key={event.id}>
                          <TableCell className="text-xs text-muted-foreground">
                            {formatTimestamp(event.timestamp)}
                          </TableCell>
                          <TableCell>
                            <code className="text-xs">{formatParty(sender)}</code>
                          </TableCell>
                          <TableCell>
                            <code className="text-xs">{formatParty(receiver)}</code>
                          </TableCell>
                          <TableCell className="font-mono text-sm">
                            {parseFloat(amount).toFixed(4)} CC
                          </TableCell>
                          <TableCell>
                            <Badge variant="outline">{choice || "Transfer"}</Badge>
                          </TableCell>
                        </TableRow>
                      );
                    })}
                  </TableBody>
                </Table>
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </DashboardLayout>
  );
};

export default Transfers;
