import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Calendar } from "@/components/ui/calendar";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { ScrollArea } from "@/components/ui/scroll-area";
import {
  Calculator,
  CalendarIcon,
  Search,
  Award,
  Coins,
  FileText,
  AlertCircle,
  Loader2,
} from "lucide-react";
import { format } from "date-fns";
import { cn } from "@/lib/utils";
import { getDuckDBApiUrl } from "@/lib/backend-config";

interface RewardResult {
  partyId: string;
  totalRewards: number;
  rewardCount: number;
  byRound: Record<string, { count: number; amount: number }>;
  events: Array<{
    event_id: string;
    round: number;
    amount: number;
    effective_at: string;
    template_id: string;
  }>;
}

const RewardCalculations = () => {
  const [partyId, setPartyId] = useState("");
  const [startDate, setStartDate] = useState<Date | undefined>();
  const [endDate, setEndDate] = useState<Date | undefined>();
  const [startRound, setStartRound] = useState("");
  const [endRound, setEndRound] = useState("");
  const [searchTriggered, setSearchTriggered] = useState(false);

  // Build query params
  const queryParams = new URLSearchParams();
  if (partyId) queryParams.set("partyId", partyId);
  if (startDate) queryParams.set("startDate", startDate.toISOString());
  if (endDate) queryParams.set("endDate", endDate.toISOString());
  if (startRound) queryParams.set("startRound", startRound);
  if (endRound) queryParams.set("endRound", endRound);

  const { data, isLoading, error, refetch } = useQuery<RewardResult>({
    queryKey: ["rewardCalculations", partyId, startDate?.toISOString(), endDate?.toISOString(), startRound, endRound],
    queryFn: async () => {
      const url = `${getDuckDBApiUrl()}/api/rewards/calculate?${queryParams.toString()}`;
      const res = await fetch(url);
      if (!res.ok) {
        const errData = await res.json().catch(() => ({}));
        throw new Error(errData.error || "Failed to fetch reward data");
      }
      return res.json();
    },
    enabled: searchTriggered && !!partyId,
    retry: false,
  });

  const handleSearch = () => {
    if (!partyId.trim()) return;
    setSearchTriggered(true);
    refetch();
  };

  const handleClear = () => {
    setPartyId("");
    setStartDate(undefined);
    setEndDate(undefined);
    setStartRound("");
    setEndRound("");
    setSearchTriggered(false);
  };

  const formatAmount = (amount: number) => {
    return amount.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 6 });
  };

  return (
    <DashboardLayout>
      <div className="space-y-6">
        {/* Header */}
        <div>
          <h1 className="text-3xl font-bold mb-2 flex items-center gap-3">
            <Calculator className="h-8 w-8 text-primary" />
            Reward Calculations
          </h1>
          <p className="text-muted-foreground">
            Calculate app rewards for a specific party ID. Filter by date range <strong>or</strong> round numbers (not both required).
          </p>
        </div>

        {/* Search Form */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Search className="h-5 w-5" />
              Query Parameters
            </CardTitle>
            <CardDescription>
              Enter a party ID and optionally filter by date range <strong>or</strong> round numbers
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-6">
            {/* Party ID Input */}
            <div className="space-y-2">
              <Label htmlFor="partyId">Party ID *</Label>
              <Input
                id="partyId"
                placeholder="Enter party ID (e.g., validator::1234...)"
                value={partyId}
                onChange={(e) => setPartyId(e.target.value)}
                className="font-mono text-sm"
              />
            </div>

            {/* Date Range */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label>Start Date</Label>
                <Popover>
                  <PopoverTrigger asChild>
                    <Button
                      variant="outline"
                      className={cn(
                        "w-full justify-start text-left font-normal",
                        !startDate && "text-muted-foreground"
                      )}
                    >
                      <CalendarIcon className="mr-2 h-4 w-4" />
                      {startDate ? format(startDate, "PPP") : "Pick start date"}
                    </Button>
                  </PopoverTrigger>
                  <PopoverContent className="w-auto p-0" align="start">
                    <Calendar
                      mode="single"
                      selected={startDate}
                      onSelect={setStartDate}
                      initialFocus
                      className={cn("p-3 pointer-events-auto")}
                    />
                  </PopoverContent>
                </Popover>
              </div>

              <div className="space-y-2">
                <Label>End Date</Label>
                <Popover>
                  <PopoverTrigger asChild>
                    <Button
                      variant="outline"
                      className={cn(
                        "w-full justify-start text-left font-normal",
                        !endDate && "text-muted-foreground"
                      )}
                    >
                      <CalendarIcon className="mr-2 h-4 w-4" />
                      {endDate ? format(endDate, "PPP") : "Pick end date"}
                    </Button>
                  </PopoverTrigger>
                  <PopoverContent className="w-auto p-0" align="start">
                    <Calendar
                      mode="single"
                      selected={endDate}
                      onSelect={setEndDate}
                      initialFocus
                      className={cn("p-3 pointer-events-auto")}
                    />
                  </PopoverContent>
                </Popover>
              </div>
            </div>

            {/* Round Range */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="startRound">Start Round</Label>
                <Input
                  id="startRound"
                  type="number"
                  placeholder="e.g., 1000000"
                  value={startRound}
                  onChange={(e) => setStartRound(e.target.value)}
                />
              </div>

              <div className="space-y-2">
                <Label htmlFor="endRound">End Round</Label>
                <Input
                  id="endRound"
                  type="number"
                  placeholder="e.g., 2000000"
                  value={endRound}
                  onChange={(e) => setEndRound(e.target.value)}
                />
              </div>
            </div>

            {/* Action Buttons */}
            <div className="flex gap-3">
              <Button onClick={handleSearch} disabled={!partyId.trim() || isLoading}>
                {isLoading ? (
                  <>
                    <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                    Calculating...
                  </>
                ) : (
                  <>
                    <Calculator className="h-4 w-4 mr-2" />
                    Calculate Rewards
                  </>
                )}
              </Button>
              <Button variant="outline" onClick={handleClear}>
                Clear
              </Button>
            </div>
          </CardContent>
        </Card>

        {/* Error State */}
        {error && (
          <Card className="border-destructive">
            <CardContent className="pt-6">
              <div className="flex items-center gap-3 text-destructive">
                <AlertCircle className="h-5 w-5" />
                <p>{(error as Error).message}</p>
              </div>
            </CardContent>
          </Card>
        )}

        {/* Loading State */}
        {isLoading && (
          <Card>
            <CardContent className="pt-6 space-y-4">
              <Skeleton className="h-8 w-48" />
              <div className="grid grid-cols-3 gap-4">
                <Skeleton className="h-24" />
                <Skeleton className="h-24" />
                <Skeleton className="h-24" />
              </div>
              <Skeleton className="h-64" />
            </CardContent>
          </Card>
        )}

        {/* Results */}
        {data && !isLoading && (
          <div className="space-y-6">
            {/* Summary Cards */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <Card className="glass-card">
                <CardContent className="pt-6">
                  <div className="flex items-center gap-4">
                    <div className="p-3 rounded-lg bg-primary/10">
                      <Coins className="h-6 w-6 text-primary" />
                    </div>
                    <div>
                      <p className="text-sm text-muted-foreground">Total Rewards</p>
                      <p className="text-2xl font-bold">{formatAmount(data.totalRewards)} CC</p>
                    </div>
                  </div>
                </CardContent>
              </Card>

              <Card className="glass-card">
                <CardContent className="pt-6">
                  <div className="flex items-center gap-4">
                    <div className="p-3 rounded-lg bg-green-500/10">
                      <Award className="h-6 w-6 text-green-500" />
                    </div>
                    <div>
                      <p className="text-sm text-muted-foreground">Reward Events</p>
                      <p className="text-2xl font-bold">{data.rewardCount.toLocaleString()}</p>
                    </div>
                  </div>
                </CardContent>
              </Card>

              <Card className="glass-card">
                <CardContent className="pt-6">
                  <div className="flex items-center gap-4">
                    <div className="p-3 rounded-lg bg-amber-500/10">
                      <FileText className="h-6 w-6 text-amber-500" />
                    </div>
                    <div>
                      <p className="text-sm text-muted-foreground">Unique Rounds</p>
                      <p className="text-2xl font-bold">{Object.keys(data.byRound || {}).length.toLocaleString()}</p>
                    </div>
                  </div>
                </CardContent>
              </Card>
            </div>

            {/* Party ID Display */}
            <Card>
              <CardHeader className="pb-3">
                <CardTitle className="text-lg">Query Result</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="flex items-center gap-2 mb-4">
                  <Badge variant="outline">Party ID</Badge>
                  <code className="text-sm font-mono bg-muted px-2 py-1 rounded break-all">
                    {data.partyId}
                  </code>
                </div>
              </CardContent>
            </Card>

            {/* Reward Events Table */}
            {data.events && data.events.length > 0 && (
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Award className="h-5 w-5" />
                    Reward Events ({data.events.length})
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <ScrollArea className="h-[400px]">
                    <div className="space-y-1">
                      {/* Header */}
                      <div className="grid grid-cols-4 gap-4 text-xs text-muted-foreground font-medium px-3 py-2 bg-muted/50 rounded sticky top-0">
                        <span>Round</span>
                        <span>Amount</span>
                        <span>Template</span>
                        <span>Effective At</span>
                      </div>
                      {data.events.map((event, idx) => (
                        <div
                          key={event.event_id || idx}
                          className={`grid grid-cols-4 gap-4 text-sm py-2 px-3 rounded hover:bg-muted/50 ${
                            idx % 2 === 0 ? "bg-muted/20" : ""
                          }`}
                        >
                          <span className="font-mono">{event.round?.toLocaleString()}</span>
                          <span className="font-mono text-green-500">
                            +{formatAmount(event.amount)} CC
                          </span>
                          <span className="text-xs truncate" title={event.template_id}>
                            {event.template_id?.split(":").pop() || "—"}
                          </span>
                          <span className="text-muted-foreground text-xs">
                            {event.effective_at
                              ? format(new Date(event.effective_at), "yyyy-MM-dd HH:mm")
                              : "—"}
                          </span>
                        </div>
                      ))}
                    </div>
                  </ScrollArea>
                </CardContent>
              </Card>
            )}

            {/* No events message */}
            {data.events && data.events.length === 0 && (
              <Card>
                <CardContent className="pt-6">
                  <div className="text-center text-muted-foreground py-8">
                    <Award className="h-12 w-12 mx-auto mb-4 opacity-50" />
                    <p>No reward events found for this party in the specified range.</p>
                  </div>
                </CardContent>
              </Card>
            )}
          </div>
        )}

        {/* Initial State */}
        {!searchTriggered && !isLoading && (
          <Card className="bg-muted/30">
            <CardContent className="pt-6">
              <div className="text-center text-muted-foreground py-8">
                <Calculator className="h-12 w-12 mx-auto mb-4 opacity-50" />
                <p className="mb-2">Enter a party ID to calculate their app rewards.</p>
                <p className="text-sm">
                  Optionally filter by date range or round numbers for more specific results.
                </p>
              </div>
            </CardContent>
          </Card>
        )}
      </div>
    </DashboardLayout>
  );
};

export default RewardCalculations;
