import { useState } from "react";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Badge } from "@/components/ui/badge";
import { Search, Package, Code, Database } from "lucide-react";
import { PaginationControls } from "@/components/PaginationControls";
import { DataSourcesFooter } from "@/components/DataSourcesFooter";
import { useAggregatedTemplateData } from "@/hooks/use-aggregated-template-data";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Button } from "@/components/ui/button";
import { useLocalACSAvailable } from "@/hooks/use-local-acs";

const Subscriptions = () => {
  const [searchTerm, setSearchTerm] = useState("");
  const [currentPage, setCurrentPage] = useState(1);
  const [expandedRequests, setExpandedRequests] = useState<Set<number>>(new Set());
  const pageSize = 100;
  const { data: dataAvailable } = useLocalACSAvailable();

  // Fetch from updates data (no snapshot required)
  const subscriptionsQuery = useAggregatedTemplateData(
    undefined,
    "Wallet:Subscriptions:Subscription",
  );

  const idleStatesQuery = useAggregatedTemplateData(
    undefined,
    "Wallet:Subscriptions:SubscriptionIdleState",
  );

  const requestsQuery = useAggregatedTemplateData(
    undefined,
    "Wallet:Subscriptions:SubscriptionRequest",
  );

  const subscriptionsData = subscriptionsQuery.data?.data || [];
  const idleStatesData = idleStatesQuery.data?.data || [];
  const requestsData = requestsQuery.data?.data || [];
  const isLoading = subscriptionsQuery.isLoading || idleStatesQuery.isLoading || requestsQuery.isLoading;
  const dataSource = subscriptionsQuery.data?.source || "unknown";

  // Helper to safely extract field values from nested structure
  const getField = (record: any, ...fieldNames: string[]) => {
    for (const field of fieldNames) {
      if (record[field] !== undefined && record[field] !== null) return record[field];
      if (record.payload?.[field] !== undefined && record.payload?.[field] !== null) return record.payload[field];
    }
    return undefined;
  };

  // Debug logging for requests data
  console.log("🔍 DEBUG: Total requestsData count:", requestsData.length);
  console.log("🔍 DEBUG: First 3 requests raw data:", requestsData.slice(0, 3));
  if (requestsData.length > 0) {
    console.log("🔍 DEBUG: First request structure:", JSON.stringify(requestsData[0], null, 2));
  }

  const formatParty = (party: string) => {
    if (!party) return "Unknown";
    if (party.length > 30) {
      return `${party.substring(0, 15)}...${party.substring(party.length - 12)}`;
    }
    return party;
  };

  const filteredSubscriptions = subscriptionsData.filter((sub: any) => {
    if (!searchTerm) return true;
    const reference = getField(sub, "subscription")?.reference || getField(sub, "reference");
    const subscriber = getField(sub, "subscription")?.subscriber || getField(sub, "subscriber");
    return (
      (reference?.toLowerCase() || "").includes(searchTerm.toLowerCase()) ||
      (subscriber?.toLowerCase() || "").includes(searchTerm.toLowerCase())
    );
  });

  const filteredIdleStates = idleStatesData.filter((state: any) => {
    if (!searchTerm) return true;
    const reference = getField(state, "subscriptionReference", "reference");
    return (reference?.toLowerCase() || "").includes(searchTerm.toLowerCase());
  });

  const filteredRequests = requestsData.filter((req: any) => {
    if (!searchTerm) return true;
    const search = searchTerm.toLowerCase();
    const subscriptionData = getField(req, "subscriptionData");
    const sender = subscriptionData?.sender || getField(req, "sender");
    const receiver = subscriptionData?.receiver || getField(req, "receiver");
    const description = subscriptionData?.description || getField(req, "description");
    const subscription = getField(req, "subscription");
    const reference = subscription?.reference || getField(req, "reference");
    return (
      (sender?.toLowerCase() || "").includes(search) ||
      (receiver?.toLowerCase() || "").includes(search) ||
      (description?.toLowerCase() || "").includes(search) ||
      (reference?.toLowerCase() || "").includes(search)
    );
  });

  const paginateData = (data: any[]) => {
    return data.slice((currentPage - 1) * pageSize, currentPage * pageSize);
  };

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <div className="flex items-center gap-2 mb-2">
            <Package className="h-8 w-8 text-primary" />
            <h1 className="text-3xl font-bold">Wallet Subscriptions</h1>
            {dataAvailable && (
              <Badge variant="outline" className="bg-green-500/10 text-green-500 border-green-500/30">
                <Database className="h-3 w-3 mr-1" />
                {dataSource === "updates" ? "Updates" : dataSource === "acs-fallback" ? "ACS" : "Local"}
              </Badge>
            )}
          </div>
          <p className="text-muted-foreground">
            View active subscriptions, idle states, and pending subscription requests.
          </p>
        </div>

        <div className="grid gap-4 md:grid-cols-3">
          <Card className="p-6">
            <h3 className="text-sm font-medium text-muted-foreground mb-2">Active Subscriptions</h3>
            {isLoading ? (
              <Skeleton className="h-8 w-24" />
            ) : (
              <p className="text-2xl font-bold">{subscriptionsData.length}</p>
            )}
          </Card>

          <Card className="p-6">
            <h3 className="text-sm font-medium text-muted-foreground mb-2">Idle States</h3>
            {isLoading ? (
              <Skeleton className="h-8 w-24" />
            ) : (
              <p className="text-2xl font-bold">{idleStatesData.length}</p>
            )}
          </Card>

          <Card className="p-6">
            <h3 className="text-sm font-medium text-muted-foreground mb-2">Pending Requests</h3>
            {isLoading ? (
              <Skeleton className="h-8 w-24" />
            ) : (
              <p className="text-2xl font-bold">{requestsData.length}</p>
            )}
          </Card>
        </div>

        <Card className="p-6">
          <div className="mb-4">
            <div className="relative">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-muted-foreground h-4 w-4" />
              <Input
                type="text"
                placeholder="Search by reference..."
                value={searchTerm}
                onChange={(e) => {
                  setSearchTerm(e.target.value);
                  setCurrentPage(1);
                }}
                className="pl-10"
              />
            </div>
          </div>

          <Tabs defaultValue="active" className="w-full" onValueChange={() => setCurrentPage(1)}>
            <TabsList className="grid w-full grid-cols-3">
              <TabsTrigger value="active">Active ({filteredSubscriptions.length})</TabsTrigger>
              <TabsTrigger value="idle">Idle ({filteredIdleStates.length})</TabsTrigger>
              <TabsTrigger value="requests">Requests ({filteredRequests.length})</TabsTrigger>
            </TabsList>

            <TabsContent value="active" className="space-y-3 mt-4">
              {isLoading ? (
                <div className="space-y-3">
                  {[1, 2, 3].map((i) => (
                    <Skeleton key={i} className="h-20 w-full" />
                  ))}
                </div>
              ) : filteredSubscriptions.length === 0 ? (
                <div className="text-center py-8 text-muted-foreground">
                  <Package className="h-12 w-12 mx-auto mb-4 opacity-50" />
                  <p>No active subscriptions found</p>
                </div>
              ) : (
                <>
                  <div className="space-y-3">
                    {paginateData(filteredSubscriptions).map((sub: any, i: number) => (
                      <Card key={i} className="p-4">
                        <div className="flex justify-between items-start">
                          <div>
                            <p className="text-sm text-muted-foreground">Reference</p>
                            <p className="font-mono text-sm">
                              {formatParty(
                                sub.payload?.subscription?.reference || sub.subscription?.reference || sub.reference,
                              )}
                            </p>
                            <p className="text-sm text-muted-foreground mt-2">Subscriber</p>
                            <p className="font-mono text-sm">
                              {formatParty(
                                sub.payload?.subscription?.subscriber || sub.subscription?.subscriber || sub.subscriber,
                              )}
                            </p>
                          </div>
                          <Badge variant="default">Active</Badge>
                        </div>
                      </Card>
                    ))}
                  </div>
                  <PaginationControls
                    currentPage={currentPage}
                    totalItems={filteredSubscriptions.length}
                    pageSize={pageSize}
                    onPageChange={setCurrentPage}
                  />
                </>
              )}
            </TabsContent>

            <TabsContent value="idle" className="space-y-3 mt-4">
              {isLoading ? (
                <div className="space-y-3">
                  {[1, 2, 3].map((i) => (
                    <Skeleton key={i} className="h-20 w-full" />
                  ))}
                </div>
              ) : filteredIdleStates.length === 0 ? (
                <div className="text-center py-8 text-muted-foreground">
                  <Package className="h-12 w-12 mx-auto mb-4 opacity-50" />
                  <p>No idle states found</p>
                </div>
              ) : (
                <>
                  <div className="space-y-3">
                    {paginateData(filteredIdleStates).map((idle: any, i: number) => (
                      <Card key={i} className="p-4">
                        <div className="flex justify-between items-start">
                          <div>
                            <p className="text-sm text-muted-foreground">Reference</p>
                            <p className="font-mono text-sm">
                              {formatParty(idle.payload?.subscriptionReference || idle.subscriptionReference)}
                            </p>
                          </div>
                          <Badge variant="secondary">Idle</Badge>
                        </div>
                      </Card>
                    ))}
                  </div>
                  <PaginationControls
                    currentPage={currentPage}
                    totalItems={filteredIdleStates.length}
                    pageSize={pageSize}
                    onPageChange={setCurrentPage}
                  />
                </>
              )}
            </TabsContent>

            <TabsContent value="requests" className="space-y-3 mt-4">
              {isLoading ? (
                <div className="space-y-3">
                  {[1, 2, 3].map((i) => (
                    <Skeleton key={i} className="h-20 w-full" />
                  ))}
                </div>
              ) : filteredRequests.length === 0 ? (
                <div className="text-center py-8 text-muted-foreground">
                  <Package className="h-12 w-12 mx-auto mb-4 opacity-50" />
                  <p>No pending requests found</p>
                </div>
              ) : (
                <>
                  <div className="space-y-3">
                    {paginateData(filteredRequests).map((req: any, i: number) => {
                      // Debug logging for each request
                      console.log(`🔍 DEBUG: Request ${i}:`, {
                        hasPayload: !!req.payload,
                        hasSubscriptionData: !!(req.payload?.subscriptionData || req.subscriptionData),
                        hasPayData: !!(req.payload?.payData || req.payData),
                        keys: Object.keys(req),
                        fullObject: req,
                      });

                      const subData = req.payload?.subscriptionData || req.subscriptionData || {};
                      const payData = req.payload?.payData || req.payData || {};
                      const paymentAmount = payData.paymentAmount?.amount || "N/A";
                      const paymentUnit = payData.paymentAmount?.unit || "";
                      const paymentInterval = payData.paymentInterval?.microseconds
                        ? (parseInt(payData.paymentInterval.microseconds) / 1000000 / 60 / 60 / 24).toFixed(0) + " days"
                        : "N/A";
                      const paymentDuration = payData.paymentDuration?.microseconds
                        ? (parseInt(payData.paymentDuration.microseconds) / 1000000 / 60 / 60 / 24).toFixed(0) + " days"
                        : "N/A";

                      return (
                        <Card key={i} className="p-4 space-y-3">
                          <div className="flex justify-between items-start">
                            <div className="flex-1 space-y-2">
                              {subData.description && (
                                <div>
                                  <p className="text-sm font-semibold text-primary">{subData.description}</p>
                                </div>
                              )}

                              <div className="grid grid-cols-2 gap-3 text-sm">
                                <div>
                                  <p className="text-xs text-muted-foreground">Sender</p>
                                  <p className="font-mono text-xs break-all">
                                    {formatParty(subData.sender || "Unknown")}
                                  </p>
                                </div>
                                <div>
                                  <p className="text-xs text-muted-foreground">Receiver</p>
                                  <p className="font-mono text-xs break-all">
                                    {formatParty(subData.receiver || "Unknown")}
                                  </p>
                                </div>
                                <div>
                                  <p className="text-xs text-muted-foreground">Provider</p>
                                  <p className="font-mono text-xs break-all">
                                    {formatParty(subData.provider || "Unknown")}
                                  </p>
                                </div>
                                <div>
                                  <p className="text-xs text-muted-foreground">DSO</p>
                                  <p className="font-mono text-xs break-all">{formatParty(subData.dso || "Unknown")}</p>
                                </div>
                              </div>

                              <div className="grid grid-cols-3 gap-3 pt-2 border-t">
                                <div>
                                  <p className="text-xs text-muted-foreground">Payment Amount</p>
                                  <p className="text-sm font-semibold">
                                    {paymentAmount} {paymentUnit}
                                  </p>
                                </div>
                                <div>
                                  <p className="text-xs text-muted-foreground">Interval</p>
                                  <p className="text-sm">{paymentInterval}</p>
                                </div>
                                <div>
                                  <p className="text-xs text-muted-foreground">Duration</p>
                                  <p className="text-sm">{paymentDuration}</p>
                                </div>
                              </div>

                              <Collapsible className="mt-3 pt-3 border-t">
                                <CollapsibleTrigger asChild>
                                  <Button variant="ghost" size="sm" className="w-full justify-start">
                                    <Code className="h-4 w-4 mr-2" />
                                    Show Raw JSON
                                  </Button>
                                </CollapsibleTrigger>
                                <CollapsibleContent className="mt-2">
                                  <pre className="text-xs bg-muted p-3 rounded overflow-auto max-h-96">
                                    {JSON.stringify(req, null, 2)}
                                  </pre>
                                </CollapsibleContent>
                              </Collapsible>
                            </div>
                            <Badge variant="outline" className="ml-3">
                              Pending
                            </Badge>
                          </div>
                        </Card>
                      );
                    })}
                  </div>
                  <PaginationControls
                    currentPage={currentPage}
                    totalItems={filteredRequests.length}
                    pageSize={pageSize}
                    onPageChange={setCurrentPage}
                  />
                </>
              )}
            </TabsContent>
          </Tabs>
        </Card>

        <DataSourcesFooter
          snapshotId={undefined}
          templateSuffixes={[
            "Wallet:Subscriptions:Subscription",
            "Wallet:Subscriptions:SubscriptionIdleState",
            "Wallet:Subscriptions:SubscriptionRequest",
          ]}
          isProcessing={false}
        />
      </div>
    </DashboardLayout>
  );
};

export default Subscriptions;
