import { useState } from "react";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Badge } from "@/components/ui/badge";
import { Search, Package, Code } from "lucide-react";
import { PaginationControls } from "@/components/PaginationControls";
import { DataSourcesFooter } from "@/components/DataSourcesFooter";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Button } from "@/components/ui/button";
import { useStateAcs } from "@/hooks/use-canton-scan-api";

const Subscriptions = () => {
  const [searchTerm, setSearchTerm] = useState("");
  const [currentPage, setCurrentPage] = useState(1);
  const pageSize = 100;

  const { data: subscriptionsData, isLoading: subLoading } = useStateAcs(
    ["Wallet.Subscriptions:Subscription"],
    1000
  );

  const { data: idleStatesData, isLoading: idleLoading } = useStateAcs(
    ["Wallet.Subscriptions:SubscriptionIdleState"],
    1000
  );

  const { data: requestsData, isLoading: reqLoading } = useStateAcs(
    ["Wallet.Subscriptions:SubscriptionRequest"],
    1000
  );

  const subscriptions = subscriptionsData || [];
  const idleStates = idleStatesData || [];
  const requests = requestsData || [];
  const isLoading = subLoading || idleLoading || reqLoading;

  const formatParty = (party: string) => {
    if (!party) return "Unknown";
    if (party.length > 30) {
      return `${party.substring(0, 15)}...${party.substring(party.length - 12)}`;
    }
    return party;
  };

  const filteredSubscriptions = subscriptions.filter((sub: any) => {
    if (!searchTerm) return true;
    const reference = sub.create_arguments?.subscription?.reference || sub.create_arguments?.reference;
    const subscriber = sub.create_arguments?.subscription?.subscriber || sub.create_arguments?.subscriber;
    return (
      (reference?.toLowerCase() || "").includes(searchTerm.toLowerCase()) ||
      (subscriber?.toLowerCase() || "").includes(searchTerm.toLowerCase())
    );
  });

  const filteredIdleStates = idleStates.filter((state: any) => {
    if (!searchTerm) return true;
    const reference = state.create_arguments?.subscriptionReference || state.create_arguments?.reference;
    return (reference?.toLowerCase() || "").includes(searchTerm.toLowerCase());
  });

  const filteredRequests = requests.filter((req: any) => {
    if (!searchTerm) return true;
    const search = searchTerm.toLowerCase();
    const subscriptionData = req.create_arguments?.subscriptionData;
    const sender = subscriptionData?.sender;
    const receiver = subscriptionData?.receiver;
    const description = subscriptionData?.description;
    return (
      (sender?.toLowerCase() || "").includes(search) ||
      (receiver?.toLowerCase() || "").includes(search) ||
      (description?.toLowerCase() || "").includes(search)
    );
  });

  const paginateData = (data: any[]) => {
    return data.slice((currentPage - 1) * pageSize, currentPage * pageSize);
  };

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <h1 className="text-3xl font-bold mb-2 flex items-center gap-2">
            <Package className="h-8 w-8 text-primary" />
            Wallet Subscriptions
          </h1>
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
              <p className="text-2xl font-bold">{subscriptions.length}</p>
            )}
          </Card>

          <Card className="p-6">
            <h3 className="text-sm font-medium text-muted-foreground mb-2">Idle States</h3>
            {isLoading ? (
              <Skeleton className="h-8 w-24" />
            ) : (
              <p className="text-2xl font-bold">{idleStates.length}</p>
            )}
          </Card>

          <Card className="p-6">
            <h3 className="text-sm font-medium text-muted-foreground mb-2">Pending Requests</h3>
            {isLoading ? (
              <Skeleton className="h-8 w-24" />
            ) : (
              <p className="text-2xl font-bold">{requests.length}</p>
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
                                sub.create_arguments?.subscription?.reference || sub.create_arguments?.reference,
                              )}
                            </p>
                            <p className="text-sm text-muted-foreground mt-2">Subscriber</p>
                            <p className="font-mono text-sm">
                              {formatParty(
                                sub.create_arguments?.subscription?.subscriber || sub.create_arguments?.subscriber,
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
                              {formatParty(idle.create_arguments?.subscriptionReference)}
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
                      const subData = req.create_arguments?.subscriptionData || {};
                      const payData = req.create_arguments?.payData || {};
                      const paymentAmount = payData.paymentAmount?.amount || "N/A";
                      const paymentUnit = payData.paymentAmount?.unit || "";

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
                              </div>

                              <div className="pt-2 border-t">
                                <p className="text-xs text-muted-foreground">Payment Amount</p>
                                <p className="text-sm font-semibold">
                                  {paymentAmount} {paymentUnit}
                                </p>
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
          templateSuffixes={[]}
          isProcessing={false}
        />
      </div>
    </DashboardLayout>
  );
};

export default Subscriptions;
