import { DashboardLayout } from "@/components/DashboardLayout";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Package, Star, Code, Database, FileText, ExternalLink, Clock, CheckCircle2, MessageCircle } from "lucide-react";
import { Skeleton } from "@/components/ui/skeleton";
import { useLatestACSSnapshot } from "@/hooks/use-acs-snapshots";
import { useAggregatedTemplateData } from "@/hooks/use-aggregated-template-data";
import { DataSourcesFooter } from "@/components/DataSourcesFooter";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Button } from "@/components/ui/button";
import { useACSStatus } from "@/hooks/use-local-acs";
import { ACSStatusBanner } from "@/components/ACSStatusBanner";
import { useFeaturedAppGovernance, STAGE_CONFIG, getCurrentStage, getLatestTopic } from "@/hooks/use-governance-lifecycle";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";

const Apps = () => {
  const { data: acsStatus } = useACSStatus();
  const { data: latestSnapshot } = useLatestACSSnapshot();

  const appsQuery = useAggregatedTemplateData(latestSnapshot?.id, "Splice:Amulet:FeaturedAppRight");
  const activityQuery = useAggregatedTemplateData(
    latestSnapshot?.id,
    "Splice:Amulet:FeaturedAppActivityMarker",
  );
  const governanceQuery = useFeaturedAppGovernance();

  const isLoading = appsQuery.isLoading || activityQuery.isLoading;
  const apps = appsQuery.data?.data || [];
  const activities = activityQuery.data?.data || [];
  const governanceItems = governanceQuery.data?.lifecycleItems || [];

  // Helper to safely extract field values from nested structure
  const getField = (record: any, ...fieldNames: string[]) => {
    for (const field of fieldNames) {
      if (record[field] !== undefined && record[field] !== null) return record[field];
      if (record.payload?.[field] !== undefined && record.payload?.[field] !== null) return record.payload[field];
    }
    return undefined;
  };

  // Normalize app name for comparison
  const normalizeAppName = (name: string) => {
    return name?.toLowerCase().trim().replace(/[^a-z0-9]/g, '') || '';
  };

  // Find governance item matching an app
  const findGovernanceMatch = (appName: string) => {
    const normalizedAppName = normalizeAppName(appName);
    return governanceItems.find(item => {
      const normalizedPrimaryId = normalizeAppName(item.primaryId);
      // Also check identifiers.appName from topics
      const topicAppNames = item.topics?.map(t => normalizeAppName(t.identifiers?.appName || '')) || [];
      return normalizedPrimaryId === normalizedAppName || 
             normalizedPrimaryId.includes(normalizedAppName) ||
             normalizedAppName.includes(normalizedPrimaryId) ||
             topicAppNames.some(name => name === normalizedAppName || name.includes(normalizedAppName));
    });
  };

  // Get set of on-chain app names for filtering pending apps
  const onChainAppNames = new Set(
    apps.map((app: any) => normalizeAppName(getField(app, "appName", "name", "applicationName") || ""))
  );

  // Filter governance items that are NOT on-chain yet (pending apps)
  const pendingApps = governanceItems.filter(item => {
    const normalizedName = normalizeAppName(item.primaryId);
    // Check if any on-chain app matches this governance item
    return !Array.from(onChainAppNames).some(onChainName => 
      onChainName === normalizedName || 
      onChainName.includes(normalizedName) || 
      normalizedName.includes(onChainName)
    );
  });

  const formatPartyId = (id: string) => id?.split("::")[0] || id;

  // Render governance stage badge
  const renderStageBadge = (stage: string, sourceUrl?: string) => {
    const config = STAGE_CONFIG[stage] || { label: stage, color: "bg-muted text-muted-foreground", order: 0 };
    const badge = (
      <Badge variant="outline" className={`${config.color} border`}>
        {stage.includes("announce") ? (
          <CheckCircle2 className="h-3 w-3 mr-1" />
        ) : (
          <MessageCircle className="h-3 w-3 mr-1" />
        )}
        {config.label}
      </Badge>
    );

    if (sourceUrl) {
      return (
        <a href={sourceUrl} target="_blank" rel="noopener noreferrer" className="hover:opacity-80 transition-opacity">
          {badge}
        </a>
      );
    }
    return badge;
  };

  return (
    <DashboardLayout>
      <TooltipProvider>
        <div className="space-y-8">
          <ACSStatusBanner />
          <div>
            <div className="flex items-center gap-2 mb-2">
              <Package className="h-8 w-8 text-primary" />
              <h1 className="text-3xl font-bold">Canton Network Apps</h1>
              {acsStatus?.available && (
                <Badge variant="outline" className="bg-green-500/10 text-green-500 border-green-500/30">
                  <Database className="h-3 w-3 mr-1" />
                  Local ACS
                </Badge>
              )}
            </div>
            <p className="text-muted-foreground">Featured applications on the Canton Network</p>
          </div>

          {isLoading && (
            <div className="space-y-4">
              {[1, 2, 3].map((i) => (
                <Skeleton key={i} className="h-32 w-full" />
              ))}
            </div>
          )}

          {!isLoading && apps.length === 0 && pendingApps.length === 0 && (
            <Card className="p-8 text-center">
              <Package className="h-12 w-12 mx-auto mb-4 text-muted-foreground" />
              <h3 className="text-lg font-semibold mb-2">No Apps Found</h3>
            </Card>
          )}

          {/* Featured Applications (On-Chain) */}
          {!isLoading && apps.length > 0 && (
            <section>
              <div className="flex items-center justify-between mb-4">
                <h2 className="text-2xl font-semibold">Featured Applications</h2>
                <Badge variant="secondary">{apps.length} On-Chain</Badge>
              </div>
              <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
                {apps.map((app: any, i: number) => {
                  const appName = getField(app, "appName", "name", "applicationName");
                  const provider = getField(app, "provider", "providerId", "providerParty");
                  const dso = getField(app, "dso");
                  const governanceMatch = appName ? findGovernanceMatch(appName) : null;
                  const currentStage = governanceMatch ? getCurrentStage(governanceMatch) : null;
                  const latestTopic = governanceMatch ? getLatestTopic(governanceMatch) : null;

                  return (
                    <Card key={i} className="p-6 space-y-3">
                      <div className="flex items-center justify-between mb-2">
                        <div className="flex items-center gap-2">
                          <Package className="h-5 w-5 text-primary" />
                          <h3 className="font-semibold text-lg">{appName || "Unknown App"}</h3>
                        </div>
                        <Badge className="gradient-primary">
                          <Star className="h-3 w-3 mr-1" />
                          Featured
                        </Badge>
                      </div>

                      {/* Governance Stage Badge */}
                      {currentStage && (
                        <div className="flex items-center gap-2">
                          <span className="text-xs text-muted-foreground">Governance:</span>
                          <Tooltip>
                            <TooltipTrigger asChild>
                              {renderStageBadge(currentStage, latestTopic?.sourceUrl)}
                            </TooltipTrigger>
                            <TooltipContent>
                              <p className="text-sm">
                                {latestTopic?.subject?.slice(0, 100)}
                                {latestTopic?.subject && latestTopic.subject.length > 100 ? "..." : ""}
                              </p>
                              {latestTopic?.date && (
                                <p className="text-xs text-muted-foreground mt-1">
                                  {new Date(latestTopic.date).toLocaleDateString()}
                                </p>
                              )}
                            </TooltipContent>
                          </Tooltip>
                          {latestTopic?.sourceUrl && (
                            <a 
                              href={latestTopic.sourceUrl} 
                              target="_blank" 
                              rel="noopener noreferrer"
                              className="text-muted-foreground hover:text-primary transition-colors"
                            >
                              <ExternalLink className="h-3 w-3" />
                            </a>
                          )}
                        </div>
                      )}

                      <div className="space-y-2">
                        <div>
                          <p className="text-xs text-muted-foreground">Provider (PartyId)</p>
                          <p className="font-mono text-xs break-all">{formatPartyId(provider || "Unknown")}</p>
                        </div>
                        {dso && (
                          <div>
                            <p className="text-xs text-muted-foreground">DSO</p>
                            <p className="font-mono text-xs break-all">{dso}</p>
                          </div>
                        )}
                      </div>

                      <Collapsible className="pt-2 border-t">
                        <CollapsibleTrigger asChild>
                          <Button variant="ghost" size="sm" className="w-full justify-start">
                            <Code className="h-4 w-4 mr-2" />
                            Show Raw JSON
                          </Button>
                        </CollapsibleTrigger>
                        <CollapsibleContent className="mt-2">
                          <pre className="text-xs bg-muted p-3 rounded overflow-auto max-h-96">
                            {JSON.stringify(app, null, 2)}
                          </pre>
                        </CollapsibleContent>
                      </Collapsible>
                    </Card>
                  );
                })}
              </div>
            </section>
          )}

          {/* Pending Apps (In Governance but not on-chain yet) */}
          {!isLoading && pendingApps.length > 0 && (
            <section>
              <div className="flex items-center justify-between mb-4">
                <div className="flex items-center gap-2">
                  <Clock className="h-6 w-6 text-muted-foreground" />
                  <h2 className="text-2xl font-semibold">Pending Apps</h2>
                </div>
                <Badge variant="outline">{pendingApps.length} In Governance</Badge>
              </div>
              <p className="text-sm text-muted-foreground mb-4">
                Apps currently in the governance process but not yet on-chain
              </p>
              <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
                {pendingApps.slice(0, 12).map((item, i) => {
                  const currentStage = getCurrentStage(item);
                  const latestTopic = getLatestTopic(item);

                  return (
                    <Card key={item.id || i} className="p-6 space-y-3 border-dashed">
                      <div className="flex items-center justify-between mb-2">
                        <div className="flex items-center gap-2">
                          <FileText className="h-5 w-5 text-muted-foreground" />
                          <h3 className="font-semibold text-lg">{item.primaryId}</h3>
                        </div>
                        {renderStageBadge(currentStage, latestTopic?.sourceUrl)}
                      </div>

                      {item.network && (
                        <Badge variant="outline" className="text-xs">
                          {item.network === "mainnet" ? "MainNet" : "TestNet"}
                        </Badge>
                      )}

                      {latestTopic && (
                        <div className="space-y-2">
                          <div>
                            <p className="text-xs text-muted-foreground">Latest Discussion</p>
                            <p className="text-sm line-clamp-2">{latestTopic.subject}</p>
                          </div>
                          <div className="flex items-center justify-between text-xs text-muted-foreground">
                            <span>{new Date(latestTopic.date).toLocaleDateString()}</span>
                            <a 
                              href={latestTopic.sourceUrl} 
                              target="_blank" 
                              rel="noopener noreferrer"
                              className="flex items-center gap-1 hover:text-primary transition-colors"
                            >
                              View Discussion
                              <ExternalLink className="h-3 w-3" />
                            </a>
                          </div>
                        </div>
                      )}
                    </Card>
                  );
                })}
              </div>
              {pendingApps.length > 12 && (
                <p className="text-center text-sm text-muted-foreground mt-4">
                  Showing 12 of {pendingApps.length} pending apps
                </p>
              )}
            </section>
          )}

          <DataSourcesFooter
            snapshotId={latestSnapshot?.id}
            templateSuffixes={["Splice:Amulet:FeaturedAppRight", "Splice:Amulet:FeaturedAppActivityMarker"]}
            isProcessing={false}
          />
        </div>
      </TooltipProvider>
    </DashboardLayout>
  );
};

export default Apps;
