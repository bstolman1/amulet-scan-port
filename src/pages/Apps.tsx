import { DashboardLayout } from "@/components/DashboardLayout";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Package, Star, Code, Database, ExternalLink, Users, History } from "lucide-react";
import { Skeleton } from "@/components/ui/skeleton";
import { useLatestACSSnapshot } from "@/hooks/use-acs-snapshots";
import { useAggregatedTemplateData } from "@/hooks/use-aggregated-template-data";
import { DataSourcesFooter } from "@/components/DataSourcesFooter";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Button } from "@/components/ui/button";
import { useACSStatus } from "@/hooks/use-local-acs";
import { ACSStatusBanner } from "@/components/ACSStatusBanner";
import { 
  useGovernanceLifecycle, 
  findLifecycleForApp, 
  getStageColor, 
  getStageLabel,
  GovernanceLifecycleItem 
} from "@/hooks/use-governance-lifecycle";

const Apps = () => {
  const { data: acsStatus } = useACSStatus();
  const { data: latestSnapshot } = useLatestACSSnapshot();
  const { data: lifecycleItems, isLoading: lifecycleLoading } = useGovernanceLifecycle("featured-app");

  const appsQuery = useAggregatedTemplateData(latestSnapshot?.id, "Splice:Amulet:FeaturedAppRight");
  const activityQuery = useAggregatedTemplateData(
    latestSnapshot?.id,
    "Splice:Amulet:FeaturedAppActivityMarker",
  );

  const isLoading = appsQuery.isLoading || activityQuery.isLoading;
  const apps = appsQuery.data?.data || [];
  const activities = activityQuery.data?.data || [];

  // Helper to safely extract field values from nested structure
  const getField = (record: any, ...fieldNames: string[]) => {
    for (const field of fieldNames) {
      if (record[field] !== undefined && record[field] !== null) return record[field];
      if (record.payload?.[field] !== undefined && record.payload?.[field] !== null) return record.payload[field];
    }
    return undefined;
  };

  // Extract all party IDs from app contract (signatories, observers, etc.)
  const getPartyIds = (app: any): string[] => {
    const parties = new Set<string>();
    
    // Add provider
    const provider = getField(app, "provider", "providerId", "providerParty");
    if (provider) parties.add(provider);
    
    // Add signatories
    const signatories = app.signatories || app.payload?.signatories || [];
    if (Array.isArray(signatories)) {
      signatories.forEach((s: string) => parties.add(s));
    }
    
    // Add observers
    const observers = app.observers || app.payload?.observers || [];
    if (Array.isArray(observers)) {
      observers.forEach((o: string) => parties.add(o));
    }
    
    // Add dso
    const dso = getField(app, "dso");
    if (dso) parties.add(dso);
    
    return Array.from(parties);
  };

  // Debug logging
  console.log("🔍 DEBUG Apps: Total apps:", apps.length);
  console.log("🔍 DEBUG Apps: Lifecycle items:", lifecycleItems?.length);

  const formatPartyId = (id: string) => id.split("::")[0] || id;

  return (
    <DashboardLayout>
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
        {!isLoading && apps.length === 0 && (
          <Card className="p-8 text-center">
            <Package className="h-12 w-12 mx-auto mb-4 text-muted-foreground" />
            <h3 className="text-lg font-semibold mb-2">No Apps Found</h3>
          </Card>
        )}
        {!isLoading && apps.length > 0 && (
          <>
            <section>
              <div className="flex items-center justify-between mb-4">
                <h2 className="text-2xl font-semibold">Featured Applications</h2>
                <Badge variant="secondary">{apps.length} Apps</Badge>
              </div>
              <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
                {apps.map((app: any, i: number) => {
                  const appName = getField(app, "appName", "name", "applicationName");
                  const provider = getField(app, "provider", "providerId", "providerParty");
                  const dso = getField(app, "dso");
                  const allParties = getPartyIds(app);
                  const lifecycle = findLifecycleForApp(lifecycleItems, appName);

                  return (
                    <Card key={i} className="p-6 space-y-3">
                      <div className="flex items-center justify-between mb-2">
                        <div className="flex items-center gap-2">
                          <Package className="h-5 w-5 text-primary" />
                          <h3 className="font-semibold text-lg">{appName || "Unknown App"}</h3>
                        </div>
                        {lifecycle && (
                          <Badge variant="outline" className={getStageColor(lifecycle.currentStage)}>
                            {getStageLabel(lifecycle.currentStage)}
                          </Badge>
                        )}
                      </div>
                      
                      <div className="space-y-2">
                        <div>
                          <p className="text-xs text-muted-foreground">Provider</p>
                          <p className="font-mono text-xs break-all">{formatPartyId(provider || "Unknown")}</p>
                        </div>
                        
                        {/* Show all parties if more than just the provider */}
                        {allParties.length > 1 && (
                          <div>
                            <p className="text-xs text-muted-foreground flex items-center gap-1">
                              <Users className="h-3 w-3" />
                              All Parties ({allParties.length})
                            </p>
                            <div className="flex flex-wrap gap-1 mt-1">
                              {allParties.map((party, idx) => (
                                <Badge key={idx} variant="outline" className="text-xs font-mono">
                                  {formatPartyId(party)}
                                </Badge>
                              ))}
                            </div>
                          </div>
                        )}
                        
                        {dso && (
                          <div>
                            <p className="text-xs text-muted-foreground">DSO</p>
                            <p className="font-mono text-xs break-all">{dso}</p>
                          </div>
                        )}
                      </div>
                      
                      <div className="flex items-center gap-2 flex-wrap">
                        <Badge className="gradient-primary">
                          <Star className="h-3 w-3 mr-1" />
                          Featured
                        </Badge>
                        {lifecycle?.network && (
                          <Badge variant="outline" className="text-xs">
                            {lifecycle.network}
                          </Badge>
                        )}
                      </div>

                      {/* Governance Lifecycle Link */}
                      {lifecycle && (
                        <div className="pt-2 border-t">
                          <p className="text-xs text-muted-foreground mb-2 flex items-center gap-1">
                            <History className="h-3 w-3" />
                            Governance History
                          </p>
                          <div className="space-y-1">
                            {lifecycle.topics?.slice(0, 3).map((topic, idx) => (
                              <a
                                key={idx}
                                href={topic.sourceUrl}
                                target="_blank"
                                rel="noopener noreferrer"
                                className="block text-xs text-primary hover:underline truncate"
                                title={topic.subject}
                              >
                                <ExternalLink className="h-3 w-3 inline mr-1" />
                                {topic.subject}
                              </a>
                            ))}
                            {(lifecycle.topics?.length || 0) > 3 && (
                              <p className="text-xs text-muted-foreground">
                                +{lifecycle.topics.length - 3} more topics
                              </p>
                            )}
                          </div>
                        </div>
                      )}

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
                          {lifecycle && (
                            <>
                              <p className="text-xs text-muted-foreground mt-2 mb-1">Governance Lifecycle:</p>
                              <pre className="text-xs bg-muted p-3 rounded overflow-auto max-h-96">
                                {JSON.stringify(lifecycle, null, 2)}
                              </pre>
                            </>
                          )}
                        </CollapsibleContent>
                      </Collapsible>
                    </Card>
                  );
                })}
              </div>
            </section>
          </>
        )}

        <DataSourcesFooter
          snapshotId={latestSnapshot?.id}
          templateSuffixes={["Splice:Amulet:FeaturedAppRight", "Splice:Amulet:FeaturedAppActivityMarker"]}
          isProcessing={false}
        />
      </div>
    </DashboardLayout>
  );
};

export default Apps;
