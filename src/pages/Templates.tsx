import { DashboardLayout } from "@/components/DashboardLayout";
import { Card } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { FileJson, Database, ChevronDown, ChevronRight, Download, FileText, Code } from "lucide-react";
import { useState } from "react";
import { getPagesThatUseTemplate } from "@/lib/template-page-map";
import { useDsoInfo, useStateAcs } from "@/hooks/use-canton-scan-api";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Link } from "react-router-dom";

const Templates = () => {
  const [selectedTemplate, setSelectedTemplate] = useState<string | null>(null);

  // Fetch DSO info to get available templates from the network
  const { data: dsoInfo, isLoading: dsoLoading } = useDsoInfo();

  // Common Canton templates that are typically available
  const commonTemplates = [
    "Splice.AmuletRules:AmuletRules",
    "Splice.Amulet:Amulet",
    "Splice.Amulet:ValidatorRewardCoupon",
    "Splice.Amulet:SvRewardCoupon",
    "Splice.Amulet:AppRewardCoupon",
    "Splice.ValidatorLicense:ValidatorLicense",
    "Splice.Ans:AnsEntry",
    "Splice.ExternalPartyAmuletRules:TransferCommandCounter",
    "Splice.ExternalPartyAmuletRules:ExternalPartySetupProposal",
    "Splice.FeaturedAppRight:FeaturedAppRight",
  ];

  // Fetch sample data for the selected template
  const { data: templateData, isLoading: dataLoading } = useStateAcs(
    selectedTemplate ? [selectedTemplate] : [],
    10 // Limit to 10 samples
  );

  const analyzeDataStructure = (data: any[]): any => {
    if (!data || data.length === 0) return null;

    const sampleEntry = data[0];
    const structure: any = {};

    const analyzeValue = (value: any, path: string = ""): any => {
      if (value === null || value === undefined) {
        return { type: "null", example: null };
      }

      if (Array.isArray(value)) {
        return {
          type: "array",
          length: value.length,
          itemType: value.length > 0 ? analyzeValue(value[0], path) : "unknown",
        };
      }

      if (typeof value === "object") {
        const nested: any = {};
        Object.keys(value).forEach((key) => {
          nested[key] = analyzeValue(value[key], `${path}.${key}`);
        });
        return { type: "object", fields: nested };
      }

      if (typeof value === "number") {
        return { type: "number", example: value };
      }

      if (typeof value === "boolean") {
        return { type: "boolean", example: value };
      }

      if (typeof value === "string") {
        if (!isNaN(Number(value)) && value !== "") {
          return { type: "string (numeric)", example: value };
        }
        return { type: "string", example: value.length > 50 ? value.substring(0, 50) + "..." : value };
      }

      return { type: typeof value, example: value };
    };

    Object.keys(sampleEntry).forEach((key) => {
      structure[key] = analyzeValue(sampleEntry[key], key);
    });

    return structure;
  };

  const renderStructure = (structure: any, depth: number = 0): JSX.Element[] => {
    if (!structure) return [];

    return Object.entries(structure).map(([key, value]: [string, any]) => {
      const indent = depth * 20;

      if (value.type === "object" && value.fields) {
        return (
          <div key={key} style={{ marginLeft: indent }}>
            <div className="flex items-center gap-2 py-1">
              <Badge variant="outline" className="text-xs">
                object
              </Badge>
              <code className="text-sm font-mono text-foreground">{key}</code>
            </div>
            {renderStructure(value.fields, depth + 1)}
          </div>
        );
      }

      if (value.type === "array") {
        return (
          <div key={key} style={{ marginLeft: indent }}>
            <div className="flex items-center gap-2 py-1">
              <Badge variant="outline" className="text-xs">
                array[{value.length}]
              </Badge>
              <code className="text-sm font-mono text-foreground">{key}</code>
            </div>
            {value.itemType && typeof value.itemType === "object" && (
              <div style={{ marginLeft: indent + 20 }}>
                <span className="text-xs text-muted-foreground">Item structure:</span>
                {renderStructure({ item: value.itemType }, depth + 1)}
              </div>
            )}
          </div>
        );
      }

      return (
        <div key={key} style={{ marginLeft: indent }} className="flex items-center gap-2 py-1">
          <Badge variant="secondary" className="text-xs">
            {value.type}
          </Badge>
          <code className="text-sm font-mono text-foreground">{key}</code>
          {value.example !== undefined && value.example !== null && (
            <span className="text-xs text-muted-foreground ml-2">
              = {typeof value.example === "string" ? `"${value.example}"` : String(value.example)}
            </span>
          )}
        </div>
      );
    });
  };

  const structure = templateData ? analyzeDataStructure(templateData) : null;

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <h2 className="text-3xl font-bold">Template Data Explorer</h2>
              <Badge variant="outline" className="bg-primary/10 text-primary border-primary/30">
                <Database className="h-3 w-3 mr-1" />
                Live API
              </Badge>
            </div>
            <Link to="/template-docs">
              <Button variant="outline" size="sm">
                <FileText className="h-4 w-4 mr-2" />
                Download All Documentation
              </Button>
            </Link>
          </div>
          <p className="text-muted-foreground mb-2">
            Explore available templates and their data structures from the live Canton network
          </p>
          <p className="text-xs text-muted-foreground bg-muted/30 p-3 rounded-lg">
            <strong>Template ID Format:</strong>{" "}
            <code className="bg-background px-1 rounded">Module:Entity</code>
            <br />
            Templates are fetched directly from the Canton Scan API <code>/v0/state/acs</code> endpoint.
          </p>
        </div>

        {/* DSO Info */}
        {dsoInfo && (
          <Card className="glass-card p-4">
            <div className="flex items-center gap-4 text-sm">
              <div className="flex items-center gap-2">
                <Database className="h-4 w-4 text-primary" />
                <span className="text-muted-foreground">Latest Round:</span>
                <code className="text-foreground">
                  {dsoInfo.latest_mining_round?.contract?.payload?.round?.number || "—"}
                </code>
              </div>
              <div className="flex items-center gap-2">
                <span className="text-muted-foreground">SVs:</span>
                <code className="text-foreground">{dsoInfo.sv_node_states?.length || 0}</code>
              </div>
            </div>
          </Card>
        )}

        {dsoLoading && <Skeleton className="h-16 w-full" />}

        <div className="space-y-4">
          <div className="grid gap-4">
            {commonTemplates.map((templateId) => (
              <div key={templateId}>
                <Card
                  className={`glass-card p-6 cursor-pointer transition-all hover:border-primary ${
                    selectedTemplate === templateId ? "border-primary" : ""
                  }`}
                  onClick={() => setSelectedTemplate(selectedTemplate === templateId ? null : templateId)}
                >
                  <div className="flex items-start justify-between">
                    <div className="flex-1">
                      <div className="flex items-center gap-3 mb-2">
                        {selectedTemplate === templateId ? (
                          <ChevronDown className="h-5 w-5 text-primary" />
                        ) : (
                          <ChevronRight className="h-5 w-5 text-muted-foreground" />
                        )}
                        <FileJson className="h-5 w-5 text-primary" />
                        <code className="text-lg font-mono text-foreground">{templateId}</code>
                      </div>
                      <div className="flex items-center gap-2 text-sm text-muted-foreground ml-10 flex-wrap">
                        {getPagesThatUseTemplate(templateId).map((page) => (
                          <Badge key={page} variant="secondary" className="text-xs">
                            Used in: {page}
                          </Badge>
                        ))}
                      </div>
                    </div>
                    <Badge variant={selectedTemplate === templateId ? "default" : "outline"}>
                      {selectedTemplate === templateId ? "Expanded" : "Click to expand"}
                    </Badge>
                  </div>
                </Card>

                {selectedTemplate === templateId && (
                  <div className="ml-6 mt-2 space-y-4">
                    {dataLoading ? (
                      <Skeleton className="h-96 w-full" />
                    ) : templateData && templateData.length > 0 && structure ? (
                      <>
                        <Card className="glass-card p-6">
                          <h3 className="text-xl font-bold mb-4">Data Structure</h3>
                          <p className="text-sm text-muted-foreground mb-4">
                            Analyzed from sample entry. Fields and types may vary across entries.
                          </p>
                          <div className="bg-muted/30 rounded-lg p-4 font-mono text-sm overflow-x-auto">
                            {renderStructure(structure)}
                          </div>
                          <p className="text-xs text-muted-foreground mt-4">
                            Based on {templateData.length} sample contract(s)
                          </p>
                        </Card>

                        <Collapsible>
                          <CollapsibleTrigger asChild>
                            <Button variant="outline" className="w-full justify-start">
                              <Code className="h-4 w-4 mr-2" />
                              View Sample Entry (First Record)
                            </Button>
                          </CollapsibleTrigger>
                          <CollapsibleContent>
                            <Card className="glass-card p-6 mt-2">
                              <div className="bg-muted/30 rounded-lg p-4 overflow-x-auto">
                                <pre className="text-xs font-mono">{JSON.stringify(templateData[0], null, 2)}</pre>
                              </div>
                            </Card>
                          </CollapsibleContent>
                        </Collapsible>
                      </>
                    ) : (
                      <Card className="glass-card p-6">
                        <p className="text-muted-foreground">
                          No data available for this template. The template may not have active contracts in the current network state.
                        </p>
                      </Card>
                    )}
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>

        <Card className="p-4 text-xs text-muted-foreground">
          <p>
            Data sourced from Canton Scan API <code>/v0/state/acs</code> endpoint.
          </p>
        </Card>
      </div>
    </DashboardLayout>
  );
};

export default Templates;