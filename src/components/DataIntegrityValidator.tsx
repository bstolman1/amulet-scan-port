import { useState } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import { ShieldCheck, ShieldAlert, AlertTriangle, CheckCircle, XCircle, Loader2, FileSearch } from "lucide-react";
import { useToast } from "@/hooks/use-toast";
import { getDuckDBApiUrl } from "@/lib/backend-config";

interface SchemaCompliance {
  eventsWithTypeOriginal: number;
  eventsWithoutTypeOriginal: number;
  eventsWithCanonicalId: number;
  eventsWithSynthesizedId: number;
  updatesWithRootEventIds: number;
  updatesWithoutRootEventIds: number;
  updatesWithRecordTime: number;
  updatesWithoutRecordTime: number;
  eventsWithChildEventIds: number;
  // Contract ID tracking
  eventsWithContractId: number;
  eventsWithoutContractId: number;
  updatesWithContractId: number;
  updatesWithoutContractId: number;
}

interface ValidationResult {
  success: boolean;
  error?: string;
  totalFiles: number;
  sampledFiles: number;
  integrityScore: number;
  schemaComplianceScore?: number;
  eventFiles: {
    checked: number;
    valid: number;
    missingRawJson: number;
    emptyRecords: number;
  };
  updateFiles: {
    checked: number;
    valid: number;
    missingUpdateDataJson: number;
    emptyRecords: number;
  };
  schemaCompliance?: SchemaCompliance;
  errors: Array<{ file: string; error: string }>;
  sampleDetails: Array<{
    file: string;
    type: string;
    recordCount: number;
    hasRequiredFields: boolean;
    missingFields: string[];
    schemaIssues?: string[];
  }>;
}

export function DataIntegrityValidator() {
  const [isValidating, setIsValidating] = useState(false);
  const [result, setResult] = useState<ValidationResult | null>(null);
  const { toast } = useToast();

  const runValidation = async () => {
    setIsValidating(true);
    setResult(null);
    
    try {
      const backendUrl = getDuckDBApiUrl();
      const response = await fetch(`${backendUrl}/api/backfill/validate-integrity`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sampleSize: 20 }),
      });
      
      // Check if response is JSON (not HTML error page)
      const contentType = response.headers.get('content-type');
      if (!contentType?.includes('application/json')) {
        toast({
          title: "Backend unavailable",
          description: "Make sure the DuckDB API server is running on localhost:3001",
          variant: "destructive",
        });
        return;
      }
      
      const data = await response.json();
      
      if (!data.success) {
        toast({
          title: "Validation failed",
          description: data.error || "Unknown error",
          variant: "destructive",
        });
        return;
      }
      
      setResult(data);
      
      toast({
        title: data.integrityScore >= 90 ? "Validation passed" : "Issues found",
        description: `Integrity score: ${data.integrityScore}%`,
        variant: data.integrityScore >= 90 ? "default" : "destructive",
      });
    } catch (err: any) {
      const isConnectionError = err.message?.includes('fetch') || err.message?.includes('network');
      toast({
        title: isConnectionError ? "Cannot connect to backend" : "Validation error",
        description: isConnectionError 
          ? "Start the DuckDB API server: cd server && npm start" 
          : err.message || "Failed to validate data",
        variant: "destructive",
      });
    } finally {
      setIsValidating(false);
    }
  };

  const getScoreColor = (score: number) => {
    if (score >= 90) return "text-green-500";
    if (score >= 70) return "text-yellow-500";
    return "text-red-500";
  };

  const getScoreIcon = (score: number) => {
    if (score >= 90) return <ShieldCheck className="h-8 w-8 text-green-500" />;
    if (score >= 70) return <AlertTriangle className="h-8 w-8 text-yellow-500" />;
    return <ShieldAlert className="h-8 w-8 text-red-500" />;
  };

  return (
    <Card className="border-primary/20">
      <CardHeader>
        <div className="flex items-center justify-between">
          <div>
            <CardTitle className="flex items-center gap-2">
              <FileSearch className="h-5 w-5" />
              Data Integrity Validator
            </CardTitle>
            <CardDescription>
              Validates raw_json, update_data_json, event_type_original, root_event_ids, record_time and canonical event IDs
            </CardDescription>
          </div>
          <Button 
            onClick={runValidation} 
            disabled={isValidating}
            variant={result ? "outline" : "default"}
          >
            {isValidating ? (
              <>
                <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                Validating...
              </>
            ) : (
              <>
                <ShieldCheck className="h-4 w-4 mr-2" />
                {result ? "Re-validate" : "Validate Data Integrity"}
              </>
            )}
          </Button>
        </div>
      </CardHeader>
      
      <CardContent>
        {!result && !isValidating && (
          <div className="text-center py-8 text-muted-foreground">
            <ShieldCheck className="h-12 w-12 mx-auto mb-3 opacity-30" />
            <p>Click the button above to validate data integrity</p>
            <p className="text-sm mt-1">This will sample 20 random files and check for required fields</p>
          </div>
        )}
        
        {isValidating && (
          <div className="text-center py-8">
            <Loader2 className="h-12 w-12 mx-auto mb-3 animate-spin text-primary" />
            <p className="text-muted-foreground">Sampling and validating files...</p>
          </div>
        )}
        
        {result && (
          <div className="space-y-6">
            {/* Score Overview */}
            <div className="flex items-center gap-6 p-4 rounded-lg bg-muted/50">
              {getScoreIcon(result.integrityScore)}
              <div className="flex-1">
                <div className="flex items-center gap-3 mb-2">
                  <span className={`text-3xl font-bold ${getScoreColor(result.integrityScore)}`}>
                    {result.integrityScore}%
                  </span>
                  <span className="text-muted-foreground">Combined Score</span>
                  {result.schemaComplianceScore !== undefined && (
                    <Badge variant="outline" className="ml-2">
                      Schema: {result.schemaComplianceScore}%
                    </Badge>
                  )}
                </div>
                <Progress 
                  value={result.integrityScore} 
                  className="h-2"
                />
              </div>
              <div className="text-right text-sm text-muted-foreground">
                <div>Sampled: {result.sampledFiles} files</div>
                <div>Total: {result.totalFiles.toLocaleString()} files</div>
              </div>
            </div>
            
            {/* Schema Compliance (NEW) */}
            {result.schemaCompliance && (
              <div className="p-4 rounded-lg border border-primary/20 bg-primary/5">
                <h4 className="font-semibold mb-3 flex items-center gap-2">
                  <ShieldCheck className="h-4 w-4" />
                  New Schema Compliance
                </h4>
                <div className="grid grid-cols-2 gap-4 text-sm">
                  <div className="space-y-2">
                    <div className="font-medium text-muted-foreground">Events</div>
                    <div className="flex justify-between">
                      <span>event_type_original:</span>
                      <span className={result.schemaCompliance.eventsWithTypeOriginal > 0 ? "text-green-500" : "text-yellow-500"}>
                        {result.schemaCompliance.eventsWithTypeOriginal}/{result.schemaCompliance.eventsWithTypeOriginal + result.schemaCompliance.eventsWithoutTypeOriginal}
                      </span>
                    </div>
                    <div className="flex justify-between">
                      <span>Canonical event_id:</span>
                      <span className={result.schemaCompliance.eventsWithCanonicalId > 0 ? "text-green-500" : "text-yellow-500"}>
                        {result.schemaCompliance.eventsWithCanonicalId}/{result.schemaCompliance.eventsWithCanonicalId + result.schemaCompliance.eventsWithSynthesizedId}
                      </span>
                    </div>
                    <div className="flex justify-between">
                      <span>child_event_ids:</span>
                      <span>{result.schemaCompliance.eventsWithChildEventIds}</span>
                    </div>
                    <div className="flex justify-between">
                      <span>contract_id:</span>
                      <span className={result.schemaCompliance.eventsWithContractId > 0 ? "text-green-500" : "text-yellow-500"}>
                        {result.schemaCompliance.eventsWithContractId}/{result.schemaCompliance.eventsWithContractId + result.schemaCompliance.eventsWithoutContractId}
                      </span>
                    </div>
                  </div>
                  <div className="space-y-2">
                    <div className="font-medium text-muted-foreground">Updates</div>
                    <div className="flex justify-between">
                      <span>record_time:</span>
                      <span className={result.schemaCompliance.updatesWithRecordTime > 0 ? "text-green-500" : "text-yellow-500"}>
                        {result.schemaCompliance.updatesWithRecordTime}/{result.schemaCompliance.updatesWithRecordTime + result.schemaCompliance.updatesWithoutRecordTime}
                      </span>
                    </div>
                    <div className="flex justify-between">
                      <span>root_event_ids:</span>
                      <span className={result.schemaCompliance.updatesWithRootEventIds > 0 ? "text-green-500" : "text-yellow-500"}>
                        {result.schemaCompliance.updatesWithRootEventIds}/{result.schemaCompliance.updatesWithRootEventIds + result.schemaCompliance.updatesWithoutRootEventIds}
                      </span>
                    </div>
                    <div className="flex justify-between">
                      <span>contract_id (in events):</span>
                      <span className={result.schemaCompliance.updatesWithContractId > 0 ? "text-green-500" : "text-yellow-500"}>
                        {result.schemaCompliance.updatesWithContractId}/{result.schemaCompliance.updatesWithContractId + result.schemaCompliance.updatesWithoutContractId}
                      </span>
                    </div>
                  </div>
                </div>
              </div>
            )}
            
            {/* Event Files */}
            <div className="grid grid-cols-2 gap-4">
              <div className="p-4 rounded-lg border">
                <h4 className="font-semibold mb-3 flex items-center gap-2">
                  Event Files
                  <Badge variant={result.eventFiles.valid === result.eventFiles.checked ? "default" : "destructive"}>
                    {result.eventFiles.valid}/{result.eventFiles.checked}
                  </Badge>
                </h4>
                <div className="space-y-2 text-sm">
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Checked:</span>
                    <span>{result.eventFiles.checked}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Valid (has raw_json):</span>
                    <span className="text-green-500">{result.eventFiles.valid}</span>
                  </div>
                  {result.eventFiles.missingRawJson > 0 && (
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Missing raw_json:</span>
                      <span className="text-red-500">{result.eventFiles.missingRawJson}</span>
                    </div>
                  )}
                  {result.eventFiles.emptyRecords > 0 && (
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Empty files:</span>
                      <span className="text-yellow-500">{result.eventFiles.emptyRecords}</span>
                    </div>
                  )}
                </div>
              </div>
              
              <div className="p-4 rounded-lg border">
                <h4 className="font-semibold mb-3 flex items-center gap-2">
                  Update Files
                  <Badge variant={result.updateFiles.valid === result.updateFiles.checked ? "default" : "destructive"}>
                    {result.updateFiles.valid}/{result.updateFiles.checked}
                  </Badge>
                </h4>
                <div className="space-y-2 text-sm">
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Checked:</span>
                    <span>{result.updateFiles.checked}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Valid (has update_data_json):</span>
                    <span className="text-green-500">{result.updateFiles.valid}</span>
                  </div>
                  {result.updateFiles.missingUpdateDataJson > 0 && (
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Missing update_data_json:</span>
                      <span className="text-red-500">{result.updateFiles.missingUpdateDataJson}</span>
                    </div>
                  )}
                  {result.updateFiles.emptyRecords > 0 && (
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Empty files:</span>
                      <span className="text-yellow-500">{result.updateFiles.emptyRecords}</span>
                    </div>
                  )}
                </div>
              </div>
            </div>
            
            {/* Sample Details */}
            {result.sampleDetails.length > 0 && (
              <div>
                <h4 className="font-semibold mb-3">Sample Details</h4>
                <div className="max-h-48 overflow-y-auto space-y-1">
                  {result.sampleDetails.map((detail, idx) => (
                    <div 
                      key={idx}
                      className="flex items-center gap-2 text-sm py-1 px-2 rounded hover:bg-muted/50"
                    >
                      {detail.hasRequiredFields ? (
                        <CheckCircle className="h-4 w-4 text-green-500 flex-shrink-0" />
                      ) : (
                        <XCircle className="h-4 w-4 text-red-500 flex-shrink-0" />
                      )}
                      <span className="font-mono text-xs truncate flex-1">{detail.file}</span>
                      <Badge variant="outline" className="text-xs">
                        {detail.recordCount} records
                      </Badge>
                      {detail.missingFields.length > 0 && (
                        <span className="text-xs text-red-500">
                          Missing: {detail.missingFields.join(', ')}
                        </span>
                      )}
                      {detail.schemaIssues && detail.schemaIssues.length > 0 && (
                        <span className="text-xs text-yellow-500">
                          Schema: {detail.schemaIssues.join(', ')}
                        </span>
                      )}
                    </div>
                  ))}
                </div>
              </div>
            )}
            
            {/* Errors */}
            {result.errors.length > 0 && (
              <div>
                <h4 className="font-semibold mb-3 text-red-500 flex items-center gap-2">
                  <AlertTriangle className="h-4 w-4" />
                  Errors ({result.errors.length})
                </h4>
                <div className="max-h-32 overflow-y-auto space-y-1 text-sm">
                  {result.errors.map((err, idx) => (
                    <div key={idx} className="text-red-400 text-xs">
                      {err.file}: {err.error}
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}
      </CardContent>
    </Card>
  );
}
