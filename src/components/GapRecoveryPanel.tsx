import { useState, useEffect, useRef } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Progress } from "@/components/ui/progress";
import { ScrollArea } from "@/components/ui/scroll-area";
import { 
  AlertTriangle, 
  CheckCircle, 
  RefreshCw, 
  Clock, 
  Play,
  Square,
  AlertCircle,
  Database,
  FileWarning
} from "lucide-react";
import { formatDistanceToNow } from "date-fns";

interface Gap {
  synchronizer: string;
  migrationId: number;
  gapStart: string;
  gapEnd: string;
  gapMs: number;
  gapDuration: string;
}

interface GapInfo {
  data: Gap[];
  totalGaps: number;
  totalGapTime: string;
  detectedAt: string | null;
  autoRecoverEnabled: boolean;
  recoveryAttempted: boolean;
  transactionsRecovered: number;
}

interface RecoveryProgress {
  gapIndex: number;
  totalGaps: number;
  currentGap: Gap | null;
  status: 'idle' | 'detecting' | 'recovering' | 'complete' | 'error';
  message: string;
  updatesRecovered: number;
  eventsRecovered: number;
  logs: string[];
}

interface ReconciliationData {
  updates: {
    cursorTotal: number;
    fileTotal: number;
    difference: number;
    percentMissing: number;
  };
  events: {
    cursorTotal: number;
    fileTotal: number;
    difference: number;
    percentMissing: number;
  };
}

interface GapRecoveryPanelProps {
  refreshInterval?: number;
}

export function GapRecoveryPanel({ refreshInterval = 30000 }: GapRecoveryPanelProps) {
  const [gapInfo, setGapInfo] = useState<GapInfo | null>(null);
  const [reconciliation, setReconciliation] = useState<ReconciliationData | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [recovery, setRecovery] = useState<RecoveryProgress>({
    gapIndex: 0,
    totalGaps: 0,
    currentGap: null,
    status: 'idle',
    message: '',
    updatesRecovered: 0,
    eventsRecovered: 0,
    logs: [],
  });
  
  const logsEndRef = useRef<HTMLDivElement>(null);
  const abortControllerRef = useRef<AbortController | null>(null);

  const localApiUrl = import.meta.env.VITE_DUCKDB_API_URL || "http://localhost:3001";

  const addLog = (message: string) => {
    const timestamp = new Date().toLocaleTimeString();
    setRecovery(prev => ({
      ...prev,
      logs: [...prev.logs.slice(-100), `[${timestamp}] ${message}`],
    }));
  };

  const fetchGaps = async () => {
    try {
      const response = await fetch(`${localApiUrl}/api/backfill/gaps`);
      if (response.ok) {
        const data = await response.json();
        setGapInfo(data);
      }
    } catch (err) {
      console.warn("Failed to fetch gap info:", err);
    }
  };

  const fetchReconciliation = async () => {
    try {
      const response = await fetch(`${localApiUrl}/api/backfill/reconciliation`);
      if (response.ok) {
        const data = await response.json();
        setReconciliation(data);
      }
    } catch (err) {
      // Endpoint may not exist yet
    }
  };

  const triggerDetection = async () => {
    setRecovery(prev => ({ ...prev, status: 'detecting', message: 'Scanning for gaps...', logs: [] }));
    addLog('🔍 Starting gap detection scan...');
    
    try {
      const response = await fetch(`${localApiUrl}/api/backfill/gaps/detect`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ autoRecover: false }),
      });
      
      if (response.ok) {
        const result = await response.json();
        addLog(`✅ Scan complete: Found ${result.gaps || 0} gaps (${result.totalGapTime || '0ms'} total)`);
        await fetchGaps();
        setRecovery(prev => ({ ...prev, status: 'idle', message: '' }));
      } else {
        throw new Error('Detection failed');
      }
    } catch (err) {
      addLog(`❌ Detection failed: ${err}`);
      setRecovery(prev => ({ ...prev, status: 'error', message: 'Detection failed' }));
    }
  };

  const startRecovery = async () => {
    if (!gapInfo || gapInfo.data.length === 0) {
      addLog('⚠️ No gaps to recover');
      return;
    }

    abortControllerRef.current = new AbortController();
    
    setRecovery({
      gapIndex: 0,
      totalGaps: gapInfo.data.length,
      currentGap: gapInfo.data[0],
      status: 'recovering',
      message: 'Starting recovery...',
      updatesRecovered: 0,
      eventsRecovered: 0,
      logs: [],
    });

    addLog(`🚀 Starting recovery of ${gapInfo.data.length} gaps...`);

    try {
      const response = await fetch(`${localApiUrl}/api/backfill/gaps/recover`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ 
          maxGaps: gapInfo.data.length,
          stream: true,
        }),
        signal: abortControllerRef.current.signal,
      });

      if (!response.ok) {
        throw new Error(`Recovery failed: ${response.status}`);
      }

      // Handle streaming response
      const reader = response.body?.getReader();
      const decoder = new TextDecoder();

      if (reader) {
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;

          const text = decoder.decode(value, { stream: true });
          const lines = text.split('\n').filter(Boolean);

          for (const line of lines) {
            try {
              if (line.startsWith('data: ')) {
                const data = JSON.parse(line.slice(6));
                handleProgressUpdate(data);
              }
            } catch {
              // Not JSON, treat as log message
              addLog(line);
            }
          }
        }
      }

      addLog('✅ Recovery process complete');
      setRecovery(prev => ({ ...prev, status: 'complete', message: 'Recovery complete' }));
      await fetchGaps();
      await fetchReconciliation();

    } catch (err: any) {
      if (err.name === 'AbortError') {
        addLog('⚠️ Recovery cancelled by user');
        setRecovery(prev => ({ ...prev, status: 'idle', message: 'Cancelled' }));
      } else {
        addLog(`❌ Recovery error: ${err.message}`);
        setRecovery(prev => ({ ...prev, status: 'error', message: err.message }));
      }
    }
  };

  const handleProgressUpdate = (data: any) => {
    if (data.type === 'progress') {
      setRecovery(prev => ({
        ...prev,
        gapIndex: data.gapIndex || prev.gapIndex,
        updatesRecovered: data.updatesRecovered || prev.updatesRecovered,
        eventsRecovered: data.eventsRecovered || prev.eventsRecovered,
        message: data.message || prev.message,
      }));
      if (data.message) {
        addLog(data.message);
      }
    } else if (data.type === 'gap_start') {
      addLog(`📍 Starting gap ${data.gapIndex + 1}/${data.totalGaps}: ${data.gapDuration}`);
      setRecovery(prev => ({
        ...prev,
        gapIndex: data.gapIndex,
        currentGap: data.gap,
      }));
    } else if (data.type === 'gap_complete') {
      addLog(`✅ Gap ${data.gapIndex + 1} complete: ${data.updates} updates, ${data.events} events`);
    } else if (data.type === 'error') {
      addLog(`❌ Error: ${data.message}`);
    }
  };

  const stopRecovery = () => {
    abortControllerRef.current?.abort();
    addLog('⏹️ Stopping recovery...');
  };

  useEffect(() => {
    fetchGaps();
    fetchReconciliation();
    const interval = setInterval(() => {
      fetchGaps();
      fetchReconciliation();
    }, refreshInterval);
    return () => clearInterval(interval);
  }, [refreshInterval]);

  useEffect(() => {
    logsEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [recovery.logs]);

  const hasGaps = gapInfo && gapInfo.totalGaps > 0;
  const hasMissingData = reconciliation && 
    (reconciliation.updates.difference > 0 || reconciliation.events.difference > 0);
  const isRecovering = recovery.status === 'recovering';
  const overallProgress = recovery.totalGaps > 0 
    ? ((recovery.gapIndex + 1) / recovery.totalGaps) * 100 
    : 0;

  return (
    <Card className="bg-card/50 backdrop-blur border-border">
      <CardHeader className="pb-3">
        <CardTitle className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            {hasMissingData || hasGaps ? (
              <AlertTriangle className="w-5 h-5 text-warning" />
            ) : (
              <CheckCircle className="w-5 h-5 text-success" />
            )}
            <span>Data Integrity & Gap Recovery</span>
          </div>
          <div className="flex items-center gap-2">
            {gapInfo?.detectedAt && (
              <Badge variant="outline" className="text-xs">
                <Clock className="w-3 h-3 mr-1" />
                {formatDistanceToNow(new Date(gapInfo.detectedAt), { addSuffix: true })}
              </Badge>
            )}
          </div>
        </CardTitle>
      </CardHeader>
      
      <CardContent className="space-y-4">
        {/* Reconciliation Summary - Cursor vs Files */}
        {reconciliation && (
          <>
            <div className="text-xs text-muted-foreground mb-2 px-1">
              <strong>Cursor vs File Reconciliation:</strong> Cursors track total records fetched from API. Files are what's actually stored.
              Large differences indicate data loss during write (fixed in bulletproof-backfill). Re-run backfill for affected time ranges.
            </div>
            <div className="grid grid-cols-2 gap-3">
              <div className={`p-3 rounded-lg ${reconciliation.updates.difference > 0 ? 'bg-warning/10 border border-warning/30' : 'bg-success/10 border border-success/30'}`}>
                <div className="flex items-center gap-2 text-sm font-medium mb-1">
                  <Database className="w-4 h-4" />
                  Updates
                </div>
                <div className="text-2xl font-bold">
                  {reconciliation.updates.fileTotal.toLocaleString()}
                </div>
                {reconciliation.updates.difference > 0 ? (
                  <div className="text-xs text-warning mt-1">
                    ⚠️ {reconciliation.updates.difference.toLocaleString()} missing ({reconciliation.updates.percentMissing.toFixed(1)}%)
                  </div>
                ) : (
                  <div className="text-xs text-success mt-1">✓ Matches cursor</div>
                )}
              </div>
              
              <div className={`p-3 rounded-lg ${reconciliation.events.difference > 0 ? 'bg-warning/10 border border-warning/30' : 'bg-success/10 border border-success/30'}`}>
                <div className="flex items-center gap-2 text-sm font-medium mb-1">
                  <FileWarning className="w-4 h-4" />
                  Events
                </div>
                <div className="text-2xl font-bold">
                  {reconciliation.events.fileTotal.toLocaleString()}
                </div>
                {reconciliation.events.difference > 0 ? (
                  <div className="text-xs text-warning mt-1">
                    ⚠️ {reconciliation.events.difference.toLocaleString()} missing ({reconciliation.events.percentMissing.toFixed(1)}%)
                  </div>
                ) : (
                  <div className="text-xs text-success mt-1">✓ Matches cursor</div>
                )}
              </div>
            </div>
          </>
        )}

        {/* Time Gap Summary */}
        <div className="flex items-center justify-between p-3 bg-muted/30 rounded-lg">
          <div className="flex flex-col gap-1">
            <span className={hasGaps ? "text-warning font-medium" : "text-success"}>
              {hasGaps 
                ? `⚠️ ${gapInfo!.totalGaps} time gap(s) detected` 
                : "✅ No time gaps detected"}
            </span>
            <span className="text-xs text-muted-foreground">
              Time gaps = periods with no data files. Different from missing records above.
            </span>
            {hasGaps && (
              <span className="text-sm text-muted-foreground">
                Total gap duration: {gapInfo!.totalGapTime}
              </span>
            )}
          </div>
        </div>

        {/* Recovery Progress */}
        {isRecovering && (
          <div className="space-y-2 p-3 bg-primary/5 rounded-lg border border-primary/20">
            <div className="flex items-center justify-between text-sm">
              <span className="font-medium">
                Recovering gap {recovery.gapIndex + 1} of {recovery.totalGaps}
              </span>
              <span className="text-muted-foreground">
                {recovery.updatesRecovered.toLocaleString()} updates, {recovery.eventsRecovered.toLocaleString()} events
              </span>
            </div>
            <Progress value={overallProgress} className="h-2" />
            <div className="text-xs text-muted-foreground">{recovery.message}</div>
          </div>
        )}

        {/* Gap List */}
        {hasGaps && gapInfo!.data.length > 0 && !isRecovering && (
          <div className="space-y-2">
            <div className="text-sm font-medium">Detected Gaps:</div>
            <div className="space-y-1 max-h-32 overflow-y-auto">
              {gapInfo!.data.slice(0, 10).map((gap, idx) => (
                <div
                  key={idx}
                  className="flex items-center justify-between text-xs bg-muted/30 p-2 rounded"
                >
                  <div className="flex items-center gap-2">
                    <Badge variant="outline" className="text-warning">{gap.gapDuration}</Badge>
                    <span className="text-muted-foreground truncate max-w-[150px]">
                      {gap.synchronizer.substring(0, 25)}...
                    </span>
                  </div>
                  <div className="text-muted-foreground">
                    {new Date(gap.gapStart).toLocaleDateString()} → {new Date(gap.gapEnd).toLocaleDateString()}
                  </div>
                </div>
              ))}
              {gapInfo!.data.length > 10 && (
                <div className="text-xs text-center text-muted-foreground py-1">
                  ... and {gapInfo!.data.length - 10} more gaps
                </div>
              )}
            </div>
          </div>
        )}

        {/* Recovery Log */}
        {recovery.logs.length > 0 && (
          <div className="space-y-2">
            <div className="text-sm font-medium">Recovery Log:</div>
            <ScrollArea className="h-32 w-full rounded border bg-background/50 p-2">
              <div className="space-y-1 font-mono text-xs">
                {recovery.logs.map((log, idx) => (
                  <div key={idx} className="text-muted-foreground">{log}</div>
                ))}
                <div ref={logsEndRef} />
              </div>
            </ScrollArea>
          </div>
        )}

        {/* Action Buttons */}
        <div className="flex gap-2">
          <Button
            size="sm"
            variant="outline"
            onClick={triggerDetection}
            disabled={isRecovering || recovery.status === 'detecting'}
            className="flex-1"
          >
            <RefreshCw className={`w-4 h-4 mr-2 ${recovery.status === 'detecting' ? 'animate-spin' : ''}`} />
            Scan for Gaps
          </Button>
          
          {hasGaps && !isRecovering && (
            <Button
              size="sm"
              variant="default"
              onClick={startRecovery}
              className="flex-1"
            >
              <Play className="w-4 h-4 mr-2" />
              Recover {gapInfo!.totalGaps} Gap{gapInfo!.totalGaps > 1 ? 's' : ''}
            </Button>
          )}
          
          {isRecovering && (
            <Button
              size="sm"
              variant="destructive"
              onClick={stopRecovery}
              className="flex-1"
            >
              <Square className="w-4 h-4 mr-2" />
              Stop Recovery
            </Button>
          )}
        </div>

        {/* Status Messages */}
        {recovery.status === 'complete' && (
          <div className="flex items-center gap-2 text-sm text-success p-2 bg-success/10 rounded">
            <CheckCircle className="w-4 h-4" />
            Recovery complete: {recovery.updatesRecovered.toLocaleString()} updates, {recovery.eventsRecovered.toLocaleString()} events recovered
          </div>
        )}
        
        {recovery.status === 'error' && (
          <div className="flex items-center gap-2 text-sm text-destructive p-2 bg-destructive/10 rounded">
            <AlertCircle className="w-4 h-4" />
            {recovery.message}
          </div>
        )}
      </CardContent>
    </Card>
  );
}