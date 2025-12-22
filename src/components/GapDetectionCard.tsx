import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { AlertTriangle, CheckCircle, RefreshCw, Clock } from "lucide-react";
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

interface GapDetectionCardProps {
  refreshInterval?: number;
}

export function GapDetectionCard({ refreshInterval = 30000 }: GapDetectionCardProps) {
  const [gapInfo, setGapInfo] = useState<GapInfo | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [isDetecting, setIsDetecting] = useState(false);

  const localApiUrl = import.meta.env.VITE_DUCKDB_API_URL || "http://localhost:3001";

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

  const triggerDetection = async (autoRecover = false) => {
    setIsDetecting(true);
    try {
      const response = await fetch(`${localApiUrl}/api/backfill/gaps/detect`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ autoRecover }),
      });
      if (response.ok) {
        await fetchGaps();
      }
    } catch (err) {
      console.error("Failed to trigger gap detection:", err);
    } finally {
      setIsDetecting(false);
    }
  };

  useEffect(() => {
    fetchGaps();
    const interval = setInterval(fetchGaps, refreshInterval);
    return () => clearInterval(interval);
  }, [refreshInterval]);

  const hasGaps = gapInfo && gapInfo.totalGaps > 0;

  return (
    <Card className={`bg-card/50 backdrop-blur ${hasGaps ? "border-warning/50" : "border-success/30"}`}>
      <CardHeader className="pb-2">
        <CardTitle className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            {hasGaps ? (
              <AlertTriangle className="w-5 h-5 text-warning" />
            ) : (
              <CheckCircle className="w-5 h-5 text-success" />
            )}
            <span>Time Gap Detection</span>
          </div>
          <div className="flex items-center gap-2">
            <Badge variant={gapInfo?.autoRecoverEnabled ? "default" : "secondary"} className="text-xs">
              Auto-recover: {gapInfo?.autoRecoverEnabled ? "ON" : "OFF"}
            </Badge>
            <Button
              size="sm"
              variant="outline"
              onClick={() => triggerDetection(false)}
              disabled={isDetecting}
            >
              <RefreshCw className={`w-4 h-4 mr-1 ${isDetecting ? "animate-spin" : ""}`} />
              Scan
            </Button>
          </div>
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        {/* Status Summary */}
        <div className="flex items-center justify-between text-sm">
          <div className="flex items-center gap-4">
            <span className={hasGaps ? "text-warning font-medium" : "text-success"}>
              {hasGaps 
                ? `⚠️ ${gapInfo.totalGaps} gap(s) detected` 
                : "✅ No gaps detected"}
            </span>
            {hasGaps && (
              <span className="text-muted-foreground">
                Total: {gapInfo.totalGapTime}
              </span>
            )}
          </div>
          {gapInfo?.detectedAt && (
            <span className="text-xs text-muted-foreground flex items-center gap-1">
              <Clock className="w-3 h-3" />
              {formatDistanceToNow(new Date(gapInfo.detectedAt), { addSuffix: true })}
            </span>
          )}
        </div>

        {/* Recovery Status */}
        {gapInfo?.recoveryAttempted && (
          <div className="text-sm text-muted-foreground bg-muted/50 p-2 rounded">
            Recovery attempted: {gapInfo.transactionsRecovered} transactions found
          </div>
        )}

        {/* Gap List */}
        {hasGaps && gapInfo.data.length > 0 && (
          <div className="space-y-2 max-h-48 overflow-y-auto">
            {gapInfo.data.slice(0, 5).map((gap, idx) => (
              <div
                key={idx}
                className="flex items-center justify-between text-xs bg-muted/30 p-2 rounded"
              >
                <div className="flex flex-col gap-0.5">
                  <span className="font-medium text-warning">{gap.gapDuration}</span>
                  <span className="text-muted-foreground truncate max-w-[200px]">
                    {gap.synchronizer.substring(0, 30)}...
                  </span>
                </div>
                <div className="text-right text-muted-foreground">
                  <div>{new Date(gap.gapStart).toLocaleDateString()}</div>
                  <div>→ {new Date(gap.gapEnd).toLocaleDateString()}</div>
                </div>
              </div>
            ))}
            {gapInfo.data.length > 5 && (
              <div className="text-xs text-center text-muted-foreground">
                ... and {gapInfo.data.length - 5} more gaps
              </div>
            )}
          </div>
        )}

        {/* Manual Recovery Button */}
        {hasGaps && (
          <Button
            size="sm"
            variant="outline"
            className="w-full mt-2"
            onClick={() => triggerDetection(true)}
            disabled={isDetecting}
          >
            <RefreshCw className={`w-4 h-4 mr-2 ${isDetecting ? "animate-spin" : ""}`} />
            Detect & Recover Gaps
          </Button>
        )}
      </CardContent>
    </Card>
  );
}
