import { useEffect, useState } from "react";
import { Wifi, WifiOff } from "lucide-react";
import { checkDuckDBConnection } from "@/lib/backend-config";
import { cn } from "@/lib/utils";

export function BackendStatusIndicator() {
  const [isConnected, setIsConnected] = useState<boolean | null>(null);
  const [isChecking, setIsChecking] = useState(false);

  const checkConnection = async () => {
    setIsChecking(true);
    const connected = await checkDuckDBConnection();
    setIsConnected(connected);
    setIsChecking(false);
  };

  useEffect(() => {
    checkConnection();
    // Re-check every 30 seconds
    const interval = setInterval(checkConnection, 30000);
    return () => clearInterval(interval);
  }, []);

  if (isConnected === null) {
    return (
      <div className="flex items-center gap-2 px-3 py-1.5 rounded-full bg-muted/50 text-muted-foreground text-xs">
        <div className="h-2 w-2 rounded-full bg-muted-foreground animate-pulse" />
        <span>Checking...</span>
      </div>
    );
  }

  return (
    <button
      onClick={checkConnection}
      disabled={isChecking}
      className={cn(
        "flex items-center gap-2 px-3 py-1.5 rounded-full text-xs font-medium transition-all",
        "hover:opacity-80 disabled:opacity-50",
        isConnected
          ? "bg-emerald-500/10 text-emerald-600 dark:text-emerald-400"
          : "bg-destructive/10 text-destructive"
      )}
      title={isConnected ? "Backend connected - Click to refresh" : "Backend disconnected - Click to retry"}
    >
      {isConnected ? (
        <>
          <Wifi className={cn("h-3.5 w-3.5", isChecking && "animate-pulse")} />
          <span>API Connected</span>
        </>
      ) : (
        <>
          <WifiOff className={cn("h-3.5 w-3.5", isChecking && "animate-pulse")} />
          <span>API Offline</span>
        </>
      )}
    </button>
  );
}
