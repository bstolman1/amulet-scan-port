import { useEffect, useState, useRef } from "react";
import { useParams } from "react-router-dom";
import { DashboardLayout } from "@/components/DashboardLayout";
import { useCDNDashboards } from "@/hooks/use-cdn-dashboards";

// Don't import react-autoql at module level - it's causing bundling issues
// We'll load it dynamically when needed
let Dashboard: any = null;
let configureTheme: any = null;
let isAutoQLLoaded = false;
import { processDashboardFileFromUrl } from "@/utils/dashboardFileUtils";
import type { DashboardData } from "@/utils/dashboardFileUtils";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Loader2 } from "lucide-react";
import { ErrorBoundary } from "@/components/ErrorBoundary";
function DashboardWrapper({ tiles }: { tiles: any[] }) {
  const [dashboardLoaded, setDashboardLoaded] = useState(false);
  const [loadError, setLoadError] = useState<string | null>(null);
  const mountedRef = useRef(true);
  const dashboardRef = useRef<any>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  
  // Load react-autoql dynamically
  useEffect(() => {
    mountedRef.current = true;
    
    if (isAutoQLLoaded) {
      setDashboardLoaded(true);
      return;
    }
    
    let cancelled = false;
    
    import("react-autoql")
      .then((module) => {
        if (cancelled || !mountedRef.current) return;
        
        Dashboard = module.Dashboard || module.default?.Dashboard || module.default;
        configureTheme = module.configureTheme || (module as any).configureTheme;
        
        if (configureTheme) {
          configureTheme({
            theme: "dark",
            fontFamily: "IBM Plex Sans, -apple-system, BlinkMacSystemFont, Segoe UI, Helvetica Neue, Arial, sans-serif",
            textColor: "hsl(0, 0.00%, 100.00%)",
            backgroundColorPrimary: "hsl(240, 95%, 15%)",
            backgroundColorSecondary: "hsl(225, 25%, 12%)",
            chartColors: [
              "#F3FF97",  // Primary yellow - most important
              "#D5A5E3",  // Accent lilac
              "#5BA3E8",  // Vibrant blue
              "#7BC8C8",  // Bright teal
              "#8FA8B8",  // Sophisticated slate-blue
            ],
          });
        }
        
        isAutoQLLoaded = true;
        if (mountedRef.current) {
          setDashboardLoaded(true);
        }
      })
      .catch((error) => {
        if (!cancelled && mountedRef.current) {
          console.error("Failed to load react-autoql:", error);
          setLoadError(String(error));
        }
      });
    
    return () => {
      cancelled = true;
      mountedRef.current = false;
    };
  }, []);
  
  // Note: react-autoql Dashboard component handles resize events internally,
  // so we don't need to add our own resize handler
  
  // Note: react-autoql Dashboard component handles its own layout refresh on resize
  // No need for us to call refreshTileLayouts manually
  
  if (loadError) {
    return (
      <div className="p-4">
        <Alert variant="destructive">
          <AlertDescription>
            Failed to load Dashboard component: {loadError}
          </AlertDescription>
        </Alert>
      </div>
    );
  }
  
  if (!dashboardLoaded || !Dashboard) {
    return (
      <div className="flex items-center justify-center min-h-[400px]">
        <Loader2 className="h-8 w-8 animate-spin text-primary" />
        <p className="ml-2 text-sm text-muted-foreground">Loading Dashboard component...</p>
      </div>
    );
  }
  
  return (
    <div 
      ref={containerRef}
      id="dashboard-mount-point"
      className="dashboard-container w-full" 
      style={{ 
        width: '100%',
        minWidth: 0,
        position: 'relative',
        overflow: 'visible',
        isolation: 'isolate',
        backgroundColor: 'transparent',
        boxSizing: 'border-box',
        // Use CSS containment to isolate layout calculations and prevent layout thrashing
        contain: 'layout style',
        // Force GPU acceleration to reduce main thread work
        transform: 'translateZ(0)',
        // Prevent layout shifts during resize
        willChange: 'contents'
      }}
    >
      <Dashboard
        ref={(ref) => {
          dashboardRef.current = ref;
        }}
        tiles={tiles}
        notExecutedText="Queries will not execute in view-only mode"
        offline
        isEditable={false}
      />
    </div>
  );
}

export default function DashboardViewer() {
  const { name } = useParams<{ name: string }>();
  
  const { data: cdnDashboards = [], isLoading: isLoadingDashboards } = useCDNDashboards();
  
  const [dashboardData, setDashboardData] = useState<DashboardData | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [shouldRenderDashboard, setShouldRenderDashboard] = useState(false);

  // Delay rendering Dashboard component to ensure everything else is mounted first
  useEffect(() => {
    if (dashboardData && dashboardData.dashboard.tiles?.length > 0) {
      const timer = setTimeout(() => {
        setShouldRenderDashboard(true);
      }, 100);
      return () => {
        clearTimeout(timer);
      };
    } else {
      setShouldRenderDashboard(false);
    }
  }, [dashboardData]);

  useEffect(() => {
    let cancelled = false;
    
    const loadDashboard = async () => {
      if (!name) {
        if (!cancelled) {
          setError("Dashboard name is required");
          setIsLoading(false);
        }
        return;
      }

      if (isLoadingDashboards) {
        return;
      }

      const decodedName = decodeURIComponent(name);
      
      const dashboard = cdnDashboards.find((d) => d.name === decodedName);
      
      if (!dashboard) {
        if (!cancelled) {
          setError(`Dashboard "${decodedName}" not found`);
          setIsLoading(false);
        }
        return;
      }

      try {
        if (!cancelled) {
          setIsLoading(true);
          setError(null);
        }
        const data = await processDashboardFileFromUrl(dashboard.url);
        if (!cancelled) {
          setDashboardData(data);
        }
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "Failed to load dashboard");
        }
      } finally {
        if (!cancelled) {
          setIsLoading(false);
        }
      }
    };

    loadDashboard();
    
    return () => {
      cancelled = true;
    };
  }, [name, cdnDashboards, isLoadingDashboards]);

  if (isLoadingDashboards || isLoading) {
    return (
      <DashboardLayout>
        <div className="flex items-center justify-center min-h-[400px]">
          <div className="flex flex-col items-center gap-4">
            <Loader2 className="h-8 w-8 animate-spin text-primary" />
            <p className="text-muted-foreground">
              {isLoadingDashboards ? "Loading dashboard list..." : "Loading dashboard..."}
            </p>
          </div>
        </div>
      </DashboardLayout>
    );
  }

  if (error) {
    return (
      <DashboardLayout>
        <Alert variant="destructive">
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      </DashboardLayout>
    );
  }

  if (!dashboardData) {
    return (
      <DashboardLayout>
        <Alert>
          <AlertDescription>No dashboard data available</AlertDescription>
        </Alert>
      </DashboardLayout>
    );
  }

  return (
    <DashboardLayout>
      <div className="space-y-4">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold">{dashboardData.dashboard.title}</h1>
            {dashboardData.exportDate && (
              <p className="text-sm text-muted-foreground mt-1">
                Exported: {(() => {
                  try {
                    const date = new Date(dashboardData.exportDate);
                    const options: Intl.DateTimeFormatOptions = {
                      year: "numeric",
                      month: "long",
                      day: "numeric",
                      hour: "numeric",
                      minute: "2-digit",
                      timeZoneName: "short",
                    };
                    return date.toLocaleDateString("en-US", options);
                  } catch (error) {
                    return new Date(dashboardData.exportDate).toLocaleString();
                  }
                })()}
              </p>
            )}
          </div>
          <a
            href="https://syncinsights.io/"
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center gap-2 text-sm text-muted-foreground hover:text-foreground transition-colors"
          >
            <span>Powered by</span>
            <img
              src="/sync-insights-logo-01.png"
              alt="Sync Insights"
              className="h-6 w-auto"
            />
          </a>
        </div>

        {dashboardData.dashboard.tiles && dashboardData.dashboard.tiles.length > 0 ? (
          shouldRenderDashboard ? (
            <div className="-mx-6">
              <ErrorBoundary title="Dashboard rendering error">
                <DashboardWrapper tiles={dashboardData.dashboard.tiles} />
              </ErrorBoundary>
            </div>
          ) : (
            <div className="flex items-center justify-center min-h-[400px]">
              <Loader2 className="h-8 w-8 animate-spin text-primary" />
              <p className="ml-2 text-sm text-muted-foreground">Waiting to render dashboard...</p>
            </div>
          )
        ) : (
          <Alert>
            <AlertDescription>This dashboard has no tiles to display.</AlertDescription>
          </Alert>
        )}
      </div>
    </DashboardLayout>
  );
}
