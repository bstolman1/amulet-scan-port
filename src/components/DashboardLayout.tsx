import { ReactNode, useMemo, useState } from "react";
import { ErrorBoundary } from "./ErrorBoundary";
import cantonLogo from "@/assets/logo.svg";
import { Link, useLocation } from "react-router-dom";
import { ConnectionStatusIndicator } from "./ConnectionStatusIndicator";
import {
  BarChart3,
  Coins,
  Database,
  Zap,
  Vote,
  Award,
  Ticket,
  GitBranch,
  CandlestickChart,
  Network,
  TrendingUp,
  DollarSign,
  ChevronDown,
  Radio,
  Layers,
  Lock,
  type LucideIcon,
} from "lucide-react";
import { SyncInsightsIcon } from "./icons/SyncInsightsIcon";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import { useCDNDashboards } from "@/hooks/use-cdn-dashboards";

interface DashboardLayoutProps {
  children: ReactNode;
}

interface NavItem {
  name: string;
  href: string;
  icon: LucideIcon;
}

interface NavGroup {
  label: string;
  items: NavItem[];
}

const baseNavigationGroups: NavGroup[] = [
  {
    label: "Overview",
    items: [
      { name: "Dashboard", href: "/", icon: BarChart3 },
      { name: "Issuance Curve", href: "/issuance-curve", icon: TrendingUp },
      { name: "Protocol Fees", href: "/protocol-fees", icon: DollarSign },
      { name: "Price Votes", href: "/price-votes", icon: DollarSign },
    ],
  },
  {
    label: "Governance",
    items: [
      { name: "Governance", href: "/governance", icon: Vote },
      { name: "Governance Flow", href: "/governance-flow", icon: GitBranch },
      { name: "Dev Fund", href: "/dev-fund", icon: Coins },
      // { name: "SV Locking", href: "/sv-locking", icon: Lock },
    ],
  },
  // {
  //   label: "Burn/Mint",
  //   items: [
  //     { name: "Mint", href: "/supply", icon: Coins },
  //     { name: "Transactions", href: "/transactions", icon: Activity },
  //     { name: "Transfers", href: "/transfers", icon: ArrowRightLeft },
  //     { name: "Rich List", href: "/rich-list", icon: Wallet },
  //   ],
  // },
  {
    label: "Network",
    items: [
      { name: "Super Validators", href: "/validators", icon: Zap },
      { name: "Validators", href: "/validator-licenses", icon: Ticket },
      { name: "Sequencers", href: "/sequencers", icon: Network },
      { name: "SV Status", href: "/sv-status", icon: Radio },
    ],
  },
  {
    label: "Exchange Data",
    items: [
      { name: "Kaiko Feed", href: "/kaiko-feed", icon: CandlestickChart },
    ],
  },
  // {
  //   label: "Services",
  //   items: [
  //     { name: "ANS", href: "/ans", icon: Globe },
  //     { name: "Featured Apps", href: "/apps", icon: Package },
  //     { name: "Subscriptions", href: "/subscriptions", icon: Package },
  //   ],
  // },
  {
    label: "Statistics",
    items: [
      { name: "Statistics", href: "/stats", icon: Database },
    ],
  },
];

const NavDropdown = ({ group }: { group: NavGroup }) => {
  const location = useLocation();
  const isGroupActive = group.items.some(item => location.pathname === item.href);
  const [open, setOpen] = useState(false);
  
  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <button
          className={`flex items-center gap-1.5 px-3 py-2 rounded-lg text-sm font-medium transition-smooth ${
            isGroupActive
              ? "bg-primary/10 text-primary"
              : "text-muted-foreground hover:text-foreground hover:bg-muted/50"
          }`}
        >
          {group.label}
          <ChevronDown className="h-3.5 w-3.5" />
        </button>
      </PopoverTrigger>
      <PopoverContent 
        className="w-48 p-1 bg-popover border border-border shadow-lg" 
        align="start"
        sideOffset={8}
      >
        <div className="flex flex-col">
          {group.items.map((item) => {
            const isActive = location.pathname === item.href;
            const Icon = item.icon;
            return (
              <Link
                key={item.name}
                to={item.href}
                onClick={() => setOpen(false)}
                className={`flex items-center gap-2 px-3 py-2 rounded-md text-sm transition-smooth ${
                  isActive
                    ? "bg-primary/10 text-primary"
                    : "text-muted-foreground hover:text-foreground hover:bg-muted/50"
                }`}
              >
                <Icon className="h-4 w-4 flex-shrink-0" />
                <span>{item.name}</span>
              </Link>
            );
          })}
        </div>
      </PopoverContent>
    </Popover>
  );
};

export const DashboardLayout = ({ children }: DashboardLayoutProps) => {
  const { data: cdnDashboards = [], error: cdnError } = useCDNDashboards();
  
  if (cdnError) {
    console.warn("⚠️ Error loading CDN dashboards:", cdnError);
  }

  // Build navigation groups with dashboards appended to Overview
  const navigationGroups = useMemo(() => {
    // Create deep copies of navigation groups to avoid mutating the base array
    const groups = baseNavigationGroups.map(group => ({
      ...group,
      items: [...group.items],
    }));
    
    // Find Overview group and append dashboards
    const overviewGroup = groups.find(g => g.label === "Overview");
    if (overviewGroup && cdnDashboards.length > 0) {
      // Create dashboard items and deduplicate by name
      const seenNames = new Set<string>();
      const dashboardItems: NavItem[] = cdnDashboards
        .map((dashboard) => {
          const name = dashboard.title || dashboard.name.replace(/\.aqldash$/, "").replace(/_/g, " ");
          return {
            name,
            href: `/dashboard/${encodeURIComponent(dashboard.name)}`,
            icon: SyncInsightsIcon as LucideIcon,
          };
        })
        .filter((item) => {
          if (seenNames.has(item.name)) {
            return false;
          }
          seenNames.add(item.name);
          return true;
        });
      
      overviewGroup.items = [...overviewGroup.items, ...dashboardItems];
    }
    
    return groups;
  }, [cdnDashboards]);

  return (
    <div className="min-h-screen">
      {/* Header */}
      <header className="glass-card border-b border-border/50 sticky top-0 z-50">
        <div className="container mx-auto px-6 py-4">
          <div className="flex items-center justify-between">
            {/* Logo */}
            <Link to="/" className="flex items-center space-x-3 group">
              <img src={cantonLogo} alt="Canton Network" className="h-10" />
            </Link>

            {/* Navigation */}
            <nav className="flex items-center gap-1">
              {navigationGroups.map((group) => (
                <NavDropdown key={group.label} group={group} />
              ))}
            </nav>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="container mx-auto px-6 py-8">
        <ErrorBoundary title="Dashboard failed to render">
          {children}
        </ErrorBoundary>
      </main>
      
      {/* Connection Status Indicator */}
      <ConnectionStatusIndicator />
    </div>
  );
};
