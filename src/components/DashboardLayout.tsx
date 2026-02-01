import { ReactNode } from "react";
import { Link, useLocation } from "react-router-dom";
import { ConnectionStatusIndicator } from "./ConnectionStatusIndicator";
import {
  Activity,
  BarChart3,
  Coins,
  Database,
  Layers,
  Zap,
  Globe,
  Package,
  Vote,
  Award,
  Shield,
  ArrowRightLeft,
  Wallet,
  Radio,
  Users,
  Ticket,
  UserPlus,
  Hash,
  GitBranch,
  CandlestickChart,
  Network,
  TrendingUp,
  DollarSign,
  type LucideIcon,
} from "lucide-react";

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

// Consolidated navigation organized by domain
const navigationGroups: NavGroup[] = [
  {
    label: "Overview",
    items: [
      { name: "Dashboard", href: "/", icon: BarChart3 },
      { name: "Supply", href: "/supply", icon: Coins },
      { name: "Network Info", href: "/network-info", icon: Network },
    ],
  },
  {
    label: "Transactions",
    items: [
      { name: "Transactions", href: "/transactions", icon: Activity },
      { name: "Transfers", href: "/transfers", icon: ArrowRightLeft },
      { name: "Rich List", href: "/rich-list", icon: Wallet },
    ],
  },
  {
    label: "Validators & Rounds",
    items: [
      { name: "Validators", href: "/validators", icon: Zap },
      { name: "Licenses", href: "/validator-licenses", icon: Ticket },
      { name: "Round Stats", href: "/round-stats", icon: Layers },
      { name: "Traffic Status", href: "/traffic-status", icon: Radio },
    ],
  },
  {
    label: "Rewards & Pricing",
    items: [
      { name: "Rewards", href: "/rewards", icon: Award },
      { name: "Price Votes", href: "/price-votes", icon: DollarSign },
      { name: "Kaiko Feed", href: "/kaiko-feed", icon: CandlestickChart },
    ],
  },
  {
    label: "Governance",
    items: [
      { name: "Governance", href: "/governance", icon: Vote },
      { name: "Governance Flow", href: "/governance-flow", icon: GitBranch },
      { name: "Elections", href: "/elections", icon: Vote },
      { name: "DSO State", href: "/dso-state", icon: Users },
    ],
  },
  {
    label: "Services",
    items: [
      { name: "ANS", href: "/ans", icon: Globe },
      { name: "Featured Apps", href: "/apps", icon: Package },
      { name: "Subscriptions", href: "/subscriptions", icon: Package },
    ],
  },
  {
    label: "Rules & Config",
    items: [
      { name: "Amulet Rules", href: "/amulet-rules", icon: Shield },
      { name: "External Party", href: "/external-party-setup", icon: UserPlus },
      { name: "External Rules", href: "/external-party-rules", icon: Shield },
      { name: "Transfer Counters", href: "/transfer-counters", icon: Hash },
    ],
  },
  {
    label: "Statistics",
    items: [
      { name: "Statistics", href: "/stats", icon: Database },
      { name: "Member Traffic", href: "/member-traffic", icon: TrendingUp },
    ],
  },
];

// Admin pages - commented out from main nav but routes still work
// {
//   label: "Admin",
//   items: [
//     { name: "Admin", href: "/admin", icon: Shield },
//     { name: "Templates", href: "/templates", icon: FileText },
//     { name: "ACS Snapshot", href: "/snapshot-progress", icon: Upload },
//     { name: "Ingestion", href: "/ingestion", icon: Database },
//     { name: "Backfill Progress", href: "/backfill-progress", icon: Clock },
//     { name: "Live Updates", href: "/live-updates", icon: TrendingUp },
//   ],
// },

export const DashboardLayout = ({ children }: DashboardLayoutProps) => {
  const location = useLocation();

  return (
    <div className="min-h-screen">
      {/* Header */}
      <header className="glass-card border-b border-border/50 sticky top-0 z-50">
        <div className="container mx-auto px-6 py-4">
          {/* Top row: Logo */}
          <div className="flex items-center justify-between mb-4">
            <Link to="/" className="flex items-center space-x-3 group">
              <div className="relative">
                <div className="absolute inset-0 gradient-primary rounded-lg blur-xl opacity-50 group-hover:opacity-100 transition-smooth" />
                <div className="relative gradient-primary p-2 rounded-lg">
                  <Database className="h-6 w-6 text-primary-foreground" />
                </div>
              </div>
              <div>
                <h1 className="text-2xl font-bold bg-gradient-to-r from-primary to-accent bg-clip-text text-transparent">
                  SCANTON
                </h1>
                <p className="text-xs text-muted-foreground">Canton Network Analytics</p>
              </div>
            </Link>
          </div>

          {/* Navigation organized by groups */}
          <nav className="space-y-2">
            {navigationGroups.map((group) => (
              <div key={group.label} className="flex flex-wrap items-center gap-1">
                <span className="text-xs font-semibold text-muted-foreground uppercase tracking-wider w-24 shrink-0">
                  {group.label}
                </span>
                <div className="flex flex-wrap gap-1">
                  {group.items.map((item) => {
                    const isActive = location.pathname === item.href;
                    const Icon = item.icon;
                    return (
                      <Link
                        key={item.name}
                        to={item.href}
                        className={`flex items-center space-x-1.5 px-3 py-1.5 rounded-lg transition-smooth text-sm ${
                          isActive
                            ? "bg-primary/10 text-primary"
                            : "text-muted-foreground hover:text-foreground hover:bg-muted/50"
                        }`}
                      >
                        <Icon className="h-3.5 w-3.5" />
                        <span className="font-medium">{item.name}</span>
                      </Link>
                    );
                  })}
                </div>
              </div>
            ))}
          </nav>
        </div>
      </header>

      {/* Main Content */}
      <main className="container mx-auto px-6 py-8">{children}</main>
      
      {/* Connection Status Indicator */}
      <ConnectionStatusIndicator />
    </div>
  );
};
