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
  ChevronDown,
  type LucideIcon,
} from "lucide-react";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";

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
    label: "Validators",
    items: [
      { name: "Validators", href: "/validators", icon: Zap },
      { name: "Licenses", href: "/validator-licenses", icon: Ticket },
      { name: "Round Stats", href: "/round-stats", icon: Layers },
      { name: "Traffic Status", href: "/traffic-status", icon: Radio },
    ],
  },
  {
    label: "Rewards",
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
    label: "Rules",
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

const NavDropdown = ({ group }: { group: NavGroup }) => {
  const location = useLocation();
  const isGroupActive = group.items.some(item => location.pathname === item.href);
  
  return (
    <Popover>
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
                className={`flex items-center gap-2 px-3 py-2 rounded-md text-sm transition-smooth ${
                  isActive
                    ? "bg-primary/10 text-primary"
                    : "text-muted-foreground hover:text-foreground hover:bg-muted/50"
                }`}
              >
                <Icon className="h-4 w-4" />
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
  return (
    <div className="min-h-screen">
      {/* Header */}
      <header className="glass-card border-b border-border/50 sticky top-0 z-50">
        <div className="container mx-auto px-6 py-4">
          <div className="flex items-center justify-between">
            {/* Logo */}
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
      <main className="container mx-auto px-6 py-8">{children}</main>
      
      {/* Connection Status Indicator */}
      <ConnectionStatusIndicator />
    </div>
  );
};