import { ReactNode } from "react";
import { Link, useLocation } from "react-router-dom";
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
  Upload,
  ArrowRightLeft,
  Wallet,
  FileText,
  Radio,
  Users,
  Ticket,
  UserPlus,
  Hash,
  Clock,
  TrendingUp,
  Compass,
} from "lucide-react";

interface DashboardLayoutProps {
  children: ReactNode;
}

const navigation = [
  { name: "Dashboard", href: "/", icon: BarChart3 },
  { name: "Supply", href: "/supply", icon: Coins },
  { name: "Rich List", href: "/rich-list", icon: Wallet },
  { name: "Transactions", href: "/transactions", icon: Activity },
  { name: "Transfers", href: "/transfers", icon: ArrowRightLeft },
  { name: "Validators/SVs", href: "/validators", icon: Zap },
  { name: "Validator Licenses", href: "/validator-licenses", icon: Ticket },
  { name: "Round Stats", href: "/round-stats", icon: Layers },
  { name: "ANS", href: "/ans", icon: Globe },
  { name: "Featured Apps", href: "/apps", icon: Package },
  { name: "Governance", href: "/governance", icon: Vote },
  { name: "Elections", href: "/elections", icon: Vote },
  { name: "External Party", href: "/external-party-setup", icon: UserPlus },
  { name: "Transfer Counters", href: "/transfer-counters", icon: Hash },
  { name: "External Party Rules", href: "/external-party-rules", icon: Shield },
  { name: "Amulet Rules", href: "/amulet-rules", icon: Shield },
  { name: "Statistics", href: "/stats", icon: Database },
  { name: "SV Rewards", href: "/unclaimed-sv-rewards", icon: Award },
  { name: "Member Traffic", href: "/member-traffic", icon: Radio },
  { name: "Subscriptions", href: "/subscriptions", icon: Package },
  { name: "DSO State", href: "/dso-state", icon: Users },
  { name: "ACS Snapshot", href: "/snapshot-progress", icon: Upload },
  { name: "Backfill Progress", href: "/backfill-progress", icon: Clock },
  { name: "Live Updates", href: "/live-updates", icon: TrendingUp },
  { name: "Explorer", href: "/explorer", icon: Compass },
  { name: "Admin", href: "/admin", icon: Shield },
  { name: "Templates", href: "/templates", icon: FileText },
];

export const DashboardLayout = ({ children }: DashboardLayoutProps) => {
  const location = useLocation();

  return (
    <div className="min-h-screen">
      {/* Header */}
      <header className="glass-card border-b border-border/50 sticky top-0 z-50">
        <div className="container mx-auto px-6 py-4">
          {/* Top row: Logo and Search */}
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

          {/* Bottom row: Navigation tabs with wrapping */}
          <nav className="flex flex-wrap gap-1">
            {navigation.map((item) => {
              const isActive = location.pathname === item.href;
              const Icon = item.icon;
              return (
                <Link
                  key={item.name}
                  to={item.href}
                  className={`flex items-center space-x-2 px-4 py-2 rounded-lg transition-smooth ${
                    isActive
                      ? "bg-primary/10 text-primary"
                      : "text-muted-foreground hover:text-foreground hover:bg-muted/50"
                  }`}
                >
                  <Icon className="h-4 w-4" />
                  <span className="text-sm font-medium">{item.name}</span>
                </Link>
              );
            })}
          </nav>
        </div>
      </header>

      {/* Main Content */}
      <main className="container mx-auto px-6 py-8">{children}</main>
    </div>
  );
};
