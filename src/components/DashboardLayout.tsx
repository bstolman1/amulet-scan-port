import { ReactNode } from "react";
import { NavLink } from "./NavLink";

interface DashboardLayoutProps {
  children: ReactNode;
}

const DashboardLayout = ({ children }: DashboardLayoutProps) => {
  return (
    <div className="min-h-screen bg-background">
      <nav className="border-b border-border bg-card">
        <div className="container mx-auto px-4 py-4">
          <div className="flex items-center justify-between mb-4">
            <h1 className="text-2xl font-bold">Amulet Ledger Probe</h1>
          </div>
          <div className="flex flex-wrap gap-2">
            <NavLink to="/">Dashboard</NavLink>
            <NavLink to="/transactions">Transactions</NavLink>
            <NavLink to="/validators">Validators</NavLink>
            <NavLink to="/round-stats">Rounds</NavLink>
            <NavLink to="/ans">ANS</NavLink>
            <NavLink to="/stats">Stats</NavLink>
            <NavLink to="/apps">Apps</NavLink>
            <NavLink to="/governance">Governance</NavLink>
            <NavLink to="/supply">Supply</NavLink>
            <NavLink to="/unclaimed-sv-rewards">SV Rewards</NavLink>
            <NavLink to="/admin">Admin</NavLink>
            <NavLink to="/snapshot-progress">Snapshots</NavLink>
            <NavLink to="/transfers">Transfers</NavLink>
            <NavLink to="/rich-list">Rich List</NavLink>
            <NavLink to="/templates">Templates</NavLink>
            <NavLink to="/template-audit">Template Audit</NavLink>
            <NavLink to="/member-traffic">Member Traffic</NavLink>
            <NavLink to="/subscriptions">Subscriptions</NavLink>
            <NavLink to="/dso-state">DSO State</NavLink>
            <NavLink to="/validator-licenses">Licenses</NavLink>
            <NavLink to="/external-party-setup">External Party</NavLink>
            <NavLink to="/backfill-progress">Backfill</NavLink>
            <NavLink to="/live-updates">Live Updates</NavLink>
            <NavLink to="/elections">Elections</NavLink>
            <NavLink to="/transfer-counters">Counters</NavLink>
            <NavLink to="/external-party-rules">External Rules</NavLink>
            <NavLink to="/amulet-rules">Amulet Rules</NavLink>
          </div>
        </div>
      </nav>
      <main className="container mx-auto px-4 py-8">{children}</main>
    </div>
  );
};

export default DashboardLayout;
