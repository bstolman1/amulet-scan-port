import { Toaster } from "@/components/ui/toaster";
import { Toaster as Sonner } from "@/components/ui/sonner";
import { TooltipProvider } from "@/components/ui/tooltip";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter, Routes, Route } from "react-router-dom";
import Dashboard from "./pages/Dashboard";
import Transactions from "./pages/Transactions";
import Validators from "./pages/Validators";

import RoundStats from "./pages/RoundStats";
import ANS from "./pages/ANS";
import Stats from "./pages/Stats";
import Apps from "./pages/Apps";
import Governance from "./pages/Governance";
import Supply from "./pages/Supply";
import UnclaimedSVRewards from "./pages/UnclaimedSVRewards";
import Admin from "./pages/Admin";
import SnapshotProgress from "./pages/SnapshotProgress";
import Transfers from "./pages/Transfers";
import RichList from "./pages/RichList";
import Templates from "./pages/Templates";
import TemplateAudit from "./pages/TemplateAudit";
import MemberTraffic from "./pages/MemberTraffic";
import Subscriptions from "./pages/Subscriptions";
import DSOState from "./pages/DSOState";
import ValidatorLicenses from "./pages/ValidatorLicenses";
import ExternalPartySetup from "./pages/ExternalPartySetup";
import BackfillProgress from "./pages/BackfillProgress";
import LiveUpdates from "./pages/LiveUpdates";
import IngestionDashboard from "./pages/IngestionDashboard";
import IndexStatus from "./pages/IndexStatus";

import Elections from "./pages/Elections";
import TransferCounters from "./pages/TransferCounters";
import ExternalPartyRules from "./pages/ExternalPartyRules";
import AmuletRules from "./pages/AmuletRules";
import GovernanceFlow from "./pages/GovernanceFlow";
import KaikoFeed from "./pages/KaikoFeed";
import NotFound from "./pages/NotFound";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 2,
      retryDelay: (attemptIndex) => Math.min(1000 * 2 ** attemptIndex, 3000),
      staleTime: 60_000,
      gcTime: 5 * 60_000,
      refetchOnWindowFocus: false,
      networkMode: "offlineFirst",
    },
  },
});

const App = () => (
  <QueryClientProvider client={queryClient}>
    <TooltipProvider>
      <Toaster />
      <Sonner />
      <BrowserRouter>
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/transactions" element={<Transactions />} />
          <Route path="/transfers" element={<Transfers />} />
          <Route path="/rich-list" element={<RichList />} />
          <Route path="/validators" element={<Validators />} />
          <Route path="/round-stats" element={<RoundStats />} />
          <Route path="/ans" element={<ANS />} />
          <Route path="/stats" element={<Stats />} />
          <Route path="/apps" element={<Apps />} />
          <Route path="/governance" element={<Governance />} />
          <Route path="/supply" element={<Supply />} />
          <Route path="/unclaimed-sv-rewards" element={<UnclaimedSVRewards />} />
          <Route path="/admin" element={<Admin />} />
          <Route path="/snapshot-progress" element={<SnapshotProgress />} />
          <Route path="/templates" element={<Templates />} />
          <Route path="/template-audit" element={<TemplateAudit />} />
          <Route path="/member-traffic" element={<MemberTraffic />} />
          <Route path="/subscriptions" element={<Subscriptions />} />
          <Route path="/dso-state" element={<DSOState />} />
          <Route path="/validator-licenses" element={<ValidatorLicenses />} />
          <Route path="/external-party-setup" element={<ExternalPartySetup />} />
          <Route path="/amulet-rules" element={<AmuletRules />} />

          <Route path="/elections" element={<Elections />} />
          <Route path="/transfer-counters" element={<TransferCounters />} />
          <Route path="/external-party-rules" element={<ExternalPartyRules />} />
          <Route path="/backfill-progress" element={<BackfillProgress />} />
          <Route path="/live-updates" element={<LiveUpdates />} />
          <Route path="/ingestion" element={<IngestionDashboard />} />
          <Route path="/governance-flow" element={<GovernanceFlow />} />
          <Route path="/kaiko-feed" element={<KaikoFeed />} />
          <Route path="/index-status" element={<IndexStatus />} />
          {/* ADD ALL CUSTOM ROUTES ABOVE THE CATCH-ALL "*" ROUTE */}
          <Route path="*" element={<NotFound />} />
        </Routes>
      </BrowserRouter>
    </TooltipProvider>
  </QueryClientProvider>
);

export default App;
