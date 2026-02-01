import { Toaster } from "@/components/ui/toaster";
import { Toaster as Sonner } from "@/components/ui/sonner";
import { TooltipProvider } from "@/components/ui/tooltip";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter, Routes, Route } from "react-router-dom";
import { ErrorBoundary } from "@/components/ErrorBoundary";

// Core pages
import Dashboard from "./pages/Dashboard";
import Supply from "./pages/Supply";
import NetworkInfo from "./pages/NetworkInfo";

// Transactions
import Transactions from "./pages/Transactions";
import Transfers from "./pages/Transfers";
import RichList from "./pages/RichList";

// Validators & Rounds
import Validators from "./pages/Validators";
import ValidatorLicenses from "./pages/ValidatorLicenses";
import RoundStats from "./pages/RoundStats";
import TrafficStatus from "./pages/TrafficStatus";

// Rewards & Pricing
import Rewards from "./pages/Rewards";
import PriceVotes from "./pages/PriceVotes";
import KaikoFeed from "./pages/KaikoFeed";

// Governance
import Governance from "./pages/Governance";
import GovernanceFlow from "./pages/GovernanceFlow";
import Elections from "./pages/Elections";
import DSOState from "./pages/DSOState";

// Services
import ANS from "./pages/ANS";
import Apps from "./pages/Apps";
import Subscriptions from "./pages/Subscriptions";

// Rules & Config
import AmuletRules from "./pages/AmuletRules";
import ExternalPartySetup from "./pages/ExternalPartySetup";
import ExternalPartyRules from "./pages/ExternalPartyRules";
import TransferCounters from "./pages/TransferCounters";

// Statistics
import Stats from "./pages/Stats";
import MemberTraffic from "./pages/MemberTraffic";

// Party details
import Party from "./pages/Party";

// Admin pages (routes still work, just not in main nav)
import Admin from "./pages/Admin";
import Templates from "./pages/Templates";
import TemplateAudit from "./pages/TemplateAudit";
import TemplateDocumentation from "./pages/TemplateDocumentation";
import SnapshotProgress from "./pages/SnapshotProgress";
import IngestionDashboard from "./pages/IngestionDashboard";
import BackfillProgress from "./pages/BackfillProgress";
import LiveUpdates from "./pages/LiveUpdates";

// Legacy pages (kept for backwards compatibility)
import UnclaimedSVRewards from "./pages/UnclaimedSVRewards";
import RewardCalculations from "./pages/RewardCalculations";

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
      <ErrorBoundary title="SCANTON crashed on this page">
        <BrowserRouter>
          <Routes>
            {/* Core pages */}
            <Route path="/" element={<Dashboard />} />
            <Route path="/supply" element={<Supply />} />
            <Route path="/network-info" element={<NetworkInfo />} />
            
            {/* Transactions */}
            <Route path="/transactions" element={<Transactions />} />
            <Route path="/transfers" element={<Transfers />} />
            <Route path="/rich-list" element={<RichList />} />
            
            {/* Validators & Rounds */}
            <Route path="/validators" element={<Validators />} />
            <Route path="/validator-licenses" element={<ValidatorLicenses />} />
            <Route path="/round-stats" element={<RoundStats />} />
            <Route path="/traffic-status" element={<TrafficStatus />} />
            
            {/* Rewards & Pricing */}
            <Route path="/rewards" element={<Rewards />} />
            <Route path="/price-votes" element={<PriceVotes />} />
            <Route path="/kaiko-feed" element={<KaikoFeed />} />
            
            {/* Governance */}
            <Route path="/governance" element={<Governance />} />
            <Route path="/governance-flow" element={<GovernanceFlow />} />
            <Route path="/elections" element={<Elections />} />
            <Route path="/dso-state" element={<DSOState />} />
            
            {/* Services */}
            <Route path="/ans" element={<ANS />} />
            <Route path="/apps" element={<Apps />} />
            <Route path="/subscriptions" element={<Subscriptions />} />
            
            {/* Rules & Config */}
            <Route path="/amulet-rules" element={<AmuletRules />} />
            <Route path="/external-party-setup" element={<ExternalPartySetup />} />
            <Route path="/external-party-rules" element={<ExternalPartyRules />} />
            <Route path="/transfer-counters" element={<TransferCounters />} />
            
            {/* Statistics */}
            <Route path="/stats" element={<Stats />} />
            <Route path="/member-traffic" element={<MemberTraffic />} />
            
            {/* Party details */}
            <Route path="/party/:partyId" element={<Party />} />
            
            {/* Admin pages (not in main nav but still accessible) */}
            <Route path="/admin" element={<Admin />} />
            <Route path="/templates" element={<Templates />} />
            <Route path="/template-docs" element={<TemplateDocumentation />} />
            <Route path="/templates-docs" element={<TemplateDocumentation />} />
            <Route path="/template-audit" element={<TemplateAudit />} />
            <Route path="/snapshot-progress" element={<SnapshotProgress />} />
            <Route path="/ingestion" element={<IngestionDashboard />} />
            <Route path="/backfill-progress" element={<BackfillProgress />} />
            <Route path="/live-updates" element={<LiveUpdates />} />
            
            {/* Legacy routes (kept for backwards compatibility) */}
            <Route path="/unclaimed-sv-rewards" element={<UnclaimedSVRewards />} />
            <Route path="/reward-calculations" element={<RewardCalculations />} />
            
            {/* ADD ALL CUSTOM ROUTES ABOVE THE CATCH-ALL "*" ROUTE */}
            <Route path="*" element={<NotFound />} />
          </Routes>
        </BrowserRouter>
      </ErrorBoundary>
    </TooltipProvider>
  </QueryClientProvider>
);

export default App;
