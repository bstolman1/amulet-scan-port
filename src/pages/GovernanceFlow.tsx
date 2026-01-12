import React, { useState, useEffect, useMemo } from "react";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Input } from "@/components/ui/input";
import { Calendar as CalendarComponent } from "@/components/ui/calendar";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Checkbox } from "@/components/ui/checkbox";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter, DialogDescription } from "@/components/ui/dialog";
import { format, startOfMonth, endOfMonth, startOfYear, endOfYear, subMonths, subYears } from "date-fns";
import { 
  ExternalLink, 
  RefreshCw, 
  FileText, 
  Calendar,
  CalendarIcon,
  AlertCircle,
  ChevronDown,
  ChevronUp,
  CheckCircle2,
  XCircle,
  Clock,
  Vote,
  ArrowRight,
  Filter,
  Search,
  X,
  MoreVertical,
  Edit2,
  CheckSquare,
  Square,
  Merge,
  SplitSquareVertical,
  MoveRight,
  Lightbulb,
} from "lucide-react";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
  DropdownMenuSub,
  DropdownMenuSubTrigger,
  DropdownMenuSubContent,
} from "@/components/ui/dropdown-menu";
import { useToast } from "@/hooks/use-toast";
import { getDuckDBApiUrl } from "@/lib/backend-config";
import { cn } from "@/lib/utils";
import { useGovernanceVoteHistory, ParsedVoteResult } from "@/hooks/use-scan-vote-results";
import { LearnFromCorrectionsPanel } from "@/components/LearnFromCorrectionsPanel";


interface TopicIdentifiers {
  cipNumber: string | null;
  appName: string | null;
  validatorName: string | null;
  keywords: string[];
  isCipDiscussion?: boolean;
}

interface Topic {
  id: string;
  subject: string;
  date: string;
  content: string;
  excerpt: string;
  sourceUrl?: string;
  linkedUrls: string[];
  groupName: string;
  groupLabel: string;
  stage: string;
  flow: string;
  messageCount?: number;
  identifiers: TopicIdentifiers;
  // Inference metadata
  postedStage?: string;
  inferredStage?: string | null;
  inferenceConfidence?: number | null;
  effectiveStage?: string;
}

interface LifecycleItem {
  id: string;
  primaryId: string;
  type: 'cip' | 'featured-app' | 'validator' | 'protocol-upgrade' | 'outcome' | 'other';
  network?: 'testnet' | 'mainnet' | null;
  stages: Record<string, Topic[]>;
  topics: Topic[];
  firstDate: string;
  lastDate: string;
  currentStage: string;
  overrideApplied?: boolean;
  overrideReason?: string;
  llmClassified?: boolean;
}

interface GovernanceData {
  lifecycleItems: LifecycleItem[];
  allTopics: Topic[];
  groups: Record<string, { id: number; label: string; stage: string; flow: string }>;
  stats: {
    totalTopics: number;
    lifecycleItems: number;
    byType: Record<string, number>;
    byStage: Record<string, number>;
    groupCounts: Record<string, number>;
  };
  cachedAt?: string;
  stale?: boolean;
}

// Type-specific workflow stages
const WORKFLOW_STAGES = {
  cip: ['cip-discuss', 'cip-vote', 'cip-announce', 'sv-announce', 'sv-onchain-vote', 'sv-milestone'],
  'featured-app': ['tokenomics', 'tokenomics-announce', 'sv-announce', 'sv-onchain-vote', 'sv-milestone'],
  validator: ['tokenomics', 'sv-announce', 'sv-onchain-vote', 'sv-milestone'],
  'protocol-upgrade': ['tokenomics', 'sv-announce', 'sv-onchain-vote'],
  outcome: ['sv-announce'],
  other: ['tokenomics', 'sv-announce', 'sv-onchain-vote'],
};

// All possible stages with their display config
const STAGE_CONFIG: Record<string, { label: string; icon: typeof FileText; color: string }> = {
  // CIP stages
  'cip-discuss': { label: 'Discuss', icon: FileText, color: 'bg-blue-500/20 text-blue-400 border-blue-500/30' },
  'cip-vote': { label: 'Vote', icon: Vote, color: 'bg-purple-500/20 text-purple-400 border-purple-500/30' },
  'cip-announce': { label: 'Announce', icon: CheckCircle2, color: 'bg-green-500/20 text-green-400 border-green-500/30' },
  // On-chain vote stage
  'sv-onchain-vote': { label: 'On-Chain Vote', icon: Vote, color: 'bg-pink-500/20 text-pink-400 border-pink-500/30' },
  // Milestone reward stage
  'sv-milestone': { label: 'Milestone', icon: CheckSquare, color: 'bg-teal-500/20 text-teal-400 border-teal-500/30' },
  // Shared stages
  'tokenomics': { label: 'Tokenomics', icon: FileText, color: 'bg-blue-500/20 text-blue-400 border-blue-500/30' },
  'tokenomics-announce': { label: 'Announced', icon: CheckCircle2, color: 'bg-green-500/20 text-green-400 border-green-500/30' },
  'sv-announce': { label: 'SV Announce', icon: Clock, color: 'bg-orange-500/20 text-orange-400 border-orange-500/30' },
};

const TYPE_CONFIG = {
  cip: { label: 'CIP', color: 'bg-primary/20 text-primary' },
  'featured-app': { label: 'Featured App', color: 'bg-emerald-500/20 text-emerald-400' },
  validator: { label: 'Validator', color: 'bg-orange-500/20 text-orange-400' },
  'protocol-upgrade': { label: 'Protocol Upgrade', color: 'bg-cyan-500/20 text-cyan-400' },
  outcome: { label: 'Tokenomics Outcomes', color: 'bg-amber-500/20 text-amber-400' },
  other: { label: 'Other', color: 'bg-muted text-muted-foreground' },
};

// Interface for VoteRequest contracts from ACS
interface VoteRequest {
  contract_id: string;
  payload: {
    voteBefore: string;
    requester: string;
    reason: {
      url: string;
      body: string;
    };
    action: {
      tag: string;
      value?: Record<string, unknown>;
    };
    votes: Array<{
      sv: string;
      accept: boolean;
      reason: { url: string; body: string };
    }>;
    trackingCid?: string;
  };
  record_time: string;
}

// Helper to extract reference key from VoteRequest for mapping
interface VoteRequestMapping {
  type: 'cip' | 'featured-app' | 'validator' | 'protocol-upgrade' | 'other';
  key: string;
  stage: 'sv-onchain-vote' | 'sv-milestone';
}

// Helper to detect if a vote represents a milestone/reward vote
// NOTE: some milestone votes don't encode “milestone” in the action tag, but do in the proposal text.
const isMilestoneVote = (actionTag: string, text: string): boolean => {
  // We keep this intentionally broad because Scan/ACS action tags and reason text have evolved over time.
  // Text variants we see include: "milestone", "milestones", and "milestone(s)".
  const milestoneText = /\bmilestone(?:s|\(s\))?\b/i;

  return (
    /MintUnclaimedRewards/i.test(actionTag) ||
    /SRARC_MintUnclaimed/i.test(actionTag) ||
    /MintRewards/i.test(actionTag) ||
    /DistributeRewards/i.test(actionTag) ||
    /Reward/i.test(actionTag) ||
    /Coupon/i.test(actionTag) ||
    milestoneText.test(text)
  );
};

// Updated interface to support multiple stages for a single vote
interface VoteRequestMappingMulti {
  type: 'cip' | 'featured-app' | 'validator' | 'protocol-upgrade' | 'other';
  key: string;
  stages: Array<'sv-onchain-vote' | 'sv-milestone'>; // A vote can appear in multiple stages
}

const extractVoteRequestMapping = (voteRequest: VoteRequest): VoteRequestMappingMulti | null => {
  const reason = voteRequest.payload?.reason;
  const actionTag = voteRequest.payload?.action?.tag || '';
  const actionValue = voteRequest.payload?.action?.value || {};
  const text = `${reason?.body || ''} ${reason?.url || ''}`;
  
  // Determine stages: milestone votes appear in BOTH on-chain vote AND milestone categories
  const stages: Array<'sv-onchain-vote' | 'sv-milestone'> = ['sv-onchain-vote'];
  if (isMilestoneVote(actionTag, text)) {
    stages.push('sv-milestone');
  }
  
  // Check for CIP references
  const cipMatch = text.match(/CIP[#\-\s]?0*(\d+)/i);
  if (cipMatch) {
    return { type: 'cip', key: `CIP-${cipMatch[1].padStart(4, '0')}`, stages };
  }
  
  // Check for Featured App actions
  if (actionTag.includes('GrantFeaturedAppRight') || actionTag.includes('RevokeFeaturedAppRight') ||
      actionTag.includes('SetFeaturedAppRight') || text.toLowerCase().includes('featured app') ||
      isMilestoneVote(actionTag, text)) {
    // Extract app name from action value or reason
    const appName =
      (actionValue as any)?.provider ||
      (actionValue as any)?.featuredAppProvider ||
      (actionValue as any)?.featuredApp ||
      (actionValue as any)?.beneficiary ||
      (actionValue as any)?.name ||
      text.match(/(?:mainnet|testnet):\s*([^\s,]+)/i)?.[1] ||
      text.match(/app[:\s]+([^\s,]+)/i)?.[1];
    if (appName) {
      const normalized = String(appName).replace(/::/g, '::').toLowerCase();
      return { type: 'featured-app', key: normalized, stages };
    }
  }
  
  // Check for Validator actions
  if (actionTag.includes('OnboardValidator') || actionTag.includes('OffboardValidator') ||
      actionTag.includes('ValidatorOnboarding') || text.toLowerCase().includes('validator')) {
    const validatorName = (actionValue as any)?.validator || (actionValue as any)?.name ||
                         text.match(/validator[:\s]+([^\s,]+)/i)?.[1];
    if (validatorName) {
      return { type: 'validator', key: validatorName.toLowerCase(), stages };
    }
  }
  
  // Check for Protocol Upgrade actions
  if (actionTag.includes('ScheduleDomainMigration') || actionTag.includes('ProtocolUpgrade') ||
      actionTag.includes('Synchronizer') || text.toLowerCase().includes('migration') ||
      text.toLowerCase().includes('splice')) {
    const version = text.match(/splice[:\s]*(\d+\.\d+)/i)?.[1] ||
                   text.match(/version[:\s]*(\d+\.\d+)/i)?.[1];
    return { type: 'protocol-upgrade', key: version || 'upgrade', stages };
  }
  
  return null;
};

// Extract reference key from ParsedVoteResult (historical votes from Scan API)
const extractHistoricalVoteMapping = (vote: ParsedVoteResult): VoteRequestMappingMulti | null => {
  const actionTag = vote.actionType || '';
  const text = `${vote.reasonBody || ''} ${vote.reasonUrl || ''} ${vote.actionTitle || ''}`;
  
  // Determine stages: milestone votes appear in BOTH on-chain vote AND milestone categories
  const stages: Array<'sv-onchain-vote' | 'sv-milestone'> = ['sv-onchain-vote'];
  if (isMilestoneVote(actionTag, text)) {
    stages.push('sv-milestone');
  }
  
  // Check for CIP references
  const cipMatch = text.match(/CIP[#\-\s]?0*(\d+)/i);
  if (cipMatch) {
    return { type: 'cip', key: `CIP-${cipMatch[1].padStart(4, '0')}`, stages };
  }
  
  // Check for Featured App actions (including milestone reward distributions)
  if (actionTag.includes('GrantFeaturedAppRight') || actionTag.includes('RevokeFeaturedAppRight') ||
      actionTag.includes('SetFeaturedAppRight') || actionTag.includes('FeaturedApp') ||
      text.toLowerCase().includes('featured app') || isMilestoneVote(actionTag, text)) {
    // Extract app name from action details or reason
    const actionValue = vote.actionDetails || {};
    const appName =
      (actionValue as any)?.provider ||
      (actionValue as any)?.featuredAppProvider ||
      (actionValue as any)?.featuredApp ||
      (actionValue as any)?.beneficiary ||
      (actionValue as any)?.name ||
      text.match(/(?:mainnet|testnet):\s*([^\s,]+)/i)?.[1] ||
      text.match(/app[:\s]+([^\s,]+)/i)?.[1];
    if (appName) {
      const normalized = String(appName).replace(/::/g, '::').toLowerCase();
      return { type: 'featured-app', key: normalized, stages };
    }
  }
  
  // Check for Validator actions
  if (actionTag.includes('OnboardValidator') || actionTag.includes('OffboardValidator') ||
      actionTag.includes('ValidatorOnboarding') || text.toLowerCase().includes('validator')) {
    const actionValue = vote.actionDetails || {};
    const validatorName = (actionValue as any)?.validator || (actionValue as any)?.name ||
                         text.match(/validator[:\s]+([^\s,]+)/i)?.[1];
    if (validatorName) {
      return { type: 'validator', key: validatorName.toLowerCase(), stages };
    }
  }
  
  // Check for Protocol Upgrade actions
  if (actionTag.includes('ScheduleDomainMigration') || actionTag.includes('ProtocolUpgrade') ||
      actionTag.includes('Synchronizer') || text.toLowerCase().includes('migration') ||
      text.toLowerCase().includes('splice')) {
    const version = text.match(/splice[:\s]*(\d+\.\d+)/i)?.[1] ||
                   text.match(/version[:\s]*(\d+\.\d+)/i)?.[1];
    return { type: 'protocol-upgrade', key: version || 'upgrade', stages };
  }
  
  return null;
};

// Legacy helper for backwards compatibility
const extractCipReference = (voteRequest: VoteRequest): string | null => {
  const mapping = extractVoteRequestMapping(voteRequest);
  if (mapping?.type === 'cip') {
    // Return just the number part for legacy CIP mapping
    const match = mapping.key.match(/CIP-(\d+)/i);
    return match ? match[1] : null;
  }
  return null;
};

const GovernanceFlow = () => {
  const [data, setData] = useState<GovernanceData | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [expandedIds, setExpandedIds] = useState<Set<string>>(new Set());
  const [typeFilter, setTypeFilter] = useState<string>('all');
  const [stageFilter, setStageFilter] = useState<string>('all');
  const [viewMode, setViewMode] = useState<'lifecycle' | 'all' | 'timeline' | 'learn'>('lifecycle');
  const [searchQuery, setSearchQuery] = useState<string>('');
  
  const [dateFrom, setDateFrom] = useState<Date | undefined>(undefined);
  const [dateTo, setDateTo] = useState<Date | undefined>(undefined);
  const [datePreset, setDatePreset] = useState<string>('all');
  const [cachedAt, setCachedAt] = useState<string | null>(null);
  const [voteRequests, setVoteRequests] = useState<VoteRequest[]>([]);
  
  // Fetch historical vote results from Scan API
  const { data: historicalVotes = [] } = useGovernanceVoteHistory(500);
  
  // Bulk selection state
  const [selectedItems, setSelectedItems] = useState<Set<string>>(new Set());
  const [bulkSelectMode, setBulkSelectMode] = useState(false);
  
  // Unified on-chain vote item for display
  interface OnChainVoteItem {
    id: string;
    source: 'acs' | 'history';
    stage: 'sv-onchain-vote' | 'sv-milestone';
    status: 'pending' | 'approved' | 'rejected' | 'expired';
    votesFor: number;
    votesAgainst: number;
    totalVotes: number;
    voteBefore: Date | null;
    reasonBody: string;
    reasonUrl: string;
    // Original data for linking
    voteRequest?: VoteRequest;
    historicalVote?: ParsedVoteResult;
  }
  
  // Map lifecycle item keys to their in-progress VoteRequests (from ACS) with stage info
  // A vote can appear in multiple stages (e.g., both sv-onchain-vote and sv-milestone)
  const acsVoteRequestMap = useMemo(() => {
    const map = new Map<string, Array<{ vr: VoteRequest; stage: 'sv-onchain-vote' | 'sv-milestone' }>>();
    voteRequests.forEach(vr => {
      const mapping = extractVoteRequestMapping(vr);
      if (mapping) {
        const key = mapping.key.toLowerCase();
        if (!map.has(key)) {
          map.set(key, []);
        }
        // Add an entry for EACH stage (vote can appear in multiple categories)
        mapping.stages.forEach(stage => {
          map.get(key)!.push({ vr, stage });
        });
      }
    });
    return map;
  }, [voteRequests]);
  
  // Map lifecycle item keys to their historical votes (from Scan API History) with stage info
  // A vote can appear in multiple stages (e.g., both sv-onchain-vote and sv-milestone)
  const historicalVoteMap = useMemo(() => {
    const map = new Map<string, Array<{ vote: ParsedVoteResult; stage: 'sv-onchain-vote' | 'sv-milestone' }>>();
    historicalVotes.forEach(vote => {
      const mapping = extractHistoricalVoteMapping(vote);
      if (mapping) {
        const key = mapping.key.toLowerCase();
        if (!map.has(key)) {
          map.set(key, []);
        }
        // Add an entry for EACH stage (vote can appear in multiple categories)
        mapping.stages.forEach(stage => {
          map.get(key)!.push({ vote, stage });
        });
      }
    });
    return map;
  }, [historicalVotes]);
  
  // Combined map: unifies ACS (in-progress) and historical votes into OnChainVoteItem[]
  const combinedVoteMap = useMemo(() => {
    const map = new Map<string, OnChainVoteItem[]>();
    
    // Add in-progress votes from ACS
    acsVoteRequestMap.forEach((entries, key) => {
      if (!map.has(key)) {
        map.set(key, []);
      }
      entries.forEach(({ vr, stage }) => {
        const payload = vr.payload;
        const voteBefore = payload?.voteBefore ? new Date(payload.voteBefore) : null;
        const isExpired = voteBefore && voteBefore < new Date();
        const votesRaw = payload?.votes || [];
        
        let votesFor = 0;
        let votesAgainst = 0;
        for (const vote of votesRaw) {
          const [, voteData] = Array.isArray(vote) ? vote : [vote.sv || "Unknown", vote];
          const isAccept = voteData?.accept === true || voteData?.Accept === true;
          const isReject = voteData?.accept === false || voteData?.reject === true || voteData?.Reject === true;
          if (isAccept) votesFor++;
          else if (isReject) votesAgainst++;
        }
        
        const threshold = 10;
        let status: 'pending' | 'approved' | 'rejected' | 'expired' = 'pending';
        if (votesFor >= threshold) status = 'approved';
        else if (isExpired && votesFor < threshold) status = isExpired ? 'expired' : 'rejected';
        
        map.get(key)!.push({
          id: payload?.trackingCid?.slice(0, 12) || vr.contract_id?.slice(0, 12) || 'unknown',
          source: 'acs',
          stage,
          status: status === 'expired' ? 'pending' : status, // ACS votes are in-progress
          votesFor,
          votesAgainst,
          totalVotes: votesRaw.length,
          voteBefore,
          reasonBody: payload?.reason?.body || '',
          reasonUrl: payload?.reason?.url || '',
          voteRequest: vr,
        });
      });
    });
    
    // Add historical votes from Scan API
    historicalVoteMap.forEach((entries, key) => {
      if (!map.has(key)) {
        map.set(key, []);
      }
      entries.forEach(({ vote, stage }) => {
        map.get(key)!.push({
          id: vote.id,
          source: 'history',
          stage,
          status: vote.outcome === 'accepted' ? 'approved' : vote.outcome === 'rejected' ? 'rejected' : 'expired',
          votesFor: vote.votesFor,
          votesAgainst: vote.votesAgainst,
          totalVotes: vote.totalVotes,
          voteBefore: vote.voteBefore ? new Date(vote.voteBefore) : null,
          reasonBody: vote.reasonBody || '',
          reasonUrl: vote.reasonUrl || '',
          historicalVote: vote,
        });
      });
    });
    
    // Sort each entry: ACS (in-progress) first, then historical by date DESC
    map.forEach((items, key) => {
      items.sort((a, b) => {
        // ACS (in-progress) always comes first
        if (a.source === 'acs' && b.source !== 'acs') return -1;
        if (a.source !== 'acs' && b.source === 'acs') return 1;
        // Then sort by voteBefore date DESC
        const dateA = a.voteBefore?.getTime() || 0;
        const dateB = b.voteBefore?.getTime() || 0;
        return dateB - dateA;
      });
      map.set(key, items);
    });
    
    return map;
  }, [acsVoteRequestMap, historicalVoteMap]);
  
  // Legacy: keep voteRequestMap for backwards compatibility with existing code
  const voteRequestMap = acsVoteRequestMap;
  
  // Legacy CIP-only map for backwards compatibility
  const cipVoteRequestMap = useMemo(() => {
    const map = new Map<string, VoteRequest[]>();
    voteRequests.forEach(vr => {
      const cipNum = extractCipReference(vr);
      if (cipNum) {
        if (!map.has(cipNum)) {
          map.set(cipNum, []);
        }
        map.get(cipNum)!.push(vr);
      }
    });
    return map;
  }, [voteRequests]);

  const fetchData = async (forceRefresh = false) => {
    if (forceRefresh) {
      setIsRefreshing(true);
    } else {
      setIsLoading(true);
    }
    setError(null);
    
    try {
      const baseUrl = getDuckDBApiUrl();
      const url = forceRefresh 
        ? `${baseUrl}/api/governance-lifecycle?refresh=true`
        : `${baseUrl}/api/governance-lifecycle`;
      const response = await fetch(url);
      
      // Handle non-2xx responses
      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(errorData.error || `Failed to fetch: ${response.status}`);
      }
      
      const result = await response.json();
      
      // Check for warning (API key not configured but has cached data)
      if (result.warning) {
        console.warn('Governance data warning:', result.warning);
      }
      
      // Only treat as error if no data AND explicit error
      if (result.error && (!result.lifecycleItems || result.lifecycleItems.length === 0)) {
        throw new Error(result.error);
      }
      
      setData(result);
      setCachedAt(result.cachedAt || null);
    } catch (err) {
      console.error('Failed to fetch governance data:', err);
      setError(err instanceof Error ? err.message : 'Failed to fetch governance data');
    } finally {
      setIsLoading(false);
      setIsRefreshing(false);
    }
  };

  const { toast } = useToast();
  
  // Handler to reclassify a lifecycle item (card-level)
  const handleReclassify = async (primaryId: string, newType: LifecycleItem['type']) => {
    try {
      const baseUrl = getDuckDBApiUrl();
      const response = await fetch(`${baseUrl}/api/governance-lifecycle/overrides`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          primaryId,
          type: newType,
          reason: 'Manual UI correction',
        }),
      });
      
      if (!response.ok) {
        throw new Error('Failed to save override');
      }
      
      toast({
        title: "Classification updated",
        description: `"${primaryId}" will now appear as ${TYPE_CONFIG[newType].label}`,
      });
      
      // Refresh data to show the change
      fetchData(false);
    } catch (err) {
      console.error('Failed to reclassify:', err);
      toast({
        title: "Error",
        description: "Failed to save classification override",
        variant: "destructive",
      });
    }
  };

  // Handler to reclassify a single topic (moves it to a different type category)
  const handleReclassifyTopic = async (topicId: string, topicSubject: string, newType: LifecycleItem['type']) => {
    try {
      const baseUrl = getDuckDBApiUrl();
      const response = await fetch(`${baseUrl}/api/governance-lifecycle/overrides/topic`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          topicId,
          newType,
          reason: 'Manual topic reclassification',
        }),
      });
      
      if (!response.ok) {
        throw new Error('Failed to save topic override');
      }
      
      toast({
        title: "Topic reclassified",
        description: `Topic will now appear as ${TYPE_CONFIG[newType].label}`,
      });
      
      // Refresh data to show the change
      fetchData(false);
    } catch (err) {
      console.error('Failed to reclassify topic:', err);
      toast({
        title: "Error",
        description: "Failed to save topic classification override",
        variant: "destructive",
      });
    }
  };

  // Handler to extract a topic to its own card (keeps same type)
  const handleExtractTopic = async (topicId: string, topicSubject: string) => {
    try {
      const baseUrl = getDuckDBApiUrl();
      const response = await fetch(`${baseUrl}/api/governance-lifecycle/overrides/extract`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          topicId,
          reason: 'Extracted via UI',
        }),
      });
      
      if (!response.ok) {
        throw new Error('Failed to save extract override');
      }
      
      toast({
        title: "Topic extracted",
        description: `"${topicSubject.slice(0, 50)}..." will now appear as its own card`,
      });
      
      // Refresh data to show the change
      fetchData(false);
    } catch (err) {
      console.error('Failed to extract topic:', err);
      toast({
        title: "Error",
        description: "Failed to extract topic to own card",
        variant: "destructive",
      });
    }
  };

  // State for CIP list (for merge dropdown)
  const [cipList, setCipList] = useState<{ primaryId: string; topicCount: number }[]>([]);
  
  // State for multi-CIP merge dialog
  const [mergeDialogOpen, setMergeDialogOpen] = useState(false);
  const [mergeSourceId, setMergeSourceId] = useState<string | null>(null);
  const [mergeSourceLabel, setMergeSourceLabel] = useState<string | null>(null);
  const [selectedMergeCips, setSelectedMergeCips] = useState<Set<string>>(new Set());
  const [mergeSearchQuery, setMergeSearchQuery] = useState('');
  const [isMerging, setIsMerging] = useState(false);
  
  // Fetch CIP list for merge dropdown
  const fetchCipList = async () => {
    try {
      const baseUrl = getDuckDBApiUrl();
      const response = await fetch(`${baseUrl}/api/governance-lifecycle/cip-list`);
      if (response.ok) {
        const { cips } = await response.json();
        setCipList(cips || []);
      }
    } catch (err) {
      console.error('Failed to fetch CIP list:', err);
    }
  };
  
  // Open the merge dialog for a specific item (id for API, label for display)
  const openMergeDialog = (sourceId: string, sourceLabel?: string) => {
    setMergeSourceId(sourceId);
    setMergeSourceLabel(sourceLabel || sourceId);
    setSelectedMergeCips(new Set());
    setMergeSearchQuery('');
    setMergeDialogOpen(true);
  };
  
  // Toggle a CIP in the selection
  const toggleMergeCip = (cipId: string) => {
    setSelectedMergeCips(prev => {
      const next = new Set(prev);
      if (next.has(cipId)) {
        next.delete(cipId);
      } else {
        next.add(cipId);
      }
      return next;
    });
  };
  
  // Handler to merge an item into multiple CIPs
  const handleMergeInto = async () => {
    if (!mergeSourceId || selectedMergeCips.size === 0) return;
    
    setIsMerging(true);
    try {
      const baseUrl = getDuckDBApiUrl();
      const response = await fetch(`${baseUrl}/api/governance-lifecycle/overrides/merge`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sourcePrimaryId: mergeSourceId,
          mergeInto: Array.from(selectedMergeCips),
          reason: `Manual merge via UI: ${mergeSourceLabel}`,
        }),
      });
      
      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(errorData.error || `Server error: ${response.status}`);
      }
      
      const targetDisplay = selectedMergeCips.size === 1 
        ? Array.from(selectedMergeCips)[0] 
        : `${selectedMergeCips.size} CIPs`;
      
      toast({
        title: "Merge saved",
        description: `Will be merged into ${targetDisplay}`,
      });
      
      setMergeDialogOpen(false);
      setMergeSourceId(null);
      setMergeSourceLabel(null);
      setSelectedMergeCips(new Set());
      
      // Refresh data to show the change
      fetchData(false);
    } catch (err) {
      console.error('Failed to merge:', err);
      toast({
        title: "Error",
        description: err instanceof Error ? err.message : "Failed to save merge override",
        variant: "destructive",
      });
    } finally {
      setIsMerging(false);
    }
  };
  
  // Filter CIP list for merge dialog
  const filteredMergeCips = useMemo(() => {
    if (!mergeSearchQuery.trim()) return cipList;
    const q = mergeSearchQuery.toLowerCase();
    return cipList.filter(cip => cip.primaryId.toLowerCase().includes(q));
  }, [cipList, mergeSearchQuery]);

  // State for card list (for "Move to card" dropdown)
  interface CardItem {
    id: string;
    primaryId: string;
    type: string;
    topicCount: number;
    preview: string;
  }
  const [cardList, setCardList] = useState<CardItem[]>([]);
  
  // State for move to card dialog
  const [moveDialogOpen, setMoveDialogOpen] = useState(false);
  const [moveSourceId, setMoveSourceId] = useState<string | null>(null);
  const [moveSourceLabel, setMoveSourceLabel] = useState<string | null>(null);
  const [selectedTargetCard, setSelectedTargetCard] = useState<string | null>(null);
  const [moveSearchQuery, setMoveSearchQuery] = useState('');
  const [isMoving, setIsMoving] = useState(false);
  
  // Fetch card list for "Move to card" dropdown
  const fetchCardList = async () => {
    try {
      const baseUrl = getDuckDBApiUrl();
      const response = await fetch(`${baseUrl}/api/governance-lifecycle/card-list`);
      if (response.ok) {
        const { cards } = await response.json();
        setCardList(cards || []);
      }
    } catch (err) {
      console.error('Failed to fetch card list:', err);
    }
  };
  
  // Open the move dialog for a specific topic
  const openMoveDialog = (topicId: string, topicLabel?: string) => {
    setMoveSourceId(topicId);
    setMoveSourceLabel(topicLabel || topicId);
    setSelectedTargetCard(null);
    setMoveSearchQuery('');
    setMoveDialogOpen(true);
    fetchCardList(); // Refresh card list when opening
  };
  
  // Handler to move a topic to a different card
  const handleMoveToCard = async () => {
    if (!moveSourceId || !selectedTargetCard) return;
    
    setIsMoving(true);
    try {
      const baseUrl = getDuckDBApiUrl();
      const response = await fetch(`${baseUrl}/api/governance-lifecycle/overrides/move-topic`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          topicId: moveSourceId,
          targetCardId: selectedTargetCard,
          reason: `Manual move via UI: ${moveSourceLabel}`,
        }),
      });
      
      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(errorData.error || `Server error: ${response.status}`);
      }
      
      const targetCard = cardList.find(c => c.id === selectedTargetCard || c.primaryId === selectedTargetCard);
      const targetName = targetCard?.primaryId || selectedTargetCard;
      
      toast({
        title: "Topic moved",
        description: `Will be moved to "${targetName}"`,
      });
      
      setMoveDialogOpen(false);
      setMoveSourceId(null);
      setMoveSourceLabel(null);
      setSelectedTargetCard(null);
      
      // Refresh data to show the change
      fetchData(false);
    } catch (err) {
      console.error('Failed to move topic:', err);
      toast({
        title: "Error",
        description: err instanceof Error ? err.message : "Failed to move topic",
        variant: "destructive",
      });
    } finally {
      setIsMoving(false);
    }
  };
  
  // Filter card list for move dialog
  const filteredCards = useMemo(() => {
    if (!moveSearchQuery.trim()) return cardList;
    const q = moveSearchQuery.toLowerCase();
    return cardList.filter(card => 
      card.primaryId.toLowerCase().includes(q) || 
      card.preview.toLowerCase().includes(q) ||
      card.type.toLowerCase().includes(q)
    );
  }, [cardList, moveSearchQuery]);

  // Bulk selection handlers
  const toggleItemSelection = (primaryId: string) => {
    setSelectedItems(prev => {
      const next = new Set(prev);
      if (next.has(primaryId)) {
        next.delete(primaryId);
      } else {
        next.add(primaryId);
      }
      return next;
    });
  };

  const selectAllVisible = () => {
    const allIds = groupedRegularItems.map(g => g.primaryId);
    setSelectedItems(new Set(allIds));
  };

  const clearSelection = () => {
    setSelectedItems(new Set());
  };

  // Bulk reclassify handler
  const handleBulkReclassify = async (newType: LifecycleItem['type']) => {
    if (selectedItems.size === 0) return;
    
    try {
      const baseUrl = getDuckDBApiUrl();
      const promises = Array.from(selectedItems).map(primaryId =>
        fetch(`${baseUrl}/api/governance-lifecycle/overrides`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            primaryId,
            type: newType,
            reason: 'Bulk reclassification via UI',
          }),
        })
      );
      
      await Promise.all(promises);
      
      toast({
        title: "Bulk reclassification complete",
        description: `${selectedItems.size} items updated to ${TYPE_CONFIG[newType].label}`,
      });
      
      clearSelection();
      setBulkSelectMode(false);
      fetchData(false);
    } catch (err) {
      console.error('Failed to bulk reclassify:', err);
      toast({
        title: "Error",
        description: "Failed to complete bulk reclassification",
        variant: "destructive",
      });
    }
  };

  useEffect(() => {
    fetchData();
    fetchCipList();
  }, []);

  // Fetch VoteRequests from local ACS
  useEffect(() => {
    const fetchVoteRequests = async () => {
      try {
        const baseUrl = getDuckDBApiUrl();
        const response = await fetch(`${baseUrl}/api/acs/contracts?template=VoteRequest&limit=100`);
        if (response.ok) {
          const result = await response.json();
          if (result.data) {
            setVoteRequests(result.data);
          }
        }
      } catch (err) {
        console.warn('Failed to fetch VoteRequests:', err);
      }
    };
    fetchVoteRequests();
  }, []);

  const toggleExpand = (id: string) => {
    setExpandedIds(prev => {
      const next = new Set(prev);
      if (next.has(id)) {
        next.delete(id);
      } else {
        next.add(id);
      }
      return next;
    });
  };

  const formatDate = (dateStr: string) => {
    try {
      return new Date(dateStr).toLocaleDateString('en-US', {
        year: 'numeric',
        month: 'short',
        day: 'numeric',
      });
    } catch {
      return dateStr;
    }
  };

  // Search matching function
  const matchesSearch = (item: LifecycleItem | Topic, query: string) => {
    if (!query.trim()) return true;
    const q = query.toLowerCase();
    
    if ('primaryId' in item) {
      // LifecycleItem
      const li = item as LifecycleItem;
      return (
        li.primaryId.toLowerCase().includes(q) ||
        li.topics.some(t => t.subject.toLowerCase().includes(q)) ||
        li.topics.some(t => t.identifiers.cipNumber?.toLowerCase().includes(q)) ||
        li.topics.some(t => t.identifiers.appName?.toLowerCase().includes(q)) ||
        li.topics.some(t => t.identifiers.validatorName?.toLowerCase().includes(q))
      );
    } else {
      // Topic
      const topic = item as Topic;
      return (
        topic.subject.toLowerCase().includes(q) ||
        topic.identifiers.cipNumber?.toLowerCase().includes(q) ||
        topic.identifiers.appName?.toLowerCase().includes(q) ||
        topic.identifiers.validatorName?.toLowerCase().includes(q) ||
        topic.excerpt?.toLowerCase().includes(q)
      );
    }
  };

  // Check if lifecycle item falls within date range
  const matchesDateRange = (item: LifecycleItem) => {
    if (!dateFrom && !dateTo) return true;
    const itemFirstDate = new Date(item.firstDate);
    const itemLastDate = new Date(item.lastDate);
    // Item matches if any part of its date range overlaps with the filter range
    if (dateFrom && itemLastDate < dateFrom) return false;
    if (dateTo && itemFirstDate > dateTo) return false;
    return true;
  };

  // Extract lifecycle key for VoteRequest lookup based on type
  const getLifecycleKey = (type: LifecycleItem['type'], primaryId: string, item: LifecycleItem): string | undefined => {
    if (type === 'cip') {
      const cipMatch = primaryId.match(/CIP[#\-\s]?0*(\d+)/i);
      return cipMatch ? `CIP-${cipMatch[1].padStart(4, '0')}` : undefined;
    }
    if (type === 'featured-app') {
      const appName = item.topics[0]?.identifiers?.appName;
      return appName?.toLowerCase() || primaryId.toLowerCase();
    }
    if (type === 'validator') {
      const validatorName = item.topics[0]?.identifiers?.validatorName;
      return validatorName?.toLowerCase() || primaryId.toLowerCase();
    }
    return primaryId.toLowerCase();
  };

  // Separate CIP-00XX items from regular items
  const { tbdItems, regularItems } = useMemo(() => {
    if (!data) return { tbdItems: [], regularItems: [] };
    const allFiltered = data.lifecycleItems.filter(item => {
      if (typeFilter !== 'all' && item.type !== typeFilter) return false;

      if (stageFilter !== 'all') {
        const isVoteStageFilter = stageFilter === 'sv-onchain-vote' || stageFilter === 'sv-milestone';

        if (!isVoteStageFilter) {
          if (item.currentStage !== stageFilter) return false;
        } else {
          // For vote-stage filters, don't rely on `currentStage` (items rarely "end" on sv-milestone).
          // Instead include any lifecycle item that has votes in that stage.
          const lifecycleKey = getLifecycleKey(item.type, item.primaryId, item);
          const matchingVotes = lifecycleKey ? combinedVoteMap.get(lifecycleKey.toLowerCase()) || [] : [];
          const hasVotesInStage = matchingVotes.some(v => v.stage === stageFilter);
          const hasTopicsInStage = (item.stages[stageFilter]?.length || 0) > 0;
          if (!(hasVotesInStage || hasTopicsInStage || item.currentStage === stageFilter)) return false;
        }
      }

      if (!matchesSearch(item, searchQuery)) return false;
      if (!matchesDateRange(item)) return false;
      return true;
    });

    return {
      tbdItems: allFiltered.filter(item => item.primaryId?.includes('00XX')),
      regularItems: allFiltered.filter(item => !item.primaryId?.includes('00XX')),
    };
  }, [data, typeFilter, stageFilter, searchQuery, dateFrom, dateTo, combinedVoteMap]);

  const filteredItems = useMemo(() => {
    return [...tbdItems, ...regularItems];
  }, [tbdItems, regularItems]);

  // Group regularItems by primaryId (combine testnet/mainnet entries)
  interface GroupedItem {
    primaryId: string;
    type: LifecycleItem['type'];
    items: LifecycleItem[];
    hasMultipleNetworks: boolean;
    firstDate: string;
    lastDate: string;
    totalTopics: number;
  }

  const groupedRegularItems = useMemo(() => {
    const groups = new Map<string, LifecycleItem[]>();
    
    regularItems.forEach(item => {
      const key = item.primaryId.toLowerCase();
      if (!groups.has(key)) {
        groups.set(key, []);
      }
      groups.get(key)!.push(item);
    });

    const result: GroupedItem[] = [];
    groups.forEach((items, key) => {
      // Sort items: mainnet first, then testnet
      items.sort((a, b) => {
        if (a.network === 'mainnet' && b.network !== 'mainnet') return -1;
        if (a.network !== 'mainnet' && b.network === 'mainnet') return 1;
        return 0;
      });

      const allDates = items.flatMap(i => [new Date(i.firstDate), new Date(i.lastDate)]);
      const firstDate = new Date(Math.min(...allDates.map(d => d.getTime()))).toISOString();
      const lastDate = new Date(Math.max(...allDates.map(d => d.getTime()))).toISOString();
      
      result.push({
        primaryId: items[0].primaryId, // Use original casing from first item
        type: items[0].type,
        items,
        hasMultipleNetworks: items.length > 1 && items.some(i => i.network === 'testnet') && items.some(i => i.network === 'mainnet'),
        firstDate,
        lastDate,
        totalTopics: items.reduce((sum, i) => sum + i.topics.length, 0),
      });
    });

    // Sort all items by lastDate descending (most recent first)
    return result.sort((a, b) => {
      return new Date(b.lastDate).getTime() - new Date(a.lastDate).getTime();
    });
  }, [regularItems]);

  // Determine the lifecycle type for a topic - should match backend logic
  const getTopicType = (topic: Topic): string => {
    const subjectTrimmed = topic.subject.trim();
    const isOutcome = /\bTokenomics\s+Outcomes\b/i.test(subjectTrimmed);
    const isProtocolUpgrade = /\b(?:synchronizer\s+migration|splice\s+\d+\.\d+|protocol\s+upgrade|network\s+upgrade|hard\s*fork|migration\s+to\s+splice)\b/i.test(subjectTrimmed);
    const isVoteProposal = /\bVote\s+Proposal\b/i.test(subjectTrimmed);
    const isValidatorOperations = /\bValidator\s+Operations\b/i.test(subjectTrimmed);
    
    // Specific vote proposal type detection
    const isCipVoteProposal = isVoteProposal && (
      /CIP[#\-\s]?\d+/i.test(subjectTrimmed) || 
      /\bCIP\s+(?:vote|voting|approval)\b/i.test(subjectTrimmed)
    );
    const isFeaturedAppVoteProposal = isVoteProposal && (
      /featured\s*app|featured\s*application|app\s+rights/i.test(subjectTrimmed) ||
      /(?:mainnet|testnet|main\s*net|test\s*net):/i.test(subjectTrimmed)
    );
    const isValidatorVoteProposal = isVoteProposal && (
      /validator\s+(?:operator|onboarding|license)/i.test(subjectTrimmed)
    );
    
    if (isOutcome) return 'outcome';
    if (isProtocolUpgrade) return 'protocol-upgrade';
    if (topic.flow === 'cip') return 'cip';
    if (topic.flow === 'featured-app') return 'featured-app';
    if (topic.flow === 'shared') {
      // Shared groups need subject-line disambiguation (matching backend logic)
      if (isCipVoteProposal || topic.identifiers.cipNumber || topic.identifiers.isCipDiscussion) {
        return 'cip';
      }
      if (isValidatorVoteProposal || isValidatorOperations || topic.identifiers.validatorName) {
        return 'validator';
      }
      if (isFeaturedAppVoteProposal || topic.identifiers.appName) {
        return 'featured-app';
      }
      if (isVoteProposal) {
        // Generic vote proposal - check for network prefix
        if (/(?:mainnet|testnet|main\s*net|test\s*net):/i.test(subjectTrimmed)) {
          return 'featured-app';
        }
        return 'featured-app';
      }
      return 'featured-app'; // Default for shared
    }
    return 'featured-app'; // Fallback
  };

  const filteredTopics = useMemo(() => {
    if (!data) return [];
    return data.allTopics.filter(topic => {
      if (typeFilter !== 'all') {
        const itemType = getTopicType(topic);
        if (itemType !== typeFilter) return false;
      }
      if (stageFilter !== 'all' && topic.stage !== stageFilter) return false;
      if (!matchesSearch(topic, searchQuery)) return false;
      
      // Date range filtering
      const topicDate = new Date(topic.date);
      if (dateFrom && topicDate < dateFrom) return false;
      if (dateTo && topicDate > dateTo) return false;
      
      return true;
    }).sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime());
  }, [data, typeFilter, stageFilter, searchQuery, dateFrom, dateTo]);

  // Handle date preset changes
  const handleDatePreset = (preset: string) => {
    setDatePreset(preset);
    const now = new Date();
    
    switch (preset) {
      case 'all':
        setDateFrom(undefined);
        setDateTo(undefined);
        break;
      case 'this-month':
        setDateFrom(startOfMonth(now));
        setDateTo(endOfMonth(now));
        break;
      case 'last-month':
        setDateFrom(startOfMonth(subMonths(now, 1)));
        setDateTo(endOfMonth(subMonths(now, 1)));
        break;
      case 'last-3-months':
        setDateFrom(startOfMonth(subMonths(now, 2)));
        setDateTo(now);
        break;
      case 'last-6-months':
        setDateFrom(startOfMonth(subMonths(now, 5)));
        setDateTo(now);
        break;
      case 'this-year':
        setDateFrom(startOfYear(now));
        setDateTo(endOfYear(now));
        break;
      case 'last-year':
        setDateFrom(startOfYear(subYears(now, 1)));
        setDateTo(endOfYear(subYears(now, 1)));
        break;
      case 'custom':
        // Keep current dates, user will pick manually
        break;
    }
  };

  const clearDateFilter = () => {
    setDateFrom(undefined);
    setDateTo(undefined);
    setDatePreset('all');
  };

  // Timeline data - group events by month
  // Create timeline entries from vote requests
  type TimelineEntry = 
    | { type: 'topic'; data: Topic; date: Date }
    | { type: 'vote-started'; data: VoteRequest; date: Date; cipRef: string | null }
    | { type: 'vote-ended'; data: VoteRequest; date: Date; cipRef: string | null; status: 'passed' | 'failed' | 'expired' };

  const timelineData = useMemo(() => {
    const entries: TimelineEntry[] = [];
    
    // Add topics as entries
    filteredTopics.forEach(topic => {
      entries.push({ type: 'topic', data: topic, date: new Date(topic.date) });
    });
    
    // Add vote requests as timeline entries
    voteRequests.forEach(vr => {
      const cipRef = extractCipReference(vr);
      const recordTime = vr.record_time ? new Date(vr.record_time) : null;
      const voteBefore = vr.payload?.voteBefore ? new Date(vr.payload.voteBefore) : null;
      
      // "Vote Started" entry - use record_time if available
      if (recordTime) {
        entries.push({ type: 'vote-started', data: vr, date: recordTime, cipRef });
      }
      
      // "Vote Ended" entry - only if vote deadline has passed
      if (voteBefore && voteBefore < new Date()) {
        // Calculate vote result
        const votesRaw = vr.payload?.votes || [];
        let votesFor = 0;
        for (const vote of votesRaw) {
          const [, voteData] = Array.isArray(vote) ? vote : [vote.sv || "Unknown", vote];
          const isAccept = voteData?.accept === true || voteData?.Accept === true;
          if (isAccept) votesFor++;
        }
        const threshold = 10; // Assuming threshold
        const status: 'passed' | 'failed' | 'expired' = votesFor >= threshold ? 'passed' : votesRaw.length > 0 ? 'failed' : 'expired';
        entries.push({ type: 'vote-ended', data: vr, date: voteBefore, cipRef, status });
      }
    });
    
    if (entries.length === 0) return [];
    
    // Group by month
    const monthGroups: Record<string, TimelineEntry[]> = {};
    entries.forEach(entry => {
      const monthKey = `${entry.date.getFullYear()}-${String(entry.date.getMonth() + 1).padStart(2, '0')}`;
      if (!monthGroups[monthKey]) {
        monthGroups[monthKey] = [];
      }
      monthGroups[monthKey].push(entry);
    });
    
    return Object.entries(monthGroups)
      .sort((a, b) => b[0].localeCompare(a[0]))
      .map(([month, monthEntries]) => {
        const [year, monthNum] = month.split('-').map(Number);
        const monthDate = new Date(year, monthNum - 1, 1);
        return {
          month,
          label: monthDate.toLocaleDateString('en-US', { year: 'numeric', month: 'long' }),
          entries: monthEntries.sort((a, b) => b.date.getTime() - a.date.getTime()),
          topicCount: monthEntries.filter(e => e.type === 'topic').length,
          voteCount: monthEntries.filter(e => e.type === 'vote-started' || e.type === 'vote-ended').length,
        };
      });
  }, [filteredTopics, voteRequests]);

  // Helper to render unified OnChainVoteItem card with correct links
  const renderOnChainVoteCard = (item: OnChainVoteItem) => {
    // Determine the link based on source
    // ACS (in-progress) → /governance?tab=active
    // History (completed) → /governance?tab=scanapi (Scan API History tab)
    const proposalParam =
      item.source === "history"
        ? item.historicalVote?.trackingCid?.slice(0, 12) || item.id
        : item.id;

    const linkUrl =
      item.source === "acs"
        ? `/governance?tab=active&proposal=${proposalParam}`
        : `/governance?tab=scanapi&proposal=${proposalParam}`;

    const isExpired = item.voteBefore && item.voteBefore < new Date();
    
    return (
      <a
        key={`${item.source}-${item.id}`}
        href={linkUrl}
        className="block p-3 rounded-lg bg-pink-500/10 border border-pink-500/30 hover:border-pink-500/50 transition-colors"
      >
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-2 flex-wrap">
              <Badge 
                variant="outline" 
                className={cn(
                  "text-[10px] h-5",
                  item.status === 'approved' ? 'border-green-500/50 text-green-400 bg-green-500/10' :
                  item.status === 'rejected' || item.status === 'expired' ? 'border-red-500/50 text-red-400 bg-red-500/10' :
                  'border-yellow-500/50 text-yellow-400 bg-yellow-500/10'
                )}
              >
                {item.status === 'approved' ? '✓ Approved' : 
                 item.status === 'rejected' ? '✗ Rejected' : 
                 item.status === 'expired' ? '✗ Expired' : 
                 '⏳ In Progress'}
              </Badge>
              <Badge variant="outline" className={cn(
                "text-[10px] h-5",
                item.source === 'acs' 
                  ? 'border-blue-500/50 text-blue-400 bg-blue-500/10'
                  : 'border-purple-500/50 text-purple-400 bg-purple-500/10'
              )}>
                {item.source === 'acs' ? 'Active (ACS)' : 'Scan API History'}
              </Badge>
            </div>
            <div className="flex items-center gap-3 mt-2 text-xs text-muted-foreground">
              <span className="flex items-center gap-1">
                <Vote className="h-3 w-3" />
                {item.votesFor} for / {item.votesAgainst} against ({item.totalVotes} total)
              </span>
              {item.voteBefore && (
                <span className="flex items-center gap-1">
                  <Clock className="h-3 w-3" />
                  {isExpired ? 'Ended' : `Due ${format(item.voteBefore, 'MMM d, yyyy')}`}
                </span>
              )}
            </div>
            {/* Reason section */}
            {(item.reasonBody || item.reasonUrl) && (
              <div className="mt-2 p-2 rounded bg-background/30 border border-border/30">
                {item.reasonBody && (
                  <p className="text-xs text-muted-foreground break-words whitespace-pre-wrap mb-1">
                    {item.reasonBody}
                  </p>
                )}
                {item.reasonUrl && (
                  <a 
                    href={item.reasonUrl} 
                    target="_blank" 
                    rel="noopener noreferrer"
                    onClick={(e) => e.stopPropagation()}
                    className="text-xs text-primary hover:underline break-all"
                  >
                    {item.reasonUrl}
                  </a>
                )}
              </div>
            )}
          </div>
          <div className="h-7 w-7 p-0 shrink-0 flex items-center justify-center text-muted-foreground">
            <ExternalLink className="h-3.5 w-3.5" />
          </div>
        </div>
      </a>
    );
  };

  // Legacy: Helper to render VoteRequest card (for backwards compatibility in timeline)
  const renderVoteRequestCard = (vr: VoteRequest) => {
    const payload = vr.payload;
    const voteBefore = payload?.voteBefore ? new Date(payload.voteBefore) : null;
    const isExpired = voteBefore && voteBefore < new Date();
    const votesRaw = payload?.votes || [];
    const reason = payload?.reason;
    
    let votesFor = 0;
    let votesAgainst = 0;
    for (const vote of votesRaw) {
      const [, voteData] = Array.isArray(vote) ? vote : [vote.sv || "Unknown", vote];
      const isAccept = voteData?.accept === true || voteData?.Accept === true;
      const isReject = voteData?.accept === false || voteData?.reject === true || voteData?.Reject === true;
      if (isAccept) votesFor++;
      else if (isReject) votesAgainst++;
    }
    const totalVotes = votesRaw.length;
    
    const proposalId = payload?.trackingCid?.slice(0, 12) || vr.contract_id?.slice(0, 12) || 'unknown';
    
    const threshold = 10;
    let status: 'pending' | 'approved' | 'rejected' = 'pending';
    if (votesFor >= threshold) status = 'approved';
    else if (isExpired && votesFor < threshold) status = 'rejected';
    
    // ACS votes link to Active (ACS) tab in Governance page
    const linkUrl = `/governance?tab=active&proposal=${proposalId}`;

    return (
      <a
        key={vr.contract_id}
        href={linkUrl}
        className="block p-3 rounded-lg bg-pink-500/10 border border-pink-500/30 hover:border-pink-500/50 transition-colors"
      >
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-2 flex-wrap">
              <Badge 
                variant="outline" 
                className={cn(
                  "text-[10px] h-5",
                  status === 'approved' ? 'border-green-500/50 text-green-400 bg-green-500/10' :
                  status === 'rejected' ? 'border-red-500/50 text-red-400 bg-red-500/10' :
                  'border-yellow-500/50 text-yellow-400 bg-yellow-500/10'
                )}
              >
                {status === 'approved' ? '✓ Approved' : status === 'rejected' ? '✗ Rejected' : '⏳ In Progress'}
              </Badge>
              <Badge variant="outline" className="text-[10px] h-5 border-blue-500/50 text-blue-400 bg-blue-500/10">
                Active (ACS)
              </Badge>
            </div>
            <div className="flex items-center gap-3 mt-2 text-xs text-muted-foreground">
              <span className="flex items-center gap-1">
                <Vote className="h-3 w-3" />
                {votesFor} for / {votesAgainst} against ({totalVotes} total)
              </span>
              {voteBefore && (
                <span className="flex items-center gap-1">
                  <Clock className="h-3 w-3" />
                  {isExpired ? 'Expired' : `Due ${format(voteBefore, 'MMM d, yyyy')}`}
                </span>
              )}
            </div>
            {(reason?.body || reason?.url) && (
              <div className="mt-2 p-2 rounded bg-background/30 border border-border/30">
                {reason?.body && (
                  <p className="text-xs text-muted-foreground break-words whitespace-pre-wrap mb-1">
                    {reason.body}
                  </p>
                )}
                {reason?.url && (
                  <a 
                    href={reason.url} 
                    target="_blank" 
                    rel="noopener noreferrer"
                    onClick={(e) => e.stopPropagation()}
                    className="text-xs text-primary hover:underline break-all"
                  >
                    {reason.url}
                  </a>
                )}
              </div>
            )}
          </div>
          <div className="h-7 w-7 p-0 shrink-0 flex items-center justify-center text-muted-foreground">
            <ExternalLink className="h-3.5 w-3.5" />
          </div>
        </div>
      </a>
    );
  };

  const renderLifecycleProgress = (item: LifecycleItem, lifecycleKey?: string) => {
    // Get the stages specific to this item's type
    const stages = WORKFLOW_STAGES[item.type] || WORKFLOW_STAGES.other;
    const currentIdx = stages.indexOf(item.currentStage);
    
    // Check combined votes (ACS + historical) for this lifecycle item
    const matchingVotes = lifecycleKey ? combinedVoteMap.get(lifecycleKey.toLowerCase()) || [] : [];
    
    return (
      <div className="flex items-center gap-1 flex-wrap">
        {stages.map((stage, idx) => {
          // For vote stages, check combined votes filtered by stage type
          const isVoteStage = stage === 'sv-onchain-vote' || stage === 'sv-milestone';
          const stageVotes = isVoteStage 
            ? matchingVotes.filter(v => v.stage === stage) 
            : [];
          const hasStage = isVoteStage
            ? stageVotes.length > 0
            : item.stages[stage] && item.stages[stage].length > 0;
          const isCurrent = stage === item.currentStage;
          const isPast = idx < currentIdx;
          const config = STAGE_CONFIG[stage];
          if (!config) return null;
          const Icon = config.icon;
          
          // Count for tooltip
          const count = isVoteStage
            ? stageVotes.length 
            : item.stages[stage]?.length || 0;
          
          return (
            <div key={stage} className="flex items-center">
              <div
                className={`flex items-center gap-1.5 px-2 py-1 rounded-md text-xs font-medium transition-all ${
                  hasStage 
                    ? config.color + ' border'
                    : 'bg-muted/30 text-muted-foreground/50 border border-transparent'
                } ${isCurrent ? 'ring-1 ring-offset-1 ring-offset-background ring-primary/50' : ''}`}
                title={`${config.label}: ${hasStage ? count + (isVoteStage ? ' vote(s)' : ' topics') : 'No activity'}`}
              >
                <Icon className="h-3 w-3" />
                <span className="hidden sm:inline">{config.label}</span>
                {hasStage && <span className="text-[10px] opacity-70">({count})</span>}
              </div>
              {idx < stages.length - 1 && (
                <ArrowRight className={`h-3 w-3 mx-0.5 ${isPast || isCurrent ? 'text-primary/50' : 'text-muted-foreground/30'}`} />
              )}
            </div>
          );
        })}
      </div>
    );
  };

  const renderTopicCard = (topic: Topic, showGroup = true, parentType?: LifecycleItem['type']) => {
    return (
      <div 
        key={topic.id}
        className={cn(
          "block p-3 rounded-lg bg-muted/30 border border-border/50 hover:border-primary/30 transition-colors",
          topic.sourceUrl ? "cursor-pointer hover:bg-muted/50" : "cursor-default"
        )}
      >
        <div className="flex items-start justify-between gap-3">
          <a 
            href={topic.sourceUrl || '#'}
            target="_blank"
            rel="noopener noreferrer"
            className="min-w-0 flex-1"
            onClick={(e) => !topic.sourceUrl && e.preventDefault()}
          >
            <h4 className="font-medium text-sm break-words">{topic.subject}</h4>
            <div className="flex flex-wrap items-center gap-2 mt-1 text-xs text-muted-foreground">
              <span className="flex items-center gap-1">
                <Calendar className="h-3 w-3" />
                {formatDate(topic.date)}
              </span>
              {showGroup && (
                <Badge variant="outline" className="text-[10px] h-5">
                  {topic.groupLabel}
                </Badge>
              )}
              {topic.messageCount && topic.messageCount > 1 && (
                <span>{topic.messageCount} msgs</span>
              )}
            </div>
          </a>
          
          <div className="flex items-center gap-1 shrink-0">
            {/* Topic actions dropdown */}
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button
                  variant="ghost"
                  size="icon"
                  className="h-7 w-7"
                  onClick={(e) => e.stopPropagation()}
                >
                  <MoreVertical className="h-3.5 w-3.5" />
                </Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end" className="bg-popover w-56">
                {/* Reclassify submenu */}
                <DropdownMenuSub>
                  <DropdownMenuSubTrigger className="text-xs">
                    <Edit2 className="mr-2 h-3 w-3" />
                    Reclassify as...
                  </DropdownMenuSubTrigger>
                  <DropdownMenuSubContent className="bg-popover">
                    {Object.entries(TYPE_CONFIG).map(([typeKey, config]) => (
                      <DropdownMenuItem
                        key={typeKey}
                        disabled={typeKey === parentType}
                        onClick={(e) => {
                          e.stopPropagation();
                          handleReclassifyTopic(topic.id, topic.subject, typeKey as LifecycleItem['type']);
                        }}
                        className="text-xs"
                      >
                        <Badge className={cn("mr-2 text-[10px]", config.color)}>
                          {config.label}
                        </Badge>
                        {typeKey === parentType && <span className="text-muted-foreground">(current)</span>}
                      </DropdownMenuItem>
                    ))}
                  </DropdownMenuSubContent>
                </DropdownMenuSub>
                
                {/* Extract to own card option */}
                <DropdownMenuSeparator />
                <DropdownMenuItem
                  onClick={(e) => {
                    e.stopPropagation();
                    handleExtractTopic(topic.id, topic.subject);
                  }}
                  className="text-xs"
                >
                  <SplitSquareVertical className="mr-2 h-3 w-3" />
                  Extract to own card
                </DropdownMenuItem>
                
                {/* Merge into CIP(s) option */}
                {cipList.length > 0 && (
                  <DropdownMenuItem
                    onClick={(e) => {
                      e.stopPropagation();
                      e.preventDefault();
                      openMergeDialog(topic.id, topic.subject);
                    }}
                    className="text-xs"
                  >
                    <Merge className="mr-2 h-3 w-3" />
                    Merge into CIP(s)...
                  </DropdownMenuItem>
                )}
                
                {/* Move to card option */}
                <DropdownMenuItem
                  onClick={(e) => {
                    e.stopPropagation();
                    e.preventDefault();
                    openMoveDialog(topic.id, topic.subject);
                  }}
                  className="text-xs"
                >
                  <MoveRight className="mr-2 h-3 w-3" />
                  Move to card...
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
            
            {topic.sourceUrl && (
              <a 
                href={topic.sourceUrl}
                target="_blank"
                rel="noopener noreferrer"
                className="h-7 w-7 p-0 flex items-center justify-center text-muted-foreground hover:text-foreground"
              >
                <ExternalLink className="h-3.5 w-3.5" />
              </a>
            )}
          </div>
        </div>
        {topic.excerpt && (
          <p className="text-xs text-muted-foreground mt-2 break-words whitespace-pre-wrap">
            {topic.excerpt}
          </p>
        )}
      </div>
    );
  };

  return (
    <DashboardLayout>
      <div className="space-y-6">
        {/* Header */}
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
          <div>
            <h1 className="text-3xl font-bold">Governance Lifecycle</h1>
            <p className="text-muted-foreground mt-1">
              Track CIPs, Featured Apps, and Validators through the governance process
            </p>
            {cachedAt && (
              <p className="text-xs text-muted-foreground mt-1">
                Last updated: {new Date(cachedAt).toLocaleString()}
              </p>
            )}
          </div>
          <div className="flex gap-2 shrink-0">
            <Button 
              onClick={() => fetchData(false)} 
              disabled={isLoading || isRefreshing}
              variant="outline"
              className="gap-2"
            >
              <RefreshCw className={`h-4 w-4 ${isLoading ? "animate-spin" : ""}`} />
              Load Cached
            </Button>
            <Button 
              onClick={() => fetchData(true)} 
              disabled={isLoading || isRefreshing}
              className="gap-2"
            >
              <RefreshCw className={`h-4 w-4 ${isRefreshing ? "animate-spin" : ""}`} />
              Refresh from Groups.io
            </Button>
          </div>
        </div>


        {/* Group Stats */}
        {data && Object.keys(data.stats.groupCounts).length > 0 && (
          <Card className="bg-muted/20">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium">Topics by Group</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="flex flex-wrap gap-2">
                {Object.entries(data.stats.groupCounts).map(([group, count]) => (
                  <Badge key={group} variant="secondary" className="text-xs">
                    {group}: {count}
                  </Badge>
                ))}
              </div>
            </CardContent>
          </Card>
        )}

        {/* Stale Cache Warning */}
        {data?.stale && (
          <Card className="border-yellow-500/50 bg-yellow-500/10">
            <CardContent className="flex items-center gap-3 py-4">
              <AlertCircle className="h-5 w-5 text-yellow-500" />
              <span className="text-yellow-500">Showing cached data (refresh failed). Click "Refresh from Groups.io" to try again.</span>
            </CardContent>
          </Card>
        )}

        {/* Error State */}
        {error && (
          <Card className="border-destructive/50 bg-destructive/10">
            <CardContent className="flex items-center gap-3 py-4">
              <AlertCircle className="h-5 w-5 text-destructive" />
              <span className="text-destructive">{error}</span>
            </CardContent>
          </Card>
        )}

        {/* Search Bar */}
        <div className="relative">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
          <Input
            placeholder="Search by subject, CIP number, app name, or validator..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="pl-10 pr-10"
          />
          {searchQuery && (
            <Button
              variant="ghost"
              size="sm"
              onClick={() => setSearchQuery('')}
              className="absolute right-1 top-1/2 -translate-y-1/2 h-7 w-7 p-0"
            >
              <X className="h-4 w-4" />
            </Button>
          )}
        </div>

        {/* Filters */}
        <div className="flex flex-wrap items-center gap-3">
          <div className="flex items-center gap-2">
            <Filter className="h-4 w-4 text-muted-foreground" />
            <span className="text-sm text-muted-foreground">Type:</span>
            <div className="flex gap-1">
              {['all', 'cip', 'featured-app', 'validator', 'protocol-upgrade', 'outcome', 'other'].map(type => (
                <Button
                  key={type}
                  variant={typeFilter === type ? 'default' : 'outline'}
                  size="sm"
                  onClick={() => {
                    setTypeFilter(type);
                    // Reset stage filter when changing type (stages differ per type)
                    setStageFilter('all');
                  }}
                  className="h-7 text-xs"
                >
                  {type === 'all' ? 'All' : TYPE_CONFIG[type as keyof typeof TYPE_CONFIG]?.label || type}
                </Button>
              ))}
            </div>
          </div>
          <div className="flex items-center gap-2">
            <span className="text-sm text-muted-foreground">Stage:</span>
            <div className="flex gap-1">
              {/* Show only stages relevant to the selected type */}
              {(() => {
                const stagesToShow = typeFilter === 'all' 
                  ? [...new Set(Object.values(WORKFLOW_STAGES).flat())]
                  : WORKFLOW_STAGES[typeFilter as keyof typeof WORKFLOW_STAGES] || [];
                return ['all', ...stagesToShow].map(stage => (
                  <Button
                    key={stage}
                    variant={stageFilter === stage ? 'default' : 'outline'}
                    size="sm"
                    onClick={() => setStageFilter(stage)}
                    className="h-7 text-xs"
                  >
                    {stage === 'all' ? 'All' : STAGE_CONFIG[stage]?.label || stage}
                  </Button>
                ));
              })()}
            </div>
          </div>
        </div>

        {/* Date Range Filter - applies to all views */}
        <div className="flex flex-wrap items-center gap-3">
          <div className="flex items-center gap-2">
            <CalendarIcon className="h-4 w-4 text-muted-foreground" />
            <span className="text-sm font-medium">Date Range:</span>
          </div>
          
          {/* Preset Selector */}
          <Select value={datePreset} onValueChange={handleDatePreset}>
            <SelectTrigger className="w-[160px] h-8">
              <SelectValue placeholder="Select period" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Time</SelectItem>
              <SelectItem value="this-month">This Month</SelectItem>
              <SelectItem value="last-month">Last Month</SelectItem>
              <SelectItem value="last-3-months">Last 3 Months</SelectItem>
              <SelectItem value="last-6-months">Last 6 Months</SelectItem>
              <SelectItem value="this-year">This Year</SelectItem>
              <SelectItem value="last-year">Last Year</SelectItem>
              <SelectItem value="custom">Custom Range</SelectItem>
            </SelectContent>
          </Select>

          {/* Custom Date Pickers */}
          {datePreset === 'custom' && (
            <>
              <Popover>
                <PopoverTrigger asChild>
                  <Button
                    variant="outline"
                    size="sm"
                    className={cn(
                      "h-8 justify-start text-left font-normal",
                      !dateFrom && "text-muted-foreground"
                    )}
                  >
                    <CalendarIcon className="mr-2 h-4 w-4" />
                    {dateFrom ? format(dateFrom, "MMM d, yyyy") : "From"}
                  </Button>
                </PopoverTrigger>
                <PopoverContent className="w-auto p-0" align="start">
                  <CalendarComponent
                    mode="single"
                    selected={dateFrom}
                    onSelect={setDateFrom}
                    initialFocus
                    className={cn("p-3 pointer-events-auto")}
                  />
                </PopoverContent>
              </Popover>
              <span className="text-muted-foreground">to</span>
              <Popover>
                <PopoverTrigger asChild>
                  <Button
                    variant="outline"
                    size="sm"
                    className={cn(
                      "h-8 justify-start text-left font-normal",
                      !dateTo && "text-muted-foreground"
                    )}
                  >
                    <CalendarIcon className="mr-2 h-4 w-4" />
                    {dateTo ? format(dateTo, "MMM d, yyyy") : "To"}
                  </Button>
                </PopoverTrigger>
                <PopoverContent className="w-auto p-0" align="start">
                  <CalendarComponent
                    mode="single"
                    selected={dateTo}
                    onSelect={setDateTo}
                    initialFocus
                    className={cn("p-3 pointer-events-auto")}
                  />
                </PopoverContent>
              </Popover>
            </>
          )}

          {/* Clear Button */}
          {(dateFrom || dateTo) && (
            <Button
              variant="ghost"
              size="sm"
              onClick={clearDateFilter}
              className="h-8 px-2"
            >
              <X className="h-4 w-4 mr-1" />
              Clear
            </Button>
          )}

          {/* Show current range */}
          {datePreset !== 'all' && datePreset !== 'custom' && (
            <Badge variant="secondary" className="text-xs">
              {dateFrom && format(dateFrom, "MMM d, yyyy")} - {dateTo && format(dateTo, "MMM d, yyyy")}
            </Badge>
          )}
        </div>

        {/* Bulk Selection Toggle & Action Bar */}
        <div className="flex items-center gap-3 flex-wrap">
          <Button
            variant={bulkSelectMode ? "default" : "outline"}
            size="sm"
            onClick={() => {
              setBulkSelectMode(!bulkSelectMode);
              if (bulkSelectMode) clearSelection();
            }}
            className="gap-2"
          >
            {bulkSelectMode ? <CheckSquare className="h-4 w-4" /> : <Square className="h-4 w-4" />}
            {bulkSelectMode ? "Exit Bulk Select" : "Bulk Select"}
          </Button>
          
          {bulkSelectMode && (
            <>
              <Button variant="outline" size="sm" onClick={selectAllVisible}>
                Select All ({groupedRegularItems.length})
              </Button>
              <Button variant="outline" size="sm" onClick={clearSelection} disabled={selectedItems.size === 0}>
                Clear ({selectedItems.size})
              </Button>
              
              {selectedItems.size > 0 && (
                <DropdownMenu>
                  <DropdownMenuTrigger asChild>
                    <Button size="sm" className="gap-2">
                      <Edit2 className="h-4 w-4" />
                      Reclassify {selectedItems.size} items
                    </Button>
                  </DropdownMenuTrigger>
                  <DropdownMenuContent align="start" className="bg-popover">
                    <DropdownMenuLabel className="text-xs">Reclassify selected as...</DropdownMenuLabel>
                    <DropdownMenuSeparator />
                    {Object.entries(TYPE_CONFIG).map(([typeKey, config]) => (
                      <DropdownMenuItem
                        key={typeKey}
                        onClick={() => handleBulkReclassify(typeKey as LifecycleItem['type'])}
                        className="text-xs"
                      >
                        <Badge className={cn("mr-2 text-[10px]", config.color)}>
                          {config.label}
                        </Badge>
                      </DropdownMenuItem>
                    ))}
                  </DropdownMenuContent>
                </DropdownMenu>
              )}
            </>
          )}
        </div>

        {/* View Toggle */}
        <Tabs value={viewMode} onValueChange={(v) => setViewMode(v as 'lifecycle' | 'all' | 'timeline' | 'learn')}>
          <TabsList>
            <TabsTrigger value="lifecycle">Lifecycle ({groupedRegularItems.length + tbdItems.length})</TabsTrigger>
            <TabsTrigger value="timeline">Timeline ({timelineData.length} months)</TabsTrigger>
            <TabsTrigger value="all">All Topics ({filteredTopics.length})</TabsTrigger>
            <TabsTrigger value="learn" className="gap-1">
              <Lightbulb className="h-3 w-3" />
              Learn
            </TabsTrigger>
          </TabsList>

          {/* Lifecycle View - Grouped by CIP/App/Validator */}
          <TabsContent value="lifecycle" className="space-y-3 mt-4">
            {isLoading ? (
              Array.from({ length: 5 }).map((_, i) => (
                <Card key={i}>
                  <CardHeader>
                    <Skeleton className="h-6 w-3/4" />
                    <Skeleton className="h-4 w-1/2 mt-2" />
                  </CardHeader>
                </Card>
              ))
            ) : (groupedRegularItems.length + tbdItems.length) === 0 ? (
              <Card>
                <CardContent className="flex flex-col items-center justify-center py-12">
                  <FileText className="h-12 w-12 text-muted-foreground mb-4" />
                  <h3 className="text-lg font-medium">No Items Found</h3>
                  <p className="text-muted-foreground text-sm mt-1">
                    Try adjusting your filters
                  </p>
                </CardContent>
              </Card>
            ) : (
              <ScrollArea className="h-[calc(100vh-500px)] pr-4">
                <div className="space-y-3">
                  {/* Pending CIP Section */}
                  {tbdItems.length > 0 && (
                    <Card className="border-amber-500/30 bg-amber-500/5">
                      <CardHeader 
                        className="pb-3 cursor-pointer" 
                        onClick={() => toggleExpand('cip-00xx-section')}
                      >
                        <div className="flex items-center justify-between">
                          <div className="space-y-1">
                            <div className="flex items-center gap-2">
                              <Badge className="bg-amber-500/20 text-amber-400 border border-amber-500/30">
                                CIP-00XX
                              </Badge>
                              <span className="text-sm text-muted-foreground">
                                {tbdItems.reduce((sum, item) => sum + item.topics.length, 0)} topics
                              </span>
                            </div>
                            <CardTitle className="text-base">Pending CIP Number Assignment</CardTitle>
                          </div>
                          {expandedIds.has('cip-00xx-section') ? (
                            <ChevronUp className="h-5 w-5 text-muted-foreground" />
                          ) : (
                            <ChevronDown className="h-5 w-5 text-muted-foreground" />
                          )}
                        </div>
                      </CardHeader>
                      {expandedIds.has('cip-00xx-section') && (
                        <CardContent className="pt-0 space-y-2">
                          {/* Sort TBD topics by date DESC (most recent first) */}
                          {tbdItems
                            .flatMap(item => item.topics.map(topic => ({ topic, type: item.type })))
                            .sort((a, b) => new Date(b.topic.date).getTime() - new Date(a.topic.date).getTime())
                            .map(({ topic, type }) => renderTopicCard(topic, true, type))}
                        </CardContent>
                      )}
                    </Card>
                  )}

                  {/* Grouped Items */}
                  {groupedRegularItems.map((group) => {
                    const groupId = `group-${group.primaryId.toLowerCase()}`;
                    const isExpanded = expandedIds.has(groupId);
                    const typeConfig = TYPE_CONFIG[group.type];
                    const isSelected = selectedItems.has(group.primaryId);
                    
                    return (
                      <Card 
                        key={groupId} 
                        className={cn(
                          "hover:border-primary/30 transition-colors",
                          isSelected && "ring-2 ring-primary/50 border-primary/50"
                        )}
                      >
                        <CardHeader 
                          className="pb-3 cursor-pointer" 
                          onClick={() => !bulkSelectMode && toggleExpand(groupId)}
                        >
                          <div className="flex items-start justify-between gap-2">
                            {/* Checkbox for bulk selection */}
                            {bulkSelectMode && (
                              <div 
                                className="shrink-0 mr-3 cursor-pointer"
                                onClick={(e) => {
                                  e.stopPropagation();
                                  e.preventDefault();
                                  toggleItemSelection(group.primaryId);
                                }}
                              >
                                <Checkbox 
                                  checked={isSelected}
                                  className="pointer-events-none"
                                />
                              </div>
                            )}
                            
                            <div className="space-y-2 flex-1 min-w-0">
                              {/* Type and Network Badges */}
                              <div className="flex items-center gap-2 flex-wrap">
                                <Badge className={typeConfig.color}>
                                  {typeConfig.label}
                                </Badge>
                                {group.hasMultipleNetworks ? (
                                  <Badge variant="outline" className="text-[10px] h-5 border-blue-500/50 text-blue-400 bg-blue-500/10">
                                    Testnet + Mainnet
                                  </Badge>
                                ) : group.items[0]?.network && (
                                  <Badge 
                                    variant="outline" 
                                    className={cn(
                                      "text-[10px] h-5",
                                      group.items[0].network === 'mainnet' 
                                        ? 'border-green-500/50 text-green-400 bg-green-500/10' 
                                        : 'border-yellow-500/50 text-yellow-400 bg-yellow-500/10'
                                    )}
                                  >
                                    {group.items[0].network}
                                  </Badge>
                                )}
                                <span className="text-sm text-muted-foreground">
                                  {group.totalTopics} topics
                                </span>
                              </div>
                              
                              {/* Title - Use first topic subject to include "Vote proposal:" prefix */}
                              <CardTitle className="text-base leading-normal break-words">
                                {group.items[0]?.topics[0]?.subject || group.primaryId}
                              </CardTitle>
                              
                              {/* Date Range */}
                              <div className="flex items-center gap-1 text-xs text-muted-foreground">
                                <Calendar className="h-3 w-3" />
                                {formatDate(group.firstDate)} → {formatDate(group.lastDate)}
                              </div>
                              
                              {/* Lifecycle Progress */}
                              <div className="pt-1">
                                {(() => {
                                  // Extract lifecycle key for VoteRequest lookup based on type
                                  const getLifecycleKey = (type: string, primaryId: string, item: LifecycleItem): string | undefined => {
                                    if (type === 'cip') {
                                      const cipMatch = primaryId.match(/CIP[#\-\s]?0*(\d+)/i);
                                      return cipMatch ? `CIP-${cipMatch[1].padStart(4, '0')}` : undefined;
                                    }
                                    if (type === 'featured-app') {
                                      // Use app name from identifiers or primaryId
                                      const appName = item.topics[0]?.identifiers?.appName;
                                      return appName?.toLowerCase() || primaryId.toLowerCase();
                                    }
                                    if (type === 'validator') {
                                      const validatorName = item.topics[0]?.identifiers?.validatorName;
                                      return validatorName?.toLowerCase() || primaryId.toLowerCase();
                                    }
                                    if (type === 'protocol-upgrade') {
                                      return primaryId.toLowerCase();
                                    }
                                    return primaryId.toLowerCase();
                                  };
                                  const lifecycleKey = getLifecycleKey(group.type, group.primaryId, group.items[0]);
                                  return renderLifecycleProgress(group.items[0], lifecycleKey);
                                })()}
                              </div>
                              
                              {/* Active Vote Badge */}
                              {(() => {
                                const getLifecycleKey = (type: string, primaryId: string, item: LifecycleItem): string | undefined => {
                                  if (type === 'cip') {
                                    const cipMatch = primaryId.match(/CIP[#\-\s]?0*(\d+)/i);
                                    return cipMatch ? `CIP-${cipMatch[1].padStart(4, '0')}` : undefined;
                                  }
                                  if (type === 'featured-app') {
                                    const appName = item.topics[0]?.identifiers?.appName;
                                    return appName?.toLowerCase() || primaryId.toLowerCase();
                                  }
                                  if (type === 'validator') {
                                    const validatorName = item.topics[0]?.identifiers?.validatorName;
                                    return validatorName?.toLowerCase() || primaryId.toLowerCase();
                                  }
                                  if (type === 'protocol-upgrade') {
                                    return primaryId.toLowerCase();
                                  }
                                  return primaryId.toLowerCase();
                                };
                                const lifecycleKey = getLifecycleKey(group.type, group.primaryId, group.items[0]);
                                // Check for active votes in ACS (source === 'acs')
                                const matchingVotes = lifecycleKey ? combinedVoteMap.get(lifecycleKey.toLowerCase()) : undefined;
                                const hasActiveVote = matchingVotes?.some(vote => 
                                  vote.source === 'acs' && vote.status === 'pending'
                                );
                                return hasActiveVote ? (
                                  <Badge className="bg-pink-500/20 text-pink-400 border border-pink-500/30 animate-pulse">
                                    🗳️ Active Vote
                                  </Badge>
                                ) : null;
                              })()}
                              {/* Override indicator */}
                              {group.items[0]?.overrideApplied && (
                                <Badge variant="outline" className="text-[10px] h-5 border-purple-500/50 text-purple-400 bg-purple-500/10">
                                  ✎ Manually classified
                                </Badge>
                              )}
                              {/* LLM classification indicator */}
                              {group.items[0]?.llmClassified && !group.items[0]?.overrideApplied && (
                                <Badge variant="outline" className="text-[10px] h-5 border-cyan-500/50 text-cyan-400 bg-cyan-500/10">
                                  🤖 AI classified
                                </Badge>
                              )}
                            </div>
                            
                            <div className="flex items-center gap-1 shrink-0">
                              {/* Reclassify dropdown */}
                              <DropdownMenu>
                                <DropdownMenuTrigger asChild>
                                  <Button 
                                    variant="ghost" 
                                    size="sm" 
                                    className="h-7 w-7 p-0"
                                    onClick={(e) => e.stopPropagation()}
                                  >
                                    <MoreVertical className="h-4 w-4 text-muted-foreground" />
                                  </Button>
                                </DropdownMenuTrigger>
                                <DropdownMenuContent align="end" className="bg-popover w-56">
                                  <DropdownMenuLabel className="text-xs">Reclassify as...</DropdownMenuLabel>
                                  <DropdownMenuSeparator />
                                  {Object.entries(TYPE_CONFIG).map(([typeKey, config]) => (
                                    <DropdownMenuItem
                                      key={typeKey}
                                      disabled={typeKey === group.type}
                                      onClick={(e) => {
                                        e.stopPropagation();
                                        handleReclassify(group.primaryId, typeKey as LifecycleItem['type']);
                                      }}
                                      className="text-xs"
                                    >
                                      <Badge className={cn("mr-2 text-[10px]", config.color)}>
                                        {config.label}
                                      </Badge>
                                      {typeKey === group.type && <span className="text-muted-foreground">(current)</span>}
                                    </DropdownMenuItem>
                                  ))}
                                  
                                  {/* Merge into CIP(s) - only show for non-CIP items */}
                                  {!group.primaryId.match(/^CIP-\d+$/i) && cipList.length > 0 && (
                                    <>
                                      <DropdownMenuSeparator />
                                      <DropdownMenuItem
                                        onClick={(e) => {
                                          e.stopPropagation();
                                          openMergeDialog(group.primaryId);
                                        }}
                                        className="text-xs"
                                      >
                                        <Merge className="mr-2 h-3 w-3" />
                                        Merge into CIP(s)...
                                      </DropdownMenuItem>
                                    </>
                                  )}
                                </DropdownMenuContent>
                              </DropdownMenu>
                              
                              {isExpanded ? (
                                <ChevronUp className="h-5 w-5 text-muted-foreground" />
                              ) : (
                                <ChevronDown className="h-5 w-5 text-muted-foreground" />
                              )}
                            </div>
                          </div>
                        </CardHeader>
                        
                        {isExpanded && (
                          <CardContent className="pt-0 space-y-3 border-t">
                            {group.items.map((item) => {
                              const stages = WORKFLOW_STAGES[item.type] || WORKFLOW_STAGES.other;
                              // Extract lifecycle key for this item based on type
                              const getLifecycleKey = (type: string, primaryId: string, lifecycleItem: LifecycleItem): string | undefined => {
                                if (type === 'cip') {
                                  const cipMatch = primaryId.match(/CIP[#\-\s]?0*(\d+)/i);
                                  return cipMatch ? `CIP-${cipMatch[1].padStart(4, '0')}` : undefined;
                                }
                                if (type === 'featured-app') {
                                  const appName = lifecycleItem.topics[0]?.identifiers?.appName;
                                  return appName?.toLowerCase() || primaryId.toLowerCase();
                                }
                                if (type === 'validator') {
                                  const validatorName = lifecycleItem.topics[0]?.identifiers?.validatorName;
                                  return validatorName?.toLowerCase() || primaryId.toLowerCase();
                                }
                                if (type === 'protocol-upgrade') {
                                  return primaryId.toLowerCase();
                                }
                                return primaryId.toLowerCase();
                              };
                              const lifecycleKey = getLifecycleKey(item.type, group.primaryId, item);
                              // Use combined map (ACS + historical votes)
                              const matchingVotes = lifecycleKey ? combinedVoteMap.get(lifecycleKey.toLowerCase()) || [] : [];
                              
                              return (
                                <div key={item.id} className="space-y-2 pt-3">
                                  {group.hasMultipleNetworks && (
                                    <Badge 
                                      variant="outline" 
                                      className={cn(
                                        "text-xs mb-2",
                                        item.network === 'mainnet' 
                                          ? 'border-green-500/50 text-green-400 bg-green-500/10' 
                                          : 'border-yellow-500/50 text-yellow-400 bg-yellow-500/10'
                                      )}
                                    >
                                      {item.network}
                                    </Badge>
                                  )}
                                  {stages.map(stage => {
                                    // Handle vote stages (on-chain vote and milestone) - use combined votes filtered by stage
                                    const isVoteStage = stage === 'sv-onchain-vote' || stage === 'sv-milestone';
                                    if (isVoteStage) {
                                      const stageVotes = matchingVotes.filter(v => v.stage === stage);
                                      if (stageVotes.length === 0) return null;
                                      const config = STAGE_CONFIG[stage];
                                      if (!config) return null;
                                      const Icon = config.icon;
                                      
                                      return (
                                        <div key={stage} className="space-y-2">
                                          <h4 className="text-sm font-medium flex items-center gap-2 text-muted-foreground">
                                            <Icon className="h-4 w-4" />
                                            {config.label} ({stageVotes.length})
                                          </h4>
                                          <div className="space-y-2 pl-6">
                                            {/* Sort votes by date DESC (most recent first) */}
                                            {[...stageVotes]
                                              .sort((a, b) => {
                                                const dateA = a.voteBefore?.getTime() || 0;
                                                const dateB = b.voteBefore?.getTime() || 0;
                                                return dateB - dateA;
                                              })
                                              .map(vote => renderOnChainVoteCard(vote))}
                                          </div>
                                        </div>
                                      );
                                    }
                                    
                                    // Regular topic stages
                                    const stageTopics = item.stages[stage];
                                    if (!stageTopics || stageTopics.length === 0) return null;
                                    const config = STAGE_CONFIG[stage];
                                    if (!config) return null;
                                    const Icon = config.icon;
                                    
                                    // Sort topics by date DESC (most recent first)
                                    const sortedTopics = [...stageTopics].sort((a, b) => 
                                      new Date(b.date).getTime() - new Date(a.date).getTime()
                                    );
                                    
                                    return (
                                      <div key={stage} className="space-y-2">
                                        <h4 className="text-sm font-medium flex items-center gap-2 text-muted-foreground">
                                          <Icon className="h-4 w-4" />
                                          {config.label} ({sortedTopics.length})
                                        </h4>
                                        <div className="space-y-2 pl-6">
                                          {sortedTopics.map(topic => renderTopicCard(topic, false, item.type))}
                                        </div>
                                      </div>
                                    );
                                  })}
                                  
                                  {/* Render topics with stages not in the expected workflow */}
                                  {(() => {
                                    // Find topics whose stage is NOT in the expected workflow stages for this type
                                    const unexpectedStageTopics = item.topics.filter(topic => 
                                      !stages.includes(topic.stage)
                                    );
                                    
                                    if (unexpectedStageTopics.length === 0) return null;
                                    
                                    // Group by their actual stage
                                    const groupedByStage = unexpectedStageTopics.reduce((acc, topic) => {
                                      if (!acc[topic.stage]) acc[topic.stage] = [];
                                      acc[topic.stage].push(topic);
                                      return acc;
                                    }, {} as Record<string, Topic[]>);
                                    
                                    return Object.entries(groupedByStage).map(([stage, topics]) => {
                                      const config = STAGE_CONFIG[stage];
                                      const Icon = config?.icon || FileText;
                                      const label = config?.label || stage;
                                      
                                      // Sort topics by date DESC (most recent first)
                                      const sortedTopics = [...topics].sort((a, b) => 
                                        new Date(b.date).getTime() - new Date(a.date).getTime()
                                      );
                                      
                                      return (
                                        <div key={`extra-${stage}`} className="space-y-2">
                                          <h4 className="text-sm font-medium flex items-center gap-2 text-muted-foreground">
                                            <Icon className="h-4 w-4" />
                                            {label} ({sortedTopics.length})
                                          </h4>
                                          <div className="space-y-2 pl-6">
                                            {sortedTopics.map(topic => renderTopicCard(topic, false, item.type))}
                                          </div>
                                        </div>
                                      );
                                    });
                                  })()}
                                </div>
                              );
                            })}
                          </CardContent>
                        )}
                      </Card>
                    );
                  })}
                </div>
              </ScrollArea>
            )}
          </TabsContent>

          {/* Timeline View */}
          <TabsContent value="timeline" className="mt-4 space-y-4">
            {isLoading ? (
              Array.from({ length: 5 }).map((_, i) => (
                <Skeleton key={i} className="h-32 w-full mb-4" />
              ))
            ) : timelineData.length === 0 ? (
              <Card>
                <CardContent className="flex flex-col items-center justify-center py-12">
                  <Calendar className="h-12 w-12 text-muted-foreground mb-4" />
                  <h3 className="text-lg font-medium">No Timeline Data</h3>
                  {(dateFrom || dateTo) && (
                    <p className="text-sm text-muted-foreground mt-2">
                      Try adjusting your date range filter
                    </p>
                  )}
                </CardContent>
              </Card>
            ) : (
              <div className="relative">
                {/* Timeline line */}
                <div className="absolute left-4 top-0 bottom-0 w-0.5 bg-border" />
                
                <div className="space-y-6">
                  {timelineData.map((monthData) => (
                    <div key={monthData.month} className="relative pl-10">
                      {/* Month marker */}
                      <div className="absolute left-0 top-0 w-8 h-8 rounded-full bg-primary flex items-center justify-center">
                        <Calendar className="h-4 w-4 text-primary-foreground" />
                      </div>
                      
                      <div className="space-y-3">
                        <h3 className="text-lg font-semibold sticky top-0 bg-background py-2 z-10">
                          {monthData.label}
                          <Badge variant="secondary" className="ml-2 text-xs">
                            {monthData.topicCount} topics
                          </Badge>
                          {monthData.voteCount > 0 && (
                            <Badge className="ml-2 text-xs bg-pink-500/20 text-pink-400 border border-pink-500/30">
                              🗳️ {monthData.voteCount} votes
                            </Badge>
                          )}
                        </h3>
                        
                        <div className="space-y-2">
                          {monthData.entries.map((entry, idx) => {
                            if (entry.type === 'topic') {
                              const topic = entry.data;
                              const stageConfig = STAGE_CONFIG[topic.stage as keyof typeof STAGE_CONFIG];
                              const StageIcon = stageConfig?.icon || FileText;
                              
                              return (
                                <div 
                                  key={topic.id}
                                  className="flex gap-3 p-3 rounded-lg bg-muted/30 border border-border/50 hover:border-primary/30 transition-colors"
                                >
                                  <div className={`shrink-0 w-8 h-8 rounded-full flex items-center justify-center ${stageConfig?.color || 'bg-muted'}`}>
                                    <StageIcon className="h-4 w-4" />
                                  </div>
                                  
                                  <div className="flex-1 min-w-0">
                                    <div className="flex items-start justify-between gap-2">
                                      <div className="min-w-0">
                                        <h4 className="font-medium text-sm break-words">{topic.subject}</h4>
                                        <div className="flex flex-wrap items-center gap-2 mt-1 text-xs text-muted-foreground">
                                          <span>{formatDate(topic.date)}</span>
                                          <Badge variant="outline" className="text-[10px] h-5">
                                            {topic.groupLabel}
                                          </Badge>
                                          <Badge className={`text-[10px] h-5 ${stageConfig?.color || ''}`}>
                                            {stageConfig?.label || topic.stage}
                                          </Badge>
                                        </div>
                                      </div>
                                      {topic.sourceUrl && (
                                        <Button variant="ghost" size="sm" asChild className="h-7 w-7 p-0 shrink-0">
                                          <a href={topic.sourceUrl} target="_blank" rel="noopener noreferrer">
                                            <ExternalLink className="h-3.5 w-3.5" />
                                          </a>
                                        </Button>
                                      )}
                                    </div>
                                    {topic.excerpt && (
                                      <p className="text-xs text-muted-foreground mt-2 break-words whitespace-pre-wrap">
                                        {topic.excerpt}
                                      </p>
                                    )}
                                  </div>
                                </div>
                              );
                            }
                            
                            // Vote timeline entries
                            const vr = entry.data;
                            const payload = vr.payload;
                            const reason = payload?.reason;
                            const voteBefore = payload?.voteBefore ? new Date(payload.voteBefore) : null;
                            const votesRaw = payload?.votes || [];
                            let votesFor = 0;
                            let votesAgainst = 0;
                            for (const vote of votesRaw) {
                              const [, voteData] = Array.isArray(vote) ? vote : [vote.sv || "Unknown", vote];
                              const isAccept = voteData?.accept === true || (voteData as any)?.Accept === true;
                              const isReject = voteData?.accept === false || (voteData as any)?.reject === true || (voteData as any)?.Reject === true;
                              if (isAccept) votesFor++;
                              else if (isReject) votesAgainst++;
                            }
                            
                            const isStartEntry = entry.type === 'vote-started';
                            const statusColors = {
                              passed: 'bg-green-500/20 border-green-500/50 text-green-400',
                              failed: 'bg-red-500/20 border-red-500/50 text-red-400',
                              expired: 'bg-yellow-500/20 border-yellow-500/50 text-yellow-400',
                            };
                            
                            const proposalId = (payload?.trackingCid || vr.contract_id)?.slice(0, 12) || 'unknown';
                            
                            return (
                              <a
                                key={`${entry.type}-${vr.contract_id}-${idx}`}
                                href={`/governance?tab=active&proposal=${proposalId}`}
                                className={cn(
                                  "flex gap-3 p-3 rounded-lg border transition-colors",
                                  isStartEntry 
                                    ? "bg-pink-500/10 border-pink-500/30 hover:border-pink-500/50"
                                    : entry.type === 'vote-ended' && statusColors[entry.status]
                                )}
                              >
                                <div className={cn(
                                  "shrink-0 w-8 h-8 rounded-full flex items-center justify-center",
                                  isStartEntry ? "bg-pink-500/30" : 
                                  entry.type === 'vote-ended' && entry.status === 'passed' ? "bg-green-500/30" :
                                  entry.type === 'vote-ended' && entry.status === 'failed' ? "bg-red-500/30" :
                                  "bg-yellow-500/30"
                                )}>
                                  {isStartEntry ? (
                                    <Vote className="h-4 w-4 text-pink-400" />
                                  ) : entry.type === 'vote-ended' && entry.status === 'passed' ? (
                                    <CheckCircle2 className="h-4 w-4 text-green-400" />
                                  ) : entry.type === 'vote-ended' && entry.status === 'failed' ? (
                                    <XCircle className="h-4 w-4 text-red-400" />
                                  ) : (
                                    <Clock className="h-4 w-4 text-yellow-400" />
                                  )}
                                </div>
                                
                                <div className="flex-1 min-w-0">
                                  <div className="flex items-start justify-between gap-2">
                                    <div className="min-w-0">
                                      <h4 className="font-medium text-sm">
                                        {isStartEntry ? '🗳️ Vote Opened' : 
                                          entry.type === 'vote-ended' && entry.status === 'passed' ? '✅ Vote Passed' :
                                          entry.type === 'vote-ended' && entry.status === 'failed' ? '❌ Vote Failed' :
                                          '⏳ Vote Expired'}
                                        {entry.cipRef && ` for CIP-${parseInt(entry.cipRef)}`}
                                      </h4>
                                      <div className="flex flex-wrap items-center gap-2 mt-1 text-xs text-muted-foreground">
                                        <span>{formatDate(entry.date.toISOString())}</span>
                                        <Badge variant="outline" className="text-[10px] h-5">
                                          On-chain Vote
                                        </Badge>
                                        <span className="flex items-center gap-1">
                                          <Vote className="h-3 w-3" />
                                          {votesFor} for / {votesAgainst} against
                                        </span>
                                        {isStartEntry && voteBefore && (
                                          <span className="flex items-center gap-1 text-yellow-400">
                                            <Clock className="h-3 w-3" />
                                            Due {format(voteBefore, 'MMM d, yyyy')}
                                          </span>
                                        )}
                                      </div>
                                    </div>
                                    <ExternalLink className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
                                  </div>
                                  {reason?.body && (
                                    <p className="text-xs text-muted-foreground mt-2 break-words whitespace-pre-wrap">
                                      {reason.body}
                                    </p>
                                  )}
                                </div>
                              </a>
                            );
                          })}
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </TabsContent>

          {/* All Topics View */}
          <TabsContent value="all" className="space-y-3 mt-4">
            {isLoading ? (
              Array.from({ length: 10 }).map((_, i) => (
                <Skeleton key={i} className="h-24 w-full" />
              ))
            ) : filteredTopics.length === 0 ? (
              <Card>
                <CardContent className="flex flex-col items-center justify-center py-12">
                  <FileText className="h-12 w-12 text-muted-foreground mb-4" />
                  <h3 className="text-lg font-medium">No Topics Found</h3>
                </CardContent>
              </Card>
            ) : (
              <ScrollArea className="h-[600px]">
                <div className="space-y-2 pr-4">
                  {filteredTopics.map(topic => renderTopicCard(topic, true))}
                </div>
              </ScrollArea>
            )}
          </TabsContent>
          
          {/* Learn from Corrections View */}
          <TabsContent value="learn" className="mt-4">
            <LearnFromCorrectionsPanel />
          </TabsContent>
        </Tabs>

      </div>
      
      {/* Multi-CIP Merge Dialog */}
      <Dialog open={mergeDialogOpen} onOpenChange={setMergeDialogOpen}>
        <DialogContent className="max-w-md">
          <DialogHeader>
            <DialogTitle>Merge into CIP(s)</DialogTitle>
            <DialogDescription className="break-words">
              Select one or more CIPs to merge "{mergeSourceLabel?.slice(0, 80)}{(mergeSourceLabel?.length || 0) > 80 ? '...' : ''}" into.
            </DialogDescription>
          </DialogHeader>
          
          <div className="space-y-4">
            {/* Search */}
            <div className="relative">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
              <Input
                placeholder="Search CIPs..."
                value={mergeSearchQuery}
                onChange={(e) => setMergeSearchQuery(e.target.value)}
                className="pl-9"
              />
            </div>
            
            {/* Selection count */}
            {selectedMergeCips.size > 0 && (
              <div className="text-sm text-muted-foreground">
                {selectedMergeCips.size} CIP{selectedMergeCips.size > 1 ? 's' : ''} selected
              </div>
            )}
            
            {/* CIP List with checkboxes */}
            <ScrollArea className="h-64 border rounded-md">
              <div className="p-2 space-y-1">
                {filteredMergeCips.map((cip) => (
                  <div
                    key={cip.primaryId}
                    onClick={() => toggleMergeCip(cip.primaryId)}
                    className={cn(
                      "flex items-center gap-3 p-2 rounded-md cursor-pointer transition-colors",
                      selectedMergeCips.has(cip.primaryId) 
                        ? "bg-primary/20 border border-primary/40" 
                        : "hover:bg-muted/50"
                    )}
                  >
                    <Checkbox 
                      checked={selectedMergeCips.has(cip.primaryId)}
                      onCheckedChange={() => toggleMergeCip(cip.primaryId)}
                    />
                    <span className="font-mono text-sm">{cip.primaryId}</span>
                    <span className="text-xs text-muted-foreground ml-auto">
                      {cip.topicCount} topics
                    </span>
                  </div>
                ))}
                {filteredMergeCips.length === 0 && (
                  <div className="text-center text-sm text-muted-foreground py-4">
                    No CIPs found
                  </div>
                )}
              </div>
            </ScrollArea>
          </div>
          
          <DialogFooter>
            <Button variant="outline" onClick={() => setMergeDialogOpen(false)} disabled={isMerging}>
              Cancel
            </Button>
            <Button 
              onClick={handleMergeInto}
              disabled={selectedMergeCips.size === 0 || isMerging}
            >
              {isMerging ? (
                <>
                  <RefreshCw className="mr-2 h-4 w-4 animate-spin" />
                  Saving...
                </>
              ) : (
                `Merge into ${selectedMergeCips.size > 0 ? selectedMergeCips.size : ''} CIP${selectedMergeCips.size !== 1 ? 's' : ''}`
              )}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
      
      {/* Move to Card Dialog */}
      <Dialog open={moveDialogOpen} onOpenChange={setMoveDialogOpen}>
        <DialogContent className="max-w-md">
          <DialogHeader>
            <DialogTitle>Move to Card</DialogTitle>
            <DialogDescription className="break-words">
              Select a card to move "{moveSourceLabel?.slice(0, 80)}{(moveSourceLabel?.length || 0) > 80 ? '...' : ''}" to.
            </DialogDescription>
          </DialogHeader>
          
          <div className="space-y-4">
            {/* Search */}
            <div className="relative">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
              <Input
                placeholder="Search cards..."
                value={moveSearchQuery}
                onChange={(e) => setMoveSearchQuery(e.target.value)}
                className="pl-9"
              />
            </div>
            
            {/* Card List with radio selection */}
            <ScrollArea className="h-64 border rounded-md">
              <div className="p-2 space-y-1">
                {filteredCards.map((card) => (
                  <div
                    key={card.id}
                    onClick={() => setSelectedTargetCard(card.id)}
                    className={cn(
                      "flex items-start gap-3 p-2 rounded-md cursor-pointer transition-colors",
                      selectedTargetCard === card.id 
                        ? "bg-primary/20 border border-primary/40" 
                        : "hover:bg-muted/50"
                    )}
                  >
                    <div className={cn(
                      "h-4 w-4 mt-0.5 rounded-full border-2 flex items-center justify-center",
                      selectedTargetCard === card.id ? "border-primary" : "border-muted-foreground"
                    )}>
                      {selectedTargetCard === card.id && (
                        <div className="h-2 w-2 rounded-full bg-primary" />
                      )}
                    </div>
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2">
                        <span className="font-medium text-sm">{card.primaryId}</span>
                        <Badge className={cn("text-[10px]", TYPE_CONFIG[card.type as keyof typeof TYPE_CONFIG]?.color || 'bg-muted')}>
                          {TYPE_CONFIG[card.type as keyof typeof TYPE_CONFIG]?.label || card.type}
                        </Badge>
                      </div>
                      {card.preview && (
                        <p className="text-xs text-muted-foreground truncate mt-0.5">
                          {card.preview}
                        </p>
                      )}
                    </div>
                    <span className="text-xs text-muted-foreground shrink-0">
                      {card.topicCount} topics
                    </span>
                  </div>
                ))}
                {filteredCards.length === 0 && (
                  <div className="text-center text-sm text-muted-foreground py-4">
                    {cardList.length === 0 ? 'Loading cards...' : 'No cards found'}
                  </div>
                )}
              </div>
            </ScrollArea>
          </div>
          
          <DialogFooter>
            <Button variant="outline" onClick={() => setMoveDialogOpen(false)} disabled={isMoving}>
              Cancel
            </Button>
            <Button 
              onClick={handleMoveToCard}
              disabled={!selectedTargetCard || isMoving}
            >
              {isMoving ? (
                <>
                  <RefreshCw className="mr-2 h-4 w-4 animate-spin" />
                  Moving...
                </>
              ) : (
                <>
                  <MoveRight className="mr-2 h-4 w-4" />
                  Move to Card
                </>
              )}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </DashboardLayout>
  );
};

export default GovernanceFlow;
