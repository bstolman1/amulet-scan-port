import React, { useState, useEffect, useMemo, useCallback } from "react";
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
import { useActiveVoteRequests } from "@/hooks/use-canton-scan-api";
import { LearnFromCorrectionsPanel } from "@/components/LearnFromCorrectionsPanel";
import { GoldenSetManagementPanel } from "@/components/GoldenSetManagementPanel";


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
  'featured-app': ['tokenomics', 'tokenomics-announce', 'sv-announce', 'sv-onchain-vote'],
  validator: ['tokenomics', 'sv-announce', 'sv-onchain-vote'],
  'protocol-upgrade': ['tokenomics', 'sv-announce', 'sv-onchain-vote'],
  outcome: [],
  other: ['tokenomics', 'sv-announce', 'sv-onchain-vote'],
};

// All possible stages with their display config
const STAGE_CONFIG: Record<string, { label: string; icon: typeof FileText; color: string }> = {
  'cip-discuss': { label: 'CIP-Discuss', icon: FileText, color: 'bg-blue-500/20 text-blue-400 border-blue-500/30' },
  'cip-vote': { label: 'CIP-Vote', icon: Vote, color: 'bg-purple-500/20 text-purple-400 border-purple-500/30' },
  'cip-announce': { label: 'CIP-Announce', icon: CheckCircle2, color: 'bg-green-500/20 text-green-400 border-green-500/30' },
  'sv-onchain-vote': { label: 'On-Chain Vote', icon: Vote, color: 'bg-pink-500/20 text-pink-400 border-pink-500/30' },
  'sv-milestone': { label: 'Milestone', icon: CheckSquare, color: 'bg-teal-500/20 text-teal-400 border-teal-500/30' },
  'tokenomics': { label: 'Tokenomics', icon: FileText, color: 'bg-blue-500/20 text-blue-400 border-blue-500/30' },
  'tokenomics-announce': { label: 'Tokenomics-Announce', icon: CheckCircle2, color: 'bg-green-500/20 text-green-400 border-green-500/30' },
  'sv-announce': { label: 'SV-Announce', icon: Clock, color: 'bg-orange-500/20 text-orange-400 border-orange-500/30' },
};

const TYPE_CONFIG = {
  cip: { label: 'CIP', color: 'bg-primary/20 text-primary' },
  'featured-app': { label: 'Featured App', color: 'bg-emerald-500/20 text-emerald-400' },
  validator: { label: 'Validator', color: 'bg-orange-500/20 text-orange-400' },
  'protocol-upgrade': { label: 'Protocol Upgrade', color: 'bg-cyan-500/20 text-cyan-400' },
  outcome: { label: 'Tokenomics Outcomes', color: 'bg-amber-500/20 text-amber-400' },
  other: { label: 'Other', color: 'bg-muted text-muted-foreground' },
};

// Interface for VoteRequest contracts from Scan API (active/pending)
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

// ─────────────────────────────────────────────────────────────────────────────
// VOTE MATCHING — URL-first, then CIP number, then party ID
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Normalise a URL for comparison.
 * Strips query string, fragment, trailing slash, lowercases.
 */
const normalizeUrl = (url: string): string => {
  if (!url) return '';
  try {
    const u = new URL(url.trim());
    return `${u.protocol}//${u.host}${u.pathname}`.toLowerCase().replace(/\/$/, '');
  } catch {
    return url.trim().toLowerCase().replace(/\/$/, '').split('?')[0].split('#')[0];
  }
};

/**
 * Normalise a Canton party ID or plain name.
 * Strips network prefix and Canton fingerprint hash (::122abc…).
 */
const normalizePartyId = (id: string): string => {
  if (!id) return '';
  let s = id.trim();
  s = s.replace(/^(mainnet|testnet):\s*/i, '');
  s = s.replace(/::[0-9a-f]{6,}$/i, '');
  return s.toLowerCase();
};

/**
 * Detect if a vote represents a milestone/reward vote.
 */
const isMilestoneVote = (actionTag: string, text: string): boolean => {
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

/**
 * Drill into a Canton action value to extract the relevant party ID.
 * Handles nested ARC_DsoRules / ARC_AmuletRules wrappers.
 */
const extractPartyIdFromAction = (actionTag: string, actionValue: any): string | null => {
  if (!actionValue) return null;
  const inner =
    actionValue?.dsoAction?.value ||
    actionValue?.amuletRulesAction?.value ||
    actionValue?.svAction?.value ||
    actionValue;
  const partyId =
    inner?.provider ||
    inner?.featuredAppProvider ||
    inner?.featuredApp ||
    inner?.beneficiary ||
    inner?.validator ||
    inner?.sv ||
    inner?.svParty ||
    inner?.name ||
    null;
  return partyId ? String(partyId) : null;
};

/** Infer governance type from a lifecycle key string */
const inferTypeFromKey = (
  key: string
): 'cip' | 'featured-app' | 'validator' | 'protocol-upgrade' | 'other' => {
  if (/^cip-\d+$/i.test(key)) return 'cip';
  if (/validator|offboard|onboard/i.test(key)) return 'validator';
  if (/migration|upgrade/i.test(key)) return 'protocol-upgrade';
  return 'featured-app';
};

/** Infer governance type from action tag and reason text */
const inferTypeFromActionTag = (
  actionTag: string,
  text: string
): 'cip' | 'featured-app' | 'validator' | 'protocol-upgrade' | 'other' => {
  if (/OffboardSv|OnboardSv|GrantSvStatus/i.test(actionTag)) return 'validator';
  if (/OnboardValidator|OffboardValidator|ValidatorOnboard/i.test(actionTag)) return 'validator';
  if (/GrantFeaturedAppRight|RevokeFeaturedAppRight|SetFeaturedAppRight|FeaturedApp/i.test(actionTag)) return 'featured-app';
  if (/ScheduleDomainMigration|ProtocolUpgrade|Synchronizer/i.test(actionTag)) return 'protocol-upgrade';
  if (/MintUnclaimed|MintRewards|DistributeRewards|Coupon/i.test(actionTag)) return 'featured-app';
  if (/validator/i.test(text)) return 'validator';
  if (/migration|splice\s*\d+\.\d+/i.test(text)) return 'protocol-upgrade';
  return 'other';
};

/**
 * Derive the canonical lifecycle key for a LifecycleItem.
 * Single source of truth — replaces all inline getLifecycleKey() definitions.
 */
const deriveLifecycleKey = (item: LifecycleItem): string | undefined => {
  const { type, primaryId, topics } = item;
  if (type === 'cip') {
    const m = primaryId.match(/CIP[#\-\s]?0*(\d+)/i);
    return m ? `CIP-${m[1].padStart(4, '0')}` : primaryId.toLowerCase();
  }
  if (type === 'featured-app') {
    const raw = topics[0]?.identifiers?.appName || primaryId;
    return normalizePartyId(raw);
  }
  if (type === 'validator') {
    const raw = topics[0]?.identifiers?.validatorName || primaryId;
    return normalizePartyId(raw);
  }
  return primaryId.toLowerCase();
};

/**
 * Build a reverse index: normalised topic sourceUrl → lifecycle key.
 * This enables O(1) vote→lifecycle matching by URL.
 */
const buildUrlIndex = (lifecycleItems: LifecycleItem[]): Map<string, string> => {
  const index = new Map<string, string>();
  for (const item of lifecycleItems) {
    const key = deriveLifecycleKey(item);
    if (!key) continue;
    for (const topic of item.topics) {
      if (topic.sourceUrl) {
        const norm = normalizeUrl(topic.sourceUrl);
        if (norm && !index.has(norm)) index.set(norm, key);
      }
      for (const linked of (topic.linkedUrls ?? [])) {
        const norm = normalizeUrl(linked);
        if (norm && !index.has(norm)) index.set(norm, key);
      }
    }
  }
  return index;
};

interface ResolvedVoteMapping {
  key: string;
  type: 'cip' | 'featured-app' | 'validator' | 'protocol-upgrade' | 'other';
  stages: Array<'sv-onchain-vote' | 'sv-milestone'>;
  matchMethod: 'url' | 'cip-number' | 'party-id' | 'base-name';
}

/**
 * Resolve a vote to a lifecycle key using 4-priority matching:
 * 1. URL match (reason URL === topic sourceUrl)
 * 2. CIP number in reason text
 * 3. Party ID from action payload (fingerprint stripped)
 * 4. Base name from reason text (loose fallback)
 */
const resolveVoteKey = (
  reasonUrl: string,
  reasonBody: string,
  actionTag: string,
  actionValue: any,
  urlIndex: Map<string, string>
): ResolvedVoteMapping | null => {
  const text = `${reasonBody ?? ''} ${reasonUrl ?? ''}`;
  const stages: Array<'sv-onchain-vote' | 'sv-milestone'> = ['sv-onchain-vote'];
  if (isMilestoneVote(actionTag, text)) stages.push('sv-milestone');

  // Priority 1: URL match
  if (reasonUrl) {
    const urlKey = urlIndex.get(normalizeUrl(reasonUrl));
    if (urlKey) {
      return { key: urlKey, type: inferTypeFromKey(urlKey), stages, matchMethod: 'url' };
    }
  }

  // Priority 2: CIP number
  const cipMatch = text.match(/CIP[#\-\s]?0*(\d+)/i);
  if (cipMatch) {
    const cipKey = `CIP-${cipMatch[1].padStart(4, '0')}`;
    return { key: cipKey, type: 'cip', stages, matchMethod: 'cip-number' };
  }

  // Priority 3: Party ID from action payload
  const partyId = extractPartyIdFromAction(actionTag, actionValue);
  if (partyId) {
    const normParty = normalizePartyId(partyId);
    if (normParty) {
      return { key: normParty, type: inferTypeFromActionTag(actionTag, text), stages, matchMethod: 'party-id' };
    }
  }

  // Priority 4: Base name from reason text
  const nameFromText =
    text.match(/(?:mainnet|testnet):\s*([^\s,\n]+)/i)?.[1] ||
    text.match(/app[:\s]+([^\s,\n]+)/i)?.[1];
  if (nameFromText) {
    const norm = normalizePartyId(nameFromText);
    if (norm && norm.length > 2) {
      return { key: norm, type: inferTypeFromActionTag(actionTag, text), stages, matchMethod: 'base-name' };
    }
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
  
  const { data: scanVoteRequests } = useActiveVoteRequests();
  const { data: historicalVotes = [] } = useGovernanceVoteHistory(500);
  
  const [selectedItems, setSelectedItems] = useState<Set<string>>(new Set());
  const [bulkSelectMode, setBulkSelectMode] = useState(false);
  
  // Unified on-chain vote item for display
  interface OnChainVoteItem {
    id: string;
    source: 'active' | 'history';
    stage: 'sv-onchain-vote' | 'sv-milestone';
    status: 'pending' | 'approved' | 'rejected' | 'expired';
    votesFor: number;
    votesAgainst: number;
    totalVotes: number;
    voteBefore: Date | null;
    reasonBody: string;
    reasonUrl: string;
    voteRequest?: VoteRequest;
    historicalVote?: ParsedVoteResult;
  }

  // URL index: normalised topic sourceUrl → lifecycle key
  // Enables O(1) vote→lifecycle matching by reason URL.
  const urlIndex = useMemo(
    () => buildUrlIndex(data?.lifecycleItems ?? []),
    [data]
  );

  // Combined vote map: lifecycle key → OnChainVoteItem[]
  // Populated from both Scan API active and history endpoints.
  const combinedVoteMap = useMemo(() => {
    const map = new Map<string, OnChainVoteItem[]>();

    const addToMap = (key: string, item: OnChainVoteItem) => {
      if (!map.has(key)) map.set(key, []);
      map.get(key)!.push(item);
    };

    // Active vote requests from Scan API
    for (const vr of voteRequests) {
      const payload = vr.payload;
      const mapping = resolveVoteKey(
        payload?.reason?.url ?? '',
        payload?.reason?.body ?? '',
        payload?.action?.tag ?? '',
        payload?.action?.value ?? null,
        urlIndex
      );
      if (!mapping) continue;

      if (import.meta.env.DEV) {
        console.debug('[vote-match] ACTIVE', mapping.matchMethod, mapping.key, vr.contract_id?.slice(0, 12));
      }

      const votesRaw = payload?.votes || [];
      let votesFor = 0, votesAgainst = 0;
      for (const vote of votesRaw) {
        const [, voteData] = Array.isArray(vote) ? vote : [vote?.sv ?? 'Unknown', vote];
        if ((voteData as any)?.accept === true) votesFor++;
        else votesAgainst++;
      }
      const voteBefore = payload?.voteBefore ? new Date(payload.voteBefore) : null;
      const isExpired = voteBefore && voteBefore < new Date();
      let status: OnChainVoteItem['status'] = 'pending';
      if (votesFor >= 10) status = 'approved';
      else if (isExpired) status = 'expired';

      const id = (payload?.trackingCid ?? vr.contract_id ?? '').slice(0, 12) || 'unknown';

      for (const stage of mapping.stages) {
        addToMap(mapping.key, {
          id, source: 'active', stage, status,
          votesFor, votesAgainst, totalVotes: votesRaw.length,
          voteBefore,
          reasonBody: payload?.reason?.body ?? '',
          reasonUrl: payload?.reason?.url ?? '',
          voteRequest: vr,
        });
      }
    }

    // Scan API historical votes
    for (const vote of historicalVotes) {
      const mapping = resolveVoteKey(
        vote.reasonUrl ?? '',
        vote.reasonBody ?? '',
        vote.actionType ?? '',
        vote.actionDetails ?? null,
        urlIndex
      );
      if (!mapping) continue;

      if (import.meta.env.DEV) {
        console.debug('[vote-match] HIST', mapping.matchMethod, mapping.key, vote.id);
      }

      for (const stage of mapping.stages) {
        addToMap(mapping.key, {
          id: vote.id,
          source: 'history',
          stage,
          status: vote.outcome === 'accepted' ? 'approved'
                : vote.outcome === 'rejected' ? 'rejected'
                : 'expired',
          votesFor: vote.votesFor,
          votesAgainst: vote.votesAgainst,
          totalVotes: vote.totalVotes,
          voteBefore: vote.voteBefore ? new Date(vote.voteBefore) : null,
          reasonBody: vote.reasonBody ?? '',
          reasonUrl: vote.reasonUrl ?? '',
          historicalVote: vote,
        });
      }
    }

    // Sort each bucket: Active first, then by date DESC
    for (const [key, items] of map.entries()) {
      map.set(key, items.sort((a, b) => {
        if (a.source === 'active' && b.source !== 'active') return -1;
        if (a.source !== 'active' && b.source === 'active') return 1;
        return (b.voteBefore?.getTime() ?? 0) - (a.voteBefore?.getTime() ?? 0);
      }));
    }

    return map;
  }, [voteRequests, historicalVotes, urlIndex]);

  // Convenience lookup — single place to get votes for any lifecycle item
  const getVotesForItem = useCallback(
    (item: LifecycleItem): OnChainVoteItem[] => {
      const key = deriveLifecycleKey(item);
      if (!key) return [];
      return combinedVoteMap.get(key) ?? [];
    },
    [combinedVoteMap]
  );

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
      
      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(errorData.error || `Failed to fetch: ${response.status}`);
      }
      
      const result = await response.json();
      
      if (result.warning) {
        console.warn('Governance data warning:', result.warning);
      }
      
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
  
  const handleReclassify = async (primaryId: string, newType: LifecycleItem['type']) => {
    try {
      const baseUrl = getDuckDBApiUrl();
      const response = await fetch(`${baseUrl}/api/governance-lifecycle/overrides`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ primaryId, type: newType, reason: 'Manual UI correction' }),
      });
      if (!response.ok) throw new Error('Failed to save override');
      toast({ title: "Classification updated", description: `"${primaryId}" will now appear as ${TYPE_CONFIG[newType].label}` });
      fetchData(false);
    } catch (err) {
      console.error('Failed to reclassify:', err);
      toast({ title: "Error", description: "Failed to save classification override", variant: "destructive" });
    }
  };

  const handleReclassifyTopic = async (topicId: string, topicSubject: string, newType: LifecycleItem['type']) => {
    try {
      const baseUrl = getDuckDBApiUrl();
      const response = await fetch(`${baseUrl}/api/governance-lifecycle/overrides/topic`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ topicId, newType, reason: 'Manual topic reclassification' }),
      });
      if (!response.ok) throw new Error('Failed to save topic override');
      toast({ title: "Topic reclassified", description: `Topic will now appear as ${TYPE_CONFIG[newType].label}` });
      fetchData(false);
    } catch (err) {
      console.error('Failed to reclassify topic:', err);
      toast({ title: "Error", description: "Failed to save topic classification override", variant: "destructive" });
    }
  };

  const handleExtractTopic = async (topicId: string, topicSubject: string) => {
    try {
      const baseUrl = getDuckDBApiUrl();
      const response = await fetch(`${baseUrl}/api/governance-lifecycle/overrides/extract`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ topicId, reason: 'Extracted via UI' }),
      });
      if (!response.ok) throw new Error('Failed to save extract override');
      toast({ title: "Topic extracted", description: `"${topicSubject.slice(0, 50)}..." will now appear as its own card` });
      fetchData(false);
    } catch (err) {
      console.error('Failed to extract topic:', err);
      toast({ title: "Error", description: "Failed to extract topic to own card", variant: "destructive" });
    }
  };

  const [cipList, setCipList] = useState<{ primaryId: string; topicCount: number }[]>([]);
  const [mergeDialogOpen, setMergeDialogOpen] = useState(false);
  const [mergeSourceId, setMergeSourceId] = useState<string | null>(null);
  const [mergeSourceLabel, setMergeSourceLabel] = useState<string | null>(null);
  const [selectedMergeCips, setSelectedMergeCips] = useState<Set<string>>(new Set());
  const [mergeSearchQuery, setMergeSearchQuery] = useState('');
  const [isMerging, setIsMerging] = useState(false);
  
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
  
  const openMergeDialog = (sourceId: string, sourceLabel?: string) => {
    setMergeSourceId(sourceId);
    setMergeSourceLabel(sourceLabel || sourceId);
    setSelectedMergeCips(new Set());
    setMergeSearchQuery('');
    setMergeDialogOpen(true);
  };
  
  const toggleMergeCip = (cipId: string) => {
    setSelectedMergeCips(prev => {
      const next = new Set(prev);
      if (next.has(cipId)) next.delete(cipId);
      else next.add(cipId);
      return next;
    });
  };
  
  const handleMergeInto = async () => {
    if (!mergeSourceId || selectedMergeCips.size === 0) return;
    setIsMerging(true);
    try {
      const baseUrl = getDuckDBApiUrl();
      const response = await fetch(`${baseUrl}/api/governance-lifecycle/overrides/merge`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sourcePrimaryId: mergeSourceId, mergeInto: Array.from(selectedMergeCips), reason: `Manual merge via UI: ${mergeSourceLabel}` }),
      });
      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(errorData.error || `Server error: ${response.status}`);
      }
      const targetDisplay = selectedMergeCips.size === 1 ? Array.from(selectedMergeCips)[0] : `${selectedMergeCips.size} CIPs`;
      toast({ title: "Merge saved", description: `Will be merged into ${targetDisplay}` });
      setMergeDialogOpen(false);
      setMergeSourceId(null);
      setMergeSourceLabel(null);
      setSelectedMergeCips(new Set());
      fetchData(false);
    } catch (err) {
      console.error('Failed to merge:', err);
      toast({ title: "Error", description: err instanceof Error ? err.message : "Failed to save merge override", variant: "destructive" });
    } finally {
      setIsMerging(false);
    }
  };
  
  const filteredMergeCips = useMemo(() => {
    if (!mergeSearchQuery.trim()) return cipList;
    const q = mergeSearchQuery.toLowerCase();
    return cipList.filter(cip => cip.primaryId.toLowerCase().includes(q));
  }, [cipList, mergeSearchQuery]);

  interface CardItem {
    id: string;
    primaryId: string;
    type: string;
    topicCount: number;
    preview: string;
  }
  const [cardList, setCardList] = useState<CardItem[]>([]);
  const [moveDialogOpen, setMoveDialogOpen] = useState(false);
  const [moveSourceId, setMoveSourceId] = useState<string | null>(null);
  const [moveSourceLabel, setMoveSourceLabel] = useState<string | null>(null);
  const [selectedTargetCard, setSelectedTargetCard] = useState<string | null>(null);
  const [moveSearchQuery, setMoveSearchQuery] = useState('');
  const [isMoving, setIsMoving] = useState(false);
  
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
  
  const openMoveDialog = (topicId: string, topicLabel?: string) => {
    setMoveSourceId(topicId);
    setMoveSourceLabel(topicLabel || topicId);
    setSelectedTargetCard(null);
    setMoveSearchQuery('');
    setMoveDialogOpen(true);
    fetchCardList();
  };
  
  const handleMoveToCard = async () => {
    if (!moveSourceId || !selectedTargetCard) return;
    setIsMoving(true);
    try {
      const baseUrl = getDuckDBApiUrl();
      const response = await fetch(`${baseUrl}/api/governance-lifecycle/overrides/move-topic`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ topicId: moveSourceId, targetCardId: selectedTargetCard, reason: `Manual move via UI: ${moveSourceLabel}` }),
      });
      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(errorData.error || `Server error: ${response.status}`);
      }
      const targetCard = cardList.find(c => c.id === selectedTargetCard || c.primaryId === selectedTargetCard);
      const targetName = targetCard?.primaryId || selectedTargetCard;
      toast({ title: "Topic moved", description: `Will be moved to "${targetName}"` });
      setMoveDialogOpen(false);
      setMoveSourceId(null);
      setMoveSourceLabel(null);
      setSelectedTargetCard(null);
      fetchData(false);
    } catch (err) {
      console.error('Failed to move topic:', err);
      toast({ title: "Error", description: err instanceof Error ? err.message : "Failed to move topic", variant: "destructive" });
    } finally {
      setIsMoving(false);
    }
  };
  
  const filteredCards = useMemo(() => {
    if (!moveSearchQuery.trim()) return cardList;
    const q = moveSearchQuery.toLowerCase();
    return cardList.filter(card => 
      card.primaryId.toLowerCase().includes(q) || 
      card.preview.toLowerCase().includes(q) ||
      card.type.toLowerCase().includes(q)
    );
  }, [cardList, moveSearchQuery]);

  const toggleItemSelection = (primaryId: string) => {
    setSelectedItems(prev => {
      const next = new Set(prev);
      if (next.has(primaryId)) next.delete(primaryId);
      else next.add(primaryId);
      return next;
    });
  };

  const selectAllVisible = () => {
    setSelectedItems(new Set(groupedRegularItems.map(g => g.primaryId)));
  };

  const clearSelection = () => setSelectedItems(new Set());

  const handleBulkReclassify = async (newType: LifecycleItem['type']) => {
    if (selectedItems.size === 0) return;
    try {
      const baseUrl = getDuckDBApiUrl();
      await Promise.all(Array.from(selectedItems).map(primaryId =>
        fetch(`${baseUrl}/api/governance-lifecycle/overrides`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ primaryId, type: newType, reason: 'Bulk reclassification via UI' }),
        })
      ));
      toast({ title: "Bulk reclassification complete", description: `${selectedItems.size} items updated to ${TYPE_CONFIG[newType].label}` });
      clearSelection();
      setBulkSelectMode(false);
      fetchData(false);
    } catch (err) {
      console.error('Failed to bulk reclassify:', err);
      toast({ title: "Error", description: "Failed to complete bulk reclassification", variant: "destructive" });
    }
  };

  useEffect(() => {
    fetchData();
    fetchCipList();
  }, []);

  useEffect(() => {
    if (scanVoteRequests) {
      const mapped = scanVoteRequests.map((vr: any) => ({
        contract_id: vr.contract_id || vr.contractId || '',
        payload: vr.payload || vr,
        record_time: vr.record_time || vr.created_at || '',
      }));
      setVoteRequests(mapped);
    }
  }, [scanVoteRequests]);

  const toggleExpand = (id: string) => {
    setExpandedIds(prev => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const formatDate = (dateStr: string) => {
    try {
      return new Date(dateStr).toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' });
    } catch { return dateStr; }
  };

  const matchesSearch = (item: LifecycleItem | Topic, query: string) => {
    if (!query.trim()) return true;
    const q = query.toLowerCase();
    if ('primaryId' in item) {
      const li = item as LifecycleItem;
      return (
        li.primaryId.toLowerCase().includes(q) ||
        li.topics.some(t => t.subject.toLowerCase().includes(q)) ||
        li.topics.some(t => t.identifiers.cipNumber?.toLowerCase().includes(q)) ||
        li.topics.some(t => t.identifiers.appName?.toLowerCase().includes(q)) ||
        li.topics.some(t => t.identifiers.validatorName?.toLowerCase().includes(q))
      );
    } else {
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

  const matchesDateRange = (item: LifecycleItem) => {
    if (!dateFrom && !dateTo) return true;
    const itemFirstDate = new Date(item.firstDate);
    const itemLastDate = new Date(item.lastDate);
    if (dateFrom && itemLastDate < dateFrom) return false;
    if (dateTo && itemFirstDate > dateTo) return false;
    return true;
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
          const matchingVotes = getVotesForItem(item);
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

  const filteredItems = useMemo(() => [...tbdItems, ...regularItems], [tbdItems, regularItems]);

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
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key)!.push(item);
    });

    const result: GroupedItem[] = [];
    groups.forEach((items) => {
      items.sort((a, b) => {
        if (a.network === 'mainnet' && b.network !== 'mainnet') return -1;
        if (a.network !== 'mainnet' && b.network === 'mainnet') return 1;
        return 0;
      });
      const allDates = items.flatMap(i => [new Date(i.firstDate), new Date(i.lastDate)]);
      const firstDate = new Date(Math.min(...allDates.map(d => d.getTime()))).toISOString();
      const lastDate = new Date(Math.max(...allDates.map(d => d.getTime()))).toISOString();
      result.push({
        primaryId: items[0].primaryId,
        type: items[0].type,
        items,
        hasMultipleNetworks: items.length > 1 && items.some(i => i.network === 'testnet') && items.some(i => i.network === 'mainnet'),
        firstDate,
        lastDate,
        totalTopics: items.reduce((sum, i) => sum + i.topics.length, 0),
      });
    });

    return result.sort((a, b) => new Date(b.lastDate).getTime() - new Date(a.lastDate).getTime());
  }, [regularItems]);

  const getTopicType = (topic: Topic): string => {
    const subjectTrimmed = topic.subject.trim();
    const isOutcome = /\bTokenomics\s+Outcomes\b/i.test(subjectTrimmed);
    const isProtocolUpgrade = /\b(?:synchronizer\s+migration|splice\s+\d+\.\d+|protocol\s+upgrade|network\s+upgrade|hard\s*fork|migration\s+to\s+splice)\b/i.test(subjectTrimmed);
    const isVoteProposal = /\bVote\s+Proposal\b/i.test(subjectTrimmed);
    const isValidatorOperations = /\bValidator\s+Operations\b/i.test(subjectTrimmed);
    const isCipVoteProposal = isVoteProposal && (/CIP[#\-\s]?\d+/i.test(subjectTrimmed) || /\bCIP\s+(?:vote|voting|approval)\b/i.test(subjectTrimmed));
    const isFeaturedAppVoteProposal = isVoteProposal && (/featured\s*app|featured\s*application|app\s+rights/i.test(subjectTrimmed) || /(?:mainnet|testnet|main\s*net|test\s*net):/i.test(subjectTrimmed));
    const isValidatorVoteProposal = isVoteProposal && /validator\s+(?:operator|onboarding|license)/i.test(subjectTrimmed);
    if (isOutcome) return 'outcome';
    if (isProtocolUpgrade) return 'protocol-upgrade';
    if (topic.flow === 'cip') return 'cip';
    if (topic.flow === 'featured-app') return 'featured-app';
    if (topic.flow === 'shared') {
      if (isCipVoteProposal || topic.identifiers.cipNumber || topic.identifiers.isCipDiscussion) return 'cip';
      if (isValidatorVoteProposal || isValidatorOperations || topic.identifiers.validatorName) return 'validator';
      if (isFeaturedAppVoteProposal || topic.identifiers.appName) return 'featured-app';
      if (isVoteProposal) return 'featured-app';
      return 'featured-app';
    }
    return 'featured-app';
  };

  const filteredTopics = useMemo(() => {
    if (!data) return [];
    return data.allTopics.filter(topic => {
      if (typeFilter !== 'all') {
        if (getTopicType(topic) !== typeFilter) return false;
      }
      if (stageFilter !== 'all' && topic.stage !== stageFilter) return false;
      if (!matchesSearch(topic, searchQuery)) return false;
      const topicDate = new Date(topic.date);
      if (dateFrom && topicDate < dateFrom) return false;
      if (dateTo && topicDate > dateTo) return false;
      return true;
    }).sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime());
  }, [data, typeFilter, stageFilter, searchQuery, dateFrom, dateTo]);

  const handleDatePreset = (preset: string) => {
    setDatePreset(preset);
    const now = new Date();
    switch (preset) {
      case 'all': setDateFrom(undefined); setDateTo(undefined); break;
      case 'this-month': setDateFrom(startOfMonth(now)); setDateTo(endOfMonth(now)); break;
      case 'last-month': setDateFrom(startOfMonth(subMonths(now, 1))); setDateTo(endOfMonth(subMonths(now, 1))); break;
      case 'last-3-months': setDateFrom(startOfMonth(subMonths(now, 2))); setDateTo(now); break;
      case 'last-6-months': setDateFrom(startOfMonth(subMonths(now, 5))); setDateTo(now); break;
      case 'this-year': setDateFrom(startOfYear(now)); setDateTo(endOfYear(now)); break;
      case 'last-year': setDateFrom(startOfYear(subYears(now, 1))); setDateTo(endOfYear(subYears(now, 1))); break;
    }
  };

  const clearDateFilter = () => { setDateFrom(undefined); setDateTo(undefined); setDatePreset('all'); };

  type TimelineEntry = 
    | { type: 'topic'; data: Topic; date: Date }
    | { type: 'vote-started'; data: VoteRequest; date: Date; cipRef: string | null }
    | { type: 'vote-ended'; data: VoteRequest; date: Date; cipRef: string | null; status: 'passed' | 'failed' | 'expired' };

  const timelineData = useMemo(() => {
    const entries: TimelineEntry[] = [];
    filteredTopics.forEach(topic => {
      entries.push({ type: 'topic', data: topic, date: new Date(topic.date) });
    });
    voteRequests.forEach(vr => {
      const cipMatch = (vr.payload?.reason?.body || '').match(/CIP[#\-\s]?0*(\d+)/i);
      const cipRef = cipMatch ? cipMatch[1] : null;
      const recordTime = vr.record_time ? new Date(vr.record_time) : null;
      const voteBefore = vr.payload?.voteBefore ? new Date(vr.payload.voteBefore) : null;
      if (recordTime) entries.push({ type: 'vote-started', data: vr, date: recordTime, cipRef });
      if (voteBefore && voteBefore < new Date()) {
        const votesRaw = vr.payload?.votes || [];
        let votesFor = 0;
        for (const vote of votesRaw) {
          const [, voteData] = Array.isArray(vote) ? vote : [vote.sv || "Unknown", vote];
          if ((voteData as any)?.accept === true) votesFor++;
        }
        const status: 'passed' | 'failed' | 'expired' = votesFor >= 10 ? 'passed' : votesRaw.length > 0 ? 'failed' : 'expired';
        entries.push({ type: 'vote-ended', data: vr, date: voteBefore, cipRef, status });
      }
    });
    if (entries.length === 0) return [];
    const monthGroups: Record<string, TimelineEntry[]> = {};
    entries.forEach(entry => {
      const monthKey = `${entry.date.getFullYear()}-${String(entry.date.getMonth() + 1).padStart(2, '0')}`;
      if (!monthGroups[monthKey]) monthGroups[monthKey] = [];
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

  const renderOnChainVoteCard = (item: OnChainVoteItem) => {
    const proposalParam = item.source === "history"
      ? item.historicalVote?.trackingCid?.slice(0, 12) || item.id
      : item.id;
    const linkUrl = item.source === "active"
      ? `/governance?tab=active&proposal=${proposalParam}`
      : `/governance?tab=scanapi&proposal=${proposalParam}`;
    const isExpired = item.voteBefore && item.voteBefore < new Date();
    return (
      <a key={`${item.source}-${item.id}`} href={linkUrl} className="block p-3 rounded-lg bg-pink-500/10 border border-pink-500/30 hover:border-pink-500/50 transition-colors">
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-2 flex-wrap">
              <Badge variant="outline" className={cn("text-[10px] h-5",
                item.status === 'approved' ? 'border-green-500/50 text-green-400 bg-green-500/10' :
                item.status === 'rejected' || item.status === 'expired' ? 'border-red-500/50 text-red-400 bg-red-500/10' :
                'border-yellow-500/50 text-yellow-400 bg-yellow-500/10'
              )}>
                {item.status === 'approved' ? '✓ Approved' : item.status === 'rejected' ? '✗ Rejected' : item.status === 'expired' ? '✗ Expired' : '⏳ In Progress'}
              </Badge>
              <Badge variant="outline" className={cn("text-[10px] h-5",
                item.source === 'active' ? 'border-blue-500/50 text-blue-400 bg-blue-500/10' : 'border-purple-500/50 text-purple-400 bg-purple-500/10'
              )}>
                {item.source === 'active' ? 'Active' : 'History'}
              </Badge>
            </div>
            <div className="flex items-center gap-3 mt-2 text-xs text-muted-foreground">
              <span className="flex items-center gap-1"><Vote className="h-3 w-3" />{item.votesFor} for / {item.votesAgainst} against ({item.totalVotes} total)</span>
              {item.voteBefore && (
                <span className="flex items-center gap-1"><Clock className="h-3 w-3" />{isExpired ? 'Ended' : `Due ${format(item.voteBefore, 'MMM d, yyyy')}`}</span>
              )}
            </div>
            {(item.reasonBody || item.reasonUrl) && (
              <div className="mt-2 p-2 rounded bg-background/30 border border-border/30">
                {item.reasonBody && <p className="text-xs text-muted-foreground break-words whitespace-pre-wrap mb-1">{item.reasonBody}</p>}
                {item.reasonUrl && (
                  <a href={item.reasonUrl} target="_blank" rel="noopener noreferrer" onClick={(e) => e.stopPropagation()} className="text-xs text-primary hover:underline break-all">
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

  const renderLifecycleProgress = (item: LifecycleItem) => {
    const stages = WORKFLOW_STAGES[item.type] || WORKFLOW_STAGES.other;
    const currentIdx = stages.indexOf(item.currentStage);
    const matchingVotes = getVotesForItem(item);

    return (
      <div className="flex items-center gap-1 flex-wrap">
        {stages.map((stage, idx) => {
          const isVoteStage = stage === 'sv-onchain-vote' || stage === 'sv-milestone';
          const stageVotes = isVoteStage ? matchingVotes.filter(v => v.stage === stage) : [];
          const hasStage = isVoteStage ? stageVotes.length > 0 : item.stages[stage] && item.stages[stage].length > 0;
          const isCurrent = stage === item.currentStage;
          const isPast = idx < currentIdx;
          const config = STAGE_CONFIG[stage];
          if (!config) return null;
          const Icon = config.icon;
          const count = isVoteStage ? stageVotes.length : item.stages[stage]?.length || 0;
          return (
            <div key={stage} className="flex items-center">
              <div
                className={`flex items-center gap-1.5 px-2 py-1 rounded-md text-xs font-medium transition-all ${hasStage ? config.color + ' border' : 'bg-muted/30 text-muted-foreground/50 border border-transparent'} ${isCurrent ? 'ring-1 ring-offset-1 ring-offset-background ring-primary/50' : ''}`}
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
      <div key={topic.id} className={cn("block p-3 rounded-lg bg-muted/30 border border-border/50 hover:border-primary/30 transition-colors", topic.sourceUrl ? "cursor-pointer hover:bg-muted/50" : "cursor-default")}>
        <div className="flex items-start justify-between gap-3">
          <a href={topic.sourceUrl || '#'} target="_blank" rel="noopener noreferrer" className="min-w-0 flex-1" onClick={(e) => !topic.sourceUrl && e.preventDefault()}>
            <h4 className="font-medium text-sm break-words">{topic.subject}</h4>
            <div className="flex flex-wrap items-center gap-2 mt-1 text-xs text-muted-foreground">
              <span className="flex items-center gap-1"><Calendar className="h-3 w-3" />{formatDate(topic.date)}</span>
              {showGroup && <Badge variant="outline" className="text-[10px] h-5">{topic.groupLabel}</Badge>}
              {topic.messageCount && topic.messageCount > 1 && <span>{topic.messageCount} msgs</span>}
            </div>
          </a>
          <div className="flex items-center gap-1 shrink-0">
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button variant="ghost" size="icon" className="h-7 w-7" onClick={(e) => e.stopPropagation()}>
                  <MoreVertical className="h-3.5 w-3.5" />
                </Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end" className="bg-popover w-56">
                <DropdownMenuSub>
                  <DropdownMenuSubTrigger className="text-xs"><Edit2 className="mr-2 h-3 w-3" />Reclassify as...</DropdownMenuSubTrigger>
                  <DropdownMenuSubContent className="bg-popover">
                    {Object.entries(TYPE_CONFIG).map(([typeKey, config]) => (
                      <DropdownMenuItem key={typeKey} disabled={typeKey === parentType} onClick={(e) => { e.stopPropagation(); handleReclassifyTopic(topic.id, topic.subject, typeKey as LifecycleItem['type']); }} className="text-xs">
                        <Badge className={cn("mr-2 text-[10px]", config.color)}>{config.label}</Badge>
                        {typeKey === parentType && <span className="text-muted-foreground">(current)</span>}
                      </DropdownMenuItem>
                    ))}
                  </DropdownMenuSubContent>
                </DropdownMenuSub>
                <DropdownMenuSeparator />
                <DropdownMenuItem onClick={(e) => { e.stopPropagation(); handleExtractTopic(topic.id, topic.subject); }} className="text-xs">
                  <SplitSquareVertical className="mr-2 h-3 w-3" />Extract to own card
                </DropdownMenuItem>
                {cipList.length > 0 && (
                  <DropdownMenuItem onClick={(e) => { e.stopPropagation(); e.preventDefault(); openMergeDialog(topic.id, topic.subject); }} className="text-xs">
                    <Merge className="mr-2 h-3 w-3" />Merge into CIP(s)...
                  </DropdownMenuItem>
                )}
                <DropdownMenuItem onClick={(e) => { e.stopPropagation(); e.preventDefault(); openMoveDialog(topic.id, topic.subject); }} className="text-xs">
                  <MoveRight className="mr-2 h-3 w-3" />Move to card...
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
            {topic.sourceUrl && (
              <a href={topic.sourceUrl} target="_blank" rel="noopener noreferrer" className="h-7 w-7 p-0 flex items-center justify-center text-muted-foreground hover:text-foreground">
                <ExternalLink className="h-3.5 w-3.5" />
              </a>
            )}
          </div>
        </div>
        {topic.excerpt && <p className="text-xs text-muted-foreground mt-2 break-words whitespace-pre-wrap">{topic.excerpt}</p>}
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
            <p className="text-muted-foreground mt-1">Track CIPs, Featured Apps, and Validators through the governance process</p>
            {cachedAt && <p className="text-xs text-muted-foreground mt-1">Last updated: {new Date(cachedAt).toLocaleString()}</p>}
          </div>
          <div className="flex gap-2 shrink-0">
            <Button onClick={() => fetchData(false)} disabled={isLoading || isRefreshing} variant="outline" className="gap-2">
              <RefreshCw className={`h-4 w-4 ${isLoading ? "animate-spin" : ""}`} />Load Cached
            </Button>
            <Button onClick={() => fetchData(true)} disabled={isLoading || isRefreshing} className="gap-2">
              <RefreshCw className={`h-4 w-4 ${isRefreshing ? "animate-spin" : ""}`} />Refresh from Groups.io
            </Button>
          </div>
        </div>

        {/* Active Votes Banner */}
        {voteRequests.length > 0 && (
          <Card className="border-pink-500/30 bg-pink-500/5">
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium flex items-center gap-2">
                <Vote className="h-4 w-4 text-pink-400" />
                <span>Active On-Chain Votes</span>
                <Badge className="bg-pink-500/20 text-pink-400 border-pink-500/30 ml-1">{voteRequests.length}</Badge>
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="flex flex-wrap gap-2">
                {voteRequests.slice(0, 8).map((vr, idx) => {
                  const reasonBody = vr.payload?.reason?.body || '';
                  const actionTag = vr.payload?.action?.tag || 'Unknown Action';
                  const label = reasonBody.slice(0, 60) || actionTag;
                  const proposalId = (vr.payload?.trackingCid || vr.contract_id)?.slice(0, 12) || 'unknown';
                  return (
                    <a key={vr.contract_id || idx} href={`/governance?tab=active&proposal=${proposalId}`} className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-md bg-pink-500/10 border border-pink-500/20 text-sm text-pink-300 hover:bg-pink-500/20 transition-colors">
                      <Vote className="h-3 w-3" />
                      <span className="truncate max-w-[200px]">{label}</span>
                      <ExternalLink className="h-3 w-3 opacity-50" />
                    </a>
                  );
                })}
                {voteRequests.length > 8 && (
                  <a href="/governance?tab=active" className="inline-flex items-center gap-1 px-3 py-1.5 rounded-md bg-muted/50 text-sm text-muted-foreground hover:bg-muted transition-colors">
                    +{voteRequests.length - 8} more<ArrowRight className="h-3 w-3" />
                  </a>
                )}
              </div>
            </CardContent>
          </Card>
        )}

        {/* Group Stats */}
        {data && data.stats?.groupCounts && Object.keys(data.stats.groupCounts).length > 0 && (
          <Card className="bg-muted/20">
            <CardHeader className="pb-3"><CardTitle className="text-sm font-medium">Topics by Group</CardTitle></CardHeader>
            <CardContent>
              <div className="flex flex-wrap gap-2">
                {Object.entries(data.stats.groupCounts).map(([group, count]) => (
                  <Badge key={group} variant="secondary" className="text-xs">{group}: {count}</Badge>
                ))}
              </div>
            </CardContent>
          </Card>
        )}

        {data?.stale && (
          <Card className="border-yellow-500/50 bg-yellow-500/10">
            <CardContent className="flex items-center gap-3 py-4">
              <AlertCircle className="h-5 w-5 text-yellow-500" />
              <span className="text-yellow-500">Showing cached data (refresh failed). Click "Refresh from Groups.io" to try again.</span>
            </CardContent>
          </Card>
        )}

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
          <Input placeholder="Search by subject, CIP number, app name, or validator..." value={searchQuery} onChange={(e) => setSearchQuery(e.target.value)} className="pl-10 pr-10" />
          {searchQuery && (
            <Button variant="ghost" size="sm" onClick={() => setSearchQuery('')} className="absolute right-1 top-1/2 -translate-y-1/2 h-7 w-7 p-0">
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
                <Button key={type} variant={typeFilter === type ? 'default' : 'outline'} size="sm" onClick={() => { setTypeFilter(type); setStageFilter('all'); }} className="h-7 text-xs">
                  {type === 'all' ? 'All' : TYPE_CONFIG[type as keyof typeof TYPE_CONFIG]?.label || type}
                </Button>
              ))}
            </div>
          </div>
          <div className="flex items-center gap-2">
            <span className="text-sm text-muted-foreground">Stage:</span>
            <div className="flex gap-1">
              {(() => {
                const stagesToShow = typeFilter === 'all' ? [] : WORKFLOW_STAGES[typeFilter as keyof typeof WORKFLOW_STAGES] || [];
                return ['all', ...stagesToShow].map(stage => (
                  <Button key={stage} variant={stageFilter === stage ? 'default' : 'outline'} size="sm" onClick={() => setStageFilter(stage)} className="h-7 text-xs">
                    {stage === 'all' ? 'All' : STAGE_CONFIG[stage]?.label || stage}
                  </Button>
                ));
              })()}
            </div>
          </div>
        </div>

        {/* Date Range Filter */}
        <div className="flex flex-wrap items-center gap-3">
          <div className="flex items-center gap-2">
            <CalendarIcon className="h-4 w-4 text-muted-foreground" />
            <span className="text-sm font-medium">Date Range:</span>
          </div>
          <Select value={datePreset} onValueChange={handleDatePreset}>
            <SelectTrigger className="w-[160px] h-8" style={{ backgroundColor: '#000', color: '#fff' }}>
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
          {datePreset === 'custom' && (
            <>
              <Popover>
                <PopoverTrigger asChild>
                  <Button variant="outline" size="sm" className={cn("h-8 justify-start text-left font-normal", !dateFrom && "text-muted-foreground")}>
                    <CalendarIcon className="mr-2 h-4 w-4" />{dateFrom ? format(dateFrom, "MMM d, yyyy") : "From"}
                  </Button>
                </PopoverTrigger>
                <PopoverContent className="w-auto p-0" align="start">
                  <CalendarComponent mode="single" selected={dateFrom} onSelect={setDateFrom} initialFocus className={cn("p-3 pointer-events-auto")} />
                </PopoverContent>
              </Popover>
              <span className="text-muted-foreground">to</span>
              <Popover>
                <PopoverTrigger asChild>
                  <Button variant="outline" size="sm" className={cn("h-8 justify-start text-left font-normal", !dateTo && "text-muted-foreground")}>
                    <CalendarIcon className="mr-2 h-4 w-4" />{dateTo ? format(dateTo, "MMM d, yyyy") : "To"}
                  </Button>
                </PopoverTrigger>
                <PopoverContent className="w-auto p-0" align="start">
                  <CalendarComponent mode="single" selected={dateTo} onSelect={setDateTo} initialFocus className={cn("p-3 pointer-events-auto")} />
                </PopoverContent>
              </Popover>
            </>
          )}
          {(dateFrom || dateTo) && (
            <Button variant="ghost" size="sm" onClick={clearDateFilter} className="h-8 px-2">
              <X className="h-4 w-4 mr-1" />Clear
            </Button>
          )}
          {datePreset !== 'all' && datePreset !== 'custom' && (
            <Badge variant="secondary" className="text-xs">
              {dateFrom && format(dateFrom, "MMM d, yyyy")} - {dateTo && format(dateTo, "MMM d, yyyy")}
            </Badge>
          )}
        </div>

        {/* Bulk Selection */}
        <div className="flex items-center gap-3 flex-wrap">
          <Button variant={bulkSelectMode ? "default" : "outline"} size="sm" onClick={() => { setBulkSelectMode(!bulkSelectMode); if (bulkSelectMode) clearSelection(); }} className="gap-2">
            {bulkSelectMode ? <CheckSquare className="h-4 w-4" /> : <Square className="h-4 w-4" />}
            {bulkSelectMode ? "Exit Bulk Select" : "Bulk Select"}
          </Button>
          {bulkSelectMode && (
            <>
              <Button variant="outline" size="sm" onClick={selectAllVisible}>Select All ({groupedRegularItems.length})</Button>
              <Button variant="outline" size="sm" onClick={clearSelection} disabled={selectedItems.size === 0}>Clear ({selectedItems.size})</Button>
              {selectedItems.size > 0 && (
                <DropdownMenu>
                  <DropdownMenuTrigger asChild>
                    <Button size="sm" className="gap-2"><Edit2 className="h-4 w-4" />Reclassify {selectedItems.size} items</Button>
                  </DropdownMenuTrigger>
                  <DropdownMenuContent align="start" className="bg-popover">
                    <DropdownMenuLabel className="text-xs">Reclassify selected as...</DropdownMenuLabel>
                    <DropdownMenuSeparator />
                    {Object.entries(TYPE_CONFIG).map(([typeKey, config]) => (
                      <DropdownMenuItem key={typeKey} onClick={() => handleBulkReclassify(typeKey as LifecycleItem['type'])} className="text-xs">
                        <Badge className={cn("mr-2 text-[10px]", config.color)}>{config.label}</Badge>
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
            <TabsTrigger value="learn" className="gap-1"><Lightbulb className="h-3 w-3" />Learn</TabsTrigger>
          </TabsList>

          {/* Lifecycle View */}
          <TabsContent value="lifecycle" className="space-y-3 mt-4">
            {isLoading ? (
              Array.from({ length: 5 }).map((_, i) => (
                <Card key={i}><CardHeader><Skeleton className="h-6 w-3/4" /><Skeleton className="h-4 w-1/2 mt-2" /></CardHeader></Card>
              ))
            ) : (groupedRegularItems.length + tbdItems.length) === 0 ? (
              <Card>
                <CardContent className="flex flex-col items-center justify-center py-12">
                  <FileText className="h-12 w-12 text-muted-foreground mb-4" />
                  <h3 className="text-lg font-medium">No Items Found</h3>
                  <p className="text-muted-foreground text-sm mt-1">Try adjusting your filters</p>
                </CardContent>
              </Card>
            ) : (
              <ScrollArea className="h-[calc(100vh-500px)] pr-4">
                <div className="space-y-3">
                  {/* Pending CIP Section */}
                  {tbdItems.length > 0 && (
                    <Card className="border-amber-500/30 bg-amber-500/5">
                      <CardHeader className="pb-3 cursor-pointer" onClick={() => toggleExpand('cip-00xx-section')}>
                        <div className="flex items-center justify-between">
                          <div className="space-y-1">
                            <div className="flex items-center gap-2">
                              <Badge className="bg-amber-500/20 text-amber-400 border border-amber-500/30">CIP-00XX</Badge>
                              <span className="text-sm text-muted-foreground">{tbdItems.reduce((sum, item) => sum + item.topics.length, 0)} topics</span>
                            </div>
                            <CardTitle className="text-base">Pending CIP Number Assignment</CardTitle>
                          </div>
                          {expandedIds.has('cip-00xx-section') ? <ChevronUp className="h-5 w-5 text-muted-foreground" /> : <ChevronDown className="h-5 w-5 text-muted-foreground" />}
                        </div>
                      </CardHeader>
                      {expandedIds.has('cip-00xx-section') && (
                        <CardContent className="pt-0 space-y-2">
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
                      <Card key={groupId} className={cn("hover:border-primary/30 transition-colors", isSelected && "ring-2 ring-primary/50 border-primary/50")}>
                        <CardHeader className="pb-3 cursor-pointer" onClick={() => !bulkSelectMode && toggleExpand(groupId)}>
                          <div className="flex items-start justify-between gap-2">
                            {bulkSelectMode && (
                              <div className="shrink-0 mr-3 cursor-pointer" onClick={(e) => { e.stopPropagation(); e.preventDefault(); toggleItemSelection(group.primaryId); }}>
                                <Checkbox checked={isSelected} className="pointer-events-none" />
                              </div>
                            )}
                            <div className="space-y-2 flex-1 min-w-0">
                              <div className="flex items-center gap-2 flex-wrap">
                                <Badge className={typeConfig.color}>{typeConfig.label}</Badge>
                                {group.hasMultipleNetworks ? (
                                  <Badge variant="outline" className="text-[10px] h-5 border-blue-500/50 text-blue-400 bg-blue-500/10">Testnet + Mainnet</Badge>
                                ) : group.items[0]?.network && (
                                  <Badge variant="outline" className={cn("text-[10px] h-5", group.items[0].network === 'mainnet' ? 'border-green-500/50 text-green-400 bg-green-500/10' : 'border-yellow-500/50 text-yellow-400 bg-yellow-500/10')}>
                                    {group.items[0].network}
                                  </Badge>
                                )}
                                <span className="text-sm text-muted-foreground">{group.totalTopics} topics</span>
                              </div>
                              <CardTitle className="text-base leading-normal break-words">
                                {group.items[0]?.topics[0]?.subject || group.primaryId}
                              </CardTitle>
                              <div className="flex items-center gap-1 text-xs text-muted-foreground">
                                <Calendar className="h-3 w-3" />{formatDate(group.firstDate)} → {formatDate(group.lastDate)}
                              </div>
                              <div className="pt-1">{renderLifecycleProgress(group.items[0])}</div>
                              {/* Active Vote Badge */}
                              {(() => {
                                const matchingVotes = getVotesForItem(group.items[0]);
                                const hasActiveVote = matchingVotes.some(v => v.source === 'active' && v.status === 'pending');
                                return hasActiveVote ? (
                                  <Badge className="bg-pink-500/20 text-pink-400 border border-pink-500/30 animate-pulse">🗳️ Active Vote</Badge>
                                ) : null;
                              })()}
                              {group.items[0]?.overrideApplied && (
                                <Badge variant="outline" className="text-[10px] h-5 border-purple-500/50 text-purple-400 bg-purple-500/10">✎ Manually classified</Badge>
                              )}
                              {group.items[0]?.llmClassified && !group.items[0]?.overrideApplied && (
                                <Badge variant="outline" className="text-[10px] h-5 border-cyan-500/50 text-cyan-400 bg-cyan-500/10">🤖 AI classified</Badge>
                              )}
                            </div>
                            <div className="flex items-center gap-1 shrink-0">
                              <DropdownMenu>
                                <DropdownMenuTrigger asChild>
                                  <Button variant="ghost" size="sm" className="h-7 w-7 p-0" onClick={(e) => e.stopPropagation()}>
                                    <MoreVertical className="h-4 w-4 text-muted-foreground" />
                                  </Button>
                                </DropdownMenuTrigger>
                                <DropdownMenuContent align="end" className="bg-popover w-56">
                                  <DropdownMenuLabel className="text-xs">Reclassify as...</DropdownMenuLabel>
                                  <DropdownMenuSeparator />
                                  {Object.entries(TYPE_CONFIG).map(([typeKey, config]) => (
                                    <DropdownMenuItem key={typeKey} disabled={typeKey === group.type} onClick={(e) => { e.stopPropagation(); handleReclassify(group.primaryId, typeKey as LifecycleItem['type']); }} className="text-xs">
                                      <Badge className={cn("mr-2 text-[10px]", config.color)}>{config.label}</Badge>
                                      {typeKey === group.type && <span className="text-muted-foreground">(current)</span>}
                                    </DropdownMenuItem>
                                  ))}
                                  {!group.primaryId.match(/^CIP-\d+$/i) && cipList.length > 0 && (
                                    <>
                                      <DropdownMenuSeparator />
                                      <DropdownMenuItem onClick={(e) => { e.stopPropagation(); openMergeDialog(group.primaryId); }} className="text-xs">
                                        <Merge className="mr-2 h-3 w-3" />Merge into CIP(s)...
                                      </DropdownMenuItem>
                                    </>
                                  )}
                                </DropdownMenuContent>
                              </DropdownMenu>
                              {isExpanded ? <ChevronUp className="h-5 w-5 text-muted-foreground" /> : <ChevronDown className="h-5 w-5 text-muted-foreground" />}
                            </div>
                          </div>
                        </CardHeader>

                        {isExpanded && (
                          <CardContent className="pt-0 space-y-3 border-t">
                            {group.items.map((item) => {
                              const stages = WORKFLOW_STAGES[item.type] || WORKFLOW_STAGES.other;
                              const matchingVotes = getVotesForItem(item);
                              return (
                                <div key={item.id} className="space-y-2 pt-3">
                                  {group.hasMultipleNetworks && (
                                    <Badge variant="outline" className={cn("text-xs mb-2", item.network === 'mainnet' ? 'border-green-500/50 text-green-400 bg-green-500/10' : 'border-yellow-500/50 text-yellow-400 bg-yellow-500/10')}>
                                      {item.network}
                                    </Badge>
                                  )}
                                  {stages.map(stage => {
                                    const isVoteStage = stage === 'sv-onchain-vote' || stage === 'sv-milestone';
                                    if (isVoteStage) {
                                      const stageVotes = matchingVotes.filter(v => v.stage === stage);
                                      if (stageVotes.length === 0) return null;
                                      const config = STAGE_CONFIG[stage];
                                      if (!config) return null;
                                      const Icon = config.icon;
                                      return (
                                        <div key={stage} className="space-y-2">
                                          <h4 className="text-sm font-medium flex items-center gap-2 text-muted-foreground"><Icon className="h-4 w-4" />{config.label} ({stageVotes.length})</h4>
                                          <div className="space-y-2 pl-6">
                                            {[...stageVotes].sort((a, b) => (b.voteBefore?.getTime() || 0) - (a.voteBefore?.getTime() || 0)).map(vote => renderOnChainVoteCard(vote))}
                                          </div>
                                        </div>
                                      );
                                    }
                                    const stageTopics = item.stages[stage];
                                    if (!stageTopics || stageTopics.length === 0) return null;
                                    const config = STAGE_CONFIG[stage];
                                    if (!config) return null;
                                    const Icon = config.icon;
                                    const sortedTopics = [...stageTopics].sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime());
                                    return (
                                      <div key={stage} className="space-y-2">
                                        <h4 className="text-sm font-medium flex items-center gap-2 text-muted-foreground"><Icon className="h-4 w-4" />{config.label} ({sortedTopics.length})</h4>
                                        <div className="space-y-2 pl-6">{sortedTopics.map(topic => renderTopicCard(topic, false, item.type))}</div>
                                      </div>
                                    );
                                  })}
                                  {/* Topics with unexpected stages */}
                                  {(() => {
                                    const unexpectedStageTopics = item.topics.filter(topic => !stages.includes(topic.stage));
                                    if (unexpectedStageTopics.length === 0) return null;
                                    const groupedByStage = unexpectedStageTopics.reduce((acc, topic) => {
                                      if (!acc[topic.stage]) acc[topic.stage] = [];
                                      acc[topic.stage].push(topic);
                                      return acc;
                                    }, {} as Record<string, Topic[]>);
                                    return Object.entries(groupedByStage).map(([stage, topics]) => {
                                      const config = STAGE_CONFIG[stage];
                                      const Icon = config?.icon || FileText;
                                      const label = config?.label || stage;
                                      const sortedTopics = [...topics].sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime());
                                      return (
                                        <div key={`extra-${stage}`} className="space-y-2">
                                          <h4 className="text-sm font-medium flex items-center gap-2 text-muted-foreground"><Icon className="h-4 w-4" />{label} ({sortedTopics.length})</h4>
                                          <div className="space-y-2 pl-6">{sortedTopics.map(topic => renderTopicCard(topic, false, item.type))}</div>
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
              Array.from({ length: 5 }).map((_, i) => <Skeleton key={i} className="h-32 w-full mb-4" />)
            ) : timelineData.length === 0 ? (
              <Card>
                <CardContent className="flex flex-col items-center justify-center py-12">
                  <Calendar className="h-12 w-12 text-muted-foreground mb-4" />
                  <h3 className="text-lg font-medium">No Timeline Data</h3>
                  {(dateFrom || dateTo) && <p className="text-sm text-muted-foreground mt-2">Try adjusting your date range filter</p>}
                </CardContent>
              </Card>
            ) : (
              <div className="relative">
                <div className="absolute left-4 top-0 bottom-0 w-0.5 bg-border" />
                <div className="space-y-6">
                  {timelineData.map((monthData) => (
                    <div key={monthData.month} className="relative pl-10">
                      <div className="absolute left-0 top-0 w-8 h-8 rounded-full bg-primary flex items-center justify-center">
                        <Calendar className="h-4 w-4 text-primary-foreground" />
                      </div>
                      <div className="space-y-3">
                        <h3 className="text-lg font-semibold sticky top-0 bg-background py-2 z-10">
                          {monthData.label}
                          <Badge variant="secondary" className="ml-2 text-xs">{monthData.topicCount} topics</Badge>
                          {monthData.voteCount > 0 && <Badge className="ml-2 text-xs bg-pink-500/20 text-pink-400 border border-pink-500/30">🗳️ {monthData.voteCount} votes</Badge>}
                        </h3>
                        <div className="space-y-2">
                          {monthData.entries.map((entry, idx) => {
                            if (entry.type === 'topic') {
                              const topic = entry.data;
                              const stageConfig = STAGE_CONFIG[topic.stage as keyof typeof STAGE_CONFIG];
                              const StageIcon = stageConfig?.icon || FileText;
                              return (
                                <div key={topic.id} className="flex gap-3 p-3 rounded-lg bg-muted/30 border border-border/50 hover:border-primary/30 transition-colors">
                                  <div className={`shrink-0 w-8 h-8 rounded-full flex items-center justify-center ${stageConfig?.color || 'bg-muted'}`}>
                                    <StageIcon className="h-4 w-4" />
                                  </div>
                                  <div className="flex-1 min-w-0">
                                    <div className="flex items-start justify-between gap-2">
                                      <div className="min-w-0">
                                        <h4 className="font-medium text-sm break-words">{topic.subject}</h4>
                                        <div className="flex flex-wrap items-center gap-2 mt-1 text-xs text-muted-foreground">
                                          <span>{formatDate(topic.date)}</span>
                                          <Badge variant="outline" className="text-[10px] h-5">{topic.groupLabel}</Badge>
                                          <Badge className={`text-[10px] h-5 ${stageConfig?.color || ''}`}>{stageConfig?.label || topic.stage}</Badge>
                                        </div>
                                      </div>
                                      {topic.sourceUrl && (
                                        <Button variant="ghost" size="sm" asChild className="h-7 w-7 p-0 shrink-0">
                                          <a href={topic.sourceUrl} target="_blank" rel="noopener noreferrer"><ExternalLink className="h-3.5 w-3.5" /></a>
                                        </Button>
                                      )}
                                    </div>
                                    {topic.excerpt && <p className="text-xs text-muted-foreground mt-2 break-words whitespace-pre-wrap">{topic.excerpt}</p>}
                                  </div>
                                </div>
                              );
                            }
                            const vr = entry.data;
                            const payload = vr.payload;
                            const reason = payload?.reason;
                            const voteBefore = payload?.voteBefore ? new Date(payload.voteBefore) : null;
                            const votesRaw = payload?.votes || [];
                            let votesFor = 0, votesAgainst = 0;
                            for (const vote of votesRaw) {
                              const [, voteData] = Array.isArray(vote) ? vote : [vote.sv || "Unknown", vote];
                              if ((voteData as any)?.accept === true) votesFor++;
                              else votesAgainst++;
                            }
                            const isStartEntry = entry.type === 'vote-started';
                            const statusColors = { passed: 'bg-green-500/20 border-green-500/50 text-green-400', failed: 'bg-red-500/20 border-red-500/50 text-red-400', expired: 'bg-yellow-500/20 border-yellow-500/50 text-yellow-400' };
                            const proposalId = (payload?.trackingCid || vr.contract_id)?.slice(0, 12) || 'unknown';
                            return (
                              <a key={`${entry.type}-${vr.contract_id}-${idx}`} href={`/governance?tab=active&proposal=${proposalId}`}
                                className={cn("flex gap-3 p-3 rounded-lg border transition-colors", isStartEntry ? "bg-pink-500/10 border-pink-500/30 hover:border-pink-500/50" : entry.type === 'vote-ended' && statusColors[entry.status])}>
                                <div className={cn("shrink-0 w-8 h-8 rounded-full flex items-center justify-center",
                                  isStartEntry ? "bg-pink-500/30" : entry.type === 'vote-ended' && entry.status === 'passed' ? "bg-green-500/30" : entry.type === 'vote-ended' && entry.status === 'failed' ? "bg-red-500/30" : "bg-yellow-500/30")}>
                                  {isStartEntry ? <Vote className="h-4 w-4 text-pink-400" /> : entry.type === 'vote-ended' && entry.status === 'passed' ? <CheckCircle2 className="h-4 w-4 text-green-400" /> : entry.type === 'vote-ended' && entry.status === 'failed' ? <XCircle className="h-4 w-4 text-red-400" /> : <Clock className="h-4 w-4 text-yellow-400" />}
                                </div>
                                <div className="flex-1 min-w-0">
                                  <div className="flex items-start justify-between gap-2">
                                    <div className="min-w-0">
                                      <h4 className="font-medium text-sm">
                                        {isStartEntry ? '🗳️ Vote Opened' : entry.type === 'vote-ended' && entry.status === 'passed' ? '✅ Vote Passed' : entry.type === 'vote-ended' && entry.status === 'failed' ? '❌ Vote Failed' : '⏳ Vote Expired'}
                                        {entry.cipRef && ` for CIP-${parseInt(entry.cipRef)}`}
                                      </h4>
                                      <div className="flex flex-wrap items-center gap-2 mt-1 text-xs text-muted-foreground">
                                        <span>{formatDate(entry.date.toISOString())}</span>
                                        <Badge variant="outline" className="text-[10px] h-5">On-chain Vote</Badge>
                                        <span className="flex items-center gap-1"><Vote className="h-3 w-3" />{votesFor} for / {votesAgainst} against</span>
                                        {isStartEntry && voteBefore && <span className="flex items-center gap-1 text-yellow-400"><Clock className="h-3 w-3" />Due {format(voteBefore, 'MMM d, yyyy')}</span>}
                                      </div>
                                    </div>
                                    <ExternalLink className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
                                  </div>
                                  {reason?.body && <p className="text-xs text-muted-foreground mt-2 break-words whitespace-pre-wrap">{reason.body}</p>}
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
              Array.from({ length: 10 }).map((_, i) => <Skeleton key={i} className="h-24 w-full" />)
            ) : filteredTopics.length === 0 ? (
              <Card>
                <CardContent className="flex flex-col items-center justify-center py-12">
                  <FileText className="h-12 w-12 text-muted-foreground mb-4" />
                  <h3 className="text-lg font-medium">No Topics Found</h3>
                </CardContent>
              </Card>
            ) : (
              <ScrollArea className="h-[600px]">
                <div className="space-y-2 pr-4">{filteredTopics.map(topic => renderTopicCard(topic, true))}</div>
              </ScrollArea>
            )}
          </TabsContent>
          
          {/* Learn View */}
          <TabsContent value="learn" className="mt-4 space-y-6">
            <GoldenSetManagementPanel />
            <LearnFromCorrectionsPanel />
          </TabsContent>
        </Tabs>
      </div>
      
      {/* Multi-CIP Merge Dialog */}
      <Dialog open={mergeDialogOpen} onOpenChange={setMergeDialogOpen}>
        <DialogContent className="max-w-md">
          <DialogHeader>
            <DialogTitle>Merge into CIP(s)</DialogTitle>
            <DialogDescription className="break-words">Select one or more CIPs to merge "{mergeSourceLabel?.slice(0, 80)}{(mergeSourceLabel?.length || 0) > 80 ? '...' : ''}" into.</DialogDescription>
          </DialogHeader>
          <div className="space-y-4">
            <div className="relative">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
              <Input placeholder="Search CIPs..." value={mergeSearchQuery} onChange={(e) => setMergeSearchQuery(e.target.value)} className="pl-9" />
            </div>
            {selectedMergeCips.size > 0 && <div className="text-sm text-muted-foreground">{selectedMergeCips.size} CIP{selectedMergeCips.size > 1 ? 's' : ''} selected</div>}
            <ScrollArea className="h-64 border rounded-md">
              <div className="p-2 space-y-1">
                {filteredMergeCips.map((cip) => (
                  <div key={cip.primaryId} onClick={() => toggleMergeCip(cip.primaryId)} className={cn("flex items-center gap-3 p-2 rounded-md cursor-pointer transition-colors", selectedMergeCips.has(cip.primaryId) ? "bg-primary/20 border border-primary/40" : "hover:bg-muted/50")}>
                    <Checkbox checked={selectedMergeCips.has(cip.primaryId)} onCheckedChange={() => toggleMergeCip(cip.primaryId)} />
                    <span className="font-mono text-sm">{cip.primaryId}</span>
                    <span className="text-xs text-muted-foreground ml-auto">{cip.topicCount} topics</span>
                  </div>
                ))}
                {filteredMergeCips.length === 0 && <div className="text-center text-sm text-muted-foreground py-4">No CIPs found</div>}
              </div>
            </ScrollArea>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setMergeDialogOpen(false)} disabled={isMerging}>Cancel</Button>
            <Button onClick={handleMergeInto} disabled={selectedMergeCips.size === 0 || isMerging}>
              {isMerging ? <><RefreshCw className="mr-2 h-4 w-4 animate-spin" />Saving...</> : `Merge into ${selectedMergeCips.size > 0 ? selectedMergeCips.size : ''} CIP${selectedMergeCips.size !== 1 ? 's' : ''}`}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
      
      {/* Move to Card Dialog */}
      <Dialog open={moveDialogOpen} onOpenChange={setMoveDialogOpen}>
        <DialogContent className="max-w-md">
          <DialogHeader>
            <DialogTitle>Move to Card</DialogTitle>
            <DialogDescription className="break-words">Select a card to move "{moveSourceLabel?.slice(0, 80)}{(moveSourceLabel?.length || 0) > 80 ? '...' : ''}" to.</DialogDescription>
          </DialogHeader>
          <div className="space-y-4">
            <div className="relative">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
              <Input placeholder="Search cards..." value={moveSearchQuery} onChange={(e) => setMoveSearchQuery(e.target.value)} className="pl-9" />
            </div>
            <ScrollArea className="h-64 border rounded-md">
              <div className="p-2 space-y-1">
                {filteredCards.map((card) => (
                  <div key={card.id} onClick={() => setSelectedTargetCard(card.id)} className={cn("flex items-start gap-3 p-2 rounded-md cursor-pointer transition-colors", selectedTargetCard === card.id ? "bg-primary/20 border border-primary/40" : "hover:bg-muted/50")}>
                    <div className={cn("h-4 w-4 mt-0.5 rounded-full border-2 flex items-center justify-center", selectedTargetCard === card.id ? "border-primary" : "border-muted-foreground")}>
                      {selectedTargetCard === card.id && <div className="h-2 w-2 rounded-full bg-primary" />}
                    </div>
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2">
                        <span className="font-medium text-sm">{card.primaryId}</span>
                        <Badge className={cn("text-[10px]", TYPE_CONFIG[card.type as keyof typeof TYPE_CONFIG]?.color || 'bg-muted')}>{TYPE_CONFIG[card.type as keyof typeof TYPE_CONFIG]?.label || card.type}</Badge>
                      </div>
                      {card.preview && <p className="text-xs text-muted-foreground truncate mt-0.5">{card.preview}</p>}
                    </div>
                    <span className="text-xs text-muted-foreground shrink-0">{card.topicCount} topics</span>
                  </div>
                ))}
                {filteredCards.length === 0 && <div className="text-center text-sm text-muted-foreground py-4">{cardList.length === 0 ? 'Loading cards...' : 'No cards found'}</div>}
              </div>
            </ScrollArea>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setMoveDialogOpen(false)} disabled={isMoving}>Cancel</Button>
            <Button onClick={handleMoveToCard} disabled={!selectedTargetCard || isMoving}>
              {isMoving ? <><RefreshCw className="mr-2 h-4 w-4 animate-spin" />Moving...</> : <><MoveRight className="mr-2 h-4 w-4" />Move to Card</>}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </DashboardLayout>
  );
};

export default GovernanceFlow;
