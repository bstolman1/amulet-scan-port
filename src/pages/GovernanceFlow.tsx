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
  Clock,
  Vote,
  ArrowRight,
  Filter,
  Search,
  X,
} from "lucide-react";
import { getDuckDBApiUrl } from "@/lib/backend-config";
import { cn } from "@/lib/utils";

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
  cip: ['cip-discuss', 'cip-vote', 'cip-announce', 'sv-announce', 'sv-onchain-vote'],
  'featured-app': ['tokenomics', 'tokenomics-announce', 'sv-announce'],
  validator: ['tokenomics', 'sv-announce'],
  'protocol-upgrade': ['tokenomics', 'sv-announce'],
  outcome: ['sv-announce'],
  other: ['tokenomics', 'sv-announce'],
};

// All possible stages with their display config
const STAGE_CONFIG: Record<string, { label: string; icon: typeof FileText; color: string }> = {
  // CIP stages
  'cip-discuss': { label: 'Discuss', icon: FileText, color: 'bg-blue-500/20 text-blue-400 border-blue-500/30' },
  'cip-vote': { label: 'Vote', icon: Vote, color: 'bg-purple-500/20 text-purple-400 border-purple-500/30' },
  'cip-announce': { label: 'Announce', icon: CheckCircle2, color: 'bg-green-500/20 text-green-400 border-green-500/30' },
  // On-chain vote stage
  'sv-onchain-vote': { label: 'On-Chain Vote', icon: Vote, color: 'bg-pink-500/20 text-pink-400 border-pink-500/30' },
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

// Helper to extract CIP number from VoteRequest reason
const extractCipReference = (voteRequest: VoteRequest): string | null => {
  const reason = voteRequest.payload?.reason;
  if (!reason) return null;
  
  // Check both body and url for CIP references
  const text = `${reason.body || ''} ${reason.url || ''}`;
  const match = text.match(/CIP[#\-\s]?0*(\d+)/i);
  if (match) {
    // Normalize to 4-digit format (e.g., "83" -> "0083")
    return match[1].padStart(4, '0');
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
  const [viewMode, setViewMode] = useState<'lifecycle' | 'all' | 'timeline'>('lifecycle');
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [dateFrom, setDateFrom] = useState<Date | undefined>(undefined);
  const [dateTo, setDateTo] = useState<Date | undefined>(undefined);
  const [datePreset, setDatePreset] = useState<string>('all');
  const [cachedAt, setCachedAt] = useState<string | null>(null);
  const [voteRequests, setVoteRequests] = useState<VoteRequest[]>([]);
  
  // Map CIP numbers to their active VoteRequests
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

  useEffect(() => {
    fetchData();
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

  // Separate CIP-00XX items from regular items
  const { tbdItems, regularItems } = useMemo(() => {
    if (!data) return { tbdItems: [], regularItems: [] };
    const allFiltered = data.lifecycleItems.filter(item => {
      if (typeFilter !== 'all' && item.type !== typeFilter) return false;
      if (stageFilter !== 'all' && item.currentStage !== stageFilter) return false;
      if (!matchesSearch(item, searchQuery)) return false;
      if (!matchesDateRange(item)) return false;
      return true;
    });
    
    return {
      tbdItems: allFiltered.filter(item => item.primaryId?.includes('00XX')),
      regularItems: allFiltered.filter(item => !item.primaryId?.includes('00XX')),
    };
  }, [data, typeFilter, stageFilter, searchQuery, dateFrom, dateTo]);

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

    // Sort: CIPs by CIP number descending, others by lastDate descending
    return result.sort((a, b) => {
      // CIPs sorted by CIP number (descending)
      if (a.type === 'cip' && b.type === 'cip') {
        const aNum = parseInt(a.primaryId.match(/CIP-?(\d+)/i)?.[1] || '0', 10);
        const bNum = parseInt(b.primaryId.match(/CIP-?(\d+)/i)?.[1] || '0', 10);
        return bNum - aNum;
      }
      // Everything else by lastDate descending
      return new Date(b.lastDate).getTime() - new Date(a.lastDate).getTime();
    });
  }, [regularItems]);

  // Determine the lifecycle type for a topic - should match backend logic
  const getTopicType = (topic: Topic): string => {
    const subjectTrimmed = topic.subject.trim();
    const isOutcome = /\bTokenomics\s+Outcomes\b/i.test(subjectTrimmed);
    const isVoteProposal = /\bVote\s+Proposal\b/i.test(subjectTrimmed);
    const isProtocolUpgrade = /\b(?:synchronizer\s+migration|splice\s+\d+\.\d+|protocol\s+upgrade|network\s+upgrade|hard\s*fork|migration\s+to\s+splice)\b/i.test(subjectTrimmed);
    
    if (isOutcome) return 'outcome';
    if (isProtocolUpgrade) return 'protocol-upgrade';
    if (topic.flow === 'cip') return 'cip';
    if (topic.flow === 'featured-app') return 'featured-app';
    if (topic.flow === 'shared') {
      // Shared groups need subject-line disambiguation (matching backend logic)
      if (isVoteProposal || topic.identifiers.cipNumber || topic.identifiers.isCipDiscussion) {
        return 'cip';
      }
      if (/\bValidator\s+Operations\b/i.test(subjectTrimmed) || topic.identifiers.validatorName) {
        return 'validator';
      }
      if (topic.identifiers.appName) {
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
  const timelineData = useMemo(() => {
    if (!filteredTopics.length) return [];
    
    const monthGroups: Record<string, Topic[]> = {};
    filteredTopics.forEach(topic => {
      const date = new Date(topic.date);
      const monthKey = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
      if (!monthGroups[monthKey]) {
        monthGroups[monthKey] = [];
      }
      monthGroups[monthKey].push(topic);
    });
    
    return Object.entries(monthGroups)
      .sort((a, b) => b[0].localeCompare(a[0]))
      .map(([month, topics]) => {
        // Parse month correctly - use explicit year/month to avoid timezone issues
        const [year, monthNum] = month.split('-').map(Number);
        const monthDate = new Date(year, monthNum - 1, 1); // month is 0-indexed
        return {
          month,
          label: monthDate.toLocaleDateString('en-US', { year: 'numeric', month: 'long' }),
          topics: topics.sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime()),
        };
      });
  }, [filteredTopics]);

  // Helper to render VoteRequest card
  const renderVoteRequestCard = (vr: VoteRequest) => {
    const payload = vr.payload;
    const actionTag = payload?.action?.tag || 'Unknown Action';
    const voteBefore = payload?.voteBefore ? new Date(payload.voteBefore) : null;
    const isExpired = voteBefore && voteBefore < new Date();
    const votesRaw = payload?.votes || [];
    const reason = payload?.reason;
    
    // Parse votes properly - votes can be [svName, voteData] tuples or objects
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
    
    // Get the proposal ID (first 12 chars of tracking CID or contract ID)
    const proposalId = payload?.trackingCid?.slice(0, 12) || vr.contract_id?.slice(0, 12) || 'unknown';
    
    // Determine status
    let status: 'pending' | 'approved' | 'rejected' = 'pending';
    // Assuming threshold of 10 for now (actual threshold would need to come from DSO rules)
    const threshold = 10;
    if (votesFor >= threshold) status = 'approved';
    else if (isExpired && votesFor < threshold) status = 'rejected';
    
    return (
      <a
        key={vr.contract_id}
        href={`/governance?proposal=${proposalId}`}
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
                {status === 'approved' ? '✓ Approved' : status === 'rejected' ? '✗ Rejected' : '⏳ Pending'}
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
            {reason?.body && (
              <p className="text-xs text-muted-foreground mt-2 line-clamp-2">
                {reason.body}
              </p>
            )}
          </div>
          <div className="h-7 w-7 p-0 shrink-0 flex items-center justify-center text-muted-foreground">
            <ExternalLink className="h-3.5 w-3.5" />
          </div>
        </div>
      </a>
    );
  };

  const renderLifecycleProgress = (item: LifecycleItem, cipNumber?: string) => {
    // Get the stages specific to this item's type
    const stages = WORKFLOW_STAGES[item.type] || WORKFLOW_STAGES.other;
    const currentIdx = stages.indexOf(item.currentStage);
    
    // Check if there are VoteRequests for this CIP
    const matchingVoteRequests = cipNumber ? cipVoteRequestMap.get(cipNumber) || [] : [];
    
    return (
      <div className="flex items-center gap-1 flex-wrap">
        {stages.map((stage, idx) => {
          // For sv-onchain-vote stage, check VoteRequests instead of topics
          const hasStage = stage === 'sv-onchain-vote' 
            ? matchingVoteRequests.length > 0
            : item.stages[stage] && item.stages[stage].length > 0;
          const isCurrent = stage === item.currentStage;
          const isPast = idx < currentIdx;
          const config = STAGE_CONFIG[stage];
          if (!config) return null;
          const Icon = config.icon;
          
          // Count for tooltip
          const count = stage === 'sv-onchain-vote' 
            ? matchingVoteRequests.length 
            : item.stages[stage]?.length || 0;
          
          return (
            <div key={stage} className="flex items-center">
              <div
                className={`flex items-center gap-1.5 px-2 py-1 rounded-md text-xs font-medium transition-all ${
                  hasStage 
                    ? config.color + ' border'
                    : 'bg-muted/30 text-muted-foreground/50 border border-transparent'
                } ${isCurrent ? 'ring-1 ring-offset-1 ring-offset-background ring-primary/50' : ''}`}
                title={`${config.label}: ${hasStage ? count + (stage === 'sv-onchain-vote' ? ' vote request(s)' : ' topics') : 'No activity'}`}
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

  const renderTopicCard = (topic: Topic, showGroup = true) => {
    return (
      <a 
        key={topic.id}
        href={topic.sourceUrl || '#'}
        target="_blank"
        rel="noopener noreferrer"
        className={cn(
          "block p-3 rounded-lg bg-muted/30 border border-border/50 hover:border-primary/30 transition-colors",
          topic.sourceUrl ? "cursor-pointer hover:bg-muted/50" : "cursor-default"
        )}
      >
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0 flex-1">
            <h4 className="font-medium text-sm">{topic.subject}</h4>
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
          </div>
          {topic.sourceUrl && (
            <div className="h-7 w-7 p-0 shrink-0 flex items-center justify-center text-muted-foreground">
              <ExternalLink className="h-3.5 w-3.5" />
            </div>
          )}
        </div>
        {topic.excerpt && (
          <p className="text-xs text-muted-foreground mt-2 line-clamp-2">
            {topic.excerpt}
          </p>
        )}
      </a>
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

        {/* Stats Overview */}
        {data && (
          <div className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-6 gap-3">
            <Card className="bg-muted/30">
              <CardContent className="p-4">
                <div className="text-2xl font-bold">{data.stats.totalTopics}</div>
                <div className="text-xs text-muted-foreground">Total Topics</div>
              </CardContent>
            </Card>
            <Card className="bg-muted/30">
              <CardContent className="p-4">
                <div className="text-2xl font-bold">{data.stats.lifecycleItems}</div>
                <div className="text-xs text-muted-foreground">Lifecycle Items</div>
              </CardContent>
            </Card>
            <Card className="bg-blue-500/10 border-blue-500/20">
              <CardContent className="p-4">
                <div className="text-2xl font-bold text-blue-400">{data.stats.byType.cip || 0}</div>
                <div className="text-xs text-muted-foreground">CIPs</div>
              </CardContent>
            </Card>
            <Card className="bg-emerald-500/10 border-emerald-500/20">
              <CardContent className="p-4">
                <div className="text-2xl font-bold text-emerald-400">{data.stats.byType['featured-app'] || 0}</div>
                <div className="text-xs text-muted-foreground">Featured Apps</div>
              </CardContent>
            </Card>
            <Card className="bg-orange-500/10 border-orange-500/20">
              <CardContent className="p-4">
                <div className="text-2xl font-bold text-orange-400">{data.stats.byType.validator || 0}</div>
                <div className="text-xs text-muted-foreground">Validators</div>
              </CardContent>
            </Card>
            <Card className="bg-cyan-500/10 border-cyan-500/20">
              <CardContent className="p-4">
                <div className="text-2xl font-bold text-cyan-400">{data.stats.byType['protocol-upgrade'] || 0}</div>
                <div className="text-xs text-muted-foreground">Protocol Upgrades</div>
              </CardContent>
            </Card>
            <Card className="bg-muted/30">
              <CardContent className="p-4">
                <div className="text-2xl font-bold">{Object.keys(data.groups).length}</div>
                <div className="text-xs text-muted-foreground">Groups</div>
              </CardContent>
            </Card>
          </div>
        )}

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

        {/* View Toggle */}
        <Tabs value={viewMode} onValueChange={(v) => setViewMode(v as 'lifecycle' | 'all' | 'timeline')}>
          <TabsList>
            <TabsTrigger value="lifecycle">Lifecycle ({groupedRegularItems.length + tbdItems.length})</TabsTrigger>
            <TabsTrigger value="timeline">Timeline ({timelineData.length} months)</TabsTrigger>
            <TabsTrigger value="all">All Topics ({filteredTopics.length})</TabsTrigger>
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
              <ScrollArea className="h-[calc(100vh-450px)] pr-4">
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
                          {tbdItems.flatMap(item => item.topics).map(topic => renderTopicCard(topic, true))}
                        </CardContent>
                      )}
                    </Card>
                  )}

                  {/* Grouped Items */}
                  {groupedRegularItems.map((group) => {
                    const groupId = `group-${group.primaryId.toLowerCase()}`;
                    const isExpanded = expandedIds.has(groupId);
                    const typeConfig = TYPE_CONFIG[group.type];
                    
                    return (
                      <Card key={groupId} className="hover:border-primary/30 transition-colors">
                        <CardHeader 
                          className="pb-3 cursor-pointer" 
                          onClick={() => toggleExpand(groupId)}
                        >
                          <div className="flex items-center justify-between">
                            <div className="space-y-2 flex-1">
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
                              
                              {/* Title */}
                              <CardTitle className="text-base leading-tight">
                                {group.primaryId}
                              </CardTitle>
                              
                              {/* Date Range */}
                              <div className="flex items-center gap-1 text-xs text-muted-foreground">
                                <Calendar className="h-3 w-3" />
                                {formatDate(group.firstDate)} → {formatDate(group.lastDate)}
                              </div>
                              
                              {/* Lifecycle Progress */}
                              <div className="pt-1">
                                {(() => {
                                  // Extract CIP number for VoteRequest lookup
                                  const cipMatch = group.primaryId.match(/CIP[#\-\s]?0*(\d+)/i);
                                  const cipNum = cipMatch ? cipMatch[1].padStart(4, '0') : undefined;
                                  return renderLifecycleProgress(group.items[0], cipNum);
                                })()}
                              </div>
                              
                              {/* Active Vote Badge */}
                              {(() => {
                                const cipMatch = group.primaryId.match(/CIP[#\-\s]?0*(\d+)/i);
                                const cipNum = cipMatch ? cipMatch[1].padStart(4, '0') : null;
                                const hasActiveVote = cipNum && cipVoteRequestMap.has(cipNum) && 
                                  cipVoteRequestMap.get(cipNum)!.some(vr => {
                                    const voteBefore = vr.payload?.voteBefore ? new Date(vr.payload.voteBefore) : null;
                                    return !voteBefore || voteBefore > new Date();
                                  });
                                return hasActiveVote ? (
                                  <Badge className="bg-pink-500/20 text-pink-400 border border-pink-500/30 animate-pulse">
                                    🗳️ Active Vote
                                  </Badge>
                                ) : null;
                              })()}
                            </div>
                            
                            {isExpanded ? (
                              <ChevronUp className="h-5 w-5 text-muted-foreground shrink-0" />
                            ) : (
                              <ChevronDown className="h-5 w-5 text-muted-foreground shrink-0" />
                            )}
                          </div>
                        </CardHeader>
                        
                        {isExpanded && (
                          <CardContent className="pt-0 space-y-3 border-t">
                            {group.items.map((item) => {
                              const stages = WORKFLOW_STAGES[item.type] || WORKFLOW_STAGES.other;
                              // Extract CIP number for this item
                              const cipMatch = group.primaryId.match(/CIP[#\-\s]?0*(\d+)/i);
                              const cipNum = cipMatch ? cipMatch[1].padStart(4, '0') : null;
                              const matchingVoteReqs = cipNum ? cipVoteRequestMap.get(cipNum) || [] : [];
                              
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
                                    // Handle sv-onchain-vote stage specially
                                    if (stage === 'sv-onchain-vote') {
                                      if (matchingVoteReqs.length === 0) return null;
                                      const config = STAGE_CONFIG[stage];
                                      if (!config) return null;
                                      const Icon = config.icon;
                                      
                                      return (
                                        <div key={stage} className="space-y-2">
                                          <h4 className="text-sm font-medium flex items-center gap-2 text-muted-foreground">
                                            <Icon className="h-4 w-4" />
                                            {config.label} ({matchingVoteReqs.length})
                                          </h4>
                                          <div className="space-y-2 pl-6">
                                            {matchingVoteReqs.map(vr => renderVoteRequestCard(vr))}
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
                                    
                                    return (
                                      <div key={stage} className="space-y-2">
                                        <h4 className="text-sm font-medium flex items-center gap-2 text-muted-foreground">
                                          <Icon className="h-4 w-4" />
                                          {config.label} ({stageTopics.length})
                                        </h4>
                                        <div className="space-y-2 pl-6">
                                          {stageTopics.map(topic => renderTopicCard(topic))}
                                        </div>
                                      </div>
                                    );
                                  })}
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
                            {monthData.topics.length} topics
                          </Badge>
                        </h3>
                        
                        <div className="space-y-2">
                          {monthData.topics.map((topic) => {
                            const stageConfig = STAGE_CONFIG[topic.stage as keyof typeof STAGE_CONFIG];
                            const StageIcon = stageConfig?.icon || FileText;
                            
                            return (
                              <div 
                                key={topic.id}
                                className="flex gap-3 p-3 rounded-lg bg-muted/30 border border-border/50 hover:border-primary/30 transition-colors"
                              >
                                {/* Stage indicator */}
                                <div className={`shrink-0 w-8 h-8 rounded-full flex items-center justify-center ${stageConfig?.color || 'bg-muted'}`}>
                                  <StageIcon className="h-4 w-4" />
                                </div>
                                
                                <div className="flex-1 min-w-0">
                                  <div className="flex items-start justify-between gap-2">
                                    <div className="min-w-0">
                                      <h4 className="font-medium text-sm">{topic.subject}</h4>
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
                                    <p className="text-xs text-muted-foreground mt-2 line-clamp-2">
                                      {topic.excerpt}
                                    </p>
                                  )}
                                </div>
                              </div>
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
        </Tabs>

      </div>
    </DashboardLayout>
  );
};

export default GovernanceFlow;
