import { useState, useEffect, useMemo } from "react";
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
}

interface LifecycleItem {
  id: string;
  primaryId: string;
  type: 'cip' | 'featured-app' | 'validator' | 'other';
  stages: Record<string, Topic[]>;
  topics: Topic[];
  firstDate: string;
  lastDate: string;
  currentStage: string;
  expectedStages?: string[];
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
}

const STAGE_CONFIG: Record<string, { label: string; icon: typeof FileText; color: string }> = {
  discuss: { label: 'Discuss', icon: FileText, color: 'bg-blue-500/20 text-blue-400 border-blue-500/30' },
  vote: { label: 'Vote', icon: Vote, color: 'bg-purple-500/20 text-purple-400 border-purple-500/30' },
  announce: { label: 'Announce', icon: CheckCircle2, color: 'bg-green-500/20 text-green-400 border-green-500/30' },
  tokenomics: { label: 'Tokenomics', icon: Vote, color: 'bg-indigo-500/20 text-indigo-400 border-indigo-500/30' },
  'tokenomics-announce': { label: 'Tokenomics Announce', icon: CheckCircle2, color: 'bg-teal-500/20 text-teal-400 border-teal-500/30' },
  'sv-vote': { label: 'SV Vote', icon: Vote, color: 'bg-amber-500/20 text-amber-400 border-amber-500/30' },
  'sv-announce': { label: 'SV Announce', icon: CheckCircle2, color: 'bg-emerald-500/20 text-emerald-400 border-emerald-500/30' },
  'weight-update': { label: 'Weight Update', icon: Clock, color: 'bg-orange-500/20 text-orange-400 border-orange-500/30' },
};

// Different lifecycle stages for different entity types
const LIFECYCLE_STAGES_BY_TYPE: Record<string, string[]> = {
  'cip': ['discuss', 'vote', 'announce', 'sv-vote', 'sv-announce', 'weight-update'],
  'featured-app': ['tokenomics', 'tokenomics-announce', 'sv-announce'],
  'validator': ['tokenomics', 'tokenomics-announce'],
  'other': ['discuss', 'vote', 'announce', 'tokenomics', 'tokenomics-announce', 'sv-vote', 'sv-announce'],
};

const TYPE_CONFIG = {
  cip: { label: 'CIP', color: 'bg-primary/20 text-primary' },
  'featured-app': { label: 'Featured App', color: 'bg-emerald-500/20 text-emerald-400' },
  validator: { label: 'Validator', color: 'bg-orange-500/20 text-orange-400' },
  other: { label: 'Other', color: 'bg-muted text-muted-foreground' },
};

const GovernanceFlow = () => {
  const [data, setData] = useState<GovernanceData | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expandedIds, setExpandedIds] = useState<Set<string>>(new Set());
  const [typeFilter, setTypeFilter] = useState<string>('all');
  const [stageFilter, setStageFilter] = useState<string>('all');
  const [viewMode, setViewMode] = useState<'lifecycle' | 'all' | 'timeline'>('lifecycle');
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [dateFrom, setDateFrom] = useState<Date | undefined>(undefined);
  const [dateTo, setDateTo] = useState<Date | undefined>(undefined);
  const [datePreset, setDatePreset] = useState<string>('all');

  const fetchData = async () => {
    setIsLoading(true);
    setError(null);
    
    try {
      const baseUrl = getDuckDBApiUrl();
      const response = await fetch(`${baseUrl}/api/governance-lifecycle`);
      
      if (!response.ok) {
        throw new Error(`Failed to fetch: ${response.status}`);
      }
      
      const result = await response.json();
      
      if (result.error) {
        throw new Error(result.error);
      }
      
      setData(result);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch governance data');
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
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

  const filteredItems = useMemo(() => {
    if (!data) return [];
    return data.lifecycleItems.filter(item => {
      if (typeFilter !== 'all' && item.type !== typeFilter) return false;
      if (stageFilter !== 'all' && item.currentStage !== stageFilter) return false;
      if (!matchesSearch(item, searchQuery)) return false;
      return true;
    });
  }, [data, typeFilter, stageFilter, searchQuery]);

  const filteredTopics = useMemo(() => {
    if (!data) return [];
    return data.allTopics.filter(topic => {
      if (typeFilter !== 'all') {
        const itemType = topic.identifiers.cipNumber ? 'cip' :
                        topic.identifiers.appName ? 'featured-app' :
                        topic.identifiers.validatorName ? 'validator' : 'other';
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
      .map(([month, topics]) => ({
        month,
        label: new Date(month + '-01').toLocaleDateString('en-US', { year: 'numeric', month: 'long' }),
        topics: topics.sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime()),
      }));
  }, [filteredTopics]);

  const renderLifecycleProgress = (item: LifecycleItem) => {
    // Only show stages that this item actually has data for (plus the expected next ones)
    const expectedStages = item.expectedStages || LIFECYCLE_STAGES_BY_TYPE[item.type] || LIFECYCLE_STAGES_BY_TYPE['other'];
    // Get stages with activity
    const activeStages = expectedStages.filter(stage => item.stages[stage] && item.stages[stage].length > 0);
    // Find the current stage index in expected stages
    const currentIdx = expectedStages.indexOf(item.currentStage);
    // Show: all active stages + one pending stage after current (if any)
    const nextStageIdx = currentIdx + 1;
    const nextStage = nextStageIdx < expectedStages.length ? expectedStages[nextStageIdx] : null;
    
    // Build the stages to display: active ones + optionally the next pending one
    const stagesToDisplay = [...activeStages];
    if (nextStage && !stagesToDisplay.includes(nextStage)) {
      stagesToDisplay.push(nextStage);
    }
    // Keep them in expected order
    const orderedStages = expectedStages.filter(s => stagesToDisplay.includes(s));
    
    return (
      <div className="flex items-center gap-1 flex-wrap">
        {orderedStages.map((stage, idx) => {
          const hasStage = item.stages[stage] && item.stages[stage].length > 0;
          const isCurrent = stage === item.currentStage;
          const config = STAGE_CONFIG[stage];
          
          if (!config) return null;
          
          const Icon = config.icon;
          
          return (
            <div key={stage} className="flex items-center">
              <div
                className={`flex items-center gap-1.5 px-2 py-1 rounded-md text-xs font-medium transition-all ${
                  hasStage 
                    ? config.color + ' border'
                    : 'bg-muted/30 text-muted-foreground/50 border border-transparent'
                } ${isCurrent ? 'ring-1 ring-offset-1 ring-offset-background ring-primary/50' : ''}`}
                title={`${config.label}: ${hasStage ? item.stages[stage].length + ' topics' : 'Pending'}`}
              >
                <Icon className="h-3 w-3" />
                <span className="hidden sm:inline">{config.label}</span>
                {hasStage && <span className="text-[10px] opacity-70">({item.stages[stage].length})</span>}
              </div>
              {idx < orderedStages.length - 1 && (
                <ArrowRight className={`h-3 w-3 mx-0.5 ${hasStage || isCurrent ? 'text-primary/50' : 'text-muted-foreground/30'}`} />
              )}
            </div>
          );
        })}
      </div>
    );
  };

  const renderTopicCard = (topic: Topic, showGroup = true) => (
    <div 
      key={topic.id}
      className="p-3 rounded-lg bg-muted/30 border border-border/50 hover:border-primary/30 transition-colors"
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
  );

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
          </div>
          <Button 
            onClick={fetchData} 
            disabled={isLoading}
            className="gap-2 shrink-0"
          >
            <RefreshCw className={`h-4 w-4 ${isLoading ? "animate-spin" : ""}`} />
            Refresh
          </Button>
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
              {['all', 'cip', 'featured-app', 'validator', 'other'].map(type => (
                <Button
                  key={type}
                  variant={typeFilter === type ? 'default' : 'outline'}
                  size="sm"
                  onClick={() => {
                    setTypeFilter(type);
                    setStageFilter('all'); // Reset stage when type changes
                  }}
                  className="h-7 text-xs"
                >
                  {type === 'all' ? 'All' : TYPE_CONFIG[type as keyof typeof TYPE_CONFIG]?.label || type}
                </Button>
              ))}
            </div>
          </div>
          
          {/* Only show stage filters for specific types (cip, featured-app, validator) */}
          {typeFilter !== 'all' && typeFilter !== 'other' && (
            <div className="flex items-center gap-2">
              <span className="text-sm text-muted-foreground">Stage:</span>
              <div className="flex gap-1 flex-wrap">
                <Button
                  variant={stageFilter === 'all' ? 'default' : 'outline'}
                  size="sm"
                  onClick={() => setStageFilter('all')}
                  className="h-7 text-xs"
                >
                  All
                </Button>
                {(LIFECYCLE_STAGES_BY_TYPE[typeFilter] || []).map(stage => {
                  const config = STAGE_CONFIG[stage];
                  if (!config) return null;
                  return (
                    <Button
                      key={stage}
                      variant={stageFilter === stage ? 'default' : 'outline'}
                      size="sm"
                      onClick={() => setStageFilter(stage)}
                      className="h-7 text-xs"
                    >
                      {config.label}
                    </Button>
                  );
                })}
              </div>
            </div>
          )}
        </div>

        {/* View Toggle */}
        <Tabs value={viewMode} onValueChange={(v) => setViewMode(v as 'lifecycle' | 'all' | 'timeline')}>
          <TabsList>
            <TabsTrigger value="lifecycle">Lifecycle View ({filteredItems.length})</TabsTrigger>
            <TabsTrigger value="timeline">Timeline ({timelineData.length} months)</TabsTrigger>
            <TabsTrigger value="all">All Topics ({filteredTopics.length})</TabsTrigger>
          </TabsList>

          {/* Lifecycle View */}
          <TabsContent value="lifecycle" className="space-y-4 mt-4">
            {isLoading ? (
              Array.from({ length: 5 }).map((_, i) => (
                <Card key={i}>
                  <CardHeader>
                    <Skeleton className="h-6 w-3/4" />
                    <Skeleton className="h-4 w-1/2 mt-2" />
                  </CardHeader>
                  <CardContent>
                    <Skeleton className="h-8 w-full" />
                  </CardContent>
                </Card>
              ))
            ) : filteredItems.length === 0 ? (
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
              filteredItems.map((item) => {
                const isExpanded = expandedIds.has(item.id);
                const typeConfig = TYPE_CONFIG[item.type];
                
                return (
                  <Card key={item.id} className="hover:border-primary/30 transition-colors">
                    <CardHeader className="pb-3">
                      <div className="flex items-start justify-between gap-4">
                        <div className="space-y-2 flex-1">
                          <Badge className={typeConfig.color}>
                            {typeConfig.label}
                          </Badge>
                          <CardTitle className="text-lg leading-tight break-words">
                            {item.primaryId}
                          </CardTitle>
                          <div className="flex items-center gap-3 text-sm text-muted-foreground">
                            <span className="flex items-center gap-1">
                              <Calendar className="h-3.5 w-3.5" />
                              {formatDate(item.firstDate)} → {formatDate(item.lastDate)}
                            </span>
                            <span>{item.topics.length} topics</span>
                          </div>
                        </div>
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => toggleExpand(item.id)}
                          className="shrink-0"
                        >
                          {isExpanded ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
                        </Button>
                      </div>
                      
                      {/* Lifecycle Progress */}
                      <div className="pt-2">
                        {renderLifecycleProgress(item)}
                      </div>
                    </CardHeader>
                    
                    {isExpanded && (
                      <CardContent className="space-y-4 border-t pt-4">
                        {/* Only show stages that have topics */}
                        {Object.keys(item.stages)
                          .filter(stage => item.stages[stage] && item.stages[stage].length > 0)
                          .sort((a, b) => {
                            // Sort by expected stage order
                            const expectedOrder = item.expectedStages || LIFECYCLE_STAGES_BY_TYPE[item.type] || [];
                            return expectedOrder.indexOf(a) - expectedOrder.indexOf(b);
                          })
                          .map((stage) => {
                            const config = STAGE_CONFIG[stage];
                            if (!config) return null;
                            
                            const stageTopics = item.stages[stage];
                            const Icon = config.icon;
                            
                            return (
                              <div key={stage} className="space-y-2">
                                <h4 className="text-sm font-medium flex items-center gap-2">
                                  <Icon className="h-4 w-4" />
                                  {config.label} ({stageTopics.length})
                                </h4>
                                <div className="space-y-2 pl-6">
                                  {stageTopics.map(topic => renderTopicCard(topic))}
                                </div>
                              </div>
                            );
                          })}
                      </CardContent>
                    )}
                  </Card>
                );
              })
            )}
          </TabsContent>

          {/* Timeline View */}
          <TabsContent value="timeline" className="mt-4 space-y-4">
            {/* Date Range Filter */}
            <Card className="bg-muted/20">
              <CardContent className="p-4">
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
              </CardContent>
            </Card>

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

        {/* Info Card */}
        <Card className="bg-muted/30 border-dashed">
          <CardContent className="py-4">
            <p className="text-sm text-muted-foreground">
              <strong>Governance Lifecycle:</strong> Tracks items through Proposal → Review → Vote → Result stages 
              across multiple Groups.io mailing lists. Items are correlated by CIP numbers, app/validator names, 
              subject similarity, and date proximity.
            </p>
          </CardContent>
        </Card>
      </div>
    </DashboardLayout>
  );
};

export default GovernanceFlow;
