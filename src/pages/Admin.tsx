import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useState, useEffect } from "react";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Trash2, Plus, Database } from "lucide-react";
import { supabase } from "@/integrations/supabase/client";
import { useToast } from "@/hooks/use-toast";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";

interface SVVote {
  organization: string;
  email: string;
  contact: string;
  weight: number;
  vote: "yes" | "no" | "abstain" | "";
}

interface CommitteeVote {
  member: string;
  email: string;
  contact: string;
  weight: number;
  vote: "yes" | "no" | "abstain" | "";
}

interface FeaturedAppCommitteeVote {
  member: string;
  email: string;
  contact: string;
  weight: number;
  vote: "yes" | "no" | "abstain" | "";
}

const Admin = () => {
  const { toast } = useToast();

  // CIP-related state
  const [cipNumber, setCipNumber] = useState("");
  const [cipTitle, setCipTitle] = useState("");
  const [voteStart, setVoteStart] = useState("");
  const [voteClose, setVoteClose] = useState("");
  const [githubLink, setGithubLink] = useState("");
  const [requiresOnchainVote, setRequiresOnchainVote] = useState(false);
  const [cipType, setCipType] = useState("");
  const [cipTypes, setCipTypes] = useState<{ id: string; type_name: string }[]>([]);
  const [newTypeName, setNewTypeName] = useState("");
  const [isAddTypeDialogOpen, setIsAddTypeDialogOpen] = useState(false);
  const [currentCipId, setCurrentCipId] = useState<string | null>(null);
  const [committeeVotesSaved, setCommitteeVotesSaved] = useState(false);

  const [svVotes, setSvVotes] = useState<SVVote[]>([{ organization: "", email: "", contact: "", weight: 0, vote: "" }]);

  const [committeeVotes, setCommitteeVotes] = useState<CommitteeVote[]>([
    { member: "", email: "", contact: "", weight: 1, vote: "" },
  ]);

  // Featured App-related state
  const [featuredAppName, setFeaturedAppName] = useState("");
  const [featuredAppDescription, setFeaturedAppDescription] = useState("");
  const [currentFeaturedAppId, setCurrentFeaturedAppId] = useState<string | null>(null);

  const [featuredAppCommitteeVotes, setFeaturedAppCommitteeVotes] = useState<FeaturedAppCommitteeVote[]>([
    { member: "", email: "", contact: "", weight: 1, vote: "" },
  ]);

  // History state
  const [cipHistory, setCipHistory] = useState<any[]>([]);
  const [featuredAppHistory, setFeaturedAppHistory] = useState<any[]>([]);

  // Fetch CIP types on mount
  useEffect(() => {
    fetchCipTypes();
    fetchCipHistory();
    fetchFeaturedAppHistory();
  }, []);

  const fetchCipTypes = async () => {
    try {
      const { data, error } = await supabase.from("cip_types").select("*").order("type_name");

      if (error) throw error;
      setCipTypes(data || []);
    } catch (error) {
      console.error("Error fetching CIP types:", error);
    }
  };

  const fetchCipHistory = async () => {
    try {
      const { data: cips, error: cipsError } = await supabase
        .from("cips")
        .select("*")
        .order("created_at", { ascending: false });

      if (cipsError) throw cipsError;

      const history = await Promise.all(
        (cips || []).map(async (cip) => {
          const [committeeVotesRes, svVotesRes] = await Promise.all([
            supabase.from("committee_votes").select("*").eq("cip_id", cip.id),
            supabase.from("sv_votes").select("*").eq("cip_id", cip.id),
          ]);

          return {
            ...cip,
            committeeVotes: committeeVotesRes.data || [],
            svVotes: svVotesRes.data || [],
          };
        }),
      );

      setCipHistory(history);
    } catch (error) {
      console.error("Error fetching CIP history:", error);
    }
  };

  const fetchFeaturedAppHistory = async () => {
    try {
      const { data: apps, error: appsError } = await supabase
        .from("featured_app_votes")
        .select("*")
        .order("created_at", { ascending: false });

      if (appsError) throw appsError;

      const history = await Promise.all(
        (apps || []).map(async (app) => {
          const { data: votes } = await supabase
            .from("featured_app_committee_votes")
            .select("*")
            .eq("featured_app_id", app.id);

          return {
            ...app,
            votes: votes || [],
          };
        }),
      );

      setFeaturedAppHistory(history);
    } catch (error) {
      console.error("Error fetching featured app history:", error);
    }
  };

  const handleAddNewType = async () => {
    if (!newTypeName.trim()) {
      toast({
        title: "Error",
        description: "Please enter a type name",
        variant: "destructive",
      });
      return;
    }

    try {
      const { error } = await supabase.from("cip_types").insert({ type_name: newTypeName.trim() });

      if (error) throw error;

      toast({
        title: "Success",
        description: "New CIP type added successfully",
      });

      setNewTypeName("");
      setIsAddTypeDialogOpen(false);
      fetchCipTypes();
    } catch (error) {
      toast({
        title: "Error",
        description: "Failed to add new CIP type",
        variant: "destructive",
      });
    }
  };

  const addSvRow = () => {
    setSvVotes([...svVotes, { organization: "", email: "", contact: "", weight: 0, vote: "" }]);
  };

  const removeSvRow = (index: number) => {
    setSvVotes(svVotes.filter((_, i) => i !== index));
  };

  const updateSvVote = (index: number, field: keyof SVVote, value: string | number) => {
    const updated = [...svVotes];
    updated[index] = { ...updated[index], [field]: value };
    setSvVotes(updated);
  };

  const addCommitteeRow = () => {
    setCommitteeVotes([...committeeVotes, { member: "", email: "", contact: "", weight: 1, vote: "" }]);
  };

  const removeCommitteeRow = (index: number) => {
    setCommitteeVotes(committeeVotes.filter((_, i) => i !== index));
  };

  const updateCommitteeVote = (index: number, field: keyof CommitteeVote, value: string | number) => {
    const updated = [...committeeVotes];
    updated[index] = { ...updated[index], [field]: value };
    setCommitteeVotes(updated);
  };

  const addFeaturedAppCommitteeRow = () => {
    setFeaturedAppCommitteeVotes([
      ...featuredAppCommitteeVotes,
      { member: "", email: "", contact: "", weight: 1, vote: "" },
    ]);
  };

  const removeFeaturedAppCommitteeRow = (index: number) => {
    setFeaturedAppCommitteeVotes(featuredAppCommitteeVotes.filter((_, i) => i !== index));
  };

  const updateFeaturedAppCommitteeVote = (
    index: number,
    field: keyof FeaturedAppCommitteeVote,
    value: string | number,
  ) => {
    const updated = [...featuredAppCommitteeVotes];
    updated[index] = { ...updated[index], [field]: value };
    setFeaturedAppCommitteeVotes(updated);
  };

  const calculateSVResult = () => {
    const totalWeight = svVotes.reduce((sum, v) => sum + (v.weight || 0), 0);
    const yesWeight = svVotes.filter((v) => v.vote === "yes").reduce((sum, v) => sum + (v.weight || 0), 0);
    const noWeight = svVotes.filter((v) => v.vote === "no").reduce((sum, v) => sum + (v.weight || 0), 0);
    const abstainWeight = svVotes.filter((v) => v.vote === "abstain").reduce((sum, v) => sum + (v.weight || 0), 0);

    const yesPercentage = totalWeight > 0 ? (yesWeight / totalWeight) * 100 : 0;
    const passed = yesPercentage >= 66.67;

    return { totalWeight, yesWeight, noWeight, abstainWeight, yesPercentage, passed };
  };

  const calculateCommitteeResult = () => {
    const totalWeight = committeeVotes.reduce((sum, v) => sum + (v.weight || 0), 0);
    const yesWeight = committeeVotes.filter((v) => v.vote === "yes").reduce((sum, v) => sum + (v.weight || 0), 0);
    const noWeight = committeeVotes.filter((v) => v.vote === "no").reduce((sum, v) => sum + (v.weight || 0), 0);
    const abstainWeight = committeeVotes
      .filter((v) => v.vote === "abstain")
      .reduce((sum, v) => sum + (v.weight || 0), 0);

    const yesPercentage = totalWeight > 0 ? (yesWeight / totalWeight) * 100 : 0;
    const passed = yesPercentage >= 66.67;

    return { totalWeight, yesWeight, noWeight, abstainWeight, yesPercentage, passed };
  };

  const calculateFeaturedAppCommitteeResult = () => {
    const totalWeight = featuredAppCommitteeVotes.reduce((sum, v) => sum + (v.weight || 0), 0);
    const yesWeight = featuredAppCommitteeVotes
      .filter((v) => v.vote === "yes")
      .reduce((sum, v) => sum + (v.weight || 0), 0);
    const noWeight = featuredAppCommitteeVotes
      .filter((v) => v.vote === "no")
      .reduce((sum, v) => sum + (v.weight || 0), 0);
    const abstainWeight = featuredAppCommitteeVotes
      .filter((v) => v.vote === "abstain")
      .reduce((sum, v) => sum + (v.weight || 0), 0);

    const yesPercentage = totalWeight > 0 ? (yesWeight / totalWeight) * 100 : 0;
    const passed = yesPercentage >= 66.67;

    return { totalWeight, yesWeight, noWeight, abstainWeight, yesPercentage, passed };
  };

  const svResult = calculateSVResult();
  const committeeResult = calculateCommitteeResult();
  const featuredAppCommitteeResult = calculateFeaturedAppCommitteeResult();

  // Validation helpers
  const isCipDetailsComplete = cipNumber && cipTitle && githubLink && cipType;
  const canSubmitCommitteeVotes = currentCipId && isCipDetailsComplete;
  const canSubmitSVVotes = currentCipId && committeeVotesSaved;

  const handleSaveFeaturedApp = async () => {
    if (!featuredAppName) {
      toast({
        title: "Error",
        description: "Please enter a featured app name",
        variant: "destructive",
      });
      return;
    }

    try {
      const { data, error } = await supabase
        .from("featured_app_votes")
        .insert({
          app_name: featuredAppName,
          description: featuredAppDescription,
          vote_count: 0,
        })
        .select()
        .single();

      if (error) throw error;

      setCurrentFeaturedAppId(data.id);
      toast({
        title: "Success",
        description: "Featured app created. You can now add committee votes.",
      });
    } catch (error) {
      toast({
        title: "Error",
        description: "Failed to create featured app",
        variant: "destructive",
      });
    }
  };

  const handleSaveFeaturedAppCommitteeVotes = async () => {
    if (!currentFeaturedAppId) {
      toast({
        title: "Error",
        description: "Please save featured app details first",
        variant: "destructive",
      });
      return;
    }

    try {
      const { error } = await supabase.from("featured_app_committee_votes").insert(
        featuredAppCommitteeVotes.map((vote) => ({
          featured_app_id: currentFeaturedAppId,
          member_name: vote.member,
          email: vote.email,
          contact: vote.contact,
          weight: vote.weight,
          vote: vote.vote,
        })),
      );

      if (error) throw error;

      toast({
        title: "Success",
        description: "Featured app committee votes saved successfully",
      });
    } catch (error) {
      toast({
        title: "Error",
        description: "Failed to save committee votes",
        variant: "destructive",
      });
    }
  };

  const handleSaveCommitteeVotes = async () => {
    if (!currentCipId) {
      toast({
        title: "Error",
        description: "Please save CIP details first",
        variant: "destructive",
      });
      return;
    }

    try {
      const { error } = await supabase.from("committee_votes").insert(
        committeeVotes.map((vote) => ({
          cip_id: currentCipId,
          member_name: vote.member,
          email: vote.email,
          contact: vote.contact,
          weight: vote.weight,
          vote: vote.vote,
        })),
      );

      if (error) throw error;

      setCommitteeVotesSaved(true);
      toast({
        title: "Success",
        description: "Committee votes saved successfully",
      });
    } catch (error) {
      toast({
        title: "Error",
        description: "Failed to save committee votes",
        variant: "destructive",
      });
    }
  };

  const handleSaveSVVotes = async () => {
    if (!currentCipId) {
      toast({
        title: "Error",
        description: "Please save CIP details first",
        variant: "destructive",
      });
      return;
    }

    try {
      const { error } = await supabase.from("sv_votes").insert(
        svVotes.map((vote) => ({
          cip_id: currentCipId,
          organization: vote.organization,
          email: vote.email,
          contact: vote.contact,
          weight: vote.weight,
          vote: vote.vote,
        })),
      );

      if (error) throw error;

      toast({
        title: "Success",
        description: "SV votes saved successfully",
      });
    } catch (error) {
      toast({
        title: "Error",
        description: "Failed to save SV votes",
        variant: "destructive",
      });
    }
  };

  const handleSaveCIP = async () => {
    if (!cipNumber || !cipTitle || !githubLink || !cipType) {
      toast({
        title: "Error",
        description: "Please fill in all required CIP fields",
        variant: "destructive",
      });
      return;
    }

    try {
      const { data, error } = await supabase
        .from("cips")
        .insert({
          cip_number: cipNumber,
          title: cipTitle,
          vote_start_date: voteStart || null,
          vote_close_date: voteClose || null,
          github_link: githubLink,
          requires_onchain_vote: requiresOnchainVote,
          cip_type: cipType,
        })
        .select()
        .single();

      if (error) throw error;

      setCurrentCipId(data.id);
      toast({
        title: "Success",
        description: "CIP saved successfully. You can now add votes.",
      });
    } catch (error) {
      toast({
        title: "Error",
        description: "Failed to save CIP",
        variant: "destructive",
      });
    }
  };

  const handleNewCIP = () => {
    setCipNumber("");
    setCipTitle("");
    setVoteStart("");
    setVoteClose("");
    setGithubLink("");
    setRequiresOnchainVote(false);
    setCipType("");
    setCurrentCipId(null);
    setCommitteeVotesSaved(false);
    setSvVotes([{ organization: "", email: "", contact: "", weight: 0, vote: "" }]);
    setCommitteeVotes([{ member: "", email: "", contact: "", weight: 1, vote: "" }]);

    toast({
      title: "New CIP",
      description: "Ready to create a new CIP",
    });
  };

  const handleNewFeaturedApp = () => {
    setFeaturedAppName("");
    setFeaturedAppDescription("");
    setCurrentFeaturedAppId(null);
    setFeaturedAppCommitteeVotes([{ member: "", email: "", contact: "", weight: 1, vote: "" }]);

    toast({
      title: "New Featured App",
      description: "Ready to create a new featured app vote",
    });
  };

  return (
    <DashboardLayout>
      <main className="space-y-8">
        <h1 className="text-3xl font-bold mb-6">Admin</h1>

        <Tabs defaultValue="cip-votes" className="w-full">
          <TabsList className="grid w-full max-w-4xl grid-cols-4 gap-1">
            <TabsTrigger value="cip-votes">CIP Votes</TabsTrigger>
            <TabsTrigger value="featured-apps">Featured App Votes</TabsTrigger>
            <TabsTrigger value="cip-history">CIP Vote History</TabsTrigger>
            <TabsTrigger value="app-history">App Vote History</TabsTrigger>
          </TabsList>

          {/* CIP Votes Tab */}
          <TabsContent value="cip-votes" className="space-y-8">
            <div className="flex items-center justify-between">
              <h2 className="text-2xl font-semibold">CIP Voting</h2>
              <Button onClick={handleNewCIP} variant="outline" className="gap-2">
                <Plus className="h-4 w-4" />
                New CIP
              </Button>
            </div>

            {/* CIP Details */}
            <Card className="glass-card">
              <CardHeader>
                <CardTitle>CIP Details</CardTitle>
                <CardDescription>Enter the basic information for the CIP vote</CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <Label htmlFor="cipNumber">CIP Number *</Label>
                    <Input
                      id="cipNumber"
                      value={cipNumber}
                      onChange={(e) => setCipNumber(e.target.value)}
                      placeholder="e.g., CIP-0001"
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="cipTitle">CIP Title *</Label>
                    <Input
                      id="cipTitle"
                      value={cipTitle}
                      onChange={(e) => setCipTitle(e.target.value)}
                      placeholder="Title of the proposal"
                    />
                  </div>
                  <div className="space-y-2 md:col-span-2">
                    <Label htmlFor="githubLink">GitHub CIP Link *</Label>
                    <Input
                      id="githubLink"
                      value={githubLink}
                      onChange={(e) => setGithubLink(e.target.value)}
                      placeholder="https://github.com/global-synchronizer-foundation/cips/..."
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="cipType">Type of CIP *</Label>
                    <div className="flex gap-2">
                      <select
                        id="cipType"
                        value={cipType}
                        onChange={(e) => setCipType(e.target.value)}
                        className="flex-1 h-10 rounded-md border border-input bg-card px-3 py-2 text-sm z-50"
                      >
                        <option value="">Select type...</option>
                        {cipTypes.map((type) => (
                          <option key={type.id} value={type.type_name}>
                            {type.type_name}
                          </option>
                        ))}
                      </select>
                      <Dialog open={isAddTypeDialogOpen} onOpenChange={setIsAddTypeDialogOpen}>
                        <DialogTrigger asChild>
                          <Button variant="outline" size="icon">
                            <Plus className="h-4 w-4" />
                          </Button>
                        </DialogTrigger>
                        <DialogContent className="bg-card z-50">
                          <DialogHeader>
                            <DialogTitle>Add New CIP Type</DialogTitle>
                            <DialogDescription>Enter the name of the new CIP type</DialogDescription>
                          </DialogHeader>
                          <div className="space-y-4">
                            <div className="space-y-2">
                              <Label htmlFor="newTypeName">Type Name</Label>
                              <Input
                                id="newTypeName"
                                value={newTypeName}
                                onChange={(e) => setNewTypeName(e.target.value)}
                                placeholder="e.g., Process"
                              />
                            </div>
                            <Button onClick={handleAddNewType} className="w-full">
                              Add Type
                            </Button>
                          </div>
                        </DialogContent>
                      </Dialog>
                    </div>
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="requiresOnchainVote">Requires Onchain Vote *</Label>
                    <select
                      id="requiresOnchainVote"
                      value={requiresOnchainVote ? "yes" : "no"}
                      onChange={(e) => setRequiresOnchainVote(e.target.value === "yes")}
                      className="w-full h-10 rounded-md border border-input bg-card px-3 py-2 text-sm z-50"
                    >
                      <option value="no">Does not require onchain vote</option>
                      <option value="yes">Requires onchain vote</option>
                    </select>
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="voteStart">Vote Start Date</Label>
                    <Input
                      id="voteStart"
                      type="date"
                      value={voteStart}
                      onChange={(e) => setVoteStart(e.target.value)}
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="voteClose">Vote Close Date</Label>
                    <Input
                      id="voteClose"
                      type="date"
                      value={voteClose}
                      onChange={(e) => setVoteClose(e.target.value)}
                    />
                  </div>
                </div>
                <Button onClick={handleSaveCIP} disabled={!!currentCipId}>
                  {currentCipId ? "CIP Saved" : "Save CIP"}
                </Button>
              </CardContent>
            </Card>

            {/* Committee Votes */}
            <Card className="glass-card">
              <CardHeader>
                <CardTitle className="flex items-center justify-between">
                  <span>Offchain Committee Vote</span>
                  <span
                    className={`text-sm px-3 py-1 rounded-full ${committeeResult.passed ? "bg-success text-success-foreground" : "bg-destructive text-destructive-foreground"}`}
                  >
                    {committeeResult.yesPercentage.toFixed(1)}% - {committeeResult.passed ? "PASSED" : "FAILED"}
                  </span>
                </CardTitle>
                <CardDescription>Committee votes (requires 2/3 majority by weight)</CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="overflow-x-auto">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Member Name</TableHead>
                        <TableHead>Email</TableHead>
                        <TableHead>Contact</TableHead>
                        <TableHead className="w-24">Weight</TableHead>
                        <TableHead className="w-32">Vote</TableHead>
                        <TableHead className="w-12"></TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {committeeVotes.map((vote, index) => (
                        <TableRow key={index}>
                          <TableCell>
                            <Input
                              value={vote.member}
                              onChange={(e) => updateCommitteeVote(index, "member", e.target.value)}
                              placeholder="Member name"
                            />
                          </TableCell>
                          <TableCell>
                            <Input
                              value={vote.email}
                              onChange={(e) => updateCommitteeVote(index, "email", e.target.value)}
                              placeholder="email@example.com"
                              type="email"
                            />
                          </TableCell>
                          <TableCell>
                            <Input
                              value={vote.contact}
                              onChange={(e) => updateCommitteeVote(index, "contact", e.target.value)}
                              placeholder="Contact name"
                            />
                          </TableCell>
                          <TableCell>
                            <Input
                              value={vote.weight || ""}
                              onChange={(e) => updateCommitteeVote(index, "weight", parseInt(e.target.value) || 1)}
                              placeholder="1"
                              type="number"
                              min="1"
                            />
                          </TableCell>
                          <TableCell>
                            <select
                              value={vote.vote}
                              onChange={(e) => updateCommitteeVote(index, "vote", e.target.value as any)}
                              className="w-full h-10 rounded-md border border-input bg-background px-3 py-2 text-sm"
                            >
                              <option value="">-</option>
                              <option value="yes">Yes</option>
                              <option value="no">No</option>
                              <option value="abstain">Abstain</option>
                            </select>
                          </TableCell>
                          <TableCell>
                            <Button
                              variant="ghost"
                              size="icon"
                              onClick={() => removeCommitteeRow(index)}
                              disabled={committeeVotes.length === 1}
                            >
                              <Trash2 className="h-4 w-4" />
                            </Button>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
                <div className="flex gap-2">
                  <Button onClick={addCommitteeRow} variant="outline">
                    Add Committee Member
                  </Button>
                  <Button
                    onClick={handleSaveCommitteeVotes}
                    disabled={!canSubmitCommitteeVotes}
                    title={
                      !isCipDetailsComplete
                        ? "Please complete all required CIP fields first"
                        : !currentCipId
                          ? "Please save CIP details first"
                          : ""
                    }
                  >
                    Submit Committee Votes
                  </Button>
                </div>
                {!canSubmitCommitteeVotes && (
                  <p className="text-sm text-muted-foreground">
                    {!isCipDetailsComplete
                      ? "⚠️ Complete all required CIP fields (CIP Number, Title, GitHub Link, Type) to enable committee voting"
                      : "⚠️ Save CIP details first"}
                  </p>
                )}
                <div className="grid grid-cols-4 gap-4 pt-4 border-t border-border">
                  <div>
                    <p className="text-sm text-muted-foreground">Total Weight</p>
                    <p className="text-2xl font-bold">{committeeVotes.reduce((sum, v) => sum + (v.weight || 0), 0)}</p>
                  </div>
                  <div>
                    <p className="text-sm text-muted-foreground">Yes</p>
                    <p className="text-2xl font-bold text-success">
                      {committeeVotes.filter((v) => v.vote === "yes").reduce((sum, v) => sum + (v.weight || 0), 0)}
                    </p>
                  </div>
                  <div>
                    <p className="text-sm text-muted-foreground">No</p>
                    <p className="text-2xl font-bold text-destructive">
                      {committeeVotes.filter((v) => v.vote === "no").reduce((sum, v) => sum + (v.weight || 0), 0)}
                    </p>
                  </div>
                  <div>
                    <p className="text-sm text-muted-foreground">Abstain</p>
                    <p className="text-2xl font-bold text-muted-foreground">
                      {committeeVotes.filter((v) => v.vote === "abstain").reduce((sum, v) => sum + (v.weight || 0), 0)}
                    </p>
                  </div>
                </div>
              </CardContent>
            </Card>

            {/* SV Votes */}
            <Card className="glass-card">
              <CardHeader>
                <CardTitle className="flex items-center justify-between">
                  <span>Offchain SV Vote</span>
                  <span
                    className={`text-sm px-3 py-1 rounded-full ${svResult.passed ? "bg-success text-success-foreground" : "bg-destructive text-destructive-foreground"}`}
                  >
                    {svResult.yesPercentage.toFixed(1)}% - {svResult.passed ? "PASSED" : "FAILED"}
                  </span>
                </CardTitle>
                <CardDescription>Super Validator votes (requires 2/3 majority by weight)</CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="overflow-x-auto">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Organization</TableHead>
                        <TableHead>Email</TableHead>
                        <TableHead>Contact</TableHead>
                        <TableHead className="w-24">Weight</TableHead>
                        <TableHead className="w-32">Vote</TableHead>
                        <TableHead className="w-12"></TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {svVotes.map((vote, index) => (
                        <TableRow key={index}>
                          <TableCell>
                            <Input
                              value={vote.organization}
                              onChange={(e) => updateSvVote(index, "organization", e.target.value)}
                              placeholder="Organization name"
                            />
                          </TableCell>
                          <TableCell>
                            <Input
                              value={vote.email}
                              onChange={(e) => updateSvVote(index, "email", e.target.value)}
                              placeholder="email@example.com"
                              type="email"
                            />
                          </TableCell>
                          <TableCell>
                            <Input
                              value={vote.contact}
                              onChange={(e) => updateSvVote(index, "contact", e.target.value)}
                              placeholder="Contact name"
                            />
                          </TableCell>
                          <TableCell>
                            <Input
                              value={vote.weight || ""}
                              onChange={(e) => updateSvVote(index, "weight", parseInt(e.target.value) || 0)}
                              placeholder="0"
                              type="number"
                              min="0"
                            />
                          </TableCell>
                          <TableCell>
                            <select
                              value={vote.vote}
                              onChange={(e) => updateSvVote(index, "vote", e.target.value as any)}
                              className="w-full h-10 rounded-md border border-input bg-background px-3 py-2 text-sm"
                            >
                              <option value="">-</option>
                              <option value="yes">Yes</option>
                              <option value="no">No</option>
                              <option value="abstain">Abstain</option>
                            </select>
                          </TableCell>
                          <TableCell>
                            <Button
                              variant="ghost"
                              size="icon"
                              onClick={() => removeSvRow(index)}
                              disabled={svVotes.length === 1}
                            >
                              <Trash2 className="h-4 w-4" />
                            </Button>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
                <div className="flex gap-2">
                  <Button onClick={addSvRow} variant="outline">
                    Add SV
                  </Button>
                  <Button
                    onClick={handleSaveSVVotes}
                    disabled={!canSubmitSVVotes}
                    title={
                      !committeeVotesSaved
                        ? "Please complete and save committee votes first"
                        : !currentCipId
                          ? "Please save CIP details first"
                          : ""
                    }
                  >
                    Submit SV Votes
                  </Button>
                </div>
                {!canSubmitSVVotes && (
                  <p className="text-sm text-muted-foreground">
                    {!committeeVotesSaved
                      ? "⚠️ Complete and save committee votes before submitting SV votes"
                      : "⚠️ Save CIP details first"}
                  </p>
                )}
                <div className="grid grid-cols-4 gap-4 pt-4 border-t border-border">
                  <div>
                    <p className="text-sm text-muted-foreground">Total Weight</p>
                    <p className="text-2xl font-bold">{svResult.totalWeight}</p>
                  </div>
                  <div>
                    <p className="text-sm text-muted-foreground">Yes</p>
                    <p className="text-2xl font-bold text-success">{svResult.yesWeight}</p>
                  </div>
                  <div>
                    <p className="text-sm text-muted-foreground">No</p>
                    <p className="text-2xl font-bold text-destructive">{svResult.noWeight}</p>
                  </div>
                  <div>
                    <p className="text-sm text-muted-foreground">Abstain</p>
                    <p className="text-2xl font-bold text-muted-foreground">{svResult.abstainWeight}</p>
                  </div>
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          {/* Featured App Votes Tab */}
          <TabsContent value="featured-apps" className="space-y-8">
            <div className="flex items-center justify-between">
              <h2 className="text-2xl font-semibold">Featured App Voting</h2>
              <Button onClick={handleNewFeaturedApp} variant="outline" className="gap-2">
                <Plus className="h-4 w-4" />
                New Featured App
              </Button>
            </div>

            {/* Featured App Details */}
            <Card className="glass-card">
              <CardHeader>
                <CardTitle>Featured App Details</CardTitle>
                <CardDescription>Enter the app information for committee voting</CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="grid grid-cols-1 gap-4">
                  <div className="space-y-2">
                    <Label htmlFor="featuredAppName">App Name</Label>
                    <Input
                      id="featuredAppName"
                      value={featuredAppName}
                      onChange={(e) => setFeaturedAppName(e.target.value)}
                      placeholder="Enter app name"
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="featuredAppDescription">Description</Label>
                    <Input
                      id="featuredAppDescription"
                      value={featuredAppDescription}
                      onChange={(e) => setFeaturedAppDescription(e.target.value)}
                      placeholder="Brief description of the app"
                    />
                  </div>
                </div>
                <Button onClick={handleSaveFeaturedApp} disabled={!!currentFeaturedAppId}>
                  {currentFeaturedAppId ? "App Saved" : "Save Featured App"}
                </Button>
              </CardContent>
            </Card>

            {/* Featured App Committee Votes */}
            <Card className="glass-card">
              <CardHeader>
                <CardTitle className="flex items-center justify-between">
                  <span>Committee Vote</span>
                  <span
                    className={`text-sm px-3 py-1 rounded-full ${featuredAppCommitteeResult.passed ? "bg-success text-success-foreground" : "bg-destructive text-destructive-foreground"}`}
                  >
                    {featuredAppCommitteeResult.yesPercentage.toFixed(1)}% -{" "}
                    {featuredAppCommitteeResult.passed ? "PASSED" : "FAILED"}
                  </span>
                </CardTitle>
                <CardDescription>Committee votes (requires 2/3 majority by weight)</CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="overflow-x-auto">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Member Name</TableHead>
                        <TableHead>Email</TableHead>
                        <TableHead>Contact</TableHead>
                        <TableHead className="w-24">Weight</TableHead>
                        <TableHead className="w-32">Vote</TableHead>
                        <TableHead className="w-12"></TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {featuredAppCommitteeVotes.map((vote, index) => (
                        <TableRow key={index}>
                          <TableCell>
                            <Input
                              value={vote.member}
                              onChange={(e) => updateFeaturedAppCommitteeVote(index, "member", e.target.value)}
                              placeholder="Member name"
                            />
                          </TableCell>
                          <TableCell>
                            <Input
                              value={vote.email}
                              onChange={(e) => updateFeaturedAppCommitteeVote(index, "email", e.target.value)}
                              placeholder="email@example.com"
                              type="email"
                            />
                          </TableCell>
                          <TableCell>
                            <Input
                              value={vote.contact}
                              onChange={(e) => updateFeaturedAppCommitteeVote(index, "contact", e.target.value)}
                              placeholder="Contact name"
                            />
                          </TableCell>
                          <TableCell>
                            <Input
                              value={vote.weight || ""}
                              onChange={(e) =>
                                updateFeaturedAppCommitteeVote(index, "weight", parseInt(e.target.value) || 1)
                              }
                              placeholder="1"
                              type="number"
                              min="1"
                            />
                          </TableCell>
                          <TableCell>
                            <select
                              value={vote.vote}
                              onChange={(e) => updateFeaturedAppCommitteeVote(index, "vote", e.target.value as any)}
                              className="w-full h-10 rounded-md border border-input bg-background px-3 py-2 text-sm"
                            >
                              <option value="">-</option>
                              <option value="yes">Yes</option>
                              <option value="no">No</option>
                              <option value="abstain">Abstain</option>
                            </select>
                          </TableCell>
                          <TableCell>
                            <Button
                              variant="ghost"
                              size="icon"
                              onClick={() => removeFeaturedAppCommitteeRow(index)}
                              disabled={featuredAppCommitteeVotes.length === 1}
                            >
                              <Trash2 className="h-4 w-4" />
                            </Button>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
                <div className="flex gap-2">
                  <Button onClick={addFeaturedAppCommitteeRow} variant="outline">
                    Add Committee Member
                  </Button>
                  <Button onClick={handleSaveFeaturedAppCommitteeVotes} disabled={!currentFeaturedAppId}>
                    Submit Committee Votes
                  </Button>
                </div>
                <div className="grid grid-cols-4 gap-4 pt-4 border-t border-border">
                  <div>
                    <p className="text-sm text-muted-foreground">Total Weight</p>
                    <p className="text-2xl font-bold">{featuredAppCommitteeResult.totalWeight}</p>
                  </div>
                  <div>
                    <p className="text-sm text-muted-foreground">Yes</p>
                    <p className="text-2xl font-bold text-success">{featuredAppCommitteeResult.yesWeight}</p>
                  </div>
                  <div>
                    <p className="text-sm text-muted-foreground">No</p>
                    <p className="text-2xl font-bold text-destructive">{featuredAppCommitteeResult.noWeight}</p>
                  </div>
                  <div>
                    <p className="text-sm text-muted-foreground">Abstain</p>
                    <p className="text-2xl font-bold text-muted-foreground">
                      {featuredAppCommitteeResult.abstainWeight}
                    </p>
                  </div>
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          {/* CIP Vote History Tab */}
          <TabsContent value="cip-history" className="space-y-6">
            <h2 className="text-2xl font-semibold">CIP Vote History</h2>

            {cipHistory.length === 0 ? (
              <Card className="glass-card">
                <CardContent className="py-8 text-center text-muted-foreground">
                  No CIP vote history available yet.
                </CardContent>
              </Card>
            ) : (
              cipHistory.map((cip) => {
                const committeeTotal = cip.committeeVotes.reduce((sum: number, v: any) => sum + (v.weight || 0), 0);
                const committeeYes = cip.committeeVotes
                  .filter((v: any) => v.vote === "yes")
                  .reduce((sum: number, v: any) => sum + (v.weight || 0), 0);
                const committeePercent = committeeTotal > 0 ? (committeeYes / committeeTotal) * 100 : 0;
                const committeePassed = committeePercent >= 66.67;

                const svTotal = cip.svVotes.reduce((sum: number, v: any) => sum + (v.weight || 0), 0);
                const svYes = cip.svVotes
                  .filter((v: any) => v.vote === "yes")
                  .reduce((sum: number, v: any) => sum + (v.weight || 0), 0);
                const svPercent = svTotal > 0 ? (svYes / svTotal) * 100 : 0;
                const svPassed = svPercent >= 66.67;

                return (
                  <Card key={cip.id} className="glass-card overflow-hidden">
                    <CardHeader className="border-b border-border/50 bg-muted/20">
                      <div className="flex items-start justify-between">
                        <div className="space-y-1">
                          <CardTitle className="text-xl">
                            {cip.cip_number}: {cip.title}
                          </CardTitle>
                          <CardDescription className="flex flex-wrap gap-3 mt-2">
                            {cip.cip_type && (
                              <span className="inline-flex items-center rounded-full bg-primary/10 px-2.5 py-0.5 text-xs font-medium text-primary">
                                {cip.cip_type}
                              </span>
                            )}
                            {cip.requires_onchain_vote && (
                              <span className="inline-flex items-center rounded-full bg-blue-500/10 px-2.5 py-0.5 text-xs font-medium text-blue-500">
                                Onchain Vote Required
                              </span>
                            )}
                            <span className="text-xs text-muted-foreground">
                              Status: <span className="capitalize">{cip.status}</span>
                            </span>
                          </CardDescription>
                        </div>
                        <div className="flex gap-2">
                          <span
                            className={`text-xs px-3 py-1.5 rounded-full font-medium ${committeePassed ? "bg-success/20 text-success" : "bg-destructive/20 text-destructive"}`}
                          >
                            Committee: {committeePassed ? "PASSED" : "FAILED"}
                          </span>
                          <span
                            className={`text-xs px-3 py-1.5 rounded-full font-medium ${svPassed ? "bg-success/20 text-success" : "bg-destructive/20 text-destructive"}`}
                          >
                            SV: {svPassed ? "PASSED" : "FAILED"}
                          </span>
                        </div>
                      </div>
                    </CardHeader>
                    <CardContent className="pt-6">
                      {cip.github_link && (
                        <div className="mb-4 pb-4 border-b border-border/50">
                          <a
                            href={cip.github_link}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="text-sm text-primary hover:underline"
                          >
                            View on GitHub →
                          </a>
                        </div>
                      )}

                      {/* Committee Votes Section */}
                      <div className="mb-6">
                        <h4 className="font-semibold mb-3 flex items-center gap-2">
                          Committee Votes
                          <span className="text-sm font-normal text-muted-foreground">
                            ({committeePercent.toFixed(1)}% approval)
                          </span>
                        </h4>
                        {cip.committeeVotes.length > 0 ? (
                          <div className="overflow-x-auto rounded-lg border border-border/50">
                            <Table>
                              <TableHeader>
                                <TableRow className="bg-muted/30">
                                  <TableHead className="font-semibold">Member</TableHead>
                                  <TableHead className="font-semibold">Contact</TableHead>
                                  <TableHead className="font-semibold text-center">Weight</TableHead>
                                  <TableHead className="font-semibold text-center">Vote</TableHead>
                                </TableRow>
                              </TableHeader>
                              <TableBody>
                                {cip.committeeVotes.map((vote: any, idx: number) => (
                                  <TableRow key={idx} className="hover:bg-muted/10">
                                    <TableCell className="font-medium">{vote.member_name}</TableCell>
                                    <TableCell className="text-muted-foreground">{vote.contact}</TableCell>
                                    <TableCell className="text-center">{vote.weight}</TableCell>
                                    <TableCell className="text-center">
                                      <span
                                        className={`inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium capitalize ${
                                          vote.vote === "yes"
                                            ? "bg-success/20 text-success"
                                            : vote.vote === "no"
                                              ? "bg-destructive/20 text-destructive"
                                              : "bg-muted text-muted-foreground"
                                        }`}
                                      >
                                        {vote.vote || "-"}
                                      </span>
                                    </TableCell>
                                  </TableRow>
                                ))}
                              </TableBody>
                            </Table>
                          </div>
                        ) : (
                          <p className="text-sm text-muted-foreground italic">No committee votes recorded</p>
                        )}
                      </div>

                      {/* SV Votes Section */}
                      <div>
                        <h4 className="font-semibold mb-3 flex items-center gap-2">
                          Super Validator Votes
                          <span className="text-sm font-normal text-muted-foreground">
                            ({svPercent.toFixed(1)}% approval)
                          </span>
                        </h4>
                        {cip.svVotes.length > 0 ? (
                          <div className="overflow-x-auto rounded-lg border border-border/50">
                            <Table>
                              <TableHeader>
                                <TableRow className="bg-muted/30">
                                  <TableHead className="font-semibold">Organization</TableHead>
                                  <TableHead className="font-semibold">Contact</TableHead>
                                  <TableHead className="font-semibold text-center">Weight</TableHead>
                                  <TableHead className="font-semibold text-center">Vote</TableHead>
                                </TableRow>
                              </TableHeader>
                              <TableBody>
                                {cip.svVotes.map((vote: any, idx: number) => (
                                  <TableRow key={idx} className="hover:bg-muted/10">
                                    <TableCell className="font-medium">{vote.organization}</TableCell>
                                    <TableCell className="text-muted-foreground">{vote.contact}</TableCell>
                                    <TableCell className="text-center">{vote.weight}</TableCell>
                                    <TableCell className="text-center">
                                      <span
                                        className={`inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium capitalize ${
                                          vote.vote === "yes"
                                            ? "bg-success/20 text-success"
                                            : vote.vote === "no"
                                              ? "bg-destructive/20 text-destructive"
                                              : "bg-muted text-muted-foreground"
                                        }`}
                                      >
                                        {vote.vote || "-"}
                                      </span>
                                    </TableCell>
                                  </TableRow>
                                ))}
                              </TableBody>
                            </Table>
                          </div>
                        ) : (
                          <p className="text-sm text-muted-foreground italic">No SV votes recorded</p>
                        )}
                      </div>
                    </CardContent>
                  </Card>
                );
              })
            )}
          </TabsContent>

          {/* Featured App Vote History Tab */}
          <TabsContent value="app-history" className="space-y-6">
            <h2 className="text-2xl font-semibold">Featured App Vote History</h2>

            {featuredAppHistory.length === 0 ? (
              <Card className="glass-card">
                <CardContent className="py-8 text-center text-muted-foreground">
                  No featured app vote history available yet.
                </CardContent>
              </Card>
            ) : (
              featuredAppHistory.map((app) => {
                const totalWeight = app.votes.reduce((sum: number, v: any) => sum + (v.weight || 0), 0);
                const yesWeight = app.votes
                  .filter((v: any) => v.vote === "yes")
                  .reduce((sum: number, v: any) => sum + (v.weight || 0), 0);
                const yesPercent = totalWeight > 0 ? (yesWeight / totalWeight) * 100 : 0;
                const passed = yesPercent >= 66.67;

                return (
                  <Card key={app.id} className="glass-card overflow-hidden">
                    <CardHeader className="border-b border-border/50 bg-muted/20">
                      <div className="flex items-start justify-between">
                        <div className="space-y-1 flex-1">
                          <CardTitle className="text-xl">{app.app_name}</CardTitle>
                          {app.description && <CardDescription className="mt-1">{app.description}</CardDescription>}
                          <div className="flex gap-3 mt-2">
                            <span
                              className={`text-xs px-3 py-1 rounded-full capitalize font-medium ${
                                app.status === "approved"
                                  ? "bg-success/20 text-success"
                                  : app.status === "rejected"
                                    ? "bg-destructive/20 text-destructive"
                                    : "bg-muted/50 text-muted-foreground"
                              }`}
                            >
                              {app.status}
                            </span>
                          </div>
                        </div>
                        <span
                          className={`text-xs px-3 py-1.5 rounded-full font-medium ${passed ? "bg-success/20 text-success" : "bg-destructive/20 text-destructive"}`}
                        >
                          {passed ? "PASSED" : "FAILED"} ({yesPercent.toFixed(1)}%)
                        </span>
                      </div>
                    </CardHeader>
                    <CardContent className="pt-6">
                      <h4 className="font-semibold mb-3 flex items-center gap-2">
                        Committee Votes
                        <span className="text-sm font-normal text-muted-foreground">(Total Weight: {totalWeight})</span>
                      </h4>
                      {app.votes.length > 0 ? (
                        <div className="overflow-x-auto rounded-lg border border-border/50">
                          <Table>
                            <TableHeader>
                              <TableRow className="bg-muted/30">
                                <TableHead className="font-semibold">Member</TableHead>
                                <TableHead className="font-semibold">Contact</TableHead>
                                <TableHead className="font-semibold text-center">Weight</TableHead>
                                <TableHead className="font-semibold text-center">Vote</TableHead>
                              </TableRow>
                            </TableHeader>
                            <TableBody>
                              {app.votes.map((vote: any, idx: number) => (
                                <TableRow key={idx} className="hover:bg-muted/10">
                                  <TableCell className="font-medium">{vote.member_name}</TableCell>
                                  <TableCell className="text-muted-foreground">{vote.contact}</TableCell>
                                  <TableCell className="text-center">{vote.weight}</TableCell>
                                  <TableCell className="text-center">
                                    <span
                                      className={`inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium capitalize ${
                                        vote.vote === "yes"
                                          ? "bg-success/20 text-success"
                                          : vote.vote === "no"
                                            ? "bg-destructive/20 text-destructive"
                                            : "bg-muted text-muted-foreground"
                                      }`}
                                    >
                                      {vote.vote || "-"}
                                    </span>
                                  </TableCell>
                                </TableRow>
                              ))}
                            </TableBody>
                          </Table>
                        </div>
                      ) : (
                        <p className="text-sm text-muted-foreground italic">No votes recorded</p>
                      )}
                    </CardContent>
                  </Card>
                );
              })
            )}
          </TabsContent>
        </Tabs>
      </main>
    </DashboardLayout>
  );
};

export default Admin;
