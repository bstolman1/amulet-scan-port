import { useState } from "react";
import { Search, X } from "lucide-react";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { useNavigate } from "react-router-dom";
import { useToast } from "@/hooks/use-toast";
import { getPartyEvents, searchAnsEntries } from "@/lib/duckdb-api-client";
import {
  Command,
  CommandDialog,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";
import { DialogTitle, DialogDescription } from "@/components/ui/dialog";
import { VisuallyHidden } from "@radix-ui/react-visually-hidden";

export const SearchBar = () => {
  const [open, setOpen] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const [isSearching, setIsSearching] = useState(false);
  const navigate = useNavigate();
  const { toast } = useToast();

  const handleSearch = async (query: string) => {
    const trimmedQuery = query.trim();
    if (!trimmedQuery) return;

    setIsSearching(true);
    try {
      if (trimmedQuery.includes("::")) {
        // Navigate directly to the Party page - it will handle the DuckDB query
        navigate(`/party/${encodeURIComponent(trimmedQuery)}`);
        setOpen(false);
        setSearchQuery("");
        toast({
          title: "Loading Party",
          description: "Fetching events from backfill data...",
        });
      } else if (trimmedQuery.startsWith("#")) {
        navigate(`/transactions?search=${encodeURIComponent(trimmedQuery)}`);
        setOpen(false);
      } else {
        try {
          const result = await searchAnsEntries(trimmedQuery.toLowerCase(), 25);
          const lowerQuery = trimmedQuery.toLowerCase();
          const matchingAns = (result.data || []).filter((entry: any) => {
            const name = entry.payload?.name || entry.name || '';
            return name.toLowerCase().includes(lowerQuery);
          });

          if (matchingAns.length > 0) {
            navigate(`/ans?search=${encodeURIComponent(trimmedQuery)}`);
            setOpen(false);
            toast({
              title: "Search Results",
              description: `Found ${matchingAns.length} ANS entry(ies)`,
            });
          } else {
            toast({
              title: "No Results",
              description: "No results found for this search",
              variant: "destructive",
            });
          }
        } catch (error) {
          console.error("ANS search failed", error);
          toast({
            title: "Search Error",
            description: "Unable to search ANS entries",
            variant: "destructive",
          });
        }
      }
    } catch (error) {
      console.error("Search failed", error);
      toast({
        title: "Search Failed",
        description: "An error occurred while searching",
        variant: "destructive",
      });
    } finally {
      setIsSearching(false);
    }
  };

  return (
    <>
      <Button variant="outline" className="gap-2 w-full sm:w-64" onClick={() => setOpen(true)}>
        <Search className="h-4 w-4" />
        <span className="hidden sm:inline">Search party, event, ANS...</span>
        <span className="sm:hidden">Search...</span>
      </Button>

      <CommandDialog open={open} onOpenChange={setOpen}>
        <VisuallyHidden>
          <DialogTitle>Search</DialogTitle>
          <DialogDescription>Search by party ID, event ID, or ANS name</DialogDescription>
        </VisuallyHidden>
        <CommandInput
          placeholder="Search by party ID, event ID, or ANS name..."
          value={searchQuery}
          onValueChange={setSearchQuery}
        />
        <CommandList>
          <CommandEmpty>{isSearching ? "Searching..." : "Type to search"}</CommandEmpty>
          <CommandGroup heading="Suggestions">
            <CommandItem
              onSelect={() => {
                setSearchQuery("example::1220");
                toast({
                  title: "Tip",
                  description: "Party IDs contain :: in the format name::hash",
                });
              }}
            >
              <Search className="mr-2 h-4 w-4" />
              <span>Search by Party ID (e.g., validator::1220...)</span>
            </CommandItem>
            <CommandItem
              onSelect={() => {
                setSearchQuery("#");
                toast({
                  title: "Tip",
                  description: "Event IDs start with # symbol",
                });
              }}
            >
              <Search className="mr-2 h-4 w-4" />
              <span>Search by Event ID (e.g., #1220...)</span>
            </CommandItem>
            <CommandItem
              onSelect={() => {
                setSearchQuery("");
                toast({
                  title: "Tip",
                  description: "Enter an ANS name to search for Canton names",
                });
              }}
            >
              <Search className="mr-2 h-4 w-4" />
              <span>Search by ANS Name</span>
            </CommandItem>
          </CommandGroup>
          {searchQuery && (
            <CommandGroup heading="Actions">
              <CommandItem onSelect={() => handleSearch(searchQuery)} disabled={isSearching}>
                <Search className="mr-2 h-4 w-4" />
                <span>Search for "{searchQuery}"</span>
              </CommandItem>
            </CommandGroup>
          )}
        </CommandList>
      </CommandDialog>
    </>
  );
};
