import { useState } from "react";
import { Search, X } from "lucide-react";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { useNavigate } from "react-router-dom";
import { useToast } from "@/hooks/use-toast";
import { scanApi } from "@/lib/api-client";
import {
  Command,
  CommandDialog,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";

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
        try {
          const transactions = await scanApi.fetchTransactionsByParty(trimmedQuery, 20);

          if (transactions.transactions.length > 0) {
            navigate(`/transactions?search=${encodeURIComponent(trimmedQuery)}`);
            setOpen(false);
            toast({
              title: "Search Results",
              description: `Found ${transactions.transactions.length} transaction(s) for this party`,
            });
            return;
          }
        } catch (partyError) {
          console.warn("Party search via API failed, falling back to local filter", partyError);
        }

        const fallback = await scanApi.fetchTransactions({
          page_size: 20,
          sort_order: "desc",
        });

        const matchingTxs = fallback.transactions.filter((tx) => {
          const lowerQuery = trimmedQuery.toLowerCase();
          const senderMatch = tx.transfer?.sender?.party?.toLowerCase().includes(lowerQuery);
          const receiverMatch = tx.transfer?.receivers?.some((r) => r.party?.toLowerCase().includes(lowerQuery));
          return senderMatch || receiverMatch;
        });

        if (matchingTxs.length > 0) {
          navigate(`/transactions?search=${encodeURIComponent(trimmedQuery)}`);
          setOpen(false);
          toast({
            title: "Search Results",
            description: `Found ${matchingTxs.length} transaction(s) for this party`,
          });
        } else {
          toast({
            title: "No Results",
            description: "No transactions found for this party ID",
            variant: "destructive",
          });
        }
      } else if (trimmedQuery.startsWith("#")) {
        navigate(`/transactions?search=${encodeURIComponent(trimmedQuery)}`);
        setOpen(false);
      } else {
        try {
          const ansResults = await scanApi.fetchAnsEntries(trimmedQuery.toLowerCase(), 25);
          const lowerQuery = trimmedQuery.toLowerCase();
          const matchingAns = ansResults.entries.filter((entry) => entry.name.toLowerCase().includes(lowerQuery));

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
