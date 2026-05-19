import { useQuery } from "@tanstack/react-query";
import { fetchTokens, type TokensResponse } from "@/lib/duckdb-api-client";

export function useTokens() {
  return useQuery<TokensResponse>({
    queryKey: ["tokens"],
    queryFn: fetchTokens,
    staleTime: 5 * 60 * 1000,
    retry: 2,
  });
}
