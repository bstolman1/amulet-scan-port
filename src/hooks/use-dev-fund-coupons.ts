import { useQuery } from "@tanstack/react-query";

export interface DevFundCoupon {
  contract: {
    template_id: string;
    contract_id: string;
    payload: Record<string, unknown>;
    created_event_blob: string;
    created_at: string;
  };
  domain_id: string;
}

interface DevFundResponse {
  "unclaimed-development-fund-coupons": DevFundCoupon[];
}

export function useDevFundCoupons() {
  return useQuery({
    queryKey: ["scan-api", "unclaimed-dev-fund-coupons"],
    queryFn: async () => {
      const res = await fetch("/api/scan-proxy/v0/unclaimed-development-fund-coupons", {
        method: "GET",
        headers: { Accept: "application/json" },
      });
      if (!res.ok) {
        const text = await res.text();
        throw new Error(`Failed (${res.status}): ${text}`);
      }
      const data: DevFundResponse = await res.json();
      return data["unclaimed-development-fund-coupons"] || [];
    },
    staleTime: 60_000,
  });
}
