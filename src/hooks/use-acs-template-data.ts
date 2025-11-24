import { useQuery } from "@tanstack/react-query";
import { supabase } from "@/integrations/supabase/client";

interface UseAcsTemplateDataOptions {
  limit?: number;
  offset?: number;
}

export function useAcsTemplateData(options: UseAcsTemplateDataOptions = {}) {
  const { limit = 50, offset = 0 } = options;

  return useQuery({
    queryKey: ["acs-template-data", limit, offset],
    queryFn: async () => {
      const { data, error } = await supabase
        .from("acs_template_stats")
        .select("*")
        .order("round", { ascending: false })
        .order("template_name", { ascending: true })
        .range(offset, offset + limit - 1);

      if (error) throw error;
      return data;
    },
  });
}
