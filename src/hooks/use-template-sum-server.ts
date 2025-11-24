import { useQuery } from "@tanstack/react-query";
import { supabase } from "@/integrations/supabase/client";

export function useTemplateSumServer(templateName: string) {
  return useQuery({
    queryKey: ["template-sum-server", templateName],
    queryFn: async () => {
      const { data, error } = await supabase
        .from("acs_template_stats")
        .select("*")
        .eq("template_name", templateName)
        .order("round", { ascending: false })
        .limit(1)
        .single();

      if (error) throw error;
      return data;
    },
  });
}
