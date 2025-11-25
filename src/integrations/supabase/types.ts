export type Json =
  | string
  | number
  | boolean
  | null
  | { [key: string]: Json | undefined }
  | Json[]

export type Database = {
  // Allows to automatically instantiate createClient with right options
  // instead of createClient<Database, { PostgrestVersion: 'XX' }>(URL, KEY)
  __InternalSupabase: {
    PostgrestVersion: "13.0.5"
  }
  public: {
    Tables: {
      acs_snapshots: {
        Row: {
          amulet_total: number | null
          canonical_package: string | null
          circulating_supply: number | null
          created_at: string
          entry_count: number | null
          error_message: string | null
          id: string
          locked_total: number | null
          migration_id: number | null
          record_time: string | null
          round: number
          snapshot_data: Json
          status: string | null
          sv_url: string | null
          timestamp: string
          updated_at: string | null
        }
        Insert: {
          amulet_total?: number | null
          canonical_package?: string | null
          circulating_supply?: number | null
          created_at?: string
          entry_count?: number | null
          error_message?: string | null
          id?: string
          locked_total?: number | null
          migration_id?: number | null
          record_time?: string | null
          round: number
          snapshot_data: Json
          status?: string | null
          sv_url?: string | null
          timestamp?: string
          updated_at?: string | null
        }
        Update: {
          amulet_total?: number | null
          canonical_package?: string | null
          circulating_supply?: number | null
          created_at?: string
          entry_count?: number | null
          error_message?: string | null
          id?: string
          locked_total?: number | null
          migration_id?: number | null
          record_time?: string | null
          round?: number
          snapshot_data?: Json
          status?: string | null
          sv_url?: string | null
          timestamp?: string
          updated_at?: string | null
        }
        Relationships: []
      }
      acs_template_stats: {
        Row: {
          contract_count: number | null
          created_at: string
          field_sums: Json | null
          id: string
          instance_count: number
          round: number
          snapshot_id: string | null
          status_tallies: Json | null
          storage_path: string | null
          template_id: string | null
          template_name: string
          updated_at: string
        }
        Insert: {
          contract_count?: number | null
          created_at?: string
          field_sums?: Json | null
          id?: string
          instance_count?: number
          round: number
          snapshot_id?: string | null
          status_tallies?: Json | null
          storage_path?: string | null
          template_id?: string | null
          template_name: string
          updated_at?: string
        }
        Update: {
          contract_count?: number | null
          created_at?: string
          field_sums?: Json | null
          id?: string
          instance_count?: number
          round?: number
          snapshot_id?: string | null
          status_tallies?: Json | null
          storage_path?: string | null
          template_id?: string | null
          template_name?: string
          updated_at?: string
        }
        Relationships: []
      }
      backfill_cursors: {
        Row: {
          complete: boolean | null
          cursor_name: string
          id: string
          last_before: string | null
          last_processed_round: number
          max_time: string | null
          migration_id: number | null
          min_time: string | null
          synchronizer_id: string | null
          updated_at: string
        }
        Insert: {
          complete?: boolean | null
          cursor_name?: string
          id?: string
          last_before?: string | null
          last_processed_round?: number
          max_time?: string | null
          migration_id?: number | null
          min_time?: string | null
          synchronizer_id?: string | null
          updated_at?: string
        }
        Update: {
          complete?: boolean | null
          cursor_name?: string
          id?: string
          last_before?: string | null
          last_processed_round?: number
          max_time?: string | null
          migration_id?: number | null
          min_time?: string | null
          synchronizer_id?: string | null
          updated_at?: string
        }
        Relationships: []
      }
      cip_types: {
        Row: {
          created_at: string
          description: string | null
          id: string
          name: string
        }
        Insert: {
          created_at?: string
          description?: string | null
          id?: string
          name: string
        }
        Update: {
          created_at?: string
          description?: string | null
          id?: string
          name?: string
        }
        Relationships: []
      }
      cips: {
        Row: {
          author: string | null
          cip_number: number
          cip_type_id: string | null
          created_at: string
          description: string | null
          id: string
          status: string | null
          title: string
          updated_at: string
        }
        Insert: {
          author?: string | null
          cip_number: number
          cip_type_id?: string | null
          created_at?: string
          description?: string | null
          id?: string
          status?: string | null
          title: string
          updated_at?: string
        }
        Update: {
          author?: string | null
          cip_number?: number
          cip_type_id?: string | null
          created_at?: string
          description?: string | null
          id?: string
          status?: string | null
          title?: string
          updated_at?: string
        }
        Relationships: [
          {
            foreignKeyName: "cips_cip_type_id_fkey"
            columns: ["cip_type_id"]
            isOneToOne: false
            referencedRelation: "cip_types"
            referencedColumns: ["id"]
          },
        ]
      }
      committee_votes: {
        Row: {
          cip_id: string
          committee_member: string
          created_at: string
          id: string
          timestamp: string
          vote: string
        }
        Insert: {
          cip_id: string
          committee_member: string
          created_at?: string
          id?: string
          timestamp?: string
          vote: string
        }
        Update: {
          cip_id?: string
          committee_member?: string
          created_at?: string
          id?: string
          timestamp?: string
          vote?: string
        }
        Relationships: [
          {
            foreignKeyName: "committee_votes_cip_id_fkey"
            columns: ["cip_id"]
            isOneToOne: false
            referencedRelation: "cips"
            referencedColumns: ["id"]
          },
        ]
      }
      featured_app_committee_votes: {
        Row: {
          app_name: string
          committee_member: string
          created_at: string
          id: string
          timestamp: string
          vote: string
        }
        Insert: {
          app_name: string
          committee_member: string
          created_at?: string
          id?: string
          timestamp?: string
          vote: string
        }
        Update: {
          app_name?: string
          committee_member?: string
          created_at?: string
          id?: string
          timestamp?: string
          vote?: string
        }
        Relationships: []
      }
      featured_app_votes: {
        Row: {
          app_name: string
          created_at: string
          id: string
          timestamp: string
          validator_address: string
          vote: string
          voting_power: number | null
        }
        Insert: {
          app_name: string
          created_at?: string
          id?: string
          timestamp?: string
          validator_address: string
          vote: string
          voting_power?: number | null
        }
        Update: {
          app_name?: string
          created_at?: string
          id?: string
          timestamp?: string
          validator_address?: string
          vote?: string
          voting_power?: number | null
        }
        Relationships: []
      }
      ledger_events: {
        Row: {
          contract_id: string | null
          created_at: string
          created_at_ts: string | null
          event_data: Json
          event_id: string | null
          event_type: string
          id: string
          migration_id: number | null
          observers: string[] | null
          package_name: string | null
          payload: Json | null
          raw: Json | null
          signatories: string[] | null
          template_id: string | null
          timestamp: string
          update_id: string | null
        }
        Insert: {
          contract_id?: string | null
          created_at?: string
          created_at_ts?: string | null
          event_data: Json
          event_id?: string | null
          event_type: string
          id?: string
          migration_id?: number | null
          observers?: string[] | null
          package_name?: string | null
          payload?: Json | null
          raw?: Json | null
          signatories?: string[] | null
          template_id?: string | null
          timestamp?: string
          update_id?: string | null
        }
        Update: {
          contract_id?: string | null
          created_at?: string
          created_at_ts?: string | null
          event_data?: Json
          event_id?: string | null
          event_type?: string
          id?: string
          migration_id?: number | null
          observers?: string[] | null
          package_name?: string | null
          payload?: Json | null
          raw?: Json | null
          signatories?: string[] | null
          template_id?: string | null
          timestamp?: string
          update_id?: string | null
        }
        Relationships: []
      }
      ledger_updates: {
        Row: {
          created_at: string
          effective_at: string | null
          id: string
          kind: string | null
          migration_id: number | null
          offset: number | null
          raw: Json | null
          record_time: string | null
          synchronizer_id: string | null
          timestamp: string
          update_data: Json
          update_id: string | null
          update_type: string
          workflow_id: string | null
        }
        Insert: {
          created_at?: string
          effective_at?: string | null
          id?: string
          kind?: string | null
          migration_id?: number | null
          offset?: number | null
          raw?: Json | null
          record_time?: string | null
          synchronizer_id?: string | null
          timestamp?: string
          update_data: Json
          update_id?: string | null
          update_type: string
          workflow_id?: string | null
        }
        Update: {
          created_at?: string
          effective_at?: string | null
          id?: string
          kind?: string | null
          migration_id?: number | null
          offset?: number | null
          raw?: Json | null
          record_time?: string | null
          synchronizer_id?: string | null
          timestamp?: string
          update_data?: Json
          update_id?: string | null
          update_type?: string
          workflow_id?: string | null
        }
        Relationships: []
      }
      live_update_cursor: {
        Row: {
          cursor_name: string
          id: string
          last_processed_round: number
          updated_at: string
        }
        Insert: {
          cursor_name: string
          id?: string
          last_processed_round?: number
          updated_at?: string
        }
        Update: {
          cursor_name?: string
          id?: string
          last_processed_round?: number
          updated_at?: string
        }
        Relationships: []
      }
      sv_votes: {
        Row: {
          cip_id: string
          created_at: string
          id: string
          timestamp: string
          validator_address: string
          vote: string
          voting_power: number | null
        }
        Insert: {
          cip_id: string
          created_at?: string
          id?: string
          timestamp?: string
          validator_address: string
          vote: string
          voting_power?: number | null
        }
        Update: {
          cip_id?: string
          created_at?: string
          id?: string
          timestamp?: string
          validator_address?: string
          vote?: string
          voting_power?: number | null
        }
        Relationships: [
          {
            foreignKeyName: "sv_votes_cip_id_fkey"
            columns: ["cip_id"]
            isOneToOne: false
            referencedRelation: "cips"
            referencedColumns: ["id"]
          },
        ]
      }
      temp_ledger_events_1764077739604_252: {
        Row: {
          contract_id: string | null
          created_at: string
          created_at_ts: string | null
          event_data: Json
          event_id: string | null
          event_type: string
          id: string
          migration_id: number | null
          observers: string[] | null
          package_name: string | null
          payload: Json | null
          raw: Json | null
          signatories: string[] | null
          template_id: string | null
          timestamp: string
          update_id: string | null
        }
        Insert: {
          contract_id?: string | null
          created_at?: string
          created_at_ts?: string | null
          event_data: Json
          event_id?: string | null
          event_type: string
          id?: string
          migration_id?: number | null
          observers?: string[] | null
          package_name?: string | null
          payload?: Json | null
          raw?: Json | null
          signatories?: string[] | null
          template_id?: string | null
          timestamp?: string
          update_id?: string | null
        }
        Update: {
          contract_id?: string | null
          created_at?: string
          created_at_ts?: string | null
          event_data?: Json
          event_id?: string | null
          event_type?: string
          id?: string
          migration_id?: number | null
          observers?: string[] | null
          package_name?: string | null
          payload?: Json | null
          raw?: Json | null
          signatories?: string[] | null
          template_id?: string | null
          timestamp?: string
          update_id?: string | null
        }
        Relationships: []
      }
      temp_ledger_events_1764091036283_92: {
        Row: {
          contract_id: string | null
          created_at: string
          created_at_ts: string | null
          event_data: Json
          event_id: string | null
          event_type: string
          id: string
          migration_id: number | null
          observers: string[] | null
          package_name: string | null
          payload: Json | null
          raw: Json | null
          signatories: string[] | null
          template_id: string | null
          timestamp: string
          update_id: string | null
        }
        Insert: {
          contract_id?: string | null
          created_at?: string
          created_at_ts?: string | null
          event_data: Json
          event_id?: string | null
          event_type: string
          id?: string
          migration_id?: number | null
          observers?: string[] | null
          package_name?: string | null
          payload?: Json | null
          raw?: Json | null
          signatories?: string[] | null
          template_id?: string | null
          timestamp?: string
          update_id?: string | null
        }
        Update: {
          contract_id?: string | null
          created_at?: string
          created_at_ts?: string | null
          event_data?: Json
          event_id?: string | null
          event_type?: string
          id?: string
          migration_id?: number | null
          observers?: string[] | null
          package_name?: string | null
          payload?: Json | null
          raw?: Json | null
          signatories?: string[] | null
          template_id?: string | null
          timestamp?: string
          update_id?: string | null
        }
        Relationships: []
      }
      temp_ledger_updates_1764079271664_901: {
        Row: {
          created_at: string
          effective_at: string | null
          id: string
          kind: string | null
          migration_id: number | null
          offset: number | null
          raw: Json | null
          record_time: string | null
          synchronizer_id: string | null
          timestamp: string
          update_data: Json
          update_id: string | null
          update_type: string
          workflow_id: string | null
        }
        Insert: {
          created_at?: string
          effective_at?: string | null
          id?: string
          kind?: string | null
          migration_id?: number | null
          offset?: number | null
          raw?: Json | null
          record_time?: string | null
          synchronizer_id?: string | null
          timestamp?: string
          update_data: Json
          update_id?: string | null
          update_type: string
          workflow_id?: string | null
        }
        Update: {
          created_at?: string
          effective_at?: string | null
          id?: string
          kind?: string | null
          migration_id?: number | null
          offset?: number | null
          raw?: Json | null
          record_time?: string | null
          synchronizer_id?: string | null
          timestamp?: string
          update_data?: Json
          update_id?: string | null
          update_type?: string
          workflow_id?: string | null
        }
        Relationships: []
      }
      temp_ledger_updates_1764086315854_375: {
        Row: {
          created_at: string
          effective_at: string | null
          id: string
          kind: string | null
          migration_id: number | null
          offset: number | null
          raw: Json | null
          record_time: string | null
          synchronizer_id: string | null
          timestamp: string
          update_data: Json
          update_id: string | null
          update_type: string
          workflow_id: string | null
        }
        Insert: {
          created_at?: string
          effective_at?: string | null
          id?: string
          kind?: string | null
          migration_id?: number | null
          offset?: number | null
          raw?: Json | null
          record_time?: string | null
          synchronizer_id?: string | null
          timestamp?: string
          update_data: Json
          update_id?: string | null
          update_type: string
          workflow_id?: string | null
        }
        Update: {
          created_at?: string
          effective_at?: string | null
          id?: string
          kind?: string | null
          migration_id?: number | null
          offset?: number | null
          raw?: Json | null
          record_time?: string | null
          synchronizer_id?: string | null
          timestamp?: string
          update_data?: Json
          update_id?: string | null
          update_type?: string
          workflow_id?: string | null
        }
        Relationships: []
      }
      user_roles: {
        Row: {
          created_at: string
          id: string
          role: Database["public"]["Enums"]["app_role"]
          user_id: string
        }
        Insert: {
          created_at?: string
          id?: string
          role: Database["public"]["Enums"]["app_role"]
          user_id: string
        }
        Update: {
          created_at?: string
          id?: string
          role?: Database["public"]["Enums"]["app_role"]
          user_id?: string
        }
        Relationships: []
      }
    }
    Views: {
      [_ in never]: never
    }
    Functions: {
      has_role: {
        Args: {
          _role: Database["public"]["Enums"]["app_role"]
          _user_id: string
        }
        Returns: boolean
      }
    }
    Enums: {
      app_role: "admin" | "moderator" | "user"
    }
    CompositeTypes: {
      [_ in never]: never
    }
  }
}

type DatabaseWithoutInternals = Omit<Database, "__InternalSupabase">

type DefaultSchema = DatabaseWithoutInternals[Extract<keyof Database, "public">]

export type Tables<
  DefaultSchemaTableNameOrOptions extends
    | keyof (DefaultSchema["Tables"] & DefaultSchema["Views"])
    | { schema: keyof DatabaseWithoutInternals },
  TableName extends DefaultSchemaTableNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof (DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"] &
        DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Views"])
    : never = never,
> = DefaultSchemaTableNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? (DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"] &
      DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Views"])[TableName] extends {
      Row: infer R
    }
    ? R
    : never
  : DefaultSchemaTableNameOrOptions extends keyof (DefaultSchema["Tables"] &
        DefaultSchema["Views"])
    ? (DefaultSchema["Tables"] &
        DefaultSchema["Views"])[DefaultSchemaTableNameOrOptions] extends {
        Row: infer R
      }
      ? R
      : never
    : never

export type TablesInsert<
  DefaultSchemaTableNameOrOptions extends
    | keyof DefaultSchema["Tables"]
    | { schema: keyof DatabaseWithoutInternals },
  TableName extends DefaultSchemaTableNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"]
    : never = never,
> = DefaultSchemaTableNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"][TableName] extends {
      Insert: infer I
    }
    ? I
    : never
  : DefaultSchemaTableNameOrOptions extends keyof DefaultSchema["Tables"]
    ? DefaultSchema["Tables"][DefaultSchemaTableNameOrOptions] extends {
        Insert: infer I
      }
      ? I
      : never
    : never

export type TablesUpdate<
  DefaultSchemaTableNameOrOptions extends
    | keyof DefaultSchema["Tables"]
    | { schema: keyof DatabaseWithoutInternals },
  TableName extends DefaultSchemaTableNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"]
    : never = never,
> = DefaultSchemaTableNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"][TableName] extends {
      Update: infer U
    }
    ? U
    : never
  : DefaultSchemaTableNameOrOptions extends keyof DefaultSchema["Tables"]
    ? DefaultSchema["Tables"][DefaultSchemaTableNameOrOptions] extends {
        Update: infer U
      }
      ? U
      : never
    : never

export type Enums<
  DefaultSchemaEnumNameOrOptions extends
    | keyof DefaultSchema["Enums"]
    | { schema: keyof DatabaseWithoutInternals },
  EnumName extends DefaultSchemaEnumNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof DatabaseWithoutInternals[DefaultSchemaEnumNameOrOptions["schema"]]["Enums"]
    : never = never,
> = DefaultSchemaEnumNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? DatabaseWithoutInternals[DefaultSchemaEnumNameOrOptions["schema"]]["Enums"][EnumName]
  : DefaultSchemaEnumNameOrOptions extends keyof DefaultSchema["Enums"]
    ? DefaultSchema["Enums"][DefaultSchemaEnumNameOrOptions]
    : never

export type CompositeTypes<
  PublicCompositeTypeNameOrOptions extends
    | keyof DefaultSchema["CompositeTypes"]
    | { schema: keyof DatabaseWithoutInternals },
  CompositeTypeName extends PublicCompositeTypeNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof DatabaseWithoutInternals[PublicCompositeTypeNameOrOptions["schema"]]["CompositeTypes"]
    : never = never,
> = PublicCompositeTypeNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? DatabaseWithoutInternals[PublicCompositeTypeNameOrOptions["schema"]]["CompositeTypes"][CompositeTypeName]
  : PublicCompositeTypeNameOrOptions extends keyof DefaultSchema["CompositeTypes"]
    ? DefaultSchema["CompositeTypes"][PublicCompositeTypeNameOrOptions]
    : never

export const Constants = {
  public: {
    Enums: {
      app_role: ["admin", "moderator", "user"],
    },
  },
} as const
