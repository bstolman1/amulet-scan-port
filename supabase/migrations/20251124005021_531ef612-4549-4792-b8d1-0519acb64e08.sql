-- Create app_role enum
CREATE TYPE public.app_role AS ENUM ('admin', 'moderator', 'user');

-- Create user_roles table
CREATE TABLE public.user_roles (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID REFERENCES auth.users(id) ON DELETE CASCADE NOT NULL,
    role public.app_role NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    UNIQUE (user_id, role)
);

-- Enable RLS on user_roles
ALTER TABLE public.user_roles ENABLE ROW LEVEL SECURITY;

-- Create security definer function for role checking
CREATE OR REPLACE FUNCTION public.has_role(_user_id UUID, _role public.app_role)
RETURNS BOOLEAN
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
  SELECT EXISTS (
    SELECT 1
    FROM public.user_roles
    WHERE user_id = _user_id
      AND role = _role
  )
$$;

-- Create updated_at trigger function
CREATE OR REPLACE FUNCTION public.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create cip_types table
CREATE TABLE public.cip_types (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL
);

-- Create cips table (Cardano Improvement Proposals)
CREATE TABLE public.cips (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cip_number INTEGER NOT NULL UNIQUE,
    title TEXT NOT NULL,
    description TEXT,
    cip_type_id UUID REFERENCES public.cip_types(id),
    status TEXT,
    author TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL
);

-- Create sv_votes table (Stake Validator votes)
CREATE TABLE public.sv_votes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cip_id UUID REFERENCES public.cips(id) ON DELETE CASCADE NOT NULL,
    validator_address TEXT NOT NULL,
    vote TEXT NOT NULL CHECK (vote IN ('yes', 'no', 'abstain')),
    voting_power BIGINT,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    UNIQUE (cip_id, validator_address)
);

-- Create committee_votes table
CREATE TABLE public.committee_votes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cip_id UUID REFERENCES public.cips(id) ON DELETE CASCADE NOT NULL,
    committee_member TEXT NOT NULL,
    vote TEXT NOT NULL CHECK (vote IN ('yes', 'no', 'abstain')),
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    UNIQUE (cip_id, committee_member)
);

-- Create featured_app_votes table
CREATE TABLE public.featured_app_votes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    app_name TEXT NOT NULL,
    validator_address TEXT NOT NULL,
    vote TEXT NOT NULL CHECK (vote IN ('yes', 'no', 'abstain')),
    voting_power BIGINT,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    UNIQUE (app_name, validator_address)
);

-- Create featured_app_committee_votes table
CREATE TABLE public.featured_app_committee_votes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    app_name TEXT NOT NULL,
    committee_member TEXT NOT NULL,
    vote TEXT NOT NULL CHECK (vote IN ('yes', 'no', 'abstain')),
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    UNIQUE (app_name, committee_member)
);

-- Create acs_snapshots table (ACS = Application Configuration Snapshot)
CREATE TABLE public.acs_snapshots (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    round BIGINT NOT NULL UNIQUE,
    snapshot_data JSONB NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL
);

-- Create acs_template_stats table
CREATE TABLE public.acs_template_stats (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    round BIGINT NOT NULL,
    template_name TEXT NOT NULL,
    instance_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    UNIQUE (round, template_name)
);

-- Create ledger_updates table
CREATE TABLE public.ledger_updates (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    round BIGINT NOT NULL,
    update_type TEXT NOT NULL,
    update_data JSONB NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL
);

-- Create ledger_events table
CREATE TABLE public.ledger_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    round BIGINT NOT NULL,
    event_type TEXT NOT NULL,
    event_data JSONB NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL
);

-- Create backfill_cursors table
CREATE TABLE public.backfill_cursors (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cursor_name TEXT NOT NULL UNIQUE,
    last_processed_round BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL
);

-- Create live_update_cursor table
CREATE TABLE public.live_update_cursor (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cursor_name TEXT NOT NULL UNIQUE,
    last_processed_round BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL
);

-- Create indexes
CREATE INDEX idx_cips_cip_number ON public.cips(cip_number);
CREATE INDEX idx_sv_votes_cip_id ON public.sv_votes(cip_id);
CREATE INDEX idx_committee_votes_cip_id ON public.committee_votes(cip_id);
CREATE INDEX idx_featured_app_votes_app_name ON public.featured_app_votes(app_name);
CREATE INDEX idx_featured_app_committee_votes_app_name ON public.featured_app_committee_votes(app_name);
CREATE INDEX idx_acs_snapshots_round ON public.acs_snapshots(round);
CREATE INDEX idx_acs_template_stats_round ON public.acs_template_stats(round);
CREATE INDEX idx_ledger_updates_round ON public.ledger_updates(round);
CREATE INDEX idx_ledger_events_round ON public.ledger_events(round);

-- Add triggers for updated_at
CREATE TRIGGER update_cips_updated_at
    BEFORE UPDATE ON public.cips
    FOR EACH ROW
    EXECUTE FUNCTION public.update_updated_at_column();

CREATE TRIGGER update_acs_template_stats_updated_at
    BEFORE UPDATE ON public.acs_template_stats
    FOR EACH ROW
    EXECUTE FUNCTION public.update_updated_at_column();

-- Enable RLS on all tables
ALTER TABLE public.cip_types ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.cips ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.sv_votes ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.committee_votes ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.featured_app_votes ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.featured_app_committee_votes ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.acs_snapshots ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.acs_template_stats ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.ledger_updates ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.ledger_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.backfill_cursors ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.live_update_cursor ENABLE ROW LEVEL SECURITY;

-- RLS Policies for user_roles (admins can manage, users can view their own)
CREATE POLICY "Users can view their own roles"
    ON public.user_roles FOR SELECT
    USING (auth.uid() = user_id);

CREATE POLICY "Admins can view all roles"
    ON public.user_roles FOR SELECT
    USING (public.has_role(auth.uid(), 'admin'));

CREATE POLICY "Admins can insert roles"
    ON public.user_roles FOR INSERT
    WITH CHECK (public.has_role(auth.uid(), 'admin'));

CREATE POLICY "Admins can update roles"
    ON public.user_roles FOR UPDATE
    USING (public.has_role(auth.uid(), 'admin'));

CREATE POLICY "Admins can delete roles"
    ON public.user_roles FOR DELETE
    USING (public.has_role(auth.uid(), 'admin'));

-- Public read policies for governance and ledger data
CREATE POLICY "Anyone can view cip types"
    ON public.cip_types FOR SELECT
    USING (true);

CREATE POLICY "Anyone can view cips"
    ON public.cips FOR SELECT
    USING (true);

CREATE POLICY "Anyone can view sv votes"
    ON public.sv_votes FOR SELECT
    USING (true);

CREATE POLICY "Anyone can view committee votes"
    ON public.committee_votes FOR SELECT
    USING (true);

CREATE POLICY "Anyone can view featured app votes"
    ON public.featured_app_votes FOR SELECT
    USING (true);

CREATE POLICY "Anyone can view featured app committee votes"
    ON public.featured_app_committee_votes FOR SELECT
    USING (true);

CREATE POLICY "Anyone can view acs snapshots"
    ON public.acs_snapshots FOR SELECT
    USING (true);

CREATE POLICY "Anyone can view acs template stats"
    ON public.acs_template_stats FOR SELECT
    USING (true);

CREATE POLICY "Anyone can view ledger updates"
    ON public.ledger_updates FOR SELECT
    USING (true);

CREATE POLICY "Anyone can view ledger events"
    ON public.ledger_events FOR SELECT
    USING (true);

CREATE POLICY "Anyone can view backfill cursors"
    ON public.backfill_cursors FOR SELECT
    USING (true);

CREATE POLICY "Anyone can view live update cursor"
    ON public.live_update_cursor FOR SELECT
    USING (true);

-- Admin write policies
CREATE POLICY "Admins can insert cip types"
    ON public.cip_types FOR INSERT
    WITH CHECK (public.has_role(auth.uid(), 'admin'));

CREATE POLICY "Admins can update cip types"
    ON public.cip_types FOR UPDATE
    USING (public.has_role(auth.uid(), 'admin'));

CREATE POLICY "Admins can delete cip types"
    ON public.cip_types FOR DELETE
    USING (public.has_role(auth.uid(), 'admin'));

CREATE POLICY "Admins can insert cips"
    ON public.cips FOR INSERT
    WITH CHECK (public.has_role(auth.uid(), 'admin'));

CREATE POLICY "Admins can update cips"
    ON public.cips FOR UPDATE
    USING (public.has_role(auth.uid(), 'admin'));

CREATE POLICY "Admins can delete cips"
    ON public.cips FOR DELETE
    USING (public.has_role(auth.uid(), 'admin'));

-- Storage bucket for ACS data
INSERT INTO storage.buckets (id, name, public) 
VALUES ('acs-data', 'acs-data', true);

-- Storage policies for acs-data bucket
CREATE POLICY "Anyone can view acs data"
    ON storage.objects FOR SELECT
    USING (bucket_id = 'acs-data');

CREATE POLICY "Admins can upload acs data"
    ON storage.objects FOR INSERT
    WITH CHECK (bucket_id = 'acs-data' AND public.has_role(auth.uid(), 'admin'));

CREATE POLICY "Admins can update acs data"
    ON storage.objects FOR UPDATE
    USING (bucket_id = 'acs-data' AND public.has_role(auth.uid(), 'admin'));

CREATE POLICY "Admins can delete acs data"
    ON storage.objects FOR DELETE
    USING (bucket_id = 'acs-data' AND public.has_role(auth.uid(), 'admin'));