-- ICP Screener Supabase Schema v2
-- Run this in Supabase SQL editor

CREATE TABLE companies (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  domain TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL,
  website TEXT NOT NULL,
  manual_score TEXT DEFAULT '',
  notes TEXT DEFAULT '',
  account_status TEXT DEFAULT 'Cold',
  last_screened_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE research_runs (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  company_id UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
  
  -- Research output
  research_raw TEXT DEFAULT '',
  product_summary TEXT DEFAULT '',
  target_customer TEXT DEFAULT '',
  target_decision_maker TEXT DEFAULT '',
  top3_outcomes TEXT DEFAULT '',
  top3_differentiators TEXT DEFAULT '',
  major_announcements TEXT DEFAULT '',
  competitors TEXT DEFAULT '',
  customers TEXT DEFAULT '',
  funding TEXT DEFAULT '',
  team_size TEXT DEFAULT '',
  homepage_sections TEXT DEFAULT '',
  homepage_nav TEXT DEFAULT '',
  product_pages TEXT DEFAULT '',
  new_direction_page TEXT DEFAULT '',
  linkedin_description TEXT DEFAULT '',
  ceo_founder_name TEXT DEFAULT '',
  ceo_recent_content TEXT DEFAULT '',
  ceo_narrative_theme TEXT DEFAULT '',
  new_marketing_leader TEXT DEFAULT '',
  product_marketing_people TEXT DEFAULT '',
  
  -- Scoring: top-level
  scoring_raw TEXT DEFAULT '',
  total_score INTEGER DEFAULT 0,
  score_summary TEXT DEFAULT '',
  icp_fit TEXT DEFAULT '',
  disqualification_reason TEXT DEFAULT '',
  
  -- Homepage section names (shared across factors)
  homepage_section_1_name TEXT DEFAULT '',
  homepage_section_2_name TEXT DEFAULT '',
  homepage_section_3_name TEXT DEFAULT '',
  homepage_section_4_name TEXT DEFAULT '',
  
  -- Factor A: Differentiation
  score_a INTEGER DEFAULT 0,
  a_differentiators TEXT DEFAULT '',
  a_section_1_finding TEXT DEFAULT '',
  a_section_1_status TEXT DEFAULT '',
  a_section_2_finding TEXT DEFAULT '',
  a_section_2_status TEXT DEFAULT '',
  a_section_3_finding TEXT DEFAULT '',
  a_section_3_status TEXT DEFAULT '',
  a_section_4_finding TEXT DEFAULT '',
  a_section_4_status TEXT DEFAULT '',
  a_verdict TEXT DEFAULT '',
  
  -- Factor B: Outcomes
  score_b INTEGER DEFAULT 0,
  b_decision_maker TEXT DEFAULT '',
  b_strategic_outcomes TEXT DEFAULT '',
  b_tactical_outcomes TEXT DEFAULT '',
  b_section_1_finding TEXT DEFAULT '',
  b_section_1_type TEXT DEFAULT '',
  b_section_2_finding TEXT DEFAULT '',
  b_section_2_type TEXT DEFAULT '',
  b_section_3_finding TEXT DEFAULT '',
  b_section_3_type TEXT DEFAULT '',
  b_section_4_finding TEXT DEFAULT '',
  b_section_4_type TEXT DEFAULT '',
  b_verdict TEXT DEFAULT '',
  
  -- Factor C: Customer-Centric
  score_c INTEGER DEFAULT 0,
  c_section_1_orientation TEXT DEFAULT '',
  c_section_1_evidence TEXT DEFAULT '',
  c_section_2_orientation TEXT DEFAULT '',
  c_section_2_evidence TEXT DEFAULT '',
  c_section_3_orientation TEXT DEFAULT '',
  c_section_3_evidence TEXT DEFAULT '',
  c_section_4_orientation TEXT DEFAULT '',
  c_section_4_evidence TEXT DEFAULT '',
  c_verdict TEXT DEFAULT '',
  
  -- Factor D: Product Change
  score_d INTEGER DEFAULT 0,
  d_changes TEXT DEFAULT '',
  d_verdict TEXT DEFAULT '',
  
  -- Factor E: Audience Change
  score_e INTEGER DEFAULT 0,
  e_audience_before TEXT DEFAULT '',
  e_audience_today TEXT DEFAULT '',
  e_verdict TEXT DEFAULT '',
  
  -- Factor F: Multi-Product
  score_f INTEGER DEFAULT 0,
  f_products TEXT DEFAULT '',
  f_description TEXT DEFAULT '',
  f_verdict TEXT DEFAULT '',
  
  -- Metadata
  status TEXT DEFAULT 'pending',
  error TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_research_runs_company ON research_runs(company_id);
CREATE INDEX idx_research_runs_created ON research_runs(created_at DESC);
CREATE INDEX idx_companies_domain ON companies(domain);

CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER companies_updated_at
  BEFORE UPDATE ON companies
  FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE VIEW company_latest AS
SELECT 
  c.*,
  r.id as run_id,
  r.research_raw, r.product_summary, r.target_customer, r.target_decision_maker,
  r.top3_outcomes, r.top3_differentiators, r.major_announcements, r.competitors,
  r.customers as research_customers, r.funding as research_funding, r.team_size,
  r.homepage_sections, r.homepage_nav, r.product_pages, r.new_direction_page,
  r.linkedin_description, r.ceo_founder_name, r.ceo_recent_content, r.ceo_narrative_theme,
  r.new_marketing_leader, r.product_marketing_people,
  r.scoring_raw, r.total_score, r.score_summary, r.icp_fit, r.disqualification_reason,
  r.homepage_section_1_name, r.homepage_section_2_name, r.homepage_section_3_name, r.homepage_section_4_name,
  r.score_a, r.a_differentiators, r.a_section_1_finding, r.a_section_1_status,
  r.a_section_2_finding, r.a_section_2_status, r.a_section_3_finding, r.a_section_3_status,
  r.a_section_4_finding, r.a_section_4_status, r.a_verdict,
  r.score_b, r.b_decision_maker, r.b_strategic_outcomes, r.b_tactical_outcomes,
  r.b_section_1_finding, r.b_section_1_type, r.b_section_2_finding, r.b_section_2_type,
  r.b_section_3_finding, r.b_section_3_type, r.b_section_4_finding, r.b_section_4_type, r.b_verdict,
  r.score_c, r.c_section_1_orientation, r.c_section_1_evidence,
  r.c_section_2_orientation, r.c_section_2_evidence,
  r.c_section_3_orientation, r.c_section_3_evidence,
  r.c_section_4_orientation, r.c_section_4_evidence, r.c_verdict,
  r.score_d, r.d_changes, r.d_verdict,
  r.score_e, r.e_audience_before, r.e_audience_today, r.e_verdict,
  r.score_f, r.f_products, r.f_description, r.f_verdict,
  r.status as run_status, r.error as run_error, r.created_at as run_created_at
FROM companies c
LEFT JOIN LATERAL (
  SELECT * FROM research_runs
  WHERE company_id = c.id
  ORDER BY created_at DESC
  LIMIT 1
) r ON true;

-- Contacts table: people linked to companies (1 company → many contacts)
CREATE TABLE contacts (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  company_id UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
  name TEXT NOT NULL DEFAULT '',
  title TEXT DEFAULT '',
  linkedin TEXT DEFAULT '',
  email_verified BOOLEAN DEFAULT false,
  seniority TEXT DEFAULT '',
  function_category TEXT DEFAULT '',
  region TEXT DEFAULT '',
  headline TEXT DEFAULT '',
  years_experience INTEGER DEFAULT 0,
  recent_job_change BOOLEAN DEFAULT false,
  company_domain TEXT DEFAULT '',
  crustdata_person_id BIGINT,
  business_email TEXT DEFAULT '',
  contact_status TEXT DEFAULT 'New',
  last_enriched_at TIMESTAMPTZ,
  last_campaign_added_at TIMESTAMPTZ,
  notes TEXT DEFAULT '',
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(linkedin)
);

CREATE INDEX idx_contacts_company ON contacts(company_id);
CREATE INDEX idx_contacts_domain ON contacts(company_domain);

CREATE TRIGGER contacts_updated_at
  BEFORE UPDATE ON contacts
  FOR EACH ROW EXECUTE FUNCTION update_updated_at();

ALTER TABLE companies ENABLE ROW LEVEL SECURITY;
ALTER TABLE research_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE contacts ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow all for service role" ON companies FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all for service role" ON research_runs FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all for service role" ON contacts FOR ALL USING (true) WITH CHECK (true);

-- Campaigns
CREATE TABLE campaigns (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  name TEXT NOT NULL DEFAULT 'Untitled Campaign',
  status TEXT DEFAULT 'draft', -- draft, active, paused, completed
  prompt TEXT DEFAULT '',
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TRIGGER campaigns_updated_at
  BEFORE UPDATE ON campaigns
  FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- Campaign-Contact junction (many-to-many)
CREATE TABLE campaign_contacts (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  campaign_id UUID NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
  contact_id UUID NOT NULL REFERENCES contacts(id) ON DELETE CASCADE,
  added_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(campaign_id, contact_id)
);

CREATE INDEX idx_campaign_contacts_campaign ON campaign_contacts(campaign_id);
CREATE INDEX idx_campaign_contacts_contact ON campaign_contacts(contact_id);

-- Campaign messages (email or linkedin steps)
CREATE TABLE campaign_messages (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  campaign_id UUID NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
  channel TEXT NOT NULL DEFAULT 'email', -- 'email' or 'linkedin'
  step_number INTEGER NOT NULL DEFAULT 1,
  subject TEXT DEFAULT '',
  body TEXT DEFAULT '',
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_campaign_messages_campaign ON campaign_messages(campaign_id);

CREATE TRIGGER campaign_messages_updated_at
  BEFORE UPDATE ON campaign_messages
  FOR EACH ROW EXECUTE FUNCTION update_updated_at();

ALTER TABLE campaigns ENABLE ROW LEVEL SECURITY;
ALTER TABLE campaign_contacts ENABLE ROW LEVEL SECURITY;
ALTER TABLE campaign_messages ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow all for service role" ON campaigns FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all for service role" ON campaign_contacts FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all for service role" ON campaign_messages FOR ALL USING (true) WITH CHECK (true);

-- Migration: Add account_status to companies (run if upgrading existing DB)
-- ALTER TABLE companies ADD COLUMN IF NOT EXISTS account_status TEXT DEFAULT 'Cold';

-- Migration: Add last_screened_at to companies
-- ALTER TABLE companies ADD COLUMN IF NOT EXISTS last_screened_at TIMESTAMPTZ;

-- Migration: Add CRM fields to contacts
-- ALTER TABLE contacts ADD COLUMN IF NOT EXISTS contact_status TEXT DEFAULT 'New';
-- ALTER TABLE contacts ADD COLUMN IF NOT EXISTS last_enriched_at TIMESTAMPTZ;
-- ALTER TABLE contacts ADD COLUMN IF NOT EXISTS last_campaign_added_at TIMESTAMPTZ;

-- Migration: Add prompt to campaigns
-- ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS prompt TEXT DEFAULT '';

-- Per-contact generated messages (LLM output)
CREATE TABLE generated_messages (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  campaign_id UUID NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
  contact_id UUID NOT NULL REFERENCES contacts(id) ON DELETE CASCADE,
  step_number INTEGER NOT NULL DEFAULT 1,
  channel TEXT NOT NULL DEFAULT 'email',
  subject TEXT DEFAULT '',
  body TEXT DEFAULT '',
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_generated_messages_campaign_contact ON generated_messages(campaign_id, contact_id);

CREATE TRIGGER generated_messages_updated_at
  BEFORE UPDATE ON generated_messages
  FOR EACH ROW EXECUTE FUNCTION update_updated_at();

ALTER TABLE generated_messages ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow all for service role" ON generated_messages FOR ALL USING (true) WITH CHECK (true);

-- Saved discovery filter presets
CREATE TABLE saved_filters (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  name TEXT NOT NULL,
  mode TEXT NOT NULL DEFAULT 'indb',
  filters JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE saved_filters ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow all for service role" ON saved_filters FOR ALL USING (true) WITH CHECK (true);
