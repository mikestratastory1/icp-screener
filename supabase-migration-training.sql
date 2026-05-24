-- Per-factor Training Examples
-- Run this in Supabase SQL editor
-- If you already created the old training_examples table, drop it first:
DROP TABLE IF EXISTS training_examples;

CREATE TABLE training_examples (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  company_id UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
  company_name TEXT NOT NULL,
  domain TEXT NOT NULL,
  factor TEXT NOT NULL,          -- 'A', 'B', 'C', 'D', 'E', or 'F'
  score INTEGER NOT NULL,        -- 1, 2, or 3
  justification TEXT NOT NULL,   -- corrected justification
  research_snapshot TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(domain, factor)         -- one example per factor per company
);

CREATE INDEX idx_training_examples_domain ON training_examples(domain);
CREATE INDEX idx_training_examples_factor ON training_examples(factor);

ALTER TABLE training_examples ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow all for service role" ON training_examples FOR ALL USING (true) WITH CHECK (true);
