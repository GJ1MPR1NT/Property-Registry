-- Registry-iQ: property enrichment review campaigns (SYNC-style HITL for matrix outliers)
-- Run on Registry-iQ Supabase (xhafhdaugmgdxckhdfov)

CREATE TABLE IF NOT EXISTS property_enrich_campaigns (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  property_id uuid NOT NULL REFERENCES property_registry(id) ON DELETE CASCADE,
  campaign_code text NOT NULL,
  title text NOT NULL,
  description text,
  status text NOT NULL DEFAULT 'draft'
    CHECK (status IN ('draft', 'live', 'completed', 'cancelled')),
  recipient_email text,
  witness_emails text[] DEFAULT '{}',
  opened_at timestamptz,
  target_completion_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT property_enrich_campaigns_code_unique UNIQUE (campaign_code)
);

CREATE INDEX IF NOT EXISTS idx_property_enrich_campaigns_property
  ON property_enrich_campaigns (property_id);

CREATE TABLE IF NOT EXISTS property_enrich_review (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  campaign_id uuid NOT NULL REFERENCES property_enrich_campaigns(id) ON DELETE CASCADE,
  property_id uuid NOT NULL REFERENCES property_registry(id) ON DELETE CASCADE,
  review_status text NOT NULL DEFAULT 'pending'
    CHECK (review_status IN ('pending', 'submitted', 'applied', 'cancelled')),
  item_payloads jsonb NOT NULL DEFAULT '[]',
  answers jsonb,
  decision_notes text,
  recipient_email text,
  witness_emails text[] DEFAULT '{}',
  token text UNIQUE,
  token_first_opened_at timestamptz,
  token_expires_at timestamptz,
  token_used_at timestamptz,
  token_revoked_at timestamptz,
  sent_at timestamptz,
  reviewed_by text,
  reviewed_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_property_enrich_review_campaign
  ON property_enrich_review (campaign_id);
CREATE INDEX IF NOT EXISTS idx_property_enrich_review_token
  ON property_enrich_review (token) WHERE token IS NOT NULL;

CREATE TABLE IF NOT EXISTS property_enrich_review_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  review_id uuid NOT NULL REFERENCES property_enrich_review(id) ON DELETE CASCADE,
  campaign_id uuid NOT NULL REFERENCES property_enrich_campaigns(id) ON DELETE CASCADE,
  property_id uuid NOT NULL REFERENCES property_registry(id) ON DELETE CASCADE,
  event_type text NOT NULL,
  actor_kind text NOT NULL DEFAULT 'system',
  actor_email text,
  payload jsonb DEFAULT '{}',
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_property_enrich_review_events_review
  ON property_enrich_review_events (review_id);

COMMENT ON TABLE property_enrich_campaigns IS 'Registry enrichment SYNC campaigns — one property, many outlier items.';
COMMENT ON TABLE property_enrich_review IS 'Token-gated enrichment review session; witness emails are separate sends (no shared token).';
COMMENT ON COLUMN property_enrich_review.item_payloads IS 'Array of EnrichReviewItemPayload (questions + citations per outlier).';
COMMENT ON COLUMN property_enrich_review.witness_emails IS 'Audit trail only — witnesses receive separate text-only emails, not true CC on primary.';
