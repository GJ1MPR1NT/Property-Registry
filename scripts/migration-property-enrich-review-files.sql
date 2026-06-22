-- Registry-iQ: campaign review file attachments (quarantine → scan → promote)
-- Apply AFTER migration-property-enrich-review.sql
-- Do NOT serve quarantine_path URLs publicly.

CREATE TABLE IF NOT EXISTS property_enrich_review_files (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  review_id uuid NOT NULL REFERENCES property_enrich_review(id) ON DELETE CASCADE,
  campaign_id uuid NOT NULL REFERENCES property_enrich_campaigns(id) ON DELETE CASCADE,
  property_id uuid NOT NULL REFERENCES property_registry(id) ON DELETE CASCADE,
  question_id text NOT NULL,
  outlier_id text,
  original_filename text NOT NULL,
  mime_type text,
  size_bytes bigint NOT NULL CHECK (size_bytes > 0),
  -- quarantine → scanning → clean | rejected → promoted
  storage_status text NOT NULL DEFAULT 'quarantine'
    CHECK (storage_status IN ('quarantine', 'scanning', 'clean', 'rejected', 'promoted')),
  quarantine_bucket text,
  quarantine_path text,
  promoted_url text,
  promoted_public_id text,
  scan_vendor text,
  scan_result jsonb DEFAULT '{}',
  rejection_reason text,
  scanned_at timestamptz,
  promoted_at timestamptz,
  uploaded_by_email text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_enrich_review_files_review
  ON property_enrich_review_files (review_id);
CREATE INDEX IF NOT EXISTS idx_enrich_review_files_status
  ON property_enrich_review_files (storage_status)
  WHERE storage_status IN ('quarantine', 'scanning');

COMMENT ON TABLE property_enrich_review_files IS
  'User-uploaded campaign evidence. Files land in quarantine, are malware-scanned server-side, then promoted to Cloudinary.';
