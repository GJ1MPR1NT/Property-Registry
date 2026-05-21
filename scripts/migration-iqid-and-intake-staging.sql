-- ============================================================================
-- Registry-iQ migration: iqid identity scheme + intake staging + dedupe review
-- Target project: Registry-iQ Supabase (xhafhdaugmgdxckhdfov)
--
-- Decisions (chat 2026-05-12 → 2026-05-20):
--   • Platform-wide identifier on every registry entity: iqid_<entity>_<10-char-base32>
--     entity prefixes: prop | proj | vend | stake | cont | fac | team | scope | pos
--   • Externally-sourced entities (property, project, vendor, stakeholder,
--     contact, facility) ingest via a staging table with HITL review before
--     an iqid is assigned. No new row lands in the canonical registry until
--     a human approves "new" or "merge into existing".
--   • Existing rows: a one-time dedupe scan populates registry_dedupe_review.
--     ≥0.98 score auto-merge with audit log; AI pre-pass drafts proposals
--     for 0.75–0.98 band; human approves bulk.
--   • Internal lookup registries (team, scope, position) get iqid only.
--
-- This migration is additive. Existing rows keep their UUID primary key.
-- A follow-up migration will add INSERT triggers blocking direct writes
-- once all ingestion paths are routed through staging.
-- ============================================================================

BEGIN;

-- ── 1. Required extensions ──────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA extensions;
CREATE EXTENSION IF NOT EXISTS unaccent WITH SCHEMA extensions;
CREATE EXTENSION IF NOT EXISTS fuzzystrmatch WITH SCHEMA extensions;
CREATE EXTENSION IF NOT EXISTS citext WITH SCHEMA extensions;

-- ── 2. Enums ────────────────────────────────────────────────────────────────
DO $$ BEGIN
  CREATE TYPE public.iqid_entity_type AS ENUM (
    'property', 'project', 'vendor', 'stakeholder', 'contact', 'facility',
    'team', 'scope', 'position'
  );
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE public.iqid_review_status AS ENUM (
    'pending',         -- just arrived, scoring not yet run
    'auto_matched',    -- ≥0.98 match; one-click confirm to merge
    'ai_proposed',     -- AI pre-pass drafted a merge proposal
    'needs_review',    -- fuzzy, multiple candidates, human required
    'approved_new',    -- human approved as new entity, iqid assigned
    'merged',          -- human merged into existing iqid
    'rejected'         -- garbage / spam
  );
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- ── 3. Core helper functions ────────────────────────────────────────────────

-- Immutable wrapper around extensions.unaccent. LANGUAGE plpgsql so Postgres
-- does not inline the body and see through to the underlying STABLE unaccent.
-- This is the documented pattern for using unaccent in generated columns
-- and expression indexes.
CREATE OR REPLACE FUNCTION public.iqid_unaccent_immutable(s text)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE PARALLEL SAFE STRICT
AS $$
BEGIN
  RETURN extensions.unaccent('extensions.unaccent', s);
END;
$$;

-- Normalize a name for fuzzy matching: lowercase, unaccent, strip punctuation,
-- collapse whitespace. pg_trgm does the rest.
CREATE OR REPLACE FUNCTION public.iqid_normalize_name(s text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT NULLIF(
    trim(
      regexp_replace(
        lower(public.iqid_unaccent_immutable(coalesce(s, ''))),
        '[^a-z0-9]+', ' ', 'g'
      )
    ),
    ''
  );
$$;

-- Prefix per entity type. Single source of truth.
CREATE OR REPLACE FUNCTION public.iqid_prefix(et public.iqid_entity_type)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT CASE et
    WHEN 'property'::public.iqid_entity_type    THEN 'prop'
    WHEN 'project'::public.iqid_entity_type     THEN 'proj'
    WHEN 'vendor'::public.iqid_entity_type      THEN 'vend'
    WHEN 'stakeholder'::public.iqid_entity_type THEN 'stake'
    WHEN 'contact'::public.iqid_entity_type     THEN 'cont'
    WHEN 'facility'::public.iqid_entity_type    THEN 'fac'
    WHEN 'team'::public.iqid_entity_type        THEN 'team'
    WHEN 'scope'::public.iqid_entity_type       THEN 'scope'
    WHEN 'position'::public.iqid_entity_type    THEN 'pos'
  END;
$$;

-- Random base32 suffix (10 chars by default). Charset excludes 0/o, 1/l/i to
-- avoid confusion in screenshots/voice. ~31^10 ≈ 8.2e14 combinations.
CREATE OR REPLACE FUNCTION public.iqid_random_suffix(n int DEFAULT 10)
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
  charset  text := 'abcdefghjkmnpqrstuvwxyz23456789';
  out_str  text := '';
  i        int;
BEGIN
  FOR i IN 1..n LOOP
    out_str := out_str ||
      substr(charset, 1 + floor(random() * length(charset))::int, 1);
  END LOOP;
  RETURN out_str;
END;
$$;

-- Mint a fresh unique iqid for the given entity type. Checks the relevant
-- registry table for collisions and retries up to 5 times.
CREATE OR REPLACE FUNCTION public.iqid_mint(et public.iqid_entity_type)
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
  candidate    text;
  registry_tbl text := et::text || '_registry';
  attempt      int  := 0;
  found_row    boolean;
BEGIN
  LOOP
    candidate := 'iqid_' || public.iqid_prefix(et) || '_' || public.iqid_random_suffix(10);

    EXECUTE format(
      'SELECT EXISTS (SELECT 1 FROM public.%I WHERE iqid = $1)',
      registry_tbl
    ) INTO found_row USING candidate;

    IF NOT found_row THEN
      RETURN candidate;
    END IF;

    attempt := attempt + 1;
    IF attempt >= 5 THEN
      RAISE EXCEPTION 'Failed to mint unique iqid for % after 5 attempts', et;
    END IF;
  END LOOP;
END;
$$;

-- ── 4. Add iqid column to every registry ────────────────────────────────────
-- Nullable until backfilled / reviewed. UNIQUE so collisions surface immediately.
ALTER TABLE public.property_registry    ADD COLUMN IF NOT EXISTS iqid text;
ALTER TABLE public.project_registry     ADD COLUMN IF NOT EXISTS iqid text;
ALTER TABLE public.vendor_registry      ADD COLUMN IF NOT EXISTS iqid text;
ALTER TABLE public.stakeholder_registry ADD COLUMN IF NOT EXISTS iqid text;
ALTER TABLE public.contact_registry     ADD COLUMN IF NOT EXISTS iqid text;
ALTER TABLE public.facility_registry    ADD COLUMN IF NOT EXISTS iqid text;
ALTER TABLE public.team_registry        ADD COLUMN IF NOT EXISTS iqid text;
ALTER TABLE public.scope_registry       ADD COLUMN IF NOT EXISTS iqid text;
ALTER TABLE public.position_registry    ADD COLUMN IF NOT EXISTS iqid text;

CREATE UNIQUE INDEX IF NOT EXISTS property_registry_iqid_uq
  ON public.property_registry (iqid) WHERE iqid IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS project_registry_iqid_uq
  ON public.project_registry (iqid) WHERE iqid IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS vendor_registry_iqid_uq
  ON public.vendor_registry (iqid) WHERE iqid IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS stakeholder_registry_iqid_uq
  ON public.stakeholder_registry (iqid) WHERE iqid IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS contact_registry_iqid_uq
  ON public.contact_registry (iqid) WHERE iqid IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS facility_registry_iqid_uq
  ON public.facility_registry (iqid) WHERE iqid IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS team_registry_iqid_uq
  ON public.team_registry (iqid) WHERE iqid IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS scope_registry_iqid_uq
  ON public.scope_registry (iqid) WHERE iqid IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS position_registry_iqid_uq
  ON public.position_registry (iqid) WHERE iqid IS NOT NULL;

-- ── 5. Normalized name + trigram index on the externally-sourced registries ─
-- Used by the fuzzy-match logic and by the dedupe scan.
ALTER TABLE public.property_registry
  ADD COLUMN IF NOT EXISTS normalized_name text
  GENERATED ALWAYS AS (public.iqid_normalize_name(property_name)) STORED;
CREATE INDEX IF NOT EXISTS property_registry_normalized_name_trgm
  ON public.property_registry USING gin (normalized_name extensions.gin_trgm_ops);

ALTER TABLE public.project_registry
  ADD COLUMN IF NOT EXISTS normalized_name text
  GENERATED ALWAYS AS (public.iqid_normalize_name(project_name)) STORED;
CREATE INDEX IF NOT EXISTS project_registry_normalized_name_trgm
  ON public.project_registry USING gin (normalized_name extensions.gin_trgm_ops);

ALTER TABLE public.vendor_registry
  ADD COLUMN IF NOT EXISTS normalized_name text
  GENERATED ALWAYS AS (
    public.iqid_normalize_name(coalesce(vendor_legal_name, vendor_name))
  ) STORED;
CREATE INDEX IF NOT EXISTS vendor_registry_normalized_name_trgm
  ON public.vendor_registry USING gin (normalized_name extensions.gin_trgm_ops);

ALTER TABLE public.stakeholder_registry
  ADD COLUMN IF NOT EXISTS normalized_name text
  GENERATED ALWAYS AS (
    public.iqid_normalize_name(coalesce(legal_name, dba_name, stakeholder_name))
  ) STORED;
CREATE INDEX IF NOT EXISTS stakeholder_registry_normalized_name_trgm
  ON public.stakeholder_registry USING gin (normalized_name extensions.gin_trgm_ops);

-- Note: contact_registry.display_name is itself a generated column, so we
-- reference first_name/last_name/email directly. Avoid concat_ws (STABLE) —
-- use || with coalesce so the expression stays IMMUTABLE.
ALTER TABLE public.contact_registry
  ADD COLUMN IF NOT EXISTS normalized_name text
  GENERATED ALWAYS AS (
    public.iqid_normalize_name(
      coalesce(
        nullif(trim(coalesce(first_name, '') || ' ' || coalesce(last_name, '')), ''),
        email
      )
    )
  ) STORED;
CREATE INDEX IF NOT EXISTS contact_registry_normalized_name_trgm
  ON public.contact_registry USING gin (normalized_name extensions.gin_trgm_ops);

ALTER TABLE public.facility_registry
  ADD COLUMN IF NOT EXISTS normalized_name text
  GENERATED ALWAYS AS (public.iqid_normalize_name(facility_name)) STORED;
CREATE INDEX IF NOT EXISTS facility_registry_normalized_name_trgm
  ON public.facility_registry USING gin (normalized_name extensions.gin_trgm_ops);

-- ── 6. registry_intake_staging — landing zone for all new entity writes ─────
-- Every external write goes here first. No iqid until reviewed.
CREATE TABLE IF NOT EXISTS public.registry_intake_staging (
  id                    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  entity_type           public.iqid_entity_type NOT NULL,

  -- Raw incoming payload — exactly as received
  raw_name              text NOT NULL,
  raw_payload           jsonb NOT NULL DEFAULT '{}'::jsonb,

  -- Common comparison fields (denormalized for fast matching; keep raw in payload)
  raw_address_line1     text,
  raw_city              text,
  raw_state             text,
  raw_postal_code       text,
  raw_country           text,
  raw_email             text,
  raw_phone             text,
  raw_website           text,
  external_ids          jsonb NOT NULL DEFAULT '{}'::jsonb,

  -- Provenance
  source                text NOT NULL,
  source_record_id      text,

  -- Derived / scoring
  normalized_name       text GENERATED ALWAYS AS (
    public.iqid_normalize_name(raw_name)
  ) STORED,
  match_score           numeric(4,3),
  candidate_matches     jsonb NOT NULL DEFAULT '[]'::jsonb,
  -- [{ registry_id: uuid, iqid: text, score: numeric, reason: text }]

  ai_proposal           jsonb,
  -- { action: 'merge' | 'new', target_registry_id?: uuid, target_iqid?: text,
  --   confidence: numeric, reasoning: text, model: text, run_at: timestamptz }

  -- Review outcome
  review_status         public.iqid_review_status NOT NULL DEFAULT 'pending',
  resolved_registry_id  uuid,
  resolved_iqid         text,
  reviewed_by           text,
  reviewed_at           timestamptz,
  reviewer_notes        text,

  created_at            timestamptz NOT NULL DEFAULT now(),
  updated_at            timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS registry_intake_staging_entity_status_idx
  ON public.registry_intake_staging (entity_type, review_status);
CREATE INDEX IF NOT EXISTS registry_intake_staging_entity_normname_idx
  ON public.registry_intake_staging (entity_type, normalized_name);
CREATE INDEX IF NOT EXISTS registry_intake_staging_normname_trgm
  ON public.registry_intake_staging USING gin (normalized_name extensions.gin_trgm_ops);
CREATE INDEX IF NOT EXISTS registry_intake_staging_external_ids_idx
  ON public.registry_intake_staging USING gin (external_ids);
CREATE INDEX IF NOT EXISTS registry_intake_staging_source_idx
  ON public.registry_intake_staging (source, source_record_id);

-- Idempotent ingest: same source + source_record_id can't double-stage
CREATE UNIQUE INDEX IF NOT EXISTS registry_intake_staging_source_unique
  ON public.registry_intake_staging (entity_type, source, source_record_id)
  WHERE source_record_id IS NOT NULL;

-- Touch updated_at on UPDATE
CREATE OR REPLACE FUNCTION public.tg_iqid_touch_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS registry_intake_staging_touch ON public.registry_intake_staging;
CREATE TRIGGER registry_intake_staging_touch
  BEFORE UPDATE ON public.registry_intake_staging
  FOR EACH ROW EXECUTE FUNCTION public.tg_iqid_touch_updated_at();

-- ── 7. registry_alias — known alternate names → one iqid ────────────────────
CREATE TABLE IF NOT EXISTS public.registry_alias (
  id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  entity_type       public.iqid_entity_type NOT NULL,
  iqid              text NOT NULL,
  registry_id       uuid NOT NULL,
  alias_name        text NOT NULL,
  normalized_alias  text GENERATED ALWAYS AS (
    public.iqid_normalize_name(alias_name)
  ) STORED,
  source            text,
  notes             text,
  created_at        timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS registry_alias_entity_iqid_idx
  ON public.registry_alias (entity_type, iqid);
CREATE INDEX IF NOT EXISTS registry_alias_entity_registry_id_idx
  ON public.registry_alias (entity_type, registry_id);
CREATE INDEX IF NOT EXISTS registry_alias_normname_trgm
  ON public.registry_alias USING gin (normalized_alias extensions.gin_trgm_ops);
CREATE UNIQUE INDEX IF NOT EXISTS registry_alias_dedupe
  ON public.registry_alias (entity_type, iqid, normalized_alias)
  WHERE normalized_alias IS NOT NULL;

-- ── 8. registry_dedupe_review — one-time dedupe of existing rows ────────────
-- Populated by the dedupe scan (separate script). Same UI handles intake
-- review + dedupe review.
CREATE TABLE IF NOT EXISTS public.registry_dedupe_review (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  entity_type     public.iqid_entity_type NOT NULL,
  left_id         uuid NOT NULL,    -- existing registry row
  right_id        uuid NOT NULL,    -- existing registry row
  match_score     numeric(4,3) NOT NULL,
  match_reason    jsonb,            -- { signals: [...], normalized_left, normalized_right }
  ai_proposal     jsonb,            -- AI pre-pass output
  review_status   public.iqid_review_status NOT NULL DEFAULT 'needs_review',
  resolution      text,
  -- 'merge_left_into_right' | 'merge_right_into_left' | 'distinct' | 'rejected'
  resolved_iqid   text,             -- the iqid of the survivor (after merge)
  reviewed_by     text,
  reviewed_at     timestamptz,
  reviewer_notes  text,
  created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS registry_dedupe_review_entity_status_idx
  ON public.registry_dedupe_review (entity_type, review_status, match_score DESC);
CREATE INDEX IF NOT EXISTS registry_dedupe_review_left_idx
  ON public.registry_dedupe_review (entity_type, left_id);
CREATE INDEX IF NOT EXISTS registry_dedupe_review_right_idx
  ON public.registry_dedupe_review (entity_type, right_id);

-- Unordered pair uniqueness — (a,b) and (b,a) are the same pair.
CREATE UNIQUE INDEX IF NOT EXISTS registry_dedupe_review_pair_unique
  ON public.registry_dedupe_review (
    entity_type,
    LEAST(left_id::text, right_id::text),
    GREATEST(left_id::text, right_id::text)
  );

-- ── 9. Documentation comments ───────────────────────────────────────────────
COMMENT ON TABLE public.registry_intake_staging IS
  'Landing zone for all new entity writes across registries. No iqid is assigned until a human (or auto-match ≥0.98) resolves the row.';
COMMENT ON TABLE public.registry_alias IS
  'Alternate names tied to one iqid. Populated whenever a merge happens so future variants auto-match.';
COMMENT ON TABLE public.registry_dedupe_review IS
  'One-time dedupe queue over existing registry rows. Same review UI as intake.';
COMMENT ON FUNCTION public.iqid_mint(public.iqid_entity_type) IS
  'Mint a unique iqid_<prefix>_<10char-base32> for the given entity type. Checks the relevant registry for collisions.';

COMMIT;
