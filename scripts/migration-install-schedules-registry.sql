-- Registry-iQ: Install schedules (DALE-Demand) ↔ project_registry + phases + documents
-- Run in Supabase SQL editor (Registry-iQ). Idempotent (IF NOT EXISTS).
--
-- CRITICAL: Project Data Integrity — Projects must NEVER be collapsed, merged,
-- overwritten, or deduplicated without explicit human approval. When linking
-- install_schedules or any source to properties, projects are ADDITIVE.
--
-- Ordering: Safe to run before Access 2013 migration. The `project_install_days`
-- block at the end runs only if that table exists (created by
-- migration-access-2013-historical.sql). After applying Access migration, re-run
-- this file once so `phase_id` is added, or run only that DO block.

-- ─── property_registry: inline Cloudinary document refs (PDFs, etc.) ─────
ALTER TABLE property_registry
  ADD COLUMN IF NOT EXISTS documents JSONB NOT NULL DEFAULT '[]'::jsonb;

COMMENT ON COLUMN property_registry.documents IS 'JSON array of Cloudinary doc refs: { url, public_id, label, type, uploaded_at }.';

-- ─── property_unit_types: layout PDFs / spec sheets ───────────────────────
ALTER TABLE property_unit_types
  ADD COLUMN IF NOT EXISTS documents JSONB NOT NULL DEFAULT '[]'::jsonb;

COMMENT ON COLUMN property_unit_types.documents IS 'JSON array of Cloudinary document refs for floorplans/specs.';

-- ─── project_registry: DALE-Demand install schedule + D365 + logistics ─────
ALTER TABLE project_registry ADD COLUMN IF NOT EXISTS division TEXT;
ALTER TABLE project_registry ADD COLUMN IF NOT EXISTS dale_install_schedule_id UUID;
ALTER TABLE project_registry ADD COLUMN IF NOT EXISTS d365_opportunity_code TEXT;
ALTER TABLE project_registry ADD COLUMN IF NOT EXISTS d365_opportunity_name TEXT;
ALTER TABLE project_registry ADD COLUMN IF NOT EXISTS d365_account_name TEXT;
ALTER TABLE project_registry ADD COLUMN IF NOT EXISTS d365_amount NUMERIC;
ALTER TABLE project_registry ADD COLUMN IF NOT EXISTS d365_status TEXT;
ALTER TABLE project_registry ADD COLUMN IF NOT EXISTS d365_division TEXT;
ALTER TABLE project_registry ADD COLUMN IF NOT EXISTS d365_delivery_date DATE;
ALTER TABLE project_registry ADD COLUMN IF NOT EXISTS schedule_year INTEGER;
ALTER TABLE project_registry ADD COLUMN IF NOT EXISTS estimated_completion_date DATE;
ALTER TABLE project_registry ADD COLUMN IF NOT EXISTS num_days INTEGER;
ALTER TABLE project_registry ADD COLUMN IF NOT EXISTS sales_person TEXT;
ALTER TABLE project_registry ADD COLUMN IF NOT EXISTS on_site_installer TEXT;
ALTER TABLE project_registry ADD COLUMN IF NOT EXISTS property_contact_name TEXT;
ALTER TABLE project_registry ADD COLUMN IF NOT EXISTS warehouse_contact_name TEXT;
ALTER TABLE project_registry ADD COLUMN IF NOT EXISTS warehouse_contact_email TEXT;
ALTER TABLE project_registry ADD COLUMN IF NOT EXISTS warehouse_address TEXT;
ALTER TABLE project_registry ADD COLUMN IF NOT EXISTS temp_labor TEXT;
ALTER TABLE project_registry ADD COLUMN IF NOT EXISTS backup_temp_labor TEXT;
ALTER TABLE project_registry ADD COLUMN IF NOT EXISTS labor_options TEXT;
ALTER TABLE project_registry ADD COLUMN IF NOT EXISTS additional_items TEXT;
ALTER TABLE project_registry ADD COLUMN IF NOT EXISTS access_notes TEXT;
ALTER TABLE project_registry ADD COLUMN IF NOT EXISTS darlas_contact TEXT;
ALTER TABLE project_registry ADD COLUMN IF NOT EXISTS barstool_confirmed TEXT;
ALTER TABLE project_registry ADD COLUMN IF NOT EXISTS source_file TEXT;
ALTER TABLE project_registry ADD COLUMN IF NOT EXISTS sheet_name TEXT;
ALTER TABLE project_registry ADD COLUMN IF NOT EXISTS line_number INTEGER;
ALTER TABLE project_registry ADD COLUMN IF NOT EXISTS images JSONB NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE project_registry ADD COLUMN IF NOT EXISTS documents JSONB NOT NULL DEFAULT '[]'::jsonb;

-- install_start_date may already exist from prior schema; keep IF NOT EXISTS
ALTER TABLE project_registry ADD COLUMN IF NOT EXISTS install_start_date DATE;

CREATE INDEX IF NOT EXISTS idx_project_registry_dale_install_schedule_id
  ON project_registry (dale_install_schedule_id)
  WHERE dale_install_schedule_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_project_registry_schedule_year
  ON project_registry (schedule_year)
  WHERE schedule_year IS NOT NULL;

COMMENT ON COLUMN project_registry.dale_install_schedule_id IS 'UUID of row in DALE-Demand install_schedules (cross-project reference; not enforced).';
COMMENT ON COLUMN project_registry.access_notes IS 'Install access constraints; renamed from source column `access` to avoid SQL keyword collision.';
COMMENT ON COLUMN project_registry.images IS 'JSON array of Cloudinary image refs for this project.';
COMMENT ON COLUMN project_registry.documents IS 'JSON array of Cloudinary document refs for this project.';

-- ─── project_install_phases: child rows per deal (main vs add-on, by year) ─
CREATE TABLE IF NOT EXISTS project_install_phases (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  project_registry_id UUID NOT NULL REFERENCES project_registry(id) ON DELETE CASCADE,
  phase_number SMALLINT NOT NULL DEFAULT 1,
  phase_label TEXT,
  schedule_year INTEGER,
  install_start_date DATE,
  estimated_completion_date DATE,
  num_days INTEGER,
  fulfillment_mode TEXT CHECK (fulfillment_mode IN ('install', 'dropship', 'unknown')),
  dale_install_schedule_id UUID,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_project_install_phases_project
  ON project_install_phases (project_registry_id);

CREATE INDEX IF NOT EXISTS idx_project_install_phases_schedule_year
  ON project_install_phases (schedule_year);

CREATE UNIQUE INDEX IF NOT EXISTS idx_project_install_phases_dale_unique
  ON project_install_phases (dale_install_schedule_id)
  WHERE dale_install_schedule_id IS NOT NULL;

CREATE OR REPLACE FUNCTION project_install_phases_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_project_install_phases_updated ON project_install_phases;
CREATE TRIGGER trg_project_install_phases_updated
  BEFORE UPDATE ON project_install_phases
  FOR EACH ROW EXECUTE FUNCTION project_install_phases_set_updated_at();

ALTER TABLE project_install_phases ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "Allow all access to project_install_phases" ON project_install_phases;
CREATE POLICY "Allow all access to project_install_phases" ON project_install_phases FOR ALL USING (true) WITH CHECK (true);

COMMENT ON TABLE project_install_phases IS 'Install phases under a project (e.g. main install vs additional items); links to DALE-Demand schedule rows.';

-- ─── project_install_days: optional link to phase ─────────────────────────
-- Table is created by migration-access-2013-historical.sql; skip if not present.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_name = 'project_install_days'
  ) THEN
    ALTER TABLE public.project_install_days
      ADD COLUMN IF NOT EXISTS phase_id UUID REFERENCES project_install_phases(id) ON DELETE SET NULL;
    CREATE INDEX IF NOT EXISTS idx_project_install_days_phase ON public.project_install_days (phase_id);
    COMMENT ON COLUMN public.project_install_days.phase_id IS 'Optional link to project_install_phases for multi-phase schedules.';
  END IF;
END $$;
