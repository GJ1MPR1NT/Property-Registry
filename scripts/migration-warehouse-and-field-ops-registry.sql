-- Registry-iQ: Warehouse registry + Field Ops / Site Management registry
-- Run in Supabase SQL editor (Registry-iQ). Idempotent (IF NOT EXISTS).
--
CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA extensions;
--
-- Purpose:
--   warehouse_registry — physical ship/staging sites (address, scale, contacts, service history)
--   field_ops_registry — people (installers, site contacts, sales on jobs) for scheduling / estimating
--   Junction tables link warehouses and field ops to projects/properties with provenance from install schedules.

-- ─── warehouse_registry ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS warehouse_registry (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  dedupe_key TEXT NOT NULL,
  warehouse_name TEXT NOT NULL,
  address_line1 TEXT,
  address_line2 TEXT,
  city TEXT,
  state_province TEXT,
  postal_code TEXT,
  country TEXT NOT NULL DEFAULT 'US',
  latitude DOUBLE PRECISION,
  longitude DOUBLE PRECISION,
  scale_notes TEXT,
  primary_contact_name TEXT,
  primary_email TEXT,
  primary_phone TEXT,
  external_ids JSONB NOT NULL DEFAULT '{}'::jsonb,
  source TEXT NOT NULL DEFAULT 'install_schedules',
  notes TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT warehouse_registry_dedupe_key_unique UNIQUE (dedupe_key)
);

CREATE INDEX IF NOT EXISTS idx_warehouse_registry_name_trgm
  ON warehouse_registry USING gin (warehouse_name extensions.gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_warehouse_registry_city_state
  ON warehouse_registry (city, state_province);

COMMENT ON TABLE warehouse_registry IS 'TLC / partner warehouse and staging locations; service history via warehouse_project_service.';
COMMENT ON COLUMN warehouse_registry.dedupe_key IS 'Stable key from normalized address + region or contact/email (app-generated).';
COMMENT ON COLUMN warehouse_registry.scale_notes IS 'Sq ft, dock doors, capacity — enrich over time.';

-- ─── Projects serviced by a warehouse (history / roll-up for reporting) ───
CREATE TABLE IF NOT EXISTS warehouse_project_service (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  warehouse_registry_id UUID NOT NULL REFERENCES warehouse_registry(id) ON DELETE CASCADE,
  project_registry_id UUID NOT NULL REFERENCES project_registry(id) ON DELETE CASCADE,
  dale_install_schedule_id UUID,
  source TEXT NOT NULL DEFAULT 'install_schedules',
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT warehouse_project_service_unique UNIQUE (warehouse_registry_id, project_registry_id)
);

CREATE INDEX IF NOT EXISTS idx_warehouse_project_service_project
  ON warehouse_project_service (project_registry_id);

CREATE INDEX IF NOT EXISTS idx_warehouse_project_service_warehouse
  ON warehouse_project_service (warehouse_registry_id);

COMMENT ON TABLE warehouse_project_service IS 'Which projects a warehouse has serviced (from schedules + future sources).';

-- ─── Field Ops / Site Management — people (enrichment-friendly) ───────────
CREATE TABLE IF NOT EXISTS field_ops_registry (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  dedupe_key TEXT NOT NULL,
  display_name TEXT NOT NULL,
  first_name TEXT,
  last_name TEXT,
  email TEXT,
  phone TEXT,
  role_category TEXT NOT NULL DEFAULT 'other' CHECK (role_category IN (
    'field_ops', 'site_management', 'installer', 'sales', 'labor', 'other'
  )),
  enrichment_status TEXT NOT NULL DEFAULT 'pending' CHECK (enrichment_status IN (
    'pending', 'partial', 'enriched'
  )),
  external_ids JSONB NOT NULL DEFAULT '{}'::jsonb,
  notes TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT field_ops_registry_dedupe_key_unique UNIQUE (dedupe_key)
);

CREATE INDEX IF NOT EXISTS idx_field_ops_registry_email
  ON field_ops_registry (email)
  WHERE email IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_field_ops_registry_display_trgm
  ON field_ops_registry USING gin (display_name extensions.gin_trgm_ops);

COMMENT ON TABLE field_ops_registry IS 'Field ops, site management, installers, sales contacts on jobs — link via field_ops_assignment.';
COMMENT ON COLUMN field_ops_registry.enrichment_status IS 'pending = name-only from schedules; enriched after email/phone/name parse.';

-- ─── Assignments: person ↔ property + project (scheduling / estimating) ─────
CREATE TABLE IF NOT EXISTS field_ops_assignment (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  field_ops_registry_id UUID NOT NULL REFERENCES field_ops_registry(id) ON DELETE CASCADE,
  property_id UUID REFERENCES property_registry(id) ON DELETE CASCADE,
  project_registry_id UUID NOT NULL REFERENCES project_registry(id) ON DELETE CASCADE,
  assignment_role TEXT NOT NULL,
  dale_install_schedule_id UUID,
  source TEXT NOT NULL DEFAULT 'install_schedules',
  notes TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT field_ops_assignment_unique UNIQUE (field_ops_registry_id, project_registry_id, assignment_role)
);

CREATE INDEX IF NOT EXISTS idx_field_ops_assignment_property
  ON field_ops_assignment (property_id);

CREATE INDEX IF NOT EXISTS idx_field_ops_assignment_project
  ON field_ops_assignment (project_registry_id);

CREATE INDEX IF NOT EXISTS idx_field_ops_assignment_field_ops
  ON field_ops_assignment (field_ops_registry_id);

COMMENT ON TABLE field_ops_assignment IS 'Which properties/projects a field-ops person touched; assignment_role = on_site_installer, property_contact, sales_person, etc.';
COMMENT ON COLUMN field_ops_assignment.assignment_role IS 'Source field role from install_schedules (not the same as field_ops_registry.role_category).';

-- ─── project_registry: default warehouse for the job (optional pointer) ─────
ALTER TABLE project_registry
  ADD COLUMN IF NOT EXISTS warehouse_registry_id UUID REFERENCES warehouse_registry(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_project_registry_warehouse
  ON project_registry (warehouse_registry_id)
  WHERE warehouse_registry_id IS NOT NULL;

COMMENT ON COLUMN project_registry.warehouse_registry_id IS 'Primary warehouse for this project when known (install schedules).';

-- ─── Triggers: updated_at ───────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION warehouse_registry_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_warehouse_registry_updated ON warehouse_registry;
CREATE TRIGGER trg_warehouse_registry_updated
  BEFORE UPDATE ON warehouse_registry
  FOR EACH ROW EXECUTE FUNCTION warehouse_registry_set_updated_at();

CREATE OR REPLACE FUNCTION field_ops_registry_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_field_ops_registry_updated ON field_ops_registry;
CREATE TRIGGER trg_field_ops_registry_updated
  BEFORE UPDATE ON field_ops_registry
  FOR EACH ROW EXECUTE FUNCTION field_ops_registry_set_updated_at();

CREATE OR REPLACE FUNCTION field_ops_assignment_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_field_ops_assignment_updated ON field_ops_assignment;
CREATE TRIGGER trg_field_ops_assignment_updated
  BEFORE UPDATE ON field_ops_assignment
  FOR EACH ROW EXECUTE FUNCTION field_ops_assignment_set_updated_at();

-- ─── RLS (match other Registry-iQ permissive policies) ─────────────────────
ALTER TABLE warehouse_registry ENABLE ROW LEVEL SECURITY;
ALTER TABLE warehouse_project_service ENABLE ROW LEVEL SECURITY;
ALTER TABLE field_ops_registry ENABLE ROW LEVEL SECURITY;
ALTER TABLE field_ops_assignment ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "Allow all access to warehouse_registry" ON warehouse_registry;
CREATE POLICY "Allow all access to warehouse_registry" ON warehouse_registry FOR ALL USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS "Allow all access to warehouse_project_service" ON warehouse_project_service;
CREATE POLICY "Allow all access to warehouse_project_service" ON warehouse_project_service FOR ALL USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS "Allow all access to field_ops_registry" ON field_ops_registry;
CREATE POLICY "Allow all access to field_ops_registry" ON field_ops_registry FOR ALL USING (true) WITH CHECK (true);

DROP POLICY IF EXISTS "Allow all access to field_ops_assignment" ON field_ops_assignment;
CREATE POLICY "Allow all access to field_ops_assignment" ON field_ops_assignment FOR ALL USING (true) WITH CHECK (true);
