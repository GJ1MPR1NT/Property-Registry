-- Registry-iQ: Access / SQLite 2013 historical ingest — columns + install schedule
-- Run in Supabase SQL editor (Registry-iQ). Idempotent (IF NOT EXISTS / OR REPLACE VIEW).
--
-- CRITICAL: Project Data Integrity — Projects must NEVER be collapsed, merged,
-- overwritten, or deduplicated without explicit human approval. Legacy Access
-- projects and install-schedule–linked projects are additive; preserve full history.

-- ─── project_registry: legacy keys + phase + fulfillment ─────────────────
ALTER TABLE project_registry
  ADD COLUMN IF NOT EXISTS legacy_access_project_id INTEGER;

ALTER TABLE project_registry
  ADD COLUMN IF NOT EXISTS access_project_description TEXT;

ALTER TABLE project_registry
  ADD COLUMN IF NOT EXISTS access_creation_date TIMESTAMPTZ;

ALTER TABLE project_registry
  ADD COLUMN IF NOT EXISTS install_phase SMALLINT NOT NULL DEFAULT 1;

ALTER TABLE project_registry
  ADD COLUMN IF NOT EXISTS fulfillment_mode TEXT NOT NULL DEFAULT 'unknown'
    CHECK (fulfillment_mode IN ('install', 'dropship', 'unknown'));

ALTER TABLE project_registry
  ADD COLUMN IF NOT EXISTS parent_project_registry_id UUID REFERENCES project_registry(id) ON DELETE SET NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_project_registry_legacy_access_project_id
  ON project_registry (legacy_access_project_id)
  WHERE legacy_access_project_id IS NOT NULL;

COMMENT ON COLUMN project_registry.legacy_access_project_id IS 'Integer ProjectID from legacy Access / 2013SQLite export.';
COMMENT ON COLUMN project_registry.access_project_description IS 'Raw Description field from Access Project table.';
COMMENT ON COLUMN project_registry.access_creation_date IS 'CreationDate from Access Project.';
COMMENT ON COLUMN project_registry.install_phase IS 'Install phase within deal (1 = default; use .1/.2 in UI via project_id rules if needed).';
COMMENT ON COLUMN project_registry.fulfillment_mode IS 'install = site install; dropship = direct ship.';
COMMENT ON COLUMN project_registry.parent_project_registry_id IS 'Add-on / fill-in project linked to main project row.';

-- ─── property_unit_types: legacy Access keys ─────────────────────────────
ALTER TABLE property_unit_types
  ADD COLUMN IF NOT EXISTS legacy_access_project_id INTEGER;

ALTER TABLE property_unit_types
  ADD COLUMN IF NOT EXISTS legacy_access_unit_type_id INTEGER;

CREATE UNIQUE INDEX IF NOT EXISTS idx_put_legacy_access_ut
  ON property_unit_types (property_id, legacy_access_unit_type_id)
  WHERE legacy_access_unit_type_id IS NOT NULL;

COMMENT ON COLUMN property_unit_types.legacy_access_project_id IS 'Access Project.ProjectID for this row.';
COMMENT ON COLUMN property_unit_types.legacy_access_unit_type_id IS 'Access UnitType.UnitTypeID (scoped per project).';

-- ─── property_unit_type_skus: legacy + pack size for pull lists ──────────
ALTER TABLE property_unit_type_skus
  ADD COLUMN IF NOT EXISTS legacy_access_project_id INTEGER;

ALTER TABLE property_unit_type_skus
  ADD COLUMN IF NOT EXISTS legacy_access_unit_type_id INTEGER;

ALTER TABLE property_unit_type_skus
  ADD COLUMN IF NOT EXISTS legacy_access_item_id INTEGER;

ALTER TABLE property_unit_type_skus
  ADD COLUMN IF NOT EXISTS qty_per_box INTEGER;

COMMENT ON COLUMN property_unit_type_skus.legacy_access_project_id IS 'Access Project.ProjectID for this BOM line.';
COMMENT ON COLUMN property_unit_type_skus.legacy_access_unit_type_id IS 'Access UnitType.UnitTypeID for this BOM line.';
COMMENT ON COLUMN property_unit_type_skus.legacy_access_item_id IS 'Access Item.ItemID (scoped per project).';
COMMENT ON COLUMN property_unit_type_skus.qty_per_box IS 'Units per sellable box (Item.QtyPerBox); pull-list box math. Prefer SKU Registry master when synced; keep here for historical line-level truth.';

-- ─── property_units: install day linkage + legacy ─────────────────────────
ALTER TABLE property_units
  ADD COLUMN IF NOT EXISTS legacy_access_project_id INTEGER;

ALTER TABLE property_units
  ADD COLUMN IF NOT EXISTS legacy_access_day_id INTEGER;

ALTER TABLE property_units
  ADD COLUMN IF NOT EXISTS install_date DATE;

COMMENT ON COLUMN property_units.legacy_access_day_id IS 'Access Day.DayID for the unit install row.';
COMMENT ON COLUMN property_units.install_date IS 'Denormalized from Access Day.DayDate for this unit.';

-- ─── Calendar install days (one row per project calendar date) ───────────
CREATE TABLE IF NOT EXISTS project_install_days (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  project_registry_id UUID NOT NULL REFERENCES project_registry(id) ON DELETE CASCADE,
  calendar_date DATE NOT NULL,
  day_type TEXT NOT NULL CHECK (day_type IN (
    'install_weekday',
    'install_weekend',
    'down_weekday',
    'other'
  )),
  truck_number INTEGER,
  units_installed_count INTEGER,
  legacy_access_project_id INTEGER NOT NULL,
  legacy_access_day_id INTEGER NOT NULL,
  opswd_units NUMERIC,
  metric_definition TEXT,
  metadata JSONB NOT NULL DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT project_install_days_unique_calendar UNIQUE (project_registry_id, calendar_date)
);

CREATE INDEX IF NOT EXISTS idx_project_install_days_project ON project_install_days (project_registry_id);
CREATE INDEX IF NOT EXISTS idx_project_install_days_date ON project_install_days (calendar_date);

COMMENT ON TABLE project_install_days IS 'Per-day install schedule from Access Day + UnitNumber; opswd_units optional snapshot.';
COMMENT ON COLUMN project_install_days.day_type IS 'Derived: install_* if units that day; down_weekday if weekday with zero units; other for edge cases.';
COMMENT ON COLUMN project_install_days.opswd_units IS 'Output per standard workday (1 team, 1 shift) — optional computed snapshot.';
COMMENT ON COLUMN project_install_days.metric_definition IS 'Human-readable note for how opswd was calculated (versioned).';

CREATE OR REPLACE FUNCTION project_install_days_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_project_install_days_updated ON project_install_days;
CREATE TRIGGER trg_project_install_days_updated
  BEFORE UPDATE ON project_install_days
  FOR EACH ROW EXECUTE FUNCTION project_install_days_set_updated_at();

ALTER TABLE project_install_days ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "Allow all access to project_install_days" ON project_install_days;
CREATE POLICY "Allow all access to project_install_days" ON project_install_days FOR ALL USING (true) WITH CHECK (true);

-- ─── Go-forward capacity plan (quote / interventions) ──────────────────────
CREATE TABLE IF NOT EXISTS project_install_capacity_plan (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  project_registry_id UUID NOT NULL REFERENCES project_registry(id) ON DELETE CASCADE,
  valid_from DATE,
  valid_to DATE,
  workday_mon BOOLEAN NOT NULL DEFAULT true,
  workday_tue BOOLEAN NOT NULL DEFAULT true,
  workday_wed BOOLEAN NOT NULL DEFAULT true,
  workday_thu BOOLEAN NOT NULL DEFAULT true,
  workday_fri BOOLEAN NOT NULL DEFAULT true,
  workday_sat BOOLEAN NOT NULL DEFAULT false,
  workday_sun BOOLEAN NOT NULL DEFAULT false,
  shifts_day NUMERIC NOT NULL DEFAULT 1,
  shifts_night NUMERIC NOT NULL DEFAULT 0,
  teams_planned INTEGER,
  teams_max INTEGER,
  notes TEXT,
  metadata JSONB NOT NULL DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_project_install_capacity_project ON project_install_capacity_plan (project_registry_id);

COMMENT ON TABLE project_install_capacity_plan IS 'Planned workdays, shifts, teams — for quoting and multi-team interventions.';

CREATE OR REPLACE FUNCTION project_install_capacity_plan_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_project_install_capacity_plan_updated ON project_install_capacity_plan;
CREATE TRIGGER trg_project_install_capacity_plan_updated
  BEFORE UPDATE ON project_install_capacity_plan
  FOR EACH ROW EXECUTE FUNCTION project_install_capacity_plan_set_updated_at();

ALTER TABLE project_install_capacity_plan ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "Allow all access to project_install_capacity_plan" ON project_install_capacity_plan;
CREATE POLICY "Allow all access to project_install_capacity_plan" ON project_install_capacity_plan FOR ALL USING (true) WITH CHECK (true);

-- ─── View: per-day unit throughput (basis for OPSWD reporting) ───────────
CREATE OR REPLACE VIEW v_project_install_day_throughput AS
SELECT
  d.id,
  d.project_registry_id,
  d.calendar_date,
  d.day_type,
  d.units_installed_count,
  d.opswd_units,
  d.truck_number,
  d.legacy_access_project_id,
  d.legacy_access_day_id
FROM project_install_days d;

COMMENT ON VIEW v_project_install_day_throughput IS 'Install-day facts; aggregate for OPSWD windows in app or BI.';
