#!/usr/bin/env node
/**
 * Registry-iQ Schema Migration
 *
 * Creates the full schema in the new Registry-iQ Supabase project:
 * - Extensions: PostGIS, pg_trgm
 * - Property tables (7): registry, buildings, floors, unit_types,
 *   stakeholders (refactored as junction), activity_log, documents
 * - Stakeholder registry (companies)
 * - Contact registry + contact_stakeholder_associations (people)
 * - Functions, triggers, indexes, RLS policies
 *
 * Usage: node scripts/migrate-to-registry-iq.mjs [--dry-run]
 */

const CONNECTION_STRING =
  "postgresql://postgres:Tlciqcortex2026%21@db.xhafhdaugmgdxckhdfov.supabase.co:5432/postgres";

const MIGRATION_SQL = `
-- ============================================================
-- Registry-iQ: Full Schema Migration
-- ============================================================

-- 1. Extensions
CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA extensions;
CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA extensions;

-- 2. Utility functions
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION compute_geo_point()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.latitude IS NOT NULL AND NEW.longitude IS NOT NULL THEN
    NEW.geo_point = ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326)::geography;
  END IF;
  RETURN NEW;
END;
$$;

-- ============================================================
-- 3. STAKEHOLDER REGISTRY (companies — the canonical source)
-- ============================================================
CREATE TABLE stakeholder_registry (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  stakeholder_name TEXT NOT NULL,
  stakeholder_type TEXT NOT NULL CHECK (stakeholder_type IN (
    'developer', 'owner', 'pe_firm', 'architect', 'interior_designer',
    'design_firm', 'gc', 'property_manager', 'asset_manager',
    'brand', 'investor', 'lender', 'ff_e_specifier',
    'purchasing_agent', 'other'
  )),
  legal_name TEXT,
  dba_name TEXT,
  website TEXT,
  hq_address_line1 TEXT,
  hq_address_line2 TEXT,
  hq_city TEXT,
  hq_state TEXT,
  hq_postal_code TEXT,
  hq_country TEXT DEFAULT 'US',
  phone TEXT,
  email TEXT,
  logo_url TEXT,
  linkedin_url TEXT,
  description TEXT,
  parent_company_id UUID REFERENCES stakeholder_registry(id),
  external_ids JSONB DEFAULT '{}',
  tags TEXT[] DEFAULT '{}',
  is_active BOOLEAN DEFAULT true,
  notes TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  created_by TEXT,
  updated_by TEXT
);

CREATE INDEX idx_stakeholder_name ON stakeholder_registry USING GIN (stakeholder_name extensions.gin_trgm_ops);
CREATE INDEX idx_stakeholder_type ON stakeholder_registry(stakeholder_type);
CREATE INDEX idx_stakeholder_active ON stakeholder_registry(is_active);

CREATE TRIGGER trg_stakeholder_registry_updated_at
  BEFORE UPDATE ON stakeholder_registry
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================
-- 4. CONTACT REGISTRY (people)
-- ============================================================
CREATE TABLE contact_registry (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  first_name TEXT NOT NULL,
  last_name TEXT NOT NULL,
  display_name TEXT GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED,
  title TEXT,
  email TEXT,
  phone TEXT,
  mobile TEXT,
  linkedin_url TEXT,
  photo_url TEXT,
  notes TEXT,
  external_ids JSONB DEFAULT '{}',
  tags TEXT[] DEFAULT '{}',
  is_active BOOLEAN DEFAULT true,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  created_by TEXT,
  updated_by TEXT
);

CREATE INDEX idx_contact_name ON contact_registry USING GIN (
  (first_name || ' ' || last_name) extensions.gin_trgm_ops
);
CREATE INDEX idx_contact_email ON contact_registry(email);

CREATE TRIGGER trg_contact_registry_updated_at
  BEFORE UPDATE ON contact_registry
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================
-- 5. CONTACT ↔ STAKEHOLDER ASSOCIATIONS (people move between companies)
-- ============================================================
CREATE TABLE contact_stakeholder_associations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  contact_id UUID NOT NULL REFERENCES contact_registry(id) ON DELETE CASCADE,
  stakeholder_id UUID NOT NULL REFERENCES stakeholder_registry(id) ON DELETE CASCADE,
  role_title TEXT,
  is_primary_contact BOOLEAN DEFAULT false,
  association_start DATE,
  association_end DATE,
  notes TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_csa_contact ON contact_stakeholder_associations(contact_id);
CREATE INDEX idx_csa_stakeholder ON contact_stakeholder_associations(stakeholder_id);
CREATE INDEX idx_csa_active ON contact_stakeholder_associations(association_end)
  WHERE association_end IS NULL;

CREATE TRIGGER trg_csa_updated_at
  BEFORE UPDATE ON contact_stakeholder_associations
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================
-- 6. PROPERTY REGISTRY (master record)
-- ============================================================
CREATE TABLE property_registry (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  property_name TEXT NOT NULL,
  property_name_alt TEXT,
  property_code TEXT UNIQUE,
  external_ids JSONB DEFAULT '{}',
  property_type TEXT NOT NULL CHECK (property_type IN (
    'student_housing', 'hospitality', 'multifamily',
    'build_to_rent', 'senior_living', 'military', 'other'
  )),
  property_subtype TEXT,
  property_status TEXT DEFAULT 'active' CHECK (property_status IN (
    'prospect', 'pre_development', 'under_construction', 'active',
    'renovation', 'turn_in_progress', 'inactive', 'competitor'
  )),
  tlc_relationship TEXT DEFAULT 'customer' CHECK (tlc_relationship IN (
    'customer', 'prospect', 'former_customer', 'competitor', 'market_comp'
  )),
  address_line1 TEXT NOT NULL,
  address_line2 TEXT,
  city TEXT NOT NULL,
  state_province TEXT NOT NULL,
  postal_code TEXT NOT NULL,
  country TEXT DEFAULT 'US',
  county TEXT,
  latitude DOUBLE PRECISION,
  longitude DOUBLE PRECISION,
  geo_point GEOGRAPHY(POINT, 4326),
  timezone TEXT,
  university_name TEXT,
  university_id TEXT,
  distance_to_campus_miles NUMERIC,
  campus_direction TEXT,
  property_phone TEXT,
  property_email TEXT,
  property_url TEXT,
  leasing_url TEXT,
  opening_date DATE,
  construction_start_date DATE,
  construction_completion_date DATE,
  year_built INTEGER,
  year_last_renovated INTEGER,
  total_buildings INTEGER DEFAULT 1,
  total_residential_floors INTEGER,
  lowest_residential_floor INTEGER DEFAULT 1,
  highest_residential_floor INTEGER,
  skip_13th_floor BOOLEAN DEFAULT false,
  total_units INTEGER,
  total_beds INTEGER,
  total_parking_spots INTEGER,
  parking_type TEXT[],
  total_elevators INTEGER,
  has_fitness_center BOOLEAN DEFAULT false,
  has_pool BOOLEAN DEFAULT false,
  has_study_rooms BOOLEAN DEFAULT false,
  has_clubhouse BOOLEAN DEFAULT false,
  has_rooftop BOOLEAN DEFAULT false,
  has_pet_friendly BOOLEAN DEFAULT false,
  amenities_list TEXT[],
  hero_image_url TEXT,
  images JSONB DEFAULT '[]',
  -- Inline stakeholder text fields for quick display (denormalized from stakeholder_registry)
  owner_name TEXT,
  developer_name TEXT,
  brand_name TEXT,
  architect_name TEXT,
  designer_name TEXT,
  gc_name TEXT,
  property_manager_name TEXT,
  source TEXT NOT NULL DEFAULT 'manual',
  source_detail TEXT,
  data_quality_score INTEGER DEFAULT 0,
  last_enrichment_at TIMESTAMPTZ,
  enrichment_sources JSONB DEFAULT '[]',
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  created_by TEXT,
  updated_by TEXT,
  notes TEXT
);

CREATE INDEX idx_property_geo ON property_registry USING GIST (geo_point);
CREATE INDEX idx_property_name ON property_registry USING GIN (to_tsvector('english', property_name));
CREATE INDEX idx_property_name_trgm ON property_registry USING GIN (property_name extensions.gin_trgm_ops);
CREATE INDEX idx_property_type ON property_registry(property_type);
CREATE INDEX idx_property_status ON property_registry(property_status);
CREATE INDEX idx_property_city_state ON property_registry(city, state_province);

CREATE TRIGGER trg_property_registry_updated_at
  BEFORE UPDATE ON property_registry
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_property_geo_point
  BEFORE INSERT OR UPDATE OF latitude, longitude ON property_registry
  FOR EACH ROW EXECUTE FUNCTION compute_geo_point();

-- ============================================================
-- 7. PROPERTY BUILDINGS
-- ============================================================
CREATE TABLE property_buildings (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  property_id UUID NOT NULL REFERENCES property_registry(id) ON DELETE CASCADE,
  building_name TEXT,
  building_number INTEGER DEFAULT 1,
  total_floors INTEGER,
  lowest_residential_floor INTEGER DEFAULT 1,
  highest_residential_floor INTEGER,
  skip_13th_floor BOOLEAN DEFAULT false,
  elevator_count INTEGER,
  year_built INTEGER,
  construction_type TEXT,
  notes TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_building_property ON property_buildings(property_id);

CREATE TRIGGER trg_property_buildings_updated_at
  BEFORE UPDATE ON property_buildings
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================
-- 8. PROPERTY FLOORS
-- ============================================================
CREATE TABLE property_floors (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  building_id UUID NOT NULL REFERENCES property_buildings(id) ON DELETE CASCADE,
  floor_number INTEGER NOT NULL,
  floor_label TEXT,
  floor_type TEXT DEFAULT 'residential' CHECK (floor_type IN (
    'residential', 'commercial', 'amenity', 'parking',
    'mechanical', 'lobby', 'mixed'
  )),
  has_study_space BOOLEAN DEFAULT false,
  has_fitness_studio BOOLEAN DEFAULT false,
  has_gym BOOLEAN DEFAULT false,
  has_laundry BOOLEAN DEFAULT false,
  floor_amenities TEXT[],
  total_units_on_floor INTEGER,
  total_sqft NUMERIC,
  notes TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_floor_building ON property_floors(building_id);

CREATE TRIGGER trg_property_floors_updated_at
  BEFORE UPDATE ON property_floors
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================
-- 9. PROPERTY UNIT TYPES
-- ============================================================
CREATE TABLE property_unit_types (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  property_id UUID NOT NULL REFERENCES property_registry(id) ON DELETE CASCADE,
  unit_type_name TEXT NOT NULL,
  unit_type_code TEXT,
  layout_type TEXT,
  total_sqft NUMERIC,
  standard_bedrooms INTEGER DEFAULT 0,
  divided_bedrooms INTEGER DEFAULT 0,
  total_bedrooms_effective INTEGER,
  bathrooms INTEGER DEFAULT 0,
  half_baths INTEGER DEFAULT 0,
  kitchen_size TEXT CHECK (kitchen_size IN ('S', 'M', 'L', 'none', 'kitchenette')),
  unit_count INTEGER DEFAULT 0,
  bed_count_per_unit INTEGER DEFAULT 1,
  total_beds_this_type INTEGER,
  floors_present TEXT[],
  is_furnished BOOLEAN DEFAULT true,
  floorplan_url TEXT,
  unit_images JSONB DEFAULT '[]',
  notes TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_unit_type_property ON property_unit_types(property_id);

CREATE TRIGGER trg_property_unit_types_updated_at
  BEFORE UPDATE ON property_unit_types
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================
-- 10. PROPERTY STAKEHOLDERS (junction: property ↔ stakeholder_registry)
-- ============================================================
CREATE TABLE property_stakeholders (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  property_id UUID NOT NULL REFERENCES property_registry(id) ON DELETE CASCADE,
  stakeholder_id UUID REFERENCES stakeholder_registry(id) ON DELETE SET NULL,
  -- Denormalized text fields for quick display / before stakeholder_registry link exists
  stakeholder_name TEXT NOT NULL,
  company_name TEXT,
  role TEXT NOT NULL CHECK (role IN (
    'owner', 'developer', 'brand', 'architect', 'designer', 'gc',
    'property_manager', 'asset_manager', 'investor', 'lender',
    'ff_e_specifier', 'interior_designer', 'purchasing_agent', 'other'
  )),
  is_primary BOOLEAN DEFAULT false,
  relationship_start DATE,
  relationship_end DATE,
  notes TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_stakeholder_property ON property_stakeholders(property_id);
CREATE INDEX idx_stakeholder_role ON property_stakeholders(role);
CREATE INDEX idx_stakeholder_link ON property_stakeholders(stakeholder_id)
  WHERE stakeholder_id IS NOT NULL;

CREATE TRIGGER trg_property_stakeholders_updated_at
  BEFORE UPDATE ON property_stakeholders
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================
-- 11. PROPERTY ACTIVITY LOG
-- ============================================================
CREATE TABLE property_activity_log (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  property_id UUID NOT NULL REFERENCES property_registry(id) ON DELETE CASCADE,
  activity_type TEXT NOT NULL,
  activity_date TIMESTAMPTZ DEFAULT NOW(),
  title TEXT NOT NULL,
  description TEXT,
  metadata JSONB DEFAULT '{}',
  related_entity_type TEXT,
  related_entity_id TEXT,
  created_by TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_activity_property ON property_activity_log(property_id);
CREATE INDEX idx_activity_date ON property_activity_log(activity_date DESC);

-- ============================================================
-- 12. PROPERTY DOCUMENTS
-- ============================================================
CREATE TABLE property_documents (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  property_id UUID NOT NULL REFERENCES property_registry(id) ON DELETE CASCADE,
  document_type TEXT NOT NULL,
  document_name TEXT NOT NULL,
  file_url TEXT NOT NULL,
  file_size_bytes BIGINT,
  mime_type TEXT,
  parsed_data JSONB,
  uploaded_by TEXT,
  uploaded_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_document_property ON property_documents(property_id);

-- ============================================================
-- 13. RLS POLICIES (permissive for service role, authenticated users)
-- ============================================================
ALTER TABLE stakeholder_registry ENABLE ROW LEVEL SECURITY;
ALTER TABLE contact_registry ENABLE ROW LEVEL SECURITY;
ALTER TABLE contact_stakeholder_associations ENABLE ROW LEVEL SECURITY;
ALTER TABLE property_registry ENABLE ROW LEVEL SECURITY;
ALTER TABLE property_buildings ENABLE ROW LEVEL SECURITY;
ALTER TABLE property_floors ENABLE ROW LEVEL SECURITY;
ALTER TABLE property_unit_types ENABLE ROW LEVEL SECURITY;
ALTER TABLE property_stakeholders ENABLE ROW LEVEL SECURITY;
ALTER TABLE property_activity_log ENABLE ROW LEVEL SECURITY;
ALTER TABLE property_documents ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Allow all access to stakeholder_registry" ON stakeholder_registry FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all access to contact_registry" ON contact_registry FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all access to contact_stakeholder_associations" ON contact_stakeholder_associations FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all access to property_registry" ON property_registry FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all access to property_buildings" ON property_buildings FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all access to property_floors" ON property_floors FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all access to property_unit_types" ON property_unit_types FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all access to property_stakeholders" ON property_stakeholders FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all access to property_activity_log" ON property_activity_log FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all access to property_documents" ON property_documents FOR ALL USING (true) WITH CHECK (true);

-- ============================================================
-- 14. Table comments
-- ============================================================
COMMENT ON TABLE stakeholder_registry IS 'Canonical registry of companies (developers, PE firms, architects, GCs, designers, etc.) across the TLC ecosystem.';
COMMENT ON TABLE contact_registry IS 'Canonical registry of individuals. Contacts are linked to stakeholder companies via contact_stakeholder_associations with date ranges.';
COMMENT ON TABLE contact_stakeholder_associations IS 'Junction: person ↔ company with start/end dates. People move between companies; the company is the contractual party.';
COMMENT ON TABLE property_registry IS 'Master property record — the center of gravity for the TLC iQ platform.';
COMMENT ON TABLE property_stakeholders IS 'Junction: property ↔ stakeholder company with role. Links to stakeholder_registry when available.';
`;

async function main() {
  const dryRun = process.argv.includes("--dry-run");

  if (dryRun) {
    console.log("DRY RUN — SQL to be executed:\n");
    console.log(MIGRATION_SQL);
    return;
  }

  // Dynamic import of pg
  let pg;
  try {
    pg = await import("pg");
  } catch {
    console.error("pg package not found. Installing...");
    const { execSync } = await import("child_process");
    execSync("npm install pg", { stdio: "inherit" });
    pg = await import("pg");
  }

  const client = new pg.default.Client({ connectionString: CONNECTION_STRING });

  try {
    console.log("Connecting to Registry-iQ...");
    await client.connect();
    console.log("Connected. Applying migration...\n");

    await client.query(MIGRATION_SQL);

    console.log("Migration applied successfully!\n");

    // Verify tables
    const { rows } = await client.query(`
      SELECT table_name FROM information_schema.tables 
      WHERE table_schema = 'public' 
      AND table_type = 'BASE TABLE'
      ORDER BY table_name
    `);
    console.log("Tables created:");
    rows.forEach((r) => console.log(`  - ${r.table_name}`));
  } catch (err) {
    console.error("Migration failed:", err.message);
    if (err.position) {
      const pos = parseInt(err.position);
      const context = MIGRATION_SQL.substring(Math.max(0, pos - 100), pos + 100);
      console.error(`\nNear position ${pos}:\n...${context}...`);
    }
    process.exit(1);
  } finally {
    await client.end();
  }
}

main();
