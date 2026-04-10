-- Registry-iQ: individual units (unit #) linked to unit type, optional building/floor
-- Run after property_unit_types, property_buildings, property_floors exist.

CREATE TABLE IF NOT EXISTS property_units (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  property_id uuid NOT NULL REFERENCES property_registry(id) ON DELETE CASCADE,
  unit_type_id uuid NOT NULL REFERENCES property_unit_types(id) ON DELETE RESTRICT,
  unit_number text NOT NULL,
  building_id uuid REFERENCES property_buildings(id) ON DELETE SET NULL,
  floor_id uuid REFERENCES property_floors(id) ON DELETE SET NULL,
  notes text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT property_units_property_unit_number UNIQUE (property_id, unit_number)
);

CREATE INDEX IF NOT EXISTS idx_property_units_property ON property_units (property_id);
CREATE INDEX IF NOT EXISTS idx_property_units_unit_type ON property_units (unit_type_id);
CREATE INDEX IF NOT EXISTS idx_property_units_floor ON property_units (floor_id);

COMMENT ON TABLE property_units IS 'Physical unit numbers; BOM/SKUs come from unit type via property_unit_type_skus.';

CREATE OR REPLACE FUNCTION property_units_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_property_units_updated ON property_units;
CREATE TRIGGER trg_property_units_updated
  BEFORE UPDATE ON property_units
  FOR EACH ROW EXECUTE FUNCTION property_units_set_updated_at();
