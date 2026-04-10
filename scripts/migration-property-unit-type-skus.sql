-- Registry-iQ: unit-type SKU matrix (Production / manual sync) + layout asset refs
-- Run in Supabase SQL editor (Registry-iQ project). Uses service role in app APIs.

-- Optional: link unit types back to Production keys for sync jobs
ALTER TABLE property_unit_types
  ADD COLUMN IF NOT EXISTS layout_asset_urls jsonb DEFAULT '[]'::jsonb;

ALTER TABLE property_unit_types
  ADD COLUMN IF NOT EXISTS production_unit_type_key text;

COMMENT ON COLUMN property_unit_types.layout_asset_urls IS 'Array of strings: URLs to PDF/JPEG unit layouts (Cloudinary or signed URLs).';

COMMENT ON COLUMN property_unit_types.production_unit_type_key IS 'Stable id from TLCiQ-Production for matching during sync.';

-- SKU lines per unit type (qty typically per unit; room_label optional for room-level BOM)
CREATE TABLE IF NOT EXISTS property_unit_type_skus (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  property_id uuid NOT NULL REFERENCES property_registry(id) ON DELETE CASCADE,
  unit_type_id uuid NOT NULL REFERENCES property_unit_types(id) ON DELETE CASCADE,
  sku text NOT NULL,
  description text,
  qty_per_unit numeric NOT NULL DEFAULT 1 CHECK (qty_per_unit >= 0),
  room_label text NOT NULL DEFAULT '',
  source text NOT NULL DEFAULT 'registry',
  production_line_key text,
  replacement_year int,
  cohort_year int,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS property_unit_type_skus_unit_sku_room
  ON property_unit_type_skus (unit_type_id, sku, room_label);

CREATE INDEX IF NOT EXISTS property_unit_type_skus_property_id
  ON property_unit_type_skus (property_id);

CREATE INDEX IF NOT EXISTS property_unit_type_skus_sku
  ON property_unit_type_skus (sku);

COMMENT ON TABLE property_unit_type_skus IS 'FF&E BOM per unit type: SKU, qty per unit, optional room; synced from Production or edited in Registry.';

-- Trigger to bump updated_at
CREATE OR REPLACE FUNCTION property_unit_type_skus_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_property_unit_type_skus_updated ON property_unit_type_skus;
CREATE TRIGGER trg_property_unit_type_skus_updated
  BEFORE UPDATE ON property_unit_type_skus
  FOR EACH ROW EXECUTE FUNCTION property_unit_type_skus_set_updated_at();
