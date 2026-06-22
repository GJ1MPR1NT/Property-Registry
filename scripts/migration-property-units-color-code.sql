-- Registry-iQ: per-unit interior finish scheme (Matrix Scheme 1 / Scheme 2)
-- Run on Registry-iQ Supabase (xhafhdaugmgdxckhdfov)

ALTER TABLE property_units
  ADD COLUMN IF NOT EXISTS color_code text
    CHECK (color_code IS NULL OR color_code IN ('scheme1', 'scheme2'));

CREATE INDEX IF NOT EXISTS idx_property_units_color_code
  ON property_units (property_id, color_code)
  WHERE color_code IS NOT NULL;

COMMENT ON COLUMN property_units.color_code IS
  'Interior finish scheme from GC Matrix: scheme1 (White Oak) or scheme2 (Weathered Gray). Drives millwork color on base SKUs.';
