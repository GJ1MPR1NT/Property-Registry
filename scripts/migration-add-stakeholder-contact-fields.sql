-- Registry-iQ: add contact fields to property_stakeholders
-- Run on: xhafhdaugmgdxckhdfov (Registry-iQ)
-- Apply via Supabase Dashboard → SQL editor

ALTER TABLE property_stakeholders
  ADD COLUMN IF NOT EXISTS contact_name  TEXT,
  ADD COLUMN IF NOT EXISTS contact_title TEXT,
  ADD COLUMN IF NOT EXISTS contact_email TEXT,
  ADD COLUMN IF NOT EXISTS contact_phone TEXT;

COMMENT ON COLUMN property_stakeholders.contact_name  IS 'Primary contact person at this stakeholder company for this property';
COMMENT ON COLUMN property_stakeholders.contact_title IS 'Job title of the contact person';
COMMENT ON COLUMN property_stakeholders.contact_email IS 'Direct email for the contact person';
COMMENT ON COLUMN property_stakeholders.contact_phone IS 'Direct phone for the contact person';
