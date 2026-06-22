-- Property Registry — Migration 001: Add form_factor to property_buildings
--
-- Materializes the canonical form_factor enumeration from
-- CANONICAL_Hierarchies_v1.1_2026-05-07 (Section 3 — Property Form Factor) into
-- the Registry-iQ schema.
--
-- Canonical source of truth:
--   /Cortex-iQ/canonical/CANONICAL_Hierarchies_v1_2026-05-07.md (v1.1)
--   /Cortex-iQ/canonical/CANONICAL_Hierarchies_v1_2026-05-07.xlsx (v1.1)
--
-- Allowed values (4):
--   High-Rise     — typically 15+ floors, single elevator-served core
--   Mid-Rise      — 5-14 floors, often with podium / parking levels
--   Wrap          — 3-5 floors wrapping a parking deck
--   Garden-Style  — 2-3 floors, walk-up, separate buildings around a complex
--
-- Backfill heuristic (applied where total_floors is known):
--   1-3   floors -> Garden-Style
--   4-5   floors -> Wrap
--   6-14  floors -> Mid-Rise
--   15+   floors -> High-Rise
--   0 or NULL    -> NULL (manual classification required)
--
-- The column is nullable because ~200 of 630 buildings have unknown floor counts.

BEGIN;

-- 1) Add the column with a CHECK constraint enforcing the canonical 4-value set.
ALTER TABLE public.property_buildings
    ADD COLUMN IF NOT EXISTS form_factor TEXT
        CHECK (form_factor IN ('High-Rise', 'Mid-Rise', 'Wrap', 'Garden-Style'));

COMMENT ON COLUMN public.property_buildings.form_factor IS
    'Canonical building form factor (High-Rise / Mid-Rise / Wrap / Garden-Style). '
    'Source of truth: CANONICAL_Hierarchies v1.1 §3. '
    'Backfilled from total_floors via the canonical heuristic; manual override allowed.';

-- 2) Backfill from total_floors using the canonical heuristic.
UPDATE public.property_buildings
SET form_factor = CASE
    WHEN total_floors IS NULL OR total_floors = 0 THEN NULL
    WHEN total_floors BETWEEN 1 AND 3   THEN 'Garden-Style'
    WHEN total_floors BETWEEN 4 AND 5   THEN 'Wrap'
    WHEN total_floors BETWEEN 6 AND 14  THEN 'Mid-Rise'
    WHEN total_floors >= 15             THEN 'High-Rise'
    ELSE NULL
END
WHERE form_factor IS NULL;

-- 3) Index for queries that filter by form factor (e.g. "all Mid-Rise BSI properties").
CREATE INDEX IF NOT EXISTS idx_property_buildings_form_factor
    ON public.property_buildings (form_factor)
    WHERE form_factor IS NOT NULL;

COMMIT;

-- Verification queries (run after migration):
--   SELECT form_factor, count(*)
--   FROM public.property_buildings
--   GROUP BY form_factor
--   ORDER BY count(*) DESC;
