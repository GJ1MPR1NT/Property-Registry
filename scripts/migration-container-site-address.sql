-- Chain-iQ Supabase (project: bpibnvwviqilpuuvcgdm) — applied 2026-05-28 via Supabase MCP.
-- Canonical copy also lives at: Chain-iQ/scripts/migration-container-site-address.sql
--
-- Install site street address (Registry-iQ property), distinct from logistics `destination`.

ALTER TABLE public.container_loads
  ADD COLUMN IF NOT EXISTS site_address text;

COMMENT ON COLUMN public.container_loads.site_address IS
  'Canonical install site street address from Registry-iQ property_registry (via project_registry link). Distinct from destination (logistics routing city/ZIP or staging warehouse).';

CREATE INDEX IF NOT EXISTS idx_container_loads_site_address
  ON public.container_loads (site_address)
  WHERE site_address IS NOT NULL;
