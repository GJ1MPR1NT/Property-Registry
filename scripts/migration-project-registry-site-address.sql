-- Registry-iQ Supabase (project: xhafhdaugmgdxckhdfov) — applied 2026-05-28 via Supabase MCP.
--
-- Denormalized install site address copied from linked property_registry.

ALTER TABLE public.project_registry
  ADD COLUMN IF NOT EXISTS site_address text;

COMMENT ON COLUMN public.project_registry.site_address IS
  'Denormalized install site address from linked property_registry (address_line1 + city/state/ZIP). Populated by sync-site-address-to-chain.mjs.';
