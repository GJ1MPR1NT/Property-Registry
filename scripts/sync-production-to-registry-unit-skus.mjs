#!/usr/bin/env node
/**
 * Scaffold: match TLCiQ-Production sites to Registry-iQ properties, upsert unit types + SKU lines.
 *
 * Prerequisites:
 *   - Run migration-property-unit-type-skus.sql on Registry-iQ
 *   - .env: REGISTRY_IQ_SUPABASE_URL, REGISTRY_IQ_SUPABASE_SERVICE_ROLE_KEY,
 *           PRODUCTION_SUPABASE_URL, PRODUCTION_SUPABASE_SERVICE_ROLE_KEY
 *   - Discover Production tables/views for: site address, unit type, SKU qty (adjust QUERIES below)
 *
 * Phases (implement incrementally):
 *   1. Normalize site address → fingerprint
 *   2. Rosetta / registry match → property_id (or create stub)
 *   3. Upsert property_unit_types.production_unit_type_key + layout_asset_urls
 *   4. POST .../unit-types/{utId}/skus via internal API or direct Supabase insert to property_unit_type_skus
 *
 * Usage: node scripts/sync-production-to-registry-unit-skus.mjs --dry-run
 */

import { createClient } from '@supabase/supabase-js';

const DRY = process.argv.includes('--dry-run');

const registryUrl = process.env.REGISTRY_IQ_SUPABASE_URL;
const registryKey = process.env.REGISTRY_IQ_SUPABASE_SERVICE_ROLE_KEY;
const prodUrl = process.env.PRODUCTION_SUPABASE_URL;
const prodKey = process.env.PRODUCTION_SUPABASE_SERVICE_ROLE_KEY;

if (!registryUrl || !registryKey || !prodUrl || !prodKey) {
  console.error('Missing env: REGISTRY_IQ_* and PRODUCTION_SUPABASE_* service credentials.');
  process.exit(1);
}

const registry = createClient(registryUrl, registryKey, { auth: { persistSession: false } });
const production = createClient(prodUrl, prodKey, { auth: { persistSession: false } });

async function main() {
  console.log(`TLCiQ-Production → Registry unit/SKU sync (${DRY ? 'DRY RUN' : 'LIVE'})\n`);

  // Example: list install deals (columns vary — adjust in your environment)
  const { data: deals, error: dErr } = await production.from('v_install_deals').select('*').limit(5);
  if (dErr) {
    console.warn('Production sample query:', dErr.message);
  } else {
    console.log(`Sample v_install_deals rows: ${deals?.length ?? 0} (inspect columns to map address / project keys)\n`);
  }

  const { count, error: cErr } = await registry.from('property_registry').select('*', { count: 'exact', head: true });
  if (cErr) console.warn('Registry count:', cErr.message);
  else console.log(`Registry property_registry count: ${count ?? '?'}\n`);

  console.log('Next steps:');
  console.log('  1. Map Production BOM / unit-type tables in this script.');
  console.log('  2. Add Rosetta resolution or address match to property_id.');
  console.log('  3. Upsert property_unit_type_skus rows.');
  console.log('  4. Schedule cron or n8n after initial backfill.\n');

  if (!DRY) {
    console.log('Use --dry-run until mapping is implemented.');
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
