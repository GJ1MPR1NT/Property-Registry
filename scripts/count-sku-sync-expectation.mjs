#!/usr/bin/env node
/**
 * Compare TLCiQ-Production BOM (`requirements` + `items`) vs Registry-iQ `property_unit_type_skus`.
 *
 * Usage: node scripts/count-sku-sync-expectation.mjs
 *
 * Explains:
 *   - Production has one row per requirement line (~72k); each line maps to `items.sku`.
 *   - Registry dedupes on UNIQUE (unit_type_id, sku, room_label) per property — fewer rows.
 */

import { createClient } from '@supabase/supabase-js';
import { config } from 'dotenv';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
for (const envFile of ['.env.local', '.env']) {
  config({ path: resolve(__dirname, '..', envFile) });
  config({ path: resolve(__dirname, '../../Derived State/dale-chat', envFile) });
}

const prod = createClient(process.env.PRODUCTION_SUPABASE_URL, process.env.PRODUCTION_SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});
const reg = createClient(process.env.REGISTRY_IQ_SUPABASE_URL, process.env.REGISTRY_IQ_SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

async function main() {
  const { count: reqAll } = await prod.from('requirements').select('*', { count: 'exact', head: true });
  const { count: reqJoin } = await prod
    .from('requirements')
    .select('id, items!inner(sku)', { count: 'exact', head: true })
    .not('items.sku', 'is', null);

  const { count: regTotal } = await reg.from('property_unit_type_skus').select('*', { count: 'exact', head: true });
  const { count: regProd } = await reg
    .from('property_unit_type_skus')
    .select('*', { count: 'exact', head: true })
    .eq('source', 'tlciq_production');

  const { count: itemsAll } = await prod.from('items').select('*', { count: 'exact', head: true });

  console.log('── TLCiQ-Production ──');
  console.log('requirements rows (BOM lines):     ', reqAll);
  console.log('requirements ∩ items.sku NOT NULL:', reqJoin, '(should match total if every line has an item with sku)');
  console.log('items rows:                        ', itemsAll);

  console.log('\n── Registry-iQ ──');
  console.log('property_unit_type_skus (all):     ', regTotal);
  console.log('property_unit_type_skus (source=tlciq_production):', regProd);

  console.log('\n── Interpretation ──');
  console.log(
    'Registry row count is **lower** than Production requirement lines because the sync upserts on',
    'UNIQUE (unit_type_id, sku, room_label): duplicate BOM lines collapse to one row per unit type + SKU + room.',
  );
  console.log(
    'Not every Production deal is in project_registry; only synced deals contribute Registry rows.',
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
