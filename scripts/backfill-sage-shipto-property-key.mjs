#!/usr/bin/env node
/**
 * Backfill sage_orders.property_key from ship_to_* where missing.
 * Does not call Sage — updates DALE-Demand rows in-place using sage-shipto-match.mjs logic.
 *
 * Usage:
 *   node scripts/backfill-sage-shipto-property-key.mjs --dry-run
 *   node scripts/backfill-sage-shipto-property-key.mjs --apply
 */

import { createClient } from '@supabase/supabase-js';
import { config } from 'dotenv';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';
import { derivePropertyKeyFromShipTo } from './lib/sage-shipto-match.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
for (const envFile of ['.env.local', '.env']) {
  config({ path: resolve(__dirname, '..', envFile) });
  config({ path: resolve(__dirname, '../../Derived State/dale-chat', envFile) });
}

const APPLY = process.argv.includes('--apply');
const PAGE = 1000;

const url = process.env.DALE_DEMAND_SUPABASE_URL;
const key =
  process.env.DALE_DEMAND_SUPABASE_SERVICE_ROLE_KEY || process.env.DALE_DEMAND_SUPABASE_KEY;
if (!url || !key) {
  console.error('Missing DALE_DEMAND_SUPABASE_URL or key');
  process.exit(1);
}

const db = createClient(url, key);

async function main() {
  let scanned = 0;
  let updated = 0;
  let skipped = 0;

  for (;;) {
    const { data, error } = await db
      .from('sage_orders')
      .select(
        'id, order_number, snapshot_date, ship_name, ship_to_address_line_1, ship_to_address_line_2, ship_to_city, ship_to_state, ship_to_zip, property_key',
      )
      .not('ship_to_city', 'is', null)
      .neq('ship_to_city', '')
      .or('property_key.is.null,property_key.eq.')
      .range(0, PAGE - 1);
    if (error) throw error;
    if (!data?.length) break;

    for (const row of data) {
      scanned++;
      const pk = derivePropertyKeyFromShipTo(row);
      if (!pk) {
        skipped++;
        continue;
      }
      if (APPLY) {
        const { error: updErr } = await db
          .from('sage_orders')
          .update({ property_key: pk })
          .eq('id', row.id);
        if (updErr) {
          console.warn(`  update failed ${row.order_number}: ${updErr.message}`);
          continue;
        }
      }
      updated++;
    }

    if (data.length < PAGE) break;
  }

  console.log(JSON.stringify({ scanned, updated, skipped, mode: APPLY ? 'apply' : 'dry-run' }, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
