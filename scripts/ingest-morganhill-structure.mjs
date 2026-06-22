#!/usr/bin/env node
/**
 * Apply Morgan Hill structure enrichment derived from Matrix NEW MASTER.
 * Consumes .firecrawl/mh-structure.json (produced by scripts/extract-mh-structure.py):
 *   { units: [[unit_number, area, truck, phase, color_code], ...],
 *     type_bath: { unit_type_name: bath_count, ... },
 *     inconsistent: { unit_type_name: [counts] } }
 *
 * Writes:
 *   - property_units.construction_area / truck_no / phase_no / color_code
 *   - property_unit_types.bathrooms                           (per type, matched on unit_type_name)
 *
 * Building/floor placement is handled separately by migration-reconcile-morganhill-buildings.sql.
 *
 * Usage:
 *   node scripts/ingest-morganhill-structure.mjs --dry-run
 *   node scripts/ingest-morganhill-structure.mjs --apply
 */
import { createClient } from '@supabase/supabase-js';
import { config } from 'dotenv';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';
import { readFileSync } from 'fs';

const __dirname = dirname(fileURLToPath(import.meta.url));
for (const envFile of ['.env.local', '.env']) {
  config({ path: resolve(__dirname, '..', envFile) });
  config({ path: resolve(__dirname, '../../Derived State/dale-chat', envFile) });
}

const DRY = !process.argv.includes('--apply');
const PROPERTY_ID = 'a30d446c-ee4a-4fe0-a76e-e4f9bed0e3b0';
const regUrl = process.env.REGISTRY_IQ_SUPABASE_URL;
const regKey = process.env.REGISTRY_IQ_SUPABASE_SERVICE_ROLE_KEY;

async function main() {
  if (!regUrl || !regKey) {
    console.error('Missing REGISTRY_IQ_SUPABASE_URL or REGISTRY_IQ_SUPABASE_SERVICE_ROLE_KEY');
    process.exit(1);
  }
  const data = JSON.parse(readFileSync(resolve(__dirname, '..', '.firecrawl', 'mh-structure.json'), 'utf8'));
  const reg = createClient(regUrl, regKey, { auth: { persistSession: false } });

  console.log(`Morgan Hill structure enrichment (${DRY ? 'DRY-RUN' : 'APPLY'})`);
  console.log(`  units: ${data.units.length}, unit types w/ bath: ${Object.keys(data.type_bath).length}`);
  if (data.inconsistent && Object.keys(data.inconsistent).length) {
    console.log('  GAP - inconsistent bath counts (used max):', JSON.stringify(data.inconsistent));
  }
  if (DRY) {
    console.log('  sample unit:', data.units[0]);
    console.log('  sample type_bath:', Object.entries(data.type_bath).slice(0, 5));
    console.log('Dry-run only. Re-run with --apply to write.');
    return;
  }

  // 1) per-unit sequencing
  let u_ok = 0, u_miss = 0;
  for (const row of data.units) {
    const [unit_number, area, truck, phase, color_code] = row;
    const { error, count } = await reg
      .from('property_units')
      .update(
        {
          construction_area: area ?? null,
          truck_no: truck ?? null,
          phase_no: phase ?? null,
          color_code: color_code ?? null,
        },
        { count: 'exact' },
      )
      .eq('property_id', PROPERTY_ID)
      .eq('unit_number', String(unit_number));
    if (error) { console.error('unit update failed', unit_number, error.message); }
    else if ((count ?? 0) === 0) { u_miss++; }
    else { u_ok += 1; }
  }
  console.log(`  units updated: ${u_ok}, unmatched: ${u_miss}`);

  // 2) per-type bathrooms
  let t_ok = 0, t_miss = 0;
  for (const [name, baths] of Object.entries(data.type_bath)) {
    const { error, count } = await reg
      .from('property_unit_types')
      .update({ bathrooms: baths }, { count: 'exact' })
      .eq('property_id', PROPERTY_ID)
      .eq('unit_type_name', name);
    if (error) { console.error('type update failed', name, error.message); }
    else if ((count ?? 0) === 0) { t_miss++; }
    else { t_ok += 1; }
  }
  console.log(`  unit types bathrooms set: ${t_ok}, unmatched: ${t_miss}`);
  console.log('Done.');
}

main().catch((e) => { console.error(e); process.exit(1); });
