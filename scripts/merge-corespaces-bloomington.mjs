#!/usr/bin/env node
/**
 * CS-03: Merge legacy Bloomington Hub dupes into canonical Prismic records.
 *
 * Usage:
 *   node scripts/merge-corespaces-bloomington.mjs --dry-run
 *   node scripts/merge-corespaces-bloomington.mjs --apply
 */

import { createClient } from '@supabase/supabase-js';
import { readFileSync, existsSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(__dirname, '..');

for (const f of ['.env.local', '.env']) {
  const p = resolve(ROOT, f);
  if (!existsSync(p)) continue;
  for (const line of readFileSync(p, 'utf8').split('\n')) {
    const m = line.match(/^([^#=]+)=(.*)$/);
    if (m) process.env[m[1].trim()] = m[2].trim().replace(/^"|"$/g, '');
  }
}

const DRY = !process.argv.includes('--apply');
const REVIEWER = 'cs-03-bloomington-merge';

/** loser_id → survivor_id */
const MERGES = [
  {
    label: 'HUB Bloomington → Hub on Campus Bloomington',
    loser: 'baad0f51-59f7-4d90-872f-7e7f5a59c457',
    survivor: '845827d4-a9a4-49b3-81f2-416c1c5d666a',
  },
  {
    label: 'Hub Bloomington Lincoln (II) → Hub on Campus Bloomington Lincoln',
    loser: 'beb742ce-c051-47ad-8582-dc95c3660dfc',
    survivor: '8d192192-ea5e-48b3-bde9-79978a3104b5',
  },
];

async function main() {
  const sb = createClient(process.env.REGISTRY_IQ_SUPABASE_URL, process.env.REGISTRY_IQ_SUPABASE_SERVICE_ROLE_KEY);

  for (const m of MERGES) {
    console.log(`\n${m.label}`);
    console.log(`  loser=${m.loser}`);
    console.log(`  survivor=${m.survivor}`);

    const { data: loser } = await sb.from('property_registry').select('id, property_name, property_status').eq('id', m.loser).single();
    const { data: survivor } = await sb.from('property_registry').select('id, property_name, property_status, external_ids').eq('id', m.survivor).single();

    if (!loser) {
      console.warn('  Loser not found — skip');
      continue;
    }
    if (!survivor) {
      console.warn('  Survivor not found — skip');
      continue;
    }
    console.log(`  ${loser.property_name} (${loser.property_status}) → ${survivor.property_name} (${survivor.property_status})`);

    if (DRY) {
      const { data: dryRun, error } = await sb.rpc('iqid_dry_run_merge', {
        p_entity_type: 'property',
        p_loser_id: m.loser,
        p_survivor_id: m.survivor,
      });
      if (error) {
        console.error('  dry_run error:', error.message);
      } else {
        console.log('  dry_run:', JSON.stringify(dryRun?.summary || dryRun, null, 2)?.slice(0, 500));
      }
      continue;
    }

    const { data: result, error } = await sb.rpc('iqid_apply_merge', {
      p_entity_type: 'property',
      p_loser_id: m.loser,
      p_survivor_id: m.survivor,
      p_reviewer: REVIEWER,
    });
    if (error) {
      console.error('  apply error:', error.message);
      throw error;
    }
    console.log('  apply result:', JSON.stringify(result, null, 2)?.slice(0, 800));
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
