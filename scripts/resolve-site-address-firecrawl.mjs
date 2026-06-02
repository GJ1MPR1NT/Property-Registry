#!/usr/bin/env node
/**
 * Firecrawl web search → install site address on property_registry.
 *
 * Run before Sage ship-to matching or site_address sync when street lines are
 * missing, TBD, or LLC placeholders.
 *
 * Usage:
 *   node scripts/resolve-site-address-firecrawl.mjs --dry-run
 *   node scripts/resolve-site-address-firecrawl.mjs --apply --limit=25
 *   node scripts/resolve-site-address-firecrawl.mjs --apply --property-id=<uuid>
 *   node scripts/resolve-site-address-firecrawl.mjs --apply --city=Bloomington --limit=10
 *   node scripts/resolve-site-address-firecrawl.mjs --apply --no-llm
 */

import { createClient } from '@supabase/supabase-js';
import { config } from 'dotenv';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';
import {
  applySiteAddressResolve,
  needsWebSiteAddressResolve,
  resolveSiteAddressWeb,
} from './lib/site-address-resolve.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
for (const envFile of ['.env.local', '.env']) {
  config({ path: resolve(__dirname, '..', envFile) });
  config({ path: resolve(__dirname, '../../Derived State/dale-chat', envFile), override: true });
}

const APPLY = process.argv.includes('--apply');
const NO_LLM = process.argv.includes('--no-llm');
const VERBOSE = process.argv.includes('--verbose');
const limit = Number(
  process.argv.find((a) => a.startsWith('--limit='))?.split('=')[1] ?? '25',
);
const propertyId = process.argv.find((a) => a.startsWith('--property-id='))?.split('=')[1];
const cityFilter = process.argv.find((a) => a.startsWith('--city='))?.split('=')[1];

const regUrl = process.env.REGISTRY_IQ_SUPABASE_URL;
const regKey = process.env.REGISTRY_IQ_SUPABASE_SERVICE_ROLE_KEY;

if (!regUrl || !regKey) {
  console.error('Missing REGISTRY_IQ_SUPABASE_*');
  process.exit(1);
}
if (!process.env.FIRECRAWL_API_KEY) {
  console.error('Missing FIRECRAWL_API_KEY (dale-chat .env.local)');
  process.exit(1);
}

const reg = createClient(regUrl, regKey);

async function fetchCandidates() {
  if (propertyId) {
    const { data, error } = await reg
      .from('property_registry')
      .select(
        'id, property_name, address_line1, address_line2, city, state_province, postal_code, property_url, developer_name, owner_name, enrichment_sources',
      )
      .eq('id', propertyId)
      .single();
    if (error) throw error;
    return [data];
  }

  const rows = [];
  for (let from = 0; ; from += 1000) {
    let q = reg
      .from('property_registry')
      .select(
        'id, property_name, address_line1, address_line2, city, state_province, postal_code, property_url, developer_name, owner_name, enrichment_sources',
      )
      .range(from, from + 999);
    if (cityFilter) q = q.ilike('city', `%${cityFilter}%`);
    const { data, error } = await q;
    if (error) throw error;
    if (!data?.length) break;
    rows.push(...data.filter(needsWebSiteAddressResolve));
    if (data.length < 1000) break;
  }
  return rows.slice(0, limit);
}

async function main() {
  console.log(`Firecrawl site address resolve — ${APPLY ? 'APPLY' : 'DRY RUN'}`);
  console.log(`Limit: ${limit}${cityFilter ? ` | city≈${cityFilter}` : ''}${NO_LLM ? ' | regex only' : ''}`);

  const candidates = await fetchCandidates();
  console.log(`Candidates: ${candidates.length}`);

  let resolved = 0;
  let skipped = 0;
  let errors = 0;

  for (const prop of candidates) {
    try {
      const { data: projects } = await reg
        .from('project_registry')
        .select('id, project_name, project_id')
        .eq('property_id', prop.id)
        .limit(1);
      const project = projects?.[0] ?? null;

      const result = await resolveSiteAddressWeb(prop, {
        project,
        useLlm: !NO_LLM,
        verbose: VERBOSE,
        delayMs: 450,
      });

      if (result.skipped) {
        skipped++;
        console.log(`  — ${prop.property_name}: ${result.reason}`);
        continue;
      }

      console.log(
        `  ✓ ${prop.property_name} → ${result.updates.address_line1}, ${result.updates.city || prop.city} ${result.updates.state_province || prop.state_province} (${(result.confidence * 100).toFixed(0)}% ${result.method})`,
      );
      if (VERBOSE) console.log(`    ${result.source_url}`);

      if (APPLY) await applySiteAddressResolve(reg, prop, result);
      resolved++;
    } catch (e) {
      errors++;
      console.warn(`  ! ${prop.property_name}: ${e.message}`);
    }
  }

  console.log(`\nResolved: ${resolved} | skipped: ${skipped} | errors: ${errors}`);
  if (!APPLY && resolved) console.log('Re-run with --apply to write property_registry.');
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
