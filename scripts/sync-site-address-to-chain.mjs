#!/usr/bin/env node
/**
 * Push Registry-iQ property site addresses → project_registry.site_address
 * and Chain-iQ container_loads.site_address (matched by project_name).
 *
 * Source of truth: property_registry.address_line1 (+ city/state/ZIP).
 * Not Sage ship_to or NetSuite job REST — those often lack the street line.
 *
 * Usage:
 *   node scripts/sync-site-address-to-chain.mjs --dry-run
 *   node scripts/sync-site-address-to-chain.mjs --apply
 *   node scripts/sync-site-address-to-chain.mjs --apply --project "Bloomington"
 *   node scripts/sync-site-address-to-chain.mjs --apply --resolve-web-first --resolve-web-limit=30
 */

import { createClient } from '@supabase/supabase-js';
import { config } from 'dotenv';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';
import { formatSiteAddress, resolveProjectStrict } from './lib/site-address.mjs';
import { norm } from './lib/container-destination-guard.mjs';
import { enrichPropertiesForMatching } from './lib/site-address-resolve.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
for (const envFile of ['.env.local', '.env']) {
  config({ path: resolve(__dirname, '..', envFile) });
  config({ path: resolve(__dirname, '../../Derived State/dale-chat', envFile), override: true });
}

const APPLY = process.argv.includes('--apply');
const RESOLVE_WEB_FIRST = process.argv.includes('--resolve-web-first');
const resolveWebLimit = Number(
  process.argv.find((a) => a.startsWith('--resolve-web-limit='))?.split('=')[1] ?? '30',
);
const projectFilter = (() => {
  const i = process.argv.indexOf('--project');
  return i >= 0 ? process.argv[i + 1] : null;
})();

const regUrl = process.env.REGISTRY_IQ_SUPABASE_URL;
const regKey = process.env.REGISTRY_IQ_SUPABASE_SERVICE_ROLE_KEY;
const chainUrl = process.env.CHAIN_IQ_SUPABASE_URL;
const chainKey = process.env.CHAIN_IQ_SUPABASE_SERVICE_ROLE_KEY;

if (!regUrl || !regKey || !chainUrl || !chainKey) {
  console.error('Missing REGISTRY_IQ_* or CHAIN_IQ_* credentials');
  process.exit(1);
}

const reg = createClient(regUrl, regKey);
const chain = createClient(chainUrl, chainKey);

async function fetchAll(db, table, select, filterFn) {
  const rows = [];
  const page = 1000;
  for (let from = 0; ; from += page) {
    let q = db.from(table).select(select).range(from, from + page - 1);
    if (filterFn) q = filterFn(q);
    const { data, error } = await q;
    if (error) throw new Error(`${table}: ${error.message}`);
    if (!data?.length) break;
    rows.push(...data);
    if (data.length < page) break;
  }
  return rows;
}

async function loadContext() {
  const projects = await fetchAll(
    reg,
    'project_registry',
    'id, project_id, project_name, normalized_name, property_id, site_address',
  );
  const properties = await fetchAll(
    reg,
    'property_registry',
    'id, property_name, address_line1, address_line2, city, state_province, postal_code, property_url, developer_name, owner_name, enrichment_sources',
  );

  if (RESOLVE_WEB_FIRST) {
    console.log('Firecrawl web resolve before site_address denorm…');
    const webStats = await enrichPropertiesForMatching(reg, properties, {
      apply: APPLY,
      limit: resolveWebLimit,
      verbose: true,
    });
    console.log(
      `  Web resolve: attempted ${webStats.attempted}, filled ${webStats.resolved}, skipped ${webStats.skipped}`,
    );
  }

  const propById = new Map(properties.map((p) => [p.id, p]));
  const projectByName = new Map();

  for (const p of projects) {
    const prop = p.property_id ? propById.get(p.property_id) : null;
    const siteAddress = formatSiteAddress(prop);
    const entry = { ...p, siteAddress, property: prop };
    projectByName.set(norm(p.project_name), entry);
    if (p.normalized_name) projectByName.set(norm(p.normalized_name), entry);
    if (p.project_id) projectByName.set(norm(p.project_id), entry);
  }

  return { projectByName, projects, propById };
}

async function syncProjectRegistry(ctx) {
  let candidates = 0;
  let updated = 0;
  const samples = [];

  for (const p of ctx.projects) {
    const prop = p.property_id ? ctx.propById.get(p.property_id) : null;
    const siteAddress = formatSiteAddress(prop);
    if (!siteAddress) continue;
    if (p.site_address === siteAddress) continue;

    candidates++;
    if (samples.length < 5) {
      samples.push({ project_name: p.project_name, from: p.site_address || '(null)', to: siteAddress });
    }

    if (APPLY) {
      const { error } = await reg.from('project_registry').update({ site_address: siteAddress }).eq('id', p.id);
      if (error) console.warn('project_registry update failed', p.id, error.message);
      else updated++;
    }
  }

  return { candidates, updated, samples };
}

async function syncContainerLoads(ctx) {
  const containers = await fetchAll(
    chain,
    'container_loads',
    'id, container_number, project_name, site_address, vendor',
    projectFilter ? (q) => q.ilike('project_name', `%${projectFilter}%`) : undefined,
  );

  let candidates = 0;
  let updated = 0;
  let unmatched = 0;
  let noAddress = 0;
  const samples = [];

  for (const row of containers) {
    const project = resolveProjectStrict(row.project_name, ctx, row);
    if (!project) {
      unmatched++;
      continue;
    }
    const siteAddress = project.siteAddress;
    if (!siteAddress) {
      noAddress++;
      continue;
    }
    if (row.site_address === siteAddress) continue;

    candidates++;
    if (samples.length < 8) {
      samples.push({
        container: row.container_number || row.id.slice(0, 8),
        project_name: row.project_name?.slice(0, 45),
        to: siteAddress,
      });
    }

    if (APPLY) {
      const { error } = await chain.from('container_loads').update({ site_address: siteAddress }).eq('id', row.id);
      if (error) console.warn('container_loads update failed', row.id, error.message);
      else updated++;
    }
  }

  return { scanned: containers.length, candidates, updated, unmatched, noAddress, samples };
}

async function main() {
  console.log(`Sync site_address — ${APPLY ? 'APPLY' : 'DRY RUN'}`);
  if (projectFilter) console.log(`Filter: project_name ILIKE %${projectFilter}%`);

  const ctx = await loadContext();
  console.log(`Projects indexed: ${ctx.projectByName.size}`);

  const projResult = await syncProjectRegistry(ctx);
  console.log(
    `\nproject_registry: ${projResult.candidates} rows to update${APPLY ? `, ${projResult.updated} applied` : ''}`,
  );
  for (const s of projResult.samples) {
    console.log(`  ${s.project_name}: ${s.from} → ${s.to}`);
  }

  const chainResult = await syncContainerLoads(ctx);
  console.log(
    `\ncontainer_loads: scanned ${chainResult.scanned} | update candidates ${chainResult.candidates}` +
      `${APPLY ? ` | applied ${chainResult.updated}` : ''}`,
  );
  console.log(`  unmatched project: ${chainResult.unmatched} | linked but no street: ${chainResult.noAddress}`);
  for (const s of chainResult.samples) {
    console.log(`  ${s.container} | ${s.project_name} → ${s.to}`);
  }

  if (!APPLY && (projResult.candidates || chainResult.candidates)) {
    console.log('\nRe-run with --apply to write site_address.');
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
