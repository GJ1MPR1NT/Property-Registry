#!/usr/bin/env node
/**
 * Match unlinked project_registry rows to property_registry using Sage ship-to
 * addresses from DALE-Demand sage_orders.
 *
 * Matching doctrine (lib/sage-shipto-match.mjs):
 *   - Ship-to address is always a first-class key (project→Sage join + Sage→property).
 *   - Rows are enriched with computed property_key when the DB column is null.
 *   - Dedupe prefers ship_to + property_key richness over raw snapshot_date alone.
 *
 * Usage:
 *   node scripts/sync-sage-shipto-project-property.mjs --dry-run
 *   node scripts/sync-sage-shipto-project-property.mjs --apply
 *   node scripts/sync-sage-shipto-project-property.mjs --apply --promote --min-confidence=95
 *   node scripts/sync-sage-shipto-project-property.mjs --apply --resolve-web --resolve-web-limit=40
 */

import { createClient } from '@supabase/supabase-js';
import { config } from 'dotenv';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';
import {
  buildAddressKey,
  buildPropertyIndexes,
  buildSageIndexes,
  enrichSageOrder,
  findSageOrdersForProject,
  formatShipToAddress,
  isWarehouseShipTo,
  matchPropertyForSage,
  sageOrderRank,
  shipToAddressKey,
} from './lib/sage-shipto-match.mjs';
import { enrichPropertiesForMatching } from './lib/site-address-resolve.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
for (const envFile of ['.env.local', '.env']) {
  config({ path: resolve(__dirname, '..', envFile) });
  config({ path: resolve(__dirname, '../../Derived State/dale-chat', envFile) });
}

const argv = process.argv.slice(2);
const DRY = !argv.includes('--apply');
const PROMOTE = argv.includes('--promote');
const minConfidence = Number(
  argv.find((a) => a.startsWith('--min-confidence='))?.split('=')[1] ?? '95',
);
const projectFilter = argv.find((a) => a.startsWith('--project-id='))?.split('=')[1]?.trim();
const RESOLVE_WEB = argv.includes('--resolve-web');
const resolveWebLimit = Number(
  argv.find((a) => a.startsWith('--resolve-web-limit='))?.split('=')[1] ?? '50',
);

const demandUrl = process.env.DALE_DEMAND_SUPABASE_URL;
const demandKey =
  process.env.DALE_DEMAND_SUPABASE_SERVICE_ROLE_KEY || process.env.DALE_DEMAND_SUPABASE_KEY || '';
const regUrl = process.env.REGISTRY_IQ_SUPABASE_URL;
const regKey = process.env.REGISTRY_IQ_SUPABASE_SERVICE_ROLE_KEY;

const PAGE = 1000;

function heading(msg) {
  console.log(`\n=== ${msg} ===`);
}

async function upsertCandidate(registry, candidate) {
  const { data: existing, error: findErr } = await registry
    .from('project_location_candidates')
    .select('id')
    .eq('project_id', candidate.project_id)
    .eq('source_system', candidate.source_system)
    .eq('source_table', candidate.source_table)
    .eq('source_record_id', candidate.source_record_id)
    .maybeSingle();
  if (findErr) return { error: findErr };

  if (existing?.id) {
    return registry.from('project_location_candidates').update(candidate).eq('id', existing.id);
  }
  return registry.from('project_location_candidates').insert(candidate);
}

async function fetchAll(client, table, select, buildQuery) {
  const rows = [];
  let from = 0;
  for (;;) {
    let q = client.from(table).select(select).range(from, from + PAGE - 1);
    if (buildQuery) q = buildQuery(q);
    const { data, error } = await q;
    if (error) throw new Error(`${table}: ${error.message}`);
    if (!data?.length) break;
    rows.push(...data);
    if (data.length < PAGE) break;
    from += PAGE;
    if (from % PAGE === 0 && from > 0 && rows.length % 5000 === 0) {
      console.log(`  … fetched ${rows.length} ${table} rows`);
    }
  }
  return rows;
}

async function main() {
  if (!demandUrl || !demandKey) {
    console.error('Missing DALE_DEMAND_SUPABASE_URL or DALE_DEMAND_SUPABASE_KEY');
    process.exit(1);
  }
  if (!regUrl || !regKey) {
    console.error('Missing REGISTRY_IQ_SUPABASE_URL or REGISTRY_IQ_SUPABASE_SERVICE_ROLE_KEY');
    process.exit(1);
  }

  const demand = createClient(demandUrl, demandKey);
  const registry = createClient(regUrl, regKey);

  heading('Load Sage orders (ship-to populated)');
  const sageRaw = await fetchAll(
    demand,
    'sage_orders',
    'id, order_number, reference, division, ship_name, ship_to_address_line_1, ship_to_address_line_2, ship_to_city, ship_to_state, ship_to_zip, ship_to_country, property_key, snapshot_date',
    (q) => q.not('ship_to_city', 'is', null).neq('ship_to_city', ''),
  );

  const sageByOrder = new Map();
  for (const row of sageRaw) {
    const enriched = enrichSageOrder(row);
    const existing = sageByOrder.get(enriched.order_number);
    if (!existing || sageOrderRank(enriched) > sageOrderRank(existing)) {
      sageByOrder.set(enriched.order_number, enriched);
    }
  }
  const sageOrders = [...sageByOrder.values()];
  console.log(`  ${sageRaw.length} raw rows → ${sageOrders.length} deduped orders with ship_to`);

  heading('Load Registry projects + properties');
  let projectQuery = (q) => q.is('property_id', null);
  if (projectFilter) {
    const base = projectQuery;
    projectQuery = (q) => base(q).eq('project_id', projectFilter);
  }
  const projects = await fetchAll(
    registry,
    'project_registry',
    'id, project_id, order_number, project_name, brand, external_ids, property_id',
    projectQuery,
  );
  console.log(`  ${projects.length} unlinked projects`);

  const properties = await fetchAll(
    registry,
    'property_registry',
    'id, property_name, address_line1, address_line2, city, state_province, postal_code, external_ids, normalized_name',
  );
  console.log(`  ${properties.length} properties`);

  if (RESOLVE_WEB) {
    heading('Firecrawl web resolve (weak address_line1)');
    const webStats = await enrichPropertiesForMatching(registry, properties, {
      apply: !DRY,
      limit: resolveWebLimit,
      verbose: true,
    });
    console.log(
      `  Web resolve: attempted ${webStats.attempted}, filled ${webStats.resolved}, skipped ${webStats.skipped}, errors ${webStats.errors}`,
    );
  }

  const propertyById = new Map(properties.map((p) => [p.id, p]));
  for (const project of projects) {
    if (project.property_id) {
      project._property = propertyById.get(project.property_id) ?? null;
    }
  }

  const sageIndexes = buildSageIndexes(sageOrders);
  const propIndexes = buildPropertyIndexes(properties);

  heading(DRY ? 'Dry run — match plan' : 'Apply — upsert candidates');
  const stats = {
    projects_with_sage: 0,
    candidates: 0,
    matched: 0,
    ambiguous: 0,
    no_property: 0,
    promoted: 0,
    skipped_existing_link: 0,
  };
  const samples = [];

  for (const project of projects) {
    const sageMatches = findSageOrdersForProject(project, sageIndexes, sageOrders);
    if (sageMatches.length === 0) continue;
    stats.projects_with_sage++;

    let bestOrder = null;
    let bestMatch = null;
    for (const order of sageMatches) {
      const m = matchPropertyForSage(order, propIndexes, properties);
      if (!bestMatch || m.confidence > bestMatch.confidence) {
        bestMatch = m;
        bestOrder = order;
      }
    }
    if (!bestOrder) continue;

    const shipAddress = formatShipToAddress(bestOrder);
    const normalizedAddress = shipToAddressKey(bestOrder) || buildAddressKey(
      bestOrder.ship_to_address_line_1,
      bestOrder.ship_to_city,
      bestOrder.ship_to_state,
      bestOrder.ship_to_zip,
    );
    const resolutionStatus = bestMatch.property
      ? 'proposed'
      : bestMatch.ambiguous
        ? 'needs_review'
        : 'needs_external_research';

    if (bestMatch.property) stats.matched++;
    else if (bestMatch.ambiguous) stats.ambiguous++;
    else stats.no_property++;

    const candidate = {
      project_id: project.id,
      source_system: 'sage_300',
      source_table: 'sage_orders',
      source_record_id: bestOrder.id,
      source_order_number: bestOrder.order_number,
      source_project_key: project.project_id,
      source_ship_to_name: bestOrder.ship_name,
      source_ship_to_address: shipAddress || null,
      source_address_line1: bestOrder.ship_to_address_line_1,
      source_address_line2: bestOrder.ship_to_address_line_2,
      source_city: bestOrder.ship_to_city,
      source_state_province: bestOrder.ship_to_state,
      source_postal_code: bestOrder.ship_to_zip,
      source_country: bestOrder.ship_to_country || 'US',
      normalized_address: normalizedAddress,
      candidate_property_id: bestMatch.property?.id ?? null,
      match_method: bestMatch.method,
      match_confidence: bestMatch.confidence || null,
      resolution_status: resolutionStatus,
      is_primary: true,
      is_property_project: true,
      evidence: {
        sage_division: bestOrder.division,
        sage_property_key: bestOrder.property_key,
        sage_property_key_computed: Boolean(bestOrder._property_key_computed),
        sage_reference: bestOrder.reference,
        warehouse_routed: isWarehouseShipTo(bestOrder.ship_to_address_line_1),
        ambiguous_candidates: bestMatch.ambiguous ? bestMatch.count : undefined,
        project_name: project.project_name,
        matched_property_name: bestMatch.property?.property_name ?? null,
        sage_match_count: sageMatches.length,
        project_sage_link: bestOrder.order_number === project.project_id ? 'deal_key' : 'name_or_deal_ext',
      },
      last_seen_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    };

    stats.candidates++;
    if (samples.length < 5) {
      samples.push({
        project: project.project_name,
        project_id: project.project_id,
        sage_order: bestOrder.order_number,
        method: bestMatch.method,
        confidence: bestMatch.confidence,
        property: bestMatch.property?.property_name ?? null,
      });
    }

    if (DRY) continue;

    const { error: upsertErr } = await upsertCandidate(registry, candidate);
    if (upsertErr) {
      console.warn(`  candidate upsert failed for ${project.project_id}: ${upsertErr.message}`);
      continue;
    }

    if (
      PROMOTE &&
      bestMatch.property &&
      bestMatch.confidence >= minConfidence &&
      !bestMatch.ambiguous
    ) {
      const { error: linkErr } = await registry
        .from('project_registry')
        .update({ property_id: bestMatch.property.id })
        .eq('id', project.id)
        .is('property_id', null);
      if (linkErr) {
        console.warn(`  promote failed ${project.project_id}: ${linkErr.message}`);
      } else {
        stats.promoted++;
        await registry
          .from('project_location_candidates')
          .update({ resolution_status: 'accepted' })
          .eq('project_id', project.id)
          .eq('source_record_id', bestOrder.id);
      }
    }
  }

  heading('Summary');
  console.log(JSON.stringify(stats, null, 2));
  if (samples.length) {
    console.log('\nSample matches:');
    for (const s of samples) {
      console.log(`  • ${s.project_id ?? '?'} / ${s.project} → ${s.property ?? '(none)'} [${s.method} ${s.confidence}]`);
    }
  }
  if (DRY) {
    console.log('\nRe-run with --apply to write project_location_candidates.');
    if (PROMOTE) console.log('Add --promote --min-confidence=95 to auto-link high-confidence matches.');
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
