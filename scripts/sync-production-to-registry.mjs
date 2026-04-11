#!/usr/bin/env node
/**
 * Sync TLCiQ-Production deal data → Registry-iQ
 *
 * What it syncs per deal:
 *   - property_unit_types (upsert via production_unit_type_key)
 *   - property_units (upsert via property_id + unit_number)
 *   - property_unit_type_skus (upsert via unit_type_id + sku + room_label)
 *   - property_buildings / property_floors (create if missing)
 *
 * Matching: project_registry.external_ids.tlciq_deal_id → deals.id in Production
 * OR pass --deal=23-016 --property=<registry uuid> to specify directly.
 *
 * Usage:
 *   node scripts/sync-production-to-registry.mjs --dry-run
 *   node scripts/sync-production-to-registry.mjs --deal=23-016 --property=9665c5d8-...
 *   node scripts/sync-production-to-registry.mjs --all --dry-run
 *   node scripts/sync-production-to-registry.mjs --all --offset=0 --limit=50   # batch (resume)
 *   node scripts/sync-production-to-registry.mjs --all --delay-ms=100        # throttle API
 *
 * Env (from dale-chat/.env.local or export):
 *   REGISTRY_IQ_SUPABASE_URL, REGISTRY_IQ_SUPABASE_SERVICE_ROLE_KEY
 *   PRODUCTION_SUPABASE_URL, PRODUCTION_SUPABASE_SERVICE_ROLE_KEY
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

const DRY = process.argv.includes('--dry-run');
const ALL = process.argv.includes('--all');
const dealArg = process.argv.find(a => a.startsWith('--deal='))?.split('=')[1];
const propArg = process.argv.find(a => a.startsWith('--property='))?.split('=')[1];
const offsetArg = parseInt(process.argv.find(a => a.startsWith('--offset='))?.split('=')[1] ?? '0', 10);
const limitArg = process.argv.find(a => a.startsWith('--limit='))?.split('=')[1];
const limitNum = limitArg !== undefined ? parseInt(limitArg, 10) : null;
const delayMs = parseInt(process.argv.find(a => a.startsWith('--delay-ms='))?.split('=')[1] ?? '0', 10);

const registryUrl = process.env.REGISTRY_IQ_SUPABASE_URL;
const registryKey = process.env.REGISTRY_IQ_SUPABASE_SERVICE_ROLE_KEY;
const prodUrl = process.env.PRODUCTION_SUPABASE_URL;
const prodKey = process.env.PRODUCTION_SUPABASE_SERVICE_ROLE_KEY;

if (!registryUrl || !registryKey || !prodUrl || !prodKey) {
  console.error('Missing env vars. Need REGISTRY_IQ_SUPABASE_URL/KEY + PRODUCTION_SUPABASE_URL/KEY');
  process.exit(1);
}

const reg = createClient(registryUrl, registryKey, { auth: { persistSession: false } });
const prod = createClient(prodUrl, prodKey, { auth: { persistSession: false } });

async function fetchAll(client, table, filter) {
  const all = [];
  let from = 0;
  const PAGE = 1000;
  while (true) {
    let q = client.from(table).select('*').range(from, from + PAGE - 1);
    if (filter) q = filter(q);
    const { data, error } = await q;
    if (error) throw new Error(`${table}: ${error.message}`);
    if (!data?.length) break;
    all.push(...data);
    if (data.length < PAGE) break;
    from += PAGE;
  }
  return all;
}

/** Chunked .in() for PostgREST (avoids huge filter strings). */
async function fetchDealsByNumbers(dealNumbers) {
  const unique = [...new Set(dealNumbers.filter(Boolean))];
  const CHUNK = 200;
  const byId = new Map();
  for (let i = 0; i < unique.length; i += CHUNK) {
    const chunk = unique.slice(i, i + CHUNK);
    const rows = await fetchAll(prod, 'deals', q => q.in('deal_number', chunk));
    for (const d of rows) byId.set(d.deal_number, d);
  }
  return byId;
}

// ─── Resolve deal → property pairs ───────────────────────────────────
async function resolvePairs() {
  const pairs = [];

  if (dealArg && propArg) {
    const { data: deal } = await prod.from('deals').select('id, deal_number, project_name, property_id')
      .eq('deal_number', dealArg).maybeSingle();
    if (!deal) {
      console.error(`Deal ${dealArg} not found in Production`);
      process.exit(1);
    }
    pairs.push({ deal, registryPropertyId: propArg });
    return pairs;
  }

  if (dealArg) {
    const { data: deal } = await prod.from('deals').select('id, deal_number, project_name, property_id')
      .eq('deal_number', dealArg).maybeSingle();
    if (!deal) { console.error(`Deal ${dealArg} not found`); process.exit(1); }
    const { data: proj } = await reg.from('project_registry').select('property_id, external_ids')
      .eq('project_id', dealArg).maybeSingle();
    if (!proj?.property_id) {
      console.error(`No project_registry row with project_id=${dealArg} (or no property_id). Use --property=<uuid>`);
      process.exit(1);
    }
    pairs.push({ deal, registryPropertyId: proj.property_id });
    return pairs;
  }

  if (ALL) {
    const projects = await fetchAll(reg, 'project_registry', q => q.not('property_id', 'is', null));
    const dealNumbers = projects.map(p => p.project_id).filter(Boolean);
    const dealMap = await fetchDealsByNumbers(dealNumbers);
    for (const p of projects) {
      const deal = dealMap.get(p.project_id);
      if (deal && p.property_id) {
        pairs.push({ deal, registryPropertyId: p.property_id });
      }
    }
    pairs.sort((a, b) => String(a.deal.deal_number || '').localeCompare(String(b.deal.deal_number || '')));
    const total = pairs.length;
    const start = Math.max(0, offsetArg);
    const end = limitNum != null && !Number.isNaN(limitNum) ? start + limitNum : pairs.length;
    const sliced = pairs.slice(start, end);
    console.log(`Resolved ${total} deal→property pairs; processing slice [${start}, ${end}) (${sliced.length} deals)\n`);
    return sliced;
  }

  console.error('Specify --deal=XX-NNN [--property=UUID] or --all');
  process.exit(1);
}

// ─── Sync one deal → one registry property ───────────────────────────
async function syncDeal(deal, registryPropertyId) {
  const label = `${deal.deal_number} → ${registryPropertyId.slice(0, 8)}`;
  console.log(`\n${'═'.repeat(60)}`);
  console.log(`Syncing ${label} (${deal.project_name || ''})`);
  console.log('═'.repeat(60));

  // 1. Pull Production data
  const prodUnitTypes = await fetchAll(prod, 'unit_types', q => q.eq('deal_id', deal.id).order('name'));
  const prodUnits = await fetchAll(prod, 'units', q => q.eq('deal_id', deal.id));
  const prodPhases = await fetchAll(prod, 'phases', q => q.eq('deal_id', deal.id));

  const utIds = prodUnitTypes.map(ut => ut.id);
  const phaseIds = prodPhases.map(p => p.id);
  let prodReqs = [];
  if (utIds.length) {
    prodReqs = await fetchAll(prod, 'requirements', q => q.in('unit_type_id', utIds));
  }
  if (!prodReqs.length && phaseIds.length) {
    prodReqs = await fetchAll(prod, 'requirements', q => q.in('phase_id', phaseIds));
  }

  let prodItems = [];
  const itemIds = [...new Set(prodReqs.map(r => r.item_id).filter(Boolean))];
  if (itemIds.length) {
    for (let i = 0; i < itemIds.length; i += 500) {
      const chunk = itemIds.slice(i, i + 500);
      const batch = await fetchAll(prod, 'items', q => q.in('id', chunk));
      prodItems.push(...batch);
    }
  }
  const itemMap = new Map(prodItems.map(it => [it.id, it]));

  console.log(`  Production: ${prodUnitTypes.length} unit_types, ${prodUnits.length} units, ${prodPhases.length} phases`);
  console.log(`              ${prodReqs.length} requirements, ${prodItems.length} items`);

  // Count units per type for bed count
  const unitsPerType = {};
  for (const u of prodUnits) {
    unitsPerType[u.unit_type_id] = (unitsPerType[u.unit_type_id] || 0) + 1;
  }

  // Parse bed count from name like "D3  4bd" or "S1   1bd"
  function parseBeds(name) {
    const m = name?.match(/(\d+)\s*bd/i);
    return m ? parseInt(m[1], 10) : null;
  }

  // ─── 2. Ensure building exists ──────────────────────────────────────
  let buildingId = null;
  const { data: existingBuildings } = await reg.from('property_buildings').select('id, building_name')
    .eq('property_id', registryPropertyId);

  if (!existingBuildings?.length) {
    if (DRY) {
      console.log('  [DRY] Would create default building');
    } else {
      const { data: newBldg, error } = await reg.from('property_buildings').insert({
        property_id: registryPropertyId,
        building_name: 'Main Building',
        building_number: 1,
        total_floors: Math.max(...prodUnits.map(u => parseInt(u.floor) || 0), 0) || null,
      }).select().single();
      if (error) console.error('  Building insert error:', error.message);
      else { buildingId = newBldg.id; console.log('  Created building:', buildingId); }
    }
  } else {
    buildingId = existingBuildings[0].id;
  }

  // ─── 3. Ensure floors exist ─────────────────────────────────────────
  const uniqueFloors = [...new Set(prodUnits.map(u => u.floor).filter(Boolean))].sort((a, b) => {
    const na = parseInt(a, 10), nb = parseInt(b, 10);
    if (!isNaN(na) && !isNaN(nb)) return na - nb;
    return String(a).localeCompare(String(b));
  });
  const { data: existingFloors } = buildingId
    ? await reg.from('property_floors').select('id, floor_number, floor_label').eq('building_id', buildingId)
    : { data: [] };
  const existingFloorMap = new Map();
  for (const f of existingFloors || []) {
    existingFloorMap.set(String(f.floor_number), f);
    if (f.floor_label) {
      const m = f.floor_label.match(/Floor\s+(.+)$/i);
      if (m) existingFloorMap.set(m[1].trim(), f);
    }
  }
  const floorMap = new Map(); // floor string → floor uuid

  for (const f of uniqueFloors) {
    const parsed = parseInt(f, 10);
    const idx = uniqueFloors.indexOf(f);
    // Registry floor_number is NOT NULL; non-numeric codes (E, S, B, T) use synthetic 1000+idx
    const floorNumber = !isNaN(parsed) ? parsed : 1000 + idx;
    if (existingFloorMap.has(f)) {
      floorMap.set(f, existingFloorMap.get(f).id);
      continue;
    }
    if (DRY) {
      console.log(`  [DRY] Would create floor ${f}`);
      continue;
    }
    if (!buildingId) continue;
    const { data: newFloor, error } = await reg.from('property_floors').insert({
      building_id: buildingId,
      floor_number: floorNumber,
      floor_label: `Floor ${f}`,
      floor_type: 'residential',
    }).select().single();
    if (error) console.error(`  Floor ${f} insert error:`, error.message);
    else { floorMap.set(f, newFloor.id); }
  }
  console.log(`  Floors: ${floorMap.size} mapped (${uniqueFloors.length} unique in Production)`);

  // ─── 4. Upsert property_unit_types ──────────────────────────────────
  const { data: existingUt } = await reg.from('property_unit_types').select('id, production_unit_type_key, unit_type_name')
    .eq('property_id', registryPropertyId);
  const existingUtMap = new Map((existingUt || []).map(ut => [ut.production_unit_type_key, ut]));
  const existingUtByName = new Map((existingUt || []).map(ut => [ut.unit_type_name?.trim()?.toLowerCase(), ut]));

  const utIdMap = new Map(); // production ut.id → registry property_unit_types.id

  let createdUt = 0, updatedUt = 0, skippedUt = 0;
  for (const put of prodUnitTypes) {
    const key = put.id;
    const name = put.name?.trim();
    const beds = parseBeds(name);
    const unitCount = unitsPerType[put.id] || 0;

    let existing = existingUtMap.get(key) || existingUtByName.get(name?.toLowerCase());

    const row = {
      property_id: registryPropertyId,
      unit_type_name: name,
      production_unit_type_key: key,
      unit_count: unitCount,
      total_bedrooms_effective: beds,
      bed_count_per_unit: beds,
      total_beds_this_type: beds ? beds * unitCount : null,
      floorplan_url: put.floorplan_url || null,
    };

    if (existing) {
      utIdMap.set(put.id, existing.id);
      if (DRY) { skippedUt++; continue; }
      const { error } = await reg.from('property_unit_types').update(row).eq('id', existing.id);
      if (error) console.error(`  UT update error (${name}):`, error.message);
      else { updatedUt++; utIdMap.set(put.id, existing.id); }
    } else {
      if (DRY) { createdUt++; continue; }
      const { data: newUt, error } = await reg.from('property_unit_types').insert(row).select('id').single();
      if (error) console.error(`  UT insert error (${name}):`, error.message);
      else { createdUt++; utIdMap.set(put.id, newUt.id); }
    }
  }
  console.log(`  Unit types: ${createdUt} created, ${updatedUt} updated, ${skippedUt} skipped`);

  // ─── 5. Upsert property_units ───────────────────────────────────────
  const { data: existingUnits } = await reg.from('property_units').select('id, unit_number')
    .eq('property_id', registryPropertyId);
  const existingUnitMap = new Map((existingUnits || []).map(u => [u.unit_number, u]));

  let createdU = 0, updatedU = 0, skippedU = 0;
  for (const pu of prodUnits) {
    const unitNum = pu.unit_number?.trim();
    if (!unitNum) continue;
    const regUtId = utIdMap.get(pu.unit_type_id);
    if (!regUtId && !DRY) { skippedU++; continue; }

    const existing = existingUnitMap.get(unitNum);
    const floorId = floorMap.get(pu.floor) || null;

    if (existing) {
      if (DRY) { skippedU++; continue; }
      const { error } = await reg.from('property_units').update({
        unit_type_id: regUtId,
        floor_id: floorId,
        building_id: buildingId || null,
      }).eq('id', existing.id);
      if (error) console.error(`  Unit ${unitNum} update error:`, error.message);
      else updatedU++;
      continue;
    }

    if (DRY) { createdU++; continue; }

    const { error } = await reg.from('property_units').insert({
      property_id: registryPropertyId,
      unit_type_id: regUtId,
      unit_number: unitNum,
      floor_id: floorId,
      building_id: buildingId || null,
    });
    if (error && !error.message.includes('duplicate')) {
      console.error(`  Unit ${unitNum} insert error:`, error.message);
    } else {
      createdU++;
    }
  }
  console.log(`  Units: ${createdU} created, ${updatedU} updated, ${skippedU} skipped`);

  // ─── 6. Upsert property_unit_type_skus ──────────────────────────────
  let createdSku = 0, skippedSku = 0;
  if (prodReqs.length) {
    for (const req of prodReqs) {
      const regUtId = utIdMap.get(req.unit_type_id);
      if (!regUtId) { skippedSku++; continue; }
      const item = itemMap.get(req.item_id);
      if (!item?.sku) { skippedSku++; continue; }

      if (DRY) { createdSku++; continue; }

      const { error } = await reg.from('property_unit_type_skus').upsert({
        property_id: registryPropertyId,
        unit_type_id: regUtId,
        sku: item.sku,
        description: item.name || null,
        qty_per_unit: req.quantity || 1,
        room_label: '',
        source: 'tlciq_production',
        production_line_key: req.id,
      }, { onConflict: 'unit_type_id,sku,room_label' });
      if (error) console.error(`  SKU upsert error (${item.sku}):`, error.message);
      else createdSku++;
    }
    console.log(`  SKUs: ${createdSku} upserted, ${skippedSku} skipped`);
  } else {
    console.log('  SKUs: 0 requirements in Production for this deal (BOM not loaded)');
  }

  // ─── 7. Update property_registry totals ─────────────────────────────
  if (!DRY) {
    const totalUnits = prodUnits.length;
    const totalBeds = prodUnitTypes.reduce((sum, ut) => {
      const beds = parseBeds(ut.name);
      const count = unitsPerType[ut.id] || 0;
      return sum + (beds || 0) * count;
    }, 0);
    const { error } = await reg.from('property_registry').update({
      total_units: totalUnits,
      total_beds: totalBeds || null,
      total_buildings: (existingBuildings?.length || 0) + (buildingId && !existingBuildings?.length ? 1 : 0),
      total_residential_floors: uniqueFloors.length,
      highest_residential_floor: uniqueFloors.length ? Math.max(...uniqueFloors.map(Number).filter(n => !isNaN(n))) : null,
    }).eq('id', registryPropertyId);
    if (error) console.error('  Property update error:', error.message);
    else console.log(`  Property updated: ${totalUnits} units, ${totalBeds} beds, ${uniqueFloors.length} floors`);
  }

  console.log(`\n  ✓ ${label} sync complete\n`);
}

// ─── Main ────────────────────────────────────────────────────────────
async function main() {
  console.log(`Production → Registry-iQ sync (${DRY ? 'DRY RUN' : 'LIVE'})`);
  console.log(`Timestamp: ${new Date().toISOString()}\n`);

  const pairs = await resolvePairs();
  if (!pairs.length) {
    console.log('No deal→property pairs to sync.');
    return;
  }

  let ok = 0, fail = 0;
  for (let i = 0; i < pairs.length; i++) {
    const { deal, registryPropertyId } = pairs[i];
    try {
      await syncDeal(deal, registryPropertyId);
      ok++;
    } catch (e) {
      console.error(`FAILED ${deal.deal_number}:`, e.message);
      fail++;
    }
    if (delayMs > 0 && i < pairs.length - 1) {
      await new Promise(r => setTimeout(r, delayMs));
    }
  }

  console.log(`\n${'═'.repeat(60)}`);
  console.log(`Done. ${ok} succeeded, ${fail} failed.`);
  if (ALL && limitNum != null) {
    const next = offsetArg + pairs.length;
    console.log(`Next batch: --offset=${next} --limit=${limitNum}`);
  }
}

main().catch(e => { console.error(e); process.exit(1); });
