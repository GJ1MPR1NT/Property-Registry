#!/usr/bin/env node
/**
 * Seed property data from Install-iQ Glide (Airtable) into Registry-iQ.
 * Usage: node scripts/seed-from-install-iq.mjs --deal=26-005 [--dry-run] [--all]
 */
import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const envPath = resolve(__dirname, '../../Derived State/dale-chat/.env.local');
try {
  const envContent = readFileSync(envPath, 'utf8');
  for (const line of envContent.split('\n')) {
    const m = line.match(/^([A-Z_][A-Z0-9_]*)=(.*)$/);
    if (m && !process.env[m[1]]) process.env[m[1]] = m[2];
  }
} catch { /* env already set */ }

const DRY_RUN = process.argv.includes('--dry-run');
const ALL_MODE = process.argv.includes('--all');
const dealArg = process.argv.find(a => a.startsWith('--deal='));
const DEAL_NUMBER = dealArg ? dealArg.split('=')[1] : null;
if (!DEAL_NUMBER && !ALL_MODE) {
  console.error('Usage: --deal=26-005 [--dry-run]  or  --all [--dry-run]');
  process.exit(1);
}

const AIRTABLE_PAT = process.env.AIRTABLE_PAT;
const BASE_ID = 'appC8sodqNVpO0Ci0';

const registryIq = createClient(
  process.env.REGISTRY_IQ_SUPABASE_URL,
  process.env.REGISTRY_IQ_SUPABASE_SERVICE_ROLE_KEY,
);

async function atFetch(table, params = {}) {
  const url = new URL(`https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent(table)}`);
  Object.entries(params).forEach(([k, v]) => url.searchParams.set(k, v));
  const res = await fetch(url, { headers: { Authorization: `Bearer ${AIRTABLE_PAT}` } });
  if (!res.ok) throw new Error(`Airtable ${table}: ${res.status} ${await res.text()}`);
  return res.json();
}

async function fetchAllPages(table, formula) {
  const all = [];
  let offset;
  do {
    const params = { pageSize: '100' };
    if (formula) params.filterByFormula = formula;
    if (offset) params.offset = offset;
    const page = await atFetch(table, params);
    all.push(...(page.records || []));
    offset = page.offset;
  } while (offset);
  return all;
}

function titleCase(str) {
  if (!str) return '';
  return str.replace(/\b\w+/g, w =>
    w.length <= 2 ? w.toUpperCase() : w[0].toUpperCase() + w.slice(1).toLowerCase()
  );
}

function inferPropertyType(projectName) {
  const n = (projectName || '').toLowerCase();
  if (/hub|university|campus|dorm|blanding|kirwan|verve|yugo|clemson|knoxville|salt lake|raleigh|durant|cityspace|telegraph|dwight/.test(n))
    return 'student_housing';
  if (/btr|build.to.rent/.test(n)) return 'build_to_rent';
  if (/hotel|inn|resort|hilton|marriott|hyatt/.test(n)) return 'hospitality';
  return 'other';
}

function inferStatus(deal) {
  const install = (deal.install_status || '').toLowerCase();
  const erp = (deal.erp_status || '').toLowerCase();
  if (install === 'completed') return 'active';
  if (install === 'in progress' || erp === 'in progress') return 'under_construction';
  return 'pre_development';
}

async function seedDeal(dealNumber) {
  console.log(`\n${'═'.repeat(70)}`);
  console.log(`  Deal: ${dealNumber}  Mode: ${DRY_RUN ? 'DRY RUN' : 'LIVE'}`);
  console.log(`${'═'.repeat(70)}`);

  // ── 1. Fetch deal ──────────────────────────────────────────────
  const deals = await fetchAllPages('Deal', `{deal_number}='${dealNumber}'`);
  if (deals.length === 0) { console.error(`  Deal ${dealNumber} not found in Airtable. Skipping.`); return; }
  const dealRecord = deals[0];
  const deal = dealRecord.fields;
  const dealRecordId = dealRecord.id;
  const projectName = deal.project_name || deal.deal_project || dealNumber;

  console.log(`  Project: ${projectName}`);
  console.log(`  Address: ${deal.address || '(none)'}`);
  console.log(`  ERP: ${deal.erp_start_date || '?'} → ${deal.erp_end_date || '?'}`);
  console.log(`  Install: ${deal.install_status || '?'}`);
  console.log(`  Units linked: ${(deal.Unit || []).length}`);

  // ── 2. Fetch all units for this deal via deal_id ───────────────
  const dealGlideId = deal.deal_id;
  if (!dealGlideId) { console.error(`  No deal_id (Glide UUID) for ${dealNumber}. Skipping.`); return; }

  const units = await fetchAllPages('Unit', `{deal_id}='${dealGlideId}'`);
  console.log(`  Fetched ${units.length} unit records from Airtable`);

  if (units.length === 0) {
    console.log(`  No units — skipping building/unit-type creation.`);
  }

  // ── 3. Analyze buildings, floors, unit types ───────────────────
  const buildings = new Map();
  const unitTypeCounts = new Map();

  for (const u of units) {
    const f = u.fields;
    const loc = f.location || (f.unit_number ? f.unit_number[0] : '?');
    const unitNum = f.unit_number || '';
    const floorMatch = unitNum.match(/^[A-Z](\d)/);
    const floor = floorMatch ? parseInt(floorMatch[1]) : null;
    const unitType = f.unit_type || 'Unknown';

    if (!buildings.has(loc)) buildings.set(loc, { floors: new Set(), units: [] });
    const bldg = buildings.get(loc);
    if (floor != null) bldg.floors.add(floor);
    bldg.units.push({ unitNum, unitType, floor });

    if (!unitTypeCounts.has(unitType)) unitTypeCounts.set(unitType, { count: 0 });
    unitTypeCounts.get(unitType).count++;
  }

  if (buildings.size > 0) {
    console.log(`  Buildings: ${[...buildings.keys()].sort().join(', ')}`);
  }

  for (const [type, data] of [...unitTypeCounts.entries()].sort()) {
    const bedMatch = type.match(/(\d+)\s*(?:BED|bd)\b/i);
    data.beds = bedMatch ? parseInt(bedMatch[1]) : null;
  }

  // ── 4. Parse address ───────────────────────────────────────────
  const addressParts = (deal.address || '').split(',').map(s => s.trim());
  let addressLine1 = '', city = '', stateProvince = '', postalCode = '';
  if (addressParts.length >= 3) {
    addressLine1 = addressParts.slice(0, -2).join(', ');
    city = addressParts[addressParts.length - 2];
    const stateZip = addressParts[addressParts.length - 1];
    const szMatch = stateZip.match(/^([A-Za-z.]+)\s*(\d{5})?/);
    if (szMatch) {
      stateProvince = szMatch[1].replace(/\./g, '').toUpperCase();
      postalCode = szMatch[2] || '';
    }
  } else if (addressParts.length === 2) {
    addressLine1 = addressParts[0];
    const szMatch = addressParts[1].match(/^([A-Za-z.]+)\s*(\d{5})?/);
    if (szMatch) {
      stateProvince = szMatch[1].replace(/\./g, '').toUpperCase();
      postalCode = szMatch[2] || '';
    }
  } else if (deal.state) {
    stateProvince = deal.state.toUpperCase();
  }

  const totalUnits = units.length;
  const totalBeds = [...unitTypeCounts.entries()].reduce((sum, [, data]) => {
    return sum + data.count * (data.beds || 0);
  }, 0);

  // ── 5. Find matching property in Registry-iQ ──────────────────
  // Priority: deal_number match > fuzzy name match
  const { data: dealMatches } = await registryIq
    .from('property_registry')
    .select('*')
    .contains('external_ids', { deal_number: dealNumber });

  let property = dealMatches?.[0] || null;

  if (!property) {
    const nameWords = titleCase(projectName).split(/\s+/).filter(w => w.length > 2);
    if (nameWords.length > 0) {
      const searchTerm = nameWords[0];
      const { data: nameMatches } = await registryIq
        .from('property_registry')
        .select('*')
        .ilike('property_name', `%${searchTerm}%`)
        .limit(10);

      const projectLower = projectName.toLowerCase();
      property = (nameMatches || []).find(m =>
        m.property_name.toLowerCase().includes(projectLower) ||
        projectLower.includes(m.property_name.toLowerCase())
      ) || null;
    }
  }

  if (property) {
    console.log(`  Matched: [${property.id.slice(0,8)}] ${property.property_name}`);
  } else {
    console.log(`  No match — will create new property.`);
  }

  // ── 6. Build property data ─────────────────────────────────────
  const cleanName = titleCase(projectName);
  const propertyData = {
    property_name: property?.property_name || cleanName,
    property_type: property?.property_type === 'other' ? inferPropertyType(projectName) : (property?.property_type || inferPropertyType(projectName)),
    property_status: inferStatus(deal),
    address_line1: addressLine1 || property?.address_line1 || 'TBD',
    city: city || property?.city || 'Unknown',
    state_province: stateProvince || property?.state_province || 'Unknown',
    postal_code: postalCode || property?.postal_code || '',
    country: 'US',
    hero_image_url: deal.image_building || property?.hero_image_url || null,
    total_buildings: buildings.size || property?.total_buildings || null,
    total_units: totalUnits || property?.total_units || null,
    total_beds: totalBeds || property?.total_beds || null,
    total_residential_floors: buildings.size > 0
      ? Math.max(...[...buildings.values()].map(b => b.floors.size > 0 ? Math.max(...b.floors) : 0))
      : (property?.total_residential_floors || null),
    opening_date: deal.erp_end_date || property?.opening_date || null,
    external_ids: {
      ...(property?.external_ids || {}),
      deal_number: dealNumber,
      glide_deal_id: dealGlideId,
      airtable_record_id: dealRecordId,
    },
    source: property?.source || 'install_iq_deal',
    source_detail: `Install-iQ Glide deal ${dealNumber}`,
    notes: [
      property?.notes || '',
      `Seeded from Install-iQ Glide deal ${dealNumber} on ${new Date().toISOString().slice(0,10)}.`,
      `Install status: ${deal.install_status || '?'}. ERP: ${deal.erp_start_date || '?'} → ${deal.erp_end_date || '?'}.`,
    ].filter(Boolean).join('\n'),
  };

  console.log(`  Name: ${propertyData.property_name}`);
  console.log(`  Location: ${propertyData.city}, ${propertyData.state_province} ${propertyData.postal_code}`);
  console.log(`  Units: ${propertyData.total_units}, Beds: ${propertyData.total_beds}, Buildings: ${propertyData.total_buildings}`);

  if (DRY_RUN) {
    console.log(`  [DRY RUN] Would ${property ? 'UPDATE' : 'INSERT'}, ${buildings.size} buildings, ${unitTypeCounts.size} unit types.`);
    return;
  }

  // ── 7. Upsert property ─────────────────────────────────────────
  let propertyId;
  if (property) {
    const { data: updated, error } = await registryIq
      .from('property_registry')
      .update(propertyData)
      .eq('id', property.id)
      .select()
      .single();
    if (error) { console.error(`  Update failed: ${error.message}`); return; }
    propertyId = updated.id;
    console.log(`  Updated property ${propertyId}`);
  } else {
    propertyData.data_quality_score = 65;
    propertyData.tlc_relationship = 'customer';
    const { data: inserted, error } = await registryIq
      .from('property_registry')
      .insert(propertyData)
      .select()
      .single();
    if (error) { console.error(`  Insert failed: ${error.message}`); return; }
    propertyId = inserted.id;
    console.log(`  Inserted property ${propertyId}`);
  }

  // ── 8. Create buildings + floors ───────────────────────────────
  if (buildings.size > 0) {
    const { data: existingBuildings } = await registryIq
      .from('property_buildings')
      .select('id')
      .eq('property_id', propertyId);

    if (existingBuildings?.length) {
      for (const b of existingBuildings) {
        await registryIq.from('property_floors').delete().eq('building_id', b.id);
      }
      await registryIq.from('property_buildings').delete().eq('property_id', propertyId);
      console.log(`  Cleared ${existingBuildings.length} existing buildings`);
    }

    let buildingNum = 1;
    for (const [loc, data] of [...buildings.entries()].sort()) {
      const floors = [...data.floors].sort();
      const { data: bldg, error } = await registryIq
        .from('property_buildings')
        .insert({
          property_id: propertyId,
          building_name: `Building ${loc}`,
          building_number: buildingNum,
          total_floors: floors.length,
          lowest_residential_floor: floors.length > 0 ? Math.min(...floors) : null,
          highest_residential_floor: floors.length > 0 ? Math.max(...floors) : null,
        })
        .select()
        .single();
      if (error) { console.error(`  Building ${loc} failed: ${error.message}`); continue; }

      for (const floorNum of floors) {
        const floorUnits = data.units.filter(u => u.floor === floorNum);
        await registryIq.from('property_floors').insert({
          building_id: bldg.id,
          floor_number: floorNum,
          floor_label: `${floorNum}`,
          floor_type: 'residential',
          total_units_on_floor: floorUnits.length,
        });
      }
      buildingNum++;
    }
    console.log(`  Created ${buildings.size} buildings`);
  }

  // ── 9. Create unit types ───────────────────────────────────────
  if (unitTypeCounts.size > 0) {
    await registryIq.from('property_unit_types').delete().eq('property_id', propertyId);

    for (const [typeName, data] of [...unitTypeCounts.entries()].sort()) {
  const bedMatch = typeName.match(/(\d+)\s*(?:BED|bd)\b/i);
      const beds = bedMatch ? parseInt(bedMatch[1]) : 0;
      await registryIq.from('property_unit_types').insert({
        property_id: propertyId,
        unit_type_name: typeName,
        unit_count: data.count,
        bed_count_per_unit: beds,
        total_beds_this_type: data.count * beds,
        standard_bedrooms: beds,
        divided_bedrooms: 0,
        total_bedrooms_effective: beds,
        bathrooms: beds > 0 ? Math.ceil(beds / 2) : 0,
        half_baths: 0,
        is_furnished: true,
      });
    }
    console.log(`  Created ${unitTypeCounts.size} unit types`);
  }

  // ── 10. Recalculate data quality ───────────────────────────────
  const { data: final } = await registryIq
    .from('property_registry')
    .select('*')
    .eq('id', propertyId)
    .single();

  if (final) {
    let score = 10;
    if (final.city && final.city !== 'Unknown') score += 10;
    if (final.state_province && final.state_province !== 'Unknown') score += 5;
    if (final.address_line1 && final.address_line1 !== 'TBD') score += 10;
    if (final.latitude) score += 5;
    if (final.total_beds) score += 5;
    if (final.total_units) score += 5;
    if (final.total_buildings) score += 5;
    if (final.hero_image_url) score += 5;
    if (final.university_name) score += 5;
    if (final.opening_date) score += 5;
    if (final.property_type !== 'other') score += 5;
    if (final.developer_name || final.owner_name) score += 5;
    if (final.external_ids?.deal_number) score += 5;
    score = Math.min(score, 100);

    await registryIq.from('property_registry').update({ data_quality_score: score }).eq('id', propertyId);
    console.log(`  Quality score: ${score}`);
  }

  console.log(`  ✓ Done: ${propertyData.property_name} (${propertyId})`);
}

// ── Main ─────────────────────────────────────────────────────────
if (ALL_MODE) {
  console.log(`\nInstall-iQ Glide → Registry-iQ  BATCH SEED  (${DRY_RUN ? 'DRY RUN' : 'LIVE'})`);
  const allDeals = await fetchAllPages('Deal');
  const realDeals = allDeals
    .map(d => d.fields)
    .filter(d => d.deal_number && !d.deal_number.startsWith('Deal') && d.deal_number !== 'SH-Demo')
    .sort((a, b) => (a.deal_number || '').localeCompare(b.deal_number || ''));

  console.log(`Found ${realDeals.length} deals to process.\n`);
  let ok = 0, skip = 0;
  for (const d of realDeals) {
    try {
      await seedDeal(d.deal_number);
      ok++;
    } catch (e) {
      console.error(`  ERROR on ${d.deal_number}: ${e.message}`);
      skip++;
    }
  }
  console.log(`\n${'═'.repeat(70)}`);
  console.log(`  BATCH COMPLETE: ${ok} seeded, ${skip} errors`);
  console.log(`${'═'.repeat(70)}`);
} else {
  await seedDeal(DEAL_NUMBER);
}
