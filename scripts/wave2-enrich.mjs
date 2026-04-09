#!/usr/bin/env node
/**
 * Wave 2 Enrichment — Align Airtable + Supabase with Registry-iQ
 *
 * Phase A: Backfill pipeline_opportunities → registry (city, state, beds, type, deal#, account)
 * Phase B: Backfill install_schedules → registry (lat/lng, city, state, address)
 * Phase C: Insert unmatched Airtable records (iQ PR + Layout-iQ) with improved matching
 * Phase D: Re-enrich matched Airtable records that still have fillable fields
 * Phase E: Recalculate data_quality_score for all touched properties
 *
 * Usage:
 *   node scripts/wave2-enrich.mjs --dry-run
 *   node scripts/wave2-enrich.mjs
 *   node scripts/wave2-enrich.mjs --phase=A
 */

import { createClient } from '@supabase/supabase-js';
import { readFileSync, existsSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';
import https from 'https';

const __dirname = dirname(fileURLToPath(import.meta.url));
const envPath = resolve(__dirname, '../.env.local');
if (existsSync(envPath)) {
  for (const line of readFileSync(envPath, 'utf8').split('\n')) {
    const m = line.match(/^([^#=]+)=(.*)$/);
    if (m) process.env[m[1].trim()] = m[2].trim();
  }
}

const DRY = process.argv.includes('--dry-run');
const VERBOSE = process.argv.includes('--verbose');
const PHASE = process.argv.find(a => a.startsWith('--phase='))?.split('=')[1]?.toUpperCase() || null;

const riq = createClient(process.env.REGISTRY_IQ_SUPABASE_URL, process.env.REGISTRY_IQ_SUPABASE_SERVICE_ROLE_KEY);
const dd  = createClient(process.env.DALE_DEMAND_SUPABASE_URL, process.env.DALE_DEMAND_SUPABASE_KEY);
const AIRTABLE_PAT = process.env.AIRTABLE_PAT;

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function heading(msg) { console.log(`\n${'═'.repeat(60)}\n  ${msg}\n${'═'.repeat(60)}`); }
function log(msg) { console.log(`  ${msg}`); }

function calculateQuality(p) {
  let score = 0;
  if (p.property_name) score += 15;
  if (p.address_line1 && p.address_line1 !== 'TBD') score += 15;
  if (p.city && p.city !== 'Unknown') score += 10;
  if (p.state_province && p.state_province !== 'Unknown') score += 10;
  if (p.postal_code && p.postal_code !== '00000') score += 5;
  if (p.latitude) score += 10;
  if (p.property_type && p.property_type !== 'other') score += 10;
  if (p.total_units) score += 5;
  if (p.total_beds) score += 5;
  if (p.property_url) score += 5;
  if (p.owner_name || p.developer_name) score += 5;
  if (p.hero_image_url) score += 5;
  return Math.min(score, 100);
}

function normalizeForMatch(raw) {
  if (!raw) return '';
  return raw
    .replace(/\r?\n/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .toUpperCase()
    .replace(/^THE\s+/, '')
    .replace(/-/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

async function fetchAllPages(client, table, select, filters) {
  const all = [];
  let from = 0;
  while (true) {
    let q = client.from(table).select(select).range(from, from + 999);
    if (filters) q = filters(q);
    const { data, error } = await q;
    if (error) { console.error(`  Error fetching ${table}:`, error.message); break; }
    if (!data?.length) break;
    all.push(...data);
    if (data.length < 1000) break;
    from += 1000;
  }
  return all;
}

async function atFetch(baseId, table) {
  const all = [];
  let offset = null;
  do {
    const url = new URL(`https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}`);
    url.searchParams.set('pageSize', '100');
    if (offset) url.searchParams.set('offset', offset);
    const res = await new Promise((resolve, reject) => {
      https.get(url.toString(), { headers: { Authorization: `Bearer ${AIRTABLE_PAT}` } }, (resp) => {
        let body = '';
        resp.on('data', c => body += c);
        resp.on('end', () => { try { resolve(JSON.parse(body)); } catch(e) { reject(e); } });
      }).on('error', reject);
    });
    if (res.error) throw new Error(`Airtable ${table}: ${JSON.stringify(res.error)}`);
    all.push(...(res.records || []));
    offset = res.offset || null;
    if (offset) await sleep(220);
  } while (offset);
  return all;
}

const stats = {
  phaseA: { matched: 0, enriched: 0, fields: 0 },
  phaseB: { matched: 0, enriched: 0, fields: 0 },
  phaseC: { matched_existing: 0, inserted: 0, skipped_junk: 0 },
  phaseD: { enriched: 0, fields: 0 },
  phaseE: { rescored: 0 },
};

/* ═══════════════════════════════════════════════════
   PHASE A: Backfill from pipeline_opportunities
   ═══════════════════════════════════════════════════ */

async function phaseA() {
  heading('Phase A: Backfill from pipeline_opportunities');

  const regProps = await fetchAllPages(riq, 'property_registry',
    'id,property_name,city,state_province,address_line1,total_beds,property_type,developer_name,external_ids,data_quality_score',
    q => q.eq('source', 'pipeline_opportunities'));
  log(`Registry pipeline properties: ${regProps.length}`);

  // Build a name→registry map
  const needsCity = regProps.filter(p => !p.city || p.city === 'Unknown');
  const needsState = regProps.filter(p => !p.state_province || p.state_province === 'Unknown');
  log(`  Needs city: ${needsCity.length}, needs state: ${needsState.length}`);

  // Load ALL pipeline_opportunities in batches — we need opportunity_name matching
  // To avoid loading 105K rows, load only those with project_city or bed_count
  log('Loading pipeline_opportunities with useful data...');
  const poWithCity = await fetchAllPages(dd, 'pipeline_opportunities',
    'opportunity_name,project_city,project_state,bed_count,type_of_project,vertical,deal_number,account_name',
    q => q.not('project_city', 'is', null));
  log(`  With project_city: ${poWithCity.length}`);

  const poWithBeds = await fetchAllPages(dd, 'pipeline_opportunities',
    'opportunity_name,bed_count,type_of_project,vertical,deal_number,account_name',
    q => q.is('project_city', null).not('bed_count', 'is', null).gt('bed_count', 0));
  log(`  With beds (no city): ${poWithBeds.length}`);

  // Build pipeline lookup: normalized opportunity_name → best row (preferring city-bearing rows)
  const poIndex = new Map();
  for (const row of [...poWithCity, ...poWithBeds]) {
    const key = normalizeForMatch(row.opportunity_name);
    if (!key) continue;
    const existing = poIndex.get(key);
    if (!existing || (row.project_city && !existing.project_city)) {
      poIndex.set(key, row);
    }
  }
  log(`  Unique opportunity names indexed: ${poIndex.size}`);

  // Match registry properties back to pipeline
  let matched = 0, enriched = 0, totalFields = 0;
  const batchUpdates = [];

  for (const prop of regProps) {
    const key = normalizeForMatch(prop.property_name);
    const poRow = poIndex.get(key);
    if (!poRow) continue;
    matched++;

    const updates = {};
    let fields = 0;

    if ((!prop.city || prop.city === 'Unknown') && poRow.project_city) {
      updates.city = poRow.project_city.trim();
      fields++;
    }
    if ((!prop.state_province || prop.state_province === 'Unknown') && poRow.project_state) {
      updates.state_province = poRow.project_state.trim().toUpperCase();
      fields++;
    }
    if (!prop.total_beds && poRow.bed_count && poRow.bed_count > 0) {
      updates.total_beds = Number(poRow.bed_count);
      fields++;
    }
    if ((!prop.property_type || prop.property_type === 'other') && poRow.type_of_project) {
      const typeMap = {
        'apartment': 'multifamily',
        'condo': 'multifamily',
        'student housing': 'student_housing',
        'senior living': 'senior_living',
        'hotel': 'hospitality',
        'motel': 'hospitality',
        'resort': 'hospitality',
        'military': 'military',
      };
      const mapped = typeMap[poRow.type_of_project.toLowerCase()] || null;
      if (mapped) {
        updates.property_type = mapped;
        fields++;
      }
    }
    if (!prop.developer_name && poRow.account_name) {
      updates.developer_name = poRow.account_name.trim();
      fields++;
    }

    // Merge deal_number into external_ids
    if (poRow.deal_number) {
      const existingExt = prop.external_ids || {};
      if (!existingExt.deal_numbers) {
        const dealStr = String(poRow.deal_number).trim();
        if (dealStr && dealStr !== '0') {
          updates.external_ids = { ...existingExt, deal_numbers: [dealStr] };
          fields++;
        }
      }
    }

    if (fields > 0) {
      enriched++;
      totalFields += fields;
      const merged = { ...prop, ...updates };
      updates.data_quality_score = calculateQuality(merged);
      batchUpdates.push({ id: prop.id, updates });
      if (VERBOSE) log(`  ${prop.property_name} +${fields} fields (city=${updates.city||'-'}, state=${updates.state_province||'-'}, beds=${updates.total_beds||'-'})`);
    }
  }

  log(`Matched: ${matched}, Enrichable: ${enriched}, Fields to fill: ${totalFields}`);

  if (!DRY && batchUpdates.length > 0) {
    log('Applying updates...');
    let applied = 0;
    for (const { id, updates } of batchUpdates) {
      const { error } = await riq.from('property_registry').update(updates).eq('id', id);
      if (error) {
        if (VERBOSE) log(`  ERROR ${id}: ${error.message}`);
      } else {
        applied++;
      }
      if (applied % 200 === 0) log(`  ...${applied}/${batchUpdates.length}`);
    }
    log(`Applied ${applied} updates`);
  }

  stats.phaseA = { matched, enriched, fields: totalFields };
}

/* ═══════════════════════════════════════════════════
   PHASE B: Backfill from install_schedules
   ═══════════════════════════════════════════════════ */

async function phaseB() {
  heading('Phase B: Backfill from install_schedules');

  const regIS = await fetchAllPages(riq, 'property_registry',
    'id,property_name,city,state_province,address_line1,latitude,longitude,external_ids,data_quality_score',
    q => q.eq('source', 'install_schedules'));
  log(`Registry install_schedules properties: ${regIS.length}`);
  log(`  Missing lat/lng: ${regIS.filter(p => !p.latitude).length}`);
  log(`  Missing city: ${regIS.filter(p => !p.city || p.city === 'Unknown').length}`);

  // Load install_schedules rows with lat/lng
  const isRows = await fetchAllPages(dd, 'install_schedules',
    'property_name_address,latitude,longitude,city,state');

  // Build index: extract property name from the multi-line property_name_address field
  const isIndex = new Map();
  for (const row of isRows) {
    if (!row.property_name_address) continue;
    const lines = row.property_name_address.split(/\r?\n/).map(l => l.trim()).filter(Boolean);
    const name = lines[0]; // first line is always the name
    const address = lines[1] || null; // second line is often the street address
    const key = normalizeForMatch(name);
    if (!key) continue;

    const existing = isIndex.get(key);
    // Prefer row with lat/lng
    if (!existing || (row.latitude && !existing.latitude)) {
      isIndex.set(key, { ...row, _parsed_address: address });
    }
  }
  log(`install_schedules indexed: ${isIndex.size} unique names`);

  let matched = 0, enriched = 0, totalFields = 0;
  const batchUpdates = [];

  for (const prop of regIS) {
    const key = normalizeForMatch(prop.property_name);
    const isRow = isIndex.get(key);
    if (!isRow) continue;
    matched++;

    const updates = {};
    let fields = 0;

    if (!prop.latitude && isRow.latitude) {
      updates.latitude = isRow.latitude;
      updates.longitude = isRow.longitude;
      fields++;
    }
    if ((!prop.city || prop.city === 'Unknown') && isRow.city) {
      updates.city = isRow.city.trim();
      fields++;
    }
    if ((!prop.state_province || prop.state_province === 'Unknown') && isRow.state) {
      updates.state_province = isRow.state.trim().toUpperCase();
      fields++;
    }
    if ((!prop.address_line1 || prop.address_line1 === 'TBD') && isRow._parsed_address) {
      updates.address_line1 = isRow._parsed_address;
      fields++;
    }

    if (fields > 0) {
      enriched++;
      totalFields += fields;
      const merged = { ...prop, ...updates };
      updates.data_quality_score = calculateQuality(merged);
      batchUpdates.push({ id: prop.id, updates });
      if (VERBOSE) log(`  ${prop.property_name} +${fields} fields`);
    }
  }

  log(`Matched: ${matched}, Enrichable: ${enriched}, Fields to fill: ${totalFields}`);

  if (!DRY && batchUpdates.length > 0) {
    log('Applying updates...');
    let applied = 0;
    for (const { id, updates } of batchUpdates) {
      const { error } = await riq.from('property_registry').update(updates).eq('id', id);
      if (error) {
        if (VERBOSE) log(`  ERROR ${id}: ${error.message}`);
      } else {
        applied++;
      }
    }
    log(`Applied ${applied} updates`);
  }

  stats.phaseB = { matched, enriched, fields: totalFields };
}

/* ═══════════════════════════════════════════════════
   PHASE C: Insert unmatched Airtable records
   ═══════════════════════════════════════════════════ */

async function phaseC() {
  heading('Phase C: Insert unmatched Airtable records');

  const allReg = await fetchAllPages(riq, 'property_registry', 'id,property_name,city,state_province');
  const regIndex = new Map();
  for (const p of allReg) {
    const key = normalizeForMatch(p.property_name);
    if (!regIndex.has(key)) regIndex.set(key, []);
    regIndex.get(key).push(p);
  }
  log(`Current registry: ${allReg.length} properties, ${regIndex.size} unique names`);

  const stkAll = await fetchAllPages(riq, 'stakeholder_registry', 'id,stakeholder_name');
  const stkIndex = new Map();
  for (const s of stkAll) stkIndex.set(s.stakeholder_name.toUpperCase().trim(), s);

  const ctAll = await fetchAllPages(riq, 'contact_registry', 'id,first_name,last_name,email');
  const ctIndex = new Map();
  for (const c of ctAll) ctIndex.set(`${c.first_name} ${c.last_name}`.toUpperCase().trim(), c);

  // Improved matching: also try without the "-CityName" suffix
  function findMatchImproved(name, city, state) {
    const key = normalizeForMatch(name);
    if (regIndex.has(key)) {
      const matches = regIndex.get(key);
      if (matches.length === 1) return matches[0];
      if (state) {
        const stateMatch = matches.find(m => m.state_province?.toUpperCase() === state.toUpperCase());
        if (stateMatch) return stateMatch;
      }
      return matches[0];
    }

    // Try stripping trailing city from names like "Hub Tallahassee-Tallahassee"
    // or "The Canyon-CEDARBROOK"
    const dashIdx = name.lastIndexOf('-');
    if (dashIdx > 2) {
      const baseName = name.substring(0, dashIdx).trim();
      const baseKey = normalizeForMatch(baseName);
      if (regIndex.has(baseKey)) {
        const matches = regIndex.get(baseKey);
        if (state) {
          const stateMatch = matches.find(m => m.state_province?.toUpperCase() === state.toUpperCase());
          if (stateMatch) return stateMatch;
        }
        return matches[0];
      }
    }

    // Substring matching for longer names
    for (const [existingKey, existingProps] of regIndex) {
      if (key.length > 8 && existingKey.length > 8) {
        if (existingKey.includes(key) || key.includes(existingKey)) {
          if (state && existingProps[0].state_province?.toUpperCase() === state?.toUpperCase()) {
            return existingProps[0];
          }
          if (!state || existingProps[0].state_province === 'Unknown') {
            return existingProps[0];
          }
        }
      }
    }

    return null;
  }

  // Load Airtable sources
  const iqPR = await atFetch('appz0l9XP1SiwQQ6c', 'Properties');
  const layoutPL = await atFetch('appG8wJwYkvtj4rFN', 'Property List');
  log(`iQ PR records: ${iqPR.length}, Layout-iQ records: ${layoutPL.length}`);

  // Process iQ Property Registry
  const toInsert = [];
  let matchedExisting = 0;
  let skippedJunk = 0;

  for (const rec of iqPR) {
    const f = rec.fields;
    const name = f.Prprty_Name;
    if (!name || name.trim().length < 2) continue;
    if (/^[A-Z]{2}$/i.test(name.trim())) { skippedJunk++; continue; }
    if (name.trim().length < 3 || /^(NA|N\/A|TBD|TEST)$/i.test(name.trim())) { skippedJunk++; continue; }

    let city = null, state = null, zip = null, street = null;
    const streetRaw = f['Prperty_StreetAddress'];
    const cityStateRaw = f['Prprty_City-State'];
    const zipRaw = f['Prperty_Zip'];

    if (streetRaw) {
      street = streetRaw.split(/\r?\n/)[0]?.trim() || null;
    }
    if (cityStateRaw) {
      const cs = cityStateRaw.split(/\r?\n/)[0]?.trim();
      const m = cs?.match(/^(.+?),\s*([A-Z]{2})$/i);
      if (m) { city = m[1].trim(); state = m[2].toUpperCase(); }
      else { city = cs; }
    }
    if (zipRaw) {
      const z = zipRaw.split(/\r?\n/)[0]?.trim();
      if (/^\d{5}/.test(z)) zip = z.match(/(\d{5}(?:-\d{4})?)/)?.[1] || null;
    }

    const match = findMatchImproved(name.trim(), city, state);
    if (match) {
      matchedExisting++;
      continue; // Phase D handles enrichment of existing
    }

    // Determine property type
    let pType = null;
    const rawType = f.Prprty_Type;
    if (rawType) {
      if (/midrise|highrise|lowrise|garden/i.test(rawType)) pType = 'multifamily';
    }

    toInsert.push({
      property_name: name.trim(),
      address_line1: street || 'TBD',
      city: city || 'Unknown',
      state_province: state || 'Unknown',
      postal_code: zip || '00000',
      property_type: pType || 'other',
      property_status: 'active',
      tlc_relationship: 'customer',
      property_url: f.Prprty_Website?.split(/\r?\n/)[0]?.trim() || null,
      total_units: f['Prprty_Ttl#Units'] ? Number(f['Prprty_Ttl#Units']) : null,
      total_beds: f['Prprty_Ttl#Beds'] ? Number(f['Prprty_Ttl#Beds']) : null,
      total_buildings: f['Prprty_#Buildings'] ? Number(f['Prprty_#Buildings']) : null,
      total_parking_spots: f['Prprty_Ttl#ParkingSpaces'] ? Number(f['Prprty_Ttl#ParkingSpaces']) : null,
      year_built: f.Year_Opened ? Number(f.Year_Opened) : null,
      hero_image_url: f.Prpty_MainImage?.[0]?.url || null,
      owner_name: f.Prprty_Ownership || null,
      gc_name: f.General_Contractor || null,
      designer_name: f.Interior_Design_Firm || null,
      external_ids: f.Site_Id ? { iq_pr_site_id: f.Site_Id } : {},
      source: 'iq_property_registry',
      _contacts: [],
      _stakeholders: [],
    });

    const r = toInsert[toInsert.length - 1];
    if (f.Developer_Contact_Name) {
      r._contacts.push({ name: f.Developer_Contact_Name.trim(), email: f.Developer_Contact_Email || null, phone: f.Developer_Contact_Phone || null, role: 'developer_contact' });
    }
    if (f.Prprty_ContactName) {
      r._contacts.push({ name: f.Prprty_ContactName.trim(), email: f.Prprty_ContactEmail || null, phone: f.Prprty_ContactPhone || null, role: 'property_contact' });
    }
    if (f.Prprty_Ownership) {
      r._stakeholders.push({ stakeholder_name: f.Prprty_Ownership.trim(), stakeholder_type: 'developer', role: 'owner' });
    }
    if (f.General_Contractor) {
      r._stakeholders.push({ stakeholder_name: f.General_Contractor.trim(), stakeholder_type: 'gc', role: 'gc' });
    }
  }

  // Process Layout-iQ
  for (const rec of layoutPL) {
    const f = rec.fields;
    const name = f.Property_Name;
    if (!name || name.trim().length < 3) continue;

    const city = f.Property_City || null;
    const state = f.Property_State || null;

    const match = findMatchImproved(name.trim(), city, state);
    if (match) {
      matchedExisting++;
      continue;
    }

    toInsert.push({
      property_name: name.trim(),
      address_line1: f.Property_StreetAddress || 'TBD',
      city: city || 'Unknown',
      state_province: state || 'Unknown',
      postal_code: f['Property_Zip Code'] ? String(f['Property_Zip Code']) : '00000',
      property_type: 'other',
      property_status: 'active',
      tlc_relationship: 'customer',
      property_url: f['Property Website'] || null,
      total_units: f['Total # Units'] ? Number(f['Total # Units']) : null,
      total_beds: f['# Beds'] ? Number(f['# Beds']) : null,
      total_parking_spots: f['# Parking Spaces'] ? Number(f['# Parking Spaces']) : null,
      year_built: f['Year Opened'] ? Number(f['Year Opened']) : null,
      gc_name: f['General Contractor'] || null,
      designer_name: f['Interior Design Firm'] || null,
      developer_name: null,
      external_ids: f['Site ID'] ? { layout_iq_site_id: f['Site ID'] } : {},
      source: 'layout_iq',
      _contacts: [],
      _stakeholders: [],
    });

    const r = toInsert[toInsert.length - 1];
    if (f.Property_ContactName) {
      r._contacts.push({ name: f.Property_ContactName.trim(), email: f.Property_ContactEmail || null, phone: f.Property_ContactPhone || null, role: 'property_contact' });
    }
    if (f.CUSTOMER_Name) {
      const custName = Array.isArray(f.CUSTOMER_Name) ? f.CUSTOMER_Name[0] : f.CUSTOMER_Name;
      if (custName) {
        r._stakeholders.push({ stakeholder_name: custName.trim(), stakeholder_type: 'developer', role: 'developer' });
        r.developer_name = custName.trim();
      }
    }
    if (f['General Contractor']) {
      r._stakeholders.push({ stakeholder_name: f['General Contractor'].trim(), stakeholder_type: 'gc', role: 'gc' });
    }
  }

  // Deduplicate within the insert batch
  const seen = new Map();
  const deduped = [];
  for (const p of toInsert) {
    const key = normalizeForMatch(p.property_name) + '|' + (p.city || '').toUpperCase() + '|' + (p.state_province || '').toUpperCase();
    if (seen.has(key)) {
      const existing = seen.get(key);
      for (const f of ['address_line1','city','state_province','postal_code','property_url','total_units','total_beds','year_built','hero_image_url','owner_name','developer_name','gc_name']) {
        if ((existing[f] == null || existing[f] === 'TBD' || existing[f] === 'Unknown' || existing[f] === '00000') && p[f] != null && p[f] !== 'TBD' && p[f] !== 'Unknown' && p[f] !== '00000') {
          existing[f] = p[f];
        }
      }
      existing._contacts.push(...(p._contacts || []));
      existing._stakeholders.push(...(p._stakeholders || []));
      existing.external_ids = { ...(existing.external_ids || {}), ...(p.external_ids || {}) };
    } else {
      seen.set(key, p);
      deduped.push(p);
    }
  }

  log(`Matched to existing: ${matchedExisting}, Skipped junk: ${skippedJunk}`);
  log(`To insert: ${deduped.length} (${toInsert.length} before dedup)`);

  if (VERBOSE) {
    for (const p of deduped) {
      log(`  INSERT: ${p.property_name} | ${p.city}, ${p.state_province} | addr=${p.address_line1 !== 'TBD' ? 'Y' : 'N'}`);
    }
  }

  let insertCount = 0;
  if (!DRY && deduped.length > 0) {
    const VALID_ROLES = ['owner', 'developer', 'brand', 'architect', 'designer', 'gc',
      'property_manager', 'asset_manager', 'investor', 'lender',
      'ff_e_specifier', 'interior_designer', 'purchasing_agent', 'other'];

    for (const newProp of deduped) {
      const qs = calculateQuality(newProp);
      const { _contacts, _stakeholders, ...propData } = newProp;
      propData.data_quality_score = qs;

      const { data: inserted, error } = await riq
        .from('property_registry')
        .insert(propData)
        .select('id')
        .single();

      if (error) {
        log(`  INSERT ERROR: ${newProp.property_name} — ${error.message}`);
        continue;
      }
      insertCount++;

      // Add contacts
      for (const c of (_contacts || [])) {
        if (!c.name || c.name.trim().length < 2) continue;
        const parts = c.name.trim().split(/\s+/);
        const first = parts[0];
        const last = parts.slice(1).join(' ') || '';
        const cKey = `${first} ${last}`.toUpperCase().trim();

        if (!ctIndex.has(cKey)) {
          const { data: cIns } = await riq.from('contact_registry').insert({
            first_name: first,
            last_name: last,
            email: c.email || null,
            phone: c.phone || null,
            is_active: true,
          }).select('id').single();
          if (cIns) ctIndex.set(cKey, { id: cIns.id });
        }
      }

      // Add stakeholders and link to property
      for (const s of (_stakeholders || [])) {
        if (!s.stakeholder_name || s.stakeholder_name.trim().length < 2) continue;
        const sKey = s.stakeholder_name.toUpperCase().trim();
        let stakeholderId = stkIndex.get(sKey)?.id;

        if (!stakeholderId) {
          const { data: sIns } = await riq.from('stakeholder_registry').insert({
            stakeholder_name: s.stakeholder_name,
            stakeholder_type: s.stakeholder_type || 'other',
            is_active: true,
          }).select('id').single();
          if (sIns) {
            stakeholderId = sIns.id;
            stkIndex.set(sKey, { id: sIns.id });
          }
        }

        if (stakeholderId && inserted) {
          const role = VALID_ROLES.includes(s.role) ? s.role : 'other';
          await riq.from('property_stakeholders').insert({
            property_id: inserted.id,
            stakeholder_id: stakeholderId,
            stakeholder_name: s.stakeholder_name,
            company_name: s.stakeholder_name,
            role,
            is_primary: role === 'developer' || role === 'gc' || role === 'owner',
          });
        }
      }

      // Update local index for subsequent dedup
      const key = normalizeForMatch(newProp.property_name);
      if (!regIndex.has(key)) regIndex.set(key, []);
      regIndex.get(key).push({ ...newProp, id: inserted?.id });
    }
    log(`Inserted ${insertCount} new properties`);
  }

  stats.phaseC = { matched_existing: matchedExisting, inserted: DRY ? deduped.length : insertCount, skipped_junk: skippedJunk };
}

/* ═══════════════════════════════════════════════════
   PHASE D: Re-enrich matched Airtable → Registry
   ═══════════════════════════════════════════════════ */

async function phaseD() {
  heading('Phase D: Re-enrich matched Airtable records');

  const allReg = await fetchAllPages(riq, 'property_registry',
    'id,property_name,address_line1,city,state_province,postal_code,property_type,property_url,total_units,total_beds,total_buildings,total_parking_spots,year_built,hero_image_url,owner_name,developer_name,gc_name,designer_name,external_ids,data_quality_score,latitude');
  const regIndex = new Map();
  for (const p of allReg) {
    const key = normalizeForMatch(p.property_name);
    if (!regIndex.has(key)) regIndex.set(key, []);
    regIndex.get(key).push(p);
  }

  const iqPR = await atFetch('appz0l9XP1SiwQQ6c', 'Properties');
  const layoutPL = await atFetch('appG8wJwYkvtj4rFN', 'Property List');

  let enriched = 0, totalFields = 0;
  const batchUpdates = [];

  // Process iQ PR
  for (const rec of iqPR) {
    const f = rec.fields;
    const name = f.Prprty_Name;
    if (!name) continue;

    const key = normalizeForMatch(name);
    const matches = regIndex.get(key);
    if (!matches?.length) continue;
    const existing = matches[0];

    let city = null, state = null, zip = null, street = null;
    if (f['Prperty_StreetAddress']) street = f['Prperty_StreetAddress'].split(/\r?\n/)[0]?.trim() || null;
    if (f['Prprty_City-State']) {
      const cs = f['Prprty_City-State'].split(/\r?\n/)[0]?.trim();
      const m = cs?.match(/^(.+?),\s*([A-Z]{2})$/i);
      if (m) { city = m[1].trim(); state = m[2].toUpperCase(); }
    }
    if (f['Prperty_Zip']) {
      const z = f['Prperty_Zip'].split(/\r?\n/)[0]?.trim();
      if (/^\d{5}/.test(z)) zip = z.match(/(\d{5}(?:-\d{4})?)/)?.[1] || null;
    }

    const updates = {};
    let fields = 0;

    const fillIfEmpty = (regField, newVal) => {
      const curr = existing[regField];
      if (newVal != null && newVal !== '' && newVal !== 0) {
        if (curr == null || curr === '' || curr === 'Unknown' || curr === 'TBD' || curr === '00000' || curr === 'other') {
          updates[regField] = newVal;
          fields++;
        }
      }
    };

    fillIfEmpty('address_line1', street);
    fillIfEmpty('city', city);
    fillIfEmpty('state_province', state);
    fillIfEmpty('postal_code', zip);
    fillIfEmpty('total_units', f['Prprty_Ttl#Units'] ? Number(f['Prprty_Ttl#Units']) : null);
    fillIfEmpty('total_beds', f['Prprty_Ttl#Beds'] ? Number(f['Prprty_Ttl#Beds']) : null);
    fillIfEmpty('total_buildings', f['Prprty_#Buildings'] ? Number(f['Prprty_#Buildings']) : null);
    fillIfEmpty('total_parking_spots', f['Prprty_Ttl#ParkingSpaces'] ? Number(f['Prprty_Ttl#ParkingSpaces']) : null);
    fillIfEmpty('year_built', f.Year_Opened ? Number(f.Year_Opened) : null);
    fillIfEmpty('hero_image_url', f.Prpty_MainImage?.[0]?.url || null);
    fillIfEmpty('owner_name', f.Prprty_Ownership || null);
    fillIfEmpty('gc_name', f.General_Contractor || null);
    fillIfEmpty('designer_name', f.Interior_Design_Firm || null);
    fillIfEmpty('property_url', f.Prprty_Website?.split(/\r?\n/)[0]?.trim() || null);

    if (f.Prprty_Type && (!existing.property_type || existing.property_type === 'other')) {
      if (/midrise|highrise|lowrise|garden/i.test(f.Prprty_Type)) {
        updates.property_type = 'multifamily';
        fields++;
      }
    }

    // Merge external_ids
    if (f.Site_Id && !(existing.external_ids || {}).iq_pr_site_id) {
      updates.external_ids = { ...(existing.external_ids || {}), iq_pr_site_id: f.Site_Id };
      fields++;
    }

    if (fields > 0) {
      enriched++;
      totalFields += fields;
      const merged = { ...existing, ...updates };
      updates.data_quality_score = calculateQuality(merged);
      batchUpdates.push({ id: existing.id, updates, name: name.trim() });
      if (VERBOSE) log(`  ENRICH: ${name.trim()} +${fields}`);
    }
  }

  // Process Layout-iQ
  for (const rec of layoutPL) {
    const f = rec.fields;
    const name = f.Property_Name;
    if (!name) continue;

    const key = normalizeForMatch(name);
    const matches = regIndex.get(key);
    if (!matches?.length) continue;
    const existing = matches[0];

    const updates = {};
    let fields = 0;

    const fillIfEmpty = (regField, newVal) => {
      const curr = existing[regField];
      if (newVal != null && newVal !== '' && newVal !== 0) {
        if (curr == null || curr === '' || curr === 'Unknown' || curr === 'TBD' || curr === '00000' || curr === 'other') {
          updates[regField] = newVal;
          fields++;
        }
      }
    };

    fillIfEmpty('address_line1', f.Property_StreetAddress || null);
    fillIfEmpty('city', f.Property_City || null);
    fillIfEmpty('state_province', f.Property_State || null);
    fillIfEmpty('postal_code', f['Property_Zip Code'] ? String(f['Property_Zip Code']) : null);
    fillIfEmpty('total_units', f['Total # Units'] ? Number(f['Total # Units']) : null);
    fillIfEmpty('total_beds', f['# Beds'] ? Number(f['# Beds']) : null);
    fillIfEmpty('total_parking_spots', f['# Parking Spaces'] ? Number(f['# Parking Spaces']) : null);
    fillIfEmpty('year_built', f['Year Opened'] ? Number(f['Year Opened']) : null);
    fillIfEmpty('gc_name', f['General Contractor'] || null);
    fillIfEmpty('designer_name', f['Interior Design Firm'] || null);
    fillIfEmpty('property_url', f['Property Website'] || null);

    if (f['Site ID'] && !(existing.external_ids || {}).layout_iq_site_id) {
      updates.external_ids = { ...(existing.external_ids || {}), layout_iq_site_id: f['Site ID'] };
      fields++;
    }

    if (fields > 0) {
      enriched++;
      totalFields += fields;
      const merged = { ...existing, ...updates };
      updates.data_quality_score = calculateQuality(merged);
      batchUpdates.push({ id: existing.id, updates, name: name.trim() });
      if (VERBOSE) log(`  ENRICH: ${name.trim()} +${fields}`);
    }
  }

  log(`Enrichable: ${enriched} properties, ${totalFields} fields`);

  if (!DRY && batchUpdates.length > 0) {
    let applied = 0;
    for (const { id, updates } of batchUpdates) {
      const { error } = await riq.from('property_registry').update(updates).eq('id', id);
      if (!error) applied++;
      else if (VERBOSE) log(`  ERROR ${id}: ${error.message}`);
    }
    log(`Applied ${applied} enrichment updates`);
  }

  stats.phaseD = { enriched, fields: totalFields };
}

/* ═══════════════════════════════════════════════════
   PHASE E: Recalculate quality scores
   ═══════════════════════════════════════════════════ */

async function phaseE() {
  heading('Phase E: Recalculate quality scores for all properties');

  const allReg = await fetchAllPages(riq, 'property_registry',
    'id,property_name,address_line1,city,state_province,postal_code,property_type,property_url,total_units,total_beds,owner_name,developer_name,hero_image_url,latitude,data_quality_score');

  let rescored = 0;
  const batchUpdates = [];

  for (const p of allReg) {
    const newScore = calculateQuality(p);
    if (newScore !== p.data_quality_score) {
      batchUpdates.push({ id: p.id, score: newScore });
      rescored++;
    }
  }

  log(`${rescored} properties need score update (of ${allReg.length})`);

  if (!DRY && batchUpdates.length > 0) {
    let applied = 0;
    for (const { id, score } of batchUpdates) {
      const { error } = await riq.from('property_registry').update({ data_quality_score: score }).eq('id', id);
      if (!error) applied++;
      if (applied % 500 === 0) log(`  ...${applied}/${batchUpdates.length}`);
    }
    log(`Rescored ${applied} properties`);
  }

  stats.phaseE = { rescored };
}

/* ═══════════════════════════════════════════════════
   MAIN
   ═══════════════════════════════════════════════════ */

async function main() {
  console.log(`\n${'═'.repeat(60)}`);
  console.log(`  Wave 2 Enrichment — Airtable + Supabase Alignment`);
  console.log(`  Mode: ${DRY ? 'DRY RUN' : 'LIVE'}${PHASE ? ` | Phase: ${PHASE}` : ' | All phases'}`);
  console.log(`${'═'.repeat(60)}`);

  if (!PHASE || PHASE === 'A') await phaseA();
  if (!PHASE || PHASE === 'B') await phaseB();
  if (!PHASE || PHASE === 'C') await phaseC();
  if (!PHASE || PHASE === 'D') await phaseD();
  if (!PHASE || PHASE === 'E') await phaseE();

  heading('FINAL SUMMARY');
  console.log(JSON.stringify(stats, null, 2));
  console.log();
}

main().catch(e => {
  console.error('FATAL:', e);
  process.exit(1);
});
