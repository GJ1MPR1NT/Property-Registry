#!/usr/bin/env node
/**
 * Property Registry — Incremental Enrichment (Wave 2)
 *
 * Enriches Registry-iQ from 3 newly discovered Airtable sources:
 *   5. iQ Property Registry / Properties (appz0l9XP1SiwQQ6c) — 234 records
 *   6. iQ Property Registry / UnitTypes (appz0l9XP1SiwQQ6c) — 91 records
 *   7. Layout-iQ / Property List (appG8wJwYkvtj4rFN) — 66 records
 *   8. Layout-iQ / Customer List (appG8wJwYkvtj4rFN) — 2 records
 *
 * Strategy: match by normalized name against existing Registry-iQ rows,
 * then enrich (fill nulls, add contacts/stakeholders) or insert new.
 *
 * Usage:
 *   node scripts/enrich-property-archaeology.mjs --dry-run --verbose
 *   node scripts/enrich-property-archaeology.mjs
 *   node scripts/enrich-property-archaeology.mjs --source=iq_property_registry
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

const DRY_RUN = process.argv.includes('--dry-run');
const SOURCE_FILTER = process.argv.find(a => a.startsWith('--source='))?.split('=')[1] || null;
const VERBOSE = process.argv.includes('--verbose');

const registryIq = createClient(
  process.env.REGISTRY_IQ_SUPABASE_URL || '',
  process.env.REGISTRY_IQ_SUPABASE_SERVICE_ROLE_KEY || '',
);

const AIRTABLE_PAT = process.env.AIRTABLE_PAT || '';
const IQ_PR_BASE = 'appz0l9XP1SiwQQ6c';
const LAYOUT_IQ_BASE = 'appG8wJwYkvtj4rFN';

const stats = {
  existing_properties: 0,
  iq_pr_fetched: 0,
  layout_iq_fetched: 0,
  layout_iq_customers: 0,
  iq_pr_unit_types: 0,
  matched: 0,
  enriched: 0,
  new_inserted: 0,
  contacts_added: 0,
  stakeholders_added: 0,
  stakeholders_enriched: 0,
  fields_filled: 0,
};

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function log(msg) { console.log(`  ${msg}`); }
function heading(msg) { console.log(`\n${'═'.repeat(60)}\n  ${msg}\n${'═'.repeat(60)}`); }

function normalizeForMatch(raw) {
  if (!raw) return '';
  return raw
    .replace(/\r?\n/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .toUpperCase()
    .replace(/^THE\s+/, '')
    .replace(/\s+(APARTMENTS?|RESIDENCES?|SUITES?|HOTEL|INN|RESORT)$/i, '')
    .replace(/-[A-Z\s]+$/i, '') // Strip trailing "-CityName" suffix
    .replace(/-/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

async function airtableFetchAll(baseId, tableName) {
  const all = [];
  let offset = null;
  do {
    const url = new URL(`https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}`);
    url.searchParams.set('pageSize', '100');
    if (offset) url.searchParams.set('offset', offset);

    const res = await new Promise((resolve, reject) => {
      https.get(url.toString(), { headers: { Authorization: `Bearer ${AIRTABLE_PAT}` } }, (resp) => {
        let body = '';
        resp.on('data', c => body += c);
        resp.on('end', () => {
          try { resolve(JSON.parse(body)); }
          catch (e) { reject(e); }
        });
      }).on('error', reject);
    });

    if (res.error) throw new Error(`Airtable ${tableName}: ${JSON.stringify(res.error)}`);
    all.push(...(res.records || []));
    offset = res.offset || null;
    if (offset) await sleep(220);
  } while (offset);
  return all;
}

/* ═══════════════════════════════════════════════════════════════
   LOAD EXISTING REGISTRY-IQ DATA
   ═══════════════════════════════════════════════════════════════ */

async function loadExistingProperties() {
  heading('Loading existing Registry-iQ properties');
  const all = [];
  let from = 0;
  while (true) {
    const { data } = await registryIq
      .from('property_registry')
      .select('id,property_name,address_line1,city,state_province,postal_code,latitude,longitude,property_type,property_url,total_units,total_beds,total_buildings,total_residential_floors,total_elevators,total_parking_spots,owner_name,developer_name,gc_name,architect_name,designer_name,property_manager_name,property_phone,property_email,year_built,opening_date,external_ids,source,data_quality_score,hero_image_url')
      .range(from, from + 999);
    if (!data?.length) break;
    all.push(...data);
    if (data.length < 1000) break;
    from += 1000;
  }
  stats.existing_properties = all.length;
  log(`Loaded ${all.length} existing properties`);

  // Build lookup index: normalized name -> property
  const index = new Map();
  for (const p of all) {
    const key = normalizeForMatch(p.property_name);
    if (!index.has(key)) index.set(key, []);
    index.get(key).push(p);
  }

  return { all, index };
}

async function loadExistingStakeholders() {
  const all = [];
  let from = 0;
  while (true) {
    const { data } = await registryIq
      .from('stakeholder_registry')
      .select('id,stakeholder_name,stakeholder_type,website,phone,email,hq_address_line1,hq_city,hq_state,hq_postal_code,external_ids')
      .range(from, from + 999);
    if (!data?.length) break;
    all.push(...data);
    if (data.length < 1000) break;
    from += 1000;
  }
  const index = new Map();
  for (const s of all) {
    index.set(s.stakeholder_name.toUpperCase().trim(), s);
  }
  log(`Loaded ${all.length} existing stakeholders`);
  return { all, index };
}

async function loadExistingContacts() {
  const all = [];
  let from = 0;
  while (true) {
    const { data } = await registryIq
      .from('contact_registry')
      .select('id,first_name,last_name,email,phone,title')
      .range(from, from + 999);
    if (!data?.length) break;
    all.push(...data);
    if (data.length < 1000) break;
    from += 1000;
  }
  const index = new Map();
  for (const c of all) {
    index.set(`${c.first_name} ${c.last_name}`.toUpperCase().trim(), c);
  }
  log(`Loaded ${all.length} existing contacts`);
  return { all, index };
}

/* ═══════════════════════════════════════════════════════════════
   EXTRACT: iQ Property Registry (appz0l9XP1SiwQQ6c)
   ═══════════════════════════════════════════════════════════════ */

async function extractIqPropertyRegistry() {
  heading('Source 5: iQ Property Registry / Properties');
  const records = await airtableFetchAll(IQ_PR_BASE, 'Properties');
  log(`Fetched ${records.length} records`);
  stats.iq_pr_fetched = records.length;

  const results = [];
  for (const rec of records) {
    const f = rec.fields;
    const name = f.Prprty_Name;
    if (!name || name.trim().length < 2) continue;

    // Parse address — City-State field may contain "City, ST" or full address blob
    let city = null, state = null, zip = null, street = null;
    const streetRaw = f['Prperty_StreetAddress'];
    const cityStateRaw = f['Prprty_City-State'];
    const zipRaw = f['Prperty_Zip'];

    if (streetRaw) {
      const lines = streetRaw.split(/\r?\n/).map(l => l.trim()).filter(Boolean);
      street = lines[0] || null;
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

    // Map property type from Prprty_Type (MidRise_4-6, HighRise_7+, LowRise-Garden_1-3)
    let pType = null;
    const rawType = f.Prprty_Type;
    if (rawType) {
      if (/midrise|highrise|lowrise|garden/i.test(rawType)) pType = 'multifamily';
    }

    const heroUrl = f.Prpty_MainImage?.[0]?.url || null;

    results.push({
      airtable_id: rec.id,
      property_name: name.trim(),
      address_line1: street,
      city,
      state_province: state,
      postal_code: zip,
      property_type: pType,
      property_url: f.Prprty_Website?.split(/\r?\n/)[0]?.trim() || null,
      total_units: f['Prprty_Ttl#Units'] || null,
      total_beds: f['Prprty_Ttl#Beds'] || null,
      total_buildings: f['Prprty_#Buildings'] || null,
      total_parking_spots: f['Prprty_Ttl#ParkingSpaces'] || null,
      year_built: f.Year_Opened || null,
      hero_image_url: heroUrl,
      owner_name: f.Prprty_Ownership || null,
      gc_name: f.General_Contractor || null,
      designer_name: f.Interior_Design_Firm || null,
      site_id: f.Site_Id || null,
      external_ids: {},
      _contacts: [],
      _stakeholders: [],
      source: 'iq_property_registry',
    });

    const r = results[results.length - 1];
    if (f.Site_Id) r.external_ids.iq_pr_site_id = f.Site_Id;

    // Developer contact -> contact_registry + stakeholder association
    if (f.Developer_Contact_Name) {
      r._contacts.push({
        name: f.Developer_Contact_Name.trim(),
        email: f.Developer_Contact_Email || null,
        phone: f.Developer_Contact_Phone || null,
        role: 'developer_contact',
        title: null,
      });
    }

    // Property contact -> contact_registry
    if (f.Prprty_ContactName) {
      r._contacts.push({
        name: f.Prprty_ContactName.trim(),
        email: f.Prprty_ContactEmail || null,
        phone: f.Prprty_ContactPhone || null,
        role: 'property_contact',
        title: null,
      });
    }

    // Purchasing agent -> stakeholder
    if (f.Purchasing_Agent) {
      r._stakeholders.push({
        stakeholder_name: f.Purchasing_Agent.trim(),
        stakeholder_type: 'other',
        role: 'purchasing_agent',
      });
    }

    // GC -> stakeholder
    if (f.General_Contractor) {
      r._stakeholders.push({
        stakeholder_name: f.General_Contractor.trim(),
        stakeholder_type: 'gc',
        role: 'gc',
      });
    }

    // Owner -> stakeholder
    if (f.Prprty_Ownership) {
      r._stakeholders.push({
        stakeholder_name: f.Prprty_Ownership.trim(),
        stakeholder_type: 'developer',
        role: 'owner',
      });
    }
  }

  log(`Extracted ${results.length} properties from iQ Property Registry`);
  return results;
}

async function extractIqPropertyRegistryUnitTypes() {
  heading('Source 6: iQ Property Registry / UnitTypes');
  const records = await airtableFetchAll(IQ_PR_BASE, 'UnitTypes');
  log(`Fetched ${records.length} unit type records`);
  stats.iq_pr_unit_types = records.length;
  return records;
}

/* ═══════════════════════════════════════════════════════════════
   EXTRACT: Layout-iQ (appG8wJwYkvtj4rFN)
   ═══════════════════════════════════════════════════════════════ */

async function extractLayoutIqProperties() {
  heading('Source 7: Layout-iQ / Property List');
  const records = await airtableFetchAll(LAYOUT_IQ_BASE, 'Property List');
  log(`Fetched ${records.length} records`);
  stats.layout_iq_fetched = records.length;

  const results = [];
  for (const rec of records) {
    const f = rec.fields;
    const name = f.Property_Name;
    if (!name || name.trim().length < 2) continue;

    results.push({
      airtable_id: rec.id,
      property_name: name.trim(),
      address_line1: f.Property_StreetAddress || null,
      city: f.Property_City || null,
      state_province: f.Property_State || null,
      postal_code: f['Property_Zip Code'] ? String(f['Property_Zip Code']) : null,
      property_url: f['Property Website'] || null,
      total_units: f['Total # Units'] || null,
      total_beds: f['# Beds'] || null,
      total_parking_spots: f['# Parking Spaces'] || null,
      year_built: f['Year Opened'] || null,
      gc_name: f['General Contractor'] || null,
      designer_name: f['Interior Design Firm'] || null,
      site_id: f['Site ID'] || null,
      external_ids: {},
      _contacts: [],
      _stakeholders: [],
      source: 'layout_iq',
    });

    const r = results[results.length - 1];
    if (f['Site ID']) r.external_ids.layout_iq_site_id = f['Site ID'];

    if (f.Property_ContactName) {
      r._contacts.push({
        name: f.Property_ContactName.trim(),
        email: f.Property_ContactEmail || null,
        phone: f.Property_ContactPhone || null,
        role: 'property_contact',
      });
    }

    if (f.CUSTOMER_Name) {
      const custName = Array.isArray(f.CUSTOMER_Name) ? f.CUSTOMER_Name[0] : f.CUSTOMER_Name;
      if (custName) {
        r._stakeholders.push({
          stakeholder_name: custName.trim(),
          stakeholder_type: 'developer',
          role: 'developer',
        });
        r.developer_name = custName.trim();
      }
    }

    if (f['General Contractor']) {
      r._stakeholders.push({
        stakeholder_name: f['General Contractor'].trim(),
        stakeholder_type: 'gc',
        role: 'gc',
      });
    }
  }

  log(`Extracted ${results.length} properties from Layout-iQ`);
  return results;
}

async function extractLayoutIqCustomers() {
  heading('Source 8: Layout-iQ / Customer List');
  const records = await airtableFetchAll(LAYOUT_IQ_BASE, 'Customer List');
  log(`Fetched ${records.length} customers`);
  stats.layout_iq_customers = records.length;

  const results = [];
  for (const rec of records) {
    const f = rec.fields;
    if (!f.CUSTOMER_Name) continue;
    results.push({
      stakeholder_name: f.CUSTOMER_Name.trim(),
      stakeholder_type: 'developer',
      website: f.CUSTOMER_Website || null,
      hq_address_line1: f['CUSTOMER_Contact Street Address'] || null,
      hq_city: f.CUSTOMER_Contact_City || null,
      hq_state: f['CUSTOMER_Contact State'] || null,
      hq_postal_code: f['CUSTOMER_Contact Zip Code'] ? String(f['CUSTOMER_Contact Zip Code']) : null,
      contact_name: f.CUSTOMER_Contact_Name || null,
      contact_title: f.CUSTOMER_Contact_Title || null,
      contact_email: f.CUSTOMER_Contact_Email || null,
      contact_phone: f.CUSTOMER_Contact_Phone || null,
      tier: f.CUSTOMER_Tier || null,
    });
  }

  log(`Extracted ${results.length} customer records`);
  return results;
}

/* ═══════════════════════════════════════════════════════════════
   MATCH + ENRICH
   ═══════════════════════════════════════════════════════════════ */

function findMatch(propName, city, state, existingIndex) {
  // Exact normalized match
  const key = normalizeForMatch(propName);
  if (existingIndex.has(key)) {
    const matches = existingIndex.get(key);
    // If only one, return it; if multiple, prefer same state
    if (matches.length === 1) return matches[0];
    if (state) {
      const stateMatch = matches.find(m => m.state_province?.toUpperCase() === state.toUpperCase());
      if (stateMatch) return stateMatch;
    }
    return matches[0];
  }

  // Try partial: strip common suffixes and prefixes
  for (const [existingKey, existingProps] of existingIndex) {
    if (key.length > 5 && existingKey.length > 5) {
      // One contains the other
      if (existingKey.includes(key) || key.includes(existingKey)) {
        if (state && existingProps[0].state_province?.toUpperCase() === state.toUpperCase()) {
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

async function enrichProperty(existingProp, newData) {
  const updates = {};
  let fieldsFilled = 0;

  const fillable = [
    'address_line1', 'city', 'state_province', 'postal_code',
    'property_type', 'property_url', 'total_units', 'total_beds',
    'total_buildings', 'total_parking_spots', 'total_residential_floors',
    'total_elevators', 'owner_name', 'developer_name', 'gc_name',
    'architect_name', 'designer_name', 'year_built', 'hero_image_url',
    'property_phone', 'property_email', 'property_manager_name',
  ];

  for (const field of fillable) {
    const existingVal = existingProp[field];
    const newVal = newData[field];
    if (newVal != null && newVal !== '' && newVal !== 0) {
      // Fill if existing is null/empty/placeholder
      if (existingVal == null || existingVal === '' || existingVal === 'Unknown' || existingVal === 'TBD' || existingVal === '00000' || existingVal === 'other') {
        // Special case: don't overwrite non-null property_type with null
        if (field === 'property_type' && existingVal === 'other' && newVal === 'multifamily') {
          updates[field] = newVal;
          fieldsFilled++;
        } else if (existingVal == null || existingVal === '' || existingVal === 'Unknown' || existingVal === 'TBD' || existingVal === '00000') {
          updates[field] = newVal;
          fieldsFilled++;
        }
      }
    }
  }

  // Merge external_ids
  if (newData.external_ids && Object.keys(newData.external_ids).length > 0) {
    const merged = { ...(existingProp.external_ids || {}), ...newData.external_ids };
    if (JSON.stringify(merged) !== JSON.stringify(existingProp.external_ids || {})) {
      updates.external_ids = merged;
      fieldsFilled++;
    }
  }

  if (fieldsFilled > 0 && !DRY_RUN) {
    // Recalculate quality
    const merged = { ...existingProp, ...updates };
    updates.data_quality_score = calculateQuality(merged);

    const { error } = await registryIq
      .from('property_registry')
      .update(updates)
      .eq('id', existingProp.id);
    if (error && VERBOSE) log(`  ENRICH ERROR ${existingProp.property_name}: ${error.message}`);
  }

  return fieldsFilled;
}

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

async function upsertContact(contactData, existingContactIndex) {
  if (!contactData.name || contactData.name.trim().length < 2) return null;
  const nameParts = contactData.name.trim().split(/\s+/);
  const first = nameParts[0];
  const last = nameParts.slice(1).join(' ') || '';
  const key = `${first} ${last}`.toUpperCase().trim();

  const existing = existingContactIndex.get(key);
  if (existing) {
    // Enrich existing contact with email/phone if missing
    const updates = {};
    if (!existing.email && contactData.email) updates.email = contactData.email;
    if (!existing.phone && contactData.phone) updates.phone = contactData.phone;
    if (!existing.title && contactData.title) updates.title = contactData.title;
    if (Object.keys(updates).length > 0 && !DRY_RUN) {
      await registryIq.from('contact_registry').update(updates).eq('id', existing.id);
      stats.contacts_added++;
    }
    return existing.id;
  }

  if (DRY_RUN) { stats.contacts_added++; return null; }

  const { data, error } = await registryIq
    .from('contact_registry')
    .insert({
      first_name: first,
      last_name: last,
      email: contactData.email || null,
      phone: contactData.phone || null,
      title: contactData.title || null,
      is_active: true,
    })
    .select('id')
    .single();

  if (!error && data) {
    existingContactIndex.set(key, { id: data.id, first_name: first, last_name: last, email: contactData.email, phone: contactData.phone });
    stats.contacts_added++;
    return data.id;
  }
  return null;
}

async function upsertStakeholder(sData, existingStakeholderIndex) {
  const key = sData.stakeholder_name.toUpperCase().trim();
  const existing = existingStakeholderIndex.get(key);

  if (existing) {
    const updates = {};
    if (!existing.website && sData.website) updates.website = sData.website;
    if (!existing.hq_address_line1 && sData.hq_address_line1) updates.hq_address_line1 = sData.hq_address_line1;
    if (!existing.hq_city && sData.hq_city) updates.hq_city = sData.hq_city;
    if (!existing.hq_state && sData.hq_state) updates.hq_state = sData.hq_state;
    if (!existing.hq_postal_code && sData.hq_postal_code) updates.hq_postal_code = sData.hq_postal_code;
    if (!existing.phone && sData.contact_phone) updates.phone = sData.contact_phone;
    if (!existing.email && sData.contact_email) updates.email = sData.contact_email;
    if (Object.keys(updates).length > 0) {
      if (!DRY_RUN) {
        await registryIq.from('stakeholder_registry').update(updates).eq('id', existing.id);
      }
      stats.stakeholders_enriched++;
    }
    return existing.id;
  }

  if (DRY_RUN) { stats.stakeholders_added++; return null; }

  const { data, error } = await registryIq
    .from('stakeholder_registry')
    .insert({
      stakeholder_name: sData.stakeholder_name,
      stakeholder_type: sData.stakeholder_type || 'other',
      website: sData.website || null,
      hq_address_line1: sData.hq_address_line1 || null,
      hq_city: sData.hq_city || null,
      hq_state: sData.hq_state || null,
      hq_postal_code: sData.hq_postal_code || null,
      phone: sData.contact_phone || null,
      email: sData.contact_email || null,
      is_active: true,
    })
    .select('id')
    .single();

  if (!error && data) {
    existingStakeholderIndex.set(key, { id: data.id, ...sData });
    stats.stakeholders_added++;
    return data.id;
  }
  return null;
}

const VALID_ROLES = ['owner', 'developer', 'brand', 'architect', 'designer', 'gc',
  'property_manager', 'asset_manager', 'investor', 'lender',
  'ff_e_specifier', 'interior_designer', 'purchasing_agent', 'other'];

async function linkPropertyStakeholder(propertyId, stakeholderId, stakeholderName, role) {
  const mappedRole = VALID_ROLES.includes(role) ? role : 'other';

  // Check if already linked
  const { data: existing } = await registryIq
    .from('property_stakeholders')
    .select('id')
    .eq('property_id', propertyId)
    .ilike('stakeholder_name', stakeholderName)
    .limit(1)
    .single();

  if (existing) return;

  if (!DRY_RUN) {
    await registryIq.from('property_stakeholders').insert({
      property_id: propertyId,
      stakeholder_id: stakeholderId,
      stakeholder_name: stakeholderName,
      company_name: stakeholderName,
      role: mappedRole,
      is_primary: role === 'developer' || role === 'gc' || role === 'owner',
    });
  }
}

/* ═══════════════════════════════════════════════════════════════
   MAIN
   ═══════════════════════════════════════════════════════════════ */

async function main() {
  console.log(`\nProperty Registry — Incremental Enrichment (Wave 2)`);
  console.log(`Mode: ${DRY_RUN ? 'DRY RUN' : 'LIVE'}${SOURCE_FILTER ? ` | Source: ${SOURCE_FILTER}` : ''}\n`);

  // Load existing data
  const { index: propIndex } = await loadExistingProperties();
  const { index: stakeholderIndex } = await loadExistingStakeholders();
  const { index: contactIndex } = await loadExistingContacts();

  // Collect all new property data
  let allNewProperties = [];

  if (!SOURCE_FILTER || SOURCE_FILTER === 'iq_property_registry') {
    allNewProperties.push(...await extractIqPropertyRegistry());
  }
  if (!SOURCE_FILTER || SOURCE_FILTER === 'layout_iq') {
    allNewProperties.push(...await extractLayoutIqProperties());
  }

  // Filter out junk entries (single-word state abbreviations, placeholders)
  allNewProperties = allNewProperties.filter(p => {
    const name = p.property_name.trim();
    if (/^[A-Z]{2}$/i.test(name)) return false; // Just a state code
    if (name.length < 3) return false;
    if (/^(NA|N\/A|TBD|TEST|SAMPLE)$/i.test(name)) return false;
    return true;
  });

  // Deduplicate within the new batch by normalized name + city + state
  const seenNew = new Map();
  const deduped = [];
  for (const p of allNewProperties) {
    const key = normalizeForMatch(p.property_name) + '|' + (p.city || '').toUpperCase() + '|' + (p.state_province || '').toUpperCase();
    if (seenNew.has(key)) {
      // Merge contacts/stakeholders into existing entry
      const existing = seenNew.get(key);
      existing._contacts.push(...(p._contacts || []));
      existing._stakeholders.push(...(p._stakeholders || []));
      // Fill nulls
      for (const f of ['address_line1','city','state_province','postal_code','property_url','total_units','total_beds','total_buildings','total_parking_spots','year_built','hero_image_url','owner_name','developer_name','gc_name']) {
        if (existing[f] == null && p[f] != null) existing[f] = p[f];
      }
      existing.external_ids = { ...(existing.external_ids || {}), ...(p.external_ids || {}) };
    } else {
      seenNew.set(key, p);
      deduped.push(p);
    }
  }
  if (allNewProperties.length !== deduped.length) {
    log(`Deduped within new batch: ${allNewProperties.length} -> ${deduped.length}`);
  }
  allNewProperties = deduped;

  // Process Layout-iQ customers (enrich stakeholder_registry)
  if (!SOURCE_FILTER || SOURCE_FILTER === 'layout_iq') {
    const customers = await extractLayoutIqCustomers();
    for (const cust of customers) {
      await upsertStakeholder(cust, stakeholderIndex);
      // Also add the customer contact
      if (cust.contact_name) {
        const contactId = await upsertContact({
          name: cust.contact_name,
          email: cust.contact_email,
          phone: cust.contact_phone,
          title: cust.contact_title,
        }, contactIndex);

        // Link contact to stakeholder
        if (contactId && !DRY_RUN) {
          const sId = stakeholderIndex.get(cust.stakeholder_name.toUpperCase().trim())?.id;
          if (sId) {
            const { data: existing } = await registryIq
              .from('contact_stakeholder_associations')
              .select('id')
              .eq('contact_id', contactId)
              .eq('stakeholder_id', sId)
              .limit(1)
              .single();
            if (!existing) {
              await registryIq.from('contact_stakeholder_associations').insert({
                contact_id: contactId,
                stakeholder_id: sId,
                role_title: cust.contact_title || 'Primary Contact',
                is_primary_contact: true,
              });
            }
          }
        }
      }
    }
  }

  // Match and enrich/insert properties
  heading('Matching & Enriching Properties');
  let matchCount = 0;
  let enrichCount = 0;
  let insertCount = 0;
  const toInsert = [];

  for (const newProp of allNewProperties) {
    const match = findMatch(newProp.property_name, newProp.city, newProp.state_province, propIndex);

    if (match) {
      matchCount++;
      const fields = await enrichProperty(match, newProp);
      if (fields > 0) {
        enrichCount++;
        stats.fields_filled += fields;
        if (VERBOSE) log(`  ENRICHED: ${newProp.property_name} (+${fields} fields)`);
      }

      // Add contacts from this source
      for (const c of (newProp._contacts || [])) {
        await upsertContact(c, contactIndex);
      }

      // Add stakeholders from this source
      for (const s of (newProp._stakeholders || [])) {
        const sId = await upsertStakeholder(s, stakeholderIndex);
        await linkPropertyStakeholder(match.id, sId, s.stakeholder_name, s.role);
      }
    } else {
      toInsert.push(newProp);
    }
  }

  stats.matched = matchCount;
  stats.enriched = enrichCount;
  log(`Matched: ${matchCount} | Enriched: ${enrichCount} | New to insert: ${toInsert.length}`);

  // Insert new properties
  if (toInsert.length > 0) {
    heading('Inserting New Properties');
    for (const newProp of toInsert) {
      if (DRY_RUN) {
        insertCount++;
        if (VERBOSE) log(`  NEW: ${newProp.property_name} | ${newProp.city || '?'}, ${newProp.state_province || '?'}`);
        continue;
      }

      const { data: inserted, error } = await registryIq
        .from('property_registry')
        .insert({
          property_name: newProp.property_name,
          address_line1: newProp.address_line1 || 'TBD',
          city: newProp.city || 'Unknown',
          state_province: newProp.state_province || 'Unknown',
          postal_code: newProp.postal_code || '00000',
          property_type: newProp.property_type || 'other',
          property_status: 'active',
          tlc_relationship: 'customer',
          property_url: newProp.property_url || null,
          total_units: newProp.total_units ? Number(newProp.total_units) : null,
          total_beds: newProp.total_beds ? Number(newProp.total_beds) : null,
          total_buildings: newProp.total_buildings ? Number(newProp.total_buildings) : null,
          total_parking_spots: newProp.total_parking_spots ? Number(newProp.total_parking_spots) : null,
          year_built: newProp.year_built ? Number(newProp.year_built) : null,
          hero_image_url: newProp.hero_image_url || null,
          owner_name: newProp.owner_name || null,
          developer_name: newProp.developer_name || null,
          gc_name: newProp.gc_name || null,
          designer_name: newProp.designer_name || null,
          external_ids: newProp.external_ids || {},
          source: newProp.source,
          data_quality_score: calculateQuality(newProp),
        })
        .select('id')
        .single();

      if (error) {
        if (VERBOSE) log(`  INSERT ERROR: ${newProp.property_name} — ${error.message}`);
        continue;
      }

      insertCount++;

      // Link stakeholders and contacts
      for (const c of (newProp._contacts || [])) {
        await upsertContact(c, contactIndex);
      }
      for (const s of (newProp._stakeholders || [])) {
        const sId = await upsertStakeholder(s, stakeholderIndex);
        if (inserted) await linkPropertyStakeholder(inserted.id, sId, s.stakeholder_name, s.role);
      }

      // Add to index so subsequent entries don't duplicate
      const key = normalizeForMatch(newProp.property_name);
      if (!propIndex.has(key)) propIndex.set(key, []);
      propIndex.get(key).push({ ...newProp, id: inserted?.id });
    }
    stats.new_inserted = insertCount;
    log(`Inserted ${insertCount} new properties`);
  }

  // Summary
  heading('Enrichment Summary');
  console.log(JSON.stringify(stats, null, 2));
  console.log();
}

main().catch(e => {
  console.error('FATAL:', e);
  process.exit(1);
});
