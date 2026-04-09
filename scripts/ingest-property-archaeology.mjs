#!/usr/bin/env node
/**
 * Property Registry — Data Archaeology Ingest
 *
 * Populates Registry-iQ from 4 data sources:
 *   1. install_schedules (DALE-Demand Supabase, 1,318 rows)
 *   2. pipeline_opportunities (DALE-Demand Supabase, filtered to Closed Won)
 *   3. BSI_ProjectSetup (Airtable appVeaJJW1qmZrDaY)
 *   4. Install-iQ Deal (Airtable appC8sodqNVpO0Ci0)
 *
 * Usage:
 *   node scripts/ingest-property-archaeology.mjs --dry-run
 *   node scripts/ingest-property-archaeology.mjs
 *   node scripts/ingest-property-archaeology.mjs --source=install_schedules
 */

import { createClient } from '@supabase/supabase-js';
import { readFileSync, existsSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';
import https from 'https';

const __dirname = dirname(fileURLToPath(import.meta.url));

/* ─── Load .env.local ─── */
const envPath = resolve(__dirname, '../.env.local');
if (existsSync(envPath)) {
  for (const line of readFileSync(envPath, 'utf8').split('\n')) {
    const m = line.match(/^([^#=]+)=(.*)$/);
    if (m) process.env[m[1].trim()] = m[2].trim();
  }
}

/* ─── CLI flags ─── */
const DRY_RUN = process.argv.includes('--dry-run');
const SOURCE_FILTER = process.argv.find(a => a.startsWith('--source='))?.split('=')[1] || null;
const VERBOSE = process.argv.includes('--verbose');

/* ─── Connections ─── */
const daleDemand = createClient(
  process.env.DALE_DEMAND_SUPABASE_URL || '',
  process.env.DALE_DEMAND_SUPABASE_KEY || '',
);

const registryIq = createClient(
  process.env.REGISTRY_IQ_SUPABASE_URL || '',
  process.env.REGISTRY_IQ_SUPABASE_SERVICE_ROLE_KEY || '',
);

const AIRTABLE_PAT = process.env.AIRTABLE_PAT || '';
const BSI_BASE_ID = 'appVeaJJW1qmZrDaY';
const INSTALL_IQ_BASE_ID = 'appC8sodqNVpO0Ci0';

/* ─── Counters ─── */
const stats = {
  sources: { install_schedules: 0, pipeline: 0, bsi: 0, install_iq: 0 },
  extracted: 0,
  deduplicated: 0,
  stakeholders: 0,
  contacts: 0,
  properties_upserted: 0,
  property_stakeholders: 0,
  skipped_inventory: 0,
  skipped_sample: 0,
  skipped_noname: 0,
};

/* ═══════════════════════════════════════════════════════════════
   UTILITIES
   ═══════════════════════════════════════════════════════════════ */

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function log(msg) { console.log(`  ${msg}`); }
function heading(msg) { console.log(`\n${'═'.repeat(60)}\n  ${msg}\n${'═'.repeat(60)}`); }

function normalizePropertyName(raw) {
  if (!raw) return '';
  return raw
    .replace(/\r?\n/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .toUpperCase()
    .replace(/^THE\s+/, '')
    .replace(/\s+(APARTMENTS?|RESIDENCES?|SUITES?|HOTEL|INN|RESORT)$/i, '');
}

function dedupeKey(name, city, state) {
  return `${normalizePropertyName(name)}|${(city || '').toUpperCase().trim()}|${(state || '').toUpperCase().trim()}`;
}

function parseInstallScheduleAddress(raw) {
  if (!raw || typeof raw !== 'string') return null;
  const lines = raw.split(/\r?\n/).map(l => l.trim()).filter(Boolean);
  if (lines.length < 2) return null;

  const lastLine = lines[lines.length - 1];
  // Try: City, ST ZIP or City, ST. ZIP or City ST ZIP
  const cityStateZip = lastLine.match(
    /^(.+?),?\s*([A-Z]{2})\.?\s+(\d{5}(?:-\d{4})?)$/i
  );
  if (!cityStateZip) {
    // Try without zip: City, ST
    const cityState = lastLine.match(/^(.+?),\s*([A-Z]{2})\.?$/i);
    if (cityState) {
      return {
        propertyName: lines[0],
        street: lines.length >= 3 ? lines[lines.length - 2] : null,
        city: cityState[1].trim(),
        state: cityState[2].toUpperCase(),
        zip: null,
      };
    }
    return null;
  }

  return {
    propertyName: lines[0],
    street: lines.length >= 3 ? lines[lines.length - 2] : null,
    city: cityStateZip[1].trim(),
    state: cityStateZip[2].toUpperCase(),
    zip: cityStateZip[3],
  };
}

function parseBSIAddress(raw) {
  if (!raw) return {};
  // Format: "150 Miller Ranch Road, Edwards CO 81632, United States"
  const noCountry = raw.replace(/,?\s*United States$/i, '').trim();
  const parts = noCountry.split(',').map(s => s.trim());
  if (parts.length >= 2) {
    const street = parts.slice(0, -1).join(', ');
    const last = parts[parts.length - 1];
    const m = last.match(/^(.+?)\s+([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$/i);
    if (m) return { street, city: m[1].trim(), state: m[2].toUpperCase(), zip: m[3] };
    const m2 = last.match(/^(.+?)\s+([A-Z]{2})$/i);
    if (m2) return { street, city: m2[1].trim(), state: m2[2].toUpperCase(), zip: null };
  }
  return { street: noCountry };
}

function parseSimpleAddress(raw) {
  if (!raw) return {};
  // Format: "1057 East Apache Blvd, Tempe, AZ 85281"
  const parts = raw.split(',').map(s => s.trim());
  if (parts.length >= 3) {
    const street = parts[0];
    const city = parts[1];
    const stateZip = parts[2].match(/^([A-Z]{2})\.?\s*(\d{5})?/i);
    if (stateZip) return { street, city, state: stateZip[1].toUpperCase(), zip: stateZip[2] || null };
  }
  if (parts.length === 2) {
    const stateZip = parts[1].match(/^([A-Z]{2})\.?\s*(\d{5})?/i);
    if (stateZip) return { street: parts[0], city: null, state: stateZip[1].toUpperCase(), zip: stateZip[2] || null };
  }
  return {};
}

const TYPE_MAP = {
  'student housing': 'student_housing', 'student': 'student_housing',
  'hotel': 'hospitality', 'hospitality': 'hospitality',
  'apartment': 'multifamily', 'multi-family': 'multifamily', 'multifamily': 'multifamily',
  'build to rent': 'build_to_rent',
  'senior housing': 'senior_living', 'assisted living': 'senior_living',
  'workforce housing': 'multifamily', 'co-living': 'multifamily',
  'renovation - sh': 'student_housing', 'renovation - other': 'other',
  'stone solution': 'other', 'millworks': 'other',
};

function mapPropertyType(raw) {
  if (!raw) return null;
  return TYPE_MAP[raw.toLowerCase().trim()] || 'other';
}

function mapRecordType(raw) {
  if (!raw) return null;
  return TYPE_MAP[raw.toLowerCase().trim()] || null;
}

async function fetchAllSupabase(client, table, query = {}) {
  const all = [];
  let from = 0;
  const pageSize = 1000;
  while (true) {
    let q = client.from(table).select(query.select || '*').range(from, from + pageSize - 1);
    if (query.filter) q = query.filter(q);
    if (query.order) q = q.order(query.order.col, { ascending: query.order.asc ?? true });
    const { data, error } = await q;
    if (error) throw new Error(`${table}: ${error.message}`);
    if (!data?.length) break;
    all.push(...data);
    if (data.length < pageSize) break;
    from += pageSize;
  }
  return all;
}

async function airtableFetchAll(baseId, tableName) {
  const all = [];
  let offset = null;
  do {
    const url = new URL(`https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}`);
    url.searchParams.set('pageSize', '100');
    if (offset) url.searchParams.set('offset', offset);

    const res = await new Promise((resolve, reject) => {
      https.get(url.toString(), { headers: { Authorization: `Bearer ${AIRTABLE_PAT}` } }, (res) => {
        let body = '';
        res.on('data', c => body += c);
        res.on('end', () => {
          try { resolve(JSON.parse(body)); }
          catch (e) { reject(e); }
        });
      }).on('error', reject);
    });

    if (res.error) throw new Error(`Airtable ${tableName}: ${JSON.stringify(res.error)}`);
    all.push(...(res.records || []));
    offset = res.offset || null;
    if (offset) await sleep(220); // rate limit
  } while (offset);
  return all;
}

/* ═══════════════════════════════════════════════════════════════
   PHASE 1: EXTRACT
   ═══════════════════════════════════════════════════════════════ */

async function extractInstallSchedules() {
  heading('Phase 1a: install_schedules (DALE-Demand)');
  const rows = await fetchAllSupabase(daleDemand, 'install_schedules');
  log(`Fetched ${rows.length} rows`);

  const properties = [];
  for (const row of rows) {
    // Skip inventory / sample / non-property rows
    const projName = (row.property_name_address || '').split(/\r?\n/)[0]?.trim() || '';
    if (/^INVENTORY\s/i.test(projName) || /^SO\s\d/i.test(projName)) { stats.skipped_inventory++; continue; }
    if (/sample/i.test(projName) || /^VN\d/i.test(projName) || /^F-\d/i.test(projName)) { stats.skipped_sample++; continue; }

    const parsed = parseInstallScheduleAddress(row.property_name_address);
    if (!parsed || !parsed.propertyName) { stats.skipped_noname++; continue; }

    const prop = {
      property_name: parsed.propertyName,
      address_line1: parsed.street || null,
      city: row.city || parsed.city || null,
      state_province: row.state || parsed.state || null,
      postal_code: parsed.zip || null,
      latitude: row.latitude || null,
      longitude: row.longitude || null,
      property_type: null,
      property_status: 'active',
      tlc_relationship: 'customer',
      source: 'install_schedules',
      external_ids: {},
      _contacts: [],
      _stakeholders: [],
    };

    if (row.deal_number) prop.external_ids.deal_number = row.deal_number;
    if (row.schedule_year) prop.external_ids.schedule_year = row.schedule_year;
    if (row.install_start_date) prop.external_ids.install_start_date = row.install_start_date;

    // Extract contacts and stakeholders
    if (row.property_contact) {
      prop._contacts.push({ name: row.property_contact, role: 'property_contact', source: 'install_schedules' });
    }
    if (row.warehouse_contact) {
      const contact = { name: row.warehouse_contact, role: 'warehouse_contact', source: 'install_schedules' };
      if (row.warehouse_email) contact.email = row.warehouse_email;
      prop._contacts.push(contact);
    }
    if (row.warehouse_address) {
      // First line of warehouse address is often the company name
      const whLines = (row.warehouse_address || '').split(/\r?\n/).map(l => l.trim()).filter(Boolean);
      if (whLines.length > 0) {
        prop._stakeholders.push({
          stakeholder_name: whLines[0],
          stakeholder_type: 'other',
          role: 'warehouse_vendor',
          hq_address: whLines.slice(1).join(', ') || null,
          source: 'install_schedules',
        });
      }
    }

    properties.push(prop);
  }

  stats.sources.install_schedules = properties.length;
  log(`Extracted ${properties.length} properties (skipped: ${stats.skipped_inventory} inventory, ${stats.skipped_sample} samples, ${stats.skipped_noname} no-name)`);
  return properties;
}

async function extractPipelineOpportunities() {
  heading('Phase 1b: pipeline_opportunities (DALE-Demand)');
  const rows = await fetchAllSupabase(daleDemand, 'pipeline_opportunities', {
    select: 'opportunity_name,account_name,project_city,project_state,type_of_project,opportunity_record_type,stage,amount,deal_number,opportunity_code,project_status_d365,opportunity_owner,project_manager,sales_rep',
    filter: (q) => q.or('stage.ilike.%Closed Won%,project_status_d365.eq.Completed'),
  });
  log(`Fetched ${rows.length} Closed Won / Completed rows`);

  const properties = [];
  const seen = new Set();
  for (const row of rows) {
    if (!row.opportunity_name) continue;
    // Skip inventory/sample lines that made it into pipeline
    const name = row.opportunity_name.trim();
    if (/^INVENTORY\s/i.test(name) || /sample/i.test(name)) continue;

    // Deduplicate within pipeline (same opportunity_name + city)
    const key = dedupeKey(name, row.project_city, row.project_state);
    if (seen.has(key)) continue;
    seen.add(key);

    const pType = mapPropertyType(row.type_of_project) || mapRecordType(row.opportunity_record_type);

    const prop = {
      property_name: name,
      address_line1: null,
      city: row.project_city || null,
      state_province: row.project_state || null,
      postal_code: null,
      latitude: null,
      longitude: null,
      property_type: pType,
      property_status: 'active',
      tlc_relationship: 'customer',
      source: 'pipeline_opportunities',
      external_ids: {},
      _contacts: [],
      _stakeholders: [],
    };

    if (row.deal_number) prop.external_ids.deal_number = row.deal_number;
    if (row.opportunity_code) prop.external_ids.opportunity_code = row.opportunity_code;
    if (row.amount) prop.external_ids.pipeline_amount = row.amount;

    if (row.account_name) {
      prop._stakeholders.push({
        stakeholder_name: row.account_name.trim(),
        stakeholder_type: 'gc',
        role: 'gc',
        source: 'pipeline_opportunities',
      });
      prop.developer_name = row.account_name.trim();
    }

    properties.push(prop);
  }

  stats.sources.pipeline = properties.length;
  log(`Extracted ${properties.length} unique properties from pipeline`);
  return properties;
}

async function extractBSI() {
  heading('Phase 1c: BSI_ProjectSetup (Airtable)');
  const records = await airtableFetchAll(BSI_BASE_ID, 'BSI_ProjectSetup');
  log(`Fetched ${records.length} records`);

  const properties = [];
  for (const rec of records) {
    const f = rec.fields;
    const propName = f.Property_Name || f.Project_Name;
    if (!propName) continue;

    const addr = parseBSIAddress(f['Property_StreetAddress_City_State_Zip_Country']);

    const prop = {
      property_name: propName.trim(),
      address_line1: addr.street || null,
      city: addr.city || null,
      state_province: addr.state || null,
      postal_code: addr.zip || null,
      latitude: null,
      longitude: null,
      property_type: mapPropertyType(f.Project_Type) || null,
      property_status: f.Status ? 'active' : 'active',
      tlc_relationship: 'customer',
      property_url: f.Property_URL || null,
      total_units: f['Property_#Units'] || null,
      total_beds: f['Property_#Beds'] || null,
      total_buildings: f['Property_#Buildings'] || null,
      total_residential_floors: f['Property_Building_#Floors'] || null,
      total_elevators: f['Property_Building_#Elevators'] || null,
      total_parking_spots: f['Property_Building_#ParkingSpots'] || null,
      skip_13th_floor: f['Property_Building_Skip13thFloor'] || false,
      source: 'bsi_project_setup',
      external_ids: {},
      _contacts: [],
      _stakeholders: [],
    };

    if (f.Project_ID) prop.external_ids.bsi_project_id = f.Project_ID;
    if (f.Customer_ID) prop.external_ids.bsi_customer_id = f.Customer_ID;
    if (f.Developer_ID) prop.external_ids.bsi_developer_id = f.Developer_ID;

    if (f.Customer_Name) {
      prop._stakeholders.push({
        stakeholder_name: f.Customer_Name.trim(),
        stakeholder_type: 'gc',
        role: 'gc',
        website: f.Customer_URL || null,
        external_ids: f.Customer_ID ? { bsi_id: f.Customer_ID } : {},
        source: 'bsi_project_setup',
      });
      prop.developer_name = f.Customer_Name.trim();
    }
    if (f.Developer_Name) {
      prop._stakeholders.push({
        stakeholder_name: f.Developer_Name.trim(),
        stakeholder_type: 'developer',
        role: 'developer',
        website: f.Developer_URL || null,
        external_ids: f.Developer_ID ? { bsi_id: f.Developer_ID } : {},
        source: 'bsi_project_setup',
      });
      prop.owner_name = f.Developer_Name.trim();
    }

    properties.push(prop);
  }

  stats.sources.bsi = properties.length;
  log(`Extracted ${properties.length} properties from BSI`);
  return properties;
}

async function extractInstallIQ() {
  heading('Phase 1d: Install-iQ Deal (Airtable)');
  const records = await airtableFetchAll(INSTALL_IQ_BASE_ID, 'Deal');
  log(`Fetched ${records.length} records`);

  const properties = [];
  for (const rec of records) {
    const f = rec.fields;
    const propName = f.project_name;
    if (!propName) continue;

    const addr = parseSimpleAddress(f.address);

    const prop = {
      property_name: propName.trim(),
      address_line1: addr.street || null,
      city: addr.city || null,
      state_province: f.state || addr.state || null,
      postal_code: addr.zip || null,
      latitude: null,
      longitude: null,
      property_type: null,
      property_status: mapInstallStatus(f.install_status),
      tlc_relationship: 'customer',
      source: 'install_iq_deal',
      external_ids: {},
      _contacts: [],
      _stakeholders: [],
    };

    if (f.deal_number) prop.external_ids.deal_number = f.deal_number;
    if (f.deal) prop.external_ids.deal_code = f.deal;

    properties.push(prop);
  }

  stats.sources.install_iq = properties.length;
  log(`Extracted ${properties.length} properties from Install-iQ`);
  return properties;
}

function mapInstallStatus(raw) {
  if (!raw) return 'active';
  const s = raw.toLowerCase();
  if (s.includes('completed') || s.includes('complete')) return 'active';
  if (s.includes('in progress')) return 'under_construction';
  if (s.includes('not started')) return 'pre_development';
  return 'active';
}

/* ═══════════════════════════════════════════════════════════════
   PHASE 2: DEDUPLICATE + MERGE
   ═══════════════════════════════════════════════════════════════ */

function deduplicateAndMerge(allProperties) {
  heading('Phase 2: Deduplicate & Merge');
  const map = new Map(); // dedupeKey -> merged property
  const dealMap = new Map(); // deal_number -> dedupeKey

  // Priority: BSI > install_schedules > install_iq > pipeline
  const sourcePriority = { bsi_project_setup: 0, install_schedules: 1, install_iq_deal: 2, pipeline_opportunities: 3 };
  allProperties.sort((a, b) => (sourcePriority[a.source] ?? 9) - (sourcePriority[b.source] ?? 9));

  for (const prop of allProperties) {
    const key = dedupeKey(prop.property_name, prop.city, prop.state_province);
    const dealNum = prop.external_ids?.deal_number;

    // Check deal_number match first
    let existingKey = null;
    if (dealNum && dealMap.has(dealNum)) {
      existingKey = dealMap.get(dealNum);
    }
    if (!existingKey && map.has(key)) {
      existingKey = key;
    }

    if (existingKey) {
      // Merge into existing
      const existing = map.get(existingKey);
      mergeProperty(existing, prop);
      if (dealNum) dealMap.set(dealNum, existingKey);
    } else {
      map.set(key, { ...prop });
      if (dealNum) dealMap.set(dealNum, key);
    }
  }

  const result = Array.from(map.values());
  stats.extracted = allProperties.length;
  stats.deduplicated = result.length;
  log(`${allProperties.length} raw -> ${result.length} unique properties (${allProperties.length - result.length} merged)`);
  return result;
}

function mergeProperty(target, source) {
  // Fill nulls from source; never overwrite non-null with null
  const fillFields = [
    'address_line1', 'city', 'state_province', 'postal_code',
    'latitude', 'longitude', 'property_type', 'property_url',
    'total_units', 'total_beds', 'total_buildings',
    'total_residential_floors', 'total_elevators', 'total_parking_spots',
    'owner_name', 'developer_name',
  ];
  for (const f of fillFields) {
    if (target[f] == null && source[f] != null) target[f] = source[f];
  }

  // Merge external_ids
  target.external_ids = { ...(target.external_ids || {}), ...(source.external_ids || {}) };

  // Append source info
  if (!target._all_sources) target._all_sources = [target.source];
  target._all_sources.push(source.source);

  // Merge contacts and stakeholders
  target._contacts = [...(target._contacts || []), ...(source._contacts || [])];
  target._stakeholders = [...(target._stakeholders || []), ...(source._stakeholders || [])];
}

/* ═══════════════════════════════════════════════════════════════
   PHASE 3-4: EXTRACT STAKEHOLDERS + CONTACTS
   ═══════════════════════════════════════════════════════════════ */

function extractStakeholdersAndContacts(properties) {
  heading('Phase 3-4: Extract Stakeholders & Contacts');

  const stakeholderMap = new Map(); // normalized name -> stakeholder
  const contactMap = new Map(); // normalized name -> contact

  for (const prop of properties) {
    for (const s of (prop._stakeholders || [])) {
      const key = s.stakeholder_name.toUpperCase().trim();
      if (!stakeholderMap.has(key)) {
        stakeholderMap.set(key, {
          stakeholder_name: s.stakeholder_name,
          stakeholder_type: s.stakeholder_type || 'other',
          website: s.website || null,
          external_ids: s.external_ids || {},
          hq_address_line1: s.hq_address || null,
        });
      } else {
        const existing = stakeholderMap.get(key);
        if (!existing.website && s.website) existing.website = s.website;
        existing.external_ids = { ...existing.external_ids, ...(s.external_ids || {}) };
      }
    }

    for (const c of (prop._contacts || [])) {
      if (!c.name || c.name.trim().length < 2) continue;
      const key = c.name.toUpperCase().trim();
      if (!contactMap.has(key)) {
        const nameParts = c.name.trim().split(/\s+/);
        contactMap.set(key, {
          first_name: nameParts[0] || c.name.trim(),
          last_name: nameParts.slice(1).join(' ') || '',
          email: c.email || null,
          _role: c.role,
        });
      } else {
        const existing = contactMap.get(key);
        if (!existing.email && c.email) existing.email = c.email;
      }
    }
  }

  stats.stakeholders = stakeholderMap.size;
  stats.contacts = contactMap.size;
  log(`Unique stakeholder companies: ${stakeholderMap.size}`);
  log(`Unique external contacts: ${contactMap.size}`);

  return {
    stakeholders: Array.from(stakeholderMap.values()),
    contacts: Array.from(contactMap.values()),
  };
}

/* ═══════════════════════════════════════════════════════════════
   PHASE 5: UPSERT TO REGISTRY-IQ
   ═══════════════════════════════════════════════════════════════ */

async function upsertToRegistryIQ(properties, stakeholders, contacts) {
  heading('Phase 5: Upsert to Registry-iQ');

  if (DRY_RUN) {
    log('DRY RUN — no writes will be performed');
    log(`Would upsert ${stakeholders.length} stakeholders`);
    log(`Would upsert ${contacts.length} contacts`);
    log(`Would upsert ${properties.length} properties`);

    if (VERBOSE) {
      log('\nSample properties (first 10):');
      for (const p of properties.slice(0, 10)) {
        log(`  ${p.property_name} | ${p.city || '?'}, ${p.state_province || '?'} | ${p.property_type || 'unknown'} | src: ${p._all_sources?.join('+') || p.source}`);
      }
      log('\nSample stakeholders (first 10):');
      for (const s of stakeholders.slice(0, 10)) {
        log(`  ${s.stakeholder_name} (${s.stakeholder_type})`);
      }
    }
    return;
  }

  // 5a. Upsert stakeholders
  log('Upserting stakeholders...');
  const stakeholderIdMap = new Map(); // name_upper -> uuid
  for (const s of stakeholders) {
    const { data, error } = await registryIq
      .from('stakeholder_registry')
      .upsert({
        stakeholder_name: s.stakeholder_name,
        stakeholder_type: s.stakeholder_type,
        website: s.website,
        external_ids: Object.keys(s.external_ids).length > 0 ? s.external_ids : {},
        hq_address_line1: s.hq_address_line1,
        is_active: true,
      }, { onConflict: 'stakeholder_name', ignoreDuplicates: false })
      .select('id, stakeholder_name')
      .single();

    if (error) {
      // Upsert failed (no unique constraint on name) — try insert or find
      const { data: existing } = await registryIq
        .from('stakeholder_registry')
        .select('id')
        .ilike('stakeholder_name', s.stakeholder_name)
        .limit(1)
        .single();

      if (existing) {
        stakeholderIdMap.set(s.stakeholder_name.toUpperCase().trim(), existing.id);
      } else {
        const { data: inserted, error: insertErr } = await registryIq
          .from('stakeholder_registry')
          .insert({
            stakeholder_name: s.stakeholder_name,
            stakeholder_type: s.stakeholder_type,
            website: s.website,
            external_ids: Object.keys(s.external_ids).length > 0 ? s.external_ids : {},
            hq_address_line1: s.hq_address_line1,
            is_active: true,
          })
          .select('id')
          .single();
        if (!insertErr && inserted) {
          stakeholderIdMap.set(s.stakeholder_name.toUpperCase().trim(), inserted.id);
        }
      }
    } else if (data) {
      stakeholderIdMap.set(s.stakeholder_name.toUpperCase().trim(), data.id);
    }
  }
  log(`  ${stakeholderIdMap.size} stakeholders in registry`);

  // 5b. Upsert contacts
  log('Upserting contacts...');
  const contactIdMap = new Map();
  for (const c of contacts) {
    if (!c.first_name || c.first_name.length < 2) continue;
    const { data: existing } = await registryIq
      .from('contact_registry')
      .select('id')
      .eq('first_name', c.first_name)
      .eq('last_name', c.last_name || '')
      .limit(1)
      .single();

    if (existing) {
      contactIdMap.set(`${c.first_name} ${c.last_name}`.toUpperCase().trim(), existing.id);
    } else {
      const { data: inserted, error } = await registryIq
        .from('contact_registry')
        .insert({
          first_name: c.first_name,
          last_name: c.last_name || '',
          email: c.email,
          is_active: true,
        })
        .select('id')
        .single();
      if (!error && inserted) {
        contactIdMap.set(`${c.first_name} ${c.last_name}`.toUpperCase().trim(), inserted.id);
        stats.contacts++;
      }
    }
  }
  log(`  ${contactIdMap.size} contacts in registry`);

  // 5c. Upsert properties
  log('Upserting properties...');
  let propCount = 0;
  let psCount = 0;
  const BATCH_SIZE = 50;

  for (let i = 0; i < properties.length; i += BATCH_SIZE) {
    const batch = properties.slice(i, i + BATCH_SIZE);
    const rows = batch.map(p => ({
      property_name: p.property_name,
      address_line1: p.address_line1 || 'TBD',
      city: p.city || 'Unknown',
      state_province: p.state_province || 'Unknown',
      postal_code: p.postal_code || '00000',
      latitude: p.latitude || null,
      longitude: p.longitude || null,
      property_type: p.property_type || 'other',
      property_status: p.property_status || 'active',
      tlc_relationship: p.tlc_relationship || 'customer',
      property_url: p.property_url || null,
      total_units: p.total_units ? Number(p.total_units) : null,
      total_beds: p.total_beds ? Number(p.total_beds) : null,
      total_buildings: p.total_buildings ? Number(p.total_buildings) : null,
      total_residential_floors: p.total_residential_floors ? Number(p.total_residential_floors) : null,
      total_elevators: p.total_elevators ? Number(p.total_elevators) : null,
      total_parking_spots: p.total_parking_spots ? Number(p.total_parking_spots) : null,
      skip_13th_floor: p.skip_13th_floor || false,
      owner_name: p.owner_name || null,
      developer_name: p.developer_name || null,
      external_ids: p.external_ids || {},
      source: p.source,
      notes: p._all_sources?.length > 1 ? `Cross-referenced from: ${p._all_sources.join(', ')}` : null,
      data_quality_score: calculateQuality(p),
    }));

    const { data: inserted, error } = await registryIq
      .from('property_registry')
      .insert(rows)
      .select('id, property_name');

    if (error) {
      // Try one by one on batch error
      for (const row of rows) {
        const { data: single, error: sErr } = await registryIq
          .from('property_registry')
          .insert(row)
          .select('id, property_name')
          .single();
        if (sErr) {
          if (VERBOSE) log(`  SKIP (error): ${row.property_name} — ${sErr.message}`);
        } else if (single) {
          propCount++;
          await linkStakeholders(single.id, batch.find(b => b.property_name === row.property_name), stakeholderIdMap);
          psCount++;
        }
      }
    } else if (inserted) {
      propCount += inserted.length;
      for (const ins of inserted) {
        const original = batch.find(b => b.property_name === ins.property_name);
        if (original) {
          await linkStakeholders(ins.id, original, stakeholderIdMap);
          psCount++;
        }
      }
    }

    if ((i + BATCH_SIZE) % 200 === 0) {
      process.stdout.write(`\r  Inserted ${propCount} properties...`);
    }
  }

  stats.properties_upserted = propCount;
  stats.property_stakeholders = psCount;
  log(`\n  ${propCount} properties inserted`);
}

async function linkStakeholders(propertyId, prop, stakeholderIdMap) {
  if (!prop?._stakeholders?.length) return;

  const seen = new Set();
  for (const s of prop._stakeholders) {
    const key = `${s.role}|${s.stakeholder_name.toUpperCase().trim()}`;
    if (seen.has(key)) continue;
    seen.add(key);

    const sId = stakeholderIdMap.get(s.stakeholder_name.toUpperCase().trim()) || null;
    await registryIq.from('property_stakeholders').insert({
      property_id: propertyId,
      stakeholder_id: sId,
      stakeholder_name: s.stakeholder_name,
      company_name: s.stakeholder_name,
      role: mapStakeholderRole(s.role),
      is_primary: s.role === 'developer' || s.role === 'gc',
    });
  }
}

function mapStakeholderRole(role) {
  const valid = ['owner', 'developer', 'brand', 'architect', 'designer', 'gc',
    'property_manager', 'asset_manager', 'investor', 'lender',
    'ff_e_specifier', 'interior_designer', 'purchasing_agent', 'other'];
  if (role === 'warehouse_vendor') return 'other';
  return valid.includes(role) ? role : 'other';
}

function calculateQuality(prop) {
  let score = 0;
  if (prop.property_name) score += 15;
  if (prop.address_line1 && prop.address_line1 !== 'TBD') score += 15;
  if (prop.city && prop.city !== 'Unknown') score += 10;
  if (prop.state_province && prop.state_province !== 'Unknown') score += 10;
  if (prop.postal_code && prop.postal_code !== '00000') score += 5;
  if (prop.latitude) score += 10;
  if (prop.property_type && prop.property_type !== 'other') score += 10;
  if (prop.total_units) score += 5;
  if (prop.total_beds) score += 5;
  if (prop.property_url) score += 5;
  if (prop.owner_name || prop.developer_name) score += 5;
  if (prop._all_sources?.length > 1) score += 5;
  return Math.min(score, 100);
}

/* ═══════════════════════════════════════════════════════════════
   MAIN
   ═══════════════════════════════════════════════════════════════ */

async function main() {
  console.log(`\nProperty Registry — Data Archaeology Ingest`);
  console.log(`Mode: ${DRY_RUN ? 'DRY RUN' : 'LIVE'}${SOURCE_FILTER ? ` | Source: ${SOURCE_FILTER}` : ''}`);

  // Phase 1: Extract
  let allProperties = [];

  if (!SOURCE_FILTER || SOURCE_FILTER === 'install_schedules') {
    allProperties.push(...await extractInstallSchedules());
  }
  if (!SOURCE_FILTER || SOURCE_FILTER === 'pipeline') {
    allProperties.push(...await extractPipelineOpportunities());
  }
  if (!SOURCE_FILTER || SOURCE_FILTER === 'bsi') {
    allProperties.push(...await extractBSI());
  }
  if (!SOURCE_FILTER || SOURCE_FILTER === 'install_iq') {
    allProperties.push(...await extractInstallIQ());
  }

  if (allProperties.length === 0) {
    log('No properties extracted. Check source connections.');
    return;
  }

  // Phase 2: Deduplicate
  const merged = deduplicateAndMerge(allProperties);

  // Phase 3-4: Stakeholders + Contacts
  const { stakeholders, contacts } = extractStakeholdersAndContacts(merged);

  // Phase 5: Upsert
  await upsertToRegistryIQ(merged, stakeholders, contacts);

  // Summary
  heading('Summary');
  console.log(JSON.stringify(stats, null, 2));
  console.log();
}

main().catch(e => {
  console.error('FATAL:', e);
  process.exit(1);
});
