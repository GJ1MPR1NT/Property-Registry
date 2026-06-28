#!/usr/bin/env node
/**
 * ingest-corespaces-prismic.mjs
 *
 * Pull Core Spaces portfolio from their public Prismic CMS (powers corespaces.com/communities)
 * and upsert into Registry-iQ property_registry + property_stakeholders.
 *
 * Source: https://core-spaces-website.cdn.prismic.io (token embedded in site JS — read-only)
 *
 * Captures: name, address, city/state/zip, lat/lng, university, brand, units/beds/SF,
 * coming-soon flag, owned-by-core (sold portfolio), property/leasing URLs, hero + gallery images.
 *
 * Sold properties (is_owned_by_core=false): developer stays Core Spaces; flags
 * enrichment_sources.needs_ownership_research for press-release / buyer assignment follow-up.
 *
 * Usage:
 *   node scripts/ingest-corespaces-prismic.mjs --dry-run
 *   node scripts/ingest-corespaces-prismic.mjs --apply
 *   node scripts/ingest-corespaces-prismic.mjs --apply --limit=10
 *   node scripts/ingest-corespaces-prismic.mjs --apply --only=bloomington
 */

import { createClient } from '@supabase/supabase-js';
import { readFileSync, writeFileSync, existsSync } from 'fs';
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

const DRY_RUN = !process.argv.includes('--apply');
const LIMIT = parseInt(process.argv.find((a) => a.startsWith('--limit='))?.split('=')[1] || '0', 10) || null;
const ONLY = process.argv.find((a) => a.startsWith('--only='))?.split('=')[1]?.toLowerCase() || null;

const PRISMIC_TOKEN =
  process.env.CORESPACES_PRISMIC_TOKEN ||
  'MC5ZZUJpZFJJQUFDTUFXRkNJ.77-977-9Vnfvv70zPO-_ve-_vXxO77-977-977-9CO-_ve-_ve-_ve-_ve-_ve-_vW5aBe-_ve-_ve-_vXte77-977-977-9';

const STATE_ABBR = {
  Alabama: 'AL', Alaska: 'AK', Arizona: 'AZ', Arkansas: 'AR', California: 'CA', Colorado: 'CO',
  Connecticut: 'CT', Delaware: 'DE', 'District of Columbia': 'DC', Florida: 'FL', Georgia: 'GA',
  Hawaii: 'HI', Idaho: 'ID', Illinois: 'IL', Indiana: 'IN', Iowa: 'IA', Kansas: 'KS',
  Kentucky: 'KY', Louisiana: 'LA', Maine: 'ME', Maryland: 'MD', Massachusetts: 'MA',
  Michigan: 'MI', Minnesota: 'MN', Mississippi: 'MS', Missouri: 'MO', Montana: 'MT',
  Nebraska: 'NE', Nevada: 'NV', 'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM',
  'New York': 'NY', 'North Carolina': 'NC', 'North Dakota': 'ND', Ohio: 'OH', Oklahoma: 'OK',
  Oregon: 'OR', Pennsylvania: 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC',
  'South Dakota': 'SD', Tennessee: 'TN', Texas: 'TX', Utah: 'UT', Vermont: 'VT',
  Virginia: 'VA', Washington: 'WA', 'West Virginia': 'WV', Wisconsin: 'WI', Wyoming: 'WY',
};

const stats = {
  fetched: 0,
  matched: 0,
  inserted: 0,
  updated: 0,
  skipped: 0,
  coming_soon: 0,
  sold_portfolio: 0,
  stakeholder_links: 0,
  errors: 0,
};

function richText(val) {
  if (!val) return null;
  if (typeof val === 'string') return val.trim() || null;
  if (Array.isArray(val)) {
    return val.map((b) => (typeof b === 'object' && b?.text ? b.text : '')).join(' ').trim() || null;
  }
  return String(val);
}

function linkUrl(val) {
  if (!val || typeof val !== 'object') return null;
  return val.url?.trim() || null;
}

function prismicImageUrl(imgField) {
  if (!imgField || typeof imgField !== 'object') return null;
  return imgField.url || null;
}

function normalizeState(raw) {
  if (!raw) return null;
  const s = String(raw).trim();
  if (s.length === 2) return s.toUpperCase();
  return STATE_ABBR[s] || s;
}

function normalizeForMatch(raw) {
  if (!raw) return '';
  return raw
    .replace(/\r?\n/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .toUpperCase()
    .replace(/^THE\s+/, '')
    .replace(/Ō|Ō|ō/gi, 'O')
    .replace(/[^A-Z0-9 ]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function mapPropertyStatus({ isComingSoon, isOwnedByCore, year }) {
  if (isComingSoon) {
    const y = year && Number(year);
    const now = new Date().getFullYear();
    if (y && y <= now) return 'under_construction';
    return 'pre_development';
  }
  if (isOwnedByCore === false) return 'inactive';
  return 'active';
}

async function fetchPrismic(type, ref, extraParams = {}) {
  const params = new URLSearchParams({
    q: `[[at(document.type,"${type}")]]`,
    pageSize: '100',
    page: '1',
    ref,
    access_token: PRISMIC_TOKEN,
    ...extraParams,
  });
  if (type === 'property') {
    params.set('orderings', '[my.property.year desc,my.property.name]');
  }
  const url = `https://core-spaces-website.cdn.prismic.io/api/v2/documents/search?${params}`;
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`Prismic ${type}: HTTP ${resp.status}`);
  const data = await resp.json();
  return data.results || [];
}

async function loadPrismicPortfolio() {
  const apiResp = await fetch(
    `https://core-spaces-website.cdn.prismic.io/api/v2?access_token=${encodeURIComponent(PRISMIC_TOKEN)}`,
  );
  const api = await apiResp.json();
  const ref = api.refs?.[0]?.ref;
  if (!ref) throw new Error('No Prismic ref');

  const [properties, brands] = await Promise.all([
    fetchPrismic('property', ref),
    fetchPrismic('property_brand', ref),
  ]);

  const brandMap = {};
  for (const b of brands) {
    brandMap[b.uid || b.id] = richText(b.data?.brand_name) || b.uid;
  }

  const normalized = properties.map((doc) => {
    const d = doc.data || {};
    const brandUid = d.brand?.uid || d.brand?.id || null;
    const images = (d.images || [])
      .map((row) => {
        const url = prismicImageUrl(row?.image);
        if (!url) return null;
        return {
          url,
          alt: row.image?.alt || null,
          dimensions: row.image?.dimensions || null,
        };
      })
      .filter(Boolean);

    const isComingSoon = Boolean(d.is_coming_soon);
    const isOwnedByCore = d.is_owned_by_core !== false;

    return {
      prismic_id: doc.id,
      prismic_uid: doc.uid || null,
      property_name: richText(d.name),
      address_line1: richText(d.address) || 'TBD',
      city: d.city || 'Unknown',
      state_province: normalizeState(d.state) || 'Unknown',
      postal_code: d.zip_code || '00000',
      latitude: d.location?.latitude ?? null,
      longitude: d.location?.longitude ?? null,
      university_name: richText(d.university) || (typeof d.university === 'string' ? d.university : null),
      brand_name: brandMap[brandUid] || brandUid,
      brand_uid: brandUid,
      property_url: linkUrl(d.url),
      year_built: d.year ? Number(d.year) : null,
      total_units: d.units ?? null,
      total_beds: d.beds ?? null,
      gross_sf: d.gross_sf ?? null,
      residential_sf: d.residential_sf ?? null,
      retail_sf: d.retail_sf ?? null,
      is_coming_soon: isComingSoon,
      is_owned_by_core: isOwnedByCore,
      property_status: mapPropertyStatus({
        isComingSoon,
        isOwnedByCore,
        year: d.year,
      }),
      hero_image_url: images[0]?.url || null,
      images,
      highlights: (d.highlights || []).map((h) => richText(h?.text || h)).filter(Boolean),
    };
  }).filter((p) => p.property_name);

  writeFileSync(
    resolve(ROOT, '.firecrawl/corespaces-prismic-normalized.json'),
    JSON.stringify({ ref, count: normalized.length, properties: normalized, brandMap }, null, 2),
  );

  return { ref, normalized, brandMap };
}

function buildUpdate(existing, incoming) {
  const patch = {};
  const fillIfEmpty = [
    'address_line1', 'city', 'state_province', 'postal_code', 'latitude', 'longitude',
    'university_name', 'brand_name', 'property_url', 'year_built', 'total_units', 'total_beds',
    'hero_image_url', 'developer_name',
  ];

  for (const key of fillIfEmpty) {
    const cur = existing[key];
    const val = incoming[key];
    if (val == null || val === '' || val === 'TBD' || val === 'Unknown' || val === '00000') continue;
    if (cur == null || cur === '' || cur === 'TBD' || cur === 'Unknown' || cur === '00000') {
      patch[key] = val;
    }
  }

  // Always refresh Core Spaces canonical fields when source is authoritative
  if (incoming.brand_name) patch.brand_name = incoming.brand_name;
  if (incoming.property_status) patch.property_status = incoming.property_status;
  if (incoming.developer_name) patch.developer_name = incoming.developer_name;
  if (incoming.property_url) patch.property_url = incoming.property_url;
  if (incoming.latitude != null) patch.latitude = incoming.latitude;
  if (incoming.longitude != null) patch.longitude = incoming.longitude;
  if (incoming.total_units != null) patch.total_units = incoming.total_units;
  if (incoming.total_beds != null) patch.total_beds = incoming.total_beds;
  if (incoming.university_name) patch.university_name = incoming.university_name;
  if (incoming.hero_image_url) patch.hero_image_url = incoming.hero_image_url;

  const ext = { ...(existing.external_ids || {}) };
  ext.prismic_id = incoming.prismic_id;
  if (incoming.prismic_uid) ext.corespaces_uid = incoming.prismic_uid;
  if (incoming.brand_uid) ext.corespaces_brand_uid = incoming.brand_uid;
  ext.corespaces_is_coming_soon = incoming.is_coming_soon;
  ext.corespaces_is_owned_by_core = incoming.is_owned_by_core;
  if (incoming.gross_sf != null) ext.corespaces_gross_sf = incoming.gross_sf;
  if (incoming.residential_sf != null) ext.corespaces_residential_sf = incoming.residential_sf;
  if (incoming.retail_sf != null) ext.corespaces_retail_sf = incoming.retail_sf;
  patch.external_ids = ext;

  const prevSources = Array.isArray(existing.enrichment_sources) ? existing.enrichment_sources : [];
  const sourceEntry = {
    type: 'corespaces_prismic',
    url: 'https://corespaces.com/communities',
    prismic_id: incoming.prismic_id,
    at: new Date().toISOString(),
  };
  if (!incoming.is_owned_by_core) {
    sourceEntry.needs_ownership_research = true;
    sourceEntry.note = 'Developed by Core Spaces; no longer owned — assign current owner/PM via press releases';
  }
  patch.enrichment_sources = [...prevSources.filter((s) => s.type !== 'corespaces_prismic'), sourceEntry];

  if (incoming.images?.length) {
    patch.images = incoming.images;
  }

  if (incoming.highlights?.length) {
    const noteLines = incoming.highlights.slice(0, 5).join('; ');
    patch.notes = existing.notes
      ? `${existing.notes}\n[Core Spaces highlights] ${noteLines}`
      : `[Core Spaces highlights] ${noteLines}`;
  }

  patch.last_enrichment_at = new Date().toISOString();
  patch.source = existing.source || 'corespaces_prismic';
  patch.source_detail = 'corespaces.com/communities via Prismic CMS';
  patch.data_quality_score = Math.max(existing.data_quality_score || 0, 75);

  return patch;
}

function buildInsert(incoming) {
  const ext = {
    prismic_id: incoming.prismic_id,
    corespaces_brand_uid: incoming.brand_uid,
    corespaces_is_coming_soon: incoming.is_coming_soon,
    corespaces_is_owned_by_core: incoming.is_owned_by_core,
  };
  if (incoming.gross_sf != null) ext.corespaces_gross_sf = incoming.gross_sf;
  if (incoming.residential_sf != null) ext.corespaces_residential_sf = incoming.residential_sf;
  if (incoming.retail_sf != null) ext.corespaces_retail_sf = incoming.retail_sf;

  const enrichment_sources = [{
    type: 'corespaces_prismic',
    url: 'https://corespaces.com/communities',
    prismic_id: incoming.prismic_id,
    at: new Date().toISOString(),
    ...(incoming.is_owned_by_core === false
      ? { needs_ownership_research: true, note: 'Developed and sold — research current owner/PM' }
      : {}),
  }];

  return {
    property_name: incoming.property_name,
    address_line1: incoming.address_line1,
    city: incoming.city,
    state_province: incoming.state_province,
    postal_code: incoming.postal_code,
    latitude: incoming.latitude,
    longitude: incoming.longitude,
    property_type: 'student_housing',
    property_subtype: incoming.brand_uid || null,
    property_status: incoming.property_status,
    tlc_relationship: incoming.is_owned_by_core === false ? 'former_customer' : 'customer',
    university_name: incoming.university_name,
    brand_name: incoming.brand_name,
    developer_name: 'Core Spaces',
    property_url: incoming.property_url,
    year_built: incoming.year_built,
    total_units: incoming.total_units,
    total_beds: incoming.total_beds,
    hero_image_url: incoming.hero_image_url,
    images: incoming.images || [],
    external_ids: ext,
    enrichment_sources,
    source: 'corespaces_prismic',
    source_detail: 'corespaces.com/communities via Prismic CMS',
    data_quality_score: 80,
    last_enrichment_at: new Date().toISOString(),
    notes: incoming.is_owned_by_core === false
      ? 'Core Spaces developed portfolio asset — sold; current owner/PM TBD (press release research queued).'
      : incoming.is_coming_soon
        ? 'Core Spaces coming-soon community (pipeline alignment candidate).'
        : null,
  };
}

async function main() {
  const regUrl = process.env.REGISTRY_IQ_SUPABASE_URL;
  const regKey = process.env.REGISTRY_IQ_SUPABASE_SERVICE_ROLE_KEY;
  if (!regUrl || !regKey) {
    console.error('Missing REGISTRY_IQ_SUPABASE_URL or REGISTRY_IQ_SUPABASE_SERVICE_ROLE_KEY');
    process.exit(1);
  }

  console.log(`Core Spaces Prismic ingest — ${DRY_RUN ? 'DRY-RUN' : 'APPLY'}`);
  const { ref, normalized } = await loadPrismicPortfolio();
  console.log(`  Prismic ref: ${ref}`);
  console.log(`  Properties fetched: ${normalized.length}`);

  let portfolio = normalized;
  if (ONLY) {
    portfolio = portfolio.filter(
      (p) =>
        p.property_name?.toLowerCase().includes(ONLY) ||
        p.city?.toLowerCase().includes(ONLY) ||
        p.university_name?.toLowerCase().includes(ONLY),
    );
    console.log(`  Filter --only=${ONLY}: ${portfolio.length} properties`);
  }
  if (LIMIT) portfolio = portfolio.slice(0, LIMIT);

  stats.fetched = portfolio.length;
  stats.coming_soon = portfolio.filter((p) => p.is_coming_soon).length;
  stats.sold_portfolio = portfolio.filter((p) => !p.is_owned_by_core).length;

  const sb = createClient(regUrl, regKey, { auth: { persistSession: false } });

  const existingRows = [];
  for (let from = 0; ; from += 1000) {
    const { data, error: loadErr } = await sb
      .from('property_registry')
      .select('id, property_name, city, state_province, address_line1, external_ids, enrichment_sources, notes, data_quality_score, source, developer_name, brand_name, property_status, property_url, latitude, longitude, total_units, total_beds, university_name, hero_image_url, images, postal_code, year_built')
      .range(from, from + 999);
    if (loadErr) throw loadErr;
    if (!data?.length) break;
    existingRows.push(...data);
    if (data.length < 1000) break;
  }
  console.log(`  Registry rows loaded for matching: ${existingRows.length}`);

  const byPrismic = new Map();
  const byMatch = new Map();
  const byExact = new Map();
  for (const row of existingRows || []) {
    const pid = row.external_ids?.prismic_id;
    if (pid) byPrismic.set(pid, row);
    const key = `${normalizeForMatch(row.property_name)}|${normalizeForMatch(row.city)}|${(row.state_province || '').toUpperCase()}`;
    if (!byMatch.has(key)) byMatch.set(key, row);
    const exact = `${(row.property_name || '').trim().toLowerCase()}|${(row.city || '').trim().toLowerCase()}|${(row.state_province || '').toUpperCase()}`;
    if (!byExact.has(exact)) byExact.set(exact, row);
  }

  const { data: coreStake } = await sb
    .from('stakeholder_registry')
    .select('id, stakeholder_name')
    .eq('stakeholder_name', 'Core Spaces')
    .limit(1)
    .maybeSingle();

  for (const incoming of portfolio) {
    let existing = byPrismic.get(incoming.prismic_id);
    if (!existing) {
      const exact = `${(incoming.property_name || '').trim().toLowerCase()}|${(incoming.city || '').trim().toLowerCase()}|${(incoming.state_province || '').toUpperCase()}`;
      existing = byExact.get(exact);
    }
    if (!existing) {
      const key = `${normalizeForMatch(incoming.property_name)}|${normalizeForMatch(incoming.city)}|${(incoming.state_province || '').toUpperCase()}`;
      existing = byMatch.get(key);
    }

    async function applyUpdate(existingRow) {
      stats.matched++;
      const patch = buildUpdate(existingRow, incoming);
      if (DRY_RUN) {
        console.log(`  [MATCH] ${incoming.property_name} (${incoming.city}, ${incoming.state_province}) → update ${Object.keys(patch).length} fields`);
        return true;
      }
      const { error } = await sb.from('property_registry').update(patch).eq('id', existingRow.id);
      if (error) {
        console.error(`  ERROR update ${incoming.property_name}:`, error.message);
        stats.errors++;
        return false;
      }
      stats.updated++;
      if (coreStake?.id) {
        const { error: psErr } = await sb.from('property_stakeholders').upsert({
          property_id: existingRow.id,
          stakeholder_id: coreStake.id,
          stakeholder_name: 'Core Spaces',
          role: 'developer',
          is_primary: true,
          notes: incoming.is_owned_by_core === false ? 'Historical developer (asset sold)' : 'Developer per corespaces.com',
        }, { onConflict: 'property_id,role,stakeholder_name', ignoreDuplicates: false });
        if (!psErr) stats.stakeholder_links++;
      }
      return true;
    }

    if (existing) {
      await applyUpdate(existing);
      continue;
    }

    const row = buildInsert(incoming);
    if (DRY_RUN) {
      console.log(`  [NEW] ${incoming.property_name} (${incoming.city}, ${incoming.state_province}) status=${incoming.property_status} brand=${incoming.brand_name}`);
      continue;
    }
    const { data: inserted, error } = await sb.from('property_registry').insert(row).select('id').single();
    if (error?.code === '23505' || error?.message?.includes('unique constraint')) {
      const exact = `${(incoming.property_name || '').trim().toLowerCase()}|${(incoming.city || '').trim().toLowerCase()}|${(incoming.state_province || '').toUpperCase()}`;
      const dupe = byExact.get(exact);
      if (dupe) {
        console.log(`  [DUPE→UPDATE] ${incoming.property_name}`);
        await applyUpdate(dupe);
        continue;
      }
    }
    if (error) {
      console.error(`  ERROR insert ${incoming.property_name}:`, error.message);
      stats.errors++;
    } else {
      stats.inserted++;
      if (coreStake?.id && inserted?.id) {
        await sb.from('property_stakeholders').insert({
          property_id: inserted.id,
          stakeholder_id: coreStake.id,
          stakeholder_name: 'Core Spaces',
          role: 'developer',
          is_primary: true,
          notes: incoming.is_owned_by_core === false ? 'Historical developer (asset sold)' : 'Developer per corespaces.com',
        });
        stats.stakeholder_links++;
      }
    }
  }

  console.log('\nSummary:', JSON.stringify(stats, null, 2));
  writeFileSync(resolve(ROOT, '.firecrawl/corespaces-ingest-report.json'), JSON.stringify(stats, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
