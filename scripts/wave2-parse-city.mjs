#!/usr/bin/env node
/**
 * Wave 2 — Phase A2: Parse city/state from property names
 *
 * Uses a known-cities lookup table (from pipeline_opportunities & install_schedules)
 * and regex patterns to extract city/state from property names that embed location info.
 *
 * Usage:
 *   node scripts/wave2-parse-city.mjs --dry-run
 *   node scripts/wave2-parse-city.mjs
 */

import { createClient } from '@supabase/supabase-js';
import { readFileSync, existsSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

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

const riq = createClient(process.env.REGISTRY_IQ_SUPABASE_URL, process.env.REGISTRY_IQ_SUPABASE_SERVICE_ROLE_KEY);
const dd  = createClient(process.env.DALE_DEMAND_SUPABASE_URL, process.env.DALE_DEMAND_SUPABASE_KEY);

const STATE_SET = new Set('AL,AK,AZ,AR,CA,CO,CT,DE,FL,GA,HI,ID,IL,IN,IA,KS,KY,LA,ME,MD,MA,MI,MN,MS,MO,MT,NE,NV,NH,NJ,NM,NY,NC,ND,OH,OK,OR,PA,RI,SC,SD,TN,TX,UT,VT,VA,WA,WV,WI,WY'.split(','));

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

async function fetchAllPages(client, table, select, filters) {
  const all = [];
  let from = 0;
  while (true) {
    let q = client.from(table).select(select).range(from, from + 999);
    if (filters) q = filters(q);
    const { data, error } = await q;
    if (error) { console.error(`Error fetching ${table}:`, error.message); break; }
    if (!data?.length) break;
    all.push(...data);
    if (data.length < 1000) break;
    from += 1000;
  }
  return all;
}

function isProperCity(s) {
  if (!s) return false;
  const t = s.trim();
  if (/^\d/.test(t)) return false;
  if (t.length < 3) return false;
  if (t.includes('#')) return false;
  if (/\b(Ave|St|Rd|Dr|Blvd|Ln|Ct|Pl|Way|Pkwy|Hwy|Trail|Circle|Loop|Court)\b/i.test(t)) return false;
  if (/^(Unknown|TBD|N\/A)$/i.test(t)) return false;
  return true;
}

async function main() {
  console.log(`\nWave 2 — Phase A2: Parse city/state from property names`);
  console.log(`Mode: ${DRY ? 'DRY RUN' : 'LIVE'}\n`);

  // 1. Build known-cities lookup from pipeline + install_schedules
  console.log('Building known-cities lookup...');
  const poWithCity = await fetchAllPages(dd, 'pipeline_opportunities',
    'project_city,project_state',
    q => q.not('project_city', 'is', null));

  const isWithCity = await fetchAllPages(dd, 'install_schedules',
    'city,state',
    q => q.not('city', 'is', null));

  const regWithCity = await fetchAllPages(riq, 'property_registry',
    'city,state_province',
    q => q.not('city', 'eq', 'Unknown'));

  // Build city→state map (prefer multi-word cities, require proper names)
  const cityStateMap = new Map();
  for (const r of poWithCity) {
    const city = r.project_city?.trim();
    const state = r.project_state?.trim()?.toUpperCase();
    if (city && state && isProperCity(city) && STATE_SET.has(state)) {
      cityStateMap.set(city.toUpperCase(), { city, state });
    }
  }
  for (const r of isWithCity) {
    const city = r.city?.trim();
    const state = r.state?.trim()?.toUpperCase();
    if (city && state && isProperCity(city) && STATE_SET.has(state)) {
      if (!cityStateMap.has(city.toUpperCase())) {
        cityStateMap.set(city.toUpperCase(), { city, state });
      }
    }
  }
  for (const r of regWithCity) {
    const city = r.city?.trim();
    const state = r.state_province?.trim()?.toUpperCase();
    if (city && state && isProperCity(city) && STATE_SET.has(state)) {
      if (!cityStateMap.has(city.toUpperCase())) {
        cityStateMap.set(city.toUpperCase(), { city, state });
      }
    }
  }

  // Filter out cities that are also common words/brand names to prevent false matches
  const falsePositiveCities = new Set([
    'WILLIAMS', 'AUSTIN', 'MADISON', 'EDEN', 'SUMMIT', 'TEMPLE', 'HOPE',
    'UNION', 'LIBERTY', 'GRACE', 'INDEPENDENCE', 'FOREST', 'PARK',
    'VISTA', 'SPRINGS', 'HARBOR', 'HEIGHTS', 'COLLEGE', 'UNIVERSITY',
    'CENTURY', 'NATIONAL', 'ROYAL', 'IMPERIAL', 'GRAND', 'GOLDEN',
    'CLINTON', 'JACKSON', 'TYLER', 'TAYLOR', 'BAKER', 'LINCOLN',
    'MONROE', 'MARSHALL', 'HAMILTON', 'FRANKLIN', 'HARRISON',
    'ARCHER', 'BELL', 'DOUGLAS', 'REED', 'BARRY', 'CLAY', 'RAY',
  ]);

  // Sort by city name length descending so longer (more specific) matches take priority
  const cityEntries = [...cityStateMap.entries()]
    .filter(([k]) => !falsePositiveCities.has(k))
    .sort((a, b) => b[0].length - a[0].length);

  console.log(`Known cities: ${cityStateMap.size} (${cityEntries.length} after filtering false positives)`);

  // 2. Load Unknown-city properties (all sources, not just pipeline)
  const allReg = await fetchAllPages(riq, 'property_registry',
    'id,property_name,city,state_province,address_line1,postal_code,property_type,property_url,total_units,total_beds,owner_name,developer_name,hero_image_url,latitude,data_quality_score');

  const unknownCity = allReg.filter(p => !p.city || p.city === 'Unknown');
  console.log(`Properties with Unknown city: ${unknownCity.length}`);

  // 3. Parse city/state from names
  const updates = [];

  for (const prop of unknownCity) {
    const name = prop.property_name;
    const nameUpper = name.toUpperCase();
    let city = null, state = null;

    // Priority 1: Explicit "City, ST" in the name after a separator (dash, comma, colon)
    // Match '- City, ST' or '- City Name, ST' (including abbreviated like Ft.)
    const csAfterSep = name.match(/[-–:]\s+([A-Z][a-z]*\.?\s?(?:[A-Z][a-z]+\.?\s?){0,3}),\s*([A-Z]{2})\b/);
    if (csAfterSep && STATE_SET.has(csAfterSep[2])) {
      city = csAfterSep[1].trim();
      state = csAfterSep[2];
    }

    // Priority 1b: "CITY, ST" in all-caps names after separator
    if (!city) {
      const capsMatch = nameUpper.match(/[-–:,]\s+([A-Z][A-Z .]+?),\s*([A-Z]{2})\b/);
      if (capsMatch && STATE_SET.has(capsMatch[2])) {
        const rawCity = capsMatch[1].trim();
        city = rawCity.split(/\s+/).map(w => w[0] + w.slice(1).toLowerCase()).join(' ');
        state = capsMatch[2];
      }
    }

    // Priority 1c: "Brand CityName, ST" — no separator, but comma+state at end
    if (!city) {
      const endMatch = name.match(/([A-Z][a-z]*\.?\s?(?:[A-Z][a-z]+\.?\s?){0,2}),\s*([A-Z]{2})\s*(?:[-–]|$)/);
      if (endMatch && STATE_SET.has(endMatch[2])) {
        city = endMatch[1].trim();
        state = endMatch[2];
      }
    }

    // Priority 2: Known city lookup (word boundary matching)
    if (!city) {
      for (const [cityUpper, data] of cityEntries) {
        // Require word boundary: the city name must appear as a standalone segment
        const re = new RegExp('\\b' + cityUpper.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '\\b', 'i');
        if (re.test(nameUpper)) {
          city = data.city;
          state = data.state;
          break;
        }
      }
    }

    // Priority 3: State code only (", ST" or "- ST -" pattern)
    if (!city && !state) {
      const stateMatch = name.match(/[,\s-]\s*([A-Z]{2})(?:\s|$|-|,)/);
      if (stateMatch && STATE_SET.has(stateMatch[1])) {
        state = stateMatch[1];
      }
    }

    if (city || state) {
      const upd = {};
      if (city) upd.city = city;
      if (state && (!prop.state_province || prop.state_province === 'Unknown')) upd.state_province = state;

      if (Object.keys(upd).length > 0) {
        const merged = { ...prop, ...upd };
        upd.data_quality_score = calculateQuality(merged);
        updates.push({ id: prop.id, updates: upd, name: prop.property_name, parsed: `${city || '?'}, ${state || '?'}` });
      }
    }
  }

  console.log(`\nParseable: ${updates.length} properties`);
  const withCity = updates.filter(u => u.updates.city);
  const stateOnly = updates.filter(u => !u.updates.city && u.updates.state_province);
  console.log(`  With city: ${withCity.length}`);
  console.log(`  State only: ${stateOnly.length}`);

  if (VERBOSE) {
    console.log('\nSamples (city+state):');
    for (const u of withCity.slice(0, 20)) {
      console.log(`  ${u.name} -> ${u.parsed}`);
    }
    console.log('\nSamples (state only):');
    for (const u of stateOnly.slice(0, 10)) {
      console.log(`  ${u.name} -> ${u.parsed}`);
    }
  }

  // 4. Apply updates
  if (!DRY && updates.length > 0) {
    console.log('\nApplying updates...');
    let applied = 0;
    for (const { id, updates: upd } of updates) {
      const { error } = await riq.from('property_registry').update(upd).eq('id', id);
      if (!error) applied++;
      if (applied % 200 === 0) console.log(`  ...${applied}/${updates.length}`);
    }
    console.log(`Applied ${applied} updates`);
  }

  // 5. Summary
  const remaining = unknownCity.length - updates.length;
  console.log(`\n${'═'.repeat(50)}`);
  console.log(`  SUMMARY`);
  console.log(`${'═'.repeat(50)}`);
  console.log(`  Total Unknown-city properties: ${unknownCity.length}`);
  console.log(`  Parsed city: ${withCity.length}`);
  console.log(`  Parsed state only: ${stateOnly.length}`);
  console.log(`  Still unknown: ${remaining}`);
  console.log();
}

main().catch(e => {
  console.error('FATAL:', e);
  process.exit(1);
});
