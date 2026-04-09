#!/usr/bin/env node
/**
 * Property Registry — Post-Load Deduplication
 *
 * Merges duplicate property_registry rows caused by:
 *   1. Same name with city=Unknown vs city=known
 *   2. Name suffix variants: (MUR), (TLO), (Millworks)
 *   3. City spelling variants (Ft. vs Fort, Tex vs TX)
 *
 * For each duplicate group, the highest-quality row is kept as the
 * canonical record; others have their data merged in, then are deleted.
 * property_stakeholders referencing deleted rows are re-pointed.
 *
 * Usage:
 *   node scripts/dedup-properties.mjs --dry-run --verbose
 *   node scripts/dedup-properties.mjs
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

const DRY_RUN = process.argv.includes('--dry-run');
const VERBOSE = process.argv.includes('--verbose');

const riq = createClient(
  process.env.REGISTRY_IQ_SUPABASE_URL || '',
  process.env.REGISTRY_IQ_SUPABASE_SERVICE_ROLE_KEY || '',
);

function log(msg) { console.log(`  ${msg}`); }
function heading(msg) { console.log(`\n${'═'.repeat(60)}\n  ${msg}\n${'═'.repeat(60)}`); }

/**
 * Normalize a property name for dedup matching.
 * Strips parenthetical suffixes like (CA), (MUR), (TLO), (Millworks),
 * leading quotes/ticks, trailing whitespace, and known variant spellings.
 */
function dedupeNorm(raw) {
  if (!raw) return '';
  return raw
    .replace(/\r?\n/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .toUpperCase()
    .replace(/^[''"`]+/, '')          // strip leading quotes/ticks
    .replace(/^THE\s+/, '')
    .replace(/\s*\(.*?\)\s*/g, ' ')   // strip all parenthetical content
    .replace(/\s*-\s*(MUR|TLO|MURF?|MILLWORKS?|TOPS?|CABS?)\s*$/i, '')
    .replace(/\s+(MUR|TLO|MURF?|MILLWORKS?)\s*$/i, '')
    .replace(/\s+/g, ' ')
    .trim();
}

/** Normalize a city for matching: Ft./Ft -> Fort, Tex -> TX, etc. */
function normCity(raw) {
  if (!raw || raw === 'Unknown') return '';
  return raw.trim().toUpperCase()
    .replace(/^FT\.?\s+/, 'FORT ')
    .replace(/,?\s*TEX\.?$/, '')
    .replace(/,?\s*TEXAS$/, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function normState(raw) {
  if (!raw || raw === 'Unknown') return '';
  const s = raw.trim().toUpperCase();
  if (s === 'AS') return 'TX'; // "Austn, Tex/AS" -> TX
  return s;
}

async function loadAllProperties() {
  const all = [];
  let from = 0;
  while (true) {
    const { data } = await riq
      .from('property_registry')
      .select('*')
      .range(from, from + 999);
    if (!data?.length) break;
    all.push(...data);
    if (data.length < 1000) break;
    from += 1000;
  }
  return all;
}

function buildDuplicateGroups(properties) {
  const map = new Map();
  for (const p of properties) {
    const normName = dedupeNorm(p.property_name);
    const nc = normCity(p.city);
    const ns = normState(p.state_province);

    // Primary key: normalized name only (city-agnostic)
    // We'll group by name alone, then within each group decide the canonical row
    if (!map.has(normName)) map.set(normName, []);
    map.get(normName).push({ ...p, _normCity: nc, _normState: ns });
  }

  // Only keep groups with >1 member
  return [...map.entries()]
    .filter(([k, v]) => v.length > 1)
    .map(([k, v]) => ({ normName: k, rows: v }));
}

/**
 * Within a group, further split by distinct real locations.
 * "Hampton FYI" in Kearney NE and in Nashville TN are NOT duplicates.
 * But "Hampton FYI" in Nashville TN and "Unknown" ARE.
 */
function splitByLocation(group) {
  const locationBuckets = new Map();

  for (const row of group.rows) {
    const locKey = row._normCity && row._normState
      ? `${row._normCity}|${row._normState}`
      : '';

    // If no location, try to attach to an existing bucket
    if (!locKey) {
      // Mark for later assignment
      if (!locationBuckets.has('__UNKNOWN__')) locationBuckets.set('__UNKNOWN__', []);
      locationBuckets.get('__UNKNOWN__').push(row);
    } else {
      if (!locationBuckets.has(locKey)) locationBuckets.set(locKey, []);
      locationBuckets.get(locKey).push(row);
    }
  }

  // Assign unknown-location rows to existing buckets if there's exactly 1 known location
  const knownLocs = [...locationBuckets.keys()].filter(k => k !== '__UNKNOWN__');
  const unknowns = locationBuckets.get('__UNKNOWN__') || [];

  if (knownLocs.length === 1 && unknowns.length > 0) {
    // All unknowns merge into the single known location
    locationBuckets.get(knownLocs[0]).push(...unknowns);
    locationBuckets.delete('__UNKNOWN__');
  } else if (knownLocs.length === 0 && unknowns.length > 1) {
    // All unknown — they're still duplicates of each other
    // Leave as-is, they'll be merged
  } else if (knownLocs.length > 1 && unknowns.length > 0) {
    // Multiple known locations — unknowns could be any of them
    // Try to match by deal_number or opportunity_code
    for (const unk of unknowns) {
      let matched = false;
      const unkDeal = unk.external_ids?.deal_number || unk.external_ids?.opportunity_code;
      if (unkDeal) {
        for (const loc of knownLocs) {
          const locRows = locationBuckets.get(loc);
          if (locRows.some(r =>
            r.external_ids?.deal_number === unkDeal ||
            r.external_ids?.opportunity_code === unkDeal
          )) {
            locRows.push(unk);
            matched = true;
            break;
          }
        }
      }
      if (!matched) {
        // Can't determine which location — skip merging this unknown
        // (leave it as its own record)
      }
    }
    locationBuckets.delete('__UNKNOWN__');
  }

  // Also merge known-location buckets that differ only by city spelling
  // e.g., "FORT COLLINS|CO" and "FT COLLINS|CO"
  const mergedBuckets = new Map();
  for (const [locKey, rows] of locationBuckets) {
    if (locKey === '__UNKNOWN__') { mergedBuckets.set(locKey, rows); continue; }
    let merged = false;
    for (const [existKey, existRows] of mergedBuckets) {
      if (existKey === '__UNKNOWN__') continue;
      const [eCity, eState] = existKey.split('|');
      const [nCity, nState] = locKey.split('|');
      if (eState === nState && citiesMatch(eCity, nCity)) {
        existRows.push(...rows);
        merged = true;
        break;
      }
    }
    if (!merged) mergedBuckets.set(locKey, rows);
  }

  return [...mergedBuckets.values()].filter(v => v.length > 1);
}

function citiesMatch(a, b) {
  if (a === b) return true;
  // Strip common abbreviation differences
  const na = a.replace(/\./g, '').replace(/\s+/g, ' ').trim();
  const nb = b.replace(/\./g, '').replace(/\s+/g, ' ').trim();
  if (na === nb) return true;
  // One contains the other
  if (na.includes(nb) || nb.includes(na)) return true;
  // Levenshtein-ish: if very similar (>80% overlap)
  if (na.length > 4 && nb.length > 4) {
    const shorter = na.length < nb.length ? na : nb;
    const longer = na.length >= nb.length ? na : nb;
    if (longer.includes(shorter)) return true;
  }
  return false;
}

/**
 * Pick the canonical (keeper) row from a group:
 * highest data_quality_score, then prefer row with real city, then most external_ids.
 */
function pickCanonical(rows) {
  return rows.sort((a, b) => {
    // Prefer known city that isn't an address (doesn't start with a digit)
    const aRealCity = a.city && a.city !== 'Unknown' && !/^\d/.test(a.city) ? 1 : 0;
    const bRealCity = b.city && b.city !== 'Unknown' && !/^\d/.test(b.city) ? 1 : 0;
    if (bRealCity !== aRealCity) return bRealCity - aRealCity;
    // Prefer higher quality
    if (b.data_quality_score !== a.data_quality_score) return b.data_quality_score - a.data_quality_score;
    // Prefer more external_ids
    const aIds = Object.keys(a.external_ids || {}).length;
    const bIds = Object.keys(b.external_ids || {}).length;
    return bIds - aIds;
  })[0];
}

/** Merge donor fields into keeper (fill nulls only). */
function mergeInto(keeper, donor) {
  const updates = {};
  const fillable = [
    'address_line1', 'city', 'state_province', 'postal_code',
    'latitude', 'longitude', 'property_type', 'property_url',
    'total_units', 'total_beds', 'total_buildings', 'total_residential_floors',
    'total_elevators', 'total_parking_spots', 'owner_name', 'developer_name',
    'gc_name', 'architect_name', 'designer_name', 'property_manager_name',
    'property_phone', 'property_email', 'year_built', 'hero_image_url',
    'opening_date', 'construction_start_date', 'brand_name',
  ];

  for (const f of fillable) {
    const kv = keeper[f];
    const dv = donor[f];
    if (dv != null && dv !== '' && dv !== 'Unknown' && dv !== 'TBD' && dv !== '00000') {
      if (kv == null || kv === '' || kv === 'Unknown' || kv === 'TBD' || kv === '00000') {
        updates[f] = dv;
      }
      // Special: if keeper's city looks like an address, prefer donor's real city
      if (f === 'city' && kv && /^\d/.test(kv) && !/^\d/.test(dv)) {
        // Keeper city is actually a street address — move it to address_line1 if that's empty
        if (!keeper.address_line1 || keeper.address_line1 === 'TBD') {
          updates.address_line1 = kv;
        }
        updates.city = dv;
      }
    }
  }

  // Merge external_ids
  const merged = { ...(keeper.external_ids || {}), ...(donor.external_ids || {}) };
  if (JSON.stringify(merged) !== JSON.stringify(keeper.external_ids || {})) {
    updates.external_ids = merged;
  }

  return updates;
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

async function main() {
  console.log(`\nProperty Registry — Post-Load Deduplication`);
  console.log(`Mode: ${DRY_RUN ? 'DRY RUN' : 'LIVE'}\n`);

  heading('Loading properties');
  const properties = await loadAllProperties();
  log(`Loaded ${properties.length} properties`);

  heading('Finding duplicate groups');
  const rawGroups = buildDuplicateGroups(properties);
  log(`${rawGroups.length} name groups with >1 row`);

  // Split each group by location to avoid merging genuinely different properties
  let mergeableSets = [];
  for (const group of rawGroups) {
    const locationSets = splitByLocation(group);
    for (const set of locationSets) {
      mergeableSets.push({ normName: group.normName, rows: set });
    }
  }
  log(`${mergeableSets.length} mergeable sets after location splitting`);

  const totalExcess = mergeableSets.reduce((s, g) => s + g.rows.length - 1, 0);
  log(`Total duplicate rows to remove: ${totalExcess}`);

  if (VERBOSE) {
    log('\nSample merges (first 20):');
    for (const g of mergeableSets.slice(0, 20)) {
      const keeper = pickCanonical(g.rows);
      const donors = g.rows.filter(r => r.id !== keeper.id);
      log(`  KEEP: "${keeper.property_name}" (${keeper.city}/${keeper.state_province}, q=${keeper.data_quality_score})`);
      for (const d of donors) {
        log(`    DEL: "${d.property_name}" (${d.city}/${d.state_province}, q=${d.data_quality_score})`);
      }
    }
  }

  if (DRY_RUN) {
    heading('Summary (DRY RUN)');
    log(`Would merge ${mergeableSets.length} duplicate groups`);
    log(`Would remove ${totalExcess} excess rows`);
    log(`Registry would go from ${properties.length} to ~${properties.length - totalExcess} properties`);
    return;
  }

  heading('Merging duplicates');
  let merged = 0;
  let deleted = 0;
  let stakeholdersRepointed = 0;

  for (const group of mergeableSets) {
    const keeper = pickCanonical(group.rows);
    const donors = group.rows.filter(r => r.id !== keeper.id);

    // Merge data from donors into keeper
    let allUpdates = {};
    for (const donor of donors) {
      const updates = mergeInto({ ...keeper, ...allUpdates }, donor);
      allUpdates = { ...allUpdates, ...updates };
    }

    // Recalculate quality score
    if (Object.keys(allUpdates).length > 0) {
      const mergedProp = { ...keeper, ...allUpdates };
      allUpdates.data_quality_score = calculateQuality(mergedProp);
    }

    // Apply updates to keeper
    if (Object.keys(allUpdates).length > 0) {
      await riq.from('property_registry').update(allUpdates).eq('id', keeper.id);
    }

    // Re-point property_stakeholders from donors to keeper
    for (const donor of donors) {
      const { data: ps } = await riq
        .from('property_stakeholders')
        .select('id,stakeholder_name,role')
        .eq('property_id', donor.id);

      for (const link of (ps || [])) {
        // Check if keeper already has this stakeholder+role
        const { data: existing } = await riq
          .from('property_stakeholders')
          .select('id')
          .eq('property_id', keeper.id)
          .eq('stakeholder_name', link.stakeholder_name)
          .eq('role', link.role)
          .limit(1)
          .single();

        if (existing) {
          await riq.from('property_stakeholders').delete().eq('id', link.id);
        } else {
          await riq.from('property_stakeholders')
            .update({ property_id: keeper.id })
            .eq('id', link.id);
          stakeholdersRepointed++;
        }
      }

      // Delete the donor property
      await riq.from('property_registry').delete().eq('id', donor.id);
      deleted++;
    }

    merged++;
    if (merged % 100 === 0) process.stdout.write(`\r  Processed ${merged} groups...`);
  }

  heading('Summary');
  log(`Groups merged: ${merged}`);
  log(`Rows deleted: ${deleted}`);
  log(`Stakeholder links re-pointed: ${stakeholdersRepointed}`);
  log(`Properties: ${properties.length} -> ${properties.length - deleted}`);

  // Final count verification
  const { count } = await riq.from('property_registry').select('*', { count: 'exact', head: true });
  log(`Verified count: ${count}`);
}

main().catch(e => {
  console.error('FATAL:', e);
  process.exit(1);
});
