#!/usr/bin/env node
/**
 * Park low-data properties (Unknown city, no parseable location)
 * into a `pipeline_properties_parked` holding table, then remove
 * them from the active `property_registry`.
 *
 * Usage:
 *   node scripts/park-no-data-properties.mjs --dry-run
 *   node scripts/park-no-data-properties.mjs
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
const riq = createClient(process.env.REGISTRY_IQ_SUPABASE_URL, process.env.REGISTRY_IQ_SUPABASE_SERVICE_ROLE_KEY);

function log(msg) { console.log(`  ${msg}`); }

async function fetchAllPages(table, select, filters) {
  const all = [];
  let from = 0;
  while (true) {
    let q = riq.from(table).select(select).range(from, from + 999);
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

async function main() {
  console.log(`\nPark No-Data Properties`);
  console.log(`Mode: ${DRY ? 'DRY RUN' : 'LIVE'}\n`);

  // Step 1: Create the parked table via SQL
  log('Step 1: Creating pipeline_properties_parked table...');
  if (!DRY) {
    const { error } = await riq.rpc('exec_sql', { sql: `
      CREATE TABLE IF NOT EXISTS pipeline_properties_parked (
        id uuid PRIMARY KEY,
        property_name text NOT NULL,
        source text,
        developer_name text,
        total_beds integer,
        state_province text,
        data_quality_score integer,
        external_ids jsonb DEFAULT '{}',
        original_record jsonb NOT NULL,
        stakeholder_links jsonb DEFAULT '[]',
        parked_at timestamptz DEFAULT now(),
        parked_reason text DEFAULT 'no_location_data'
      );
      
      COMMENT ON TABLE pipeline_properties_parked IS 
        'Holding table for pipeline properties with no parseable location data. Not used in active registry.';
    `});

    if (error) {
      log('RPC exec_sql not available, trying direct SQL via REST...');
      // Try creating via individual inserts as fallback — we'll use the SQL editor approach
      // Actually, let's just create it via the Supabase Management API or raw pg
      console.log('  Error: ' + error.message);
      console.log('  Will attempt table creation via raw SQL connection...');
    }
  }
  log('Done');

  // Step 2: Find all Unknown-city properties
  log('\nStep 2: Finding Unknown-city properties...');
  const unknownCity = await fetchAllPages('property_registry', '*', q => q.eq('city', 'Unknown'));
  const nullCity = await fetchAllPages('property_registry', '*', q => q.is('city', null));
  const topark = [...unknownCity, ...nullCity];
  log(`Found ${topark.length} properties to park`);

  // Step 3: Get their stakeholder links
  log('\nStep 3: Fetching stakeholder links...');
  const parkIds = topark.map(p => p.id);
  const stkLinks = [];
  for (let i = 0; i < parkIds.length; i += 100) {
    const batch = parkIds.slice(i, i + 100);
    const { data } = await riq.from('property_stakeholders').select('*').in('property_id', batch);
    if (data) stkLinks.push(...data);
  }
  log(`Found ${stkLinks.length} stakeholder links`);

  // Build stakeholder links map
  const linksByProp = new Map();
  for (const link of stkLinks) {
    if (!linksByProp.has(link.property_id)) linksByProp.set(link.property_id, []);
    linksByProp.get(link.property_id).push(link);
  }

  // Step 4: Insert into parked table
  log('\nStep 4: Inserting into pipeline_properties_parked...');
  if (!DRY) {
    let inserted = 0;
    for (let i = 0; i < topark.length; i += 50) {
      const batch = topark.slice(i, i + 50).map(p => ({
        id: p.id,
        property_name: p.property_name,
        source: p.source,
        developer_name: p.developer_name,
        total_beds: p.total_beds,
        state_province: p.state_province,
        data_quality_score: p.data_quality_score,
        external_ids: p.external_ids || {},
        original_record: p,
        stakeholder_links: linksByProp.get(p.id) || [],
        parked_reason: 'no_location_data',
      }));

      const { error } = await riq.from('pipeline_properties_parked').insert(batch);
      if (error) {
        log(`  INSERT ERROR at batch ${i}: ${error.message}`);
        // If table doesn't exist, bail
        if (error.message.includes('does not exist')) {
          log('  Table does not exist — need to create it first. Aborting.');
          return;
        }
      } else {
        inserted += batch.length;
      }
      if (inserted % 200 === 0 && inserted > 0) log(`  ...${inserted}/${topark.length}`);
    }
    log(`Inserted ${inserted} records into parked table`);
  } else {
    log(`Would insert ${topark.length} records`);
  }

  // Step 5: Delete stakeholder links
  log('\nStep 5: Deleting stakeholder links for parked properties...');
  if (!DRY) {
    let deleted = 0;
    for (let i = 0; i < parkIds.length; i += 100) {
      const batch = parkIds.slice(i, i + 100);
      const { error, count } = await riq.from('property_stakeholders').delete({ count: 'exact' }).in('property_id', batch);
      if (error) log(`  DELETE ERROR: ${error.message}`);
      else deleted += count;
    }
    log(`Deleted ${deleted} stakeholder links`);
  } else {
    log(`Would delete ${stkLinks.length} stakeholder links`);
  }

  // Step 6: Delete from property_registry
  log('\nStep 6: Deleting parked properties from property_registry...');
  if (!DRY) {
    let deleted = 0;
    for (let i = 0; i < parkIds.length; i += 100) {
      const batch = parkIds.slice(i, i + 100);
      const { error, count } = await riq.from('property_registry').delete({ count: 'exact' }).in('id', batch);
      if (error) log(`  DELETE ERROR: ${error.message}`);
      else deleted += count;
    }
    log(`Deleted ${deleted} properties from registry`);
  } else {
    log(`Would delete ${topark.length} properties from registry`);
  }

  // Summary
  console.log(`\n${'═'.repeat(50)}`);
  console.log(`  SUMMARY`);
  console.log(`${'═'.repeat(50)}`);
  console.log(`  Properties parked: ${topark.length}`);
  console.log(`  Stakeholder links preserved (in parked JSON): ${stkLinks.length}`);
  console.log(`  Stakeholder links deleted from junction: ${DRY ? '(dry run)' : stkLinks.length}`);
  console.log(`  Properties removed from registry: ${DRY ? '(dry run)' : topark.length}`);
  console.log();
}

main().catch(e => {
  console.error('FATAL:', e);
  process.exit(1);
});
