#!/usr/bin/env node
/**
 * Hold inbound containers whose destination is not a known Registry property site
 * or approved warehouse.
 *
 * Writes to Chain-iQ container_loads:
 *   status2 = HOLD:UNKNOWN_DEST
 *   note    = prepended audit line
 *
 * Usage:
 *   node scripts/enforce-container-destination-guard.mjs --dry-run
 *   node scripts/enforce-container-destination-guard.mjs --apply
 *   node scripts/enforce-container-destination-guard.mjs --dry-run --project "Bloomington"
 */

import { createClient } from '@supabase/supabase-js';
import { config } from 'dotenv';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';
import {
  CANONICAL_WAREHOUSES,
  HOLD_STATUS2,
  buildPropertyKeys,
  buildWarehouseKeys,
  destinationKey,
  evaluateContainerDestination,
  holdNote,
  norm,
  projectMatchTerms,
} from './lib/container-destination-guard.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const envPaths = [
  resolve(__dirname, '..', '.env.local'),
  resolve(__dirname, '..', '.env'),
  resolve(__dirname, '../../Derived State/dale-chat/.env.local'),
  resolve(__dirname, '../../Derived State/dale-chat/.env'),
];
for (const p of envPaths) {
  config({ path: p, override: true });
}

const APPLY = process.argv.includes('--apply');
const projectFilter = (() => {
  const i = process.argv.indexOf('--project');
  return i >= 0 ? process.argv[i + 1] : null;
})();

const regUrl = process.env.REGISTRY_IQ_SUPABASE_URL;
const regKey = process.env.REGISTRY_IQ_SUPABASE_SERVICE_ROLE_KEY;
const chainUrl = process.env.CHAIN_IQ_SUPABASE_URL;
const chainKey = process.env.CHAIN_IQ_SUPABASE_SERVICE_ROLE_KEY;

if (!regUrl || !regKey || !chainUrl || !chainKey) {
  console.error('Missing REGISTRY_IQ_* or CHAIN_IQ_* credentials');
  process.exit(1);
}

const reg = createClient(regUrl, regKey);
const chain = createClient(chainUrl, chainKey);

async function fetchAll(db, table, select) {
  const rows = [];
  const page = 1000;
  for (let from = 0; ; from += page) {
    const { data, error } = await db.from(table).select(select).range(from, from + page - 1);
    if (error) throw new Error(`${table}: ${error.message}`);
    if (!data?.length) break;
    rows.push(...data);
    if (data.length < page) break;
  }
  return rows;
}

async function loadContext() {
  const globalAllowedKeys = new Set(
    CANONICAL_WAREHOUSES.flatMap((w) => buildWarehouseKeys(w)),
  );

  let warehouses = [];
  const { data: wh, error: whErr } = await reg.from('warehouse_registry').select('*');
  if (!whErr && wh?.length) {
    warehouses = wh;
    for (const w of wh) for (const k of buildWarehouseKeys(w)) globalAllowedKeys.add(k);
  }

  const projects = await fetchAll(
    reg,
    'project_registry',
    'id, project_id, project_name, property_id, normalized_name',
  );
  const properties = await fetchAll(
    reg,
    'property_registry',
    'id, property_name, address_line1, city, state_province, postal_code',
  );

  const propById = new Map((properties || []).map((p) => [p.id, p]));
  const whById = new Map(warehouses.map((w) => [w.id, w]));

  const projectByName = new Map();
  for (const p of projects || []) {
    const allowedKeys = new Set();
    const prop = p.property_id ? propById.get(p.property_id) : null;
    if (prop) for (const k of buildPropertyKeys(prop)) allowedKeys.add(k);
    const whId = p.warehouse_registry_id;
    const whRow = whId ? whById.get(whId) : null;
    if (whRow) for (const k of buildWarehouseKeys(whRow)) allowedKeys.add(k);

    const entry = { ...p, allowedKeys: [...allowedKeys] };
    projectByName.set(norm(p.project_name), entry);
    projectByName.set(norm(p.normalized_name), entry);
    projectByName.set(norm(p.project_id), entry);
  }

  return { globalAllowedKeys, projectByName, warehouseCount: warehouses.length };
}

async function fetchContainers() {
  const rows = [];
  const page = 1000;
  for (let from = 0; ; from += page) {
    let q = chain
      .from('container_loads')
      .select(
        'id, container_number, project_name, destination, final_destination, status, status2, note, vendor, updated_at',
      )
      .range(from, from + page - 1);
    if (projectFilter) q = q.ilike('project_name', `%${projectFilter}%`);
    const { data, error } = await q;
    if (error) throw new Error(error.message);
    if (!data?.length) break;
    rows.push(...data);
    if (data.length < page) break;
  }
  return rows;
}

async function main() {
  console.log(`Container destination guard — ${APPLY ? 'APPLY' : 'DRY RUN'}`);
  const ctx = await loadContext();
  console.log(
    `Known warehouses: ${CANONICAL_WAREHOUSES.length} canonical + ${ctx.warehouseCount} registry rows`,
  );
  console.log(`Projects indexed: ${ctx.projectByName.size}`);

  const containers = await fetchContainers();
  console.log(`Scanned container_loads: ${containers.length}`);

  const violations = [];
  let skipped = 0;
  let ok = 0;

  for (const row of containers) {
    const result = evaluateContainerDestination(row, ctx);
    if (result.skipped) {
      skipped++;
      continue;
    }
    if (result.ok) {
      ok++;
      continue;
    }
    violations.push({ row, result });
  }

  console.log(`OK: ${ok} | skipped: ${skipped} | HOLD candidates: ${violations.length}`);

  for (const { row, result } of violations.slice(0, 40)) {
    console.log(
      [
        row.container_number || '(no CN)',
        row.status,
        row.destination,
        row.project_name?.slice(0, 50),
        result.reason,
      ].join(' | '),
    );
  }
  if (violations.length > 40) console.log(`… and ${violations.length - 40} more`);

  if (!APPLY) {
    if (violations.length) {
      console.log('\nRe-run with --apply to set status2=HOLD:UNKNOWN_DEST on these rows.');
    }
    return;
  }

  let held = 0;
  for (const { row, result } of violations) {
    const prefix = holdNote(result.reason);
    const note = row.note ? `${prefix}\n${row.note}` : prefix;
    const { error } = await chain
      .from('container_loads')
      .update({ status2: HOLD_STATUS2, note })
      .eq('id', row.id);
    if (error) {
      console.warn('Update failed', row.id, error.message);
    } else {
      held++;
    }
  }
  console.log(`Applied hold on ${held} containers.`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
