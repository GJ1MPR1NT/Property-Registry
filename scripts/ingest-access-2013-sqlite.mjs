#!/usr/bin/env node
/**
 * Load 2013 Access export (SQLite) into Registry-iQ — properties, projects, unit types,
 * BOM lines, units, and project_install_days.
 *
 * Prerequisites:
 *   - Run scripts/migration-access-2013-historical.sql on Registry-iQ.
 *
 * Resolution order (per Access Project row):
 *   1) Optional mapping override (project_registry_id and/or property_id)
 *   2) project_registry where legacy_access_project_id = ProjectID
 *   3) project_registry where project_id = parsed deal (YY-NNN)
 *   4) property_registry where external_ids.access_2013.legacy_project_id matches
 *   5) property_registry where external_ids.deal_number = deal (if parsed)
 *   6) Create property stub + project row (--auto-create, default on for --apply)
 *
 * Usage:
 *   node scripts/ingest-access-2013-sqlite.mjs --dry-run
 *   node scripts/ingest-access-2013-sqlite.mjs --apply --auto-create
 *   node scripts/ingest-access-2013-sqlite.mjs --apply --mapping=./Access/access-2013-mapping.json
 *   node scripts/ingest-access-2013-sqlite.mjs --apply --no-auto-create --mapping=...
 *
 * Env: REGISTRY_IQ_SUPABASE_URL, REGISTRY_IQ_SUPABASE_SERVICE_ROLE_KEY
 */

import { createClient } from '@supabase/supabase-js';
import { config } from 'dotenv';
import { readFileSync, existsSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';
import { spawnSync } from 'child_process';

const __dirname = dirname(fileURLToPath(import.meta.url));

for (const envFile of ['.env.local', '.env']) {
  config({ path: resolve(__dirname, '..', envFile) });
  config({ path: resolve(__dirname, '../../Derived State/dale-chat', envFile) });
}

const argv = process.argv.slice(2);
const DRY = !argv.includes('--apply');
const AUTO_CREATE = !argv.includes('--no-auto-create');
const dbPath = argv.find((a) => a.startsWith('--db='))?.split('=')[1]
  ?? resolve(__dirname, '../Access/2013SQLite.db');
const mappingPath = argv.find((a) => a.startsWith('--mapping='))?.split('=')[1];
const minLegacy = parseInt(argv.find((a) => a.startsWith('--min-legacy='))?.split('=')[1] ?? '46', 10);

const registryUrl = process.env.REGISTRY_IQ_SUPABASE_URL;
const registryKey = process.env.REGISTRY_IQ_SUPABASE_SERVICE_ROLE_KEY;

function sqliteQuery(sql) {
  const py = `
import sqlite3, json, sys
sql = sys.stdin.read()
conn = sqlite3.connect(sys.argv[1])
conn.row_factory = sqlite3.Row
rows = conn.execute(sql).fetchall()
print(json.dumps([dict(r) for r in rows]))
`;
  const r = spawnSync('python3', ['-c', py, dbPath], {
    input: sql,
    encoding: 'utf8',
    maxBuffer: 32 * 1024 * 1024,
  });
  if (r.status !== 0) {
    throw new Error(r.stderr || r.stdout || 'sqlite query failed');
  }
  return JSON.parse(r.stdout);
}

function parseDealCode(description) {
  if (!description || typeof description !== 'string') return null;
  const m = description.match(/\b(\d{2}-\d{3})\b/);
  return m ? m[1] : null;
}

/** Display name: strip leading YY-NNN token from Access Description. */
function propertyNameFromDescription(description, deal, fallbackPid) {
  if (!description || typeof description !== 'string') return `Access project ${fallbackPid}`;
  let s = description.trim();
  if (deal) {
    s = s.replace(new RegExp(`^\\s*${deal.replace(/-/g, '\\-')}\\s+`, 'i'), '').trim();
    s = s.replace(new RegExp(`\\b${deal.replace(/-/g, '\\-')}\\b`, 'gi'), ' ').replace(/\s+/g, ' ').trim();
  }
  return s.length >= 2 ? s : description.trim();
}

function classifyDayType(isoDateStr, unitCount) {
  const d = new Date(`${isoDateStr}T12:00:00Z`);
  const dow = d.getUTCDay();
  const isWeekend = dow === 0 || dow === 6;
  const n = unitCount || 0;
  if (n > 0) return isWeekend ? 'install_weekend' : 'install_weekday';
  if (isWeekend) return 'other';
  return 'down_weekday';
}

function skuFromItem(row) {
  const s = (row.ItemSKU ?? '').toString().trim();
  if (s) return s;
  return `ACCESS-${row.ProjectID}-${row.ItemID}`;
}

/** Parse mapping value: string = project_registry_id, object = overrides. */
function parseMappingEntry(val) {
  if (val == null) return {};
  if (typeof val === 'string') return { project_registry_id: val };
  if (typeof val === 'object') {
    return {
      project_registry_id: val.project_registry_id,
      property_id: val.property_id,
    };
  }
  return {};
}

/**
 * Resolve or create property + project_registry row.
 * @returns {{ propertyId: string, projectRegistryId: string, plan: object }}
 */
async function resolvePropertyAndProject(reg, accessProject, pid, deal, unitCountForProject, override, dryRun) {
  const desc = accessProject.Description ?? '';
  const displayName = propertyNameFromDescription(desc, deal, pid);
  const plan = { steps: [], created_property: false, created_project: false };

  const o = parseMappingEntry(override);

  if (o.project_registry_id) {
    const { data: pr, error } = await reg
      .from('project_registry')
      .select('id, property_id, project_id')
      .eq('id', o.project_registry_id)
      .maybeSingle();
    if (error || !pr?.property_id) {
      throw new Error(`Mapping project_registry_id ${o.project_registry_id}: ${error?.message ?? 'missing property_id'}`);
    }
    plan.steps.push(`use mapping → project_registry ${pr.id}`);
    if (o.property_id && o.property_id !== pr.property_id) {
      console.warn(`  Warning: mapping property_id differs from project row; using row’s property_id`);
    }
    return { propertyId: pr.property_id, projectRegistryId: pr.id, plan };
  }

  if (o.property_id) {
    let projRow = null;
    if (deal) {
      const { data } = await reg
        .from('project_registry')
        .select('id, property_id, project_id')
        .eq('property_id', o.property_id)
        .eq('project_id', deal)
        .maybeSingle();
      projRow = data;
    }
    if (!projRow) {
      const { data: byLegacy } = await reg
        .from('project_registry')
        .select('id, property_id, project_id')
        .eq('legacy_access_project_id', pid)
        .eq('property_id', o.property_id)
        .maybeSingle();
      projRow = byLegacy;
    }

    if (projRow) {
      plan.steps.push(`mapping property_id + existing project ${projRow.id}`);
      return { propertyId: projRow.property_id, projectRegistryId: projRow.id, plan };
    }

    if (!AUTO_CREATE) {
      throw new Error(`No project_registry for deal ${deal ?? '?'} under property ${o.property_id}; use --auto-create or insert manually`);
    }

    plan.steps.push(`create project under mapped property ${o.property_id}`);
    if (dryRun) {
      return { propertyId: o.property_id, projectRegistryId: 'dry-run-new-project', plan };
    }

    const { data: createdProj, error: cErr } = await reg
      .from('project_registry')
      .insert(buildProjectInsert(accessProject, pid, deal, o.property_id))
      .select('id')
      .single();
    if (cErr) throw new Error(`project_registry insert: ${cErr.message}`);
    plan.created_project = true;
    return { propertyId: o.property_id, projectRegistryId: createdProj.id, plan };
  }

  const { data: byLegacyProj } = await reg
    .from('project_registry')
    .select('id, property_id, project_id')
    .eq('legacy_access_project_id', pid)
    .maybeSingle();
  if (byLegacyProj?.property_id) {
    plan.steps.push(`found project_registry by legacy_access_project_id`);
    return { propertyId: byLegacyProj.property_id, projectRegistryId: byLegacyProj.id, plan };
  }

  if (deal) {
    const { data: byDealProj } = await reg
      .from('project_registry')
      .select('id, property_id, project_id')
      .eq('project_id', deal)
      .maybeSingle();
    if (byDealProj?.property_id) {
      plan.steps.push(`found project_registry by project_id=${deal}`);
      return { propertyId: byDealProj.property_id, projectRegistryId: byDealProj.id, plan };
    }
  }

  const { data: byExtProp } = await reg
    .from('property_registry')
    .select('id')
    .filter('external_ids->access_2013->>legacy_project_id', 'eq', String(pid))
    .maybeSingle();
  if (byExtProp?.id) {
    plan.steps.push(`found property_registry by external_ids.access_2013.legacy_project_id`);
    return await ensureProjectForProperty(reg, accessProject, pid, deal, byExtProp.id, unitCountForProject, plan, dryRun);
  }

  if (deal) {
    const { data: byDealProp } = await reg
      .from('property_registry')
      .select('id')
      .filter('external_ids->>deal_number', 'eq', deal)
      .maybeSingle();
    if (byDealProp?.id) {
      plan.steps.push(`found property_registry by external_ids.deal_number`);
      return await ensureProjectForProperty(reg, accessProject, pid, deal, byDealProp.id, unitCountForProject, plan, dryRun);
    }
  }

  if (!AUTO_CREATE) {
    throw new Error(
      `No match for Access ProjectID ${pid} (deal ${deal ?? 'n/a'}); create property/project manually or use --auto-create`,
    );
  }

  plan.steps.push('create property stub + project');
  if (dryRun) {
    return { propertyId: 'dry-run-new-property', projectRegistryId: 'dry-run-new-project', plan };
  }

  const propertyRow = buildPropertyInsert(accessProject, pid, deal, displayName, unitCountForProject);
  const { data: newProp, error: pErr } = await reg.from('property_registry').insert(propertyRow).select('id').single();
  if (pErr) throw new Error(`property_registry insert: ${pErr.message}`);
  plan.created_property = true;

  const { data: newProj, error: jErr } = await reg
    .from('project_registry')
    .insert(buildProjectInsert(accessProject, pid, deal, newProp.id))
    .select('id')
    .single();
  if (jErr) throw new Error(`project_registry insert: ${jErr.message}`);
  plan.created_project = true;

  return { propertyId: newProp.id, projectRegistryId: newProj.id, plan };
}

function buildPropertyInsert(accessProject, pid, deal, displayName, unitCount) {
  const ext = {
    access_2013: {
      legacy_project_id: pid,
      deal_number: deal,
      raw_description: accessProject.Description ?? null,
      frozen: accessProject.Frozen ?? null,
    },
  };
  if (deal) ext.deal_number = deal;

  return {
    property_name: displayName.slice(0, 500),
    property_type: 'student_housing',
    property_status: 'active',
    tlc_relationship: 'customer',
    address_line1: 'TBD',
    city: 'Unknown',
    state_province: 'Unknown',
    postal_code: '00000',
    country: 'US',
    total_units: unitCount > 0 ? unitCount : null,
    external_ids: ext,
    source: 'access_2013_import',
    notes: 'Stub from Access 2013 SQLite — enrich address / geo before production use.',
  };
}

function buildProjectInsert(accessProject, pid, deal, propertyId) {
  const projectIdStr = deal ?? `access-2013-${pid}`;
  return {
    project_id: projectIdStr,
    property_id: propertyId,
    legacy_access_project_id: pid,
    access_project_description: accessProject.Description ?? null,
    access_creation_date: accessProject.CreationDate ?? null,
    install_phase: 1,
    fulfillment_mode: 'install',
    external_ids: {
      access_2013: true,
      source: 'access_2013_sqlite',
      pull_list_comment: accessProject.PullListComment ?? null,
    },
  };
}

async function ensureProjectForProperty(reg, accessProject, pid, deal, propertyId, unitCount, plan, dryRun) {
  if (deal) {
    const { data: existing } = await reg
      .from('project_registry')
      .select('id')
      .eq('property_id', propertyId)
      .eq('project_id', deal)
      .maybeSingle();
    if (existing?.id) {
      plan.steps.push(`existing project_registry for property + deal`);
      return { propertyId, projectRegistryId: existing.id, plan };
    }
  }

  const { data: byLegacy } = await reg
    .from('project_registry')
    .select('id')
    .eq('legacy_access_project_id', pid)
    .eq('property_id', propertyId)
    .maybeSingle();
  if (byLegacy?.id) {
    plan.steps.push(`project_registry by legacy + property`);
    return { propertyId, projectRegistryId: byLegacy.id, plan };
  }

  if (!AUTO_CREATE) {
    throw new Error(`Property ${propertyId} exists but no project_registry for Access ProjectID ${pid}; use --auto-create`);
  }

  plan.steps.push('create project_registry for existing property');
  if (dryRun) {
    return { propertyId, projectRegistryId: 'dry-run-new-project', plan };
  }

  const { data: created, error } = await reg
    .from('project_registry')
    .insert(buildProjectInsert(accessProject, pid, deal, propertyId))
    .select('id')
    .single();
  if (error) throw new Error(`project_registry insert: ${error.message}`);
  plan.created_project = true;
  return { propertyId, projectRegistryId: created.id, plan };
}

async function main() {
  if (!existsSync(dbPath)) {
    console.error(`SQLite file not found: ${dbPath}`);
    process.exit(1);
  }

  console.log(`Access 2013 ingest (${DRY ? 'DRY-RUN' : 'APPLY'})  auto-create=${AUTO_CREATE}`);
  console.log(`DB: ${dbPath}\n`);

  const projects = sqliteQuery('SELECT * FROM Project ORDER BY ProjectID');
  const unitTypes = sqliteQuery('SELECT * FROM UnitType');
  const items = sqliteQuery('SELECT * FROM Item');
  const reqs = sqliteQuery('SELECT * FROM UnitTypeItemRequirement');
  const unitNums = sqliteQuery('SELECT * FROM UnitNumber');
  const days = sqliteQuery('SELECT * FROM Day');

  const itemsByProj = new Map();
  for (const it of items) {
    const k = it.ProjectID;
    if (!itemsByProj.has(k)) itemsByProj.set(k, new Map());
    itemsByProj.get(k).set(it.ItemID, it);
  }

  const utByProj = new Map();
  for (const ut of unitTypes) {
    if (!utByProj.has(ut.ProjectID)) utByProj.set(ut.ProjectID, new Map());
    utByProj.get(ut.ProjectID).set(ut.UnitTypeID, ut);
  }

  const unitsPerDay = new Map();
  for (const un of unitNums) {
    if (un.DayID == null) continue;
    const key = `${un.ProjectID}|${un.DayID}`;
    unitsPerDay.set(key, (unitsPerDay.get(key) || 0) + 1);
  }

  const unitsPerProject = new Map();
  for (const un of unitNums) {
    const k = un.ProjectID;
    unitsPerProject.set(k, (unitsPerProject.get(k) || 0) + 1);
  }

  const realProjects = projects.filter((p) => p.ProjectID >= minLegacy);
  console.log(`Projects in file: ${projects.length}; legacy >= ${minLegacy}: ${realProjects.length}`);
  console.log(`Unit types: ${unitTypes.length}, Items: ${items.length}, BOM lines: ${reqs.length}`);
  console.log(`Unit numbers: ${unitNums.length}, Days: ${days.length}\n`);

  let mapping = {};
  if (mappingPath) {
    if (!existsSync(mappingPath)) {
      console.error(`Mapping file not found: ${mappingPath}`);
      process.exit(1);
    }
    const rawMap = JSON.parse(readFileSync(mappingPath, 'utf8'));
    mapping = Object.fromEntries(Object.entries(rawMap).filter(([k]) => /^\d+$/.test(k)));
  }

  if (DRY && !registryUrl) {
    console.log('Sample parsed deal codes (first 15 real projects):');
    for (const p of realProjects.slice(0, 15)) {
      const code = parseDealCode(p.Description);
      console.log(`  ProjectID ${p.ProjectID}: ${JSON.stringify(p.Description?.slice(0, 60))} → ${code ?? '(no YY-NNN)'}`);
    }
    console.log('\nSet REGISTRY_IQ_* env and run with --apply --auto-create to load (mapping optional).');
    return;
  }

  if (!registryUrl || !registryKey) {
    console.error('Missing REGISTRY_IQ_SUPABASE_URL or REGISTRY_IQ_SUPABASE_SERVICE_ROLE_KEY');
    process.exit(1);
  }

  if (!AUTO_CREATE && !mappingPath) {
    console.error('Use --mapping when --no-auto-create, or omit --no-auto-create for automatic match/create.');
    process.exit(1);
  }

  const reg = createClient(registryUrl, registryKey, { auth: { persistSession: false } });

  for (const p of realProjects) {
    const pid = p.ProjectID;
    const deal = parseDealCode(p.Description);
    const override = mapping[String(pid)];
    const unitCount = unitsPerProject.get(pid) || 0;

    let propertyId;
    let projectRegistryId;
    let plan;

    try {
      const resolved = await resolvePropertyAndProject(reg, p, pid, deal, unitCount, override, DRY);
      propertyId = resolved.propertyId;
      projectRegistryId = resolved.projectRegistryId;
      plan = resolved.plan;
    } catch (e) {
      console.error(`\n── ProjectID ${pid}: ${e.message}`);
      continue;
    }

    console.log(`\n── ProjectID ${pid} deal=${deal ?? 'n/a'} → ${plan.steps.join(' → ')}`);
    if (DRY && (propertyId.startsWith('dry-run') || projectRegistryId.startsWith('dry-run'))) {
      console.log(`  [DRY] would use property ${propertyId} / project ${projectRegistryId}`);
    } else {
      console.log(`  property ${propertyId.slice(0, 8)}… / project_registry ${projectRegistryId.slice(0, 8)}…`);
    }

    const projPatch = {
      legacy_access_project_id: pid,
      access_project_description: p.Description ?? null,
      access_creation_date: p.CreationDate ?? null,
      install_phase: 1,
      fulfillment_mode: 'install',
    };
    if (deal) {
      projPatch.project_id = deal;
    }

    if (!DRY && !String(projectRegistryId).startsWith('dry-run')) {
      const { error } = await reg.from('project_registry').update(projPatch).eq('id', projectRegistryId);
      if (error) console.error('  project_registry update:', error.message);
    } else if (DRY) {
      console.log('  [DRY] project_registry patch:', projPatch);
    }

    const utMap = utByProj.get(pid) ?? new Map();
    const utIdToReg = new Map();

    for (const ut of utMap.values()) {
      const row = {
        property_id: propertyId,
        unit_type_name: ut.UnitTypeName ?? 'Unknown',
        unit_count: ut.Count ?? 0,
        legacy_access_project_id: pid,
        legacy_access_unit_type_id: ut.UnitTypeID,
      };
      if (DRY) {
        console.log('  [DRY] unit_type:', ut.UnitTypeID, row.unit_type_name);
        utIdToReg.set(ut.UnitTypeID, `dry-ut-${ut.UnitTypeID}`);
        continue;
      }

      const { data: existing } = await reg
        .from('property_unit_types')
        .select('id')
        .eq('property_id', propertyId)
        .eq('legacy_access_unit_type_id', ut.UnitTypeID)
        .maybeSingle();

      if (existing?.id) {
        await reg.from('property_unit_types').update(row).eq('id', existing.id);
        utIdToReg.set(ut.UnitTypeID, existing.id);
      } else {
        const { data: ins, error } = await reg.from('property_unit_types').insert(row).select('id').single();
        if (error) console.error('  unit_type insert:', error.message);
        else utIdToReg.set(ut.UnitTypeID, ins.id);
      }
    }

    if (DRY) continue;

    const itemMap = itemsByProj.get(pid) ?? new Map();
    for (const req of reqs.filter((r) => r.ProjectID === pid)) {
      const regUtId = utIdToReg.get(req.UnitTypeID);
      if (!regUtId) continue;
      const item = itemMap.get(req.ItemID);
      if (!item) continue;

      const sku = skuFromItem({ ...item, ProjectID: pid });
      const qtyRaw = req.Requirement;
      const qty =
        qtyRaw != null && qtyRaw !== '' && !Number.isNaN(Number(qtyRaw)) ? Number(qtyRaw) : 1;
      const qb = item.QtyPerBox;
      const qtyPerBox =
        qb != null && qb !== '' && !Number.isNaN(parseInt(String(qb), 10))
          ? parseInt(String(qb), 10)
          : null;

      const { error } = await reg.from('property_unit_type_skus').upsert(
        {
          property_id: propertyId,
          unit_type_id: regUtId,
          sku,
          description: item.ItemName ?? null,
          qty_per_unit: Number.isFinite(qty) ? qty : 1,
          qty_per_box: Number.isFinite(qtyPerBox) ? qtyPerBox : null,
          room_label: '',
          source: 'access_2013',
          legacy_access_project_id: pid,
          legacy_access_unit_type_id: req.UnitTypeID,
          legacy_access_item_id: req.ItemID,
          metadata: { ingest: 'access_2013_sqlite' },
        },
        { onConflict: 'unit_type_id,sku,room_label' },
      );
      if (error) console.error('  sku upsert:', sku, error.message);
    }

    const unRows = unitNums.filter((u) => u.ProjectID === pid);
    for (const un of unRows) {
      const regUtId = utIdToReg.get(un.UnitTypeID);
      if (!regUtId) continue;

      let installDate = null;
      if (un.DayID != null) {
        const drow = days.find((d) => d.ProjectID === pid && d.DayID === un.DayID);
        if (drow?.DayDate) {
          installDate = String(drow.DayDate).slice(0, 10);
        }
      }

      const { error } = await reg.from('property_units').upsert(
        {
          property_id: propertyId,
          unit_type_id: regUtId,
          unit_number: String(un.UnitNumber ?? '').trim(),
          legacy_access_project_id: pid,
          legacy_access_day_id: un.DayID ?? null,
          install_date: installDate,
        },
        { onConflict: 'property_id,unit_number' },
      );
      if (error && !String(error.message).includes('duplicate')) {
        console.error('  unit upsert:', un.UnitNumber, error.message);
      }
    }

    const projDays = days.filter((d) => d.ProjectID === pid);
    for (const drow of projDays) {
      const cal = drow.DayDate ? String(drow.DayDate).slice(0, 10) : null;
      if (!cal) continue;
      const uCount = unitsPerDay.get(`${pid}|${drow.DayID}`) || 0;
      const dayType = classifyDayType(cal, uCount);
      const opswd = uCount > 0 && (dayType === 'install_weekday' || dayType === 'install_weekend')
        ? uCount
        : null;

      const { error } = await reg.from('project_install_days').upsert(
        {
          project_registry_id: projectRegistryId,
          calendar_date: cal,
          day_type: dayType,
          truck_number: drow.TruckNo ?? null,
          units_installed_count: uCount,
          legacy_access_project_id: pid,
          legacy_access_day_id: drow.DayID,
          opswd_units: opswd,
          metric_definition: opswd != null ? 'access_2013: units_installed_count as OPSWD proxy (1 team, 1 shift assumed)' : null,
          metadata: { ingest: 'access_2013_sqlite' },
        },
        { onConflict: 'project_registry_id,calendar_date' },
      );
      if (error) console.error('  install_day:', cal, error.message);
    }
  }

  console.log(`\nDone (${DRY ? 'dry-run' : 'applied'}).`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
