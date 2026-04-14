#!/usr/bin/env node
/**
 * Sync DALE-Demand install_schedules_enriched → Registry-iQ property_registry + project_registry
 * + project_install_phases + property_stakeholders (install contacts).
 *
 * CRITICAL: Projects must NEVER be collapsed, merged, or overwritten without explicit approval.
 * This script is additive: fills NULLs on project_registry, logs conflicts on differing non-NULL values.
 *
 * Prerequisites:
 *   Run scripts/migration-install-schedules-registry.sql on Registry-iQ.
 *   Run scripts/migration-warehouse-and-field-ops-registry.sql (warehouse_registry, field_ops_registry, links).
 *
 * Env:
 *   DALE_DEMAND_SUPABASE_URL, DALE_DEMAND_SUPABASE_KEY (same as dale-chat; optional: DALE_DEMAND_SUPABASE_SERVICE_ROLE_KEY)
 *   REGISTRY_IQ_SUPABASE_URL, REGISTRY_IQ_SUPABASE_SERVICE_ROLE_KEY
 *
 * Usage:
 *   node scripts/sync-install-schedules-to-registry.mjs --dry-run
 *   node scripts/sync-install-schedules-to-registry.mjs --apply
 *   node scripts/sync-install-schedules-to-registry.mjs --apply --deal=24-123-I
 *   node scripts/sync-install-schedules-to-registry.mjs --apply --year=2025
 *   node scripts/sync-install-schedules-to-registry.mjs --apply --all
 */

import { createClient } from '@supabase/supabase-js';
import { config } from 'dotenv';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));

for (const envFile of ['.env.local', '.env']) {
  config({ path: resolve(__dirname, '..', envFile) });
  config({ path: resolve(__dirname, '../../Derived State/dale-chat', envFile) });
}

const argv = process.argv.slice(2);
const DRY = !argv.includes('--apply');
const ALL = argv.includes('--all');
const dealArg = argv.find((a) => a.startsWith('--deal='))?.split('=')[1]?.trim();
const yearArg = argv.find((a) => a.startsWith('--year='))?.split('=')[1]?.trim();

const demandUrl = process.env.DALE_DEMAND_SUPABASE_URL;
/** Matches dale-chat `lib/supabase.ts` (`DALE_DEMAND_SUPABASE_KEY`). */
const demandKey =
  process.env.DALE_DEMAND_SUPABASE_SERVICE_ROLE_KEY || process.env.DALE_DEMAND_SUPABASE_KEY || '';
const regUrl = process.env.REGISTRY_IQ_SUPABASE_URL;
const regKey = process.env.REGISTRY_IQ_SUPABASE_SERVICE_ROLE_KEY;

function pick(row, keys) {
  for (const k of keys) {
    if (row[k] !== undefined && row[k] !== null && row[k] !== '') return row[k];
  }
  return null;
}

/** @param {string|null|undefined} deal */
function parseDealNumber(deal) {
  if (!deal || typeof deal !== 'string') return null;
  const s = deal.trim();
  const mSuffix = s.match(/^(.+?)(-I|-D)$/i);
  const fulfillment_mode = mSuffix
    ? (mSuffix[2].toUpperCase() === '-D' ? 'dropship' : 'install')
    : 'unknown';
  const base = s.replace(/-I$/i, '').replace(/-D$/i, '');
  return { projectId: s, baseDeal: base, fulfillment_mode };
}

function truncate(str, n) {
  if (!str || typeof str !== 'string') return '';
  return str.length > n ? str.slice(0, n) : str;
}

/**
 * @param {Record<string, unknown>} existing
 * @param {Record<string, unknown>} incoming
 * @param {string} label
 */
function mergeAdditive(existing, incoming, label) {
  const patch = {};
  const conflicts = [];
  for (const [k, v] of Object.entries(incoming)) {
    if (v === undefined || v === null || v === '') continue;
    const cur = existing[k];
    if (cur === undefined || cur === null || cur === '') {
      patch[k] = v;
    } else if (String(cur) !== String(v)) {
      conflicts.push(`${label}.${k}: existing=${cur} incoming=${v}`);
    }
  }
  return { patch, conflicts };
}

async function fetchAllFromTable(client, table) {
  const pageSize = 1000;
  let all = [];
  let from = 0;
  for (;;) {
    const { data, error } = await client.from(table).select('*').range(from, from + pageSize - 1);
    if (error) throw new Error(`${table}: ${error.message}`);
    if (!data?.length) break;
    all = all.concat(data);
    if (data.length < pageSize) break;
    from += pageSize;
    if (all.length % 1000 === 0 && all.length > 0) {
      console.warn(`[sync-install] fetched ${all.length} rows from ${table} — verify pagination if total should be higher`);
    }
  }
  return all;
}

async function fetchInstallSchedules(demand) {
  try {
    const rows = await fetchAllFromTable(demand, 'install_schedules_enriched');
    return { rows, table: 'install_schedules_enriched' };
  } catch (e) {
    console.warn('[sync-install] install_schedules_enriched failed, trying install_schedules:', e?.message ?? e);
    const rows = await fetchAllFromTable(demand, 'install_schedules');
    return { rows, table: 'install_schedules' };
  }
}

function buildProjectPayload(row, parsed) {
  const d = (x) => (x === undefined || x === null || x === '' ? null : x);
  const num = (x) => (x === undefined || x === null || x === '' ? null : Number(x));
  return {
    project_id: parsed.projectId,
    project_name: truncate(
      pick(row, ['property_name_address', 'PropertyNameAddress', 'property_name']) ?? parsed.projectId,
      500,
    ),
    fulfillment_mode: parsed.fulfillment_mode === 'unknown' ? 'install' : parsed.fulfillment_mode,
    division: d(pick(row, ['d365_division', 'division'])),
    dale_install_schedule_id: row.id ?? null,
    d365_opportunity_code: d(pick(row, ['d365_opportunity_code'])),
    d365_opportunity_name: d(pick(row, ['d365_opportunity_name'])),
    d365_account_name: d(pick(row, ['d365_account_name'])),
    d365_amount: num(pick(row, ['d365_amount'])),
    d365_status: d(pick(row, ['d365_status'])),
    d365_division: d(pick(row, ['d365_division'])),
    d365_delivery_date: d(pick(row, ['d365_delivery_date'])),
    schedule_year: num(pick(row, ['schedule_year', 'ScheduleYear'])),
    install_start_date: d(pick(row, ['install_start_date', 'InstallStartDate'])),
    estimated_completion_date: d(pick(row, ['estimated_completion_date', 'EstimatedCompletionDate'])),
    num_days: num(pick(row, ['num_days', 'NumDays'])),
    sales_person: d(pick(row, ['sales_person', 'SalesPerson'])),
    on_site_installer: d(pick(row, ['on_site_installer', 'OnSiteInstaller'])),
    property_contact_name: d(pick(row, ['property_contact', 'PropertyContact'])),
    warehouse_contact_name: d(pick(row, ['warehouse_contact', 'WarehouseContact'])),
    warehouse_contact_email: d(pick(row, ['warehouse_email', 'warehouse_email_address', 'WarehouseEmail'])),
    warehouse_address: d(pick(row, ['warehouse_address', 'WarehouseAddress'])),
    temp_labor: d(pick(row, ['temp_labor', 'TempLabor'])),
    backup_temp_labor: d(pick(row, ['backup_temp_labor', 'BackupTempLabor'])),
    labor_options: d(pick(row, ['labor_options', 'LaborOptions'])),
    additional_items: d(pick(row, ['additional_items', 'AdditionalItems'])),
    access_notes: d(pick(row, ['access', 'Access'])),
    darlas_contact: d(pick(row, ['darlas_contact', 'DarlasContact'])),
    barstool_confirmed: d(pick(row, ['barstool_confirmed', 'BarstoolConfirmed'])),
    source_file: d(pick(row, ['source_file', 'SourceFile'])),
    sheet_name: d(pick(row, ['sheet_name', 'SheetName'])),
    line_number: num(pick(row, ['line_number', 'LineNumber'])),
  };
}

function installScheduleExternalIds(row) {
  return {
    install_schedules: {
      source_row_id: row.id ?? null,
      source_table: 'dale_demand',
    },
  };
}

function buildPhasePayload(row, projectRegistryId, parsed) {
  const d = (x) => (x === undefined || x === null || x === '' ? null : x);
  const num = (x) => (x === undefined || x === null || x === '' ? null : Number(x));
  return {
    project_registry_id: projectRegistryId,
    phase_number: 1,
    phase_label: 'Install schedule',
    schedule_year: num(pick(row, ['schedule_year', 'ScheduleYear'])),
    install_start_date: d(pick(row, ['install_start_date', 'InstallStartDate'])),
    estimated_completion_date: d(pick(row, ['estimated_completion_date', 'EstimatedCompletionDate'])),
    num_days: num(pick(row, ['num_days', 'NumDays'])),
    fulfillment_mode: parsed.fulfillment_mode === 'unknown' ? null : parsed.fulfillment_mode,
    dale_install_schedule_id: row.id ?? null,
    metadata: { deal_number: parsed.projectId },
  };
}

function parsePropertyNameAddress(raw) {
  if (!raw || typeof raw !== 'string') return { name: 'Unknown property', line1: 'TBD' };

  // Strip trailing project code (e.g. "22-044-I-A", "24-123-I", "24-BSI-abc123")
  // and optional leading state abbreviation before it
  const cleaned = raw
    .trim()
    .replace(/\s+\d{2}-\d{3,4}(-[A-Z](-[A-Z0-9]+)?)+\s*$/i, '')  // 22-044-I-A style
    .replace(/\s+\d{2}-[A-Z]{2,6}-[0-9a-f]{4,}\s*$/i, '')          // 24-BSI-a3f9c2 style
    .replace(/\s+[A-Z]{2}\s*$/i, '')                                  // trailing state abbrev
    .trim();

  const parts = cleaned.split(',').map((s) => s.trim()).filter(Boolean);
  if (parts.length >= 2) {
    return { name: truncate(parts[0], 500), line1: truncate(parts.slice(1).join(', '), 500) };
  }
  return { name: truncate(cleaned || raw, 500), line1: 'TBD' };
}

async function findProperty(reg, row, baseDeal) {
  const city = pick(row, ['city', 'City']);
  const state = pick(row, ['state', 'State']);
  const rawName = pick(row, ['property_name_address', 'PropertyNameAddress']);
  const { name } = parsePropertyNameAddress(rawName ?? '');

  if (baseDeal) {
    const { data: byDeal } = await reg
      .from('property_registry')
      .select('id, property_name')
      .filter('external_ids->>deal_number', 'eq', baseDeal)
      .limit(5);
    if (byDeal?.length === 1) return byDeal[0];
    if (byDeal && byDeal.length > 1) {
      console.warn(`[sync-install] multiple properties with deal_number=${baseDeal}, using first`);
      return byDeal[0];
    }
  }

  if (city && state && name.length > 2) {
    const { data: candidates } = await reg
      .from('property_registry')
      .select('id, property_name, city, state_province')
      .ilike('city', city)
      .eq('state_province', state)
      .limit(50);
    if (candidates?.length) {
      const n = name.slice(0, 48).toLowerCase();
      const hit = candidates.find((p) => p.property_name?.toLowerCase().includes(n.slice(0, 20)));
      if (hit) return hit;
    }
  }

  return null;
}

async function ensurePropertyStub(reg, row, baseDeal, dryRun) {
  const city = pick(row, ['city', 'City']) || 'Unknown';
  const state = pick(row, ['state', 'State']) || 'Unknown';
  const lat = pick(row, ['lat', 'latitude']);
  const lng = pick(row, ['lng', 'longitude', 'long']);
  const rawName = pick(row, ['property_name_address', 'PropertyNameAddress']) || 'Unknown';
  const { name, line1 } = parsePropertyNameAddress(rawName);

  const ext = {
    deal_number: baseDeal ?? undefined,
    install_schedules: { stub: true },
  };

  const insert = {
    property_name: name,
    property_type: 'student_housing',
    property_status: 'active',
    tlc_relationship: 'customer',
    address_line1: line1,
    city,
    state_province: state,
    postal_code: '00000',
    country: 'US',
    latitude: lat != null ? Number(lat) : null,
    longitude: lng != null ? Number(lng) : null,
    external_ids: ext,
    source: 'install_schedules',
    notes: 'Stub from DALE-Demand install_schedules sync — verify address and enrich.',
  };

  if (dryRun) {
    console.log('[DRY] would create property_registry stub:', insert.property_name, city, state);
    return 'dry-run-property-id';
  }

  const { data, error } = await reg.from('property_registry').insert(insert).select('id').single();
  if (error) throw new Error(`property_registry insert: ${error.message}`);
  return data.id;
}

/**
 * @param {import('@supabase/supabase-js').SupabaseClient} reg
 * @param {string} propertyId
 * @param {string} displayName
 * @param {string} roleNote
 */
async function linkContact(reg, propertyId, displayName, roleNote, dryRun) {
  if (!displayName || typeof displayName !== 'string' || displayName.trim().length < 2) return;
  const name = displayName.trim().slice(0, 500);

  const { data: existing } = await reg
    .from('property_stakeholders')
    .select('id')
    .eq('property_id', propertyId)
    .ilike('stakeholder_name', name)
    .limit(1);

  if (existing?.length) return;

  const row = {
    property_id: propertyId,
    stakeholder_id: null,
    stakeholder_name: name,
    company_name: name,
    role: 'other',
    is_primary: false,
    notes: `Install schedule: ${roleNote}`,
  };

  if (dryRun) {
    console.log(`  [DRY] property_stakeholders: ${roleNote} → ${name}`);
    return;
  }

  const { error } = await reg.from('property_stakeholders').insert(row);
  if (error) console.warn(`  property_stakeholders insert (${roleNote}):`, error.message);
}

/** Split multi-line / multi-person schedule blobs into individual contact strings. */
const SPLIT_CONTACT_MAX = 25;

function extractEmailFromSegment(segment) {
  const m = String(segment).match(/[\w.+-]+@[\w.-]+\.[a-z]{2,}/i);
  return m ? m[0] : null;
}

function isNoiseLine(s) {
  const t = String(s).trim();
  if (t.length < 2) return true;
  if (!/[a-zA-Z]/.test(t) && /^\d[\d\s.\-()]{6,}$/.test(t.replace(/\s/g, ''))) return true;
  if (/^[\d\s.\-/]{4,}$/.test(t) && !/[a-zA-Z]/.test(t)) return true;
  if (/^\d{1,2}\/\d{1,2}-\d{1,2}\/\d{1,2}/.test(t) && t.length < 28) return true;
  if (/^\d{1,2}\/\d{1,2}\s*-\s*\d{1,2}\/\d{1,2}/.test(t) && t.length < 28) return true;
  return false;
}

function cleanContactLine(line) {
  let s = String(line).trim();
  if (!s) return '';
  s = s.replace(/^Ops\s*Mgr\s*|^CSMgr\s*|^CSTeam\s*/gi, '');
  s = s.replace(/^O:\s*|^C:\s*|^D:\s*/i, '');
  const nameDate = s.match(/^([A-Za-z][A-Za-z\s.'-]{0,48}?)[-–]\s*\d{1,2}\/\d{1,2}/);
  if (nameDate) s = nameDate[1].trim();
  else {
    const shortRicky = s.match(/^([A-Za-z][A-Za-z\s.'-]{0,48}?)[-–]\d/);
    if (shortRicky && s.length < 90) s = shortRicky[1].trim();
  }
  s = s.replace(/\s+\d{3}[-.]?\d{3}[-.]?\d{4}\s*$/g, '').trim();
  s = s.replace(/\s+\(\d{3}\)\s*\d{3}[-.]?\d{4}\s*$/g, '').trim();
  s = s.replace(/\s+\d{3}[-.]?\d{3}[-.]?\d{4}\s*$/g, '').trim();
  return s.trim();
}

/**
 * @returns {string[]}
 */
function splitContactBlob(raw) {
  if (!raw || typeof raw !== 'string') return [];
  const s = raw.replace(/\r\n/g, '\n').replace(/\r/g, '\n');
  const chunks = [];
  for (const line of s.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    if (trimmed.includes(';') && trimmed.length > 40) {
      for (const part of trimmed.split(';')) {
        const p = part.trim();
        if (p) chunks.push(p);
      }
    } else if (
      trimmed.includes('/') &&
      trimmed.length < 150 &&
      !trimmed.includes('://') &&
      !trimmed.includes('@')
    ) {
      const parts = trimmed.split('/').map((x) => x.trim()).filter(Boolean);
      if (parts.length >= 2 && parts.every((p) => p.length < 80 && /[a-zA-Z]/.test(p))) {
        for (const p of parts) chunks.push(p);
      } else {
        chunks.push(trimmed);
      }
    } else {
      chunks.push(trimmed);
    }
  }
  const out = [];
  const seen = new Set();
  for (const chunk of chunks) {
    let cleaned = cleanContactLine(chunk);
    if (!cleaned || isNoiseLine(cleaned)) continue;
    const key = normalizeKey(cleaned);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(cleaned.slice(0, 500));
    if (out.length >= SPLIT_CONTACT_MAX) break;
  }
  return out;
}

/**
 * Insert one property_stakeholders row per split segment (or one row if split yields nothing usable).
 */
async function linkContactsSplit(reg, propertyId, rawValue, roleNote, dryRun) {
  if (!rawValue || String(rawValue).trim().length < 2) return;
  const segments = splitContactBlob(String(rawValue));
  if (segments.length === 0) {
    await linkContact(reg, propertyId, String(rawValue).trim(), roleNote, dryRun);
    return;
  }
  for (const seg of segments) {
    await linkContact(reg, propertyId, seg, roleNote, dryRun);
  }
}

function normalizeKey(s) {
  if (!s || typeof s !== 'string') return '';
  return s
    .toLowerCase()
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, 500);
}

/** Dedupe key for warehouse: prefer full address + region; else email; else contact name. */
function warehouseDedupeKey(projectPayload, row) {
  const addr = (projectPayload.warehouse_address || '').trim();
  const city = pick(row, ['warehouse_city', 'WarehouseCity']);
  const state = pick(row, ['warehouse_state', 'WarehouseState']);
  const email = (projectPayload.warehouse_contact_email || '').trim().toLowerCase();
  const contact = (projectPayload.warehouse_contact_name || '').trim();
  if (addr) {
    const c = city || '';
    const st = state || '';
    return normalizeKey(`${addr}|${c}|${st}`);
  }
  if (email) return normalizeKey(`email:${email}`);
  if (contact) return normalizeKey(`contact:${contact}`);
  return null;
}

function warehouseDisplayName(projectPayload) {
  const addr = (projectPayload.warehouse_address || '').trim();
  const contact = (projectPayload.warehouse_contact_name || '').trim();
  if (addr) return truncate(addr, 200);
  if (contact) return truncate(contact, 200);
  return 'Warehouse';
}

function parseHumanName(displayName) {
  const s = (displayName || '').trim();
  if (s.length < 2) return { first_name: null, last_name: null };
  if (s.includes(',')) {
    const [a, b] = s.split(',').map((x) => x.trim());
    if (b) return { first_name: b, last_name: a };
    return { first_name: null, last_name: a };
  }
  const parts = s.split(/\s+/).filter(Boolean);
  if (parts.length === 1) return { first_name: parts[0], last_name: null };
  return { first_name: parts[0], last_name: parts.slice(1).join(' ') };
}

function fieldOpsDedupeKey(displayName, email) {
  const e = (email || '').trim().toLowerCase();
  if (e) return normalizeKey(`email:${e}`);
  return normalizeKey(`name:${displayName}`);
}

/**
 * @returns {Promise<string|null>}
 */
async function findOrCreateWarehouse(reg, projectPayload, row, dryRun) {
  const dk = warehouseDedupeKey(projectPayload, row);
  if (!dk) return null;

  const { data: existing } = await reg.from('warehouse_registry').select('id').eq('dedupe_key', dk).maybeSingle();
  if (existing?.id) return existing.id;

  const addr = (projectPayload.warehouse_address || '').trim();
  const insert = {
    dedupe_key: dk,
    warehouse_name: warehouseDisplayName(projectPayload),
    address_line1: addr || null,
    primary_contact_name: projectPayload.warehouse_contact_name || null,
    primary_email: projectPayload.warehouse_contact_email || null,
    primary_phone: null,
    scale_notes: null,
    external_ids: { install_schedules: true },
    source: 'install_schedules',
    notes: 'From DALE-Demand install_schedules — enrich site + scale.',
  };

  if (dryRun) {
    console.log(`  [DRY] warehouse_registry: ${insert.warehouse_name.slice(0, 60)}…`);
    return 'dry-run-warehouse-id';
  }

  const { data, error } = await reg.from('warehouse_registry').insert(insert).select('id').single();
  if (error) {
    console.warn('  warehouse_registry insert:', error.message);
    return null;
  }
  return data?.id ?? null;
}

async function linkWarehouseProject(reg, warehouseId, projectRegistryId, row, dryRun) {
  if (!warehouseId) return;
  const sid = row.id ?? null;
  if (dryRun) {
    console.log(
      `  [DRY] warehouse_project_service: project=${projectRegistryId.slice(0, 8)}… schedule=${sid ? String(sid).slice(0, 8) : '—'}…`,
    );
    return;
  }
  if (warehouseId === 'dry-run-warehouse-id') return;
  const { data: existing } = await reg
    .from('warehouse_project_service')
    .select('id')
    .eq('warehouse_registry_id', warehouseId)
    .eq('project_registry_id', projectRegistryId)
    .maybeSingle();
  if (existing?.id) {
    await reg
      .from('warehouse_project_service')
      .update({
        last_seen_at: new Date().toISOString(),
        dale_install_schedule_id: sid,
        metadata: { last_schedule: sid },
      })
      .eq('id', existing.id);
    return;
  }
  const { error } = await reg.from('warehouse_project_service').insert({
    warehouse_registry_id: warehouseId,
    project_registry_id: projectRegistryId,
    dale_install_schedule_id: sid,
    source: 'install_schedules',
  });
  if (error) console.warn('  warehouse_project_service insert:', error.message);
}

async function patchProjectWarehouseIfNull(reg, projectRegistryId, warehouseId, dryRun) {
  if (!warehouseId || warehouseId === 'dry-run-warehouse-id' || dryRun) return;
  const { data: pr } = await reg.from('project_registry').select('warehouse_registry_id').eq('id', projectRegistryId).maybeSingle();
  if (pr?.warehouse_registry_id) return;
  const { error } = await reg
    .from('project_registry')
    .update({ warehouse_registry_id: warehouseId })
    .eq('id', projectRegistryId);
  if (error) console.warn('  project_registry warehouse_registry_id:', error.message);
}

/**
 * @returns {Promise<string|null>}
 */
async function findOrCreateFieldOps(reg, displayName, roleCategory, email, dryRun) {
  const d = (displayName || '').trim();
  if (d.length < 2) return null;
  const dk = fieldOpsDedupeKey(d, email);
  const { first_name, last_name } = parseHumanName(d);

  const { data: existing } = await reg.from('field_ops_registry').select('id, email').eq('dedupe_key', dk).maybeSingle();
  if (existing?.id) {
    if (!dryRun && email && !existing.email) {
      await reg
        .from('field_ops_registry')
        .update({ email, enrichment_status: 'partial' })
        .eq('id', existing.id);
    }
    return existing.id;
  }

  const insert = {
    dedupe_key: dk,
    display_name: truncate(d, 500),
    first_name,
    last_name,
    email: email || null,
    phone: null,
    role_category: roleCategory,
    enrichment_status: email ? 'partial' : 'pending',
    external_ids: { install_schedules: true },
    notes: 'From install_schedules — enrich phone and full name.',
  };

  if (dryRun) {
    console.log(`  [DRY] field_ops_registry [${roleCategory}]: ${d.slice(0, 48)}…`);
    return 'dry-run-field-ops-id';
  }

  const { data, error } = await reg.from('field_ops_registry').insert(insert).select('id').single();
  if (error) {
    console.warn('  field_ops_registry insert:', error.message);
    return null;
  }
  return data?.id ?? null;
}

async function upsertFieldOpsAssignment(
  reg,
  fieldOpsId,
  propertyId,
  projectRegistryId,
  assignmentRole,
  row,
  dryRun,
) {
  if (!fieldOpsId || fieldOpsId === 'dry-run-field-ops-id') return;
  const sid = row.id ?? null;
  if (dryRun) {
    console.log(`  [DRY] field_ops_assignment: ${assignmentRole} project=${projectRegistryId.slice(0, 8)}…`);
    return;
  }
  const { data: existing } = await reg
    .from('field_ops_assignment')
    .select('id')
    .eq('field_ops_registry_id', fieldOpsId)
    .eq('project_registry_id', projectRegistryId)
    .eq('assignment_role', assignmentRole)
    .maybeSingle();

  const payload = {
    field_ops_registry_id: fieldOpsId,
    property_id: propertyId,
    project_registry_id: projectRegistryId,
    assignment_role: assignmentRole,
    dale_install_schedule_id: sid,
    source: 'install_schedules',
  };

  if (existing?.id) {
    await reg.from('field_ops_assignment').update({ ...payload, updated_at: new Date().toISOString() }).eq('id', existing.id);
  } else {
    const { error } = await reg.from('field_ops_assignment').insert(payload);
    if (error) console.warn(`  field_ops_assignment insert (${assignmentRole}):`, error.message);
  }
}

/** Map install schedule fields → field_ops role_category + assignment_role */
const FIELD_OPS_FROM_SCHEDULE = [
  { field: 'on_site_installer', category: 'installer', assignment: 'on_site_installer', emailField: null },
  { field: 'property_contact_name', category: 'site_management', assignment: 'property_contact', emailField: null },
  { field: 'sales_person', category: 'sales', assignment: 'sales_person', emailField: null },
  { field: 'darlas_contact', category: 'other', assignment: 'darlas_contact', emailField: null },
  { field: 'temp_labor', category: 'labor', assignment: 'temp_labor', emailField: null },
  { field: 'backup_temp_labor', category: 'labor', assignment: 'backup_temp_labor', emailField: null },
  { field: 'warehouse_contact_name', category: 'field_ops', assignment: 'warehouse_contact', emailField: 'warehouse_contact_email' },
];

async function syncWarehouseAndFieldOps(
  reg,
  row,
  projectPayload,
  projectRegistryId,
  propertyId,
  dryRun,
) {
  if (!propertyId || propertyId === 'dry-run-property-id') return;

  const hasWarehouse =
    projectPayload.warehouse_address ||
    projectPayload.warehouse_contact_name ||
    projectPayload.warehouse_contact_email;
  if (hasWarehouse) {
    const wid = await findOrCreateWarehouse(reg, projectPayload, row, dryRun);
    if (wid) {
      await linkWarehouseProject(reg, wid, projectRegistryId, row, dryRun);
      await patchProjectWarehouseIfNull(reg, projectRegistryId, wid, dryRun);
    }
  }

  for (const spec of FIELD_OPS_FROM_SCHEDULE) {
    const nameVal = projectPayload[spec.field];
    if (!nameVal || String(nameVal).trim().length < 2) continue;
    const singleEmail =
      spec.emailField && projectPayload[spec.emailField]
        ? String(projectPayload[spec.emailField]).trim()
        : null;
    const segments = splitContactBlob(String(nameVal));
    const toProcess =
      segments.length > 0 ? segments : [String(nameVal).trim().slice(0, 500)];
    for (let i = 0; i < toProcess.length; i++) {
      const seg = toProcess[i];
      const segEmail =
        extractEmailFromSegment(seg) || (toProcess.length === 1 ? singleEmail : null);
      const fid = await findOrCreateFieldOps(reg, seg, spec.category, segEmail, dryRun);
      await upsertFieldOpsAssignment(reg, fid, propertyId, projectRegistryId, spec.assignment, row, dryRun);
    }
  }
}

async function upsertPhase(reg, row, projectRegistryId, parsed, dryRun) {
  const phasePayload = buildPhasePayload(row, projectRegistryId, parsed);
  const sid = row.id;
  if (!sid) {
    console.warn('[sync-install] row missing id, skipping phase row');
    return;
  }

  const { data: existing } = await reg
    .from('project_install_phases')
    .select('id')
    .eq('dale_install_schedule_id', sid)
    .maybeSingle();

  if (dryRun) {
    console.log(`  [DRY] project_install_phases ${existing ? 'update' : 'insert'} schedule_id=${sid}`);
    return;
  }

  if (existing?.id) {
    const { error } = await reg.from('project_install_phases').update(phasePayload).eq('id', existing.id);
    if (error) console.warn('  project_install_phases update:', error.message);
  } else {
    const { error } = await reg.from('project_install_phases').insert(phasePayload);
    if (error) console.warn('  project_install_phases insert:', error.message);
  }
}

async function main() {
  if (!demandUrl || !demandKey) {
    console.error(
      'Missing DALE_DEMAND_SUPABASE_URL or DALE_DEMAND_SUPABASE_KEY (or DALE_DEMAND_SUPABASE_SERVICE_ROLE_KEY)',
    );
    process.exit(1);
  }
  if (!regUrl || !regKey) {
    console.error('Missing REGISTRY_IQ_SUPABASE_URL or REGISTRY_IQ_SUPABASE_SERVICE_ROLE_KEY');
    process.exit(1);
  }

  const demand = createClient(demandUrl, demandKey, { auth: { persistSession: false } });
  const reg = createClient(regUrl, regKey, { auth: { persistSession: false } });

  console.log(`Install schedules → Registry (${DRY ? 'DRY-RUN' : 'APPLY'})`);
  const { rows, table } = await fetchInstallSchedules(demand);
  console.log(`Source: ${table}, rows=${rows.length}`);

  let filtered = rows;
  if (dealArg) {
    filtered = filtered.filter((r) => {
      const d = pick(r, ['deal_number', 'DealNumber', 'dealNumber']);
      return d && String(d).trim() === dealArg;
    });
  }
  if (yearArg) {
    const y = Number(yearArg);
    filtered = filtered.filter((r) => {
      const sy = pick(r, ['schedule_year', 'ScheduleYear']);
      return sy != null && Number(sy) === y;
    });
  }
  if (!ALL && !dealArg && !yearArg) {
    console.error('Specify --all, or filter with --deal= or --year= (safety).');
    process.exit(1);
  }

  console.log(`Processing ${filtered.length} row(s) after filters.`);

  let createdProjects = 0;
  let updatedProjects = 0;
  let conflictLog = [];

  for (const row of filtered) {
    const dealRaw = pick(row, ['deal_number', 'DealNumber', 'dealNumber']);
    if (!dealRaw) {
      console.warn('[sync-install] skip row with no deal_number', row.id);
      continue;
    }
    const parsed = parseDealNumber(String(dealRaw));
    if (!parsed) continue;

    const projectPayload = buildProjectPayload(row, parsed);
    const baseDeal = parsed.baseDeal;

    let property = await findProperty(reg, row, baseDeal);
    let propertyId = property?.id;
    if (!propertyId) {
      propertyId = await ensurePropertyStub(reg, row, baseDeal, DRY);
    }

    const { data: existingProject } = await reg
      .from('project_registry')
      .select('*')
      .eq('project_id', parsed.projectId)
      .maybeSingle();

    /** @type {string|null|undefined} */
    let projectRegistryId = existingProject?.id;

    if (!existingProject) {
      const insertPayload = {
        ...projectPayload,
        property_id: propertyId,
        external_ids: installScheduleExternalIds(row),
      };
      if (DRY) {
        console.log(`[DRY] insert project_registry ${parsed.projectId} property=${propertyId}`);
      } else {
        const { data: ins, error } = await reg
          .from('project_registry')
          .insert(insertPayload)
          .select('id')
          .single();
        if (error) {
          console.error(`insert project ${parsed.projectId}:`, error.message);
          continue;
        }
        createdProjects++;
        projectRegistryId = ins?.id;
      }
    } else {
      const { patch, conflicts } = mergeAdditive(existingProject, projectPayload, parsed.projectId);
      conflictLog.push(...conflicts);
      // Link orphan project rows (property_id null) to the resolved property
      if (
        existingProject.property_id == null &&
        propertyId &&
        propertyId !== 'dry-run-property-id'
      ) {
        patch.property_id = propertyId;
      }
      if (Object.keys(patch).length > 0) {
        if (DRY) {
          console.log(`[DRY] update project_registry ${parsed.projectId}`, Object.keys(patch));
        } else {
          const { error } = await reg.from('project_registry').update(patch).eq('id', existingProject.id);
          if (error) console.error(`update project ${parsed.projectId}:`, error.message);
          else updatedProjects++;
        }
      }
    }

    if (!projectRegistryId && !DRY) {
      const { data: projRow } = await reg
        .from('project_registry')
        .select('id')
        .eq('project_id', parsed.projectId)
        .maybeSingle();
      projectRegistryId = projRow?.id;
    }
    if (DRY && !projectRegistryId) {
      projectRegistryId = 'dry-run-project-id';
    }
    if (!projectRegistryId) continue;

    await upsertPhase(reg, row, projectRegistryId, parsed, DRY);

    if (propertyId && propertyId !== 'dry-run-property-id') {
      await linkContactsSplit(reg, propertyId, projectPayload.property_contact_name, 'property_contact', DRY);
      await linkContactsSplit(reg, propertyId, projectPayload.warehouse_contact_name, 'warehouse_contact', DRY);
      await linkContactsSplit(reg, propertyId, projectPayload.on_site_installer, 'on_site_installer', DRY);
      await linkContactsSplit(reg, propertyId, projectPayload.sales_person, 'sales_person', DRY);
      await linkContactsSplit(reg, propertyId, projectPayload.darlas_contact, 'darlas_contact', DRY);
      await linkContactsSplit(reg, propertyId, projectPayload.temp_labor, 'temp_labor', DRY);
      await linkContactsSplit(reg, propertyId, projectPayload.backup_temp_labor, 'backup_temp_labor', DRY);
      await syncWarehouseAndFieldOps(reg, row, projectPayload, projectRegistryId, propertyId, DRY);
    }
  }

  if (conflictLog.length) {
    console.log('\n--- Field conflicts (not overwritten) ---');
    for (const c of conflictLog.slice(0, 80)) console.log(c);
    if (conflictLog.length > 80) console.log(`... and ${conflictLog.length - 80} more`);
  }

  console.log(
    `\nDone. createdProjects=${createdProjects} updatedProjects=${updatedProjects} dryRun=${DRY}`,
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
