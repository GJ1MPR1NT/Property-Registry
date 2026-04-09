#!/usr/bin/env node
/**
 * Cross-base Airtable sync:
 *   iQ Project Registry → Projects.Property_Name
 *   → iQ Property Registry → Properties.Prprty_Name
 *
 * Collects unique non-empty Property_Name values from Projects, compares (normalized)
 * to existing Prprty_Name on Properties, and creates missing property rows.
 *
 * Usage:
 *   node scripts/sync-projects-property-names-to-properties-airtable.mjs           # dry-run
 *   node scripts/sync-projects-property-names-to-properties-airtable.mjs --apply # create records
 *
 * Env: AIRTABLE_PAT or AIRTABLE_API_KEY (Personal Access Token with both bases)
 */

const PROJECT_REGISTRY_BASE = 'appGJzC17TXngWMMx';
const PROPERTY_REGISTRY_BASE = 'appz0l9XP1SiwQQ6c';
const PROJECTS_TABLE = 'Projects';
const PROPERTIES_TABLE = 'Properties';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function normKey(s) {
  if (!s || typeof s !== 'string') return '';
  return s
    .trim()
    .replace(/\s+/g, ' ')
    .toLowerCase();
}

function displayName(s) {
  return typeof s === 'string' ? s.trim().replace(/\s+/g, ' ') : '';
}

/** Fallback when Property_Name is empty: text after first " - " in Project_Name */
function propertyNameFromProjectName(projectName) {
  const p = displayName(projectName);
  if (!p) return '';
  const idx = p.indexOf(' - ');
  if (idx >= 0) return displayName(p.slice(idx + 3));
  return '';
}

async function fetchAllRecords(baseId, tableName, fieldNames) {
  const token = process.env.AIRTABLE_PAT || process.env.AIRTABLE_API_KEY;
  if (!token) {
    throw new Error('Set AIRTABLE_PAT or AIRTABLE_API_KEY');
  }
  const rows = [];
  let offset = null;
  for (;;) {
    const url = new URL(
      `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}`,
    );
    for (const f of fieldNames) {
      url.searchParams.append('fields[]', f);
    }
    if (offset) url.searchParams.set('offset', offset);
    const res = await fetch(url, {
      headers: { Authorization: `Bearer ${token}` },
    });
    const text = await res.text();
    if (!res.ok) {
      throw new Error(`Airtable ${tableName}: ${res.status} ${text.slice(0, 400)}`);
    }
    const body = JSON.parse(text);
    rows.push(...(body.records || []));
    offset = body.offset || null;
    if (!offset) break;
    await sleep(220);
  }
  return rows;
}

async function createRecords(baseId, tableName, records) {
  const token = process.env.AIRTABLE_PAT || process.env.AIRTABLE_API_KEY;
  const BATCH = 10;
  const created = [];
  for (let i = 0; i < records.length; i += BATCH) {
    const chunk = records.slice(i, i + BATCH);
    const res = await fetch(
      `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}`,
      {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${token}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ records: chunk.map((fields) => ({ fields })) }),
      },
    );
    const text = await res.text();
    if (!res.ok) {
      throw new Error(`Airtable create batch: ${res.status} ${text.slice(0, 600)}`);
    }
    const body = JSON.parse(text);
    created.push(...(body.records || []));
    await sleep(220);
  }
  return created;
}

async function main() {
  const apply = process.argv.includes('--apply');

  const projectRows = await fetchAllRecords(PROJECT_REGISTRY_BASE, PROJECTS_TABLE, [
    'Property_Name',
    'Project_Name',
    'City, State',
  ]);

  const propertyRows = await fetchAllRecords(PROPERTY_REGISTRY_BASE, PROPERTIES_TABLE, [
    'Prprty_Name',
  ]);

  const existing = new Set();
  for (const r of propertyRows) {
    const n = r.fields?.Prprty_Name;
    const key = normKey(typeof n === 'string' ? n : '');
    if (key) existing.add(key);
  }

  /** @type {Map<string, { name: string, cityState: string }>} */
  const fromProjects = new Map();
  for (const r of projectRows) {
    const f = r.fields || {};
    let name = displayName(f['Property_Name'] || '');
    if (!name) {
      name = propertyNameFromProjectName(f['Project_Name']);
    }
    if (!name) continue;
    const key = normKey(name);
    if (!key) continue;
    const cityState =
      typeof f['City, State'] === 'string' ? displayName(f['City, State']) : '';
    if (!fromProjects.has(key)) {
      fromProjects.set(key, { name, cityState });
    }
  }

  const missing = [];
  for (const [key, v] of fromProjects) {
    if (!existing.has(key)) {
      missing.push(v);
    }
  }

  console.log(`Projects scanned: ${projectRows.length} rows`);
  console.log(`Unique property names from Projects: ${fromProjects.size}`);
  console.log(`Existing Properties (by Prprty_Name): ${existing.size}`);
  console.log(`Missing in Properties: ${missing.length}`);

  if (missing.length === 0) {
    console.log('Nothing to add.');
    return;
  }

  missing.sort((a, b) => a.name.localeCompare(b.name));
  console.log('\nFirst 25 to add:');
  for (const m of missing.slice(0, 25)) {
    console.log(`  - ${m.name}${m.cityState ? `  (${m.cityState})` : ''}`);
  }
  if (missing.length > 25) console.log(`  ... and ${missing.length - 25} more`);

  if (!apply) {
    console.log('\nDry-run only. Re-run with --apply to create records in iQ Property Registry → Properties.');
    return;
  }

  const records = missing.map((m) => {
    const fields = {
      Prprty_Name: m.name,
      object_uuid: crypto.randomUUID(),
    };
    if (m.cityState) {
      fields.Prprty_Full_Address = m.cityState;
      fields.Prprty_Misc_Notes = `Auto-created from iQ Project Registry / Projects.Property_Name (${new Date().toISOString().slice(0, 10)}).`;
    } else {
      fields.Prprty_Misc_Notes = `Auto-created from iQ Project Registry / Projects (${new Date().toISOString().slice(0, 10)}).`;
    }
    return fields;
  });

  const created = await createRecords(
    PROPERTY_REGISTRY_BASE,
    PROPERTIES_TABLE,
    records,
  );
  console.log(`\nCreated ${created.length} property record(s).`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
