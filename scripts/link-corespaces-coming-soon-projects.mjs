#!/usr/bin/env node
/**
 * CS-05: Link coming-soon Core Spaces Prismic properties to project_registry deals.
 *
 * Usage:
 *   node scripts/link-corespaces-coming-soon-projects.mjs --dry-run
 *   node scripts/link-corespaces-coming-soon-projects.mjs --apply
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

const DRY = !process.argv.includes('--apply');

/** property_name → project_id or project name substring for disambiguation */
const EXPLICIT_MATCHES = [
  { property: 'Hub on Campus Bloomington Lincoln', project_id: null, project_name_contains: 'bloomington lincoln', exclude: [] },
  { property: 'Hub on Campus Tallahassee', project_id: '25-071-I', project_name_contains: 'hub tallahassee', exclude: ['glass door', 'inventory', 'tower', 'tlo', 'roomy', 'attic'] },
  { property: 'Hub on Campus William', project_id: null, project_name_contains: 'william', city: 'Ann Arbor' },
  { property: 'Hub on Campus Ann Arbor State', project_id: null, project_name_contains: 'ann arbor state', city: 'Ann Arbor' },
  { property: 'Hub on Campus Clemson', project_id: '26-002-I', project_name_contains: 'clemson 2', exclude: ['dockside', 'tlo', 'pier', 'collective'] },
  { property: 'Hub on Campus Knoxville 18th', project_id: '25-005-I', project_name_contains: 'hub knoxville', exclude: ['building 3 mirrors', 'attic', 'phase 2-bldg 3'] },
  { property: 'Hub on Campus Tampa Fowler', project_id: '25-1631-D', project_name_contains: 'hub on campus tampa', exclude: ['roomy'] },
  { property: 'Hub on Campus West Lafayette Chauncey', project_id: null, project_name_contains: 'hub chauncey', city: 'West Lafayette', prefer: "'27" },
  { property: 'Hub on Campus Boulder', project_id: '26-006-I', project_name_contains: 'hub boulder', exclude: ['tlo', 'cabinet', 'add on'] },
  { property: 'Hub on Campus Raleigh', project_id: '26-001-I', project_name_contains: 'hub at raleigh', exclude: ['tlo', '5735'] },
  { property: 'Hub on Campus Madison Bassett', project_id: null, project_name_contains: 'madison bassett', exclude: [] },
  { property: 'ōLiv Madison Broom', project_id: null, project_name_contains: 'madison broom', exclude: [] },
  { property: 'Oxenfree Rowlett', project_id: null, project_name_contains: 'rowlett', brand: 'oxenfree', exclude: ['sink', 'cabinet', 'countertop', 'amenity'] },
  { property: 'Oxenfree Clear Creek', project_id: 'MX-004', project_name_contains: 'clear creek', exclude: ['sink', 'cabinet', 'countertop', 'amenity'] },
  { property: 'Oxenfree West Oak', project_id: null, project_name_contains: 'west oak', exclude: ['sink', 'cabinet', 'countertop', 'amenity', 'b18', 'b30'] },
  { property: 'Oxenfree Commerce City', project_id: null, project_name_contains: 'commerce', city: 'Henderson' },
  { property: 'Oxenfree Liberty Hill', project_id: null, project_name_contains: 'liberty hill', city: 'Georgetown' },
  { property: 'Oxenfree Parklin', project_id: null, project_name_contains: 'parklin', city: 'Gastonia' },
  { property: 'Oxenfree Stonebriar', project_id: null, project_name_contains: 'stonebriar', city: 'Frisco' },
];

function norm(s) {
  return (s || '').toLowerCase().replace(/[^a-z0-9\s']/g, ' ').replace(/\s+/g, ' ').trim();
}

function scoreProject(project, rule, property) {
  const pn = norm(project.project_name);
  const needle = norm(rule.project_name_contains);
  if (rule.project_id && project.project_id === rule.project_id) return 100;
  if (!pn.includes(needle)) return 0;
  if ((rule.exclude || []).some((ex) => pn.includes(norm(ex)))) return 0;
  if (rule.city && !pn.includes(norm(rule.city)) && !norm(project.site_address).includes(norm(rule.city))) return 0;
  let score = 50;
  if (['active', 'planning'].includes(project.project_status)) score += 20;
  if (rule.prefer && pn.includes(norm(rule.prefer))) score += 15;
  if (project.property_id) score -= 30;
  if (pn.startsWith(needle) || pn === needle) score += 10;
  return score;
}

async function fetchAll(sb, table, select) {
  let all = [];
  let from = 0;
  for (;;) {
    const { data, error } = await sb.from(table).select(select).range(from, from + 999);
    if (error) throw error;
    if (!data?.length) break;
    all = all.concat(data);
    if (data.length < 1000) break;
    from += 1000;
  }
  return all;
}

async function main() {
  const sb = createClient(process.env.REGISTRY_IQ_SUPABASE_URL, process.env.REGISTRY_IQ_SUPABASE_SERVICE_ROLE_KEY);

  const properties = (await fetchAll(sb, 'property_registry', 'id, property_name, city, state_province, external_ids'))
    .filter((r) => r.external_ids?.corespaces_is_coming_soon === true);

  const projects = await fetchAll(
    sb,
    'project_registry',
    'id, project_name, project_id, property_id, project_status, site_address, external_ids',
  );

  console.log(`Coming soon: ${properties.length} | Projects: ${projects.length} | dryRun=${DRY}`);

  const report = { linked: [], unmatched: [], skipped: [] };

  for (const rule of EXPLICIT_MATCHES) {
    const prop = properties.find((p) => p.property_name === rule.property);
    if (!prop) {
      report.skipped.push({ property: rule.property, reason: 'not in registry' });
      continue;
    }

    const candidates = projects
      .map((p) => ({ p, score: scoreProject(p, rule, prop) }))
      .filter((x) => x.score > 0)
      .sort((a, b) => b.score - a.score);

    const best = candidates[0];
    if (!best) {
      console.log(`NO MATCH: ${prop.property_name}`);
      report.unmatched.push({ property_name: prop.property_name, property_id: prop.id, rule });
      continue;
    }

    const proj = best.p;
    console.log(`MATCH ${prop.property_name} → ${proj.project_id || '?'} ${proj.project_name} (score=${best.score})`);

    if (proj.property_id && proj.property_id !== prop.id) {
      report.skipped.push({
        property: prop.property_name,
        project_id: proj.project_id,
        reason: `project already linked to ${proj.property_id}`,
      });
      continue;
    }

    const ext = { ...(proj.external_ids || {}), corespaces_prismic_property_id: prop.id, corespaces_property_name: prop.property_name };
    const propExt = { ...(prop.external_ids || {}), pipeline_project_id: proj.project_id, pipeline_project_registry_id: proj.id };

    if (DRY) {
      report.linked.push({ property_name: prop.property_name, project_id: proj.project_id, project_name: proj.project_name, dry: true });
      continue;
    }

    const { error: pErr } = await sb.from('project_registry').update({ property_id: prop.id, external_ids: ext }).eq('id', proj.id);
    if (pErr) throw pErr;

    const { error: rErr } = await sb.from('property_registry').update({ external_ids: propExt }).eq('id', prop.id);
    if (rErr) throw rErr;

    report.linked.push({ property_name: prop.property_name, property_id: prop.id, project_id: proj.project_id, project_name: proj.project_name });
  }

  const outPath = resolve(ROOT, '.firecrawl/cs-coming-soon-project-links.json');
  writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log('\n', JSON.stringify(report, null, 2));
  console.log(`Saved ${outPath}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
