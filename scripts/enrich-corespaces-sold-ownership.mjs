#!/usr/bin/env node
/**
 * CS-02: Assign current owner + PM for sold Core Spaces portfolio (is_owned_by_core=false).
 * Sources documented per property in enrichment_sources.
 *
 * Usage:
 *   node scripts/enrich-corespaces-sold-ownership.mjs --dry-run
 *   node scripts/enrich-corespaces-sold-ownership.mjs --apply
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
const ACC_ID = '9a1dc8e5-97d3-42e9-8889-3321dd87608d';

/** @type {Record<string, { owner: string, ownerType: string, pm: string, pmType: string, source: string, url: string, confidence: string }>} */
const OWNERSHIP = {
  'Hub on Campus Ann Arbor': {
    owner: 'American Campus Communities',
    ownerType: 'owner',
    pm: 'American Campus Communities',
    pmType: 'property_manager',
    source: 'MHN Sep 2017 — ACC $560.6M portfolio (Hub Ann Arbor upcoming delivery)',
    url: 'https://www.multihousingnews.com/american-campus-nabs-561m-student-housing-portfolio/',
    confidence: 'high',
  },
  'Hub on Campus Eugene': {
    owner: 'American Campus Communities',
    ownerType: 'owner',
    pm: 'American Campus Communities',
    pmType: 'property_manager',
    source: 'MHN Sep 2017 — existing community in ACC portfolio',
    url: 'https://www.multihousingnews.com/american-campus-nabs-561m-student-housing-portfolio/',
    confidence: 'high',
  },
  'Hub on Campus Flagstaff': {
    owner: 'American Campus Communities',
    ownerType: 'owner',
    pm: 'American Campus Communities',
    pmType: 'property_manager',
    source: 'MHN Sep 2017 — upcoming delivery in ACC portfolio; property_url on americancampus.com',
    url: 'https://www.americancampus.com/student-apartments/az/flagstaff/the-jack',
    confidence: 'high',
  },
  'Hub on Campus U District Seattle': {
    owner: 'American Campus Communities',
    ownerType: 'owner',
    pm: 'American Campus Communities',
    pmType: 'property_manager',
    source: 'MHN Sep 2017 — Hub U District Seattle in ACC portfolio',
    url: 'https://www.multihousingnews.com/american-campus-nabs-561m-student-housing-portfolio/',
    confidence: 'high',
  },
  'State on Campus Fort Collins': {
    owner: 'American Campus Communities',
    ownerType: 'owner',
    pm: 'American Campus Communities',
    pmType: 'property_manager',
    source: 'MHN Sep 2017 — "State" (665-bed CSU) in ACC portfolio',
    url: 'https://www.americancampus.com/student-apartments/co/fort-collins/state',
    confidence: 'high',
  },
  'Hub on Campus Madison': {
    owner: 'American Campus Communities',
    ownerType: 'owner',
    pm: 'American Campus Communities',
    pmType: 'property_manager',
    source: 'MHN Sep 2017 — "The James" (850-bed, UW) acquired by ACC; operates as Hub Madison',
    url: 'https://www.multihousingnews.com/american-campus-nabs-561m-student-housing-portfolio/',
    confidence: 'high',
  },
  'Hub on Campus Oxford': {
    owner: 'Lark Living',
    ownerType: 'owner',
    pm: 'Lark Living',
    pmType: 'property_manager',
    source: 'Current operator site larkoxford.com (Lark student housing brand)',
    url: 'https://larkoxford.com/',
    confidence: 'medium',
  },
  'Hub on Campus Tempe': {
    owner: 'Scion Group',
    ownerType: 'owner',
    pm: 'University House',
    pmType: 'property_manager',
    source: 'Rebranded to University House Tempe (uhtempe.com); Scion Group operates University House brand',
    url: 'https://uhtempe.com/',
    confidence: 'medium',
  },
  'Hub on Campus Tucson': {
    owner: 'Yugo',
    ownerType: 'owner',
    pm: 'Yugo',
    pmType: 'property_manager',
    source: 'Current operator site yugotucsoncampus.com',
    url: 'https://www.yugotucsoncampus.com/',
    confidence: 'medium',
  },
  'Hub on Campus Minneapolis': {
    owner: null,
    ownerType: null,
    pm: null,
    pmType: null,
    source: 'Press research inconclusive — hubminneapolis.com (not americancampus.com); Core no longer owns per Prismic',
    url: 'https://www.hubminneapolis.com/',
    confidence: 'low',
  },
};

async function ensureStakeholder(sb, name, type, website) {
  const { data: existing } = await sb
    .from('stakeholder_registry')
    .select('id, stakeholder_name, stakeholder_type')
    .ilike('stakeholder_name', name)
    .limit(5);

  const exact = (existing || []).find((s) => s.stakeholder_name.toLowerCase() === name.toLowerCase());
  if (exact) return exact.id;

  if (DRY) {
    console.log(`  [NEW stakeholder] ${name} (${type})`);
    return `dry-${name}`;
  }

  const { data, error } = await sb
    .from('stakeholder_registry')
    .insert({
      stakeholder_name: name,
      stakeholder_type: type === 'property_manager' ? 'property_manager' : type,
      website: website || null,
      notes: `[CS-02] Created from Core Spaces sold-portfolio ownership research`,
    })
    .select('id')
    .single();
  if (error) throw error;
  return data.id;
}

async function upsertPropertyStakeholder(sb, propertyId, stakeholderId, name, role) {
  if (typeof stakeholderId === 'string' && stakeholderId.startsWith('dry-')) return;
  const { data: existing } = await sb
    .from('property_stakeholders')
    .select('id')
    .eq('property_id', propertyId)
    .eq('stakeholder_id', stakeholderId)
    .eq('role', role)
    .maybeSingle();

  if (existing) return;

  if (DRY) {
    console.log(`  [LINK] property ${propertyId.slice(0, 8)} ← ${name} (${role})`);
    return;
  }

  const { error } = await sb.from('property_stakeholders').insert({
    property_id: propertyId,
    stakeholder_id: stakeholderId,
    stakeholder_name: name,
    company_name: name,
    role,
    is_primary: true,
    notes: '[CS-02] Core Spaces sold portfolio ownership research',
  });
  if (error) throw error;
}

async function main() {
  const sb = createClient(process.env.REGISTRY_IQ_SUPABASE_URL, process.env.REGISTRY_IQ_SUPABASE_SERVICE_ROLE_KEY);

  let all = [];
  let from = 0;
  for (;;) {
    const { data, error } = await sb
      .from('property_registry')
      .select('id, property_name, external_ids, enrichment_sources, owner_name, property_manager_name')
      .range(from, from + 999);
    if (error) throw error;
    if (!data?.length) break;
    all = all.concat(data);
    if (data.length < 1000) break;
    from += 1000;
  }

  const sold = all.filter((r) => r.external_ids?.prismic_id && r.external_ids?.corespaces_is_owned_by_core === false);
  console.log(`Sold portfolio: ${sold.length} properties | dryRun=${DRY}`);

  const report = { updated: 0, linked: 0, skipped: 0, unresolved: [], assignments: [] };

  for (const prop of sold) {
    const cfg = OWNERSHIP[prop.property_name];
    if (!cfg) {
      console.warn(`  No mapping for ${prop.property_name}`);
      report.skipped++;
      continue;
    }

    console.log(`\n${prop.property_name} (confidence=${cfg.confidence})`);

    if (!cfg.owner) {
      report.unresolved.push({ property_name: prop.property_name, id: prop.id, note: cfg.source });
      report.skipped++;
      continue;
    }

    let ownerId = cfg.owner === 'American Campus Communities' ? ACC_ID : await ensureStakeholder(sb, cfg.owner, cfg.ownerType, cfg.url);
    let pmId =
      cfg.pm === cfg.owner
        ? ownerId
        : cfg.pm === 'American Campus Communities'
          ? ACC_ID
          : await ensureStakeholder(sb, cfg.pm, cfg.pmType, cfg.url);

    const sourceEntry = {
      type: 'corespaces_sold_ownership',
      at: new Date().toISOString(),
      owner: cfg.owner,
      property_manager: cfg.pm,
      source: cfg.source,
      url: cfg.url,
      confidence: cfg.confidence,
      needs_ownership_research: cfg.confidence === 'low',
    };

    const prevSources = Array.isArray(prop.enrichment_sources) ? prop.enrichment_sources : [];
    const nextSources = [
      ...prevSources.filter((s) => s.type !== 'corespaces_sold_ownership'),
      sourceEntry,
      ...prevSources
        .filter((s) => s.type === 'corespaces_prismic')
        .map((s) => ({ ...s, needs_ownership_research: false })),
    ];

    const patch = {
      owner_name: cfg.owner,
      property_manager_name: cfg.pm,
      enrichment_sources: nextSources,
      last_enrichment_at: new Date().toISOString(),
    };

    if (DRY) {
      console.log(`  [UPDATE] owner=${cfg.owner} pm=${cfg.pm}`);
    } else {
      const { error } = await sb.from('property_registry').update(patch).eq('id', prop.id);
      if (error) throw error;
    }
    report.updated++;

    await upsertPropertyStakeholder(sb, prop.id, ownerId, cfg.owner, 'owner');
    if (cfg.pm !== cfg.owner) {
      await upsertPropertyStakeholder(sb, prop.id, pmId, cfg.pm, 'property_manager');
    } else {
      await upsertPropertyStakeholder(sb, prop.id, ownerId, cfg.owner, 'property_manager');
    }
    report.linked += 2;
    report.assignments.push({ property_name: prop.property_name, owner: cfg.owner, pm: cfg.pm, confidence: cfg.confidence });
  }

  const outPath = resolve(ROOT, '.firecrawl/cs-sold-ownership-report.json');
  writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log('\nReport:', JSON.stringify(report, null, 2));
  console.log(`Saved ${outPath}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
