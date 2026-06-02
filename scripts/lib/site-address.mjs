/**
 * Format Registry-iQ property rows into a single site_address string.
 */

import { resolveProject, norm } from './container-destination-guard.mjs';

export function formatSiteAddress(prop) {
  if (!prop) return null;
  const parts = [];
  if (prop.address_line1?.trim()) parts.push(prop.address_line1.trim());
  if (prop.address_line2?.trim()) parts.push(prop.address_line2.trim());

  const city = prop.city?.trim();
  const state = prop.state_province?.trim();
  const postal = prop.postal_code?.trim();
  const cityState = [city, state].filter(Boolean).join(', ');
  const cityLine = [cityState, postal].filter(Boolean).join(' ');
  if (cityLine) parts.push(cityLine);

  return parts.length ? parts.join(', ') : null;
}

/** Parse site_address back to components for destination guard matching. */
export function parseSiteAddress(raw) {
  if (!raw?.trim()) return null;
  const s = raw.trim();
  const m = s.match(/^(.+),\s*([^,]+),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$/i);
  if (m) {
    return {
      line1: m[1].trim(),
      city: m[2].trim(),
      state: m[3].trim().toUpperCase(),
      postal: m[4].trim().slice(0, 5),
    };
  }
  return { line1: s, city: '', state: '', postal: '' };
}

/** Stricter project match for site_address writes (avoids false CSL/SO blanket hits). */
export function resolveProjectStrict(projectName, ctx, row = {}) {
  const n = norm(projectName);
  if (!n) return null;
  if (ctx.projectByName.has(n)) return ctx.projectByName.get(n);

  const deal = String(projectName || '').match(/(\d{2}-\d{3,4}-[ID])/i);
  if (deal) {
    const dk = norm(deal[1]);
    if (ctx.projectByName.has(dk)) return ctx.projectByName.get(dk);
    for (const [, proj] of ctx.projectByName) {
      const pid = norm(proj.project_id);
      if (pid === dk || pid.startsWith(`${dk} `) || pid.startsWith(`${dk}-`)) return proj;
    }
  }

  const lead = n.match(/^(\d{4,5})[\s-]/);
  if (lead) {
    for (const [, proj] of ctx.projectByName) {
      const pn = norm(proj.project_name);
      const pid = norm(proj.project_id);
      if (pn.includes(lead[1]) || pid.includes(lead[1])) return proj;
    }
  }

  return resolveProject(projectName, ctx, row);
}
