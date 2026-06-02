/**
 * Container destination guard — hold inbound containers whose destination does not
 * resolve to a known Registry-iQ property site or warehouse.
 *
 * Used by scripts/enforce-container-destination-guard.mjs
 */

/** TLC-operated / approved staging warehouses (not always in warehouse_registry yet). */
export const CANONICAL_WAREHOUSES = [
  { label: 'BSI Brighton TN staging', city: 'BRIGHTON', state: 'TN', postal: '38011', line1: '' },
  { label: 'UF WEST OAK BTR', city: 'FORT WORTH', state: 'TX', postal: '76114', line1: '101 ACADEMY BLVD' },
  { label: 'UF CLEAR CREEK BTR', city: 'ARVADA', state: 'CO', postal: '80003', line1: '6501 LOWELL BLVD' },
  { label: 'UF Garland TX warehouse', city: 'GARLAND', state: 'TX', postal: '75041', line1: '' },
  { label: 'UF ROWLETT BTR', city: 'ROWLETT', state: 'TX', postal: '75088', line1: '9305 MERRITT ROAD' },
];

export const HOLD_STATUS2 = 'HOLD:UNKNOWN_DEST';

/** Statuses where a bad destination should block movement (incoming / active). */
export const INBOUND_GUARD_STATUSES = new Set([
  'SHIPPED',
  'LOADED',
  'ARRIVED',
  'AVAILABLE',
  'PICKED',
  'IN TRANSIT',
  'IN_TRANSIT',
  'AT PORT',
  'AT_PORT',
  'BOOKED',
]);

export const TERMINAL_STATUSES = new Set(['DELIVERED', 'CANCELLED', 'CANCELED', 'EMPTY RETURNED']);

export function norm(value) {
  if (value == null) return '';
  return String(value)
    .replace(/\u00a0/g, ' ')
    .trim()
    .toUpperCase()
    .replace(/\s+/g, ' ')
    .replace(/[.,#]/g, '');
}

export function normZip(value) {
  return norm(value).replace(/\D/g, '').slice(0, 5);
}

/** Parse free-text destination like "Bloomington IN 47408" or "Brighton, TN 38011". */
export function parseDestination(raw) {
  const s = norm(raw);
  if (!s) return null;

  // City ST ZIP (optional comma)
  let m = s.match(/^(.+?)[,\s]+([A-Z]{2})\s+(\d{5})(?:\b|$)/);
  if (m) {
    return { raw: s, city: m[1].trim(), state: m[2], postal: m[3], line1: '' };
  }

  m = s.match(/^(.+?)[,\s]+([A-Z]{2})\b/);
  if (m) {
    return { raw: s, city: m[1].trim(), state: m[2], postal: '', line1: '' };
  }

  return { raw: s, city: s, state: '', postal: '', line1: '' };
}

export function destinationKey({ city, state, postal, line1 }) {
  return [norm(line1), norm(city), norm(state), normZip(postal)].filter(Boolean).join('|');
}

export function buildPropertyKeys(prop) {
  if (!prop) return [];
  const keys = new Set();
  keys.add(
    destinationKey({
      line1: prop.address_line1,
      city: prop.city,
      state: prop.state_province,
      postal: prop.postal_code,
    }),
  );
  if (prop.city && prop.state_province) {
    keys.add(destinationKey({ line1: '', city: prop.city, state: prop.state_province, postal: prop.postal_code }));
    keys.add(destinationKey({ line1: '', city: prop.city, state: prop.state_province, postal: '' }));
  }
  return [...keys].filter(Boolean);
}

export function buildWarehouseKeys(wh) {
  const city = wh.city;
  const state = wh.state_province || wh.state;
  const postal = wh.postal_code || wh.postal;
  const line1 = wh.address_line1 || wh.line1;
  return [
    destinationKey({ line1, city, state, postal }),
    destinationKey({ line1: '', city, state, postal }),
    destinationKey({ line1: '', city, state, postal: '' }),
  ].filter(Boolean);
}

export function cityStateKey(city, state) {
  const c = norm(city);
  const st = norm(state);
  if (!c || !st) return '';
  return `${c}|${st}`;
}

export function destinationMatches(parsed, allowedKeys) {
  if (!parsed) return false;
  const candidates = [
    destinationKey(parsed),
    destinationKey({ ...parsed, line1: '' }),
    destinationKey({ ...parsed, postal: '' }),
  ].filter(Boolean);
  if (candidates.some((k) => allowedKeys.has(k))) return true;

  const cs = cityStateKey(parsed.city, parsed.state);
  if (!cs) return false;
  for (const k of allowedKeys) {
    const parts = k.split('|');
    if (parts.length >= 2) {
      const pk = cityStateKey(parts[parts.length - 3] || parts[0], parts[parts.length - 2]);
      // keys are line1|city|state|zip — city/state are last two or three segments
      const city = parts.length >= 3 ? parts[parts.length - 3] : parts[0];
      const state = parts.length >= 2 ? parts[parts.length - 2] : '';
      if (cityStateKey(city, state) === cs) return true;
    }
  }
  return false;
}

export function projectMatchTerms(projectName) {
  const n = norm(projectName);
  if (!n) return [];
  const terms = new Set([n]);
  const lead = n.match(/^(\d{4,5})\b/);
  if (lead) terms.add(lead[1]);
  if (n.includes('HUB') && n.includes('BLOOMINGTON')) {
    terms.add('BLOOMINGTON IN HUB II');
    terms.add('BLOOMINGTON, IN - HUB II');
    terms.add('25326');
  }
  return [...terms];
}

export function shouldGuardContainer(row) {
  const status = norm(row.status);
  if (TERMINAL_STATUSES.has(status)) return false;
  const dest = (row.destination || row.final_destination || '').trim();
  if (!dest) return false;
  if (norm(row.status2) === norm(HOLD_STATUS2)) return false;

  const cn = (row.container_number || '').trim();
  const activeTransit = new Set([
    'SHIPPED',
    'LOADED',
    'ARRIVED',
    'AVAILABLE',
    'PICKED',
    'IN TRANSIT',
    'IN_TRANSIT',
    'AT PORT',
    'AT_PORT',
  ]);

  if (cn) return activeTransit.has(status) || status === 'BOOKED';
  return activeTransit.has(status);
}

export function resolveProject(projectName, ctx, row = {}) {
  const n = norm(projectName);
  if (!n) return null;

  if (ctx.projectByName.has(n)) return ctx.projectByName.get(n);

  // CSL furniture SO lines (Hub I Bloomington only) — never blanket-match all SO/CSL rows.
  const isCslOrSo = norm(row.vendor) === 'CSL' || /^SO\s+\d/.test(String(projectName || ''));
  const isHubIBloomington =
    n.includes('BLOOMINGTON') && n.includes('HUB') && !n.includes('II') && !n.includes('HUB 2');
  if (isCslOrSo && isHubIBloomington) {
    for (const [, proj] of ctx.projectByName) {
      if (norm(proj.project_id).startsWith('25-007')) return proj;
      const pn = norm(proj.project_name);
      if (pn.includes('HUB') && pn.includes('BLOOMINGTON') && !pn.includes('II') && !pn.includes('2')) {
        return proj;
      }
    }
  }

  // CSMX / BSI Hub II streams (25126, 25148, …).
  if (n.includes('BLOOMINGTON') && n.includes('HUB') && (n.includes('II') || n.includes('HUB 2') || n.includes('HUB2'))) {
    for (const [, proj] of ctx.projectByName) {
      const pn = norm(proj.project_name);
      if (pn.includes('BLOOMINGTON') && pn.includes('HUB') && (pn.includes('II') || pn.includes('2'))) {
        return proj;
      }
    }
  }

  // Generic substring — prefer linked properties first.
  let fallback = null;
  for (const [key, proj] of ctx.projectByName) {
    if (key.length > 8 && (n.includes(key) || key.includes(n))) {
      if (proj.property_id) return proj;
      if (!fallback) fallback = proj;
    }
  }
  return fallback;
}

/**
 * @returns {{ ok: boolean, reason?: string, parsed?: object, projectId?: string, propertyId?: string }}
 */
export function evaluateContainerDestination(row, ctx) {
  if (!shouldGuardContainer(row)) return { ok: true, skipped: true };

  const parsed = parseDestination(row.destination || row.final_destination);
  if (!parsed) return { ok: true, skipped: true };

  const globalAllowed = new Set(ctx.globalAllowedKeys);
  if (destinationMatches(parsed, globalAllowed)) {
    return { ok: true, parsed, scope: 'global_warehouse' };
  }

  const project = resolveProject(row.project_name, ctx, row);
  if (project) {
    const scoped = new Set(globalAllowed);
    for (const k of project.allowedKeys || []) scoped.add(k);
    if (destinationMatches(parsed, scoped)) {
      return {
        ok: true,
        parsed,
        scope: 'project_property',
        projectId: project.id,
        propertyId: project.property_id,
      };
    }
    return {
      ok: false,
      parsed,
      reason: `Destination "${row.destination}" does not match project property or known warehouses`,
      projectId: project.id,
      propertyId: project.property_id,
    };
  }

  return {
    ok: false,
    parsed,
    reason: `Destination "${row.destination}" does not match any known warehouse (project "${row.project_name}" not linked in Registry)`,
  };
}

export function holdNote(reason, reviewer = 'container-destination-guard') {
  const ts = new Date().toISOString().slice(0, 19) + 'Z';
  return `[DEST HOLD ${ts} by ${reviewer}] ${reason}`;
}
