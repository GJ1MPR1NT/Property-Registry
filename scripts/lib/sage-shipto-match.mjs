/**
 * Shared helpers for matching Sage ship-to signals to property_registry rows.
 *
 * Doctrine: ship-to address is always a first-class matching key for
 * project→Sage joins and Sage→property resolution. Call enrichSageOrder()
 * on every sage_orders row before matching.
 */

const WAREHOUSE_LINE1_BLOCKLIST = [
  '101 ACADEMY BLVD',
  '6501 LOWELL BLVD',
  '9305 MERRITT ROAD',
];

/** Warehouse line1 → development label used in property_key bucket 1. */
const WAREHOUSE_DEV_BY_LINE1 = [
  ['101 ACADEMY BLVD', 'WEST OAK BTR'],
  ['6501 LOWELL BLVD', 'CLEAR CREEK BTR'],
  ['9305 MERRITT ROAD', 'ROWLETT BTR'],
];

const CORE_LLC_LINE1 = /^2PR\d+\s+CORE\s+.+\s+LLC$/i;

export function norm(value) {
  if (value == null) return '';
  return String(value)
    .trim()
    .toUpperCase()
    .replace(/\s+/g, ' ')
    .replace(/[.,#]/g, '');
}

export function normZip(value) {
  const z = norm(value).replace(/\D/g, '');
  return z.slice(0, 5);
}

export function isWarehouseShipTo(line1) {
  const n = norm(line1);
  if (!n) return false;
  return WAREHOUSE_LINE1_BLOCKLIST.some((w) => n.includes(norm(w)) || norm(w).includes(n));
}

/** Mirrors DALE-Demand sage_orders.property_key for direct-ship orders. */
export function buildDirectPropertyKey(line1, city, state) {
  const l1 = norm(line1);
  const c = norm(city);
  const st = norm(state);
  if (!l1 || !c || !st) return null;
  return `${l1} · ${c} ${st}`;
}

export function buildAddressKey(line1, city, state, postalCode) {
  const parts = [norm(line1), norm(city), norm(state), normZip(postalCode)].filter(Boolean);
  if (parts.length < 3) return null;
  return parts.join('|');
}

/** Preferred ship-to line1 for direct-ship property_key (Core Spaces LLC ref → line2). */
export function preferredShipToLine1(order) {
  const line1 = order.ship_to_address_line_1;
  const line2 = order.ship_to_address_line_2;
  if (line1 && CORE_LLC_LINE1.test(String(line1).trim()) && line2) {
    return String(line2).trim();
  }
  return line1 ? String(line1).trim() : null;
}

/**
 * Two-bucket property_key derivation (mirrors Vantage sage-pacing-map.ts).
 * Warehouse-routed: `<DEV> · <UNIT>` from ship_name + warehouse line1.
 * Direct-ship: `<line> · <city> <state>`.
 */
export function derivePropertyKeyFromShipTo(order) {
  const line1 = order.ship_to_address_line_1;
  if (isWarehouseShipTo(line1)) {
    return parseWarehousePropertyKey(order.ship_name, line1);
  }
  const addrLine = preferredShipToLine1(order);
  return buildDirectPropertyKey(addrLine, order.ship_to_city, order.ship_to_state);
}

export function parseWarehousePropertyKey(shipName, shipToLine1) {
  if (!shipName) return null;
  const nLine = norm(shipToLine1);
  const dev =
    WAREHOUSE_DEV_BY_LINE1.find(([w]) => nLine.includes(norm(w)))?.[1] ?? null;
  if (!dev) return null;

  const s = String(shipName).trim();
  let unit = '';
  const llcMatch = s.match(/LLC[-\s]+(.+)$/i);
  if (llcMatch) unit = llcMatch[1].trim();
  else if (s.includes('·')) {
    unit = s.split('·').slice(1).join('·').trim();
  } else {
    const dash = s.split('-').pop()?.trim();
    if (dash && dash.length > 3) unit = dash;
  }
  if (!unit) return null;
  return `${norm(dev)} · ${norm(unit)}`;
}

/** Fill computed property_key when the DB column is null. */
export function enrichSageOrder(order) {
  if (!order) return order;
  const existing = order.property_key && String(order.property_key).trim();
  const computed = existing || derivePropertyKeyFromShipTo(order);
  return computed && computed !== existing
    ? { ...order, property_key: computed, _property_key_computed: !existing }
    : order;
}

/** Rank sage rows for dedupe: ship_to richness, property_key, then snapshot date. */
export function sageOrderRank(row) {
  const hasShip = Boolean(row.ship_to_city && String(row.ship_to_city).trim());
  const hasPk = Boolean(row.property_key && String(row.property_key).trim());
  const datePart = row.snapshot_date ? String(row.snapshot_date).replace(/-/g, '') : '0';
  return (hasShip ? 1_000_000_000_000_000 : 0) + (hasPk ? 1_000_000_000_000 : 0) + Number(datePart);
}

export function formatShipToAddress(order) {
  const parts = [
    order.ship_to_address_line_1,
    order.ship_to_address_line_2,
    order.ship_to_city,
    order.ship_to_state,
    order.ship_to_zip,
  ]
    .map((p) => (p || '').trim())
    .filter(Boolean);
  return parts.join(', ');
}

export function shipToAddressKey(order) {
  return buildAddressKey(
    preferredShipToLine1(order) || order.ship_to_address_line_1,
    order.ship_to_city,
    order.ship_to_state,
    order.ship_to_zip,
  );
}

export function dealVariants(raw) {
  if (!raw || typeof raw !== 'string') return [];
  const s = raw.trim().toUpperCase();
  if (!s) return [];
  const out = new Set([s]);
  const base = s.replace(/-I$/i, '').replace(/-D$/i, '');
  if (base !== s) out.add(base);
  const m = s.match(/^(\d{2}-\d{3,4})$/);
  if (m) {
    out.add(`${m[1]}-D`);
    out.add(`${m[1]}-I`);
  }
  return [...out];
}

export function projectJoinKeys(project) {
  const keys = new Set();
  for (const v of dealVariants(project.project_id)) keys.add(v);
  for (const v of dealVariants(project.order_number)) keys.add(v);
  const ext = project.external_ids || {};
  for (const v of dealVariants(ext.deal_number)) keys.add(v);
  if (Array.isArray(ext.deal_numbers)) {
    for (const d of ext.deal_numbers) for (const v of dealVariants(String(d))) keys.add(v);
  }
  return keys;
}

export function sageJoinKeys(order) {
  const keys = new Set();
  for (const v of dealVariants(order.order_number)) keys.add(v);
  for (const v of dealVariants(order.reference)) keys.add(v);
  const ref = order.reference || '';
  const dealMatch = ref.match(/DEAL\s+(\d+)-(\d{4})/i);
  if (dealMatch) {
    const yy = dealMatch[2].slice(-2);
    keys.add(`${yy}-${dealMatch[1]}-D`);
    keys.add(`${yy}-${dealMatch[1]}-I`);
  }
  return keys;
}

export function warehouseKeyTokens(propertyKey) {
  if (!propertyKey) return [];
  const [devPart, unitPart] = propertyKey.split('·').map((s) => s.trim());
  const tokens = [];
  if (devPart) tokens.push(norm(devPart));
  if (unitPart) tokens.push(norm(unitPart));
  return tokens.filter(Boolean);
}

export function scoreNameOverlap(needle, haystack) {
  const a = norm(needle);
  const b = norm(haystack);
  if (!a || !b) return 0;
  if (a === b) return 1;
  if (b.includes(a) || a.includes(b)) return 0.92;
  const aWords = a.split(' ').filter((w) => w.length > 2);
  if (aWords.length === 0) return 0;
  const hits = aWords.filter((w) => b.includes(w)).length;
  return hits / aWords.length;
}

export function buildSageIndexes(orders) {
  const byKey = new Map();
  const byOrderNumber = new Map();
  const byAddressKey = new Map();
  const byPropertyKey = new Map();

  for (const raw of orders) {
    const o = enrichSageOrder(raw);
    byOrderNumber.set(norm(o.order_number), o);
    for (const k of sageJoinKeys(o)) {
      if (!byKey.has(k)) byKey.set(k, []);
      byKey.get(k).push(o);
    }
    const ak = shipToAddressKey(o);
    if (ak) {
      if (!byAddressKey.has(ak)) byAddressKey.set(ak, []);
      byAddressKey.get(ak).push(o);
    }
    if (o.property_key) {
      const pk = norm(o.property_key);
      if (!byPropertyKey.has(pk)) byPropertyKey.set(pk, []);
      byPropertyKey.get(pk).push(o);
    }
  }
  return { byKey, byOrderNumber, byAddressKey, byPropertyKey };
}

const BRAND_TO_DIVISION = {
  UF: 'UF',
  CSL: 'CSL',
  TLCH: 'TLCH',
  CSMX: 'CSMX',
  BSI: 'BSI',
  Web: 'UF',
};

/**
 * Join project_registry → sage_orders.
 * Priority: deal keys → ship-to address (linked property or external_ids) → name.
 */
export function findSageOrdersForProject(project, sageIndexes, sageOrders) {
  const { byKey, byOrderNumber, byAddressKey, byPropertyKey } = sageIndexes;
  const seen = new Map();

  for (const k of projectJoinKeys(project)) {
    for (const o of byKey.get(k) || []) seen.set(o.order_number, o);
  }

  const ext = project.external_ids || {};
  for (const raw of [ext.deal_number, ...(Array.isArray(ext.deal_numbers) ? ext.deal_numbers : [])]) {
    for (const v of dealVariants(String(raw || ''))) {
      const o = byOrderNumber.get(v);
      if (o) seen.set(o.order_number, o);
    }
  }

  // Ship-to address join via linked property
  if (project._property) {
    const p = project._property;
    const ak = buildAddressKey(p.address_line1, p.city, p.state_province, p.postal_code);
    if (ak) {
      for (const o of byAddressKey.get(ak) || []) seen.set(o.order_number, o);
    }
    const pk = buildDirectPropertyKey(p.address_line1, p.city, p.state_province);
    if (pk) {
      for (const o of byPropertyKey.get(norm(pk)) || []) seen.set(o.order_number, o);
    }
  }

  // Ship-to hints stored on project external_ids (from prior ingest / archaeology)
  for (const hint of [ext.sage_ship_to_address, ext.ship_to_address, ext.normalized_ship_to_address]) {
    if (!hint || typeof hint !== 'string') continue;
    const hintNorm = norm(hint);
    for (const o of sageOrders) {
      const ship = formatShipToAddress(o);
      if (ship && norm(ship) === hintNorm) seen.set(o.order_number, o);
    }
  }

  const division = BRAND_TO_DIVISION[project.brand] || project.brand;
  const namePool =
    division != null
      ? sageOrders.filter((o) => !o.division || o.division === division)
      : sageOrders;

  if (seen.size === 0) {
    const scored = namePool
      .map((o) => ({ o, score: scoreNameOverlap(project.project_name, o.ship_name) }))
      .filter((x) => x.score >= 0.72)
      .sort((a, b) => b.score - a.score);
    if (scored.length) {
      const best = scored[0].score;
      for (const { o, score } of scored) {
        if (score < best - 0.08) break;
        seen.set(o.order_number, o);
        if (seen.size >= 8) break;
      }
    }
  }

  return [...seen.values()];
}

export function pickUnique(candidates, method) {
  if (!candidates?.length) return { property: null, confidence: 0, method, ambiguous: false };
  if (candidates.length === 1) {
    return { property: candidates[0], confidence: method.confidence, method: method.name, ambiguous: false };
  }
  return { property: null, confidence: 0, method: method.name, ambiguous: true, count: candidates.length };
}

/**
 * Match enriched Sage order → property_registry.
 * Ship-to address tiers run before fuzzy name-only fallback.
 */
export function matchPropertyForSage(order, indexes, properties) {
  const o = enrichSageOrder(order);
  const warehouse = isWarehouseShipTo(o.ship_to_address_line_1);

  if (o.property_key) {
    const pk = norm(o.property_key);
    const direct = indexes.byPropertyKey.get(pk);
    if (direct?.length === 1) {
      return { property: direct[0], confidence: 100, method: 'sage_property_key_exact', ambiguous: false };
    }
    if (direct?.length > 1) {
      return { property: null, confidence: 0, method: 'sage_property_key_exact', ambiguous: true, count: direct.length };
    }
  }

  if (!warehouse) {
    const ak = shipToAddressKey(o);
    if (ak) {
      const addrHits = indexes.byAddressKey.get(ak);
      const r = pickUnique(addrHits, { name: 'ship_to_address_exact', confidence: 98 });
      if (r.property) return r;
      if (r.ambiguous) return r;
    }

    const pk = derivePropertyKeyFromShipTo(o);
    if (pk) {
      const pkHits = indexes.byPropertyKey.get(norm(pk));
      const r = pickUnique(pkHits, { name: 'computed_property_key', confidence: 97 });
      if (r.property) return r;
      if (r.ambiguous) return r;
    }
  }

  if (warehouse && o.property_key) {
    const tokens = warehouseKeyTokens(o.property_key);
    if (tokens.length >= 2) {
      const hits = properties.filter((p) => {
        const name = norm(p.property_name);
        return tokens.every((t) => name.includes(t) || t.includes(name));
      });
      const r = pickUnique(hits, { name: 'warehouse_property_key_tokens', confidence: 92 });
      if (r.property) return r;
      if (r.ambiguous) return r;
    }
  }

  const dealKey = norm(o.order_number);
  if (dealKey) {
    const dealHits = indexes.byDeal.get(dealKey);
    const r = pickUnique(dealHits, { name: 'deal_number_external_ids', confidence: 90 });
    if (r.property) return r;
    if (r.ambiguous) return r;
  }

  const nameNeedle = o.ship_name || '';
  if (nameNeedle) {
    const cs = `${norm(o.ship_to_city)}|${norm(o.ship_to_state)}`;
    const pool = indexes.byCityState.get(cs) || properties;
    const scored = pool
      .map((p) => ({ p, score: scoreNameOverlap(nameNeedle, p.property_name) }))
      .filter((x) => x.score >= 0.75)
      .sort((a, b) => b.score - a.score);
    if (scored.length === 1) {
      return {
        property: scored[0].p,
        confidence: Math.round(scored[0].score * 85),
        method: 'ship_name_city_state',
        ambiguous: false,
      };
    }
    if (scored.length > 1 && scored[0].score - scored[1].score >= 0.15) {
      return {
        property: scored[0].p,
        confidence: Math.round(scored[0].score * 80),
        method: 'ship_name_city_state_best',
        ambiguous: false,
      };
    }
    if (scored.length > 1) {
      return { property: null, confidence: 0, method: 'ship_name_city_state', ambiguous: true, count: scored.length };
    }
  }

  return { property: null, confidence: 0, method: null, ambiguous: false };
}

export function buildPropertyIndexes(properties) {
  const byPropertyKey = new Map();
  const byAddressKey = new Map();
  const byDeal = new Map();
  const byCityState = new Map();

  for (const p of properties) {
    const pk = buildDirectPropertyKey(p.address_line1, p.city, p.state_province);
    if (pk) {
      const n = norm(pk);
      if (!byPropertyKey.has(n)) byPropertyKey.set(n, []);
      byPropertyKey.get(n).push(p);
    }
    const ak = buildAddressKey(p.address_line1, p.city, p.state_province, p.postal_code);
    if (ak) {
      if (!byAddressKey.has(ak)) byAddressKey.set(ak, []);
      byAddressKey.get(ak).push(p);
    }
    const ext = p.external_ids || {};
    const deals = [];
    if (ext.deal_number) deals.push(String(ext.deal_number));
    if (Array.isArray(ext.deal_numbers)) deals.push(...ext.deal_numbers.map(String));
    for (const d of deals) {
      const k = norm(d);
      if (!k) continue;
      if (!byDeal.has(k)) byDeal.set(k, []);
      byDeal.get(k).push(p);
    }
    const cs = `${norm(p.city)}|${norm(p.state_province)}`;
    if (cs !== '|') {
      if (!byCityState.has(cs)) byCityState.set(cs, []);
      byCityState.get(cs).push(p);
    }
  }
  return { byPropertyKey, byAddressKey, byDeal, byCityState };
}
