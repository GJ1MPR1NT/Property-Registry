/**
 * Firecrawl web resolve for property install site addresses.
 *
 * Used before Sage ↔ property matching and site_address denorm when
 * address_line1 is missing, TBD, or a non-street placeholder (LLC / ATTN).
 */

import { buildAddressKey, norm, normZip } from './sage-shipto-match.mjs';

const FIRECRAWL_BASE = 'https://api.firecrawl.dev/v1';
const EMPTY_ADDRESS = new Set(['', 'TBD', 'UNKNOWN', 'N/A', 'NA', 'NULL', 'NONE']);
const LLC_LINE1 = /^2PR\d+\s+CORE\s+.+\s+LLC$/i;
const ATTN_ONLY = /^(ATTN|ATTENTION|C\/O|CARE OF)\b/i;

const US_STREET =
  /\b(\d{1,5}[\s½/\-]*(?:\d{1,4})?\s+(?:[NSEW]\.?\s+)?[\w\s.'-]{2,40}?\s+(?:Street|St\.?|Avenue|Ave\.?|Road|Rd\.?|Boulevard|Blvd\.?|Drive|Dr\.?|Lane|Ln\.?|Way|Court|Ct\.?|Place|Pl\.?|Parkway|Pkwy\.?|Circle|Cir\.?|Trail|Trl\.?|Highway|Hwy\.?))(?:[,\s]+([^,]+?))?(?:[,\s]+([A-Z]{2}))?(?:[,\s]+(\d{5}(?:-\d{4})?))?/gi;

const JUNK_LINE = /per acre|\bacre|bedroom|zoning|presentation|applicant|square feet|height\d|handle=|million|expertise/i;
const STREET_SUFFIX =
  /^(Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Drive|Dr|Lane|Ln|Way|Court|Ct|Place|Pl|Parkway|Pkwy)\.?$/i;

export function isPlausibleStreetLine(line1) {
  if (!line1 || JUNK_LINE.test(line1)) return false;
  const m = line1.trim().match(/^(\d{1,5}[\s½/\-]*)\s+(.+?)\s+([A-Za-z.]+)\.?$/i);
  if (!m) return false;
  const streetName = m[2].trim();
  const suffix = m[3].trim();
  if (!STREET_SUFFIX.test(suffix)) return false;
  const words = streetName.split(/\s+/);
  if (words.length < 1 || words.length > 6) return false;
  if (/\d{2,}/.test(streetName)) return false;
  if (!/^[A-Za-z0-9\s.'½-]+$/i.test(streetName)) return false;
  if (/\b(million|expertise|acres|bedroom|roadmap|cookie)\b/i.test(streetName)) return false;
  return true;
}

export function cleanPropertyName(name) {
  if (!name) return '';
  return String(name)
    .trim()
    .replace(/^['']?\d{2}\s+/, '')
    .replace(/^["']+|["']+$/g, '')
    .trim();
}

export function needsWebSiteAddressResolve(prop) {
  if (!prop) return false;
  const line1 = (prop.address_line1 || '').trim();
  if (!line1 || EMPTY_ADDRESS.has(line1.toUpperCase())) return true;
  if (LLC_LINE1.test(line1)) return true;
  if (ATTN_ONLY.test(line1)) return true;
  if (line1.length < 6 && !/\d/.test(line1)) return true;
  return false;
}

export function buildFirecrawlQueries(prop, project = null) {
  const name = cleanPropertyName(prop.property_name || project?.project_name || '');
  const city =
    prop.city && !EMPTY_ADDRESS.has(String(prop.city).toUpperCase()) ? prop.city.trim() : '';
  const state =
    prop.state_province && !EMPTY_ADDRESS.has(String(prop.state_province).toUpperCase())
      ? prop.state_province.trim()
      : '';
  const loc = [city, state].filter(Boolean).join(' ');
  const dev = prop.developer_name || prop.owner_name || null;
  const queries = [];

  if (name && loc) {
    queries.push(`"${name}" ${loc} address student housing`);
    queries.push(`${name} ${loc} site address opening`);
  }
  if (project?.project_name && loc) {
    queries.push(`"${project.project_name}" ${loc} address`);
  }
  if (dev && name && loc) {
    queries.push(`"${dev}" "${name}" ${loc} groundbreaking`);
  }
  if (/hub/i.test(name) && city) {
    const hubCity = city.replace(/\s+/g, ' ');
    queries.push(`site:huboncampus.com ${hubCity}`);
    queries.push(`"${name}" ${hubCity} hub on campus address`);
  }
  if (prop.property_url) {
    try {
      const host = new URL(prop.property_url).hostname;
      queries.push(`site:${host} contact address`);
    } catch {
      /* ignore */
    }
  }

  const seen = new Set();
  return queries.filter((q) => {
    const k = q.toLowerCase();
    if (seen.has(k)) return false;
    seen.add(k);
    return true;
  });
}

export async function firecrawlSearch(apiKey, query, { limit = 3, scrape = true } = {}) {
  const body = { query, limit };
  if (scrape) body.scrapeOptions = { formats: ['markdown'] };
  const res = await fetch(`${FIRECRAWL_BASE}/search`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${apiKey}` },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Firecrawl search ${res.status}: ${text.slice(0, 200)}`);
  }
  const json = await res.json();
  return json.data || [];
}

function extractAddressesFromMarkdown(md, hint = {}) {
  if (!md) return [];
  const found = [];
  const text = md.replace(/\s+/g, ' ');
  let m;
  US_STREET.lastIndex = 0;
  while ((m = US_STREET.exec(text)) !== null) {
    const line1 = m[1].replace(/\s+/g, ' ').trim();
    const city = (m[2] || hint.city || '').trim();
    const state = (m[3] || hint.state || '').trim().toUpperCase();
    const postal = (m[4] || '').trim();
    if (line1.length < 8) continue;
    if (/roadmap|smartmaps|cookieyes/i.test(line1)) continue;
    if (/\bsf\b|sq\.?\s*ft|square\s+feet|height\d|page\s+\d/i.test(line1)) continue;
    if (!/^\d{1,5}([\s½/]|\.)/.test(line1)) continue;
    if (!/[a-z]/i.test(line1)) continue;
    if (!isPlausibleStreetLine(line1)) continue;
    found.push({ address_line1: line1, city, state_province: state, postal_code: postal });
  }
  return found;
}

function scoreCandidate(candidate, prop, sourceUrl = '') {
  let score = 0.4;
  const hintCity = norm(prop.city);
  const hintState = norm(prop.state_province);
  const cCity = norm(candidate.city);
  const cState = norm(candidate.state_province);

  if (hintState && cState) {
    if (cState === hintState) score += 0.25;
    else return 0;
  }
  if (hintCity && cCity) {
    if (cCity === hintCity || cCity.includes(hintCity) || hintCity.includes(cCity)) score += 0.2;
    else if (hintCity && cCity) score -= 0.15;
  }

  const nameTokens = cleanPropertyName(prop.property_name)
    .toLowerCase()
    .split(/\s+/)
    .filter((w) => w.length > 3);
  const urlL = (sourceUrl || '').toLowerCase();
  if (/huboncampus|corespaces|student|housing|multifamily|apartments/i.test(urlL)) score += 0.1;

  const line = candidate.address_line1.toLowerCase();
  if (/\d/.test(line) && !ATTN_ONLY.test(line) && !LLC_LINE1.test(candidate.address_line1)) score += 0.15;

  const propName = cleanPropertyName(prop.property_name).toLowerCase();
  if (propName && nameTokens.some((t) => urlL.includes(t) || line.includes(t))) score += 0.05;

  return Math.min(1, Math.max(0, score));
}

function pickBestCandidate(candidates, prop) {
  let best = null;
  for (const c of candidates) {
    const scored = { ...c, confidence: scoreCandidate(c, prop, c.source_url) };
    if (!best || scored.confidence > best.confidence) best = scored;
  }
  return best;
}

function parseLlmAddressJson(text) {
  if (!text) return null;
  let cleaned = text.trim();
  const start = cleaned.indexOf('{');
  const end = cleaned.lastIndexOf('}');
  if (start >= 0 && end > start) cleaned = cleaned.slice(start, end + 1);
  try {
    return JSON.parse(cleaned);
  } catch {
    return null;
  }
}

const LLM_PROMPT = `You extract the physical INSTALL/SITE street address for a construction or student-housing property.
Use ONLY addresses explicitly stated in the content. Never invent.
If multiple addresses appear, prefer: (1) official property or developer project page, (2) press release about this specific project, (3) contact/footer on property website.
Ignore corporate HQs, leasing offices for other properties, and warehouse addresses.

Return ONLY JSON:
{
  "address_line1": "string|null",
  "city": "string|null",
  "state_province": "two-letter|null",
  "postal_code": "string|null",
  "property_url": "string|null",
  "confidence": 0.0,
  "rationale": "one sentence"
}`;

export async function extractAddressWithLlm(pages, prop, apiKey) {
  const excerpt = pages
    .slice(0, 4)
    .map((p) => `URL: ${p.url}\n${(p.markdown || p.description || '').slice(0, 6000)}`)
    .join('\n\n---\n\n');
  const res = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': apiKey,
      'anthropic-version': '2023-06-01',
    },
    body: JSON.stringify({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 600,
      messages: [
        {
          role: 'user',
          content: `${LLM_PROMPT}\n\nProperty: ${prop.property_name}\nExpected city/state: ${prop.city || '?'}, ${prop.state_province || '?'}\n\nContent:\n${excerpt.slice(0, 24000)}`,
        },
      ],
    }),
  });
  if (!res.ok) throw new Error(`Anthropic ${res.status}`);
  const json = await res.json();
  const text = json.content?.find((c) => c.type === 'text')?.text || '';
  const parsed = parseLlmAddressJson(text);
  if (!parsed?.address_line1) return null;
  return {
    address_line1: parsed.address_line1,
    city: parsed.city || prop.city,
    state_province: parsed.state_province || prop.state_province,
    postal_code: parsed.postal_code || null,
    property_url: parsed.property_url || pages[0]?.url || null,
    confidence: Number(parsed.confidence) || 0.7,
    rationale: parsed.rationale || 'llm',
    method: 'firecrawl_llm',
  };
}

/**
 * @param {object} prop property_registry row
 * @param {object} [opts]
 * @param {object} [opts.project] linked project_registry row
 * @param {boolean} [opts.useLlm=true]
 * @param {number} [opts.searchLimit=3]
 * @param {number} [opts.maxQueries=3]
 */
export async function resolveSiteAddressWeb(prop, opts = {}) {
  const apiKey = opts.firecrawlApiKey || process.env.FIRECRAWL_API_KEY;
  if (!apiKey) throw new Error('FIRECRAWL_API_KEY required for web resolve');

  if (!needsWebSiteAddressResolve(prop)) {
    return { skipped: true, reason: 'address already present' };
  }

  const queries = buildFirecrawlQueries(prop, opts.project).slice(0, opts.maxQueries ?? 3);
  const pages = [];
  const seenUrl = new Set();

  for (const query of queries) {
    const results = await firecrawlSearch(apiKey, query, {
      limit: opts.searchLimit ?? 3,
      scrape: true,
    });
    for (const r of results) {
      const url = r.url || r.metadata?.sourceURL;
      if (!url || seenUrl.has(url)) continue;
      seenUrl.add(url);
      pages.push({
        url,
        title: r.title,
        markdown: r.markdown || '',
        description: r.description || '',
        query,
      });
    }
    if (opts.delayMs) await sleep(opts.delayMs);
    if (pages.length >= 6) break;
  }

  if (!pages.length) {
    return { skipped: true, reason: 'no search results', queries };
  }

  const hint = { city: prop.city, state: prop.state_province };
  const regexCandidates = [];
  for (const p of pages) {
    for (const c of extractAddressesFromMarkdown(p.markdown || p.description, hint)) {
      regexCandidates.push({ ...c, source_url: p.url, method: 'firecrawl_regex' });
    }
  }

  const anthropicKey = opts.anthropicApiKey || process.env.ANTHROPIC_API_KEY;
  let best = null;

  if (opts.useLlm !== false && anthropicKey) {
    try {
      const llm = await extractAddressWithLlm(pages, prop, anthropicKey);
      if (llm?.address_line1 && isPlausibleStreetLine(llm.address_line1)) {
        best = { ...llm, source_url: llm.property_url || pages[0]?.url };
      }
    } catch (e) {
      if (opts.verbose) console.warn('  LLM extract failed:', e.message);
    }
  }

  if (!best && opts.useLlm === false) {
    best = pickBestCandidate(regexCandidates, prop);
    if (best && !isPlausibleStreetLine(best.address_line1)) best = null;
  }

  if (!best || best.confidence < (opts.minConfidence ?? 0.72)) {
    return {
      skipped: true,
      reason: 'no confident address',
      queries,
      pages: pages.map((p) => p.url),
      topConfidence: best?.confidence ?? 0,
    };
  }

  const updates = {
    address_line1: best.address_line1,
  };
  if (best.city && (!prop.city || EMPTY_ADDRESS.has(String(prop.city).toUpperCase()))) {
    updates.city = best.city;
  }
  if (best.state_province) updates.state_province = best.state_province;
  if (best.postal_code) updates.postal_code = best.postal_code;
  if (best.property_url && !prop.property_url) updates.property_url = best.property_url;

  const ak = buildAddressKey(
    updates.address_line1,
    updates.city || prop.city,
    updates.state_province || prop.state_province,
    updates.postal_code || prop.postal_code,
  );

  return {
    updates,
    confidence: best.confidence,
    method: best.method || 'firecrawl_regex',
    source_url: best.source_url,
    queries,
    pages: pages.map((p) => p.url),
    address_key: ak,
    rationale: best.rationale,
  };
}

export function buildEnrichmentSourceEntry(result) {
  return {
    type: 'site_address_firecrawl',
    at: new Date().toISOString(),
    confidence: result.confidence,
    method: result.method,
    source_url: result.source_url,
    queries: result.queries,
    pages: result.pages,
    address_key: result.address_key,
    rationale: result.rationale,
  };
}

export async function applySiteAddressResolve(registry, prop, result) {
  if (!result?.updates) return { applied: false };
  const prev = Array.isArray(prop.enrichment_sources) ? prop.enrichment_sources : [];
  const nextSources = [...prev, buildEnrichmentSourceEntry(result)];
  const { error } = await registry
    .from('property_registry')
    .update({ ...result.updates, enrichment_sources: nextSources })
    .eq('id', prop.id);
  if (error) throw new Error(error.message);
  Object.assign(prop, result.updates, { enrichment_sources: nextSources });
  return { applied: true };
}

/**
 * Enrich in-memory property rows used by matching indexes (Sage, containers).
 */
export async function enrichPropertiesForMatching(registry, properties, opts = {}) {
  const limit = opts.limit ?? 50;
  const minConfidence = opts.minConfidence ?? 0.72;
  const toResolve = properties.filter(needsWebSiteAddressResolve).slice(0, limit);
  const stats = { attempted: 0, resolved: 0, skipped: 0, errors: 0 };

  for (const prop of toResolve) {
    stats.attempted++;
    try {
      const result = await resolveSiteAddressWeb(prop, {
        ...opts,
        minConfidence,
        delayMs: opts.delayMs ?? 400,
      });
      if (result.skipped) {
        stats.skipped++;
        if (opts.verbose) console.log(`  skip ${prop.property_name}: ${result.reason}`);
        continue;
      }
      if (opts.apply && registry) {
        await applySiteAddressResolve(registry, prop, result);
      } else {
        Object.assign(prop, result.updates);
      }
      stats.resolved++;
      if (opts.verbose) {
        console.log(
          `  resolved ${prop.property_name} → ${result.updates.address_line1} (${(result.confidence * 100).toFixed(0)}%)`,
        );
      }
    } catch (e) {
      stats.errors++;
      if (opts.verbose) console.warn(`  error ${prop.property_name}:`, e.message);
    }
  }
  return stats;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
