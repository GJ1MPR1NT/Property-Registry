#!/usr/bin/env node
/**
 * Batch RITA enrichment for TLCH hospitality properties.
 *
 * Pipeline per property:
 *   1. Firecrawl search + scrape (up to 5 results)
 *   2. Claude analysis → structured JSON
 *   3. Cloudinary hero-image upload
 *   4. Supabase PATCH (only fills empty/null fields, preserves existing data)
 *   5. Activity log entry
 */

import { readFileSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';
import { createRequire } from 'module';

const daleChatDir = resolve(dirname(fileURLToPath(import.meta.url)), '../../Derived State/dale-chat');
const require = createRequire(resolve(daleChatDir, 'node_modules', '.package-lock.json'));

const { createClient } = require('@supabase/supabase-js');
const Anthropic = require('@anthropic-ai/sdk').default || require('@anthropic-ai/sdk');
const { v2: cloudinary } = require('cloudinary');

const __dirname = dirname(fileURLToPath(import.meta.url));

function loadEnv() {
  // Try dale-chat/.env.local first (when run from dale-chat), fall back to relative path
  let envPath;
  try {
    envPath = resolve(process.cwd(), '.env.local');
    readFileSync(envPath, 'utf8');
  } catch {
    envPath = resolve(__dirname, '../../Derived State/dale-chat/.env.local');
  }
  const lines = readFileSync(envPath, 'utf8').split('\n');
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const eqIdx = trimmed.indexOf('=');
    if (eqIdx < 0) continue;
    const key = trimmed.slice(0, eqIdx).trim();
    const val = trimmed.slice(eqIdx + 1).trim();
    if (!process.env[key]) process.env[key] = val;
  }
}
loadEnv();

const db = createClient(
  process.env.REGISTRY_IQ_SUPABASE_URL,
  process.env.REGISTRY_IQ_SUPABASE_SERVICE_ROLE_KEY,
);

const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

const FIRECRAWL_KEY = process.env.FIRECRAWL_API_KEY;
const FIRECRAWL_BASE = 'https://api.firecrawl.dev/v1';

async function firecrawlSearch(query, limit = 5) {
  const res = await fetch(`${FIRECRAWL_BASE}/search`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${FIRECRAWL_KEY}` },
    body: JSON.stringify({ query, limit }),
  });
  if (!res.ok) throw new Error(`Search failed: ${res.status}`);
  return res.json();
}

async function firecrawlScrape(url) {
  const res = await fetch(`${FIRECRAWL_BASE}/scrape`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${FIRECRAWL_KEY}` },
    body: JSON.stringify({ url, formats: ['markdown'] }),
  });
  if (!res.ok) throw new Error(`Scrape failed: ${res.status}`);
  return res.json();
}

function extractMarkdownImages(md) {
  const imgs = [];
  const re = /!\[.*?\]\((https?:\/\/[^\s)]+)\)/g;
  let m;
  while ((m = re.exec(md)) !== null) {
    if (!/svg|icon|logo|badge|pixel/i.test(m[1])) imgs.push(m[1]);
  }
  return imgs;
}

function cleanPropertyName(name) {
  let cleaned = name.trim();
  cleaned = cleaned.replace(/^['']?\d{2}\s+/, '');
  cleaned = cleaned.replace(/^["']+|["']+$/g, '');
  return cleaned.trim();
}

function parseJSON(text) {
  let cleaned = text.trim();
  const jsonStart = cleaned.indexOf('{');
  const jsonEnd = cleaned.lastIndexOf('}');
  if (jsonStart >= 0 && jsonEnd > jsonStart) cleaned = cleaned.slice(jsonStart, jsonEnd + 1);
  try { return JSON.parse(cleaned); } catch { return null; }
}

async function uploadToCloudinary(imageUrl, propertyId) {
  const cloudinaryUrl = process.env.CLOUDINARY_URL;
  if (!cloudinaryUrl) return null;
  try {
    const match = cloudinaryUrl.match(/cloudinary:\/\/(\d+):([^@]+)@(.+)/);
    if (!match) return null;
    cloudinary.config({ cloud_name: match[3], api_key: match[1], api_secret: match[2] });
    const result = await cloudinary.uploader.upload(imageUrl, {
      folder: 'property-registry',
      public_id: `hero_${propertyId}`,
      overwrite: true,
      resource_type: 'image',
      transformation: [{ width: 1600, crop: 'limit', quality: 'auto', fetch_format: 'auto' }],
    });
    return result.secure_url;
  } catch (err) {
    console.error('  Cloudinary upload failed:', err.message);
    return null;
  }
}

const ANALYSIS_PROMPT = `You are RITA, TLC's Rapid Ingestion & Transformation Agent, specializing in student housing, multifamily, build-to-rent, and hospitality properties.

Analyze the following web content scraped from property-related pages. Extract all available property data.

IMPORTANT RULES:
- Only include data you find directly stated or strongly implied in the content. Never fabricate.
- For property_type, use one of: student_housing, build_to_rent, multifamily, hospitality, senior_living, military, other
- For property_status, use one of: prospect, pre_development, under_construction, active, renovation, inactive
- For amenities, list individual amenity names (e.g. "fitness center", "pool", "conference room", "restaurant", "spa")
- Confidence should be 0.0-1.0 reflecting how much data you found

Return ONLY valid JSON matching this schema:
{
  "property_name": "string",
  "property_type": "string|null",
  "property_status": "string|null",
  "address_line1": "string|null",
  "city": "string|null",
  "state_province": "two-letter state code|null",
  "postal_code": "string|null",
  "latitude": "number|null",
  "longitude": "number|null",
  "total_units": "number|null",
  "total_beds": "number|null",
  "year_built": "number|null",
  "opening_date": "YYYY-MM-DD|null",
  "owner_name": "string|null",
  "developer_name": "string|null",
  "brand_name": "string|null",
  "property_manager_name": "string|null",
  "property_phone": "string|null",
  "property_email": "string|null",
  "property_url": "string|null",
  "hero_image_url": "best exterior/hero image URL|null",
  "amenities": ["list", "of", "amenities"],
  "notes": "any other notable findings about the property - history, renovations, star rating, room count, meeting space, restaurants, etc.|null",
  "confidence": 0.0
}`;

const FILLABLE_FIELDS = [
  'property_type', 'property_status', 'address_line1', 'city', 'state_province',
  'postal_code', 'latitude', 'longitude', 'total_units', 'total_beds',
  'year_built', 'opening_date', 'owner_name', 'developer_name', 'brand_name',
  'property_manager_name', 'property_phone', 'property_email', 'property_url',
  'leasing_url', 'hero_image_url',
];

const EMPTY_VALUES = [null, '', 'Unknown', 'TBD', 'unknown', 'tbd', 0, undefined];

const PROPERTY_IDS = [
  'da9c78ab-1b2f-45de-b64b-f7ee9259bbad', // Capital Hilton
  '1c5d1054-58de-4491-811c-796eadd1864c',  // Hilton Houston NASA Clear Lake
  '93bb5b08-4c3c-4d4c-b66a-296d137a4d61',  // Marriott Sugar Land Town Square
  '28f798e8-467a-452f-9eba-2bde008b9d2b',  // Hilton Cameo Beverly Hills
  'b7350a13-c15a-48cb-806b-e7d3f97a4a89',  // Hyatt Regency Los Angeles International Airport
  '063847f5-12c6-4ff1-85f6-2dbbb59bf1ae',  // Park Hyatt Beaver Creek Resort and Spa
];

async function enrichProperty(propertyId) {
  const { data: property, error: fetchErr } = await db
    .from('property_registry')
    .select('*')
    .eq('id', propertyId)
    .single();

  if (fetchErr || !property) {
    console.error(`  Property ${propertyId} not found`);
    return;
  }

  console.log(`\n${'='.repeat(60)}`);
  console.log(`RITA enriching: ${property.property_name}`);
  console.log(`  City: ${property.city || 'unknown'}, State: ${property.state_province || 'unknown'}`);
  console.log(`${'='.repeat(60)}`);

  const sources = [];
  const scrapedContent = [];
  const imageUrls = [];
  const name = cleanPropertyName(property.property_name);
  const city = property.city && property.city !== 'Unknown' ? property.city : '';
  const state = property.state_province && property.state_province !== 'Unknown' ? property.state_province : '';
  const location = [city, state].filter(Boolean).join(' ');

  const queries = [
    `${name} ${location} hotel`,
    `"${name}" ${location}`,
    `${name} Ashford Hospitality Trust hotel`,
  ];

  // Phase 1: Web search + scrape
  for (const query of queries) {
    if (scrapedContent.length >= 4) break;
    console.log(`  Searching: "${query}"`);
    try {
      const searchResult = await firecrawlSearch(query, 5);
      const webResults = searchResult?.data || [];
      console.log(`  → ${webResults.length} results`);
      for (const result of webResults.slice(0, 3)) {
        const url = result.url;
        if (!url || sources.includes(url)) continue;
        sources.push(url);
        try {
          const pageScrape = await firecrawlScrape(url);
          const md = pageScrape?.data?.markdown;
          if (md) {
            scrapedContent.push(`## Source: ${url}\n\n${md.slice(0, 10000)}`);
            if (pageScrape.data?.metadata?.ogImage) imageUrls.push(pageScrape.data.metadata.ogImage);
            for (const img of extractMarkdownImages(md)) {
              if (!imageUrls.includes(img)) imageUrls.push(img);
            }
            console.log(`  Scraped: ${url} (${md.length} chars)`);
          }
        } catch (e) { console.log(`  Skip scrape: ${url} (${e.message})`); }
      }
    } catch (e) { console.log(`  Search failed: ${e.message}`); }

    // Rate limit buffer between queries
    await new Promise(r => setTimeout(r, 1500));
  }

  if (scrapedContent.length === 0) {
    console.log('  ⚠ No web content found. Skipping.');
    return;
  }

  // Phase 2: Claude analysis
  console.log(`  Analyzing ${scrapedContent.length} pages with Claude...`);
  const combinedContent = scrapedContent.join('\n\n---\n\n').slice(0, 40000);
  const userPrompt = `Research target: "${name}"${city ? `, ${city}` : ''}${state ? `, ${state}` : ''}
Known info: type=${property.property_type || 'unknown'}, owner/developer=${property.owner_name || 'Ashford Hospitality Trust'}

SCRAPED WEB CONTENT (${sources.length} pages):

${combinedContent}

${imageUrls.length > 0 ? `\nIMAGES FOUND:\n${imageUrls.map((u, i) => `${i + 1}. ${u}`).join('\n')}` : ''}

Extract all property data from the above content. Return ONLY JSON.`;

  let ritaData;
  try {
    const response = await anthropic.messages.create({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 2000,
      messages: [{ role: 'user', content: userPrompt }],
      system: ANALYSIS_PROMPT,
    });
    const text = response.content.filter(b => b.type === 'text').map(b => b.text).join('');
    ritaData = parseJSON(text);
    if (!ritaData) {
      console.log('  ⚠ Claude returned unparseable response. Skipping.');
      return;
    }
  } catch (e) {
    console.error('  Claude analysis failed:', e.message);
    return;
  }

  console.log(`  Claude confidence: ${ritaData.confidence || 0}`);

  // Phase 3: Build update (only fill empty fields)
  const update = {};
  const applied = [];
  const skipped = [];

  for (const field of FILLABLE_FIELDS) {
    const ritaValue = ritaData[field];
    if (ritaValue === null || ritaValue === undefined || ritaValue === '') continue;

    const existing = property[field];
    const isEmpty = EMPTY_VALUES.includes(existing);

    if (isEmpty) {
      update[field] = ritaValue;
      applied.push(field);
    } else {
      skipped.push(`${field} (keeping: "${existing}")`);
    }
  }

  // Handle amenities
  if (ritaData.amenities && Array.isArray(ritaData.amenities) && ritaData.amenities.length > 0) {
    if (!property.amenities_list || (Array.isArray(property.amenities_list) && property.amenities_list.length === 0)) {
      update.amenities_list = ritaData.amenities;
      applied.push('amenities_list');
    }
    update.has_fitness_center = ritaData.amenities.some(a => /fitness|gym/i.test(a));
    update.has_pool = ritaData.amenities.some(a => /pool/i.test(a));
    update.has_clubhouse = ritaData.amenities.some(a => /clubhouse|club house|lounge/i.test(a));
    update.has_rooftop = ritaData.amenities.some(a => /rooftop|roof/i.test(a));
  }

  // Handle notes (append, never overwrite)
  if (ritaData.notes) {
    const existing = property.notes || '';
    update.notes = existing ? `${existing}\n[RITA] ${ritaData.notes}` : `[RITA] ${ritaData.notes}`;
    applied.push('notes');
  }

  // Phase 4: Cloudinary hero image upload
  let heroUploaded = false;
  const heroSource = update.hero_image_url || (!property.hero_image_url && imageUrls[0]);
  if (heroSource) {
    console.log(`  Uploading hero image to Cloudinary...`);
    const cloudinaryUrl = await uploadToCloudinary(heroSource, propertyId);
    if (cloudinaryUrl) {
      update.hero_image_url = cloudinaryUrl;
      heroUploaded = true;
      console.log(`  → Cloudinary: ${cloudinaryUrl}`);
    }
  }

  if (Object.keys(update).length === 0) {
    console.log('  No new fields to apply (all already populated).');
    return;
  }

  // Phase 5: Apply update
  const { error: updateErr } = await db
    .from('property_registry')
    .update(update)
    .eq('id', propertyId);

  if (updateErr) {
    console.error(`  Update failed: ${updateErr.message}`);
    return;
  }

  console.log(`  ✓ Applied ${applied.length} fields: ${applied.join(', ')}`);
  if (skipped.length > 0) console.log(`  ○ Skipped ${skipped.length}: ${skipped.join(', ')}`);
  if (heroUploaded) console.log(`  ✓ Hero image uploaded to Cloudinary`);

  // Activity log
  await db.from('property_activity_log').insert({
    property_id: propertyId,
    activity_type: 'rita_enrichment',
    description: `RITA batch-enriched ${applied.length} fields from ${sources.length} web sources`,
    details: {
      applied_fields: applied,
      skipped_fields: skipped.map(s => s.split(' ')[0]),
      sources,
      confidence: ritaData.confidence || 0,
      hero_uploaded: heroUploaded,
      mode: 'batch_script',
    },
  }).then(() => {}, () => {});

  return { applied: applied.length, skipped: skipped.length, confidence: ritaData.confidence };
}

// ── Main ──
(async () => {
  console.log(`RITA Batch Enrichment — ${PROPERTY_IDS.length} properties`);
  console.log(`Started: ${new Date().toISOString()}\n`);

  const results = [];
  for (const pid of PROPERTY_IDS) {
    try {
      const result = await enrichProperty(pid);
      results.push({ id: pid, ...(result || { applied: 0, skipped: 0 }) });
    } catch (err) {
      console.error(`  FATAL for ${pid}:`, err.message);
      results.push({ id: pid, error: err.message });
    }

    // Pause between properties to avoid rate limits
    console.log('  --- Pausing 3s before next property ---');
    await new Promise(r => setTimeout(r, 3000));
  }

  console.log(`\n${'='.repeat(60)}`);
  console.log('BATCH COMPLETE');
  console.log(`${'='.repeat(60)}`);
  for (const r of results) {
    if (r.error) {
      console.log(`  ✗ ${r.id}: ${r.error}`);
    } else {
      console.log(`  ✓ ${r.id}: ${r.applied} applied, ${r.skipped} skipped, confidence=${r.confidence || 'n/a'}`);
    }
  }
  console.log(`\nFinished: ${new Date().toISOString()}`);
})();
