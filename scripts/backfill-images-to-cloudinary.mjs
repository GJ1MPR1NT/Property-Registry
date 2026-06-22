#!/usr/bin/env node
/**
 * Backfill property_registry.hero_image_url: download external URLs to Cloudinary,
 * update row, append provenance to enrichment_sources.
 *
 * Env: REGISTRY_IQ_SUPABASE_URL, REGISTRY_IQ_SUPABASE_SERVICE_ROLE_KEY
 *      CLOUDINARY_URL or CLOUDINARY_CLOUD_NAME + CLOUDINARY_API_KEY + CLOUDINARY_API_SECRET
 *
 * Usage:
 *   node scripts/backfill-images-to-cloudinary.mjs --dry-run
 *   node scripts/backfill-images-to-cloudinary.mjs --apply
 *   node scripts/backfill-images-to-cloudinary.mjs --apply --limit=50
 */

import { createClient } from '@supabase/supabase-js';
import { v2 as cloudinary } from 'cloudinary';
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
const limitArg = argv.find((a) => a.startsWith('--limit='))?.split('=')[1];
const limit = limitArg ? parseInt(limitArg, 10) : 5000;

const regUrl = process.env.REGISTRY_IQ_SUPABASE_URL;
const regKey = process.env.REGISTRY_IQ_SUPABASE_SERVICE_ROLE_KEY;

function configureCloudinaryFromEnv() {
  const cloudName = process.env.CLOUDINARY_CLOUD_NAME;
  const apiKey = process.env.CLOUDINARY_API_KEY;
  const apiSecret = process.env.CLOUDINARY_API_SECRET;
  if (cloudName && apiKey && apiSecret) {
    cloudinary.config({ cloud_name: cloudName, api_key: apiKey, api_secret: apiSecret });
    return true;
  }
  const url = process.env.CLOUDINARY_URL;
  if (!url) return false;
  const match = url.match(/cloudinary:\/\/(\d+):([^@]+)@(.+)/);
  if (!match) return false;
  cloudinary.config({ api_key: match[1], api_secret: match[2], cloud_name: match[3] });
  return true;
}

async function fetchCandidates(client, max) {
  const pageSize = 500;
  let all = [];
  let from = 0;
  for (;;) {
    const { data, error } = await client
      .from('property_registry')
      .select('id, hero_image_url, enrichment_sources')
      .not('hero_image_url', 'is', null)
      .range(from, from + pageSize - 1);
    if (error) throw error;
    if (!data?.length) break;
    const ext = data.filter((r) => {
      const u = r.hero_image_url;
      return typeof u === 'string' && u.startsWith('http') && !u.includes('res.cloudinary.com');
    });
    all = all.concat(ext);
    if (all.length >= max || data.length < pageSize) break;
    from += pageSize;
  }
  return all.slice(0, max);
}

async function uploadHero(externalUrl, propertyId) {
  const result = await cloudinary.uploader.upload(externalUrl, {
    folder: 'property-registry',
    public_id: `hero_${propertyId}`,
    overwrite: true,
    resource_type: 'image',
    context: `source_url=${String(externalUrl).replace(/\|/g, '_')}|uploaded_by=backfill|entity_id=${propertyId}`,
    transformation: [{ width: 1600, crop: 'limit', quality: 'auto', fetch_format: 'auto' }],
  });
  return result.secure_url;
}

async function main() {
  if (!regUrl || !regKey) {
    console.error('Missing REGISTRY_IQ_SUPABASE_URL or REGISTRY_IQ_SUPABASE_SERVICE_ROLE_KEY');
    process.exit(1);
  }
  if (!configureCloudinaryFromEnv()) {
    console.error('Missing Cloudinary configuration');
    process.exit(1);
  }

  const reg = createClient(regUrl, regKey, { auth: { persistSession: false } });
  const rows = await fetchCandidates(reg, limit);
  console.log(`Backfill hero images (${DRY ? 'DRY-RUN' : 'APPLY'}): ${rows.length} candidate(s), limit=${limit}`);

  let ok = 0;
  for (const row of rows) {
    const sourceUrl = row.hero_image_url;
    if (DRY) {
      console.log(`[DRY] would upload ${row.id.slice(0, 8)}… ${sourceUrl?.slice(0, 72)}…`);
      continue;
    }
    try {
      const secure = await uploadHero(sourceUrl, row.id);
      const prev = Array.isArray(row.enrichment_sources) ? row.enrichment_sources : [];
      const nextSources = [
        ...prev,
        {
          type: 'hero_cloudinary_backfill',
          source_url: sourceUrl,
          cloudinary_url: secure,
          at: new Date().toISOString(),
        },
      ];
      const { error } = await reg
        .from('property_registry')
        .update({ hero_image_url: secure, enrichment_sources: nextSources })
        .eq('id', row.id);
      if (error) console.error('update failed', row.id, error.message);
      else ok++;
    } catch (e) {
      console.error('upload failed', row.id, e?.message ?? e);
    }
  }

  console.log(`Done. updated=${ok} dryRun=${DRY}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
