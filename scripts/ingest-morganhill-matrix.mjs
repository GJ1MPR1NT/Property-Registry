#!/usr/bin/env node
/**
 * Morgan Hill: ingest unit-type finish layouts (UNIT PLANS pages) + Matrix kitchen/vanity links.
 *
 * Prereq: python3 scripts/extract-mh-matrix-mappings.py
 * Prereq: scripts/migration-unit-type-room-drawings.sql applied
 *
 * Usage: node scripts/ingest-morganhill-matrix.mjs --dry-run | --apply
 */
import { createClient } from '@supabase/supabase-js';
import { v2 as cloudinary } from 'cloudinary';
import { config } from 'dotenv';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';
import { readFileSync, existsSync, writeFileSync } from 'fs';
import { execSync } from 'child_process';

const __dirname = dirname(fileURLToPath(import.meta.url));
for (const f of ['.env.local', '.env']) {
  config({ path: resolve(__dirname, '..', f) });
  config({ path: resolve(__dirname, '../../Derived State/dale-chat', f) });
}

const DRY = !process.argv.includes('--apply');
const PID = 'a30d446c-ee4a-4fe0-a76e-e4f9bed0e3b0';
const MAP_JSON = resolve(__dirname, '..', '.firecrawl', 'mh-matrix-drawings.json');
const CABINET_GAP = new Set(['MW04.5', 'MW05', 'MW06']);

const regUrl = process.env.REGISTRY_IQ_SUPABASE_URL;
const regKey = process.env.REGISTRY_IQ_SUPABASE_SERVICE_ROLE_KEY;

function configureCloudinary() {
  const m = (process.env.CLOUDINARY_URL || '').match(/cloudinary:\/\/(\d+):([^@]+)@(.+)/);
  if (m) { cloudinary.config({ api_key: m[1], api_secret: m[2], cloud_name: m[3] }); return true; }
  return false;
}

function pngUrl(publicId) {
  return cloudinary.url(publicId, { resource_type: 'image', format: 'png', transformation: [{ page: 1, width: 600, crop: 'fit' }] });
}

async function uploadPdf(localPath, publicId) {
  const r = await cloudinary.uploader.upload(localPath, {
    public_id: publicId, overwrite: true, resource_type: 'image',
  });
  return { pdf_url: r.secure_url, png_url: pngUrl(publicId), pages: r.pages ?? 1 };
}

function linkShopDrawing(shopByNo, drawingNo) {
  if (!drawingNo || CABINET_GAP.has(drawingNo)) return null;
  const row = shopByNo.get(drawingNo);
  if (!row) return { drawing_no: drawingNo, thumbnail_url: null, pdf_url: null, gap: 'not_in_shop_drawings' };
  return {
    drawing_no: row.drawing_no,
    thumbnail_url: row.thumbnail_url,
    pdf_url: row.pdf_url,
    page_count: row.page_count,
  };
}

async function main() {
  if (!regUrl || !regKey) { console.error('Missing REGISTRY_IQ creds'); process.exit(1); }
  if (!existsSync(MAP_JSON)) {
    console.log('Running extract-mh-matrix-mappings.py...');
    execSync(`python3 "${resolve(__dirname, 'extract-mh-matrix-mappings.py')}"`, { stdio: 'inherit' });
  }
  const map = JSON.parse(readFileSync(MAP_JSON, 'utf8'));
  const reg = createClient(regUrl, regKey, { auth: { persistSession: false } });

  if (!DRY && !configureCloudinary()) { console.error('Missing Cloudinary config'); process.exit(1); }

  const { data: shopRows } = await reg.from('property_shop_drawings').select('drawing_no,thumbnail_url,pdf_url,page_count').eq('property_id', PID);
  const shopByNo = new Map((shopRows || []).map((r) => [r.drawing_no, r]));

  const { data: utRows, error: utErr } = await reg.from('property_unit_types').select('id,unit_type_name,bathrooms').eq('property_id', PID);
  if (utErr) { console.error(utErr.message); process.exit(1); }
  const utByName = new Map((utRows || []).map((u) => [u.unit_type_name, u]));

  // Upload each unique layout page once
  const pageCloud = new Map();
  const pagesNeeded = [...new Set(Object.values(map.types).map((t) => t.layout_page).filter(Boolean))];
  console.log(`Layout pages (${DRY ? 'DRY' : 'APPLY'}): ${pagesNeeded.length}`);

  for (const pageNum of pagesNeeded.sort((a, b) => a - b)) {
    const localPath = map.page_paths?.[String(pageNum)] || map.types[Object.keys(map.types).find((k) => map.types[k].layout_page === pageNum)]?.layout_page_pdf;
    const path = localPath || resolve(__dirname, '..', '.firecrawl', 'mh-unit-plan-pages', `page_${String(pageNum).padStart(2, '0')}.pdf`);
    if (!existsSync(path)) { console.error('Missing page pdf', pageNum, path); continue; }
    if (DRY) { console.log(`  [DRY] page ${pageNum}`); pageCloud.set(pageNum, { pdf_url: `dry://page-${pageNum}.pdf`, png_url: `dry://page-${pageNum}.png` }); continue; }
    const publicId = `property-registry/${PID}/unit-plan-sheets/page_${pageNum}`;
    const up = await uploadPdf(path, publicId);
    pageCloud.set(pageNum, { pdf_url: up.pdf_url, png_url: up.png_url });
    console.log(`  ok page ${pageNum}`);
  }

  // Per-type cropped layouts (correct unit label on thumbnail)
  const cropCloud = new Map();
  const cropNeeded = Object.entries(map.types).filter(([, spec]) => spec.layout_crop_pdf);
  console.log(`Layout crops (${DRY ? 'DRY' : 'APPLY'}): ${cropNeeded.length}`);
  for (const [typeName, spec] of cropNeeded) {
    const path = spec.layout_crop_pdf;
    if (!path || !existsSync(path)) { console.warn('  missing crop', typeName, path); continue; }
    const safe = typeName.replace(/[^A-Za-z0-9._-]+/g, '_');
    if (DRY) {
      cropCloud.set(typeName, { pdf_url: `dry://crop-${safe}.pdf`, png_url: `dry://crop-${safe}.png` });
      continue;
    }
    const publicId = `property-registry/${PID}/unit-plan-crops/${safe}`;
    const up = await uploadPdf(path, publicId);
    cropCloud.set(typeName, { pdf_url: up.pdf_url, png_url: up.png_url });
  }
  if (!DRY) console.log(`  uploaded ${cropCloud.size} crops`);

  let layoutOk = 0, kitchenOk = 0, bathOk = 0, gaps = { layout: [], kitchen: [] };

  for (const [typeName, spec] of Object.entries(map.types)) {
    const ut = utByName.get(typeName);
    if (!ut) { console.warn('  registry missing type:', typeName); continue; }

    const roomDrawings = {};
    const pageNum = spec.layout_page;
    let layoutAssets = [];

    const crop = cropCloud.get(typeName);
    if (crop) {
      layoutAssets = [{
        role: 'finish_layout',
        url: crop.pdf_url,
        png_url: crop.png_url,
        label: `${typeName} — finish layout`,
        source_path: 'UNIT PLANS_5.2025.pdf',
        source_page: pageNum,
        unit_type_name: typeName,
        cropped: true,
      }];
      layoutOk++;
    } else if (pageNum && pageCloud.has(pageNum)) {
      const c = pageCloud.get(pageNum);
      layoutAssets = [{
        role: 'finish_layout',
        url: c.pdf_url,
        png_url: c.png_url,
        label: `${typeName} — UNIT PLANS p${pageNum}`,
        source_path: 'UNIT PLANS_5.2025.pdf',
        source_page: pageNum,
        unit_type_name: typeName,
      }];
      layoutOk++;
    } else {
      gaps.layout.push(typeName);
    }

    const mw = spec.kitchen_drawing_no;
    if (mw && !CABINET_GAP.has(mw)) {
      const k = linkShopDrawing(shopByNo, mw);
      if (k?.pdf_url) {
        roomDrawings.kitchen = {
          ...k,
          source_ref: spec.kitchen_cab_raw,
          note: 'Cabinet shop drawing is labeled with a representative unit type; Matrix Kitchen Cab code is authoritative for this Unit Type - GC.',
        };
        kitchenOk++;
      } else if (k) {
        roomDrawings.kitchen = { drawing_no: mw, source_ref: spec.kitchen_cab_raw, thumbnail_url: null, pdf_url: null };
        gaps.kitchen.push(typeName);
      }
    } else if (mw && CABINET_GAP.has(mw)) {
      gaps.kitchen.push(`${typeName}:${mw}`);
    }

    if (spec.vanity_1) {
      const v = linkShopDrawing(shopByNo, spec.vanity_1.drawing_no || spec.vanity_1.sheet);
      roomDrawings.bath_1 = { ...(v || {}), detail: spec.vanity_1.detail, label: spec.vanity_1.label };
      if (v?.pdf_url) bathOk++;
    }
    if (spec.vanity_2 && (ut.bathrooms ?? 0) >= 2) {
      const v = linkShopDrawing(shopByNo, spec.vanity_2.drawing_no || spec.vanity_2.sheet);
      roomDrawings.bath_2 = { ...(v || {}), detail: spec.vanity_2.detail, label: spec.vanity_2.label };
      if (v?.pdf_url) bathOk++;
    }

    if (DRY) {
      console.log(`  [DRY] ${typeName} page=${pageNum} kitchen=${mw || '—'} baths=${spec.vanity_1?.label || '—'}`);
      continue;
    }

    const { error } = await reg.from('property_unit_types').update({
      layout_asset_urls: layoutAssets,
      room_drawings: roomDrawings,
    }).eq('id', ut.id);
    if (error) console.error('  update failed', typeName, error.message);
  }

  const report = { layout_ok: layoutOk, kitchen_ok: kitchenOk, bath_links: bathOk, gaps };
  if (!DRY) writeFileSync(resolve(__dirname, '..', '.firecrawl', 'mh-matrix-ingest-report.json'), JSON.stringify(report, null, 2));
  console.log('\nSummary:', report);
  console.log('Done.');
}

main().catch((e) => { console.error(e); process.exit(1); });
