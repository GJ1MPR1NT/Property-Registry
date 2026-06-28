# PROJECT_CONTEXT — Property Registry

**Last updated:** Jun 27, 2026

## Session: Jun 27, 2026 — Core Spaces follow-ups CS-02 through CS-05

**Completed all four recommended next steps from May 20 ingest:**

### CS-02 — Sold portfolio ownership (9/10 resolved)
Script: `scripts/enrich-corespaces-sold-ownership.mjs`
- **American Campus Communities** (owner + PM): Ann Arbor, Eugene, Flagstaff, Madison (The James), U District Seattle, State Fort Collins — sourced from MHN Sep 2017 $560.6M ACC portfolio article
- **Lark Living**: Hub Oxford (larkoxford.com)
- **Scion Group / University House**: Hub Tempe (uhtempe.com rebrand)
- **Yugo**: Hub Tucson (yugotucsoncampus.com)
- **Unresolved:** Hub Minneapolis — press research inconclusive; still flagged `needs_ownership_research`
- Report: `.firecrawl/cs-sold-ownership-report.json`

### CS-03 — Bloomington dedupe merges
Script: `scripts/merge-corespaces-bloomington.mjs` → `iqid_apply_merge`
- `HUB Bloomington` (baad0f51) → `Hub on Campus Bloomington` (845827d4)
- `Hub Bloomington Lincoln (II)` (beb742ce) → `Hub on Campus Bloomington Lincoln` (8d192192)

### CS-04 — Cloudinary hero backfill
`scripts/backfill-images-to-cloudinary.mjs --apply --source=corespaces_prismic` — **91/91** Prismic hero URLs uploaded to Cloudinary

### CS-05 — Coming-soon → project_registry alignment
Script: `scripts/link-corespaces-coming-soon-projects.mjs` + manual repoint of legacy duplicate property_ids
- **Linked/repointed (14):** Tallahassee (25-071-I), Clemson (26-002-I), Knoxville (25-005-I), Boulder (26-006-I), Tampa (25-1631-D), West Lafayette Chauncey, Madison Broom, Raleigh (26-001-I), Bloomington Lincoln (27-006-I/27-007-I), Clear Creek (MX-004), West Oak (DFW-West Oak)
- **No open pipeline deal found (7):** William, Ann Arbor State, Madison Bassett, Oxenfree Liberty Hill/Parklin/Stonebriar/Rowlett
- Report: `.firecrawl/cs-coming-soon-project-links.json`

---

## Session: May 20, 2026 — Core Spaces website → Registry-iQ ingest

**Goal:** Methodically capture Core Spaces portfolio from new [corespaces.com/communities](https://corespaces.com/communities) site (coming-soon pipeline + sold portfolio) into `property_registry`.

**Discovery:** Site is a Next.js + **Prismic CMS** SPA. Static scrape/Firecrawl returns "0 Communities"; data comes from `core-spaces-website.cdn.prismic.io/api/v2` (public read token embedded in site JS).

**Deliverable:** `scripts/ingest-corespaces-prismic.mjs`
- Pulls all 89 `property` documents + 8 `property_brand` documents from Prismic
- Maps: name, address, city/state/zip, lat/lng, university, brand, units/beds/SF, hero + gallery images, property URL, coming-soon flag, owned-by-core (sold) flag
- Upserts into Registry-iQ; links **Core Spaces** stakeholder as `developer`
- Sold assets (`is_owned_by_core=false`, 10): `property_status=inactive`, `tlc_relationship=former_customer`, `enrichment_sources.needs_ownership_research=true`
- Coming soon (19): `property_status=pre_development` or `under_construction`

**Run (May 20):** 89/89 matched+updated, 0 errors. Registry now has **91** rows with `external_ids.prismic_id` (includes prior partial inserts).

**Bloomington (priority) — 3 canonical Core Spaces rows enriched:**
| Property | Status | Units | Beds | Address |
|---|---|---|---|---|
| Hub on Campus Bloomington | active | 170 | 591 | 2038 N Walnut St |
| Hub on Campus Bloomington Lincoln | pre_development | 461 | 1,429 | 1303 N Lincoln St |
| State on Campus Bloomington | active | 198 | 468 | 2036 N Walnut St |

**Artifacts:** `.firecrawl/corespaces-prismic-normalized.json`, `.firecrawl/corespaces-ingest-report.json`

**Follow-ups (completed Jun 27 — see session above):**
1. ~~Press-release / buyer research for 10 sold Hub/State assets~~ → CS-02 DONE (9/10)
2. ~~Dedupe merge: legacy Bloomington rows~~ → CS-03 DONE
3. ~~Cloudinary hero backfill~~ → CS-04 DONE (91 images)
4. ~~Pipeline alignment: coming-soon → project_registry~~ → CS-05 DONE (14 linked)

---

## Session: Jun 20, 2026 — Registry drill-through navigation (dale-chat)

**Goal:** Property Registry data should be travelable — clicking a linked entity opens detail or jumps to the right tab.

**dale-chat changes (not yet deployed):**
- `app/components/RegistryDrillLink.tsx` — shared cyan link button for drill-through fields.
- `lib/property-registry-nav.ts` — drill filter types + merge/clear helpers (nested AND semantics).
- `app/components/RegistryDrillFilterBar.tsx` — removable filter chips + “Showing N of M” counts.
- **Nested filters:** clicking a link merges dimensions (building + floor + unit type + unit # + SKU). Filters persist across tab switches until cleared. Changing building clears floor (child scope reset).
- **Unit #s tab:** filters table; building/floor/SKU drills merge filters + navigate.
- **Floors tab:** building/floor filters hide non-matching sections + scroll/highlight; rollup ×count → filtered Unit #s with parent building/floor context.
- **SKUs tab:** filters assignments by unit type and/or SKU.
- **Unit type modal:** “N units” / “SKU lines” jump to filtered tabs.
- **URL hash persistence:** tab + nested filters encoded in `location.hash` (shareable, survives refresh). Drill navigation uses `pushState`; tab bar / chip edits use `replaceState`. Back/forward restores hash.

**Already linked before this pass:** Overview + Unit Type Matrix tabs opened unit type modal.

---

## Session: Jun 20, 2026 — Morgan Hill enrichment campaign + RITA outlier scan

**Pilot:** `MH-REGISTRY-ENRICH-001` for property `a30d446c-ee4a-4fe0-a76e-e4f9bed0e3b0`.

**Schema (pending apply on Registry-iQ):** `scripts/migration-property-enrich-review.sql` — `property_enrich_campaigns`, `property_enrich_review` (token + `item_payloads` jsonb + `answers` jsonb), `property_enrich_review_events`.

**dale-chat additions:**
- `lib/enrich-review-schema.ts` — question kinds: boolean, single/multi-select, rank, text, number, image_annotation (phase 2).
- `lib/matrix-outlier-detector.ts` — Tier A/B/C outlier rules (BR/bath, mixed bath/kitchen within type, missing shop PDFs).
- `lib/property-enrich-review-engine.ts` — primary token email + **separate witness emails** (text-only, no link; NOT Resend CC).
- `app/property-enrich-review/[token]/page.tsx` — structured Q&A review surface.
- `app/components/RitaMatrixOutliersPanel.tsx` — proactive outlier list + "Launch enrichment campaign" (admin).
- `app/api/property-registry/[id]/matrix-outliers/route.ts`

**Witness pattern:** `witness_emails` stored for audit; each witness gets their own `buildWitnessEmailContent()` send with questions listed in prose — no token, no button. Primary reviewer gets the only interactive link.

**Apply chain:** submitted answers → `review_status=submitted` (apply worker to PATCH `property_units` / `property_unit_types` is NEXT after first pilot responses).

**Migration applied (Jun 20):** `property_enrich_campaigns`, `property_enrich_review`, `property_enrich_review_events` live on Registry-iQ `xhafhdaugmgdxckhdfov`.

**File collection (deferred — MH-12):** add `file_upload` question kind + `property_enrich_review_files` table (`scripts/migration-property-enrich-review-files.sql`). Quarantine → malware scan → promote to Cloudinary. Multi-file per question supported in schema design.

---

Plan: `~/.cursor/plans/morgan_hill_layouts_drawings_7e73dd7e.plan.md`. Premise: the Buildings/Floors/Units/Unit Types tabs are already built + interlinked; this pass adds the missing **image layer** + small fixes, with **no surrogates** (missing assets stay empty and are recorded as gaps). Target property `a30d446c-ee4a-4fe0-a76e-e4f9bed0e3b0`; Registry-iQ Supabase `xhafhdaugmgdxckhdfov`; Cloudinary `dut68koei`.

**Schema (additive; applied):** `property_floors.floor_plan_url text` + `images jsonb`; new `property_shop_drawings` table (drawing_no, title, drawing_type, version, state, thumbnail_url, pdf_url, page_count, unit_type_code, source_path, notes). Migration files: `scripts/migration-floorplans-and-shop-drawings.sql`. (Reconcile + unit-tags migrations from the prior pass: `migration-reconcile-morganhill-buildings.sql`, `ingest-morganhill-structure.mjs`.)

**Asset ingestion** (`scripts/ingest-morganhill-assets.mjs`, signed Cloudinary upload, `pg_1,f_png` thumbnail + source PDF):
- **Shop drawings = 23 rows.** 17 cabinet configs (`MW01..MW18` that have a `KM PDFs/MWxx.pdf`) as `kitchen_cabs`; 6 interior-elevation sheets (`A701–A706`, A705/A706 → `vanity`, rest → `kitchen_cabs`). `version='2026-04-06'` (FOR INSTALL set), `state='not_started'` (unverified). `unit_type_code` left null (M:N rollups deferred).
- **Floor plans = 11/11 floors.** Building 1 Levels 1–5 → per-level `1-A2x0 OVERALL PLAN` PDFs; Buildings 2 & 3 → the combined SCHEME building PDF on each of their 3 floors (flagged `combined:true` in `images`). Thumbnail verified HTTP 200 `image/png`.

**Gaps recorded (no surrogates):** cabinet configs `MW04.5/MW05/MW06` referenced by the Matrix have **no PDF** → no shop drawing. **Unit-type finish layouts**: `UNIT PLANS_5.2025.pdf` has no per-type page map → `layout_asset_urls` left empty for all 86 types (Matrix Layout column shows the empty placeholder). Saved to `.firecrawl/mh-asset-gaps.json`.

**Bedrooms backfilled (Jun 19, user-confirmed):** `A*` → `standard_bedrooms=1`, `B*` → `2` for all 86 types (`bedrooms_structural` GENERATED). Structural bed sum = 479 (Σ bedrooms×unit_count). **`total_sqft` still NULL**; `beds_per_unit=1` uniformly (default, MH-02 still open) — so Total Beds in the matrix footer stays 390 while 26 B-types show **Beds=1** vs **Bedrooms=2** (honest gap, no surrogate). `bathrooms`/`unit_count` from Matrix. Totals: 390 units, 480 total baths.

**UI (dale-chat `app/property-registry/[id]/page.tsx` + components):**
- New shared `AssetPreviewModal.tsx` (page-1 PNG preview + "Download PDF").
- **Floors tab** (`PropertyFloorsTab.tsx`) + **Buildings tab** floor rows: clickable floor-plan thumbnail → preview modal. Fixed misleading rollup help text and relabeled the FF&E SKU piece estimate as a **separate optional estimate** (decoupled from the unit/bed/bath rollup).
- **"Unit Types" tab → "Unit Type Matrix":** clickable layout thumbnail + name → `UnitTypeDetailModal` (now has "Download as PDF"); columns `Bedrooms` (structural, "—" when unknown), `Beds` (only shown when a known bedroom count differs), `Bathrooms`, `Unit Count`, `Total Beds`, new `Total Baths` (Σ bathrooms×unit_count, footer-summed); dropped the legacy `layout_type` text column + its add-form input.
- New **"Shop Drawings" tab** (`PropertyShopDrawingsTab.tsx` + `app/api/property-registry/[id]/shop-drawings/route.ts` GET/PATCH): thumbnail grid with drawing #, Drawing Type, Version, editable State (admin), type/state filters, preview modal w/ Download. Cross-link rollups (building/floor/unit/room counts) deferred.
- **Opening-year timezone fix:** header + `openingYear` now use `opening_date.slice(0,4)`; Overview "Opening Date" formats with `timeZone:'UTC'`. DB `opening_year` realigned 2026 → **2027** (matches `opening_date` 2027-01-01).
- **RITA relocation:** the two persistent RITA surfaces (Proposals panel + Reads history) moved from the top of the tab content to the **bottom of the page**. The three transient run-status banners (no-results prompt, staged-run banner, findings review) were left near the RITA action button (they appear only during/after an active run).

**Validation:** `npm run build` (dale-chat) green; property route compiles. DB spot-checks: 23 shop drawings, 11 floor plans (thumbnail 200), 86 unit types / 390 units / 480 baths.

---

## Session: Jun 15, 2026 — Morgan Hill enrichment via Firecrawl (morganhillapts.com)

User: "please use firecrawl and scour this site for images and information to enrich the morgan hill property registry https://morganhillapts.com/"

**Target:** `property_registry` id `a30d446c-ee4a-4fe0-a76e-e4f9bed0e3b0` (Morgan Hill Apartments, Carrollton TX) — Registry-iQ Supabase `xhafhdaugmgdxckhdfov`.

**Method:** Firecrawl CLI (`map` + `scrape --format markdown,links`). Site has only 2 pages (home + `?page_id=61` floorplans). Scrape artifacts under `.firecrawl/` (gitignored).

**Images:** 4 official assets uploaded to Cloudinary (`dut68koei`) under `property-registry/<id>/` via signed upload (`.firecrawl/scratchpad/upload-cloudinary.mjs`, reads `CLOUDINARY_URL` from dale-chat `.env.local`):
- `morgan-hill-final-exterior-rendering.jpg` (2560×1440) → **hero** (also set `hero_image_url`, replacing prior research image)
- `morgan-hill-rendering.png` (exterior), `morgan-hill-billingsley-collection.jpg` (exterior), `morgan-hill-illustrated-site-map.jpg` (`site_plan`)
- Excluded: a generic stock photo (`man-earphones…`) and a leftover template image (`AH_Not-Approved-IMages…`).

**Fields updated:** `opening_year=2026` (consistent with existing `opening_date` 2026-12-01 / site "Coming Winter 2026"); `has_pool/has_fitness_center/has_pet_friendly/has_clubhouse=true`; `enrichment_sources += morganhillapts.com`; `source_detail`, `last_enrichment_at`, `notes` appended (Billingsley Collection brand, taglines, opening timing).

**Coordinates:** site map embed was unusable (plotted in Mississippi). Instead geocoded the canonical address (3885 Midway Road, Carrollton TX 75007) via Google Maps → `latitude=33.0179734, longitude=-96.8435763`, cross-checked with OSM/Nominatim (~165 m agreement; address-level precision for a pre-opening build). `geo_point` auto-synced by trigger to `POINT(-96.8435763 33.0179734)`.

**Data-quality flags (NOT ingested):** (1) the site's Google-map embed uses **wrong coordinates** (lat 33.27 / lng -90.99 plots in Mississippi) — did not use them. (2) Floorplans page is **unreplaced template copy** ("Hartwood… Sloan Corners") with an unrendered `[spaces asset_id="276"]` widget → unit mix not confirmable from site; left `total_units/total_beds` untouched. (3) `data_quality_score` left at 0 (scoring formula unknown — did not fabricate).

---

## Session: May 29, 2026 — Contact review: editable linked stakeholder

User: "for contact matching can you include the linked stakeholder as an editable field"

**dale-chat registry-review (needs deploy):**
- `lib/contact-stakeholder-link.ts` — load/set primary `contact_stakeholder_associations` link
- `lib/registry-edit.ts` — contact field `linked_stakeholder_id` (`stakeholder_ref` type)
- `GET /api/registry-review/[id]` — hydrates `linked_stakeholder_id` + name on contact rows
- `POST /api/registry-review/[id]/edit-row` — persists stakeholder association changes + audit note
- `app/registry-review/page.tsx` — compare row + search picker when editing L/R side

---

User: "the key now is to match projects to properties using the ship to addresses from sage"

**Pipeline:** `scripts/sync-sage-shipto-project-property.mjs` + `scripts/lib/sage-shipto-match.mjs`

1. Load DALE-Demand `sage_orders` with ship_to populated (2,638 orders deduped by `order_number`)
2. Join unlinked `project_registry` via deal keys / `external_ids` / name vs `ship_name`
3. Match `property_registry` via `property_key`, address key, warehouse tokens, deal in external_ids, fuzzy ship_name+city/state
4. Upsert `project_location_candidates`; optional `--promote --min-confidence=95` writes `project_registry.property_id`

**First apply (live Registry-iQ):**
- 727 candidates written
- 55 projects auto-linked (`sage_property_key_exact`, confidence 100)
- **1,407 / 2,799** projects now have `property_id` (was 1,352)
- Queue: 74 proposed, 438 needs_review (ambiguous), 160 needs_external_research, 55 accepted

**Fix during apply:** PostgREST upsert failed on partial unique index → script uses select-then-update/insert.

**Docs:** `docs/SAGE_SHIPTO_PROJECT_PROPERTY_LINK.md`

**Follow-ups:** Review ambiguous queue; repopulate ship_to on latest Sage snapshot in DALE-Demand; consider promoting `ship_to_address_exact` (98) after spot-check.

**Snapshot date note (2026-05-28):** `2026-12-31` in `sage_orders` is the FY2026 **year-end pacing** batch (878 rows, no ship_to) — not the latest API sync. Operational sync date = **2026-05-31**; `property_key` enrichment = **2026-05-27**. Script dedupe prefers `property_key` over raw snapshot_date.

**Ship-to matching doctrine (2026-05-28):** `scripts/lib/sage-shipto-match.mjs` is the shared module — always call `enrichSageOrder()` before matching; project→Sage joins include ship-to address; Sage→property tiers lead with address/property_key before fuzzy name. Sage header sync updated in `Sage-iQ/scripts/sage_oe_map.py` to write `property_key` on every row.

**Apply run (2026-05-31):** Backfilled `property_key` on ~5,600 ship_to rows (`backfill-sage-shipto-property-key.mjs`). Re-ran `--apply --promote --min-confidence=95` → **477 promoted** (1,884 / 2,799 projects linked). Sage header sync to snapshot `2026-05-28` running for current-window refresh. Historical Excel snapshots (pre-2025-03-22) still lack ship_to — only ~5,703 / 13,356 distinct order_numbers have ship_to anywhere.

---

## Session: May 31, 2026 — Comprehensive Sage ship-to backfill (2019→)

User: "execute an expanded sage backfill. go as far back and be as comprehensive as possible"

**Blocker fixed:** `run-comprehensive-sage-shipto-backfill.sh` was aborting (exit 127) because `source .env.local` hit invalid shell identifier `PDF.CO_API_KEY=...`. Script now relies on `SAGE_ENV_FILE` (Python) + dotenv (Node) only.

**Running (PID ~54611, resumed skip=300):**
```bash
bash scripts/run-comprehensive-sage-shipto-backfill.sh --resume
```
- Phase 1: `Sage-iQ/scripts/sync_sage_orders_headers_from_sage.py --comprehensive --snapshot-date 2026-05-28 --state-dir data/sage_header_sync/comprehensive`
- Phase 2: `backfill-sage-shipto-property-key.mjs --apply`
- Phase 3: `sync-sage-shipto-project-property.mjs --apply --promote --min-confidence=95`
- Log: `logs/sage-comprehensive-backfill-20260531-114859.log`
- Pace: ~100 orders/page, ~40–55s/page, ~77% map rate, ~100% ship_to on mapped rows; ~23% skipped (no TYPE in Sage optional fields)
- ETA: ~100+ pages → **several hours** for Phase 1 alone

**Monitor:**
```bash
tail -f "/Users/geoffreyjackson/Dropbox/The Living Company/TLC iQ/Property_Registry/logs/sage-comprehensive-backfill-20260531-114859.log"
cat "/Users/geoffreyjackson/Dropbox/The Living Company/TLC iQ/Sage-iQ/data/sage_header_sync/comprehensive/sync_state.json"
```

**Resume if interrupted:** `bash scripts/run-comprehensive-sage-shipto-backfill.sh --resume`

**Baseline at launch:** DALE `sage_orders` total 39,816 rows; 11,344 with ship_to; snapshot `2026-05-28` had 3,006 rows and growing as sync runs.

---

## Session: May 28, 2026 — `site_address` wiring (Registry → Chain-iQ)

User: wire install site street address to supply/chain; column name **`site_address`**.

**Where the street line lives (Hub II Bloomington):**

| Source | Has site street? | Example |
|--------|------------------|---------|
| **Registry-iQ** `property_registry` | **Yes (authoritative)** | `208 ½ East 19th Street, Bloomington, IN 47408` |
| **Sage** `sage_orders.ship_to_*` | Partial — UF furniture SOs often have real streets; **Hub II CSL line has LLC name only** | `2pr00088 Core Bloomington Linclon LLC` (not 19th St) |
| **NetSuite** job 10201 REST | **No** in verified pulls — ROSD/order-by yes; `custentityoff_site` blank | Project `25326 Bloomington, IN - HUB II` |
| **Chain-iQ** `destination` | Logistics routing only | `Brighton, TN 38011` or `Bloomington IN 47408` |

**Schema (applied live 2026-05-28):**
- Chain-iQ `container_loads.site_address` — `scripts/migration-container-site-address.sql`
- Registry-iQ `project_registry.site_address` — `scripts/migration-project-registry-site-address.sql`

**Sync:** `scripts/sync-site-address-to-chain.mjs` (+ `scripts/lib/site-address.mjs`)

```bash
node scripts/sync-site-address-to-chain.mjs --dry-run
node scripts/sync-site-address-to-chain.mjs --apply
```

**First apply:** 1,906 `project_registry` rows + 2,631 `container_loads` rows. Hub II BSI containers now carry `site_address = 208 ½ East 19th Street…` while `destination` remains Brighton staging.

**Fix:** tightened CSL/SO project match in `container-destination-guard.mjs` (no blanket Hub I Bloomington on all SO lines).

**Docs:** `docs/SITE_ADDRESS_WIRING.md`

---

## Session: May 28, 2026 — Firecrawl web resolve in matching pipeline

User: use Firecrawl web search to resolve site addresses (property websites, press releases) and integrate into all matching.

**Module:** `scripts/lib/site-address-resolve.mjs` — queries, search+scrape, regex + optional Claude extract, city/state validation, `enrichment_sources` provenance.

**Scripts / hooks:**
- `scripts/resolve-site-address-firecrawl.mjs` — batch `--apply --limit=N`
- `sync-sage-shipto-project-property.mjs` — `--resolve-web --resolve-web-limit=50`
- `sync-site-address-to-chain.mjs` — `--resolve-web-first`
- `run-comprehensive-sage-shipto-backfill.sh` — Phase 2b + Phase 3 with `--resolve-web`

**Scale:** ~1,239 / 2,126 properties lack a real `address_line1` (run with limits; Firecrawl credits).

**Docs:** `docs/SITE_ADDRESS_WEB_RESOLVE.md`

---

## Session: May 31, 2026 — Container destination guard (stop unknown warehouse/property)

User: inbound containers to unknown warehouse or property **should be stopped**.

**Policy + tooling:** `docs/CONTAINER_DESTINATION_GUARD.md`, `scripts/lib/container-destination-guard.mjs`, `scripts/enforce-container-destination-guard.mjs`

- Validates Chain-iQ `container_loads.destination` against Registry-iQ property sites + canonical warehouses (Brighton TN BSI staging, UF blocklist warehouses, Garland TX, …).
- **Hold:** `status2 = HOLD:UNKNOWN_DEST` + audit note on `note`.
- Active inbound only (container # assigned or SHIPPED/LOADED/ARRIVED/… — not inert BOOKED placeholders).

**Bloomington dry-run (323 rows):** 282 OK (Hub II → Brighton TN staging + Bloomington IN site allowed), **3 HOLD** — air-freight rows to `INDIANPOLIS, IN 46219` (2×) and typo `BLOMINGTON, IN 47404`.

```bash
node scripts/enforce-container-destination-guard.mjs --dry-run --project "Bloomington"
node scripts/enforce-container-destination-guard.mjs --apply --project "Bloomington"
```

---

## Session: May 29, 2026 — Block project merge when project_id differs

User: "in the registry review if the project ids are different they should not be merge candidates"

**Rule:** If both sides of a **project** dedupe pair have non-null, non-empty `project_id` values and they differ, the pair is **not mergeable** (distinct deals — e.g. UF Sage deal # vs BSI NetSuite #). Pairs may still appear in the queue for Distinct / Reject; merge buttons and auto-sweep are blocked.

**SQL (Registry-iQ, applied live):**
- `scripts/migration-iqid-project-id-merge-block.sql` — `iqid_project_ids_block_merge(a,b)`, backfill `match_reason` (`project_id_left/right`, `project_id_conflict`, `merge_blocked`), sweep filter
- `scripts/migration-iqid-apply-merge-project-id-guard.sql` — guard at top of `iqid_apply_merge`
- `scripts/dedupe-scan-existing-registries.sql` — future project scan excludes conflicting pairs at INSERT

**dale-chat (needs deploy):**
- `lib/registry-review.ts` — `projectIdsBlockMerge`, `canMergePair`; `meetsAutoMergeRule` respects block
- `app/registry-review/page.tsx` — "not mergeable" badge, amber banner, disabled merge buttons
- `app/api/registry-review/[id]/decision/route.ts` — 422 on merge when blocked

**Live counts (post-backfill):** ~1,564 open project pairs flagged `merge_blocked`; project auto-sweep qualifying dropped to 0 (property sweep unchanged).

---

## Session: May 29, 2026 — Auto-merge sweep fixed (three blockers)

User: "the automerge is not working"

**Symptoms:** `iqid_sweep_auto_merge` found 843 qualifying pairs but every apply failed; 22+ review rows stuck at `review_status='merged'` with `applied_at IS NULL` and `apply_error` set.

**Root causes (all fixed live on Registry-iQ `xhafhdaugmgdxckhdfov`):**

1. **FK repoint type cast** — `iqid_apply_merge` read registry id values as `::text` then assigned them to uuid FK columns without cast → `column "property_id" is of type uuid but expression is of type text`. Fixed in `scripts/migration-iqid-fix-fk-repoint-types.sql` using `format_type()` + `$1::source_col_type`.

2. **Unique constraint on repoint** — project merges failed on `project_team_assignments (project_id, team_member_id, role)` when both projects had the same PM. Fixed in `scripts/migration-iqid-fk-repoint-dedupe-conflicts.sql`: DELETE loser child rows that would duplicate survivor rows on any unique constraint including the FK column, then repoint.

3. **project_status CHECK** — merge soft-delete sets `project_status = 'inactive'` but `project_registry` CHECK only allowed operational states + `deleted` (never `inactive`). Fixed in `scripts/migration-project-status-inactive.sql` (property already had `inactive`).

**Validation:** sweep `max_count=3` → **3/3 succeeded**. Retried all previously stuck merged pairs via `scripts/retry-stuck-merge-applies.sql` → **0 remaining apply_errors** on merged rows.

**Queue state post-fix:** ~840 qualifying auto-merge pairs remain; sweep button in dale-chat `/registry-review` should now increment `succeeded` counts.

---

## Session: May 29, 2026 — Combine identifiers on merge (don't discard loser's refs)

User: "can we arrange to combine project number or external references rather than writing over on a merge"

**Problem:** Prior merge logic kept survivor values on conflict — loser's `project_id`, `order_number`, or conflicting `external_ids` keys were silently dropped.

**SQL (Registry-iQ, applied live):**
- `scripts/migration-iqid-combine-identifiers-on-merge.sql`
- Helpers: `iqid_merge_external_ids`, `iqid_combine_scalar_identifiers`, `iqid_ext_append_unique`
- **external_ids:** survivor stays primary at each key; conflicting values combined in `external_ids.merged_refs[key][]` (e.g. `deal_number: ["26-1002-D","26-1002-E"]`)
- **Project scalars** (both non-null, different): survivor column unchanged; loser appended to:
  - `alternate_project_ids`, `alternate_order_numbers`, `alternate_d365_opportunity_codes`, `alternate_legacy_access_project_ids`
- **Property:** `property_code` → `alternate_property_codes`
- Coalesce-NULL + additive-only keys unchanged

**dale-chat:** Apply preview shows "Identifiers combined" + `merged_refs` detail (Derived-State).

---

## Session: May 29, 2026 — Registry review JSON crash + project detail edit

User: "i need to be able to edit project details + review on properties just crashed: Unexpected token '<', "<!DOCTYPE "... is not valid JSON"

**Root cause (review crash):** Client `fetch().json()` on `/api/registry-review/*` when the response is HTML (Clerk auth 404 page, stale deploy 404, or session expired) throws the opaque `Unexpected token '<'` error.

**dale-chat fixes (Derived-State, not yet pushed unless user commits):**
- `lib/read-json-response.ts` — checks `Content-Type` before parsing; surfaces actionable messages ("sign in again", "API route not found").
- `app/registry-review/page.tsx` — all review fetches use `readJsonResponse`.
- `app/api/registry-review/route.ts` — `status=resolved` now maps to `merged|approved_new|rejected`; `needs_review` includes `pending|auto_matched|ai_proposed`.
- `app/api/registry-review/stats/route.ts` — paginates past Supabase's 1000-row cap (~2934 pairs in queue).
- `app/api/project-registry/[id]/route.ts` — new admin-gated `PATCH` for core project fields.
- `app/project-registry/[id]/page.tsx` — **Edit project** mode (name, brand, division, type/status, stats grid, notes).

---

## Session: May 28, 2026 — Delete-row danger zone (L / R / both)

User: "what if there is something in the db for property that isnt a property at all. can you add a delete L, delete R, and Delete Both button? for any delete action ask a confirm click. not rows. entire columns/records"

Reviewers need a way to remove registry rows that aren't real entities (junk import, mis-categorized building, placeholder) without going through merge/distinct/reject — those decisions all assume both sides are real.

**SQL (Registry-iQ):**
- `scripts/migration-iqid-soft-delete-row.sql` — extends `property_status` / `project_status` CHECK constraints to accept new `'deleted'` value alongside the existing operational states, then adds `iqid_soft_delete_row(entity_type, row_id, reviewer, reason) → jsonb`.
- For property/project: sets `<entity>_status = 'deleted'`. For vendor/stakeholder/contact/facility: sets `is_active = false`. Always appends `[DELETED <ts> reason: ... by <reviewer>] (row marked deleted via registry-review; FKs left intact)` to `notes`.
- **FK policy: left intact.** Child rows continue to reference the soft-deleted row for audit. The row simply disappears from active queries. Hard DELETE was rejected as a design — RESTRICT would block the reviewer; CASCADE would silently nuke downstream data.
- **Cascade behavior:** any OTHER open dedupe_review pair referencing this row gets auto-rejected with `resolution='peer_deleted'` and an `[AUTO-REJECTED]` marker on reviewer_notes. Keeps the queue clean.

**dale-chat (Derived-State):**
- `POST /api/registry-review/[id]/delete-row` (admin-gated). Body: `{ which: 'left' | 'right' | 'both', reason?: string }`. Calls `iqid_soft_delete_row` for each target row sequentially, then overrides the current pair's resolution with a more specific tag (`'deleted_left' | 'deleted_right' | 'deleted_both'`) so the audit trail says exactly what the reviewer did versus what was a peer-cascade.
- New `DangerZone` component in `DetailPanel`'s action bar (below the 4 decision buttons): three rose-toned "🗑 Delete L / Delete R / Delete both" chips.
- Two-click confirm pattern (per request): first click arms the deletion — the panel morphs to a rose-bordered confirm card explaining what will happen (status flip + notes marker + FK-intact policy + peer-pair auto-reject) with an optional reason input. Second click executes; Cancel reverts.

**Status semantics post-change (property/project):**
- `active`/`prospect`/`under_construction`/etc. — operational
- `inactive` — manually inactive OR merged via `iqid_apply_merge`
- `deleted` — declared "not a real entity" via `iqid_soft_delete_row` (NEW)

---

## Session: May 28, 2026 — Coalesce loser values into NULL survivor columns on merge

User: "when i say merge L into R which fields take precedence if both are populated"

Tracing `iqid_apply_merge` revealed that the answer was "**R wins absolutely** — loser data never copied forward, even when survivor was NULL." Surprising and not what reviewers want. Standard CRM dedupe behavior is "survivor wins where populated; loser fills gaps." Switched to that.

**SQL (Registry-iQ):**
- `scripts/migration-iqid-coalesce-on-merge.sql` — updates both `iqid_dry_run_merge` and `iqid_apply_merge`.
- New step between external_ids merge and FK repoints: walk `jsonb_each(loser_row)`, find columns where `(survivor.col IS NULL AND loser.col IS NOT NULL)`, build dynamic `UPDATE ... SET col1 = COALESCE(s.col1, l.col1), col2 = COALESCE(s.col2, l.col2), ... FROM <table> l WHERE s.id=$1 AND l.id=$2`.
- Blacklist (never coalesced): `id, iqid, external_ids, notes, geo_point, created_at, updated_at, created_by, updated_by, normalized_name, display_name, property_status, project_status, is_active`.
- New action emitted in the plan: `{ kind: 'coalesce_fields', fields_filled: [{col, value}, ...], merge_policy: '...' }`. Both functions emit it — dry-run shows what *would* happen, apply shows what *did*.
- Verified live on Hub Gainesville University pair: dry-run reported 6 columns to fill (address_line1, owner_name, postal_code, property_url, total_beds=370, total_units=160).

**dale-chat (Derived-State):**
- `ApplyPanel` preview now renders a "Fields filled from loser" block: emerald header + 2-col grid (col-name | value) listing every coalesce target. Summary chip updated to include "N fields filled" count.

**Semantics summary (post-this-change):**
| Merge L into R, where R is survivor | Result |
|---|---|
| Both populated, different values | R wins (loser value preserved on soft-deleted row only) |
| R populated, L null | R wins (unchanged) |
| **R null, L populated** | **L fills the gap** (NEW) |
| Both null | stays null |
| `external_ids` keys | Additive merge: L keys not on R get copied. On key collision R wins. (Unchanged) |

---

## Session: May 28, 2026 — Inline edit of dedupe-relevant fields (replaces rename)

User: "rather than just rename can we open all fields to edit?"

Decision after scoping discussion: **Option A** — edit the ~10-20 fields per entity that actually drive a merge decision, not the full 20-50 column schema. The dedupe-relevant fields are: name, address, city/state/postal, brand/type, status/active, external_ids, plus user-requested additions `total_units`, `total_beds`, `opening_year` (the "year opened" column on property_registry), and `property_url` / `leasing_url`. For everything else, an "Open full record →" link on each side header opens the canonical detail page in a new tab.

Implementation:
- `dale-chat/lib/registry-edit.ts` — per-entity FieldSpec allowlist + `coerceFieldValue` (text/longtext/integer/boolean/jsonb), `fullRecordUrl(entity, id)` (falls back to list page for vendor/facility which don't have detail pages yet).
- `dale-chat/app/api/registry-review/[id]/edit-row/route.ts` — admin-gated. Filters body against allowlist, drops unchanged fields, runs one UPDATE, appends `[EDIT ts] col=old→new; ...` to notes. Refreshes `match_reason.name_left/right` if a name-bearing column changed. Does NOT recompute `match_score` (intentional — score is historical, edits inform the next decision).
- `dale-chat/app/registry-review/page.tsx` — replaced `NameHeader` with `SideHeader` (carrying ↗ link + ✎ edit chip + Save/Cancel). `EditableRow` + `EditableCell` render type-aware inputs (text/number/select/textarea). Allowlist columns that are NULL on both rows still surface in edit mode so reviewers can fill them. The decision/apply bar is locked while editing — Save or Cancel first.
- Deleted `dale-chat/app/api/registry-review/[id]/rename-row/route.ts` — the name column is now editable through the generic endpoint like every other field.

**Build-break prevention:** ran `npm install` + `next build` locally (first time `node_modules` has ever existed in `Derived-State/` — Vercel always built from scratch). Caught one TS error (Supabase `GenericStringError` union from `.select(<comma-string>)`) before push. Vercel build of `01a1881` succeeded first try, READY on production. Recommend running `next build` locally before every push from here on to avoid the silent ERROR'd-production state we hit earlier today.

---

## Session: May 28, 2026 — Production deploy unblock

User reported "still not seeing anything" after the AI pre-pass + rename-button work was pushed. Investigation: **both prior production deploys on `tlciq-platform` (`af34306`, `7c35782`) were in ERROR state** — production was stuck on the last successful build, so none of the recent work was actually live.

Two type errors caught by Vercel `tsc` that local dev never surfaced (the monorepo's TS install is broken at the workspace level, so `npm run dev` skipped these):

1. `dale-chat/app/components/RegistryLogoBox.tsx:242` — leftover hack `{publicIdFromCloudinaryUrl && null}` from an earlier unused-import suppression. Fixed by removing the import (`7a50744`).
2. `dale-chat/app/registry-review/page.tsx:450` — `isResolvedTerminal(status: ReviewStatus)` referenced a type that was never imported from `@/lib/registry-review`. Fixed (`9efb210`).

`9efb210` deployed `READY` to production. The AI pre-pass, sweep, apply worker, rename feature, stakeholder/project image galleries, and everything else since `59634f9` are all live on `tlciq-platform.vercel.app` for the first time.

**Lesson:** local `npm run dev` does not run `tsc --noEmit` and the workspace TS toolchain isn't reachable via `npx`. Need to either fix the local TS install or add a pre-push hook running the same lint+typecheck step Vercel runs. Otherwise build breaks ship silently to ERROR'd prod.

---

## Session: May 28, 2026 — Apply worker + Review UI Apply panel (closes the HITL loop)

**The full HITL dedupe pipeline is now end-to-end functional:** scan → review queue → decision → preview → apply → audit.

**SQL (Registry-iQ Supabase `xhafhdaugmgdxckhdfov`):**

- `scripts/migration-iqid-dry-run-merge.sql` — `iqid_dry_run_merge(entity_type, loser_id, survivor_id) → jsonb`. Walks `pg_constraint` at runtime, returns a plan: FK repoint counts per source table, external_ids merge with conflicts, soft-delete target, alias_insert. No mutations. Used by the preview endpoint.
- `scripts/migration-iqid-apply-merge.sql` — `iqid_apply_merge(entity_type, loser_id, survivor_id, reviewer) → jsonb`. Mutating companion: mints survivor.iqid if missing → merges external_ids additively (survivor wins on conflict) → UPDATEs every FK row → inserts `registry_alias` (loser name → survivor iqid, ON CONFLICT DO NOTHING) → soft-deletes loser (`property_status='inactive'`, `project_status='inactive'`, or `is_active=false`) → appends `[MERGED ...]` marker to notes. Atomic plpgsql transaction.
- `registry_dedupe_review` columns added: `applied_at timestamptz`, `apply_report jsonb`, `apply_error text`; partial index `(entity_type, applied_at) WHERE review_status='merged' AND applied_at IS NULL` for pending-apply queries.

**dale-chat code (Derived-State):**

- `/api/registry-review/[id]/apply-preview` (GET, admin-gated) — calls `iqid_dry_run_merge` for the resolved-merge pair, returns the plan jsonb.
- `/api/registry-review/[id]/apply` (POST, admin-gated, idempotent on `applied_at`) — calls `iqid_apply_merge`, persists `applied_at` + `apply_report`, or records `apply_error` on failure.
- `ApplyPanel` component on the registry-review page: three modes depending on pair state — `needs_review` shows the 4 decision buttons + notes; `merged & !applied_at` shows Preview → Confirm & Apply flow with FK row counts and external_ids conflicts surfaced; `merged & applied_at` shows the Applied badge + collapsible report; `apply_error` set shows a Retry path.
- `DedupePair` type and list-API select expanded with the new columns.

**FK landscape (queried via `pg_constraint` so it stays current):** 42 FK constraints across the 6 entity registries. Highest-coverage targets: `project_registry` (18 inbound FKs across schedule, install, scopes, milestones, etc.), `property_registry` (10 inbound), `stakeholder_registry` (7 inbound, includes self-FK on `parent_company_id`).

**Tested live against CAMPUS EDGE-RALEIGH × CAMPUS EDGE - RALEIGH (score 1.000):** dry-run reports 2 `property_stakeholders` rows to repoint, 5 external_ids conflicts (survivor wins, loser values audited), 1 soft-delete, 1 alias_insert. Total: 2 FK rows + 0 keys added (full overlap) + status flip.

**Remaining queue:**
- **#7c admin sweep** — bulk-apply all `merged & !applied_at` pairs that match the compound auto-merge rule (`score >= 0.98 AND (city_match+state_match OR external_ids_overlap OR per-entity-signal)`).
- **#6 ingestion rerouting** — switch all writes to go through `registry_intake_staging` instead of direct registry INSERT.
- **#4 AI pre-pass** — LLM drafts merge proposals into `ai_proposal` for the 0.75–0.98 band.
- **#13 Cloudinary MCP re-tag** — 33 Raleigh floorplan assets to `category:unit_layout` (DB already correct; cosmetic).

**Cumulative session totals: 8 Property_Registry commits + 7 Derived-State commits.**

---

## Session: May 20, 2026 — `iqid` identity scheme + intake staging + dedupe review

**Decisions (chat thread):**
- **Platform-wide identifier** on every registry entity: `iqid_<entity>_<10-char base32>` where prefixes are `prop | proj | vend | stake | cont | fac | team | scope | pos`. Charset excludes `0/o`, `1/l/i` so IDs are unambiguous on screen and in voice. Format chosen over `TLC-Pxxxxx` because `TLC-` is already overloaded in other systems and entity-typed prefixes (Stripe/Clerk-style) are self-describing.
- **HITL gating** — iqids are **not auto-assigned on insert**. Externally-sourced entities (property, project, vendor, stakeholder, contact, facility) flow through `registry_intake_staging`. Fuzzy match against existing rows + `registry_alias` → one of:
  - `auto_matched` (≥0.98) — one-click confirm
  - `ai_proposed` — AI pre-pass drafted merge proposal
  - `needs_review` (0.75–0.98) — human picks from top candidates
  - `approved_new` / `merged` / `rejected`
- **Smoke-tested:** `HUB Bloomington II Lincoln` vs `HUB Cloomington Lincoln II` → trigram similarity **0.800** → correctly lands in `needs_review`.
- **Existing dedupe** — same model: a one-time scan populates `registry_dedupe_review` with pairwise candidates. ≥0.98 auto-merge with audit log; **AI pre-pass** drafts proposals for the 0.75–0.98 band for bulk approval. Internal lookup registries (team, scope, position) get `iqid` only — no staging+HITL.
- **Same pattern across all entity types** — polymorphic single staging / alias / dedupe tables keyed by `entity_type` enum (`property | project | vendor | stakeholder | contact | facility | team | scope | position`).

**Migration applied (Registry-iQ Supabase `xhafhdaugmgdxckhdfov`):** `scripts/migration-iqid-and-intake-staging.sql`

**What landed:**
- Enums: `iqid_entity_type`, `iqid_review_status`
- Functions: `iqid_normalize_name`, `iqid_unaccent_immutable` (immutable plpgsql wrapper around `extensions.unaccent` — required for use in generated columns), `iqid_prefix`, `iqid_random_suffix`, `iqid_mint`
- New `iqid` column + partial unique index on all 9 registries (`property_registry`, `project_registry`, `vendor_registry`, `stakeholder_registry`, `contact_registry`, `facility_registry`, `team_registry`, `scope_registry`, `position_registry`)
- New `normalized_name` generated column + GIN trigram index on the 6 externally-sourced registries. **Note:** for `contact_registry`, the expression is `coalesce(first_name, '') || ' ' || coalesce(last_name, '')` because `display_name` is itself generated and `concat_ws` is STABLE (not IMMUTABLE).
- Tables: `registry_intake_staging`, `registry_alias`, `registry_dedupe_review` — all polymorphic over `entity_type`
- Unordered-pair uniqueness on `registry_dedupe_review` so `(A,B)` and `(B,A)` are the same review

**What's next (sequenced):**
1. **Dedupe scan** over existing 2,125 properties + 2,799 projects + 990 stakeholders + 943 contacts + 151 vendors + 26 facilities → populate `registry_dedupe_review` with pairwise candidates ≥0.75. Already-identical normalized names (e.g. `CAMPUS EDGE - RALEIGH` vs `CAMPUS EDGE-RALEIGH`, both normalize to `campus edge raleigh`) will surface as 1.0 matches.
2. **AI pre-pass** drafts merge proposals for the 0.75–0.98 band → stored in `registry_dedupe_review.ai_proposal`.
3. **HITL review UI** in dale-chat at `/property-registry/review` (and sibling entity types). Reviewer actions: approve-new (mint iqid), merge (alias the loser's name into winner's `registry_alias`), reject.
4. **Reroute ingestion paths** to write into `registry_intake_staging` instead of direct registry INSERT. First: `scripts/sync-install-schedules-to-registry.mjs`.
5. **Backfill iqids** on survivors only — after dedupe is settled.
6. **Guardrail follow-up migration** — add CHECK constraint or trigger blocking inserts without iqid once all ingestion paths are switched over.

---

## Session: May 28, 2026 — Dedupe scan ran; auto-merge rule tightened

**Applied:** `scripts/dedupe-scan-existing-registries.sql` (Registry-iQ Supabase migration `dedupe_scan_existing_registries`).

Two helper functions added: `iqid_external_ids_overlap(a, b)` (boolean — do two `external_ids` jsonbs share any non-empty key/value?) and `iqid_external_ids_shared(a, b)` (jsonb — the actual shared key/value pairs). Used by the scan to flag corroborating signals.

**Queue populated — 2,934 pairs total:**

| Entity | Total | Exact (1.000) | 0.90–0.97 | 0.85–0.89 | 0.75–0.84 |
|---|---:|---:|---:|---:|---:|
| project | 2,398 | 855 | 39 | 108 | 1,396 |
| property | 308 | 93 | 19 | 38 | 158 |
| contact | 190 | 157 | 3 | 6 | 24 |
| stakeholder | 36 | 1 | 3 | 7 | 25 |
| vendor | 1 | — | — | — | 1 |
| facility | 1 | — | — | — | 1 |

**Critical finding — name-similarity alone is NOT enough for auto-merge.** Score=1.000 pairs include genuine brand collisions where multiple distinct properties share the brand name (e.g. `Holiday Inn Express` × `Holiday Inn Express` — different hotels; `Hub on Campus` ×3 — same brand, different cities). Auto-merging on name alone would collapse brand chains.

**Tightened auto-merge rule (replaces "≥0.98 auto-merge"):**

```text
auto_merge IF
  match_score >= 0.98
  AND (
    (city_match AND state_match)
    OR external_ids_overlap
    OR (entity_type='contact'     AND email_match)
    OR (entity_type='stakeholder' AND website_match)
  )
```

Everything else (including score=1.000 brand-only matches) goes to HITL. The compound rule on the property queue surfaces ~30 obviously-true duplicate pairs as auto-merge candidates (e.g. `CAMPUS EDGE - RALEIGH` × `CAMPUS EDGE-RALEIGH` in Raleigh NC; `OLIV- SEATTLE` × `oLiv Seattle` with shared external_id).

**Next:** HITL review UI in dale-chat at `/registry-review` (polymorphic over entity_type). Then AI pre-pass.

---

## Session: May 28, 2026 — Standard registry image gallery + 8-category model

**User decisions (chat thread):**
- 8 image categories (framing A — keep `floorplan` + `unit_layout` split):
  `logo` (single, top-left brand box) · `hero` (single, banner) · `exterior` · `common_amenity` · `units` · `floorplan` · `site_plan` · `unit_layout`
- Standard image gallery component keyed by entity type — applies to property, project, vendor, stakeholder, facility (contact opts out; `photo_url` is a person headshot, distinct concept).
- All real binaries live in Cloudinary; Supabase stores metadata + `public_id` pointers. Use the Cloudinary MCP for any binary/tag drift cleanup.

**Migrations applied to Registry-iQ Supabase (`xhafhdaugmgdxckhdfov`):**

1. `scripts/migration-property-image-categories.sql` — normalize legacy URL-string entries into full image objects + default `role=exterior` for `gallery`/`null`. Hero entries preserved.
2. `scripts/migration-property-image-publicid-backfill.sql` — extract `public_id` + `format` from Cloudinary URL pattern (`image/upload/.../v{N}/{public_id}.{format}`) for entries with `public_id=''`. Deterministic, no Cloudinary API call needed. Backfilled the 33 Raleigh entries.
3. `scripts/migration-property-image-floorplan-recategorize.sql` — URL pattern `/floorplans/` or `_fp_` → `role=unit_layout` (per-unit-type drawings, per chat decision).
4. `scripts/migration-registry-image-columns.sql` — add standard image columns (`images jsonb`, `logo_image_url text`, `hero_image_url text`) to `project_registry`, `vendor_registry`, `stakeholder_registry`, `facility_registry` where missing. Stakeholder legacy `logo_url` migrated to `logo_image_url` + synthesized `images[role=logo]` entry; `logo_url` kept (deprecated). `contact_registry` intentionally untouched.

**Verification after all four:**
- property images: 267 `exterior`, 33 `unit_layout` (was 33 floorplan URLs), 4 `hero`. All objects now have non-empty `public_id` + `format`.
- 9 of 993 stakeholders had `logo_url`; all 9 now have `logo_image_url` populated AND a corresponding `images[role=logo]` entry.

**dale-chat code shipped (commit `4ff4531`):**

- `lib/registry-images.ts` — canonical types, `IMAGE_CATEGORIES` spec, `VALID_CATEGORIES_BY_ENTITY` map, single-occupancy spec, Cloudinary URL helpers (`cloudinaryFillUrl`, `cloudinaryLogoUrl`, `publicIdFromCloudinaryUrl`).
- `/api/registry-images/sign-upload` — generic Cloudinary signed-upload route accepting `entityType + entityId + category`; stable `public_id` for logo/hero (`<entityType>_<entityId>_logo|hero`) so re-uploads replace cleanly. Tags every Cloudinary asset with `entity:<type>`, `category:<value>` for future MCP-driven cleanup.
- `/api/registry-images/[entityType]/[id]` — GET/PATCH/DELETE; server-side validates category-per-entity + single-occupancy.
- `app/components/RegistryImageGallery.tsx` — new component: optional logo box, hero focal-point editor (preserved from old `PropertyImageGallery`), category filter chips, gallery grouped by category with per-image category dropdown.
- `app/property-registry/[id]/page.tsx` — uses `RegistryImageGallery` with `showLogoBox={false}` (the existing `PropertyLogoUploader` continues to fill the page-header brand box; phase-2 will migrate it to the new system).
- `enrich/route.ts` — RITA-enriched images now write `role='exterior'` instead of legacy `'gallery'`.

**Phase 2 (not yet shipped):**
- Wire `RegistryImageGallery` into stakeholder, vendor, facility, project detail pages.
- Add a `RegistryLogoBox` component (or migrate `PropertyLogoUploader`) that uses the new system and lives in the page header of every registry detail page.
- Cloudinary MCP pass: re-tag the 33 reclassified Raleigh assets to `category:unit_layout` so Cloudinary's media library matches the DB.

---

## Session: May 28, 2026 (cont) — HITL review UI built in dale-chat

**Repo:** `MyApps_Air/Derived-State` → `dale-chat/` (separate from Property_Registry repo). Not yet committed there — pre-existing unrelated work is also in flight in that checkout, so the iqid UI is staged separately and awaiting an explicit push directive.

**Files added to dale-chat:**
- `lib/registry-review.ts` — shared types (`EntityType`, `DedupePair`, `DecisionAction`), `meetsAutoMergeRule()` implementing the compound rule from this session, `displayNameFromRegistryRow()` for per-entity name extraction.
- `app/api/registry-review/stats/route.ts` — GET; counts per entity_type + score band.
- `app/api/registry-review/route.ts` — GET; lists pairs with entity_type / status / score-band filters, paginated, sorted by `match_score DESC`.
- `app/api/registry-review/[id]/route.ts` — GET; one pair with both underlying registry rows fully hydrated for side-by-side compare.
- `app/api/registry-review/[id]/decision/route.ts` — POST; records `merge_left_into_right` | `merge_right_into_left` | `distinct` | `rejected`. Calls `iqid_mint(et)` to mint iqids on survivor(s), inserts `registry_alias` for the loser's name on merge actions. Admin-gated via `isAdmin()`. **Does NOT yet move FK references or soft-delete losers** — that is the step-7 apply worker.
- `app/registry-review/page.tsx` — single polymorphic page with entity-type tabs (count badge per tab from stats), status + score-band filters, left list + right side-by-side compare panel, 4 action buttons (Merge L→R, Merge R→L, Distinct, Reject) + reviewer notes. Pairs auto-advance after a decision.
- `app/components/NavBar.tsx` — one line added: `Review` link next to `Property Registry`.

**Design:**
- Polymorphic, one URL (`/registry-review`) over all 6 entity types — backed by the polymorphic `registry_dedupe_review` table.
- Side-by-side compare auto-highlights fields where left ≠ right (amber background) so reviewers see at a glance which row has more data.
- Auto-merge candidates get an `auto` pill in the list using `meetsAutoMergeRule()` — same compound rule defined for the apply worker, so the UI hints at what the auto-pass will sweep up later.
- iqid minting happens at decision time (merge: survivor; distinct: both). Aliases captured for future fuzzy matches without needing a second pass.

**Important — RPC dependency:** the decision endpoint calls `registryIq.rpc('iqid_mint', { et: entityType })`. Verified callable with bare string (PostgREST casts to `iqid_entity_type` enum at the function boundary).

**What's not yet built:**
- Apply worker (step 7): walks resolved `merged` rows, copies `external_ids` from loser into survivor (jsonb merge), reassigns all FK references (property_buildings, property_floors, property_stakeholders, property_documents, project_registry.property_id, etc.) to survivor, soft-deletes loser. Per-entity FK landscape needs auditing before this runs.
- AI pre-pass (step 4): drafts merge proposals for 0.75–0.98 band into `registry_dedupe_review.ai_proposal` for reviewer consideration.
- Ingestion rerouting (step 6).

---

## Session: May 7, 2026 — Canonical taxonomy v1.1 — `property_buildings.form_factor` materialized

**Migration applied (Registry-iQ Supabase `xhafhdaugmgdxckhdfov`):** `Property_Registry/migrations/001_add_form_factor.sql`

**Canonical source of truth:** `/Cortex-iQ/canonical/CANONICAL_Hierarchies_v1_2026-05-07.md` (v1.1, §3 — Property Form Factor). Cortex embeddings live in `ddojpqanmxxfhpjnuqip.knowledge_embeddings` where `source_type='canonical_taxonomy'` AND `metadata->>'version'='1.1'`.

**What changed:**
- New nullable column `property_buildings.form_factor TEXT CHECK (form_factor IN ('High-Rise','Mid-Rise','Wrap','Garden-Style'))`.
- Backfilled from `total_floors` heuristic: 1-3 → `Garden-Style`, 4-5 → `Wrap`, 6-14 → `Mid-Rise`, 15+ → `High-Rise`. NULL where `total_floors` is unknown (manual classification needed).
- Index `idx_property_buildings_form_factor` on non-null values.
- Column comment cites canonical v1.1 §3.

**Why it matters here:** `form_factor` is the canonical 4-value enum used platform-wide (RITA classification, Process-iQ rate-card stratification by form, Vantage-iQ portfolio segmentation). Buildings whose floor count is unknown stay NULL — do NOT default-assign them.

**Linked migrations elsewhere (same canonical commit):** Chain-iQ `piq_ops_taxonomy` + `piq_uom`, Workforce-iQ migration 009 (canonical taxonomy materialization). See `/Workforce-iQ/MASTER_RECOMMENDATIONS.md` for the full v1.1 commit log.

---

## Session: April 13, 2026 — n8n → Chain-iQ automations audit

- **Instance:** `https://lcrdigital.app.n8n.cloud` (API healthy).
- **Primary scheduled ingest:** workflow **`Chain-iQ — Daily Box Ingestion`** (`gqlpPJ5fs7pOk091`) — **ACTIVE**; cron `0 12 * * 1-5` (timezone America/Chicago) → `POST https://chain-iq-ingestion-production.up.railway.app/ingest/from-box` → poll `/ingest/status/{job_id}`.
- **Issue found (fixed in n8n):** **Store Job ID**, **Log Success**, and **Log Failure** used legacy Set node `fields.values` / `stringValue`, which in Set v3.4 did not populate fields — **`job_id` was always empty**, so **Job ID Valid?** always failed and the run exited in ~0.6s without waits/polls (Railway job was still started in the first HTTP call). Migrated those three nodes to **`assignments`** (`type: string`).
- **Related (not Chain-iQ primary path):** `Chain-iQ — Auto Ingest from Box` (`li2Ed2VpFc5U261s`) and `TLCiQ — Daily Box Fetcher` (`yTsN8wpnksP8svGx`) are **inactive**. `TLCiQ Pipeline - Daily Scheduler` triggers **GitHub Actions** + Supabase log (pipeline, not Box ingest). Active **FreightPop**-named workflows may be legacy/alternate paths — confirm in n8n if still needed beside Railway Box ingest.

## Session: April 13, 2026 — EDISON vessel: Airtable + Chain-iQ link

- **Chain-iQ Supabase** (`vessel_registry`, id `6d434e5f-f52c-4209-a5d9-5fec37b832af`): MMSI **`235082896`** (already set); **`airtable_record_id`** was null — **VESSEL PROFILES** had no EDISON row (search by name returned empty).
- **Airtable** base `appErM42NIacUz8W6`, table **VESSEL PROFILES**: created record **`recfFzr9RFdd1FlXG`** — CMA, EDISON, MMSI# `235082896` (computed **`VESSEL_ID`** = `CMA_EDISON`).
- **Supabase update:** `airtable_record_id` = `recfFzr9RFdd1FlXG` so Chain-iQ vessel PATCH can dual-write MMSI/IMO to Airtable when those fields change.

## Session: April 12, 2026 — Warehouse registry + Field Ops / Site Management registry

- **`scripts/migration-warehouse-and-field-ops-registry.sql`** — Registry-iQ: `warehouse_registry` (site, scale_notes, contacts, dedupe_key), `warehouse_project_service` (warehouse ↔ project history, `last_seen_at`), `field_ops_registry` (people, `enrichment_status`, `role_category`), `field_ops_assignment` (person ↔ property + project + `assignment_role`), `project_registry.warehouse_registry_id`. Requires `pg_trgm` (script runs `CREATE EXTENSION IF NOT EXISTS`).
- **`scripts/sync-install-schedules-to-registry.mjs`** — After install-schedule row processing, upserts warehouses + links projects; find-or-create field ops from installer / site / sales / labor / warehouse contact; warehouse contact uses `warehouse_contact_email` when present. Prerequisite: warehouse migration applied. **`splitContactBlob()`** splits multi-person / multi-line schedule text into separate `property_stakeholders` + `field_ops` rows (newlines, `;`, `/`, noise-line filter).
- **`docs/WAREHOUSE_AND_FIELD_OPS_REGISTRY.md`** — schema + enrichment roadmap; **`docs/INSTALL_SCHEDULES_REGISTRY_LINK.md`** — cross-links.

## Session: April 11, 2026 — Sync retries + SKU count verification

- **`sync-production-to-registry.mjs`** — `withRetry` on PostgREST pages + whole-deal sync; flags `--retries` (default 4), `--retry-base-ms` (default 400); transient errors: `fetch failed`, 502/503, timeouts, etc.
- **`scripts/count-sku-sync-expectation.mjs`** — prints Production `requirements` (72,022 lines, all join `items.sku`) vs Registry `property_unit_type_skus` (~14.9k, deduped + project_registry scope).
- **Re-ran failed deals** `22-037`, `22-038` (transient fetch during mass run); both succeeded.

## Session: April 10, 2026 — Mass enrichment (in progress)

- **`sync-production-to-registry.mjs`** — added `--offset` / `--limit`, `--delay-ms`, chunked Production `deals` fetch by `deal_number` (200 chunks), non-numeric floor codes (E/S/B/T) → synthetic `floor_number` 1000+idx, floor lookup by label.
- **Mass run:** background loop from `--offset=8 --limit=50` through all pairs (1018 total; first 8 offsets covered by earlier smoke tests). Log: `scripts/enrich-mass-run.log` (gitignored `*.log`). If interrupted, resume from last **Next batch:** line in log.
- **`docs/PRODUCTION_REGISTRY_UNIT_PIPELINE.md`** — documented full sync script + batching.

## Session: April 10, 2026 — Production → Registry sync script (working)

### Delivered
- **`scripts/sync-production-to-registry.mjs`** — fully working sync: Production `deals` → Registry-iQ `property_unit_types`, `property_units`, `property_buildings`, `property_floors`, `property_unit_type_skus` (when `requirements` exist). Flags: `--deal=XX-NNN --property=UUID`, `--all`, `--dry-run`.
- **Test case: 23-016 Rambler Austin** — synced to Registry-iQ (`9665c5d8`): 70 unit types, 215 units, 1 building, 8 floors, 817 beds. Cross-checked Production vs Registry counts. 6 pre-existing marketing-named unit types preserved (no `production_unit_type_key`).
- **Mass enrichment audit:** `--all` resolves **1,018 deal→property pairs** via `project_registry.project_id` → `deals.deal_number`. Of those, ~5 deals currently have `requirements` rows (BOM/SKUs); the rest have unit types + units but no BOM.
- **`docs/PRODUCTION_REGISTRY_UNIT_PIPELINE.md`** updated with Sage crosswalk section + Rambler worked example.

### Validation
- Registry counts match Production: 215/215 units, 70/70 synced unit types
- Units have correct `floor_id` and `building_id` assignments
- `property_registry` totals updated: 215 units, 817 beds, 8 floors

### To run mass enrichment
```bash
node scripts/sync-production-to-registry.mjs --all --dry-run   # preview
node scripts/sync-production-to-registry.mjs --all              # live
```

## Session: April 9, 2026 — Sage pacing crosswalk doc + Rambler enrichment scan

### Delivered
- **`docs/PRODUCTION_REGISTRY_UNIT_PIPELINE.md`** — new section **Sage pacing reports → crosswalk (DALE-Demand)** (`sage_orders`, Vantage ETL pointers, join strategy). **Worked example: Rambler** — query filters, UF vs Blake Ramblers, pipeline rows with **null `deal_number`** (Ann Arbor, Riverfront A/B, College Park, West Lafayette, ’28 Blacksburg/Clemson) vs numbered deals (**`23-016-I`**, Athens, Columbus, Atlanta, **`26-048`** Tempe). Confirms Registry-iQ already has multiple RAMBLER property rows; gap is FF&E/unit data not stubs.

### How validated
- DALE-Demand: `sage_orders` + `pipeline_current` (ilike `%Rambler%`). Registry-iQ: `property_registry` ilike Rambler / Seton.

> **Git (canonical):** https://github.com/GJ1MPR1NT/Property-Registry — version history, scripts, and this file; clone for agents and CI.  
> **Local workspace:** this folder may still live under Dropbox on your machine; **push here** when you want a durable snapshot.

## Session: April 9, 2026 — Unit #s + Floors tabs (dale-chat)

### Delivered
- **Registry-iQ:** `property_units` migration (`scripts/migration-property-units.sql` in Property_Registry workspace / prior session) — required before APIs.
- **dale-chat UI:** **Unit #s** tab (`PropertyUnitsTab`) — table of unit numbers → unit type, building/floor, SKU line count & piece estimate from unit-type BOM; admin add/remove.
- **dale-chat UI:** **Floors** tab (`PropertyFloorsTab`) — `GET /floors-summary`: property total, per-building rollup, per-floor unit list + counts by unit type + SKU piece totals; **unassigned** (units with no `floor_id`).
- **Wiring:** `property-registry/[id]/page.tsx` — tabs after FF&E SKUs; `unitsFloorsRefreshKey` bumps when Buildings, Unit Types, FF&E SKUs refresh, or when units change so Floors stays current.
- **API tweak:** `floors-summary` tolerates missing `property_unit_type_skus` (empty SKU rollups).

### Ops
- Apply **`migration-property-units.sql`** on Registry-iQ if not already run.

### Follow-up (same period)
- **DALE-Supply split:** Tab renamed **Shipping** (was *Inventory & Shipping*). **Orders & inventory** (POs, line items, SKU lifecycle, match terms) lives on **FF&E SKUs**; **Shipping** shows containers + status only. Shared type: `property-dale-supply-types.ts`; components `PropertyDaleSupplyInventory`, `PropertyDaleSupplyShipping`.

## Session: April 10, 2026 — Production ↔ Registry unit types + SKU matrix (phase 1)

### Delivered
- **Registry-iQ migration** — `scripts/migration-property-unit-type-skus.sql`: table `property_unit_type_skus`; columns on `property_unit_types`: `layout_asset_urls`, `production_unit_type_key`. **Apply in Supabase** before APIs/UI.
- **dale-chat** — `GET/POST .../unit-types/[utId]/detail|skus`; unit-type create/update includes new columns; **`UnitTypeDetailModal`** on Overview (Unit mix) + Unit Types tab.
- **Docs / scaffold** — `docs/PRODUCTION_REGISTRY_UNIT_PIPELINE.md`, `scripts/sync-production-to-registry-unit-skus.mjs` (extend for Production BOM + Rosetta).

### Next
- Map Production BOM tables; batch match via Rosetta/address; RITA queue for new stubs.

## Session: April 10, 2026 — Context file refresh

- **Repo:** `main` aligned with `origin/main` (Property-Registry workspace).
- **Recent commits (this repo):** documentation-only updates that mirror **dale-chat** session notes already summarized below — Property Registry **API 500** fix (count-on-first-chunk + JSON error handling, April 8) and **map fly-to on list selection** (split layout + `focusCoordsKey`, April 9).
- **Update:** phase 1 of **Production ↔ unit-type SKU** pipeline now has migration + dale-chat UI/API in Derived-State; artifacts listed in session above.

## Related context (ecosystem)

**Hub (full index):** `/Users/geoffreyjackson/Dropbox/The Living Company/TLC iQ/Derived State/PROJECT_CONTEXT_Derived_State.md` — *Cross-module linkages (pointer)*. Same table on GitHub: [Derived-State/PROJECT_CONTEXT_Derived_State.md](https://github.com/GJ1MPR1NT/Derived-State/blob/main/PROJECT_CONTEXT_Derived_State.md).

**Most relevant siblings:** **Vantage-iQ** (Rosetta, pipeline context) `/Users/geoffreyjackson/MyApps/Vantage-iQ/PROJECT_CONTEXT_Vantage_iQ.md`; **RITA-v1** (enrichment, registry writes) `/Users/geoffreyjackson/MyApps/RITA-v1/PROJECT_CONTEXT_RITA_v1.md`; **DALE-Demand** `/Users/geoffreyjackson/MyApps/DALE-Demand/PROJECT_CONTEXT_DALE_Demand.md`; **Chain-iQ** (project tables substrate) `/Users/geoffreyjackson/MyApps/Chain-iQ/Chain-iQ/PROJECT_CONTEXT_Chain_iQ.md`; **TURBO / PMO-iQ** (stakeholder ownership on schedules) `/Users/geoffreyjackson/MyApps/TURBO/PROJECT_CONTEXT_TURBO.md`; **dale-chat** (platform app) `/Users/geoffreyjackson/Dropbox/The Living Company/TLC iQ/Derived State/dale-chat/PROJECT_CONTEXT_dale_chat.md`.

## Session: April 9, 2026 — Property Registry map fly-to on list selection

### Problem
- Choosing a property from the list did not **fly** the map to its pin because the right panel swapped to **detail-only** and **unmounted** `PropertyMapView`, so no map ran `flyTo`.

### Fix (dale-chat)
- **`app/property-registry/page.tsx`**: Split right column when a property is selected — **map on top** (~38vh max) + **scrollable `PropertyDetail` below**; `focusPropertyId={selectedProperty.id}` so the map animates to the pin. Optional **Back to full map** clears selection. **`mapProperties`** merges the selected property into the pin list when pin filters would exclude it (same pattern as map summary).
- **`app/components/PropertyMapView.tsx`**: Fly effect depends on **`focusCoordsKey`** (focus id + lat/lng presence) so `flyTo` retries when the focused row appears in `mapped` after markers refresh.

## Session: April 7, 2026 — Property Registry list cap + “hub” search

### Problem
- UI showed **1,000 of 2,134** properties: PostgREST default **max ~1,000 rows per response** capped the client even when the route asked for more.
- Search for **“hub”** could miss Hub-branded rows if matches sat **outside** the first 1,000 rows.

### Fix (dale-chat `app/api/property-registry/route.ts`)
- **`POSTGREST_PAGE = 1000`**: loop `range(from, to)` in 1k steps for both **search OR scans** and **non-search ordered lists**, merging chunks until the requested window is filled or the scan cap is hit.
- **`brand_name`** included in searchable columns and relevance haystack.
- **TypeScript**: narrowed `registry` after null check; `PropertyRegistryOrQuery` typed from a concrete `from().select().or()` template so filter chaining type-checks.
- **`npm run build`** — passes (April 7, 2026).

### Git repository (same day)
- **Remote:** https://github.com/GJ1MPR1NT/Property-Registry — `README.md`, `.gitignore`, `.env.example`; scripts + this file are the tracked workspace. **Derived-State** `PROJECT_CONTEXT_Derived_State.md` (*Cross-module linkages*) and **dale-chat** `PROJECT_CONTEXT_dale_chat.md` link to this repo.

## Session: April 8, 2026 — Property Registry list API 500

### Problem
- `/property-registry` showed **Failed to load Property Registry** / **API error: 500** (generic message when the response body was not `{ error: string }`).

### Cause
- List pagination (`POSTGREST_PAGE` loop) used **`select('*', { count: 'exact' })` on every chunk** (up to ~6 requests for `limit=5000`). PostgREST runs a full **COUNT** for each such request; repeated counts on a large `property_registry` table can **time out** or overload the API → **500**.

### Fix (dale-chat)
- **`app/api/property-registry/route.ts`**: pass **`includeTotalCount` only for the first `range()`** (`from === offset`); later chunks use `select('*')` without `count: 'exact'`.
- Wrap handler in **try/catch** so unexpected errors return JSON `{ error: string }` instead of an HTML error page.
- **`app/property-registry/page.tsx`**: parse **`error.message`** when `error` is an object.

### Deploy
- Change is under **Derived State / dale-chat**; deploy **tlciq-platform** (or your Vercel project for dale-chat) for production.

## Session: April 8, 2026 — Airtable: Projects → Properties sync

### Work completed
- **Cross-base Airtable sync** — iQ **Project Registry** (`appGJzC17TXngWMMx`) table **Projects** field `Property_Name` (fallback: text after ` - ` in `Project_Name`) compared to iQ **Property Registry** (`appz0l9XP1SiwQQ6c`) table **Properties** primary field **`Prprty_Name`** (normalized trim + lowercase).
- **Script:** `scripts/sync-projects-property-names-to-properties-airtable.mjs` — dry-run by default; `--apply` creates `Prprty_Name`, `object_uuid`, optional `Prprty_Full_Address` from Projects `City, State`, and `Prprty_Misc_Notes` provenance.
- **Run (April 8, 2026):** 33 Projects rows → 23 unique property names → **17** new Properties created (Hub/Oxenfree/House of Tricks sites that were not yet in the Property Registry base).
- **Note:** Airtable cannot natively link records **across bases**; alignment is by **name** (and optional address). Re-run the script after adding Projects to pick up new property names.

## Session: April 7, 2026 — RITA apply UX + registry links

### Work completed (dale-chat)
1. **RITA review panel** — “Select all fields” master checkbox; per-section “All” for stakeholders and unit types; unit types are individually selectable (`ut-{i}`). Footer summarizes fields, stakeholders, unit types, and gallery images. **Apply** uses `canApplyRita` so applying works when only stakeholders, unit types, or gallery images are selected (not only scalar fields). New-field checkboxes treat “unset” as selected (`!== false`) so defaults match initial `ritaAccepted`.
2. **RITA preview routing** — If `fields_found === 0` but unit types, stakeholders, or images exist, show the review panel instead of the archive prompt.
3. **Enrich confirm** — `external_ids` merge into `property_registry` for Sage/deal codes (inventory matching). `ANALYSIS_PROMPT` documents `external_ids`. Preview diff includes `external_ids`. Unit type inserts include `divided_bedrooms`, `half_baths`, `is_furnished`, etc., and log Supabase errors. Stakeholder duplicate check uses `stakeholder_id` when present. **`linkOrphanProjectsToProperty`** sets `project_registry.property_id` for orphan rows matched by name (exact ilike first, then substring; cap 500 with warning). Response adds `projects_linked`.
4. **Property detail UI** — Overview “Stakeholders” card lists registry-linked `property_stakeholders` with links to `/stakeholder-registry/{id}`. Stakeholders tab rows link to the registry when `stakeholder_id` is set. After RITA apply, refetch unit types, stakeholders, projects, and **inventory** so DALE-Supply data updates when `external_ids` change.

## Session: March 31, 2026

### Work completed
1. **Embassy Suites Flagstaff data fix (Registry-iQ)** — Canonical property `18d88b1d-cd0f-4fb4-9afc-60cf7eb9e206` renamed from "Embassy Suites Flagstaff - Model Room" to **Embassy Suites Flagstaff**. Project **23-1641-D** relinked to that property as **Model Room — Air Freight**; duplicate stub property `91837a4f-0bf5-44de-9266-cfd69a2c249d` (placeholder address) removed.
2. **Property detail — Projects tab** — New API `GET /api/property-registry/[id]/projects` reads `project_registry` by `property_id`, ordered by `updated_at` desc. UI tab **Projects** (immediately after Stakeholders) shows deal/project #, scope (product_scope / product_sub_scope / project_type), units (+ beds), revenue (total_contract_value) with financial visibility toggle respected.
3. **Financial data visibility (platform-wide preference)** — Clerk **Manage account → Financial data** page: Yes/No toggles `unsafeMetadata.showFinancialData` (`Y`/`N`) plus `localStorage` key `tlciq_show_financial_data`. `FinancialDataProvider` + `formatMoney` used on Property Projects and SKU Registry; DALE chat tools use `filterToolOutput` so summaries and structured payloads redact financial fields when preference is No (and cost/revenue tools are hidden). Also fixed stray backticks inside `DALE_SYSTEM_PROMPT` in `lib/dale-context.ts` that broke `next build`.
4. **Property Image Gallery with Cloudinary** — Full image management system for properties:
   - **Shared Cloudinary config** (`lib/cloudinary-config.ts`) — parses `CLOUDINARY_URL` env var; replaces inline parsing in enrich route.
   - **Sign-upload API** (`/api/property-registry/sign-upload`) — Cloudinary signed upload for browser-direct uploads; folder `property-registry/{propertyId}`, supports hero/gallery/logo roles.
   - **Images API** (`/api/property-registry/[id]/images`) — GET/PATCH/DELETE for property images JSONB array; syncs `hero_image_url` on every write; DELETE also removes from Cloudinary.
   - **PropertyImageGallery component** — replaces old hero-image-paste-URL section. View mode: hero with Cloudinary focal-point transforms, gallery thumbnails. Edit mode: drag-to-reposition focal point, scroll-to-zoom, add/remove images via Cloudinary upload, rename labels, set-as-hero, metadata display (dimensions, size, format).
   - **PropertyLogoUploader component** — separate logo field above property name in header. View: renders logo (max 200x80px) or nothing. Edit: dashed placeholder with + to upload, or replace/remove overlays. Uploads to Cloudinary as `logo_{propertyId}`.
   - **New DB column** `logo_image_url` on `property_registry` — migration SQL at `scripts/migration-add-logo-image-url.sql` (needs to be run on Registry-iQ dashboard).
   - `logo_image_url` added to API `allowedFields` on both POST and PATCH routes.
   - Enrich route refactored to use shared `getCloudinaryConfig()` instead of inline parsing.
   - Data model: `PropertyImage` interface with id, url, public_id, label, role (hero|gallery), focal_x, focal_y, zoom, width, height, format, bytes, uploaded_at — stored in existing `property_registry.images` JSONB array.
5. **RITA Deep Research Enhancement** — Major upgrade to RITA's enrichment pipeline:
   - **Expanded ANALYSIS_PROMPT** — property-type-specific unit counting (standard_units/upgrade_units for student/multifamily; guest_rooms/guest_suites for hospitality), stakeholder discovery (architect, designer, GC, PM, purchasing agent with firm names + website URLs), skip_13th_floor, loading dock, remodel year, unit/room type extraction, and comprehensive amenity capture including common areas.
   - **Phase 2c targeted searches (deep mode)** — 6-8 additional Firecrawl searches for units, bedrooms/rooms, floors/elevators/loading dock, year/remodel, architect/GC, PM/purchasing agent, amenities, and unit types. Property-type-aware queries.
   - **Stakeholder discovery pipeline** — after Claude extraction, each discovered stakeholder is looked up in stakeholder_registry (fuzzy match). Preview response includes status (`exists` with registry_id, or `new`). On confirm: new stakeholders are created in stakeholder_registry and all are linked to the property via property_stakeholders.
   - **FILLABLE_FIELDS expanded** — added standard_units, upgrade_units, guest_rooms, guest_suites, has_loading_dock, skip_13th_floor, designer_name, year_last_renovated, total_elevators.
   - **UI: Stats cards** — Units card shows sub-breakdown (80 std / 40 upg, or 200 rooms / 15 suites). Floors card shows "skips 13th" indicator. Elevators card shows "Loading dock" indicator. New `unitBreakdownLabel()` helper function.
   - **UI: RITA preview panel** — new Discovered Stakeholders section with name/role, "In Registry" (green) or "New — will create" (amber) badges, per-stakeholder checkboxes. New Unit/Room Types section showing extracted floorplan details.
   - **5 new DB columns** — standard_units, upgrade_units, guest_rooms, guest_suites, has_loading_dock. Migration: `scripts/migration-rita-deep-research-columns.sql`.
   - Claude max_tokens increased from 2000 to 4000 to handle richer schema.
6. **Bugfixes — Image gallery, logo, stakeholder linking:**
   - **Logo glassmorphic frame** — Logo image wrapped in `bg-white/[0.06]` backdrop-blur container so dark logos are visible on the dark platform background.
   - **Legacy hero preservation** — Images API PATCH no longer nulls `hero_image_url` when the `images` array is empty; preserves RITA-enriched hero URLs. Gallery component accepts `legacyHeroUrl` prop to display the existing hero even when gallery array is empty.
   - **Gallery file input fix** — Separate `<input>` elements for hero and gallery uploads; fixes role assignment bug where all uploads defaulted to gallery.
   - **Sign-upload error handling** — Catches non-JSON responses (Clerk auth redirects) instead of crashing on `JSON.parse`.
   - **Hero metadata overlay** — Edit mode shows label, dimensions, file size, and format in bottom-right overlay.
   - **Stakeholder link bug** — `POST /api/stakeholder-registry/[id]/projects` was storing the **property name** as `stakeholder_name` in the junction table instead of the **stakeholder's** name. Fixed to query `stakeholder_registry` for the correct company name.
7. **DB migrations applied (Registry-iQ):**
   - `logo_image_url` text column — `scripts/migration-add-logo-image-url.sql`
   - `standard_units`, `upgrade_units`, `guest_rooms`, `guest_suites` (integer), `has_loading_dock` (boolean) — `scripts/migration-rita-deep-research-columns.sql`

### Commits pushed (March 31)
- `e2316ac` — image gallery, projects tab, financial toggle, logo field (22 files, +1,706/-187)
- `f8457d0` — RITA deep research: targeted searches, stakeholder pipeline, unit breakdowns (5 files, +306/-26)
- `3baffd2` — logo glassmorphic frame, preserve legacy hero, gallery error handling (4 files)
- `dd6c013` — stakeholder link name fix (1 file)

## Session: March 29, 2026

### Work completed this session
1. **TLCH Hospitality Properties** — Created 6 missing properties (Capital Hilton, Hilton NASA, Marriott Sugar Land, Hilton Cameo Beverly Hills, Hyatt Regency LAX, Park Hyatt Beaver Creek) with Ashford Hospitality Trust as owner, Premier as PM, deal numbers associated. RITA batch-enriched all 6 (addresses, phones, URLs, hero images via Cloudinary, amenities, notes).
2. **Amenities bug fix** — `amenities_list` column is a Postgres `text[]` array; the enrich route was passing a comma-joined string. Fixed to pass the array directly.
3. **RITA web research tool** — Added `web_research` tool to RITA's chat interface (Firecrawl search + scrape), so she can research properties/companies/people from conversation. Previously she could only use web scraping through the wizards and "Call RITA" button.
4. **RITA consolidation** — Merged standalone RITA-v1 app (`rita-v1.vercel.app`, repo `GJ1MPR1NT/RITA-v1`) into the TLC iQ Platform. Migrated 7 AI sub-agents, 11 API routes, 9 lib modules, 6 UI components. One RITA now, at `tlciq-platform.vercel.app/rita`.
5. **Gemini API key** — Added `GEMINI_API_KEY` to dale-chat `.env.local` and Vercel production, enabling Gemini 2.5 Pro vision extraction and floorplan parsing for the first time.

### Commits pushed
- `b7d1820` — amenities fix + RITA web research tool
- `d211952` — RITA-v1 merge into platform (38 files, +8,634 lines)

## Project
- **Module:** Property Registry (`/property-registry`)
- **Codebase:** `/Users/geoffreyjackson/Dropbox/The Living Company/TLC iQ/Derived State/dale-chat/`
- **Supabase:** Registry-iQ (`xhafhdaugmgdxckhdfov` / `https://xhafhdaugmgdxckhdfov.supabase.co`)
- **Spec:** `iQ Property Registry — Cursor Development Prompt.md` (root of Property_Registry workspace)

## Supabase Project Landscape

| Project | ID | Purpose | Used by Property Registry? |
|---|---|---|---|
| **Cortex-iQ** | `ddojpqanmxxfhpjnuqip` | RAG, knowledge embeddings, feedback | No |
| **Chain-iQ** | `bpibnvwviqilpuuvcgdm` | Cascade-iQ, RITA, entity registry, sentinel scans | No (orphaned property tables dropped March 10) |
| **Registry-iQ** | `xhafhdaugmgdxckhdfov` | Canonical master data: properties, stakeholders, contacts, (future: SKUs) | **Yes — primary DB** |
| **DALE-Supply** | `rsswmgohprofvppvqlxu` | Supply chain data | No (future: Inventory Flow Machine) |
| **DALE-Demand** | `zfpscpxzmnkhhceoitig` | `sku_master`, demand/sales data | No (SKU Registry uses this) |

### Why Registry-iQ exists
Registries are **canonical master data** shared across the entire platform. Chain-iQ is operationally scoped (performance management, extraction AI). A dedicated Registry-iQ project cleanly separates master data from operational data, enables cross-module access, and scales independently.

## Key Decisions

1. **Build order:** Schema → vertical slice (list page + API) → NavBar/Home updates → create flow → detail page → enrichment/maps
2. **Registry-iQ Supabase created** — All property, stakeholder, and contact data now lives in a dedicated `Registry-iQ` Supabase project, separate from Chain-iQ and Cortex-iQ.
3. **Stakeholders are companies** — Developers, PE firms, architects, GCs, design firms, property managers. The contracted party (the company) is the stakeholder.
4. **Contacts are people** — Individuals stored in `contact_registry`, linked to stakeholder companies via `contact_stakeholder_associations` with `association_start` / `association_end` date ranges.
5. **TLC team handled separately** — TLC internal personnel managed through Team-iQ (sibling to Cascade-iQ), sourcing from `cascade_employees`. Not stored in stakeholder/contact registries.
6. **Google Maps: iframe embed pattern** (from Vantage-iQ Install Portal) — Uses Google Maps Embed API via `<iframe>` for satellite bird's-eye view.
7. **PostGIS + pg_trgm** enabled for geo queries and fuzzy dedup matching

## Registry-iQ Table Summary

| Table | Purpose | Key Indexes |
|---|---|---|
| `stakeholder_registry` | Companies | trigram on name, type, active flag |
| `contact_registry` | People | trigram on name, email |
| `contact_stakeholder_associations` | Person ↔ Company (date-ranged) | contact_id, stakeholder_id |
| `property_registry` | Master property record | PostGIS GIST on geo_point, GIN on name, type, status, city/state |
| `property_buildings` | Buildings per property | property_id |
| `property_floors` | Floors per building | building_id |
| `property_unit_types` | Unit type catalog per property | property_id |
| `property_stakeholders` | Property ↔ Stakeholder junction | property_id, role, stakeholder_id |
| `property_activity_log` | Audit trail | property_id, activity_date DESC |
| `property_documents` | File attachments | property_id |
| `pipeline_properties_parked` | Holding table for pipeline line items with no location data | parked_reason, parked_at |

## Data Archaeology

### Wave 1 — Initial Load (March 9, 2026)

**Script:** `scripts/ingest-property-archaeology.mjs` (supports `--dry-run`, `--verbose`, `--source=<name>`)

#### Sources Ingested

| Source | Table/Base | Rows Fetched | Properties Extracted | Notes |
|---|---|---|---|---|
| install_schedules | DALE-Demand Supabase | 1,318 | 1,146 | 172 skipped (no parseable name). Richest for lat/lng, warehouse contacts. |
| pipeline_opportunities | DALE-Demand Supabase | 46,948 (Closed Won) | 3,801 (deduped) | Only 1% had `project_city`/`project_state` populated. Most city/state is "Unknown". |
| BSI_ProjectSetup | Airtable `appVeaJJW1qmZrDaY` | 123 | 123 | Richest for building detail (units, beds, floors, elevators, parking). |
| Install-iQ Deal | Airtable `appC8sodqNVpO0Ci0` | 29 | 29 | Small but has direct address field and install status. |

#### Wave 1 Results

- **5,099** raw → **4,304** unique (795 merged via name+city+state key and deal_number fallback)
- **933** stakeholder companies, **890** contacts, **4,670** property-stakeholder links

### Wave 2 — Incremental Enrichment (March 9, 2026)

**Script:** `scripts/enrich-property-archaeology.mjs`

Three additional Airtable bases discovered and explored:

#### New Sources Explored

| Base | Base ID | Tables | Key Content |
|---|---|---|---|
| **iQ Property Registry** | `appz0l9XP1SiwQQ6c` | Properties (234), UnitTypes (91), Buildings (1), Floors (0) | Canonical property hierarchy with rich contact data: Developer_Contact (name/email/phone), Property_Contact (name/email/phone), GC, Purchasing Agent, Ownership, Year_Opened, property images. 68 fully enriched Core Spaces records + 166 sparser records. |
| **TLC Layout iQ** | `appG8wJwYkvtj4rFN` | Property List (66), Customer List (2), BSI Projects (123) | Clean separated address fields (City, State, Zip). 2 rich customer records (Core Spaces, LV Collective) with full HQ addresses, contacts, and websites. |
| **TLC Synapt iQ** | `appvYIeo91F0vI5XS` | Properties (8), Customer List (3), Projects (3) | Very early stage — only 2 real properties (Loews Miami Beach, Hilton Anatole). **Skipped for ingestion.** |

#### Wave 2 Results

| Metric | Before | After | Delta |
|---|---|---|---|
| Properties | 4,304 | 4,368 | +64 new |
| Stakeholders | 933 | 936 | +3 new |
| Contacts | 890 | 938 | +48 new/enriched |
| Property-stakeholder links | 4,670 | 4,778 | +108 |
| Contact-stakeholder associations | 0 | 2 | +2 (Core Spaces → Chad Mattesi, LV Collective → Taylor Johnson) |
| High quality (60+) | 500 | 605 | +105 |
| Unknown city | 3,055 | 2,959 | -96 filled |
| Fields enriched | — | 472 | Addresses, websites, unit counts, beds, parking, year built, images |

### Current Registry-iQ State (Post Wave 3 + Parking + Install-iQ Seeding + iQ PR UnitTypes)

| Table | Rows | Notes |
|---|---|---|
| `property_registry` | 2,491 | Active, location-verified properties |
| `stakeholder_registry` | 936 | Companies |
| `contact_registry` | 938 | People |
| `property_stakeholders` | 3,033 | Property ↔ Company links |
| `contact_stakeholder_associations` | 46 | Person ↔ Company links (44 from iQ PR + 2 from Layout-iQ) |
| `property_buildings` | 133 | Buildings (27 Install-iQ deals seeded) |
| `property_floors` | 46 | Floors |
| `property_unit_types` | 664 | Unit type definitions (577 Install-iQ + 87 iQ Property Registry) |
| `pipeline_properties_parked` | 1,197 | **Parked** — pipeline line items with no parseable location (PO numbers, model rooms, freight, etc.) |

#### Active Registry Field Completeness (2,490 properties)

| Field | Count | % |
|---|---|---|
| **City** | 2,490 | **100%** |
| **State** | 2,424 | 97.3% |
| **Property type** | 1,745 | 70.1% |
| **Developer** | 2,055 | 82.5% |
| **Address** | 858 | 34.5% |
| **Lat/Lng** | 362 | 14.5% |
| **Beds** | 360 | 14.5% |
| **Units** | 103 | 4.1% |

#### Quality Tiers (Active Registry)

| Tier | Count | % |
|---|---|---|
| High (60+) | 611 | 24.5% |
| Medium (30-59) | 1,879 | 75.5% |
| Low (0-29) | **0** | **0%** |

### Parking Operation (March 9, 2026)

**Script:** `scripts/park-no-data-properties.mjs`

Moved 1,197 properties with no parseable location data from `property_registry` to `pipeline_properties_parked`:
- 1,175 from pipeline_opportunities (PO numbers, model rooms, freight shipments, add-on orders)
- 11 from bsi_project_setup (contractor names used as property names, e.g., "Nancy Blake")
- 8 from iq_property_registry (state abbreviation placeholders)
- 3 from install_iq_deal

Each parked record preserves:
- Original complete property record (as `original_record` JSONB)
- Associated stakeholder links (as `stakeholder_links` JSONB array)
- `parked_reason: 'no_location_data'`

Properties can be un-parked if location data becomes available later.

### Wave 3 — Supabase ↔ Airtable Alignment (March 9, 2026)

**Scripts:** `scripts/wave2-enrich.mjs` (5 phases) + `scripts/wave2-parse-city.mjs` (Phase A2)

#### What was done

| Phase | Description | Impact |
|---|---|---|
| **A** | Backfill from pipeline_opportunities: match registry properties back to source for `bed_count`, `type_of_project`, `vertical`, `deal_number`, `account_name` | 1,117 properties enriched, 1,706 fields filled |
| **A2** | Parse city/state from property names using known-city lookup + regex patterns | 1,224 got city, 29 got state only = **1,253 properties enriched** |
| **B** | Backfill from install_schedules: lat/lng, city, state, address | 12 properties enriched |
| **C** | Insert unmatched Airtable records (iQ PR + Layout-iQ) with improved fuzzy matching | 17 new properties, 278 correctly matched to existing |
| **D** | Re-enrich matched Airtable records that still had fillable fields | 53 properties, 112 fields |
| **E** | Recalculate data_quality_score | 318 properties rescored |

#### Before → After Comparison

| Field | Before (Wave 2) | After (Wave 3) | Delta |
|---|---|---|---|
| **Total properties** | 3,670 | 3,687 | +17 new |
| **city = Unknown** | 2,420 (66%) | 1,197 (32%) | **↓1,223 (−51%)** |
| **state = Unknown** | 2,476 (67%) | 1,232 (33%) | **↓1,244 (−50%)** |
| **address = TBD** | 2,830 (77%) | 2,828 (77%) | ↓2 |
| **no lat/lng** | 3,315 (90%) | 3,325 (90%) | +10 (new inserts) |
| **type = other** | 967 (26%) | 943 (26%) | ↓24 |
| **no beds** | 3,646 (99%) | 3,010 (82%) | **↓636** |
| **no developer** | 435 (12%) | 446 (12%) | +11 (new inserts) |
| **Low quality (0-29)** | 358 | 196 | **↓162 (−45%)** |
| **Medium quality (30-59)** | 2,695 | 2,880 | +185 |
| **High quality (60+)** | 617 | 611 | −6 |

**Key wins:** City coverage jumped from 34% → 68%. Bed count coverage jumped from 1% → 18%. Low-quality properties nearly halved.

### Post-Load Deduplication (March 9, 2026)

**Script:** `scripts/dedup-properties.mjs`

**Root cause of duplicates:** The pipeline_opportunities source had ~47K "Closed Won" rows, many representing the same property with different `project_city`/`project_state` values (one populated, one null). Since Wave 1 dedup keyed on `name|city|state`, the same property appeared as separate rows when one had a city and another had "Unknown".

**Three duplication patterns addressed:**
1. **Same name, city known vs "Unknown"** — 489 groups
2. **Name suffix variants** — `(MUR)`, `(TLO)`, `(Millworks)` suffixes (product lines at same property) — 36 groups
3. **City spelling variants** — "Ft. Collins" vs "Fort Collins", "Austn, Tex" vs "Austin, TX" — 7 groups

**Dedup strategy:**
- Normalize names: strip parenthetical content, known suffixes, leading ticks/quotes
- Group by normalized name, then split by distinct real locations (avoid merging truly different properties like Hampton FYI in 8 different cities)
- Pick canonical (keeper) row: prefer real city > highest quality score > most external_ids
- Merge donor fields into keeper (fill nulls, combine external_ids)
- Re-point property_stakeholders from donors to keeper, delete donor rows

**Results:**
- 557 duplicate groups merged + 7 typo-based merges
- **698 rows deleted** (4,368 → 3,670)
- 164 stakeholder links re-pointed
- 7 remaining exact-name groups are legitimate (same brand, different cities)

#### Data Quality After Dedup

| Tier | Count | % |
|---|---|---|
| High (60+) | 621 | 16.9% |
| Medium (30-59) | 2,698 | 73.5% |
| Low (0-29) | 358 | 9.8% |

Unknown city reduced from 3,055 to ~2,420 (34% improvement through merging).

#### Data Quality Distribution

| Tier | Count | % |
|---|---|---|
| High (60+) | 605 | 13.9% |
| Medium (30-59) | 2,974 | 68.1% |
| Low (0-29) | 789 | 18.1% |

### Install-iQ Glide Seeding (March 9, 2026)

**Script:** `scripts/seed-from-install-iq.mjs --deal=<number>` (supports `--dry-run`)

**Source:** Airtable base `appC8sodqNVpO0Ci0` (Install-iQ Glide Data)

Tables used: Deal, Unit, Unit Requirement, Phase

| Deal | Property | Units | Beds | Buildings | Floors | Unit Types | Quality |
|---|---|---|---|---|---|---|---|
| 26-005 | Blanding-Kirwan (Univ of Kentucky) | 334 | 649 | 5 (A-E) | 4 each | 7 | 80 |

**What the script does:**
1. Fetches deal + all units from Airtable
2. Analyzes buildings (from unit location prefix A/B/C/D/E), floors (from unit number pattern), and unit types
3. Finds matching property in Registry-iQ by deal_number or name
4. Updates/creates property with address, hero image, counts, university, opening date
5. Creates buildings with floor breakdowns (units per floor)
6. Creates unit types with bed counts, unit counts
7. Recalculates data quality score

**Data extracted per deal:**
- Address, city, state, zip (parsed from deal address field)
- Hero image URL (from `image_building`)
- ERP dates, install status
- Building structure (from unit location codes)
- Floor-by-floor unit counts
- Unit type definitions with bedroom counts (parsed from type name, e.g. "2 BED TYPE B")
- SKU requirements per unit type

### Known Data Gaps
- **2,490 active properties** — all have city, 97.3% have state
- **1,632 properties** missing street address (65.5%) — not available in any connected source
- **2,128 properties** missing lat/lng (85.5%) — requires geocoding service (Google Geocoding API)
- **TLC internal contacts skipped** — `sales_person`, `project_manager`, `Sales_Rep` fields reserved for Team-iQ
- **Contact-stakeholder associations** — 46 created (44 from iQ Property Registry, 2 from Layout-iQ). Remaining ~892 contacts from install_schedules lack clean company associations (phone numbers in name fields, no company reference). Would need manual curation.
- **UnitTypes** from iQ Property Registry — ~~91 records not yet loaded~~ → **87 loaded** (4 had no property match: no Site_Id)
- **1,197 parked properties** in `pipeline_properties_parked` — can be un-parked if location data becomes available

## Cleanup Needed
- [x] ~~Drop empty `property_*` tables from Chain-iQ~~ → 8 tables dropped (property_registry, property_buildings, property_floors, property_unit_types, property_stakeholders, property_activity_log, property_documents, data_quality_issues)
- [x] ~~Enrich 2,959 properties with "Unknown" city/state~~ → 1,253 enriched via Wave 3, 1,197 parked
- [x] ~~Geocode Install-iQ properties~~ → 24 geocoded using server-side GOOGLE_MAPS_API_KEY
- [x] ~~Load 91 UnitTypes from iQ Property Registry into property_unit_types~~ → 87 loaded (4 unmatched)
- [x] ~~Build out contact_stakeholder_associations from all contact data~~ → 44 new associations created (46 total)
- [ ] Geocode remaining 2,104 properties missing lat/lng (requires batch geocoding with rate limits)
- [ ] Build contact_stakeholder_associations for install_schedules contacts (~892 remaining, data quality issues)

## Admin Editing Feature (March 8, 2026)

### What was built
Admin-only inline editing of property profiles directly from the app UI.

### Files Created/Modified
| File | Change |
|---|---|
| `lib/admin.ts` | Server-side admin check — uses `ADMIN_USER_IDS` env var or Clerk `publicMetadata.role=admin`. Falls back to allowing all users if no admin IDs configured (bootstrap mode). |
| `app/api/auth/admin-check/route.ts` | Client-facing endpoint returns `{ isAdmin: true/false }` for UI toggle |
| `app/api/property-registry/[id]/route.ts` | Added `isAdmin()` guard to PATCH and DELETE routes — returns 403 if not admin |
| `app/property-registry/[id]/page.tsx` | Added edit mode toggle (pencil button), inline editing for all overview fields, hero image URL editor with live preview, save/cancel with PATCH API |

### Editable Fields (Overview tab)
- **Hero image** — paste URL, live preview, remove button
- **Quick stats** — units, beds, buildings, floors, year built, elevators
- **Stakeholders** — owner, developer, brand, architect, designer, GC, property manager
- **Contact & Links** — phone, email, website, leasing URL
- **Campus** — university name, distance to campus
- **Location** — address, city, state, zip, latitude, longitude
- **Timeline** — opening date, year built, year last renovated
- **Notes** — multi-line textarea

### How it works
1. On page load, client calls `/api/auth/admin-check` to determine if user is admin
2. Admin users see an "Edit" pencil button in the property header
3. Clicking "Edit" enters edit mode — all overview fields become inline-editable
4. Changes are tracked in a diff object; "Save Changes (N)" button shows count of changed fields
5. Save sends PATCH to `/api/property-registry/[id]` with only changed fields
6. Success: property state updated optimistically, green toast shown for 3s
7. Error: red toast with error message, edit mode stays open
8. Cancel: discards all unsaved changes

### Configuration
To restrict admin to specific Clerk user IDs, set `ADMIN_USER_IDS` in `.env.local`:
```
ADMIN_USER_IDS=user_abc123,user_def456
```
If not set, all authenticated users are treated as admin (bootstrap mode).

## Deployment (March 8, 2026)

### TLC iQ Platform (dale-chat)
- **GitHub**: `GJ1MPR1NT/Derived-State` (branch: `main`)
- **Vercel**: `tlciq-platform` — https://tlciq-platform.vercel.app
- **Property Registry URL**: https://tlciq-platform.vercel.app/property-registry
- **Auth**: Clerk (all routes protected), admin editing via `ADMIN_USER_IDS` env var
- **Env vars added to Vercel**: `REGISTRY_IQ_SUPABASE_URL`, `REGISTRY_IQ_SUPABASE_ANON_KEY`, `REGISTRY_IQ_SUPABASE_SERVICE_ROLE_KEY`, `DALE_SUPPLY_SUPABASE_URL`, `DALE_SUPPLY_SUPABASE_ANON_KEY`, `NEXT_PUBLIC_GOOGLE_MAPS_KEY`, `FIRECRAWL_API_KEY`, `GOOGLE_MAPS_API_KEY`, `ANTHROPIC_API_KEY`, `GEMINI_API_KEY`

### Vantage-iQ (Registry State integration)
- **GitHub**: `GJ1MPR1NT/Vantage-iQ` (branch: `main`)
- **Vercel**: `dashboard` — https://vantage-iq.vercel.app
- **Changes**: Property Registry and Stakeholder Registry cards in Registry State page now show live status with real counts from Registry-iQ. Property Registry card has "Open in iQ Platform" button linking to tlciq-platform.
- **Env vars added to Vercel**: `REGISTRY_IQ_SUPABASE_URL`, `REGISTRY_IQ_SUPABASE_SERVICE_ROLE_KEY`
- **New file**: `dashboard/src/lib/supabase-registry-iq.ts` — Registry-iQ Supabase client for Vantage

## TLCH Hospitality Properties — Batch Add + RITA Enrichment (March 29, 2026)

### What was done
Added 6 missing Ashford Hospitality Trust / Premier-managed hotel properties to the registry, associating deal numbers from the TLCH pipeline. Each property was then batch-enriched by RITA.

### Stakeholders Created
| Name | Type | ID |
|---|---|---|
| Ashford Hospitality Trust | owner | `aa3490ee-f496-434f-b510-4ecac8ee945b` |
| Premier | property_manager | `4ff63cc9-fbee-42e7-be50-7ba25eed43a0` |

Note: "Premier Project Management" already existed as a GC (`106deb04`). The new "Premier" entry is the hotel management company.

### Properties Created/Updated

| Property | City | State | Deal Numbers | RITA Fields Applied | Hero Image | Confidence |
|---|---|---|---|---|---|---|
| Capital Hilton | Washington | DC | 24-1451-D, 24-1854-D, 24-2020-D, 24-1853-D, 25-1023-D | 4 (lat/lng, amenities, notes) — already had rich data from prior RITA run | Yes (Hilton.com) | 0.9 |
| Hilton Houston NASA Clear Lake | Houston | TX | — | 11 (address, zip, units, year, phone, email, url, hero, amenities, notes) | Yes (Cloudinary) | 0.95 |
| Marriott Sugar Land Town Square | Sugar Land | TX | 24-2102-D | 10 (address, zip, lat/lng, phone, email, url, hero, amenities, notes) | Yes (Cloudinary) | 0.95 |
| Hilton Cameo Beverly Hills | Beverly Hills | CA | 24-1208-D | 9 (address, zip, units, year, phone, url, hero, amenities, notes) | Yes (Cloudinary) | 0.95 |
| Hyatt Regency LAX Airport | Los Angeles | CA | 24-1909-D | 8 (address, zip, units, phone, url, hero, amenities, notes) | Yes (Cloudinary) | 0.9 |
| Park Hyatt Beaver Creek Resort and Spa | Beaver Creek | CO | 24-1940-D, 24-2184-D | 9 (address, zip, units, phone, url, hero, amenities, notes) | Yes (Cloudinary) | 0.9 |

### Key findings from RITA research
- **Capital Hilton**: 1001 16th St NW, DC 20036. Historic hotel less than 1 mile from McPherson Square Metro.
- **Hilton Houston NASA**: 3000 NASA Pkwy, Houston TX 77058. 242 rooms, built 1982, waterfront on Clear Lake across from Johnson Space Center.
- **Marriott Sugar Land**: 16090 City Walk, Sugar Land TX 77479. Full-service in Town Square, newly renovated rooms.
- **Hilton Cameo Beverly Hills**: 1224 Beverwil Dr, Beverly Hills CA 90035. 138 rooms, formerly Mr. C Beverly Hills, 5-star luxury boutique, completed transformation.
- **Hyatt Regency LAX**: 6225 W Century Blvd, LA CA 90045. 580 rooms including 21 suites, soundproof windows.
- **Park Hyatt Beaver Creek**: 136 E Thomas Pl, Beaver Creek CO 81620. 193 rooms, ski-in/ski-out luxury resort.

### Previously found in registry (from earlier search)
- Embassy Suites Flagstaff (2 records) — Flagstaff, AZ
- Embassy Suites Austin — Austin, TX
- Embassy Suites Palm Beach Gardens — Palm Beach Gardens, FL

### Script
`scripts/rita-enrich-batch.mjs` — Reusable batch enrichment script. Pipeline: Firecrawl search/scrape → Claude analysis → Cloudinary hero upload → Supabase PATCH (only fills empty fields) → activity log. Fixed `amenities_list` column type (Postgres array, not text).

## RITA Web Research Tool (March 29, 2026)

### What was built
Added `web_research` tool to RITA's chat interface, giving her the ability to search the web and scrape pages in conversation. Previously, RITA could only access the web through the wizard API routes and the "Call RITA" button — when users asked her to research something in chat, she correctly reported she couldn't access the web.

### How it works
- Uses Firecrawl API (search + scrape) directly from the chat tool handler
- User asks RITA to research a property/company/person → RITA calls `web_research` → Firecrawl searches and scrapes up to 5 pages → content returned to RITA for analysis
- Optionally accepts a direct URL to scrape (e.g., a specific property website or LinkedIn page)
- Results are role-filtered like all other RITA tools

### Also fixed
- `amenities_list` in `enrich/route.ts`: was passing a comma-joined string to a Postgres `text[]` column, causing "malformed array literal" errors when the "Call RITA" button tried to save amenities. Now passes the array directly.
- `maxDuration` increased from 60s to 120s for web research
- `maxSteps` increased from 6 to 8 to accommodate web research + follow-up analysis

### Files Modified
| File | Change |
|---|---|
| `app/api/rita-chat/route.ts` | Added `web_research` tool, increased maxDuration/maxSteps |
| `lib/rita-context.ts` | Added web_research to tool table + "WEB RESEARCH" section with usage guidance |
| `app/api/property-registry/[id]/enrich/route.ts` | Fixed amenities_list to pass array instead of joined string |

### Deployment
Commit `b7d1820` pushed to `main`. Vercel auto-deploy will pick up the changes.

## RITA Consolidation — Single Unified Agent (March 29, 2026)

### What was done
Merged the standalone RITA-v1 application (`GJ1MPR1NT/RITA-v1`, deployed at `rita-v1.vercel.app`) into the TLC iQ Platform (`dale-chat`). There is now **one RITA** at `tlciq-platform.vercel.app/rita`.

### Before (two RITA deployments)
| | Platform RITA | RITA-v1 (standalone) |
|---|---|---|
| **URL** | tlciq-platform.vercel.app/rita | rita-v1.vercel.app |
| **Repo** | GJ1MPR1NT/Derived-State (dale-chat/) | GJ1MPR1NT/RITA-v1 |
| **Features** | Chat, voice/TTS, 11 tools, registry enrichment, dedup | Document extraction, 7 sub-agents, PDF/CAD parsing |
| **AI models** | Claude | Claude + Gemini + GPT-4o |
| **Backend** | Serverless only | Next.js + Python FastAPI on Railway |

### After (one RITA)
Everything now lives in the platform (`dale-chat`). The Python extraction backend stays on Railway as a microservice — it's Python, can't merge into Next.js.

### What was migrated

**7 AI sub-agents** → `lib/rita-agents/`
| Agent | Model | Role |
|---|---|---|
| orchestrator.ts | Claude | Task routing, validation, cost hypothesis |
| vision.ts | Gemini 2.5 Pro | PDF/CAD parsing, field extraction |
| research.ts | GPT-4o | URL scraping, stakeholder lookup |
| linker.ts | Claude | Fuzzy match, dedup, CRM cross-ref |
| context-analyzer.ts | Claude | Entity extraction from user context |
| cost.ts | Claude | Cost hypothesis generation |
| floorplan.ts | Gemini | Floor plan parsing |

**11 API routes** → `/api/rita/*`
| Route | Purpose |
|---|---|
| /api/rita/upload | Document upload (public, no auth) |
| /api/rita/extract | Document extraction via orchestrator |
| /api/rita/status | Extraction job status |
| /api/rita/review | Human review interface |
| /api/rita/push | Push to registries |
| /api/rita/link | Entity linking/matching |
| /api/rita/identity | Entity identity resolution |
| /api/rita/analyze-context | Context analysis pipeline |
| /api/rita/projects | Project management |
| /api/rita/research | Web research agent |
| /api/rita/sign-upload | Signed upload URLs |

**9 library modules** → `lib/rita-*.ts`
| File | Purpose |
|---|---|
| rita-learning.ts | Supabase learning loop (corrections, patterns, events) |
| rita-cloudinary.ts | Buffer-based uploads for PDFs and page images |
| rita-cortex-v1.ts | Glossary, RAG search, terminology reference |
| rita-rosetta-sku.ts | SKU resolution via Rosetta canonical registry |
| rita-airtable.ts | Airtable CRUD helpers |
| rita-identity.ts | Entity identity service |
| rita-dynamics.ts | Dynamics 365 CRM integration |
| rita-registries.ts | Airtable registry writers |
| rita-document-types.ts | Document type classification profiles |

**6 UI components** → `app/components/Rita*.tsx`
FileUpload, ExtractionReview, ProgressTracker, ProjectSelector, RegistryPreview, AmbientBackground

### Import strategy
All migrated files use a `rita-` prefix to avoid conflicts with existing dale-chat modules (e.g., `lib/supabase.ts` → `lib/rita-learning.ts`, `lib/cloudinary` usage in enrich route stays separate from `lib/rita-cloudinary.ts`).

### Rewired references
- `RitaDashboard.tsx`: upload URL changed from `https://rita-v1.vercel.app/api/upload` → `/api/rita/upload`
- `property-registry/route.ts`: dead `parse-property-names` fire-and-forget call removed

### Env var alignment
RITA-v1 used `SUPABASE_URL` + `SUPABASE_SERVICE_ROLE_KEY_CORTEX` for Cortex. Updated to use dale-chat's existing `NEXT_PUBLIC_SUPABASE_URL` + `SUPABASE_SERVICE_ROLE_KEY`. One new env var needed: `GEMINI_API_KEY` (for Gemini vision extraction; degrades gracefully without it). `GEMINI_API_KEY` has been added to both `.env.local` and Vercel production via `vercel env add`.

### New dependencies
- `@google/generative-ai` — Gemini 2.5 Pro for vision extraction
- `lucide-react` — icons used by RITA-v1 components
- `openai` — GPT-4o for research agent

### What to decommission
- `rita-v1.vercel.app` — can be taken offline once confirmed working in platform
- `GJ1MPR1NT/RITA-v1` repo — archive, do not delete (historical reference)
- Railway Python service — **keep running**, called by `/api/rita/extract`

### Deployment
Commit `d211952` pushed to `main`. Vercel auto-deploy will build with all 11 new routes.

## Future Modules
- **Inventory Flow Machine** — dedicated pipeline tracker for Orders → Manufacturing → Ocean → Domestic → Warehouse/Pull → Install/Punch
- **Competitor Scanning** — requires Google Places nearby search
- **Team-iQ** — TLC internal team management, sourcing from Cascade-iQ `cascade_employees`
- **SKU Registry migration** — consolidate from Airtable + DALE-Demand into Registry-iQ
- **Batch Geocoding** — geocode the ~2,104 properties with addresses using Google Geocoding API (requires rate limiting)
- **Decommission RITA-v1** — archive `GJ1MPR1NT/RITA-v1` repo, take down `rita-v1.vercel.app` Vercel project (Railway Python service stays)

## Admin Editing — All Tabs (March 8, 2026)

Extended admin editing capabilities from Overview-only to all data tabs. When an admin is logged in, each tab now shows add/edit/delete controls.

### Stakeholders Tab
- **+ Add Stakeholder** button opens an inline panel
- **Search-first workflow**: type a name → debounced search against `stakeholder_registry` table → select existing or create new
- If stakeholder exists in registry, it's linked by `stakeholder_id` reference
- If new, simultaneously creates a `stakeholder_registry` record AND the `property_stakeholders` link
- Role, company, contact details, primary flag all editable during add
- **Delete** (X button, appears on hover) removes the `property_stakeholders` link

### Unit Types Tab
- **+ Add Unit Type** button opens a form with name, bedrooms, bathrooms, unit count, beds/unit, sqft, layout
- **Inline editing**: click pencil icon on any row to edit fields in-place, Save/Esc to commit
- `total_beds_this_type` auto-calculated from `unit_count × bed_count_per_unit`
- **Delete** via trash icon with confirmation

### Buildings Tab
- **+ Add Building** button with name and floor count
- Floors auto-created based on floor count
- **Delete** removes building and all associated floors (cascading)

### API Routes Created
- `POST/GET/DELETE /api/property-registry/[id]/stakeholders` — stakeholder CRUD + registry search
- `POST/PATCH/DELETE/GET /api/property-registry/[id]/unit-types` — unit type CRUD
- `POST/PATCH/DELETE` added to existing `/api/property-registry/[id]/buildings` — building CRUD
- All write operations guarded by `isAdmin()` check

## Batch Data Operations (March 9, 2026)

### Install-iQ Batch Seeding
- **Script:** `scripts/seed-from-install-iq.mjs --all` processed 27 deals from Airtable `appC8sodqNVpO0Ci0`
- Updated 26 existing properties, created 1 new property (MX-004 BTR Clear Creek)
- Created 139 buildings and 551 unit types across all seeded properties
- Fixed bed-parsing regex to match `(\d+)\s*(?:BED|bd)\b` (previously missed `2bd`, `1bd` suffixes)

### HUB Ann Arbor Deduplication (March 10, 2026)
- Deal 26-008 (keeper) had stakeholders + rich external_ids but no units
- Deal 26-009 (donor) had 11 buildings, 44 unit types, 267 units but no stakeholders
- Merged: moved buildings/unit-types from donor → keeper, combined external_ids, deleted donor
- Result: single `d7aea0dc` record with 267 units, 11 buildings, 2 stakeholders

### Bed Count Gap Fill (March 10, 2026)
- 481 unit types had `bed_count_per_unit = 0`; parsed bed counts from names like `C1 2bd`, `TH3 5bd`
- Updated 390 unit types, recalculated totals for 12 properties
- Key results: HUB Raleigh → 3,891 beds, HUB Ann Arbor → 871 beds, Clemson → 488 beds

## Create Property Wizard (March 10, 2026)

### What was built
- **Route:** `/property-registry/new` — multi-step form (5 steps: Identity → Location → RITA → Details → Review)
- **Admin-only:** Guarded by `isAdmin()` check on both client and API
- **Google Places Autocomplete:** Address field auto-fills city, state, zip, and lat/lng from Google
- **RITA Web Intelligence (Step 3):** Firecrawl search + scrape → Claude analysis → auto-fill form fields
- **"+ New Property" button** added to registry listing page header (visible to admins only)
- **POST API route** now guarded by `isAdmin()` check

### RITA Enrichment Pipeline
1. User enters property name + location in steps 1-2
2. Step 3 (RITA): user clicks "Research with RITA" (optionally provides direct URL)
3. **Firecrawl search** finds property pages on the web (up to 5 results)
4. **Firecrawl scrape** extracts markdown content from top 3 results (+ direct URL if provided)
5. **Claude (claude-sonnet-4-20250514)** analyzes all scraped content against a structured schema
6. Results displayed as field cards; user clicks "Apply to form" to fill empty fields
7. Only empty fields are filled — user's manual input is never overwritten
8. Source `rita_research` is recorded on the property if enrichment was applied

### Files Created/Modified
| File | Change |
|---|---|
| `app/property-registry/new/page.tsx` | 5-step wizard with RITA enrichment step (purple-themed) |
| `app/api/property-registry/research/route.ts` | RITA research API: Firecrawl search/scrape → Claude analysis |
| `app/property-registry/page.tsx` | Added admin check + "+ New Property" button |
| `app/api/property-registry/route.ts` | Added `isAdmin()` guard to POST handler |

### Dependencies Added
- `@types/google.maps` (dev) — TypeScript types for Google Maps API
- `@mendable/firecrawl-js` — Firecrawl SDK for web search and scraping
- `@anthropic-ai/sdk` — Claude API for content analysis
- `undici` — HTTP client required by Firecrawl SDK

### Environment Variables Required
- `FIRECRAWL_API_KEY` — Firecrawl API key for web search/scrape
- `ANTHROPIC_API_KEY` — Claude API key for content analysis
- `NEXT_PUBLIC_GOOGLE_MAPS_KEY` — Google Maps API key for Places autocomplete

## Inventory Tab Matching Improvement (March 10, 2026)

### What was changed
- **File:** `app/api/property-registry/[id]/inventory/route.ts`
- Added `deal_number` and `deal_code` from `external_ids` as match terms
- Added PO number prefix matching: searches for POs starting with the deal number (e.g., `26-005%`)
- Previously only matched on `property_name`, `property_name_alt`, and `sage_project`

## Geocoding — Resolved (March 10, 2026)
- Initial attempt failed: `NEXT_PUBLIC_GOOGLE_MAPS_KEY` was restricted to Maps Embed API only
- Discovered server-side `GOOGLE_MAPS_API_KEY` had Geocoding API enabled
- **24 Install-iQ properties geocoded** using the server-side key
- Remaining ~2,104 properties lack street addresses (65.5%), making geocoding impractical without address enrichment

## Contact-Stakeholder Associations (March 10, 2026)
- Re-derived contact→company links from iQ Property Registry Airtable source data
- **44 new associations created** (45 pairs found; 1 already existed for Chad Mattesi→Core Spaces)
- Data sources: Developer_Contact_Name→Prprty_Ownership, Prprty_ContactName→Prprty_Ownership
- All 45 contacts and stakeholders resolved in Registry-iQ (zero misses)
- Remaining ~892 contacts from install_schedules have data quality issues (phone numbers in name fields, multi-person records, no company reference)

## iQ Property Registry UnitTypes Load (March 10, 2026)
- Loaded 87 of 91 unit types from Airtable `appz0l9XP1SiwQQ6c` / UnitTypes
- 4 records had no `Site_Id` field (could not match to a property)
- Matched properties using normalized `Site_Id` (format: "PropertyName-City")
- Fields populated: unit_type_name, unit_count, bed_count_per_unit, total_beds_this_type, bathrooms, standard_bedrooms, divided_bedrooms
- Unit type count: 577 → 664

## Chain-iQ Cleanup (March 10, 2026)
- Dropped 8 empty orphaned tables from Chain-iQ Supabase (`bpibnvwviqilpuuvcgdm`)
- Tables removed: property_registry, property_buildings, property_floors, property_unit_types, property_stakeholders, property_activity_log, property_documents, data_quality_issues
- All had 0 rows — left behind after migration to Registry-iQ
- Applied via Supabase MCP `apply_migration`

## Deployment Updates (March 10, 2026)
- Pushed RITA wizard + inventory matching + new deps to GitHub (`4dd5af6`)
- Added `FIRECRAWL_API_KEY` and `GOOGLE_MAPS_API_KEY` to Vercel production env vars
- `ANTHROPIC_API_KEY` was already configured
- Successful production deployment: all 38 pages built, including `/property-registry/new` wizard and `/api/property-registry/research`

## Stakeholder Registry (March 10, 2026)

### What was built
Standalone stakeholder (company) management with RITA-powered enrichment and contact association.

### Pages
| Route | Purpose |
|---|---|
| `/stakeholder-registry` | Listing page with search, type filter, pagination |
| `/stakeholder-registry/new` | 4-step wizard: Identity → RITA Company Research → Contact Association → Review |

### RITA Company Intelligence
- Firecrawl scrapes company website (homepage, /about, /team, /leadership)
- Firecrawl search for company in web results
- Claude (claude-sonnet-4-20250514) extracts: type, legal name, HQ address, phone, email, logo, LinkedIn, description, year founded, portfolio size, key people (leadership team), notable properties, specialties
- "Apply to form" fills only empty fields

### Contact Association (Step 3)
- Search existing contacts from `contact_registry`
- Create new contacts inline (first name, last name, title, email)
- Associate multiple contacts to the stakeholder being created
- Marks first contact as primary

### API Routes
| Route | Methods | Purpose |
|---|---|---|
| `/api/stakeholder-registry` | GET, POST | List + create stakeholders |
| `/api/stakeholder-registry/research` | POST | RITA company web intelligence |

### Files Created
| File | Description |
|---|---|
| `app/stakeholder-registry/page.tsx` | Listing page with type badges, logo/initial display, LinkedIn links |
| `app/stakeholder-registry/new/page.tsx` | 4-step wizard following property wizard pattern |
| `app/api/stakeholder-registry/route.ts` | CRUD API with search/filter/pagination |
| `app/api/stakeholder-registry/research/route.ts` | RITA company research pipeline |

## Contact Registry (March 10, 2026)

### Data remediation (May 2026)
Scripts in `Property_Registry/scripts/`:
| Script | Purpose |
|---|---|
| `remediate-contact-registry.mjs` | Normalize phones/names; re-harvest iQ PR contacts; runs domain + property linkers |
| `link-contacts-by-domain.mjs` | Email domain → stakeholder match/create; Firecrawl/homepage company names; cache in `.cache/domain-company-cache.json` |
| `link-contacts-by-property.mjs` | `external_ids.property_hints` → property_stakeholders → company link |
| `reparse-install-schedules-contacts.mjs` | DALE `install_schedules_enriched` → `contact_registry` (warehouse + on_site_installer); warehouse address → stakeholder; Firecrawl domain pass + `link-contacts-by-domain` |

dale-chat: `/contact-registry/[id]` detail UI; `scripts/batch-contact-rita-enrich.mjs` for capped LinkedIn/photo backfill.

**May 2026 reparse run:** 1,168 enriched schedule rows → **775** new/updated contacts, **323** new person↔company associations, **1,297** `property_hints` on contacts; domain linker + Firecrawl polished remaining email domains.

### What was built
Standalone contact (person) management with RITA deep profiling — LinkedIn scraping, work history capture, behavioral/psychological profiling.

### Pages
| Route | Purpose |
|---|---|
| `/contact-registry` | Listing page with search, pagination, photo avatars |
| `/contact-registry/new` | 4-step wizard: Identity → RITA Deep Profile → Company Association → Review |

### RITA Deep Person Profile
- Firecrawl scrapes LinkedIn profile (if URL provided)
- Firecrawl search: `"FirstName LastName" "Company"` + `"FirstName LastName" interview OR profile OR bio`
- Claude (claude-sonnet-4-20250514) extracts structured profile:
  - **Identity**: title, email, phone, LinkedIn URL, photo/headshot URL
  - **Bio**: 1-2 paragraph professional summary
  - **Work History**: chronological positions with company, title, years, description
  - **Education**: institution, degree, field, year
  - **Notable Achievements**: awards, recognitions, published work
  - **Behavioral Profile** (only if substantial public presence): communication style, decision-making approach, priorities, likely motivators, engagement tips, public persona summary

### Psych Profile Design
The behavioral profile is designed as a practical business intelligence tool, not clinical assessment:
- **Communication Style**: How they communicate publicly (e.g., "Direct and data-driven")
- **Decision Making**: How they appear to make decisions (e.g., "Analytical — wants data and ROI")
- **Priorities**: What they publicly care about (e.g., ["Resident experience", "Sustainability"])
- **Likely Motivators**: What drives them professionally
- **Engagement Tips**: Practical advice for engaging this person (e.g., "Lead with data on resident satisfaction")
- **Public Persona Summary**: 2-3 sentence professional identity summary
- Only generated when RITA finds substantial public content (interviews, articles, speaking engagements)

### Company Association (Step 3)
- Search stakeholder registry for companies
- Link contact to multiple companies with role titles
- Surfaces RITA-found current company as a hint

### Data Storage
- Work history, education, achievements, and psych profile stored in `contact_registry.external_ids` JSONB
- Source tagged as `rita_research`
- Stakeholder links via `contact_stakeholder_associations`

### API Routes
| Route | Methods | Purpose |
|---|---|---|
| `/api/contact-registry` | GET, POST | List + create contacts. POST supports 3 modes: create new, link-existing (associate existing contact to stakeholder), link-only (create association for already-created contact) |
| `/api/contact-registry/research` | POST | RITA deep person research pipeline |

### Files Created
| File | Description |
|---|---|
| `app/contact-registry/page.tsx` | Listing page with photo avatars, name/title/email display |
| `app/contact-registry/new/page.tsx` | 4-step wizard with RITA deep profile display (work history timeline, education, psych profile cards) |
| `app/api/contact-registry/route.ts` | CRUD API with 3-mode POST (create, link-existing, link-only) |
| `app/api/contact-registry/research/route.ts` | RITA person research pipeline (Firecrawl + Claude) |

## Navigation Updates (March 10, 2026)
- **NavBar**: Added "Stakeholders" and "Contacts" links between Property Registry and SKU Registry
- **Home page**: Added Stakeholder Registry and Contact Registry app cards with descriptions and Live status badges

## Call RITA — Property Enrichment Button (March 10, 2026)

### What was built
"Call RITA" button on every property detail page (admin-only, purple-themed, next to Edit button) that triggers a full enrichment pipeline on the existing property.

### Pipeline
1. **Firecrawl search** — searches for property by name + city + state, returns up to 5 results
2. **Firecrawl scrape** — scrapes up to 6 pages total (direct URL from `property_url`/`leasing_url` if available + top 5 search results)
3. **Claude analysis** — extracts structured property data from all scraped content (claude-sonnet-4-20250514)
4. **Smart diff** — compares RITA findings against existing data; only fills empty/null/placeholder fields ("Unknown", "TBD", 0) — never overwrites real data
5. **Cloudinary image upload** — if RITA finds a hero image, uploads to Cloudinary (`property-registry/hero_{id}`) with auto-optimization (1600px max, auto format/quality) for permanent, fast-loading URLs
6. **PATCH property** — applies changes to the property record
7. **Activity log** — records what RITA found and applied in the property history tab

### UI
- Button shows spinning animation while RITA works (~20-40 seconds)
- Dismissible results panel appears below header showing: fields applied, fields skipped (already filled), Cloudinary upload status, source URLs
- Toast message summarizes outcome

### Files Created/Modified
| File | Change |
|---|---|
| `app/api/property-registry/[id]/enrich/route.ts` | **New** — RITA enrichment API with Cloudinary integration |
| `app/property-registry/[id]/page.tsx` | Added RITA state, `callRita()` function, button in header, results panel |

### RITA Scrape Depth Increase
- All RITA research routes (property, stakeholder, contact) bumped from 3 to 5 search result scrapes (6 pages total with direct URL)
- Provides better coverage for less prominent properties

### Dependencies Used
- `cloudinary` (v2, already installed) — image upload and transformation
- `CLOUDINARY_URL` env var — already configured locally and on Vercel

## Vercel Deployment Fix (March 10, 2026)
- GitHub auto-deploys were failing: "Couldn't find any `pages` or `app` directory"
- **Root cause**: Vercel was building from repo root (`Derived-State/`) instead of `dale-chat/` subdirectory
- **Fix**: Set Root Directory to `dale-chat` in Vercel project settings (Settings → General → Root Directory)
- Manual `vercel --prod` deploy confirmed working; GitHub auto-deploys will work once Root Directory is saved

### Google Maps API Key
- `NEXT_PUBLIC_GOOGLE_MAPS_KEY` — client-side key for Maps Embed (satellite view) and Places autocomplete
- `GOOGLE_MAPS_API_KEY` — server-side key for Geocoding API
- Maps Embed API must be enabled on the Google Cloud project for satellite views to work
- Required APIs: Maps Embed API, Maps JavaScript API, Places API, Geocoding API

## Future Roadmap (from deprecated spec)

The original 21-page spec (`iQ_Property_Registry_Cursor_Prompt_3.pdf`, March 2026) has been deprecated. The live system diverged in three fundamental ways: database moved from Chain-iQ to a dedicated Registry-iQ Supabase project, a project-centric model was added alongside the property model, and Rosetta-iQ replaced the proposed inventory flow tables. The following unbuilt features from that spec retain strategic value.

### 1. Competitor Scanning

Google Places API nearby search to discover competing properties around a subject property. Three-tier competitive radius based on NMHC Research Foundation data on student-competitive housing:

| Tier | Radius | Marker Color | Basis |
|------|--------|--------------|-------|
| Direct | 0–1 mile (or same campus) | Red | Pre-lease rates ~62% at <0.5mi |
| Moderate | 1–3 miles | Orange | Industry-standard competitive radius |
| Peripheral | 3–5 miles | Yellow | Fringe competition |

Requires a `property_competitors` table (property_id, competitor_property_id, distance_miles, competitive_intensity). Competitor properties would be created in `property_registry` with `tlc_relationship = 'competitor'` and enriched via RITA (web scrape for images, unit counts, opening date, developer, GC, architect).

**Dependencies:** Google Places API (nearbySearch), Google Distance Matrix API, PostGIS `ST_DWithin`.

### 2. Document Upload + RITA Parse — NOW LIVE

RITA-v1's document parsing pipeline is now consolidated into the TLC iQ Platform at `/rita`. Capabilities: PDF/DOCX/image upload → Gemini 2.5 Pro vision extraction + Claude structured analysis → extraction review UI → push to registries. Floorplan/CAD parsing via Gemini. Python FastAPI backend (Railway) handles heavy document processing. The Create Property wizard Path 2 can now leverage this pipeline.

### 3. Batch Geocoding

~2,100 properties still missing lat/lng. Strategy: use server-side `GOOGLE_MAPS_API_KEY` (Geocoding API enabled) with rate limiting (50 req/sec Google limit). Only viable for the ~860 properties that have street addresses. The remaining ~1,240 with "TBD" addresses need address enrichment first (RITA or manual). Estimated cost: ~$4 at Google's $5/1000 requests.

### 4. Portfolio Dashboard

Aggregate views across the entire property registry:
- Property type distribution (donut chart)
- Geographic distribution map (all properties plotted, clustered)
- Properties by status (bar chart)
- Top 10 properties by order value (requires Rosetta cross-reference to sales data)
- Data quality heatmap (which properties need enrichment — leverage existing `data_quality_score`)

### 5. Pipeline Kanban

Kanban board for property lifecycle: Prospect → Pre-Development → Under Construction → Active → Renovation → Turn in Progress. Drag-and-drop status changes that update `property_status` in registry. Value roll-up per column (requires Rosetta to pull contract values from DALE-Demand/Sage-iQ).

### 6. Map View

Full-width map mode for the property listing page:
- All properties as markers, color-coded by type or status
- Marker clustering when zoomed out (Leaflet MarkerCluster or Google Maps clustering)
- Click marker → popup card with property summary
- Filter controls overlay on left side of map
- Uses existing lat/lng from `property_registry` (362 properties currently geocoded)

### 7. Property Comparison

Side-by-side comparison of 2–4 properties using spider/radar charts. Axes: total beds, total units, amenity count, proximity to campus, property age, data quality score. Useful for competitive analysis and client presentations.

### 8. Turn Schedule View

Student housing–specific calendar view showing turn dates by property. Turn = the annual move-out/move-in cycle where all units are refurnished. Shows resource requirements per property, materials pipeline status, and delivery windows. Depends on `project_install_schedule` data being populated.

### 9. VesselFinder Integration

Low priority. API details for when container-to-property tracking is needed:
- **API:** `https://api.vesselfinder.com/vessels` (by IMO/MMSI)
- **Container Tracking:** `container.vesselfinder.com/api/1.0/` (by container number)
- **Cost:** Credit-based (1 credit per terrestrial AIS position, 10 per satellite)
- **Fleet option:** VESSELSLIST method (fixed monthly fee) — more cost-effective for regular carrier tracking
- Chain-iQ already has vessel data in `vessel_registry` and `container_loads`; VesselFinder adds real-time AIS positions

### 10. Cross-Module Deep Links

UI-level navigation between modules using Rosetta entity resolution:
- Vantage-iQ pipeline → click deal → open linked property in Property Registry
- SKU Registry → "Often appears at" section → property links
- Container tracking (Chain-iQ) → link to receiving property
- Stakeholder Registry → "All properties for this stakeholder" view
- Rosetta `/api/rosetta/resolve` provides the entity graph; UI needs link components

### Deprecated Spec Reference

**File:** `iQ_Property_Registry_Cursor_Prompt_3.pdf` (deleted March 2026)
**Original location:** `/Users/geoffreyjackson/Dropbox/The Living Company/TLC iQ/Property_Registry/`
**Reason for deprecation:** Core architectural assumptions (Chain-iQ as database, no project model, no Rosetta) were superseded by the live implementation. All valuable future-work items preserved above.

---

## Schema currency note (2026-05-09)

Earlier sections of this doc reference pre-migration-006 column names (`divided_bedrooms`, `total_bedrooms_effective`, `bed_count_per_unit`, `total_beds_this_type`). Those names are historical — they were renamed/replaced by migration 006 (April 2026, applied via `/Users/geoffreyjackson/MyApps/TURBO/migrations/006_property_unit_types_bedroom_taxonomy.sql`). The current `property_unit_types` schema is:

| Old name | New name | Notes |
|---|---|---|
| `divided_bedrooms` | `divider_bedrooms` | clearer (the divider is the barn door / vestibule) |
| `bed_count_per_unit` | `beds_per_unit` | shorter; sets up the floor → building → property bed-count hierarchy |
| `total_bedrooms_effective` | `bedrooms_structural` | now `GENERATED ALWAYS AS (...) STORED` — never write directly |
| `total_beds_this_type` | `total_beds_for_unit_type` | now `GENERATED ALWAYS AS (unit_count * beds_per_unit) STORED` |

New atomic categories: `shared_bedrooms`, `pod_bedrooms`, `murphy_bedrooms`, `super_murphy_living_rooms` (the last is a flex sleep zone, NOT a bedroom and NOT counted in `bedrooms_structural`).

New attributes: `unit_features text[]` (flexible tags) and `upgrade_tier` (`standard | compact | premium | vip`).

New bed-count roll-up columns on parent tables: `property_floors.total_beds_on_floor`, `property_buildings.total_beds_in_building`. These complement the existing `property_registry.total_beds`.

**Scripts in `scripts/`** updated 2026-05-09 to use the post-006 column set: `sync-production-to-registry.mjs`, `seed-from-install-iq.mjs`, `migrate-to-registry-iq.mjs` (the embedded `CREATE TABLE` for `property_unit_types` now reflects the post-006 schema; later additions like RITA enrichment tables and `fn_apply_rita_proposal` live in their own migration files in `/Users/geoffreyjackson/MyApps/TURBO/migrations/`).

---

## Session log — Jun 20, 2026 (enrichment campaign monitor)

**Deliverable:** Admin **Campaign monitor** on Morgan Hill property page (dale-chat `PropertyEnrichCampaignSection`).

- **API:** `GET /api/property-registry/[id]/enrich-campaign-status` (admin-only; includes active review URL).
- **Engine:** `loadEnrichCampaignStatus()` in `lib/property-enrich-review-engine.ts`.
- **UI:** Cyan panel above campaign preview — sent time, recipient, link opened, submitted, file counts, expires, copy active link, collapsible event log, refresh + 60s poll while pending.
- **SYNC-iQ dash:** Enrich-iQ `/sync/registry-enrich` — cross-property table + KPI cards; nav under Outbound; home page tile links to monitor. Reads Registry-iQ directly.
- **Live state (verified earlier):** `MH-REGISTRY-ENRICH-001` sent to `gjackson@livingcompany.com`, review pending (not opened), 9 items, 0 files.
- **Remaining:** Deploy dale-chat + Enrich-iQ to prod so monitors are visible without Supabase SQL.
