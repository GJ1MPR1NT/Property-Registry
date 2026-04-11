# PROJECT_CONTEXT — Property Registry

**Last updated:** April 10, 2026

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
