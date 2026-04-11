# Production ↔ Registry: unit types, rooms, SKUs

## Goal

Connect **TLCiQ-Production** FF&E / unit-type / SKU data to **Registry-iQ** so each property has:

- `property_unit_types` with optional `layout_asset_urls`, `production_unit_type_key`
- `property_unit_type_skus` — SKU, qty per unit, optional room label, replacement/cohort years

**UI:** Property Overview **Unit mix** and **Unit Types** tab open a modal (`UnitTypeDetailModal`) with layout + SKU list + Cloudinary thumbnails via `/api/sku-images`.

## Database

Run in Supabase (Registry-iQ):

`scripts/migration-property-unit-type-skus.sql`

## APIs (dale-chat)

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/api/property-registry/[id]/unit-types/[utId]/detail` | Unit type + SKUs + rollups |
| GET | `/api/property-registry/[id]/skus` | Property-wide FF&E matrix: `lines`, `by_sku`, `summary` (many SKUs ↔ many unit types) |
| POST | `/api/property-registry/[id]/unit-types/[utId]/skus` | Admin: replace SKU lines (`{ skus: [...] }`) |
| PATCH | `/api/property-registry/[id]/unit-types` | Includes `layout_asset_urls`, `production_unit_type_key` |

**UI:** Property detail has an **FF&E SKUs** tab (alongside **Unit Types**) with “By assignment” and “By SKU” views.

## Sync script

`scripts/sync-production-to-registry-unit-skus.mjs` — scaffold; extend with Production table mapping and Rosetta/address matching.

### Full production → Registry sync (unit types, units, floors, SKUs) — **recommended**

`scripts/sync-production-to-registry.mjs` pulls **TLCiQ-Production** `deals` → Registry-iQ `property_unit_types`, `property_units`, `property_buildings`, `property_floors`, and **`property_unit_type_skus`** when `requirements` + `items` exist for that deal.

| Flag | Purpose |
|------|---------|
| `--deal=23-016` | Single deal (uses `project_registry` for `property_id` if omitted) |
| `--deal=… --property=<uuid>` | Explicit Registry property |
| `--all` | Every `project_registry.project_id` ↔ Production `deal_number` pair (~1,018) |
| `--offset=N --limit=50` | Batch (resume after interruption — use **Next batch** line from prior run) |
| `--delay-ms=35` | Throttle between deals (optional) |
| `--retries=4` | Retry transient network errors (`fetch failed`, 502/503, timeouts) per page + per deal |
| `--retry-base-ms=400` | Base delay for exponential backoff (default 400) |
| `--dry-run` | No writes |

**Mass run (example):** process 50 deals at a time, then continue from printed offset until `1008` (covers deals 8–1017 of the sorted list; deals 0–7 run separately or overlap is idempotent).

**SKUs — how many rows should exist?**

| Source | Count (verified via PostgREST) |
|--------|----------------------------------|
| Production `requirements` (BOM lines) | **72,022** |
| Same, inner-join `items` where `sku` not null | **72,022** (every line maps to an item with a SKU) |
| Registry `property_unit_type_skus` after sync (`source = tlciq_production`) | **~14–15k** (dedupe + only deals in `project_registry`) |

Rows are **not** 1:1 with Production lines: `property_unit_type_skus` has `UNIQUE (unit_type_id, sku, room_label)`, so repeated BOM lines collapse. Run `node scripts/count-sku-sync-expectation.mjs` anytime to re-check.

**SKUs:** Only deals with **non-zero `requirements`** rows linked to `unit_types` get `property_unit_type_skus` — roughly **13%** of Production deals today; the rest still get unit mix + units + floors.

## Rosetta & DALE

- Register Production **source_system** and identifiers (site id, deal, normalized address hash).
- Use **rosetta_resolve** / batch jobs to link Production → `property_registry.id`.
- **RITA** enriches new stubs after create.

## Pacing / aging

- Join **replacement_year** / **cohort_year** on `property_unit_type_skus` when Production or pacing reports supply them.
- Historic pacing reports: validate totals; optional separate ingest table later.

## Sage pacing reports → crosswalk (DALE-Demand)

Sage **Order Detail** sheets from the periodic pacing `.xlsx` files are loaded into **DALE-Demand** Supabase table **`sage_orders`** (same project as `pipeline_current`, `install_schedules`, etc.). This is **not** Registry-iQ data, but it is a strong join layer for **deal number**, **Sage order number**, and **names**.

| Source | Location |
|--------|----------|
| ETL | Vantage-iQ repo: `scripts/load-sage-pacing.js` (sheets like `2026 Order Detail`, `2025 Order Detail`) |
| DDL | `supabase/migrations/add_sage_orders.sql` |
| Docs | `PROJECT_CONTEXT_Vantage_iQ.md` — Order Archive + pacing dedupe by `order_number` |

**Useful columns for Production ↔ Registry matching**

| Column | Role |
|--------|------|
| `order_number` | Sage order number (from `ORDNUMBER`) |
| `reference` | Sage `REFERENCE` — often encodes deal-style tokens parsed by the ETL |
| `ship_name`, `customer_name` | `SHPNAME`, `CUSTOMER NAME` — fuzzy match fallback to D365 |
| `d365_opportunity_code` | When the ETL matched Sage → Dynamics (`pipeline_current`) |
| `delivery_year`, `snapshot_date` | Pacing / archive cut |

**How to use it in this pipeline**

1. Match **`sage_orders.reference`** / **`order_number`** parsing → **`pipeline_current.deal_number`** (or use pre-filled **`d365_opportunity_code`**).
2. Same **`deal_number`** appears on **TLCiQ-Production** `deals.deal_number` and install views — use it to drive **`production_unit_type_key`** and unit/SKU sync once **`property_registry.id`** is resolved (Rosetta, address, or manual).
3. **`ship_name`** / **`customer_name`** help disambiguate duplicate deal codes or find sibling phases (e.g. multiple “Rambler” sites) before enriching **`property_unit_types`** / **`property_units`**.

Keep **`sage_orders`** as **read-only** input for crosswalk; authoritative FF&E lines still come from Production **`requirements`** / **`items`** (or Registry after sync).

### Worked example: Rambler (UF) — Feb 2026 snapshot

Run against **DALE-Demand** (same project as `sage_orders` / `pipeline_current`):

```text
sage_orders:  .or('ship_name.ilike.%Rambler%,customer_name.ilike.%Rambler%,reference.ilike.%Rambler%')
pipeline_current:  .or('opportunity_name.ilike.%Rambler%,account_name.ilike.%Rambler%')
```

**Findings**

- **`sage_orders`** returns UF Sage lines (e.g. `23-016-I`, `25-010-I`, `26-048-I`) with **`ship_name`** / **`customer_name`** disambiguating **Austin, Athens, Atlanta, Columbus, Tempe**, etc. Rows may appear twice with different **`snapshot_date`** (Order Archive year-end vs pacing load); dedupe by **`order_number`** preferring the latest snapshot (same rule as Vantage `/api/pacing`).
- **`pipeline_current`** lists **commercial deal numbers** on many rows (e.g. **`23-016-I`** → opportunity *Rambler - 2513 Seton, ATX*). Those map to **TLCiQ-Production** `deals.deal_number` for install/unit/SKU pulls.
- **Registry-iQ** already has multiple **RAMBLER \*** property rows (Austin Seton, Athens, Atlanta, Columbus, Tempe, etc.); **`project_registry`** can carry **`23-016`** ↔ Austin property. Gaps are mostly **FF&E matrix** (`property_unit_type_skus`, `property_units`), not the property stub.

**Higher-value “more deals to enrich” (pipeline Rambler, `deal_number` is null)**

These opportunities exist in **D365** but have **no `deal_number` yet** — harder to join to Sage order refs and Production install keys until numbering or Rosetta catches up:

| Opportunity (abridged) | Account | Status |
|------------------------|---------|--------|
| Rambler West Lafayette | LV Collective | Lost |
| Rambler Ann Arbor | LV Collective | Open |
| Rambler Riverfront Bldg A / B | LV Collective | Open |
| Rambler College Park | LV Collective | Open |
| ’28 Rambler Blacksburg / Clemson | Blake / GC accounts | Open |

Prioritize **Open** LV Collective rows for **property stub + external_ids**, then backfill **`deal_number`** when sales assigns codes. **Blake Solutions** Ramblers use a different division path — still enrich Registry if they are customer sites.

**Already numbered (good for Production ↔ Registry sync)**

Examples: **`23-016-I`**, **`23-016-I-AC1`**, **`23-016-I-AC2`** (Austin); **`24-033-I`**, **`24-205-I`** (Athens); **`25-004-I`**, **`25-010-I`**, **`25-1920-D`**, **`25-1979-D`** (Columbus / Atlanta); **`26-048`** (Tempe — note D365 `deal_number` **`26-048`** vs Sage order **`26-048-I`**). Use **`deal_number`** + property match to drive **`production_unit_type_key`** and unit/SKU sync.
