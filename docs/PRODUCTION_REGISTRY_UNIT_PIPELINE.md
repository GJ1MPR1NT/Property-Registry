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

## Rosetta & DALE

- Register Production **source_system** and identifiers (site id, deal, normalized address hash).
- Use **rosetta_resolve** / batch jobs to link Production → `property_registry.id`.
- **RITA** enriches new stubs after create.

## Pacing / aging

- Join **replacement_year** / **cohort_year** on `property_unit_type_skus` when Production or pacing reports supply them.
- Historic pacing reports: validate totals; optional separate ingest table later.
