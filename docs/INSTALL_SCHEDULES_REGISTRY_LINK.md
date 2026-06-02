# Install Schedules ↔ Registry-iQ

Links **DALE-Demand** `install_schedules` / `install_schedules_enriched` into **Registry-iQ** as first-class `project_registry` rows under properties, with **`project_install_phases`** for each source row.

## Critical rule

**Projects must NEVER be collapsed, merged, overwritten, or deduplicated without explicit human approval.**  
Every install schedule row represents a real job. Sync is **additive**: empty fields are filled; differing non-null values **log a conflict** and are not overwritten.

See also: `.cursor/rules/project-data-integrity.mdc`, headers in `scripts/migration-access-2013-historical.sql` and `scripts/migration-install-schedules-registry.sql`.

See also **`docs/WAREHOUSE_AND_FIELD_OPS_REGISTRY.md`** — `warehouse_registry`, `field_ops_registry`, and junction tables (run `scripts/migration-warehouse-and-field-ops-registry.sql`).

## Schema (Registry-iQ)

Apply migrations as needed:

1. **`scripts/migration-install-schedules-registry.sql`** — `project_registry` DALE/install columns, `project_install_phases`, `documents` JSONB on `property_registry` / `property_unit_types` / `project_registry`.  
   The **`project_install_days.phase_id`** section runs **only if** `project_install_days` already exists (that table comes from **`scripts/migration-access-2013-historical.sql`**). If you apply Access 2013 **after** this file, **re-run** `migration-install-schedules-registry.sql` once so `phase_id` is added (or run the final `DO $$ … $$` block from that file only).

2. **`scripts/migration-access-2013-historical.sql`** — when used, creates `project_install_days` and related objects; run before or after step 1 as above.

3. **`scripts/migration-warehouse-and-field-ops-registry.sql`** — warehouses + field ops / site management registries and `project_registry.warehouse_registry_id`.

## Grain

- **One `project_registry` per `deal_number`** (e.g. `24-123-I`). Suffix `-I` / `-D` maps to `fulfillment_mode` (`install` / `dropship`).
- **`project_install_phases`**: one row per DALE schedule row (keyed by `dale_install_schedule_id` = source row UUID).
- **Property**: find by `external_ids.deal_number` (base deal) or fuzzy **city + state + name**; otherwise create stub (`source: install_schedules`, `property_type: student_housing` default).

## Field mapping (schedule → `project_registry`)

| DALE / enriched column | Registry column |
|------------------------|-----------------|
| `deal_number` | `project_id` (full string) |
| `property_name_address` | `project_name` (display) |
| `id` | `dale_install_schedule_id` |
| `d365_opportunity_code` | `d365_opportunity_code` |
| `d365_opportunity_name` | `d365_opportunity_name` |
| `d365_account_name` | `d365_account_name` |
| `d365_amount` | `d365_amount` |
| `d365_status` | `d365_status` |
| `d365_division` | `d365_division`, `division` |
| `d365_delivery_date` | `d365_delivery_date` |
| `schedule_year` | `schedule_year` |
| `install_start_date` | `install_start_date` |
| `estimated_completion_date` | `estimated_completion_date` |
| `num_days` | `num_days` |
| `property_contact` | `property_contact_name` |
| `warehouse_contact` | `warehouse_contact_name` |
| `warehouse_email` | `warehouse_contact_email` |
| `warehouse_address` | `warehouse_address` |
| `sales_person` | `sales_person` |
| `on_site_installer` | `on_site_installer` |
| `temp_labor` | `temp_labor` |
| `backup_temp_labor` | `backup_temp_labor` |
| `labor_options` | `labor_options` |
| `additional_items` | `additional_items` |
| `access` | `access_notes` (renamed — `access` is a SQL keyword) |
| `darlas_contact` | `darlas_contact` |
| `barstool_confirmed` | `barstool_confirmed` |
| `source_file` | `source_file` |
| `sheet_name` | `sheet_name` |
| `line_number` | `line_number` |

`images` / `documents` on `project_registry` are JSONB arrays for Cloudinary refs (same pattern as `property_registry.images`).

## Contacts → `property_stakeholders`

Free-text fields are attached as **`role: other`** with `notes` identifying the role (`property_contact`, `warehouse_contact`, `on_site_installer`, `sales_person`, `darlas_contact`, `temp_labor`, `backup_temp_labor`). Deduped by `stakeholder_name` per property.

**Multi-person cells:** The sync splits blobs on newlines, `;` (long lines), `/` (name pairs, not URLs), drops date-only and phone-only lines, strips common prefixes (`O:`, `C:`, `Ops Mgr`), and trims trailing phone patterns — then creates **one row per segment** in `property_stakeholders` and `field_ops_registry` / `field_ops_assignment`.

## What gets created (and what does not)

| Entity / concept | Created by sync? | Notes |
|------------------|------------------|--------|
| **`property_registry`** | **Yes** if no match by `external_ids.deal_number` or city/state/name fuzzy match | Stub: `source: install_schedules`, `tlc_relationship: customer`, placeholder `postal_code` until enriched |
| **`project_registry`** | **Yes** if no row with same `project_id` (full deal string) | Additive updates only fill NULL columns; conflicts logged |
| **`project_registry.property_id`** | **Linked** when missing | If project exists but `property_id` was null, sync sets it to the resolved/created property |
| **`project_install_phases`** | **Yes** (insert/update per `dale_install_schedule_id`) | One phase row per DALE schedule row |
| **`property_stakeholders`** | **Yes** for install text fields | Inserts **junction-only** rows: `stakeholder_id` **null**, `role: other`, `notes` = role hint. Deduped by property + `stakeholder_name` |
| **`stakeholder_registry`** (companies) | **No** | Names stay denormalized on `property_stakeholders` unless you extend the script |
| **`contact_registry`** (people) | **No** | Use **`field_ops_registry`** for schedule-sourced people; `contact_registry` not populated here |
| **`contact_stakeholder_associations`** | **No** | — |
| **`project_registry.customer_stakeholder_id`** | **No** | Not set from install schedules |
| **Vendors** | **No** | No separate vendor entity; warehouse contact is a `property_stakeholders` row with notes `warehouse_contact` |
| **`warehouse_registry` + `warehouse_project_service`** | **Yes** (after `migration-warehouse-and-field-ops-registry.sql`) | Deduped warehouse from address/contact/email; service history per project; sets `project_registry.warehouse_registry_id` when null |
| **`field_ops_registry` + `field_ops_assignment`** | **Yes** (same migration) | People from installer / site / sales / labor / warehouse contact fields; assignments to property + project for scheduling |

“**Customer**” in the sense of **`property_registry.tlc_relationship = 'customer'`** is set on **new property stubs** only (TLC’s relationship to that site). It does **not** create a `stakeholder_registry` customer company from D365 account.

## Sync script

**Env:** `DALE_DEMAND_SUPABASE_URL`, `DALE_DEMAND_SUPABASE_KEY` (same as dale-chat; `DALE_DEMAND_SUPABASE_SERVICE_ROLE_KEY` also accepted), `REGISTRY_IQ_SUPABASE_URL`, `REGISTRY_IQ_SUPABASE_SERVICE_ROLE_KEY`

```bash
node scripts/sync-install-schedules-to-registry.mjs --dry-run --all
node scripts/sync-install-schedules-to-registry.mjs --apply --year=2025
node scripts/sync-install-schedules-to-registry.mjs --apply --deal=24-123-I
node scripts/sync-install-schedules-to-registry.mjs --apply --all
```

Requires `--all` or a filter (`--deal=` / `--year=`) to avoid accidental full runs.

Source table: prefers **`install_schedules_enriched`**, falls back to **`install_schedules`**.

## RITA / Cloudinary

- **dale-chat** `lib/cloudinary-ingest.ts` — `downloadToCloudinary()` for remote URLs + metadata.
- **Research** `POST /api/property-registry/research` — ingests `hero_image_url` to Cloudinary; sets **`hero_image_source_url`** when the source was external.
- **Enrich confirm** — hero + gallery use `downloadToCloudinary`.
- **Backfill** (Registry repo): `scripts/backfill-images-to-cloudinary.mjs` — non-Cloudinary `hero_image_url` values → Cloudinary + `enrichment_sources` audit entry.
