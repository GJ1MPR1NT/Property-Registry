# Warehouse registry + Field Ops / Site Management registry (Registry-iQ)

Orchestrates **physical warehouses** (staging / ship points) and **people** (installers, site contacts, sales on jobs) so scheduling and estimating can roll up **which warehouse served which project** and **which field-ops people touched which property/project**.

## Schema

Apply in Supabase (Registry-iQ):

```text
scripts/migration-warehouse-and-field-ops-registry.sql
```

### Tables

| Table | Purpose |
|-------|---------|
| **`warehouse_registry`** | Canonical warehouse/site: `dedupe_key`, name, address, geo, `scale_notes` (enrich later: sq ft, docks), primary contact/email/phone, `external_ids`, `source`. |
| **`warehouse_project_service`** | Many-to-many: warehouse ↔ project, with `dale_install_schedule_id` provenance, `first_seen_at` / `last_seen_at`. Unique `(warehouse_registry_id, project_registry_id)`. |
| **`field_ops_registry`** | People: `dedupe_key` (email preferred, else normalized name), `display_name`, `first_name` / `last_name` (parsed from schedules), `email`, `phone`, `role_category` (`field_ops`, `site_management`, `installer`, `sales`, `labor`, `other`), `enrichment_status` (`pending` → `partial` → `enriched`). |
| **`field_ops_assignment`** | Links a person to **property + project** with `assignment_role` (`on_site_installer`, `property_contact`, `warehouse_contact`, `sales_person`, …). Unique `(field_ops_registry_id, project_registry_id, assignment_role)`. |

### `project_registry`

- **`warehouse_registry_id`** — Optional FK to the primary warehouse for that job (additive; set only when null).

## Install schedule sync

`scripts/sync-install-schedules-to-registry.mjs` (with `--apply`):

1. **Warehouse** — If `warehouse_address`, `warehouse_contact_name`, or `warehouse_email` is present: find-or-create `warehouse_registry` by normalized dedupe key, insert/update `warehouse_project_service`, set `project_registry.warehouse_registry_id` if still null.
2. **Field ops** — For each non-empty field (installer, property contact, sales, Darla’s, temp labor, warehouse contact): find-or-create `field_ops_registry`, upsert `field_ops_assignment` to the current property + project. Warehouse contact row gets `warehouse_contact_email` when available.

**Denormalized `property_stakeholders` rows** are still written for quick UI parity; canonical people live in **`field_ops_registry`** + **`field_ops_assignment`**.

## Enrichment (next steps)

- **Warehouses:** Parse city/state/ZIP from `warehouse_address`; fill `scale_notes` from site visits or contracts; photos/docs in `external_ids` or a future `documents` JSONB.
- **Field ops:** Resolve duplicate names via email; backfill phone from CRM; promote `enrichment_status` to `enriched` when validated.

## API / UI

Not in this repo yet: add dale-chat routes or views that list `warehouse_registry`, `warehouse_project_service` by project, and `field_ops_assignment` by property or person for scheduling screens.
