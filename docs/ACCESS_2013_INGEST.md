# Access 2013 → Registry-iQ ingest

Historical FF&E data from `Database2.accdb` / `Access/2013SQLite.db` (SQLite export).

## 1. Apply migration (Registry-iQ SQL editor)

Run **`scripts/migration-access-2013-historical.sql`** once. It adds legacy columns, `project_install_days`, `project_install_capacity_plan`, and the view (see script header).

## 2. Auto-create (default)

The loader **`creates `property_registry` and `project_registry` rows when none match`**. You do **not** need a mapping file unless you want overrides or to disable auto-create.

**Match order (per Access `ProjectID`):**

1. Optional **mapping** override (`project_registry_id` and/or `property_id`)
2. `project_registry.legacy_access_project_id` = Access `ProjectID`
3. `project_registry.project_id` = parsed deal (`YY-NNN` from `Description`)
4. `property_registry.external_ids->access_2013->>legacy_project_id`
5. `property_registry.external_ids->>deal_number` = parsed deal
6. **Create** stub property + project (`--auto-create`, **default**)

**New property stub** uses: `property_name` from `Description` (deal code stripped), `property_type='student_housing'`, address `TBD` / `Unknown` / `00000`, `source='access_2013_import'`, `external_ids.access_2013` + `deal_number`.

**New project** uses: `project_id` = deal code, or `access-2013-{ProjectID}` if no `YY-NNN` in `Description`, plus legacy Access columns from migration.

**Flags:**

| Flag | Meaning |
|------|---------|
| `--auto-create` | Default on **implied**; use **`--no-auto-create`** to require a mapping file for every row |
| `--no-auto-create` | Only match existing rows; **requires `--mapping`** |
| `--mapping=` | Optional JSON overrides (see below) |
| `--min-legacy=` | Skip Access `ProjectID` below this (default **46** — test projects) |
| `--apply` | Without this, dry-run (no writes) |

**Examples:**

```bash
# SQLite only (no env — sample deal codes)
node scripts/ingest-access-2013-sqlite.mjs --dry-run

# Preview resolution against Registry (needs REGISTRY_IQ_*)
node scripts/ingest-access-2013-sqlite.mjs --dry-run

# Full load with auto-create
node scripts/ingest-access-2013-sqlite.mjs --apply

# Force existing rows only + manual mapping
node scripts/ingest-access-2013-sqlite.mjs --apply --no-auto-create --mapping=Access/access-2013-mapping.json
```

## 3. Optional mapping overrides

`Access/access-2013-mapping.example.json`:

```json
{
  "_comment": "Optional. Numeric keys = Access ProjectID.",
  "46": { "project_registry_id": "uuid-of-existing-project-row" },
  "47": { "property_id": "uuid-of-property", "project_registry_id": null }
}
```

- **String value** = treat as **`project_registry_id`** (legacy behavior).
- **Object** with **`project_registry_id`** → use that row (must have `property_id`).
- **Object** with **`property_id`** only → attach or create **`project_registry`** for that property + deal.

Non-numeric keys (e.g. `_comment`) are ignored.

## 4. SKU / pull lists

- **`property_unit_type_skus.qty_per_box`** stores Access `Item.QtyPerBox` for line-level pull math.
- Master pack size should still align with **SKU Registry** when available.

## 5. Add-on / parent projects

Not automated; set **`parent_project_registry_id`** manually or a follow-up SQL pass.
