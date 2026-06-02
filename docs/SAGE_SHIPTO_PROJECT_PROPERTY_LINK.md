# Sage ship-to → project → property linking

Links unlinked `project_registry` rows to `property_registry` using Sage 300 ship-to data from DALE-Demand `sage_orders`.

## Data sources

| System | Project | Table |
|--------|---------|-------|
| DALE-Demand | `zfpscpxzmnkhhceoitig` | `sage_orders` (ship_to_* + `property_key`) |
| Registry-iQ | `xhafhdaugmgdxckhdfov` | `project_registry`, `property_registry`, `project_location_candidates` |

**Snapshot dates in DALE-Demand `sage_orders` (verified 2026-05-28):**

| `snapshot_date` | Rows | Role |
|-----------------|-----:|------|
| **2026-05-31** | 5,702 | **Latest Sage API header sync** — ship_to populated on 5,701 rows |
| **2026-05-27** | 2,638 | Enrichment pass — includes `property_key` (2,631 rows) |
| **2026-12-31** | 878 | **FY2026 year-end pacing snapshot** (Excel-style), **not** a live sync — zero ship_to / property_key |

The date `2026-12-31` is a **fiscal year-end pacing label**, same convention as `2025-12-31`, `2024-12-31`, etc. It is stored with a calendar date in the future relative to “today” because it represents the **end of FY2026**, not “data as of Dec 31, 2026.” For operational joins, treat **2026-05-31** as the current sync and **2026-05-27** when you need `property_key`.

This script loads only rows with `ship_to_city` populated, then dedupes by `order_number` preferring richer rows (`property_key`, then newer snapshot). Year-end pacing rows without ship_to never enter the pipeline.

## Matching doctrine — ship-to is always in scope

All Sage ↔ registry matching in this repo should use `scripts/lib/sage-shipto-match.mjs`:

1. **`enrichSageOrder(row)`** — compute `property_key` from ship-to when the DB column is null.
2. **Project → Sage join** — deal keys, then **ship-to address** (linked property address, `external_ids` ship hints), then name.
3. **Sage → property** — `property_key` exact → **ship_to address exact** → computed key → warehouse tokens → deal → fuzzy name (city/state scoped).

Sage header sync (`Sage-iQ/scripts/sage_oe_map.py`) should populate `property_key` inline on every write so downstream jobs do not depend on a separate enrichment pass.

## Pipeline

```
[optional] Firecrawl web resolve on weak property_registry.address_line1
    → sage_orders (ship_to)
    → join project_registry (deal keys, external_ids, name vs ship_name)
    → match property_registry (tiers below)
    → upsert project_location_candidates
    → optional --promote writes project_registry.property_id
```

Web resolve: `scripts/lib/site-address-resolve.mjs` + `--resolve-web` on the sync script. See `docs/SITE_ADDRESS_WEB_RESOLVE.md`.

### Project ↔ Sage join

1. Deal / order key overlap (`project_id`, `order_number`, `external_ids.deal_number`, Sage `reference` DEAL patterns)
2. Fallback: fuzzy `project_name` vs `ship_name` (brand/division filter, score ≥ 0.72)

### Property match tiers (highest wins)

| Method | Confidence | Signal |
|--------|------------|--------|
| `sage_property_key_exact` | 100 | Sage `property_key` = computed `<line1> · <city> <state>` |
| `ship_to_address_exact` | 98 | Normalized line1+city+state+zip |
| `computed_property_key` | 97 | Same key formula from ship_to lines |
| `warehouse_property_key_tokens` | 92 | Warehouse-routed: tokens from `property_key` vs `property_name` |
| `deal_number_external_ids` | 90 | Order number in property `external_ids` |
| `ship_name_city_state` | ~64–78 | Fuzzy name in city/state pool |
| `ship_name_city_state_best` | ~80 | Best of multiple name hits with score gap ≥ 0.15 |

Warehouse blocklist (ship_to line1): Fort Worth 101 Academy, Arvada 6501 Lowell, Rowlett 9305 Merritt — property identity is in `ship_name` / `property_key`, not the warehouse address.

## Commands

```bash
cd "/Users/geoffreyjackson/Dropbox/The Living Company/TLC iQ/Property_Registry"

# Preview matches (no writes)
node scripts/sync-sage-shipto-project-property.mjs --dry-run

# Write candidates only
node scripts/sync-sage-shipto-project-property.mjs --apply

# Auto-link high-confidence matches (property_key / address exact)
node scripts/sync-sage-shipto-project-property.mjs --apply --promote --min-confidence=95

# Single project
node scripts/sync-sage-shipto-project-property.mjs --apply --project-id=26-027-I
```

### Env

- `DALE_DEMAND_SUPABASE_URL`, `DALE_DEMAND_SUPABASE_KEY` (or `_SERVICE_ROLE_KEY`)
- `REGISTRY_IQ_SUPABASE_URL`, `REGISTRY_IQ_SUPABASE_SERVICE_ROLE_KEY`

Loaded from repo `.env.local` or `Derived State/dale-chat/.env.local`.

## Review queue

Candidates land in `project_location_candidates`. Reviewers use `v_project_property_resolution_queue` (dale-chat migrations `0033`–`0035`).

| `resolution_status` | Meaning |
|---------------------|---------|
| `proposed` | Single property match, not yet promoted |
| `needs_review` | Ambiguous (multiple properties) |
| `needs_external_research` | No property match |
| `accepted` | Promoted to `project_registry.property_id` |

## Apply run (2026-05-29)

| Metric | Count |
|--------|------:|
| Unlinked projects scanned | 1,447 |
| Projects with Sage ship-to signal | 727 |
| Candidates upserted | 727 |
| Property match (proposed) | 129 |
| Ambiguous | 438 |
| No property match | 160 |
| Auto-promoted (confidence ≥ 95) | 55 |
| Projects linked (total) | 1,407 / 2,799 |

Promoted matches were all `sage_property_key_exact` (100 confidence).

## Implementation notes

- Upsert uses select-then-update/insert because PostgREST `ON CONFLICT` does not target the partial unique index on `(project_id, source_system, source_table, source_record_id)`.
- Scripts: `scripts/sync-sage-shipto-project-property.mjs`, `scripts/lib/sage-shipto-match.mjs`.
