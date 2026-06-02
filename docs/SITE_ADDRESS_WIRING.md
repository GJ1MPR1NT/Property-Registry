# site_address wiring (Registry → Chain-iQ)

## What it is

`site_address` is the **canonical install-site street address** for a project — distinct from Chain-iQ `destination`, which is logistics routing (city/ZIP, staging warehouse, etc.).

| Column | Table | Home |
|--------|-------|------|
| `site_address` | `project_registry` | Registry-iQ |
| `site_address` | `container_loads` | Chain-iQ |

**Source of truth:** `property_registry.address_line1` (+ city, state, ZIP) on the project’s linked property.

## Where site address does *not* live (today)

| System | Field | Hub II Bloomington example |
|--------|-------|----------------------------|
| **Sage sales order** | `ship_to_address_line_1` | `2pr00088 Core Bloomington Linclon LLC` (Core Spaces LLC name), **not** `208 East 19th` |
| **NetSuite job 10201** | REST job fields | Schedule/ROSD populated; **no site street** in standard job REST (`custentityoff_site` blank) |
| **Chain-iQ destination** | `destination` | `Bloomington IN 47408` or `Brighton, TN 38011` (routing) |

Registry enrichment (manual / install schedules / property research) holds the real site line.

## Migrations

```bash
cd "/Users/geoffreyjackson/Dropbox/The Living Company/TLC iQ/Property_Registry"
# Already applied on live Supabase 2026-05-28; re-run only on fresh envs:
# scripts/migration-container-site-address.sql      → Chain-iQ
# scripts/migration-project-registry-site-address.sql → Registry-iQ
```

## Sync

```bash
cd "/Users/geoffreyjackson/Dropbox/The Living Company/TLC iQ/Property_Registry"
node scripts/sync-site-address-to-chain.mjs --dry-run
node scripts/sync-site-address-to-chain.mjs --apply
node scripts/sync-site-address-to-chain.mjs --apply --project "Bloomington"
```

Re-run after Registry property address edits or new project↔property links.

## Web resolve (missing street lines)

When `address_line1` is null/TBD/LLC-only, run Firecrawl first — see **`docs/SITE_ADDRESS_WEB_RESOLVE.md`**.

```bash
node scripts/resolve-site-address-firecrawl.mjs --apply --limit=25
node scripts/sync-site-address-to-chain.mjs --apply --resolve-web-first
```
