# TLC iQ Platform — Property Registry Master Backlog

**Last reviewed:** Jun 27, 2026

## Priority Legend
- **NOW** — User wants this done in the current or next session
- **NEXT** — High priority, tackle when current NOW items are clear
- **LATER** — Deferred; revisit when user indicates
- **BLOCKED** — Waiting on external input, access, or a dependency
- **DONE** — Completed (keep for 2 weeks as audit trail, then archive)
- **WONT** — User explicitly declined this item

## Items
| ID | Status | Category | Description | Added | Updated |
|----|--------|----------|-------------|-------|---------|
| MH-01 | DONE | data-gap | Morgan Hill: backfilled `standard_bedrooms` for all 86 types — `A*`→1, `B*`→2 (user confirmed Jun 19). `total_sqft` still NULL. Structural bed sum = 479 (br×units). | Jun 19 | Jun 19 |
| MH-02 | DONE | data-gap | Morgan Hill: `beds_per_unit` aligned to `bedrooms_structural` when no specialty fields (26 B-types fixed). Total beds = 479. Beds column UI only for divider/shared/pod/murphy/super-murphy. | Jun 19 | Jun 19 |
| MH-09 | WONT | enrichment | `total_sqft` backfill deferred indefinitely (user Jun 20). | Jun 19 | Jun 20 |
| MH-03 | DONE | assets | Unit-type finish layouts from `UNIT PLANS_5.2025.pdf` — 86/86 types mapped + Cloudinary; sheet thumbnail per page (multi-unit sheets). | Jun 19 | Jun 20 |
| MH-04 | LATER | assets | Cabinet configs `MW04.5/MW05/MW06` (+ `MW01.5/MW01.6` for A1.2A) referenced by Matrix have no PDF in Box KM PDFs. | Jun 19 | Jun 20 |
| MH-05 | DONE | ontology | Matrix kitchen/vanity → `property_unit_types.room_drawings` + Kitchen/Bath 1/Bath 2 columns on Unit Type Matrix. | Jun 19 | Jun 20 |
| MH-06 | LATER | ontology | Finish→Furnish taxonomy split (`property_furnish_unit_types` + finish→furnish FK) + the mapping (no furnish column in Matrix). | Jun 19 | Jun 19 |
| MH-07 | LATER | ontology | Materialize rooms as LOCATION rows (kitchen/living/bath_x/bed_x…) per the broader ontology vision. | Jun 19 | Jun 19 |
| MH-08 | LATER | rollups | Custom PM rollups for construction zones/sequence areas (Area/Truck/Phase tags already on units; Area 66 blank). | Jun 19 | Jun 19 |
| UI-01 | LATER | ui | Optionally relocate the 3 transient RITA run-status banners (no-results, staged-run, findings review) to the bottom too (currently kept near the RITA action button by design). | Jun 19 | Jun 19 |
| MH-10 | DONE | campaign | Morgan Hill `MH-REGISTRY-ENRICH-001`: deployed + reseeded (7 items, simplified if/then survey). | Jun 20 | Jun 21 |
| MH-13 | DONE | campaign | Admin **Campaign monitor** on property page + SYNC-iQ `/sync/registry-enrich`: sent/opened/submitted/files, active link copy, event log. APIs: property status route + Registry-iQ read in Enrich-iQ. | Jun 20 | Jun 20 |
| MH-11 | NEXT | campaign | Image annotation on enrich review page (react-konva) + full Matrix unit-facts ingest (not hardcoded MH sample). | Jun 20 | Jun 20 |
| CS-01 | DONE | ingest | Core Spaces Prismic ingest: 89 communities → Registry-iQ (`ingest-corespaces-prismic.mjs`). | May 20 | May 20 |
| CS-02 | DONE | enrichment | Sold Core Spaces portfolio (10): press-release research → current owner + PM stakeholders. 9/10 assigned; Minneapolis unresolved. | May 20 | Jun 27 |
| CS-03 | DONE | dedupe | Merge legacy Bloomington Hub rows (`HUB Bloomington`, `Hub Bloomington Lincoln (II)`) into canonical Prismic records. | May 20 | Jun 27 |
| CS-04 | DONE | assets | Cloudinary backfill for Core Spaces Prismic hero/gallery images (91 heroes). | May 20 | Jun 27 |
| CS-05 | DONE | pipeline | Align coming-soon Prismic communities to project_registry deals (14 linked; 7 no open deal). | May 20 | Jun 27 |
| CS-06 | NEXT | enrichment | Hub Minneapolis sold-portfolio: identify current owner/PM (press research inconclusive). | Jun 27 | Jun 27 |
| CS-07 | LATER | pipeline | Coming-soon with no pipeline deal: William, Ann Arbor State, Madison Bassett, Oxenfree Liberty Hill/Parklin/Stonebriar/Rowlett — create deals or wait for CRM. | Jun 27 | Jun 27 |

## Done (recent)
| ID | Status | Category | Description | Added | Updated |
|----|--------|----------|-------------|-------|---------|
| MH-D1 | DONE | assets | Ingest 23 shop drawings + 11 floor plans to Cloudinary + registry (no surrogates; gaps recorded). | Jun 19 | Jun 19 |
| MH-D2 | DONE | ui | "Unit Type Matrix" redesign (labels, Total Baths, clickable detail modal, dropped legacy Layout column). | Jun 19 | Jun 19 |
| MH-D3 | DONE | ui | New "Shop Drawings" tab (thumbnail grid, type/state filters, editable state, preview+download). | Jun 19 | Jun 19 |
| MH-D4 | DONE | ui | Floor-plan thumbnails on Floors + Buildings tabs (preview modal + Download PDF). | Jun 19 | Jun 19 |
| MH-D5 | DONE | fix | Decouple FF&E SKU estimate from floor rollups (relabel + fix help text). | Jun 19 | Jun 19 |
| MH-D6 | DONE | fix | Opening-year timezone fix (header shows 2027) + DB `opening_year` realigned 2026→2027. | Jun 19 | Jun 19 |
| MH-D7 | DONE | ui | Move persistent RITA surfaces (Proposals + Reads) to bottom of property page. | Jun 19 | Jun 19 |
