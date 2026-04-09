# Rosetta-iQ: Acquisition Data Onboarding Playbook

## Purpose

This document describes the step-by-step process for onboarding an acquired company's data into the TLC iQ platform using Rosetta-iQ, the cross-system entity resolution service. Rosetta acts as an **airlock** — the acquired company's data enters the ecosystem through Rosetta without touching any existing module (Registry-iQ, Chain-iQ, DALE, etc.) until it has been profiled, matched, and verified.

---

## Current State (as of March 2026)

Rosetta-iQ is live with:

| Metric | Count |
|--------|-------|
| Source systems registered | 8 (7 Supabase + D365 CRM) |
| Total source identifiers | 36,260 |
| Canonical entities | 30,000 |
| Crosswalk links | 36,253 |
| Projects matched across 2+ systems | 2,743 |
| Entity types tracked | 6 (project, PO, SKU, factory, property, stakeholder) |

The existing TLC iQ data is already connected. The process below describes how to bring a new company's data into the same graph.

---

## Phase 0: Pre-Acquisition Data Assessment

**Timeline**: Before or immediately after LOI signing.
**Owner**: Data / Integration team.

### Objectives
- Understand what systems the target company uses
- Identify their key entity types and identifier formats
- Assess data quality and volume
- Determine overlap with TLC's existing entity graph

### Deliverables

1. **System inventory** — List every data system (ERP, CRM, spreadsheets, cloud apps) with:
   - System name and type
   - Data volumes (row counts by table)
   - Primary key formats (e.g., "PO numbers are 7-digit numeric", "project codes are YYYY-NNN")
   - Export capabilities (API, CSV, database access)

2. **Entity type mapping** — For each of the 6 Rosetta entity types, identify:
   - Where that entity lives in their systems
   - What they call it (their terminology)
   - Their identifier format vs. TLC's format
   - Known overlaps (shared customers, shared properties, shared factories)

3. **Sample data extract** — Request sample exports (50–100 records per entity type) to:
   - Validate identifier patterns
   - Test normalization rules
   - Estimate match rates before committing to full ingestion

### Decision Gate

After Phase 0, the team should know:
- How many of their entities likely overlap with TLC's existing graph
- Which entity types have clean keys vs. require fuzzy matching
- Whether their data needs cleanup before ingestion (null keys, duplicates, encoding issues)

---

## Phase 1: Register Source Systems

**Timeline**: Day 1 of data integration.
**Owner**: Integration engineer.

### Actions

For each of the target company's data systems, insert a row into `rosetta_source_systems`:

```sql
INSERT INTO rosetta_source_systems (system_name, system_type, identifier_patterns, status)
VALUES (
  'acq_company_erp',
  'netsuite',                  -- or 'sage', 'dynamics', 'spreadsheet', etc.
  '{
    "project_id": "YYYY-NNN format",
    "po_number": "PO-NNNNNN format",
    "factory_code": "3-letter code"
  }'::jsonb,
  'active'
);
```

If their data will be loaded into a dedicated Supabase project, also set `supabase_project_ref`.

### Naming Convention

Use the prefix `acq_` followed by an abbreviated company name:
- `acq_acme_erp` — their ERP system
- `acq_acme_crm` — their CRM system
- `acq_acme_spreadsheets` — any manual/Excel data

This keeps acquired systems visually distinct from TLC's native systems in any query or report.

---

## Phase 2: Profile Identifier Formats

**Timeline**: Days 1–3.
**Owner**: Integration engineer + Rosetta normalization scripts.

### Actions

1. **Analyze sample data** to confirm identifier patterns:
   - Deal/project number format
   - PO number format
   - Factory codes
   - Customer/property naming conventions

2. **Extend normalization rules** in `scripts/rosetta/normalize.mjs` if needed. For example, if they use a different deal number format:

```javascript
// Example: Acquired company uses "ACQ-2026-0042" format
export function normalizeAcqDealNumber(raw) {
  if (!raw || typeof raw !== "string") return null;
  let s = raw.trim().toUpperCase();
  // Strip their prefix
  s = s.replace(/^ACQ-/, "");
  // Convert to TLC's YY-NNNN format: "2026-0042" → "26-42"
  const m = s.match(/^(\d{4})-0*(\d+)$/);
  if (m) return m[1].slice(2) + "-" + m[2];
  return s;
}
```

3. **Update entity types** in `rosetta_entity_types` if the acquired company introduces new normalization rules.

### Output

A confirmed set of normalization functions that can transform the acquired company's identifiers into candidate keys comparable to TLC's existing canonical keys.

---

## Phase 3: Bulk Ingest Identifiers

**Timeline**: Days 3–5.
**Owner**: Integration engineer.

### Actions

1. **Write an ingestion function** in `scripts/rosetta/ingest-all.mjs` (or a separate `ingest-acq.mjs` file) for each of their source systems. Pattern follows the existing ingestion functions:

```javascript
async function ingestAcqErp(systemId) {
  console.log("\n--- Acquired Company ERP ---");
  
  // Connect to their data (Supabase, CSV import, API, etc.)
  const projects = await fetchFromAcqSystem("projects");
  
  const rows = [];
  for (const p of projects) {
    rows.push({
      source_system_id: systemId,
      entity_type: "project",
      source_key: p.project_number,      // their raw key
      source_name: p.project_name,        // their display name
      source_table: "acq_projects",
      raw_metadata: {                     // preserve everything
        their_status: p.status,
        their_region: p.region,
        their_client: p.client_name,
      },
    });
  }
  
  await upsertIdentifiers(rows);
}
```

2. **Run the ingestion**:

```bash
node --env-file=dashboard/.env.local scripts/rosetta/ingest-acq.mjs
```

3. **Verify counts**:

```sql
SELECT entity_type, count(*)
FROM rosetta_source_identifiers
WHERE source_system_id = '<acq_system_uuid>'
GROUP BY entity_type;
```

### Data Sources

The acquired company's data can arrive in any form:
- **Direct database access** → Query their tables with a Supabase client
- **CSV/Excel exports** → Parse with `xlsx` or `csv-parse`, then insert
- **API access** → Fetch from their REST/GraphQL endpoints
- **Manual entry** → For small datasets, use the Supabase dashboard

The ingestion script normalizes all formats into the same `rosetta_source_identifiers` structure.

---

## Phase 4: Run Matching Engine

**Timeline**: Day 5.
**Owner**: Integration engineer.

### Actions

Run the batch matching engine against all systems (including the newly ingested ones):

```bash
node --env-file=dashboard/.env.local scripts/rosetta/match-batch.mjs
```

Or run only for the new system's entity types:

```bash
node --env-file=dashboard/.env.local scripts/rosetta/match-batch.mjs --entity-type=project
```

### What Happens

The matching engine uses a three-tier approach:

**Tier 1: Exact Key Match** (confidence: 1.0)
- Normalizes all identifiers (strip suffixes, convert year formats, uppercase)
- Uses union-find clustering to group identifiers sharing any normalized key variant
- Example: Their `ACQ-2026-0042` normalizes to `26-42`, which matches TLC's `26-42` in DALE-Demand

**Tier 2: Token Overlap / Fuzzy Name Match** (confidence: 0.70–0.95)
- For identifiers that don't match on key but have similar names
- Jaccard similarity on tokenized names (e.g., "Courtyard Thousand Oaks Phase 2" vs. "Thousand Oaks Courtyard — Ph. II")
- Substring containment scoring
- Threshold: 0.70 for auto-match, below that goes to review queue

**Tier 3: LLM-Assisted Match** (planned, not yet implemented)
- For genuinely ambiguous cases where names and keys don't overlap
- Send unmatched pairs to Claude with domain context
- All LLM suggestions require human verification before being confirmed

### Output

A `match_runs` record with statistics:

```sql
SELECT run_type, entity_type, total_processed, exact_matches, fuzzy_matches, unmatched
FROM rosetta_match_runs
WHERE source_system_id = '<acq_system_uuid>'
ORDER BY started_at DESC
LIMIT 1;
```

---

## Phase 5: Review and Verify

**Timeline**: Days 5–10.
**Owner**: Data team + business stakeholders.

### Actions

1. **Review high-confidence matches** (≥0.95) — these are auto-verified:

```sql
SELECT ce.canonical_key, ce.canonical_name,
       si.source_key, si.source_name, ss.system_name,
       cw.match_confidence, cw.match_method
FROM rosetta_crosswalk cw
JOIN rosetta_canonical_entities ce ON cw.canonical_entity_id = ce.id
JOIN rosetta_source_identifiers si ON cw.source_identifier_id = si.id
JOIN rosetta_source_systems ss ON si.source_system_id = ss.id
WHERE ss.system_name LIKE 'acq_%'
  AND cw.match_confidence >= 0.95
ORDER BY ce.canonical_key;
```

2. **Review low-confidence matches** (0.50–0.94) — need human confirmation:

```sql
SELECT ce.canonical_key, ce.canonical_name,
       si.source_key, si.source_name,
       cw.match_confidence, cw.match_method
FROM rosetta_crosswalk cw
JOIN rosetta_canonical_entities ce ON cw.canonical_entity_id = ce.id
JOIN rosetta_source_identifiers si ON cw.source_identifier_id = si.id
JOIN rosetta_source_systems ss ON si.source_system_id = ss.id
WHERE ss.system_name LIKE 'acq_%'
  AND cw.match_confidence < 0.95
ORDER BY cw.match_confidence DESC;
```

For each, a human confirms or rejects. Confirmed matches get:

```sql
UPDATE rosetta_crosswalk
SET verified_by = 'analyst@tlciq.app', verified_at = now()
WHERE id = '<crosswalk_uuid>';
```

3. **Review unmatched identifiers** — entities that exist only in the acquired company's world:

```sql
SELECT si.source_key, si.source_name, si.entity_type
FROM rosetta_source_identifiers si
LEFT JOIN rosetta_crosswalk cw ON cw.source_identifier_id = si.id
JOIN rosetta_source_systems ss ON si.source_system_id = ss.id
WHERE ss.system_name LIKE 'acq_%'
  AND cw.id IS NULL;
```

These either:
- Need manual matching (create crosswalk entry with `match_method = 'manual'`)
- Are genuinely new entities (create new canonical entities for them)
- Are junk data that should be ignored

---

## Phase 6: Publish to Ecosystem

**Timeline**: Day 10+.
**Owner**: Integration engineer.

### Actions

Once matches are verified, the acquired company's data is automatically available everywhere Rosetta is used:

1. **Vantage Viewport** — Projects with cross-system matches now show the acquired company's data in the `rosetta_sources[]` enrichment:
   - No code changes needed. The `/api/vantage/viewport` route already queries Rosetta for any project.
   - Their POs, install schedules, and factory data appear alongside TLC's.

2. **Resolution API** — Any module can resolve the acquired company's identifiers:

```
GET /api/rosetta/resolve?key=ACQ-2026-0042&type=project&system=acq_acme_erp
```

3. **Registry-iQ Backfill** — Run the backfill script to populate missing milestones, stakeholders, and delivery years from the acquired company's cross-referenced data:

```bash
node --env-file=dashboard/.env.local scripts/rosetta/backfill-registry.mjs
```

4. **Customer Vantage Point** — If the acquired company serves shared customers (e.g., Core Spaces), their data automatically enriches the customer-level aggregations through Rosetta's crosswalk.

### No Module Changes Required

This is the key architectural benefit: **no existing module needs to be modified**. Rosetta absorbs the new data, resolves it to the existing entity graph, and the resolution API surfaces it wherever it's needed. The acquired company's systems are just another set of rows in `rosetta_source_systems` and `rosetta_source_identifiers`.

---

## Phase 7: Ongoing Sync

**Timeline**: Ongoing post-integration.
**Owner**: Platform team.

### Options

1. **Scheduled re-ingestion** — An n8n workflow runs `ingest-acq.mjs` on a schedule (daily or weekly) to pick up new records from the acquired company's systems. Followed by an incremental match run.

2. **Event-driven ingestion** — If their systems can emit webhooks or change data capture events, Rosetta can ingest new identifiers in real-time via an API endpoint.

3. **Manual batch runs** — For companies with static data (historical records, not a live system), a one-time ingestion is sufficient.

### Data Quality Monitoring

Track ongoing match health:

```sql
-- Match rate by system
SELECT ss.system_name,
       count(DISTINCT si.id) as total_identifiers,
       count(DISTINCT cw.id) as matched,
       round(100.0 * count(DISTINCT cw.id) / count(DISTINCT si.id), 1) as match_pct
FROM rosetta_source_identifiers si
JOIN rosetta_source_systems ss ON si.source_system_id = ss.id
LEFT JOIN rosetta_crosswalk cw ON cw.source_identifier_id = si.id
GROUP BY ss.system_name
ORDER BY match_pct;
```

---

## Estimated Timeline

| Phase | Duration | Dependencies |
|-------|----------|-------------|
| 0. Data assessment | 1–2 weeks | Access to target company's systems |
| 1. Register systems | 1 day | Phase 0 complete |
| 2. Profile identifiers | 2–3 days | Sample data available |
| 3. Bulk ingest | 1–2 days | Data export or API access |
| 4. Run matching | 1 day | Phases 1–3 complete |
| 5. Review & verify | 3–5 days | Business stakeholder availability |
| 6. Publish | 1 day | Phase 5 complete |
| 7. Ongoing sync | Continuous | Phase 6 complete |

**Total: 2–4 weeks** from data access to full integration, depending on data quality and review speed.

---

## Risk Mitigation

| Risk | Mitigation |
|------|-----------|
| **Low match rates** | Tier 2 fuzzy matching catches name variations. Tier 3 LLM-assisted matching (v2) handles the hardest cases. Manual matching is always available. |
| **Identifier format conflicts** | Each system's patterns are stored in `identifier_patterns`. Normalization functions are composable and testable. |
| **Data quality issues** | The `raw_metadata` JSONB field preserves everything from the source. Nothing is lost during normalization. Original keys are always recoverable. |
| **Overlapping entities** | The crosswalk's confidence scores and verification workflow prevent false merges. Low-confidence matches require human review. |
| **Scale concerns** | The current 36,260 identifiers process in under 60 seconds. Rosetta can handle 10x this volume with the existing batch architecture. Beyond that, incremental matching (process only new records) keeps run times constant. |
| **Rollback** | Deleting all rows from `rosetta_source_identifiers` where `source_system_id = '<acq_system_uuid>'` and cascading through `rosetta_crosswalk` cleanly removes an acquired company's data without affecting any other system. |

---

## What Rosetta Is Not

- **Not a data warehouse** — Rosetta stores identity mappings, not the actual data. PO details stay in SupplyiQ, containers stay in Chain-iQ, installs stay in DALE-Demand. Rosetta tells you *which* PO in SupplyiQ maps to *which* project in Registry-iQ.
- **Not an autonomous agent** — Rosetta doesn't make decisions. It resolves identities on request. Agents (RITA, DALE) *call* Rosetta; Rosetta doesn't call agents.
- **Not a replacement for source systems** — The acquired company's ERP/CRM continues to run. Rosetta reads from it; it never writes to it.
