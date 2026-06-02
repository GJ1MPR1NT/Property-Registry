# Container destination guard

**Policy:** Any inbound container routed to an **unknown warehouse or property** must be **stopped** (held) until the destination is validated or the allowlist is updated.

## What counts as "known"

A destination is allowed when it matches any of:

1. **Canonical TLC warehouses** — BSI Brighton TN staging, UF warehouse blocklist sites (see `scripts/lib/container-destination-guard.mjs`)
2. **`warehouse_registry`** rows in Registry-iQ (when migration applied)
3. **Linked property address** for the container's resolved `project_registry` row (city/state/zip/line1)

## What gets held

Chain-iQ `container_loads` rows where:

- `status` is inbound/active (`SHIPPED`, `LOADED`, `ARRIVED`, `AVAILABLE`, `BOOKED`, …)
- `destination` or `final_destination` is populated
- Destination does **not** match the allowlist above
- Not already `status2 = HOLD:UNKNOWN_DEST`

**Not held:** `DELIVERED`, empty destination, terminal/cancelled rows.

## Hold mechanics

Updates Chain-iQ only:

| Field | Value |
|-------|--------|
| `status2` | `HOLD:UNKNOWN_DEST` |
| `note` | Prepended `[DEST HOLD …]` audit line with reason |

Ops should treat `HOLD:UNKNOWN_DEST` as **do not release / do not schedule final delivery** until cleared.

## Commands

```bash
cd "/Users/geoffreyjackson/Dropbox/The Living Company/TLC iQ/Property_Registry"

# Audit (default)
node scripts/enforce-container-destination-guard.mjs --dry-run

# Scope to a project name fragment
node scripts/enforce-container-destination-guard.mjs --dry-run --project "Bloomington"

# Apply holds
node scripts/enforce-container-destination-guard.mjs --apply
```

## Clearing a hold

After confirming the destination (register warehouse in `warehouse_registry`, fix project→property link, or add canonical warehouse):

1. Correct `destination` in Chain-iQ if wrong
2. Clear `status2` (set to null or prior value)
3. Append resolution note

## Follow-ups

- Wire into Chain-iQ `flexport_sync.py` post-enrichment hook (block promotion while held)
- Surface holds on Chain-iQ dashboard + Registry property Shipping tab
- Apply `scripts/migration-warehouse-and-field-ops-registry.sql` on Registry-iQ so install-schedule warehouses join the allowlist automatically
