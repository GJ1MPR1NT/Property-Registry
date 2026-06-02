#!/usr/bin/env bash
# Comprehensive Sage OE header backfill → property_key → project→property promote
#
# Pulls ALL orders from Sage 300 OE since 2019-01-01 with full ship_to_* +
# property_key into DALE-Demand sage_orders snapshot 2026-05-28, then
# backfills property_key gaps and re-runs Registry-iQ project linking.
#
# Resumable: re-run with --resume to continue an interrupted Sage pull.
#
# Usage:
#   cd "/Users/geoffreyjackson/Dropbox/The Living Company/TLC iQ/Property_Registry"
#   bash scripts/run-comprehensive-sage-shipto-backfill.sh
#   bash scripts/run-comprehensive-sage-shipto-backfill.sh --resume
#   bash scripts/run-comprehensive-sage-shipto-backfill.sh --sage-only

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SAGE_ROOT="$(cd "$ROOT/../Sage-iQ" && pwd)"
ENV_FILE="$ROOT/.env.local"
LOG_DIR="$ROOT/logs"
LOG_FILE="$LOG_DIR/sage-comprehensive-backfill-$(date +%Y%m%d-%H%M%S).log"
SNAPSHOT_DATE="2026-05-28"
SINCE="2019-01-01T00:00:00Z"
STATE_DIR="data/sage_header_sync/comprehensive"

RESUME=""
SAGE_ONLY=""
for arg in "$@"; do
  case "$arg" in
    --resume) RESUME="--resume" ;;
    --sage-only) SAGE_ONLY=1 ;;
  esac
done

mkdir -p "$LOG_DIR"

exec > >(tee -a "$LOG_FILE") 2>&1

echo "=== Comprehensive Sage ship-to backfill ==="
echo "Started: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
echo "Log: $LOG_FILE"
echo "Since: $SINCE | Snapshot: $SNAPSHOT_DATE"
echo ""

# Python + Node scripts load .env.local themselves (SAGE_ENV_FILE / dotenv).
# Do NOT `source` .env.local here — invalid shell identifiers (e.g. PDF.CO_API_KEY) abort the run.
export SAGE_ENV_FILE="$ENV_FILE"

echo "--- Phase 1: Sage OE headers (ship_to + property_key) ---"
cd "$SAGE_ROOT"
python3 scripts/sync_sage_orders_headers_from_sage.py \
  --comprehensive \
  --snapshot-date "$SNAPSHOT_DATE" \
  --state-dir "$STATE_DIR" \
  $RESUME

echo ""
echo "--- Phase 2: Backfill property_key on any remaining ship_to rows ---"
cd "$ROOT"
node scripts/backfill-sage-shipto-property-key.mjs --apply

if [[ -n "$SAGE_ONLY" ]]; then
  echo "Sage-only mode — skipping Registry promote."
  exit 0
fi

echo ""
echo "--- Phase 2b: Firecrawl site address (weak property street lines) ---"
node scripts/resolve-site-address-firecrawl.mjs --apply --limit=50

echo ""
echo "--- Phase 3: Registry project→property promote (confidence >= 95) ---"
node scripts/sync-sage-shipto-project-property.mjs --apply --promote --min-confidence=95 --resolve-web --resolve-web-limit=50

echo ""
echo "=== Complete: $(date -u +"%Y-%m-%dT%H:%M:%SZ") ==="
