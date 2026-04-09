# Property Registry (workspace)

Scripts, notes, and **session context** for TLC iQ **Registry-iQ** property data (Supabase `property_registry` and related tables) and **Airtable** Property Registry workflows. The **Property Registry UI** lives in the **Derived-State** / **dale-chat** app (`/property-registry` on tlciq-platform).

## Repository

| | |
|---|---|
| **Git (canonical)** | https://github.com/GJ1MPR1NT/Property-Registry |
| **Platform app** | [Derived-State](https://github.com/GJ1MPR1NT/Derived-State) → `dale-chat` |
| **Hub doc index** | [PROJECT_CONTEXT_Derived_State.md](https://github.com/GJ1MPR1NT/Derived-State/blob/main/PROJECT_CONTEXT_Derived_State.md) |

Use this repo for **versioned** `PROJECT_CONTEXT_Property_Registry.md`, SQL/migration notes, and Node scripts. A local copy may still live under Dropbox for day-to-day work; **commit and push** here when you want a durable snapshot others (and agents) can clone.

## Contents

- **`PROJECT_CONTEXT_Property_Registry.md`** — running session history and integration notes for this domain.
- **`scripts/`** — one-off maintenance (Airtable sync, enrichment, dedup, etc.). Read each script’s header for env vars and flags.
- **`package.json`** — minimal Node deps for scripts (`@supabase/supabase-js`, `pg`).

## Setup

```bash
cp .env.example .env.local
# Edit .env.local with Registry-iQ credentials (never commit).

npm install
```

Run individual scripts with `node scripts/<name>.mjs` (see script comments).

## Related docs

- Ecosystem pointer table: **Derived-State** `PROJECT_CONTEXT_Derived_State.md` (*Cross-module linkages*).
- Dale-chat app context: **Derived-State** `dale-chat/PROJECT_CONTEXT_dale_chat.md`.
