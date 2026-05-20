# Data Architecture

## Overview

Canton ledger data flows through a three-stage pipeline:

1. **Ingestion** — Live polling from Canton Scan API → Parquet files on GCS
2. **Transformation** — Raw Parquet → typed BigQuery tables (timestamps, arrays, JSON)
3. **Analytical layers** — Bronze views + silver materialized tables in BigQuery

A local DuckDB API server also queries the same Parquet files directly for the frontend.

---

## Data Authority Contract

> **Parquet files on GCS are the sole authoritative data source.**
>
> All governance, rewards, party state, and analytics **must** be derived from
> Parquet files — either via DuckDB (local API) or BigQuery (analytics).
> Legacy binary formats (JSONL, PBZST) are **deprecated** and must not be
> read by API routes or business logic.

### Enforcement

| Directory | Allowed Operations |
|-----------|-------------------|
| `server/api/` | DuckDB queries over Parquet **only** |
| `server/engine/` | DuckDB analytical queries **only** |
| `scripts/ingest/` | Write-only (produces Parquet → GCS) |
| `scripts/bigquery/` | BigQuery DDL and transforms |

CI enforces this via `data-authority-check` in `.github/workflows/test.yml`.

---

## Data Flow

```
Canton Scan API
      │
      ▼
┌─────────────────────────────────┐
│  fetch-updates.js (systemd)     │  Live polling, BATCH_SIZE=1000
│  canton-live-ingest.service     │  Auto-restart, cursor persistence
└─────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────────────────────────────────────┐
│  GCS: gs://BUCKET/raw/updates/                                      │
│                                                                      │
│  ├── events/migration=M/year=Y/month=M/day=D/*.parquet              │
│  ├── updates/migration=M/year=Y/month=M/day=D/*.parquet             │
│  └── ../cursors/live-cursor.json                                     │
│                                                                      │
│  Hive-partitioned, ZSTD-compressed Parquet                           │
│  697 days ingested (2024-06-24 → 2026-05-17), 248M+ records         │
│  5 migrations (M0–M4)                                                │
└─────────────────────────────────────────────────────────────────────┘
      │                                    │
      ▼                                    ▼
┌──────────────────┐         ┌──────────────────────────────────────┐
│  DuckDB Server   │         │  BigQuery Pipeline                   │
│  (local API)     │         │                                      │
│  Port 3001       │         │  raw.events / raw.updates            │
│  Read-only       │         │       ↓ 02-transform-raw-data.sql   │
└──────────────────┘         │  transformed.events_parsed           │
      │                      │  transformed.updates_parsed          │
      ▼                      │       ↓ bronze views (03–08)        │
┌──────────────────┐         │  transformed.parsed_*                │
│  React Frontend  │         │       ↓ silver tables (01–06)       │
└──────────────────┘         │  canton_silver.*                     │
                             └──────────────────────────────────────┘
```

---

## GCS Layout

```
gs://BUCKET/
  raw/updates/
    events/migration={0-4}/year=Y/month=M/day=D/*.parquet
    updates/migration={0-4}/year=Y/month=M/day=D/*.parquet
  cursors/
    live-cursor.json
```

- `raw/backfill/` was deleted after full archive remediation
- Old backfill cursor files (cursor-0 through cursor-4) are pending deletion
- All data now lives under `raw/updates/` regardless of whether it was
  backfilled or live-ingested

### Parquet Schema

Defined in `scripts/ingest/data-schema.js`:

**Events** (`LEDGER_EVENTS_SCHEMA` / `EVENTS_COLUMNS`):
- Identifiers: `event_id`, `update_id`, `contract_id`, `template_id`, `package_name`
- Types: `event_type`, `event_type_original`, `choice`, `consuming`
- Timestamps: `effective_at`, `recorded_at`, `timestamp`, `created_at_ts`
- Parties: `signatories`, `observers`, `acting_parties`, `witness_parties` (arrays)
- Payloads: `payload`, `contract_key`, `exercise_result`, `raw_event` (JSON strings)
- Reassignment: `source_synchronizer`, `target_synchronizer`, `unassign_id`, `submitter`
- Partitioning: `migration_id`, plus Hive columns `year`, `month`, `day`

**Updates** (`LEDGER_UPDATES_SCHEMA` / `UPDATES_COLUMNS`):
- Identifiers: `update_id`, `update_type`, `workflow_id`, `command_id`
- Timestamps: `record_time`, `effective_at`, `recorded_at`, `timestamp`
- Metadata: `offset`, `event_count`, `root_event_ids`, `kind`
- Payloads: `trace_context`, `update_data` (JSON strings)

### Parquet Type Notes

Raw Parquet files store types that BigQuery cannot auto-resolve:
- Timestamps → ISO 8601 strings (not TIMESTAMP)
- Arrays → Parquet LIST structs with a `.list` field
- JSON → plain strings

The BigQuery transform step (`02-transform-raw-data.sql`) handles these conversions.
DuckDB reads them natively without issues.

---

## Live Ingestion

**Service**: `canton-live-ingest.service` (systemd on governance-dashboard)

| Setting | Value |
|---------|-------|
| Script | `scripts/ingest/fetch-updates.js` |
| Batch size | 1000 |
| Auto-restart | Yes (on failure + on boot) |
| Shutdown | SIGINT → cursor save |
| Logs | `journalctl -u canton-live-ingest -f` |
| Runbook | `scripts/ingest/LIVE-INGEST-RUNBOOK.sh` |

**Environment files on governance-dashboard**:
- `~/amulet-scan-port/scripts/ingest/.env` — Scan API config
- `~/.gcs_hmac_env` — HMAC keys with `export` (for shell/tmux)
- `~/.gcs_hmac_env.systemd` — HMAC keys without `export` (for systemd)

Service file source: `scripts/ingest/canton-live-ingest.service`

### Deprecated Scripts

- `fetch-backfill.js` — Has a systematic data-loss bug: JavaScript `Date`
  truncates Canton's microsecond-precision `record_time` to milliseconds
  during cursor advancement, losing 0.1–0.4% of records per batch boundary.
  See `scripts/ingest/DEPRECATED.md`.

---

## BigQuery Pipeline

### Datasets

| Dataset | Purpose |
|---------|---------|
| `raw` | External tables on GCS Parquet (read-only, no materialization cost) |
| `transformed` | Materialized tables with proper types + bronze analytical views |
| `canton_silver` | Silver layer: partitioned, clustered materialized tables |

### Scripts (`scripts/bigquery/`)

**Bronze layer** (`bronze/`):

| Script | Purpose |
|--------|---------|
| `01-create-external-tables.sql` | External tables on GCS Parquet (`raw.events`, `raw.updates`) |
| `02-transform-raw-data.sql` | Parse timestamps, extract arrays, parse JSON → `transformed.*_parsed` |
| `03-parse-events-by-template.sql` | Views per template (Amulet, ValidatorLicense, rewards, etc.) |
| `04-parse-updates.sql` | Transaction and reassignment views, daily activity |
| `05-governance-tables.sql` | DsoRules, VoteRequest, Confirmation, ElectionRequest views |
| `06-rewards-tables.sql` | Materialized rewards summary, leaderboards |
| `07-amulet-tables.sql` | Amulet creation/archive/supply/transfer views |
| `08-exercised-choices.sql` | Exercised event views (choices, transfers, governance actions) |
| `09-verify-transforms.sql` | Row count, type, array, and data-loss verification queries |

**Silver layer** (`silver/`):

| Script | Purpose |
|--------|---------|
| `01-amulet-silver.sql` | Amulet contracts, archives, locked amulets, lifecycle, daily supply |
| `02-governance-silver.sql` | Vote requests, individual votes, DSO rules state, SV membership, proposal lifecycle |
| `03-rewards-silver.sql` | Reward coupons (app/SV/validator), unified rewards, leaderboard, SV weight history |
| `04-validators-silver.sql` | Licenses, rights, faucet coupons, liveness, lifecycle, performance |
| `05-network-silver.sql` | Traffic, mining rounds, round lifecycle, ANS entries, SV nodes, price votes, daily metrics |
| `06-exercised-silver.sql` | Choices, transfers, traffic purchases, governance actions, reward claims, round ops |

### Deployment

```bash
# Dry run (prints rendered SQL)
PROJECT_ID=my-project BUCKET_NAME=my-bucket DRY_RUN=1 ./scripts/bigquery/deploy.sh

# Deploy all layers
PROJECT_ID=my-project BUCKET_NAME=my-bucket ./scripts/bigquery/deploy.sh

# Deploy one layer
PROJECT_ID=my-project BUCKET_NAME=my-bucket ./scripts/bigquery/deploy.sh bronze
PROJECT_ID=my-project BUCKET_NAME=my-bucket ./scripts/bigquery/deploy.sh silver
```

### Refresh Strategy

The transform step (`02-transform-raw-data.sql`) is a full `DROP + CREATE + INSERT`.
For incremental refresh, the external tables (`raw.events`, `raw.updates`) automatically
pick up new Parquet files as they land in GCS from live ingestion. Silver tables
need to be re-materialized to include new data.

---

## Local DuckDB API

The API server queries the same Parquet files directly via DuckDB for the React frontend.

| Characteristic | Value |
|---------------|-------|
| Port | 3001 |
| Mode | Read-only, request-driven, stateless |
| Connection | `server/duckdb/connection.js` |
| Query style | `db.safeQuery()` with parameterized queries |
| Data source | `db.getEventsSource()` → Parquet glob |

### Query Patterns

```javascript
// Template-filtered query
const events = await db.safeQuery(`
  SELECT * FROM ${db.getEventsSource()}
  WHERE template_name = $1
  ORDER BY record_time DESC
  LIMIT $2
`, [templateName, limit]);

// Direct Parquet glob
const result = await db.safeQuery(`
  SELECT COUNT(*) as count
  FROM read_parquet('${basePath}/**/events-*.parquet', union_by_name=true)
  WHERE migration_id = $1
`, [migrationId]);
```

### Performance

| Metric | Value |
|--------|-------|
| Cold query | 100–500ms |
| Warm query | 10–50ms |
| Full table scan | 2–10s |
| Template-filtered | 50–200ms |

---

## Archive Remediation History

The full archive (2024-06-24 → 2026-05-17) was re-ingested via
`reingest-updates.js` to fix the `fetch-backfill.js` cursor bug:

| Migration | Date Range | Days | Records |
|-----------|-----------|------|---------|
| M0 | 2024-06-24 → 2024-10-16 | 115 | 2,746,625 |
| M1 | 2024-10-16 → 2024-12-11 | 57 | 1,655,530 |
| M2 | 2024-12-11 → 2025-06-25 | 197 | 12,584,874 |
| M3 | 2025-06-25 → 2025-12-10 | 169 | 77,849,404 |
| M4 | 2025-12-10 → 2026-05-17 | 159 | 153,900,412 |
| **Total** | | **697** | **248,736,845** |

15-day random sample verified: 15/15 days MATCH Scan API. Zero gaps, zero duplicates.

### Bugs Fixed During Remediation

| Bug | Fix |
|-----|-----|
| Backfill cursor truncates microseconds | Deprecated `fetch-backfill.js`, used `reingest-updates.js` |
| `effective_at` filter dropped stragglers | Removed from `processAndWrite` |
| Migration ID override on boundary days | Added `migration_id` filter |
| gsutil reauth failures | Replaced all gsutil with `@google-cloud/storage` SDK |
| DuckDB timeout on large days | Configurable via `VSC_DUCKDB_TIMEOUT_MS` |
| Live ingest file proliferation | `BATCH_SIZE` 100 → 1000 |

---

## GCS Operations

All GCS operations use `@google-cloud/storage` SDK with Application Default
Credentials (ADC). No script depends on gsutil.

| Script | GCS Usage |
|--------|-----------|
| `verify-scan-completeness.js` | `listExistingGlobs()` via SDK |
| `gcs-scanner.js` | Walks Hive partitions via SDK |
| `gcs-preflight.js` | Read/write checks via SDK |
| `fetch-updates.js` | Writes Parquet via SDK |

---

## Design Decisions

### Why Parquet?

1. **Direct SQL** — DuckDB and BigQuery read Parquet natively
2. **Column pruning** — Only reads columns needed for each query
3. **Predicate pushdown** — Filters applied at file level
4. **Schema evolution** — `union_by_name=true` handles schema changes
5. **Dual use** — Same files serve local DuckDB and cloud BigQuery

### Why GCS as Source of Truth?

1. **Durability** — Cloud storage with redundancy, not a single VM disk
2. **Shared access** — BigQuery external tables read directly from GCS
3. **Live ingest** — systemd service writes continuously, BigQuery picks up new files
4. **Cost** — External tables avoid BigQuery storage costs for raw data

### Why Three BigQuery Datasets?

1. **`raw`** — Zero-cost external tables; always current as files land in GCS
2. **`transformed`** — Fixes Parquet type mismatches; query-ready with proper types
3. **`canton_silver`** — Pre-joined, partitioned, clustered tables for dashboards
