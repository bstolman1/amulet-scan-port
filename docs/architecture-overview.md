# Architecture Overview

This document describes the high-level architecture of Amulet Scan, a ledger explorer for Canton Network.

## System Design Principles

1. **Local-First**: All data is stored locally in binary files, enabling offline operation and zero cloud costs
2. **Streaming-First**: Large datasets are processed via streaming to avoid memory exhaustion
3. **Incremental Processing**: Indexes are built incrementally, processing only new files
4. **Source of Truth**: Binary `.pb.zst` files are immutable and serve as the canonical data source

## Component Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              FRONTEND (React + Vite)                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │  Dashboard  │  │ Governance  │  │   Supply    │  │     Transactions        │  │
│  │   Pages     │  │   Tracker   │  │  Analytics  │  │       Explorer          │  │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────────────────┘  │
│                                        │                                         │
│                            TanStack Query (Data Fetching)                        │
└───────────────────────────────────────────│──────────────────────────────────────┘
                                            │ HTTP REST
                                            ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              API SERVER (Express.js)                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │   /events   │  │    /acs     │  │   /stats    │  │       /engine           │  │
│  │   Router    │  │   Router    │  │   Router    │  │        Router           │  │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────────────────┘  │
│                                        │                                         │
│  ┌─────────────────────────────────────────────────────────────────────────────┐ │
│  │                         Warehouse Engine                                     │ │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌────────────────────────┐ │ │
│  │  │   File     │  │  Template  │  │   Vote     │  │       Party            │ │ │
│  │  │  Indexer   │  │   Index    │  │   Index    │  │       Index            │ │ │
│  │  └────────────┘  └────────────┘  └────────────┘  └────────────────────────┘ │ │
│  └─────────────────────────────────────────────────────────────────────────────┘ │
│                                        │                                         │
│  ┌─────────────────────────────────────────────────────────────────────────────┐ │
│  │                         DuckDB Connection Pool                               │ │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌────────────────────────┐ │ │
│  │  │   Query    │  │   Binary   │  │   Safe     │  │       Metrics          │ │ │
│  │  │  Executor  │  │   Reader   │  │   Query    │  │       Tracking         │ │ │
│  │  └────────────┘  └────────────┘  └────────────┘  └────────────────────────┘ │ │
│  └─────────────────────────────────────────────────────────────────────────────┘ │
└───────────────────────────────────────────│──────────────────────────────────────┘
                                            │
                                            ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              DATA LAYER (Local Files)                            │
│  ┌─────────────────────────────────────────────────────────────────────────────┐ │
│  │                         Binary Files (.pb.zst)                               │ │
│  │  data/ledger_raw/                                                            │ │
│  │  └── migration=1/year=2024/month=12/day=15/events-{timestamp}-{batch}.pb.zst│ │
│  └─────────────────────────────────────────────────────────────────────────────┘ │
│  ┌─────────────────────────────────────────────────────────────────────────────┐ │
│  │                         ACS Snapshots (Parquet)                              │ │
│  │  data/acs/migration=1/year=2024/month=12/day=15/snapshot=120000/*.parquet   │ │
│  └─────────────────────────────────────────────────────────────────────────────┘ │
│  ┌─────────────────────────────────────────────────────────────────────────────┐ │
│  │                         DuckDB Tables (Indexes)                              │ │
│  │  template_file_index, vote_request_index, aggregation_state                  │ │
│  └─────────────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Data Flow

### 1. Ingestion Pipeline

```
Canton Scan API  ──▶  Ingestion Scripts  ──▶  Binary Files
     │                      │                      │
     │                      ├── fetch-updates.js   │
     │                      ├── fetch-backfill.js  │
     │                      └── fetch-acs.js       │
     │                                             │
     │              Protobuf encode + Zstd compress│
     └──────────────────────────────────────────────
```

**Key characteristics:**
- Parallel fetching with configurable worker pools
- Atomic writes with cursor tracking for resumability
- Partition-based storage for efficient querying

### 2. Index Building

```
Binary Files  ──▶  Warehouse Engine  ──▶  DuckDB Tables
     │                   │                     │
     │                   ├── Template Index    │
     │                   ├── Vote Request Index│
     │                   ├── Party Index       │
     │                   └── Aggregations      │
     │                                         │
     │      Streaming decode + incremental     │
     └──────────────────────────────────────────
```

**Key characteristics:**
- Incremental indexing (only processes new files)
- Worker pools for parallel processing
- Background execution (non-blocking)

### 3. Query Execution

```
Frontend  ──▶  API Server  ──▶  Index Lookup  ──▶  Binary Read
    │              │                │                  │
    │              │                │   Template Index │
    │              │                │   reduces 35K    │
    │              │                │   files to ~100  │
    │              │                                   │
    │              │   Streaming decompress + filter   │
    └──────────────────────────────────────────────────
```

**Key characteristics:**
- Index-accelerated queries (100-350x speedup)
- Streaming decompression (memory-safe)
- Connection pooling for concurrent requests

## Component Details

### Frontend (`src/`)

| Component | Purpose |
|-----------|---------|
| `pages/` | Route-level components for each view |
| `components/` | Reusable UI components |
| `hooks/` | Data fetching and state management |
| `lib/` | Utilities and API clients |

### API Server (`server/`)

| Component | Purpose |
|-----------|---------|
| `api/` | Express route handlers |
| `engine/` | Warehouse engine (indexing, aggregation) |
| `duckdb/` | Database connection and binary reader |
| `cache/` | In-memory caching layer |
| `inference/` | LLM-based classification (optional) |

### Ingestion (`scripts/ingest/`)

| Script | Purpose |
|--------|---------|
| `fetch-updates.js` | Poll for live updates |
| `fetch-backfill.js` | Historical data ingestion |
| `fetch-acs.js` | ACS snapshot capture |
| `materialize-parquet.js` | Binary → Parquet conversion |

## Performance Optimizations

### Binary File Design
- **Protobuf**: Compact binary serialization
- **Zstandard**: High compression ratio (10:1)
- **Partitioning**: Date-based partitions for locality

### Indexing Strategy
- **Template File Index**: Map template → files containing that template
- **Party Index**: Map party → files with their events
- **Vote Request Index**: Pre-computed governance data

### Query Optimization
- **Connection Pool**: Reuse DuckDB connections
- **Streaming**: Decompress and filter in chunks
- **Caching**: In-memory caches for hot data

## Scalability

| Metric | Current Capacity |
|--------|------------------|
| Binary files | 35,000+ |
| Compressed size | 1.8 TB |
| Template index build | 10-15 min (parallel) |
| Query latency | <1s for indexed queries |

## Security

- **SQL Sanitization**: All queries use centralized sanitization
- **Input Validation**: Numeric bounds, pattern matching
- **Dangerous Pattern Detection**: UNION injection, DROP statements

## Deployment Options

1. **Local Development**: Single machine, all components
2. **VM Deployment**: Linux VM with systemd services
3. **Cloud Integration**: Optional BigQuery upload for analytics
