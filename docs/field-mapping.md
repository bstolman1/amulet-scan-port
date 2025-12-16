# Complete Field Mapping: Scan API → .pb.zst Files

This document provides the complete field mapping from the Canton Scan API through to the compressed protobuf files.

## Data Flow Overview

```
Scan API Response
       ↓
normalizeUpdate() / normalizeEvent()  [parquet-schema.js]
       ↓
mapUpdateRecord() / mapEventRecord()  [write-binary.js]
       ↓
Protobuf Encoding                     [encoding.js + ledger.proto]
       ↓
ZSTD Compression                      [compression-worker.js]
       ↓
.pb.zst File                          [data/raw/migration=N/year=YYYY/month=MM/day=DD/]
```

---

## UPDATE Records

### Scan API Response Structure (Transaction)
```json
{
  "transaction": {
    "update_id": "string",
    "synchronizer_id": "string",
    "workflow_id": "string",
    "command_id": "string",
    "offset": "string (numeric)",
    "record_time": "ISO8601 timestamp",
    "effective_at": "ISO8601 timestamp",
    "root_event_ids": ["string"],
    "events_by_id": { "eventId": {...} },
    "trace_context": { ... }
  }
}
```

### Scan API Response Structure (Reassignment)
```json
{
  "reassignment": {
    "update_id": "string",
    "source": "string (synchronizer)",
    "target": "string (synchronizer)",
    "synchronizer_id": "string",
    "unassign_id": "string",
    "submitter": "string",
    "counter": "number",
    "kind": "assign | unassign",
    "record_time": "ISO8601 timestamp",
    "effective_at": "ISO8601 timestamp"
  },
  "event": {
    "created_event": {...} | null,
    "archived_event": {...} | null
  }
}
```

### Normalized Update (parquet-schema.js → normalizeUpdate)
| Field | Type | Source |
|-------|------|--------|
| `update_id` | string | `update.update_id \|\| raw.update_id` |
| `update_type` | string | `'transaction'` or `'reassignment'` |
| `migration_id` | int64 | From backfill context |
| `synchronizer_id` | string | `update.synchronizer_id` |
| `workflow_id` | string | `update.workflow_id` |
| `command_id` | string | `update.command_id` |
| `offset` | int64 | `update.offset` |
| `record_time` | timestamp | `update.record_time` |
| `effective_at` | timestamp | `update.effective_at` |
| `recorded_at` | timestamp | Current time (when we recorded it) |
| `timestamp` | timestamp | Current time |
| `kind` | string | `update.kind` (for reassignments) |
| `root_event_ids` | string[] | `update.root_event_ids` |
| `event_count` | int32 | `Object.keys(events_by_id).length` |
| `source_synchronizer` | string | `update.source` (reassignment) |
| `target_synchronizer` | string | `update.target` (reassignment) |
| `unassign_id` | string | `update.unassign_id` |
| `submitter` | string | `update.submitter` |
| `reassignment_counter` | int64 | `update.counter` |
| `trace_context` | JSON string | `JSON.stringify(update.trace_context)` |
| `update_data` | JSON string | **`JSON.stringify(update)` - FULL ORIGINAL** |

### Protobuf Schema (ledger.proto → Update)
| Proto Field | Proto Type | Mapped From |
|-------------|------------|-------------|
| `id` | string | `update_id` |
| `type` | string | `update_type` |
| `synchronizer` | string | `synchronizer_id` |
| `effective_at` | int64 | Unix ms from `effective_at` |
| `recorded_at` | int64 | Unix ms from `recorded_at` |
| `record_time` | int64 | Unix ms from `record_time` |
| `command_id` | string | `command_id` |
| `workflow_id` | string | `workflow_id` |
| `kind` | string | `kind` |
| `migration_id` | int64 | `migration_id` |
| `offset` | int64 | `offset` |
| `root_event_ids` | repeated string | `root_event_ids` |
| `event_count` | int32 | `event_count` |
| `source_synchronizer` | string | `source_synchronizer` |
| `target_synchronizer` | string | `target_synchronizer` |
| `unassign_id` | string | `unassign_id` |
| `submitter` | string | `submitter` |
| `reassignment_counter` | int64 | `reassignment_counter` |
| `trace_context_json` | string | `trace_context` |
| `update_data_json` | string | **`update_data` - FULL ORIGINAL** |

---

## EVENT Records

### Scan API Event Types

#### Created Event
```json
{
  "created_event": {
    "contract_id": "string",
    "template_id": "string (package:module:template)",
    "create_arguments": { ... },
    "signatories": ["string"],
    "observers": ["string"],
    "witness_parties": ["string"],
    "contract_key": { ... } | null,
    "created_at": "ISO8601 timestamp"
  }
}
```

#### Archived Event
```json
{
  "archived_event": {
    "contract_id": "string",
    "template_id": "string"
  }
}
```

#### Exercised Event
```json
{
  "exercised_event": {
    "contract_id": "string",
    "template_id": "string",
    "choice": "string",
    "choice_argument": { ... },
    "acting_parties": ["string"],
    "consuming": boolean,
    "interface_id": "string" | null,
    "child_event_ids": ["string"],
    "exercise_result": { ... } | null
  }
}
```

### Normalized Event (parquet-schema.js → normalizeEvent)
| Field | Type | Source |
|-------|------|--------|
| `event_id` | string | `event.event_id \|\| "${updateId}-${contractId}"` |
| `update_id` | string | Parent update's ID |
| `event_type` | string | `'created'`, `'archived'`, `'exercised'`, `'reassign_create'`, `'reassign_archive'` |
| `contract_id` | string | From created/archived/exercised event |
| `template_id` | string | From event |
| `package_name` | string | Extracted from template_id (first part before `:`) |
| `migration_id` | int64 | From backfill context |
| `synchronizer_id` | string | From updateInfo |
| `effective_at` | timestamp | `created_at` or `record_time` |
| `recorded_at` | timestamp | Current time |
| `timestamp` | timestamp | Current time |
| `created_at_ts` | timestamp | Same as effective_at |
| `signatories` | string[] | From created event |
| `observers` | string[] | From created event |
| `acting_parties` | string[] | From exercised event |
| `witness_parties` | string[] | From created event |
| `payload` | JSON string | `create_arguments` or `choice_argument` |
| `contract_key` | JSON string | From created event |
| `choice` | string | From exercised event |
| `consuming` | boolean | From exercised event |
| `interface_id` | string | From exercised event |
| `child_event_ids` | string[] | From exercised event |
| `exercise_result` | JSON string | From exercised event |
| `source_synchronizer` | string | Reassignment source |
| `target_synchronizer` | string | Reassignment target |
| `unassign_id` | string | Reassignment unassign ID |
| `submitter` | string | Reassignment submitter |
| `reassignment_counter` | int64 | Reassignment counter |
| `raw` | object | **COMPLETE ORIGINAL EVENT** |

### Protobuf Schema (ledger.proto → Event)
| Proto Field | Proto Type | Mapped From |
|-------------|------------|-------------|
| `id` | string | `event_id` |
| `update_id` | string | `update_id` |
| `type` | string | `event_type` |
| `synchronizer` | string | `synchronizer_id` |
| `effective_at` | int64 | Unix ms |
| `recorded_at` | int64 | Unix ms |
| `created_at_ts` | int64 | Unix ms |
| `contract_id` | string | `contract_id` |
| `template` | string | `template_id` |
| `package_name` | string | `package_name` |
| `migration_id` | int64 | `migration_id` |
| `signatories` | repeated string | `signatories` |
| `observers` | repeated string | `observers` |
| `acting_parties` | repeated string | `acting_parties` |
| `witness_parties` | repeated string | `witness_parties` |
| `payload_json` | string | `payload` |
| `contract_key_json` | string | `contract_key` |
| `choice` | string | `choice` |
| `consuming` | bool | `consuming` |
| `interface_id` | string | `interface_id` |
| `child_event_ids` | repeated string | `child_event_ids` |
| `exercise_result_json` | string | `exercise_result` |
| `source_synchronizer` | string | `source_synchronizer` |
| `target_synchronizer` | string | `target_synchronizer` |
| `unassign_id` | string | `unassign_id` |
| `submitter` | string | `submitter` |
| `reassignment_counter` | int64 | `reassignment_counter` |
| `raw_json` | string | **`JSON.stringify(raw)` - FULL ORIGINAL** |
| `party` | string | Deprecated, kept for backwards compat |

---

## Critical Data Preservation Fields

### For Updates
- **`update_data_json`**: Contains the COMPLETE original `transaction` or `reassignment` object as JSON
- This means even if individual field extraction has bugs, the full original data is preserved

### For Events
- **`raw_json`**: Contains the COMPLETE original event object as JSON
- This means even if individual field extraction has bugs, the full original data is preserved

---

## File Naming Convention

```
{type}-{timestamp_ms}-{random_hex}.pb.zst

Examples:
- updates-1734567890123-a1b2c3d4.pb.zst
- events-1734567890456-e5f6g7h8.pb.zst
```

## Directory Structure

```
data/raw/
├── migration=1/
│   ├── year=2024/
│   │   ├── month=06/
│   │   │   ├── day=15/
│   │   │   │   ├── updates-1718467200000-abc123.pb.zst
│   │   │   │   └── events-1718467200001-def456.pb.zst
│   │   │   └── day=16/
│   │   │       └── ...
│   │   └── month=07/
│   │       └── ...
│   └── year=2025/
│       └── ...
└── migration=3/
    └── ...
```

---

## Verification Checklist

To verify data integrity, check that:

1. **Updates have `update_data_json`** - Can recover all original data
2. **Events have `raw_json`** - Can recover all original data
3. **Timestamps are valid** - `effective_at > 0`, `recorded_at > 0`
4. **IDs are present** - `id` not empty for updates, events
5. **Contract info preserved** - Events have `contract_id`, `template`
6. **Parties preserved** - `signatories`, `observers` arrays populated for created events
7. **Payload preserved** - `payload_json` contains create_arguments/choice_argument
