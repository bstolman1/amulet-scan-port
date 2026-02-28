

## Plan: Update Tests to Reflect Data Integrity Fixes

### Problem
The 10 data integrity fixes changed behavior in `fetch-updates.js`, `fetch-backfill.js`, and `write-parquet.js`, but 2 existing test files contain outdated code replicas and assertions that no longer match the source. Additionally, no tests verify the new fix behaviors.

### Files to modify

#### 1. `scripts/ingest/test/decode-main-thread.test.js` — Full replica rewrite

The replicated `decodeInMainThread` function (lines 20-75) uses old, buggy patterns that were fixed in the source:

| Line | Old (replica) | New (must match source) |
|------|--------------|------------------------|
| 21 | `!!tx.event` | `!!tx.reassignment` |
| 22 | `normalizeUpdate(tx)` then `update.migration_id = migrationId` | `normalizeUpdate({...tx, migration_id: migrationId})` |
| 40 | `tx.event?.created_event` | `tx.reassignment?.event?.created_event` |
| 46-50 | `if (ev.effective_at) { push } else { warn }` | Remove guard — `normalizeEvent` throws |
| 63 | `Object.entries(eventsById)` | `flattenEventsInTreeOrder(eventsById, rootEventIds)` |
| 65 | `ev.event_id = eventId` (silent) | Add mismatch warning logic |
| 66-69 | `if (ev.effective_at) { push } else { warn }` | Remove guard |

Test fixture updates needed:
- Reassignment tests (lines 178-231) use `tx.event` structure — must change to `tx.reassignment.event` structure
- The "should include reassign events" test (line 202) puts created_event under `tx.event` — must move to `tx.reassignment.event.created_event`

#### 2. `scripts/ingest/test/recommendations-fixes.test.js` — Add structural assertions for new fixes

Add new describe blocks verifying each new fix exists in the source:

- **Fix #13** (audit fix #2): `fetch-updates.js` uses `Math.max` pattern for `maxRecordTime` instead of `transactions[last].record_time`
- **Fix #14** (audit fix #1): `fetch-updates.js` checks `batchErrors > 0` and holds cursor (`cursor_hold_on_errors`)
- **Fix #15** (audit fix #7): Both `fetch-updates.js` and `fetch-backfill.js` use `Promise.allSettled` in `bufferUpdates`/`bufferEvents`
- **Fix #16** (audit fix #3): `fetch-backfill.js` `processBackfillItems` no-pool path has per-tx try/catch
- **Fix #17** (audit fix #4): `fetch-backfill.js` pool fallback `.catch` handler has per-tx try/catch
- **Fix #18** (audit fix #5): `fetch-backfill.js` `seenUpdateIds` uses LRU eviction (keeps newest 250k) instead of `.clear()`
- **Fix #19** (audit fix #8): `fetch-backfill.js` `fetchTimeSliceStreaming` uses `Promise.allSettled` for `inflightProcesses`
- **Fix #20** (audit fix #10): `fetch-backfill.js` finalization flush catches re-throw instead of `catch {}`

Each assertion reads the source file and checks for the presence/absence of specific patterns.

#### 3. New file: `scripts/ingest/test/data-integrity-fixes.test.js`

Behavioral tests for fixes that can be tested without importing the full modules:

- **LRU eviction logic**: Verify that when a Set exceeds 500k entries, it retains the newest 250k (not a full clear)
- **`Promise.allSettled` per-partition requeue**: Verify that only failed partition records are re-queued (not all records)
- **Cursor hold on errors**: Verify the `processUpdates` return shape includes `errors` count
- **`max(record_time)` cursor**: Verify that given out-of-order transactions, the highest record_time is selected

### Implementation order

1. Rewrite `decode-main-thread.test.js` replica + update fixtures
2. Add new structural assertions to `recommendations-fixes.test.js`
3. Create `data-integrity-fixes.test.js` with behavioral tests

### Technical details

All three test files use the same patterns already established in the codebase:
- **Structural tests**: Read source with `fs.readFileSync` and assert on string patterns (same as existing `recommendations-fixes.test.js`)
- **Behavioral tests**: Replicate isolated logic and test directly (same as existing `decode-main-thread.test.js`)
- No new dependencies needed

