# Wallet Lockup Audit — Scan API Reference

How to manually replicate every API call the audit script makes, using curl.
All calls go to the Canton Scan API. Any healthy SV endpoint works — the script
tries all of them and uses the first that succeeds.

## Pick an endpoint

Use any of the 10 healthy SV endpoints. Global Synchronizer Foundation (GSF) is
a reliable default:

```
BASE=https://scan.sv-1.global.canton.network.sync.global/api/scan
```

Other options: `digitalasset.com`, `cumberland.io`, `tradeweb.com`,
`proofgroup.xyz`, `fivenorth.io`, `sv-nodeops.com`, `c7.digital`.

All curl examples below use `$BASE`.

---

## Step 1 — Health check

Verify the endpoint is alive:

```bash
curl -s "$BASE/v0/dso" | head -c 200
```

200 = healthy. 403 = IP not allowed (try a different SV).

---

## Step 2 — Get the current round

```bash
curl -s "$BASE/v0/round-of-latest-data"
```

Response:

```json
{
  "round": 95798,
  "effectiveAt": "2026-05-13T18:29:05.311463Z"
}
```

Save `effectiveAt` — you need it for the next step.

---

## Step 3 — Discover the migration ID

The Canton Network has undergone multiple migrations. You need the current
migration ID to query ACS snapshots. The script probes IDs 0–10 across
endpoints. The quickest manual approach:

```bash
# Try the migrations schedule endpoint
curl -s "$BASE/v0/migrations/schedule"

# Or probe migration-info for IDs 0, 1, 2, 3, 4, ...
curl -s -X POST "$BASE/v0/backfilling/migration-info" \
  -H 'Content-Type: application/json' \
  -d '{"migration_id": 4}'
```

As of May 2026, the current migration ID is **4**. If a probe returns 200,
that migration ID exists.

---

## Step 4 — Find the ACS snapshot timestamp

The holdings endpoints require a `record_time` that corresponds to an existing
ACS snapshot. Use the `effectiveAt` from Step 2 and the migration ID from Step 3:

```bash
curl -s "$BASE/v0/state/acs/snapshot-timestamp?before=2026-05-13T18:29:05.311463Z&migration_id=4"
```

Response:

```json
{
  "record_time": "2026-05-13T18:00:00Z"
}
```

Save `record_time` — this is the anchor for all holdings queries below.

---

## Step 5 — Holdings summary (balance check)

This is the primary balance query. Returns unlocked, locked, total, and holding
fees for a party.

```bash
curl -s -X POST "$BASE/v0/holdings/summary" \
  -H 'Content-Type: application/json' \
  -d '{
    "migration_id": 4,
    "record_time": "2026-05-13T18:00:00Z",
    "record_time_match": "exact",
    "owner_party_ids": [
      "23d169c2-0909-4c70-81d1-1922de6febaa::1220b770cd6350fe69e14bb55a42588237a15747a22392faa3fa8fe60cd83843585f"
    ]
  }'
```

Response:

```json
{
  "record_time": "2026-05-13T18:00:00Z",
  "migration_id": 4,
  "computed_as_of_round": 95802,
  "summaries": [
    {
      "party_id": "23d169c2-...585f",
      "total_unlocked_coin": "31746081.7500000000",
      "total_locked_coin": "0.0000000000",
      "total_coin_holdings": "31746081.7500000000",
      "total_available_coin": "31746078.6798789283",
      "accumulated_holding_fees_unlocked": "3.0701210717",
      "accumulated_holding_fees_locked": "0.0000000000",
      "accumulated_holding_fees_total": "3.0701210717"
    }
  ]
}
```

**How to read it:**

| Field | Meaning |
|---|---|
| `total_unlocked_coin` | CC in unlocked Amulet contracts (can be transferred) |
| `total_locked_coin` | CC in LockedAmulet contracts (cannot be transferred until lock expires) |
| `total_coin_holdings` | `unlocked + locked` (gross balance before fees) |
| `total_available_coin` | `holdings - accumulated_holding_fees` (net spendable balance) |
| `accumulated_holding_fees_*` | Fees accrued since contract creation (deducted from balance over time) |
| `computed_as_of_round` | The round the API used for fee calculations |

**Locked vs unlocked:** If `total_locked_coin` is 0 and `total_unlocked_coin`
equals `total_coin_holdings`, all funds are in plain `Splice.Amulet:Amulet`
contracts — unlocked and transferable.

---

## Step 6 — Holdings state (contract-level detail)

Returns the individual Amulet/LockedAmulet contracts held by a party. This is
where you see initial amounts, creation dates, lock expiry, and contract IDs.

```bash
curl -s -X POST "$BASE/v0/holdings/state" \
  -H 'Content-Type: application/json' \
  -d '{
    "migration_id": 4,
    "record_time": "2026-05-13T18:00:00Z",
    "record_time_match": "exact",
    "page_size": 500,
    "owner_party_ids": [
      "23d169c2-0909-4c70-81d1-1922de6febaa::1220b770cd6350fe69e14bb55a42588237a15747a22392faa3fa8fe60cd83843585f"
    ]
  }'
```

Response:

```json
{
  "record_time": "2026-05-13T18:00:00Z",
  "migration_id": 4,
  "created_events": [
    {
      "contract_id": "00bf993f...667b",
      "template_id": "3ca134...bf9ec1:Splice.Amulet:Amulet",
      "package_name": "splice-amulet",
      "create_arguments": {
        "dso": "DSO::1220b1431ef2...accc",
        "owner": "23d169c2-...585f",
        "amount": {
          "initialAmount": "50.0000000000",
          "ratePerRound": { "rate": "0.0001106279" }
        }
      },
      "created_at": "2026-01-30T14:43:31.743799Z",
      "signatories": ["23d169c2-...585f", "DSO::..."],
      "observers": []
    },
    {
      "contract_id": "00eaca5e...f316",
      "template_id": "3ca134...bf9ec1:Splice.Amulet:Amulet",
      "package_name": "splice-amulet",
      "create_arguments": {
        "dso": "DSO::1220b1431ef2...accc",
        "owner": "23d169c2-...585f",
        "amount": {
          "initialAmount": "31746031.7500000000",
          "ratePerRound": { "rate": "0.0001076259" }
        }
      },
      "created_at": "2026-02-05T01:32:36.011021Z",
      "signatories": ["23d169c2-...585f", "DSO::..."],
      "observers": []
    }
  ],
  "next_page_token": null
}
```

**How to read it:**

| Field | Meaning |
|---|---|
| `template_id` | Ends with `:Amulet` (unlocked) or `:LockedAmulet` (locked) |
| `create_arguments.amount.initialAmount` | The original CC amount when the contract was created |
| `create_arguments.amount.ratePerRound.rate` | Holding fee rate deducted each round |
| `create_arguments.owner` | Party ID that owns this contract |
| `create_arguments.lock` | Only present on LockedAmulet — contains `holders` and `expiresAt` |
| `created_at` | When this contract was created on the ledger |
| `contract_id` | Unique identifier for this specific contract |
| `next_page_token` | If not null, pass as `"after"` in next request to get more contracts |

**Pagination:** If `next_page_token` is not null, make another request with
`"after": <token>` added to the body. Repeat until null.

**Locked contract example** (not present in these wallets, shown for reference):

```json
{
  "template_id": "...:Splice.Amulet:LockedAmulet",
  "create_arguments": {
    "owner": "...",
    "amount": { "initialAmount": "1000000.00", "ratePerRound": { "rate": "0.0001" } },
    "lock": {
      "holders": ["lock-holder-party::..."],
      "expiresAt": "2027-05-13T00:00:00Z"
    }
  }
}
```

---

## Step 7 — Transaction history (if available)

Not all SVs support this endpoint. Returns typed transaction history for a party.

```bash
curl -s -X POST "$BASE/v0/transactions/by-party" \
  -H 'Content-Type: application/json' \
  -d '{
    "party": "23d169c2-0909-4c70-81d1-1922de6febaa::1220b770cd6350fe69e14bb55a42588237a15747a22392faa3fa8fe60cd83843585f",
    "limit": 200
  }'
```

Response (when supported):

```json
{
  "transactions": [
    {
      "transaction_type": "transfer",
      "date": "2026-03-01T10:00:00Z",
      "transfer": {
        "sender": { "party": "...", "input_amulet_amount": "100.00" },
        "receivers": [{ "party": "...", "amount": "99.50" }],
        "balance_changes": [...]
      }
    },
    {
      "transaction_type": "mint",
      "date": "2026-02-05T01:32:36Z",
      "mint": { "amulet_owner": "...", "amulet_amount": "31746031.75" }
    }
  ]
}
```

**Transaction types relevant to the audit:**

| Type | Meaning | Audit impact |
|---|---|---|
| `transfer` | CC moved between parties | Outbound = lockup violation |
| `mint` | New CC created (initial deposit) | Expected at funding time |
| `tap` | CC created from faucet | Expected at setup |

Returns 404 on many SVs. The script treats this as non-fatal.

---

## Step 8 — Ledger update scan (activity detection fallback)

Scans recent ledger updates for any events involving a party. Works on all SVs.

```bash
curl -s -X POST "$BASE/v2/updates" \
  -H 'Content-Type: application/json' \
  -d '{"page_size": 100}'
```

Response:

```json
{
  "transactions": [
    {
      "update_id": "...",
      "migration_id": 4,
      "record_time": "2026-05-13T18:29:00Z",
      "effective_at": "2026-05-13T18:29:00Z",
      "events_by_id": {
        "event-001": {
          "event_type": "created_event",
          "template_id": "...:Splice.Amulet:Amulet",
          "contract_id": "...",
          "signatories": ["party-A", "DSO::..."],
          "observers": []
        }
      }
    }
  ]
}
```

**How the audit uses it:** For each event in each transaction, it checks if any
of the audited party IDs appear in `signatories`, `observers`, or
`acting_parties`. Any match = that wallet was involved in a ledger event.

**Pagination:** For subsequent pages, add `"after"` to the body:

```json
{
  "page_size": 100,
  "after": {
    "after_migration_id": 4,
    "after_record_time": "2026-05-13T18:29:00Z"
  }
}
```

The script scans up to 10 pages (1,000 recent updates).

---

## Quick single-wallet audit (all 4 calls)

Replace `$PARTY` with the wallet's party ID:

```bash
BASE=https://scan.sv-1.global.canton.network.sync.global/api/scan
PARTY="23d169c2-0909-4c70-81d1-1922de6febaa::1220b770cd6350fe69e14bb55a42588237a15747a22392faa3fa8fe60cd83843585f"

# 1. Get current round
ROUND=$(curl -s "$BASE/v0/round-of-latest-data")
EFFECTIVE_AT=$(echo "$ROUND" | python3 -c "import sys,json; print(json.load(sys.stdin)['effectiveAt'])")
echo "Round: $ROUND"

# 2. Get snapshot timestamp (migration 4)
SNAP=$(curl -s "$BASE/v0/state/acs/snapshot-timestamp?before=$EFFECTIVE_AT&migration_id=4")
RECORD_TIME=$(echo "$SNAP" | python3 -c "import sys,json; print(json.load(sys.stdin)['record_time'])")
echo "Snapshot: $RECORD_TIME"

# 3. Holdings summary
echo "=== Holdings Summary ==="
curl -s -X POST "$BASE/v0/holdings/summary" \
  -H 'Content-Type: application/json' \
  -d "{
    \"migration_id\": 4,
    \"record_time\": \"$RECORD_TIME\",
    \"record_time_match\": \"exact\",
    \"owner_party_ids\": [\"$PARTY\"]
  }" | python3 -m json.tool

# 4. Holdings state (contract details)
echo "=== Contracts ==="
curl -s -X POST "$BASE/v0/holdings/state" \
  -H 'Content-Type: application/json' \
  -d "{
    \"migration_id\": 4,
    \"record_time\": \"$RECORD_TIME\",
    \"record_time_match\": \"exact\",
    \"page_size\": 500,
    \"owner_party_ids\": [\"$PARTY\"]
  }" | python3 -m json.tool
```

---

## How balances are computed

The Scan API computes balances from the active Amulet contracts on the ledger:

1. **Total holdings** = sum of `initialAmount` across all Amulet + LockedAmulet
   contracts owned by the party
2. **Holding fees** = cumulative fees deducted based on `ratePerRound` since
   each contract's creation (up to `computed_as_of_round`)
3. **Available balance** = `total_holdings - accumulated_holding_fees`
4. **Locked vs unlocked** = split by contract template:
   - `Splice.Amulet:Amulet` → unlocked (transferable)
   - `Splice.Amulet:LockedAmulet` → locked (has `lock.expiresAt`)

---

## The 3 wallets

| Label | Party ID |
|---|---|
| Wallet 1 | `23d169c2-0909-4c70-81d1-1922de6febaa::1220b770cd6350fe69e14bb55a42588237a15747a22392faa3fa8fe60cd83843585f` |
| Wallet 2 | `23d169c2-0909-4c70-81d1-1922de6febaa::1220e226ff1393b8a1e4954f41ccb55b9cc71b85aee2d6f6c30e4b553803e635ed22` |
| Wallet 3 | `23d169c2-0909-4c70-81d1-1922de6febaa::12206c4c9c59523446ba3057497faeef75e589f6120e5f38121a23ad3632386a49c7` |

All three share the same namespace (`23d169c2-...febaa`) — they belong to the
same participant but are distinct parties with different fingerprints.
