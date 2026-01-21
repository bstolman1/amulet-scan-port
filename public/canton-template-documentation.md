# Canton Network Template Documentation

**Generated:** 2026-01-21

This document provides comprehensive documentation for all Canton Network templates, explaining their JSON structure, purpose, and use cases.

---

## Table of Contents

1. [Core Currency Templates](#core-currency-templates)
2. [Validator Operations Templates](#validator-operations-templates)
3. [Rewards Templates](#rewards-templates)
4. [Governance Templates](#governance-templates)
5. [SV Operations Templates](#sv-operations-templates)
6. [Network Rules Templates](#network-rules-templates)
7. [Mining Rounds Templates](#mining-rounds-templates)
8. [ANS (Naming Service) Templates](#ans-naming-service-templates)
9. [Transfers Templates](#transfers-templates)
10. [External Party Templates](#external-party-templates)
11. [Subscriptions Templates](#subscriptions-templates)
12. [Network Traffic Templates](#network-traffic-templates)
13. [Applications Templates](#applications-templates)

---

# Core Currency Templates

## Splice.Amulet:Amulet

**Purpose:** Represents the primary Amulet token in the Canton Network

**Description:**
Amulet is the native digital currency of the Canton Network. Each Amulet contract represents a specific amount of tokens held by an owner. Amulets can be transferred, locked, and used for various network operations including paying transaction fees and participating in governance.

**Use Cases:**
- Storing value on the network
- Paying transaction fees
- Rewarding validators and super validators
- Participating in governance decisions

**JSON Structure:**
```json
{
  "dso": "<party_id>",
  "owner": "<party_id>",
  "amount": {
    "initialAmount": "1000.0000000000",
    "createdAt": {
      "number": "79500"
    },
    "ratePerRound": {
      "rate": "0.0000001903"
    }
  }
}
```

**Field Descriptions:**

| Field | Type | Description |
|-------|------|-------------|
| `dso` | Party ID | The DSO (Decentralized Synchronizer Operator) party that manages the network |
| `owner` | Party ID | The party that owns this Amulet |
| `amount.initialAmount` | Numeric String | The initial amount of Amulet tokens (with high precision) |
| `amount.createdAt.number` | Numeric String | Round number when this Amulet was created |
| `amount.ratePerRound.rate` | Numeric String | The holding fee rate applied per round (decreases balance over time) |

---

## Splice.Amulet:LockedAmulet

**Purpose:** Represents Amulet tokens that are locked for a specific purpose

**Description:**
LockedAmulet contracts represent Amulet tokens that have been locked, typically for time-based vesting, collateral, or other purposes requiring temporary immobilization of funds. The lock specifies conditions under which the Amulet can be released.

**Use Cases:**
- Vesting schedules for rewards
- Collateral for operations
- Time-locked payments
- Escrow arrangements

**JSON Structure:**
```json
{
  "dso": "<party_id>",
  "amulet": {
    "owner": "<party_id>",
    "amount": {
      "initialAmount": "500.0000000000",
      "createdAt": { "number": "79000" },
      "ratePerRound": { "rate": "0.0000001903" }
    }
  },
  "lock": {
    "holders": ["<party_id>"],
    "expiresAt": { "number": "80000" }
  }
}
```

**Field Descriptions:**

| Field | Type | Description |
|-------|------|-------------|
| `dso` | Party ID | The DSO party managing the network |
| `amulet` | Object | The underlying Amulet being locked |
| `amulet.owner` | Party ID | The party that owns the locked Amulet |
| `lock` | Object | The lock conditions specifying when/how the Amulet can be released |
| `lock.holders` | Array | Parties that can unlock the Amulet |
| `lock.expiresAt` | Object | Optional time-based lock with expiration round |

---

# Validator Operations Templates

## Splice.ValidatorLicense:ValidatorLicense

**Purpose:** Authorizes a party to operate as a validator

**Description:**
ValidatorLicense contracts grant parties the authority to operate as validators on the Canton Network. Validators are responsible for processing transactions and maintaining network consensus.

**Use Cases:**
- Validator registration
- Validator authorization
- Network participation rights

**JSON Structure:**
```json
{
  "dso": "<party_id>",
  "validator": "<party_id>",
  "sponsor": "<party_id>",
  "faucetState": {
    "tag": "FaucetState",
    "value": {
      "numCouponsMissed": "0",
      "firstReceivedFor": { "number": "1000" },
      "lastReceivedFor": { "number": "79500" }
    }
  },
  "metadata": {
    "version": "0.1.0",
    "contactPoint": "validator@example.com"
  }
}
```

**Field Descriptions:**

| Field | Type | Description |
|-------|------|-------------|
| `dso` | Party ID | The DSO party |
| `validator` | Party ID | The party authorized to validate |
| `sponsor` | Party ID | The SV that sponsored this validator |
| `faucetState` | Object | State of the validator faucet for onboarding |
| `metadata` | Object | Optional metadata including version and contact info |

---

## Splice.Amulet:ValidatorRight

**Purpose:** Grants rights to operate as a validator on the network

**Description:**
ValidatorRight contracts authorize a party to act as a validator for a specific user. Validators process transactions and earn rewards for their participation in the network consensus.

**Use Cases:**
- Authorizing validator operations
- Linking users to their chosen validator
- Validator reward distribution

**JSON Structure:**
```json
{
  "dso": "<party_id>",
  "user": "<party_id>",
  "validator": "<party_id>"
}
```

**Field Descriptions:**

| Field | Type | Description |
|-------|------|-------------|
| `dso` | Party ID | The DSO party |
| `user` | Party ID | The user party being served by this validator |
| `validator` | Party ID | The validator party authorized to validate for this user |

---

## Splice.ValidatorLicense:ValidatorLivenessActivityRecord

**Purpose:** Records validator uptime and activity

**Description:**
ValidatorLivenessActivityRecord contracts track validator uptime and activity, used for reward calculations and network health monitoring.

**Use Cases:**
- Uptime tracking
- Reward calculation
- Network health monitoring

**JSON Structure:**
```json
{
  "validator": "<party_id>",
  "round": { "number": "79500" },
  "domain": "<synchronizer_id>",
  "activityRecord": {
    "trafficReceived": "1000000",
    "lastActiveAt": "2026-01-21T20:00:00Z"
  }
}
```

---

# Rewards Templates

## Splice.Amulet:AppRewardCoupon

**Purpose:** Represents unclaimed rewards earned by application providers

**Description:**
AppRewardCoupon contracts accumulate rewards for applications that facilitate transactions on the network. These coupons can be redeemed for Amulet tokens.

**Use Cases:**
- Tracking app provider rewards
- Incentivizing application development
- Reward redemption

**JSON Structure:**
```json
{
  "dso": "<party_id>",
  "provider": "<party_id>",
  "round": { "number": "79500" },
  "amount": "10.5000000000",
  "featured": true
}
```

**Field Descriptions:**

| Field | Type | Description |
|-------|------|-------------|
| `dso` | Party ID | The DSO party |
| `provider` | Party ID | The application provider earning the reward |
| `round` | Object | The mining round in which this reward was earned |
| `amount` | Numeric String | The reward amount in Amulet units |
| `featured` | Boolean | Whether this is from a featured app with enhanced rewards |

---

## Splice.Amulet:SvRewardCoupon

**Purpose:** Represents unclaimed rewards for Super Validators

**Description:**
SvRewardCoupon contracts track rewards earned by Super Validators (SVs) for their role in network governance and infrastructure. SVs earn rewards for voting, maintaining uptime, and other governance activities.

**Use Cases:**
- Tracking SV governance rewards
- Incentivizing network participation
- SV reward claiming

**JSON Structure:**
```json
{
  "dso": "<party_id>",
  "sv": "<party_id>",
  "round": { "number": "79500" },
  "weight": "10000"
}
```

**Field Descriptions:**

| Field | Type | Description |
|-------|------|-------------|
| `dso` | Party ID | The DSO party |
| `sv` | Party ID | The Super Validator party earning the reward |
| `round` | Object | The round in which this reward was earned |
| `weight` | Numeric String | The SV weight determining reward share |

---

## Splice.Amulet:ValidatorRewardCoupon

**Purpose:** Represents unclaimed rewards for validators

**Description:**
ValidatorRewardCoupon contracts track rewards earned by validators for processing transactions and maintaining network consensus.

**Use Cases:**
- Tracking validator rewards
- Incentivizing validator participation
- Validator reward claiming

**JSON Structure:**
```json
{
  "dso": "<party_id>",
  "user": "<party_id>",
  "validator": "<party_id>",
  "round": { "number": "79500" }
}
```

---

## Splice.Amulet:UnclaimedReward

**Purpose:** Represents rewards that have not yet been claimed

**Description:**
UnclaimedReward contracts represent rewards that are available for claiming but have not yet been converted to Amulet tokens.

**JSON Structure:**
```json
{
  "dso": "<party_id>",
  "amount": "100.0000000000"
}
```

---

# Governance Templates

## Splice.DsoRules:DsoRules

**Purpose:** Defines the core rules for DSO operation

**Description:**
DsoRules is the central governance contract that defines how the Decentralized Synchronizer Operator functions. It contains network parameters, fee schedules, SV weights, and governance rules.

**Use Cases:**
- Network parameter configuration
- Fee schedule definition
- SV weight management
- Governance process rules

**JSON Structure:**
```json
{
  "dso": "<party_id>",
  "epoch": "42",
  "svs": {
    "<sv_party_id>": {
      "name": "SV-1",
      "weight": "10000",
      "joinedAt": "2025-01-01T00:00:00Z"
    }
  },
  "config": {
    "decentralizedSynchronizer": {
      "synchronizers": ["<sync_id>"],
      "requiredSynchronizers": 1
    },
    "numMemberTrafficContractsThreshold": 5,
    "actionConfirmationTimeout": "PT1H",
    "svOnboardingRequestTimeout": "PT24H"
  },
  "dsoDelegate": "<party_id>"
}
```

**Field Descriptions:**

| Field | Type | Description |
|-------|------|-------------|
| `dso` | Party ID | The DSO party identifier |
| `svs` | Map | Map of Super Validators and their metadata |
| `config` | Object | Current network configuration parameters |
| `epoch` | String | Current governance epoch |
| `dsoDelegate` | Party ID | Current DSO delegate for operations |

---

## Splice.DsoRules:VoteRequest

**Purpose:** Represents a governance proposal awaiting votes

**Description:**
VoteRequest contracts represent proposals submitted for governance voting. SVs vote on these proposals to approve or reject changes to network parameters, SV membership, or other governance actions.

**Use Cases:**
- Proposing network changes
- SV membership changes
- Parameter updates
- Governance decisions

**JSON Structure:**
```json
{
  "dso": "<party_id>",
  "requester": "<party_id>",
  "action": {
    "tag": "ARC_DsoRules",
    "value": {
      "tag": "SRARC_OffboardSv",
      "value": {
        "sv": "<sv_party_id>",
        "reason": "Inactivity"
      }
    }
  },
  "reason": {
    "url": "https://governance.example.com/proposal/123",
    "body": "Proposal to offboard inactive SV"
  },
  "votes": {
    "<sv_party_id>": {
      "sv": "<sv_party_id>",
      "accept": true,
      "reason": {
        "url": "",
        "body": "Approved"
      }
    }
  },
  "trackingCid": null,
  "expiresAt": "2026-01-28T00:00:00Z"
}
```

**Field Descriptions:**

| Field | Type | Description |
|-------|------|-------------|
| `dso` | Party ID | The DSO party |
| `requester` | Party ID | The party that submitted the proposal |
| `action` | Tagged Union | The proposed action to be executed if approved |
| `reason` | Object | The reason/justification for the proposal |
| `votes` | Map | Current votes from SVs |
| `trackingCid` | Optional | CID linking to previous related contracts |
| `expiresAt` | Timestamp | When the vote expires |

---

## Splice.DsoRules:Confirmation

**Purpose:** Records confirmation of governance actions

**Description:**
Confirmation contracts record that a governance action has been confirmed and is pending execution.

**JSON Structure:**
```json
{
  "dso": "<party_id>",
  "action": { "tag": "...", "value": "..." },
  "confirmedBy": ["<party_id>"]
}
```

---

## Splice.DsoRules:ElectionRequest

**Purpose:** Manages SV leader election process

**Description:**
ElectionRequest contracts manage the process of electing an SV leader for specific epochs or functions.

**JSON Structure:**
```json
{
  "dso": "<party_id>",
  "epoch": "43",
  "ranking": [
    { "sv": "<sv_party_id>", "rank": 1 },
    { "sv": "<sv_party_id>", "rank": 2 }
  ]
}
```

---

# SV Operations Templates

## Splice.DSO.SvState:SvNodeState

**Purpose:** Tracks the operational state of an SV node

**Description:**
SvNodeState contracts track the current operational state of Super Validator nodes, including their network connectivity and synchronization status.

**JSON Structure:**
```json
{
  "dso": "<party_id>",
  "sv": "<party_id>",
  "state": {
    "synchronizerNodes": {
      "<sync_id>": {
        "sequencer": { "url": "https://sequencer.example.com" },
        "mediator": { "url": "https://mediator.example.com" }
      }
    },
    "cometBftNode": {
      "endpoint": "https://cometbft.example.com"
    }
  }
}
```

---

## Splice.DSO.SvState:SvRewardState

**Purpose:** Tracks accumulated SV rewards

**Description:**
SvRewardState contracts track the accumulated rewards for Super Validators over time.

**JSON Structure:**
```json
{
  "dso": "<party_id>",
  "sv": "<party_id>",
  "rewardState": {
    "lastRoundClaimed": { "number": "79000" },
    "unclaimedRounds": 500
  }
}
```

---

## Splice.DSO.SvState:SvStatusReport

**Purpose:** Records SV status reports for monitoring

**Description:**
SvStatusReport contracts contain status reports submitted by Super Validators for network monitoring and health checks.

**JSON Structure:**
```json
{
  "dso": "<party_id>",
  "sv": "<party_id>",
  "report": {
    "sequencerUptime": "99.99%",
    "mediatorUptime": "99.95%"
  },
  "round": { "number": "79500" }
}
```

---

## Splice.DSO.AmuletPrice:AmuletPriceVote

**Purpose:** Records SV votes on Amulet price

**Description:**
AmuletPriceVote contracts record Super Validator votes on the USD price of Amulet, used to determine the reference exchange rate.

**JSON Structure:**
```json
{
  "dso": "<party_id>",
  "sv": "<party_id>",
  "price": "0.005",
  "round": { "number": "79500" }
}
```

---

# Network Rules Templates

## Splice.AmuletRules:AmuletRules

**Purpose:** Defines rules for Amulet token operations

**Description:**
AmuletRules contracts define the operational rules for Amulet tokens, including transfer rules, fee calculations, and token lifecycle management.

**JSON Structure:**
```json
{
  "dso": "<party_id>",
  "configSchedule": {
    "initialValue": {
      "transferConfig": {
        "createFee": { "fee": "0.03" },
        "transferFee": { "initialRate": "0.0001", "steps": [] },
        "lockHolderFee": { "fee": "0.005" }
      },
      "issuanceCurve": {
        "initialValue": { "amuletToIssuePerYear": "40000000.0" },
        "futureValues": []
      }
    }
  }
}
```

---

## Splice.AmuletRules:TransferPreapproval

**Purpose:** Pre-authorizes a future transfer

**Description:**
TransferPreapproval contracts authorize transfers before they occur, enabling scheduled or conditional transfers.

**JSON Structure:**
```json
{
  "dso": "<party_id>",
  "sender": "<party_id>",
  "receiver": "<party_id>",
  "provider": "<party_id>"
}
```

---

# Mining Rounds Templates

## Splice.Round:OpenMiningRound

**Purpose:** Represents a currently open mining round

**Description:**
OpenMiningRound contracts represent mining rounds that are currently accepting transactions and generating rewards.

**JSON Structure:**
```json
{
  "dso": "<party_id>",
  "round": { "number": "79500" },
  "amuletPrice": "0.005",
  "opensAt": "2026-01-21T20:00:00Z",
  "targetClosesAt": "2026-01-21T20:10:00Z",
  "issuingFor": { "number": "79497" }
}
```

**Field Descriptions:**

| Field | Type | Description |
|-------|------|-------------|
| `dso` | Party ID | The DSO party |
| `round` | Object | The round number |
| `amuletPrice` | Numeric String | Current Amulet price in USD |
| `opensAt` | Timestamp | When this round opened |
| `targetClosesAt` | Timestamp | Expected closing time |
| `issuingFor` | Object | Round number for which rewards are being issued |

---

## Splice.Round:ClosedMiningRound

**Purpose:** Represents a completed mining round

**Description:**
ClosedMiningRound contracts represent mining rounds that have completed and whose rewards are being distributed.

**JSON Structure:**
```json
{
  "dso": "<party_id>",
  "round": { "number": "79497" },
  "amuletPrice": "0.005"
}
```

---

## Splice.Round:IssuingMiningRound

**Purpose:** Represents a round currently issuing rewards

**Description:**
IssuingMiningRound contracts represent rounds in the process of issuing rewards to participants.

**JSON Structure:**
```json
{
  "dso": "<party_id>",
  "round": { "number": "79495" },
  "issuanceConfig": {
    "amuletToIssuePerYear": "40000000.0",
    "validatorRewardPercentage": "0.05",
    "appRewardPercentage": "0.15"
  },
  "optIssuancePerValidatorFaucetCoupon": "2.85",
  "optIssuancePerSvRewardCoupon": "0.15"
}
```

---

# ANS (Naming Service) Templates

## Splice.Ans:AnsEntry

**Purpose:** Represents a registered name in the ANS

**Description:**
AnsEntry contracts represent registered names in the Amulet Naming Service, providing human-readable aliases for party identifiers.

**Use Cases:**
- Human-readable addresses
- Identity management
- Name resolution

**JSON Structure:**
```json
{
  "dso": "<party_id>",
  "user": "<party_id>",
  "name": "alice.canton",
  "url": "https://alice.example.com",
  "description": "Alice's Canton identity",
  "expiresAt": "2027-01-21T00:00:00Z"
}
```

**Field Descriptions:**

| Field | Type | Description |
|-------|------|-------------|
| `dso` | Party ID | The DSO party |
| `user` | Party ID | The owner of the name |
| `name` | String | The registered name |
| `url` | String | Optional URL associated with the name |
| `description` | String | Optional description |
| `expiresAt` | Timestamp | When the registration expires |

---

## Splice.Ans:AnsEntryContext

**Purpose:** Provides context for ANS entries

**Description:**
AnsEntryContext contracts provide additional context and metadata for ANS entries.

**JSON Structure:**
```json
{
  "dso": "<party_id>",
  "reference": "<contract_id>"
}
```

---

## Splice.Ans:AnsRules

**Purpose:** Defines rules for ANS operations

**Description:**
AnsRules contracts define the operational rules for the Amulet Naming Service.

**JSON Structure:**
```json
{
  "dso": "<party_id>",
  "config": {
    "renewalDuration": "P365D",
    "entryFee": "1.0"
  }
}
```

---

# Transfers Templates

## Splice.ExternalPartyAmuletRules:TransferCommand

**Purpose:** Commands a transfer from an external party

**Description:**
TransferCommand contracts represent transfer instructions from external parties.

**JSON Structure:**
```json
{
  "dso": "<party_id>",
  "sender": "<party_id>",
  "receiver": "<party_id>",
  "amount": "100.0000000000",
  "nonce": "12345"
}
```

---

## Splice.ExternalPartyAmuletRules:TransferCommandCounter

**Purpose:** Tracks transfer command sequence numbers

**Description:**
TransferCommandCounter contracts track sequence numbers for external party transfers to prevent replay attacks.

**JSON Structure:**
```json
{
  "dso": "<party_id>",
  "party": "<party_id>",
  "counter": "12345"
}
```

---

## Splice.AmuletTransferInstruction:AmuletTransferInstruction

**Purpose:** Represents a transfer instruction

**Description:**
AmuletTransferInstruction contracts represent instructions to transfer Amulet tokens between parties.

**JSON Structure:**
```json
{
  "dso": "<party_id>",
  "sender": "<party_id>",
  "receiver": "<party_id>",
  "amount": "50.0000000000"
}
```

---

# External Party Templates

## Splice.AmuletRules:ExternalPartySetupProposal

**Purpose:** Proposes setup of an external party

**Description:**
ExternalPartySetupProposal contracts propose the onboarding of external parties to the network.

**JSON Structure:**
```json
{
  "dso": "<party_id>",
  "party": "<party_id>"
}
```

---

## Splice.ExternalPartyAmuletRules:ExternalPartyAmuletRules

**Purpose:** Defines rules for external party operations

**Description:**
ExternalPartyAmuletRules contracts define how external parties can interact with the Amulet network.

**JSON Structure:**
```json
{
  "dso": "<party_id>",
  "party": "<party_id>"
}
```

---

# Subscriptions Templates

## Splice.Wallet.Subscriptions:Subscription

**Purpose:** Represents an active subscription

**Description:**
Subscription contracts represent active recurring payment arrangements between parties.

**JSON Structure:**
```json
{
  "dso": "<party_id>",
  "subscriber": "<party_id>",
  "provider": "<party_id>",
  "subscriptionData": {
    "paymentAmount": "10.0",
    "paymentInterval": "P30D",
    "description": "Monthly service fee"
  }
}
```

---

## Splice.Wallet.Subscriptions:SubscriptionRequest

**Purpose:** Represents a pending subscription request

**Description:**
SubscriptionRequest contracts represent subscription requests awaiting approval.

**JSON Structure:**
```json
{
  "dso": "<party_id>",
  "subscriber": "<party_id>",
  "provider": "<party_id>"
}
```

---

## Splice.Wallet.Subscriptions:SubscriptionIdleState

**Purpose:** Represents a paused subscription

**Description:**
SubscriptionIdleState contracts represent subscriptions that are temporarily paused or idle.

**JSON Structure:**
```json
{
  "dso": "<party_id>",
  "subscription": "<contract_id>"
}
```

---

# Network Traffic Templates

## Splice.DecentralizedSynchronizer:MemberTraffic

**Purpose:** Tracks member traffic on the synchronizer

**Description:**
MemberTraffic contracts track the traffic generated by network members, used for fee calculations and capacity management.

**Use Cases:**
- Traffic monitoring
- Fee calculation
- Capacity planning

**JSON Structure:**
```json
{
  "dso": "<party_id>",
  "member": "<party_id>",
  "synchronizer": "<synchronizer_id>",
  "totalTrafficPurchased": "1000000000",
  "totalTrafficConsumed": "500000000",
  "migrationId": 4
}
```

**Field Descriptions:**

| Field | Type | Description |
|-------|------|-------------|
| `dso` | Party ID | The DSO party |
| `member` | Party ID | The network member |
| `synchronizer` | String | The synchronizer domain |
| `totalTrafficPurchased` | Numeric String | Total traffic purchased in bytes |
| `totalTrafficConsumed` | Numeric String | Total traffic used in bytes |

---

# Applications Templates

## Splice.Amulet:FeaturedAppRight

**Purpose:** Grants featured status to an application

**Description:**
FeaturedAppRight contracts designate applications as "featured" on the network, typically granting enhanced visibility and potentially higher reward rates for facilitating transactions.

**JSON Structure:**
```json
{
  "dso": "<party_id>",
  "provider": "<party_id>"
}
```

---

## Splice.Amulet:FeaturedAppActivityMarker

**Purpose:** Tracks activity from featured applications

**Description:**
FeaturedAppActivityMarker contracts record activity from featured applications, used for calculating enhanced rewards.

**JSON Structure:**
```json
{
  "dso": "<party_id>",
  "provider": "<party_id>"
}
```

---

## Splice.AmuletAllocation:AmuletAllocation

**Purpose:** Represents allocated Amulet tokens

**Description:**
AmuletAllocation contracts represent Amulet tokens that have been allocated for specific purposes.

**JSON Structure:**
```json
{
  "dso": "<party_id>",
  "owner": "<party_id>",
  "amount": "1000000.0"
}
```

---

# Appendix: Common Field Types

## Party ID
A unique identifier for a party on the Canton Network. Format: `<namespace>::<fingerprint>`

Example: `DSO::1220abcd1234...`

## Contract ID
A unique identifier for a contract instance. Format: `00<hash>`

Example: `00abc123def456...`

## Round Number
Represents a mining round on the network. Rounds occur approximately every 10 minutes.

```json
{ "number": "79500" }
```

## Numeric Amounts
High-precision decimal numbers stored as strings to preserve precision.

Example: `"1000.0000000000"` (10 decimal places)

## Timestamps
ISO 8601 formatted timestamps in UTC.

Example: `"2026-01-21T20:00:00Z"`

---

*This documentation was auto-generated for Canton Network ACS data.*
*Template structures may vary between package versions.*
