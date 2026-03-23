// ─── SV Locking Data ──────────────────────────────────────────────────────────
//
// Maintained by Foundation staff. Update this file when SVs submit their
// PartyIDs, tier selection, and weight via sv@canton.foundation
//
// After editing, redeploy (or merge a PR) for changes to go live.
//
// Field reference:
//   svName            — Display name of the Super Validator
//   svWeight          — Base SV weight (from on-chain governance)
//   lifetimeEarned    — Total CC earned since network genesis (from dashboard)
//   lockedAmount      — Current total CC locked across ALL reported wallets
//   currentTier       — Tier the SV is currently credited with: "tier1" | "tier2" | "tier3" | "none"
//   impliedTier       — Tier implied by their current locked % (may differ if they're under threshold)
//   daysUnderThreshold — null if compliant; number of days since they dropped below their tier threshold
//   weightChangeProposalUrl — link to active on-chain proposal, if any
//   roundsUnderThreshold   — array of round numbers (last 35 days) when locked % dropped below threshold
//
//   wallets[].type    — "locking" = disclosed locking wallet, "unlocking" = disclosed unlocking wallet
//   wallets[].lockedAmount — current locked balance in that wallet (0 for unlocking wallets)
//
//   unlockTranches[]  — one entry per active unlock initiated by the SV
//     .initiatedDate      — ISO date string when unlock was initiated e.g. "2026-04-10"
//     .originalAmount     — CC amount when tranche was opened
//     .vestedAmount       — CC that has vested so far (liquid)
//     .remainingUnvested  — CC still locked in this tranche (= original - vested)
//
// ─────────────────────────────────────────────────────────────────────────────

import type { SVLockingRecord } from "@/pages/SVLocking";

// Last time Foundation staff updated this file.
// Update this timestamp whenever you make any changes.
export const LAST_UPDATED = "2026-04-01";

export const SV_LOCKING_DATA: SVLockingRecord[] = [
  // ── Example SV (remove and replace with real submissions) ──────────────────
  // {
  //   svName: "Example Validator",
  //   svWeight: 10,
  //   lifetimeEarned: 1_000_000,
  //   lockedAmount: 720_000,
  //   currentTier: "tier1",
  //   impliedTier: "tier1",
  //   wallets: [
  //     { partyId: "ExampleVault::abc123def456", lockedAmount: 720_000, type: "locking" },
  //   ],
  //   unlockTranches: [],
  //   daysUnderThreshold: null,
  //   roundsUnderThreshold: [],
  // },
];
