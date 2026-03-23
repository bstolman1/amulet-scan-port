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
  // ── Replace entries below with real SV submissions as they come in ──────────

  {
    svName: "Alpha Validator",
    svWeight: 10,
    lifetimeEarned: 1_000_000,
    lockedAmount: 720_000,
    currentTier: "tier1",
    impliedTier: "tier1",
    wallets: [
      { partyId: "AlphaVault-1::abc123def456", lockedAmount: 500_000, type: "locking" },
      { partyId: "AlphaVault-2::def456ghi789", lockedAmount: 220_000, type: "locking" },
    ],
    unlockTranches: [],
    daysUnderThreshold: null,
    roundsUnderThreshold: [],
  },

  {
    svName: "Beta Validator",
    svWeight: 8,
    lifetimeEarned: 800_000,
    lockedAmount: 380_000,
    currentTier: "tier1",
    impliedTier: "tier2",
    wallets: [
      { partyId: "BetaMain::gh789ij012jkl", lockedAmount: 380_000, type: "locking" },
      { partyId: "BetaUnlock::ij012kl345mno", lockedAmount: 0, type: "unlocking" },
    ],
    unlockTranches: [
      {
        initiatedDate: "2026-04-10",
        originalAmount: 120_000,
        vestedAmount: 10_000,
        remainingUnvested: 110_000,
      },
    ],
    daysUnderThreshold: 12,
    roundsUnderThreshold: [42, 43, 44],
    weightChangeProposalUrl: "#",
  },

  {
    svName: "Gamma Validator",
    svWeight: 6,
    lifetimeEarned: 500_000,
    lockedAmount: 180_000,
    currentTier: "tier3",
    impliedTier: "none",
    wallets: [
      { partyId: "GammaCustody::kl345mn678opq", lockedAmount: 180_000, type: "locking" },
    ],
    unlockTranches: [
      {
        initiatedDate: "2026-03-20",
        originalAmount: 200_000,
        vestedAmount: 60_000,
        remainingUnvested: 140_000,
      },
    ],
    daysUnderThreshold: 25,
    roundsUnderThreshold: [38, 39, 40, 41, 42, 43],
    weightChangeProposalUrl: "#",
  },
];
