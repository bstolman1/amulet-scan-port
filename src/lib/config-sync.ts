import YAML from "yaml";

const CONFIG_URL =
  "https://raw.githubusercontent.com/global-synchronizer-foundation/configs/refs/heads/main/configs/MainNet/approved-sv-id-values.yaml";

export interface ConfigData {
  superValidators: {
    name: string;
    address: string;
    fullPartyId: string;
    operatorName: string;
    weight: number;
    parentWeight: number;
    joinRound?: number | null;
    isGhost: boolean;
    comment?: string;
  }[];
  operators: {
    name: string;
    rewardWeightBps: number;
    joinRound?: number | null;
    comment?: string;
    extraBeneficiaries: {
      beneficiary: string;
      weight: number;
      comment?: string;
    }[];
  }[];
  totalRewardBps: number;
  lastUpdated: number;
}

export async function fetchConfigData(forceRefresh = false): Promise<ConfigData> {
  const cacheKey = "sv-config-cache-v5";

  if (!forceRefresh) {
    const cached = localStorage.getItem(cacheKey);
    if (cached) return JSON.parse(cached);
  }

  const res = await fetch(CONFIG_URL);
  if (!res.ok) throw new Error("Failed to fetch config file from GitHub");
  const text = await res.text();

  // Extract inline comments before YAML parsing strips them.
  //
  // BUG FIX 2: The same party ID can appear multiple times with DIFFERENT
  // meanings (e.g. CoinMetrics' party ID is reused for Talos in CIP-0085).
  // A single Map<partyId, string> overwrites the first entry with the second.
  // Fix: store ALL comments in insertion order as Map<partyId, string[]>,
  // then pick comment[N] for the Nth occurrence of that party ID.
  const inlineComments = new Map<string, string[]>();
  for (const line of text.split('\n')) {
    const m = line.match(/beneficiary:\s*"([^"]+)"\s*#\s*(.+)/);
    if (m) {
      const [, partyId, comment] = m;
      if (!inlineComments.has(partyId)) inlineComments.set(partyId, []);
      inlineComments.get(partyId)!.push(comment.trim());
    }
  }

  const parsed = YAML.parse(text);
  const approved = parsed.approvedSvIdentities || [];

  const operators: ConfigData["operators"] = [];
  const flattened: ConfigData["superValidators"] = [];
  let totalRewardBps = 0;

  for (const sv of approved) {
    const operatorName = sv.name;
    const rewardWeightBps = Number(String(sv.rewardWeightBps).replace(/_/g, ""));
    totalRewardBps += rewardWeightBps;

    const extras = sv.extraBeneficiaries || [];

    // Per-operator counter so we pick the right comment for repeated party IDs.
    const seenCounts = new Map<string, number>();

    const normalizedExtras = extras.map((ex: any) => {
      // BUG FIX 1: Five-North-1's bsv-ghost-1 entry uses `rewardWeightBps`
      // instead of `weight`. Fall back to it when `weight` is absent.
      const rawWeight = ex.weight ?? ex.rewardWeightBps;
      const parsedWeight =
        rawWeight == null
          ? 0
          : Number(String(rawWeight).replace(/_/g, ""));

      const partyId = ex.beneficiary;
      const idx = seenCounts.get(partyId) ?? 0;
      seenCounts.set(partyId, idx + 1);

      const comment = (inlineComments.get(partyId) ?? [])[idx];

      return { beneficiary: partyId, weight: parsedWeight, comment };
    });

    operators.push({
      name: operatorName,
      rewardWeightBps,
      joinRound: sv.joinRound ?? null,
      comment: sv.comment || undefined,
      extraBeneficiaries: normalizedExtras,
    });

    for (const ex of normalizedExtras) {
      const [partyName, address] = ex.beneficiary.split("::");
      const isGhost = partyName.toLowerCase().includes("ghost");

      // Derive display name from comment only when it follows the pattern
      // "Friendly Name CIP-XXXX ..." — i.e. the comment starts with a name
      // followed by a CIP reference. This avoids using free-form comments
      // (like the GhostSV annotation) as display names.
      let displayName = partyName;
      if (ex.comment) {
        const cipMatch = ex.comment.match(/^(.+?)\s+CIP-\d+/);
        if (cipMatch) {
          // "Talos CIP-0085 # ..." → "Talos"
          displayName = cipMatch[1].trim();
        }
        // Otherwise keep partyName as-is (e.g. GhostSV-validator-1, GSF-SVRewards-1)
      }

      flattened.push({
        name: displayName,
        address: address || "",
        fullPartyId: ex.beneficiary,
        operatorName,
        weight: ex.weight,
        parentWeight: rewardWeightBps,
        joinRound: sv.joinRound ?? null,
        isGhost,
        comment: ex.comment || undefined,
      });
    }
  }

  const data: ConfigData = {
    superValidators: flattened,
    operators,
    totalRewardBps,
    lastUpdated: Date.now(),
  };

  localStorage.setItem(cacheKey, JSON.stringify(data));
  return data;
}

export function scheduleDailySync() {
  const interval = setInterval(fetchConfigData, 60 * 60 * 1000);
  return () => clearInterval(interval);
}
