import YAML from "yaml";

const CONFIG_URL =
  "https://raw.githubusercontent.com/global-synchronizer-foundation/configs/refs/heads/main/configs/MainNet/approved-sv-id-values.yaml";

export interface ConfigData {
  superValidators: {
    name: string;
    address: string;
    operatorName: string;
    weight: number;
    parentWeight: number;
    joinRound?: number | null;
    isGhost: boolean;
  }[];
  operators: {
    name: string;
    rewardWeightBps: number;
    joinRound?: number | null;
    extraBeneficiaries: {
      beneficiary: string;
      weight: number;
    }[];
  }[];
  totalRewardBps: number;
  lastUpdated: number;
}

export async function fetchConfigData(forceRefresh = false): Promise<ConfigData> {
  const cacheKey = "sv-config-cache-v3";

  // ─────────────────────────────
  // Cache
  // ─────────────────────────────
  if (!forceRefresh) {
    const cached = localStorage.getItem(cacheKey);
    if (cached) return JSON.parse(cached);
  }

  // ─────────────────────────────
  // Fetch YAML from GitHub
  // ─────────────────────────────
  const res = await fetch(CONFIG_URL);
  if (!res.ok) throw new Error("Failed to fetch config file from GitHub");
  const text = await res.text();
  const parsed = YAML.parse(text);

  const approved = parsed.approvedSvIdentities || [];

  // ─────────────────────────────
  // Build Operators and Flatten Beneficiaries
  // ─────────────────────────────
  const operators: ConfigData["operators"] = [];
  const flattened: ConfigData["superValidators"] = [];
  let totalRewardBps = 0;

  for (const sv of approved) {
    const operatorName = sv.name;
    const rewardWeightBps = Number(String(sv.rewardWeightBps).replace(/_/g, ""));
    totalRewardBps += rewardWeightBps;

    const extras = sv.extraBeneficiaries || [];
    const normalizedExtras = extras.map((ex: any) => ({
      beneficiary: ex.beneficiary,
      weight: Number(String(ex.weight).replace(/_/g, "")),
    }));

    // Save operator entry
    operators.push({
      name: operatorName,
      rewardWeightBps,
      joinRound: sv.joinRound ?? null,
      extraBeneficiaries: normalizedExtras,
    });

    // Flatten beneficiaries for UI tables
    for (const ex of normalizedExtras) {
      const [beneficiaryName, address] = ex.beneficiary.split("::");
      flattened.push({
        name: beneficiaryName,
        address: address || "",
        operatorName,
        weight: ex.weight,
        parentWeight: rewardWeightBps,
        joinRound: sv.joinRound ?? null,
        isGhost: beneficiaryName.toLowerCase().includes("ghost"),
      });
    }
  }

  // ─────────────────────────────
  // Compose Final Data
  // ─────────────────────────────
  const data: ConfigData = {
    superValidators: flattened,
    operators,
    totalRewardBps,
    lastUpdated: Date.now(),
  };

  // Cache locally
  localStorage.setItem(cacheKey, JSON.stringify(data));
  return data;
}

// ─────────────────────────────
// Schedule periodic background refresh
// ─────────────────────────────
export function scheduleDailySync() {
  const interval = setInterval(fetchConfigData, 24 * 60 * 60 * 1000);
  return () => clearInterval(interval);
}
