// @ts-nocheck
function stryNS_9fa48() {
  var g = typeof globalThis === 'object' && globalThis && globalThis.Math === Math && globalThis || new Function("return this")();
  var ns = g.__stryker__ || (g.__stryker__ = {});
  if (ns.activeMutant === undefined && g.process && g.process.env && g.process.env.__STRYKER_ACTIVE_MUTANT__) {
    ns.activeMutant = g.process.env.__STRYKER_ACTIVE_MUTANT__;
  }
  function retrieveNS() {
    return ns;
  }
  stryNS_9fa48 = retrieveNS;
  return retrieveNS();
}
stryNS_9fa48();
function stryCov_9fa48() {
  var ns = stryNS_9fa48();
  var cov = ns.mutantCoverage || (ns.mutantCoverage = {
    static: {},
    perTest: {}
  });
  function cover() {
    var c = cov.static;
    if (ns.currentTestId) {
      c = cov.perTest[ns.currentTestId] = cov.perTest[ns.currentTestId] || {};
    }
    var a = arguments;
    for (var i = 0; i < a.length; i++) {
      c[a[i]] = (c[a[i]] || 0) + 1;
    }
  }
  stryCov_9fa48 = cover;
  cover.apply(null, arguments);
}
function stryMutAct_9fa48(id) {
  var ns = stryNS_9fa48();
  function isActive(id) {
    if (ns.activeMutant === id) {
      if (ns.hitCount !== void 0 && ++ns.hitCount > ns.hitLimit) {
        throw new Error('Stryker: Hit count limit reached (' + ns.hitCount + ')');
      }
      return true;
    }
    return false;
  }
  stryMutAct_9fa48 = isActive;
  return isActive(id);
}
import YAML from "yaml";
const CONFIG_URL = "https://raw.githubusercontent.com/global-synchronizer-foundation/configs/refs/heads/main/configs/MainNet/approved-sv-id-values.yaml";
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
export async function fetchConfigData(forceRefresh = stryMutAct_9fa48("4312") ? true : (stryCov_9fa48("4312"), false)): Promise<ConfigData> {
  if (stryMutAct_9fa48("4313")) {
    {}
  } else {
    stryCov_9fa48("4313");
    const cacheKey = "sv-config-cache-v5";

    // ─────────────────────────────
    // Cache
    // ─────────────────────────────
    if (stryMutAct_9fa48("4317") ? false : stryMutAct_9fa48("4316") ? true : stryMutAct_9fa48("4315") ? forceRefresh : (stryCov_9fa48("4315", "4316", "4317"), !forceRefresh)) {
      if (stryMutAct_9fa48("4318")) {
        {}
      } else {
        stryCov_9fa48("4318");
        const cached = localStorage.getItem(cacheKey);
        if (stryMutAct_9fa48("4320") ? false : stryMutAct_9fa48("4319") ? true : (stryCov_9fa48("4319", "4320"), cached)) return JSON.parse(cached);
      }
    }

    // ─────────────────────────────
    // Fetch YAML from GitHub
    // ─────────────────────────────
    const res = await fetch(CONFIG_URL);
    if (stryMutAct_9fa48("4323") ? false : stryMutAct_9fa48("4322") ? true : stryMutAct_9fa48("4321") ? res.ok : (stryCov_9fa48("4321", "4322", "4323"), !res.ok)) throw new Error("Failed to fetch config file from GitHub");
    const text = await res.text();

    // Extract inline comments (# ...) from raw YAML before parsing
    const inlineComments = new Map<string, string>();
    const lines = text.split('\n');
    for (const line of lines) {
      if (stryMutAct_9fa48("4326")) {
        {}
      } else {
        stryCov_9fa48("4326");
        const beneficiaryMatch = line.match(stryMutAct_9fa48("4335") ? /beneficiary:\s*"([^"]+)"\s*#\s*(.)/ : stryMutAct_9fa48("4334") ? /beneficiary:\s*"([^"]+)"\s*#\S*(.+)/ : stryMutAct_9fa48("4333") ? /beneficiary:\s*"([^"]+)"\s*#\s(.+)/ : stryMutAct_9fa48("4332") ? /beneficiary:\s*"([^"]+)"\S*#\s*(.+)/ : stryMutAct_9fa48("4331") ? /beneficiary:\s*"([^"]+)"\s#\s*(.+)/ : stryMutAct_9fa48("4330") ? /beneficiary:\s*"(["]+)"\s*#\s*(.+)/ : stryMutAct_9fa48("4329") ? /beneficiary:\s*"([^"])"\s*#\s*(.+)/ : stryMutAct_9fa48("4328") ? /beneficiary:\S*"([^"]+)"\s*#\s*(.+)/ : stryMutAct_9fa48("4327") ? /beneficiary:\s"([^"]+)"\s*#\s*(.+)/ : (stryCov_9fa48("4327", "4328", "4329", "4330", "4331", "4332", "4333", "4334", "4335"), /beneficiary:\s*"([^"]+)"\s*#\s*(.+)/));
        if (stryMutAct_9fa48("4337") ? false : stryMutAct_9fa48("4336") ? true : (stryCov_9fa48("4336", "4337"), beneficiaryMatch)) {
          if (stryMutAct_9fa48("4338")) {
            {}
          } else {
            stryCov_9fa48("4338");
            inlineComments.set(beneficiaryMatch[1], stryMutAct_9fa48("4339") ? beneficiaryMatch[2] : (stryCov_9fa48("4339"), beneficiaryMatch[2].trim()));
          }
        }
      }
    }
    const parsed = YAML.parse(text);
    const approved = stryMutAct_9fa48("4342") ? parsed.approvedSvIdentities && [] : stryMutAct_9fa48("4341") ? false : stryMutAct_9fa48("4340") ? true : (stryCov_9fa48("4340", "4341", "4342"), parsed.approvedSvIdentities || (stryMutAct_9fa48("4343") ? ["Stryker was here"] : (stryCov_9fa48("4343"), [])));

    // ─────────────────────────────
    // Build Operators and Flatten Beneficiaries
    // ─────────────────────────────
    const operators: ConfigData["operators"] = stryMutAct_9fa48("4344") ? ["Stryker was here"] : (stryCov_9fa48("4344"), []);
    const flattened: ConfigData["superValidators"] = stryMutAct_9fa48("4345") ? ["Stryker was here"] : (stryCov_9fa48("4345"), []);
    let totalRewardBps = 0;
    for (const sv of approved) {
      if (stryMutAct_9fa48("4346")) {
        {}
      } else {
        stryCov_9fa48("4346");
        const operatorName = sv.name;
        const rewardWeightBps = Number(String(sv.rewardWeightBps).replace(/_/g, ""));
        stryMutAct_9fa48("4348") ? totalRewardBps -= rewardWeightBps : (stryCov_9fa48("4348"), totalRewardBps += rewardWeightBps);
        const operatorComment = stryMutAct_9fa48("4351") ? sv.comment && undefined : stryMutAct_9fa48("4350") ? false : stryMutAct_9fa48("4349") ? true : (stryCov_9fa48("4349", "4350", "4351"), sv.comment || undefined);
        const extras = stryMutAct_9fa48("4354") ? sv.extraBeneficiaries && [] : stryMutAct_9fa48("4353") ? false : stryMutAct_9fa48("4352") ? true : (stryCov_9fa48("4352", "4353", "4354"), sv.extraBeneficiaries || (stryMutAct_9fa48("4355") ? ["Stryker was here"] : (stryCov_9fa48("4355"), [])));
        const normalizedExtras = extras.map(stryMutAct_9fa48("4356") ? () => undefined : (stryCov_9fa48("4356"), (ex: any) => ({
          beneficiary: ex.beneficiary,
          weight: Number(String(ex.weight).replace(/_/g, "")),
          comment: stryMutAct_9fa48("4361") ? inlineComments.get(ex.beneficiary) && undefined : stryMutAct_9fa48("4360") ? false : stryMutAct_9fa48("4359") ? true : (stryCov_9fa48("4359", "4360", "4361"), inlineComments.get(ex.beneficiary) || undefined)
        })));

        // Save operator entry
        operators.push({
          name: operatorName,
          rewardWeightBps,
          joinRound: stryMutAct_9fa48("4363") ? sv.joinRound && null : (stryCov_9fa48("4363"), sv.joinRound ?? null),
          comment: operatorComment,
          extraBeneficiaries: normalizedExtras
        });

        // Flatten beneficiaries for UI tables
        for (const ex of normalizedExtras) {
          if (stryMutAct_9fa48("4364")) {
            {}
          } else {
            stryCov_9fa48("4364");
            const fullPartyId = ex.beneficiary;
            const [beneficiaryName, address] = ex.beneficiary.split("::");
            flattened.push({
              name: beneficiaryName,
              address: stryMutAct_9fa48("4369") ? address && "" : stryMutAct_9fa48("4368") ? false : stryMutAct_9fa48("4367") ? true : (stryCov_9fa48("4367", "4368", "4369"), address || ""),
              fullPartyId,
              operatorName,
              weight: ex.weight,
              parentWeight: rewardWeightBps,
              joinRound: stryMutAct_9fa48("4371") ? sv.joinRound && null : (stryCov_9fa48("4371"), sv.joinRound ?? null),
              isGhost: stryMutAct_9fa48("4372") ? beneficiaryName.toUpperCase().includes("ghost") : (stryCov_9fa48("4372"), beneficiaryName.toLowerCase().includes("ghost")),
              comment: stryMutAct_9fa48("4376") ? ex.comment && undefined : stryMutAct_9fa48("4375") ? false : stryMutAct_9fa48("4374") ? true : (stryCov_9fa48("4374", "4375", "4376"), ex.comment || undefined)
            });
          }
        }
      }
    }

    // ─────────────────────────────
    // Compose Final Data
    // ─────────────────────────────
    const data: ConfigData = {
      superValidators: flattened,
      operators,
      totalRewardBps,
      lastUpdated: Date.now()
    };

    // Cache locally
    localStorage.setItem(cacheKey, JSON.stringify(data));
    return data;
  }
}

// ─────────────────────────────
// Schedule periodic background refresh
// ─────────────────────────────
export function scheduleDailySync() {
  if (stryMutAct_9fa48("4378")) {
    {}
  } else {
    stryCov_9fa48("4378");
    const interval = setInterval(fetchConfigData, stryMutAct_9fa48("4379") ? 24 * 60 * 60 / 1000 : (stryCov_9fa48("4379"), (stryMutAct_9fa48("4380") ? 24 * 60 / 60 : (stryCov_9fa48("4380"), (stryMutAct_9fa48("4381") ? 24 / 60 : (stryCov_9fa48("4381"), 24 * 60)) * 60)) * 1000));
    return stryMutAct_9fa48("4382") ? () => undefined : (stryCov_9fa48("4382"), () => clearInterval(interval));
  }
}