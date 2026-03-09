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
import { useQuery } from "@tanstack/react-query";
import { scanApi, AnsEntry, Contract, ValidatorFaucetInfo, ContractWithState } from "@/lib/api-client";

/**
 * Hooks that use Canton Scan API directly instead of ACS aggregates.
 * These provide real-time data from the Canton network.
 */

// ============ ANS Entries ============
export function useAnsEntries(namePrefix?: string, pageSize: number = 1000) {
  if (stryMutAct_9fa48("1507")) {
    {}
  } else {
    stryCov_9fa48("1507");
    return useQuery({
      queryKey: stryMutAct_9fa48("1509") ? [] : (stryCov_9fa48("1509"), ["scan-api", "ans-entries", namePrefix, pageSize]),
      queryFn: async () => {
        if (stryMutAct_9fa48("1512")) {
          {}
        } else {
          stryCov_9fa48("1512");
          const response = await scanApi.fetchAnsEntries(namePrefix, pageSize);
          return stryMutAct_9fa48("1515") ? response.entries && [] : stryMutAct_9fa48("1514") ? false : stryMutAct_9fa48("1513") ? true : (stryCov_9fa48("1513", "1514", "1515"), response.entries || (stryMutAct_9fa48("1516") ? ["Stryker was here"] : (stryCov_9fa48("1516"), [])));
        }
      },
      staleTime: stryMutAct_9fa48("1517") ? 60 / 1000 : (stryCov_9fa48("1517"), 60 * 1000) // 1 minute
    });
  }
}
export function useAnsEntryByParty(party: string | undefined) {
  if (stryMutAct_9fa48("1518")) {
    {}
  } else {
    stryCov_9fa48("1518");
    return useQuery({
      queryKey: stryMutAct_9fa48("1520") ? [] : (stryCov_9fa48("1520"), ["scan-api", "ans-entry-by-party", party]),
      queryFn: async () => {
        if (stryMutAct_9fa48("1523")) {
          {}
        } else {
          stryCov_9fa48("1523");
          if (stryMutAct_9fa48("1526") ? false : stryMutAct_9fa48("1525") ? true : stryMutAct_9fa48("1524") ? party : (stryCov_9fa48("1524", "1525", "1526"), !party)) throw new Error("Party required");
          const response = await scanApi.fetchAnsEntryByParty(party);
          return response.entry;
        }
      },
      enabled: stryMutAct_9fa48("1528") ? !party : (stryCov_9fa48("1528"), !(stryMutAct_9fa48("1529") ? party : (stryCov_9fa48("1529"), !party))),
      staleTime: stryMutAct_9fa48("1530") ? 60 / 1000 : (stryCov_9fa48("1530"), 60 * 1000)
    });
  }
}
export function useAnsEntryByName(name: string | undefined) {
  if (stryMutAct_9fa48("1531")) {
    {}
  } else {
    stryCov_9fa48("1531");
    return useQuery({
      queryKey: stryMutAct_9fa48("1533") ? [] : (stryCov_9fa48("1533"), ["scan-api", "ans-entry-by-name", name]),
      queryFn: async () => {
        if (stryMutAct_9fa48("1536")) {
          {}
        } else {
          stryCov_9fa48("1536");
          if (stryMutAct_9fa48("1539") ? false : stryMutAct_9fa48("1538") ? true : stryMutAct_9fa48("1537") ? name : (stryCov_9fa48("1537", "1538", "1539"), !name)) throw new Error("Name required");
          const response = await scanApi.fetchAnsEntryByName(name);
          return response.entry;
        }
      },
      enabled: stryMutAct_9fa48("1541") ? !name : (stryCov_9fa48("1541"), !(stryMutAct_9fa48("1542") ? name : (stryCov_9fa48("1542"), !name))),
      staleTime: stryMutAct_9fa48("1543") ? 60 / 1000 : (stryCov_9fa48("1543"), 60 * 1000)
    });
  }
}

// ============ Featured Apps ============
export function useFeaturedApps() {
  if (stryMutAct_9fa48("1544")) {
    {}
  } else {
    stryCov_9fa48("1544");
    return useQuery({
      queryKey: stryMutAct_9fa48("1546") ? [] : (stryCov_9fa48("1546"), ["scan-api", "featured-apps"]),
      queryFn: async () => {
        if (stryMutAct_9fa48("1549")) {
          {}
        } else {
          stryCov_9fa48("1549");
          const response = await scanApi.fetchFeaturedApps();
          return stryMutAct_9fa48("1552") ? response.featured_apps && [] : stryMutAct_9fa48("1551") ? false : stryMutAct_9fa48("1550") ? true : (stryCov_9fa48("1550", "1551", "1552"), response.featured_apps || (stryMutAct_9fa48("1553") ? ["Stryker was here"] : (stryCov_9fa48("1553"), [])));
        }
      },
      staleTime: stryMutAct_9fa48("1554") ? 60 / 1000 : (stryCov_9fa48("1554"), 60 * 1000)
    });
  }
}
export function useFeaturedApp(providerPartyId: string | undefined) {
  if (stryMutAct_9fa48("1555")) {
    {}
  } else {
    stryCov_9fa48("1555");
    return useQuery({
      queryKey: stryMutAct_9fa48("1557") ? [] : (stryCov_9fa48("1557"), ["scan-api", "featured-app", providerPartyId]),
      queryFn: async () => {
        if (stryMutAct_9fa48("1560")) {
          {}
        } else {
          stryCov_9fa48("1560");
          if (stryMutAct_9fa48("1563") ? false : stryMutAct_9fa48("1562") ? true : stryMutAct_9fa48("1561") ? providerPartyId : (stryCov_9fa48("1561", "1562", "1563"), !providerPartyId)) throw new Error("Provider party ID required");
          const response = await scanApi.fetchFeaturedApp(providerPartyId);
          return response.featured_app_right;
        }
      },
      enabled: stryMutAct_9fa48("1565") ? !providerPartyId : (stryCov_9fa48("1565"), !(stryMutAct_9fa48("1566") ? providerPartyId : (stryCov_9fa48("1566"), !providerPartyId))),
      staleTime: stryMutAct_9fa48("1567") ? 60 / 1000 : (stryCov_9fa48("1567"), 60 * 1000)
    });
  }
}

// ============ Validator Licenses ============
export function useValidatorLicenses(limit: number = 1000) {
  if (stryMutAct_9fa48("1568")) {
    {}
  } else {
    stryCov_9fa48("1568");
    return useQuery({
      queryKey: stryMutAct_9fa48("1570") ? [] : (stryCov_9fa48("1570"), ["scan-api", "validator-licenses", limit]),
      queryFn: async () => {
        if (stryMutAct_9fa48("1573")) {
          {}
        } else {
          stryCov_9fa48("1573");
          // Fetch all pages
          const allLicenses: Contract[] = stryMutAct_9fa48("1574") ? ["Stryker was here"] : (stryCov_9fa48("1574"), []);
          let after: number | undefined;
          while (stryMutAct_9fa48("1576") ? false : stryMutAct_9fa48("1575") ? false : (stryCov_9fa48("1575", "1576"), true)) {
            if (stryMutAct_9fa48("1577")) {
              {}
            } else {
              stryCov_9fa48("1577");
              const response = await scanApi.fetchValidatorLicenses(after, limit);
              allLicenses.push(...(stryMutAct_9fa48("1580") ? response.validator_licenses && [] : stryMutAct_9fa48("1579") ? false : stryMutAct_9fa48("1578") ? true : (stryCov_9fa48("1578", "1579", "1580"), response.validator_licenses || (stryMutAct_9fa48("1581") ? ["Stryker was here"] : (stryCov_9fa48("1581"), [])))));
              if (stryMutAct_9fa48("1584") ? false : stryMutAct_9fa48("1583") ? true : stryMutAct_9fa48("1582") ? response.next_page_token : (stryCov_9fa48("1582", "1583", "1584"), !response.next_page_token)) break;
              after = response.next_page_token;
            }
          }
          return allLicenses;
        }
      },
      staleTime: stryMutAct_9fa48("1585") ? 60 / 1000 : (stryCov_9fa48("1585"), 60 * 1000)
    });
  }
}

// ============ Top Validators ============
export function useTopValidatorsByFaucets(limit: number = 1000) {
  if (stryMutAct_9fa48("1586")) {
    {}
  } else {
    stryCov_9fa48("1586");
    return useQuery({
      queryKey: stryMutAct_9fa48("1588") ? [] : (stryCov_9fa48("1588"), ["scan-api", "top-validators-by-faucets", limit]),
      queryFn: async () => {
        if (stryMutAct_9fa48("1591")) {
          {}
        } else {
          stryCov_9fa48("1591");
          const response = await scanApi.fetchTopValidatorsByFaucets(limit);
          return stryMutAct_9fa48("1594") ? response.validatorsByReceivedFaucets && [] : stryMutAct_9fa48("1593") ? false : stryMutAct_9fa48("1592") ? true : (stryCov_9fa48("1592", "1593", "1594"), response.validatorsByReceivedFaucets || (stryMutAct_9fa48("1595") ? ["Stryker was here"] : (stryCov_9fa48("1595"), [])));
        }
      },
      staleTime: stryMutAct_9fa48("1596") ? 60 / 1000 : (stryCov_9fa48("1596"), 60 * 1000)
    });
  }
}
export function useValidatorLiveness(validatorIds: string[]) {
  if (stryMutAct_9fa48("1597")) {
    {}
  } else {
    stryCov_9fa48("1597");
    return useQuery({
      queryKey: stryMutAct_9fa48("1599") ? [] : (stryCov_9fa48("1599"), ["scan-api", "validator-liveness", validatorIds]),
      queryFn: async () => {
        if (stryMutAct_9fa48("1602")) {
          {}
        } else {
          stryCov_9fa48("1602");
          if (stryMutAct_9fa48("1605") ? validatorIds.length !== 0 : stryMutAct_9fa48("1604") ? false : stryMutAct_9fa48("1603") ? true : (stryCov_9fa48("1603", "1604", "1605"), validatorIds.length === 0)) return {
            validatorsReceivedFaucets: stryMutAct_9fa48("1607") ? ["Stryker was here"] : (stryCov_9fa48("1607"), [])
          };
          const response = await scanApi.fetchValidatorLiveness(validatorIds);
          return response;
        }
      },
      enabled: stryMutAct_9fa48("1611") ? validatorIds.length <= 0 : stryMutAct_9fa48("1610") ? validatorIds.length >= 0 : stryMutAct_9fa48("1609") ? false : stryMutAct_9fa48("1608") ? true : (stryCov_9fa48("1608", "1609", "1610", "1611"), validatorIds.length > 0),
      staleTime: stryMutAct_9fa48("1612") ? 60 / 1000 : (stryCov_9fa48("1612"), 60 * 1000)
    });
  }
}

// ============ DSO Info ============
export function useDsoInfo() {
  if (stryMutAct_9fa48("1613")) {
    {}
  } else {
    stryCov_9fa48("1613");
    return useQuery({
      queryKey: stryMutAct_9fa48("1615") ? [] : (stryCov_9fa48("1615"), ["scan-api", "dso-info"]),
      queryFn: stryMutAct_9fa48("1618") ? () => undefined : (stryCov_9fa48("1618"), () => scanApi.fetchDsoInfo()),
      staleTime: stryMutAct_9fa48("1619") ? 60 / 1000 : (stryCov_9fa48("1619"), 60 * 1000)
    });
  }
}
export function useDsoPartyId() {
  if (stryMutAct_9fa48("1620")) {
    {}
  } else {
    stryCov_9fa48("1620");
    return useQuery({
      queryKey: stryMutAct_9fa48("1622") ? [] : (stryCov_9fa48("1622"), ["scan-api", "dso-party-id"]),
      queryFn: async () => {
        if (stryMutAct_9fa48("1625")) {
          {}
        } else {
          stryCov_9fa48("1625");
          const response = await scanApi.fetchDsoPartyId();
          return response.dso_party_id;
        }
      },
      staleTime: stryMutAct_9fa48("1626") ? 5 * 60 / 1000 : (stryCov_9fa48("1626"), (stryMutAct_9fa48("1627") ? 5 / 60 : (stryCov_9fa48("1627"), 5 * 60)) * 1000) // 5 minutes - rarely changes
    });
  }
}

// ============ Mining Rounds ============
export function useClosedRounds() {
  if (stryMutAct_9fa48("1628")) {
    {}
  } else {
    stryCov_9fa48("1628");
    return useQuery({
      queryKey: stryMutAct_9fa48("1630") ? [] : (stryCov_9fa48("1630"), ["scan-api", "closed-rounds"]),
      queryFn: async () => {
        if (stryMutAct_9fa48("1633")) {
          {}
        } else {
          stryCov_9fa48("1633");
          const response = await scanApi.fetchClosedRounds();
          return stryMutAct_9fa48("1636") ? response.rounds && [] : stryMutAct_9fa48("1635") ? false : stryMutAct_9fa48("1634") ? true : (stryCov_9fa48("1634", "1635", "1636"), response.rounds || (stryMutAct_9fa48("1637") ? ["Stryker was here"] : (stryCov_9fa48("1637"), [])));
        }
      },
      staleTime: stryMutAct_9fa48("1638") ? 30 / 1000 : (stryCov_9fa48("1638"), 30 * 1000) // 30 seconds
    });
  }
}
export function useOpenAndIssuingRounds() {
  if (stryMutAct_9fa48("1639")) {
    {}
  } else {
    stryCov_9fa48("1639");
    return useQuery({
      queryKey: stryMutAct_9fa48("1641") ? [] : (stryCov_9fa48("1641"), ["scan-api", "open-and-issuing-rounds"]),
      queryFn: async () => {
        if (stryMutAct_9fa48("1644")) {
          {}
        } else {
          stryCov_9fa48("1644");
          const response = await scanApi.fetchOpenAndIssuingRounds();
          return {
            openRounds: Object.values(stryMutAct_9fa48("1648") ? response.open_mining_rounds && {} : stryMutAct_9fa48("1647") ? false : stryMutAct_9fa48("1646") ? true : (stryCov_9fa48("1646", "1647", "1648"), response.open_mining_rounds || {})),
            issuingRounds: Object.values(stryMutAct_9fa48("1651") ? response.issuing_mining_rounds && {} : stryMutAct_9fa48("1650") ? false : stryMutAct_9fa48("1649") ? true : (stryCov_9fa48("1649", "1650", "1651"), response.issuing_mining_rounds || {})),
            ttl: response.time_to_live_in_microseconds
          };
        }
      },
      staleTime: stryMutAct_9fa48("1652") ? 30 / 1000 : (stryCov_9fa48("1652"), 30 * 1000)
    });
  }
}
export function useAllMiningRounds() {
  if (stryMutAct_9fa48("1653")) {
    {}
  } else {
    stryCov_9fa48("1653");
    return useQuery({
      queryKey: stryMutAct_9fa48("1655") ? [] : (stryCov_9fa48("1655"), ["scan-api", "all-mining-rounds"]),
      queryFn: stryMutAct_9fa48("1658") ? () => undefined : (stryCov_9fa48("1658"), () => scanApi.fetchAllMiningRoundsCurrent()),
      staleTime: stryMutAct_9fa48("1659") ? 30 / 1000 : (stryCov_9fa48("1659"), 30 * 1000)
    });
  }
}
export function useLatestRound() {
  if (stryMutAct_9fa48("1660")) {
    {}
  } else {
    stryCov_9fa48("1660");
    return useQuery({
      queryKey: stryMutAct_9fa48("1662") ? [] : (stryCov_9fa48("1662"), ["scan-api", "latest-round"]),
      queryFn: stryMutAct_9fa48("1665") ? () => undefined : (stryCov_9fa48("1665"), () => scanApi.fetchLatestRound()),
      staleTime: stryMutAct_9fa48("1666") ? 30 / 1000 : (stryCov_9fa48("1666"), 30 * 1000)
    });
  }
}

// ============ Scans & Sequencers ============
export function useScans() {
  if (stryMutAct_9fa48("1667")) {
    {}
  } else {
    stryCov_9fa48("1667");
    return useQuery({
      queryKey: stryMutAct_9fa48("1669") ? [] : (stryCov_9fa48("1669"), ["scan-api", "scans"]),
      queryFn: async () => {
        if (stryMutAct_9fa48("1672")) {
          {}
        } else {
          stryCov_9fa48("1672");
          const response = await scanApi.fetchScans();
          return stryMutAct_9fa48("1675") ? response.scans && [] : stryMutAct_9fa48("1674") ? false : stryMutAct_9fa48("1673") ? true : (stryCov_9fa48("1673", "1674", "1675"), response.scans || (stryMutAct_9fa48("1676") ? ["Stryker was here"] : (stryCov_9fa48("1676"), [])));
        }
      },
      staleTime: stryMutAct_9fa48("1677") ? 5 * 60 / 1000 : (stryCov_9fa48("1677"), (stryMutAct_9fa48("1678") ? 5 / 60 : (stryCov_9fa48("1678"), 5 * 60)) * 1000)
    });
  }
}
export function useDsoSequencers() {
  if (stryMutAct_9fa48("1679")) {
    {}
  } else {
    stryCov_9fa48("1679");
    return useQuery({
      queryKey: stryMutAct_9fa48("1681") ? [] : (stryCov_9fa48("1681"), ["scan-api", "dso-sequencers"]),
      queryFn: async () => {
        if (stryMutAct_9fa48("1684")) {
          {}
        } else {
          stryCov_9fa48("1684");
          const response = await scanApi.fetchDsoSequencers();
          return stryMutAct_9fa48("1687") ? response.domainSequencers && [] : stryMutAct_9fa48("1686") ? false : stryMutAct_9fa48("1685") ? true : (stryCov_9fa48("1685", "1686", "1687"), response.domainSequencers || (stryMutAct_9fa48("1688") ? ["Stryker was here"] : (stryCov_9fa48("1688"), [])));
        }
      },
      staleTime: stryMutAct_9fa48("1689") ? 5 * 60 / 1000 : (stryCov_9fa48("1689"), (stryMutAct_9fa48("1690") ? 5 / 60 : (stryCov_9fa48("1690"), 5 * 60)) * 1000)
    });
  }
}

// ============ Transfer APIs ============
export function useTransferPreapproval(party: string | undefined) {
  if (stryMutAct_9fa48("1691")) {
    {}
  } else {
    stryCov_9fa48("1691");
    return useQuery({
      queryKey: stryMutAct_9fa48("1693") ? [] : (stryCov_9fa48("1693"), ["scan-api", "transfer-preapproval", party]),
      queryFn: async () => {
        if (stryMutAct_9fa48("1696")) {
          {}
        } else {
          stryCov_9fa48("1696");
          if (stryMutAct_9fa48("1699") ? false : stryMutAct_9fa48("1698") ? true : stryMutAct_9fa48("1697") ? party : (stryCov_9fa48("1697", "1698", "1699"), !party)) throw new Error("Party required");
          try {
            if (stryMutAct_9fa48("1701")) {
              {}
            } else {
              stryCov_9fa48("1701");
              const response = await scanApi.fetchTransferPreapprovalByParty(party);
              return response.transfer_preapproval;
            }
          } catch {
            if (stryMutAct_9fa48("1702")) {
              {}
            } else {
              stryCov_9fa48("1702");
              return null;
            }
          }
        }
      },
      enabled: stryMutAct_9fa48("1703") ? !party : (stryCov_9fa48("1703"), !(stryMutAct_9fa48("1704") ? party : (stryCov_9fa48("1704"), !party))),
      staleTime: stryMutAct_9fa48("1705") ? 60 / 1000 : (stryCov_9fa48("1705"), 60 * 1000)
    });
  }
}
export function useTransferCommandCounter(party: string | undefined) {
  if (stryMutAct_9fa48("1706")) {
    {}
  } else {
    stryCov_9fa48("1706");
    return useQuery({
      queryKey: stryMutAct_9fa48("1708") ? [] : (stryCov_9fa48("1708"), ["scan-api", "transfer-command-counter", party]),
      queryFn: async () => {
        if (stryMutAct_9fa48("1711")) {
          {}
        } else {
          stryCov_9fa48("1711");
          if (stryMutAct_9fa48("1714") ? false : stryMutAct_9fa48("1713") ? true : stryMutAct_9fa48("1712") ? party : (stryCov_9fa48("1712", "1713", "1714"), !party)) throw new Error("Party required");
          try {
            if (stryMutAct_9fa48("1716")) {
              {}
            } else {
              stryCov_9fa48("1716");
              const response = await scanApi.fetchTransferCommandCounter(party);
              return response.transfer_command_counter;
            }
          } catch {
            if (stryMutAct_9fa48("1717")) {
              {}
            } else {
              stryCov_9fa48("1717");
              return null;
            }
          }
        }
      },
      enabled: stryMutAct_9fa48("1718") ? !party : (stryCov_9fa48("1718"), !(stryMutAct_9fa48("1719") ? party : (stryCov_9fa48("1719"), !party))),
      staleTime: stryMutAct_9fa48("1720") ? 60 / 1000 : (stryCov_9fa48("1720"), 60 * 1000)
    });
  }
}

// ============ Governance ============
export function useActiveVoteRequests() {
  if (stryMutAct_9fa48("1721")) {
    {}
  } else {
    stryCov_9fa48("1721");
    return useQuery({
      queryKey: stryMutAct_9fa48("1723") ? [] : (stryCov_9fa48("1723"), ["scan-api", "active-vote-requests"]),
      queryFn: async () => {
        if (stryMutAct_9fa48("1726")) {
          {}
        } else {
          stryCov_9fa48("1726");
          const response = await fetch(`${stryMutAct_9fa48("1730") ? import.meta.env.VITE_SCAN_API_URL && "https://scan.sv-1.global.canton.network.sync.global/api/scan" : stryMutAct_9fa48("1729") ? false : stryMutAct_9fa48("1728") ? true : (stryCov_9fa48("1728", "1729", "1730"), import.meta.env.VITE_SCAN_API_URL || "https://scan.sv-1.global.canton.network.sync.global/api/scan")}/v0/admin/sv/voterequests`, {
            mode: "cors"
          });
          if (stryMutAct_9fa48("1736") ? false : stryMutAct_9fa48("1735") ? true : stryMutAct_9fa48("1734") ? response.ok : (stryCov_9fa48("1734", "1735", "1736"), !response.ok)) throw new Error("Failed to fetch vote requests");
          const data = await response.json();
          return stryMutAct_9fa48("1740") ? data.dso_rules_vote_requests && [] : stryMutAct_9fa48("1739") ? false : stryMutAct_9fa48("1738") ? true : (stryCov_9fa48("1738", "1739", "1740"), data.dso_rules_vote_requests || (stryMutAct_9fa48("1741") ? ["Stryker was here"] : (stryCov_9fa48("1741"), [])));
        }
      },
      staleTime: stryMutAct_9fa48("1742") ? 30 / 1000 : (stryCov_9fa48("1742"), 30 * 1000)
    });
  }
}
export interface VoteResultsRequest {
  actionName?: string;
  accepted?: boolean;
  requester?: string;
  effectiveFrom?: string;
  effectiveTo?: string;
  limit?: number;
}
export function useVoteResults(request: VoteResultsRequest = {}) {
  if (stryMutAct_9fa48("1743")) {
    {}
  } else {
    stryCov_9fa48("1743");
    return useQuery({
      queryKey: stryMutAct_9fa48("1745") ? [] : (stryCov_9fa48("1745"), ["scan-api", "vote-results", request]),
      queryFn: async () => {
        if (stryMutAct_9fa48("1748")) {
          {}
        } else {
          stryCov_9fa48("1748");
          const response = await fetch(`${stryMutAct_9fa48("1752") ? import.meta.env.VITE_SCAN_API_URL && "https://scan.sv-1.global.canton.network.sync.global/api/scan" : stryMutAct_9fa48("1751") ? false : stryMutAct_9fa48("1750") ? true : (stryCov_9fa48("1750", "1751", "1752"), import.meta.env.VITE_SCAN_API_URL || "https://scan.sv-1.global.canton.network.sync.global/api/scan")}/v0/admin/sv/voteresults`, {
            method: "POST",
            headers: {
              "Content-Type": "application/json"
            },
            body: JSON.stringify(request),
            mode: "cors"
          });
          if (stryMutAct_9fa48("1761") ? false : stryMutAct_9fa48("1760") ? true : stryMutAct_9fa48("1759") ? response.ok : (stryCov_9fa48("1759", "1760", "1761"), !response.ok)) throw new Error("Failed to fetch vote results");
          const data = await response.json();
          return stryMutAct_9fa48("1765") ? data.dso_rules_vote_results && [] : stryMutAct_9fa48("1764") ? false : stryMutAct_9fa48("1763") ? true : (stryCov_9fa48("1763", "1764", "1765"), data.dso_rules_vote_results || (stryMutAct_9fa48("1766") ? ["Stryker was here"] : (stryCov_9fa48("1766"), [])));
        }
      },
      staleTime: stryMutAct_9fa48("1767") ? 60 / 1000 : (stryCov_9fa48("1767"), 60 * 1000)
    });
  }
}

// ============ Network Info ============
export function useSpliceInstanceNames() {
  if (stryMutAct_9fa48("1768")) {
    {}
  } else {
    stryCov_9fa48("1768");
    return useQuery({
      queryKey: stryMutAct_9fa48("1770") ? [] : (stryCov_9fa48("1770"), ["scan-api", "splice-instance-names"]),
      queryFn: stryMutAct_9fa48("1773") ? () => undefined : (stryCov_9fa48("1773"), () => scanApi.fetchSpliceInstanceNames()),
      staleTime: stryMutAct_9fa48("1774") ? 5 * 60 / 1000 : (stryCov_9fa48("1774"), (stryMutAct_9fa48("1775") ? 5 / 60 : (stryCov_9fa48("1775"), 5 * 60)) * 1000)
    });
  }
}
export function useMigrationSchedule() {
  if (stryMutAct_9fa48("1776")) {
    {}
  } else {
    stryCov_9fa48("1776");
    return useQuery({
      queryKey: stryMutAct_9fa48("1778") ? [] : (stryCov_9fa48("1778"), ["scan-api", "migration-schedule"]),
      queryFn: async () => {
        if (stryMutAct_9fa48("1781")) {
          {}
        } else {
          stryCov_9fa48("1781");
          try {
            if (stryMutAct_9fa48("1782")) {
              {}
            } else {
              stryCov_9fa48("1782");
              return await scanApi.fetchMigrationSchedule();
            }
          } catch {
            if (stryMutAct_9fa48("1783")) {
              {}
            } else {
              stryCov_9fa48("1783");
              return null; // No migration scheduled
            }
          }
        }
      },
      staleTime: stryMutAct_9fa48("1784") ? 5 * 60 / 1000 : (stryCov_9fa48("1784"), (stryMutAct_9fa48("1785") ? 5 / 60 : (stryCov_9fa48("1785"), 5 * 60)) * 1000)
    });
  }
}

// ============ ACS State (for data that doesn't have direct endpoints) ============
export function useStateAcs(templates: string[], pageSize: number = 1000) {
  if (stryMutAct_9fa48("1786")) {
    {}
  } else {
    stryCov_9fa48("1786");
    return useQuery({
      queryKey: stryMutAct_9fa48("1788") ? [] : (stryCov_9fa48("1788"), ["scan-api", "state-acs", templates, pageSize]),
      queryFn: async () => {
        if (stryMutAct_9fa48("1791")) {
          {}
        } else {
          stryCov_9fa48("1791");
          const latest = await scanApi.fetchLatestRound();
          const snap = await scanApi.fetchAcsSnapshotTimestamp(latest.effectiveAt, 0);
          const response = await scanApi.fetchStateAcs({
            migration_id: 0,
            record_time: snap.record_time,
            page_size: pageSize,
            templates
          });
          return stryMutAct_9fa48("1795") ? response.created_events && [] : stryMutAct_9fa48("1794") ? false : stryMutAct_9fa48("1793") ? true : (stryCov_9fa48("1793", "1794", "1795"), response.created_events || (stryMutAct_9fa48("1796") ? ["Stryker was here"] : (stryCov_9fa48("1796"), [])));
        }
      },
      staleTime: stryMutAct_9fa48("1797") ? 60 / 1000 : (stryCov_9fa48("1797"), 60 * 1000),
      enabled: stryMutAct_9fa48("1801") ? templates.length <= 0 : stryMutAct_9fa48("1800") ? templates.length >= 0 : stryMutAct_9fa48("1799") ? false : stryMutAct_9fa48("1798") ? true : (stryCov_9fa48("1798", "1799", "1800", "1801"), templates.length > 0)
    });
  }
}

// ============ Holdings ============
export function useHoldingsSummary(partyIds: string[], asOfRound?: number) {
  if (stryMutAct_9fa48("1802")) {
    {}
  } else {
    stryCov_9fa48("1802");
    return useQuery({
      queryKey: stryMutAct_9fa48("1804") ? [] : (stryCov_9fa48("1804"), ["scan-api", "holdings-summary", partyIds, asOfRound]),
      queryFn: async () => {
        if (stryMutAct_9fa48("1807")) {
          {}
        } else {
          stryCov_9fa48("1807");
          if (stryMutAct_9fa48("1810") ? partyIds.length !== 0 : stryMutAct_9fa48("1809") ? false : stryMutAct_9fa48("1808") ? true : (stryCov_9fa48("1808", "1809", "1810"), partyIds.length === 0)) return {
            summaries: stryMutAct_9fa48("1812") ? ["Stryker was here"] : (stryCov_9fa48("1812"), [])
          };
          const latest = await scanApi.fetchLatestRound();
          const snap = await scanApi.fetchAcsSnapshotTimestamp(latest.effectiveAt, 0);
          const response = await scanApi.fetchHoldingsSummary({
            migration_id: 0,
            record_time: snap.record_time,
            owner_party_ids: partyIds,
            as_of_round: asOfRound
          });
          return response;
        }
      },
      enabled: stryMutAct_9fa48("1817") ? partyIds.length <= 0 : stryMutAct_9fa48("1816") ? partyIds.length >= 0 : stryMutAct_9fa48("1815") ? false : stryMutAct_9fa48("1814") ? true : (stryCov_9fa48("1814", "1815", "1816", "1817"), partyIds.length > 0),
      staleTime: stryMutAct_9fa48("1818") ? 60 / 1000 : (stryCov_9fa48("1818"), 60 * 1000)
    });
  }
}

// ============ Amulet Rules ============
export function useAmuletRules() {
  if (stryMutAct_9fa48("1819")) {
    {}
  } else {
    stryCov_9fa48("1819");
    return useQuery({
      queryKey: stryMutAct_9fa48("1821") ? [] : (stryCov_9fa48("1821"), ["scan-api", "amulet-rules"]),
      queryFn: async () => {
        if (stryMutAct_9fa48("1824")) {
          {}
        } else {
          stryCov_9fa48("1824");
          const dsoInfo = await scanApi.fetchDsoInfo();
          return dsoInfo.amulet_rules;
        }
      },
      staleTime: stryMutAct_9fa48("1825") ? 60 / 1000 : (stryCov_9fa48("1825"), 60 * 1000)
    });
  }
}
export function useExternalPartyAmuletRules() {
  if (stryMutAct_9fa48("1826")) {
    {}
  } else {
    stryCov_9fa48("1826");
    return useQuery({
      queryKey: stryMutAct_9fa48("1828") ? [] : (stryCov_9fa48("1828"), ["scan-api", "external-party-amulet-rules"]),
      queryFn: async () => {
        if (stryMutAct_9fa48("1831")) {
          {}
        } else {
          stryCov_9fa48("1831");
          const response = await fetch(`${stryMutAct_9fa48("1835") ? import.meta.env.VITE_SCAN_API_URL && "https://scan.sv-1.global.canton.network.sync.global/api/scan" : stryMutAct_9fa48("1834") ? false : stryMutAct_9fa48("1833") ? true : (stryCov_9fa48("1833", "1834", "1835"), import.meta.env.VITE_SCAN_API_URL || "https://scan.sv-1.global.canton.network.sync.global/api/scan")}/v0/external-party-amulet-rules`, {
            method: "POST",
            headers: {
              "Content-Type": "application/json"
            },
            body: JSON.stringify({}),
            mode: "cors"
          });
          if (stryMutAct_9fa48("1844") ? false : stryMutAct_9fa48("1843") ? true : stryMutAct_9fa48("1842") ? response.ok : (stryCov_9fa48("1842", "1843", "1844"), !response.ok)) throw new Error("Failed to fetch external party amulet rules");
          const data = await response.json();
          return data.external_party_amulet_rules_update;
        }
      },
      staleTime: stryMutAct_9fa48("1846") ? 60 / 1000 : (stryCov_9fa48("1846"), 60 * 1000)
    });
  }
}

// ============ DSO State - SV Nodes ============
export function useSvNodeStates() {
  if (stryMutAct_9fa48("1847")) {
    {}
  } else {
    stryCov_9fa48("1847");
    return useQuery({
      queryKey: stryMutAct_9fa48("1849") ? [] : (stryCov_9fa48("1849"), ["scan-api", "sv-node-states"]),
      queryFn: async () => {
        if (stryMutAct_9fa48("1852")) {
          {}
        } else {
          stryCov_9fa48("1852");
          const dsoInfo = await scanApi.fetchDsoInfo();
          return stryMutAct_9fa48("1855") ? dsoInfo.sv_node_states && [] : stryMutAct_9fa48("1854") ? false : stryMutAct_9fa48("1853") ? true : (stryCov_9fa48("1853", "1854", "1855"), dsoInfo.sv_node_states || (stryMutAct_9fa48("1856") ? ["Stryker was here"] : (stryCov_9fa48("1856"), [])));
        }
      },
      staleTime: stryMutAct_9fa48("1857") ? 60 / 1000 : (stryCov_9fa48("1857"), 60 * 1000)
    });
  }
}
export function useDsoRules() {
  if (stryMutAct_9fa48("1858")) {
    {}
  } else {
    stryCov_9fa48("1858");
    return useQuery({
      queryKey: stryMutAct_9fa48("1860") ? [] : (stryCov_9fa48("1860"), ["scan-api", "dso-rules"]),
      queryFn: async () => {
        if (stryMutAct_9fa48("1863")) {
          {}
        } else {
          stryCov_9fa48("1863");
          const dsoInfo = await scanApi.fetchDsoInfo();
          return dsoInfo.dso_rules;
        }
      },
      staleTime: stryMutAct_9fa48("1864") ? 60 / 1000 : (stryCov_9fa48("1864"), 60 * 1000)
    });
  }
}

// ============ Traffic Status ============
export function useTrafficStatus(domainId: string | undefined, memberId: string | undefined) {
  if (stryMutAct_9fa48("1865")) {
    {}
  } else {
    stryCov_9fa48("1865");
    return useQuery({
      queryKey: stryMutAct_9fa48("1867") ? [] : (stryCov_9fa48("1867"), ["scan-api", "traffic-status", domainId, memberId]),
      queryFn: async () => {
        if (stryMutAct_9fa48("1870")) {
          {}
        } else {
          stryCov_9fa48("1870");
          if (stryMutAct_9fa48("1873") ? !domainId && !memberId : stryMutAct_9fa48("1872") ? false : stryMutAct_9fa48("1871") ? true : (stryCov_9fa48("1871", "1872", "1873"), (stryMutAct_9fa48("1874") ? domainId : (stryCov_9fa48("1874"), !domainId)) || (stryMutAct_9fa48("1875") ? memberId : (stryCov_9fa48("1875"), !memberId)))) throw new Error("Domain ID and Member ID required");
          return scanApi.fetchTrafficStatus(domainId, memberId);
        }
      },
      enabled: stryMutAct_9fa48("1879") ? !!domainId || !!memberId : stryMutAct_9fa48("1878") ? false : stryMutAct_9fa48("1877") ? true : (stryCov_9fa48("1877", "1878", "1879"), (stryMutAct_9fa48("1880") ? !domainId : (stryCov_9fa48("1880"), !(stryMutAct_9fa48("1881") ? domainId : (stryCov_9fa48("1881"), !domainId)))) && (stryMutAct_9fa48("1882") ? !memberId : (stryCov_9fa48("1882"), !(stryMutAct_9fa48("1883") ? memberId : (stryCov_9fa48("1883"), !memberId))))),
      staleTime: stryMutAct_9fa48("1884") ? 30 / 1000 : (stryCov_9fa48("1884"), 30 * 1000)
    });
  }
}