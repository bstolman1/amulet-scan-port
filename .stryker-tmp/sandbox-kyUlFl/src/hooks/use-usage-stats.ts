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
import { scanApi, TransactionHistoryItem } from "@/lib/api-client";
export type UsageCharts = {
  cumulativeParties: {
    date: string;
    parties: number;
  }[];
  dailyActiveUsers: {
    date: string;
    daily: number;
    avg7d: number;
  }[];
  dailyTransactions: {
    date: string;
    daily: number;
    avg7d: number;
  }[];
  // helpful rollups for empty/error UI states
  totalParties: number;
  totalDailyUsers: number;
  totalTransactions: number;
};
function toDateKey(dateStr: string | Date): string {
  if (stryMutAct_9fa48("3356")) {
    {}
  } else {
    stryCov_9fa48("3356");
    const d = (stryMutAct_9fa48("3359") ? typeof dateStr !== "string" : stryMutAct_9fa48("3358") ? false : stryMutAct_9fa48("3357") ? true : (stryCov_9fa48("3357", "3358", "3359"), typeof dateStr === "string")) ? new Date(dateStr) : dateStr;
    const year = d.getFullYear();
    const month = String(stryMutAct_9fa48("3361") ? d.getMonth() - 1 : (stryCov_9fa48("3361"), d.getMonth() + 1)).padStart(2, "0");
    const day = String(d.getDate()).padStart(2, "0");
    return `${year}-${month}-${day}`;
  }
}
function extractParties(tx: TransactionHistoryItem): string[] {
  if (stryMutAct_9fa48("3365")) {
    {}
  } else {
    stryCov_9fa48("3365");
    // Count only parties that SENT tokens (positive-value transfers) on this day
    const t = tx as any;
    if (stryMutAct_9fa48("3368") ? !t.transfer && !t.transfer.sender?.party : stryMutAct_9fa48("3367") ? false : stryMutAct_9fa48("3366") ? true : (stryCov_9fa48("3366", "3367", "3368"), (stryMutAct_9fa48("3369") ? t.transfer : (stryCov_9fa48("3369"), !t.transfer)) || (stryMutAct_9fa48("3370") ? t.transfer.sender?.party : (stryCov_9fa48("3370"), !(stryMutAct_9fa48("3371") ? t.transfer.sender.party : (stryCov_9fa48("3371"), t.transfer.sender?.party)))))) return stryMutAct_9fa48("3372") ? ["Stryker was here"] : (stryCov_9fa48("3372"), []);
    const totalSent = (Array.isArray(t.transfer.receivers) ? t.transfer.receivers : stryMutAct_9fa48("3373") ? ["Stryker was here"] : (stryCov_9fa48("3373"), [])).reduce((sum: number, r: any) => {
      if (stryMutAct_9fa48("3374")) {
        {}
      } else {
        stryCov_9fa48("3374");
        const n = parseFloat(stryMutAct_9fa48("3375") ? r?.amount && "0" : (stryCov_9fa48("3375"), (stryMutAct_9fa48("3376") ? r.amount : (stryCov_9fa48("3376"), r?.amount)) ?? "0"));
        return stryMutAct_9fa48("3378") ? sum - (isNaN(n) ? 0 : n) : (stryCov_9fa48("3378"), sum + (isNaN(n) ? 0 : n));
      }
    }, 0);
    return (stryMutAct_9fa48("3382") ? totalSent <= 0 : stryMutAct_9fa48("3381") ? totalSent >= 0 : stryMutAct_9fa48("3380") ? false : stryMutAct_9fa48("3379") ? true : (stryCov_9fa48("3379", "3380", "3381", "3382"), totalSent > 0)) ? stryMutAct_9fa48("3383") ? [] : (stryCov_9fa48("3383"), [t.transfer.sender.party]) : stryMutAct_9fa48("3384") ? ["Stryker was here"] : (stryCov_9fa48("3384"), []);
  }
}
function buildSeriesFromDaily(perDay: Record<string, {
  partySet: Set<string>;
  txCount: number;
}>, startDate: Date, endDate: Date): UsageCharts {
  if (stryMutAct_9fa48("3385")) {
    {}
  } else {
    stryCov_9fa48("3385");
    const allDates: string[] = stryMutAct_9fa48("3386") ? ["Stryker was here"] : (stryCov_9fa48("3386"), []);
    const cursor = new Date(startDate);
    const end = new Date(endDate);
    stryMutAct_9fa48("3387") ? cursor.setMinutes(0, 0, 0, 0) : (stryCov_9fa48("3387"), cursor.setHours(0, 0, 0, 0));
    stryMutAct_9fa48("3388") ? end.setMinutes(0, 0, 0, 0) : (stryCov_9fa48("3388"), end.setHours(0, 0, 0, 0));
    while (stryMutAct_9fa48("3391") ? cursor > end : stryMutAct_9fa48("3390") ? cursor < end : stryMutAct_9fa48("3389") ? false : (stryCov_9fa48("3389", "3390", "3391"), cursor <= end)) {
      if (stryMutAct_9fa48("3392")) {
        {}
      } else {
        stryCov_9fa48("3392");
        allDates.push(toDateKey(cursor));
        stryMutAct_9fa48("3393") ? cursor.setTime(cursor.getDate() + 1) : (stryCov_9fa48("3393"), cursor.setDate(stryMutAct_9fa48("3394") ? cursor.getDate() - 1 : (stryCov_9fa48("3394"), cursor.getDate() + 1)));
      }
    }
    const cumulativeParties: {
      date: string;
      parties: number;
    }[] = stryMutAct_9fa48("3395") ? ["Stryker was here"] : (stryCov_9fa48("3395"), []);
    const dailyActiveUsers: {
      date: string;
      daily: number;
      avg7d: number;
    }[] = stryMutAct_9fa48("3396") ? ["Stryker was here"] : (stryCov_9fa48("3396"), []);
    const dailyTransactions: {
      date: string;
      daily: number;
      avg7d: number;
    }[] = stryMutAct_9fa48("3397") ? ["Stryker was here"] : (stryCov_9fa48("3397"), []);
    const seen = new Set<string>();
    allDates.forEach((dateKey, idx) => {
      if (stryMutAct_9fa48("3398")) {
        {}
      } else {
        stryCov_9fa48("3398");
        const dayEntry = stryMutAct_9fa48("3401") ? perDay[dateKey] && {
          partySet: new Set<string>(),
          txCount: 0
        } : stryMutAct_9fa48("3400") ? false : stryMutAct_9fa48("3399") ? true : (stryCov_9fa48("3399", "3400", "3401"), perDay[dateKey] || {
          partySet: new Set<string>(),
          txCount: 0
        });
        // cumulative
        dayEntry.partySet.forEach(stryMutAct_9fa48("3403") ? () => undefined : (stryCov_9fa48("3403"), p => seen.add(p)));
        cumulativeParties.push({
          date: dateKey,
          parties: seen.size
        });

        // daily users + 7d avg
        const daily = dayEntry.partySet.size;
        const start = stryMutAct_9fa48("3405") ? Math.min(0, idx - 6) : (stryCov_9fa48("3405"), Math.max(0, stryMutAct_9fa48("3406") ? idx + 6 : (stryCov_9fa48("3406"), idx - 6)));
        const window = stryMutAct_9fa48("3407") ? allDates : (stryCov_9fa48("3407"), allDates.slice(start, stryMutAct_9fa48("3408") ? idx - 1 : (stryCov_9fa48("3408"), idx + 1)));
        const avg7d = Math.round(stryMutAct_9fa48("3409") ? window.reduce((sum, d) => sum + (perDay[d]?.partySet.size || 0), 0) * window.length : (stryCov_9fa48("3409"), window.reduce(stryMutAct_9fa48("3410") ? () => undefined : (stryCov_9fa48("3410"), (sum, d) => stryMutAct_9fa48("3411") ? sum - (perDay[d]?.partySet.size || 0) : (stryCov_9fa48("3411"), sum + (stryMutAct_9fa48("3414") ? perDay[d]?.partySet.size && 0 : stryMutAct_9fa48("3413") ? false : stryMutAct_9fa48("3412") ? true : (stryCov_9fa48("3412", "3413", "3414"), (stryMutAct_9fa48("3415") ? perDay[d].partySet.size : (stryCov_9fa48("3415"), perDay[d]?.partySet.size)) || 0)))), 0) / window.length));
        dailyActiveUsers.push({
          date: dateKey,
          daily,
          avg7d
        });

        // daily tx + 7d avg
        const txDaily = dayEntry.txCount;
        const txAvg7 = Math.round(stryMutAct_9fa48("3417") ? window.reduce((sum, d) => sum + (perDay[d]?.txCount || 0), 0) * window.length : (stryCov_9fa48("3417"), window.reduce(stryMutAct_9fa48("3418") ? () => undefined : (stryCov_9fa48("3418"), (sum, d) => stryMutAct_9fa48("3419") ? sum - (perDay[d]?.txCount || 0) : (stryCov_9fa48("3419"), sum + (stryMutAct_9fa48("3422") ? perDay[d]?.txCount && 0 : stryMutAct_9fa48("3421") ? false : stryMutAct_9fa48("3420") ? true : (stryCov_9fa48("3420", "3421", "3422"), (stryMutAct_9fa48("3423") ? perDay[d].txCount : (stryCov_9fa48("3423"), perDay[d]?.txCount)) || 0)))), 0) / window.length));
        dailyTransactions.push({
          date: dateKey,
          daily: txDaily,
          avg7d: txAvg7
        });
      }
    });
    return {
      cumulativeParties,
      dailyActiveUsers,
      dailyTransactions,
      totalParties: seen.size,
      totalDailyUsers: (stryMutAct_9fa48("3429") ? dailyActiveUsers.length <= 0 : stryMutAct_9fa48("3428") ? dailyActiveUsers.length >= 0 : stryMutAct_9fa48("3427") ? false : stryMutAct_9fa48("3426") ? true : (stryCov_9fa48("3426", "3427", "3428", "3429"), dailyActiveUsers.length > 0)) ? dailyActiveUsers[stryMutAct_9fa48("3430") ? dailyActiveUsers.length + 1 : (stryCov_9fa48("3430"), dailyActiveUsers.length - 1)].avg7d : 0,
      totalTransactions: dailyTransactions.reduce(stryMutAct_9fa48("3431") ? () => undefined : (stryCov_9fa48("3431"), (sum, d) => stryMutAct_9fa48("3432") ? sum - d.daily : (stryCov_9fa48("3432"), sum + d.daily)), 0)
    };
  }
}
export function useUsageStats(days: number = 90) {
  if (stryMutAct_9fa48("3433")) {
    {}
  } else {
    stryCov_9fa48("3433");
    return useQuery<UsageCharts>({
      queryKey: stryMutAct_9fa48("3435") ? [] : (stryCov_9fa48("3435"), ["usage-stats", days]),
      queryFn: async () => {
        if (stryMutAct_9fa48("3437")) {
          {}
        } else {
          stryCov_9fa48("3437");
          const end = new Date();
          const start = new Date();
          stryMutAct_9fa48("3438") ? start.setMinutes(0, 0, 0, 0) : (stryCov_9fa48("3438"), start.setHours(0, 0, 0, 0));
          stryMutAct_9fa48("3439") ? end.setMinutes(0, 0, 0, 0) : (stryCov_9fa48("3439"), end.setHours(0, 0, 0, 0));
          stryMutAct_9fa48("3440") ? start.setTime(end.getDate() - Math.max(1, days)) : (stryCov_9fa48("3440"), start.setDate(stryMutAct_9fa48("3441") ? end.getDate() + Math.max(1, days) : (stryCov_9fa48("3441"), end.getDate() - (stryMutAct_9fa48("3442") ? Math.min(1, days) : (stryCov_9fa48("3442"), Math.max(1, days))))));
          const perDay: Record<string, {
            partySet: Set<string>;
            txCount: number;
          }> = {};
          let pageEnd: string | undefined = undefined;
          let pagesFetched = 0;
          const maxPages = 100; // Increased to fetch more historical data
          let totalTransactions = 0;
          console.log(`Starting to fetch transactions for ${days} days (from ${start.toISOString()} to ${end.toISOString()})`);
          while (stryMutAct_9fa48("3446") ? pagesFetched >= maxPages : stryMutAct_9fa48("3445") ? pagesFetched <= maxPages : stryMutAct_9fa48("3444") ? false : (stryCov_9fa48("3444", "3445", "3446"), pagesFetched < maxPages)) {
            if (stryMutAct_9fa48("3447")) {
              {}
            } else {
              stryCov_9fa48("3447");
              try {
                if (stryMutAct_9fa48("3448")) {
                  {}
                } else {
                  stryCov_9fa48("3448");
                  const res = await scanApi.fetchTransactions({
                    page_end_event_id: pageEnd,
                    sort_order: "desc",
                    page_size: 500 // Reduced page size to improve reliability
                  });
                  const txs = stryMutAct_9fa48("3453") ? res.transactions && [] : stryMutAct_9fa48("3452") ? false : stryMutAct_9fa48("3451") ? true : (stryCov_9fa48("3451", "3452", "3453"), res.transactions || (stryMutAct_9fa48("3454") ? ["Stryker was here"] : (stryCov_9fa48("3454"), [])));
                  if (stryMutAct_9fa48("3457") ? txs.length !== 0 : stryMutAct_9fa48("3456") ? false : stryMutAct_9fa48("3455") ? true : (stryCov_9fa48("3455", "3456", "3457"), txs.length === 0)) {
                    if (stryMutAct_9fa48("3458")) {
                      {}
                    } else {
                      stryCov_9fa48("3458");
                      console.log(`No more transactions found after ${pagesFetched} pages`);
                      break;
                    }
                  }
                  let reachedCutoff = stryMutAct_9fa48("3460") ? true : (stryCov_9fa48("3460"), false);
                  let txProcessedThisPage = 0;
                  for (const tx of txs) {
                    if (stryMutAct_9fa48("3461")) {
                      {}
                    } else {
                      stryCov_9fa48("3461");
                      const d = new Date(tx.date);
                      if (stryMutAct_9fa48("3465") ? d >= start : stryMutAct_9fa48("3464") ? d <= start : stryMutAct_9fa48("3463") ? false : stryMutAct_9fa48("3462") ? true : (stryCov_9fa48("3462", "3463", "3464", "3465"), d < start)) {
                        if (stryMutAct_9fa48("3466")) {
                          {}
                        } else {
                          stryCov_9fa48("3466");
                          reachedCutoff = stryMutAct_9fa48("3467") ? false : (stryCov_9fa48("3467"), true);
                          break; // Stop processing this page once we reach the cutoff
                        }
                      }
                      const key = toDateKey(tx.date);
                      if (stryMutAct_9fa48("3470") ? false : stryMutAct_9fa48("3469") ? true : stryMutAct_9fa48("3468") ? perDay[key] : (stryCov_9fa48("3468", "3469", "3470"), !perDay[key])) perDay[key] = {
                        partySet: new Set(),
                        txCount: 0
                      };
                      const senders = extractParties(tx);
                      senders.forEach(stryMutAct_9fa48("3472") ? () => undefined : (stryCov_9fa48("3472"), p => perDay[key].partySet.add(p)));
                      if (stryMutAct_9fa48("3476") ? senders.length <= 0 : stryMutAct_9fa48("3475") ? senders.length >= 0 : stryMutAct_9fa48("3474") ? false : stryMutAct_9fa48("3473") ? true : (stryCov_9fa48("3473", "3474", "3475", "3476"), senders.length > 0)) {
                        if (stryMutAct_9fa48("3477")) {
                          {}
                        } else {
                          stryCov_9fa48("3477");
                          stryMutAct_9fa48("3478") ? perDay[key].txCount -= 1 : (stryCov_9fa48("3478"), perDay[key].txCount += 1); // count only positive-value transfer transactions
                        }
                      }
                      stryMutAct_9fa48("3479") ? txProcessedThisPage-- : (stryCov_9fa48("3479"), txProcessedThisPage++);
                      stryMutAct_9fa48("3480") ? totalTransactions-- : (stryCov_9fa48("3480"), totalTransactions++);
                    }
                  }
                  pageEnd = txs[stryMutAct_9fa48("3481") ? txs.length + 1 : (stryCov_9fa48("3481"), txs.length - 1)].event_id;
                  stryMutAct_9fa48("3482") ? pagesFetched-- : (stryCov_9fa48("3482"), pagesFetched++);
                  console.log(`Page ${pagesFetched}/${maxPages}: Processed ${txProcessedThisPage} txs, ` + `Total: ${totalTransactions} txs across ${Object.keys(perDay).length} days, ` + `Oldest date: ${stryMutAct_9fa48("3488") ? txs[txs.length - 1]?.date && "N/A" : stryMutAct_9fa48("3487") ? false : stryMutAct_9fa48("3486") ? true : (stryCov_9fa48("3486", "3487", "3488"), (stryMutAct_9fa48("3489") ? txs[txs.length - 1].date : (stryCov_9fa48("3489"), txs[stryMutAct_9fa48("3490") ? txs.length + 1 : (stryCov_9fa48("3490"), txs.length - 1)]?.date)) || "N/A")}`);
                  if (stryMutAct_9fa48("3493") ? false : stryMutAct_9fa48("3492") ? true : (stryCov_9fa48("3492", "3493"), reachedCutoff)) {
                    if (stryMutAct_9fa48("3494")) {
                      {}
                    } else {
                      stryCov_9fa48("3494");
                      console.log(`Reached date cutoff at page ${pagesFetched}`);
                      break;
                    }
                  }
                }
              } catch (error) {
                if (stryMutAct_9fa48("3496")) {
                  {}
                } else {
                  stryCov_9fa48("3496");
                  console.error(`Error fetching page ${pagesFetched}:`, error);
                  break;
                }
              }
            }
          }
          console.log(`Finished fetching. Total: ${totalTransactions} transactions across ${Object.keys(perDay).length} days`);
          if (stryMutAct_9fa48("3501") ? totalTransactions !== 0 : stryMutAct_9fa48("3500") ? false : stryMutAct_9fa48("3499") ? true : (stryCov_9fa48("3499", "3500", "3501"), totalTransactions === 0)) {
            if (stryMutAct_9fa48("3502")) {
              {}
            } else {
              stryCov_9fa48("3502");
              throw new Error("No transactions fetched");
            }
          }
          return buildSeriesFromDaily(perDay, start, end);
        }
      },
      staleTime: stryMutAct_9fa48("3504") ? 5 * 60 / 1000 : (stryCov_9fa48("3504"), (stryMutAct_9fa48("3505") ? 5 / 60 : (stryCov_9fa48("3505"), 5 * 60)) * 1000),
      retry: 1
    });
  }
}