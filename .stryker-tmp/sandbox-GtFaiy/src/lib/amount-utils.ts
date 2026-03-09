/**
 * CC amounts are stored with 10 decimal places in the ledger
 * Divide raw amounts by this to get human-readable CC values
 */
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
export const CC_DECIMALS = 10;
export const CC_DIVISOR = Math.pow(10, CC_DECIMALS);

/**
 * Convert raw ledger amount to human-readable CC value
 */
export function toCC(rawAmount: number | string): number {
  if (stryMutAct_9fa48("344")) {
    {}
  } else {
    stryCov_9fa48("344");
    const parsed = (stryMutAct_9fa48("347") ? typeof rawAmount !== 'string' : stryMutAct_9fa48("346") ? false : stryMutAct_9fa48("345") ? true : (stryCov_9fa48("345", "346", "347"), typeof rawAmount === 'string')) ? parseFloat(rawAmount) : rawAmount;
    if (stryMutAct_9fa48("350") ? false : stryMutAct_9fa48("349") ? true : (stryCov_9fa48("349", "350"), isNaN(parsed))) return 0;
    return stryMutAct_9fa48("351") ? parsed * CC_DIVISOR : (stryCov_9fa48("351"), parsed / CC_DIVISOR);
  }
}

/**
 * Safely extracts numeric amount from various possible paths in contract data
 * Returns 0 if no valid amount found (in raw ledger units)
 */
export function pickAmount(obj: any): number {
  if (stryMutAct_9fa48("352")) {
    {}
  } else {
    stryCov_9fa48("352");
    if (stryMutAct_9fa48("355") ? false : stryMutAct_9fa48("354") ? true : stryMutAct_9fa48("353") ? obj : (stryCov_9fa48("353", "354", "355"), !obj)) return 0;

    // Try various paths where amount might be stored
    const paths = stryMutAct_9fa48("356") ? [] : (stryCov_9fa48("356"), [stryMutAct_9fa48("357") ? obj.amount.initialAmount : (stryCov_9fa48("357"), obj.amount?.initialAmount), stryMutAct_9fa48("359") ? obj.amulet.amount?.initialAmount : stryMutAct_9fa48("358") ? obj.amulet?.amount.initialAmount : (stryCov_9fa48("358", "359"), obj.amulet?.amount?.initialAmount), stryMutAct_9fa48("361") ? obj.state.amount?.initialAmount : stryMutAct_9fa48("360") ? obj.state?.amount.initialAmount : (stryCov_9fa48("360", "361"), obj.state?.amount?.initialAmount), stryMutAct_9fa48("363") ? obj.create_arguments.amount?.initialAmount : stryMutAct_9fa48("362") ? obj.create_arguments?.amount.initialAmount : (stryCov_9fa48("362", "363"), obj.create_arguments?.amount?.initialAmount), stryMutAct_9fa48("364") ? obj.balance.initialAmount : (stryCov_9fa48("364"), obj.balance?.initialAmount), obj.amount]);
    for (const value of paths) {
      if (stryMutAct_9fa48("365")) {
        {}
      } else {
        stryCov_9fa48("365");
        if (stryMutAct_9fa48("368") ? value !== undefined || value !== null : stryMutAct_9fa48("367") ? false : stryMutAct_9fa48("366") ? true : (stryCov_9fa48("366", "367", "368"), (stryMutAct_9fa48("370") ? value === undefined : stryMutAct_9fa48("369") ? true : (stryCov_9fa48("369", "370"), value !== undefined)) && (stryMutAct_9fa48("372") ? value === null : stryMutAct_9fa48("371") ? true : (stryCov_9fa48("371", "372"), value !== null)))) {
          if (stryMutAct_9fa48("373")) {
            {}
          } else {
            stryCov_9fa48("373");
            const parsed = (stryMutAct_9fa48("376") ? typeof value !== 'string' : stryMutAct_9fa48("375") ? false : stryMutAct_9fa48("374") ? true : (stryCov_9fa48("374", "375", "376"), typeof value === 'string')) ? parseFloat(value) : Number(value);
            if (stryMutAct_9fa48("380") ? false : stryMutAct_9fa48("379") ? true : stryMutAct_9fa48("378") ? isNaN(parsed) : (stryCov_9fa48("378", "379", "380"), !isNaN(parsed))) {
              if (stryMutAct_9fa48("381")) {
                {}
              } else {
                stryCov_9fa48("381");
                return parsed;
              }
            }
          }
        }
      }
    }
    return 0;
  }
}

/**
 * Safely extracts and converts amount to CC (human-readable)
 */
export function pickAmountAsCC(obj: any): number {
  if (stryMutAct_9fa48("382")) {
    {}
  } else {
    stryCov_9fa48("382");
    return toCC(pickAmount(obj));
  }
}

/**
 * Specialized picker for locked amulet amounts
 * Prioritizes contract.amulet.amount.initialAmount
 */
export function pickLockedAmount(obj: any): number {
  if (stryMutAct_9fa48("383")) {
    {}
  } else {
    stryCov_9fa48("383");
    if (stryMutAct_9fa48("386") ? false : stryMutAct_9fa48("385") ? true : stryMutAct_9fa48("384") ? obj : (stryCov_9fa48("384", "385", "386"), !obj)) return 0;

    // First try the locked-specific path
    if (stryMutAct_9fa48("390") ? obj.amulet.amount?.initialAmount : stryMutAct_9fa48("389") ? obj.amulet?.amount.initialAmount : stryMutAct_9fa48("388") ? false : stryMutAct_9fa48("387") ? true : (stryCov_9fa48("387", "388", "389", "390"), obj.amulet?.amount?.initialAmount)) {
      if (stryMutAct_9fa48("391")) {
        {}
      } else {
        stryCov_9fa48("391");
        const parsed = parseFloat(obj.amulet.amount.initialAmount);
        if (stryMutAct_9fa48("394") ? false : stryMutAct_9fa48("393") ? true : stryMutAct_9fa48("392") ? isNaN(parsed) : (stryCov_9fa48("392", "393", "394"), !isNaN(parsed))) return parsed;
      }
    }

    // Fallback to generic picker
    return pickAmount(obj);
  }
}