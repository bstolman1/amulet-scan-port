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
  if (stryMutAct_9fa48("3506")) {
    {}
  } else {
    stryCov_9fa48("3506");
    const parsed = (stryMutAct_9fa48("3509") ? typeof rawAmount !== 'string' : stryMutAct_9fa48("3508") ? false : stryMutAct_9fa48("3507") ? true : (stryCov_9fa48("3507", "3508", "3509"), typeof rawAmount === 'string')) ? parseFloat(rawAmount) : rawAmount;
    if (stryMutAct_9fa48("3512") ? false : stryMutAct_9fa48("3511") ? true : (stryCov_9fa48("3511", "3512"), isNaN(parsed))) return 0;
    return stryMutAct_9fa48("3513") ? parsed * CC_DIVISOR : (stryCov_9fa48("3513"), parsed / CC_DIVISOR);
  }
}

/**
 * Safely extracts numeric amount from various possible paths in contract data
 * Returns 0 if no valid amount found (in raw ledger units)
 */
export function pickAmount(obj: any): number {
  if (stryMutAct_9fa48("3514")) {
    {}
  } else {
    stryCov_9fa48("3514");
    if (stryMutAct_9fa48("3517") ? false : stryMutAct_9fa48("3516") ? true : stryMutAct_9fa48("3515") ? obj : (stryCov_9fa48("3515", "3516", "3517"), !obj)) return 0;

    // Try various paths where amount might be stored
    const paths = stryMutAct_9fa48("3518") ? [] : (stryCov_9fa48("3518"), [stryMutAct_9fa48("3519") ? obj.amount.initialAmount : (stryCov_9fa48("3519"), obj.amount?.initialAmount), stryMutAct_9fa48("3521") ? obj.amulet.amount?.initialAmount : stryMutAct_9fa48("3520") ? obj.amulet?.amount.initialAmount : (stryCov_9fa48("3520", "3521"), obj.amulet?.amount?.initialAmount), stryMutAct_9fa48("3523") ? obj.state.amount?.initialAmount : stryMutAct_9fa48("3522") ? obj.state?.amount.initialAmount : (stryCov_9fa48("3522", "3523"), obj.state?.amount?.initialAmount), stryMutAct_9fa48("3525") ? obj.create_arguments.amount?.initialAmount : stryMutAct_9fa48("3524") ? obj.create_arguments?.amount.initialAmount : (stryCov_9fa48("3524", "3525"), obj.create_arguments?.amount?.initialAmount), stryMutAct_9fa48("3526") ? obj.balance.initialAmount : (stryCov_9fa48("3526"), obj.balance?.initialAmount), obj.amount]);
    for (const value of paths) {
      if (stryMutAct_9fa48("3527")) {
        {}
      } else {
        stryCov_9fa48("3527");
        if (stryMutAct_9fa48("3530") ? value !== undefined || value !== null : stryMutAct_9fa48("3529") ? false : stryMutAct_9fa48("3528") ? true : (stryCov_9fa48("3528", "3529", "3530"), (stryMutAct_9fa48("3532") ? value === undefined : stryMutAct_9fa48("3531") ? true : (stryCov_9fa48("3531", "3532"), value !== undefined)) && (stryMutAct_9fa48("3534") ? value === null : stryMutAct_9fa48("3533") ? true : (stryCov_9fa48("3533", "3534"), value !== null)))) {
          if (stryMutAct_9fa48("3535")) {
            {}
          } else {
            stryCov_9fa48("3535");
            const parsed = (stryMutAct_9fa48("3538") ? typeof value !== 'string' : stryMutAct_9fa48("3537") ? false : stryMutAct_9fa48("3536") ? true : (stryCov_9fa48("3536", "3537", "3538"), typeof value === 'string')) ? parseFloat(value) : Number(value);
            if (stryMutAct_9fa48("3542") ? false : stryMutAct_9fa48("3541") ? true : stryMutAct_9fa48("3540") ? isNaN(parsed) : (stryCov_9fa48("3540", "3541", "3542"), !isNaN(parsed))) {
              if (stryMutAct_9fa48("3543")) {
                {}
              } else {
                stryCov_9fa48("3543");
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
  if (stryMutAct_9fa48("3544")) {
    {}
  } else {
    stryCov_9fa48("3544");
    return toCC(pickAmount(obj));
  }
}

/**
 * Specialized picker for locked amulet amounts
 * Prioritizes contract.amulet.amount.initialAmount
 */
export function pickLockedAmount(obj: any): number {
  if (stryMutAct_9fa48("3545")) {
    {}
  } else {
    stryCov_9fa48("3545");
    if (stryMutAct_9fa48("3548") ? false : stryMutAct_9fa48("3547") ? true : stryMutAct_9fa48("3546") ? obj : (stryCov_9fa48("3546", "3547", "3548"), !obj)) return 0;

    // First try the locked-specific path
    if (stryMutAct_9fa48("3552") ? obj.amulet.amount?.initialAmount : stryMutAct_9fa48("3551") ? obj.amulet?.amount.initialAmount : stryMutAct_9fa48("3550") ? false : stryMutAct_9fa48("3549") ? true : (stryCov_9fa48("3549", "3550", "3551", "3552"), obj.amulet?.amount?.initialAmount)) {
      if (stryMutAct_9fa48("3553")) {
        {}
      } else {
        stryCov_9fa48("3553");
        const parsed = parseFloat(obj.amulet.amount.initialAmount);
        if (stryMutAct_9fa48("3556") ? false : stryMutAct_9fa48("3555") ? true : stryMutAct_9fa48("3554") ? isNaN(parsed) : (stryCov_9fa48("3554", "3555", "3556"), !isNaN(parsed))) return parsed;
      }
    }

    // Fallback to generic picker
    return pickAmount(obj);
  }
}