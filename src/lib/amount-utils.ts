/**
 * CC amounts are stored with 10 decimal places in the ledger
 * Divide raw amounts by this to get human-readable CC values
 */
export const CC_DECIMALS = 10;
export const CC_DIVISOR = Math.pow(10, CC_DECIMALS);

/**
 * Convert raw ledger amount to human-readable CC value
 */
export function toCC(rawAmount: number | string): number {
  const parsed = (() => {
    if (typeof rawAmount === 'string') {
      const s = rawAmount.trim();
      // Strict numeric parsing: reject strings that are only partially numeric (e.g. "100abc")
      // Accepts integers and decimals; ledger amounts are expected to be plain base-10 values.
      const isStrictNumber = /^-?\d+(?:\.\d+)?$/.test(s);
      if (!isStrictNumber) return NaN;
      return parseFloat(s);
    }
    return rawAmount;
  })();
  if (isNaN(parsed)) return 0;
  return parsed / CC_DIVISOR;
}

/**
 * Safely extracts numeric amount from various possible paths in contract data
 * Returns 0 if no valid amount found (in raw ledger units)
 */
export function pickAmount(obj: any): number {
  if (!obj) return 0;

  // Try various paths where amount might be stored
  const paths = [
    obj.amount?.initialAmount,
    obj.amulet?.amount?.initialAmount,
    obj.state?.amount?.initialAmount,
    obj.create_arguments?.amount?.initialAmount,
    obj.balance?.initialAmount,
    obj.amount,
  ];

  for (const value of paths) {
    if (value !== undefined && value !== null) {
      const parsed = typeof value === 'string' ? parseFloat(value) : Number(value);
      if (!isNaN(parsed)) {
        return parsed;
      }
    }
  }

  return 0;
}

/**
 * Safely extracts and converts amount to CC (human-readable)
 */
export function pickAmountAsCC(obj: any): number {
  return toCC(pickAmount(obj));
}

/**
 * Specialized picker for locked amulet amounts
 * Prioritizes contract.amulet.amount.initialAmount
 */
export function pickLockedAmount(obj: any): number {
  if (!obj) return 0;

  // First try the locked-specific path with explicit null/undefined check
  // (truthy check would incorrectly skip numeric 0)
  const lockedValue = obj.amulet?.amount?.initialAmount;
  if (lockedValue !== undefined && lockedValue !== null) {
    const parsed = parseFloat(lockedValue);
    if (!isNaN(parsed)) return parsed;
  }

  // Fallback to generic picker
  return pickAmount(obj);
}
