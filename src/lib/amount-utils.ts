/**
 * Safely extracts numeric amount from various possible paths in contract data
 * Returns 0 if no valid amount found
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
 * Specialized picker for locked amulet amounts
 * Prioritizes contract.amulet.amount.initialAmount
 */
export function pickLockedAmount(obj: any): number {
  if (!obj) return 0;

  // First try the locked-specific path
  if (obj.amulet?.amount?.initialAmount) {
    const parsed = parseFloat(obj.amulet.amount.initialAmount);
    if (!isNaN(parsed)) return parsed;
  }

  // Fallback to generic picker
  return pickAmount(obj);
}
