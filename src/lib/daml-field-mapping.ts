/**
 * Daml field mappings for translating positional JSON fields to human-readable labels
 */

export interface ParsedField {
  label: string;
  value: string | number | boolean;
  category: 'amount' | 'fee' | 'metadata' | 'party';
}

export interface ParsedEventData {
  eventType: string;
  primaryAmount?: ParsedField;
  details: ParsedField[];
}

// Navigate nested object using dot notation path
function getNestedValue(obj: any, path: string): any {
  const parts = path.split(/[.\[\]]+/).filter(Boolean);
  let current = obj;
  
  for (const part of parts) {
    if (current === null || current === undefined) return undefined;
    current = current[part];
  }
  
  return current;
}

// Format party ID for display
function formatParty(party: string): string {
  if (!party) return '';
  const parts = party.split('::');
  return parts[parts.length - 1] || party;
}

// Format numeric value
function formatNumeric(value: any): string {
  if (value === null || value === undefined) return '0';
  const num = typeof value === 'string' ? parseFloat(value) : Number(value);
  if (isNaN(num)) return '0';
  return num.toLocaleString(undefined, { 
    minimumFractionDigits: 2, 
    maximumFractionDigits: 10 
  });
}

// Template-specific field mappings
const TEMPLATE_MAPPINGS: Record<string, {
  type: string;
  primaryPath?: string;
  fields: Record<string, { label: string; category: ParsedField['category'] }>;
}> = {
  'Splice.Amulet:Amulet': {
    type: 'Amulet Created',
    primaryPath: 'create_arguments.amount.initialAmount',
    fields: {
      'create_arguments.dso': { label: 'DSO', category: 'party' },
      'create_arguments.owner': { label: 'Owner', category: 'party' },
      'create_arguments.amount.initialAmount': { label: 'Initial Amount', category: 'amount' },
      'create_arguments.amount.createdAt.number': { label: 'Created At Round', category: 'metadata' },
      'create_arguments.amount.ratePerRound.rate': { label: 'Holding Fee Rate', category: 'fee' },
    }
  },
  'Splice.Amulet:LockedAmulet': {
    type: 'Locked Amulet',
    primaryPath: 'create_arguments.amulet.amount.initialAmount',
    fields: {
      'create_arguments.amulet.dso': { label: 'DSO', category: 'party' },
      'create_arguments.amulet.owner': { label: 'Owner', category: 'party' },
      'create_arguments.amulet.amount.initialAmount': { label: 'Locked Amount', category: 'amount' },
      'create_arguments.amulet.amount.createdAt.number': { label: 'Created At Round', category: 'metadata' },
      'create_arguments.lock.holders': { label: 'Lock Holders', category: 'party' },
    }
  },
  'Splice.Amulet:ValidatorRewardCoupon': {
    type: 'Validator Reward',
    primaryPath: 'create_arguments.amount',
    fields: {
      'create_arguments.dso': { label: 'DSO', category: 'party' },
      'create_arguments.user': { label: 'Validator', category: 'party' },
      'create_arguments.amount': { label: 'Reward Amount', category: 'amount' },
      'create_arguments.round.number': { label: 'Round', category: 'metadata' },
    }
  },
  'Splice.Amulet:AppRewardCoupon': {
    type: 'App Reward',
    primaryPath: 'create_arguments.amount',
    fields: {
      'create_arguments.dso': { label: 'DSO', category: 'party' },
      'create_arguments.provider': { label: 'App Provider', category: 'party' },
      'create_arguments.featured': { label: 'Featured App', category: 'metadata' },
      'create_arguments.amount': { label: 'Reward Amount', category: 'amount' },
      'create_arguments.round.number': { label: 'Round', category: 'metadata' },
    }
  },
  'Splice.Amulet:SvRewardCoupon': {
    type: 'SV Reward',
    primaryPath: 'create_arguments.weight',
    fields: {
      'create_arguments.dso': { label: 'DSO', category: 'party' },
      'create_arguments.sv': { label: 'Super Validator', category: 'party' },
      'create_arguments.beneficiary': { label: 'Beneficiary', category: 'party' },
      'create_arguments.round.number': { label: 'Round', category: 'metadata' },
      'create_arguments.weight': { label: 'Weight', category: 'amount' },
    }
  },
};

// Transfer exercise result field mappings
const TRANSFER_FIELDS: Record<string, { label: string; category: ParsedField['category'] }> = {
  'exercise_result.summary.round.number': { label: 'Round', category: 'metadata' },
  'exercise_result.summary.balanceChanges.holdingFees': { label: 'Holding Fees', category: 'fee' },
  'exercise_result.summary.balanceChanges.senderChangeFee': { label: 'Sender Change Fee', category: 'fee' },
  'exercise_result.summary.balanceChanges.amuletPrice': { label: 'Amulet Price (USD)', category: 'metadata' },
  'exercise_result.summary.balanceChanges.senderChangeAmount': { label: 'Sender Change Amount', category: 'amount' },
  'exercise_result.summary.balanceChanges.inputAppRewardAmount': { label: 'Input App Reward', category: 'amount' },
  'exercise_result.summary.balanceChanges.inputValidatorRewardAmount': { label: 'Input Validator Reward', category: 'amount' },
  'exercise_result.summary.balanceChanges.inputSvRewardAmount': { label: 'Input SV Reward', category: 'amount' },
};

export function parseEventData(eventData: any, templateId: string): ParsedEventData {
  const templateType = templateId?.split(':').pop() || '';
  const choice = eventData.choice;
  
  // Handle Transfer events
  if (choice === 'AmuletRules_Transfer' || choice === 'Transfer') {
    return parseTransferEvent(eventData);
  }
  
  // Handle created events based on template
  const mapping = TEMPLATE_MAPPINGS[templateId];
  if (!mapping) {
    return {
      eventType: templateType || 'Unknown Event',
      details: []
    };
  }
  
  const details: ParsedField[] = [];
  let primaryAmount: ParsedField | undefined;
  
  // Extract fields based on mapping
  for (const [path, config] of Object.entries(mapping.fields)) {
    const value = getNestedValue(eventData, path);
    if (value === undefined || value === null) continue;
    
    let formattedValue: string;
    if (typeof value === 'boolean') {
      formattedValue = value ? 'Yes' : 'No';
    } else if (typeof value === 'string' && value.includes('::')) {
      formattedValue = formatParty(value);
    } else if (typeof value === 'number' || (typeof value === 'string' && !isNaN(parseFloat(value)))) {
      formattedValue = formatNumeric(value);
    } else if (Array.isArray(value)) {
      formattedValue = value.map(v => typeof v === 'string' && v.includes('::') ? formatParty(v) : v).join(', ');
    } else {
      formattedValue = String(value);
    }
    
    const field: ParsedField = {
      label: config.label,
      value: formattedValue,
      category: config.category
    };
    
    // Set primary amount if this is the primary path
    if (path === mapping.primaryPath && config.category === 'amount') {
      primaryAmount = field;
    }
    
    details.push(field);
  }
  
  return {
    eventType: mapping.type,
    primaryAmount,
    details
  };
}

function parseTransferEvent(eventData: any): ParsedEventData {
  const details: ParsedField[] = [];
  let transferAmount: number = 0;
  
  // Extract transfer amount from inputs/outputs
  const inputs = eventData.exercise_result?.summary?.balanceChanges?.inputAmuletAmount;
  const outputs = eventData.exercise_result?.summary?.balanceChanges?.outputAmuletAmount;
  
  if (inputs) transferAmount += parseFloat(inputs) || 0;
  if (outputs) transferAmount += parseFloat(outputs) || 0;
  
  // Get sender and receiver
  const sender = eventData.argument?.transfer?.sender;
  const receivers = eventData.argument?.transfer?.receivers;
  
  if (sender) {
    details.push({
      label: 'Sender',
      value: formatParty(sender),
      category: 'party'
    });
  }
  
  if (receivers && Array.isArray(receivers)) {
    receivers.forEach((receiver: any, idx: number) => {
      const party = receiver.receiver?.party || receiver.party;
      const amount = receiver.amount || receiver.receiverAmount;
      
      if (party) {
        details.push({
          label: idx === 0 ? 'Receiver' : `Receiver ${idx + 1}`,
          value: formatParty(party),
          category: 'party'
        });
      }
      
      if (amount) {
        details.push({
          label: `Amount to ${formatParty(party) || `Receiver ${idx + 1}`}`,
          value: formatNumeric(amount),
          category: 'amount'
        });
        transferAmount = parseFloat(amount) || transferAmount;
      }
    });
  }
  
  // Extract other fields
  for (const [path, config] of Object.entries(TRANSFER_FIELDS)) {
    const value = getNestedValue(eventData, path);
    if (value === undefined || value === null) continue;
    
    details.push({
      label: config.label,
      value: formatNumeric(value),
      category: config.category
    });
  }
  
  const primaryAmount: ParsedField = {
    label: 'Transfer Amount',
    value: formatNumeric(transferAmount),
    category: 'amount'
  };
  
  return {
    eventType: 'Transfer',
    primaryAmount,
    details
  };
}
