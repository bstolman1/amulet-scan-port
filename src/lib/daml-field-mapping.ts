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

// Navigate nested object using dot notation path with array indices
function getNestedValue(obj: any, path: string): any {
  const parts = path.split(/[.\[\]]+/).filter(Boolean);
  let current = obj;
  
  for (const part of parts) {
    if (current === null || current === undefined) return undefined;
    current = current[part];
  }
  
  return current;
}

// Extract value from Daml record structure
function extractValue(fieldValue: any): any {
  if (!fieldValue || typeof fieldValue !== 'object') return fieldValue;
  
  // Check for value wrapper
  if (fieldValue.value) {
    const value = fieldValue.value;
    
    // Check different value types
    if (value.numeric !== undefined) return value.numeric;
    if (value.int64 !== undefined) return value.int64;
    if (value.party !== undefined) return value.party;
    if (value.bool !== undefined) return value.bool;
    if (value.text !== undefined) return value.text;
    if (value.timestamp !== undefined) return value.timestamp;
    if (value.contractId !== undefined) return value.contractId;
    
    // Recursively extract from nested structures
    if (value.record) return value.record;
    if (value.optional?.value) return extractValue(value.optional.value);
    
    return value;
  }
  
  return fieldValue;
}

// Format party ID for display
function formatParty(party: string): string {
  if (!party) return '';
  const parts = party.split('::');
  return parts[parts.length - 1]?.substring(0, 12) + '...' || party;
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

// Recursively find all numeric values in the structure
function findNumericValues(obj: any, path: string = '', results: Array<{path: string, value: any}> = []): Array<{path: string, value: any}> {
  if (!obj || typeof obj !== 'object') return results;
  
  if (obj.numeric !== undefined || obj.int64 !== undefined) {
    results.push({ 
      path, 
      value: obj.numeric !== undefined ? obj.numeric : obj.int64 
    });
  }
  
  if (obj.value) {
    findNumericValues(obj.value, path, results);
  }
  
  if (obj.record?.fields && Array.isArray(obj.record.fields)) {
    obj.record.fields.forEach((field: any, idx: number) => {
      findNumericValues(field, `${path}[${idx}]`, results);
    });
  }
  
  if (Array.isArray(obj)) {
    obj.forEach((item, idx) => {
      findNumericValues(item, `${path}[${idx}]`, results);
    });
  }
  
  if (typeof obj === 'object' && !obj.numeric && !obj.int64 && !obj.value && !obj.record) {
    Object.keys(obj).forEach(key => {
      if (key !== 'numeric' && key !== 'int64') {
        findNumericValues(obj[key], path ? `${path}.${key}` : key, results);
      }
    });
  }
  
  return results;
}

// Find all party values
function findPartyValues(obj: any, path: string = '', results: Array<{path: string, value: string}> = []): Array<{path: string, value: string}> {
  if (!obj || typeof obj !== 'object') return results;
  
  if (obj.party !== undefined && typeof obj.party === 'string') {
    results.push({ path, value: obj.party });
  }
  
  if (obj.value) {
    findPartyValues(obj.value, path, results);
  }
  
  if (obj.record?.fields && Array.isArray(obj.record.fields)) {
    obj.record.fields.forEach((field: any, idx: number) => {
      findPartyValues(field, `${path}[${idx}]`, results);
    });
  }
  
  if (Array.isArray(obj)) {
    obj.forEach((item, idx) => {
      findPartyValues(item, `${path}[${idx}]`, results);
    });
  }
  
  return results;
}

export function parseEventData(eventData: any, templateId: string): ParsedEventData {
  const templateType = templateId?.split(':').pop() || '';
  const choice = eventData.choice;
  const eventType = eventData.event_type;
  
  // Handle Transfer events (exercised_event with AmuletRules_Transfer choice)
  if (choice === 'AmuletRules_Transfer' || choice === 'Transfer') {
    return parseTransferEvent(eventData);
  }
  
  // Handle created events
  if (eventType === 'created_event') {
    return parseCreatedEvent(eventData, templateId, templateType);
  }
  
  // Fallback: extract all values
  return {
    eventType: templateType || 'Unknown Event',
    details: extractAllFields(eventData)
  };
}

function parseCreatedEvent(eventData: any, templateId: string, templateType: string): ParsedEventData {
  const details: ParsedField[] = [];
  
  // Find all numeric and party values
  const numerics = findNumericValues(eventData);
  const parties = findPartyValues(eventData);
  
  // Determine event type based on template
  let eventTypeName = templateType;
  let primaryAmount: ParsedField | undefined;
  
  if (templateId.includes('Amulet:Amulet') && !templateId.includes('Locked')) {
    eventTypeName = 'Amulet Created';
    // Primary amount is usually the first numeric value (initial amount)
    if (numerics.length > 0) {
      primaryAmount = {
        label: 'Initial Amount',
        value: formatNumeric(numerics[0].value),
        category: 'amount'
      };
    }
  } else if (templateId.includes('LockedAmulet')) {
    eventTypeName = 'Locked Amulet';
    if (numerics.length > 0) {
      primaryAmount = {
        label: 'Locked Amount',
        value: formatNumeric(numerics[0].value),
        category: 'amount'
      };
    }
  } else if (templateId.includes('ValidatorRewardCoupon')) {
    eventTypeName = 'Validator Reward';
    // Find reward amount (usually first numeric)
    if (numerics.length > 0) {
      primaryAmount = {
        label: 'Reward Amount',
        value: formatNumeric(numerics[0].value),
        category: 'amount'
      };
    }
  } else if (templateId.includes('AppRewardCoupon')) {
    eventTypeName = 'App Reward';
    // Find reward amount
    if (numerics.length > 0) {
      primaryAmount = {
        label: 'Reward Amount',
        value: formatNumeric(numerics[0].value),
        category: 'amount'
      };
    }
  } else if (templateId.includes('SvRewardCoupon')) {
    eventTypeName = 'SV Reward';
    // For SV rewards, show round and weight
    if (numerics.length > 0) {
      primaryAmount = {
        label: 'Weight',
        value: formatNumeric(numerics[numerics.length - 1].value),
        category: 'amount'
      };
    }
  }
  
  // Add numeric fields (excluding the primary amount)
  numerics.forEach((numeric, idx) => {
    if (idx === 0 && primaryAmount) return; // Skip if it's the primary amount
    
    let label = 'Amount';
    let category: ParsedField['category'] = 'amount';
    
    // Try to infer label from context
    if (numeric.path.includes('round')) {
      label = 'Round';
      category = 'metadata';
    } else if (numeric.path.includes('rate') || numeric.path.includes('fee')) {
      label = 'Fee Rate';
      category = 'fee';
    } else if (numeric.path.includes('weight')) {
      label = 'Weight';
      category = 'amount';
    } else {
      label = `Amount ${idx + 1}`;
    }
    
    details.push({
      label,
      value: formatNumeric(numeric.value),
      category
    });
  });
  
  // Add party fields
  parties.forEach((party, idx) => {
    let label = 'Party';
    
    if (idx === 0) label = 'DSO';
    else if (idx === 1) label = templateId.includes('ValidatorReward') ? 'Validator' : 
                                templateId.includes('AppReward') ? 'Provider' :
                                templateId.includes('SvReward') ? 'Super Validator' : 'Owner';
    else if (idx === 2 && templateId.includes('SvReward')) label = 'Beneficiary';
    else label = `Party ${idx + 1}`;
    
    details.push({
      label,
      value: formatParty(party.value),
      category: 'party'
    });
  });
  
  return {
    eventType: eventTypeName,
    primaryAmount,
    details
  };
}

function parseTransferEvent(eventData: any): ParsedEventData {
  const details: ParsedField[] = [];
  
  // Extract round from exercise_result
  const exerciseResult = eventData.exercise_result;
  if (exerciseResult?.record?.fields) {
    const fields = exerciseResult.record.fields;
    
    // Field 0 is usually the round
    if (fields[0]?.value?.record?.fields?.[0]?.value?.int64) {
      details.push({
        label: 'Round',
        value: fields[0].value.record.fields[0].value.int64.toString(),
        category: 'metadata'
      });
    }
    
    // Field 1 contains balance changes
    if (fields[1]?.value?.record?.fields) {
      const balanceFields = fields[1].value.record.fields;
      
      const balanceLabels = [
        { label: 'Holding Fees', category: 'fee' as const },
        { label: 'Sender Change Fee', category: 'fee' as const },
        { label: 'Output Fees', category: 'fee' as const },
        { label: 'Amulet Price (USD)', category: 'metadata' as const },
      ];
      
      balanceFields.slice(0, 4).forEach((field: any, idx: number) => {
        const value = extractValue(field);
        if (value !== undefined && value !== null) {
          details.push({
            label: balanceLabels[idx]?.label || `Value ${idx}`,
            value: formatNumeric(value),
            category: balanceLabels[idx]?.category || 'amount'
          });
        }
      });
    }
  }
  
  // Extract parties from choice_argument
  const choiceArg = eventData.choice_argument;
  if (choiceArg?.record?.fields) {
    const argFields = choiceArg.record.fields;
    
    // Field 0 usually contains transfer details with sender/receiver
    if (argFields[0]?.value?.record?.fields) {
      const transferFields = argFields[0].value.record.fields;
      
      // Field 0 is sender, field 1 is provider
      if (transferFields[0]?.value?.party) {
        details.push({
          label: 'Sender',
          value: formatParty(transferFields[0].value.party),
          category: 'party'
        });
      }
      
      // Field 3 contains receivers list
      if (transferFields[3]?.value?.list?.elements) {
        const receivers = transferFields[3].value.list.elements;
        receivers.forEach((receiver: any, idx: number) => {
          if (receiver.record?.fields) {
            const partyField = receiver.record.fields[0];
            const amountField = receiver.record.fields[1];
            const usdField = receiver.record.fields[2];
            
            if (partyField?.value?.party) {
              details.push({
                label: idx === 0 ? 'Receiver' : `Receiver ${idx + 1}`,
                value: formatParty(partyField.value.party),
                category: 'party'
              });
            }
            
            if (amountField?.value?.numeric) {
              details.push({
                label: `Amount to ${idx === 0 ? 'Receiver' : `Receiver ${idx + 1}`}`,
                value: formatNumeric(amountField.value.numeric),
                category: 'amount'
              });
            }
          }
        });
      }
    }
  }
  
  // Find primary transfer amount (first receiver amount)
  const primaryAmountField = details.find(f => f.label.includes('Amount to'));
  const primaryAmount = primaryAmountField ? {
    label: 'Transfer Amount',
    value: primaryAmountField.value,
    category: 'amount' as const
  } : undefined;
  
  return {
    eventType: 'Transfer',
    primaryAmount,
    details
  };
}

function extractAllFields(eventData: any): ParsedField[] {
  const details: ParsedField[] = [];
  const numerics = findNumericValues(eventData);
  const parties = findPartyValues(eventData);
  
  numerics.forEach((numeric, idx) => {
    details.push({
      label: `Numeric ${idx + 1}`,
      value: formatNumeric(numeric.value),
      category: 'amount'
    });
  });
  
  parties.forEach((party, idx) => {
    details.push({
      label: `Party ${idx + 1}`,
      value: formatParty(party.value),
      category: 'party'
    });
  });
  
  return details;
}
