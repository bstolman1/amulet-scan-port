/**
 * Daml field mappings for translating positional JSON fields to human-readable labels
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
  if (stryMutAct_9fa48("4383")) {
    {}
  } else {
    stryCov_9fa48("4383");
    const parts = stryMutAct_9fa48("4384") ? path.split(/[.\[\]]+/) : (stryCov_9fa48("4384"), path.split(stryMutAct_9fa48("4386") ? /[^.\[\]]+/ : stryMutAct_9fa48("4385") ? /[.\[\]]/ : (stryCov_9fa48("4385", "4386"), /[.\[\]]+/)).filter(Boolean));
    let current = obj;
    for (const part of parts) {
      if (stryMutAct_9fa48("4387")) {
        {}
      } else {
        stryCov_9fa48("4387");
        if (stryMutAct_9fa48("4390") ? current === null && current === undefined : stryMutAct_9fa48("4389") ? false : stryMutAct_9fa48("4388") ? true : (stryCov_9fa48("4388", "4389", "4390"), (stryMutAct_9fa48("4392") ? current !== null : stryMutAct_9fa48("4391") ? false : (stryCov_9fa48("4391", "4392"), current === null)) || (stryMutAct_9fa48("4394") ? current !== undefined : stryMutAct_9fa48("4393") ? false : (stryCov_9fa48("4393", "4394"), current === undefined)))) return undefined;
        current = current[part];
      }
    }
    return current;
  }
}

// Extract value from Daml record structure
function extractValue(fieldValue: any): any {
  if (stryMutAct_9fa48("4395")) {
    {}
  } else {
    stryCov_9fa48("4395");
    if (stryMutAct_9fa48("4398") ? !fieldValue && typeof fieldValue !== 'object' : stryMutAct_9fa48("4397") ? false : stryMutAct_9fa48("4396") ? true : (stryCov_9fa48("4396", "4397", "4398"), (stryMutAct_9fa48("4399") ? fieldValue : (stryCov_9fa48("4399"), !fieldValue)) || (stryMutAct_9fa48("4401") ? typeof fieldValue === 'object' : stryMutAct_9fa48("4400") ? false : (stryCov_9fa48("4400", "4401"), typeof fieldValue !== 'object')))) return fieldValue;

    // Check for value wrapper
    if (stryMutAct_9fa48("4404") ? false : stryMutAct_9fa48("4403") ? true : (stryCov_9fa48("4403", "4404"), fieldValue.value)) {
      if (stryMutAct_9fa48("4405")) {
        {}
      } else {
        stryCov_9fa48("4405");
        const value = fieldValue.value;

        // Check different value types
        if (stryMutAct_9fa48("4408") ? value.numeric === undefined : stryMutAct_9fa48("4407") ? false : stryMutAct_9fa48("4406") ? true : (stryCov_9fa48("4406", "4407", "4408"), value.numeric !== undefined)) return value.numeric;
        if (stryMutAct_9fa48("4411") ? value.int64 === undefined : stryMutAct_9fa48("4410") ? false : stryMutAct_9fa48("4409") ? true : (stryCov_9fa48("4409", "4410", "4411"), value.int64 !== undefined)) return value.int64;
        if (stryMutAct_9fa48("4414") ? value.party === undefined : stryMutAct_9fa48("4413") ? false : stryMutAct_9fa48("4412") ? true : (stryCov_9fa48("4412", "4413", "4414"), value.party !== undefined)) return value.party;
        if (stryMutAct_9fa48("4417") ? value.bool === undefined : stryMutAct_9fa48("4416") ? false : stryMutAct_9fa48("4415") ? true : (stryCov_9fa48("4415", "4416", "4417"), value.bool !== undefined)) return value.bool;
        if (stryMutAct_9fa48("4420") ? value.text === undefined : stryMutAct_9fa48("4419") ? false : stryMutAct_9fa48("4418") ? true : (stryCov_9fa48("4418", "4419", "4420"), value.text !== undefined)) return value.text;
        if (stryMutAct_9fa48("4423") ? value.timestamp === undefined : stryMutAct_9fa48("4422") ? false : stryMutAct_9fa48("4421") ? true : (stryCov_9fa48("4421", "4422", "4423"), value.timestamp !== undefined)) return value.timestamp;
        if (stryMutAct_9fa48("4426") ? value.contractId === undefined : stryMutAct_9fa48("4425") ? false : stryMutAct_9fa48("4424") ? true : (stryCov_9fa48("4424", "4425", "4426"), value.contractId !== undefined)) return value.contractId;

        // Recursively extract from nested structures
        if (stryMutAct_9fa48("4428") ? false : stryMutAct_9fa48("4427") ? true : (stryCov_9fa48("4427", "4428"), value.record)) return value.record;
        if (stryMutAct_9fa48("4431") ? value.optional.value : stryMutAct_9fa48("4430") ? false : stryMutAct_9fa48("4429") ? true : (stryCov_9fa48("4429", "4430", "4431"), value.optional?.value)) return extractValue(value.optional.value);
        return value;
      }
    }
    return fieldValue;
  }
}

// Format party ID for display - return full ID
function formatParty(party: string): string {
  if (stryMutAct_9fa48("4432")) {
    {}
  } else {
    stryCov_9fa48("4432");
    if (stryMutAct_9fa48("4435") ? false : stryMutAct_9fa48("4434") ? true : stryMutAct_9fa48("4433") ? party : (stryCov_9fa48("4433", "4434", "4435"), !party)) return '';
    return party;
  }
}

// Format numeric value
function formatNumeric(value: any): string {
  if (stryMutAct_9fa48("4437")) {
    {}
  } else {
    stryCov_9fa48("4437");
    if (stryMutAct_9fa48("4440") ? value === null && value === undefined : stryMutAct_9fa48("4439") ? false : stryMutAct_9fa48("4438") ? true : (stryCov_9fa48("4438", "4439", "4440"), (stryMutAct_9fa48("4442") ? value !== null : stryMutAct_9fa48("4441") ? false : (stryCov_9fa48("4441", "4442"), value === null)) || (stryMutAct_9fa48("4444") ? value !== undefined : stryMutAct_9fa48("4443") ? false : (stryCov_9fa48("4443", "4444"), value === undefined)))) return '0';
    const num = (stryMutAct_9fa48("4448") ? typeof value !== 'string' : stryMutAct_9fa48("4447") ? false : stryMutAct_9fa48("4446") ? true : (stryCov_9fa48("4446", "4447", "4448"), typeof value === 'string')) ? parseFloat(value) : Number(value);
    if (stryMutAct_9fa48("4451") ? false : stryMutAct_9fa48("4450") ? true : (stryCov_9fa48("4450", "4451"), isNaN(num))) return '0';
    return num.toLocaleString(undefined, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 10
    });
  }
}

// Recursively find all numeric values in the structure
function findNumericValues(obj: any, path: string = '', results: Array<{
  path: string;
  value: any;
}> = stryMutAct_9fa48("4455") ? ["Stryker was here"] : (stryCov_9fa48("4455"), [])): Array<{
  path: string;
  value: any;
}> {
  if (stryMutAct_9fa48("4456")) {
    {}
  } else {
    stryCov_9fa48("4456");
    if (stryMutAct_9fa48("4459") ? !obj && typeof obj !== 'object' : stryMutAct_9fa48("4458") ? false : stryMutAct_9fa48("4457") ? true : (stryCov_9fa48("4457", "4458", "4459"), (stryMutAct_9fa48("4460") ? obj : (stryCov_9fa48("4460"), !obj)) || (stryMutAct_9fa48("4462") ? typeof obj === 'object' : stryMutAct_9fa48("4461") ? false : (stryCov_9fa48("4461", "4462"), typeof obj !== 'object')))) return results;
    if (stryMutAct_9fa48("4466") ? obj.numeric !== undefined && obj.int64 !== undefined : stryMutAct_9fa48("4465") ? false : stryMutAct_9fa48("4464") ? true : (stryCov_9fa48("4464", "4465", "4466"), (stryMutAct_9fa48("4468") ? obj.numeric === undefined : stryMutAct_9fa48("4467") ? false : (stryCov_9fa48("4467", "4468"), obj.numeric !== undefined)) || (stryMutAct_9fa48("4470") ? obj.int64 === undefined : stryMutAct_9fa48("4469") ? false : (stryCov_9fa48("4469", "4470"), obj.int64 !== undefined)))) {
      if (stryMutAct_9fa48("4471")) {
        {}
      } else {
        stryCov_9fa48("4471");
        results.push({
          path,
          value: (stryMutAct_9fa48("4475") ? obj.numeric === undefined : stryMutAct_9fa48("4474") ? false : stryMutAct_9fa48("4473") ? true : (stryCov_9fa48("4473", "4474", "4475"), obj.numeric !== undefined)) ? obj.numeric : obj.int64
        });
      }
    }
    if (stryMutAct_9fa48("4477") ? false : stryMutAct_9fa48("4476") ? true : (stryCov_9fa48("4476", "4477"), obj.value)) {
      if (stryMutAct_9fa48("4478")) {
        {}
      } else {
        stryCov_9fa48("4478");
        findNumericValues(obj.value, path, results);
      }
    }
    if (stryMutAct_9fa48("4481") ? obj.record?.fields || Array.isArray(obj.record.fields) : stryMutAct_9fa48("4480") ? false : stryMutAct_9fa48("4479") ? true : (stryCov_9fa48("4479", "4480", "4481"), (stryMutAct_9fa48("4482") ? obj.record.fields : (stryCov_9fa48("4482"), obj.record?.fields)) && Array.isArray(obj.record.fields))) {
      if (stryMutAct_9fa48("4483")) {
        {}
      } else {
        stryCov_9fa48("4483");
        obj.record.fields.forEach((field: any, idx: number) => {
          if (stryMutAct_9fa48("4484")) {
            {}
          } else {
            stryCov_9fa48("4484");
            findNumericValues(field, `${path}[${idx}]`, results);
          }
        });
      }
    }
    if (stryMutAct_9fa48("4487") ? false : stryMutAct_9fa48("4486") ? true : (stryCov_9fa48("4486", "4487"), Array.isArray(obj))) {
      if (stryMutAct_9fa48("4488")) {
        {}
      } else {
        stryCov_9fa48("4488");
        obj.forEach((item, idx) => {
          if (stryMutAct_9fa48("4489")) {
            {}
          } else {
            stryCov_9fa48("4489");
            findNumericValues(item, `${path}[${idx}]`, results);
          }
        });
      }
    }
    if (stryMutAct_9fa48("4493") ? typeof obj === 'object' && !obj.numeric && !obj.int64 && !obj.value || !obj.record : stryMutAct_9fa48("4492") ? false : stryMutAct_9fa48("4491") ? true : (stryCov_9fa48("4491", "4492", "4493"), (stryMutAct_9fa48("4495") ? typeof obj === 'object' && !obj.numeric && !obj.int64 || !obj.value : stryMutAct_9fa48("4494") ? true : (stryCov_9fa48("4494", "4495"), (stryMutAct_9fa48("4497") ? typeof obj === 'object' && !obj.numeric || !obj.int64 : stryMutAct_9fa48("4496") ? true : (stryCov_9fa48("4496", "4497"), (stryMutAct_9fa48("4499") ? typeof obj === 'object' || !obj.numeric : stryMutAct_9fa48("4498") ? true : (stryCov_9fa48("4498", "4499"), (stryMutAct_9fa48("4501") ? typeof obj !== 'object' : stryMutAct_9fa48("4500") ? true : (stryCov_9fa48("4500", "4501"), typeof obj === 'object')) && (stryMutAct_9fa48("4503") ? obj.numeric : (stryCov_9fa48("4503"), !obj.numeric)))) && (stryMutAct_9fa48("4504") ? obj.int64 : (stryCov_9fa48("4504"), !obj.int64)))) && (stryMutAct_9fa48("4505") ? obj.value : (stryCov_9fa48("4505"), !obj.value)))) && (stryMutAct_9fa48("4506") ? obj.record : (stryCov_9fa48("4506"), !obj.record)))) {
      if (stryMutAct_9fa48("4507")) {
        {}
      } else {
        stryCov_9fa48("4507");
        Object.keys(obj).forEach(key => {
          if (stryMutAct_9fa48("4508")) {
            {}
          } else {
            stryCov_9fa48("4508");
            if (stryMutAct_9fa48("4511") ? key !== 'numeric' || key !== 'int64' : stryMutAct_9fa48("4510") ? false : stryMutAct_9fa48("4509") ? true : (stryCov_9fa48("4509", "4510", "4511"), (stryMutAct_9fa48("4513") ? key === 'numeric' : stryMutAct_9fa48("4512") ? true : (stryCov_9fa48("4512", "4513"), key !== 'numeric')) && (stryMutAct_9fa48("4516") ? key === 'int64' : stryMutAct_9fa48("4515") ? true : (stryCov_9fa48("4515", "4516"), key !== 'int64')))) {
              if (stryMutAct_9fa48("4518")) {
                {}
              } else {
                stryCov_9fa48("4518");
                findNumericValues(obj[key], path ? `${path}.${key}` : key, results);
              }
            }
          }
        });
      }
    }
    return results;
  }
}

// Find all party values
function findPartyValues(obj: any, path: string = '', results: Array<{
  path: string;
  value: string;
}> = stryMutAct_9fa48("4521") ? ["Stryker was here"] : (stryCov_9fa48("4521"), [])): Array<{
  path: string;
  value: string;
}> {
  if (stryMutAct_9fa48("4522")) {
    {}
  } else {
    stryCov_9fa48("4522");
    if (stryMutAct_9fa48("4525") ? !obj && typeof obj !== 'object' : stryMutAct_9fa48("4524") ? false : stryMutAct_9fa48("4523") ? true : (stryCov_9fa48("4523", "4524", "4525"), (stryMutAct_9fa48("4526") ? obj : (stryCov_9fa48("4526"), !obj)) || (stryMutAct_9fa48("4528") ? typeof obj === 'object' : stryMutAct_9fa48("4527") ? false : (stryCov_9fa48("4527", "4528"), typeof obj !== 'object')))) return results;
    if (stryMutAct_9fa48("4532") ? obj.party !== undefined || typeof obj.party === 'string' : stryMutAct_9fa48("4531") ? false : stryMutAct_9fa48("4530") ? true : (stryCov_9fa48("4530", "4531", "4532"), (stryMutAct_9fa48("4534") ? obj.party === undefined : stryMutAct_9fa48("4533") ? true : (stryCov_9fa48("4533", "4534"), obj.party !== undefined)) && (stryMutAct_9fa48("4536") ? typeof obj.party !== 'string' : stryMutAct_9fa48("4535") ? true : (stryCov_9fa48("4535", "4536"), typeof obj.party === 'string')))) {
      if (stryMutAct_9fa48("4538")) {
        {}
      } else {
        stryCov_9fa48("4538");
        results.push({
          path,
          value: obj.party
        });
      }
    }
    if (stryMutAct_9fa48("4541") ? false : stryMutAct_9fa48("4540") ? true : (stryCov_9fa48("4540", "4541"), obj.value)) {
      if (stryMutAct_9fa48("4542")) {
        {}
      } else {
        stryCov_9fa48("4542");
        findPartyValues(obj.value, path, results);
      }
    }
    if (stryMutAct_9fa48("4545") ? obj.record?.fields || Array.isArray(obj.record.fields) : stryMutAct_9fa48("4544") ? false : stryMutAct_9fa48("4543") ? true : (stryCov_9fa48("4543", "4544", "4545"), (stryMutAct_9fa48("4546") ? obj.record.fields : (stryCov_9fa48("4546"), obj.record?.fields)) && Array.isArray(obj.record.fields))) {
      if (stryMutAct_9fa48("4547")) {
        {}
      } else {
        stryCov_9fa48("4547");
        obj.record.fields.forEach((field: any, idx: number) => {
          if (stryMutAct_9fa48("4548")) {
            {}
          } else {
            stryCov_9fa48("4548");
            findPartyValues(field, `${path}[${idx}]`, results);
          }
        });
      }
    }
    if (stryMutAct_9fa48("4551") ? false : stryMutAct_9fa48("4550") ? true : (stryCov_9fa48("4550", "4551"), Array.isArray(obj))) {
      if (stryMutAct_9fa48("4552")) {
        {}
      } else {
        stryCov_9fa48("4552");
        obj.forEach((item, idx) => {
          if (stryMutAct_9fa48("4553")) {
            {}
          } else {
            stryCov_9fa48("4553");
            findPartyValues(item, `${path}[${idx}]`, results);
          }
        });
      }
    }
    return results;
  }
}
export function parseEventData(eventData: any, templateId: string): ParsedEventData {
  if (stryMutAct_9fa48("4555")) {
    {}
  } else {
    stryCov_9fa48("4555");
    const templateType = stryMutAct_9fa48("4558") ? templateId?.split(':').pop() && '' : stryMutAct_9fa48("4557") ? false : stryMutAct_9fa48("4556") ? true : (stryCov_9fa48("4556", "4557", "4558"), (stryMutAct_9fa48("4559") ? templateId.split(':').pop() : (stryCov_9fa48("4559"), templateId?.split(':').pop())) || '');
    const choice = eventData.choice;
    const eventType = eventData.event_type;

    // Handle Transfer events (exercised_event with AmuletRules_Transfer choice)
    if (stryMutAct_9fa48("4564") ? choice === 'AmuletRules_Transfer' && choice === 'Transfer' : stryMutAct_9fa48("4563") ? false : stryMutAct_9fa48("4562") ? true : (stryCov_9fa48("4562", "4563", "4564"), (stryMutAct_9fa48("4566") ? choice !== 'AmuletRules_Transfer' : stryMutAct_9fa48("4565") ? false : (stryCov_9fa48("4565", "4566"), choice === 'AmuletRules_Transfer')) || (stryMutAct_9fa48("4569") ? choice !== 'Transfer' : stryMutAct_9fa48("4568") ? false : (stryCov_9fa48("4568", "4569"), choice === 'Transfer')))) {
      if (stryMutAct_9fa48("4571")) {
        {}
      } else {
        stryCov_9fa48("4571");
        return parseTransferEvent(eventData);
      }
    }

    // Handle created events
    if (stryMutAct_9fa48("4574") ? eventType !== 'created_event' : stryMutAct_9fa48("4573") ? false : stryMutAct_9fa48("4572") ? true : (stryCov_9fa48("4572", "4573", "4574"), eventType === 'created_event')) {
      if (stryMutAct_9fa48("4576")) {
        {}
      } else {
        stryCov_9fa48("4576");
        return parseCreatedEvent(eventData, templateId, templateType);
      }
    }

    // Fallback: extract all values
    return {
      eventType: stryMutAct_9fa48("4580") ? templateType && 'Unknown Event' : stryMutAct_9fa48("4579") ? false : stryMutAct_9fa48("4578") ? true : (stryCov_9fa48("4578", "4579", "4580"), templateType || 'Unknown Event'),
      details: extractAllFields(eventData)
    };
  }
}
function parseCreatedEvent(eventData: any, templateId: string, templateType: string): ParsedEventData {
  if (stryMutAct_9fa48("4582")) {
    {}
  } else {
    stryCov_9fa48("4582");
    const details: ParsedField[] = stryMutAct_9fa48("4583") ? ["Stryker was here"] : (stryCov_9fa48("4583"), []);

    // Find all numeric and party values
    const numerics = findNumericValues(eventData);
    const parties = findPartyValues(eventData);

    // Determine event type based on template
    let eventTypeName = templateType;
    let primaryAmount: ParsedField | undefined;
    if (stryMutAct_9fa48("4586") ? templateId.includes('Amulet:Amulet') || !templateId.includes('Locked') : stryMutAct_9fa48("4585") ? false : stryMutAct_9fa48("4584") ? true : (stryCov_9fa48("4584", "4585", "4586"), templateId.includes('Amulet:Amulet') && (stryMutAct_9fa48("4588") ? templateId.includes('Locked') : (stryCov_9fa48("4588"), !templateId.includes('Locked'))))) {
      if (stryMutAct_9fa48("4590")) {
        {}
      } else {
        stryCov_9fa48("4590");
        eventTypeName = 'Amulet Created';
        // Primary amount is usually the first numeric value (initial amount)
        if (stryMutAct_9fa48("4595") ? numerics.length <= 0 : stryMutAct_9fa48("4594") ? numerics.length >= 0 : stryMutAct_9fa48("4593") ? false : stryMutAct_9fa48("4592") ? true : (stryCov_9fa48("4592", "4593", "4594", "4595"), numerics.length > 0)) {
          if (stryMutAct_9fa48("4596")) {
            {}
          } else {
            stryCov_9fa48("4596");
            primaryAmount = {
              label: 'Initial Amount',
              value: formatNumeric(numerics[0].value),
              category: 'amount'
            };
          }
        }
      }
    } else if (stryMutAct_9fa48("4601") ? false : stryMutAct_9fa48("4600") ? true : (stryCov_9fa48("4600", "4601"), templateId.includes('LockedAmulet'))) {
      if (stryMutAct_9fa48("4603")) {
        {}
      } else {
        stryCov_9fa48("4603");
        eventTypeName = 'Locked Amulet';
        if (stryMutAct_9fa48("4608") ? numerics.length <= 0 : stryMutAct_9fa48("4607") ? numerics.length >= 0 : stryMutAct_9fa48("4606") ? false : stryMutAct_9fa48("4605") ? true : (stryCov_9fa48("4605", "4606", "4607", "4608"), numerics.length > 0)) {
          if (stryMutAct_9fa48("4609")) {
            {}
          } else {
            stryCov_9fa48("4609");
            primaryAmount = {
              label: 'Locked Amount',
              value: formatNumeric(numerics[0].value),
              category: 'amount'
            };
          }
        }
      }
    } else if (stryMutAct_9fa48("4614") ? false : stryMutAct_9fa48("4613") ? true : (stryCov_9fa48("4613", "4614"), templateId.includes('ValidatorRewardCoupon'))) {
      if (stryMutAct_9fa48("4616")) {
        {}
      } else {
        stryCov_9fa48("4616");
        eventTypeName = 'Validator Reward';
        // Find reward amount (usually first numeric)
        if (stryMutAct_9fa48("4621") ? numerics.length <= 0 : stryMutAct_9fa48("4620") ? numerics.length >= 0 : stryMutAct_9fa48("4619") ? false : stryMutAct_9fa48("4618") ? true : (stryCov_9fa48("4618", "4619", "4620", "4621"), numerics.length > 0)) {
          if (stryMutAct_9fa48("4622")) {
            {}
          } else {
            stryCov_9fa48("4622");
            primaryAmount = {
              label: 'Reward Amount',
              value: formatNumeric(numerics[0].value),
              category: 'amount'
            };
          }
        }
      }
    } else if (stryMutAct_9fa48("4627") ? false : stryMutAct_9fa48("4626") ? true : (stryCov_9fa48("4626", "4627"), templateId.includes('AppRewardCoupon'))) {
      if (stryMutAct_9fa48("4629")) {
        {}
      } else {
        stryCov_9fa48("4629");
        eventTypeName = 'App Reward';
        // Find reward amount
        if (stryMutAct_9fa48("4634") ? numerics.length <= 0 : stryMutAct_9fa48("4633") ? numerics.length >= 0 : stryMutAct_9fa48("4632") ? false : stryMutAct_9fa48("4631") ? true : (stryCov_9fa48("4631", "4632", "4633", "4634"), numerics.length > 0)) {
          if (stryMutAct_9fa48("4635")) {
            {}
          } else {
            stryCov_9fa48("4635");
            primaryAmount = {
              label: 'Reward Amount',
              value: formatNumeric(numerics[0].value),
              category: 'amount'
            };
          }
        }
      }
    } else if (stryMutAct_9fa48("4640") ? false : stryMutAct_9fa48("4639") ? true : (stryCov_9fa48("4639", "4640"), templateId.includes('SvRewardCoupon'))) {
      if (stryMutAct_9fa48("4642")) {
        {}
      } else {
        stryCov_9fa48("4642");
        eventTypeName = 'SV Reward';
        // For SV rewards, show round and weight
        if (stryMutAct_9fa48("4647") ? numerics.length <= 0 : stryMutAct_9fa48("4646") ? numerics.length >= 0 : stryMutAct_9fa48("4645") ? false : stryMutAct_9fa48("4644") ? true : (stryCov_9fa48("4644", "4645", "4646", "4647"), numerics.length > 0)) {
          if (stryMutAct_9fa48("4648")) {
            {}
          } else {
            stryCov_9fa48("4648");
            primaryAmount = {
              label: 'Weight',
              value: formatNumeric(numerics[stryMutAct_9fa48("4651") ? numerics.length + 1 : (stryCov_9fa48("4651"), numerics.length - 1)].value),
              category: 'amount'
            };
          }
        }
      }
    }

    // Add numeric fields (excluding the primary amount)
    numerics.forEach((numeric, idx) => {
      if (stryMutAct_9fa48("4653")) {
        {}
      } else {
        stryCov_9fa48("4653");
        if (stryMutAct_9fa48("4656") ? idx === 0 || primaryAmount : stryMutAct_9fa48("4655") ? false : stryMutAct_9fa48("4654") ? true : (stryCov_9fa48("4654", "4655", "4656"), (stryMutAct_9fa48("4658") ? idx !== 0 : stryMutAct_9fa48("4657") ? true : (stryCov_9fa48("4657", "4658"), idx === 0)) && primaryAmount)) return; // Skip if it's the primary amount

        let label = 'Amount';
        let category: ParsedField['category'] = 'amount';

        // Try to infer label from context
        if (stryMutAct_9fa48("4662") ? false : stryMutAct_9fa48("4661") ? true : (stryCov_9fa48("4661", "4662"), numeric.path.includes('round'))) {
          if (stryMutAct_9fa48("4664")) {
            {}
          } else {
            stryCov_9fa48("4664");
            label = 'Round';
            category = 'metadata';
          }
        } else if (stryMutAct_9fa48("4669") ? numeric.path.includes('rate') && numeric.path.includes('fee') : stryMutAct_9fa48("4668") ? false : stryMutAct_9fa48("4667") ? true : (stryCov_9fa48("4667", "4668", "4669"), numeric.path.includes('rate') || numeric.path.includes('fee'))) {
          if (stryMutAct_9fa48("4672")) {
            {}
          } else {
            stryCov_9fa48("4672");
            label = 'Fee Rate';
            category = 'fee';
          }
        } else if (stryMutAct_9fa48("4676") ? false : stryMutAct_9fa48("4675") ? true : (stryCov_9fa48("4675", "4676"), numeric.path.includes('weight'))) {
          if (stryMutAct_9fa48("4678")) {
            {}
          } else {
            stryCov_9fa48("4678");
            label = 'Weight';
            category = 'amount';
          }
        } else {
          if (stryMutAct_9fa48("4681")) {
            {}
          } else {
            stryCov_9fa48("4681");
            label = `Amount ${stryMutAct_9fa48("4683") ? idx - 1 : (stryCov_9fa48("4683"), idx + 1)}`;
          }
        }
        details.push({
          label,
          value: formatNumeric(numeric.value),
          category
        });
      }
    });

    // Add party fields
    parties.forEach((party, idx) => {
      if (stryMutAct_9fa48("4685")) {
        {}
      } else {
        stryCov_9fa48("4685");
        let label = 'Party';
        if (stryMutAct_9fa48("4689") ? idx !== 0 : stryMutAct_9fa48("4688") ? false : stryMutAct_9fa48("4687") ? true : (stryCov_9fa48("4687", "4688", "4689"), idx === 0)) label = 'DSO';else if (stryMutAct_9fa48("4693") ? idx !== 1 : stryMutAct_9fa48("4692") ? false : stryMutAct_9fa48("4691") ? true : (stryCov_9fa48("4691", "4692", "4693"), idx === 1)) label = templateId.includes('ValidatorReward') ? 'Validator' : templateId.includes('AppReward') ? 'Provider' : templateId.includes('SvReward') ? 'Super Validator' : 'Owner';else if (stryMutAct_9fa48("4703") ? idx === 2 || templateId.includes('SvReward') : stryMutAct_9fa48("4702") ? false : stryMutAct_9fa48("4701") ? true : (stryCov_9fa48("4701", "4702", "4703"), (stryMutAct_9fa48("4705") ? idx !== 2 : stryMutAct_9fa48("4704") ? true : (stryCov_9fa48("4704", "4705"), idx === 2)) && templateId.includes('SvReward'))) label = 'Beneficiary';else label = `Party ${stryMutAct_9fa48("4709") ? idx - 1 : (stryCov_9fa48("4709"), idx + 1)}`;
        details.push({
          label,
          value: formatParty(party.value),
          category: 'party'
        });
      }
    });
    return {
      eventType: eventTypeName,
      primaryAmount,
      details
    };
  }
}
function parseTransferEvent(eventData: any): ParsedEventData {
  if (stryMutAct_9fa48("4713")) {
    {}
  } else {
    stryCov_9fa48("4713");
    const details: ParsedField[] = stryMutAct_9fa48("4714") ? ["Stryker was here"] : (stryCov_9fa48("4714"), []);

    // Extract round from exercise_result
    const exerciseResult = eventData.exercise_result;
    if (stryMutAct_9fa48("4718") ? exerciseResult.record?.fields : stryMutAct_9fa48("4717") ? exerciseResult?.record.fields : stryMutAct_9fa48("4716") ? false : stryMutAct_9fa48("4715") ? true : (stryCov_9fa48("4715", "4716", "4717", "4718"), exerciseResult?.record?.fields)) {
      if (stryMutAct_9fa48("4719")) {
        {}
      } else {
        stryCov_9fa48("4719");
        const fields = exerciseResult.record.fields;

        // Field 0 is usually the round
        if (stryMutAct_9fa48("4727") ? fields[0].value?.record?.fields?.[0]?.value?.int64 : stryMutAct_9fa48("4726") ? fields[0]?.value.record?.fields?.[0]?.value?.int64 : stryMutAct_9fa48("4725") ? fields[0]?.value?.record.fields?.[0]?.value?.int64 : stryMutAct_9fa48("4724") ? fields[0]?.value?.record?.fields[0]?.value?.int64 : stryMutAct_9fa48("4723") ? fields[0]?.value?.record?.fields?.[0].value?.int64 : stryMutAct_9fa48("4722") ? fields[0]?.value?.record?.fields?.[0]?.value.int64 : stryMutAct_9fa48("4721") ? false : stryMutAct_9fa48("4720") ? true : (stryCov_9fa48("4720", "4721", "4722", "4723", "4724", "4725", "4726", "4727"), fields[0]?.value?.record?.fields?.[0]?.value?.int64)) {
          if (stryMutAct_9fa48("4728")) {
            {}
          } else {
            stryCov_9fa48("4728");
            details.push({
              label: 'Round',
              value: fields[0].value.record.fields[0].value.int64.toString(),
              category: 'metadata'
            });
          }
        }

        // Field 1 contains balance changes
        if (stryMutAct_9fa48("4736") ? fields[1].value?.record?.fields : stryMutAct_9fa48("4735") ? fields[1]?.value.record?.fields : stryMutAct_9fa48("4734") ? fields[1]?.value?.record.fields : stryMutAct_9fa48("4733") ? false : stryMutAct_9fa48("4732") ? true : (stryCov_9fa48("4732", "4733", "4734", "4735", "4736"), fields[1]?.value?.record?.fields)) {
          if (stryMutAct_9fa48("4737")) {
            {}
          } else {
            stryCov_9fa48("4737");
            const balanceFields = fields[1].value.record.fields;
            const balanceLabels = stryMutAct_9fa48("4738") ? [] : (stryCov_9fa48("4738"), [{
              label: 'Holding Fees',
              category: 'fee' as const
            }, {
              label: 'Sender Change Fee',
              category: 'fee' as const
            }, {
              label: 'Output Fees',
              category: 'fee' as const
            }, {
              label: 'Sender Balance (USD)',
              category: 'metadata' as const
            }]);
            stryMutAct_9fa48("4747") ? balanceFields.forEach((field: any, idx: number) => {
              const value = extractValue(field);
              if (value !== undefined && value !== null) {
                details.push({
                  label: balanceLabels[idx]?.label || `Value ${idx}`,
                  value: formatNumeric(value),
                  category: balanceLabels[idx]?.category || 'amount'
                });
              }
            }) : (stryCov_9fa48("4747"), balanceFields.slice(0, 4).forEach((field: any, idx: number) => {
              if (stryMutAct_9fa48("4748")) {
                {}
              } else {
                stryCov_9fa48("4748");
                const value = extractValue(field);
                if (stryMutAct_9fa48("4751") ? value !== undefined || value !== null : stryMutAct_9fa48("4750") ? false : stryMutAct_9fa48("4749") ? true : (stryCov_9fa48("4749", "4750", "4751"), (stryMutAct_9fa48("4753") ? value === undefined : stryMutAct_9fa48("4752") ? true : (stryCov_9fa48("4752", "4753"), value !== undefined)) && (stryMutAct_9fa48("4755") ? value === null : stryMutAct_9fa48("4754") ? true : (stryCov_9fa48("4754", "4755"), value !== null)))) {
                  if (stryMutAct_9fa48("4756")) {
                    {}
                  } else {
                    stryCov_9fa48("4756");
                    details.push({
                      label: stryMutAct_9fa48("4760") ? balanceLabels[idx]?.label && `Value ${idx}` : stryMutAct_9fa48("4759") ? false : stryMutAct_9fa48("4758") ? true : (stryCov_9fa48("4758", "4759", "4760"), (stryMutAct_9fa48("4761") ? balanceLabels[idx].label : (stryCov_9fa48("4761"), balanceLabels[idx]?.label)) || `Value ${idx}`),
                      value: formatNumeric(value),
                      category: stryMutAct_9fa48("4765") ? balanceLabels[idx]?.category && 'amount' : stryMutAct_9fa48("4764") ? false : stryMutAct_9fa48("4763") ? true : (stryCov_9fa48("4763", "4764", "4765"), (stryMutAct_9fa48("4766") ? balanceLabels[idx].category : (stryCov_9fa48("4766"), balanceLabels[idx]?.category)) || 'amount')
                    });
                  }
                }
              }
            }));
          }
        }
      }
    }

    // Extract parties from choice_argument
    const choiceArg = eventData.choice_argument;
    if (stryMutAct_9fa48("4771") ? choiceArg.record?.fields : stryMutAct_9fa48("4770") ? choiceArg?.record.fields : stryMutAct_9fa48("4769") ? false : stryMutAct_9fa48("4768") ? true : (stryCov_9fa48("4768", "4769", "4770", "4771"), choiceArg?.record?.fields)) {
      if (stryMutAct_9fa48("4772")) {
        {}
      } else {
        stryCov_9fa48("4772");
        const argFields = choiceArg.record.fields;

        // Field 0 usually contains transfer details with sender/receiver
        if (stryMutAct_9fa48("4777") ? argFields[0].value?.record?.fields : stryMutAct_9fa48("4776") ? argFields[0]?.value.record?.fields : stryMutAct_9fa48("4775") ? argFields[0]?.value?.record.fields : stryMutAct_9fa48("4774") ? false : stryMutAct_9fa48("4773") ? true : (stryCov_9fa48("4773", "4774", "4775", "4776", "4777"), argFields[0]?.value?.record?.fields)) {
          if (stryMutAct_9fa48("4778")) {
            {}
          } else {
            stryCov_9fa48("4778");
            const transferFields = argFields[0].value.record.fields;

            // Field 0 is sender, field 1 is provider
            if (stryMutAct_9fa48("4782") ? transferFields[0].value?.party : stryMutAct_9fa48("4781") ? transferFields[0]?.value.party : stryMutAct_9fa48("4780") ? false : stryMutAct_9fa48("4779") ? true : (stryCov_9fa48("4779", "4780", "4781", "4782"), transferFields[0]?.value?.party)) {
              if (stryMutAct_9fa48("4783")) {
                {}
              } else {
                stryCov_9fa48("4783");
                details.push({
                  label: 'Sender',
                  value: formatParty(transferFields[0].value.party),
                  category: 'party'
                });
              }
            }

            // Field 3 contains receivers list
            if (stryMutAct_9fa48("4791") ? transferFields[3].value?.list?.elements : stryMutAct_9fa48("4790") ? transferFields[3]?.value.list?.elements : stryMutAct_9fa48("4789") ? transferFields[3]?.value?.list.elements : stryMutAct_9fa48("4788") ? false : stryMutAct_9fa48("4787") ? true : (stryCov_9fa48("4787", "4788", "4789", "4790", "4791"), transferFields[3]?.value?.list?.elements)) {
              if (stryMutAct_9fa48("4792")) {
                {}
              } else {
                stryCov_9fa48("4792");
                const receivers = transferFields[3].value.list.elements;
                receivers.forEach((receiver: any, idx: number) => {
                  if (stryMutAct_9fa48("4793")) {
                    {}
                  } else {
                    stryCov_9fa48("4793");
                    if (stryMutAct_9fa48("4796") ? receiver.record.fields : stryMutAct_9fa48("4795") ? false : stryMutAct_9fa48("4794") ? true : (stryCov_9fa48("4794", "4795", "4796"), receiver.record?.fields)) {
                      if (stryMutAct_9fa48("4797")) {
                        {}
                      } else {
                        stryCov_9fa48("4797");
                        const partyField = receiver.record.fields[0];
                        const amountField = receiver.record.fields[1];
                        const usdField = receiver.record.fields[2];
                        if (stryMutAct_9fa48("4801") ? partyField.value?.party : stryMutAct_9fa48("4800") ? partyField?.value.party : stryMutAct_9fa48("4799") ? false : stryMutAct_9fa48("4798") ? true : (stryCov_9fa48("4798", "4799", "4800", "4801"), partyField?.value?.party)) {
                          if (stryMutAct_9fa48("4802")) {
                            {}
                          } else {
                            stryCov_9fa48("4802");
                            details.push({
                              label: (stryMutAct_9fa48("4806") ? idx !== 0 : stryMutAct_9fa48("4805") ? false : stryMutAct_9fa48("4804") ? true : (stryCov_9fa48("4804", "4805", "4806"), idx === 0)) ? 'Receiver' : `Receiver ${stryMutAct_9fa48("4809") ? idx - 1 : (stryCov_9fa48("4809"), idx + 1)}`,
                              value: formatParty(partyField.value.party),
                              category: 'party'
                            });
                          }
                        }
                        if (stryMutAct_9fa48("4814") ? amountField.value?.numeric : stryMutAct_9fa48("4813") ? amountField?.value.numeric : stryMutAct_9fa48("4812") ? false : stryMutAct_9fa48("4811") ? true : (stryCov_9fa48("4811", "4812", "4813", "4814"), amountField?.value?.numeric)) {
                          if (stryMutAct_9fa48("4815")) {
                            {}
                          } else {
                            stryCov_9fa48("4815");
                            details.push({
                              label: `Amount to ${(stryMutAct_9fa48("4820") ? idx !== 0 : stryMutAct_9fa48("4819") ? false : stryMutAct_9fa48("4818") ? true : (stryCov_9fa48("4818", "4819", "4820"), idx === 0)) ? 'Receiver' : `Receiver ${stryMutAct_9fa48("4823") ? idx - 1 : (stryCov_9fa48("4823"), idx + 1)}`}`,
                              value: formatNumeric(amountField.value.numeric),
                              category: 'amount'
                            });
                          }
                        }
                      }
                    }
                  }
                });
              }
            }
          }
        }
      }
    }

    // Find primary transfer amount (first receiver amount)
    const primaryAmountField = details.find(stryMutAct_9fa48("4825") ? () => undefined : (stryCov_9fa48("4825"), f => f.label.includes('Amount to')));
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
}
function extractAllFields(eventData: any): ParsedField[] {
  if (stryMutAct_9fa48("4831")) {
    {}
  } else {
    stryCov_9fa48("4831");
    const details: ParsedField[] = stryMutAct_9fa48("4832") ? ["Stryker was here"] : (stryCov_9fa48("4832"), []);
    const numerics = findNumericValues(eventData);
    const parties = findPartyValues(eventData);
    numerics.forEach((numeric, idx) => {
      if (stryMutAct_9fa48("4833")) {
        {}
      } else {
        stryCov_9fa48("4833");
        details.push({
          label: `Numeric ${stryMutAct_9fa48("4836") ? idx - 1 : (stryCov_9fa48("4836"), idx + 1)}`,
          value: formatNumeric(numeric.value),
          category: 'amount'
        });
      }
    });
    parties.forEach((party, idx) => {
      if (stryMutAct_9fa48("4838")) {
        {}
      } else {
        stryCov_9fa48("4838");
        details.push({
          label: `Party ${stryMutAct_9fa48("4841") ? idx - 1 : (stryCov_9fa48("4841"), idx + 1)}`,
          value: formatParty(party.value),
          category: 'party'
        });
      }
    });
    return details;
  }
}