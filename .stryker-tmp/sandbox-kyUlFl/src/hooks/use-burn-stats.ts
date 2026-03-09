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
import { scanApi } from "@/lib/api-client";

/**
 * Calculate total burnt Canton Coin from transaction events.
 * Based on Canton Network documentation for computing burnt tokens.
 *
 * Sources of burn:
 * 1. Traffic purchases (AmuletRules_BuyMemberTraffic): holdingFees + senderChangeFee + amuletPaid
 * 2. Transfers (AmuletRules_Transfer): holdingFees + outputFees + senderChangeFee
 * 3. CNS entries (SubscriptionInitialPayment_Collect): temp Amulet amount + transfer fees
 * 4. Pre-approvals (AmuletRules_CreateTransferPreapproval): amuletPaid + outputFees + senderChangeFee + holdingFees
 */

interface BurnCalculationResult {
  totalBurn: number;
  trafficBurn: number;
  transferBurn: number;
  cnsBurn: number;
  preapprovalBurn: number;
}

/**
 * Parse a single exercised event for burn calculation
 */
function calculateBurnFromEvent(event: any, eventsById: Record<string, any>): Partial<BurnCalculationResult> {
  if (stryMutAct_9fa48("1108")) {
    {}
  } else {
    stryCov_9fa48("1108");
    const result: Partial<BurnCalculationResult> = {
      trafficBurn: 0,
      transferBurn: 0,
      cnsBurn: 0,
      preapprovalBurn: 0
    };
    if (stryMutAct_9fa48("1112") ? event.event_type === "exercised_event" : stryMutAct_9fa48("1111") ? false : stryMutAct_9fa48("1110") ? true : (stryCov_9fa48("1110", "1111", "1112"), event.event_type !== "exercised_event")) return result;
    const choice = event.choice;
    const exerciseResult = event.exercise_result;
    const summary = stryMutAct_9fa48("1114") ? exerciseResult.summary : (stryCov_9fa48("1114"), exerciseResult?.summary);

    // 1. Traffic Purchases: AmuletRules_BuyMemberTraffic
    if (stryMutAct_9fa48("1117") ? choice === "AmuletRules_BuyMemberTraffic" || summary : stryMutAct_9fa48("1116") ? false : stryMutAct_9fa48("1115") ? true : (stryCov_9fa48("1115", "1116", "1117"), (stryMutAct_9fa48("1119") ? choice !== "AmuletRules_BuyMemberTraffic" : stryMutAct_9fa48("1118") ? true : (stryCov_9fa48("1118", "1119"), choice === "AmuletRules_BuyMemberTraffic")) && summary)) {
      if (stryMutAct_9fa48("1121")) {
        {}
      } else {
        stryCov_9fa48("1121");
        const holdingFees = parseFloat(stryMutAct_9fa48("1124") ? summary.holdingFees && "0" : stryMutAct_9fa48("1123") ? false : stryMutAct_9fa48("1122") ? true : (stryCov_9fa48("1122", "1123", "1124"), summary.holdingFees || "0"));
        const senderChangeFee = parseFloat(stryMutAct_9fa48("1128") ? summary.senderChangeFee && "0" : stryMutAct_9fa48("1127") ? false : stryMutAct_9fa48("1126") ? true : (stryCov_9fa48("1126", "1127", "1128"), summary.senderChangeFee || "0"));
        const amuletPaid = parseFloat(stryMutAct_9fa48("1132") ? exerciseResult.amuletPaid && "0" : stryMutAct_9fa48("1131") ? false : stryMutAct_9fa48("1130") ? true : (stryCov_9fa48("1130", "1131", "1132"), exerciseResult.amuletPaid || "0"));
        result.trafficBurn = stryMutAct_9fa48("1134") ? holdingFees + senderChangeFee - amuletPaid : (stryCov_9fa48("1134"), (stryMutAct_9fa48("1135") ? holdingFees - senderChangeFee : (stryCov_9fa48("1135"), holdingFees + senderChangeFee)) + amuletPaid);
      }
    } // 2. Transfers: AmuletRules_Transfer
    else if (stryMutAct_9fa48("1138") ? choice === "AmuletRules_Transfer" || summary : stryMutAct_9fa48("1137") ? false : stryMutAct_9fa48("1136") ? true : (stryCov_9fa48("1136", "1137", "1138"), (stryMutAct_9fa48("1140") ? choice !== "AmuletRules_Transfer" : stryMutAct_9fa48("1139") ? true : (stryCov_9fa48("1139", "1140"), choice === "AmuletRules_Transfer")) && summary)) {
      if (stryMutAct_9fa48("1142")) {
        {}
      } else {
        stryCov_9fa48("1142");
        const holdingFees = parseFloat(stryMutAct_9fa48("1145") ? summary.holdingFees && "0" : stryMutAct_9fa48("1144") ? false : stryMutAct_9fa48("1143") ? true : (stryCov_9fa48("1143", "1144", "1145"), summary.holdingFees || "0"));
        const senderChangeFee = parseFloat(stryMutAct_9fa48("1149") ? summary.senderChangeFee && "0" : stryMutAct_9fa48("1148") ? false : stryMutAct_9fa48("1147") ? true : (stryCov_9fa48("1147", "1148", "1149"), summary.senderChangeFee || "0"));

        // Sum all outputFees
        let outputFeesTotal = 0;
        if (stryMutAct_9fa48("1152") ? false : stryMutAct_9fa48("1151") ? true : (stryCov_9fa48("1151", "1152"), Array.isArray(summary.outputFees))) {
          if (stryMutAct_9fa48("1153")) {
            {}
          } else {
            stryCov_9fa48("1153");
            for (const fee of summary.outputFees) {
              if (stryMutAct_9fa48("1154")) {
                {}
              } else {
                stryCov_9fa48("1154");
                stryMutAct_9fa48("1155") ? outputFeesTotal -= parseFloat(fee || "0") : (stryCov_9fa48("1155"), outputFeesTotal += parseFloat(stryMutAct_9fa48("1158") ? fee && "0" : stryMutAct_9fa48("1157") ? false : stryMutAct_9fa48("1156") ? true : (stryCov_9fa48("1156", "1157", "1158"), fee || "0")));
              }
            }
          }
        }
        result.transferBurn = stryMutAct_9fa48("1160") ? holdingFees + senderChangeFee - outputFeesTotal : (stryCov_9fa48("1160"), (stryMutAct_9fa48("1161") ? holdingFees - senderChangeFee : (stryCov_9fa48("1161"), holdingFees + senderChangeFee)) + outputFeesTotal);
      }
    } // 3. CNS Entry Purchases: SubscriptionInitialPayment_Collect
    else if (stryMutAct_9fa48("1164") ? choice === "SubscriptionInitialPayment_Collect" || exerciseResult : stryMutAct_9fa48("1163") ? false : stryMutAct_9fa48("1162") ? true : (stryCov_9fa48("1162", "1163", "1164"), (stryMutAct_9fa48("1166") ? choice !== "SubscriptionInitialPayment_Collect" : stryMutAct_9fa48("1165") ? true : (stryCov_9fa48("1165", "1166"), choice === "SubscriptionInitialPayment_Collect")) && exerciseResult)) {
      if (stryMutAct_9fa48("1168")) {
        {}
      } else {
        stryCov_9fa48("1168");
        // Find the temporary Amulet contract that was created and burnt
        const amuletContractId = exerciseResult.amulet;
        if (stryMutAct_9fa48("1171") ? amuletContractId || eventsById[amuletContractId] : stryMutAct_9fa48("1170") ? false : stryMutAct_9fa48("1169") ? true : (stryCov_9fa48("1169", "1170", "1171"), amuletContractId && eventsById[amuletContractId])) {
          if (stryMutAct_9fa48("1172")) {
            {}
          } else {
            stryCov_9fa48("1172");
            const amuletEvent = eventsById[amuletContractId];
            if (stryMutAct_9fa48("1175") ? amuletEvent.event_type !== "created_event" : stryMutAct_9fa48("1174") ? false : stryMutAct_9fa48("1173") ? true : (stryCov_9fa48("1173", "1174", "1175"), amuletEvent.event_type === "created_event")) {
              if (stryMutAct_9fa48("1177")) {
                {}
              } else {
                stryCov_9fa48("1177");
                const amount = stryMutAct_9fa48("1179") ? amuletEvent.create_arguments.amount?.initialAmount : stryMutAct_9fa48("1178") ? amuletEvent.create_arguments?.amount.initialAmount : (stryCov_9fa48("1178", "1179"), amuletEvent.create_arguments?.amount?.initialAmount);
                if (stryMutAct_9fa48("1181") ? false : stryMutAct_9fa48("1180") ? true : (stryCov_9fa48("1180", "1181"), amount)) {
                  if (stryMutAct_9fa48("1182")) {
                    {}
                  } else {
                    stryCov_9fa48("1182");
                    result.cnsBurn = parseFloat(amount);
                  }
                }
              }
            }
          }
        }

        // Also look for child transfer events to add transfer fees
        if (stryMutAct_9fa48("1184") ? false : stryMutAct_9fa48("1183") ? true : (stryCov_9fa48("1183", "1184"), Array.isArray(event.child_event_ids))) {
          if (stryMutAct_9fa48("1185")) {
            {}
          } else {
            stryCov_9fa48("1185");
            for (const childId of event.child_event_ids) {
              if (stryMutAct_9fa48("1186")) {
                {}
              } else {
                stryCov_9fa48("1186");
                const childEvent = eventsById[childId];
                if (stryMutAct_9fa48("1189") ? childEvent?.choice !== "AmuletRules_Transfer" : stryMutAct_9fa48("1188") ? false : stryMutAct_9fa48("1187") ? true : (stryCov_9fa48("1187", "1188", "1189"), (stryMutAct_9fa48("1190") ? childEvent.choice : (stryCov_9fa48("1190"), childEvent?.choice)) === "AmuletRules_Transfer")) {
                  if (stryMutAct_9fa48("1192")) {
                    {}
                  } else {
                    stryCov_9fa48("1192");
                    const childBurn = calculateBurnFromEvent(childEvent, eventsById);
                    result.cnsBurn = stryMutAct_9fa48("1193") ? (result.cnsBurn || 0) - (childBurn.transferBurn || 0) : (stryCov_9fa48("1193"), (stryMutAct_9fa48("1196") ? result.cnsBurn && 0 : stryMutAct_9fa48("1195") ? false : stryMutAct_9fa48("1194") ? true : (stryCov_9fa48("1194", "1195", "1196"), result.cnsBurn || 0)) + (stryMutAct_9fa48("1199") ? childBurn.transferBurn && 0 : stryMutAct_9fa48("1198") ? false : stryMutAct_9fa48("1197") ? true : (stryCov_9fa48("1197", "1198", "1199"), childBurn.transferBurn || 0)));
                  }
                }
              }
            }
          }
        }
      }
    } // 4. CNS Entry Renewals: AnsEntryContext_CollectRenewalEntryPayment
    else if (stryMutAct_9fa48("1202") ? choice === "AnsEntryContext_CollectRenewalEntryPayment" || exerciseResult : stryMutAct_9fa48("1201") ? false : stryMutAct_9fa48("1200") ? true : (stryCov_9fa48("1200", "1201", "1202"), (stryMutAct_9fa48("1204") ? choice !== "AnsEntryContext_CollectRenewalEntryPayment" : stryMutAct_9fa48("1203") ? true : (stryCov_9fa48("1203", "1204"), choice === "AnsEntryContext_CollectRenewalEntryPayment")) && exerciseResult)) {
      if (stryMutAct_9fa48("1206")) {
        {}
      } else {
        stryCov_9fa48("1206");
        // Similar logic to SubscriptionInitialPayment_Collect
        const amuletContractId = exerciseResult.amulet;
        if (stryMutAct_9fa48("1209") ? amuletContractId || eventsById[amuletContractId] : stryMutAct_9fa48("1208") ? false : stryMutAct_9fa48("1207") ? true : (stryCov_9fa48("1207", "1208", "1209"), amuletContractId && eventsById[amuletContractId])) {
          if (stryMutAct_9fa48("1210")) {
            {}
          } else {
            stryCov_9fa48("1210");
            const amuletEvent = eventsById[amuletContractId];
            if (stryMutAct_9fa48("1213") ? amuletEvent.event_type !== "created_event" : stryMutAct_9fa48("1212") ? false : stryMutAct_9fa48("1211") ? true : (stryCov_9fa48("1211", "1212", "1213"), amuletEvent.event_type === "created_event")) {
              if (stryMutAct_9fa48("1215")) {
                {}
              } else {
                stryCov_9fa48("1215");
                const amount = stryMutAct_9fa48("1217") ? amuletEvent.create_arguments.amount?.initialAmount : stryMutAct_9fa48("1216") ? amuletEvent.create_arguments?.amount.initialAmount : (stryCov_9fa48("1216", "1217"), amuletEvent.create_arguments?.amount?.initialAmount);
                if (stryMutAct_9fa48("1219") ? false : stryMutAct_9fa48("1218") ? true : (stryCov_9fa48("1218", "1219"), amount)) {
                  if (stryMutAct_9fa48("1220")) {
                    {}
                  } else {
                    stryCov_9fa48("1220");
                    result.cnsBurn = parseFloat(amount);
                  }
                }
              }
            }
          }
        }
        if (stryMutAct_9fa48("1222") ? false : stryMutAct_9fa48("1221") ? true : (stryCov_9fa48("1221", "1222"), Array.isArray(event.child_event_ids))) {
          if (stryMutAct_9fa48("1223")) {
            {}
          } else {
            stryCov_9fa48("1223");
            for (const childId of event.child_event_ids) {
              if (stryMutAct_9fa48("1224")) {
                {}
              } else {
                stryCov_9fa48("1224");
                const childEvent = eventsById[childId];
                if (stryMutAct_9fa48("1227") ? childEvent?.choice !== "AmuletRules_Transfer" : stryMutAct_9fa48("1226") ? false : stryMutAct_9fa48("1225") ? true : (stryCov_9fa48("1225", "1226", "1227"), (stryMutAct_9fa48("1228") ? childEvent.choice : (stryCov_9fa48("1228"), childEvent?.choice)) === "AmuletRules_Transfer")) {
                  if (stryMutAct_9fa48("1230")) {
                    {}
                  } else {
                    stryCov_9fa48("1230");
                    const childBurn = calculateBurnFromEvent(childEvent, eventsById);
                    result.cnsBurn = stryMutAct_9fa48("1231") ? (result.cnsBurn || 0) - (childBurn.transferBurn || 0) : (stryCov_9fa48("1231"), (stryMutAct_9fa48("1234") ? result.cnsBurn && 0 : stryMutAct_9fa48("1233") ? false : stryMutAct_9fa48("1232") ? true : (stryCov_9fa48("1232", "1233", "1234"), result.cnsBurn || 0)) + (stryMutAct_9fa48("1237") ? childBurn.transferBurn && 0 : stryMutAct_9fa48("1236") ? false : stryMutAct_9fa48("1235") ? true : (stryCov_9fa48("1235", "1236", "1237"), childBurn.transferBurn || 0)));
                  }
                }
              }
            }
          }
        }
      }
    } // 5. Pre-approvals: AmuletRules_CreateTransferPreapproval, AmuletRules_CreateExternalPartySetupProposal, TransferPreapproval_Renew
    else if (stryMutAct_9fa48("1240") ? choice === "AmuletRules_CreateTransferPreapproval" || choice === "AmuletRules_CreateExternalPartySetupProposal" || choice === "TransferPreapproval_Renew" || exerciseResult?.transferResult : stryMutAct_9fa48("1239") ? false : stryMutAct_9fa48("1238") ? true : (stryCov_9fa48("1238", "1239", "1240"), (stryMutAct_9fa48("1242") ? (choice === "AmuletRules_CreateTransferPreapproval" || choice === "AmuletRules_CreateExternalPartySetupProposal") && choice === "TransferPreapproval_Renew" : stryMutAct_9fa48("1241") ? true : (stryCov_9fa48("1241", "1242"), (stryMutAct_9fa48("1244") ? choice === "AmuletRules_CreateTransferPreapproval" && choice === "AmuletRules_CreateExternalPartySetupProposal" : stryMutAct_9fa48("1243") ? false : (stryCov_9fa48("1243", "1244"), (stryMutAct_9fa48("1246") ? choice !== "AmuletRules_CreateTransferPreapproval" : stryMutAct_9fa48("1245") ? false : (stryCov_9fa48("1245", "1246"), choice === "AmuletRules_CreateTransferPreapproval")) || (stryMutAct_9fa48("1249") ? choice !== "AmuletRules_CreateExternalPartySetupProposal" : stryMutAct_9fa48("1248") ? false : (stryCov_9fa48("1248", "1249"), choice === "AmuletRules_CreateExternalPartySetupProposal")))) || (stryMutAct_9fa48("1252") ? choice !== "TransferPreapproval_Renew" : stryMutAct_9fa48("1251") ? false : (stryCov_9fa48("1251", "1252"), choice === "TransferPreapproval_Renew")))) && (stryMutAct_9fa48("1254") ? exerciseResult.transferResult : (stryCov_9fa48("1254"), exerciseResult?.transferResult)))) {
      if (stryMutAct_9fa48("1255")) {
        {}
      } else {
        stryCov_9fa48("1255");
        const transferResult = exerciseResult.transferResult;
        const summary = transferResult.summary;
        if (stryMutAct_9fa48("1257") ? false : stryMutAct_9fa48("1256") ? true : (stryCov_9fa48("1256", "1257"), summary)) {
          if (stryMutAct_9fa48("1258")) {
            {}
          } else {
            stryCov_9fa48("1258");
            const amuletPaid = parseFloat(stryMutAct_9fa48("1261") ? exerciseResult.amuletPaid && "0" : stryMutAct_9fa48("1260") ? false : stryMutAct_9fa48("1259") ? true : (stryCov_9fa48("1259", "1260", "1261"), exerciseResult.amuletPaid || "0"));
            const holdingFees = parseFloat(stryMutAct_9fa48("1265") ? summary.holdingFees && "0" : stryMutAct_9fa48("1264") ? false : stryMutAct_9fa48("1263") ? true : (stryCov_9fa48("1263", "1264", "1265"), summary.holdingFees || "0"));
            const senderChangeFee = parseFloat(stryMutAct_9fa48("1269") ? summary.senderChangeFee && "0" : stryMutAct_9fa48("1268") ? false : stryMutAct_9fa48("1267") ? true : (stryCov_9fa48("1267", "1268", "1269"), summary.senderChangeFee || "0"));

            // Sum all outputFees
            let outputFeesTotal = 0;
            if (stryMutAct_9fa48("1272") ? false : stryMutAct_9fa48("1271") ? true : (stryCov_9fa48("1271", "1272"), Array.isArray(summary.outputFees))) {
              if (stryMutAct_9fa48("1273")) {
                {}
              } else {
                stryCov_9fa48("1273");
                for (const fee of summary.outputFees) {
                  if (stryMutAct_9fa48("1274")) {
                    {}
                  } else {
                    stryCov_9fa48("1274");
                    stryMutAct_9fa48("1275") ? outputFeesTotal -= parseFloat(fee || "0") : (stryCov_9fa48("1275"), outputFeesTotal += parseFloat(stryMutAct_9fa48("1278") ? fee && "0" : stryMutAct_9fa48("1277") ? false : stryMutAct_9fa48("1276") ? true : (stryCov_9fa48("1276", "1277", "1278"), fee || "0")));
                  }
                }
              }
            }

            // Note: outputFee is NOT included in amuletPaid for pre-approvals (unlike traffic purchases)
            result.preapprovalBurn = stryMutAct_9fa48("1280") ? amuletPaid + holdingFees + senderChangeFee - outputFeesTotal : (stryCov_9fa48("1280"), (stryMutAct_9fa48("1281") ? amuletPaid + holdingFees - senderChangeFee : (stryCov_9fa48("1281"), (stryMutAct_9fa48("1282") ? amuletPaid - holdingFees : (stryCov_9fa48("1282"), amuletPaid + holdingFees)) + senderChangeFee)) + outputFeesTotal);
          }
        }
      }
    }
    return result;
  }
}

/**
 * Calculate total burn from all events in a transaction
 */
function calculateBurnFromTransaction(transaction: any): BurnCalculationResult {
  if (stryMutAct_9fa48("1283")) {
    {}
  } else {
    stryCov_9fa48("1283");
    const result: BurnCalculationResult = {
      totalBurn: 0,
      trafficBurn: 0,
      transferBurn: 0,
      cnsBurn: 0,
      preapprovalBurn: 0
    };
    if (stryMutAct_9fa48("1287") ? false : stryMutAct_9fa48("1286") ? true : stryMutAct_9fa48("1285") ? transaction.events_by_id : (stryCov_9fa48("1285", "1286", "1287"), !transaction.events_by_id)) return result;
    const eventsById = transaction.events_by_id;

    // Process all events
    for (const eventId of Object.keys(eventsById)) {
      if (stryMutAct_9fa48("1288")) {
        {}
      } else {
        stryCov_9fa48("1288");
        const event = eventsById[eventId];
        const eventBurn = calculateBurnFromEvent(event, eventsById);
        stryMutAct_9fa48("1289") ? result.trafficBurn -= eventBurn.trafficBurn || 0 : (stryCov_9fa48("1289"), result.trafficBurn += stryMutAct_9fa48("1292") ? eventBurn.trafficBurn && 0 : stryMutAct_9fa48("1291") ? false : stryMutAct_9fa48("1290") ? true : (stryCov_9fa48("1290", "1291", "1292"), eventBurn.trafficBurn || 0));
        stryMutAct_9fa48("1293") ? result.transferBurn -= eventBurn.transferBurn || 0 : (stryCov_9fa48("1293"), result.transferBurn += stryMutAct_9fa48("1296") ? eventBurn.transferBurn && 0 : stryMutAct_9fa48("1295") ? false : stryMutAct_9fa48("1294") ? true : (stryCov_9fa48("1294", "1295", "1296"), eventBurn.transferBurn || 0));
        stryMutAct_9fa48("1297") ? result.cnsBurn -= eventBurn.cnsBurn || 0 : (stryCov_9fa48("1297"), result.cnsBurn += stryMutAct_9fa48("1300") ? eventBurn.cnsBurn && 0 : stryMutAct_9fa48("1299") ? false : stryMutAct_9fa48("1298") ? true : (stryCov_9fa48("1298", "1299", "1300"), eventBurn.cnsBurn || 0));
        stryMutAct_9fa48("1301") ? result.preapprovalBurn -= eventBurn.preapprovalBurn || 0 : (stryCov_9fa48("1301"), result.preapprovalBurn += stryMutAct_9fa48("1304") ? eventBurn.preapprovalBurn && 0 : stryMutAct_9fa48("1303") ? false : stryMutAct_9fa48("1302") ? true : (stryCov_9fa48("1302", "1303", "1304"), eventBurn.preapprovalBurn || 0));
      }
    }
    result.totalBurn = stryMutAct_9fa48("1305") ? result.trafficBurn + result.transferBurn + result.cnsBurn - result.preapprovalBurn : (stryCov_9fa48("1305"), (stryMutAct_9fa48("1306") ? result.trafficBurn + result.transferBurn - result.cnsBurn : (stryCov_9fa48("1306"), (stryMutAct_9fa48("1307") ? result.trafficBurn - result.transferBurn : (stryCov_9fa48("1307"), result.trafficBurn + result.transferBurn)) + result.cnsBurn)) + result.preapprovalBurn);
    return result;
  }
}
interface UseBurnStatsOptions {
  /** Number of days to look back (default: 1 for 24h) */
  days?: number;
}
export function useBurnStats(options: UseBurnStatsOptions = {}) {
  if (stryMutAct_9fa48("1308")) {
    {}
  } else {
    stryCov_9fa48("1308");
    const {
      days = 1
    } = options;
    const {
      data: latestRound
    } = useQuery({
      queryKey: stryMutAct_9fa48("1310") ? [] : (stryCov_9fa48("1310"), ["latestRound"]),
      queryFn: stryMutAct_9fa48("1312") ? () => undefined : (stryCov_9fa48("1312"), () => scanApi.fetchLatestRound()),
      staleTime: 60_000
    });
    return useQuery({
      queryKey: stryMutAct_9fa48("1314") ? [] : (stryCov_9fa48("1314"), ["burnStats", stryMutAct_9fa48("1316") ? latestRound.round : (stryCov_9fa48("1316"), latestRound?.round), days]),
      queryFn: async () => {
        if (stryMutAct_9fa48("1317")) {
          {}
        } else {
          stryCov_9fa48("1317");
          if (stryMutAct_9fa48("1320") ? false : stryMutAct_9fa48("1319") ? true : stryMutAct_9fa48("1318") ? latestRound : (stryCov_9fa48("1318", "1319", "1320"), !latestRound)) return null;

          // Calculate the time range
          const now = new Date(latestRound.effectiveAt);
          const startTime = new Date(stryMutAct_9fa48("1321") ? now.getTime() + days * 24 * 60 * 60 * 1000 : (stryCov_9fa48("1321"), now.getTime() - (stryMutAct_9fa48("1322") ? days * 24 * 60 * 60 / 1000 : (stryCov_9fa48("1322"), (stryMutAct_9fa48("1323") ? days * 24 * 60 / 60 : (stryCov_9fa48("1323"), (stryMutAct_9fa48("1324") ? days * 24 / 60 : (stryCov_9fa48("1324"), (stryMutAct_9fa48("1325") ? days / 24 : (stryCov_9fa48("1325"), days * 24)) * 60)) * 60)) * 1000))));
          const result: BurnCalculationResult & {
            byDay: Record<string, BurnCalculationResult>;
          } = {
            totalBurn: 0,
            trafficBurn: 0,
            transferBurn: 0,
            cnsBurn: 0,
            preapprovalBurn: 0,
            byDay: {}
          };

          // Fetch transactions page by page
          let hasMore = stryMutAct_9fa48("1327") ? false : (stryCov_9fa48("1327"), true);
          let pageEndEventId: string | undefined;
          const maxPages = 100; // Safety limit
          let pagesProcessed = 0;
          while (stryMutAct_9fa48("1329") ? hasMore || pagesProcessed < maxPages : stryMutAct_9fa48("1328") ? false : (stryCov_9fa48("1328", "1329"), hasMore && (stryMutAct_9fa48("1332") ? pagesProcessed >= maxPages : stryMutAct_9fa48("1331") ? pagesProcessed <= maxPages : stryMutAct_9fa48("1330") ? true : (stryCov_9fa48("1330", "1331", "1332"), pagesProcessed < maxPages)))) {
            if (stryMutAct_9fa48("1333")) {
              {}
            } else {
              stryCov_9fa48("1333");
              const response = await scanApi.fetchUpdates({
                page_size: 100,
                after: pageEndEventId ? {
                  after_migration_id: 0,
                  after_record_time: pageEndEventId
                } : undefined
              });
              if (stryMutAct_9fa48("1338") ? !response.transactions && response.transactions.length === 0 : stryMutAct_9fa48("1337") ? false : stryMutAct_9fa48("1336") ? true : (stryCov_9fa48("1336", "1337", "1338"), (stryMutAct_9fa48("1339") ? response.transactions : (stryCov_9fa48("1339"), !response.transactions)) || (stryMutAct_9fa48("1341") ? response.transactions.length !== 0 : stryMutAct_9fa48("1340") ? false : (stryCov_9fa48("1340", "1341"), response.transactions.length === 0)))) {
                if (stryMutAct_9fa48("1342")) {
                  {}
                } else {
                  stryCov_9fa48("1342");
                  hasMore = stryMutAct_9fa48("1343") ? true : (stryCov_9fa48("1343"), false);
                  break;
                }
              }
              for (const transaction of response.transactions) {
                if (stryMutAct_9fa48("1344")) {
                  {}
                } else {
                  stryCov_9fa48("1344");
                  // Check if transaction is within our time range
                  const txTime = new Date(transaction.record_time);
                  if (stryMutAct_9fa48("1348") ? txTime >= startTime : stryMutAct_9fa48("1347") ? txTime <= startTime : stryMutAct_9fa48("1346") ? false : stryMutAct_9fa48("1345") ? true : (stryCov_9fa48("1345", "1346", "1347", "1348"), txTime < startTime)) {
                    if (stryMutAct_9fa48("1349")) {
                      {}
                    } else {
                      stryCov_9fa48("1349");
                      hasMore = stryMutAct_9fa48("1350") ? true : (stryCov_9fa48("1350"), false);
                      break;
                    }
                  }

                  // Calculate burn for this transaction
                  const txBurn = calculateBurnFromTransaction(transaction);

                  // Add to totals
                  stryMutAct_9fa48("1351") ? result.totalBurn -= txBurn.totalBurn : (stryCov_9fa48("1351"), result.totalBurn += txBurn.totalBurn);
                  stryMutAct_9fa48("1352") ? result.trafficBurn -= txBurn.trafficBurn : (stryCov_9fa48("1352"), result.trafficBurn += txBurn.trafficBurn);
                  stryMutAct_9fa48("1353") ? result.transferBurn -= txBurn.transferBurn : (stryCov_9fa48("1353"), result.transferBurn += txBurn.transferBurn);
                  stryMutAct_9fa48("1354") ? result.cnsBurn -= txBurn.cnsBurn : (stryCov_9fa48("1354"), result.cnsBurn += txBurn.cnsBurn);
                  stryMutAct_9fa48("1355") ? result.preapprovalBurn -= txBurn.preapprovalBurn : (stryCov_9fa48("1355"), result.preapprovalBurn += txBurn.preapprovalBurn);

                  // Add to daily breakdown
                  const dateKey = stryMutAct_9fa48("1356") ? txTime.toISOString() : (stryCov_9fa48("1356"), txTime.toISOString().slice(0, 10));
                  if (stryMutAct_9fa48("1359") ? false : stryMutAct_9fa48("1358") ? true : stryMutAct_9fa48("1357") ? result.byDay[dateKey] : (stryCov_9fa48("1357", "1358", "1359"), !result.byDay[dateKey])) {
                    if (stryMutAct_9fa48("1360")) {
                      {}
                    } else {
                      stryCov_9fa48("1360");
                      result.byDay[dateKey] = {
                        totalBurn: 0,
                        trafficBurn: 0,
                        transferBurn: 0,
                        cnsBurn: 0,
                        preapprovalBurn: 0
                      };
                    }
                  }
                  stryMutAct_9fa48("1362") ? result.byDay[dateKey].totalBurn -= txBurn.totalBurn : (stryCov_9fa48("1362"), result.byDay[dateKey].totalBurn += txBurn.totalBurn);
                  stryMutAct_9fa48("1363") ? result.byDay[dateKey].trafficBurn -= txBurn.trafficBurn : (stryCov_9fa48("1363"), result.byDay[dateKey].trafficBurn += txBurn.trafficBurn);
                  stryMutAct_9fa48("1364") ? result.byDay[dateKey].transferBurn -= txBurn.transferBurn : (stryCov_9fa48("1364"), result.byDay[dateKey].transferBurn += txBurn.transferBurn);
                  stryMutAct_9fa48("1365") ? result.byDay[dateKey].cnsBurn -= txBurn.cnsBurn : (stryCov_9fa48("1365"), result.byDay[dateKey].cnsBurn += txBurn.cnsBurn);
                  stryMutAct_9fa48("1366") ? result.byDay[dateKey].preapprovalBurn -= txBurn.preapprovalBurn : (stryCov_9fa48("1366"), result.byDay[dateKey].preapprovalBurn += txBurn.preapprovalBurn);
                }
              }

              // Set up for next page
              const lastTx = response.transactions[stryMutAct_9fa48("1367") ? response.transactions.length + 1 : (stryCov_9fa48("1367"), response.transactions.length - 1)];
              pageEndEventId = lastTx.record_time;
              stryMutAct_9fa48("1368") ? pagesProcessed-- : (stryCov_9fa48("1368"), pagesProcessed++);
            }
          }
          return result;
        }
      },
      enabled: stryMutAct_9fa48("1369") ? !latestRound : (stryCov_9fa48("1369"), !(stryMutAct_9fa48("1370") ? latestRound : (stryCov_9fa48("1370"), !latestRound))),
      staleTime: 60_000,
      retry: 1
    });
  }
}