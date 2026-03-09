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
export type TemplateSuffix = string;

// Central map of template suffixes to pages that actually consume them
export const templatePageMap: Record<TemplateSuffix, string[]> = {
  // Supply / Balances
  'Splice:Amulet:Amulet': stryMutAct_9fa48("5687") ? [] : (stryCov_9fa48("5687"), ['Supply', 'Balances']),
  'Splice:Amulet:LockedAmulet': stryMutAct_9fa48("5690") ? [] : (stryCov_9fa48("5690"), ['Supply', 'Balances']),
  'Splice:Round:IssuingMiningRound': stryMutAct_9fa48("5693") ? [] : (stryCov_9fa48("5693"), ['Supply', 'Mining Rounds']),
  'Splice:Round:ClosedMiningRound': stryMutAct_9fa48("5696") ? [] : (stryCov_9fa48("5696"), ['Supply', 'Mining Rounds']),
  // Mining Rounds
  'Splice:Round:OpenMiningRound': stryMutAct_9fa48("5699") ? [] : (stryCov_9fa48("5699"), ['Mining Rounds']),
  // Transfers
  'Splice:AmuletRules:TransferPreapproval': stryMutAct_9fa48("5701") ? [] : (stryCov_9fa48("5701"), ['Transfers']),
  'Splice:ExternalPartyAmuletRules:TransferCommand': stryMutAct_9fa48("5703") ? [] : (stryCov_9fa48("5703"), ['Transfers']),
  'Splice:AmuletTransferInstruction:AmuletTransferInstruction': stryMutAct_9fa48("5705") ? [] : (stryCov_9fa48("5705"), ['Transfers']),
  // Governance
  'Splice:DsoRules:DsoRules': stryMutAct_9fa48("5707") ? [] : (stryCov_9fa48("5707"), ['Governance']),
  'Splice:DsoRules:VoteRequest': stryMutAct_9fa48("5709") ? [] : (stryCov_9fa48("5709"), ['Governance']),
  'Splice:DSO:AmuletPrice:AmuletPriceVote': stryMutAct_9fa48("5711") ? [] : (stryCov_9fa48("5711"), ['Governance']),
  'Splice:DsoRules:Confirmation': stryMutAct_9fa48("5713") ? [] : (stryCov_9fa48("5713"), ['Governance']),
  'Splice:AmuletRules:AmuletRules': stryMutAct_9fa48("5715") ? [] : (stryCov_9fa48("5715"), ['Governance']),
  // Unclaimed SV Rewards
  'Splice:Amulet:ValidatorRewardCoupon': stryMutAct_9fa48("5717") ? [] : (stryCov_9fa48("5717"), ['Unclaimed SV Rewards']),
  'Splice:Amulet:SvRewardCoupon': stryMutAct_9fa48("5719") ? [] : (stryCov_9fa48("5719"), ['Unclaimed SV Rewards']),
  'Splice:Amulet:AppRewardCoupon': stryMutAct_9fa48("5721") ? [] : (stryCov_9fa48("5721"), ['Unclaimed SV Rewards']),
  'Splice:Amulet:UnclaimedReward': stryMutAct_9fa48("5723") ? [] : (stryCov_9fa48("5723"), ['Unclaimed SV Rewards']),
  // Apps
  'Splice:Amulet:FeaturedAppRight': stryMutAct_9fa48("5725") ? [] : (stryCov_9fa48("5725"), ['Apps']),
  'Splice:Amulet:FeaturedAppActivityMarker': stryMutAct_9fa48("5727") ? [] : (stryCov_9fa48("5727"), ['Apps']),
  // ANS
  'Splice:Ans:AnsEntry': stryMutAct_9fa48("5729") ? [] : (stryCov_9fa48("5729"), ['ANS']),
  'Splice:Ans:AnsEntryContext': stryMutAct_9fa48("5731") ? [] : (stryCov_9fa48("5731"), ['ANS']),
  'Splice:Ans:AmuletConversionRateFeed:AmuletConversionRateFeed': stryMutAct_9fa48("5733") ? [] : (stryCov_9fa48("5733"), ['ANS']),
  // Validator Licenses
  'Splice:ValidatorLicense:ValidatorLicense': stryMutAct_9fa48("5735") ? [] : (stryCov_9fa48("5735"), ['Validator Licenses']),
  'Splice:ValidatorLicense:ValidatorFaucetCoupon': stryMutAct_9fa48("5737") ? [] : (stryCov_9fa48("5737"), ['Validator Licenses']),
  'Splice:ValidatorLicense:ValidatorLivenessActivityRecord': stryMutAct_9fa48("5739") ? [] : (stryCov_9fa48("5739"), ['Validator Licenses']),
  'Splice:Amulet:ValidatorRight': stryMutAct_9fa48("5741") ? [] : (stryCov_9fa48("5741"), ['Validator Licenses']),
  // DSO State
  'DSO:SvState:SvNodeState': stryMutAct_9fa48("5743") ? [] : (stryCov_9fa48("5743"), ['DSO State']),
  'DSO:SvState:SvStatusReport': stryMutAct_9fa48("5745") ? [] : (stryCov_9fa48("5745"), ['DSO State']),
  'DSO:SvState:SvRewardState': stryMutAct_9fa48("5747") ? [] : (stryCov_9fa48("5747"), ['DSO State']),
  // Member Traffic
  'Splice:DecentralizedSynchronizer:MemberTraffic': stryMutAct_9fa48("5749") ? [] : (stryCov_9fa48("5749"), ['Member Traffic']),
  // Subscriptions
  'Wallet:Subscriptions:Subscription': stryMutAct_9fa48("5751") ? [] : (stryCov_9fa48("5751"), ['Subscriptions']),
  'Wallet:Subscriptions:SubscriptionIdleState': stryMutAct_9fa48("5753") ? [] : (stryCov_9fa48("5753"), ['Subscriptions']),
  'Wallet:Subscriptions:SubscriptionRequest': stryMutAct_9fa48("5755") ? [] : (stryCov_9fa48("5755"), ['Subscriptions']),
  // External Party Setup
  'Splice:AmuletRules:ExternalPartySetupProposal': stryMutAct_9fa48("5757") ? [] : (stryCov_9fa48("5757"), ['External Party Setup']),
  // Allocations
  'Splice:AmuletAllocation:AmuletAllocation': stryMutAct_9fa48("5759") ? [] : (stryCov_9fa48("5759"), ['Allocations']),
  // Elections
  'Splice:DsoRules:ElectionRequest': stryMutAct_9fa48("5761") ? [] : (stryCov_9fa48("5761"), ['Elections']),
  // Transfer Counters
  'Splice:ExternalPartyAmuletRules:TransferCommandCounter': stryMutAct_9fa48("5763") ? [] : (stryCov_9fa48("5763"), ['Transfer Counters']),
  // External Party Rules
  'Splice:ExternalPartyAmuletRules:ExternalPartyAmuletRules': stryMutAct_9fa48("5765") ? [] : (stryCov_9fa48("5765"), ['External Party Rules'])
};
export const getPagesThatUseTemplate = (templateId: string): string[] => {
  if (stryMutAct_9fa48("5767")) {
    {}
  } else {
    stryCov_9fa48("5767");
    const suffix = stryMutAct_9fa48("5768") ? templateId.split(':').join(':') : (stryCov_9fa48("5768"), templateId.split(':').slice(stryMutAct_9fa48("5770") ? +3 : (stryCov_9fa48("5770"), -3)).join(':'));
    return stryMutAct_9fa48("5774") ? templatePageMap[suffix] && [] : stryMutAct_9fa48("5773") ? false : stryMutAct_9fa48("5772") ? true : (stryCov_9fa48("5772", "5773", "5774"), templatePageMap[suffix] || (stryMutAct_9fa48("5775") ? ["Stryker was here"] : (stryCov_9fa48("5775"), [])));
  }
};