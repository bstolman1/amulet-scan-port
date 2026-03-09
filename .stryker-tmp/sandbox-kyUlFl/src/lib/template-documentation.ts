/**
 * Template Documentation Generator
 * 
 * Generates comprehensive documentation for Canton Network templates,
 * explaining the JSON structure and purpose of each template type.
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
import { templatePageMap } from './template-page-map';

// Template metadata with detailed descriptions and field explanations
export const TEMPLATE_METADATA: Record<string, {
  category: string;
  purpose: string;
  description: string;
  useCases: string[];
  fieldDescriptions: Record<string, string>;
}> = {
  // Core Amulet templates
  'Splice.Amulet:Amulet': {
    category: 'Core Currency',
    purpose: 'Represents the primary Amulet token in the Canton Network',
    description: 'Amulet is the native digital currency of the Canton Network. Each Amulet contract represents a specific amount of tokens held by an owner. Amulets can be transferred, locked, and used for various network operations including paying transaction fees and participating in governance.',
    useCases: stryMutAct_9fa48("5018") ? [] : (stryCov_9fa48("5018"), ['Storing value on the network', 'Paying transaction fees', 'Rewarding validators and super validators', 'Participating in governance decisions']),
    fieldDescriptions: {
      'dso': 'The DSO (Decentralized Synchronizer Operator) party that manages the network',
      'owner': 'The party that owns this Amulet',
      'amount': 'The numeric amount of Amulet tokens (with high precision)',
      'createdAt': 'Round number when this Amulet was created',
      'ratePerRound': 'The holding fee rate applied per round'
    }
  },
  'Splice.Amulet:LockedAmulet': {
    category: 'Core Currency',
    purpose: 'Represents Amulet tokens that are locked for a specific purpose',
    description: 'LockedAmulet contracts represent Amulet tokens that have been locked, typically for time-based vesting, collateral, or other purposes requiring temporary immobilization of funds. The lock specifies conditions under which the Amulet can be released.',
    useCases: stryMutAct_9fa48("5033") ? [] : (stryCov_9fa48("5033"), ['Vesting schedules for rewards', 'Collateral for operations', 'Time-locked payments', 'Escrow arrangements']),
    fieldDescriptions: {
      'dso': 'The DSO party managing the network',
      'owner': 'The party that owns the locked Amulet',
      'amulet': 'The underlying Amulet being locked',
      'lock': 'The lock conditions specifying when/how the Amulet can be released',
      'timedLock': 'Optional time-based lock with expiration round'
    }
  },
  'Splice.Amulet:ValidatorRight': {
    category: 'Validator Operations',
    purpose: 'Grants rights to operate as a validator on the network',
    description: 'ValidatorRight contracts authorize a party to act as a validator for a specific user. Validators process transactions and earn rewards for their participation in the network consensus.',
    useCases: stryMutAct_9fa48("5048") ? [] : (stryCov_9fa48("5048"), ['Authorizing validator operations', 'Linking users to their chosen validator', 'Validator reward distribution']),
    fieldDescriptions: {
      'dso': 'The DSO party',
      'user': 'The user party being served by this validator',
      'validator': 'The validator party authorized to validate for this user'
    }
  },
  'Splice.Amulet:AppRewardCoupon': {
    category: 'Rewards',
    purpose: 'Represents unclaimed rewards earned by application providers',
    description: 'AppRewardCoupon contracts accumulate rewards for applications that facilitate transactions on the network. These coupons can be redeemed for Amulet tokens.',
    useCases: stryMutAct_9fa48("5060") ? [] : (stryCov_9fa48("5060"), ['Tracking app provider rewards', 'Incentivizing application development', 'Reward redemption']),
    fieldDescriptions: {
      'dso': 'The DSO party',
      'provider': 'The application provider earning the reward',
      'round': 'The mining round in which this reward was earned',
      'amount': 'The reward amount in Amulet units',
      'featured': 'Whether this is from a featured app with enhanced rewards'
    }
  },
  'Splice.Amulet:SvRewardCoupon': {
    category: 'Rewards',
    purpose: 'Represents unclaimed rewards for Super Validators',
    description: 'SvRewardCoupon contracts track rewards earned by Super Validators (SVs) for their role in network governance and infrastructure. SVs earn rewards for voting, maintaining uptime, and other governance activities.',
    useCases: stryMutAct_9fa48("5074") ? [] : (stryCov_9fa48("5074"), ['Tracking SV governance rewards', 'Incentivizing network participation', 'SV reward claiming']),
    fieldDescriptions: {
      'dso': 'The DSO party',
      'sv': 'The Super Validator party earning the reward',
      'round': 'The round in which this reward was earned',
      'weight': 'The SV weight determining reward share'
    }
  },
  'Splice.Amulet:ValidatorRewardCoupon': {
    category: 'Rewards',
    purpose: 'Represents unclaimed rewards for validators',
    description: 'ValidatorRewardCoupon contracts track rewards earned by validators for processing transactions and maintaining network consensus.',
    useCases: stryMutAct_9fa48("5087") ? [] : (stryCov_9fa48("5087"), ['Tracking validator rewards', 'Incentivizing validator participation', 'Validator reward claiming']),
    fieldDescriptions: {
      'dso': 'The DSO party',
      'user': 'The user whose transactions generated this reward',
      'validator': 'The validator earning the reward',
      'round': 'The round in which this reward was earned'
    }
  },
  'Splice.Amulet:UnclaimedReward': {
    category: 'Rewards',
    purpose: 'Represents rewards that have not yet been claimed',
    description: 'UnclaimedReward contracts represent rewards that are available for claiming but have not yet been converted to Amulet tokens.',
    useCases: stryMutAct_9fa48("5100") ? [] : (stryCov_9fa48("5100"), ['Tracking pending rewards', 'Reward distribution management']),
    fieldDescriptions: {
      'dso': 'The DSO party',
      'amount': 'The unclaimed reward amount'
    }
  },
  'Splice.Amulet:FeaturedAppRight': {
    category: 'Applications',
    purpose: 'Grants featured status to an application',
    description: 'FeaturedAppRight contracts designate applications as "featured" on the network, typically granting enhanced visibility and potentially higher reward rates for facilitating transactions.',
    useCases: stryMutAct_9fa48("5110") ? [] : (stryCov_9fa48("5110"), ['Promoting high-quality applications', 'Enhanced reward rates', 'Application discovery']),
    fieldDescriptions: {
      'dso': 'The DSO party',
      'provider': 'The application provider'
    }
  },
  'Splice.Amulet:FeaturedAppActivityMarker': {
    category: 'Applications',
    purpose: 'Tracks activity from featured applications',
    description: 'FeaturedAppActivityMarker contracts record activity from featured applications, used for calculating enhanced rewards.',
    useCases: stryMutAct_9fa48("5121") ? [] : (stryCov_9fa48("5121"), ['Activity tracking for featured apps', 'Reward calculation']),
    fieldDescriptions: {
      'dso': 'The DSO party',
      'provider': 'The featured app provider'
    }
  },
  // Validator License templates
  'Splice.ValidatorLicense:ValidatorLicense': {
    category: 'Validator Operations',
    purpose: 'Authorizes a party to operate as a validator',
    description: 'ValidatorLicense contracts grant parties the authority to operate as validators on the Canton Network. Validators are responsible for processing transactions and maintaining network consensus.',
    useCases: stryMutAct_9fa48("5131") ? [] : (stryCov_9fa48("5131"), ['Validator registration', 'Validator authorization', 'Network participation rights']),
    fieldDescriptions: {
      'dso': 'The DSO party',
      'validator': 'The party authorized to validate',
      'sponsor': 'The SV that sponsored this validator',
      'faucetState': 'State of the validator faucet for onboarding'
    }
  },
  'Splice.ValidatorLicense:ValidatorFaucetCoupon': {
    category: 'Validator Operations',
    purpose: 'Provides initial Amulet for new validators',
    description: 'ValidatorFaucetCoupon contracts allow new validators to receive initial Amulet tokens to begin operations on the network.',
    useCases: stryMutAct_9fa48("5144") ? [] : (stryCov_9fa48("5144"), ['Validator onboarding', 'Initial token distribution']),
    fieldDescriptions: {
      'dso': 'The DSO party',
      'validator': 'The validator receiving the faucet'
    }
  },
  'Splice.ValidatorLicense:ValidatorLivenessActivityRecord': {
    category: 'Validator Operations',
    purpose: 'Records validator uptime and activity',
    description: 'ValidatorLivenessActivityRecord contracts track validator uptime and activity, used for reward calculations and network health monitoring.',
    useCases: stryMutAct_9fa48("5154") ? [] : (stryCov_9fa48("5154"), ['Uptime tracking', 'Reward calculation', 'Network health monitoring']),
    fieldDescriptions: {
      'validator': 'The validator being tracked',
      'round': 'The round of activity',
      'domain': 'The synchronizer domain'
    }
  },
  // DSO/Governance templates
  'Splice.DsoRules:DsoRules': {
    category: 'Governance',
    purpose: 'Defines the core rules for DSO operation',
    description: 'DsoRules is the central governance contract that defines how the Decentralized Synchronizer Operator functions. It contains network parameters, fee schedules, SV weights, and governance rules.',
    useCases: stryMutAct_9fa48("5166") ? [] : (stryCov_9fa48("5166"), ['Network parameter configuration', 'Fee schedule definition', 'SV weight management', 'Governance process rules']),
    fieldDescriptions: {
      'dso': 'The DSO party identifier',
      'svs': 'Map of Super Validators and their metadata',
      'config': 'Current network configuration parameters',
      'epoch': 'Current governance epoch'
    }
  },
  'Splice.DsoRules:VoteRequest': {
    category: 'Governance',
    purpose: 'Represents a governance proposal awaiting votes',
    description: 'VoteRequest contracts represent proposals submitted for governance voting. SVs vote on these proposals to approve or reject changes to network parameters, SV membership, or other governance actions.',
    useCases: stryMutAct_9fa48("5180") ? [] : (stryCov_9fa48("5180"), ['Proposing network changes', 'SV membership changes', 'Parameter updates', 'Governance decisions']),
    fieldDescriptions: {
      'dso': 'The DSO party',
      'requester': 'The party that submitted the proposal',
      'action': 'The proposed action to be executed if approved',
      'reason': 'The reason/justification for the proposal',
      'votes': 'Current votes from SVs',
      'trackingCid': 'Optional CID linking to previous related contracts',
      'expiresAt': 'When the vote expires'
    }
  },
  'Splice.DsoRules:Confirmation': {
    category: 'Governance',
    purpose: 'Records confirmation of governance actions',
    description: 'Confirmation contracts record that a governance action has been confirmed and is pending execution.',
    useCases: stryMutAct_9fa48("5197") ? [] : (stryCov_9fa48("5197"), ['Tracking confirmed proposals', 'Governance audit trail']),
    fieldDescriptions: {
      'dso': 'The DSO party',
      'action': 'The confirmed action',
      'confirmedBy': 'Parties that confirmed'
    }
  },
  'Splice.DsoRules:ElectionRequest': {
    category: 'Governance',
    purpose: 'Manages SV leader election process',
    description: 'ElectionRequest contracts manage the process of electing an SV leader for specific epochs or functions.',
    useCases: stryMutAct_9fa48("5208") ? [] : (stryCov_9fa48("5208"), ['Leader election', 'Epoch transitions']),
    fieldDescriptions: {
      'dso': 'The DSO party',
      'epoch': 'The election epoch',
      'ranking': 'Current ranking of candidates'
    }
  },
  // DSO SV State templates
  'Splice.DSO.SvState:SvNodeState': {
    category: 'SV Operations',
    purpose: 'Tracks the operational state of an SV node',
    description: 'SvNodeState contracts track the current operational state of Super Validator nodes, including their network connectivity and synchronization status.',
    useCases: stryMutAct_9fa48("5219") ? [] : (stryCov_9fa48("5219"), ['SV monitoring', 'Network health tracking', 'Failover management']),
    fieldDescriptions: {
      'dso': 'The DSO party',
      'sv': 'The Super Validator',
      'state': 'Current node state'
    }
  },
  'Splice.DSO.SvState:SvRewardState': {
    category: 'SV Operations',
    purpose: 'Tracks accumulated SV rewards',
    description: 'SvRewardState contracts track the accumulated rewards for Super Validators over time.',
    useCases: stryMutAct_9fa48("5231") ? [] : (stryCov_9fa48("5231"), ['Reward tracking', 'SV incentive management']),
    fieldDescriptions: {
      'dso': 'The DSO party',
      'sv': 'The Super Validator',
      'rewardState': 'Current reward accumulation state'
    }
  },
  'Splice.DSO.SvState:SvStatusReport': {
    category: 'SV Operations',
    purpose: 'Records SV status reports for monitoring',
    description: 'SvStatusReport contracts contain status reports submitted by Super Validators for network monitoring and health checks.',
    useCases: stryMutAct_9fa48("5242") ? [] : (stryCov_9fa48("5242"), ['SV health monitoring', 'Network diagnostics', 'Uptime verification']),
    fieldDescriptions: {
      'dso': 'The DSO party',
      'sv': 'The reporting Super Validator',
      'report': 'The status report content',
      'round': 'The round of the report'
    }
  },
  'Splice.DSO.AmuletPrice:AmuletPriceVote': {
    category: 'Governance',
    purpose: 'Records SV votes on Amulet price',
    description: 'AmuletPriceVote contracts record Super Validator votes on the USD price of Amulet, used to determine the reference exchange rate.',
    useCases: stryMutAct_9fa48("5255") ? [] : (stryCov_9fa48("5255"), ['Price oracle voting', 'Exchange rate determination']),
    fieldDescriptions: {
      'dso': 'The DSO party',
      'sv': 'The voting Super Validator',
      'price': 'The voted price in USD',
      'round': 'The voting round'
    }
  },
  // Amulet Rules templates
  'Splice.AmuletRules:AmuletRules': {
    category: 'Network Rules',
    purpose: 'Defines rules for Amulet token operations',
    description: 'AmuletRules contracts define the operational rules for Amulet tokens, including transfer rules, fee calculations, and token lifecycle management.',
    useCases: stryMutAct_9fa48("5267") ? [] : (stryCov_9fa48("5267"), ['Token operation rules', 'Fee calculation', 'Transfer validation']),
    fieldDescriptions: {
      'dso': 'The DSO party',
      'configSchedule': 'Schedule of configuration changes'
    }
  },
  'Splice.AmuletRules:TransferPreapproval': {
    category: 'Transfers',
    purpose: 'Pre-authorizes a future transfer',
    description: 'TransferPreapproval contracts authorize transfers before they occur, enabling scheduled or conditional transfers.',
    useCases: stryMutAct_9fa48("5278") ? [] : (stryCov_9fa48("5278"), ['Scheduled payments', 'Conditional transfers', 'Pre-authorized withdrawals']),
    fieldDescriptions: {
      'dso': 'The DSO party',
      'sender': 'The sender authorizing the transfer',
      'receiver': 'The intended receiver',
      'provider': 'The app provider facilitating the transfer'
    }
  },
  'Splice.AmuletRules:ExternalPartySetupProposal': {
    category: 'External Parties',
    purpose: 'Proposes setup of an external party',
    description: 'ExternalPartySetupProposal contracts propose the onboarding of external parties to the network.',
    useCases: stryMutAct_9fa48("5291") ? [] : (stryCov_9fa48("5291"), ['External party onboarding', 'Party setup workflows']),
    fieldDescriptions: {
      'dso': 'The DSO party',
      'party': 'The proposed external party'
    }
  },
  // External Party templates
  'Splice.ExternalPartyAmuletRules:ExternalPartyAmuletRules': {
    category: 'External Parties',
    purpose: 'Defines rules for external party operations',
    description: 'ExternalPartyAmuletRules contracts define how external parties can interact with the Amulet network.',
    useCases: stryMutAct_9fa48("5301") ? [] : (stryCov_9fa48("5301"), ['External party integration', 'Cross-network operations']),
    fieldDescriptions: {
      'dso': 'The DSO party',
      'party': 'The external party'
    }
  },
  'Splice.ExternalPartyAmuletRules:TransferCommand': {
    category: 'Transfers',
    purpose: 'Commands a transfer from an external party',
    description: 'TransferCommand contracts represent transfer instructions from external parties.',
    useCases: stryMutAct_9fa48("5311") ? [] : (stryCov_9fa48("5311"), ['External party transfers', 'Automated payments']),
    fieldDescriptions: {
      'dso': 'The DSO party',
      'sender': 'The sending party',
      'receiver': 'The receiving party',
      'amount': 'The transfer amount'
    }
  },
  'Splice.ExternalPartyAmuletRules:TransferCommandCounter': {
    category: 'Transfers',
    purpose: 'Tracks transfer command sequence numbers',
    description: 'TransferCommandCounter contracts track sequence numbers for external party transfers to prevent replay attacks.',
    useCases: stryMutAct_9fa48("5323") ? [] : (stryCov_9fa48("5323"), ['Transfer sequencing', 'Replay attack prevention']),
    fieldDescriptions: {
      'dso': 'The DSO party',
      'party': 'The external party',
      'counter': 'Current sequence number'
    }
  },
  // Round templates
  'Splice.Round:OpenMiningRound': {
    category: 'Mining Rounds',
    purpose: 'Represents a currently open mining round',
    description: 'OpenMiningRound contracts represent mining rounds that are currently accepting transactions and generating rewards.',
    useCases: stryMutAct_9fa48("5334") ? [] : (stryCov_9fa48("5334"), ['Active round tracking', 'Transaction processing', 'Reward accumulation']),
    fieldDescriptions: {
      'dso': 'The DSO party',
      'round': 'The round number',
      'opensAt': 'When this round opened',
      'targetClosesAt': 'Expected closing time'
    }
  },
  'Splice.Round:ClosedMiningRound': {
    category: 'Mining Rounds',
    purpose: 'Represents a completed mining round',
    description: 'ClosedMiningRound contracts represent mining rounds that have completed and whose rewards are being distributed.',
    useCases: stryMutAct_9fa48("5347") ? [] : (stryCov_9fa48("5347"), ['Historical round data', 'Reward distribution', 'Audit trail']),
    fieldDescriptions: {
      'dso': 'The DSO party',
      'round': 'The round number',
      'issuedCC': 'Total CC issued in this round',
      'optBurnedCC': 'Optional burned CC amount'
    }
  },
  'Splice.Round:IssuingMiningRound': {
    category: 'Mining Rounds',
    purpose: 'Represents a round currently issuing rewards',
    description: 'IssuingMiningRound contracts represent rounds in the process of issuing rewards to participants.',
    useCases: stryMutAct_9fa48("5360") ? [] : (stryCov_9fa48("5360"), ['Reward issuance tracking', 'Distribution management']),
    fieldDescriptions: {
      'dso': 'The DSO party',
      'round': 'The round number',
      'issuanceConfig': 'Configuration for reward issuance'
    }
  },
  'Splice.Round:SummarizingMiningRound': {
    category: 'Mining Rounds',
    purpose: 'Represents a round being summarized',
    description: 'SummarizingMiningRound contracts represent rounds that are being aggregated for final reporting.',
    useCases: stryMutAct_9fa48("5371") ? [] : (stryCov_9fa48("5371"), ['Round finalization', 'Summary generation']),
    fieldDescriptions: {
      'dso': 'The DSO party',
      'round': 'The round number'
    }
  },
  // ANS templates
  'Splice.Ans:AnsEntry': {
    category: 'ANS (Naming Service)',
    purpose: 'Represents a registered name in the ANS',
    description: 'AnsEntry contracts represent registered names in the Amulet Naming Service, providing human-readable aliases for party identifiers.',
    useCases: stryMutAct_9fa48("5381") ? [] : (stryCov_9fa48("5381"), ['Human-readable addresses', 'Identity management', 'Name resolution']),
    fieldDescriptions: {
      'dso': 'The DSO party',
      'user': 'The owner of the name',
      'name': 'The registered name',
      'url': 'Optional URL associated with the name',
      'description': 'Optional description',
      'expiresAt': 'When the registration expires'
    }
  },
  'Splice.Ans:AnsEntryContext': {
    category: 'ANS (Naming Service)',
    purpose: 'Provides context for ANS entries',
    description: 'AnsEntryContext contracts provide additional context and metadata for ANS entries.',
    useCases: stryMutAct_9fa48("5396") ? [] : (stryCov_9fa48("5396"), ['Extended metadata', 'Context management']),
    fieldDescriptions: {
      'dso': 'The DSO party',
      'reference': 'Reference to the ANS entry'
    }
  },
  'Splice.Ans:AnsRules': {
    category: 'ANS (Naming Service)',
    purpose: 'Defines rules for ANS operations',
    description: 'AnsRules contracts define the operational rules for the Amulet Naming Service.',
    useCases: stryMutAct_9fa48("5406") ? [] : (stryCov_9fa48("5406"), ['Name registration rules', 'Pricing configuration']),
    fieldDescriptions: {
      'dso': 'The DSO party',
      'config': 'ANS configuration'
    }
  },
  // Traffic templates
  'Splice.DecentralizedSynchronizer:MemberTraffic': {
    category: 'Network Traffic',
    purpose: 'Tracks member traffic on the synchronizer',
    description: 'MemberTraffic contracts track the traffic generated by network members, used for fee calculations and capacity management.',
    useCases: stryMutAct_9fa48("5416") ? [] : (stryCov_9fa48("5416"), ['Traffic monitoring', 'Fee calculation', 'Capacity planning']),
    fieldDescriptions: {
      'dso': 'The DSO party',
      'member': 'The network member',
      'synchronizer': 'The synchronizer domain',
      'totalTrafficPurchased': 'Total traffic purchased',
      'totalTrafficConsumed': 'Total traffic used'
    }
  },
  // Subscription templates
  'Splice.Wallet.Subscriptions:Subscription': {
    category: 'Subscriptions',
    purpose: 'Represents an active subscription',
    description: 'Subscription contracts represent active recurring payment arrangements between parties.',
    useCases: stryMutAct_9fa48("5430") ? [] : (stryCov_9fa48("5430"), ['Recurring payments', 'Service subscriptions', 'Automated billing']),
    fieldDescriptions: {
      'dso': 'The DSO party',
      'subscriber': 'The subscribing party',
      'provider': 'The service provider',
      'subscriptionData': 'Subscription details and terms'
    }
  },
  'Splice.Wallet.Subscriptions:SubscriptionRequest': {
    category: 'Subscriptions',
    purpose: 'Represents a pending subscription request',
    description: 'SubscriptionRequest contracts represent subscription requests awaiting approval.',
    useCases: stryMutAct_9fa48("5443") ? [] : (stryCov_9fa48("5443"), ['Subscription initiation', 'Approval workflows']),
    fieldDescriptions: {
      'dso': 'The DSO party',
      'subscriber': 'The requesting subscriber',
      'provider': 'The target provider'
    }
  },
  'Splice.Wallet.Subscriptions:SubscriptionIdleState': {
    category: 'Subscriptions',
    purpose: 'Represents a paused subscription',
    description: 'SubscriptionIdleState contracts represent subscriptions that are temporarily paused or idle.',
    useCases: stryMutAct_9fa48("5454") ? [] : (stryCov_9fa48("5454"), ['Subscription management', 'Pause functionality']),
    fieldDescriptions: {
      'dso': 'The DSO party',
      'subscription': 'The paused subscription reference'
    }
  },
  // Transfer Instruction templates
  'Splice.AmuletTransferInstruction:AmuletTransferInstruction': {
    category: 'Transfers',
    purpose: 'Represents a transfer instruction',
    description: 'AmuletTransferInstruction contracts represent instructions to transfer Amulet tokens between parties.',
    useCases: stryMutAct_9fa48("5464") ? [] : (stryCov_9fa48("5464"), ['Token transfers', 'Payment instructions']),
    fieldDescriptions: {
      'dso': 'The DSO party',
      'sender': 'The sending party',
      'receiver': 'The receiving party',
      'amount': 'The transfer amount'
    }
  },
  // Allocation templates
  'Splice.AmuletAllocation:AmuletAllocation': {
    category: 'Allocations',
    purpose: 'Represents allocated Amulet tokens',
    description: 'AmuletAllocation contracts represent Amulet tokens that have been allocated for specific purposes.',
    useCases: stryMutAct_9fa48("5476") ? [] : (stryCov_9fa48("5476"), ['Token allocation', 'Reserved funds']),
    fieldDescriptions: {
      'dso': 'The DSO party',
      'owner': 'The allocation owner',
      'amount': 'The allocated amount'
    }
  }
};

/**
 * Generates a markdown documentation for a template
 */
export function generateTemplateDocumentation(templateId: string, sampleData: any, contractCount: number): string {
  if (stryMutAct_9fa48("5483")) {
    {}
  } else {
    stryCov_9fa48("5483");
    // Extract template key from full template ID
    const parts = templateId.split(':');
    const templateKey = stryMutAct_9fa48("5485") ? parts.join(':') : (stryCov_9fa48("5485"), parts.slice(stryMutAct_9fa48("5486") ? +2 : (stryCov_9fa48("5486"), -2)).join(':'));
    const modulePath = stryMutAct_9fa48("5488") ? parts.join(':') : (stryCov_9fa48("5488"), parts.slice(0, stryMutAct_9fa48("5489") ? +1 : (stryCov_9fa48("5489"), -1)).join(':'));

    // Find matching metadata (try different key formats)
    let metadata = TEMPLATE_METADATA[templateKey];
    if (stryMutAct_9fa48("5493") ? false : stryMutAct_9fa48("5492") ? true : stryMutAct_9fa48("5491") ? metadata : (stryCov_9fa48("5491", "5492", "5493"), !metadata)) {
      if (stryMutAct_9fa48("5494")) {
        {}
      } else {
        stryCov_9fa48("5494");
        // Try with module prefix
        const fullKey = stryMutAct_9fa48("5495") ? parts.join('.') : (stryCov_9fa48("5495"), parts.slice(stryMutAct_9fa48("5496") ? +3 : (stryCov_9fa48("5496"), -3)).join('.'));
        metadata = TEMPLATE_METADATA[fullKey];
      }
    }
    if (stryMutAct_9fa48("5500") ? false : stryMutAct_9fa48("5499") ? true : stryMutAct_9fa48("5498") ? metadata : (stryCov_9fa48("5498", "5499", "5500"), !metadata)) {
      if (stryMutAct_9fa48("5501")) {
        {}
      } else {
        stryCov_9fa48("5501");
        // Try other format
        const altKey = templateId.includes('.') ? templateId : stryMutAct_9fa48("5503") ? parts.join('.') : (stryCov_9fa48("5503"), parts.slice(stryMutAct_9fa48("5504") ? +3 : (stryCov_9fa48("5504"), -3)).join('.'));
        metadata = TEMPLATE_METADATA[altKey];
      }
    }

    // Get pages that use this template
    const suffix = stryMutAct_9fa48("5506") ? templateId.split(':').join(':') : (stryCov_9fa48("5506"), templateId.split(':').slice(stryMutAct_9fa48("5508") ? +3 : (stryCov_9fa48("5508"), -3)).join(':'));
    const usedInPages = stryMutAct_9fa48("5512") ? templatePageMap[suffix] && [] : stryMutAct_9fa48("5511") ? false : stryMutAct_9fa48("5510") ? true : (stryCov_9fa48("5510", "5511", "5512"), templatePageMap[suffix] || (stryMutAct_9fa48("5513") ? ["Stryker was here"] : (stryCov_9fa48("5513"), [])));

    // Build the documentation
    let doc = `# ${templateKey} Template Documentation\n\n`;
    doc += `**Generated:** ${new Date().toISOString()}\n\n`;
    doc += `---\n\n`;

    // Template Overview
    doc += `## Overview\n\n`;
    doc += `- **Full Template ID:** \`${templateId}\`\n`;
    doc += `- **Module Path:** \`${modulePath}\`\n`;
    doc += `- **Current Contract Count:** ${contractCount.toLocaleString()}\n`;
    if (stryMutAct_9fa48("5522") ? false : stryMutAct_9fa48("5521") ? true : (stryCov_9fa48("5521", "5522"), metadata)) {
      if (stryMutAct_9fa48("5523")) {
        {}
      } else {
        stryCov_9fa48("5523");
        doc += `- **Category:** ${metadata.category}\n`;
        doc += `- **Purpose:** ${metadata.purpose}\n`;
      }
    }
    if (stryMutAct_9fa48("5529") ? usedInPages.length <= 0 : stryMutAct_9fa48("5528") ? usedInPages.length >= 0 : stryMutAct_9fa48("5527") ? false : stryMutAct_9fa48("5526") ? true : (stryCov_9fa48("5526", "5527", "5528", "5529"), usedInPages.length > 0)) {
      if (stryMutAct_9fa48("5530")) {
        {}
      } else {
        stryCov_9fa48("5530");
        doc += `- **Used in Pages:** ${usedInPages.join(', ')}\n`;
      }
    }
    doc += `\n`;

    // Description
    if (stryMutAct_9fa48("5536") ? metadata.description : stryMutAct_9fa48("5535") ? false : stryMutAct_9fa48("5534") ? true : (stryCov_9fa48("5534", "5535", "5536"), metadata?.description)) {
      if (stryMutAct_9fa48("5537")) {
        {}
      } else {
        stryCov_9fa48("5537");
        doc += `## Description\n\n`;
        doc += `${metadata.description}\n\n`;
      }
    }

    // Use Cases
    if (stryMutAct_9fa48("5542") ? metadata?.useCases || metadata.useCases.length > 0 : stryMutAct_9fa48("5541") ? false : stryMutAct_9fa48("5540") ? true : (stryCov_9fa48("5540", "5541", "5542"), (stryMutAct_9fa48("5543") ? metadata.useCases : (stryCov_9fa48("5543"), metadata?.useCases)) && (stryMutAct_9fa48("5546") ? metadata.useCases.length <= 0 : stryMutAct_9fa48("5545") ? metadata.useCases.length >= 0 : stryMutAct_9fa48("5544") ? true : (stryCov_9fa48("5544", "5545", "5546"), metadata.useCases.length > 0)))) {
      if (stryMutAct_9fa48("5547")) {
        {}
      } else {
        stryCov_9fa48("5547");
        doc += `## Use Cases\n\n`;
        metadata.useCases.forEach(useCase => {
          if (stryMutAct_9fa48("5549")) {
            {}
          } else {
            stryCov_9fa48("5549");
            doc += `- ${useCase}\n`;
          }
        });
        doc += `\n`;
      }
    }

    // JSON Structure Analysis
    doc += `## JSON Structure\n\n`;
    if (stryMutAct_9fa48("5554") ? false : stryMutAct_9fa48("5553") ? true : (stryCov_9fa48("5553", "5554"), sampleData)) {
      if (stryMutAct_9fa48("5555")) {
        {}
      } else {
        stryCov_9fa48("5555");
        doc += `The following structure is derived from analyzing sample contract data:\n\n`;
        doc += "```json\n";
        stryMutAct_9fa48("5558") ? doc -= JSON.stringify(sampleData, null, 2) : (stryCov_9fa48("5558"), doc += JSON.stringify(sampleData, null, 2));
        doc += "\n```\n\n";

        // Field Analysis
        doc += `## Field Analysis\n\n`;
        stryMutAct_9fa48("5561") ? doc -= analyzeFields(sampleData, metadata?.fieldDescriptions || {}) : (stryCov_9fa48("5561"), doc += analyzeFields(sampleData, stryMutAct_9fa48("5564") ? metadata?.fieldDescriptions && {} : stryMutAct_9fa48("5563") ? false : stryMutAct_9fa48("5562") ? true : (stryCov_9fa48("5562", "5563", "5564"), (stryMutAct_9fa48("5565") ? metadata.fieldDescriptions : (stryCov_9fa48("5565"), metadata?.fieldDescriptions)) || {})));
      }
    } else {
      if (stryMutAct_9fa48("5566")) {
        {}
      } else {
        stryCov_9fa48("5566");
        doc += `*No sample data available for this template.*\n\n`;
      }
    }

    // Field Descriptions from metadata
    if (stryMutAct_9fa48("5570") ? metadata?.fieldDescriptions || Object.keys(metadata.fieldDescriptions).length > 0 : stryMutAct_9fa48("5569") ? false : stryMutAct_9fa48("5568") ? true : (stryCov_9fa48("5568", "5569", "5570"), (stryMutAct_9fa48("5571") ? metadata.fieldDescriptions : (stryCov_9fa48("5571"), metadata?.fieldDescriptions)) && (stryMutAct_9fa48("5574") ? Object.keys(metadata.fieldDescriptions).length <= 0 : stryMutAct_9fa48("5573") ? Object.keys(metadata.fieldDescriptions).length >= 0 : stryMutAct_9fa48("5572") ? true : (stryCov_9fa48("5572", "5573", "5574"), Object.keys(metadata.fieldDescriptions).length > 0)))) {
      if (stryMutAct_9fa48("5575")) {
        {}
      } else {
        stryCov_9fa48("5575");
        doc += `## Field Descriptions\n\n`;
        doc += `| Field | Description |\n`;
        doc += `|-------|-------------|\n`;
        Object.entries(metadata.fieldDescriptions).forEach(([field, description]) => {
          if (stryMutAct_9fa48("5579")) {
            {}
          } else {
            stryCov_9fa48("5579");
            doc += `| \`${field}\` | ${description} |\n`;
          }
        });
        doc += `\n`;
      }
    }

    // Footer
    doc += `---\n\n`;
    doc += `*This documentation was auto-generated from Canton Network ACS snapshot data.*\n`;
    doc += `*Template structures may vary between package versions.*\n`;
    return doc;
  }
}

/**
 * Analyzes JSON fields and generates documentation
 */
function analyzeFields(data: any, knownDescriptions: Record<string, string>, path: string = '', depth: number = 0): string {
  if (stryMutAct_9fa48("5586")) {
    {}
  } else {
    stryCov_9fa48("5586");
    let result = '';
    const indent = '  '.repeat(depth);
    if (stryMutAct_9fa48("5591") ? data === null && data === undefined : stryMutAct_9fa48("5590") ? false : stryMutAct_9fa48("5589") ? true : (stryCov_9fa48("5589", "5590", "5591"), (stryMutAct_9fa48("5593") ? data !== null : stryMutAct_9fa48("5592") ? false : (stryCov_9fa48("5592", "5593"), data === null)) || (stryMutAct_9fa48("5595") ? data !== undefined : stryMutAct_9fa48("5594") ? false : (stryCov_9fa48("5594", "5595"), data === undefined)))) {
      if (stryMutAct_9fa48("5596")) {
        {}
      } else {
        stryCov_9fa48("5596");
        return `${indent}- \`${stryMutAct_9fa48("5600") ? path && 'root' : stryMutAct_9fa48("5599") ? false : stryMutAct_9fa48("5598") ? true : (stryCov_9fa48("5598", "5599", "5600"), path || 'root')}\`: null\n`;
      }
    }
    if (stryMutAct_9fa48("5603") ? false : stryMutAct_9fa48("5602") ? true : (stryCov_9fa48("5602", "5603"), Array.isArray(data))) {
      if (stryMutAct_9fa48("5604")) {
        {}
      } else {
        stryCov_9fa48("5604");
        result += `${indent}- \`${stryMutAct_9fa48("5608") ? path && 'root' : stryMutAct_9fa48("5607") ? false : stryMutAct_9fa48("5606") ? true : (stryCov_9fa48("5606", "5607", "5608"), path || 'root')}\` **(array)**: Contains ${data.length} item(s)\n`;
        if (stryMutAct_9fa48("5613") ? data.length <= 0 : stryMutAct_9fa48("5612") ? data.length >= 0 : stryMutAct_9fa48("5611") ? false : stryMutAct_9fa48("5610") ? true : (stryCov_9fa48("5610", "5611", "5612", "5613"), data.length > 0)) {
          if (stryMutAct_9fa48("5614")) {
            {}
          } else {
            stryCov_9fa48("5614");
            stryMutAct_9fa48("5615") ? result -= analyzeFields(data[0], knownDescriptions, `${path}[0]`, depth + 1) : (stryCov_9fa48("5615"), result += analyzeFields(data[0], knownDescriptions, `${path}[0]`, stryMutAct_9fa48("5617") ? depth - 1 : (stryCov_9fa48("5617"), depth + 1)));
          }
        }
        return result;
      }
    }
    if (stryMutAct_9fa48("5620") ? typeof data !== 'object' : stryMutAct_9fa48("5619") ? false : stryMutAct_9fa48("5618") ? true : (stryCov_9fa48("5618", "5619", "5620"), typeof data === 'object')) {
      if (stryMutAct_9fa48("5622")) {
        {}
      } else {
        stryCov_9fa48("5622");
        if (stryMutAct_9fa48("5624") ? false : stryMutAct_9fa48("5623") ? true : (stryCov_9fa48("5623", "5624"), path)) {
          if (stryMutAct_9fa48("5625")) {
            {}
          } else {
            stryCov_9fa48("5625");
            result += `${indent}- \`${path}\` **(object)**:\n`;
          }
        }
        Object.entries(data).forEach(([key, value]) => {
          if (stryMutAct_9fa48("5627")) {
            {}
          } else {
            stryCov_9fa48("5627");
            const fieldPath = path ? `${path}.${key}` : key;
            const description = knownDescriptions[key] ? ` - ${knownDescriptions[key]}` : '';
            if (stryMutAct_9fa48("5633") ? value !== null : stryMutAct_9fa48("5632") ? false : stryMutAct_9fa48("5631") ? true : (stryCov_9fa48("5631", "5632", "5633"), value === null)) {
              if (stryMutAct_9fa48("5634")) {
                {}
              } else {
                stryCov_9fa48("5634");
                result += `${indent}  - \`${key}\`: null${description}\n`;
              }
            } else if (stryMutAct_9fa48("5637") ? false : stryMutAct_9fa48("5636") ? true : (stryCov_9fa48("5636", "5637"), Array.isArray(value))) {
              if (stryMutAct_9fa48("5638")) {
                {}
              } else {
                stryCov_9fa48("5638");
                result += `${indent}  - \`${key}\` **(array)**: ${value.length} item(s)${description}\n`;
                if (stryMutAct_9fa48("5642") ? value.length > 0 || typeof value[0] === 'object' : stryMutAct_9fa48("5641") ? false : stryMutAct_9fa48("5640") ? true : (stryCov_9fa48("5640", "5641", "5642"), (stryMutAct_9fa48("5645") ? value.length <= 0 : stryMutAct_9fa48("5644") ? value.length >= 0 : stryMutAct_9fa48("5643") ? true : (stryCov_9fa48("5643", "5644", "5645"), value.length > 0)) && (stryMutAct_9fa48("5647") ? typeof value[0] !== 'object' : stryMutAct_9fa48("5646") ? true : (stryCov_9fa48("5646", "5647"), typeof value[0] === 'object')))) {
                  if (stryMutAct_9fa48("5649")) {
                    {}
                  } else {
                    stryCov_9fa48("5649");
                    stryMutAct_9fa48("5650") ? result -= analyzeFields(value[0], knownDescriptions, `${fieldPath}[0]`, depth + 2) : (stryCov_9fa48("5650"), result += analyzeFields(value[0], knownDescriptions, `${fieldPath}[0]`, stryMutAct_9fa48("5652") ? depth - 2 : (stryCov_9fa48("5652"), depth + 2)));
                  }
                }
              }
            } else if (stryMutAct_9fa48("5655") ? typeof value !== 'object' : stryMutAct_9fa48("5654") ? false : stryMutAct_9fa48("5653") ? true : (stryCov_9fa48("5653", "5654", "5655"), typeof value === 'object')) {
              if (stryMutAct_9fa48("5657")) {
                {}
              } else {
                stryCov_9fa48("5657");
                result += `${indent}  - \`${key}\` **(object)**:${description}\n`;
                stryMutAct_9fa48("5659") ? result -= analyzeFields(value, knownDescriptions, fieldPath, depth + 2) : (stryCov_9fa48("5659"), result += analyzeFields(value, knownDescriptions, fieldPath, stryMutAct_9fa48("5660") ? depth - 2 : (stryCov_9fa48("5660"), depth + 2)));
              }
            } else {
              if (stryMutAct_9fa48("5661")) {
                {}
              } else {
                stryCov_9fa48("5661");
                const valueType = typeof value;
                const sampleValue = (stryMutAct_9fa48("5664") ? typeof value === 'string' || value.length > 50 : stryMutAct_9fa48("5663") ? false : stryMutAct_9fa48("5662") ? true : (stryCov_9fa48("5662", "5663", "5664"), (stryMutAct_9fa48("5666") ? typeof value !== 'string' : stryMutAct_9fa48("5665") ? true : (stryCov_9fa48("5665", "5666"), typeof value === 'string')) && (stryMutAct_9fa48("5670") ? value.length <= 50 : stryMutAct_9fa48("5669") ? value.length >= 50 : stryMutAct_9fa48("5668") ? true : (stryCov_9fa48("5668", "5669", "5670"), value.length > 50)))) ? `"${stryMutAct_9fa48("5672") ? value : (stryCov_9fa48("5672"), value.substring(0, 50))}..."` : JSON.stringify(value);
                result += `${indent}  - \`${key}\` **(${valueType})**: ${sampleValue}${description}\n`;
              }
            }
          }
        });
        return result;
      }
    }
    return `${indent}- \`${path}\` **(${typeof data})**: ${JSON.stringify(data)}\n`;
  }
}

/**
 * Creates a downloadable blob from markdown content
 */
export function downloadMarkdown(content: string, filename: string): void {
  if (stryMutAct_9fa48("5675")) {
    {}
  } else {
    stryCov_9fa48("5675");
    const blob = new Blob(stryMutAct_9fa48("5676") ? [] : (stryCov_9fa48("5676"), [content]), {
      type: 'text/markdown;charset=utf-8'
    });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  }
}

/**
 * Generates a filename-safe version of a template ID
 */
export function getTemplateFilename(templateId: string): string {
  if (stryMutAct_9fa48("5680")) {
    {}
  } else {
    stryCov_9fa48("5680");
    const parts = templateId.split(':');
    const templateName = stryMutAct_9fa48("5682") ? parts.join('-') : (stryCov_9fa48("5682"), parts.slice(stryMutAct_9fa48("5683") ? +2 : (stryCov_9fa48("5683"), -2)).join('-'));
    return `${templateName}-documentation.md`;
  }
}