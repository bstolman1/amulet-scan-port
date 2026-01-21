/**
 * Template Documentation Generator
 * 
 * Generates comprehensive documentation for Canton Network templates,
 * explaining the JSON structure and purpose of each template type.
 */

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
    useCases: [
      'Storing value on the network',
      'Paying transaction fees',
      'Rewarding validators and super validators',
      'Participating in governance decisions',
    ],
    fieldDescriptions: {
      'dso': 'The DSO (Decentralized Synchronizer Operator) party that manages the network',
      'owner': 'The party that owns this Amulet',
      'amount': 'The numeric amount of Amulet tokens (with high precision)',
      'createdAt': 'Round number when this Amulet was created',
      'ratePerRound': 'The holding fee rate applied per round',
    },
  },

  'Splice.Amulet:LockedAmulet': {
    category: 'Core Currency',
    purpose: 'Represents Amulet tokens that are locked for a specific purpose',
    description: 'LockedAmulet contracts represent Amulet tokens that have been locked, typically for time-based vesting, collateral, or other purposes requiring temporary immobilization of funds. The lock specifies conditions under which the Amulet can be released.',
    useCases: [
      'Vesting schedules for rewards',
      'Collateral for operations',
      'Time-locked payments',
      'Escrow arrangements',
    ],
    fieldDescriptions: {
      'dso': 'The DSO party managing the network',
      'owner': 'The party that owns the locked Amulet',
      'amulet': 'The underlying Amulet being locked',
      'lock': 'The lock conditions specifying when/how the Amulet can be released',
      'timedLock': 'Optional time-based lock with expiration round',
    },
  },

  'Splice.Amulet:ValidatorRight': {
    category: 'Validator Operations',
    purpose: 'Grants rights to operate as a validator on the network',
    description: 'ValidatorRight contracts authorize a party to act as a validator for a specific user. Validators process transactions and earn rewards for their participation in the network consensus.',
    useCases: [
      'Authorizing validator operations',
      'Linking users to their chosen validator',
      'Validator reward distribution',
    ],
    fieldDescriptions: {
      'dso': 'The DSO party',
      'user': 'The user party being served by this validator',
      'validator': 'The validator party authorized to validate for this user',
    },
  },

  'Splice.Amulet:AppRewardCoupon': {
    category: 'Rewards',
    purpose: 'Represents unclaimed rewards earned by application providers',
    description: 'AppRewardCoupon contracts accumulate rewards for applications that facilitate transactions on the network. These coupons can be redeemed for Amulet tokens.',
    useCases: [
      'Tracking app provider rewards',
      'Incentivizing application development',
      'Reward redemption',
    ],
    fieldDescriptions: {
      'dso': 'The DSO party',
      'provider': 'The application provider earning the reward',
      'round': 'The mining round in which this reward was earned',
      'amount': 'The reward amount in Amulet units',
      'featured': 'Whether this is from a featured app with enhanced rewards',
    },
  },

  'Splice.Amulet:SvRewardCoupon': {
    category: 'Rewards',
    purpose: 'Represents unclaimed rewards for Super Validators',
    description: 'SvRewardCoupon contracts track rewards earned by Super Validators (SVs) for their role in network governance and infrastructure. SVs earn rewards for voting, maintaining uptime, and other governance activities.',
    useCases: [
      'Tracking SV governance rewards',
      'Incentivizing network participation',
      'SV reward claiming',
    ],
    fieldDescriptions: {
      'dso': 'The DSO party',
      'sv': 'The Super Validator party earning the reward',
      'round': 'The round in which this reward was earned',
      'weight': 'The SV weight determining reward share',
    },
  },

  'Splice.Amulet:ValidatorRewardCoupon': {
    category: 'Rewards',
    purpose: 'Represents unclaimed rewards for validators',
    description: 'ValidatorRewardCoupon contracts track rewards earned by validators for processing transactions and maintaining network consensus.',
    useCases: [
      'Tracking validator rewards',
      'Incentivizing validator participation',
      'Validator reward claiming',
    ],
    fieldDescriptions: {
      'dso': 'The DSO party',
      'user': 'The user whose transactions generated this reward',
      'validator': 'The validator earning the reward',
      'round': 'The round in which this reward was earned',
    },
  },

  'Splice.Amulet:UnclaimedReward': {
    category: 'Rewards',
    purpose: 'Represents rewards that have not yet been claimed',
    description: 'UnclaimedReward contracts represent rewards that are available for claiming but have not yet been converted to Amulet tokens.',
    useCases: [
      'Tracking pending rewards',
      'Reward distribution management',
    ],
    fieldDescriptions: {
      'dso': 'The DSO party',
      'amount': 'The unclaimed reward amount',
    },
  },

  'Splice.Amulet:FeaturedAppRight': {
    category: 'Applications',
    purpose: 'Grants featured status to an application',
    description: 'FeaturedAppRight contracts designate applications as "featured" on the network, typically granting enhanced visibility and potentially higher reward rates for facilitating transactions.',
    useCases: [
      'Promoting high-quality applications',
      'Enhanced reward rates',
      'Application discovery',
    ],
    fieldDescriptions: {
      'dso': 'The DSO party',
      'provider': 'The application provider',
    },
  },

  'Splice.Amulet:FeaturedAppActivityMarker': {
    category: 'Applications',
    purpose: 'Tracks activity from featured applications',
    description: 'FeaturedAppActivityMarker contracts record activity from featured applications, used for calculating enhanced rewards.',
    useCases: [
      'Activity tracking for featured apps',
      'Reward calculation',
    ],
    fieldDescriptions: {
      'dso': 'The DSO party',
      'provider': 'The featured app provider',
    },
  },

  // Validator License templates
  'Splice.ValidatorLicense:ValidatorLicense': {
    category: 'Validator Operations',
    purpose: 'Authorizes a party to operate as a validator',
    description: 'ValidatorLicense contracts grant parties the authority to operate as validators on the Canton Network. Validators are responsible for processing transactions and maintaining network consensus.',
    useCases: [
      'Validator registration',
      'Validator authorization',
      'Network participation rights',
    ],
    fieldDescriptions: {
      'dso': 'The DSO party',
      'validator': 'The party authorized to validate',
      'sponsor': 'The SV that sponsored this validator',
      'faucetState': 'State of the validator faucet for onboarding',
    },
  },

  'Splice.ValidatorLicense:ValidatorFaucetCoupon': {
    category: 'Validator Operations',
    purpose: 'Provides initial Amulet for new validators',
    description: 'ValidatorFaucetCoupon contracts allow new validators to receive initial Amulet tokens to begin operations on the network.',
    useCases: [
      'Validator onboarding',
      'Initial token distribution',
    ],
    fieldDescriptions: {
      'dso': 'The DSO party',
      'validator': 'The validator receiving the faucet',
    },
  },

  'Splice.ValidatorLicense:ValidatorLivenessActivityRecord': {
    category: 'Validator Operations',
    purpose: 'Records validator uptime and activity',
    description: 'ValidatorLivenessActivityRecord contracts track validator uptime and activity, used for reward calculations and network health monitoring.',
    useCases: [
      'Uptime tracking',
      'Reward calculation',
      'Network health monitoring',
    ],
    fieldDescriptions: {
      'validator': 'The validator being tracked',
      'round': 'The round of activity',
      'domain': 'The synchronizer domain',
    },
  },

  // DSO/Governance templates
  'Splice.DsoRules:DsoRules': {
    category: 'Governance',
    purpose: 'Defines the core rules for DSO operation',
    description: 'DsoRules is the central governance contract that defines how the Decentralized Synchronizer Operator functions. It contains network parameters, fee schedules, SV weights, and governance rules.',
    useCases: [
      'Network parameter configuration',
      'Fee schedule definition',
      'SV weight management',
      'Governance process rules',
    ],
    fieldDescriptions: {
      'dso': 'The DSO party identifier',
      'svs': 'Map of Super Validators and their metadata',
      'config': 'Current network configuration parameters',
      'epoch': 'Current governance epoch',
    },
  },

  'Splice.DsoRules:VoteRequest': {
    category: 'Governance',
    purpose: 'Represents a governance proposal awaiting votes',
    description: 'VoteRequest contracts represent proposals submitted for governance voting. SVs vote on these proposals to approve or reject changes to network parameters, SV membership, or other governance actions.',
    useCases: [
      'Proposing network changes',
      'SV membership changes',
      'Parameter updates',
      'Governance decisions',
    ],
    fieldDescriptions: {
      'dso': 'The DSO party',
      'requester': 'The party that submitted the proposal',
      'action': 'The proposed action to be executed if approved',
      'reason': 'The reason/justification for the proposal',
      'votes': 'Current votes from SVs',
      'trackingCid': 'Optional CID linking to previous related contracts',
      'expiresAt': 'When the vote expires',
    },
  },

  'Splice.DsoRules:Confirmation': {
    category: 'Governance',
    purpose: 'Records confirmation of governance actions',
    description: 'Confirmation contracts record that a governance action has been confirmed and is pending execution.',
    useCases: [
      'Tracking confirmed proposals',
      'Governance audit trail',
    ],
    fieldDescriptions: {
      'dso': 'The DSO party',
      'action': 'The confirmed action',
      'confirmedBy': 'Parties that confirmed',
    },
  },

  'Splice.DsoRules:ElectionRequest': {
    category: 'Governance',
    purpose: 'Manages SV leader election process',
    description: 'ElectionRequest contracts manage the process of electing an SV leader for specific epochs or functions.',
    useCases: [
      'Leader election',
      'Epoch transitions',
    ],
    fieldDescriptions: {
      'dso': 'The DSO party',
      'epoch': 'The election epoch',
      'ranking': 'Current ranking of candidates',
    },
  },

  // DSO SV State templates
  'Splice.DSO.SvState:SvNodeState': {
    category: 'SV Operations',
    purpose: 'Tracks the operational state of an SV node',
    description: 'SvNodeState contracts track the current operational state of Super Validator nodes, including their network connectivity and synchronization status.',
    useCases: [
      'SV monitoring',
      'Network health tracking',
      'Failover management',
    ],
    fieldDescriptions: {
      'dso': 'The DSO party',
      'sv': 'The Super Validator',
      'state': 'Current node state',
    },
  },

  'Splice.DSO.SvState:SvRewardState': {
    category: 'SV Operations',
    purpose: 'Tracks accumulated SV rewards',
    description: 'SvRewardState contracts track the accumulated rewards for Super Validators over time.',
    useCases: [
      'Reward tracking',
      'SV incentive management',
    ],
    fieldDescriptions: {
      'dso': 'The DSO party',
      'sv': 'The Super Validator',
      'rewardState': 'Current reward accumulation state',
    },
  },

  'Splice.DSO.SvState:SvStatusReport': {
    category: 'SV Operations',
    purpose: 'Records SV status reports for monitoring',
    description: 'SvStatusReport contracts contain status reports submitted by Super Validators for network monitoring and health checks.',
    useCases: [
      'SV health monitoring',
      'Network diagnostics',
      'Uptime verification',
    ],
    fieldDescriptions: {
      'dso': 'The DSO party',
      'sv': 'The reporting Super Validator',
      'report': 'The status report content',
      'round': 'The round of the report',
    },
  },

  'Splice.DSO.AmuletPrice:AmuletPriceVote': {
    category: 'Governance',
    purpose: 'Records SV votes on Amulet price',
    description: 'AmuletPriceVote contracts record Super Validator votes on the USD price of Amulet, used to determine the reference exchange rate.',
    useCases: [
      'Price oracle voting',
      'Exchange rate determination',
    ],
    fieldDescriptions: {
      'dso': 'The DSO party',
      'sv': 'The voting Super Validator',
      'price': 'The voted price in USD',
      'round': 'The voting round',
    },
  },

  // Amulet Rules templates
  'Splice.AmuletRules:AmuletRules': {
    category: 'Network Rules',
    purpose: 'Defines rules for Amulet token operations',
    description: 'AmuletRules contracts define the operational rules for Amulet tokens, including transfer rules, fee calculations, and token lifecycle management.',
    useCases: [
      'Token operation rules',
      'Fee calculation',
      'Transfer validation',
    ],
    fieldDescriptions: {
      'dso': 'The DSO party',
      'configSchedule': 'Schedule of configuration changes',
    },
  },

  'Splice.AmuletRules:TransferPreapproval': {
    category: 'Transfers',
    purpose: 'Pre-authorizes a future transfer',
    description: 'TransferPreapproval contracts authorize transfers before they occur, enabling scheduled or conditional transfers.',
    useCases: [
      'Scheduled payments',
      'Conditional transfers',
      'Pre-authorized withdrawals',
    ],
    fieldDescriptions: {
      'dso': 'The DSO party',
      'sender': 'The sender authorizing the transfer',
      'receiver': 'The intended receiver',
      'provider': 'The app provider facilitating the transfer',
    },
  },

  'Splice.AmuletRules:ExternalPartySetupProposal': {
    category: 'External Parties',
    purpose: 'Proposes setup of an external party',
    description: 'ExternalPartySetupProposal contracts propose the onboarding of external parties to the network.',
    useCases: [
      'External party onboarding',
      'Party setup workflows',
    ],
    fieldDescriptions: {
      'dso': 'The DSO party',
      'party': 'The proposed external party',
    },
  },

  // External Party templates
  'Splice.ExternalPartyAmuletRules:ExternalPartyAmuletRules': {
    category: 'External Parties',
    purpose: 'Defines rules for external party operations',
    description: 'ExternalPartyAmuletRules contracts define how external parties can interact with the Amulet network.',
    useCases: [
      'External party integration',
      'Cross-network operations',
    ],
    fieldDescriptions: {
      'dso': 'The DSO party',
      'party': 'The external party',
    },
  },

  'Splice.ExternalPartyAmuletRules:TransferCommand': {
    category: 'Transfers',
    purpose: 'Commands a transfer from an external party',
    description: 'TransferCommand contracts represent transfer instructions from external parties.',
    useCases: [
      'External party transfers',
      'Automated payments',
    ],
    fieldDescriptions: {
      'dso': 'The DSO party',
      'sender': 'The sending party',
      'receiver': 'The receiving party',
      'amount': 'The transfer amount',
    },
  },

  'Splice.ExternalPartyAmuletRules:TransferCommandCounter': {
    category: 'Transfers',
    purpose: 'Tracks transfer command sequence numbers',
    description: 'TransferCommandCounter contracts track sequence numbers for external party transfers to prevent replay attacks.',
    useCases: [
      'Transfer sequencing',
      'Replay attack prevention',
    ],
    fieldDescriptions: {
      'dso': 'The DSO party',
      'party': 'The external party',
      'counter': 'Current sequence number',
    },
  },

  // Round templates
  'Splice.Round:OpenMiningRound': {
    category: 'Mining Rounds',
    purpose: 'Represents a currently open mining round',
    description: 'OpenMiningRound contracts represent mining rounds that are currently accepting transactions and generating rewards.',
    useCases: [
      'Active round tracking',
      'Transaction processing',
      'Reward accumulation',
    ],
    fieldDescriptions: {
      'dso': 'The DSO party',
      'round': 'The round number',
      'opensAt': 'When this round opened',
      'targetClosesAt': 'Expected closing time',
    },
  },

  'Splice.Round:ClosedMiningRound': {
    category: 'Mining Rounds',
    purpose: 'Represents a completed mining round',
    description: 'ClosedMiningRound contracts represent mining rounds that have completed and whose rewards are being distributed.',
    useCases: [
      'Historical round data',
      'Reward distribution',
      'Audit trail',
    ],
    fieldDescriptions: {
      'dso': 'The DSO party',
      'round': 'The round number',
      'issuedCC': 'Total CC issued in this round',
      'optBurnedCC': 'Optional burned CC amount',
    },
  },

  'Splice.Round:IssuingMiningRound': {
    category: 'Mining Rounds',
    purpose: 'Represents a round currently issuing rewards',
    description: 'IssuingMiningRound contracts represent rounds in the process of issuing rewards to participants.',
    useCases: [
      'Reward issuance tracking',
      'Distribution management',
    ],
    fieldDescriptions: {
      'dso': 'The DSO party',
      'round': 'The round number',
      'issuanceConfig': 'Configuration for reward issuance',
    },
  },

  'Splice.Round:SummarizingMiningRound': {
    category: 'Mining Rounds',
    purpose: 'Represents a round being summarized',
    description: 'SummarizingMiningRound contracts represent rounds that are being aggregated for final reporting.',
    useCases: [
      'Round finalization',
      'Summary generation',
    ],
    fieldDescriptions: {
      'dso': 'The DSO party',
      'round': 'The round number',
    },
  },

  // ANS templates
  'Splice.Ans:AnsEntry': {
    category: 'ANS (Naming Service)',
    purpose: 'Represents a registered name in the ANS',
    description: 'AnsEntry contracts represent registered names in the Amulet Naming Service, providing human-readable aliases for party identifiers.',
    useCases: [
      'Human-readable addresses',
      'Identity management',
      'Name resolution',
    ],
    fieldDescriptions: {
      'dso': 'The DSO party',
      'user': 'The owner of the name',
      'name': 'The registered name',
      'url': 'Optional URL associated with the name',
      'description': 'Optional description',
      'expiresAt': 'When the registration expires',
    },
  },

  'Splice.Ans:AnsEntryContext': {
    category: 'ANS (Naming Service)',
    purpose: 'Provides context for ANS entries',
    description: 'AnsEntryContext contracts provide additional context and metadata for ANS entries.',
    useCases: [
      'Extended metadata',
      'Context management',
    ],
    fieldDescriptions: {
      'dso': 'The DSO party',
      'reference': 'Reference to the ANS entry',
    },
  },

  'Splice.Ans:AnsRules': {
    category: 'ANS (Naming Service)',
    purpose: 'Defines rules for ANS operations',
    description: 'AnsRules contracts define the operational rules for the Amulet Naming Service.',
    useCases: [
      'Name registration rules',
      'Pricing configuration',
    ],
    fieldDescriptions: {
      'dso': 'The DSO party',
      'config': 'ANS configuration',
    },
  },

  // Traffic templates
  'Splice.DecentralizedSynchronizer:MemberTraffic': {
    category: 'Network Traffic',
    purpose: 'Tracks member traffic on the synchronizer',
    description: 'MemberTraffic contracts track the traffic generated by network members, used for fee calculations and capacity management.',
    useCases: [
      'Traffic monitoring',
      'Fee calculation',
      'Capacity planning',
    ],
    fieldDescriptions: {
      'dso': 'The DSO party',
      'member': 'The network member',
      'synchronizer': 'The synchronizer domain',
      'totalTrafficPurchased': 'Total traffic purchased',
      'totalTrafficConsumed': 'Total traffic used',
    },
  },

  // Subscription templates
  'Splice.Wallet.Subscriptions:Subscription': {
    category: 'Subscriptions',
    purpose: 'Represents an active subscription',
    description: 'Subscription contracts represent active recurring payment arrangements between parties.',
    useCases: [
      'Recurring payments',
      'Service subscriptions',
      'Automated billing',
    ],
    fieldDescriptions: {
      'dso': 'The DSO party',
      'subscriber': 'The subscribing party',
      'provider': 'The service provider',
      'subscriptionData': 'Subscription details and terms',
    },
  },

  'Splice.Wallet.Subscriptions:SubscriptionRequest': {
    category: 'Subscriptions',
    purpose: 'Represents a pending subscription request',
    description: 'SubscriptionRequest contracts represent subscription requests awaiting approval.',
    useCases: [
      'Subscription initiation',
      'Approval workflows',
    ],
    fieldDescriptions: {
      'dso': 'The DSO party',
      'subscriber': 'The requesting subscriber',
      'provider': 'The target provider',
    },
  },

  'Splice.Wallet.Subscriptions:SubscriptionIdleState': {
    category: 'Subscriptions',
    purpose: 'Represents a paused subscription',
    description: 'SubscriptionIdleState contracts represent subscriptions that are temporarily paused or idle.',
    useCases: [
      'Subscription management',
      'Pause functionality',
    ],
    fieldDescriptions: {
      'dso': 'The DSO party',
      'subscription': 'The paused subscription reference',
    },
  },

  // Transfer Instruction templates
  'Splice.AmuletTransferInstruction:AmuletTransferInstruction': {
    category: 'Transfers',
    purpose: 'Represents a transfer instruction',
    description: 'AmuletTransferInstruction contracts represent instructions to transfer Amulet tokens between parties.',
    useCases: [
      'Token transfers',
      'Payment instructions',
    ],
    fieldDescriptions: {
      'dso': 'The DSO party',
      'sender': 'The sending party',
      'receiver': 'The receiving party',
      'amount': 'The transfer amount',
    },
  },

  // Allocation templates
  'Splice.AmuletAllocation:AmuletAllocation': {
    category: 'Allocations',
    purpose: 'Represents allocated Amulet tokens',
    description: 'AmuletAllocation contracts represent Amulet tokens that have been allocated for specific purposes.',
    useCases: [
      'Token allocation',
      'Reserved funds',
    ],
    fieldDescriptions: {
      'dso': 'The DSO party',
      'owner': 'The allocation owner',
      'amount': 'The allocated amount',
    },
  },
};

/**
 * Generates a markdown documentation for a template
 */
export function generateTemplateDocumentation(
  templateId: string,
  sampleData: any,
  contractCount: number
): string {
  // Extract template key from full template ID
  const parts = templateId.split(':');
  const templateKey = parts.slice(-2).join(':');
  const modulePath = parts.slice(0, -1).join(':');
  
  // Find matching metadata (try different key formats)
  let metadata = TEMPLATE_METADATA[templateKey];
  if (!metadata) {
    // Try with module prefix
    const fullKey = parts.slice(-3).join('.');
    metadata = TEMPLATE_METADATA[fullKey];
  }
  if (!metadata) {
    // Try other format
    const altKey = templateId.includes('.') 
      ? templateId 
      : parts.slice(-3).join('.');
    metadata = TEMPLATE_METADATA[altKey];
  }

  // Get pages that use this template
  const suffix = templateId.split(':').slice(-3).join(':');
  const usedInPages = templatePageMap[suffix] || [];

  // Build the documentation
  let doc = `# ${templateKey} Template Documentation\n\n`;
  doc += `**Generated:** ${new Date().toISOString()}\n\n`;
  doc += `---\n\n`;

  // Template Overview
  doc += `## Overview\n\n`;
  doc += `- **Full Template ID:** \`${templateId}\`\n`;
  doc += `- **Module Path:** \`${modulePath}\`\n`;
  doc += `- **Current Contract Count:** ${contractCount.toLocaleString()}\n`;
  
  if (metadata) {
    doc += `- **Category:** ${metadata.category}\n`;
    doc += `- **Purpose:** ${metadata.purpose}\n`;
  }
  
  if (usedInPages.length > 0) {
    doc += `- **Used in Pages:** ${usedInPages.join(', ')}\n`;
  }
  
  doc += `\n`;

  // Description
  if (metadata?.description) {
    doc += `## Description\n\n`;
    doc += `${metadata.description}\n\n`;
  }

  // Use Cases
  if (metadata?.useCases && metadata.useCases.length > 0) {
    doc += `## Use Cases\n\n`;
    metadata.useCases.forEach(useCase => {
      doc += `- ${useCase}\n`;
    });
    doc += `\n`;
  }

  // JSON Structure Analysis
  doc += `## JSON Structure\n\n`;
  
  if (sampleData) {
    doc += `The following structure is derived from analyzing sample contract data:\n\n`;
    doc += "```json\n";
    doc += JSON.stringify(sampleData, null, 2);
    doc += "\n```\n\n";

    // Field Analysis
    doc += `## Field Analysis\n\n`;
    doc += analyzeFields(sampleData, metadata?.fieldDescriptions || {});
  } else {
    doc += `*No sample data available for this template.*\n\n`;
  }

  // Field Descriptions from metadata
  if (metadata?.fieldDescriptions && Object.keys(metadata.fieldDescriptions).length > 0) {
    doc += `## Field Descriptions\n\n`;
    doc += `| Field | Description |\n`;
    doc += `|-------|-------------|\n`;
    Object.entries(metadata.fieldDescriptions).forEach(([field, description]) => {
      doc += `| \`${field}\` | ${description} |\n`;
    });
    doc += `\n`;
  }

  // Footer
  doc += `---\n\n`;
  doc += `*This documentation was auto-generated from Canton Network ACS snapshot data.*\n`;
  doc += `*Template structures may vary between package versions.*\n`;

  return doc;
}

/**
 * Analyzes JSON fields and generates documentation
 */
function analyzeFields(data: any, knownDescriptions: Record<string, string>, path: string = '', depth: number = 0): string {
  let result = '';
  const indent = '  '.repeat(depth);

  if (data === null || data === undefined) {
    return `${indent}- \`${path || 'root'}\`: null\n`;
  }

  if (Array.isArray(data)) {
    result += `${indent}- \`${path || 'root'}\` **(array)**: Contains ${data.length} item(s)\n`;
    if (data.length > 0) {
      result += analyzeFields(data[0], knownDescriptions, `${path}[0]`, depth + 1);
    }
    return result;
  }

  if (typeof data === 'object') {
    if (path) {
      result += `${indent}- \`${path}\` **(object)**:\n`;
    }
    Object.entries(data).forEach(([key, value]) => {
      const fieldPath = path ? `${path}.${key}` : key;
      const description = knownDescriptions[key] ? ` - ${knownDescriptions[key]}` : '';
      
      if (value === null) {
        result += `${indent}  - \`${key}\`: null${description}\n`;
      } else if (Array.isArray(value)) {
        result += `${indent}  - \`${key}\` **(array)**: ${value.length} item(s)${description}\n`;
        if (value.length > 0 && typeof value[0] === 'object') {
          result += analyzeFields(value[0], knownDescriptions, `${fieldPath}[0]`, depth + 2);
        }
      } else if (typeof value === 'object') {
        result += `${indent}  - \`${key}\` **(object)**:${description}\n`;
        result += analyzeFields(value, knownDescriptions, fieldPath, depth + 2);
      } else {
        const valueType = typeof value;
        const sampleValue = typeof value === 'string' && value.length > 50 
          ? `"${value.substring(0, 50)}..."` 
          : JSON.stringify(value);
        result += `${indent}  - \`${key}\` **(${valueType})**: ${sampleValue}${description}\n`;
      }
    });
    return result;
  }

  return `${indent}- \`${path}\` **(${typeof data})**: ${JSON.stringify(data)}\n`;
}

/**
 * Creates a downloadable blob from markdown content
 */
export function downloadMarkdown(content: string, filename: string): void {
  const blob = new Blob([content], { type: 'text/markdown;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
}

/**
 * Generates a filename-safe version of a template ID
 */
export function getTemplateFilename(templateId: string): string {
  const parts = templateId.split(':');
  const templateName = parts.slice(-2).join('-');
  return `${templateName}-documentation.md`;
}
