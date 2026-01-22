/**
 * Mock API Responses for Pipeline Integration Tests
 * 
 * These fixtures simulate Canton Scan API responses for end-to-end testing.
 */

// Sample migration info
export const MOCK_MIGRATIONS = [0, 1];

// Sample backfill update with transaction
export const MOCK_BACKFILL_TRANSACTION = {
  update_id: 'upd-txn-001',
  migration_id: 0,
  transaction: {
    update_id: 'upd-txn-001',
    synchronizer_id: 'sync-global-001',
    workflow_id: 'wf-001',
    command_id: 'cmd-001',
    offset: '12345',
    record_time: '2024-06-15T10:30:00.000Z',
    effective_at: '2024-06-15T10:30:00.000Z',
    root_event_ids: ['upd-txn-001:0', 'upd-txn-001:1'],
    trace_context: { traceId: 'trace-abc', spanId: 'span-123' },
    events_by_id: {
      'upd-txn-001:0': {
        created_event: {
          event_id: 'upd-txn-001:0',
          contract_id: '00abc123::amulet-contract-1',
          template_id: 'splice-amulet:Splice.Amulet:Amulet',
          package_name: 'splice-amulet',
          signatories: ['DSO::party1'],
          observers: ['party2'],
          create_arguments: {
            owner: 'DSO::party1',
            amount: { initialAmount: '1000000000', createdAt: { microseconds: '1718445000000000' } },
            lock: null,
          },
          created_at: '2024-06-15T10:30:00.000Z',
        },
      },
      'upd-txn-001:1': {
        created_event: {
          event_id: 'upd-txn-001:1',
          contract_id: '00abc123::validator-license-1',
          template_id: 'splice-validator-license:Splice.ValidatorLicense:ValidatorLicense',
          package_name: 'splice-validator-license',
          signatories: ['validator-party'],
          observers: ['DSO::party1'],
          create_arguments: {
            validator: 'validator-party',
            sponsor: 'DSO::party1',
            validatorVersion: '0.3.0',
          },
          created_at: '2024-06-15T10:30:00.000Z',
        },
      },
    },
  },
};

// Sample backfill update with exercised event (with children)
export const MOCK_BACKFILL_EXERCISE = {
  update_id: 'upd-txn-002',
  migration_id: 0,
  transaction: {
    update_id: 'upd-txn-002',
    synchronizer_id: 'sync-global-001',
    offset: '12346',
    record_time: '2024-06-15T10:31:00.000Z',
    effective_at: '2024-06-15T10:31:00.000Z',
    root_event_ids: ['upd-txn-002:0'],
    events_by_id: {
      'upd-txn-002:0': {
        exercised_event: {
          event_id: 'upd-txn-002:0',
          contract_id: '00abc123::amulet-contract-1',
          template_id: 'splice-amulet:Splice.Amulet:Amulet',
          choice: 'Amulet_Transfer',
          consuming: true,
          acting_parties: ['DSO::party1'],
          child_event_ids: ['upd-txn-002:1', 'upd-txn-002:2'],
          choice_argument: {
            recipient: 'party2',
            amount: '500000000',
          },
          exercise_result: { success: true },
        },
      },
      'upd-txn-002:1': {
        archived_event: {
          event_id: 'upd-txn-002:1',
          contract_id: '00abc123::amulet-contract-1',
          template_id: 'splice-amulet:Splice.Amulet:Amulet',
        },
      },
      'upd-txn-002:2': {
        created_event: {
          event_id: 'upd-txn-002:2',
          contract_id: '00abc123::amulet-contract-2',
          template_id: 'splice-amulet:Splice.Amulet:Amulet',
          signatories: ['party2'],
          observers: [],
          create_arguments: {
            owner: 'party2',
            amount: { initialAmount: '500000000', createdAt: { microseconds: '1718445060000000' } },
          },
          created_at: '2024-06-15T10:31:00.000Z',
        },
      },
    },
  },
};

// Sample reassignment update
export const MOCK_BACKFILL_REASSIGNMENT = {
  update_id: 'upd-reassign-001',
  migration_id: 0,
  reassignment: {
    update_id: 'upd-reassign-001',
    synchronizer_id: 'sync-global-001',
    offset: '12347',
    record_time: '2024-06-15T10:32:00.000Z',
    kind: 'assign',
    source: 'sync-source-001',
    target: 'sync-target-001',
    unassign_id: 'unassign-123',
    submitter: 'party1',
    counter: 5,
    contract_id: '00abc123::reassigned-contract',
    template_id: 'splice-amulet:Splice.Amulet:Amulet',
  },
};

// Sample governance update (VoteRequest)
export const MOCK_GOVERNANCE_UPDATE = {
  update_id: 'upd-gov-001',
  migration_id: 0,
  transaction: {
    update_id: 'upd-gov-001',
    synchronizer_id: 'sync-global-001',
    offset: '12348',
    record_time: '2024-06-15T10:33:00.000Z',
    effective_at: '2024-06-15T10:33:00.000Z',
    root_event_ids: ['upd-gov-001:0'],
    events_by_id: {
      'upd-gov-001:0': {
        created_event: {
          event_id: 'upd-gov-001:0',
          contract_id: '00gov001::vote-request-1',
          template_id: 'splice-dso:Splice.DsoRules:VoteRequest',
          package_name: 'splice-dso',
          signatories: ['DSO::dso-party'],
          observers: [],
          create_arguments: {
            requestor: 'sv-party-1',
            action: {
              tag: 'ARC_AmuletRules',
              value: {
                amuletRulesChange: {
                  tag: 'ARCV_SetMaxNumInputs',
                  value: { newMaxNumInputs: 100 },
                },
              },
            },
            reason: {
              url: 'https://governance.example.com/proposal/1',
              body: 'Increase max inputs for better throughput',
            },
            voteBefore: '2024-06-20T10:00:00.000Z',
            votes: [],
          },
          created_at: '2024-06-15T10:33:00.000Z',
        },
      },
    },
  },
};

// All backfill updates for a complete test batch
export const MOCK_BACKFILL_BATCH = [
  MOCK_BACKFILL_TRANSACTION,
  MOCK_BACKFILL_EXERCISE,
  MOCK_BACKFILL_REASSIGNMENT,
  MOCK_GOVERNANCE_UPDATE,
];

// Sample ACS contract events
export const MOCK_ACS_AMULET = {
  event_id: 'acs-evt-001',
  contract_id: '00acs001::amulet-1',
  template_id: 'splice-amulet:Splice.Amulet:Amulet',
  package_name: 'splice-amulet',
  signatories: ['DSO::owner-party'],
  observers: ['witness-party'],
  witness_parties: ['witness-party'],
  contract_key: { owner: 'DSO::owner-party' },
  create_arguments: {
    owner: 'DSO::owner-party',
    amount: { initialAmount: '2500000000', createdAt: { microseconds: '1718445000000000' } },
    lock: null,
  },
};

export const MOCK_ACS_VALIDATOR_LICENSE = {
  event_id: 'acs-evt-002',
  contract_id: '00acs002::validator-license-1',
  template_id: 'splice-validator-license:Splice.ValidatorLicense:ValidatorLicense',
  package_name: 'splice-validator-license',
  signatories: ['validator-party'],
  observers: ['DSO::dso-party'],
  create_arguments: {
    validator: 'validator-party',
    sponsor: 'DSO::dso-party',
    validatorVersion: '0.3.1',
    lastActiveAt: { microseconds: '1718445000000000' },
  },
};

export const MOCK_ACS_LOCKED_AMULET = {
  event_id: 'acs-evt-003',
  contract_id: '00acs003::locked-amulet-1',
  template_id: 'splice-amulet:Splice.Amulet:LockedAmulet',
  package_name: 'splice-amulet',
  signatories: ['DSO::owner-party'],
  observers: [],
  create_arguments: {
    amulet: {
      owner: 'DSO::owner-party',
      amount: { initialAmount: '500000000', createdAt: { microseconds: '1718445000000000' } },
    },
    lock: {
      expiresAt: { microseconds: '1718531400000000' },
      holders: ['lock-holder-party'],
    },
  },
};

export const MOCK_ACS_DSO_RULES = {
  event_id: 'acs-evt-004',
  contract_id: '00acs004::dso-rules-1',
  template_id: 'splice-dso:Splice.DsoRules:DsoRules',
  package_name: 'splice-dso',
  signatories: ['DSO::dso-party'],
  observers: [],
  create_arguments: {
    dso: 'DSO::dso-party',
    config: {
      numUnclaimedRewardsThreshold: 10,
      maxNumInputs: 100,
    },
    svs: ['sv-1', 'sv-2', 'sv-3'],
  },
};

export const MOCK_ACS_VOTE_REQUEST = {
  event_id: 'acs-evt-005',
  contract_id: '00acs005::vote-request-1',
  template_id: 'splice-dso:Splice.DsoRules:VoteRequest',
  package_name: 'splice-dso',
  signatories: ['DSO::dso-party'],
  observers: ['sv-1', 'sv-2'],
  create_arguments: {
    requestor: 'sv-1',
    action: { tag: 'ARC_DsoRules', value: {} },
    reason: { url: 'https://gov.example.com/1', body: 'Test proposal' },
    voteBefore: '2024-06-20T00:00:00.000Z',
    votes: [
      { sv: 'sv-1', accept: true, reason: { url: '', body: 'Approve' } },
    ],
  },
};

// All ACS contracts for a complete test snapshot
export const MOCK_ACS_BATCH = [
  MOCK_ACS_AMULET,
  MOCK_ACS_VALIDATOR_LICENSE,
  MOCK_ACS_LOCKED_AMULET,
  MOCK_ACS_DSO_RULES,
  MOCK_ACS_VOTE_REQUEST,
];

// API response wrapper for backfill endpoint
export function createBackfillResponse(updates, afterTimestamp = null) {
  return {
    updates,
    after: afterTimestamp,
  };
}

// API response wrapper for ACS endpoint
export function createACSResponse(createdEvents, nextPageToken = null) {
  return {
    created_events: createdEvents,
    next_page_token: nextPageToken,
  };
}

// Migration info response
export function createMigrationResponse(migrationId, recordTime) {
  return {
    migration_id: migrationId,
    record_time: recordTime,
  };
}
